//! `Take` (`ivm/take.ts`, spec `07` §3.6) — bounded top-N per partition (the
//! `LIMIT` operator). The **first real consumer of the [`Graph`] storage arena**
//! (a [`StorageId`] slot holding the per-partition `TakeState`).
//!
//! A faithful port of `take.ts`. Take keeps the first `limit` rows of its input —
//! per *partition* (a correlation key for a limited subquery; absent for a
//! top-level limit) — in input sort order, and maintains, per partition, a
//! `TakeState { size, bound }` where `bound` is the largest accepted row. It holds
//! the invariant **output size ≤ limit at all times, even mid-push**
//! (remove-before-add ordering). It also tracks a global `MAX_BOUND` (the max bound
//! across partitions) for the cross-partition fetch path.
//!
//! Like [`Skip`](crate::op::Skip), Take is *ordered* and forwards on its edge's own
//! [`Port`](crate::change::Port), so a limited relationship can feed a parent join.
//! Unlike Skip it is **stateful**: it reads/writes its [`StorageId`] slot through a
//! shared `&Graph` borrow, so the reentrant fetch-during-push composes for free.

use std::cell::{Cell, RefCell};
use std::cmp::Ordering;

use crate::change::{Basis, Change, Constraint, FetchRequest, Node, NodeStream, OutEdge, Start};
use crate::graph::{Graph, NodeId, StorageId};
use crate::op::partition::{
    constraint_matches_partition_key, encode_state_key, partition_key_unchanged,
};
use crate::storage::StorageValue;
use crate::value::{compare_rows, ColId, OwnedRow as Row, OwnedValue, Sort};

/// The storage key for the global max-bound slot (`take.ts:27` `MAX_BOUND_KEY`).
const MAX_BOUND_KEY: &str = "maxBound";

/// `Take`: ordered, bounded top-N per partition, backed by a [`StorageId`] slot.
pub struct Take {
    /// Upstream input (the connection, a `Skip`, or — once EXISTS lands — a join).
    pub input: NodeId,
    /// Handle into the [`Graph`] storage arena holding this op's per-partition
    /// `TakeState` (under `encode_state_key("take", …)`) + the `MAX_BOUND` row.
    pub storage: StorageId,
    /// The `LIMIT` count. `0` ⇒ the operator yields nothing and stores no state.
    pub limit: u32,
    /// The partition columns (the correlation **child** field of a limited
    /// relationship), or `None` for a top-level limit (a single global partition).
    pub partition_key: Option<Vec<ColId>>,
    /// Single-row fetch overlay for displacement pushes (`take.ts:62`,
    /// `#rowHiddenFromFetch`): while set, [`Take::fetch`] skips the row equal to it,
    /// hiding an in-flight row from a reentrant refetch. Cleared via a [`Drop`] guard
    /// (the JS `finally`).
    row_hidden_from_fetch: RefCell<Option<Row>>,
    /// The single downstream edge as a **port-carrying** [`OutEdge`] (like
    /// [`Skip`](crate::op::Skip)/[`Join`](crate::graph)): `Port::Single` to a
    /// terminal sink, `Port::JoinParent` when this Take feeds a relationship join's
    /// parent port (a limited relationship). Wired via
    /// [`Graph::set_output`](crate::graph::Graph::set_output) (Single) or
    /// [`Graph::set_out_edge`](crate::graph::Graph::set_out_edge) (explicit port).
    pub output: Cell<Option<OutEdge>>,
    /// The input sort, resolved at build time (the connection's completed,
    /// PK-including order). Take requires sorted input (`take.ts:74`); the
    /// comparator only ever reads these columns.
    pub sort: Sort,
}

/// Clears [`Take::row_hidden_from_fetch`] on drop — the RAII rendering of the JS
/// `#pushWithRowHiddenFromFetch` `finally` (`take.ts:681`), so the overlay is
/// cleared on early return / `?`. It is also cleared if the downstream push panics
/// ONLY in unwinding builds; under the shipping `panic = "abort"` client profile a
/// panic aborts the process and the overlay-clearing `Drop` does not run. See WS02.
struct HiddenGuard<'a>(&'a RefCell<Option<Row>>);
impl Drop for HiddenGuard<'_> {
    fn drop(&mut self) {
        *self.0.borrow_mut() = None;
    }
}

impl Take {
    pub fn new(
        input: NodeId,
        storage: StorageId,
        limit: u32,
        partition_key: Option<Vec<ColId>>,
        sort: Sort,
    ) -> Take {
        Take {
            input,
            storage,
            limit,
            partition_key,
            row_hidden_from_fetch: RefCell::new(None),
            output: Cell::new(None),
            sort,
        }
    }

    // --- storage helpers ----------------------------------------------------

    /// Read the `TakeState` (`size`, `bound`) at `key`, or `None` if the partition
    /// was never hydrated.
    fn get_state(&self, g: &Graph, key: &str) -> Option<(u32, Option<Row>)> {
        match g.storage(self.storage).get(key) {
            None => None,
            Some(StorageValue::Take { size, bound }) => Some((size, bound)),
            Some(other) => unreachable!("take state slot held {other:?}"),
        }
    }

    /// Read the global `MAX_BOUND` row (`None` until any partition sets a bound).
    fn get_max_bound(&self, g: &Graph) -> Option<Row> {
        match g.storage(self.storage).get(MAX_BOUND_KEY) {
            None => None,
            Some(StorageValue::Bound(r)) => Some(r),
            Some(other) => unreachable!("maxBound slot held {other:?}"),
        }
    }

    /// `#setTakeState` (`take.ts:686`): write the partition's `{size, bound}`, then
    /// bump `MAX_BOUND` iff the new `bound` exceeds the `max_bound` read by the
    /// caller (stale-within-a-push, matching the JS which reads it once per push).
    fn set_state(
        &self,
        g: &Graph,
        key: &str,
        size: u32,
        bound: Option<Row>,
        max_bound: Option<&Row>,
    ) {
        let st = g.storage(self.storage);
        st.set(
            key,
            StorageValue::Take {
                size,
                bound: bound.clone(),
            },
        );
        if let Some(b) = &bound {
            if max_bound.is_none_or(|mb| compare_rows(&self.sort, b, mb) == Ordering::Greater) {
                st.set(MAX_BOUND_KEY, StorageValue::Bound(b.clone()));
            }
        }
    }

    /// The take-state key for a partition identified by a **row** (`take.ts:710`
    /// `getTakeStateKey` over a `Row`). Unpartitioned ⇒ the single global key.
    fn key_from_row(&self, row: &Row) -> String {
        match &self.partition_key {
            None => encode_state_key("take", &[]),
            Some(pk) => {
                let vals: Vec<OwnedValue> = pk.iter().map(|&c| row.col(c).to_owned()).collect();
                encode_state_key("take", &vals)
            }
        }
    }

    /// The take-state key for a partition identified by a fetch **constraint**
    /// (`getTakeStateKey` over a `Constraint`). Unpartitioned ⇒ the global key.
    fn key_from_constraint(&self, c: Option<&Constraint>) -> String {
        match (&self.partition_key, c) {
            (Some(pk), Some(c)) => {
                let vals: Vec<OwnedValue> = pk
                    .iter()
                    .map(|&col| {
                        c.iter()
                            .find(|(cc, _)| *cc == col)
                            .map(|(_, v)| v.clone())
                            .unwrap_or(OwnedValue::Null)
                    })
                    .collect();
                encode_state_key("take", &vals)
            }
            // Unpartitioned (or partitioned-but-no-constraint, which the callers
            // never hit): the single global partition.
            _ => encode_state_key("take", &[]),
        }
    }

    /// The reentrant-fetch constraint for a push on `row` (`#getStateAndConstraint`,
    /// `take.ts:225`): the partition columns mapped to the row's values, or `None`
    /// when unpartitioned (the reentrant fetch is then a bare scan).
    fn constraint_for(&self, row: &Row) -> Option<Constraint> {
        self.partition_key
            .as_ref()
            .map(|pk| pk.iter().map(|&c| (c, row.col(c).to_owned())).collect())
    }

    /// Collect the first `n` nodes of a reentrant input fetch starting at `bound`
    /// (with `basis`/`reverse`/`constraint`). The Take push paths only ever need the
    /// first one or two, so this bounds the pull.
    fn fetch_n<'g>(
        &self,
        g: &'g Graph,
        bound: &Row,
        basis: Basis,
        constraint: &Option<Constraint>,
        reverse: bool,
        n: usize,
    ) -> Vec<Node<'g>> {
        let req = FetchRequest {
            start: Some(Start {
                row: bound.clone(),
                basis,
            }),
            constraint: constraint.clone(),
            reverse,
            ..Default::default()
        };
        g.fetch(self.input, &req).take(n).collect()
    }

    // --- fetch --------------------------------------------------------------

    /// Lazy pull (`take.ts:93`). Two regimes:
    /// 1. No partition key, or the request constraint matches the partition key:
    ///    bound by that single partition's `TakeState` (hydrating it on first sight).
    /// 2. Partitioned but the constraint is absent / on a different key (nested
    ///    subqueries): bound by `MAX_BOUND`, re-checking each row against its own
    ///    partition's bound.
    pub fn fetch<'g>(&'g self, g: &'g Graph, req: &FetchRequest) -> NodeStream<'g> {
        let partition_match = match &self.partition_key {
            None => true,
            Some(pk) => req
                .constraint
                .as_ref()
                .is_some_and(|c| constraint_matches_partition_key(c, pk)),
        };

        if partition_match {
            let key = self.key_from_constraint(req.constraint.as_ref());
            match self.get_state(g, &key) {
                None => return self.initial_fetch(g, req),
                // Empty partition (hydrated, no rows) ⇒ nothing.
                Some((_, None)) => return Box::new(std::iter::empty()),
                Some((_, Some(bound))) => {
                    // Stream input; stop at the first row past the bound; skip the
                    // in-flight `row_hidden_from_fetch` row if one is set.
                    let hidden = self.row_hidden_from_fetch.borrow().clone();
                    let nodes = g.fetch(self.input, req);
                    return Box::new(
                        nodes
                            .take_while(move |n| {
                                compare_rows(&self.sort, &bound, &n.row) != Ordering::Less
                            })
                            .filter(move |n| {
                                hidden.as_ref().is_none_or(|h| {
                                    compare_rows(&self.sort, h, &n.row) != Ordering::Equal
                                })
                            }),
                    );
                }
            }
        }

        // Partitioned, no matching constraint: bound by MAX_BOUND, re-check each
        // row against its own partition's bound (`take.ts:135`).
        let Some(max_bound) = self.get_max_bound(g) else {
            return Box::new(std::iter::empty());
        };
        let nodes = g.fetch(self.input, req);
        Box::new(
            nodes
                .take_while(move |n| {
                    compare_rows(&self.sort, &n.row, &max_bound) != Ordering::Greater
                })
                .filter_map(move |n| {
                    let key = self.key_from_row(&n.row);
                    match self.get_state(g, &key) {
                        Some((_, Some(bound)))
                            if compare_rows(&self.sort, &bound, &n.row) != Ordering::Less =>
                        {
                            Some(n)
                        }
                        _ => None,
                    }
                }),
        )
    }

    /// `#initialFetch` (`take.ts:158`): hydrate a partition from the top. Pulls the
    /// first `limit` rows (tracking `size`/`bound`), writes the take-state, and
    /// yields them. Eager (vs the JS lazy generator + `finally`): every real
    /// consumer fully drains a hydrate, so pulling up to `limit` up front yields an
    /// identical result and state while keeping the borrow plumbing simple. The JS
    /// `downstreamEarlyReturn` assert is therefore vacuous here.
    fn initial_fetch<'g>(&'g self, g: &'g Graph, req: &FetchRequest) -> NodeStream<'g> {
        debug_assert!(
            req.start.is_none(),
            "Take initial fetch: start must be None"
        );
        debug_assert!(!req.reverse, "Take initial fetch: reverse must be false");
        if self.limit == 0 {
            // limit 0 stores no state (`take.ts:162`); stays permanently empty.
            return Box::new(std::iter::empty());
        }
        let key = self.key_from_constraint(req.constraint.as_ref());
        debug_assert!(
            self.get_state(g, &key).is_none(),
            "Take initial fetch: state should be undefined"
        );

        let mut size = 0u32;
        let mut bound: Option<Row> = None;
        let mut out: Vec<Node<'g>> = Vec::with_capacity(self.limit as usize);
        for node in g.fetch(self.input, req) {
            bound = Some(node.row.clone());
            out.push(node);
            size += 1;
            if size == self.limit {
                break;
            }
        }
        let max_bound = self.get_max_bound(g);
        self.set_state(g, &key, size, bound, max_bound.as_ref());
        Box::new(out.into_iter())
    }

    /// Delete the per-partition slot identified by `constraint` (the parent→child
    /// correlation key), if this Take is partitioned and the constraint covers its
    /// partition key. Called by the relationship join when a parent **leaves a bounded
    /// parent view** (a parent `Remove` on `Port::JoinParent`): the child partition that
    /// parent hydrated is no longer referenced by any result row, but — unlike the
    /// drain-to-empty case in `push_remove` — its child rows are still live in the
    /// window, so nothing else ever deletes the slot. Without this, a partitioned child
    /// Take accumulates one zombie slot per distinct parent that *ever* floated through
    /// the bounded view (e.g. every page that hit the wiki demo's "latest" board as the
    /// `last_ts` ordering churns), leaking operator state proportional to the retention
    /// window rather than the (bounded) view.
    ///
    /// Safe — and symmetric with the drain-time delete — because a partitioned Take is
    /// re-hydrated from source by the parent join's constrained `fetch` when its parent
    /// re-enters ([`Take::fetch`]'s no-state → `initial_fetch`), and a `push` to an
    /// un-hydrated partition is already dropped (`get_state == None`). The caller only
    /// invokes this when the parent correlation key is unique (the parent's primary key),
    /// so the evicted slot belongs to exactly the one parent that left. No-op for an
    /// unpartitioned (top-level) Take or a constraint that does not cover the partition
    /// key.
    pub fn evict_partition(&self, g: &Graph, constraint: &Constraint) {
        let Some(pk) = &self.partition_key else {
            return;
        };
        if !constraint_matches_partition_key(constraint, pk) {
            return;
        }
        let key = self.key_from_constraint(Some(constraint));
        g.storage(self.storage).del(&key);
    }

    // --- push ---------------------------------------------------------------

    /// Eager push (`take.ts:247`). Edit routes to `Take::push_edit`; otherwise
    /// look up the partition's state (drop the change if it was never hydrated) and
    /// dispatch Add / Remove / Child. Forwards on the edge's own port so a limited
    /// relationship can feed a parent join.
    pub fn push<'g>(&'g self, g: &'g Graph, change: Change<'g>) {
        if matches!(change, Change::Edit { .. }) {
            return self.push_edit(g, change);
        }

        let row = match &change {
            Change::Add(n) | Change::Remove(n) => &n.row,
            Change::Child { node, .. } => &node.row,
            Change::Edit { .. } => unreachable!(),
        };
        let key = self.key_from_row(row);
        // No state for this partition ⇒ it was never observed; drop (`take.ts:255`).
        let Some((size, bound)) = self.get_state(g, &key) else {
            return;
        };
        let max_bound = self.get_max_bound(g);
        let constraint = self.constraint_for(row);
        let out = self.output.get().expect("Take output not wired");

        match change {
            Change::Add(node) => {
                self.push_add(g, node, out, &key, size, bound, max_bound, &constraint)
            }
            Change::Remove(node) => {
                self.push_remove(g, node, out, &key, size, bound, max_bound, &constraint)
            }
            Change::Child { node, rel, child } => {
                // Forward iff the row is within the window (`take.ts:420`).
                if bound
                    .as_ref()
                    .is_some_and(|b| compare_rows(&self.sort, &node.row, b) != Ordering::Greater)
                {
                    g.push(out.node, Change::Child { node, rel, child }, out.port);
                }
            }
            Change::Edit { .. } => unreachable!(),
        }
    }

    /// Add (`take.ts:261`). Room (`size < limit`) ⇒ extend bound, forward. Full and
    /// the new row is outside the window ⇒ drop. Full and the new row displaces the
    /// boundary ⇒ Remove(boundNode) then Add(new), keeping size ≤ limit.
    #[allow(clippy::too_many_arguments)]
    fn push_add<'g>(
        &'g self,
        g: &'g Graph,
        node: Node<'g>,
        out: OutEdge,
        key: &str,
        size: u32,
        bound: Option<Row>,
        max_bound: Option<Row>,
        constraint: &Option<Constraint>,
    ) {
        if size < self.limit {
            // Extend the bound iff the new row sorts past it (or there was none).
            let new_bound = match &bound {
                Some(b) if compare_rows(&self.sort, b, &node.row) != Ordering::Less => b.clone(),
                _ => node.row.clone(),
            };
            self.set_state(g, key, size + 1, Some(new_bound), max_bound.as_ref());
            g.push(out.node, Change::Add(node), out.port);
            return;
        }

        // size == limit. Drop unless the new row sorts strictly before the bound.
        let bound = match &bound {
            Some(b) if compare_rows(&self.sort, &node.row, b) == Ordering::Less => b.clone(),
            _ => return,
        };

        // Displacement: fetch the boundary (and, for limit > 1, the row before it).
        let mut found = if self.limit == 1 {
            self.fetch_n(g, &bound, Basis::At, constraint, false, 1)
        } else {
            self.fetch_n(g, &bound, Basis::At, constraint, true, 2)
        };
        assert!(
            !found.is_empty(),
            "Take: boundNode must be found during fetch"
        );
        let bound_node = found.remove(0);
        let before_bound_node = (!found.is_empty()).then(|| found.remove(0));

        // New bound = max(new row, the row before the displaced boundary).
        let new_bound = match &before_bound_node {
            Some(bbn) if compare_rows(&self.sort, &node.row, &bbn.row) != Ordering::Greater => {
                bbn.row.clone()
            }
            _ => node.row.clone(),
        };
        // Remove before add to hold size ≤ limit; hide the new row from the remove's
        // reentrant refetch.
        self.set_state(g, key, size, Some(new_bound), max_bound.as_ref());
        self.push_with_row_hidden(g, node.row.clone(), out, Change::Remove(bound_node));
        g.push(out.node, Change::Add(node), out.port);
    }

    /// Remove (`take.ts:341`). After the bound ⇒ drop. Otherwise it was inside the
    /// window: refill from the next row past the bound if the partition has one
    /// (Remove then Add(refill)); else just Remove, shrinking the partition.
    #[allow(clippy::too_many_arguments)]
    fn push_remove<'g>(
        &'g self,
        g: &'g Graph,
        node: Node<'g>,
        out: OutEdge,
        key: &str,
        size: u32,
        bound: Option<Row>,
        max_bound: Option<Row>,
        constraint: &Option<Constraint>,
    ) {
        let Some(bound) = bound else {
            return; // empty partition ⇒ change is after the (absent) bound
        };
        if compare_rows(&self.sort, &node.row, &bound) == Ordering::Greater {
            return; // after the bound ⇒ outside the window
        }

        // Look for the row just before the bound (reverse from after the bound).
        let mut new_bound: Option<(Node<'g>, bool)> = self
            .fetch_n(g, &bound, Basis::After, constraint, true, 1)
            .into_iter()
            .next()
            .map(|bbn| {
                let push = compare_rows(&self.sort, &bbn.row, &bound) == Ordering::Greater;
                (bbn, push)
            });

        // If that didn't yield a refill, scan forward from the bound for the first
        // row strictly after it (the refill candidate). This backfill scan is one leg of the
        // take-refill cascade (O(N)-by-design since the committed-membership fix), so it
        // carries a push-deadline checkpoint (FOLLOWER-LAG-SHED §6.6) — on expiry, park and
        // bail with the window state untouched (torn is fine; the host discards the engine).
        if !matches!(&new_bound, Some((_, true))) {
            let req = FetchRequest {
                start: Some(Start {
                    row: bound.clone(),
                    basis: Basis::At,
                }),
                constraint: constraint.clone(),
                ..Default::default()
            };
            for n in g.fetch(self.input, &req) {
                if g.push_deadline_exceeded() {
                    g.park_runtime_error(crate::error::RindleError::PushDeadlineExceeded {
                        site: "take backfill scan",
                    });
                    return;
                }
                let push = compare_rows(&self.sort, &n.row, &bound) == Ordering::Greater;
                new_bound = Some((n, push));
                if push {
                    break;
                }
            }
        }

        match new_bound {
            Some((refill, true)) => {
                // Refill exists: remove the gone row, slide the bound, add the refill.
                g.push(out.node, Change::Remove(node), out.port);
                self.set_state(g, key, size, Some(refill.row.clone()), max_bound.as_ref());
                g.push(out.node, Change::Add(refill), out.port);
            }
            other => {
                // No refill: the partition shrank by one.
                let new_b = other.map(|(n, _)| n.row.clone());
                if self.partition_key.is_some() && size - 1 == 0 {
                    // A PARTITIONED partition just drained to empty. Delete its slot
                    // instead of parking a zombie `Take{size:0, bound:None}` — otherwise
                    // every correlation key that ever passed through (e.g. every wiki
                    // page/editor that floated into a board's window and was later
                    // pruned) leaks one operator-state entry forever. This mirrors
                    // `Reduce`'s eager-death slot delete. Safe because a partitioned Take
                    // is re-hydrated from source by the parent join's constrained `fetch`
                    // when its parent re-enters (Take::fetch's no-state → initial_fetch),
                    // and a `push` to an un-hydrated partition is already dropped
                    // (get_state == None) — so an emptied partition behaves exactly like
                    // one never observed. A TOP-LEVEL (unpartitioned) Take must instead
                    // keep its size-0 state: it has no parent to trigger re-hydration, so
                    // a later `Add` push must find the slot to grow the window.
                    debug_assert!(
                        new_b.is_none(),
                        "Take: partition drained to size 0 but a bound remained"
                    );
                    g.storage(self.storage).del(key);
                } else {
                    self.set_state(g, key, size - 1, new_b, max_bound.as_ref());
                }
                g.push(out.node, Change::Remove(node), out.port);
            }
        }
    }

    /// `#pushEditChange` (`take.ts:432`): the full bound-crossing matrix on
    /// `(oldCmp, newCmp)` = the old/new rows compared against the bound. Asserts the
    /// partition key is unchanged (the source split-edits a key change into
    /// remove+add, so an Edit never crosses a partition).
    fn push_edit<'g>(&'g self, g: &'g Graph, change: Change<'g>) {
        let Change::Edit { node, old } = change else {
            unreachable!()
        };
        let out = self.output.get().expect("Take output not wired");

        if let Some(pk) = &self.partition_key {
            debug_assert!(
                partition_key_unchanged(&old.row, &node.row, pk),
                "Take: unexpected change of partition key"
            );
        }

        let key = self.key_from_row(&old.row);
        let Some((size, bound)) = self.get_state(g, &key) else {
            return; // partition never observed ⇒ drop
        };
        let bound = bound.expect("Take: bound should be set");
        let max_bound = self.get_max_bound(g);
        let constraint = self.constraint_for(&old.row);

        let old_cmp = compare_rows(&self.sort, &old.row, &bound);
        let new_cmp = compare_rows(&self.sort, &node.row, &bound);

        // Replace the bound with the new row and forward the Edit unchanged.
        let replace_and_forward = |node: Node<'g>, old: Node<'g>| {
            self.set_state(g, &key, size, Some(node.row.clone()), max_bound.as_ref());
            g.push(out.node, Change::Edit { node, old }, out.port);
        };

        match old_cmp {
            // The bound row itself was edited.
            Ordering::Equal => match new_cmp {
                // Still the bound: forward, no state change.
                Ordering::Equal => g.push(out.node, Change::Edit { node, old }, out.port),
                // New moved before the bound.
                Ordering::Less => {
                    if self.limit == 1 {
                        replace_and_forward(node, old);
                        return;
                    }
                    // Find the row before the old bound; it becomes the new bound.
                    let bbn = self
                        .fetch_n(g, &bound, Basis::After, &constraint, true, 1)
                        .into_iter()
                        .next()
                        .expect("Take: beforeBoundNode must be found during fetch");
                    self.set_state(g, &key, size, Some(bbn.row.clone()), max_bound.as_ref());
                    g.push(out.node, Change::Edit { node, old }, out.port);
                }
                // New moved past the bound: the first row at the old bound is the
                // next candidate.
                Ordering::Greater => {
                    let nbn = self
                        .fetch_n(g, &bound, Basis::At, &constraint, false, 1)
                        .into_iter()
                        .next()
                        .expect("Take: newBoundNode must be found during fetch");
                    if compare_rows(&self.sort, &nbn.row, &node.row) == Ordering::Equal {
                        // New is still the bound: forward the Edit.
                        replace_and_forward(node, old);
                    } else {
                        // New fell outside the window: drop it, pull the candidate in.
                        self.set_state(g, &key, size, Some(nbn.row.clone()), max_bound.as_ref());
                        self.push_with_row_hidden(g, nbn.row.clone(), out, Change::Remove(old));
                        g.push(out.node, Change::Add(nbn), out.port);
                    }
                }
            },
            // The old row was outside the window.
            Ordering::Greater => {
                debug_assert!(
                    new_cmp != Ordering::Equal,
                    "Invalid state. Row has duplicate primary key"
                );
                match new_cmp {
                    Ordering::Greater => {} // both outside ⇒ drop
                    // New moves into the window, pushing the current boundary out.
                    _ => {
                        let mut v = self.fetch_n(g, &bound, Basis::At, &constraint, true, 2);
                        assert!(
                            v.len() >= 2,
                            "Take: old/newBoundNode must be found during fetch"
                        );
                        let old_bound_node = v.remove(0);
                        let new_bound_node = v.remove(0);
                        self.set_state(
                            g,
                            &key,
                            size,
                            Some(new_bound_node.row.clone()),
                            max_bound.as_ref(),
                        );
                        self.push_with_row_hidden(
                            g,
                            node.row.clone(),
                            out,
                            Change::Remove(old_bound_node),
                        );
                        g.push(out.node, Change::Add(node), out.port);
                    }
                }
            }
            // The old row was inside the window.
            Ordering::Less => {
                debug_assert!(
                    new_cmp != Ordering::Equal,
                    "Invalid state. Row has duplicate primary key"
                );
                match new_cmp {
                    Ordering::Less => g.push(out.node, Change::Edit { node, old }, out.port),
                    // New moves past the bound: pull the first row after the bound.
                    _ => {
                        let abn = self
                            .fetch_n(g, &bound, Basis::After, &constraint, false, 1)
                            .into_iter()
                            .next()
                            .expect("Take: afterBoundNode must be found during fetch");
                        if compare_rows(&self.sort, &abn.row, &node.row) == Ordering::Equal {
                            // New is the new bound: forward the Edit.
                            replace_and_forward(node, old);
                        } else {
                            g.push(out.node, Change::Remove(old), out.port);
                            self.set_state(
                                g,
                                &key,
                                size,
                                Some(abn.row.clone()),
                                max_bound.as_ref(),
                            );
                            g.push(out.node, Change::Add(abn), out.port);
                        }
                    }
                }
            }
        }
    }

    /// `#pushWithRowHiddenFromFetch` (`take.ts:677`): set the single-row fetch
    /// overlay, push `change` (which may reentrantly refetch this Take), then clear
    /// the overlay via the [`HiddenGuard`] `Drop` — on normal return, `?`, or early
    /// return. The `Drop` also clears it on panic ONLY in unwinding builds; under the
    /// shipping `panic = "abort"` client profile a panic aborts and `Drop` is skipped.
    /// See WS02.
    fn push_with_row_hidden<'g>(
        &'g self,
        g: &'g Graph,
        row: Row,
        out: OutEdge,
        change: Change<'g>,
    ) {
        *self.row_hidden_from_fetch.borrow_mut() = Some(row);
        let _guard = HiddenGuard(&self.row_hidden_from_fetch);
        g.push(out.node, change, out.port);
    }
}
