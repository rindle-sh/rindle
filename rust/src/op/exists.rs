//! `Exists` (`ivm/exists.ts`, spec `07` §3.5) — keep a parent row iff its child
//! relationship is non-empty (`EXISTS`) or empty (`NOT EXISTS`). A **`FilterChain`
//! link**: it has no `fetch`; it `filter(node) -> bool` gates the upstream scan and
//! `push`es incremental changes. The child relationship is already on the `Node`
//! (attached by an upstream `Join` whose child pipeline is bounded to `EXISTS_LIMIT`
//! by [`Cap`](crate::op::Cap)/[`Take`](crate::op::Take)), so Exists just **counts**
//! it.
//!
//! Dispatched through the graph's filter-chain seam: [`Graph`]'s
//! `begin_filter`/`chain_filter`/`end_filter`/`chain_push` each carry a one-line
//! `Operator::Exists` arm that delegates here, and this file drives the rest of the
//! chain back through the same `pub(crate)` graph methods (`g.chain_filter(out, …)`
//! / `g.chain_push(out, …)`). All Exists logic lives here (the fan-out seam); only
//! the dispatch arms live in `graph.rs`.
//!
//! Three subtleties ported faithfully:
//! - a **per-fetch-loop size cache** (`begin`/`end` bracket it; reset on `end`),
//!   suppressed when `no_size_reuse` (the parent join key is the PK ⇒ each parent is
//!   unique) or `in_push` (relationships are transiently inconsistent mid-push);
//! - the **`in_push` reentrancy guard** (a push within a push is a bug);
//! - the **boundary-flip forced-relationship rewrite**: when a child Add/Remove
//!   flips membership, Exists converts it into a parent Add/Remove and rewrites the
//!   parent's relationship so the downstream view stays consistent (an EXISTS-remove
//!   carries just the removed child; a NOT-EXISTS-add carries an empty relationship).

use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use crate::change::{Change, ChangeType, Node, NodeStream, Relationship};
use crate::graph::{Graph, NodeId};
use crate::op::partition::encode_state_key;
use crate::value::{ColId, OwnedValue, RelId};

/// `Exists`: a relationship-size gate in the `where` filter sub-graph.
pub struct Exists {
    /// Upstream `FilterInput` (the `FilterStart` / prior link) — for `input_schema`.
    pub input: NodeId,
    /// The relationship slot to count (resolved from the subquery alias against the
    /// parent schema at build time).
    pub rel: RelId,
    /// `NOT EXISTS`.
    pub not: bool,
    /// The parent-join-key columns — the cache key (`condition.parentField`).
    pub parent_join_key: Vec<ColId>,
    /// `parentJoinKey == primaryKey` ⇒ each parent is unique ⇒ the cache can never
    /// hit, so skip it entirely (`exists.ts:60`).
    no_size_reuse: bool,
    /// Per-fetch-loop size cache keyed by the parent-join-key tuple (reset on
    /// `end`). Ephemeral to one fetch loop — never persisted to `Storage`.
    cache: RefCell<HashMap<String, bool>>,
    /// `true` while processing a push: doubles as the reentrancy guard and the
    /// cache-suppression flag (`exists.ts:39`).
    in_push: Cell<bool>,
    /// The downstream `FilterOutput` chain link (wired late via
    /// [`Graph::set_output`](crate::graph::Graph::set_output)).
    pub output: Cell<Option<NodeId>>,
}

/// Clears [`Exists::in_push`] on drop — the RAII rendering of the JS `finally`
/// (`exists.ts:205`).
struct InPushGuard<'a>(&'a Cell<bool>);
impl Drop for InPushGuard<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

impl Exists {
    pub fn new(
        input: NodeId,
        rel: RelId,
        parent_join_key: Vec<ColId>,
        not: bool,
        primary_key: &[ColId],
    ) -> Exists {
        let no_size_reuse = parent_join_key.as_slice() == primary_key;
        Exists {
            input,
            rel,
            not,
            parent_join_key,
            no_size_reuse,
            cache: RefCell::new(HashMap::new()),
            in_push: Cell::new(false),
            output: Cell::new(None),
        }
    }

    fn out(&self) -> NodeId {
        self.output.get().expect("Exists output not wired")
    }

    // --- filter-chain lifecycle (driven by graph.rs dispatch arms) ----------

    /// `beginFilter` (`exists.ts:71`): just delegate downstream (the cache is reset
    /// on `end`, not begin).
    pub fn begin(&self, g: &Graph) {
        g.begin_filter(self.out());
    }

    /// `endFilter` (`exists.ts:75`): reset the per-loop cache, then delegate.
    pub fn end(&self, g: &Graph) {
        self.cache.borrow_mut().clear();
        g.end_filter(self.out());
    }

    /// `filter(node)` (`exists.ts:80`): the epoch-gated cache lookup, then
    /// `#filter(node) && output.filter(node)`.
    pub fn filter(&self, g: &Graph, node: &Node) -> bool {
        let exists = if !self.no_size_reuse && !self.in_push.get() {
            let key = self.cache_key(node);
            // Drop the immutable borrow (via `.copied()`) before any `borrow_mut`.
            let cached = self.cache.borrow().get(&key).copied();
            Some(cached.unwrap_or_else(|| {
                let e = self.fetch_exists(node);
                self.cache.borrow_mut().insert(key, e);
                e
            }))
        } else {
            None
        };
        self.gate(node, exists) && g.chain_filter(self.out(), node)
    }

    // --- push ---------------------------------------------------------------

    /// `push` (`exists.ts:109`). Add/Edit/Remove of the *parent* cannot change the
    /// child relationship's size → `#pushWithFilter`. A Child **Add/Remove to this
    /// relationship** can flip membership → refetch the size and either pass it
    /// through filtered or convert it into a parent Add/Remove. Returns the change(s)
    /// to forward (the chassis bubbles them up; a terminal `FilterEnd` consumes them).
    pub fn push_chain<'g>(&'g self, g: &'g Graph, change: Change<'g>) -> Vec<Change<'g>> {
        assert!(!self.in_push.get(), "Exists: unexpected re-entrancy");
        self.in_push.set(true);
        let _guard = InPushGuard(&self.in_push);

        match change {
            Change::Add(_) | Change::Edit { .. } | Change::Remove(_) => {
                self.push_with_filter(g, change, None)
            }
            Change::Child { node, rel, child } => {
                let child_type = child.change_type();
                // A change to a *different* relationship, or a nested Edit/Child to
                // this one, cannot change this relationship's size.
                if rel != self.rel || matches!(child_type, ChangeType::Edit | ChangeType::Child) {
                    return self.push_with_filter(g, Change::Child { node, rel, child }, None);
                }
                match child_type {
                    ChangeType::Add => {
                        let size = self.fetch_size(&node);
                        if size == 1 {
                            // The relationship just became non-empty.
                            if self.not {
                                // NOT EXISTS: parent newly fails. The just-added child
                                // was never in the output, so force the relationship
                                // empty on the Remove.
                                g.chain_push(
                                    self.out(),
                                    Change::Remove(force_rel_empty(node, self.rel)),
                                )
                            } else {
                                // EXISTS: parent newly satisfies → Add (its relationship
                                // thunk already includes the added child via the overlay).
                                g.chain_push(self.out(), Change::Add(node))
                            }
                        } else {
                            self.push_with_filter(
                                g,
                                Change::Child { node, rel, child },
                                Some(size > 0),
                            )
                        }
                    }
                    ChangeType::Remove => {
                        let size = self.fetch_size(&node);
                        if size == 0 {
                            // The relationship just became empty.
                            if self.not {
                                // NOT EXISTS: parent newly satisfies → Add.
                                g.chain_push(self.out(), Change::Add(node))
                            } else {
                                // EXISTS: parent newly fails → Remove, carrying just the
                                // removed child (still present in the view's copy).
                                let Change::Remove(removed) = *child else {
                                    unreachable!()
                                };
                                let forced = force_rel_single(node, self.rel, removed);
                                g.chain_push(self.out(), Change::Remove(forced))
                            }
                        } else {
                            self.push_with_filter(
                                g,
                                Change::Child { node, rel, child },
                                Some(size > 0),
                            )
                        }
                    }
                    ChangeType::Edit | ChangeType::Child => unreachable!("handled above"),
                }
            }
        }
    }

    /// `#pushWithFilter` (`exists.ts:235`): forward iff `#filter` passes for the
    /// change's node (using `exists` if supplied, else computing the size).
    fn push_with_filter<'g>(
        &'g self,
        g: &'g Graph,
        change: Change<'g>,
        exists: Option<bool>,
    ) -> Vec<Change<'g>> {
        let pass = {
            let node = match &change {
                Change::Add(n) | Change::Remove(n) => n,
                Change::Edit { node, .. } | Change::Child { node, .. } => node,
            };
            self.gate(node, exists)
        };
        if pass {
            g.chain_push(self.out(), change)
        } else {
            Vec::new()
        }
    }

    // --- size / gate / cache ------------------------------------------------

    /// `#filter` (`exists.ts:219`): `not ? !exists : exists`, computing the size if
    /// `exists` was not supplied.
    fn gate(&self, node: &Node, exists: Option<bool>) -> bool {
        let e = exists.unwrap_or_else(|| self.fetch_exists(node));
        // `not ? !exists : exists` == `not XOR exists`.
        self.not ^ e
    }

    fn fetch_exists(&self, node: &Node) -> bool {
        // Can't early-return at the first child: Take/Cap don't support early return
        // during initial fetch, so count up to the limit (`exists.ts:241`).
        self.fetch_size(node) > 0
    }

    fn fetch_size(&self, node: &Node) -> usize {
        let rel = node
            .rels
            .iter()
            .find(|r| r.slot == self.rel)
            .expect("Exists: relationship not found on node");
        (rel.thunk)().count()
    }

    fn cache_key(&self, node: &Node) -> String {
        let vals: Vec<OwnedValue> = self
            .parent_join_key
            .iter()
            .map(|&c| node.row.col(c).to_owned())
            .collect();
        // The prefix is irrelevant (each Exists has its own cache); reuse the shared
        // value-tuple encoder for the JSON-of-normalized-values key.
        encode_state_key("", &vals)
    }
}

/// Replace `node`'s `slot` relationship with one that yields a single `child` (the
/// EXISTS-remove forced relationship, `exists.ts:181`). Single-use: the held node is
/// not `Clone` (its own thunks aren't), so the stream yields once then is empty —
/// sufficient for the materialize-once consumers (the view / `Catch`).
fn force_rel_single<'g>(mut node: Node<'g>, slot: RelId, child: Node<'g>) -> Node<'g> {
    node.rels.retain(|r| r.slot != slot);
    let held = RefCell::new(Some(vec![child]));
    node.rels.push(Relationship {
        slot,
        thunk: Box::new(move || {
            let v: Vec<Node<'g>> = held.borrow_mut().take().unwrap_or_default();
            Box::new(v.into_iter()) as NodeStream<'g>
        }),
    });
    node
}

/// Replace `node`'s `slot` relationship with an empty one (the NOT-EXISTS-add forced
/// relationship, `exists.ts:152`).
fn force_rel_empty(mut node: Node<'_>, slot: RelId) -> Node<'_> {
    node.rels.retain(|r| r.slot != slot);
    node.rels.push(Relationship {
        slot,
        thunk: Box::new(|| Box::new(std::iter::empty())),
    });
    node
}
