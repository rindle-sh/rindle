//! `FlippedJoin` (`ivm/flipped-join.ts`, spec `06` §3.2) — the **flipped EXISTS**
//! inner join: child-driven, batched, outputs **parent** rows that have ≥1 related
//! child.
//!
//! Where [`Join`](crate::graph::Join) is parent-driven (fetch parents, attach a
//! lazy child relationship to each), `FlippedJoin` is child-driven: it fetches
//! **all children first**, groups them by their join key, batch-fetches the
//! matching parents in one `multi_constraints` request (the source does the k-way
//! IN-fanout merge), and yields each returned parent **with its related children
//! attached** — but only parents some child pointed at. That inner-join gate (drop
//! a parent with no related child) is exactly the `where EXISTS(rel)` semantics, so
//! the builder lowers a *flipped* EXISTS condition to a bare `FlippedJoin` (no
//! separate [`Exists`](crate::op::Exists) gate).
//!
//! ## What it reuses from `Join` (the re-materialize overlay model)
//!
//! The relationship `FlippedJoin` attaches to each parent is built **identically**
//! to `Join`'s — [`Graph::attach_child_rel`](crate::graph::Graph::attach_child_rel)
//! with the same by-value [`JoinOverlay`] + `splice_join_overlay` (Primitive #5).
//! So `FlippedJoin` inherits `Join`'s proven, View-`re-materialize` overlay
//! behavior verbatim; the spec's `{change, position}` suppress model + node-level
//! generator stay deferred with the production `ArrayView` (`09`).
//!
//! ## What is `FlippedJoin`-specific
//!
//! - **fetch** (child-first): translate the request constraint parent→child, fetch
//!   all children, group by [`canonical_key`], one batched parent fetch, drop a
//!   returned parent whose key no child produced (the inner-join gate + the
//!   pass-through filter for chained flipped joins).
//! - **push child port**: the changed child is re-projected onto its parents; for
//!   each, decide whether the parent still has *another* related child (`exists`).
//!   If yes → a `Child` change (the parent stays, its relationship changed); if no →
//!   the changed child is the parent's **sole** child, so its existence flips:
//!   `Add(parent)` on a child add, `Remove(parent)` on a child remove.
//! - **push parent port**: forward the change with the relationship flipped on,
//!   **unless** the parent has no related child (inner-join drop).
//!
//! ## Fully implemented (formerly deferred)
//!
//! - **Chunked fetch** (`computed_multi.len() > chunk_size`): slices the IN-batch and
//!   node-level-merges the chunk streams via
//!   [`merge_node_streams`](crate::op::merge_node_streams) (default `chunk_size` 256;
//!   the `chunk_size` field lets a test drive it).
//! - **Nested `Change::Child` on the child port** (a grandchild change under the
//!   flipped child): carried up as a full `Change` and rebuilt per parent, exactly as
//!   [`Join`](crate::graph)'s child port — see `push_child`/`push_child_change`.

use std::cell::Cell;
use std::collections::HashSet;

use crate::change::{
    build_join_constraint, constraints_are_compatible, materialize_change, rebuild_change, Change,
    Constraint, FetchRequest, JoinOverlay, MultiConstraint, Node, NodeStream, OutEdge, Port,
    Relationship,
};
use crate::graph::{Graph, NodeId};
use crate::value::{ColId, OwnedRow as Row, RelId};

use super::join_util::{
    canonical_key, canonical_key_of_constraint, row_equals_for_compound_key, CanonKey,
};

/// `MULTI_CONSTRAINT_CHUNK_SIZE` (`flipped-join.ts:55`): the IN-batch is sliced
/// into windows of this many constraints, each its own `parent.fetch`, merged.
pub const DEFAULT_CHUNK_SIZE: usize = 256;

/// `FlippedJoin`: child-driven inner join (spec `06` §3.2). Two input ports
/// ([`Port::JoinParent`]/[`Port::JoinChild`]) like [`Join`](crate::graph::Join);
/// the **child** drives the fetch but the **parent** is the output row, so
/// `rel_slot` resolves against the parent schema and `input_schema` is the parent's.
pub struct FlippedJoin {
    /// The output side (parent rows). The attached relationship hangs here.
    pub parent: NodeId,
    /// The driving side: children are fetched first and grouped by join key.
    pub child: NodeId,
    pub parent_key: Vec<ColId>,
    pub child_key: Vec<ColId>,
    /// Relationship slot (resolved from the alias against the **parent** schema at
    /// build time, like [`Join`](crate::graph::Join)).
    pub rel_slot: RelId,
    /// IN-batch chunking threshold (test/seam; default `DEFAULT_CHUNK_SIZE`). A
    /// batch above this needs the node-level merge (deferred — see module docs).
    pub chunk_size: usize,
    /// The single downstream edge as a port-carrying [`OutEdge`] (like
    /// [`Join`](crate::graph::Join)): `Port::Single` to a terminal sink, or a join
    /// port when this flipped join feeds another join.
    pub output: Cell<Option<OutEdge>>,
}

impl FlippedJoin {
    pub fn new(
        parent: NodeId,
        child: NodeId,
        parent_key: Vec<ColId>,
        child_key: Vec<ColId>,
        rel_slot: RelId,
    ) -> FlippedJoin {
        FlippedJoin {
            parent,
            child,
            parent_key,
            child_key,
            rel_slot,
            chunk_size: DEFAULT_CHUNK_SIZE,
            output: Cell::new(None),
        }
    }

    /// Override the IN-batch chunk size (the `setMultiConstraintChunkSizeForTest`
    /// seam, `flipped-join.ts:57`). Used by the chunking-equivalence test once the
    /// chunked fetch path lands.
    pub fn with_chunk_size(mut self, chunk_size: usize) -> FlippedJoin {
        self.chunk_size = chunk_size;
        self
    }

    // ---------------------------------------------------------------------
    // FETCH (child-driven, batched)
    // ---------------------------------------------------------------------

    /// Lazy pull (`flipped-join.ts:161`): translate the request constraint onto the
    /// child key, fetch **all** children, then `fetch_batched`.
    pub fn fetch<'g>(&'g self, g: &'g Graph, req: &FetchRequest) -> NodeStream<'g> {
        let child_constraint = self.translate_constraint(req.constraint.as_ref());
        let child_req = FetchRequest {
            constraint: child_constraint,
            ..Default::default()
        };
        let children: Vec<Node<'g>> = g.fetch(self.child, &child_req).collect();
        self.fetch_batched(g, req, children)
    }

    /// `#fetchBatched` (`flipped-join.ts:230`): build the deduped IN-batch of
    /// child-derived parent constraints + the set of parent keys that some child
    /// produced, then one batched `parent.fetch`. Each returned parent whose key a
    /// child produced is yielded with the join relationship attached; a parent whose
    /// key no child produced is dropped (the inner-join gate, and the pass-through
    /// filter for a chained flipped join's `multi_constraints`).
    fn fetch_batched<'g>(
        &'g self,
        g: &'g Graph,
        req: &FetchRequest,
        children: Vec<Node<'g>>,
    ) -> NodeStream<'g> {
        let mut seen: HashSet<CanonKey> = HashSet::with_capacity(children.len());
        let mut computed_multi: MultiConstraint = Vec::new();
        for child in &children {
            let c = match build_join_constraint(&child.row, &self.child_key, &self.parent_key) {
                Some(c) => c,
                None => continue, // null child key cannot join
            };
            // Drop a child-derived constraint that contradicts the incoming request
            // constraint — it could never match, so it must not enter the batch.
            if let Some(rc) = req.constraint.as_ref() {
                if !constraints_are_compatible(&c, rc) {
                    continue;
                }
            }
            let k = canonical_key_of_constraint(&c);
            if seen.insert(k) {
                // First sight of this key → one IN-batch entry (dedup).
                computed_multi.push(c);
            }
        }
        if computed_multi.is_empty() {
            return Box::new(std::iter::empty());
        }

        // Build the parent stream: one batched fetch when the IN-batch fits a single
        // chunk, else one fetch per `chunk_size` window, k-way-merged in parent order
        // (`#fetchChunked`, `flipped-join.ts:311`). The windows are disjoint key-sets
        // so the merge does NOT dedup. The source turns each request's
        // `multi_constraints` into the per-entry IN-fanout + merge.
        let parent_stream: NodeStream<'g> = if computed_multi.len() <= self.chunk_size {
            g.fetch(self.parent, &self.parent_batch_req(req, computed_multi))
        } else {
            // Each chunk fetch streams in the parent's **resolved per-query order**
            // (`input_sort` — e.g. `order by Milliseconds`), NOT the base table's PK
            // sort (`input_schema().sort`). The k-way merge must compare on that same
            // resolved order or it interleaves the chunks wrongly and a downstream
            // `Take` keeps the wrong top-N. (Only bites above `chunk_size` distinct
            // child keys, so the mini fixtures never hit it — the chinook scale sweep
            // did.) See `input_sort` vs `input_schema` in `graph.rs`.
            let parent_sort = g.input_sort(self.parent);
            let chunks: Vec<NodeStream<'g>> = computed_multi
                .chunks(self.chunk_size)
                .map(|window| g.fetch(self.parent, &self.parent_batch_req(req, window.to_vec())))
                .collect();
            crate::op::merge_node_streams(chunks, parent_sort, req.reverse, None)
        };

        // Capture by value for the `'g` filter_map closure (don't borrow `self`).
        let parent_key = self.parent_key.clone();
        let child_key = self.child_key.clone();
        let child_id = self.child;
        let rel_slot = self.rel_slot;
        Box::new(parent_stream.filter_map(move |pnode| {
            let k = canonical_key(&pnode.row, &parent_key);
            // Miss ⇒ no child produced this parent's key ⇒ drop it (inner-join gate
            // / chained-flip pass-through filter, `flipped-join.ts:295`).
            if !seen.contains(&k) {
                return None;
            }
            Some(g.attach_child_rel(child_id, &parent_key, &child_key, rel_slot, pnode, None))
        }))
    }

    /// Build the parent fetch request for one IN-batch (the whole `computed_multi`
    /// or a single chunk window): the incoming constraint ANDed with
    /// `[...incoming_multi, batch]` (a chained flipped join above prepends its own
    /// `multi_constraints`), carrying the request's `start`/`reverse`.
    fn parent_batch_req(&self, req: &FetchRequest, batch: MultiConstraint) -> FetchRequest {
        let mut multi = req.multi_constraints.clone();
        multi.push(batch);
        FetchRequest {
            constraint: req.constraint.clone(),
            multi_constraints: multi,
            start: req.start.clone(),
            reverse: req.reverse,
        }
    }

    /// Translate the parent-key columns of an incoming request constraint to the
    /// corresponding child-key columns (`flipped-join.ts:164`). Columns that aren't
    /// parent-key columns are dropped; if none translate, the child is fetched
    /// unconstrained (`None`).
    fn translate_constraint(&self, c: Option<&Constraint>) -> Option<Constraint> {
        let c = c?;
        let mut out = Constraint::new();
        for (col, val) in c {
            if let Some(i) = self.parent_key.iter().position(|pk| pk == col) {
                out.push((self.child_key[i], val.clone()));
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    // ---------------------------------------------------------------------
    // PUSH (two ports)
    // ---------------------------------------------------------------------

    pub fn push<'g>(&'g self, g: &'g Graph, change: Change<'g>, port: Port) {
        match port {
            Port::JoinChild => self.push_child(g, change),
            Port::JoinParent => self.push_parent(g, change),
            Port::Single => panic!("FlippedJoin received Single port"),
        }
    }

    /// Child port (`flipped-join.ts:385`): the changed child is re-projected onto
    /// its parents with the existence decision. EDIT/CHILD can't flip a parent's
    /// existence, so they pre-set `exists = true`. A key edit is rejected (the
    /// source split it, like [`Join`](crate::graph)).
    fn push_child<'g>(&'g self, g: &'g Graph, change: Change<'g>) {
        let exists_pre = match &change {
            Change::Add(_) | Change::Remove(_) => false,
            Change::Edit { node, old } => {
                assert!(
                    row_equals_for_compound_key(&old.row, &node.row, &self.child_key),
                    "flipped child edit must not change the join relationship key"
                );
                // A non-key edit cannot make a parent appear/disappear → forward a
                // Child change (existence is unchanged).
                true
            }
            // A nested `Change::Child` (a grandchild change) leaves the direct child
            // present, so the parent's EXISTS-membership is unchanged → forward it as
            // a `Child`, carried verbatim. `exists_pre = true` skips the membership
            // re-check; without it, `parent_has_other_child` (which excludes the
            // *changed* child) could see "no other child" and wrongly emit an
            // Add/Remove of the parent for what is only a grandchild edit.
            Change::Child { .. } => true,
        };
        self.push_child_change(g, change, exists_pre);
    }

    /// `#pushChildChange` (`flipped-join.ts:409`), the reentrant crux. Build the
    /// parent constraint from the changed child, re-enter `parent.fetch`, and for
    /// each parent decide existence (does it have *another* related child?). Yes →
    /// `Child`; no → the child is the parent's sole child, so the parent's existence
    /// flips (`Add` on child-add / `Remove` on child-remove).
    fn push_child_change<'g>(&'g self, g: &'g Graph, child_change: Change<'g>, exists_pre: bool) {
        let out = self.output.get().expect("FlippedJoin output not wired");
        let key_row = child_change.primary_row().clone();
        let constraint = match build_join_constraint(&key_row, &self.child_key, &self.parent_key) {
            Some(c) => c,
            None => return, // null child key cannot join
        };
        let overlay = JoinOverlay::for_child_change(&child_change);
        let child_pk = g.input_schema(self.child).primary_key.clone();
        let is_add = matches!(child_change, Change::Add(_));
        // Materialize once (drains thunks now), rebuild a fresh `Change<'g>` per
        // parent in the `exists` branch — a child can join to many parents.
        let owned = materialize_change(child_change);

        // Collect the reentrant parent fetch (relationships preserved) so its stream
        // borrow is dropped before the per-parent existence fetch + downstream push.
        let parents: Vec<Node> = g
            .fetch(self.parent, &FetchRequest::with_constraint(constraint))
            .collect();

        // `exists` is sticky across the parent loop (the JS captures it in method
        // scope, `flipped-join.ts:436`): once any parent has another child, later
        // parents stay in the `Child` branch. Moot in the common 1:1 case.
        let mut exists = exists_pre;
        // The flipped twin of the unbounded join fan-out (RUNAWAY-PUSH-FINDINGS §3), with a
        // per-parent existence fetch on top — deadline-checkpointed like the non-flipped loop
        // (FOLLOWER-LAG-SHED §6.6).
        for pnode in parents {
            if g.push_deadline_exceeded() {
                g.park_runtime_error(crate::error::RindleError::PushDeadlineExceeded {
                    site: "flipped-join child fan-out",
                });
                break; // torn is fine — the host discards the engine
            }
            if !exists {
                exists = self.parent_has_other_child(g, &pnode.row, &key_row, &child_pk);
            }
            if exists {
                // Parent stays; its relationship changed → a Child change. (Both the
                // View and Catch read only the parent row on a `Child` and recurse
                // into the carried sub-change; the overlay is `None` for a nested
                // Child and a leaf-row splice otherwise.)
                let pn = g.attach_child_rel(
                    self.child,
                    &self.parent_key,
                    &self.child_key,
                    self.rel_slot,
                    pnode,
                    overlay.clone(),
                );
                let child = Box::new(rebuild_change(owned.clone()));
                g.push(
                    out.node,
                    Change::Child {
                        node: pn,
                        rel: self.rel_slot,
                        child,
                    },
                    out.port,
                );
            } else {
                // The changed child is the parent's SOLE related child → existence
                // flips. The relationship is exactly that one child (JS `[change[NODE]]`,
                // `flipped-join.ts:474`) — NOT a re-fetch (which, on a remove, would
                // show the child already gone). This makes the emitted Add/Remove
                // carry the changed child, matching the `Exists`-gate oracle.
                let pn = self.parent_with_only_child(pnode, key_row.clone());
                let flipped = if is_add {
                    Change::Add(pn)
                } else {
                    Change::Remove(pn)
                };
                g.push(out.node, flipped, out.port);
            }
        }
    }

    /// Does `parent_row` have a related child whose PK differs from the changed
    /// child (`change.node`)? Reentrant `child.fetch` (the child source overlay is
    /// active, so a child add is visible / a child remove is gone), then look for any
    /// child that is **not** the changed one — a PK match (the robust port of the JS
    /// `compareRows(child, change.node) !== 0`, `flipped-join.ts:441`; spec OQ-7).
    fn parent_has_other_child(
        &self,
        g: &Graph,
        parent_row: &Row,
        changed_child: &Row,
        child_pk: &[ColId],
    ) -> bool {
        let Some(cc) = build_join_constraint(parent_row, &self.parent_key, &self.child_key) else {
            return false;
        };
        for cn in g.fetch(self.child, &FetchRequest::with_constraint(cc)) {
            if !row_equals_for_compound_key(&cn.row, changed_child, child_pk) {
                return true;
            }
        }
        false
    }

    /// Parent port (`flipped-join.ts:490`): forward the change with the relationship
    /// flipped on — **unless** the parent has no related child, in which case it is
    /// not in the (inner-join) output and the change is dropped.
    fn push_parent<'g>(&'g self, g: &'g Graph, change: Change<'g>) {
        let out = self.output.get().expect("FlippedJoin output not wired");
        match change {
            Change::Add(node) => {
                if !self.has_related_child(g, &node.row) {
                    return;
                }
                let pn = self.flip(g, node);
                g.push(out.node, Change::Add(pn), out.port);
            }
            Change::Remove(node) => {
                if !self.has_related_child(g, &node.row) {
                    return;
                }
                let pn = self.flip(g, node);
                g.push(out.node, Change::Remove(pn), out.port);
            }
            Change::Edit { node, old } => {
                assert!(
                    row_equals_for_compound_key(&old.row, &node.row, &self.parent_key),
                    "flipped parent edit must not change the join key"
                );
                // Key unchanged ⇒ old and new have the same related children; gate on
                // either. No children ⇒ the parent isn't in the output ⇒ drop.
                if !self.has_related_child(g, &node.row) {
                    return;
                }
                let pn = self.flip(g, node);
                let po = self.flip(g, old);
                g.push(out.node, Change::Edit { node: pn, old: po }, out.port);
            }
            Change::Child { node, rel, child } => {
                // Passthrough of a relationship above this flipped join.
                if !self.has_related_child(g, &node.row) {
                    return;
                }
                let pn = self.flip(g, node);
                g.push(
                    out.node,
                    Change::Child {
                        node: pn,
                        rel,
                        child,
                    },
                    out.port,
                );
            }
        }
    }

    /// The inner-join gate: does `parent_row` have ≥1 related child? (`child.fetch`
    /// of the correlation constraint has a first row.)
    fn has_related_child(&self, g: &Graph, parent_row: &Row) -> bool {
        match build_join_constraint(parent_row, &self.parent_key, &self.child_key) {
            Some(c) => g
                .fetch(self.child, &FetchRequest::with_constraint(c))
                .next()
                .is_some(),
            None => false,
        }
    }

    /// Attach the join's child relationship to a parent node (the re-materialize
    /// thunk, no overlay — the parent-port push has no in-flight child of its own).
    fn flip<'g>(&self, g: &'g Graph, node: Node<'g>) -> Node<'g> {
        g.attach_child_rel(
            self.child,
            &self.parent_key,
            &self.child_key,
            self.rel_slot,
            node,
            None,
        )
    }

    /// Attach a relationship that yields exactly the one given child row — the
    /// parent's sole related child on an existence flip (JS `[change[NODE]]`).
    fn parent_with_only_child<'g>(&self, mut parent: Node<'g>, child_row: Row) -> Node<'g> {
        parent.rels.push(Relationship {
            slot: self.rel_slot,
            thunk: Box::new(move || Box::new(std::iter::once(Node::leaf(child_row.clone())))),
        });
        parent
    }
}
