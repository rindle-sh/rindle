//! The **union OR fan** (`union-fan-out.ts` / `union-fan-in.ts` / `push-accumulated.ts`,
//! spec `06` §3.6) — the node-level OR fan built **over the source** (unlike the
//! relationship-free [`FanOut`](crate::graph)/[`FanIn`](crate::graph) filter fan).
//!
//! The builder uses it when an `OR` branch contains a flipped subquery: a
//! [`UnionFanOut`] broadcasts the source change to each branch (a filter pipeline
//! and/or a [`FlippedJoin`](crate::op::FlippedJoin)), and the shared [`UnionFanIn`]
//! collapses the branches' outputs back to **one** change (the OR dedup).
//!
//! ## The owned-accumulation model (the load-bearing decision)
//!
//! `UnionFanIn` must accumulate each branch's output across the broadcast, but a
//! [`Change<'g>`] borrows the graph (its relationship thunks are `+ 'g`) so it
//! cannot live in a `'static` operator field. Instead, when a branch pushes during
//! the broadcast, `UnionFanIn` **materializes** the change's relationships into an
//! owned ([`OwnedChange`]) tree — exactly what the `View`/`Catch` already do under
//! the re-materialize model — and accumulates *that* in a plain
//! `RefCell<Vec<OwnedChange>>` field. On drain it collapses the owned changes
//! ([`push_accumulated_changes`]) and rebuilds a `Change<'g>` to forward. No
//! `unsafe`, no `'static`-`Change` field.

use std::cell::{Cell, RefCell};

use crate::change::{
    constraints_are_compatible, materialize_change_preserving_node, merge_constraints,
    rebuild_change, Change, ChangeType, Constraint, FetchRequest, NodeStream, OutEdge, OwnedChange,
    OwnedNode,
};
use crate::graph::{Graph, NodeId};
use crate::value::{RelId, Schema};

// The owned (no-`'g`) change model ([`OwnedChange`]/[`OwnedNode`] +
// `materialize_change_preserving_node`/`rebuild_change`) lives in [`crate::change`] — it
// is shared with the join child-push overlay. The union fan accumulates `OwnedChange`s
// across a broadcast (a `Change<'g>` can't live in a `'static` field), collapses them,
// and rebuilds one `Change<'g>` to forward. A `Child` **can** reach the union fan: the
// fan-out broadcasts it (rels intact) to the branches and the CHILD collapse
// ([`push_accumulated_changes`]) gives a preserved Child precedence over a converted
// add/remove (the non-flipped EXISTS spine `Join` lives below the fan-out).

// ---------------------------------------------------------------------------
// push_accumulated_changes (the OR-correctness collapse) — owned
// ---------------------------------------------------------------------------

/// Union the relationship slots of `right` into `left`, **left wins** on a slot
/// conflict (the JS `{...right, ...left}`, `push-accumulated.ts:275`).
fn merge_owned_rels(
    mut left: Vec<(RelId, Vec<OwnedNode>)>,
    right: Vec<(RelId, Vec<OwnedNode>)>,
) -> Vec<(RelId, Vec<OwnedNode>)> {
    for (slot, children) in right {
        if !left.iter().any(|(s, _)| *s == slot) {
            left.push((slot, children));
        }
    }
    left
}

/// Phase-1 collapse helper: keep the first node of a type (its row is the same
/// across branches), merging in any later node's relationships (left/existing wins).
fn merge_node_into(slot: &mut Option<OwnedNode>, incoming: OwnedNode) {
    match slot {
        Some(existing) => {
            existing.rels = merge_owned_rels(std::mem::take(&mut existing.rels), incoming.rels);
        }
        None => *slot = Some(incoming),
    }
}

fn merge_edit_into(slot: &mut Option<(OwnedNode, OwnedNode)>, node: OwnedNode, old: OwnedNode) {
    match slot {
        Some((en, eo)) => {
            en.rels = merge_owned_rels(std::mem::take(&mut en.rels), node.rels);
            eo.rels = merge_owned_rels(std::mem::take(&mut eo.rels), old.rels);
        }
        None => *slot = Some((node, old)),
    }
}

/// Phase-1 collapse for a preserved `Child` (`mergeRelationships` CHILD-CHILD arm,
/// `push-accumulated.ts:313-332`): keep the **first** branch's `Child` — its
/// relationship slot AND its sub-change — unioning in only later branches' **top-node**
/// relationships (left/first wins). Never concatenate or replace the sub-change:
/// multiple branches preserving the same `Child` carry the identical sub-change, so
/// keeping the first (and merging top-node rels) is the whole job; replacing it would
/// double-apply or drop the grandchild.
fn merge_child_into(slot: &mut Option<OwnedChange>, incoming: OwnedChange) {
    debug_assert!(
        matches!(incoming, OwnedChange::Child { .. }),
        "merge_child_into on a non-Child"
    );
    match slot {
        Some(OwnedChange::Child { node: existing, .. }) => {
            if let OwnedChange::Child { node: inc, .. } = incoming {
                existing.rels = merge_owned_rels(std::mem::take(&mut existing.rels), inc.rels);
            }
        }
        Some(_) => unreachable!("merge_child_into on a non-Child slot"),
        None => *slot = Some(incoming),
    }
}

/// `makeAddEmptyRelationships` (`push-accumulated.ts:369`): for every schema
/// relationship slot the change's node(s) lack, append an **empty** relationship, so
/// the downstream always sees a complete relationship set. No-op for a
/// relationship-free schema.
fn add_empty_relationships(change: OwnedChange, schema: &Schema) -> OwnedChange {
    if schema.relationships.is_empty() {
        return change;
    }
    let fill = |mut node: OwnedNode| -> OwnedNode {
        for i in 0..schema.relationships.len() {
            let slot = RelId(i as u32);
            if !node.rels.iter().any(|(s, _)| *s == slot) {
                node.rels.push((slot, Vec::new()));
            }
        }
        node
    };
    match change {
        OwnedChange::Add(n) => OwnedChange::Add(fill(n)),
        OwnedChange::Remove(n) => OwnedChange::Remove(fill(n)),
        OwnedChange::Edit { node, old } => OwnedChange::Edit {
            node: fill(node),
            old: fill(old),
        },
        // A `Child` is a **strict no-op** (`makeAddEmptyRelationships`' CHILD arm,
        // `push-accumulated.ts:409-410`: "children only have relationships along the path
        // to the change"). The CHILD-survivor precedence emits the Child RAW anyway (it
        // never reaches this wrap), but keep the arm a no-op for safety.
        c @ OwnedChange::Child { .. } => c,
    }
}

/// `pushAccumulatedChanges` (`push-accumulated.ts:87`) over **owned** changes — the
/// OR-correctness collapse. Given the branch outputs and the **fan-out's** original
/// change type, collapse to **exactly one** (or zero) output change, unioning
/// relationships across branches that kept the same row and reconstructing an `Edit`
/// from a branch-split add+remove. Always the relationship-**merge** strategy
/// (UnionFanIn); the filter `FanIn`'s identity path stays in `graph.rs`
/// `collapse_accumulated`.
///
/// A **`Child`** reaches here when the fan-out broadcast a `Child` (a non-flipped EXISTS
/// child change, the spine `Join` sitting below the fan-out): each branch either
/// **preserves** the `Child` (a leaf `Filter` passes it through) or **converts** it to
/// an Add/Remove (an `Exists` gate when the relationship change flips membership 0↔1).
/// The CHILD collapse (`push-accumulated.ts:221-256`) gives a preserved Child
/// **precedence** — emitted RAW (no `add_empty_relationships`), siblings discarded —
/// else the single Add/Remove survivor is emitted through `add_empty_relationships`.
pub fn push_accumulated_changes(
    acc: Vec<OwnedChange>,
    fan_out_type: ChangeType,
    schema: &Schema,
) -> Option<OwnedChange> {
    if acc.is_empty() {
        // It is possible for no fork to pass the push along (every branch dropped it).
        return None;
    }

    // Phase 1: collapse to one change per type, merging relationships on collision.
    let mut add: Option<OwnedNode> = None;
    let mut remove: Option<OwnedNode> = None;
    let mut edit: Option<(OwnedNode, OwnedNode)> = None;
    let mut child: Option<OwnedChange> = None;
    for c in acc {
        match c {
            OwnedChange::Add(n) => {
                // Under a CHILD fan-out a child flips exactly one gate, so at most one
                // branch converts to Add (`push-accumulated.ts:104-113`).
                debug_assert!(
                    !(fan_out_type == ChangeType::Child && add.is_some()),
                    "Fan-in:child expected at most one add when fan-out is of type child"
                );
                merge_node_into(&mut add, n);
            }
            OwnedChange::Remove(n) => {
                debug_assert!(
                    !(fan_out_type == ChangeType::Child && remove.is_some()),
                    "Fan-in:child expected at most one remove when fan-out is of type child"
                );
                merge_node_into(&mut remove, n);
            }
            OwnedChange::Edit { node, old } => merge_edit_into(&mut edit, node, old),
            child_change @ OwnedChange::Child { .. } => merge_child_into(&mut child, child_change),
        }
    }

    // Phase 2: emit by the fan-out's change type. Most arms wrap the survivor in
    // `add_empty_relationships`; the CHILD-survivor precedence emits RAW.
    match fan_out_type {
        ChangeType::Remove => {
            debug_assert!(
                add.is_none() && edit.is_none() && child.is_none(),
                "Fan-in:remove expected all removes"
            );
            remove.map(|r| add_empty_relationships(OwnedChange::Remove(r), schema))
        }
        ChangeType::Add => {
            debug_assert!(
                remove.is_none() && edit.is_none() && child.is_none(),
                "Fan-in:add expected all adds"
            );
            add.map(|a| add_empty_relationships(OwnedChange::Add(a), schema))
        }
        ChangeType::Edit => {
            debug_assert!(
                child.is_none(),
                "Fan-in:edit produced a Child branch change"
            );
            let result = if let Some((mut enode, mut eold)) = edit {
                // An Edit survived → it supersedes; merge any add into its new node
                // and any remove into its old node (`push-accumulated.ts:174-183`).
                if let Some(a) = add {
                    enode.rels = merge_owned_rels(enode.rels, a.rels);
                }
                if let Some(r) = remove {
                    eold.rels = merge_owned_rels(eold.rels, r.rels);
                }
                Some(OwnedChange::Edit {
                    node: enode,
                    old: eold,
                })
            } else {
                // No edit survived: both add+remove ⇒ reconstruct the edit; else the
                // single survivor (`push-accumulated.ts:202-218`).
                match (add, remove) {
                    (Some(a), Some(r)) => Some(OwnedChange::Edit { node: a, old: r }),
                    (Some(a), None) => Some(OwnedChange::Add(a)),
                    (None, Some(r)) => Some(OwnedChange::Remove(r)),
                    (None, None) => None,
                }
            };
            result.map(|c| add_empty_relationships(c, schema))
        }
        // CHILD collapse (`push-accumulated.ts:221-256`). Among {add, remove, child} at
        // most 2 types appear (a child flips exactly one gate), and `edit` is impossible.
        ChangeType::Child => {
            debug_assert!(
                edit.is_none(),
                "Fan-in:child produced an Edit branch change"
            );
            debug_assert!(
                [add.is_some(), remove.is_some(), child.is_some()]
                    .iter()
                    .filter(|b| **b)
                    .count()
                    <= 2,
                "Fan-in:child expected at most 2 types on a child change from fan-out"
            );
            // A preserved `Child` takes precedence over a converted Add/Remove: emit it
            // **RAW** (no `add_empty_relationships` — children only carry path rels) and
            // **discard** any sibling add/remove (no rel merge across the precedence
            // boundary, `push-accumulated.ts:237-241`).
            if let Some(c) = child {
                return Some(c);
            }
            // Else exactly one of add/remove survived (the relationship is unique to one
            // exists check, so the converters can't disagree).
            debug_assert!(
                !(add.is_some() && remove.is_some()),
                "Fan-in:child expected either add or remove, not both"
            );
            let survivor = match (add, remove) {
                (Some(a), _) => OwnedChange::Add(a),
                (None, Some(r)) => OwnedChange::Remove(r),
                (None, None) => return None,
            };
            Some(add_empty_relationships(survivor, schema))
        }
    }
}

// ---------------------------------------------------------------------------
// UnionFanOut / UnionFanIn operators
// ---------------------------------------------------------------------------

/// `UnionFanOut` (`union-fan-out.ts`): one input, **N branches**, built over the
/// source. `fetch` delegates to the input; `push` broadcasts the change to every
/// branch (each a filter pipeline and/or a [`FlippedJoin`](crate::op::FlippedJoin))
/// then drives the paired [`UnionFanIn`]'s collapse. Unlike the filter
/// [`FanOut`](crate::graph) it operates on full nodes via side-effecting
/// `Graph::push`, not the return-based filter `chain_push`.
pub struct UnionFanOut {
    pub input: NodeId,
    /// Branch heads + the port to push each on (a flipped branch receives on
    /// `JoinParent`, a filter branch on `Single`). Wired two-phase via `set_fan`.
    outputs: RefCell<Vec<OutEdge>>,
    fan_in: Cell<Option<NodeId>>,
}

impl UnionFanOut {
    pub fn new(input: NodeId) -> UnionFanOut {
        UnionFanOut {
            input,
            outputs: RefCell::new(Vec::new()),
            fan_in: Cell::new(None),
        }
    }

    /// Wire the branch broadcast edges + the paired fan-in (the `set_fan` analogue).
    pub(crate) fn set_fan(&self, branches: Vec<OutEdge>, fan_in: NodeId) {
        *self.outputs.borrow_mut() = branches;
        self.fan_in.set(Some(fan_in));
    }

    /// `fetch` delegates straight to the single input (`union-fan-out.ts:43`).
    pub fn fetch<'g>(&'g self, g: &'g Graph, req: &FetchRequest) -> NodeStream<'g> {
        g.fetch(self.input, req)
    }

    /// `push` (`union-fan-out.ts:27`): signal the fan-in, broadcast the **same change**
    /// (relationships intact, any type including `Child`) to each branch, then drain the
    /// fan-in's accumulation. The branches side-effect-push their outputs to the fan-in.
    ///
    /// The broadcast **preserves relationships for every change type** (the
    /// `materialize_change_preserving_node` contract, mirroring `graph.rs::fan_out_push`):
    /// a non-flipped EXISTS `Join` sits on the spine BELOW the fan-out, so the branches'
    /// `Exists` gates count their gated relationship off the broadcast change's node. The
    /// materialize runs **now**, under the active source overlay (post-change membership —
    /// see `source_common::gen_push`), then a fresh `Change<'g>` is rebuilt per branch (a
    /// `Change<'g>` is neither `Clone` nor `'static`).
    pub fn push<'g>(&'g self, g: &'g Graph, change: Change<'g>) {
        let fan_in = self.fan_in.get().expect("UnionFanOut fan_in not wired");
        // Clone the branch list out before broadcasting — never hold the RefCell
        // borrow across the reentrant branch pushes (the cardinal rule).
        let branches = self.outputs.borrow().clone();
        let fan_out_type = change.change_type();

        g.union_fan_in_op(fan_in).fan_out_started();
        let owned = materialize_change_preserving_node(change);
        for edge in &branches {
            g.push(edge.node, rebuild_change(owned.clone()), edge.port);
        }
        g.union_fan_in_op(fan_in).fan_out_done(g, fan_out_type);
    }
}

/// `UnionFanIn` (`union-fan-in.ts`): **N branch inputs**, one output. `fetch`
/// k-way-merges the branch fetches with PK dedup ([`merge_node_streams`](crate::op::merge_node_streams)).
/// `push` either **accumulates** (during a fan-out broadcast — the owned model) or
/// does a direct cross-branch dedup (a flipped child pushed while the fan-out is
/// idle). On drain it collapses the accumulation via `push_accumulated_changes`
/// and forwards the single result.
pub struct UnionFanIn {
    pub fan_out: NodeId,
    /// Branch tails — the nodes whose `fetch` is merged, and (for the
    /// internal-change dedup) the branches to cross-check.
    inputs: Vec<NodeId>,
    /// Per-branch **pushable constraint**, parallel to `inputs`: the necessary leaf
    /// equalities of each branch's `where` condition, derived at build time
    /// ([`pushable_constraint`](crate::builder)). Empty for a branch with no pushable
    /// equality. At `fetch` each branch's constraint is merged into the incoming
    /// request so a branch like `eq(pk)` **seeks** the shared source connection instead
    /// of full-scanning it — without this, every OR branch fetches the shared source
    /// with the *same* request, so a `eq(pk) OR exists` query full-scans the table on
    /// the eq branch even though it resolves to a single PK. The branch's own filter
    /// chain still applies the full predicate, so this only narrows the rows scanned;
    /// the result set is identical (the constraint is a *necessary* condition for the
    /// branch to keep a row).
    branch_constraints: Vec<Constraint>,
    /// The merged branch schema (carries the `primary_key` + the asserted-`Some`
    /// `sort` the merge needs, plus the unioned relationship slots).
    schema: Schema,
    /// The post-fan downstream **edge** — port-bearing (the `FilterEnd`/`Skip`/`Take`
    /// template), so the union-fan tail can feed a relationship join's `JoinParent`
    /// port (a `related` over a flipped `where`) as well as a plain `Single` sink.
    output: Cell<Option<OutEdge>>,
    /// The `#fanOutPushStarted` flag: `true` between `fan_out_started` and
    /// `fan_out_done` (accumulate); `false` ⇒ a direct internal change.
    fan_out_push_started: Cell<bool>,
    /// The owned accumulation (no `'g` — materialized at push time). See the module
    /// docs for why this is owned rather than `Vec<Change<'g>>`.
    accumulated: RefCell<Vec<OwnedChange>>,
}

impl UnionFanIn {
    pub fn new(
        fan_out: NodeId,
        inputs: Vec<NodeId>,
        branch_constraints: Vec<Constraint>,
        schema: Schema,
    ) -> UnionFanIn {
        assert!(
            !schema.sort.is_empty(),
            "UnionFanIn requires a defined sort (the branch-fetch merge needs sorted inputs)"
        );
        assert_eq!(
            inputs.len(),
            branch_constraints.len(),
            "UnionFanIn needs exactly one pushable constraint per branch input"
        );
        UnionFanIn {
            fan_out,
            inputs,
            branch_constraints,
            schema,
            output: Cell::new(None),
            fan_out_push_started: Cell::new(false),
            accumulated: RefCell::new(Vec::new()),
        }
    }

    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(crate) fn set_output(&self, edge: OutEdge) {
        self.output.set(Some(edge));
    }

    /// `fetch` (`union-fan-in.ts:103`): k-way merge of the branch fetches in
    /// `compare_rows` order (reverse-aware), deduping consecutive PK-equal rows (a
    /// row matched by two branches is yielded once). `Drop`-clean.
    ///
    /// Each branch is fetched through `fetch_branch`, which merges
    /// the branch's build-time pushable constraint into the request — so an `eq(pk)`
    /// branch seeks the shared source instead of full-scanning it (an OR-branch
    /// constraint accumulation, not in the JS, which re-scans per branch).
    pub fn fetch<'g>(&'g self, g: &'g Graph, req: &FetchRequest) -> NodeStream<'g> {
        let streams: Vec<NodeStream<'g>> = self
            .inputs
            .iter()
            .zip(&self.branch_constraints)
            .map(|(&i, bc)| self.fetch_branch(g, i, bc, req))
            .collect();
        crate::op::merge_node_streams(
            streams,
            self.schema.sort.clone(),
            req.reverse,
            Some(self.schema.primary_key.clone()),
        )
    }

    /// Fetch one OR branch, merging its build-time **pushable constraint** into the
    /// request so the branch seeks the shared source rather than full-scanning it
    /// (the OR-branch constraint accumulation — see [`branch_constraints`]).
    ///
    /// - an **empty** branch constraint passes `req` through unchanged;
    /// - a constraint that **contradicts** `req.constraint` (no row can satisfy both,
    ///   e.g. a `id = 5` branch under an incoming `id = 7` join key) yields an empty
    ///   stream — the branch matches nothing;
    /// - otherwise the branch fetches with `req.constraint ∧ branch_constraint`.
    ///
    /// This never changes results: the branch constraint is a *necessary* condition for
    /// the branch to keep a row, and the branch's filter chain still re-applies the full
    /// predicate downstream. The merged constraint keeps the row stream in connection
    /// sort order, so the fan-in's k-way merge + PK dedup are unaffected.
    ///
    /// [`branch_constraints`]: Self::branch_constraints
    fn fetch_branch<'g>(
        &'g self,
        g: &'g Graph,
        input: NodeId,
        branch_constraint: &Constraint,
        req: &FetchRequest,
    ) -> NodeStream<'g> {
        if branch_constraint.is_empty() {
            return g.fetch(input, req);
        }
        if let Some(c) = &req.constraint {
            if !constraints_are_compatible(c, branch_constraint) {
                return Box::new(std::iter::empty());
            }
        }
        let branch_req = FetchRequest {
            constraint: Some(merge_constraints(
                req.constraint.as_ref(),
                branch_constraint,
            )),
            multi_constraints: req.multi_constraints.clone(),
            start: req.start.clone(),
            reverse: req.reverse,
        };
        g.fetch(input, &branch_req)
    }

    /// `fanOutStartedPushing` (`union-fan-out.ts:29`): begin accumulating. Assert we
    /// are not already mid-broadcast (no nested fan-out into the *same* fan-in).
    pub(crate) fn fan_out_started(&self) {
        assert!(
            !self.fan_out_push_started.get(),
            "UnionFanIn already in a fan-out push"
        );
        debug_assert!(self.accumulated.borrow().is_empty());
        self.fan_out_push_started.set(true);
    }

    /// `push` (`union-fan-in.ts:116`): accumulate during a broadcast (materialize to
    /// owned **before** taking the borrow, so the reentrant relationship fetch in
    /// `materialize_change_preserving_node` never runs while the accumulation is
    /// borrowed), else a direct internal change.
    pub fn push<'g>(&'g self, g: &'g Graph, change: Change<'g>) {
        if self.fan_out_push_started.get() {
            // Preserve the top node's relationships across the accumulation (the
            // broadcast preserves them; the `Child` arm must keep them for the CHILD
            // collapse + `merge_owned_rels`). Behavior-identical to `materialize_change`
            // for Add/Remove/Edit — it only overrides the `Child` arm.
            let owned = materialize_change_preserving_node(change);
            self.accumulated.borrow_mut().push(owned);
        } else {
            self.push_internal_change(g, change);
        }
    }

    /// `fanOutDonePushing` (`union-fan-in.ts:193`): drain the accumulation, collapse
    /// it to one change, forward it. Take the accumulation out (dropping the borrow)
    /// **before** the collapse + downstream push re-enter the graph.
    pub(crate) fn fan_out_done<'g>(&'g self, g: &'g Graph, fan_out_type: ChangeType) {
        self.fan_out_push_started.set(false);
        let acc = std::mem::take(&mut *self.accumulated.borrow_mut());
        if self.inputs.is_empty() {
            return; // degenerate union (no branches)
        }
        if let Some(collapsed) = push_accumulated_changes(acc, fan_out_type, &self.schema) {
            let out = self.output.get().expect("UnionFanIn output not wired");
            g.push(out.node, rebuild_change(collapsed), out.port);
        }
    }

    /// `#pushInternalChange` (`union-fan-in.ts:145`): a branch's source pushed
    /// **directly** into the fan-in while the fan-out is idle — now ONLY a **flipped**
    /// EXISTS *child* change (a comment add/remove) that the branch's `FlippedJoin` turned
    /// into a parent Add/Remove. (Non-flipped EXISTS Joins sit on the spine BELOW the
    /// fan-out, so their child changes arrive through the broadcast + CHILD collapse, not
    /// here — matching JS `union-fan-in.ts:131-133`: normal exists joins are before the
    /// fan-out, related/take after.) It must be deduped across branches so an OR doesn't
    /// double-emit a row another branch already keeps.
    ///
    /// - **CHILD / EDIT** → forward unconditionally: a child's grandchild change (or a
    ///   non-key edit) keeps the row in exactly the same branches, so there is nothing
    ///   to dedup (each branch's child relationship is its own — `union-fan-in.ts:148`).
    /// - **ADD / REMOVE** → cross-branch existence check (`union-fan-in.ts:158`). The
    ///   JS skips the *pushing* branch and forwards iff no **other** branch still has
    ///   the row. We don't thread the pusher: instead we **count** the branches whose
    ///   fetch (the row's PK constraint, the source overlay active so the count is the
    ///   post-change membership) still yields the row. The pushing branch contributes
    ///   1 on an Add (its row is present post-add) and 0 on a Remove (its row is gone
    ///   post-remove), so "no other branch has it" is exactly `count == 1` for an Add
    ///   and `count == 0` for a Remove. (Equivalent to the pusher-skipping check, with
    ///   the overlay doing the accounting.)
    fn push_internal_change<'g>(&'g self, g: &'g Graph, change: Change<'g>) {
        debug_assert!(
            !self.inputs.is_empty(),
            "internal change into an empty union"
        );
        let out = self.output.get().expect("UnionFanIn output not wired");
        let (out_node, out_port) = (out.node, out.port);
        let forward = match &change {
            Change::Child { .. } | Change::Edit { .. } => true,
            Change::Add(_) | Change::Remove(_) => {
                let row = change.primary_row();
                let constraint: Constraint = self
                    .schema
                    .primary_key
                    .iter()
                    .map(|&c| (c, row.col(c).to_owned()))
                    .collect();
                // Count branches that still yield the row. Never holds a borrow across
                // the reentrant fetch (the cardinal rule): each stream is consumed by
                // `.next()` and dropped inside the closure.
                let count = self
                    .inputs
                    .iter()
                    .filter(|&&inp| {
                        g.fetch(inp, &FetchRequest::with_constraint(constraint.clone()))
                            .next()
                            .is_some()
                    })
                    .count();
                match &change {
                    Change::Add(_) => count == 1,    // only the pusher has it
                    Change::Remove(_) => count == 0, // no branch has it (pusher gone too)
                    _ => unreachable!(),
                }
            }
        };
        if forward {
            g.push(out_node, change, out_port);
        }
    }
}

#[cfg(test)]
mod accumulate_tests {
    use super::*;
    use crate::value::{owned_row, OwnedValue as V, RelDef};

    fn onode(id: i64, rels: Vec<(u32, Vec<OwnedNode>)>) -> OwnedNode {
        OwnedNode {
            row: owned_row(vec![V::Int(id)]),
            rels: rels.into_iter().map(|(s, c)| (RelId(s), c)).collect(),
        }
    }
    fn leaf(id: i64) -> OwnedNode {
        onode(id, vec![])
    }
    fn schema_with_rels(n: usize) -> Schema {
        let names = ["a", "b", "c"];
        Schema::new(vec!["id"], vec![0], vec![(0, true)])
            .with_relationships(names[..n].iter().map(|name| RelDef::new(name)).collect())
    }
    fn rels_of(c: &OwnedChange) -> Vec<u32> {
        let n = match c {
            OwnedChange::Add(n) | OwnedChange::Remove(n) => n,
            OwnedChange::Edit { node, .. } => node,
            OwnedChange::Child { .. } => unreachable!("no Child in the union collapse tests"),
        };
        let mut s: Vec<u32> = n.rels.iter().map(|(r, _)| r.0).collect();
        s.sort_unstable();
        s
    }

    #[test]
    fn empty_accumulation_yields_nothing() {
        assert!(push_accumulated_changes(vec![], ChangeType::Add, &schema_with_rels(0)).is_none());
    }

    #[test]
    fn add_collapses_many_to_one_unioning_relationships() {
        // Two branches keep the same Add row, each attaching a different rel slot →
        // one Add carrying both (the relationship-merge), plus the empty 3rd slot.
        let acc = vec![
            OwnedChange::Add(onode(1, vec![(0, vec![leaf(10)])])),
            OwnedChange::Add(onode(1, vec![(1, vec![leaf(20)])])),
        ];
        let out = push_accumulated_changes(acc, ChangeType::Add, &schema_with_rels(3)).unwrap();
        assert!(matches!(out, OwnedChange::Add(_)));
        assert_eq!(rels_of(&out), vec![0, 1, 2]); // 0,1 from branches + 2 empty-filled
    }

    #[test]
    fn edit_reconstructed_from_branch_split_add_and_remove() {
        // An edit fanned out; one branch turned it into a Remove(old), another into an
        // Add(new) → reconstruct Edit{node: add, old: remove}.
        let acc = vec![OwnedChange::Remove(leaf(1)), OwnedChange::Add(leaf(2))];
        let out = push_accumulated_changes(acc, ChangeType::Edit, &schema_with_rels(0)).unwrap();
        let int = |n: &OwnedNode| match n.row.col(0) {
            crate::value::Value::Int(i) => i,
            _ => panic!("expected Int"),
        };
        match out {
            OwnedChange::Edit { node, old } => {
                assert_eq!(int(&node), 2);
                assert_eq!(int(&old), 1);
            }
            _ => panic!("expected reconstructed Edit"),
        }
    }

    #[test]
    fn edit_survivor_supersedes_and_absorbs_add_remove_rels() {
        let acc = vec![
            OwnedChange::Edit {
                node: onode(2, vec![(0, vec![])]),
                old: onode(1, vec![]),
            },
            OwnedChange::Add(onode(2, vec![(1, vec![leaf(9)])])),
        ];
        let out = push_accumulated_changes(acc, ChangeType::Edit, &schema_with_rels(2)).unwrap();
        // Edit survives; its node absorbs the add's rel slot 1 (plus its own slot 0).
        assert_eq!(rels_of(&out), vec![0, 1]);
    }

    #[test]
    fn edit_with_only_one_survivor_emits_that() {
        let acc = vec![OwnedChange::Remove(leaf(1))];
        let out = push_accumulated_changes(acc, ChangeType::Edit, &schema_with_rels(0)).unwrap();
        assert!(matches!(out, OwnedChange::Remove(_)));
    }

    // --- CHILD collapse (push-accumulated.ts:221-256) ---

    fn child(node: OwnedNode, slot: u32, sub: OwnedChange) -> OwnedChange {
        OwnedChange::Child {
            node,
            rel: RelId(slot),
            sub: Box::new(sub),
        }
    }

    #[test]
    fn child_survivor_wins_and_discards_a_converted_add() {
        // A child-add to relationship `a` (slot 0): the leaf-filter branch PRESERVES the
        // Child; the `Exists` branch converts it to an Add. Child precedence → emit the
        // Child RAW, discard the Add, and do NOT empty-fill (a Child keeps only path rels).
        let acc = vec![
            child(
                onode(7, vec![(0, vec![leaf(70)])]),
                0,
                OwnedChange::Add(leaf(70)),
            ),
            OwnedChange::Add(onode(7, vec![(1, vec![leaf(71)])])),
        ];
        let out = push_accumulated_changes(acc, ChangeType::Child, &schema_with_rels(3)).unwrap();
        match out {
            OwnedChange::Child { node, rel, sub } => {
                assert_eq!(rel, RelId(0));
                // RAW: only slot 0 (its path rel) — NOT empty-filled to slots 1,2.
                let slots: Vec<u32> = node.rels.iter().map(|(r, _)| r.0).collect();
                assert_eq!(slots, vec![0]);
                assert!(matches!(*sub, OwnedChange::Add(_)));
            }
            _ => panic!("expected the preserved Child to win"),
        }
    }

    #[test]
    fn child_converted_to_add_when_no_branch_preserves_it() {
        // No branch preserved the Child; one `Exists` gate flipped 0→1 → Add. Empty-filled.
        let acc = vec![OwnedChange::Add(onode(7, vec![(0, vec![leaf(70)])]))];
        let out = push_accumulated_changes(acc, ChangeType::Child, &schema_with_rels(2)).unwrap();
        assert!(matches!(out, OwnedChange::Add(_)));
        assert_eq!(rels_of(&out), vec![0, 1]); // slot 0 + empty-filled slot 1
    }

    #[test]
    fn child_converted_to_remove_when_no_branch_preserves_it() {
        // One `Exists` gate flipped 1→0 → Remove. Empty-filled.
        let acc = vec![OwnedChange::Remove(onode(7, vec![(0, vec![])]))];
        let out = push_accumulated_changes(acc, ChangeType::Child, &schema_with_rels(2)).unwrap();
        assert!(matches!(out, OwnedChange::Remove(_)));
        assert_eq!(rels_of(&out), vec![0, 1]);
    }

    #[test]
    fn two_preserved_children_merge_top_node_rels_keeping_first_subchange() {
        // Two branches preserve the same Child, each attaching a different top-node rel
        // slot. Merge unions the top-node rels (left/first wins) and keeps the FIRST
        // branch's sub-change + slot — never concatenating the grandchild.
        let acc = vec![
            child(
                onode(7, vec![(0, vec![leaf(70)])]),
                0,
                OwnedChange::Add(leaf(700)),
            ),
            child(
                onode(7, vec![(1, vec![leaf(71)])]),
                0,
                OwnedChange::Add(leaf(999)),
            ),
        ];
        let out = push_accumulated_changes(acc, ChangeType::Child, &schema_with_rels(2)).unwrap();
        match out {
            OwnedChange::Child { node, rel, sub } => {
                assert_eq!(rel, RelId(0));
                let mut slots: Vec<u32> = node.rels.iter().map(|(r, _)| r.0).collect();
                slots.sort_unstable();
                assert_eq!(slots, vec![0, 1]); // unioned top-node rels, NOT empty-filled
                                               // First branch's sub-change kept (700, not 999).
                match *sub {
                    OwnedChange::Add(n) => {
                        assert!(matches!(n.row.col(0), crate::value::Value::Int(700)))
                    }
                    _ => panic!("expected the first branch's Add sub-change"),
                }
            }
            _ => panic!("expected a merged Child"),
        }
    }

    #[test]
    fn child_fanout_with_no_surviving_branch_yields_nothing() {
        // Every branch dropped the child (a filter excluded it on all branches).
        assert!(
            push_accumulated_changes(vec![], ChangeType::Child, &schema_with_rels(2)).is_none()
        );
    }
}
