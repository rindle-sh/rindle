//! The dataflow value types: `Node`, the lazy relationship stream, `Change`,
//! `SourceChange`, `Constraint`, `FetchRequest`, and the overlay snapshots.
//!
//! Primitive #6: the `'yield'` sentinel is **dropped**. In the JS engine every
//! stream element is `Node | 'yield'` (a cooperative-responsiveness hack for the
//! single JS event loop, stripped before the view by `skipYields`). The port
//! drops it entirely: `NodeStream`'s item is just `Node`. Responsiveness, if
//! ever needed, is an explicit budget threaded in — never unioned into the item.

use crate::graph::Graph;
// `Row` aliases the canonical owned row; the dataflow types are always owned (a
// `Node` is buffered, spliced with overlays, and handed to the view — none of
// which a step-buffer borrow could outlive), so there is no `Value<'a>` here.
use crate::value::{ColId, OwnedRow as Row, OwnedValue, RelId};

/// A lazy, single-call stream of nodes. Boxed `dyn Iterator` because the
/// pipeline is built from a runtime AST (see README) — operator dispatch is
/// inherently dynamic; only the leaves can be monomorphized.
///
/// `'g` is the graph borrow: streams and relationship thunks hold `&'g Graph`
/// (a *shared* borrow). Reentrancy works precisely because every fetch is a
/// shared borrow of the same graph — no `&mut Graph`, no `RefCell<dyn Operator>`.
pub type NodeStream<'g> = Box<dyn Iterator<Item = Node<'g>> + 'g>;

/// A lazy stream of **owned rows** — the currency of the *source* read pipeline.
///
/// A source emits rows, NOT nodes: the overlay splice, start/constraint/filter
/// chain, and k-way merge ([`source_common`](crate::source_common)) all operate on
/// `RowFlow`. Rows become [`Node`]s only when they cross the **connection**
/// boundary (`Graph::fetch` on a `SourceConn` wraps each row in a leaf node);
/// downstream **joins** then attach relationships to those nodes. This keeps the
/// shared source machinery free of any `Node`/relationship concept (`04` §1.1).
///
/// `'g` mirrors [`NodeStream`] for symmetry and to admit a borrowing leaf (the
/// SQLite cursor borrows `&'g self`); the memory leaf's flow is effectively owned.
pub type RowFlow<'g> = Box<dyn Iterator<Item = Row> + 'g>;

/// A row plus its lazily-produced relationships.
///
/// Relationships are index-addressed (`Vec`, not a string map) and each is a
/// **thunk** producing a *fresh* stream per call — mirroring JS
/// `relationships: Record<string, () => Stream>`. The thunk owns everything it
/// needs (graph ref + captured-by-value overlay), so it can be invoked at view
/// time without reading any live operator field.
pub struct Node<'g> {
    pub row: Row,
    pub rels: Vec<Relationship<'g>>,
}

pub struct Relationship<'g> {
    /// The resolved relationship slot (foundations §3.4 — index, never a string;
    /// the name lives in `Schema::relationships[slot]`). The View maps it back to
    /// a name for output.
    pub slot: RelId,
    pub thunk: Box<dyn Fn() -> NodeStream<'g> + 'g>,
}

impl<'g> Node<'g> {
    /// A leaf node (source row): no relationships.
    pub fn leaf(row: Row) -> Node<'g> {
        Node {
            row,
            rels: Vec::new(),
        }
    }
}

/// A downstream incremental change. Mirrors `zql/src/ivm/change.ts` but as a
/// real tagged union (the JS uses a tuple where slot 2 aliases OLD_NODE /
/// CHILD_DATA — the enum removes that aliasing footgun).
pub enum Change<'g> {
    Add(Node<'g>),
    Remove(Node<'g>),
    /// An in-place edit: the row's PK is unchanged but some columns differ. Holds
    /// both the new node and the old node (`makeEditChange` — `change.ts`). A
    /// connection with split-edit keys never *sees* this (the source decomposes it
    /// into Remove(old)+Add(new) before fan-out, §3.6); it survives only for
    /// connections that don't split.
    Edit {
        node: Node<'g>,
        old: Node<'g>,
    },
    Child {
        node: Node<'g>,
        /// The relationship slot the child change belongs to (index, not a name —
        /// foundations §3.4). The View resolves it to a name via the schema.
        rel: RelId,
        child: Box<Change<'g>>,
    },
}

/// The *kind* of a [`Change`], independent of its payload. Used by the OR
/// fan-in collapse (`push_accumulated_changes`, `06` §3.5): the dedup decision
/// keys off the **fan-out's** original change type (was it an Add/Remove/Edit/
/// Child that entered the fan), so we need to name the type without owning a
/// `Change`. Mirrors the JS `change.type` string discriminant.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ChangeType {
    Add,
    Remove,
    Edit,
    Child,
}

impl<'g> Change<'g> {
    /// This change's [`ChangeType`] discriminant (payload-free).
    pub fn change_type(&self) -> ChangeType {
        match self {
            Change::Add(_) => ChangeType::Add,
            Change::Remove(_) => ChangeType::Remove,
            Change::Edit { .. } => ChangeType::Edit,
            Change::Child { .. } => ChangeType::Child,
        }
    }

    /// The row identifying this change's node (the *new* row for an `Edit`; the
    /// changed-child's row for a `Child`). On a child-side join push this is the row
    /// whose join key selects the matching parents (`join.ts#pushChildChange`).
    pub fn primary_row(&self) -> &Row {
        match self {
            Change::Add(n) | Change::Remove(n) => &n.row,
            Change::Edit { node, .. } => &node.row,
            Change::Child { node, .. } => &node.row,
        }
    }
}

/// A change applied at the source (before fan-out to connections). Mirrors the
/// `SourceChange` tagged tuple (`source.ts`): Add/Remove carry one row, Edit
/// carries the new `row` and the `old` row it replaces.
#[derive(Clone, Debug)]
pub enum SourceChange {
    Add(Row),
    Remove(Row),
    Edit { row: Row, old: Row },
}

impl SourceChange {
    /// The "primary" row of the change (the new row for an edit). For existence
    /// asserts an Edit checks `old`, handled separately (`source_common::gen_push`).
    pub fn row(&self) -> &Row {
        match self {
            SourceChange::Add(r) | SourceChange::Remove(r) => r,
            SourceChange::Edit { row, .. } => row,
        }
    }
}

/// `(column, value)` equality constraints, ANDed. Index-addressed. Owned values
/// (`Vec<(ColId, OwnedValue)>`) because a constraint outlives the fetch that
/// produced its source row (foundations §3.3). Insertion order is significant: it
/// determines the index sort the source builds to seek the constraint (mirrors
/// the JS `Object.keys(constraint)` order — `memory-source.ts:289`).
pub type Constraint = Vec<(ColId, OwnedValue)>;

/// A disjunction of [`Constraint`]s — a row matches the multiConstraint iff it
/// matches **some** entry. FlippedJoin lowers an IN-batch to these
/// (`operator.ts` `MultiConstraint`). The memory leaf drives a sub-fetch per
/// entry and k-way-merges (`#fetchMulti`); the SQLite leaf lowers to native `IN`.
pub type MultiConstraint = Vec<Constraint>;

/// True if `row` satisfies every `(col, value)` pair. Uses
/// [`values_equal`](crate::value::values_equal)
/// (null ≠ null — SQL/join semantics), matching the JS `constraintMatchesRow`
/// (`constraint.ts:17`). **Not** `compare_values` (which treats null == null):
/// a constraint value of null can never match (and joins never produce one —
/// `build_join_constraint` returns `None` on null, §3.8).
pub fn constraint_matches(row: &Row, c: &Constraint) -> bool {
    c.iter()
        .all(|(col, v)| crate::value::values_equal(row.col(*col), v.as_ref()))
}

/// True if the constraint's columns are exactly the primary-key columns (as a
/// set). Mirrors `constraintMatchesPrimaryKey` (`constraint.ts:46`): when this
/// holds and the PK is a single column, the fetch can skip appending the
/// requested sort to the index sort (there is at most one matching row, §3.3).
pub fn constraint_matches_primary_key(c: &Constraint, pk: &[ColId]) -> bool {
    if c.len() != pk.len() {
        return false;
    }
    pk.iter().all(|p| c.iter().any(|(col, _)| col == p))
}

/// Merge two constraints, `extra` overriding `base` on shared columns — the
/// `{...base, ...extra}` of `#fetchMulti` (`memory-source.ts:398`). Order: base
/// columns first (values overridden in place), then `extra`'s new columns
/// appended, matching JS object-spread key order.
/// `constraintsAreCompatible` (`constraint.ts`): are two constraints jointly
/// satisfiable? Compatible iff **no shared column disagrees** — for every column
/// present in *both* `a` and `b`, the values must be
/// [`values_equal`](crate::value::values_equal). Columns appearing in only one
/// side place no requirement on the other.
///
/// Used by [`FlippedJoin`](crate::op::FlippedJoin)'s batched fetch: a child-derived
/// parent constraint that contradicts the incoming `req.constraint` (e.g. a chained
/// flipped join passing a parent-key constraint through) could never match, so it
/// is dropped before it enters the IN-batch (`flipped-join.ts:250`).
pub fn constraints_are_compatible(a: &Constraint, b: &Constraint) -> bool {
    a.iter().all(|(col, va)| {
        b.iter()
            .find(|(c, _)| c == col)
            .is_none_or(|(_, vb)| crate::value::values_equal(va.as_ref(), vb.as_ref()))
    })
}

pub fn merge_constraints(base: Option<&Constraint>, extra: &Constraint) -> Constraint {
    let Some(base) = base else {
        return extra.clone();
    };
    let mut out: Constraint = base
        .iter()
        .map(|(col, v)| {
            match extra.iter().find(|(c, _)| c == col) {
                Some((_, ev)) => (*col, ev.clone()), // overridden value, base position
                None => (*col, v.clone()),
            }
        })
        .collect();
    for (col, v) in extra {
        if !base.iter().any(|(c, _)| c == col) {
            out.push((*col, v.clone()));
        }
    }
    out
}

/// Build the constraint for a join hop: maps each `to_key` column to the
/// `from_row`'s `from_key` value. Returns `None` if any value is null (null
/// cannot join — `values_equal` semantics).
pub fn build_join_constraint(
    from_row: &Row,
    from_key: &[ColId],
    to_key: &[ColId],
) -> Option<Constraint> {
    let mut out = Vec::with_capacity(from_key.len());
    for i in 0..from_key.len() {
        let v = from_row.col(from_key[i]);
        if v.is_null() {
            return None;
        }
        out.push((to_key[i], v.to_owned()));
    }
    Some(out)
}

#[derive(Clone)]
pub enum Basis {
    At,
    After,
}

#[derive(Clone)]
pub struct Start {
    pub row: Row,
    pub basis: Basis,
}

/// What `Input::fetch` receives. `multi_constraints` (from FlippedJoin) is a
/// disjunction-of-conjunctions IN-batch; empty ⇒ the single-constraint path.
#[derive(Clone, Default)]
pub struct FetchRequest {
    pub constraint: Option<Constraint>,
    pub multi_constraints: Vec<MultiConstraint>,
    pub start: Option<Start>,
    pub reverse: bool,
}

impl FetchRequest {
    pub fn all() -> FetchRequest {
        FetchRequest::default()
    }
    pub fn with_constraint(c: Constraint) -> FetchRequest {
        FetchRequest {
            constraint: Some(c),
            ..Default::default()
        }
    }
    /// Mirrors the `#fetch` guard: `multiConstraints?.some(mc => mc.length > 0)`
    /// (`memory-source.ts:264`). Empty entries don't count.
    pub fn has_multi(&self) -> bool {
        self.multi_constraints.iter().any(|mc| !mc.is_empty())
    }
}

/// Primitive #5: the JOIN overlay, **snapshotted by value** into a relationship
/// thunk. In JS the child-stream closure reads the live `#inprogressChildChange`
/// field mid-push (the borrow checker's nemesis). Here the in-progress child
/// change is *captured by value* at Node construction, so the thunk owns its
/// overlay and never touches a shared mutable field.
///
/// Carries the in-progress child change as a [`SourceChange`] (rows): `Add` splices
/// the child in, `Remove` splices it out, `Edit` does both (remove old, add new).
/// `SourceChange` (not the spec-`06` `Change`) keeps the overlay **cloneable** —
/// the thunk captures a fresh copy per parent — and matches the spike's reality
/// that an in-progress child change is a leaf source row.
///
/// **Deviation from spec `06` (deferred, coupled to the View):** the production
/// overlay is `{change, position}` with a `compareRows(parent, position) > 0` gate
/// and a node-level overlay that *suppresses* the in-flight child (the child is
/// delivered incrementally by the production `ArrayView`, spec `09`). The spike's
/// `View` instead **re-materializes** each parent's children, so the overlay
/// *adds* the in-flight child (opposite polarity) and needs no `position`. Adopting
/// the production `{change, position}` gate is co-designed with the production View
/// and lands with it.
#[derive(Clone)]
pub struct JoinOverlay {
    pub change: SourceChange,
}

impl JoinOverlay {
    /// The row-level overlay for an in-flight child change, or `None` for a nested
    /// [`Change::Child`].
    ///
    /// A row-level overlay (`Add`/`Remove`/`Edit`) splices a leaf child row into a
    /// parent's child stream (the spike re-materialize polarity, see the struct
    /// doc). A nested `Child` returns `None` because no consumer needs the
    /// overlay-spliced parent stream on a `Change::Child`: the [`View`](crate::view)
    /// (`ViewChange::from_change`) and the testkit `Catch` (`expand_change`) keep only
    /// the parent **row** and recurse into the carried sub-change — they never drain
    /// the parent's relationship thunks. (The one operator that *does* drain a parent
    /// thunk on a `Child` — [`Exists`](crate::op::Exists), re-counting its gated
    /// relationship — wants the source-truth size, which the active source overlay
    /// already supplies; the join overlay would be the wrong input there.)
    pub(crate) fn for_child_change(change: &Change) -> Option<JoinOverlay> {
        match change {
            Change::Add(n) => Some(JoinOverlay {
                change: SourceChange::Add(n.row.clone()),
            }),
            Change::Remove(n) => Some(JoinOverlay {
                change: SourceChange::Remove(n.row.clone()),
            }),
            Change::Edit { node, old } => Some(JoinOverlay {
                change: SourceChange::Edit {
                    row: node.row.clone(),
                    old: old.row.clone(),
                },
            }),
            Change::Child { .. } => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Owned (no-`'g`) change materialization
// ---------------------------------------------------------------------------
//
// A [`Change<'g>`] borrows the graph (its relationship thunks are `+ 'g`), so it
// is neither `'static` nor `Clone`. Two places need to hold a change *off the
// stack* and reconstitute it later:
//
//   * the join child-push ([`Graph::push_child_change`](crate::graph)) materializes
//     the in-flight child change **once**, then rebuilds a fresh `Change<'g>` per
//     matching parent (a child can join to many parents, each needing its own
//     forwardable copy); and
//   * the [`UnionFanIn`](crate::op::UnionFanIn) accumulates each OR branch's output
//     across a broadcast, collapses, and rebuilds one change to forward.
//
// The owned tree is exactly what the `View`/`Catch` already build under the
// re-materialize model: the thunks run **now** (during the push, while the source
// overlay is active), capturing the post-change relationship state. No `unsafe`,
// no `'static`-`Change` field.

/// An owned (no-`'g`) materialization of a [`Node`]: its row plus its fully-drained
/// relationship subtrees (slot → children). Mirrors the testkit's `CaughtNode`.
#[derive(Clone)]
pub struct OwnedNode {
    pub row: Row,
    pub rels: Vec<(RelId, Vec<OwnedNode>)>,
}

/// An owned change. `Add`/`Remove`/`Edit` carry materialized nodes; `Child` carries
/// the changed node, the relationship slot, and the owned sub-change.
///
/// The `Child` **node** usually carries an empty `rels` (the cheap default — the
/// View/Catch read only its row and recurse into `sub`, see
/// `JoinOverlay::for_child_change`). `materialize_change_preserving_node`
/// instead drains the node's relationships, which the **filter fan** broadcast
/// needs: an [`Exists`](crate::op::Exists) branch downstream of a `FanOut` counts
/// its gated relationship off the change's node, so the broadcast rebuild must keep
/// it (the keystone join path leaves it empty to stay cheap).
#[derive(Clone)]
pub enum OwnedChange {
    Add(OwnedNode),
    Remove(OwnedNode),
    Edit {
        node: OwnedNode,
        old: OwnedNode,
    },
    Child {
        node: OwnedNode,
        rel: RelId,
        sub: Box<OwnedChange>,
    },
}

/// Drain a node's relationship thunks into an owned subtree (recursively). The
/// thunks run **now**, capturing the post-change relationship state.
pub(crate) fn materialize_node(n: &Node) -> OwnedNode {
    OwnedNode {
        row: n.row.clone(),
        rels: n
            .rels
            .iter()
            .map(|r| (r.slot, (r.thunk)().map(|c| materialize_node(&c)).collect()))
            .collect(),
    }
}

/// Materialize a [`Change<'g>`] into an owned change (consumes the change).
pub(crate) fn materialize_change(c: Change) -> OwnedChange {
    match c {
        Change::Add(n) => OwnedChange::Add(materialize_node(&n)),
        Change::Remove(n) => OwnedChange::Remove(materialize_node(&n)),
        Change::Edit { node, old } => OwnedChange::Edit {
            node: materialize_node(&node),
            old: materialize_node(&old),
        },
        // A `Child` keeps only the parent row + recurses; the parent node's own
        // relationships are not consumed by the View/Catch (they read the row and
        // recurse into `sub`), so the default drops them — the keystone optimization.
        // The filter-fan broadcast needs them and uses
        // [`materialize_change_preserving_node`] instead.
        Change::Child { node, rel, child } => OwnedChange::Child {
            node: OwnedNode {
                row: node.row,
                rels: Vec::new(),
            },
            rel,
            sub: Box::new(materialize_change(*child)),
        },
    }
}

/// Materialize a [`Change<'g>`] for the **filter-fan broadcast**, preserving the
/// change's node relationships across every variant. A `FanOut` replays one change to
/// every OR branch; an [`Exists`](crate::op::Exists) branch counts its gated
/// relationship off the change's node, so the rebuilt copy each branch receives must
/// carry that relationship — drained **now**, under the active source overlay, so the
/// count reflects the post-change membership (exactly what the live, non-fanned
/// `Exists` reads).
///
/// `Add`/`Remove`/`Edit` already materialize full nodes ([`materialize_change`]); only
/// [`Change::Child`] differs — [`materialize_change`] drops a Child's node rels (the
/// keystone optimization, since the join child-push's downstream View/Catch never read
/// them), so this overrides that one arm to keep the **top** node's relationships (the
/// OR branches gate the top node; the nested sub-change rides along via the cheap path).
pub(crate) fn materialize_change_preserving_node(c: Change) -> OwnedChange {
    match c {
        Change::Child { node, rel, child } => OwnedChange::Child {
            node: materialize_node(&node),
            rel,
            sub: Box::new(materialize_change(*child)),
        },
        other => materialize_change(other),
    }
}

/// Like [`materialize_change`], but an `Edit`'s **old** node is materialized
/// ROW-ONLY (empty `rels`) — its relationship subtree is drained by no downstream.
/// [`ViewChange::from_change`](crate::view::ViewChange::from_change) reduces an Edit
/// to `(row, old_row)`, [`Take`](crate::op::Take)'s edit matrix keys off `old.row`,
/// and [`Exists`](crate::op::Exists) gates only the **new** node's slot — nothing ever
/// reads `old`'s children. Draining them is a reentrant leaf refetch of a subtree the
/// consumer already holds unchanged (the join-key-immutability contract guarantees a
/// child edit cannot alter this relationship's correlation, so `old`'s subtree equals
/// the one already materialized). This skips that work in [`Join::push_child_change`].
/// The **new** node is still fully materialized (an `Exists` on the forward path
/// legitimately reads its gated slot).
pub(crate) fn materialize_change_edit_old_row_only(c: Change) -> OwnedChange {
    match c {
        Change::Edit { node, old } => OwnedChange::Edit {
            node: materialize_node(&node),
            old: OwnedNode {
                row: old.row,
                rels: Vec::new(),
            },
        },
        other => materialize_change(other),
    }
}

/// Rebuild a `Node<'g>` from an owned node: each relationship becomes a thunk that
/// yields its owned children (cloned per call). Owned data is `'static`, so the
/// rebuilt thunk is valid for any `'g`.
fn rebuild_node<'g>(on: OwnedNode) -> Node<'g> {
    let rels = on
        .rels
        .into_iter()
        .map(|(slot, children)| Relationship {
            slot,
            thunk: Box::new(move || {
                let children = children.clone();
                Box::new(children.into_iter().map(rebuild_node)) as NodeStream<'g>
            }),
        })
        .collect();
    Node { row: on.row, rels }
}

/// Rebuild a forwardable `Change<'g>` from an owned change.
pub(crate) fn rebuild_change<'g>(oc: OwnedChange) -> Change<'g> {
    match oc {
        OwnedChange::Add(n) => Change::Add(rebuild_node(n)),
        OwnedChange::Remove(n) => Change::Remove(rebuild_node(n)),
        OwnedChange::Edit { node, old } => Change::Edit {
            node: rebuild_node(node),
            old: rebuild_node(old),
        },
        OwnedChange::Child { node, rel, sub } => Change::Child {
            node: rebuild_node(node),
            rel,
            child: Box::new(rebuild_change(*sub)),
        },
    }
}

/// Marker for which join port a change arrived on (replaces JS's two distinct
/// `setOutput({push: ...})` closures).
#[derive(Clone, Copy, Debug)]
pub enum Port {
    /// The sole downstream edge of a single-output operator.
    Single,
    /// Arrived via the join's parent input.
    JoinParent,
    /// Arrived via the join's child input.
    JoinChild,
}

/// A downstream edge: who to push to, and on which of their input ports.
#[derive(Clone, Copy)]
pub struct OutEdge {
    pub node: crate::graph::NodeId,
    pub port: Port,
}

/// Silence "unused" for fields kept for fidelity/clarity.
#[allow(dead_code)]
fn _doc_link(_: &Graph) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::OwnedValue as V;

    #[test]
    fn constraints_compatible_on_shared_columns() {
        // Disjoint columns: always compatible (neither constrains the other).
        let a: Constraint = vec![(0, V::Int(1))];
        let b: Constraint = vec![(1, V::Int(2))];
        assert!(constraints_are_compatible(&a, &b));

        // Shared column, equal value: compatible.
        let a: Constraint = vec![(0, V::Int(1)), (1, V::Int(2))];
        let b: Constraint = vec![(0, V::Int(1))];
        assert!(constraints_are_compatible(&a, &b));
        assert!(constraints_are_compatible(&b, &a)); // symmetric on the shared set

        // Shared column, conflicting value: incompatible.
        let b: Constraint = vec![(0, V::Int(9))];
        assert!(!constraints_are_compatible(&a, &b));

        // Empty constraint imposes nothing.
        let empty: Constraint = Vec::new();
        assert!(constraints_are_compatible(&a, &empty));
        assert!(constraints_are_compatible(&empty, &a));
    }
}
