//! Owned, fully-materialized change events off the dataflow pipeline.
//!
//! These types ([`CaughtChange`] / [`CaughtNode`]) and the [`expand_change`] /
//! [`expand_node`] materializers were originally part of the spec-`11` test oracle
//! (`crate::testkit`). They are **graduated into the production crate** here because
//! they are the canonical *owned* form of a downstream [`Change`](crate::change::Change):
//! the production change-stream sink ([`Graph::add_change_sink`](crate::graph::Graph))
//! and out-of-crate consumers (the `rindle-replica` live-query wrapper) need them outside
//! `cfg(test)`/`testkit` builds. `testkit` re-exports them, so existing oracle code and
//! `rindle::testkit::*` paths are unchanged.
//!
//! Why owned: a dataflow [`Change<'g>`](crate::change::Change) borrows the graph (its
//! relationship thunks are `+ 'g`), so it cannot survive past the push that produced it.
//! [`expand_change`] drains every lazy relationship thunk eagerly into an owned tree,
//! which *can* cross a callback / thread / process boundary.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::change::{Change, Node};
use crate::value::{compare_values, OwnedRow, RelId};

// ---------------------------------------------------------------------------
// The comparison tree: CaughtNode / CaughtChange (catch.ts `CaughtNode` /
// `CaughtChange`, with the `'yield'` variant gone — Primitive #6)
// ---------------------------------------------------------------------------

/// A fully-materialized node: the row cells plus **eagerly drained**
/// relationships. The comparison unit for a fetch/push assertion, and the owned
/// node a [`Graph::add_change_sink`](crate::graph::Graph) consumer receives.
///
/// Relationships are keyed by their resolved [`RelId`] slot in a `BTreeMap`, so
/// the diff is slot-stable regardless of insertion order; the child `Vec` within
/// a relationship preserves the **operator's sort order** (the order *is* part of
/// the contract — never re-sort it, `11` §3.8).
#[derive(Clone, Debug)]
pub struct CaughtNode {
    pub row: OwnedRow,
    pub relationships: BTreeMap<RelId, Vec<CaughtNode>>,
}

/// A caught downstream change. Mirrors `catch.ts` `expandChange` output: an
/// `Edit` carries only the two rows (no node, `catch.ts:104-109`); a `Child`
/// carries the parent row, the relationship slot, and the nested change
/// (`catch.ts:110-118`).
#[derive(Clone, Debug)]
pub enum CaughtChange {
    Add(CaughtNode),
    Remove(CaughtNode),
    Edit {
        old: OwnedRow,
        row: OwnedRow,
    },
    Child {
        row: OwnedRow,
        rel: RelId,
        change: Box<CaughtChange>,
    },
}

/// `expandNode` (`catch.ts:124-136`): clone the row and **drain every
/// relationship `NodeStream` thunk eagerly**, recursing. This is the only place a
/// consumer fully forces the lazy thunks — exactly what reveals overlay/position
/// bugs. No graph handle is needed: each thunk already owns its `&'g Graph`
/// (Primitive #5, captured by value at `Node` construction).
pub fn expand_node(node: &Node<'_>) -> CaughtNode {
    let mut relationships: BTreeMap<RelId, Vec<CaughtNode>> = BTreeMap::new();
    for r in &node.rels {
        let children: Vec<CaughtNode> = (r.thunk)().map(|c| expand_node(&c)).collect();
        // A node never carries two relationships for the same slot; if it somehow
        // did, the later one wins (matching JS object-key overwrite).
        relationships.insert(r.slot, children);
    }
    CaughtNode {
        row: node.row.clone(),
        relationships,
    }
}

/// `expandChange` (`catch.ts:92-122`).
pub fn expand_change(change: &Change<'_>) -> CaughtChange {
    match change {
        Change::Add(n) => CaughtChange::Add(expand_node(n)),
        Change::Remove(n) => CaughtChange::Remove(expand_node(n)),
        Change::Edit { node, old } => CaughtChange::Edit {
            old: old.row.clone(),
            row: node.row.clone(),
        },
        Change::Child { node, rel, child } => CaughtChange::Child {
            row: node.row.clone(),
            rel: *rel,
            change: Box::new(expand_change(child)),
        },
    }
}

// ---------------------------------------------------------------------------
// The diff predicate (§3.8): structural equality with full-cell content
// comparison. `OwnedValue` has no derived `PartialEq` (it would invite the wrong
// comparator), so we spell row equality out via `compare_values` (null == null,
// floats via total_cmp — `11` §3.8).
// ---------------------------------------------------------------------------

#[inline]
fn row_eq(a: &OwnedRow, b: &OwnedRow) -> bool {
    a.len() == b.len()
        && a.cells()
            .zip(b.cells())
            .all(|(x, y)| compare_values(x, y) == Ordering::Equal)
}

impl PartialEq for CaughtNode {
    fn eq(&self, o: &Self) -> bool {
        // `BTreeMap`/`Vec<CaughtNode>` equality recurses into this impl; the only
        // hand-written part is the row (no `OwnedValue: Eq`).
        row_eq(&self.row, &o.row) && self.relationships == o.relationships
    }
}
impl Eq for CaughtNode {}

impl PartialEq for CaughtChange {
    fn eq(&self, o: &Self) -> bool {
        use CaughtChange::*;
        match (self, o) {
            (Add(a), Add(b)) | (Remove(a), Remove(b)) => a == b,
            (Edit { old: o1, row: r1 }, Edit { old: o2, row: r2 }) => {
                row_eq(o1, o2) && row_eq(r1, r2)
            }
            (
                Child {
                    row: r1,
                    rel: s1,
                    change: c1,
                },
                Child {
                    row: r2,
                    rel: s2,
                    change: c2,
                },
            ) => row_eq(r1, r2) && s1 == s2 && c1 == c2,
            _ => false,
        }
    }
}
impl Eq for CaughtChange {}
