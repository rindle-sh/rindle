//! Flat change events for cross-process / cross-language view reconstruction.
//!
//! See `FLAT-CHANGES-DESIGN.md` for the full wire format **and** the receiver
//! `apply_change` contract. This module is the **sender-side** transform: it
//! linearizes the nested owned [`CaughtChange`] tree (the form emitted by
//! [`Graph::add_change_sink`](crate::graph::Graph)) into a flat list of
//! [`FlatChange`]s, each carrying the root→leaf path to its mutation point plus the
//! op (Add / Remove / Edit).
//!
//! The nesting in [`CaughtChange::Child`] **is** the path; [`flatten`] peels it into
//! [`PathSeg`]s. Each segment carries the parent's **full row** — the sort-key
//! locator *and* PK identity — NOT just the PK, because the View locates every parent
//! by `binary_search` over the level's `Sort`, whose leading columns are the
//! `orderBy`, not the PK (`FLAT-CHANGES-DESIGN.md` §5.3). That full row is already in
//! `CaughtChange::Child.row`, so the linearization is lossless and free.
//!
//! Serde derives are gated behind the `serde` / `testkit` feature (the same
//! `cfg_attr` pattern as [`crate::ast::Ast`]); the cell encoding is the externally
//! tagged, faithful [`OwnedValue`](crate::value::OwnedValue) form (§5.1).

use crate::changes::{CaughtChange, CaughtNode};
use crate::value::{OwnedValue, RelId};

/// A row on the wire: positional cells, aligned to the level's `Schema.columns`.
pub type WireRow = Vec<OwnedValue>;

/// A materialized node on the wire: its row plus slot-keyed, pre-sorted child
/// subtrees — the wire-shaped twin of [`CaughtNode`] (`Vec` rows; relationships as a
/// sorted `Vec<(slot, children)>` rather than a `BTreeMap`).
///
/// Children within a slot are in the **operator's sort order** and MUST NOT be
/// re-sorted by the receiver (`FLAT-CHANGES-DESIGN.md` §7.5).
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug)]
pub struct WireNode {
    pub row: WireRow,
    pub rels: Vec<(RelId, Vec<WireNode>)>,
}

/// One hop of a [`FlatChange`] path: descend relationship `rel`, locating the parent
/// at the current level by `parent_row` (the full sort-key locator).
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug)]
pub struct PathSeg {
    pub rel: RelId,
    pub parent_row: WireRow,
}

/// The op at the end of a flat change's path (the reached level).
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug)]
pub enum FlatOp {
    /// Insert `node` (with its inline, slot-keyed, pre-sorted subtree).
    Add(WireNode),
    /// Remove the row at this level. **Row-only** — the View's remove path consumes
    /// only `node.row` (the drained subtree the nested [`CaughtChange::Remove`] carries
    /// is intentionally dropped).
    Remove { row: WireRow },
    /// Edit: `old` **locates**, `new` **places** (both required, at every depth — the
    /// receiver needs `old` to find the entry even when the sort key moved).
    Edit { old: WireRow, new: WireRow },
}

/// One flat change: the root→leaf parent path, then the op applied at the reached
/// level. An empty `path` means the op applies at the top level.
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug)]
pub struct FlatChange {
    pub path: Vec<PathSeg>,
    pub op: FlatOp,
}

/// Linearize one [`CaughtChange`] into a [`FlatChange`]: peel each
/// [`CaughtChange::Child`] into a [`PathSeg`] until a non-`Child` op is reached.
pub fn flatten(change: &CaughtChange) -> FlatChange {
    let mut path = Vec::new();
    let mut cur = change;
    loop {
        match cur {
            CaughtChange::Child { row, rel, change } => {
                path.push(PathSeg {
                    rel: *rel,
                    parent_row: row.to_value_vec(),
                });
                cur = &**change;
            }
            CaughtChange::Add(node) => {
                return FlatChange {
                    path,
                    op: FlatOp::Add(wire_node(node)),
                };
            }
            CaughtChange::Remove(node) => {
                return FlatChange {
                    path,
                    op: FlatOp::Remove {
                        row: node.row.to_value_vec(),
                    },
                };
            }
            CaughtChange::Edit { old, row } => {
                return FlatChange {
                    path,
                    op: FlatOp::Edit {
                        old: old.to_value_vec(),
                        new: row.to_value_vec(),
                    },
                };
            }
        }
    }
}

/// Linearize a batch of [`CaughtChange`]s (e.g. one transaction's drained sink),
/// preserving order. **The order is significant** — the receiver must apply the
/// resulting flat changes in exactly this order (`FLAT-CHANGES-DESIGN.md` §5.4).
pub fn flatten_all(changes: &[CaughtChange]) -> Vec<FlatChange> {
    changes.iter().map(flatten).collect()
}

/// Convert a [`CaughtNode`] into its wire twin. The `BTreeMap<RelId, _>` iterates in
/// ascending slot order (deterministic); the child `Vec` preserves the operator's
/// sort order (never re-sorted).
fn wire_node(node: &CaughtNode) -> WireNode {
    WireNode {
        row: node.row.to_value_vec(),
        rels: node
            .relationships
            .iter()
            .map(|(slot, children)| (*slot, children.iter().map(wire_node).collect()))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changes::{CaughtChange, CaughtNode};
    use crate::value::{owned_row, OwnedValue as V};

    fn cnode(row: Vec<V>, rels: Vec<(RelId, Vec<CaughtNode>)>) -> CaughtNode {
        CaughtNode {
            row: owned_row(row),
            relationships: rels.into_iter().collect(),
        }
    }

    fn as_int(v: &V) -> i64 {
        match v {
            V::Int(i) => *i,
            other => panic!("expected Int, got {other:?}"),
        }
    }

    #[test]
    fn flatten_peels_child_chain_into_path() {
        // Child(parentA) -> Child(parentB) -> Edit(leaf): the nesting linearizes into
        // a 2-hop path with the leaf Edit as the terminal op.
        let change = CaughtChange::Child {
            row: owned_row(vec![V::Int(1)]),
            rel: RelId(0),
            change: Box::new(CaughtChange::Child {
                row: owned_row(vec![V::Int(2)]),
                rel: RelId(1),
                change: Box::new(CaughtChange::Edit {
                    old: owned_row(vec![V::Int(3), V::Int(10)]),
                    row: owned_row(vec![V::Int(3), V::Int(20)]),
                }),
            }),
        };
        let flat = flatten(&change);
        assert_eq!(flat.path.len(), 2);
        assert_eq!(flat.path[0].rel, RelId(0));
        assert_eq!(as_int(&flat.path[0].parent_row[0]), 1);
        assert_eq!(flat.path[1].rel, RelId(1));
        assert_eq!(as_int(&flat.path[1].parent_row[0]), 2);
        match &flat.op {
            FlatOp::Edit { old, new } => {
                assert_eq!(as_int(&old[1]), 10);
                assert_eq!(as_int(&new[1]), 20);
            }
            other => panic!("expected Edit, got {other:?}"),
        }
    }

    #[test]
    fn flatten_add_carries_slot_keyed_subtree() {
        // Add( parent { rel0: [childX] } ): the inline subtree survives, slot-keyed.
        let node = cnode(
            vec![V::Int(1)],
            vec![(RelId(0), vec![cnode(vec![V::Int(100)], vec![])])],
        );
        let flat = flatten(&CaughtChange::Add(node));
        assert!(flat.path.is_empty());
        match &flat.op {
            FlatOp::Add(wn) => {
                assert_eq!(as_int(&wn.row[0]), 1);
                assert_eq!(wn.rels.len(), 1);
                assert_eq!(wn.rels[0].0, RelId(0));
                assert_eq!(wn.rels[0].1.len(), 1);
                assert_eq!(as_int(&wn.rels[0].1[0].row[0]), 100);
            }
            other => panic!("expected Add, got {other:?}"),
        }
    }

    #[test]
    fn flatten_remove_is_row_only() {
        // The drained subtree on a nested Remove is dropped; only the row crosses.
        let node = cnode(
            vec![V::Int(7)],
            vec![(RelId(0), vec![cnode(vec![V::Int(1)], vec![])])],
        );
        let flat = flatten(&CaughtChange::Remove(node));
        match &flat.op {
            FlatOp::Remove { row } => assert_eq!(as_int(&row[0]), 7),
            other => panic!("expected Remove, got {other:?}"),
        }
    }

    #[cfg(any(feature = "testkit", feature = "serde"))]
    #[test]
    fn flat_change_serde_round_trips() {
        // A Child→Add carrying every cell variant must survive serialize→deserialize
        // byte-identically (the tagged encoding preserves Int/Float/Str/Json/Null/Bool).
        let change = CaughtChange::Child {
            row: owned_row(vec![V::Int(1), V::str("a")]),
            rel: RelId(0),
            change: Box::new(CaughtChange::Add(cnode(
                vec![
                    V::Int(2),
                    V::Float(1.5),
                    V::Null,
                    V::Bool(true),
                    V::str("hi"),
                    V::Json(std::sync::Arc::from("{\"k\":1}")),
                ],
                vec![(RelId(0), vec![cnode(vec![V::Int(3)], vec![])])],
            ))),
        };
        let flat = flatten(&change);
        let json = serde_json::to_value(&flat).expect("serialize");
        let back: FlatChange = serde_json::from_value(json.clone()).expect("deserialize");
        let json2 = serde_json::to_value(&back).expect("re-serialize");
        assert_eq!(json, json2, "flat change must round-trip through serde");
    }
}
