//! JS-boundary safe-integer enforcement (productionization 09.8; design 226 Stage A).
//!
//! An [`OwnedValue::Int`] crosses every JS boundary as an f64 `number` today (the
//! single-`number` model, R10). Above `Number.MAX_SAFE_INTEGER` (2^53 − 1) that
//! conversion collides adjacent values — fatal for PK identity (two distinct ids
//! become equal in JS). These walkers find the first offending `Int` in each shape
//! that crosses, so a boundary with `strict_i64` enabled refuses the frame with a
//! typed error instead of silently rounding. **Mandatory since design 226 Stage C
//! (§8)** — `int64` columns exist, and every JS boundary (wasm, napi, the daemon's
//! bare-cell HTTP read wire) runs the walk unconditionally; Stage E adds the bigint
//! lane and makes it column-aware.
//!
//! Note the bound is deliberately **stricter than the round-trip guards** (the CDC
//! capture guard and the scan-path `check_number_bounds`): 2^54 round-trips
//! i64 → f64 → i64 exactly, yet is NOT a safe JS integer — its f64 *neighbors* are
//! more than 1 apart, so distinct nearby i64s would collide. `Float` cells are never
//! checked: they are f64 already, whatever their magnitude.

use crate::changes::CaughtNode;
use crate::flat::{FlatChange, FlatOp, WireNode};
use crate::value::{OwnedRow, OwnedValue, Value};
use crate::view::ViewData;

/// `Number.MAX_SAFE_INTEGER` (2^53 − 1): the largest magnitude at which every i64 is
/// exactly — and *distinctly* — representable as a JS `number`.
pub const JS_MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Whether `i` crosses to a JS `number` without safe-integer loss.
pub fn i64_in_js_safe_range(i: i64) -> bool {
    i.unsigned_abs() <= JS_MAX_SAFE_INTEGER
}

/// The first out-of-safe-range `Int` in a positional cell slice, if any.
pub fn unsafe_int_in_cells(row: &[OwnedValue]) -> Option<i64> {
    row.iter().find_map(|v| match v {
        OwnedValue::Int(i) if !i64_in_js_safe_range(*i) => Some(*i),
        _ => None,
    })
}

/// The first out-of-safe-range `Int` in a packed row, if any.
pub fn unsafe_int_in_row(row: &OwnedRow) -> Option<i64> {
    row.cells().find_map(|v| match v {
        Value::Int(i) if !i64_in_js_safe_range(i) => Some(i),
        _ => None,
    })
}

fn unsafe_int_in_wire_node(node: &WireNode) -> Option<i64> {
    unsafe_int_in_cells(&node.row).or_else(|| {
        node.rels
            .iter()
            .flat_map(|(_, children)| children.iter())
            .find_map(unsafe_int_in_wire_node)
    })
}

/// The first out-of-safe-range `Int` anywhere in a flat-change batch — path parent
/// rows, op rows, and every nested [`WireNode`].
pub fn unsafe_int_in_flat_changes(changes: &[FlatChange]) -> Option<i64> {
    changes.iter().find_map(|c| {
        c.path
            .iter()
            .find_map(|seg| unsafe_int_in_cells(&seg.parent_row))
            .or_else(|| match &c.op {
                FlatOp::Add(node) => unsafe_int_in_wire_node(node),
                FlatOp::Remove { row } => unsafe_int_in_cells(row),
                FlatOp::Edit { old, new } => {
                    unsafe_int_in_cells(old).or_else(|| unsafe_int_in_cells(new))
                }
            })
    })
}

/// The first out-of-safe-range `Int` in a materialized view tree.
pub fn unsafe_int_in_view_data(data: &ViewData) -> Option<i64> {
    data.items.iter().find_map(|e| {
        unsafe_int_in_row(&e.row).or_else(|| e.rels.iter().find_map(unsafe_int_in_view_data))
    })
}

/// The first out-of-safe-range `Int` in a one-shot hydrate tree.
pub fn unsafe_int_in_caught_node(node: &CaughtNode) -> Option<i64> {
    unsafe_int_in_row(&node.row).or_else(|| {
        node.relationships
            .values()
            .flat_map(|v| v.iter())
            .find_map(unsafe_int_in_caught_node)
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::flat::PathSeg;
    use crate::value::{owned_row, RelId};
    use crate::view::{Entry, EntryListInner, TxnGen};

    const UNSAFE: i64 = (1_i64 << 53) + 1;

    #[test]
    fn the_bound_is_max_safe_integer_not_round_trip() {
        // 09.8's acceptance value: 2^53 itself is out of the SAFE range even though it
        // round-trips f64 exactly. 2^54 likewise. The guard is about *distinctness*.
        assert!(i64_in_js_safe_range(9_007_199_254_740_991));
        assert!(i64_in_js_safe_range(-9_007_199_254_740_991));
        assert!(!i64_in_js_safe_range(1 << 53));
        assert!(!i64_in_js_safe_range(-(1 << 53)));
        assert!(!i64_in_js_safe_range(1 << 54));
        assert!(!i64_in_js_safe_range(i64::MAX));
        assert!(!i64_in_js_safe_range(i64::MIN));
        assert!(i64_in_js_safe_range(0));
    }

    #[test]
    fn floats_are_never_flagged_whatever_their_magnitude() {
        let cells = [
            OwnedValue::Float((1u64 << 60) as f64),
            OwnedValue::Float(f64::MAX),
            OwnedValue::Int(5),
        ];
        assert_eq!(unsafe_int_in_cells(&cells), None);
    }

    #[test]
    fn flat_changes_are_walked_to_every_row_position() {
        let node = |cell: i64| WireNode {
            row: vec![OwnedValue::Int(cell)],
            rels: Vec::new(),
        };
        // Offender nested two levels down an Add's child tree.
        let deep = FlatChange {
            path: vec![PathSeg {
                rel: RelId(0),
                parent_row: vec![OwnedValue::Int(1)],
            }],
            op: FlatOp::Add(WireNode {
                row: vec![OwnedValue::Int(2)],
                rels: vec![(RelId(0), vec![node(UNSAFE)])],
            }),
        };
        assert_eq!(unsafe_int_in_flat_changes(&[deep]), Some(UNSAFE));
        // Offender in an Edit's `old` side.
        let edit = FlatChange {
            path: Vec::new(),
            op: FlatOp::Edit {
                old: vec![OwnedValue::Int(UNSAFE)],
                new: vec![OwnedValue::Int(3)],
            },
        };
        assert_eq!(unsafe_int_in_flat_changes(&[edit]), Some(UNSAFE));
        // Offender in a path parent row.
        let in_path = FlatChange {
            path: vec![PathSeg {
                rel: RelId(0),
                parent_row: vec![OwnedValue::Int(UNSAFE)],
            }],
            op: FlatOp::Remove {
                row: vec![OwnedValue::Int(4)],
            },
        };
        assert_eq!(unsafe_int_in_flat_changes(&[in_path]), Some(UNSAFE));
        // A clean batch passes.
        let clean = FlatChange {
            path: Vec::new(),
            op: FlatOp::Remove {
                row: vec![OwnedValue::Int(5), OwnedValue::Float(1e300)],
            },
        };
        assert_eq!(unsafe_int_in_flat_changes(&[clean]), None);
    }

    #[test]
    fn view_trees_and_caught_nodes_are_walked_recursively() {
        let leaf = Arc::new(Entry {
            row: owned_row(vec![OwnedValue::Int(UNSAFE)]),
            rc: 1,
            created: TxnGen(0),
            id: None,
            rels: Box::new([]),
        });
        let root = Arc::new(Entry {
            row: owned_row(vec![OwnedValue::Int(1)]),
            rc: 1,
            created: TxnGen(0),
            id: None,
            rels: Box::new([Arc::new(EntryListInner {
                created: TxnGen(0),
                items: vec![leaf],
            })]),
        });
        let data: ViewData = Arc::new(EntryListInner {
            created: TxnGen(0),
            items: vec![root],
        });
        assert_eq!(unsafe_int_in_view_data(&data), Some(UNSAFE));

        let child = CaughtNode {
            row: owned_row(vec![OwnedValue::Int(UNSAFE)]),
            relationships: BTreeMap::new(),
        };
        let parent = CaughtNode {
            row: owned_row(vec![OwnedValue::Int(1)]),
            relationships: BTreeMap::from([(RelId(0), vec![child])]),
        };
        assert_eq!(unsafe_int_in_caught_node(&parent), Some(UNSAFE));
    }
}
