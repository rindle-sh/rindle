//! A **reference receiver**: a host-language-agnostic port of the View's
//! `apply_change` (`src/view.rs`) fed by the flat change stream ([`FlatChange`]),
//! reconstructing the exact `ArrayView` tree. It is the executable form of the
//! receiver contract in `FLAT-CHANGES-DESIGN.md` §6, and doubles as the conformance
//! oracle (`tests/flat_oracle.rs` differentially checks it against the real View).
//!
//! Per `FLAT-CHANGES-DESIGN.md` §3 the receiver deliberately **drops** all the
//! sender-only machinery — `Arc` reference stability, copy-on-write, `TxnGen`,
//! `EntryId`, singular/`Format` unwrapping. It keeps only the *logical* operations:
//! `binary_search`-locate over the level's `Sort`, `rc` multi-path counting, and the
//! edit-as-move ghost/adjusted-position/merge protocol. A receiver rebuilding its own
//! copy mutates freely; if it ever wants reference stability for its own UI it can
//! re-introduce COW exactly as the sender does.
//!
//! The comparator is the engine's own [`compare_rows`]/[`compare_values`] — this is a
//! Rust reference impl, so it holds the comparator fixed and tests the
//! *flatten + apply* algorithm. A cross-language receiver instead reimplements
//! `compare_values` byte-for-byte (`FLAT-CHANGES-DESIGN.md` §4).

use std::cmp::Ordering;

use crate::flat::{FlatChange, FlatOp, PathSeg, WireNode, WireRow};
use crate::value::{compare_rows, owned_row, OwnedRow, Schema, Sort};

/// A reconstructed node: row + multi-path refcount + per-slot child lists.
///
/// `rels` is indexed by [`RelId`](crate::value::RelId) slot, in the same order as the
/// level's `Schema::relationships` (one list per declared slot; out-of-view / gating
/// slots stay empty — mirroring the sender's [`Entry`](crate::view::Entry)).
#[derive(Clone, Debug)]
pub struct RecvNode {
    pub row: OwnedRow,
    pub rc: u32,
    pub rels: Vec<Vec<RecvNode>>,
}

/// The reference receiver: holds the hierarchical view [`Schema`] (the shipped query
/// schema) and the reconstructed top-level list (`root[""]`).
#[derive(Debug)]
pub struct Receiver {
    schema: Schema,
    top: Vec<RecvNode>,
}

impl Receiver {
    /// A fresh receiver over the given hierarchical view schema (the same `Schema` the
    /// sender's View was built with — its per-level `sort` is the comparator input and
    /// `rel_child(slot)` is the in-view gate).
    pub fn new(schema: Schema) -> Receiver {
        Receiver {
            schema,
            top: Vec::new(),
        }
    }

    /// Apply one [`FlatChange`]: descend its path, then fold the op at the reached
    /// level. Panics on an inconsistent stream (mirrors the differ's "node does not
    /// exist" — `view.rs:450`).
    pub fn apply(&mut self, change: &FlatChange) {
        apply_at(&mut self.top, &self.schema, &change.path, &change.op);
    }

    /// Apply a batch in order (the per-transaction delta, or the hydrate snapshot).
    /// Order is significant — see `FLAT-CHANGES-DESIGN.md` §5.4.
    pub fn apply_all(&mut self, changes: &[FlatChange]) {
        for c in changes {
            self.apply(c);
        }
    }

    /// The reconstructed top-level result (`root[""]`).
    pub fn top(&self) -> &[RecvNode] {
        &self.top
    }

    /// The hierarchical view schema this receiver reconstructs against.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[inline]
fn binary_search(list: &[RecvNode], row: &OwnedRow, sort: &Sort) -> Result<usize, usize> {
    list.binary_search_by(|n| compare_rows(sort, &n.row, row))
}

/// Descend `path` from `list` (whose entries have schema `schema`), then apply `op` at
/// the reached level. Mirrors the `apply` recursion in `view.rs`, but operates on a
/// plain `&mut Vec<RecvNode>` (no COW/`Arc`).
fn apply_at(list: &mut Vec<RecvNode>, schema: &Schema, path: &[PathSeg], op: &FlatOp) {
    let Some((seg, rest)) = path.split_first() else {
        return apply_op(list, schema, op);
    };
    // In-view gate (`view.rs:486`): a slot with no child schema is join-only / gating
    // (a non-flipped EXISTS forwards a Child on its own out-of-view slot) — drop it.
    let Some(child_schema) = schema.rel_child(seg.rel) else {
        return;
    };
    // Locate the parent by its FULL row (the sort-key locator) — never by PK.
    let pr = owned_row(seg.parent_row.clone());
    let pos = match binary_search(list, &pr, &schema.sort) {
        Ok(p) => p,
        Err(_) => panic!("flat receiver: parent not found at path hop (inconsistent stream)"),
    };
    apply_at(&mut list[pos].rels[seg.rel.ix()], child_schema, rest, op);
}

fn apply_op(list: &mut Vec<RecvNode>, schema: &Schema, op: &FlatOp) {
    match op {
        FlatOp::Add(node) => apply_add(list, schema, node),
        FlatOp::Remove { row } => apply_remove(list, schema, row),
        FlatOp::Edit { old, new } => apply_edit(list, schema, old, new),
    }
}

/// `apply_add` (`view.rs:412`): duplicate sort-key ⇒ `rc += 1`; else build + insert.
fn apply_add(list: &mut Vec<RecvNode>, schema: &Schema, node: &WireNode) {
    let row = owned_row(node.row.clone());
    match binary_search(list, &row, &schema.sort) {
        Ok(p) => list[p].rc += 1,
        Err(ins) => list.insert(ins, build_node(node, schema)),
    }
}

/// Build a fresh node (`rc = 1`) with its inline subtree, mirroring
/// `init_rels_for_new_entry`: one empty list per declared slot; fill in-view slots
/// from the payload, folding same-sort-key children to `rc += 1`; children arrive in
/// the operator's sort order and are NOT re-sorted.
fn build_node(node: &WireNode, schema: &Schema) -> RecvNode {
    let mut rels: Vec<Vec<RecvNode>> = (0..schema.relationships.len())
        .map(|_| Vec::new())
        .collect();
    for (slot, children) in &node.rels {
        let Some(child_schema) = schema.rel_child(*slot) else {
            continue; // join-only / out of view (init_rels `continue`s)
        };
        let mut built: Vec<RecvNode> = Vec::with_capacity(children.len());
        for child in children {
            let crow = owned_row(child.row.clone());
            match binary_search(&built, &crow, &child_schema.sort) {
                Ok(p) => built[p].rc += 1,
                Err(ins) => built.insert(ins, build_node(child, child_schema)),
            }
        }
        rels[slot.ix()] = built;
    }
    RecvNode {
        row: owned_row(node.row.clone()),
        rc: 1,
        rels,
    }
}

/// `apply_remove` (`view.rs:440`): physical removal at `rc == 1`, else decrement.
fn apply_remove(list: &mut Vec<RecvNode>, schema: &Schema, row: &WireRow) {
    let r = owned_row(row.clone());
    let pos = match binary_search(list, &r, &schema.sort) {
        Ok(p) => p,
        Err(_) => panic!("flat receiver: remove of non-existent node"),
    };
    if list[pos].rc == 1 {
        list.remove(pos);
    } else {
        list[pos].rc -= 1;
    }
}

/// `apply_edit_change` (`view.rs:540`): `old` locates, `new` places. Branches on
/// whether the sort key changed (in-place vs the move/ghost/merge protocol).
fn apply_edit(list: &mut Vec<RecvNode>, schema: &Schema, old: &WireRow, new: &WireRow) {
    let sort = &schema.sort;
    let old_r = owned_row(old.clone());
    let new_r = owned_row(new.clone());

    if compare_rows(sort, &old_r, &new_r) == Ordering::Equal {
        // Sort key unchanged → edit in place (keeps position + children + rc).
        let pos = match binary_search(list, &old_r, sort) {
            Ok(p) => p,
            Err(_) => panic!("flat receiver: edit of non-existent node"),
        };
        list[pos].row = new_r;
        return;
    }

    // Sort key changed → the row may move; rc may be > 1.
    let old_pos = match binary_search(list, &old_r, sort) {
        Ok(p) => p,
        Err(_) => panic!("flat receiver: edit old node does not exist"),
    };
    let raw = binary_search(list, &new_r, sort);
    let old_rc = list[old_pos].rc;
    let found = raw.is_ok();
    let pos = raw.unwrap_or_else(|e| e);

    // Fast path (`view.rs:566`): rc==1 and the row lands in the same slot after removal.
    if old_rc == 1 && (pos == old_pos || pos.checked_sub(1) == Some(old_pos)) {
        list[old_pos].row = new_r;
        return;
    }

    // General move (`view.rs:581`).
    let old_entry = list[old_pos].clone(); // capture before mutating
    let new_rc = old_rc - 1;
    let adjusted_pos;
    if new_rc == 0 {
        list.remove(old_pos);
        adjusted_pos = if old_pos < pos { pos - 1 } else { pos };
    } else {
        list[old_pos].rc = new_rc; // ghost survives for the other path(s)
        adjusted_pos = pos;
    }
    if found {
        // Merge into the existing destination entry (keep its children), bump its rc.
        let existing_rc = list[adjusted_pos].rc;
        list[adjusted_pos].row = new_r;
        list[adjusted_pos].rc = existing_rc + 1;
    } else {
        // Move the (edited) old entry — keeping its children — to the new position.
        let mut moved = old_entry;
        moved.row = new_r;
        moved.rc = 1;
        list.insert(adjusted_pos, moved);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flat::{FlatChange, FlatOp, PathSeg, WireNode};
    use crate::value::{OwnedValue as V, RelDef, RelId, Schema};

    // issue(id, val) ordered by (val asc, id asc) — a NON-PK leading sort, so edits to
    // `val` move rows (exercising the move/ghost/merge protocol). One in-view rel
    // "kids" → kid(id, parent) ordered by id, plus one GATING slot "gate" (no child
    // schema) to exercise the in-view gate.
    fn kid_schema() -> Schema {
        Schema::new(vec!["id", "parent"], vec![0], vec![(0, true)])
    }
    fn root_schema() -> Schema {
        Schema::new(vec!["id", "val"], vec![0], vec![(1, true), (0, true)]).with_relationships(
            vec![RelDef::related("kids", kid_schema()), RelDef::new("gate")],
        )
    }

    fn wnode(cells: Vec<V>, rels: Vec<(RelId, Vec<WireNode>)>) -> WireNode {
        WireNode { row: cells, rels }
    }
    fn add(cells: Vec<V>) -> FlatChange {
        FlatChange {
            path: vec![],
            op: FlatOp::Add(wnode(cells, vec![])),
        }
    }
    fn add_node(node: WireNode) -> FlatChange {
        FlatChange {
            path: vec![],
            op: FlatOp::Add(node),
        }
    }
    fn remove(cells: Vec<V>) -> FlatChange {
        FlatChange {
            path: vec![],
            op: FlatOp::Remove { row: cells },
        }
    }
    fn edit(old: Vec<V>, new: Vec<V>) -> FlatChange {
        FlatChange {
            path: vec![],
            op: FlatOp::Edit { old, new },
        }
    }

    fn ids(list: &[RecvNode]) -> Vec<i64> {
        list.iter()
            .map(|n| match n.row.col(0) {
                crate::value::Value::Int(i) => i,
                o => panic!("{o:?}"),
            })
            .collect()
    }
    fn rcs(list: &[RecvNode]) -> Vec<u32> {
        list.iter().map(|n| n.rc).collect()
    }

    #[test]
    fn add_remove_keep_sort_order_and_rc() {
        let mut r = Receiver::new(root_schema());
        // Insert out of order; sorted by val (col 1).
        r.apply(&add(vec![V::Int(1), V::Int(30)]));
        r.apply(&add(vec![V::Int(2), V::Int(10)]));
        r.apply(&add(vec![V::Int(3), V::Int(20)]));
        assert_eq!(ids(r.top()), vec![2, 3, 1]); // by val 10,20,30
        assert_eq!(rcs(r.top()), vec![1, 1, 1]);

        // Duplicate sort-key add (same val+id) → rc++, no new node.
        r.apply(&add(vec![V::Int(3), V::Int(20)]));
        assert_eq!(ids(r.top()), vec![2, 3, 1]);
        assert_eq!(rcs(r.top()), vec![1, 2, 1]);

        // Remove the rc==2 row once → survives at rc 1.
        r.apply(&remove(vec![V::Int(3), V::Int(20)]));
        assert_eq!(rcs(r.top()), vec![1, 1, 1]);
        // Remove again → physically gone.
        r.apply(&remove(vec![V::Int(3), V::Int(20)]));
        assert_eq!(ids(r.top()), vec![2, 1]);
    }

    #[test]
    fn edit_in_place_when_sort_key_unchanged() {
        let mut r = Receiver::new(root_schema());
        r.apply(&add(vec![V::Int(1), V::Int(10)]));
        r.apply(&add(vec![V::Int(2), V::Int(20)]));
        // Edit id=1's PK-irrelevant... here val is the sort key; change a NON-sort col?
        // Both cols are in the sort (val, id), so to keep the sort key we must keep both.
        // Instead edit with identical sort key (no-op move) — exercises the Equal branch.
        r.apply(&edit(
            vec![V::Int(1), V::Int(10)],
            vec![V::Int(1), V::Int(10)],
        ));
        assert_eq!(ids(r.top()), vec![1, 2]);
        assert_eq!(rcs(r.top()), vec![1, 1]);
    }

    #[test]
    fn edit_move_rc1_relocates() {
        let mut r = Receiver::new(root_schema());
        r.apply(&add(vec![V::Int(1), V::Int(10)]));
        r.apply(&add(vec![V::Int(2), V::Int(20)]));
        r.apply(&add(vec![V::Int(3), V::Int(30)]));
        assert_eq!(ids(r.top()), vec![1, 2, 3]);
        // Move id=1 from val 10 → 25 (between 20 and 30).
        r.apply(&edit(
            vec![V::Int(1), V::Int(10)],
            vec![V::Int(1), V::Int(25)],
        ));
        assert_eq!(ids(r.top()), vec![2, 1, 3]); // 20, 25, 30
        assert_eq!(rcs(r.top()), vec![1, 1, 1]);
    }

    #[test]
    fn edit_move_with_ghost_when_rc_gt_1() {
        let mut r = Receiver::new(root_schema());
        r.apply(&add(vec![V::Int(1), V::Int(10)]));
        r.apply(&add(vec![V::Int(1), V::Int(10)])); // rc=2 at (val10,id1)
        r.apply(&add(vec![V::Int(2), V::Int(30)]));
        assert_eq!(ids(r.top()), vec![1, 2]);
        assert_eq!(rcs(r.top()), vec![2, 1]);
        // Edit ONE ref of id=1 to val 40 (past id=2). rc>1 → ghost stays at val10 (rc1),
        // a fresh entry inserted at the new position (rc1).
        r.apply(&edit(
            vec![V::Int(1), V::Int(10)],
            vec![V::Int(1), V::Int(40)],
        ));
        assert_eq!(ids(r.top()), vec![1, 2, 1]); // val 10 (ghost), 30, 40
        assert_eq!(rcs(r.top()), vec![1, 1, 1]);
    }

    #[test]
    fn edit_move_merges_into_existing_destination() {
        let mut r = Receiver::new(root_schema());
        r.apply(&add(vec![V::Int(1), V::Int(10)])); // rc1
        r.apply(&add(vec![V::Int(1), V::Int(10)])); // rc2 at (10,1)
        r.apply(&add(vec![V::Int(1), V::Int(20)])); // a DIFFERENT sort key, same id col0
        assert_eq!(rcs(r.top()), vec![2, 1]); // (10,1)=2, (20,1)=1
                                              // Move one ref of (10,1) to sort key (20,1) — destination already exists → merge.
        r.apply(&edit(
            vec![V::Int(1), V::Int(10)],
            vec![V::Int(1), V::Int(20)],
        ));
        // ghost (10,1) drops to rc1; destination (20,1) bumps to rc2.
        assert_eq!(ids(r.top()), vec![1, 1]);
        assert_eq!(rcs(r.top()), vec![1, 2]);
    }

    #[test]
    fn child_path_descends_and_in_view_gate_drops_gating_slot() {
        let mut r = Receiver::new(root_schema());
        // A parent with one kid inline.
        let parent = wnode(
            vec![V::Int(1), V::Int(10)],
            vec![(RelId(0), vec![wnode(vec![V::Int(100), V::Int(1)], vec![])])],
        );
        r.apply(&add_node(parent));
        assert_eq!(ids(r.top()), vec![1]);
        assert_eq!(ids(&r.top()[0].rels[0]), vec![100]); // kid present under slot 0

        // Child Add of a second kid, addressed via the "kids" slot (RelId 0).
        r.apply(&FlatChange {
            path: vec![PathSeg {
                rel: RelId(0),
                parent_row: vec![V::Int(1), V::Int(10)],
            }],
            op: FlatOp::Add(wnode(vec![V::Int(50), V::Int(1)], vec![])),
        });
        assert_eq!(ids(&r.top()[0].rels[0]), vec![50, 100]); // sorted by kid id

        // Child Add addressed via the GATING slot (RelId 1, child schema None) → no-op.
        r.apply(&FlatChange {
            path: vec![PathSeg {
                rel: RelId(1),
                parent_row: vec![V::Int(1), V::Int(10)],
            }],
            op: FlatOp::Add(wnode(vec![V::Int(999), V::Int(1)], vec![])),
        });
        // The gating slot stays empty; kids unchanged.
        assert!(r.top()[0].rels[1].is_empty());
        assert_eq!(ids(&r.top()[0].rels[0]), vec![50, 100]);
    }
}
