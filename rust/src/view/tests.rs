//! Unit tests for the immutable differ — ported from the JS oracle
//! `packages/zql/src/ivm/view-apply-change.test.ts` (2651 lines). These drive
//! [`apply_change`] directly (hand-built schemas / node trees), exactly as the JS
//! test file drives `applyChange`. Reference identity is asserted with
//! [`Arc::ptr_eq`] (the JS `toBe` / `.not.toBe`).
//!
//! The view is plural at every level (singular `.one()` is a presentation-time
//! concern, not part of the materialized tree), so out-of-view relationships are
//! expressed by a join-only `RelDef::new` (no child schema) rather than a format slot.

#![allow(clippy::bool_assert_comparison)]

use super::*;
use crate::change::{Node, Relationship};
use crate::value::{owned_row, OwnedValue as V, RelDef, Schema};
use std::cell::RefCell;

// --- fixtures -------------------------------------------------------------

fn sstr(s: &str) -> V {
    V::str(s)
}

/// A leaf node (no relationships).
fn leaf(row: OwnedRow) -> Node<'static> {
    Node::leaf(row)
}

/// A node carrying single-use relationship thunks (test data is owned, so the
/// thunk takes the captured `Vec<Node>` out once).
fn node(row: OwnedRow, rels: Vec<(RelId, Vec<Node<'static>>)>) -> Node<'static> {
    Node {
        row,
        rels: rels
            .into_iter()
            .map(|(slot, kids)| {
                let cell = RefCell::new(Some(kids));
                Relationship {
                    slot,
                    thunk: Box::new(move || {
                        Box::new(
                            cell.borrow_mut()
                                .take()
                                .expect("thunk drained twice")
                                .into_iter(),
                        )
                    }),
                }
            })
            .collect(),
    }
}

/// `{id, name}` PK=id, sort by id asc, no relationships.
fn simple_schema() -> Schema {
    Schema::new(vec!["id", "name"], vec![0], vec![(0, true)])
}

fn d(gen: u64) -> TxnDirty {
    TxnDirty { gen: TxnGen(gen) }
}

fn new_root() -> Arc<Entry> {
    Arc::new(synthetic_root())
}

fn top_list(root: &Arc<Entry>) -> EntryList {
    root.rels[REL_ROOT.ix()].clone()
}
fn child_list(e: &Entry, rel: usize) -> EntryList {
    e.rels[rel].clone()
}

fn id_str(e: &Arc<Entry>) -> String {
    match e.row.col(0) {
        crate::value::Value::Str(s) => String::from_utf8_lossy(s).to_string(),
        crate::value::Value::Int(i) => i.to_string(),
        other => panic!("unexpected id {other:?}"),
    }
}

// driving helpers (top-level "" relationship)
fn add(root: &mut Arc<Entry>, sc: &Schema, n: Node<'static>, mode: Mutate, gen: u64) {
    apply_change(
        root,
        &ViewChange::Add { node: n },
        sc,
        REL_ROOT,
        false,
        mode,
        &d(gen),
    );
}
fn add_ids(root: &mut Arc<Entry>, sc: &Schema, n: Node<'static>, gen: u64) {
    apply_change(
        root,
        &ViewChange::Add { node: n },
        sc,
        REL_ROOT,
        true,
        Mutate::Immutable,
        &d(gen),
    );
}
fn remove(root: &mut Arc<Entry>, sc: &Schema, row: OwnedRow, mode: Mutate, gen: u64) {
    apply_change(
        root,
        &ViewChange::Remove { node: leaf(row) },
        sc,
        REL_ROOT,
        false,
        mode,
        &d(gen),
    );
}
fn edit(
    root: &mut Arc<Entry>,
    sc: &Schema,
    new_row: OwnedRow,
    old_row: OwnedRow,
    mode: Mutate,
    gen: u64,
) {
    apply_change(
        root,
        &ViewChange::Edit {
            row: new_row,
            old: old_row,
        },
        sc,
        REL_ROOT,
        false,
        mode,
        &d(gen),
    );
}

// =========================================================================
// Suite B — Simple add/remove with refcounts (view-apply-change.test.ts:588)
// =========================================================================

#[test]
fn simple_plural_refcounts() {
    let sc = simple_schema();
    let mut root = new_root();

    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("a")])),
        Mutate::Immutable,
        0,
    );
    {
        let l = top_list(&root);
        assert_eq!(l.items.len(), 1);
        assert_eq!(l.items[0].rc, 1);
    }
    for _ in 0..5 {
        add(
            &mut root,
            &sc,
            leaf(owned_row(vec![sstr("2"), sstr("b")])),
            Mutate::Immutable,
            0,
        );
    }
    {
        let l = top_list(&root);
        assert_eq!(l.items.len(), 2);
        assert_eq!(l.items[1].rc, 5);
    }
    for _ in 0..4 {
        remove(
            &mut root,
            &sc,
            owned_row(vec![sstr("2"), sstr("b")]),
            Mutate::Immutable,
            0,
        );
    }
    {
        let l = top_list(&root);
        assert_eq!(l.items.len(), 2);
        assert_eq!(l.items[1].rc, 1);
    }
    remove(
        &mut root,
        &sc,
        owned_row(vec![sstr("2"), sstr("b")]),
        Mutate::Immutable,
        0,
    );
    {
        let l = top_list(&root);
        assert_eq!(l.items.len(), 1);
        assert_eq!(id_str(&l.items[0]), "1");
    }
}

#[test]
#[should_panic(expected = "node does not exist")]
fn remove_nonexistent_panics() {
    let sc = simple_schema();
    let mut root = new_root();
    remove(
        &mut root,
        &sc,
        owned_row(vec![sstr("2"), sstr("b")]),
        Mutate::Immutable,
        0,
    );
}

// =========================================================================
// Edit — sort key unchanged (in-place merge) and PK move (ghost/merge)
// =========================================================================

#[test]
fn edit_sort_key_unchanged() {
    let sc = simple_schema();
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("Aaron")])),
        Mutate::Immutable,
        0,
    );
    edit(
        &mut root,
        &sc,
        owned_row(vec![sstr("1"), sstr("Greg")]),
        owned_row(vec![sstr("1"), sstr("Aaron")]),
        Mutate::Immutable,
        0,
    );
    let l = top_list(&root);
    assert_eq!(l.items.len(), 1);
    assert_eq!(l.items[0].rc, 1);
    match l.items[0].row.col(1) {
        crate::value::Value::Str(s) => assert_eq!(s, b"Greg"),
        _ => panic!(),
    }
}

#[test]
fn edit_pk_move_simple() {
    let sc = simple_schema();
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("Aaron")])),
        Mutate::Immutable,
        0,
    );
    edit(
        &mut root,
        &sc,
        owned_row(vec![sstr("2"), sstr("Aaron")]),
        owned_row(vec![sstr("1"), sstr("Aaron")]),
        Mutate::Immutable,
        0,
    );
    let l = top_list(&root);
    assert_eq!(l.items.len(), 1);
    assert_eq!(id_str(&l.items[0]), "2");
    assert_eq!(l.items[0].rc, 1);
}

#[test]
fn edit_pk_move_merge_at_occupied_destination() {
    // The rc-ghost / merge protocol (view-apply-change.test.ts:1199-1231).
    let sc = simple_schema();
    let mut root = new_root();
    for _ in 0..2 {
        add(
            &mut root,
            &sc,
            leaf(owned_row(vec![sstr("1"), sstr("Aaron")])),
            Mutate::Immutable,
            0,
        );
    }
    for _ in 0..2 {
        add(
            &mut root,
            &sc,
            leaf(owned_row(vec![sstr("2"), sstr("Bob")])),
            Mutate::Immutable,
            0,
        );
    }
    edit(
        &mut root,
        &sc,
        owned_row(vec![sstr("2"), sstr("Bob")]),
        owned_row(vec![sstr("1"), sstr("Aaron")]),
        Mutate::Immutable,
        0,
    );
    let l = top_list(&root);
    assert_eq!(l.items.len(), 2);
    assert_eq!(id_str(&l.items[0]), "1");
    assert_eq!(l.items[0].rc, 1, "ghost left at old pos with rc-1");
    assert_eq!(id_str(&l.items[1]), "2");
    assert_eq!(l.items[1].rc, 3, "destination merged: 2 + 1");
}

#[test]
fn edit_pk_move_with_remove() {
    let sc = simple_schema();
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("a")])),
        Mutate::Immutable,
        0,
    );
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("3"), sstr("c")])),
        Mutate::Immutable,
        0,
    );
    edit(
        &mut root,
        &sc,
        owned_row(vec![sstr("5"), sstr("a")]),
        owned_row(vec![sstr("1"), sstr("a")]),
        Mutate::Immutable,
        0,
    );
    let l = top_list(&root);
    assert_eq!(
        l.items.iter().map(id_str).collect::<Vec<_>>(),
        vec!["3", "5"]
    );
    assert!(l.items.iter().all(|e| e.rc == 1));
}

// =========================================================================
// Suite D — add with initialized relationships at correct positions
// =========================================================================

fn parent_child_schema() -> Schema {
    let child = Schema::new(vec!["id"], vec![0], vec![(0, true)]);
    Schema::new(vec!["id"], vec![0], vec![(0, true)])
        .with_relationships(vec![RelDef::related("children", child)])
}

#[test]
fn add_entries_with_children_sorted_by_binary_search() {
    let sc = parent_child_schema();
    let mut root = new_root();

    let mk = |pid: &str, cid: &str| {
        node(
            owned_row(vec![sstr(pid)]),
            vec![(RelId(0), vec![leaf(owned_row(vec![sstr(cid)]))])],
        )
    };
    add(&mut root, &sc, mk("b", "c1"), Mutate::Immutable, 0);
    add(&mut root, &sc, mk("d", "c2"), Mutate::Immutable, 0);
    add(&mut root, &sc, mk("a", "c3"), Mutate::Immutable, 0);

    let l = top_list(&root);
    assert_eq!(
        l.items.iter().map(id_str).collect::<Vec<_>>(),
        vec!["a", "b", "d"]
    );
    for (p, c) in [("a", "c3"), ("b", "c1"), ("d", "c2")] {
        let e = l.items.iter().find(|e| id_str(e) == p).unwrap();
        let kids = child_list(e, 0);
        assert_eq!(kids.items.len(), 1, "parent {p}");
        assert_eq!(id_str(&kids.items[0]), c);
        assert_eq!(kids.items[0].rc, 1);
    }
}

#[test]
fn add_entry_with_two_children() {
    let sc = parent_child_schema();
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        node(owned_row(vec![sstr("a")]), vec![(RelId(0), vec![])]),
        Mutate::Immutable,
        0,
    );
    add(
        &mut root,
        &sc,
        node(owned_row(vec![sstr("c")]), vec![(RelId(0), vec![])]),
        Mutate::Immutable,
        0,
    );
    add(
        &mut root,
        &sc,
        node(
            owned_row(vec![sstr("b")]),
            vec![(
                RelId(0),
                vec![
                    leaf(owned_row(vec![sstr("child2")])),
                    leaf(owned_row(vec![sstr("child1")])),
                ],
            )],
        ),
        Mutate::Immutable,
        0,
    );
    let l = top_list(&root);
    assert_eq!(
        l.items.iter().map(id_str).collect::<Vec<_>>(),
        vec!["a", "b", "c"]
    );
    let bkids = child_list(&l.items[1], 0);
    assert_eq!(
        bkids.items.iter().map(id_str).collect::<Vec<_>>(),
        vec!["child1", "child2"]
    );
}

// =========================================================================
// §9.2 Reference stability (immutable) — Arc::ptr_eq
// =========================================================================

fn ptr(e: &Arc<Entry>) -> *const Entry {
    Arc::as_ptr(e)
}
fn lptr(l: &EntryList) -> *const EntryListInner {
    Arc::as_ptr(l)
}

#[test]
fn unchanged_siblings_keep_reference_on_add() {
    let sc = simple_schema();
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("a")])),
        Mutate::Immutable,
        0,
    );
    let first = top_list(&root).items[0].clone();
    let first_ptr = ptr(&first);
    let root_ptr = ptr(&root);
    let list_ptr = lptr(&top_list(&root));

    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("2"), sstr("b")])),
        Mutate::Immutable,
        0,
    );
    let l = top_list(&root);
    assert_eq!(l.items.len(), 2);
    assert_eq!(
        ptr(&l.items[0]),
        first_ptr,
        "unchanged sibling keeps its Arc"
    );
    assert_ne!(ptr(&root), root_ptr);
    assert_ne!(lptr(&l), list_ptr);
}

#[test]
fn unchanged_siblings_keep_reference_on_remove() {
    let sc = simple_schema();
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("a")])),
        Mutate::Immutable,
        0,
    );
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("2"), sstr("b")])),
        Mutate::Immutable,
        0,
    );
    let first_ptr = ptr(&top_list(&root).items[0]);
    remove(
        &mut root,
        &sc,
        owned_row(vec![sstr("2"), sstr("b")]),
        Mutate::Immutable,
        0,
    );
    let l = top_list(&root);
    assert_eq!(l.items.len(), 1);
    assert_eq!(ptr(&l.items[0]), first_ptr);
}

#[test]
fn edited_row_gets_new_reference_others_stable() {
    let sc = simple_schema();
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("Alice")])),
        Mutate::Immutable,
        0,
    );
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("2"), sstr("Bob")])),
        Mutate::Immutable,
        0,
    );
    let p0 = ptr(&top_list(&root).items[0]);
    let p1 = ptr(&top_list(&root).items[1]);
    edit(
        &mut root,
        &sc,
        owned_row(vec![sstr("2"), sstr("Bobby")]),
        owned_row(vec![sstr("2"), sstr("Bob")]),
        Mutate::Immutable,
        0,
    );
    let l = top_list(&root);
    assert_eq!(ptr(&l.items[0]), p0, "unedited row keeps ref");
    assert_ne!(ptr(&l.items[1]), p1, "edited row gets a new ref");
}

#[test]
fn child_change_isolates_other_parents() {
    let sc = parent_child_schema();
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        node(
            owned_row(vec![sstr("p1")]),
            vec![(RelId(0), vec![leaf(owned_row(vec![sstr("c1")]))])],
        ),
        Mutate::Immutable,
        0,
    );
    add(
        &mut root,
        &sc,
        node(
            owned_row(vec![sstr("p2")]),
            vec![(RelId(0), vec![leaf(owned_row(vec![sstr("c2")]))])],
        ),
        Mutate::Immutable,
        0,
    );
    let p1_ptr = ptr(&top_list(&root).items[0]);
    let p2 = top_list(&root).items[1].clone();
    let p2_ptr = ptr(&p2);
    let p2_children_ptr = lptr(&child_list(&p2, 0));

    apply_change(
        &mut root,
        &ViewChange::Child {
            row: owned_row(vec![sstr("p1")]),
            rel: RelId(0),
            change: Box::new(ViewChange::Add {
                node: leaf(owned_row(vec![sstr("c3")])),
            }),
        },
        &sc,
        REL_ROOT,
        false,
        Mutate::Immutable,
        &d(0),
    );
    let l = top_list(&root);
    assert_ne!(ptr(&l.items[0]), p1_ptr, "p1 changed");
    assert_eq!(ptr(&l.items[1]), p2_ptr, "p2 untouched");
    assert_eq!(
        lptr(&child_list(&l.items[1], 0)),
        p2_children_ptr,
        "p2.children untouched"
    );
    assert_eq!(child_list(&l.items[0], 0).items.len(), 2, "c3 added to p1");
}

#[test]
fn child_no_op_on_join_only_relationship_does_not_panic() {
    // A Child change to an out-of-view slot declared JOIN-ONLY (no child schema —
    // `RelDef::new`) is a no-op, NOT a panic, and preserves parent + root identity.
    // This is the production path for a gating EXISTS relationship: the `Exists` gate
    // forwards a Child on its own slot when membership does not flip (`exists.ts`
    // `#pushWithFilter`), and the view schema declares that slot join-only / out of
    // view. `apply_child` checks `rel_child` first (`view-apply-change.ts:360`'s
    // format-undefined check, now schema-carried) and returns the parent unchanged.
    let sc = Schema::new(vec!["id"], vec![0], vec![(0, true)])
        .with_relationships(vec![RelDef::new("gating")]); // join-only: no child schema
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        node(owned_row(vec![sstr("p1")]), vec![]),
        Mutate::Immutable,
        0,
    );
    let p1_ptr = ptr(&top_list(&root).items[0]);
    let root_ptr = ptr(&root);
    apply_change(
        &mut root,
        &ViewChange::Child {
            row: owned_row(vec![sstr("p1")]),
            rel: RelId(0),
            change: Box::new(ViewChange::Add {
                node: leaf(owned_row(vec![sstr("c1")])),
            }),
        },
        &sc,
        REL_ROOT,
        false,
        Mutate::Immutable,
        &d(0),
    );
    assert_eq!(ptr(&top_list(&root).items[0]), p1_ptr, "parent unchanged");
    assert_eq!(ptr(&root), root_ptr, "root unchanged (no path copy)");
}

#[test]
fn rc_increment_immutable_new_ref_same_data() {
    let sc = simple_schema();
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("a")])),
        Mutate::Immutable,
        0,
    );
    let orig = top_list(&root).items[0].clone();
    let orig_ptr = ptr(&orig);
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("a")])),
        Mutate::Immutable,
        0,
    );
    let l = top_list(&root);
    assert_ne!(
        ptr(&l.items[0]),
        orig_ptr,
        "rc bump creates new ref in immutable mode"
    );
    assert_eq!(l.items[0].rc, orig.rc + 1);
    assert_eq!(id_str(&l.items[0]), "1");
}

// =========================================================================
// §9.3 In-place mutation (Mutate::InPlace) — refs preserved
// =========================================================================

#[test]
fn in_place_add_keeps_root_and_list_refs() {
    let sc = simple_schema();
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("a")])),
        Mutate::InPlace,
        0,
    );
    let root_ptr = ptr(&root);
    let list_ptr = lptr(&top_list(&root));
    let first_ptr = ptr(&top_list(&root).items[0]);

    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("2"), sstr("b")])),
        Mutate::InPlace,
        0,
    );
    assert_eq!(ptr(&root), root_ptr, "root mutated in place");
    assert_eq!(lptr(&top_list(&root)), list_ptr, "list mutated in place");
    assert_eq!(
        ptr(&top_list(&root).items[0]),
        first_ptr,
        "existing entry kept"
    );
    assert_eq!(top_list(&root).items.len(), 2);
}

#[test]
fn in_place_edit_keeps_entry_ref() {
    let sc = simple_schema();
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("Alice")])),
        Mutate::InPlace,
        0,
    );
    let root_ptr = ptr(&root);
    let row_ptr = ptr(&top_list(&root).items[0]);
    edit(
        &mut root,
        &sc,
        owned_row(vec![sstr("1"), sstr("Alicia")]),
        owned_row(vec![sstr("1"), sstr("Alice")]),
        Mutate::InPlace,
        0,
    );
    assert_eq!(ptr(&root), root_ptr);
    assert_eq!(
        ptr(&top_list(&root).items[0]),
        row_ptr,
        "edited row mutated in place"
    );
    match top_list(&root).items[0].row.col(1) {
        crate::value::Value::Str(s) => assert_eq!(s, b"Alicia"),
        _ => panic!(),
    }
}

#[test]
fn in_place_rc_increment_mutates_same_entry() {
    let sc = simple_schema();
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("a")])),
        Mutate::InPlace,
        0,
    );
    let p = ptr(&top_list(&root).items[0]);
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("a")])),
        Mutate::InPlace,
        0,
    );
    assert_eq!(
        ptr(&top_list(&root).items[0]),
        p,
        "rc bump mutates same entry"
    );
    assert_eq!(top_list(&root).items[0].rc, 2);
}

#[test]
fn in_place_child_change_keeps_parent_refs() {
    let sc = parent_child_schema();
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        node(
            owned_row(vec![sstr("p1")]),
            vec![(RelId(0), vec![leaf(owned_row(vec![sstr("c1")]))])],
        ),
        Mutate::InPlace,
        0,
    );
    add(
        &mut root,
        &sc,
        node(
            owned_row(vec![sstr("p2")]),
            vec![(RelId(0), vec![leaf(owned_row(vec![sstr("c2")]))])],
        ),
        Mutate::InPlace,
        0,
    );
    let root_ptr = ptr(&root);
    let list_ptr = lptr(&top_list(&root));
    let p1_ptr = ptr(&top_list(&root).items[0]);
    let p2_ptr = ptr(&top_list(&root).items[1]);
    let p1_children_ptr = lptr(&child_list(&top_list(&root).items[0], 0));

    apply_change(
        &mut root,
        &ViewChange::Child {
            row: owned_row(vec![sstr("p1")]),
            rel: RelId(0),
            change: Box::new(ViewChange::Add {
                node: leaf(owned_row(vec![sstr("c3")])),
            }),
        },
        &sc,
        REL_ROOT,
        false,
        Mutate::InPlace,
        &d(0),
    );
    assert_eq!(ptr(&root), root_ptr, "root in place");
    assert_eq!(lptr(&top_list(&root)), list_ptr, "list in place");
    assert_eq!(
        ptr(&top_list(&root).items[0]),
        p1_ptr,
        "p1 mutated in place (keeps ref)"
    );
    assert_eq!(ptr(&top_list(&root).items[1]), p2_ptr, "p2 untouched");
    assert_eq!(
        lptr(&child_list(&top_list(&root).items[0], 0)),
        p1_children_ptr,
        "p1.children in place"
    );
    assert_eq!(child_list(&top_list(&root).items[0], 0).items.len(), 2);
}

// =========================================================================
// §9.3 COW / transaction (Mutate::Cow) — gen-stamp behavior
// =========================================================================

#[test]
fn cow_first_touch_copies_then_in_place_then_flush_copies() {
    let sc = simple_schema();
    let mut root = new_root();
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("a")])),
        Mutate::InPlace,
        0,
    );
    add(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("2"), sstr("b")])),
        Mutate::InPlace,
        0,
    );

    // "commit" the snapshot: bump gen to 1. Capture committed list + entry.
    let committed_list = top_list(&root);
    let committed_e2 = committed_list.items[1].clone();
    let committed_list_ptr = lptr(&committed_list);
    let committed_e2_ptr = ptr(&committed_e2);

    // First push in txn gen 1: edits "2" → COWs the committed entry.
    edit(
        &mut root,
        &sc,
        owned_row(vec![sstr("2"), sstr("bb")]),
        owned_row(vec![sstr("2"), sstr("b")]),
        Mutate::Cow,
        1,
    );
    // Scope these clones so they DON'T linger and inflate the owned list's Arc
    // count (a lingering clone would force the 2nd in-txn touch to make_mut-clone).
    // The real `View` takes the root out (count 1) and a listener only sees data at
    // flush, so an owned object is never aliased mid-txn.
    {
        let after1 = top_list(&root);
        assert_ne!(
            lptr(&after1),
            committed_list_ptr,
            "first touch copied the list"
        );
        assert_ne!(
            ptr(&after1.items[1]),
            committed_e2_ptr,
            "first touch copied the entry"
        );
    }
    assert_eq!(lptr(&committed_list), committed_list_ptr);
    match committed_e2.row.col(1) {
        crate::value::Value::Str(s) => assert_eq!(s, b"b", "committed snapshot row not mutated"),
        _ => panic!(),
    }
    drop(committed_list);
    drop(committed_e2);

    // Second push in the SAME txn (gen 1): owned → in place (same refs).
    let owned_e2_ptr = ptr(&top_list(&root).items[1]);
    let owned_list_ptr = lptr(&top_list(&root));
    let root_ptr = ptr(&root);
    edit(
        &mut root,
        &sc,
        owned_row(vec![sstr("2"), sstr("bbb")]),
        owned_row(vec![sstr("2"), sstr("bb")]),
        Mutate::Cow,
        1,
    );
    assert_eq!(ptr(&root), root_ptr, "root in place (2nd touch this txn)");
    assert_eq!(lptr(&top_list(&root)), owned_list_ptr, "list in place");
    assert_eq!(
        ptr(&top_list(&root).items[1]),
        owned_e2_ptr,
        "entry mutated in place"
    );

    // After flush (gen 2), next push copies-on-write again.
    let pre_ptr = ptr(&top_list(&root).items[1]);
    edit(
        &mut root,
        &sc,
        owned_row(vec![sstr("2"), sstr("z")]),
        owned_row(vec![sstr("2"), sstr("bbb")]),
        Mutate::Cow,
        2,
    );
    assert_ne!(
        ptr(&top_list(&root).items[1]),
        pre_ptr,
        "new txn copies again"
    );
}

#[test]
fn cow_noop_child_preserves_committed_sibling_list_identity() {
    // Regression (review finding #1): under Cow, a parent made owned-this-txn by a
    // change to ONE relationship must not lose the `Arc` identity of a DIFFERENT,
    // still-committed relationship list when a *deep no-op* CHILD touches it.
    //
    // `relA` + `relB` are in view (plural). `b.dead` is OUT of view: declared
    // join-only via `RelDef::new` (no child schema), so a Child reaching it is a
    // no-op (`apply_child` returns early on `rel_child` == `None`).
    let b_schema = Schema::new(vec!["id"], vec![0], vec![(0, true)])
        .with_relationships(vec![RelDef::new("dead")]);
    let a_schema = Schema::new(vec!["id"], vec![0], vec![(0, true)]);
    let m_schema = Schema::new(vec!["id"], vec![0], vec![(0, true)]).with_relationships(vec![
        RelDef::related("relA", a_schema),
        RelDef::related("relB", b_schema),
    ]);

    let mut root = new_root();
    add(
        &mut root,
        &m_schema,
        node(
            owned_row(vec![sstr("m1")]),
            vec![
                (RelId(0), vec![]),
                (RelId(1), vec![leaf(owned_row(vec![sstr("b1")]))]),
            ],
        ),
        Mutate::InPlace,
        0,
    );
    let committed_relb = child_list(&top_list(&root).items[0], 1);
    let committed_ptr = lptr(&committed_relb);
    drop(committed_relb);

    // Txn gen 1, change 1: add a1 to relA → m1 owned, relB stays committed.
    apply_change(
        &mut root,
        &ViewChange::Child {
            row: owned_row(vec![sstr("m1")]),
            rel: RelId(0),
            change: Box::new(ViewChange::Add {
                node: leaf(owned_row(vec![sstr("a1")])),
            }),
        },
        &m_schema,
        REL_ROOT,
        false,
        Mutate::Cow,
        &d(1),
    );
    assert_eq!(
        lptr(&child_list(&top_list(&root).items[0], 1)),
        committed_ptr
    );

    // Change 2 (same txn): deep NO-OP — Child relB → b1 → "dead" (not in view).
    apply_change(
        &mut root,
        &ViewChange::Child {
            row: owned_row(vec![sstr("m1")]),
            rel: RelId(1),
            change: Box::new(ViewChange::Child {
                row: owned_row(vec![sstr("b1")]),
                rel: RelId(0),
                change: Box::new(ViewChange::Add {
                    node: leaf(owned_row(vec![sstr("x")])),
                }),
            }),
        },
        &m_schema,
        REL_ROOT,
        false,
        Mutate::Cow,
        &d(1),
    );
    assert_eq!(
        lptr(&child_list(&top_list(&root).items[0], 1)),
        committed_ptr,
        "deep no-op must preserve the committed relB list's Arc identity",
    );
}

#[test]
fn different_views_never_share_entries() {
    let sc = simple_schema();
    let mut a = new_root();
    let mut b = new_root();
    let row = owned_row(vec![sstr("1"), sstr("x")]);
    add(&mut a, &sc, leaf(row.clone()), Mutate::InPlace, 0);
    add(&mut b, &sc, leaf(row), Mutate::InPlace, 0);
    let ea = top_list(&a).items[0].clone();
    let eb = top_list(&b).items[0].clone();
    assert_ne!(ptr(&ea), ptr(&eb), "distinct entry instances per view");
    assert_ne!(ptr(&a), ptr(&b));
}

// =========================================================================
// withIDs (the SolidView/test path) — id derivation + recompute
// =========================================================================

#[test]
fn with_ids_edit_keeps_id_when_pk_unchanged() {
    let sc = simple_schema();
    let mut root = new_root();
    add_ids(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("Alice")])),
        0,
    );
    let orig = top_list(&root).items[0].clone();
    let orig_id = orig.id.clone();
    assert_eq!(orig_id, Some(Box::new(EntryId::Scalar("\"1\"".into()))));
    apply_change(
        &mut root,
        &ViewChange::Edit {
            row: owned_row(vec![sstr("1"), sstr("Alicia")]),
            old: owned_row(vec![sstr("1"), sstr("Alice")]),
        },
        &sc,
        REL_ROOT,
        true,
        Mutate::Immutable,
        &d(0),
    );
    let l = top_list(&root);
    assert_ne!(ptr(&l.items[0]), ptr(&orig));
    assert_eq!(l.items[0].id, orig_id, "id unchanged when PK unchanged");
}

#[test]
fn with_ids_edit_pk_changes_id() {
    let sc = simple_schema();
    let mut root = new_root();
    add_ids(
        &mut root,
        &sc,
        leaf(owned_row(vec![sstr("1"), sstr("Aaron")])),
        0,
    );
    apply_change(
        &mut root,
        &ViewChange::Edit {
            row: owned_row(vec![sstr("2"), sstr("Aaron")]),
            old: owned_row(vec![sstr("1"), sstr("Aaron")]),
        },
        &sc,
        REL_ROOT,
        true,
        Mutate::Immutable,
        &d(0),
    );
    let l = top_list(&root);
    assert_eq!(
        l.items[0].id,
        Some(Box::new(EntryId::Scalar("\"2\"".into()))),
        "id tracks new PK"
    );
}

// =========================================================================
// from_change / ViewChange conversion
// =========================================================================

#[test]
fn view_change_from_dataflow_change() {
    use crate::change::Change;
    let c = Change::Add(leaf(owned_row(vec![sstr("1")])));
    assert!(matches!(ViewChange::from_change(c), ViewChange::Add { .. }));
    let c2: Change = Change::Child {
        node: leaf(owned_row(vec![sstr("p")])),
        rel: RelId(0),
        child: Box::new(Change::Add(leaf(owned_row(vec![sstr("c")])))),
    };
    match ViewChange::from_change(c2) {
        ViewChange::Child { rel, change, .. } => {
            assert_eq!(rel, RelId(0));
            assert!(matches!(*change, ViewChange::Add { .. }));
        }
        _ => panic!("expected Child"),
    }
}
