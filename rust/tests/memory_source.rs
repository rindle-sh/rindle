//! Source-level tests for the production [`MemorySource`]: lazy/kept indexes,
//! index picking + constraint seek (incl. the desc-next-part sentinel), reverse,
//! the multiConstraint k-way path, edit-split polarity, write-to-every-index, and
//! the reentrant push (overlay + epoch gate via a `Collector`). Direct ports of
//! the JS `memory-source.test.ts` source-level cases.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use rindle::change::{Basis, FetchRequest, Start};
use rindle::graph::{Graph, NodeId};
use rindle::memory_source::MemorySource;
use rindle::value::{
    owned_row as orow, OwnedRow, OwnedValue, OwnedValue::Int, OwnedValue::Null, Sort,
};
use rindle::{
    CollectedChange, ConnId, ConnectionFilters, Constraint, MultiConstraint, OutEdge, Port,
    RowPredicate, Schema, Source, SourceChange, SourceSchema,
};

// --- schema / data helpers ----------------------------------------------

const ID: usize = 0;
const OWNER: usize = 1;
const PRIORITY: usize = 2;

/// issue(id, owner, priority); PK = id. (The `MemorySource`/pipeline `Schema`.)
fn issues_schema() -> Schema {
    Schema::new(vec!["id", "owner", "priority"], vec![ID], vec![(ID, true)])
}
/// The same table as a SOURCE schema (fed to `Graph::add_source`).
fn issues_source_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "owner", "priority"], vec![ID], vec![(ID, true)])
}
fn issue(id: i64, owner: i64, priority: i64) -> OwnedRow {
    orow(vec![Int(id), Int(owner), Int(priority)])
}

/// id-only schema (id is PK and the whole row).
fn id_schema() -> Schema {
    Schema::new(vec!["id"], vec![ID], vec![(ID, true)])
}
fn r1(id: i64) -> OwnedRow {
    orow(vec![Int(id)])
}

fn id_of(row: &OwnedRow) -> i64 {
    match &row.col(0).to_owned() {
        Int(i) => *i,
        other => panic!("expected Int id, got {other:?}"),
    }
}

fn fetch_ids(ms: &MemorySource, conn: ConnId, req: &FetchRequest) -> Vec<i64> {
    // The source emits rows directly (the node wrapper is the connection's job).
    ms.fetch(conn, req).map(|r| id_of(&r)).collect()
}

fn constraint(col: usize, v: i64) -> Constraint {
    vec![(col, Int(v))]
}

fn index_key_set(ms: &MemorySource) -> BTreeSet<Sort> {
    ms.get_index_keys().into_iter().collect()
}

// =========================================================================
// Indexes: lazy build on fetch, kept across destroy (§3.10)
// =========================================================================

#[test]
fn indexes_built_lazily_and_kept_after_destroy() {
    let ms = MemorySource::new(issues_schema(), vec![]);
    // Only the primary index (pk = id) exists initially.
    assert_eq!(index_key_set(&ms), BTreeSet::from([vec![(ID, true)]]));

    // A fetch with sort (id, owner) builds that index lazily.
    let c1 = ms.connect(Some(vec![(ID, true), (OWNER, true)]), None, vec![]);
    ms.fetch(c1, &FetchRequest::all()).for_each(drop);
    assert_eq!(
        index_key_set(&ms),
        BTreeSet::from([vec![(ID, true)], vec![(ID, true), (OWNER, true)]])
    );

    // A second connection on the SAME sort reuses the index (no new key).
    let c2 = ms.connect(Some(vec![(ID, true), (OWNER, true)]), None, vec![]);
    ms.fetch(c2, &FetchRequest::all()).for_each(drop);
    assert_eq!(
        index_key_set(&ms),
        BTreeSet::from([vec![(ID, true)], vec![(ID, true), (OWNER, true)]])
    );

    // A third sort adds a third index.
    let c3 = ms.connect(Some(vec![(ID, true), (PRIORITY, true)]), None, vec![]);
    ms.fetch(c3, &FetchRequest::all()).for_each(drop);
    assert_eq!(
        index_key_set(&ms),
        BTreeSet::from([
            vec![(ID, true)],
            vec![(ID, true), (OWNER, true)],
            vec![(ID, true), (PRIORITY, true)],
        ])
    );

    // Destroying connections does NOT remove indexes (the deliberate decision).
    ms.destroy(c3);
    ms.destroy(c2);
    ms.destroy(c1);
    assert_eq!(
        index_key_set(&ms),
        BTreeSet::from([
            vec![(ID, true)],
            vec![(ID, true), (OWNER, true)],
            vec![(ID, true), (PRIORITY, true)],
        ])
    );
}

// =========================================================================
// Fetch: forward / reverse / start
// =========================================================================

#[test]
fn forward_and_reverse_full_scan() {
    let ms = MemorySource::new(
        id_schema(),
        vec![r1(3), r1(1), r1(2), r1(4)], // unsorted input
    );
    let conn = ms.connect(Some(vec![(ID, true)]), None, vec![]);
    assert_eq!(fetch_ids(&ms, conn, &FetchRequest::all()), vec![1, 2, 3, 4]);

    let rev = FetchRequest {
        reverse: true,
        ..Default::default()
    };
    assert_eq!(fetch_ids(&ms, conn, &rev), vec![4, 3, 2, 1]);
}

#[test]
fn start_basis_seek_and_gate() {
    let ms = MemorySource::new(id_schema(), vec![r1(1), r1(2), r1(3), r1(4)]);
    let conn = ms.connect(Some(vec![(ID, true)]), None, vec![]);
    let at = FetchRequest {
        start: Some(Start {
            row: r1(2),
            basis: Basis::At,
        }),
        ..Default::default()
    };
    assert_eq!(fetch_ids(&ms, conn, &at), vec![2, 3, 4]);
    let after = FetchRequest {
        start: Some(Start {
            row: r1(2),
            basis: Basis::After,
        }),
        ..Default::default()
    };
    assert_eq!(fetch_ids(&ms, conn, &after), vec![3, 4]);
}

// =========================================================================
// Index picking + constraint seek
// =========================================================================

#[test]
fn constraint_picks_index_sorted_by_constraint_first() {
    let ms = MemorySource::new(
        issues_schema(),
        vec![
            issue(1, 10, 0),
            issue(2, 20, 0),
            issue(3, 10, 0),
            issue(4, 20, 0),
            issue(5, 10, 0),
        ],
    );
    let conn = ms.connect(Some(vec![(ID, true)]), None, vec![]);
    // owner == 10 -> rows 1,3,5 in id order.
    let req = FetchRequest::with_constraint(constraint(OWNER, 10));
    assert_eq!(fetch_ids(&ms, conn, &req), vec![1, 3, 5]);
    // The index sorted by (owner asc, id asc) was built for the seek.
    assert!(ms
        .get_index_keys()
        .contains(&vec![(OWNER, true), (ID, true)]));
}

#[test]
fn constraint_break_stops_at_first_non_match() {
    // generate_with_constraint is a break: an index sorted by the constraint col
    // first means once we pass the owner group, every later row fails.
    let ms = MemorySource::new(
        issues_schema(),
        vec![issue(1, 10, 0), issue(2, 10, 0), issue(3, 20, 0)],
    );
    let conn = ms.connect(Some(vec![(ID, true)]), None, vec![]);
    let req = FetchRequest::with_constraint(constraint(OWNER, 10));
    assert_eq!(fetch_ids(&ms, conn, &req), vec![1, 2]);
}

#[test]
fn single_col_pk_constraint_fast_path() {
    // A fully-constrained single-column PK yields at most one row; the requested
    // sort is NOT appended to the index sort (the index is just (id asc)).
    let ms = MemorySource::new(issues_schema(), vec![issue(1, 10, 0), issue(2, 20, 0)]);
    let conn = ms.connect(Some(vec![(ID, true), (PRIORITY, true)]), None, vec![]);
    let req = FetchRequest::with_constraint(constraint(ID, 2));
    assert_eq!(fetch_ids(&ms, conn, &req), vec![2]);
    // No (id asc, id asc, priority asc) index was created — the primary suffices.
    assert!(!ms.get_index_keys().iter().any(|k| k.len() == 3));
}

#[test]
fn scan_start_with_desc_next_order_part() {
    // Constraint on owner, then `priority desc, id asc`. The seek must land at the
    // first row of the owner group under the index comparator even though the
    // next order part is desc (the min/max sentinel — §4.3).
    let ms = MemorySource::new(
        issues_schema(),
        vec![
            issue(1, 10, 5),
            issue(2, 10, 9),
            issue(3, 10, 1),
            issue(4, 20, 7),
        ],
    );
    let conn = ms.connect(Some(vec![(PRIORITY, false), (ID, true)]), None, vec![]);
    let req = FetchRequest::with_constraint(constraint(OWNER, 10));
    // owner 10 rows by priority desc: 2(p9), 1(p5), 3(p1).
    assert_eq!(fetch_ids(&ms, conn, &req), vec![2, 1, 3]);

    // Reverse: priority asc, id desc -> 3(p1), 1(p5), 2(p9).
    let rev = FetchRequest {
        constraint: Some(constraint(OWNER, 10)),
        reverse: true,
        ..Default::default()
    };
    assert_eq!(fetch_ids(&ms, conn, &rev), vec![3, 1, 2]);
}

// =========================================================================
// PK-constraint-from-filters
// =========================================================================

#[test]
fn pk_constraint_from_filters_beats_req_constraint() {
    // The connection's filters pin the PK (id == 2); the fetch seeks by the PK
    // constraint, while req.constraint (owner == 10) acts as an additional filter.
    let ms = MemorySource::new(
        issues_schema(),
        vec![issue(1, 10, 0), issue(2, 10, 0), issue(3, 20, 0)],
    );
    let predicate: RowPredicate = Rc::new(|row: &OwnedRow| id_of(row) == 2);
    let filters = ConnectionFilters {
        predicate,
        pk_constraint: Some(constraint(ID, 2)),
        fully_applied: true,
        sql_condition: None,
        push_guard: None,
    };
    let conn = ms.connect(Some(vec![(ID, true)]), Some(filters), vec![]);

    // req.constraint owner == 10: id 2 has owner 10 -> kept.
    let req = FetchRequest::with_constraint(constraint(OWNER, 10));
    assert_eq!(fetch_ids(&ms, conn, &req), vec![2]);

    // req.constraint owner == 20: id 2 has owner 10 -> filtered out -> empty.
    let req2 = FetchRequest::with_constraint(constraint(OWNER, 20));
    assert_eq!(fetch_ids(&ms, conn, &req2), Vec::<i64>::new());
}

// =========================================================================
// Filters (predicate over committed rows)
// =========================================================================

#[test]
fn filter_predicate_filters_committed_rows() {
    let ms = MemorySource::new(
        issues_schema(),
        vec![issue(1, 10, 5), issue(2, 10, 1), issue(3, 10, 9)],
    );
    // keep priority >= 5.
    let predicate: RowPredicate = Rc::new(|row: &OwnedRow| match &row.col(PRIORITY).to_owned() {
        Int(p) => *p >= 5,
        _ => false,
    });
    let filters = ConnectionFilters {
        predicate,
        pk_constraint: None,
        fully_applied: true,
        sql_condition: None,
        push_guard: None,
    };
    let conn = ms.connect(Some(vec![(ID, true)]), Some(filters), vec![]);
    assert_eq!(fetch_ids(&ms, conn, &FetchRequest::all()), vec![1, 3]);
}

// =========================================================================
// multiConstraint (#fetchMulti)
// =========================================================================

fn multi_req(mcs: Vec<MultiConstraint>) -> FetchRequest {
    FetchRequest {
        multi_constraints: mcs,
        ..Default::default()
    }
}

#[test]
fn fetch_multi_kway_merges_per_entry() {
    let ms = MemorySource::new(
        issues_schema(),
        vec![
            issue(1, 10, 0),
            issue(2, 20, 0),
            issue(3, 30, 0),
            issue(4, 10, 0),
            issue(5, 20, 0),
        ],
    );
    let conn = ms.connect(Some(vec![(ID, true)]), None, vec![]);
    // owner IN {10, 20} -> ids 1,2,4,5 in merged id order.
    let mc: MultiConstraint = vec![constraint(OWNER, 10), constraint(OWNER, 20)];
    assert_eq!(fetch_ids(&ms, conn, &multi_req(vec![mc])), vec![1, 2, 4, 5]);
}

#[test]
fn fetch_multi_post_filters_against_rest() {
    let ms = MemorySource::new(
        issues_schema(),
        vec![
            issue(1, 10, 5),
            issue(2, 20, 5),
            issue(3, 10, 9),
            issue(4, 20, 1),
        ],
    );
    let conn = ms.connect(Some(vec![(ID, true)]), None, vec![]);
    // (owner IN {10,20}) AND (priority IN {5}) -> ids 1,2.
    let owners: MultiConstraint = vec![constraint(OWNER, 10), constraint(OWNER, 20)];
    let prios: MultiConstraint = vec![constraint(PRIORITY, 5)];
    assert_eq!(
        fetch_ids(&ms, conn, &multi_req(vec![owners, prios])),
        vec![1, 2]
    );
}

#[test]
fn fetch_multi_duplicate_entry_yields_row_twice() {
    // Per the MultiConstraint contract, duplicate entries are a caller invariant
    // (FlippedJoin dedups upstream); the source yields the match per entry.
    let ms = MemorySource::new(issues_schema(), vec![issue(1, 10, 0), issue(2, 20, 0)]);
    let conn = ms.connect(Some(vec![(ID, true)]), None, vec![]);
    let mc: MultiConstraint = vec![constraint(OWNER, 10), constraint(OWNER, 10)];
    assert_eq!(fetch_ids(&ms, conn, &multi_req(vec![mc])), vec![1, 1]);
}

#[test]
fn fetch_multi_reverse_order() {
    let ms = MemorySource::new(
        issues_schema(),
        vec![issue(1, 10, 0), issue(2, 20, 0), issue(3, 10, 0)],
    );
    let conn = ms.connect(Some(vec![(ID, true)]), None, vec![]);
    let mc: MultiConstraint = vec![constraint(OWNER, 10), constraint(OWNER, 20)];
    let req = FetchRequest {
        multi_constraints: vec![mc],
        reverse: true,
        ..Default::default()
    };
    assert_eq!(fetch_ids(&ms, conn, &req), vec![3, 2, 1]);
}

// =========================================================================
// filter_push: predicate-driven edit transform on the push path
// =========================================================================

/// Push an Edit (no split keys) through a connection whose predicate is
/// `owner == 10`, and classify the row-change the source fans out downstream.
fn filtered_edit_kind(old_owner: i64, new_owner: i64) -> Vec<&'static str> {
    let ms = MemorySource::new(issues_schema(), vec![issue(1, old_owner, 0)]);
    let predicate: RowPredicate = Rc::new(|row: &OwnedRow| owner_of(row) == 10);
    let filters = ConnectionFilters {
        predicate,
        pk_constraint: None,
        fully_applied: true,
        sql_condition: None,
        push_guard: None,
    };
    let conn = ms.connect(Some(vec![(ID, true)]), Some(filters), vec![]);
    ms.set_conn_output(
        conn,
        OutEdge {
            node: NodeId::new(0, 0),
            port: Port::Single,
        },
    );
    let rec = RefCell::new(Vec::new());
    ms.push(
        SourceChange::Edit {
            row: issue(1, new_owner, 0),
            old: issue(1, old_owner, 0),
        },
        &|_c, ch: SourceChange| {
            rec.borrow_mut().push(match ch {
                SourceChange::Add(_) => "add",
                SourceChange::Remove(_) => "remove",
                SourceChange::Edit { .. } => "edit",
            });
        },
    );
    rec.into_inner()
}

#[test]
fn filter_push_splits_edit_by_predicate() {
    // old passes (10), new passes (10) -> Edit flows through.
    assert_eq!(filtered_edit_kind(10, 10), vec!["edit"]);
    // old passes (10), new fails (20) -> downstream Remove(old).
    assert_eq!(filtered_edit_kind(10, 20), vec!["remove"]);
    // old fails (20), new passes (10) -> downstream Add(new).
    assert_eq!(filtered_edit_kind(20, 10), vec!["add"]);
    // neither passes -> nothing pushed downstream.
    assert_eq!(filtered_edit_kind(20, 30), Vec::<&'static str>::new());
}

// =========================================================================
// Write to every index (edit consistency across sorts)
// =========================================================================

#[test]
fn edit_writes_to_every_index() {
    let ms = MemorySource::new(issues_schema(), vec![issue(1, 10, 5), issue(2, 20, 1)]);
    // Build a secondary index by fetching on (owner asc, id asc).
    let by_owner = ms.connect(Some(vec![(OWNER, true), (ID, true)]), None, vec![]);
    ms.fetch(by_owner, &FetchRequest::all()).for_each(drop);
    let by_id = ms.connect(Some(vec![(ID, true)]), None, vec![]);
    // Wire an output so the push fans out (no downstream effect needed here).
    ms.set_conn_output(
        by_id,
        OutEdge {
            node: NodeId::new(0, 0),
            port: Port::Single,
        },
    );
    ms.set_conn_output(
        by_owner,
        OutEdge {
            node: NodeId::new(0, 0),
            port: Port::Single,
        },
    );

    // Edit issue 1: owner 10 -> 30. Must update BOTH indexes.
    ms.push(
        SourceChange::Edit {
            row: issue(1, 30, 5),
            old: issue(1, 10, 5),
        },
        &|_c, _ch| {},
    );

    // Fetch via the (owner, id) index: owner 30 now matches issue 1.
    let by_owner_30 = fetch_ids(
        &ms,
        by_owner,
        &FetchRequest::with_constraint(constraint(OWNER, 30)),
    );
    assert_eq!(by_owner_30, vec![1]);
    // Old owner 10 no longer matches.
    let by_owner_10 = fetch_ids(
        &ms,
        by_owner,
        &FetchRequest::with_constraint(constraint(OWNER, 10)),
    );
    assert_eq!(by_owner_10, Vec::<i64>::new());
    // The primary (id) index still has both rows.
    assert_eq!(fetch_ids(&ms, by_id, &FetchRequest::all()), vec![1, 2]);
}

// =========================================================================
// Edit-split polarity (§3.6)
// =========================================================================

/// Edit col 1 of row (1, old_b, 0) to (1, new_b, 0) with the given split keys;
/// return the downstream change kinds the source fans out.
fn edit_kinds(old_b: OwnedValue, new_b: OwnedValue, split_keys: Vec<usize>) -> Vec<&'static str> {
    let ms = MemorySource::new(
        issues_schema(),
        vec![orow(vec![Int(1), old_b.clone(), Int(0)])],
    );
    let conn = ms.connect(Some(vec![(ID, true)]), None, split_keys);
    ms.set_conn_output(
        conn,
        OutEdge {
            node: NodeId::new(0, 0),
            port: Port::Single,
        },
    );
    let rec = RefCell::new(Vec::new());
    // The source fans out `SourceChange` (rows); the node wrapper is added later
    // at the connection boundary. So we classify the row-change kind here.
    ms.push(
        SourceChange::Edit {
            row: orow(vec![Int(1), new_b, Int(0)]),
            old: orow(vec![Int(1), old_b, Int(0)]),
        },
        &|_c, ch: SourceChange| {
            rec.borrow_mut().push(match ch {
                SourceChange::Add(_) => "add",
                SourceChange::Remove(_) => "remove",
                SourceChange::Edit { .. } => "edit",
            });
        },
    );
    rec.into_inner()
}

#[test]
fn edit_split_polarity() {
    // The only NON-splitting case is two equal non-null values.
    assert_eq!(edit_kinds(Int(5), Int(5), vec![OWNER]), vec!["edit"]);
    // Everything else on a split key splits into remove+add.
    assert_eq!(
        edit_kinds(Int(5), Int(6), vec![OWNER]),
        vec!["remove", "add"]
    );
    assert_eq!(edit_kinds(Int(5), Null, vec![OWNER]), vec!["remove", "add"]);
    assert_eq!(edit_kinds(Null, Int(5), vec![OWNER]), vec!["remove", "add"]);
    // null -> null DOES split (the polarity that trips people up).
    assert_eq!(edit_kinds(Null, Null, vec![OWNER]), vec!["remove", "add"]);
    // A split key that did not change does not trigger a split.
    assert_eq!(edit_kinds(Int(5), Int(6), vec![PRIORITY]), vec!["edit"]);
    // No split keys -> always a single edit.
    assert_eq!(edit_kinds(Int(5), Int(6), vec![]), vec!["edit"]);
}

// =========================================================================
// fork — O(1) structural share + independence
// =========================================================================

#[test]
fn fork_shares_then_independent() {
    let ms = MemorySource::new(id_schema(), (0..1000).map(r1).collect());
    let forked = ms.fork();
    let c0 = ms.connect(Some(vec![(ID, true)]), None, vec![]);
    let c1 = forked.connect(Some(vec![(ID, true)]), None, vec![]);

    // Mutate the original; the fork is unaffected (COW).
    ms.set_conn_output(
        c0,
        OutEdge {
            node: NodeId::new(0, 0),
            port: Port::Single,
        },
    );
    ms.push(SourceChange::Add(r1(5000)), &|_c, _ch| {});

    assert!(fetch_ids(&ms, c0, &FetchRequest::all()).contains(&5000));
    assert!(!fetch_ids(&forked, c1, &FetchRequest::all()).contains(&5000));
    assert_eq!(fetch_ids(&forked, c1, &FetchRequest::all()).len(), 1000);
}

// =========================================================================
// Push fan-out + reentrant fetch (overlay + epoch gate) — via the graph
// =========================================================================

/// Build: source -> conn -> Collector. Returns (graph, src, conn, collector).
fn graph_with_collector(initial: Vec<OwnedRow>) -> (Graph, NodeId, NodeId, NodeId) {
    let mut g = Graph::new();
    let src = g.add_source(issues_source_schema(), initial);
    let conn = g.connect(src, Some(vec![(ID, true)]), None, vec![]);
    let coll = g.add_collector(conn);
    g.set_conn_output(
        conn,
        OutEdge {
            node: coll,
            port: Port::Single,
        },
    );
    (g, src, conn, coll)
}

#[test]
fn push_edit_change_fans_out_and_commits() {
    let (g, src, conn, coll) = graph_with_collector(vec![issue(1, 10, 0)]);

    g.source_push(
        src,
        SourceChange::Edit {
            row: issue(1, 20, 5),
            old: issue(1, 10, 0),
        },
    );

    // The connection (no split keys) receives a single Edit change.
    assert_eq!(
        g.collector_changes(coll),
        vec![CollectedChange::Edit {
            row: issue(1, 20, 5),
            old: issue(1, 10, 0),
        }]
    );
    // A fresh fetch reflects the committed new row.
    let rows: Vec<OwnedRow> = g.fetch(conn, &FetchRequest::all()).map(|n| n.row).collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (id_of(&rows[0]), owner_of(&rows[0]), prio_of(&rows[0])),
        (1, 20, 5)
    );
}

#[test]
fn fetch_during_push_edit_sees_overlay() {
    let (g, src, _conn, coll) = graph_with_collector(vec![issue(1, 10, 0)]);
    g.set_collector_fetch_on_push(coll, true);

    g.source_push(
        src,
        SourceChange::Edit {
            row: issue(1, 20, 5),
            old: issue(1, 10, 0),
        },
    );

    // The reentrant fetch taken DURING the push observed the in-flight overlay
    // (the edited row), even though the write commits only after the drain.
    let fetched = g.collector_fetched(coll);
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].len(), 1);
    assert_eq!(
        (
            id_of(&fetched[0][0]),
            owner_of(&fetched[0][0]),
            prio_of(&fetched[0][0])
        ),
        (1, 20, 5)
    );
}

#[test]
fn push_add_and_remove_via_collector() {
    let (g, src, _conn, coll) = graph_with_collector(vec![]);
    g.source_push(src, SourceChange::Add(issue(7, 1, 0)));
    g.source_push(src, SourceChange::Remove(issue(7, 1, 0)));
    assert_eq!(
        g.collector_changes(coll),
        vec![
            CollectedChange::Add(issue(7, 1, 0)),
            CollectedChange::Remove(issue(7, 1, 0)),
        ]
    );
}

fn owner_of(row: &OwnedRow) -> i64 {
    match &row.col(OWNER).to_owned() {
        Int(i) => *i,
        _ => panic!(),
    }
}
fn prio_of(row: &OwnedRow) -> i64 {
    match &row.col(PRIORITY).to_owned() {
        Int(i) => *i,
        _ => panic!(),
    }
}
