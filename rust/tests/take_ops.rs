#![cfg(feature = "testkit")]

//! Operator-level tests for `Take` (`op/take.rs` / `take.ts`, spec `07` §3.6): the
//! bounded top-N fetch and the full push matrix (Add / Remove / Child + the
//! `#pushEditChange` bound-crossing cases). The unpartitioned cases drive the
//! operator through the **real source push** (`run_push_test`, overlay-correct);
//! the partitioned cases run end-to-end through `build_pipeline` (a limited
//! relationship), where the parent join supplies the per-partition fetch
//! constraint. Expected outputs are anchored to the JS `take.push.test.ts` oracle.

use rindle::change::{Change, FetchRequest, Node, OutEdge, Port, SourceChange};
use rindle::graph::{Graph, NodeId};
use rindle::op::Take;
use rindle::testkit::{
    add, child, cleaf, edit, rel, remove, run_fetch_test, run_fetch_test_ast, run_push_test,
    run_push_test_ast, CaughtChange, SourceSpec, TableSpec,
};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{Ast, CorrelatedSubquery, Correlation, Dir, OrderPart};

// row = (id PK, created, text); sort = (created asc, id asc) — `created` is the
// main sort key, `id` the tiebreak (mirrors the JS take suite). `text` is an
// out-of-sort payload so an edit that does not move the row is still observable.
fn take_schema() -> SourceSchema {
    SourceSchema::new(
        vec!["id", "created", "text"],
        vec![0],
        vec![(1, true), (0, true)],
    )
}
fn sort() -> Vec<(usize, bool)> {
    vec![(1, true), (0, true)]
}
fn r(id: i64, created: i64, text: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(created), V::Int(text)]
}

/// A(1,100), B(2,200), C(3,300), D(4,400), E(5,500) — the standard fixture; with
/// `limit 3` the window is `[A,B,C]` and the bound is `C` (created 300).
fn abcde() -> Vec<Vec<V>> {
    vec![
        r(1, 100, 11),
        r(2, 200, 22),
        r(3, 300, 33),
        r(4, 400, 44),
        r(5, 500, 55),
    ]
}

/// A build closure wiring `conn → take` over source 0 (unpartitioned). The runner
/// sinks the returned op into a `Catch`.
fn take_build(limit: u32) -> impl FnOnce(&mut Graph, &[NodeId]) -> NodeId {
    move |g, srcs| {
        let conn = g.connect(srcs[0], Some(sort()), None, vec![]);
        let storage = g.alloc_storage();
        let take = g.add_take(Take::new(conn, storage, limit, None, sort()));
        g.set_conn_output(
            conn,
            OutEdge {
                node: take,
                port: Port::Single,
            },
        );
        take
    }
}

/// Run one push over the ABCDE fixture at `limit` and return the caught downstream
/// change stream.
fn push_one(limit: u32, change: SourceChange) -> Vec<CaughtChange> {
    run_push_test(
        vec![SourceSpec::new(take_schema(), abcde())],
        take_build(limit),
        vec![(0, change)],
        false,
    )
    .pushes
}

fn add_c(id: i64, created: i64, text: i64) -> SourceChange {
    SourceChange::Add(row(r(id, created, text)))
}
fn remove_c(id: i64, created: i64, text: i64) -> SourceChange {
    SourceChange::Remove(row(r(id, created, text)))
}
fn edit_c(old: Vec<V>, new: Vec<V>) -> SourceChange {
    SourceChange::Edit {
        row: row(new),
        old: row(old),
    }
}

// --- fetch -----------------------------------------------------------------

#[test]
fn take_fetch_keeps_first_n() {
    // limit 3 over A..E keeps the first three by (created, id).
    let initial = run_fetch_test(vec![SourceSpec::new(take_schema(), abcde())], take_build(3));
    assert_eq!(
        initial,
        vec![
            cleaf(r(1, 100, 11)),
            cleaf(r(2, 200, 22)),
            cleaf(r(3, 300, 33))
        ]
    );
}

#[test]
fn take_fetch_limit_zero_is_empty() {
    assert!(
        run_fetch_test(vec![SourceSpec::new(take_schema(), abcde())], take_build(0)).is_empty()
    );
}

// --- Add -------------------------------------------------------------------

#[test]
fn add_with_room_forwards() {
    // limit 5, only A..E present (5 rows == limit, so add a 6th below the window):
    // size 5 == limit, so instead use limit 6 to leave room.
    let pushes = push_one(6, add_c(6, 50, 66));
    assert_eq!(pushes, vec![add(cleaf(r(6, 50, 66)))]);
}

#[test]
fn add_at_limit_after_bound_is_dropped() {
    // limit 3, bound C(300). A new row at created 350 is outside the window.
    assert_eq!(push_one(3, add_c(6, 350, 66)), vec![]);
}

#[test]
fn add_at_limit_displaces_boundary_limit_gt1() {
    // limit 3, bound C(300). A row at created 50 displaces the boundary: remove the
    // bound (C), add the new row; the new bound becomes B(200).
    assert_eq!(
        push_one(3, add_c(6, 50, 66)),
        vec![remove(cleaf(r(3, 300, 33))), add(cleaf(r(6, 50, 66)))]
    );
}

#[test]
fn add_at_limit_displaces_boundary_limit_1() {
    // limit 1, bound A(100). A row at created 50 replaces it.
    assert_eq!(
        push_one(1, add_c(6, 50, 66)),
        vec![remove(cleaf(r(1, 100, 11))), add(cleaf(r(6, 50, 66)))]
    );
}

// --- Remove ----------------------------------------------------------------

#[test]
fn remove_after_bound_is_dropped() {
    // limit 3, bound C(300). Removing D(400) (outside) emits nothing.
    assert_eq!(push_one(3, remove_c(4, 400, 44)), vec![]);
}

#[test]
fn remove_inside_window_refills() {
    // limit 3, bound C(300). Removing A(100) (inside) refills with D(400) — the
    // first row past the bound.
    assert_eq!(
        push_one(3, remove_c(1, 100, 11)),
        vec![remove(cleaf(r(1, 100, 11))), add(cleaf(r(4, 400, 44)))]
    );
}

#[test]
fn remove_inside_window_no_refill_shrinks() {
    // Only A,B,C present (== limit 3), bound C(300). Removing A has no refill (no
    // row past the bound): just the remove, partition shrinks to size 2.
    let pushes = run_push_test(
        vec![SourceSpec::new(
            take_schema(),
            vec![r(1, 100, 11), r(2, 200, 22), r(3, 300, 33)],
        )],
        take_build(3),
        vec![(0, remove_c(1, 100, 11))],
        false,
    )
    .pushes;
    assert_eq!(pushes, vec![remove(cleaf(r(1, 100, 11)))]);
}

// --- Child -----------------------------------------------------------------

#[test]
fn child_gates_on_bound() {
    // A Child change forwards iff its row is within the window (<= bound). Pushed
    // directly (a Child needs no source overlay); state is hydrated first.
    let mut g = Graph::new();
    let src = g.add_source(take_schema(), abcde().into_iter().map(row).collect());
    let conn = g.connect(src, Some(sort()), None, vec![]);
    let storage = g.alloc_storage();
    let take = g.add_take(Take::new(conn, storage, 3, None, sort()));
    let catch = g.add_catch(take, false);
    g.set_conn_output(
        conn,
        OutEdge {
            node: take,
            port: Port::Single,
        },
    );
    g.set_output(take, catch);
    let _ = g.catch_fetch(catch, &FetchRequest::all()); // hydrate the take state

    let child_change = |id: i64, created: i64| Change::Child {
        node: Node::leaf(row(r(id, created, 0))),
        rel: rel(0),
        child: Box::new(Change::Add(Node::leaf(row(vec![
            V::Int(99),
            V::Int(0),
            V::Int(0),
        ])))),
    };
    g.push_at(take, child_change(2, 200), Port::Single); // <= bound C(300) → kept
    g.push_at(take, child_change(4, 400), Port::Single); // > bound → dropped
    assert_eq!(
        g.catch_pushes(catch),
        vec![child(
            r(2, 200, 0),
            rel(0),
            add(cleaf(vec![V::Int(99), V::Int(0), V::Int(0)]))
        )]
    );
}

// --- Edit matrix (#pushEditChange) -----------------------------------------

#[test]
fn edit_bound_unchanged_position_forwards() {
    // oldCmp==0, newCmp==0: edit the bound row's payload, it stays the bound.
    assert_eq!(
        push_one(3, edit_c(r(3, 300, 33), r(3, 300, 99))),
        vec![edit(r(3, 300, 33), r(3, 300, 99))]
    );
}

#[test]
fn edit_both_inside_forwards() {
    // oldCmp<0, newCmp<0: B stays inside the window.
    assert_eq!(
        push_one(3, edit_c(r(2, 200, 22), r(2, 250, 99))),
        vec![edit(r(2, 200, 22), r(2, 250, 99))]
    );
}

#[test]
fn edit_both_outside_is_dropped() {
    // oldCmp>0, newCmp>0: D stays outside the window.
    assert_eq!(push_one(3, edit_c(r(4, 400, 44), r(4, 420, 99))), vec![]);
}

#[test]
fn edit_bound_moves_inside_limit_gt1() {
    // oldCmp==0, newCmp<0, limit>1: the bound C moves earlier (still inside); it
    // remains the largest in the window → forward the edit.
    assert_eq!(
        push_one(3, edit_c(r(3, 300, 33), r(3, 250, 99))),
        vec![edit(r(3, 300, 33), r(3, 250, 99))]
    );
}

#[test]
fn edit_bound_moves_outside_replaces_with_next() {
    // oldCmp==0, newCmp>0, next-row != new: the bound C edits past D, falling out of
    // the window; D becomes the new bound. Remove(old C) + Add(D).
    assert_eq!(
        push_one(3, edit_c(r(3, 300, 33), r(3, 450, 99))),
        vec![remove(cleaf(r(3, 300, 33))), add(cleaf(r(4, 400, 44)))]
    );
}

#[test]
fn edit_outside_moves_inside_displaces_boundary() {
    // oldCmp>0, newCmp<0: D was outside, edits to created 250 (inside), pushing the
    // current bound C out. Remove(C) + Add(new D).
    assert_eq!(
        push_one(3, edit_c(r(4, 400, 44), r(4, 250, 99))),
        vec![remove(cleaf(r(3, 300, 33))), add(cleaf(r(4, 250, 99)))]
    );
}

#[test]
fn edit_inside_moves_outside_becomes_new_bound() {
    // oldCmp<0, newCmp>0, after-row == new: B edits to created 350 — the first row
    // past the bound is B itself → it becomes the new bound, forward the edit.
    assert_eq!(
        push_one(3, edit_c(r(2, 200, 22), r(2, 350, 99))),
        vec![edit(r(2, 200, 22), r(2, 350, 99))]
    );
}

#[test]
fn edit_inside_moves_outside_displaces() {
    // oldCmp<0, newCmp>0, after-row != new: B edits to created 550 (past D,E); the
    // first row past the bound is D → Remove(old B) + Add(D).
    assert_eq!(
        push_one(3, edit_c(r(2, 200, 22), r(2, 550, 99))),
        vec![remove(cleaf(r(2, 200, 22))), add(cleaf(r(4, 400, 44)))]
    );
}

#[test]
fn edit_bound_moves_inside_limit_1() {
    // oldCmp==0, newCmp<0, limit==1: the single bound A moves earlier; replace the
    // bound and forward the edit.
    assert_eq!(
        push_one(1, edit_c(r(1, 100, 11), r(1, 50, 99))),
        vec![edit(r(1, 100, 11), r(1, 50, 99))]
    );
}

// --- end-to-end through build_pipeline -------------------------------------

#[test]
fn ast_top_level_limit_fetch() {
    // `issue (limit 2)` over four issues keeps the first two by id.
    let issue_s = SourceSchema::new(vec!["id"], vec![0], vec![(0, true)]);
    let ast = Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        limit: Some(2),
        ..Default::default()
    };
    let tree = run_fetch_test_ast(
        vec![TableSpec::new(
            "issue",
            issue_s,
            vec![
                vec![V::Int(1)],
                vec![V::Int(2)],
                vec![V::Int(3)],
                vec![V::Int(4)],
            ],
        )],
        &ast,
    )
    .unwrap();
    assert_eq!(tree, vec![cleaf(vec![V::Int(1)]), cleaf(vec![V::Int(2)])]);
}

#[test]
fn ast_partitioned_limit_in_relationship_fetch() {
    // `issue { comments (limit 1) }` — a limited relationship lowers to a Take
    // PARTITIONED by the correlation child field (issueID). The parent join fetches
    // each issue's comments with a constraint matching the partition key, so each
    // partition hydrates independently and yields its single lowest-`created` row.
    let issue_s = SourceSchema::new(vec!["id"], vec![0], vec![(0, true)]);
    let comment_s = SourceSchema::new(
        vec!["id", "issueID", "created"],
        vec![0],
        vec![(2, true), (0, true)],
    );

    let ast = Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        related: vec![CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                table: "comment".into(),
                alias: Some("comments".into()),
                order_by: vec![OrderPart("created".into(), Dir::Asc)],
                limit: Some(1),
                ..Default::default()
            }),
            system: None,
        }],
        ..Default::default()
    };

    let tree = run_fetch_test_ast(
        vec![
            TableSpec::new("issue", issue_s, vec![vec![V::Int(1)], vec![V::Int(2)]]),
            TableSpec::new(
                "comment",
                comment_s,
                vec![
                    vec![V::Int(10), V::Int(1), V::Int(10)], // i1, created 10
                    vec![V::Int(11), V::Int(1), V::Int(20)], // i1, created 20 (dropped by limit)
                    vec![V::Int(12), V::Int(2), V::Int(30)], // i2, created 30
                ],
            ),
        ],
        &ast,
    )
    .unwrap();

    assert_eq!(
        tree,
        vec![
            rindle::testkit::cnode(
                vec![V::Int(1)],
                vec![(rel(0), vec![cleaf(vec![V::Int(10), V::Int(1), V::Int(10)])])],
            ),
            rindle::testkit::cnode(
                vec![V::Int(2)],
                vec![(rel(0), vec![cleaf(vec![V::Int(12), V::Int(2), V::Int(30)])])],
            ),
        ]
    );
}

#[test]
fn ast_partitioned_limit_in_relationship_push_displaces() {
    // `issue { comments (limit 1) }`. Issue 1 shows its single lowest-`created`
    // comment (c10, created 10). Adding a comment with a smaller `created`
    // displaces the boundary inside that partition; the Take emits Remove+Add on its
    // JoinChild edge, which the issue join lifts into two Child changes on issue 1.
    let issue_s = SourceSchema::new(vec!["id"], vec![0], vec![(0, true)]);
    let comment_s = SourceSchema::new(
        vec!["id", "issueID", "created"],
        vec![0],
        vec![(2, true), (0, true)],
    );
    let ast = Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        related: vec![CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                table: "comment".into(),
                alias: Some("comments".into()),
                order_by: vec![OrderPart("created".into(), Dir::Asc)],
                limit: Some(1),
                ..Default::default()
            }),
            system: None,
        }],
        ..Default::default()
    };

    let res = run_push_test_ast(
        vec![
            TableSpec::new("issue", issue_s, vec![vec![V::Int(1)]]),
            TableSpec::new(
                "comment",
                comment_s,
                vec![vec![V::Int(10), V::Int(1), V::Int(10)]],
            ),
        ],
        &ast,
        // add a comment for issue 1 with a smaller `created` (5 < 10): displaces.
        vec![(
            "comment",
            SourceChange::Add(row(vec![V::Int(9), V::Int(1), V::Int(5)])),
        )],
        false,
    )
    .unwrap();

    // The displacement lifts into Child(Remove old boundary) then Child(Add new).
    assert_eq!(
        res.pushes,
        vec![
            child(
                vec![V::Int(1)],
                rel(0),
                remove(cleaf(vec![V::Int(10), V::Int(1), V::Int(10)])),
            ),
            child(
                vec![V::Int(1)],
                rel(0),
                add(cleaf(vec![V::Int(9), V::Int(1), V::Int(5)])),
            ),
        ]
    );
}
