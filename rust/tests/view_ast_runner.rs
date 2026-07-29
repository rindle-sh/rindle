#![cfg(feature = "testkit")]

//! End-to-end tests for the **View-based AST runners** (`run_push_test_ast_view` /
//! `run_fetch_test_ast_view`) — the e2e payoff: `build_pipeline` → production
//! `ArrayView` → push → read `view.data`, with the JS build-twice storage compare.
//! Exercised through the public crate boundary exactly as a JS `runPushTest` port
//! consumes them.
//!
//! The Catch-sink AST runners (`tests/testkit.rs`) prove the raw dataflow change
//! stream; these prove the **materialized view tree** the consumer actually sees —
//! relationship-nested, gating-EXISTS-excluded, copy-on-write-folded.

use rindle::testkit::{
    add, child, cleaf, cnode, rel, run_fetch_test_ast, run_fetch_test_ast_view,
    run_push_test_ast_view, CaughtNode, TableSpec,
};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{
    table, Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir,
    ExistsOp, OrderPart, SourceChange,
};

// ---------------------------------------------------------------------------
// schemas
// ---------------------------------------------------------------------------

/// issue(id, val); PK id. The `comments` slot is query-derived (not declared here).
fn issues_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}
/// issue(id, val); PK id; relationship-free.
fn issues_only() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}
/// comment(id, issue, val); PK id; joins to issue on comment.issue == issue.id.
fn comments_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)])
}

/// issue { comments } as a fluent `Ast`.
fn issue_comments_ast() -> Ast {
    table("issue")
        .sub_as("comments", |r| {
            table("comment").r#where("issue", r.col("id"))
        })
        .build()
}

// ---------------------------------------------------------------------------
// fetch: the View materializes the nested tree — identical to the Catch tree
// ---------------------------------------------------------------------------

#[test]
fn view_fetch_materializes_the_join_tree() {
    let sources = || {
        vec![
            TableSpec::new(
                "issue",
                issues_schema(),
                vec![vec![V::Int(1), V::Int(100)], vec![V::Int(2), V::Int(200)]],
            ),
            TableSpec::new(
                "comment",
                comments_schema(),
                vec![
                    vec![V::Int(10), V::Int(1), V::Int(500)],
                    vec![V::Int(11), V::Int(1), V::Int(501)],
                    vec![V::Int(20), V::Int(2), V::Int(900)],
                ],
            ),
        ]
    };
    let data = run_fetch_test_ast_view(sources(), &issue_comments_ast()).unwrap();
    assert_eq!(
        data,
        vec![
            cnode(
                vec![V::Int(1), V::Int(100)],
                vec![(
                    rel(0),
                    vec![
                        cleaf(vec![V::Int(10), V::Int(1), V::Int(500)]),
                        cleaf(vec![V::Int(11), V::Int(1), V::Int(501)]),
                    ],
                )],
            ),
            cnode(
                vec![V::Int(2), V::Int(200)],
                vec![(
                    rel(0),
                    vec![cleaf(vec![V::Int(20), V::Int(2), V::Int(900)])]
                )],
            ),
        ]
    );

    // For a relationship-only query (no gating EXISTS) the production-View tree is
    // identical to the raw Catch fetch tree — a free cross-sink data differential.
    assert_eq!(
        data,
        run_fetch_test_ast(sources(), &issue_comments_ast()).unwrap()
    );
}

// ---------------------------------------------------------------------------
// push: a child add folds into view.data under its relationship slot
// ---------------------------------------------------------------------------

#[test]
fn view_push_add_child_updates_data() {
    let r = run_push_test_ast_view(
        vec![
            TableSpec::new("issue", issues_schema(), vec![vec![V::Int(1), V::Int(100)]]),
            TableSpec::new("comment", comments_schema(), vec![]),
        ],
        &issue_comments_ast(),
        vec![(
            "comment",
            SourceChange::Add(row(vec![V::Int(10), V::Int(1), V::Int(500)])),
        )],
        false,
    )
    .unwrap();

    // hydrate: issue 1 with an empty comments relationship (slot 0).
    assert_eq!(
        r.initial,
        vec![cnode(vec![V::Int(1), V::Int(100)], vec![(rel(0), vec![])])]
    );
    // after the push: the comment is materialized under slot 0.
    assert_eq!(
        r.data,
        vec![cnode(
            vec![V::Int(1), V::Int(100)],
            vec![(
                rel(0),
                vec![cleaf(vec![V::Int(10), V::Int(1), V::Int(500)])]
            )],
        )]
    );
    // the parallel Catch build caught the corresponding Child change.
    assert_eq!(
        r.pushes,
        vec![child(
            vec![V::Int(1), V::Int(100)],
            rel(0),
            add(cleaf(vec![V::Int(10), V::Int(1), V::Int(500)])),
        )]
    );
}

// ---------------------------------------------------------------------------
// duplicate `related` alias: the View must derive the child schema/sort from the
// LAST writer (matching the dataflow's `dedup_related_by_alias`), so materialized
// child order agrees with the Catch fetch tree. (Regression for the review finding
// where view_schema_format used `.find` first-writer while the Join uses last.)
// ---------------------------------------------------------------------------

fn dup_alias_ast(dir_first: Dir, dir_last: Dir) -> Ast {
    let mk = |dir| CorrelatedSubquery {
        correlation: Correlation {
            parent_field: vec!["id".into()],
            child_field: vec!["issue".into()],
        },
        subquery: Box::new(Ast {
            table: "comment".into(),
            alias: Some("comments".into()),
            order_by: vec![OrderPart("val".into(), dir)],
            ..Default::default()
        }),
        system: None,
    };
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        related: vec![mk(dir_first), mk(dir_last)],
        ..Default::default()
    }
}

#[test]
fn view_duplicate_related_alias_uses_last_writer_matching_dataflow() {
    let sources = || {
        vec![
            TableSpec::new("issue", issues_schema(), vec![vec![V::Int(1), V::Int(100)]]),
            TableSpec::new(
                "comment",
                comments_schema(),
                vec![
                    vec![V::Int(10), V::Int(1), V::Int(500)],
                    vec![V::Int(11), V::Int(1), V::Int(900)],
                    vec![V::Int(12), V::Int(1), V::Int(100)],
                ],
            ),
        ]
    };
    // First writer orders comments val-ASC, last writer val-DESC. The dataflow Join is
    // built from the LAST writer (val-DESC), so the View must materialize children
    // val-DESC too — identical to the Catch fetch tree.
    let ast = dup_alias_ast(Dir::Asc, Dir::Desc);
    let view = run_fetch_test_ast_view(sources(), &ast).unwrap();
    let catch = run_fetch_test_ast(sources(), &ast).unwrap();
    assert_eq!(
        view, catch,
        "View child order must match the dataflow (last-writer); a first-writer pick diverges"
    );
    // Explicit: children in val-DESC order (900, 500, 100).
    assert_eq!(
        view,
        vec![cnode(
            vec![V::Int(1), V::Int(100)],
            vec![(
                rel(0),
                vec![
                    cleaf(vec![V::Int(11), V::Int(1), V::Int(900)]),
                    cleaf(vec![V::Int(10), V::Int(1), V::Int(500)]),
                    cleaf(vec![V::Int(12), V::Int(1), V::Int(100)]),
                ],
            )],
        )]
    );
}

#[test]
fn view_push_parent_add_remove_edit() {
    let ast = table("issue").order_by("id", "asc").build();
    let r = run_push_test_ast_view(
        vec![TableSpec::new(
            "issue",
            issues_only(),
            vec![vec![V::Int(1), V::Int(100)], vec![V::Int(2), V::Int(200)]],
        )],
        &ast,
        vec![
            (
                "issue",
                SourceChange::Add(row(vec![V::Int(3), V::Int(300)])),
            ),
            (
                "issue",
                SourceChange::Remove(row(vec![V::Int(1), V::Int(100)])),
            ),
            (
                "issue",
                SourceChange::Edit {
                    row: row(vec![V::Int(2), V::Int(222)]),
                    old: row(vec![V::Int(2), V::Int(200)]),
                },
            ),
        ],
        false,
    )
    .unwrap();

    assert_eq!(
        r.initial,
        vec![
            cleaf(vec![V::Int(1), V::Int(100)]),
            cleaf(vec![V::Int(2), V::Int(200)])
        ]
    );
    // id 1 removed, id 2 edited to val 222, id 3 added — sorted by id.
    assert_eq!(
        r.data,
        vec![
            cleaf(vec![V::Int(2), V::Int(222)]),
            cleaf(vec![V::Int(3), V::Int(300)])
        ]
    );
}

// ---------------------------------------------------------------------------
// EXISTS: the gating relationship is EXCLUDED from view.data, and a Child change
// on that out-of-view slot is ignored — not a panic (the `apply_child` format-first
// reorder). issue WHERE EXISTS(labels).
// ---------------------------------------------------------------------------

fn issue_labels_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}
fn label_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}
fn exists_labels_ast() -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        r#where: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueID".into()],
                },
                subquery: Box::new(Ast {
                    table: "label".into(),
                    alias: Some("labels".into()),
                    order_by: vec![OrderPart("id".into(), Dir::Asc)],
                    ..Default::default()
                }),
                system: None,
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        })),
        ..Default::default()
    }
}

#[test]
fn view_exists_excludes_gating_rel_and_forwards_child_without_panic() {
    // issue 1 has a label (matches EXISTS); issue 2 has none (excluded).
    let r = run_push_test_ast_view(
        vec![
            TableSpec::new(
                "issue",
                issue_labels_schema(),
                vec![vec![V::Int(1), V::Int(10)], vec![V::Int(2), V::Int(20)]],
            ),
            TableSpec::new("label", label_schema(), vec![vec![V::Int(100), V::Int(1)]]),
        ],
        &exists_labels_ast(),
        // Add a 2nd label to issue 1: size 1->2, existence does NOT flip, so the
        // Exists gate forwards a Child change on its own (out-of-view) slot. The
        // View must IGNORE it via the format check — the apply_child reorder.
        // (The old rel_child-first order panicked here on the missing child schema.)
        vec![(
            "label",
            SourceChange::Add(row(vec![V::Int(101), V::Int(1)])),
        )],
        false,
    )
    .unwrap();

    // Only issue 1 is present, and the gating `labels` slot is NOT in the view tree
    // (a bare leaf, no relationships).
    assert_eq!(r.initial, vec![cleaf(vec![V::Int(1), V::Int(10)])]);
    assert_eq!(r.data, vec![cleaf(vec![V::Int(1), V::Int(10)])]);
}

#[test]
fn view_exists_membership_flip_adds_then_removes_the_row() {
    // Start with no labels → issue 1 excluded; add a label → it appears.
    let added = run_push_test_ast_view(
        vec![
            TableSpec::new(
                "issue",
                issue_labels_schema(),
                vec![vec![V::Int(1), V::Int(10)]],
            ),
            TableSpec::new("label", label_schema(), vec![]),
        ],
        &exists_labels_ast(),
        vec![(
            "label",
            SourceChange::Add(row(vec![V::Int(100), V::Int(1)])),
        )],
        false,
    )
    .unwrap();
    assert_eq!(added.initial, Vec::<CaughtNode>::new());
    assert_eq!(added.data, vec![cleaf(vec![V::Int(1), V::Int(10)])]);

    // Start with one label → present; remove the last label → it drops out.
    let removed = run_push_test_ast_view(
        vec![
            TableSpec::new(
                "issue",
                issue_labels_schema(),
                vec![vec![V::Int(1), V::Int(10)]],
            ),
            TableSpec::new("label", label_schema(), vec![vec![V::Int(100), V::Int(1)]]),
        ],
        &exists_labels_ast(),
        vec![(
            "label",
            SourceChange::Remove(row(vec![V::Int(100), V::Int(1)])),
        )],
        false,
    )
    .unwrap();
    assert_eq!(removed.initial, vec![cleaf(vec![V::Int(1), V::Int(10)])]);
    assert_eq!(removed.data, Vec::<CaughtNode>::new());
}

// ---------------------------------------------------------------------------
// limit: a stateful Take writes scratch storage → the build-twice storage compare
// is exercised (a sink-dependence bug would panic inside the runner).
// ---------------------------------------------------------------------------

#[test]
fn view_limit_window_holds_and_storage_compare_passes() {
    let ast = table("issue").order_by("id", "asc").limit(2).build();
    let r = run_push_test_ast_view(
        vec![TableSpec::new(
            "issue",
            issues_only(),
            vec![
                vec![V::Int(1), V::Int(1)],
                vec![V::Int(2), V::Int(2)],
                vec![V::Int(3), V::Int(3)],
            ],
        )],
        &ast,
        // id 0 sorts before the window boundary → displaces id 2 out of the limit-2.
        vec![("issue", SourceChange::Add(row(vec![V::Int(0), V::Int(0)])))],
        false,
    )
    .unwrap();

    assert_eq!(
        r.initial,
        vec![
            cleaf(vec![V::Int(1), V::Int(1)]),
            cleaf(vec![V::Int(2), V::Int(2)])
        ]
    );
    // window of 2 after the add: [0, 1].
    assert_eq!(
        r.data,
        vec![
            cleaf(vec![V::Int(0), V::Int(0)]),
            cleaf(vec![V::Int(1), V::Int(1)])
        ]
    );
    // (The build-twice storage compare ran inside the runner: the Take operator's
    // scratch state is identical for the Catch and View builds — otherwise the
    // runner would have panicked with a "build-twice storage mismatch".)
}

// ---------------------------------------------------------------------------
// union fan: the View upholds view-after-push == fresh-query directly on view.data
// (the documented Rust-correct behavior, where JS drops the non-flipped EXISTS push).
// issue WHERE (x = 1 OR EXISTS_flipped(comments) OR EXISTS(labels)).
// ---------------------------------------------------------------------------

fn union_schema() -> SourceSchema {
    // The source declares no relationships; slots are query-derived (EXISTS
    // pre-order → comments=0, labels=1).
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
fn child_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}
fn exists_cond(alias: &'static str, tbl: &'static str, flip: bool) -> Condition {
    Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                table: tbl.into(),
                alias: Some(alias.into()),
                order_by: vec![OrderPart("id".into(), Dir::Asc)],
                ..Default::default()
            }),
            system: None,
        },
        op: ExistsOp::Exists,
        flip: if flip { Some(true) } else { None },
        scalar: None,
        plan_id: None,
    })
}
fn union_ast() -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        r#where: Some(Condition::Or {
            conditions: vec![
                Condition::Simple(rindle::SimpleCondition {
                    op: rindle::Op::Eq,
                    left: rindle::ValuePosition::Column { name: "x".into() },
                    right: rindle::ValuePosition::Literal {
                        value: rindle::Lit::Number(1.0),
                    },
                }),
                exists_cond("comments", "comment", true),
                exists_cond("labels", "label", false),
            ],
        }),
        ..Default::default()
    }
}
fn data_ids(data: &[CaughtNode]) -> Vec<i64> {
    let mut ids: Vec<i64> = data
        .iter()
        .map(|n| match n.row.col(0).to_owned() {
            V::Int(i) => i,
            _ => panic!("non-int id"),
        })
        .collect();
    ids.sort_unstable();
    ids
}
fn sources(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>, labels: Vec<Vec<V>>) -> Vec<TableSpec> {
    vec![
        TableSpec::new("issue", union_schema(), issues),
        TableSpec::new("comment", child_schema(), comments),
        TableSpec::new("label", child_schema(), labels),
    ]
}

/// The maintained `view.data` after the push must equal a fresh fetch of the
/// post-push state — read directly off the production View (no Catch-stream fold).
fn assert_view_consistent(s0: Vec<TableSpec>, push: (&str, SourceChange), s1: Vec<TableSpec>) {
    let maintained = run_push_test_ast_view(s0, &union_ast(), vec![push], false).unwrap();
    let fresh = run_fetch_test_ast_view(s1, &union_ast()).unwrap();
    assert_eq!(
        data_ids(&maintained.data),
        data_ids(&fresh),
        "view-after-push != fresh-query (IVM consistency)"
    );
}

#[test]
fn view_union_fan_add_label_to_bare_row_is_consistent() {
    // issue 4 (x=5) bare → absent; add a label → it now matches EXISTS(labels).
    assert_view_consistent(
        sources(vec![ir(1, 1), ir(4, 5)], vec![], vec![]),
        ("label", SourceChange::Add(row(cr(21, 4)))),
        sources(vec![ir(1, 1), ir(4, 5)], vec![], vec![cr(21, 4)]),
    );
}

#[test]
fn view_union_fan_remove_last_label_is_consistent() {
    // issue 3 (x=5) matched only via its label; remove it → issue 3 drops out.
    assert_view_consistent(
        sources(vec![ir(1, 1), ir(3, 5)], vec![], vec![cr(20, 3)]),
        ("label", SourceChange::Remove(row(cr(20, 3)))),
        sources(vec![ir(1, 1), ir(3, 5)], vec![], vec![]),
    );
}

#[test]
fn view_union_fan_flipped_comment_add_is_consistent() {
    // Control: the flipped EXISTS(comments) path (push-correct in both engines).
    assert_view_consistent(
        sources(vec![ir(1, 1), ir(4, 5)], vec![], vec![]),
        ("comment", SourceChange::Add(row(cr(11, 4)))),
        sources(vec![ir(1, 1), ir(4, 5)], vec![cr(11, 4)], vec![]),
    );
}

fn ir(id: i64, x: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(x)]
}
fn cr(id: i64, issue: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue)]
}
