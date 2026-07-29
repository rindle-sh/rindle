#![cfg(feature = "testkit")]

//! Integration tests for the spec-`11` testkit harness, exercised through the
//! **public crate boundary** — i.e. exactly how an operator agent's `tests/*.rs`
//! suite will consume it (`rindle::testkit::*` + the public `Graph` API). The
//! harness's own internals are unit-tested in `src/testkit.rs`; this file proves
//! the externally-visible surface is wired and ergonomic.

use rindle::change::{OutEdge, Port};
use rindle::graph::{Graph, NodeId};
use rindle::testkit::{
    add, child, cleaf, cnode, edit, fingerprint, rel, remove, run_fetch_test, run_fetch_test_ast,
    run_push_test, run_push_test_ast, ChangeRecord, SnitchMessage, SourceSpec, TableSpec,
};
use rindle::value::{owned_row as row, OwnedValue as V, RelId, SourceSchema};
use rindle::{table, Condition, SourceChange};

// issue(id, val); PK id; the "comments" relationship is slot 0 on the parent
// (passed to `add_join_slot` as `RelId(0)`).
fn issues_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}
// comment(id, issue, val); PK id; joins to issue on comment.issue == issue.id.
fn comments_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)])
}

/// Wire issues ⟕ comments (rel "comments") off the two source connections and
/// return the join (the op feeding the sink). This is the shape every operator
/// suite's `build` closure takes.
fn build_issues_join_comments(g: &mut Graph, srcs: &[NodeId]) -> NodeId {
    let pconn = g.connect(srcs[0], Some(vec![(0, true)]), None, vec![]);
    let cconn = g.connect(srcs[1], Some(vec![(0, true)]), None, vec![]);
    let join = g.add_join_slot(pconn, cconn, vec![0], vec![1], RelId(0));
    g.set_conn_output(
        pconn,
        OutEdge {
            node: join,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        cconn,
        OutEdge {
            node: join,
            port: Port::JoinChild,
        },
    );
    join
}

#[test]
fn run_fetch_test_materializes_the_join_tree() {
    let caught = run_fetch_test(
        vec![
            SourceSpec::new(
                issues_schema(),
                vec![vec![V::Int(1), V::Int(100)], vec![V::Int(2), V::Int(200)]],
            ),
            SourceSpec::new(
                comments_schema(),
                vec![
                    vec![V::Int(10), V::Int(1), V::Int(500)],
                    vec![V::Int(11), V::Int(1), V::Int(501)],
                    vec![V::Int(20), V::Int(2), V::Int(900)],
                ],
            ),
        ],
        build_issues_join_comments,
    );

    assert_eq!(
        caught,
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
}

#[test]
fn run_push_test_catches_child_add_as_a_child_change() {
    let r = run_push_test(
        vec![
            SourceSpec::new(issues_schema(), vec![vec![V::Int(1), V::Int(100)]]),
            SourceSpec::new(comments_schema(), vec![]),
        ],
        build_issues_join_comments,
        vec![(
            1,
            SourceChange::Add(row(vec![V::Int(10), V::Int(1), V::Int(500)])),
        )],
        false,
    );

    // hydrate saw issue 1 with no comments.
    assert_eq!(
        r.initial,
        vec![cnode(vec![V::Int(1), V::Int(100)], vec![(rel(0), vec![])])]
    );
    // the comment add fans up to the parent as a Child change on slot 0.
    assert_eq!(
        r.pushes,
        vec![child(
            vec![V::Int(1), V::Int(100)],
            rel(0),
            add(cleaf(vec![V::Int(10), V::Int(1), V::Int(500)])),
        )]
    );
}

#[test]
fn run_push_test_catches_parent_add_and_remove() {
    let r = run_push_test(
        vec![
            SourceSpec::new(issues_schema(), vec![]),
            SourceSpec::new(comments_schema(), vec![]),
        ],
        build_issues_join_comments,
        vec![
            (0, SourceChange::Add(row(vec![V::Int(1), V::Int(100)]))),
            (0, SourceChange::Remove(row(vec![V::Int(1), V::Int(100)]))),
        ],
        false,
    );

    // a parent add/remove carries the (empty) "comments" relationship.
    assert_eq!(
        r.pushes,
        vec![
            add(cnode(vec![V::Int(1), V::Int(100)], vec![(rel(0), vec![])])),
            remove(cnode(vec![V::Int(1), V::Int(100)], vec![(rel(0), vec![])])),
        ]
    );
}

#[test]
fn run_push_test_catches_parent_edit() {
    let r = run_push_test(
        vec![
            SourceSpec::new(issues_schema(), vec![vec![V::Int(1), V::Int(100)]]),
            SourceSpec::new(comments_schema(), vec![]),
        ],
        build_issues_join_comments,
        // edit the non-key `val` 100 -> 101 (join key `id` unchanged → a real Edit).
        vec![(
            0,
            SourceChange::Edit {
                row: row(vec![V::Int(1), V::Int(101)]),
                old: row(vec![V::Int(1), V::Int(100)]),
            },
        )],
        false,
    );
    // Edit carries only the two rows (catch.ts:104-109).
    assert_eq!(
        r.pushes,
        vec![edit(
            vec![V::Int(1), V::Int(100)],
            vec![V::Int(1), V::Int(101)]
        )]
    );
}

#[test]
fn fetch_on_push_pairs_each_change_with_a_refetch() {
    let r = run_push_test(
        vec![
            SourceSpec::new(issues_schema(), vec![vec![V::Int(1), V::Int(100)]]),
            SourceSpec::new(comments_schema(), vec![]),
        ],
        build_issues_join_comments,
        vec![(
            1,
            SourceChange::Add(row(vec![V::Int(10), V::Int(1), V::Int(500)])),
        )],
        true, // fetch_on_push
    );

    assert_eq!(r.pushes_with_fetch.len(), 1);
    let (change, refetch) = &r.pushes_with_fetch[0];
    assert_eq!(
        *change,
        child(
            vec![V::Int(1), V::Int(100)],
            rel(0),
            add(cleaf(vec![V::Int(10), V::Int(1), V::Int(500)])),
        )
    );
    // the re-fetch taken alongside the push sees the committed child.
    assert_eq!(
        *refetch,
        vec![cnode(
            vec![V::Int(1), V::Int(100)],
            vec![(
                rel(0),
                vec![cleaf(vec![V::Int(10), V::Int(1), V::Int(500)])]
            )],
        )]
    );
}

#[test]
fn snitch_logs_the_join_parent_message_stream() {
    // issues -> Snitch -> Join(parent) ; comments -> Join(child) ; Join -> Catch.
    let mut g = Graph::new();
    let issues = g.add_source(issues_schema(), vec![row(vec![V::Int(1), V::Int(100)])]);
    let comments = g.add_source(comments_schema(), vec![]);

    let pconn = g.connect(issues, Some(vec![(0, true)]), None, vec![]);
    let cconn = g.connect(comments, Some(vec![(0, true)]), None, vec![]);

    // Tap the parent stream just before the join's parent port.
    let snitch = g.add_snitch(pconn, ":source(issue)");
    let join = g.add_join_slot(snitch, cconn, vec![0], vec![1], RelId(0));
    let catch = g.add_catch(join, false);

    g.set_conn_output(
        pconn,
        OutEdge {
            node: snitch,
            port: Port::JoinParent,
        },
    );
    g.set_output(snitch, join);
    g.set_conn_output(
        cconn,
        OutEdge {
            node: join,
            port: Port::JoinChild,
        },
    );
    g.set_output(join, catch);

    // Hydrate: the join fetches its parent through the snitch.
    let _ = g.catch_fetch(catch, &rindle::FetchRequest::all());
    let log = g.snitch_log(snitch);
    assert!(
        matches!(log.first(), Some(SnitchMessage::Fetch { name, .. }) if name == ":source(issue)"),
        "first message is the parent Fetch, got {log:#?}"
    );
    match log.last() {
        Some(SnitchMessage::FetchCount { count, .. }) => assert_eq!(*count, 1),
        other => panic!("expected a FetchCount last, got {other:?}"),
    }

    // After clearing, a parent add is logged as a push on the snitch.
    g.clear_all_snitch_logs();
    g.source_push(issues, SourceChange::Add(row(vec![V::Int(2), V::Int(200)])));
    let pushes: Vec<_> = g
        .snitch_log(snitch)
        .into_iter()
        .filter(|m| matches!(m, SnitchMessage::Push { .. }))
        .collect();
    assert_eq!(
        pushes,
        vec![SnitchMessage::Push {
            name: ":source(issue)".to_string(),
            change: ChangeRecord::Add(row(vec![V::Int(2), V::Int(200)])),
        }]
    );

    // The push reached the Catch end-to-end as a parent Add — proof the Snitch is
    // **port-transparent**: it forwarded the change on `Port::JoinParent`, so the
    // join took its parent arm (a `Port::Single` would have panicked the join).
    assert_eq!(
        g.catch_pushes(catch),
        vec![add(cnode(
            vec![V::Int(2), V::Int(200)],
            vec![(rel(0), vec![])]
        ))]
    );
}

#[test]
fn fingerprint_agrees_across_identical_builds_and_splits_on_change() {
    let sources = || {
        vec![
            SourceSpec::new(
                issues_schema(),
                vec![vec![V::Int(1), V::Int(100)], vec![V::Int(2), V::Int(200)]],
            ),
            SourceSpec::new(
                comments_schema(),
                vec![vec![V::Int(10), V::Int(1), V::Int(500)]],
            ),
        ]
    };
    let a = run_fetch_test(sources(), build_issues_join_comments);
    let b = run_fetch_test(sources(), build_issues_join_comments);
    // Two independent builds of the same fixture must fingerprint identically
    // (the cross-build / chunk-equivalence equality probe, §3.5/§5.8).
    assert_eq!(fingerprint(&a), fingerprint(&b));

    let changed = run_fetch_test(
        vec![
            SourceSpec::new(
                issues_schema(),
                vec![vec![V::Int(1), V::Int(100)], vec![V::Int(2), V::Int(200)]],
            ),
            SourceSpec::new(
                comments_schema(),
                // comment moved from issue 1 to issue 2 → different tree.
                vec![vec![V::Int(10), V::Int(2), V::Int(500)]],
            ),
        ],
        build_issues_join_comments,
    );
    assert_ne!(fingerprint(&a), fingerprint(&changed));
}

// ===========================================================================
// The AST-driven runners — `build_pipeline` + Catch (the JS suites as a Rust
// differential corpus, spec `11` §3.1 deviation #1 lifted for the built subset).
// ===========================================================================

// label(id, issue); PK id; joins to issue on label.issue == issue.id.
fn labels_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issue"], vec![0], vec![(0, true)])
}
// issue(id, val); PK id; on the parent, "comments" is slot 0 and "labels" is slot 1
// (the order the builder assigns query-local slots; not pre-declared on the source).
fn issues_schema_2rel() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}

/// issue ⋈ comments (comment.issue == issue.id) as a JS-shaped `Ast`, built with
/// the fluent query builder — `.r#where(childField, row.col(parentField))` defines
/// the correlation, `sub_as` names the relationship.
fn issue_comments_ast() -> rindle::Ast {
    table("issue")
        .sub_as("comments", |row| {
            table("comment").r#where("issue", row.col("id"))
        })
        .build()
}

#[test]
fn run_push_test_ast_reproduces_the_build_closure_child_add() {
    // The SAME issue⟕comments pipeline, lowered from an `Ast` by `build_pipeline`
    // rather than wired by hand — the differential corpus payoff. A comment add must
    // fan up to the parent as a Child(comments) change, IDENTICALLY to the hand-wired
    // `run_push_test`. (Add-only push, so the differ isn't perturbed by the AST
    // runner's faithful non-empty split-edit keys vs the closure's empty ones —
    // those matter only when decomposing an Edit on a split key.)
    let ast_r = run_push_test_ast(
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

    // hydrate saw issue 1 with an empty "comments" relationship (slot 0).
    assert_eq!(
        ast_r.initial,
        vec![cnode(vec![V::Int(1), V::Int(100)], vec![(rel(0), vec![])])]
    );
    // the comment add fans up to the parent as a Child change on slot 0.
    assert_eq!(
        ast_r.pushes,
        vec![child(
            vec![V::Int(1), V::Int(100)],
            rel(0),
            add(cleaf(vec![V::Int(10), V::Int(1), V::Int(500)])),
        )]
    );

    // Differential: the hand-wired build closure produces the identical result.
    let closure_r = run_push_test(
        vec![
            SourceSpec::new(issues_schema(), vec![vec![V::Int(1), V::Int(100)]]),
            SourceSpec::new(comments_schema(), vec![]),
        ],
        build_issues_join_comments,
        vec![(
            1,
            SourceChange::Add(row(vec![V::Int(10), V::Int(1), V::Int(500)])),
        )],
        false,
    );
    assert_eq!(ast_r.initial, closure_r.initial);
    assert_eq!(ast_r.pushes, closure_r.pushes);
}

#[test]
fn run_fetch_test_ast_materializes_the_same_tree() {
    let issue_rows = vec![vec![V::Int(1), V::Int(100)], vec![V::Int(2), V::Int(200)]];
    let comment_rows = vec![
        vec![V::Int(10), V::Int(1), V::Int(500)],
        vec![V::Int(11), V::Int(1), V::Int(501)],
        vec![V::Int(20), V::Int(2), V::Int(900)],
    ];

    let ast_tree = run_fetch_test_ast(
        vec![
            TableSpec::new("issue", issues_schema(), issue_rows.clone()),
            TableSpec::new("comment", comments_schema(), comment_rows.clone()),
        ],
        &issue_comments_ast(),
    )
    .unwrap();

    // Exact expected tree (same shape the hand-wired fetch test pins).
    assert_eq!(
        ast_tree,
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

    // And it's identical to the hand-wired build-closure fetch.
    let closure_tree = run_fetch_test(
        vec![
            SourceSpec::new(issues_schema(), issue_rows),
            SourceSpec::new(comments_schema(), comment_rows),
        ],
        build_issues_join_comments,
    );
    assert_eq!(ast_tree, closure_tree);
}

#[test]
fn run_push_test_ast_multiple_sibling_relationships() {
    // issue { comments, labels } via the fluent `Ast` → two stacked joins through
    // `build_pipeline`. A comment add routes Child(comments) through the labels
    // join's parent-port passthrough; a label add routes Child(labels) on the outer
    // join's own child port. End-to-end proof of the step-4 join chaining driven by
    // the builder + the AST testkit.
    let ast = table("issue")
        .sub_as("comments", |row| {
            table("comment").r#where("issue", row.col("id"))
        })
        .sub_as("labels", |row| {
            table("label").r#where("issue", row.col("id"))
        })
        .build();

    let r = run_push_test_ast(
        vec![
            TableSpec::new(
                "issue",
                issues_schema_2rel(),
                vec![vec![V::Int(1), V::Int(100)]],
            ),
            TableSpec::new("comment", comments_schema(), vec![]),
            TableSpec::new("label", labels_schema(), vec![]),
        ],
        &ast,
        vec![
            (
                "comment",
                SourceChange::Add(row(vec![V::Int(10), V::Int(1), V::Int(500)])),
            ),
            ("label", SourceChange::Add(row(vec![V::Int(20), V::Int(1)]))),
        ],
        false,
    )
    .unwrap();

    // hydrate: issue 1 with both relationships empty (comments slot 0, labels slot 1).
    assert_eq!(
        r.initial,
        vec![cnode(
            vec![V::Int(1), V::Int(100)],
            vec![(rel(0), vec![]), (rel(1), vec![])],
        )]
    );
    // the two child adds, each landing on its own relationship slot.
    assert_eq!(
        r.pushes,
        vec![
            child(
                vec![V::Int(1), V::Int(100)],
                rel(0),
                add(cleaf(vec![V::Int(10), V::Int(1), V::Int(500)])),
            ),
            child(
                vec![V::Int(1), V::Int(100)],
                rel(1),
                add(cleaf(vec![V::Int(20), V::Int(1)])),
            ),
        ]
    );
}

#[test]
fn run_push_test_ast_start_after_gates_pushes() {
    // `start_after id 2` lowers to a Skip over the connection: hydrate drops ids
    // <= 2, and each push is gated through it. No relationship → leaf nodes.
    let ast = table("issue").start_after("id", 2).build();
    let r = run_push_test_ast(
        vec![TableSpec::new(
            "issue",
            issues_schema(),
            vec![
                vec![V::Int(1), V::Int(100)],
                vec![V::Int(2), V::Int(200)],
                vec![V::Int(3), V::Int(300)],
            ],
        )],
        &ast,
        vec![
            (
                "issue",
                SourceChange::Add(row(vec![V::Int(0), V::Int(999)])),
            ), // dropped (id 0)
            (
                "issue",
                SourceChange::Add(row(vec![V::Int(5), V::Int(555)])),
            ), // kept (id 5)
        ],
        false,
    )
    .unwrap();

    assert_eq!(r.initial, vec![cleaf(vec![V::Int(3), V::Int(300)])]);
    assert_eq!(r.pushes, vec![add(cleaf(vec![V::Int(5), V::Int(555)]))]);
}

#[test]
fn run_push_test_ast_start_feeds_a_relationship_join() {
    // `start_after id 1` lowers to a Skip whose output feeds the comments join's
    // PARENT port (Port::JoinParent — the capability the OutEdge generalization
    // unlocked). Issue 1 is gated out, so it never appears; and a comment added for
    // it produces no Child change, because `push_child_change` re-fetches the parent
    // THROUGH the Skip (constraint id=1 + start after 1 → empty).
    let ast = table("issue")
        .start_after("id", 1)
        .sub_as("comments", |row| {
            table("comment").r#where("issue", row.col("id"))
        })
        .build();

    let r = run_push_test_ast(
        vec![
            TableSpec::new(
                "issue",
                issues_schema(),
                vec![
                    vec![V::Int(1), V::Int(100)],
                    vec![V::Int(2), V::Int(200)],
                    vec![V::Int(3), V::Int(300)],
                ],
            ),
            TableSpec::new(
                "comment",
                comments_schema(),
                vec![vec![V::Int(10), V::Int(2), V::Int(500)]],
            ),
        ],
        &ast,
        vec![
            // a comment for the surviving issue 3 → Child(comments) add.
            (
                "comment",
                SourceChange::Add(row(vec![V::Int(11), V::Int(3), V::Int(600)])),
            ),
            // a comment for the gated-out issue 1 → no parent through the Skip → nothing.
            (
                "comment",
                SourceChange::Add(row(vec![V::Int(12), V::Int(1), V::Int(700)])),
            ),
        ],
        false,
    )
    .unwrap();

    // hydrate: issues 2 (with comment 10) and 3 (no comments); issue 1 dropped.
    assert_eq!(
        r.initial,
        vec![
            cnode(
                vec![V::Int(2), V::Int(200)],
                vec![(
                    rel(0),
                    vec![cleaf(vec![V::Int(10), V::Int(2), V::Int(500)])]
                )],
            ),
            cnode(vec![V::Int(3), V::Int(300)], vec![(rel(0), vec![])]),
        ]
    );
    // only the comment for the surviving issue 3 fans up as a Child change.
    assert_eq!(
        r.pushes,
        vec![child(
            vec![V::Int(3), V::Int(300)],
            rel(0),
            add(cleaf(vec![V::Int(11), V::Int(3), V::Int(600)])),
        )]
    );
}

#[test]
fn run_push_test_ast_rejects_unsupported_ast() {
    // Flipped NOT EXISTS still needs an anti-join operator → typed BuildError,
    // surfaced by the runner so a test can assert the rejection.
    let mut ast = table("issue")
        .where_not_exists(|row| table("comment").r#where("issue", row.col("id")))
        .build();
    if let Some(Condition::CorrelatedSubquery(c)) = &mut ast.r#where {
        c.flip = Some(true);
    }
    let r = run_push_test_ast(
        vec![
            TableSpec::new("issue", issues_schema(), vec![]),
            TableSpec::new("comment", comments_schema(), vec![]),
        ],
        &ast,
        vec![],
        false,
    );
    assert!(matches!(r, Err(rindle::BuildError::Unsupported(_))));
}
