#![cfg(feature = "testkit")]

//! Filtering a **parent by a child aggregate** — `issue WHERE count(comments) > n`
//! (`PARENT-AGGREGATE-FILTER-DESIGN.md`). [`Query::having_count`](rindle::query::Query)
//! binds an existing [`count_as`](rindle::query::Query) relationship and pushes a hidden
//! `EXISTS` whose subquery carries the same `count` aggregate plus a `HAVING count <op> n`.
//! The builder lowers it (`apply_agg_exists_join`) to an **uncapped** child → lazy grouped
//! `reduce` → `HAVING` Filter → parent join slot, gated by the existing `Exists` operator;
//! the display `count_as` is untouched, so a survivor still shows its real count.
//!
//! These pin the lowering through the **production `View`**: hydrate, then push a child
//! add/remove that crosses (or doesn't cross) the threshold and watch the parent
//! appear / disappear / stay.

use rindle::testkit::{cleaf, cnode, rel, run_push_test_ast_view, CaughtNode, TableSpec};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{table, Ast, BuildError, SourceChange};

fn issues_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}
fn comments_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issue", "spam"], vec![0], vec![(0, true)])
}
fn cm(id: i64, issue: i64, spam: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue), V::Int(spam)]
}

/// issues 1..=4 (val 100·id) with comment counts 3, 2, 1, 0 — all non-spam.
fn sources() -> Vec<TableSpec> {
    vec![
        TableSpec::new(
            "issue",
            issues_schema(),
            vec![
                vec![V::Int(1), V::Int(100)],
                vec![V::Int(2), V::Int(200)],
                vec![V::Int(3), V::Int(300)],
                vec![V::Int(4), V::Int(400)],
            ],
        ),
        TableSpec::new(
            "comment",
            comments_schema(),
            vec![
                cm(10, 1, 0),
                cm(11, 1, 0),
                cm(12, 1, 0),
                cm(20, 2, 0),
                cm(21, 2, 0),
                cm(30, 3, 0),
            ],
        ),
    ]
}

/// `issue { commentCount: count(comments) } WHERE count(comments) <op> n`.
fn having_ast(op: &str, n: i64) -> Ast {
    table("issue")
        .count_as("commentCount", |r| {
            table("comment").r#where("issue", r.col("id"))
        })
        .having_count("commentCount", op, n)
        .build()
}

/// An issue node carrying its display `commentCount` aggregate (slot 0 = `[issue, count]`).
/// The gate `EXISTS` is out-of-view, so it never appears in the materialized tree.
fn issue_with_count(id: i64, val: i64, count: i64) -> CaughtNode {
    cnode(
        vec![V::Int(id), V::Int(val)],
        vec![(rel(0), vec![cleaf(vec![V::Int(id), V::Int(count)])])],
    )
}

// --- hydrate -----------------------------------------------------------------

#[test]
fn hydrate_keeps_only_high_count_parents() {
    // count > 1 → issues 1 (3) and 2 (2); issue 3 (1) and childless 4 (0) are dropped.
    // Survivors still show their real count (the display projection coexists with the gate).
    let r = run_push_test_ast_view(sources(), &having_ast(">", 1), vec![], false).unwrap();
    assert_eq!(
        r.initial,
        vec![issue_with_count(1, 100, 3), issue_with_count(2, 200, 2)]
    );
}

// --- incremental: crossing the threshold -------------------------------------

#[test]
fn add_crossing_threshold_makes_parent_appear() {
    // Issue 3 sits at count 1 (excluded by `> 1`). A new comment takes it to 2: the reduce
    // emits Edit{1→2}, the HAVING Filter edit-splits false→true into an Add, the Exists gate
    // goes 0→1, and issue 3 appears — showing its real count 2.
    let r = run_push_test_ast_view(
        sources(),
        &having_ast(">", 1),
        vec![("comment", SourceChange::Add(row(cm(31, 3, 0))))],
        false,
    )
    .unwrap();
    assert_eq!(
        r.data,
        vec![
            issue_with_count(1, 100, 3),
            issue_with_count(2, 200, 2),
            issue_with_count(3, 300, 2),
        ]
    );
}

#[test]
fn remove_crossing_threshold_makes_parent_disappear() {
    // Issue 2 (count 2) loses a comment → 1: HAVING true→false ⇒ Remove ⇒ Exists 1→0 ⇒ the
    // parent disappears. Issue 1 (still 3) remains.
    let r = run_push_test_ast_view(
        sources(),
        &having_ast(">", 1),
        vec![("comment", SourceChange::Remove(row(cm(21, 2, 0))))],
        false,
    )
    .unwrap();
    assert_eq!(r.data, vec![issue_with_count(1, 100, 3)]);
}

#[test]
fn same_side_edit_above_threshold_no_flicker() {
    // A comment added to issue 1 (already passing at 3) takes it to 4: the parent stays put
    // (no Remove+Add churn), only its displayed count edits 3→4. Issue 2 is untouched.
    let r = run_push_test_ast_view(
        sources(),
        &having_ast(">", 1),
        vec![("comment", SourceChange::Add(row(cm(13, 1, 0))))],
        false,
    )
    .unwrap();
    assert_eq!(
        r.data,
        vec![issue_with_count(1, 100, 4), issue_with_count(2, 200, 2)]
    );
}

// --- the count-0 caveat ------------------------------------------------------

#[test]
fn childless_parent_is_absent_even_for_gt_zero() {
    // `> 0` is the lowest high-pass predicate. A childless parent forms no group, so issue 4
    // (0 comments) is still dropped — issues 1·2·3 (counts ≥ 1) survive. This is the §5
    // count-0 limitation made concrete: `> 0` keeps only parents that *have* a child.
    let r = run_push_test_ast_view(sources(), &having_ast(">", 0), vec![], false).unwrap();
    assert_eq!(
        r.initial,
        vec![
            issue_with_count(1, 100, 3),
            issue_with_count(2, 200, 2),
            issue_with_count(3, 300, 1),
        ]
    );
}

// --- coexistence with a parent `where` ---------------------------------------

#[test]
fn coexists_with_a_parent_where() {
    // `issue WHERE val > 150 AND count(comments) > 1`: the parent `where` keeps issues 2·3·4,
    // then the child-count gate keeps only issue 2 (count 2 > 1); issue 3 (1) and 4 (0) fail.
    let ast = table("issue")
        .where_op("val", ">", 150)
        .count_as("commentCount", |r| {
            table("comment").r#where("issue", r.col("id"))
        })
        .having_count("commentCount", ">", 1)
        .build();
    let r = run_push_test_ast_view(sources(), &ast, vec![], false).unwrap();
    assert_eq!(r.data, vec![issue_with_count(2, 200, 2)]);
}

// --- coexistence with a child `where` (count of a *filtered* relationship) ----

#[test]
fn coexists_with_a_child_where() {
    // `count(comments WHERE spam = 0) > 1`. Issue 3 gets two **spam** comments (spam=1): they
    // inflate the raw row count but the child `where` excludes them from BOTH the display and
    // the gate (the gate clones the same child), so issue 3's non-spam count is 1 → dropped.
    let sources = vec![
        TableSpec::new(
            "issue",
            issues_schema(),
            vec![vec![V::Int(2), V::Int(200)], vec![V::Int(3), V::Int(300)]],
        ),
        TableSpec::new(
            "comment",
            comments_schema(),
            vec![
                cm(20, 2, 0),
                cm(21, 2, 0), // issue 2: 2 non-spam
                cm(30, 3, 0), // issue 3: 1 non-spam …
                cm(31, 3, 1),
                cm(32, 3, 1), // … + 2 spam (must not count)
            ],
        ),
    ];
    let ast = table("issue")
        .count_as("commentCount", |r| {
            table("comment")
                .r#where("issue", r.col("id"))
                .where_op("spam", "=", 0)
        })
        .having_count("commentCount", ">", 1)
        .build();
    let r = run_push_test_ast_view(sources, &ast, vec![], false).unwrap();
    // Only issue 2 (2 non-spam). Issue 3's display count is its non-spam count (1).
    assert_eq!(r.data, vec![issue_with_count(2, 200, 2)]);
}

// --- AST / serde round-trip --------------------------------------------------

#[test]
fn synthesized_exists_having_round_trips_through_serde() {
    let ast = having_ast(">", 1);
    let json = serde_json::to_string(&ast).expect("serialize");
    let back: Ast = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(
        ast, back,
        "the synthesized aggregate-EXISTS-having round-trips"
    );
    // It really is a `where` EXISTS over a count aggregate carrying a HAVING.
    assert!(json.contains("correlatedSubquery"));
    assert!(json.contains("\"having\""));
    assert!(json.contains("\"aggregate\":\"count\""));
}

// --- v1 guard: count-0-satisfying (low-pass) predicates are rejected ---------

fn build_err(ast: &Ast) -> BuildError {
    run_push_test_ast_view(sources(), ast, vec![], false)
        .expect_err("a count-0-satisfying predicate must be rejected at build")
}

#[test]
fn low_pass_predicates_are_rejected() {
    // Each is *true at count 0*, so a childless parent (no group) can't be seen: deferred.
    for (op, n) in [("<=", 2), ("<", 1), (">=", 0)] {
        assert!(
            matches!(build_err(&having_ast(op, n)), BuildError::Unsupported(_)),
            "`count {op} {n}` is low-pass and must be Unsupported"
        );
    }
}

#[test]
fn equal_zero_is_rejected() {
    assert!(matches!(
        build_err(&having_ast("=", 0)),
        BuildError::Unsupported(_)
    ));
}

#[test]
fn high_pass_equality_is_accepted() {
    // `= 2` is false at 0 (high-pass): keep parents with exactly 2 comments — only issue 2.
    let r = run_push_test_ast_view(sources(), &having_ast("=", 2), vec![], false).unwrap();
    assert_eq!(r.data, vec![issue_with_count(2, 200, 2)]);
}

// --- Rust API guard: binding a non-existent aggregate alias panics -----------

#[test]
#[should_panic(expected = "no `count_as")]
fn having_count_on_unknown_alias_panics() {
    let _ = table("issue")
        .count_as("commentCount", |r| {
            table("comment").r#where("issue", r.col("id"))
        })
        .having_count("nope", ">", 1)
        .build();
}
