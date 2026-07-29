#![cfg(feature = "testkit")]

//! End-to-end tests for **nested** mixed flipped + non-flipped EXISTS — the shapes
//! where the flip and the non-flip are NOT both direct children of one top-level OR
//! (the last EXISTS-under-OR gap). Driven through `apply_where_with_flips`
//! (`applyFilterWithFlips`):
//!   - `x = 1 OR (EXISTS_flipped(comments) AND EXISTS(labels))` — an AND branch with a
//!     flip nested under the top OR (a `FlippedJoin` over the AND's `Exists(labels)` gate
//!     filter pipeline, the non-flipped `labels` Join on the spine);
//!   - `EXISTS_flipped(comments) AND (x = 300 OR EXISTS(labels))` — a flip AND-ed with a
//!     non-flipped subquery-under-OR (the without-flip filter pipeline rides above the
//!     `FlippedJoin`);
//!   - `(EXISTS_flipped(comments) OR EXISTS(labels)) AND x >= 200` — a flipped-OR AND-ed
//!     with a leaf (a nested union fan above a leaf filter);
//!   - `x = 1 OR (y >= 10 AND (EXISTS_flipped(comments) OR EXISTS(labels)))` — a nested
//!     union fan *inside* an outer union branch.
//!
//! All non-flipped EXISTS Joins now sit on the SPINE (the JS `applyFilterWithFlips`
//! layout); the `Exists` gates in the union/AND branches count them through the
//! rel-preserving broadcast. Plain `EXISTS_flipped(comments) AND EXISTS(labels)` is
//! covered too. As in `exists_or_mixed.rs`, non-flipped-EXISTS PUSH paths are strictly
//! more correct than the (push-lossy) JS — see that file's header + the f-case.

use rindle::change::SourceChange;
use rindle::testkit::{
    add, cleaf, cnode, rel, remove, run_fetch_test_ast, run_push_test_ast, CaughtChange,
    CaughtNode, TableSpec,
};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    Lit, Op, OrderPart, SimpleCondition, ValuePosition,
};

// issue(id, x). Production-shaped real relationship names; the query-local slot tree
// puts comments=slot 0 (flipped EXISTS), labels=slot 1 (non-flipped EXISTS).
// comment(id, issueID); label(id, issueID).
fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
fn child_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}
fn ir(id: i64, x: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(x)]
}
fn cr(id: i64, issue: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue)]
}
fn lr(id: i64, issue: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue)]
}
fn sources(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>, labels: Vec<Vec<V>>) -> Vec<TableSpec> {
    vec![
        TableSpec::new("issue", issue_schema(), issues),
        TableSpec::new("comment", child_schema(), comments),
        TableSpec::new("label", child_schema(), labels),
    ]
}
fn issue_schema_related_label() -> SourceSchema {
    // Slots are query-derived (see `g_flip_or_with_related_lowers`); the source
    // declares no relationships.
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
fn sources_related_label(
    issues: Vec<Vec<V>>,
    comments: Vec<Vec<V>>,
    labels: Vec<Vec<V>>,
) -> Vec<TableSpec> {
    vec![
        TableSpec::new("issue", issue_schema_related_label(), issues),
        TableSpec::new("comment", child_schema(), comments),
        TableSpec::new("label", child_schema(), labels),
    ]
}
// issue carrying ONLY its comments (slot 0).
fn with_comments(id: i64, x: i64, comments: Vec<Vec<V>>) -> CaughtNode {
    cnode(
        ir(id, x),
        vec![(rel(0), comments.into_iter().map(cleaf).collect())],
    )
}
// issue carrying ONLY its labels (slot 1).
fn with_labels(id: i64, x: i64, labels: Vec<Vec<V>>) -> CaughtNode {
    cnode(
        ir(id, x),
        vec![(rel(1), labels.into_iter().map(cleaf).collect())],
    )
}
// issue carrying BOTH slots (comments slot 0, labels slot 1).
fn with_both(id: i64, x: i64, comments: Vec<Vec<V>>, labels: Vec<Vec<V>>) -> CaughtNode {
    cnode(
        ir(id, x),
        vec![
            (rel(0), comments.into_iter().map(cleaf).collect()),
            (rel(1), labels.into_iter().map(cleaf).collect()),
        ],
    )
}

fn x_eq(n: i64) -> Condition {
    Condition::Simple(SimpleCondition {
        op: Op::Eq,
        left: ValuePosition::Column { name: "x".into() },
        right: ValuePosition::Literal {
            value: Lit::Number(n as f64),
        },
    })
}
fn x_ge(n: i64) -> Condition {
    Condition::Simple(SimpleCondition {
        op: Op::Ge,
        left: ValuePosition::Column { name: "x".into() },
        right: ValuePosition::Literal {
            value: Lit::Number(n as f64),
        },
    })
}

/// An EXISTS condition over `alias`/`table` correlated on issue.id == child.issueID.
fn exists(alias: &str, table: &str, flip: bool) -> Condition {
    Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                table: table.into(),
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
fn comments() -> Condition {
    exists("comments", "comment", true)
}
fn labels() -> Condition {
    exists("labels", "label", false)
}

fn issue_ast(where_: Condition) -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        r#where: Some(where_),
        ..Default::default()
    }
}

// ===========================================================================
// A. `x = 1 OR (EXISTS_flipped(comments) AND EXISTS(labels))`
// ===========================================================================

fn a_ast() -> Ast {
    issue_ast(Condition::Or {
        conditions: vec![
            x_eq(1),
            Condition::And {
                conditions: vec![comments(), labels()],
            },
        ],
    })
}

#[test]
fn a_fetch_leaf_and_nested_and_branch() {
    // issue 1 (x=1) via the leaf; issue 2 (comment + label) via the AND branch;
    // issue 3 (comment only) and issue 4 (label only) fail the AND → dropped.
    let tree = run_fetch_test_ast(
        sources(
            vec![ir(1, 1), ir(2, 5), ir(3, 5), ir(4, 5)],
            vec![cr(10, 2), cr(11, 3)],
            vec![lr(20, 2), lr(21, 4)],
        ),
        &a_ast(),
    )
    .unwrap();
    // JS parity (`a.fetch` oracle): the non-flipped `labels` Join is on the spine, so issue 1
    // (kept via the leaf) carries an empty `labels` slot.
    assert_eq!(
        tree,
        vec![
            with_labels(1, 1, vec![]),
            with_both(2, 5, vec![cr(10, 2)], vec![lr(20, 2)]),
        ]
    );
}

#[test]
fn a_child_label_add_completes_the_and_branch() {
    // issue 3 (x=5, has a comment, no label) is absent (AND needs both). Adding its first
    // LABEL completes the AND; no other branch keeps it → Add carrying both rels.
    let res = run_push_test_ast(
        sources(vec![ir(1, 1), ir(3, 5)], vec![cr(30, 3)], vec![]),
        &a_ast(),
        vec![("label", SourceChange::Add(row(lr(31, 3))))],
        false,
    )
    .unwrap();
    assert_eq!(res.initial, vec![with_labels(1, 1, vec![])]);
    assert_eq!(
        res.pushes,
        vec![add(with_both(3, 5, vec![cr(30, 3)], vec![lr(31, 3)]))]
    );
}

#[test]
fn a_child_completing_and_on_a_leaf_row_is_suppressed() {
    // issue 1 (x=1) is kept by the leaf and has a label. Adding a COMMENT completes the
    // AND branch too, but the leaf still keeps issue 1 (cross-branch count ≥ 2) → suppress.
    let res = run_push_test_ast(
        sources(vec![ir(1, 1)], vec![], vec![lr(20, 1)]),
        &a_ast(),
        vec![("comment", SourceChange::Add(row(cr(11, 1))))],
        false,
    )
    .unwrap();
    assert_eq!(res.pushes, Vec::<CaughtChange>::new());
}

#[test]
fn a_child_label_remove_breaks_the_and_branch() {
    // issue 2 (x=5) is in ONLY via the AND branch (comment + label). Removing its last
    // LABEL breaks the AND; no branch keeps it → Remove.
    let res = run_push_test_ast(
        sources(vec![ir(1, 1), ir(2, 5)], vec![cr(10, 2)], vec![lr(20, 2)]),
        &a_ast(),
        vec![("label", SourceChange::Remove(row(lr(20, 2))))],
        false,
    )
    .unwrap();
    assert_eq!(
        res.pushes,
        vec![remove(with_both(2, 5, vec![cr(10, 2)], vec![lr(20, 2)]))]
    );
}

// ===========================================================================
// B. `EXISTS_flipped(comments) AND (x = 300 OR EXISTS(labels))`
// ===========================================================================

fn b_ast() -> Ast {
    issue_ast(Condition::And {
        conditions: vec![
            comments(),
            Condition::Or {
                conditions: vec![x_eq(300), labels()],
            },
        ],
    })
}

#[test]
fn b_fetch_flip_anded_with_subquery_or() {
    // Needs a comment AND (x=300 OR a label).
    // issue 1 (x=300, comment, no label) → comment AND x=300 → in.
    // issue 2 (x=5, comment, label) → comment AND label → in.
    // issue 3 (x=5, comment, no label) → fails the inner OR → dropped.
    // issue 4 (x=300, no comment) → fails the flip → dropped.
    let tree = run_fetch_test_ast(
        sources(
            vec![ir(1, 300), ir(2, 5), ir(3, 5), ir(4, 300)],
            vec![cr(10, 1), cr(11, 2), cr(12, 3)],
            vec![lr(20, 2)],
        ),
        &b_ast(),
    )
    .unwrap();
    assert_eq!(
        tree,
        vec![
            with_both(1, 300, vec![cr(10, 1)], vec![]),
            with_both(2, 5, vec![cr(11, 2)], vec![lr(20, 2)]),
        ]
    );
}

#[test]
fn b_child_label_add_satisfies_the_inner_or() {
    // issue 3 (x=5, comment, no label) is absent (inner OR fails). Adding a LABEL satisfies
    // the inner OR → Add.
    let res = run_push_test_ast(
        sources(vec![ir(3, 5)], vec![cr(12, 3)], vec![]),
        &b_ast(),
        vec![("label", SourceChange::Add(row(lr(30, 3))))],
        false,
    )
    .unwrap();
    assert_eq!(res.initial, Vec::<CaughtNode>::new());
    assert_eq!(
        res.pushes,
        vec![add(with_both(3, 5, vec![cr(12, 3)], vec![lr(30, 3)]))]
    );
}

#[test]
fn b_child_comment_remove_breaks_the_flip() {
    // issue 2 (comment + label, x=5) is in. Removing its only COMMENT breaks the outer
    // flipped AND → Remove.
    let res = run_push_test_ast(
        sources(vec![ir(2, 5)], vec![cr(11, 2)], vec![lr(20, 2)]),
        &b_ast(),
        vec![("comment", SourceChange::Remove(row(cr(11, 2))))],
        false,
    )
    .unwrap();
    assert_eq!(
        res.pushes,
        vec![remove(with_both(2, 5, vec![cr(11, 2)], vec![lr(20, 2)]))]
    );
}

// ===========================================================================
// C. `(EXISTS_flipped(comments) OR EXISTS(labels)) AND x >= 200`
// ===========================================================================

fn c_ast() -> Ast {
    issue_ast(Condition::And {
        conditions: vec![
            Condition::Or {
                conditions: vec![comments(), labels()],
            },
            x_ge(200),
        ],
    })
}

#[test]
fn c_fetch_flipped_or_anded_with_leaf() {
    // Needs (comment OR label) AND x>=200.
    // issue 1 (x=200, comment) → in via comments.
    // issue 2 (x=300, label) → in via labels.
    // issue 3 (x=100, comment) → fails x>=200 → dropped.
    // issue 4 (x=500, nothing) → fails the OR → dropped.
    let tree = run_fetch_test_ast(
        sources(
            vec![ir(1, 200), ir(2, 300), ir(3, 100), ir(4, 500)],
            vec![cr(10, 1), cr(12, 3)],
            vec![lr(20, 2)],
        ),
        &c_ast(),
    )
    .unwrap();
    // JS parity (`c.fetch` oracle): issue 1 (kept via `comments`) also carries the spine's
    // empty `labels` slot.
    assert_eq!(
        tree,
        vec![
            with_both(1, 200, vec![cr(10, 1)], vec![]),
            with_labels(2, 300, vec![lr(20, 2)]),
        ]
    );
}

#[test]
fn c_child_comment_add_under_the_leaf_gate_flips_on() {
    // issue 5 (x=400, nothing) fails the OR. Adding a COMMENT flips the comments branch on;
    // x>=200 holds → Add.
    let res = run_push_test_ast(
        sources(vec![ir(5, 400)], vec![], vec![]),
        &c_ast(),
        vec![("comment", SourceChange::Add(row(cr(50, 5))))],
        false,
    )
    .unwrap();
    // JS parity (`c.comment_add` oracle): Add carries `comments` + the spine's empty `labels`.
    assert_eq!(
        res.pushes,
        vec![add(with_both(5, 400, vec![cr(50, 5)], vec![]))]
    );
}

#[test]
fn c_child_add_below_the_leaf_gate_is_filtered_out() {
    // issue 6 (x=100) fails x>=200. Adding a comment flips the OR, but the leaf gate above
    // the union still drops it → no output.
    let res = run_push_test_ast(
        sources(vec![ir(6, 100)], vec![], vec![]),
        &c_ast(),
        vec![("comment", SourceChange::Add(row(cr(60, 6))))],
        false,
    )
    .unwrap();
    assert_eq!(res.pushes, Vec::<CaughtChange>::new());
}

#[test]
fn c_child_comment_remove_suppressed_when_label_branch_keeps_it() {
    // issue 7 (x=300) has a comment AND a label → in via both union branches. Removing the
    // comment flips the comments branch off, but the labels branch still keeps it → suppress.
    let res = run_push_test_ast(
        sources(vec![ir(7, 300)], vec![cr(70, 7)], vec![lr(71, 7)]),
        &c_ast(),
        vec![("comment", SourceChange::Remove(row(cr(70, 7))))],
        false,
    )
    .unwrap();
    assert_eq!(res.pushes, Vec::<CaughtChange>::new());
}

// ===========================================================================
// D. Plain `EXISTS_flipped(comments) AND EXISTS(labels)` (no OR) — locks the
//    routing through the recursion.
// ===========================================================================

fn d_ast() -> Ast {
    issue_ast(Condition::And {
        conditions: vec![comments(), labels()],
    })
}

#[test]
fn d_fetch_plain_flipped_and_nonflipped_and() {
    let tree = run_fetch_test_ast(
        sources(
            vec![ir(1, 5), ir(2, 5), ir(3, 5)],
            vec![cr(10, 1), cr(11, 2)],
            vec![lr(20, 1), lr(21, 3)],
        ),
        &d_ast(),
    )
    .unwrap();
    // Only issue 1 has BOTH a comment and a label.
    assert_eq!(
        tree,
        vec![with_both(1, 5, vec![cr(10, 1)], vec![lr(20, 1)])]
    );
}

#[test]
fn d_child_label_add_completes_and() {
    // issue 2 (comment, no label) absent. Add a label → in.
    let res = run_push_test_ast(
        sources(vec![ir(2, 5)], vec![cr(11, 2)], vec![]),
        &d_ast(),
        vec![("label", SourceChange::Add(row(lr(22, 2))))],
        false,
    )
    .unwrap();
    assert_eq!(res.initial, Vec::<CaughtNode>::new());
    assert_eq!(
        res.pushes,
        vec![add(with_both(2, 5, vec![cr(11, 2)], vec![lr(22, 2)]))]
    );
}

// PROBE: the COMMENT-side push (the FlippedJoin's CHILD port at the BOTTOM of the
// Rust stack). issue with a label, no comment → add a comment. The flipped existence
// flips on at the bottom, the Add flows UP through localJoin(labels) (which re-attaches
// labels) → must carry BOTH comments and labels.
#[test]
fn probe_d_child_comment_add_completes_and() {
    let res = run_push_test_ast(
        sources(vec![ir(2, 5)], vec![], vec![lr(22, 2)]),
        &d_ast(),
        vec![("comment", SourceChange::Add(row(cr(11, 2))))],
        false,
    )
    .unwrap();
    assert_eq!(res.initial, Vec::<CaughtNode>::new());
    assert_eq!(
        res.pushes,
        vec![add(with_both(2, 5, vec![cr(11, 2)], vec![lr(22, 2)]))]
    );
}

// PROBE: comment add to a NO-label issue → must stay dropped (the localJoin(labels)
// emits Add(issue+comments) up into Exists(labels) which gates it out — size 0).
#[test]
fn probe_d_child_comment_add_no_label_dropped() {
    let res = run_push_test_ast(
        sources(vec![ir(2, 5)], vec![], vec![]),
        &d_ast(),
        vec![("comment", SourceChange::Add(row(cr(11, 2))))],
        false,
    )
    .unwrap();
    assert_eq!(res.pushes, Vec::<CaughtChange>::new());
}

// PROBE: comment REMOVE that breaks the flip while a label is present. issue has one
// comment + one label → in. Remove the comment → flipped existence off → Remove,
// carrying both rels.
#[test]
fn probe_d_child_comment_remove_breaks_and() {
    let res = run_push_test_ast(
        sources(vec![ir(2, 5)], vec![cr(11, 2)], vec![lr(22, 2)]),
        &d_ast(),
        vec![("comment", SourceChange::Remove(row(cr(11, 2))))],
        false,
    )
    .unwrap();
    assert_eq!(
        res.pushes,
        vec![remove(with_both(2, 5, vec![cr(11, 2)], vec![lr(22, 2)]))]
    );
}

// PROBE: two issues, one commented (3) one not (4); add a label to EACH. Only the
// commented one (3) should surface (the FlippedJoin inner-join gate drops 4's label).
#[test]
fn probe_d_two_label_adds_only_commented_surfaces() {
    let res = run_push_test_ast(
        sources(vec![ir(3, 5), ir(4, 5)], vec![cr(30, 3)], vec![]),
        &d_ast(),
        vec![
            ("label", SourceChange::Add(row(lr(33, 3)))),
            ("label", SourceChange::Add(row(lr(44, 4)))),
        ],
        false,
    )
    .unwrap();
    assert_eq!(
        res.pushes,
        vec![add(with_both(3, 5, vec![cr(30, 3)], vec![lr(33, 3)]))]
    );
}

// PROBE: a SECOND comment add (issue already has a comment + label, so already IN).
// The flipped existence does NOT flip (size 2) → FlippedJoin emits a Child for the
// comments rel, which flows up through localJoin(labels)+Exists(labels) as a
// passthrough Child (different rel). The view already has the row; a comments-rel
// Child is observable in Catch.
#[test]
fn probe_d_second_comment_add_is_child() {
    let res = run_push_test_ast(
        sources(vec![ir(2, 5)], vec![cr(11, 2)], vec![lr(22, 2)]),
        &d_ast(),
        vec![("comment", SourceChange::Add(row(cr(12, 2))))],
        false,
    )
    .unwrap();
    // Not an Add/Remove (membership unchanged); the comments-rel child propagates.
    // Assert it is NOT an Add or Remove of the parent.
    for ch in &res.pushes {
        assert!(
            !matches!(ch, CaughtChange::Add(_) | CaughtChange::Remove(_)),
            "second comment add must not flip parent membership, got {ch:?}"
        );
    }
}

// ===========================================================================
// E. `x = 1 OR (y >= 10 AND (EXISTS_flipped(comments) OR EXISTS(labels)))` —
//    a nested union fan INSIDE an outer union branch.
// ===========================================================================

fn e_ast() -> Ast {
    // reuse `x` as both the leaf selector (=1) and the inner AND leaf (>=10)
    issue_ast(Condition::Or {
        conditions: vec![
            x_eq(1),
            Condition::And {
                conditions: vec![
                    x_ge(10),
                    Condition::Or {
                        conditions: vec![comments(), labels()],
                    },
                ],
            },
        ],
    })
}

#[test]
fn e_fetch_nested_union_fan_in_outer_branch() {
    // issue 1 (x=1) via the leaf.
    // issue 2 (x=20, comment) via the inner (x>=10 AND (comment OR label)).
    // issue 3 (x=20, label) via the inner OR's labels branch.
    // issue 4 (x=5, comment) fails x>=10 and x=1 → dropped.
    let tree = run_fetch_test_ast(
        sources(
            vec![ir(1, 1), ir(2, 20), ir(3, 20), ir(4, 5)],
            vec![cr(10, 2), cr(12, 4)],
            vec![lr(20, 3)],
        ),
        &e_ast(),
    )
    .unwrap();
    // JS parity (`e.fetch` oracle): the spine `labels` Join attaches an (empty) `labels`
    // slot to the leaf-matched issue 1 and the comments-matched issue 2.
    assert_eq!(
        tree,
        vec![
            with_labels(1, 1, vec![]),
            with_both(2, 20, vec![cr(10, 2)], vec![]),
            with_labels(3, 20, vec![lr(20, 3)]),
        ]
    );
}

#[test]
fn e_child_comment_add_through_nested_fan() {
    // issue 5 (x=50, nothing) fails. Adding a comment flips the inner OR's comments branch
    // on; x>=10 holds → the inner union forwards, the outer union forwards (no other branch
    // keeps it) → Add.
    let res = run_push_test_ast(
        sources(vec![ir(5, 50)], vec![], vec![]),
        &e_ast(),
        vec![("comment", SourceChange::Add(row(cr(50, 5))))],
        false,
    )
    .unwrap();
    // JS parity (`e.comment_add` oracle): Add carries `comments` + the spine's empty `labels`.
    assert_eq!(
        res.pushes,
        vec![add(with_both(5, 50, vec![cr(50, 5)], vec![]))]
    );
}

#[test]
fn e_inner_and_gate_blocks_below_threshold() {
    // issue 6 (x=5) fails both the outer leaf (x=1) and the inner AND gate (x>=10). Adding a
    // comment flips the inner OR's comments branch, but the inner AND's x>=10 leaf — which
    // sits BELOW the inner union fan — drops it, so nothing reaches either fan-in.
    let res = run_push_test_ast(
        sources(vec![ir(6, 5)], vec![], vec![]),
        &e_ast(),
        vec![("comment", SourceChange::Add(row(cr(60, 6))))],
        false,
    )
    .unwrap();
    assert_eq!(res.pushes, Vec::<CaughtChange>::new());
}

#[test]
fn e_nested_cross_exists_suppression_then_remove() {
    // issue 8 (x=20) has a comment AND a label → kept by BOTH branches of the INNER union
    // fan. Removing the comment flips the inner comments branch off, but the inner labels
    // branch still keeps it → the INNER fan-in suppresses (nested mode-2, count == 1). Then
    // removing the label too leaves no branch → the inner fan-in forwards Remove, which the
    // outer fan-in also forwards (the outer leaf x=1 does not keep x=20) → one Remove.
    let res = run_push_test_ast(
        sources(vec![ir(8, 20)], vec![cr(80, 8)], vec![lr(81, 8)]),
        &e_ast(),
        vec![
            ("comment", SourceChange::Remove(row(cr(80, 8)))),
            ("label", SourceChange::Remove(row(lr(81, 8)))),
        ],
        false,
    )
    .unwrap();
    // Initial: issue 8 via the inner fan. The inner OR's first branch is now the combined
    // `withoutFlipped` (= `labels`), so the merge keeps `labels` (JS parity).
    assert_eq!(res.initial, vec![with_labels(8, 20, vec![lr(81, 8)])]);
    // First remove (comment) suppressed by the labels branch; second (label) forwards the
    // Remove, carrying the labels branch's last copy + the spine's empty `comments` slot.
    // DIVERGENCE FROM JS (overlay-model): the JS emits `[]` (the non-flipped `labels`
    // remove is push-lossy in JS); the Rust correctly forwards the Remove. See the f-case
    // and `nested-exists-or-decision` memory.
    assert_eq!(
        res.pushes,
        vec![remove(with_both(8, 20, vec![], vec![lr(81, 8)]))]
    );
}

// ===========================================================================
// F. `EXISTS(tags) AND (EXISTS_flipped(comments) OR EXISTS(labels))` — the (former)
//    case-F scope boundary, NOW LOWERED. A non-flipped EXISTS at AND level (`tags`)
//    AND-ed with a flipped subquery under a sibling OR. The AND-level `tags` Join now
//    sits on the SPINE, and the rel-preserving union broadcast (Gap A) carries it down
//    to the branches, so it is no longer stripped on push — the case-F bug is dissolved
//    and the build/fetch/push all match JS (`f.fetch` / `f.tag_add` oracle).
// ===========================================================================

fn issue_schema3() -> SourceSchema {
    // The builder derives the query-local slot tree from the AST (no synthesized `_N`
    // gate slots to pre-declare), so the source declares no relationships.
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
fn f_ast() -> Ast {
    issue_ast(Condition::And {
        conditions: vec![
            exists("tags", "tag", false),
            Condition::Or {
                conditions: vec![exists("comments", "comment", true), labels()],
            },
        ],
    })
}

// issue 1 carrying all three slots. The slot layout is QUERY-LOCAL: gating EXISTS in
// `where`-tree pre-order. For `AND[ EXISTS(tags), OR[ flip-EXISTS(comments), EXISTS(labels) ] ]`
// that pre-order is [tags (the first AND child), comments, labels] → tags=0, comments=1,
// labels=2 (params stay (comments, labels, tags); only the slot each lands on changes).
fn with_three(
    id: i64,
    x: i64,
    comments: Vec<Vec<V>>,
    labels: Vec<Vec<V>>,
    tags: Vec<Vec<V>>,
) -> CaughtNode {
    cnode(
        ir(id, x),
        vec![
            (rel(0), tags.into_iter().map(cleaf).collect()),
            (rel(1), comments.into_iter().map(cleaf).collect()),
            (rel(2), labels.into_iter().map(cleaf).collect()),
        ],
    )
}

fn f_sources(
    issues: Vec<Vec<V>>,
    comments: Vec<Vec<V>>,
    labels: Vec<Vec<V>>,
    tags: Vec<Vec<V>>,
) -> Vec<TableSpec> {
    vec![
        TableSpec::new("issue", issue_schema3(), issues),
        TableSpec::new("comment", child_schema(), comments),
        TableSpec::new("label", child_schema(), labels),
        TableSpec::new("tag", child_schema(), tags),
    ]
}

#[test]
fn f_and_level_exists_with_flipped_or_now_lowers() {
    // `EXISTS(tags) AND (EXISTS_flipped(comments) OR EXISTS(labels))`. The case-F guard is
    // GONE: the AND-level non-flipped `tags` Join is on the SPINE and the rel-preserving
    // union broadcast keeps it, so it is no longer stripped (the divergence the guard
    // avoided is dissolved). issue 1 has a tag (✓ tags) and a comment (✓ the OR) → matches.
    // JS parity (`f.fetch` oracle): carries `tags` + `comments` + an empty `labels` slot.
    let tree = run_fetch_test_ast(
        f_sources(vec![ir(1, 5)], vec![cr(60, 1)], vec![], vec![cr(80, 1)]),
        &f_ast(),
    )
    .unwrap();
    assert_eq!(
        tree,
        vec![with_three(1, 5, vec![cr(60, 1)], vec![], vec![cr(80, 1)])]
    );
}

#[test]
fn f_tag_add_keeps_the_and_level_relationship() {
    // The case-F PUSH scenario: issue 1 (a comment, no tag) is absent (fails EXISTS(tags)).
    // Adding its first TAG completes the AND. JS parity (`f.tag_add` oracle): Add carrying
    // ALL THREE slots — crucially the AND-level `tags` is NOT stripped (the original case-F
    // bug). The `labels` slot is empty-filled.
    let res = run_push_test_ast(
        f_sources(vec![ir(1, 5)], vec![cr(60, 1)], vec![], vec![]),
        &f_ast(),
        vec![("tag", SourceChange::Add(row(cr(80, 1))))],
        false,
    )
    .unwrap();
    assert_eq!(res.initial, Vec::<CaughtNode>::new());
    assert_eq!(
        res.pushes,
        vec![add(with_three(
            1,
            5,
            vec![cr(60, 1)],
            vec![],
            vec![cr(80, 1)]
        ))]
    );
}

// ===========================================================================
// G. A flipped `where` composed with a root `limit` / `related` — the lowering
//    tail is a UnionFanIn (not port-aware). `limit` now lowers through `Take`
//    over the fan (the JS `useCap`=false / ordered-Take fallback); `related`
//    over such a tail is rejected (the port-carrying filter-tail output is
//    deferred) rather than panicking.
// ===========================================================================

fn flip_or_ast() -> Ast {
    issue_ast(Condition::Or {
        conditions: vec![x_eq(1), comments()],
    })
}

#[test]
fn g_flip_or_with_root_limit_lowers_through_take() {
    // `(x=1 OR EXISTS_flipped(comments)) ORDER BY id LIMIT 2`. Matches: 1,3 (x=1) and
    // 2,4 (comment). Ordered by id, LIMIT 2 keeps issues 1 and 2.
    let mut ast = flip_or_ast();
    ast.limit = Some(2);
    let tree = run_fetch_test_ast(
        sources(
            vec![ir(1, 1), ir(2, 5), ir(3, 1), ir(4, 5)],
            vec![cr(10, 2), cr(11, 4)],
            vec![],
        ),
        &ast,
    )
    .unwrap();
    assert_eq!(
        tree,
        vec![cleaf(ir(1, 1)), with_comments(2, 5, vec![cr(10, 2)])]
    );
}

#[test]
fn g_flip_or_with_root_limit_push_respects_take_boundary() {
    // Same shape, LIMIT 2. Adding issue 0 (x=1) pushes issue 2 out of the LIMIT-2 window
    // (Take emits Add(issue0) + Remove(issue2)). Confirms the Take sits over the fan and
    // pushes correctly (no panic).
    let mut ast = flip_or_ast();
    ast.limit = Some(2);
    let res = run_push_test_ast(
        sources(vec![ir(1, 1), ir(2, 5)], vec![cr(10, 2)], vec![]),
        &ast,
        vec![("issue", SourceChange::Add(row(ir(0, 1))))],
        false,
    )
    .unwrap();
    assert_eq!(
        res.initial,
        vec![cleaf(ir(1, 1)), with_comments(2, 5, vec![cr(10, 2)])]
    );
    // Take bumps issue 2 out of the LIMIT-2 window for the new issue 0 (id 0 sorts first):
    // Remove(issue 2) then Add(issue 0). The union collapse empty-fills the `comments` slot
    // on the Add (issue 0 entered via the leaf branch, carrying no relationship). This frame
    // references only `comments` (flip_or_ast has no `labels`), so the slot tree has exactly
    // one slot — the phantom `labels` slot the over-declared schema used to add is gone.
    assert_eq!(
        res.pushes,
        vec![
            remove(with_comments(2, 5, vec![cr(10, 2)])),
            add(with_comments(0, 1, vec![])),
        ]
    );
}

#[test]
fn g_flip_or_with_related_lowers() {
    // `related(labels)` over a flipped-OR `where` — the UnionFanIn tail now feeds the
    // relationship join's `JoinParent` port (Gap B), so this lowers end-to-end. Matches
    // the JS Catch oracle (`CATCH g.fetch.related`): issue 1 (x=1) carries only `labels`
    // (the leaf branch attaches no comments); issue 2 (has a comment) carries BOTH its
    // flipped-branch `comments` AND the related `labels`.
    let mut ast = flip_or_ast();
    ast.related = vec![CorrelatedSubquery {
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
    }];
    let tree = run_fetch_test_ast(
        sources_related_label(
            vec![ir(1, 1), ir(2, 5)],
            vec![cr(10, 2)],
            vec![lr(50, 1), lr(51, 2)],
        ),
        &ast,
    )
    .unwrap();
    // Query-local slot order is materialized-`related`-FIRST then EXISTS gating, so for
    // `related(labels)` + `where EXISTS_flipped(comments)`: labels=slot 0, comments=slot 1
    // (the shared `with_*` helpers assume the pure-gating comments=0/labels=1 layout, so
    // this mixed frame builds its expected tree inline).
    assert_eq!(
        tree,
        vec![
            // issue 1 (x=1, leaf branch): only its materialized `labels`.
            cnode(ir(1, 1), vec![(rel(0), vec![cleaf(lr(50, 1))])]),
            // issue 2 (has a comment): materialized `labels` (slot 0) + flipped `comments` (slot 1).
            cnode(
                ir(2, 5),
                vec![
                    (rel(0), vec![cleaf(lr(51, 2))]),
                    (rel(1), vec![cleaf(cr(10, 2))]),
                ],
            ),
        ]
    );
}

#[test]
fn g_exists_child_with_flipped_subquery_where_lowers() {
    // A non-flipped EXISTS whose SUBQUERY's own `where` carries a flip:
    // `issue WHERE EXISTS(labels WHERE EXISTS_flipped(stars))`. The labels EXISTS child is
    // forced to LIMIT=EXISTS_LIMIT; because its `where` has a flip its tail is a fan, so it
    // lowers through an ordered `Take` (useCap=false) instead of a `Cap` — previously this
    // panicked. issue 1 has a label that has a star → kept; issue 2's label has no star →
    // dropped.
    let issue_s = SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)]);
    let label_s = SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)]);
    let star_s = SourceSchema::new(vec!["id", "labelID"], vec![0], vec![(0, true)]);
    let star_exists = Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["labelID".into()],
            },
            subquery: Box::new(Ast {
                table: "star".into(),
                alias: Some("stars".into()),
                order_by: vec![OrderPart("id".into(), Dir::Asc)],
                ..Default::default()
            }),
            system: None,
        },
        op: ExistsOp::Exists,
        flip: Some(true),
        scalar: None,
        plan_id: None,
    });
    let ast = issue_ast(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                table: "label".into(),
                alias: Some("labels".into()),
                order_by: vec![OrderPart("id".into(), Dir::Asc)],
                r#where: Some(star_exists),
                ..Default::default()
            }),
            system: None,
        },
        op: ExistsOp::Exists,
        flip: None,
        scalar: None,
        plan_id: None,
    }));
    let srcs = vec![
        TableSpec::new("issue", issue_s, vec![ir(1, 5), ir(2, 5)]),
        TableSpec::new(
            "label",
            label_s,
            vec![vec![V::Int(100), V::Int(1)], vec![V::Int(200), V::Int(2)]],
        ),
        TableSpec::new("star", star_s, vec![vec![V::Int(1000), V::Int(100)]]),
    ];
    let tree = run_fetch_test_ast(srcs, &ast).unwrap();
    // issue 1 kept (its label 100 has star 1000); issue 2 dropped (label 200 has no star).
    let ids: Vec<i64> = tree
        .iter()
        .map(|n| match &n.row.col(0).to_owned() {
            V::Int(i) => *i,
            _ => -1,
        })
        .collect();
    assert_eq!(ids, vec![1]);
}

// ===========================================================================
// G/H. AND-within-AND carrying a flipped EXISTS — PREVIOUSLY REJECTED.
//
// Before WS05.4, `where = and[ x, and[ EXISTS_flipped(comments), EXISTS(labels) ] ]`
// returned `BuildError::Unsupported("a flipped EXISTS nested in an AND-within-AND
// (should be flattened by simplifyCondition) is not lowered")`. `flatten_condition`
// in `normalize_pipeline_ast` now splices the inner AND inline before lowering, so the
// 2-deep shape becomes the (already-supported) flat AND, and the 3-deep linear shape
// leaves a single residual nested AND that `apply_flips_and` handles by recursion
// (mirroring the JS `applyFilterWithFlips` AND recursion, builder.ts:443-445).
//
// Logical predicate (all G/H shapes): x>=0 AND EXISTS_flipped(comments) AND
// EXISTS(labels) == "issues with at least one comment AND one label". `x>=0` holds for
// every issue here, so the answer is purely the comment∩label set.
// ===========================================================================

fn ids_of(tree: &[CaughtNode]) -> Vec<i64> {
    tree.iter()
        .map(|n| match &n.row.col(0).to_owned() {
            V::Int(i) => *i,
            other => panic!("non-int id {other:?}"),
        })
        .collect()
}
fn change_id(c: &CaughtChange) -> i64 {
    let row = match c {
        CaughtChange::Add(n) | CaughtChange::Remove(n) => &n.row,
        CaughtChange::Edit { row, .. } | CaughtChange::Child { row, .. } => row,
    };
    match &row.col(0).to_owned() {
        V::Int(i) => *i,
        other => panic!("non-int id {other:?}"),
    }
}

// issue 1: comment+label (kept); 2: comment only; 3: label only; 4: neither.
fn g_data() -> Vec<TableSpec> {
    sources(
        vec![ir(1, 5), ir(2, 5), ir(3, 5), ir(4, 5)],
        vec![cr(10, 1), cr(11, 2)],
        vec![lr(20, 1), lr(21, 3)],
    )
}

// Flat baseline (already supported). Flattening the 2-deep form must produce an
// IDENTICAL graph, hence identical fetch/push output.
fn flat_and_ast() -> Ast {
    issue_ast(Condition::And {
        conditions: vec![x_ge(0), comments(), labels()],
    })
}
// 2-deep: and[ x>=0, and[ EXISTS_flipped(comments), EXISTS(labels) ] ]
fn and2_ast() -> Ast {
    issue_ast(Condition::And {
        conditions: vec![
            x_ge(0),
            Condition::And {
                conditions: vec![comments(), labels()],
            },
        ],
    })
}
// 3-deep linear: and[ x>=0, and[ x>=2, and[ EXISTS_flipped(comments), EXISTS(labels) ] ] ]
fn and3_ast() -> Ast {
    issue_ast(Condition::And {
        conditions: vec![
            x_ge(0),
            Condition::And {
                conditions: vec![
                    x_ge(2),
                    Condition::And {
                        conditions: vec![comments(), labels()],
                    },
                ],
            },
        ],
    })
}

#[test]
fn g_and2_builds_and_filters_to_the_comment_and_label_set() {
    // The build itself (`.unwrap()`) proves the AND-within-AND rejection is gone.
    let tree = run_fetch_test_ast(g_data(), &and2_ast()).unwrap();
    assert_eq!(ids_of(&tree), vec![1]);
}

#[test]
fn g_and2_fetch_is_identical_to_the_flat_form() {
    // 2-deep AND fully flattens to the flat form → identical graph → identical tree.
    let nested = run_fetch_test_ast(g_data(), &and2_ast()).unwrap();
    let flat = run_fetch_test_ast(g_data(), &flat_and_ast()).unwrap();
    assert_eq!(nested, flat);
}

#[test]
fn g_and2_push_is_identical_to_the_flat_form() {
    // Adding issue 2's missing label completes the conjunction for issue 2.
    let pushes = || vec![("label", SourceChange::Add(row(lr(22, 2))))];
    let nested = run_push_test_ast(g_data(), &and2_ast(), pushes(), false).unwrap();
    let flat = run_push_test_ast(g_data(), &flat_and_ast(), pushes(), false).unwrap();
    assert_eq!(nested.initial, flat.initial, "initial fetch parity");
    assert_eq!(nested.pushes, flat.pushes, "push stream parity");
    // Correctness anchor: issue 2 joins the result on its first label.
    assert_eq!(ids_of(&nested.initial), vec![1]);
    let pushed: Vec<i64> = nested.pushes.iter().map(change_id).collect();
    assert_eq!(pushed, vec![2]);
}

#[test]
fn h_and3_linear_nesting_builds_via_recursion_and_filters() {
    // The residual nested AND (flatten is a one-level splice) is handled by the
    // apply_flips_and → apply_where_with_flips → apply_flips_and recursion — NOT a
    // rejection. x>=2 holds for all (x=5), so the surviving set is unchanged.
    let tree = run_fetch_test_ast(g_data(), &and3_ast()).unwrap();
    assert_eq!(ids_of(&tree), vec![1]);
}

#[test]
fn h_and3_push_completes_the_conjunction() {
    let nested = run_push_test_ast(
        g_data(),
        &and3_ast(),
        vec![("label", SourceChange::Add(row(lr(22, 2))))],
        false,
    )
    .unwrap();
    assert_eq!(ids_of(&nested.initial), vec![1]);
    let pushed: Vec<i64> = nested.pushes.iter().map(change_id).collect();
    assert_eq!(pushed, vec![2]);
}
