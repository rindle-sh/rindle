#![cfg(feature = "testkit")]

//! End-to-end tests for a **mixed** OR carrying BOTH a flipped and a non-flipped
//! EXISTS: `issue WHERE (x = 1 OR EXISTS_flipped(comments) OR EXISTS(labels))`, lowered
//! the **JS way** (`applyFilterWithFlips`): the non-flipped `labels` Join sits on the
//! SPINE below the `UnionFanOut`, and `apply_flips_or` builds ONE combined `withoutFlipped`
//! branch (`x = 1 OR labels`, a filter pipeline whose `Exists(labels)` gate counts the
//! spine Join through the rel-preserving broadcast) plus one branch per flipped condition
//! (the `comments` `FlippedJoin`). The `UnionFanIn` collapse (incl. the CHILD collapse)
//! dedups branch outputs. The NESTED mixed shapes (an AND under the OR, etc.) live in
//! `exists_or_nested.rs`.
//!
//! DIVERGENCE FROM JS (documented, accepted): for a non-flipped EXISTS as a direct OR
//! branch, the JS incremental PUSH is lossy (it drops label/parent adds, removes, edits —
//! see the `is_push_lossy`/`emits_a_child` cases), because its production `{change,
//! position}` source overlay suppresses the in-flight child from the `Exists` size
//! re-count. The Rust re-materialize overlay counts it, so the Rust is strictly MORE
//! correct on these push paths. FETCH and flipped-EXISTS pushes match JS exactly. See
//! `nested-exists-or-decision` memory.

use rindle::change::SourceChange;
use rindle::testkit::{
    add, child, cleaf, cnode, rel, remove, run_fetch_test_ast, run_push_test_ast, CaughtChange,
    CaughtNode, TableSpec,
};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    Lit, Op, OrderPart, SimpleCondition, ValuePosition,
};

// issue(id, x). Production-shaped: the table's real relationship names. The query-local
// slot layout (EXISTS where-pre-order) puts comments=slot 0 (flipped), labels=slot 1
// (non-flipped). comment(id, issueID); label(id, issueID).
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
// issue carrying ONLY its labels (slot 1) — the non-flipped-branch output shape.
fn with_labels(id: i64, x: i64, labels: Vec<Vec<V>>) -> CaughtNode {
    cnode(
        ir(id, x),
        vec![(rel(1), labels.into_iter().map(cleaf).collect())],
    )
}
// issue carrying BOTH slots — the shape a fan-OUT (parent) push collapses to, where
// `add_empty_relationships` empty-fills every schema relationship the change lacks.
fn with_both(id: i64, x: i64, comments: Vec<Vec<V>>, labels: Vec<Vec<V>>) -> CaughtNode {
    cnode(
        ir(id, x),
        vec![
            (rel(0), comments.into_iter().map(cleaf).collect()),
            (rel(1), labels.into_iter().map(cleaf).collect()),
        ],
    )
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

/// `issue WHERE (x = 1 OR EXISTS_flipped(comments) OR EXISTS(labels))`.
fn mixed_ast() -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        r#where: Some(Condition::Or {
            conditions: vec![
                Condition::Simple(SimpleCondition {
                    op: Op::Eq,
                    left: ValuePosition::Column { name: "x".into() },
                    right: ValuePosition::Literal {
                        value: Lit::Number(1.0),
                    },
                }),
                exists("comments", "comment", true), // flipped → FlippedJoin branch
                exists("labels", "label", false),    // non-flipped → Join + Exists branch
            ],
        }),
        ..Default::default()
    }
}

#[test]
fn fetch_merges_leaf_flipped_and_nonflipped_branches() {
    // issue 1 via x=1 (leaf, no rels); issue 2 via flipped comments; issue 3 via
    // non-flipped labels; issue 4 matches nothing → dropped.
    let tree = run_fetch_test_ast(
        sources(
            vec![ir(1, 1), ir(2, 5), ir(3, 5), ir(4, 5)],
            vec![cr(10, 2)],
            vec![lr(20, 3)],
        ),
        &mixed_ast(),
    )
    .unwrap();
    // JS parity (`mixed.fetch_merges` oracle): the non-flipped `labels` Join is on the
    // SPINE below the fan-out, so it attaches `labels` to EVERY row (empty where absent);
    // issue 2 (kept via the flipped `comments` branch) carries BOTH its `comments` and the
    // spine's (empty) `labels`.
    assert_eq!(
        tree,
        vec![
            with_labels(1, 1, vec![]),
            with_both(2, 5, vec![cr(10, 2)], vec![]),
            with_labels(3, 5, vec![lr(20, 3)]),
        ]
    );
}

#[test]
fn nonflipped_child_add_flips_a_bare_row_to_add() {
    // issue 4 (x=5, no comments, no labels) is absent. Adding its first LABEL flips the
    // non-flipped `labels` branch on → Add carrying the spine's `labels` (and an empty
    // `comments` slot). The non-flipped `labels` Join now sits on the SPINE below the
    // fan-out; the label child-add broadcasts as a Child, the branch-0 `Exists(labels)`
    // gate flips it to an Add, the union CHILD-collapse forwards it.
    //
    // DIVERGENCE FROM JS (overlay-model, documented): the JS incremental push DROPS this
    // (emits `[]`) — a non-flipped EXISTS under OR is push-lossy in JS because its
    // production `{change, position}` source overlay suppresses the in-flight child from
    // the `Exists` size re-count. The Rust re-materialize overlay counts the new child, so
    // it is strictly MORE correct here (a fresh fetch includes issue 4; so does the Rust
    // push). See `nested-exists-or-decision` memory. The initial fetch now carries the
    // spine `labels` slot (empty) on issue 1.
    let res = run_push_test_ast(
        sources(vec![ir(1, 1), ir(4, 5)], vec![], vec![]),
        &mixed_ast(),
        vec![("label", SourceChange::Add(row(lr(21, 4))))],
        false,
    )
    .unwrap();
    assert_eq!(res.initial, vec![with_labels(1, 1, vec![])]);
    assert_eq!(
        res.pushes,
        vec![add(with_both(4, 5, vec![], vec![lr(21, 4)]))]
    );
}

#[test]
fn flipped_child_add_flips_a_bare_row_to_add() {
    // Same row, but adding a COMMENT flips the FLIPPED branch on — which IS push-correct
    // (`FlippedJoin`). JS parity (`mixed.flip_add_bare`): Add(issue 4) carrying its
    // `comments` AND the spine's (empty) `labels` slot.
    let res = run_push_test_ast(
        sources(vec![ir(1, 1), ir(4, 5)], vec![], vec![]),
        &mixed_ast(),
        vec![("comment", SourceChange::Add(row(cr(11, 4))))],
        false,
    )
    .unwrap();
    assert_eq!(
        res.pushes,
        vec![add(with_both(4, 5, vec![cr(11, 4)], vec![]))]
    );
}

#[test]
fn nonflipped_child_add_on_a_leaf_matched_row_emits_a_child() {
    // issue 1 (x=1) is already kept by the simple branch. Adding a label flips the
    // `Exists(labels)` gate on in branch 0, but the `x=1` leaf in the SAME branch preserves
    // the Child → CHILD precedence keeps the Child(labels). The row stays present; the
    // `labels` relationship update surfaces as a Child(labels, Add) — the spine `labels`
    // Join now sits below the fan-out, so the child-add broadcasts as a Child (exactly the
    // shape the handoff predicted for "JS"). DIVERGENCE FROM JS (overlay-model): the actual
    // JS emits `[]` here (its `{change, position}` overlay suppresses the in-flight child
    // from the gate). The Rust is more correct — it reflects the new `labels` child.
    let res = run_push_test_ast(
        sources(vec![ir(1, 1)], vec![], vec![]),
        &mixed_ast(),
        vec![("label", SourceChange::Add(row(lr(22, 1))))],
        false,
    )
    .unwrap();
    assert_eq!(
        res.pushes,
        vec![child(ir(1, 1), rel(1), add(cleaf(lr(22, 1))))]
    );
}

#[test]
fn nonflipped_child_remove_last_label_flips_to_remove() {
    // issue 3 (x=5) is in ONLY via the non-flipped labels branch. Removing its last
    // label flips it off; no branch keeps it (count == 0) → Remove.
    let res = run_push_test_ast(
        sources(vec![ir(1, 1), ir(3, 5)], vec![], vec![lr(20, 3)]),
        &mixed_ast(),
        vec![("label", SourceChange::Remove(row(lr(20, 3))))],
        false,
    )
    .unwrap();
    assert_eq!(
        res.initial,
        vec![
            with_labels(1, 1, vec![]),
            with_labels(3, 5, vec![lr(20, 3)])
        ]
    );
    assert_eq!(
        res.pushes,
        vec![remove(with_both(3, 5, vec![], vec![lr(20, 3)]))]
    );
}

#[test]
fn parent_add_matching_only_the_nonflipped_branch_via_a_preexisting_label() {
    // A fresh issue 5 (x=5) with a pre-existing label: the spine `labels` Join attaches the
    // label, the fan-OUT broadcasts the parent Add to all branches; only the combined
    // `withoutFlipped` branch's `Exists(labels)` gate keeps it. The collapse runs
    // `add_empty_relationships`, so the single Add carries the labels AND an empty comments
    // slot. DIVERGENCE FROM JS (overlay-model): the JS drops this parent-add (`[]`) — a
    // non-flipped EXISTS-under-OR push is lossy in JS; the Rust correctly Adds issue 5.
    let res = run_push_test_ast(
        sources(vec![ir(1, 1)], vec![], vec![lr(40, 5)]),
        &mixed_ast(),
        vec![("issue", SourceChange::Add(row(ir(5, 5))))],
        false,
    )
    .unwrap();
    assert_eq!(
        res.pushes,
        vec![add(with_both(5, 5, vec![], vec![lr(40, 5)]))]
    );
}

#[test]
fn fetch_row_matching_two_exists_branches_appears_once_first_branch_wins() {
    // issue 7 (x=5) matches BOTH EXISTS branches (a comment AND a label). The fan-in's
    // k-way merge dedups it to ONE row, keeping the **first branch**'s relationship
    // attachment. With the JS layout the first branch is the combined `withoutFlipped`
    // branch (`x=1 OR labels`, index 0), and the `labels` Join is on the spine below the
    // fan-out, so the merge keeps `labels`. JS PARITY (`mixed.two_exists` oracle).
    let tree = run_fetch_test_ast(
        sources(vec![ir(7, 5)], vec![cr(70, 7)], vec![lr(71, 7)]),
        &mixed_ast(),
    )
    .unwrap();
    assert_eq!(tree, vec![with_labels(7, 5, vec![lr(71, 7)])]);
}

#[test]
fn parent_edit_out_of_the_leaf_branch_removes() {
    // issue 1 (x=1) is in via the leaf branch. A non-key Edit to x=5 (no comment, no
    // label) fails all branches: the fan-OUT broadcasts the Edit, the leaf `Filter`
    // splits it (pred(old)=true, pred(new)=false) -> Remove(old), the EXISTS branches drop
    // it, and the collapse keeps the Remove (empty-filling both relationship slots).
    let res = run_push_test_ast(
        sources(vec![ir(1, 1)], vec![], vec![]),
        &mixed_ast(),
        vec![(
            "issue",
            SourceChange::Edit {
                row: row(ir(1, 5)),
                old: row(ir(1, 1)),
            },
        )],
        false,
    )
    .unwrap();
    assert_eq!(res.initial, vec![with_labels(1, 1, vec![])]);
    assert_eq!(res.pushes, vec![remove(with_both(1, 1, vec![], vec![]))]);
}

#[test]
fn child_remove_is_suppressed_by_a_second_exists_branch() {
    // issue 7 (x=5) is in via BOTH EXISTS branches (it has a comment AND a label).
    // Removing its only comment flips the flipped branch off, but the NON-flipped labels
    // branch still keeps it (cross-EXISTS dedup, count == 1 → suppress). No view delta.
    let res = run_push_test_ast(
        sources(vec![ir(7, 5)], vec![cr(70, 7)], vec![lr(71, 7)]),
        &mixed_ast(),
        vec![("comment", SourceChange::Remove(row(cr(70, 7))))],
        false,
    )
    .unwrap();
    assert_eq!(res.pushes, Vec::<CaughtChange>::new());
}

#[test]
fn child_remove_forwards_when_no_other_exists_branch_keeps_it() {
    // Mirror of the above with the label removed first: issue 7 has a comment (flipped)
    // and a label (non-flipped). Remove the LABEL → the non-flipped branch flips off, but
    // the flipped comments branch still keeps it → suppressed (count == 1). Then remove
    // the COMMENT too → now no branch keeps it (count == 0) → Remove forwards.
    let res = run_push_test_ast(
        sources(vec![ir(7, 5)], vec![cr(70, 7)], vec![lr(71, 7)]),
        &mixed_ast(),
        vec![
            ("label", SourceChange::Remove(row(lr(71, 7)))),
            ("comment", SourceChange::Remove(row(cr(70, 7)))),
        ],
        false,
    )
    .unwrap();
    // JS PARITY (`mixed.child_remove_forwards` oracle): the first push (label remove) flips
    // the spine `labels` gate in branch 0, but the `comments` branch still keeps issue 7 →
    // the union forwards a Child(labels, Remove) (the row stays, its `labels` rel emptied).
    // The second push (comment remove) flips the flipped `comments` branch off, no branch
    // keeps it → Remove, carrying the removed comment + the (empty) spine `labels` slot.
    assert_eq!(
        res.pushes,
        vec![
            child(ir(7, 5), rel(1), remove(cleaf(lr(71, 7)))),
            remove(with_both(7, 5, vec![cr(70, 7)], vec![])),
        ]
    );
}
