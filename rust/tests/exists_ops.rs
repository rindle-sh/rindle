#![cfg(feature = "testkit")]

//! Operator-level tests for `Exists` (`op/exists.rs` / `exists.ts`, spec `07` §3.5):
//! the relationship-size gate and its push boundary flips. Wired by hand as a
//! `where`-EXISTS sub-graph — `issues → Join(comments) → FilterStart → Exists →
//! FilterEnd → Catch` — so the operator is exercised in isolation; the
//! `build_pipeline` lowering of `where`-EXISTS lands separately. Pushes drive the
//! real source so the Join's child port + the in-flight overlay are exercised.

use rindle::change::{OutEdge, Port, SourceChange};
use rindle::graph::{Graph, NodeId};
use rindle::op::Exists;
use rindle::testkit::{
    add, child, cleaf, cnode, rel, remove, run_fetch_test, run_fetch_test_ast, run_push_test,
    run_push_test_ast, CaughtChange, CaughtNode, SourceSpec, TableSpec,
};
use rindle::value::{owned_row as row, OwnedValue as V, RelId, SourceSchema};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    OrderPart,
};

// issue(id, x); PK id; declares the "comments" relationship (slot 0).
fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
// comment(id, issueID); PK id; joins to issue on comment.issueID == issue.id.
fn comment_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}
fn ir(id: i64, x: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(x)]
}
fn cr(id: i64, issue: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue)]
}

/// issues(0) → Join(comments) → FilterStart → Exists → FilterEnd. Returns the
/// FilterEnd (the op the runner sinks into a Catch).
fn build_exists(g: &mut Graph, srcs: &[NodeId]) -> NodeId {
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

    let comments_slot = RelId(0);
    let fs = g.add_filter_start(join);
    g.set_output(join, fs); // join → FilterStart
                            // parent_join_key = [id]; pk = [id] ⇒ no_size_reuse (cache off — exercises the
                            // direct-count path).
    let exists = g.add_exists(Exists::new(fs, comments_slot, vec![0], false, &[0]));
    let fe = g.add_filter_end(fs);
    g.set_chain_head(fs, exists);
    g.set_output(exists, fe);
    fe
}

fn sources(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>) -> Vec<SourceSpec> {
    vec![
        SourceSpec::new(issue_schema(), issues),
        SourceSpec::new(comment_schema(), comments),
    ]
}

/// Run a fetch (hydrate) over the given data.
fn fetch(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>) -> Vec<CaughtNode> {
    run_fetch_test(sources(issues, comments), build_exists)
}

/// Run one source push (source index 0 = issues, 1 = comments) and return the
/// caught downstream changes.
fn push(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>, p: (usize, SourceChange)) -> Vec<CaughtChange> {
    run_push_test(sources(issues, comments), build_exists, vec![p], false).pushes
}

// An issue node with its comments relationship (slot 0).
fn issue_with(id: i64, x: i64, comments: Vec<Vec<V>>) -> CaughtNode {
    cnode(
        ir(id, x),
        vec![(rel(0), comments.into_iter().map(cleaf).collect())],
    )
}

// --- fetch (the EXISTS gate) -----------------------------------------------

#[test]
fn fetch_keeps_only_parents_with_a_child() {
    // issues 1,2,3; only issue 1 has comments → EXISTS keeps just issue 1.
    let tree = fetch(
        vec![ir(1, 100), ir(2, 200), ir(3, 300)],
        vec![cr(10, 1), cr(11, 1)],
    );
    assert_eq!(tree, vec![issue_with(1, 100, vec![cr(10, 1), cr(11, 1)])]);
}

#[test]
fn fetch_drops_all_when_no_children() {
    assert!(fetch(vec![ir(1, 100), ir(2, 200)], vec![]).is_empty());
}

// --- push: parent changes (#pushWithFilter) --------------------------------

#[test]
fn parent_add_without_children_is_dropped() {
    // A newly-added issue has no comments yet → fails EXISTS → dropped.
    assert_eq!(
        push(
            vec![ir(1, 100)],
            vec![cr(10, 1)],
            (0, SourceChange::Add(row(ir(2, 200))))
        ),
        vec![]
    );
}

#[test]
fn parent_remove_with_children_forwards() {
    // Removing an issue that still has comments → size > 0 → forward the Remove.
    assert_eq!(
        push(
            vec![ir(1, 100)],
            vec![cr(10, 1)],
            (0, SourceChange::Remove(row(ir(1, 100))))
        ),
        vec![remove(issue_with(1, 100, vec![cr(10, 1)]))]
    );
}

// --- push: child changes (boundary flips) ----------------------------------

#[test]
fn child_add_makes_parent_appear() {
    // issue 1 has no comments (absent from the view); adding its first comment
    // flips EXISTS → emit Add(issue 1) carrying the new comment.
    assert_eq!(
        push(
            vec![ir(1, 100), ir(2, 200)],
            vec![],
            (1, SourceChange::Add(row(cr(10, 1))))
        ),
        vec![add(issue_with(1, 100, vec![cr(10, 1)]))]
    );
}

#[test]
fn child_add_to_existing_passes_through() {
    // issue 1 already has a comment (size 1→2): not a membership flip → forward the
    // Child change (the relationship changed).
    assert_eq!(
        push(
            vec![ir(1, 100)],
            vec![cr(10, 1)],
            (1, SourceChange::Add(row(cr(11, 1))))
        ),
        vec![child(ir(1, 100), rel(0), add(cleaf(cr(11, 1))))]
    );
}

#[test]
fn child_remove_makes_parent_disappear() {
    // Removing issue 1's only comment flips EXISTS → emit Remove(issue 1), carrying
    // the just-removed comment (the forced relationship).
    assert_eq!(
        push(
            vec![ir(1, 100)],
            vec![cr(10, 1)],
            (1, SourceChange::Remove(row(cr(10, 1))))
        ),
        vec![remove(issue_with(1, 100, vec![cr(10, 1)]))]
    );
}

#[test]
fn child_remove_with_remainder_passes_through() {
    // issue 1 keeps a comment after the remove (size 2→1): not a flip → forward the
    // Child change.
    assert_eq!(
        push(
            vec![ir(1, 100)],
            vec![cr(10, 1), cr(11, 1)],
            (1, SourceChange::Remove(row(cr(10, 1)))),
        ),
        vec![child(ir(1, 100), rel(0), remove(cleaf(cr(10, 1))))]
    );
}

// --- end-to-end through build_pipeline (EXISTS lowering) -------------------

/// `issue WHERE EXISTS(comments)` — the where-EXISTS lowering builds a relationship
/// Join (the child bounded to EXISTS_LIMIT by a `Cap`) + an `Exists` gate.
fn exists_ast() -> Ast {
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
                    table: "comment".into(),
                    alias: Some("comments".into()),
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

fn ast_sources(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>) -> Vec<TableSpec> {
    vec![
        TableSpec::new("issue", issue_schema(), issues),
        TableSpec::new("comment", comment_schema(), comments),
    ]
}

#[test]
fn ast_exists_fetch_keeps_parents_with_a_child() {
    // build_pipeline lowers EXISTS to Cap(EXISTS_LIMIT) + Exists; only issue 1 has
    // comments, and they ride along on the kept node (bounded to 3 by the Cap).
    let tree = run_fetch_test_ast(
        ast_sources(
            vec![ir(1, 100), ir(2, 200), ir(3, 300)],
            vec![cr(10, 1), cr(11, 1)],
        ),
        &exists_ast(),
    )
    .unwrap();
    assert_eq!(tree, vec![issue_with(1, 100, vec![cr(10, 1), cr(11, 1)])]);
}

fn not_exists_ast() -> Ast {
    let mut ast = exists_ast();
    if let Some(Condition::CorrelatedSubquery(c)) = &mut ast.r#where {
        c.op = ExistsOp::NotExists;
    }
    ast
}

#[test]
fn ast_not_exists_fetch_keeps_parents_without_a_child() {
    let tree = run_fetch_test_ast(
        ast_sources(
            vec![ir(1, 100), ir(2, 200), ir(3, 300)],
            vec![cr(10, 1), cr(11, 1), cr(12, 3)],
        ),
        &not_exists_ast(),
    )
    .unwrap();
    assert_eq!(tree, vec![issue_with(2, 200, vec![])]);
}

#[test]
fn ast_exists_limit_zero_drops_all() {
    // A user `limit(0)` on the EXISTS subquery is constant-false (the subquery has
    // no rows) → every parent is dropped, even one that has a matching child.
    let mut ast = exists_ast();
    if let Some(Condition::CorrelatedSubquery(c)) = &mut ast.r#where {
        c.related.subquery.limit = Some(0);
    }
    let tree = run_fetch_test_ast(ast_sources(vec![ir(1, 100)], vec![cr(10, 1)]), &ast).unwrap();
    assert!(tree.is_empty());
}

#[test]
fn ast_not_exists_limit_zero_keeps_all_without_joining_child_rel() {
    // A user `limit(0)` on the NOT EXISTS subquery is constant-true. The EXISTS
    // relationship Join is not built for this short-circuit, so nodes carry no
    // comments relationship.
    let mut ast = not_exists_ast();
    if let Some(Condition::CorrelatedSubquery(c)) = &mut ast.r#where {
        c.related.subquery.limit = Some(0);
    }
    let tree = run_fetch_test_ast(
        ast_sources(vec![ir(1, 100), ir(2, 200)], vec![cr(10, 1)]),
        &ast,
    )
    .unwrap();
    assert_eq!(tree, vec![cleaf(ir(1, 100)), cleaf(ir(2, 200))]);
}

fn issue_schema_same_alias_exists() -> SourceSchema {
    // One real `comments` relationship; the query (`AND[EXISTS(comments), EXISTS(comments)]`)
    // uniquifies it into two query-local gate slots (comments_0=0, comments_1=1) all on its own.
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
fn ast_sources_same_alias_exists(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>) -> Vec<TableSpec> {
    vec![
        TableSpec::new("issue", issue_schema_same_alias_exists(), issues),
        TableSpec::new("comment", comment_schema(), comments),
    ]
}

#[test]
fn ast_same_alias_exists_is_uniquified() {
    // Two EXISTS on the SAME raw relationship alias are suffixed like JS
    // (`comments_0`, `comments_1`) so each gate counts its own relationship slot.
    let one = || {
        Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueID".into()],
                },
                subquery: Box::new(Ast {
                    table: "comment".into(),
                    alias: Some("comments".into()),
                    ..Default::default()
                }),
                system: None,
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        })
    };
    let ast = Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        r#where: Some(Condition::And {
            conditions: vec![one(), one()],
        }),
        ..Default::default()
    };
    let tree = run_fetch_test_ast(
        ast_sources_same_alias_exists(vec![ir(1, 100)], vec![cr(10, 1)]),
        &ast,
    )
    .unwrap();
    assert_eq!(
        tree,
        vec![cnode(
            ir(1, 100),
            vec![
                (rel(0), vec![cleaf(cr(10, 1))]),
                (rel(1), vec![cleaf(cr(10, 1))]),
            ],
        )]
    );
}

#[test]
fn ast_exists_push_child_add_makes_parent_appear() {
    // No issue has comments at hydrate (view empty); adding issue 1's first comment
    // flips EXISTS through the full lowered pipeline → Add(issue 1).
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 100), ir(2, 200)], vec![]),
        &exists_ast(),
        vec![("comment", SourceChange::Add(row(cr(10, 1))))],
        false,
    )
    .unwrap();
    assert!(res.initial.is_empty());
    assert_eq!(res.pushes, vec![add(issue_with(1, 100, vec![cr(10, 1)]))]);
}

#[test]
fn ast_not_exists_push_child_add_makes_parent_disappear() {
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 100), ir(2, 200)], vec![]),
        &not_exists_ast(),
        vec![("comment", SourceChange::Add(row(cr(10, 1))))],
        false,
    )
    .unwrap();
    assert_eq!(
        res.initial,
        vec![issue_with(1, 100, vec![]), issue_with(2, 200, vec![])]
    );
    assert_eq!(res.pushes, vec![remove(issue_with(1, 100, vec![]))]);
}

#[test]
fn ast_not_exists_push_child_remove_makes_parent_appear() {
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 100), ir(2, 200)], vec![cr(10, 1)]),
        &not_exists_ast(),
        vec![("comment", SourceChange::Remove(row(cr(10, 1))))],
        false,
    )
    .unwrap();
    assert_eq!(res.initial, vec![issue_with(2, 200, vec![])]);
    assert_eq!(res.pushes, vec![add(issue_with(1, 100, vec![]))]);
}
