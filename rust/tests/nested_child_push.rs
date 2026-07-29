#![cfg(feature = "testkit")]

//! The keystone: the **deepest-nested `Change::Child` push** —
//! `issue{comments{reactions}}` where a source change lands at the *deepest*
//! returned relationship. Before this, a `Change::Child` arriving on a join's child
//! port was `unimplemented!()` (graph.rs `join_push`, `Port::JoinChild`); the inner
//! join now carries the full child change up and `push_child_change` materializes it
//! once and rebuilds it per matching parent (`src/change.rs` owned-change model).
//!
//! Topology: `(issues ⟕ (comments ⟕ reactions))`. A reaction add/remove/edit must
//! route a nested `Child(issue → Child(comment → <reaction change>))` to the sink.
//! Also covers the latent fix this generalization brings: a *middle-level* child add
//! (a comment) now carries its **own** relationships (its reactions) instead of
//! being flattened to a bare row.

use rindle::change::{OutEdge, Port};
use rindle::graph::{Graph, NodeId};
use rindle::testkit::CaughtChange;
use rindle::value::{
    owned_row as row, OwnedRow, OwnedValue as V, RelDef, RelId, Schema, SourceSchema,
};
use rindle::view::Col0Node;
use rindle::{build_pipeline, Ast, CorrelatedSubquery, Correlation, Dir, OrderPart, SourceChange};

// reaction(id, comment, val); PK id — leaf.
fn reaction_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "comment", "val"], vec![0], vec![(0, true)])
}
// comment(id, issue, val); PK id.
fn comment_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)])
}
// issue(id, val); PK id.
fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}

// The VIEW schema for `issue{comments{reactions}}` — the nested relationship tree the
// `View` materializes (rel "comments" at slot 0 over issue; rel "reactions" at slot 0
// over comment). Stays a `Schema` (sources are relationship-free `SourceSchema`).
fn reaction_view_schema() -> Schema {
    Schema::new(vec!["id", "comment", "val"], vec![0], vec![(0, true)])
}
fn comment_view_schema() -> Schema {
    Schema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)])
        .with_relationships(vec![RelDef::related("reactions", reaction_view_schema())])
}
fn view_schema() -> Schema {
    Schema::new(vec!["id", "val"], vec![0], vec![(0, true)])
        .with_relationships(vec![RelDef::related("comments", comment_view_schema())])
}

fn issue(id: i64, val: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(val)]
}
fn comment(id: i64, issue: i64, val: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue), V::Int(val)]
}
fn reaction(id: i64, comment: i64, val: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(comment), V::Int(val)]
}

/// A [`Col0Node`] shorthand for the deep-view assertions.
fn n(id: i64, children: Vec<Col0Node>) -> Col0Node {
    Col0Node { id, children }
}

fn col0(r: &OwnedRow) -> i64 {
    match &r.col(0).to_owned() {
        V::Int(i) => *i,
        other => panic!("expected Int col0, got {other:?}"),
    }
}

/// Wire `(issues ⟕ (comments ⟕ reactions))` and return the graph + the three source
/// ids + the top join (whose output the caller wires to a View or Catch).
fn build_nested(
    issues: Vec<Vec<V>>,
    comments: Vec<Vec<V>>,
    reactions: Vec<Vec<V>>,
) -> (Graph, NodeId, NodeId, NodeId, NodeId) {
    let mut g = Graph::new();
    let issues_src = g.add_source(issue_schema(), issues.into_iter().map(row).collect());
    let comments_src = g.add_source(comment_schema(), comments.into_iter().map(row).collect());
    let reactions_src = g.add_source(reaction_schema(), reactions.into_iter().map(row).collect());

    let pconn = g.connect(issues_src, Some(vec![(0, true)]), None, vec![]);
    let cconn = g.connect(comments_src, Some(vec![(0, true)]), None, vec![]);
    let rconn = g.connect(reactions_src, Some(vec![(0, true)]), None, vec![]);

    // inner: comment.id (col 0) == reaction.comment (col 1), rel "reactions".
    let join_reactions = g.add_join_slot(cconn, rconn, vec![0], vec![1], RelId(0));
    // outer: issue.id (col 0) == comment.issue (col 1), rel "comments"; the child is
    // the *reactions sub-pipeline* (join_reactions), feeding the outer join's child.
    let join_comments = g.add_join_slot(pconn, join_reactions, vec![0], vec![1], RelId(0));

    g.set_conn_output(
        pconn,
        OutEdge {
            node: join_comments,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        cconn,
        OutEdge {
            node: join_reactions,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        rconn,
        OutEdge {
            node: join_reactions,
            port: Port::JoinChild,
        },
    );
    // The reactions sub-pipeline feeds the OUTER join's CHILD port — the path the
    // deepest nested Child travels.
    g.set_out_edge(
        join_reactions,
        OutEdge {
            node: join_comments,
            port: Port::JoinChild,
        },
    );

    (g, issues_src, comments_src, reactions_src, join_comments)
}

#[test]
fn deepest_nested_reaction_add_routes_to_view() {
    let (mut g, _i, _c, reactions_src, top) =
        build_nested(vec![issue(1, 100)], vec![comment(10, 1, 500)], vec![]);
    let view = g.add_view(top, view_schema());
    g.set_output(top, view);
    g.hydrate(view);
    assert_eq!(g.dump_view_deep(view), vec![n(1, vec![n(10, vec![])])]);

    // Add a reaction under comment 10 — the deepest level. This is the push that was
    // `unimplemented!()`; it must land nested under issue 1 → comment 10.
    g.source_push(reactions_src, SourceChange::Add(row(reaction(100, 10, 7))));
    assert_eq!(
        g.dump_view_deep(view),
        vec![n(1, vec![n(10, vec![n(100, vec![])])])]
    );
}

#[test]
fn deepest_nested_reaction_remove_routes_to_view() {
    let (mut g, _i, _c, reactions_src, top) = build_nested(
        vec![issue(1, 100)],
        vec![comment(10, 1, 500)],
        vec![reaction(100, 10, 7)],
    );
    let view = g.add_view(top, view_schema());
    g.set_output(top, view);
    g.hydrate(view);
    assert_eq!(
        g.dump_view_deep(view),
        vec![n(1, vec![n(10, vec![n(100, vec![])])])]
    );

    g.source_push(
        reactions_src,
        SourceChange::Remove(row(reaction(100, 10, 7))),
    );
    assert_eq!(g.dump_view_deep(view), vec![n(1, vec![n(10, vec![])])]);
}

#[test]
fn deepest_nested_reaction_edit_updates_value_in_view() {
    let (mut g, _i, _c, reactions_src, top) = build_nested(
        vec![issue(1, 100)],
        vec![comment(10, 1, 500)],
        vec![reaction(100, 10, 7)],
    );
    let view = g.add_view(top, view_schema());
    g.set_output(top, view);
    g.hydrate(view);

    // Non-key edit of the deepest row (reaction.val 7 -> 8): structure is unchanged
    // and the push routes without panic.
    g.source_push(
        reactions_src,
        SourceChange::Edit {
            row: row(reaction(100, 10, 8)),
            old: row(reaction(100, 10, 7)),
        },
    );
    assert_eq!(
        g.dump_view_deep(view),
        vec![n(1, vec![n(10, vec![n(100, vec![])])])]
    );
}

#[test]
fn nested_reaction_routes_only_to_the_owning_issue() {
    // Two issues, each with one comment; a reaction lands under issue 2's comment.
    let (mut g, _i, _c, reactions_src, top) = build_nested(
        vec![issue(1, 100), issue(2, 200)],
        vec![comment(10, 1, 500), comment(20, 2, 500)],
        vec![],
    );
    let view = g.add_view(top, view_schema());
    g.set_output(top, view);
    g.hydrate(view);
    assert_eq!(
        g.dump_view_deep(view),
        vec![n(1, vec![n(10, vec![])]), n(2, vec![n(20, vec![])])]
    );

    g.source_push(reactions_src, SourceChange::Add(row(reaction(100, 20, 7))));
    assert_eq!(
        g.dump_view_deep(view),
        vec![
            n(1, vec![n(10, vec![])]),
            n(2, vec![n(20, vec![n(100, vec![])])])
        ]
    );
}

#[test]
fn deepest_nested_reaction_add_emits_nested_child_change_to_catch() {
    // The differential oracle: the emitted change is exactly
    // `Child(issue 1, "comments" → Child(comment 10, "reactions" → Add(reaction 100)))`.
    let (mut g, _i, _c, reactions_src, top) =
        build_nested(vec![issue(1, 100)], vec![comment(10, 1, 500)], vec![]);
    let catch = g.add_catch(top, false);
    g.set_output(top, catch);

    g.source_push(reactions_src, SourceChange::Add(row(reaction(100, 10, 7))));

    let pushes = g.catch_pushes(catch);
    assert_eq!(pushes.len(), 1, "exactly one downstream change");
    match &pushes[0] {
        CaughtChange::Child {
            row: irow,
            rel: crel,
            change,
        } => {
            assert_eq!(col0(irow), 1, "outer parent is issue 1");
            assert_eq!(crel.0, 0, "issue's \"comments\" slot");
            match change.as_ref() {
                CaughtChange::Child {
                    row: crow,
                    rel: rrel,
                    change,
                } => {
                    assert_eq!(col0(crow), 10, "inner parent is comment 10");
                    assert_eq!(rrel.0, 0, "comment's \"reactions\" slot");
                    match change.as_ref() {
                        CaughtChange::Add(node) => {
                            assert_eq!(col0(&node.row), 100, "leaf add is reaction 100");
                            assert!(node.relationships.is_empty(), "reaction is a leaf");
                        }
                        other => panic!("expected Add(reaction), got {other:?}"),
                    }
                }
                other => panic!("expected Child(comment), got {other:?}"),
            }
        }
        other => panic!("expected Child(issue), got {other:?}"),
    }
}

// --- true end-to-end through build_pipeline (AST → graph → View → push) -----

/// `issue { comments: comment WHERE issue == id { reactions: reaction WHERE comment == id } }`
/// as a wire AST — a two-level nested returned relationship.
fn nested_related_ast() -> Ast {
    let reactions = CorrelatedSubquery {
        correlation: Correlation {
            parent_field: vec!["id".into()],
            child_field: vec!["comment".into()],
        },
        subquery: Box::new(Ast {
            table: "reaction".into(),
            alias: Some("reactions".into()),
            order_by: vec![OrderPart("id".into(), Dir::Asc)],
            ..Default::default()
        }),
        system: None,
    };
    let comments = CorrelatedSubquery {
        correlation: Correlation {
            parent_field: vec!["id".into()],
            child_field: vec!["issue".into()],
        },
        subquery: Box::new(Ast {
            table: "comment".into(),
            alias: Some("comments".into()),
            order_by: vec![OrderPart("id".into(), Dir::Asc)],
            related: vec![reactions],
            ..Default::default()
        }),
        system: None,
    };
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        related: vec![comments],
        ..Default::default()
    }
}

#[test]
fn ast_pipeline_deepest_nested_reaction_add_routes_to_view() {
    // The user's actual goal: an AST lowered by `build_pipeline` into a MemorySource
    // graph + production View, then a deepest-level source push routed end-to-end.
    let mut g = Graph::new();
    let issues_src = g.add_source(issue_schema(), vec![row(issue(1, 100))]);
    let comments_src = g.add_source(comment_schema(), vec![row(comment(10, 1, 500))]);
    let reactions_src = g.add_source(reaction_schema(), Vec::new());
    let resolve = |t: &str| match t {
        "issue" => Some((issues_src, issue_schema())),
        "comment" => Some((comments_src, comment_schema())),
        "reaction" => Some((reactions_src, reaction_schema())),
        _ => None,
    };

    let top = build_pipeline(&mut g, &nested_related_ast(), &resolve).unwrap();
    let view = g.add_view(top, view_schema());
    g.set_output(top, view);
    g.hydrate(view);
    assert_eq!(g.dump_view_deep(view), vec![n(1, vec![n(10, vec![])])]);

    g.source_push(reactions_src, SourceChange::Add(row(reaction(100, 10, 7))));
    assert_eq!(
        g.dump_view_deep(view),
        vec![n(1, vec![n(10, vec![n(100, vec![])])])]
    );

    // ...and remove it again, round-tripping the deepest level.
    g.source_push(
        reactions_src,
        SourceChange::Remove(row(reaction(100, 10, 7))),
    );
    assert_eq!(g.dump_view_deep(view), vec![n(1, vec![n(10, vec![])])]);
}

/// `(issues ⟕flip (comments ⟕ reactions))`: a flipped EXISTS over comments, whose
/// child sub-pipeline itself returns `reactions`. The reactions sub-pipeline feeds
/// the **FlippedJoin's** child port — the path the deepest nested Child travels
/// through `FlippedJoin::push_child` (flipped_join.rs, previously `unimplemented!`).
fn build_nested_flipped(
    issues: Vec<Vec<V>>,
    comments: Vec<Vec<V>>,
    reactions: Vec<Vec<V>>,
) -> (Graph, NodeId, NodeId) {
    let mut g = Graph::new();
    let issues_src = g.add_source(issue_schema(), issues.into_iter().map(row).collect());
    let comments_src = g.add_source(comment_schema(), comments.into_iter().map(row).collect());
    let reactions_src = g.add_source(reaction_schema(), reactions.into_iter().map(row).collect());

    let pconn = g.connect(issues_src, Some(vec![(0, true)]), None, vec![]);
    let cconn = g.connect(comments_src, Some(vec![(0, true)]), None, vec![1]);
    let rconn = g.connect(reactions_src, Some(vec![(0, true)]), None, vec![]);

    let join_reactions = g.add_join_slot(cconn, rconn, vec![0], vec![1], RelId(0));
    let fj = g.add_flipped_join_slot(pconn, join_reactions, vec![0], vec![1], RelId(0));

    g.set_conn_output(
        pconn,
        OutEdge {
            node: fj,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        cconn,
        OutEdge {
            node: join_reactions,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        rconn,
        OutEdge {
            node: join_reactions,
            port: Port::JoinChild,
        },
    );
    g.set_out_edge(
        join_reactions,
        OutEdge {
            node: fj,
            port: Port::JoinChild,
        },
    );

    (g, reactions_src, fj)
}

#[test]
fn deepest_nested_reaction_routes_through_flipped_join() {
    let (mut g, reactions_src, fj) =
        build_nested_flipped(vec![issue(1, 100)], vec![comment(10, 1, 500)], vec![]);
    let view = g.add_view(fj, view_schema());
    g.set_output(fj, view);
    g.hydrate(view);
    // issue 1 is kept (it has comment 10); comment 10 has no reactions yet.
    assert_eq!(g.dump_view_deep(view), vec![n(1, vec![n(10, vec![])])]);

    // A reaction add under comment 10 arrives as a nested `Child` on the FlippedJoin's
    // child port (existence can't flip — the comment still exists).
    g.source_push(reactions_src, SourceChange::Add(row(reaction(100, 10, 7))));
    assert_eq!(
        g.dump_view_deep(view),
        vec![n(1, vec![n(10, vec![n(100, vec![])])])]
    );

    g.source_push(
        reactions_src,
        SourceChange::Remove(row(reaction(100, 10, 7))),
    );
    assert_eq!(g.dump_view_deep(view), vec![n(1, vec![n(10, vec![])])]);
}

#[test]
fn comment_add_carries_its_existing_reactions_into_view() {
    // The latent fix the generalization brings: a *middle-level* child add (a comment)
    // now carries its OWN relationships. Reaction 200 for comment 11 already exists in
    // the source; when comment 11 is pushed, it must appear WITH that reaction nested.
    // (Pre-fix, the child node was flattened to a bare row and the reaction was lost.)
    let (mut g, _i, comments_src, _r, top) =
        build_nested(vec![issue(1, 100)], vec![], vec![reaction(200, 11, 9)]);
    let view = g.add_view(top, view_schema());
    g.set_output(top, view);
    g.hydrate(view);
    assert_eq!(g.dump_view_deep(view), vec![n(1, vec![])]);

    g.source_push(comments_src, SourceChange::Add(row(comment(11, 1, 600))));
    assert_eq!(
        g.dump_view_deep(view),
        vec![n(1, vec![n(11, vec![n(200, vec![])])])]
    );
}
