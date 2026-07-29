//! Tests for the fluent AST builder (`src/query.rs`). These double as the
//! executable spec for the "loosely typed, schema-free" surface: each pins the
//! exact [`Ast`] a chain produces, so a behavior change is a structural diff.

use std::collections::BTreeMap;

use rindle::ast::{
    Ast, Bound, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir,
    ExistsOp, Lit, Op, OrderPart, SimpleCondition, System, ValuePosition,
};
use rindle::query::{table, ExistsOpts};

// --- small constructors to keep expectations readable ----------------------

fn simple(field: &str, op: Op, value: Lit) -> Condition {
    Condition::Simple(SimpleCondition {
        op,
        left: ValuePosition::Column { name: field.into() },
        right: ValuePosition::Literal { value },
    })
}

fn corr(parent: &[&str], child: &[&str]) -> Correlation {
    Correlation {
        parent_field: parent.iter().map(|s| (*s).into()).collect(),
        child_field: child.iter().map(|s| (*s).into()).collect(),
    }
}

// =========================================================================
// The headline example from the request
// =========================================================================

#[test]
fn sub_derives_correlation_from_closure() {
    // issue.select('title').sub(row => user.where('id', row.creatorID).select('name'))
    let q = table("issue")
        .select("title")
        .sub(|row| {
            table("user")
                .r#where("id", row.col("creatorID"))
                .select("name")
        })
        .build();

    let expected = Ast {
        table: "issue".into(),
        select: Some(vec!["title".into()]),
        related: vec![CorrelatedSubquery {
            // child `id` ↔ parent `creatorID`
            correlation: corr(&["creatorID"], &["id"]),
            subquery: Box::new(Ast {
                table: "user".into(),
                select: Some(vec!["name".into()]),
                ..Ast::default()
            }),
            system: None,
        }],
        ..Ast::default()
    };

    assert_eq!(q, expected);
}

// =========================================================================
// select
// =========================================================================

#[test]
fn no_select_means_select_all() {
    assert_eq!(table("issue").build(), Ast::new("issue"));
    assert_eq!(table("issue").build().select, None);
}

#[test]
fn select_accumulates_in_order() {
    let q = table("issue").select("id").select("title").build();
    assert_eq!(q.select, Some(vec!["id".into(), "title".into()]));
}

// =========================================================================
// where: literals, explicit op, IN, AND-combination, null
// =========================================================================

#[test]
fn where_eq_infers_literal_type() {
    assert_eq!(
        table("t").r#where("name", "bob").build().r#where,
        Some(simple("name", Op::Eq, Lit::Str("bob".into())))
    );
    assert_eq!(
        table("t").r#where("age", 5).build().r#where,
        Some(simple("age", Op::Eq, Lit::Int(5)))
    );
    assert_eq!(
        table("t").r#where("ratio", 1.5).build().r#where,
        Some(simple("ratio", Op::Eq, Lit::Number(1.5)))
    );
    assert_eq!(
        table("t").r#where("closed", true).build().r#where,
        Some(simple("closed", Op::Eq, Lit::Bool(true)))
    );
}

#[test]
fn where_null_via_option_and_is() {
    // `where(field, None)` => `field = NULL`
    assert_eq!(
        table("t").r#where("owner", None::<i64>).build().r#where,
        Some(simple("owner", Op::Eq, Lit::Null))
    );
    // explicit IS for proper null matching
    assert_eq!(
        table("t")
            .where_op("owner", "IS", None::<i64>)
            .build()
            .r#where,
        Some(simple("owner", Op::Is, Lit::Null))
    );
}

#[test]
fn where_op_takes_string_or_enum_operator() {
    assert_eq!(
        table("t").where_op("age", ">", 18).build().r#where,
        Some(simple("age", Op::Gt, Lit::Int(18)))
    );
    assert_eq!(
        table("t").where_op("name", Op::Like, "a%").build().r#where,
        Some(simple("name", Op::Like, Lit::Str("a%".into())))
    );
}

#[test]
fn where_in_builds_an_array() {
    let q = table("t").where_in("id", [1, 2, 3]).build();
    assert_eq!(
        q.r#where,
        Some(simple(
            "id",
            Op::In,
            Lit::Array(vec![Lit::Int(1), Lit::Int(2), Lit::Int(3)])
        ))
    );
}

#[test]
fn multiple_wheres_and_combine_and_flatten() {
    let q = table("issue")
        .r#where("closed", false)
        .where_op("priority", ">", 3)
        .where_in("kind", ["bug", "task"])
        .build();
    assert_eq!(
        q.r#where,
        Some(Condition::And {
            conditions: vec![
                simple("closed", Op::Eq, Lit::Bool(false)),
                simple("priority", Op::Gt, Lit::Int(3)),
                simple(
                    "kind",
                    Op::In,
                    Lit::Array(vec![Lit::Str("bug".into()), Lit::Str("task".into())])
                ),
            ],
        })
    );
}

// =========================================================================
// sub: aliases, compound keys, filter-vs-correlation split, nesting
// =========================================================================

#[test]
fn sub_as_names_the_relationship() {
    let q = table("issue")
        .sub_as("author", |row| {
            table("user").r#where("id", row.col("creatorID"))
        })
        .build();
    assert_eq!(q.related[0].subquery.alias.as_deref(), Some("author"));
}

#[test]
fn sub_supports_compound_correlation_keys() {
    let q = table("a")
        .sub(|row| {
            table("b")
                .r#where("x", row.col("p"))
                .r#where("y", row.col("q"))
        })
        .build();
    assert_eq!(q.related[0].correlation, corr(&["p", "q"], &["x", "y"]));
    // and no filter leaked into the child's where
    assert_eq!(q.related[0].subquery.r#where, None);
}

#[test]
fn sub_separates_correlation_from_real_filters() {
    // A parent-ref `where` is a correlation; a literal `where` is a filter.
    let q = table("issue")
        .sub(|row| {
            table("comment")
                .r#where("issueID", row.col("id"))
                .r#where("spam", false)
        })
        .build();
    let child = &q.related[0];
    assert_eq!(child.correlation, corr(&["id"], &["issueID"]));
    assert_eq!(
        child.subquery.r#where,
        Some(simple("spam", Op::Eq, Lit::Bool(false)))
    );
}

#[test]
fn subs_nest() {
    let q = table("issue")
        .sub(|row| {
            table("comment")
                .r#where("issueID", row.col("id"))
                .sub(|row2| table("user").r#where("id", row2.col("authorID")))
        })
        .build();

    let comment = &q.related[0];
    assert_eq!(comment.correlation, corr(&["id"], &["issueID"]));
    assert_eq!(comment.subquery.table.as_ref(), "comment");

    let user = &comment.subquery.related[0];
    assert_eq!(user.correlation, corr(&["authorID"], &["id"]));
    assert_eq!(user.subquery.table.as_ref(), "user");
}

#[test]
fn multiple_subs_append_in_order() {
    let q = table("issue")
        .sub_as("author", |row| {
            table("user").r#where("id", row.col("creatorID"))
        })
        .sub_as("comments", |row| {
            table("comment").r#where("issueID", row.col("id"))
        })
        .build();
    assert_eq!(q.related.len(), 2);
    assert_eq!(q.related[0].subquery.alias.as_deref(), Some("author"));
    assert_eq!(q.related[1].subquery.alias.as_deref(), Some("comments"));
}

// =========================================================================
// where_exists / where_not_exists
// =========================================================================

#[test]
fn where_exists_and_not_exists() {
    let exists = table("issue")
        .where_exists(|row| table("comment").r#where("issueID", row.col("id")))
        .build();
    assert_eq!(
        exists.r#where,
        Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: CorrelatedSubquery {
                correlation: corr(&["id"], &["issueID"]),
                subquery: Box::new(Ast::new("comment")),
                system: None,
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        }))
    );

    let not_exists = table("issue")
        .where_not_exists(|row| table("comment").r#where("issueID", row.col("id")))
        .build();
    assert!(matches!(
        not_exists.r#where,
        Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            op: ExistsOp::NotExists,
            ..
        }))
    ));
}

#[test]
fn where_exists_no_sync_stamps_permissions_provenance() {
    // `exists_noSync`: the gate is identical to `where_exists` except the subquery is stamped
    // `system: Permissions`, which the normalized serializer reads to prune its witnesses.
    let q = table("issue")
        .where_exists_no_sync(|row| table("acl").r#where("issueID", row.col("id")))
        .build();
    assert_eq!(
        q.r#where,
        Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: CorrelatedSubquery {
                correlation: corr(&["id"], &["issueID"]),
                subquery: Box::new(Ast::new("acl")),
                system: Some(System::Permissions),
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        }))
    );

    // The NOT EXISTS form stamps the same provenance.
    let not = table("issue")
        .where_not_exists_no_sync(|row| table("acl").r#where("issueID", row.col("id")))
        .build();
    assert!(matches!(
        not.r#where,
        Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            op: ExistsOp::NotExists,
            related: CorrelatedSubquery {
                system: Some(System::Permissions),
                ..
            },
            ..
        }))
    ));
}

#[test]
fn exists_ands_with_a_plain_filter() {
    let q = table("issue")
        .r#where("closed", false)
        .where_exists(|row| table("comment").r#where("issueID", row.col("id")))
        .build();
    match q.r#where {
        Some(Condition::And { conditions }) => {
            assert_eq!(conditions.len(), 2);
            assert_eq!(conditions[0], simple("closed", Op::Eq, Lit::Bool(false)));
            assert!(matches!(
                conditions[1],
                Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
                    op: ExistsOp::Exists,
                    ..
                })
            ));
        }
        other => panic!("expected And([filter, exists]), got {other:?}"),
    }
}

// =========================================================================
// where_any / where_all: OR groups, nesting, EXISTS-in-OR
// =========================================================================

#[test]
fn where_any_builds_a_top_level_or() {
    // GenreId = 1 OR Milliseconds > 400000
    let q = table("Track")
        .where_any(|c| {
            c.r#where("GenreId", 1)
                .where_op("Milliseconds", ">", 400_000)
        })
        .build();
    assert_eq!(
        q.r#where,
        Some(Condition::Or {
            conditions: vec![
                simple("GenreId", Op::Eq, Lit::Int(1)),
                simple("Milliseconds", Op::Gt, Lit::Int(400_000)),
            ],
        })
    );
}

#[test]
fn where_any_and_combines_with_a_plain_where() {
    // closed = false AND (priority > 3 OR kind = 'bug')
    let q = table("issue")
        .r#where("closed", false)
        .where_any(|c| c.where_op("priority", ">", 3).r#where("kind", "bug"))
        .build();
    assert_eq!(
        q.r#where,
        Some(Condition::And {
            conditions: vec![
                simple("closed", Op::Eq, Lit::Bool(false)),
                Condition::Or {
                    conditions: vec![
                        simple("priority", Op::Gt, Lit::Int(3)),
                        simple("kind", Op::Eq, Lit::Str("bug".into())),
                    ],
                },
            ],
        })
    );
}

#[test]
fn groups_nest_and_within_or() {
    // (a = 1 AND b = 2) OR c = 3
    let q = table("t")
        .where_any(|c| c.all(|c| c.r#where("a", 1).r#where("b", 2)).r#where("c", 3))
        .build();
    assert_eq!(
        q.r#where,
        Some(Condition::Or {
            conditions: vec![
                Condition::And {
                    conditions: vec![
                        simple("a", Op::Eq, Lit::Int(1)),
                        simple("b", Op::Eq, Lit::Int(2)),
                    ],
                },
                simple("c", Op::Eq, Lit::Int(3)),
            ],
        })
    );
}

#[test]
fn where_any_admits_an_exists_clause() {
    // Title LIKE '%Greatest%' OR EXISTS(Track where AlbumId=parent.AlbumId AND GenreId=1)
    let q = table("Album")
        .where_any(|c| {
            c.where_op("Title", "LIKE", "%Greatest%")
                .where_exists(|row| {
                    table("Track")
                        .r#where("AlbumId", row.col("AlbumId"))
                        .r#where("GenreId", 1)
                })
        })
        .build();
    match q.r#where {
        Some(Condition::Or { conditions }) => {
            assert_eq!(conditions.len(), 2);
            assert_eq!(
                conditions[0],
                simple("Title", Op::Like, Lit::Str("%Greatest%".into()))
            );
            match &conditions[1] {
                Condition::CorrelatedSubquery(c) => {
                    assert_eq!(c.op, ExistsOp::Exists);
                    // correlation siphoned from the child closure; filter stays in the child
                    assert_eq!(c.related.correlation, corr(&["AlbumId"], &["AlbumId"]));
                    assert_eq!(
                        c.related.subquery.r#where,
                        Some(simple("GenreId", Op::Eq, Lit::Int(1)))
                    );
                }
                other => panic!("expected an EXISTS clause, got {other:?}"),
            }
        }
        other => panic!("expected Or([like, exists]), got {other:?}"),
    }
}

#[test]
fn singleton_group_is_unwrapped() {
    // A group of one clause needs no Or/And wrapper.
    assert_eq!(
        table("t").where_any(|c| c.r#where("a", 1)).build().r#where,
        Some(simple("a", Op::Eq, Lit::Int(1)))
    );
    assert_eq!(
        table("t").where_all(|c| c.r#where("a", 1)).build().r#where,
        Some(simple("a", Op::Eq, Lit::Int(1)))
    );
}

#[test]
#[should_panic(expected = "is empty")]
fn empty_group_panics() {
    let _ = table("t").where_any(|c| c).build();
}

// =========================================================================
// limit / orderBy / start
// =========================================================================

#[test]
fn limit_order_and_start() {
    let q = table("issue")
        .order_by("created", Dir::Desc)
        .order_by("id", "asc")
        .limit(10)
        .start_after("id", 100)
        .build();

    assert_eq!(q.limit, Some(10));
    assert_eq!(
        q.order_by,
        vec![
            OrderPart("created".into(), Dir::Desc),
            OrderPart("id".into(), Dir::Asc),
        ]
    );
    assert_eq!(
        q.start,
        Some(Bound {
            row: BTreeMap::from([("id".into(), Lit::Int(100))]),
            exclusive: true,
        })
    );
}

#[test]
fn start_at_is_inclusive() {
    let q = table("issue").start_at("id", 5).build();
    assert_eq!(
        q.start,
        Some(Bound {
            row: BTreeMap::from([("id".into(), Lit::Int(5))]),
            exclusive: false,
        })
    );
}

// =========================================================================
// guard rails
// =========================================================================

#[test]
#[should_panic(expected = "no correlation")]
fn sub_without_correlation_panics() {
    // forgot to relate the child to the parent
    let _ = table("issue")
        .sub(|_row| table("user").select("name"))
        .build();
}

// --- scalar EXISTS opt-in (ExistsOpts) -------------------------------------

#[test]
fn where_exists_with_scalar_sets_the_flag_and_plain_does_not() {
    let scalar = table("issue")
        .where_exists_with(
            |p| table("assignee").r#where("issueID", p.col("id")),
            ExistsOpts { scalar: true },
        )
        .build();
    let Some(Condition::CorrelatedSubquery(c)) = scalar.r#where else {
        panic!("expected a correlated-subquery condition");
    };
    assert_eq!(c.scalar, Some(true));
    assert_eq!(c.op, ExistsOp::Exists);

    // The plain form leaves `scalar` unset (a live EXISTS join).
    let plain = table("issue")
        .where_exists(|p| table("assignee").r#where("issueID", p.col("id")))
        .build();
    let Some(Condition::CorrelatedSubquery(c)) = plain.r#where else {
        unreachable!()
    };
    assert_eq!(c.scalar, None);
}

#[test]
fn where_not_exists_with_scalar_sets_the_flag() {
    let q = table("issue")
        .where_not_exists_with(
            |p| table("assignee").r#where("issueID", p.col("id")),
            ExistsOpts { scalar: true },
        )
        .build();
    let Some(Condition::CorrelatedSubquery(c)) = q.r#where else {
        panic!("expected a correlated-subquery condition");
    };
    assert_eq!(c.scalar, Some(true));
    assert_eq!(c.op, ExistsOp::NotExists);
}
