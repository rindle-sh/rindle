#![cfg(feature = "testkit")]

//! serde round-trip + JS-wire-JSON parse proofs for the AST (`src/ast.rs`).
//!
//! Step 0 of the builder port: the Rust AST must deserialize the *same JSON* the
//! JS `astSchema` (`zero-protocol/src/ast.ts`) emits, so JS query fixtures can
//! drive the Rust pipeline builder (the shared-AST differential corpus, spec 11
//! §3.6). These tests pin (a) untagged literal encoding incl. the JS-`number`→`f64`
//! collapse, (b) a fluent-built AST surviving a JSON round-trip, and (c) hand-written
//! JS-shaped wire JSON parsing to the expected structure.
//!
//! serde is `testkit`-gated (it is never in the shipping artifact), so these run
//! under `cargo test --features "sqlite testkit"`; without `testkit` this file
//! compiles to an empty test binary.
use std::collections::BTreeMap;

use rindle::ast::{
    Ast, Bound, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir,
    ExistsOp, Lit, Op, OrderPart, SimpleCondition, ValuePosition,
};
use rindle::query::table;

#[test]
fn literal_values_use_the_wire_encoding() {
    // A JSON integer token parses EXACTLY into `Lit::Int` (design 226 Stage B — the
    // old single-`f64` collapse rounded above 2^53); a float token stays `Number`.
    // The two spellings lower to the same scalar for every integral value in ±2^53,
    // so this parse change is unobservable to the pipeline.
    assert_eq!(serde_json::from_str::<Lit>("5").unwrap(), Lit::Int(5));
    assert_eq!(
        serde_json::from_str::<Lit>("1.5").unwrap(),
        Lit::Number(1.5)
    );
    // Each variant round-trips through its bare untagged JSON form — INCLUDING the
    // integral float: serde_json serializes `5.0_f64` as the token `"5.0"` (ryu keeps
    // a `.`/exponent on every float), which re-parses to `Number(5.0)`. The plane is
    // PRESERVED through serialize→deserialize; a re-serialization path that emitted
    // `"5"` instead would silently flip the literal onto the int plane on re-parse.
    for lit in [
        Lit::Null,
        Lit::Bool(true),
        Lit::Int(5),
        Lit::Int(9_007_199_254_740_993), // 2^53 + 1 — survives exactly now
        Lit::Number(5.0),
        Lit::Number(1.5),
        Lit::Str("hi".into()),
        Lit::Array(vec![Lit::Int(1), Lit::Str("x".into())]),
    ] {
        let s = serde_json::to_string(&lit).unwrap();
        assert_eq!(
            serde_json::from_str::<Lit>(&s).unwrap(),
            lit,
            "round-trip failed: {s}"
        );
    }
    assert_eq!(serde_json::to_string(&Lit::Number(5.0)).unwrap(), "5.0");
    assert_eq!(serde_json::to_string(&Lit::Int(5)).unwrap(), "5");
}

#[test]
fn fluent_ast_round_trips_through_json() {
    let q = table("issue")
        .select("title")
        .r#where("closed", false)
        .where_op("priority", ">", 3)
        .where_in("kind", ["bug", "task"])
        .order_by("created", "desc")
        .order_by("id", "asc")
        .limit(10)
        .start_after("id", 100)
        .sub_as("author", |row| {
            table("user").r#where("id", row.col("creatorID"))
        })
        .where_exists(|row| table("comment").r#where("issueID", row.col("id")))
        .build();

    let json = serde_json::to_string(&q).unwrap();
    let back: Ast = serde_json::from_str(&json).unwrap();
    assert_eq!(
        q, back,
        "AST did not survive a JSON round-trip\njson = {json}"
    );
}

#[test]
fn aggregate_ast_round_trips_and_uses_camelcase_keys() {
    // (added during review) The top-level-aggregate wire fields — `group_by`, `having`,
    // and the `count` aggregate — survive a JSON round-trip, and the keys are the
    // camelCase `groupBy` / `having` the JS astSchema would emit.
    let q = table("comment")
        .where_op("issueID", ">=", 10)
        .group_by("issueID")
        .count()
        .having(|h| h.where_op("count", ">", 1))
        .build();
    let json = serde_json::to_string(&q).unwrap();
    assert!(json.contains("\"groupBy\""), "groupBy key present: {json}");
    assert!(json.contains("\"having\""), "having key present: {json}");
    let back: Ast = serde_json::from_str(&json).unwrap();
    assert_eq!(
        q, back,
        "aggregate AST did not survive a round-trip\njson = {json}"
    );

    // A global count omits the optional group_by/having (skip_serializing_if).
    let global = table("comment").count().build();
    let gjson = serde_json::to_string(&global).unwrap();
    assert!(!gjson.contains("groupBy"), "empty groupBy omitted: {gjson}");
    assert!(!gjson.contains("having"), "absent having omitted: {gjson}");
}

#[test]
fn sum_and_avg_aggregates_round_trip_carrying_their_column() {
    // `Count` is the bare string `"count"`; `Sum`/`Avg` are externally tagged, carrying
    // the aggregated column name (`{"sum": "amt"}` / `{"avg": "amt"}`). All round-trip.
    for q in [
        table("sale").sum("amt").build(),
        table("sale").group_by("cat").avg("amt").build(),
        table("issue")
            .sum_as("total", "amt", |r| {
                table("line").r#where("issueID", r.col("id"))
            })
            .build(),
    ] {
        let json = serde_json::to_string(&q).unwrap();
        let back: rindle::Ast = serde_json::from_str(&json).unwrap();
        assert_eq!(q, back, "sum/avg AST did not survive a round-trip: {json}");
    }

    // The externally-tagged shape carries the column under the lowercase variant key.
    let json = serde_json::to_string(&table("sale").sum("amt").build()).unwrap();
    assert!(
        json.contains("\"sum\":\"amt\""),
        "sum carries its column: {json}"
    );
}

#[test]
fn parses_js_wire_shaped_json() {
    // The shape `zero-protocol`'s astSchema emits: camelCase keys, "type"-tagged
    // conditions, SQL-string operators, [field, dir] orderBy tuples, EXISTS.
    let wire = r#"{
        "table": "issue",
        "orderBy": [["created", "desc"], ["id", "asc"]],
        "limit": 10,
        "start": { "row": { "id": 100 }, "exclusive": true },
        "where": {
            "type": "and",
            "conditions": [
                { "type": "simple", "op": "=",
                  "left": { "type": "column", "name": "closed" },
                  "right": { "type": "literal", "value": false } },
                { "type": "correlatedSubquery", "op": "EXISTS",
                  "related": {
                      "correlation": { "parentField": ["id"], "childField": ["issueID"] },
                      "subquery": { "table": "comment" }
                  } }
            ]
        },
        "related": [
            { "correlation": { "parentField": ["creatorID"], "childField": ["id"] },
              "subquery": { "table": "user", "alias": "author" } }
        ]
    }"#;

    let ast: Ast = serde_json::from_str(wire).unwrap();

    assert_eq!(ast.table.as_ref(), "issue");
    assert_eq!(
        ast.order_by,
        vec![
            OrderPart("created".into(), Dir::Desc),
            OrderPart("id".into(), Dir::Asc)
        ]
    );
    assert_eq!(ast.limit, Some(10));
    assert_eq!(
        ast.start,
        Some(Bound {
            row: BTreeMap::from([("id".into(), Lit::Int(100))]),
            exclusive: true,
        })
    );

    // where = AND[ simple(closed = false), EXISTS(comment) ]
    let conditions = match ast.r#where.as_ref().unwrap() {
        Condition::And { conditions } => conditions,
        other => panic!("expected And, got {other:?}"),
    };
    assert_eq!(conditions.len(), 2);
    assert_eq!(
        conditions[0],
        Condition::Simple(SimpleCondition {
            op: Op::Eq,
            left: ValuePosition::Column {
                name: "closed".into()
            },
            right: ValuePosition::Literal {
                value: Lit::Bool(false)
            },
        })
    );
    match &conditions[1] {
        Condition::CorrelatedSubquery(CorrelatedSubqueryCondition { related, op, .. }) => {
            assert_eq!(*op, ExistsOp::Exists);
            assert_eq!(related.subquery.table.as_ref(), "comment");
            assert_eq!(related.correlation.parent_field, vec!["id".into()]);
            assert_eq!(related.correlation.child_field, vec!["issueID".into()]);
        }
        other => panic!("expected EXISTS correlatedSubquery, got {other:?}"),
    }

    // related[0] = user AS author, correlated creatorID <-> id
    assert_eq!(ast.related.len(), 1);
    let author = &ast.related[0];
    assert_eq!(author.subquery.table.as_ref(), "user");
    assert_eq!(author.subquery.alias.as_deref(), Some("author"));
    assert_eq!(author.correlation.parent_field, vec!["creatorID".into()]);
    assert_eq!(author.correlation.child_field, vec!["id".into()]);
}

/// Design 226 §6 — ONE literal identity per query text on every home. A JSON home
/// parses `JSON.stringify`'s shortest-round-trip token (`2**60` goes out as
/// `"1152921504606847000"`, NOT the binary `…846976`); serde-wasm-bindgen instead
/// hands the wasm home the binary f64. `canonicalize_wire_number_lits` re-keys the
/// wasm home onto the token rule so a predicate literal or resume cursor denotes
/// the SAME exact value everywhere.
#[test]
fn wasm_number_lits_canonicalize_to_the_wire_token_identity() {
    use rindle::ast::canonicalize_wire_number_lits;

    let binary_2_60 = 1_152_921_504_606_846_976_f64; // == 2^60, what serde-wasm-bindgen visits
                                                     // The Display assumption the walker rests on: Rust formats the same
                                                     // shortest-round-trip digits `JSON.stringify` emits for the integer-form range.
    assert_eq!(format!("{binary_2_60}"), "1152921504606847000");
    // And serde_json parses that token to the SAME `Lit::Int` the walker produces.
    assert_eq!(
        serde_json::from_str::<Lit>("1152921504606847000").unwrap(),
        Lit::Int(1_152_921_504_606_847_000)
    );

    let lit_filter = |value: Lit| {
        Some(Condition::Simple(SimpleCondition {
            op: Op::Eq,
            left: ValuePosition::Column {
                name: "score".into(),
            },
            right: ValuePosition::Literal { value },
        }))
    };

    // The headline: the binary f64 re-keys to the token identity.
    let mut ast = Ast {
        table: "t".into(),
        r#where: lit_filter(Lit::Number(binary_2_60)),
        ..Default::default()
    };
    canonicalize_wire_number_lits(&mut ast);
    assert_eq!(
        ast.r#where,
        lit_filter(Lit::Int(1_152_921_504_606_847_000)),
        "the wasm home must land on the JSON home's literal identity"
    );

    // Start bounds (resume cursors) and IN arrays walk too, and nested subqueries
    // recurse — a cursor echoing a >2^53 sort key is exactly failure mode 2.
    let mut ast = Ast {
        table: "t".into(),
        start: Some(Bound {
            row: BTreeMap::from([("score".into(), Lit::Number(binary_2_60))]),
            exclusive: false,
        }),
        r#where: Some(Condition::And {
            conditions: vec![Condition::Simple(SimpleCondition {
                op: Op::In,
                left: ValuePosition::Column {
                    name: "score".into(),
                },
                right: ValuePosition::Literal {
                    value: Lit::Array(vec![Lit::Number(binary_2_60), Lit::Number(2.5)]),
                },
            })],
        }),
        ..Default::default()
    };
    canonicalize_wire_number_lits(&mut ast);
    assert_eq!(
        ast.start.as_ref().unwrap().row["score"],
        Lit::Int(1_152_921_504_606_847_000)
    );
    let Some(Condition::And { conditions }) = &ast.r#where else {
        panic!("shape preserved")
    };
    let Condition::Simple(SimpleCondition {
        right: ValuePosition::Literal {
            value: Lit::Array(items),
        },
        ..
    }) = &conditions[0]
    else {
        panic!("shape preserved")
    };
    assert_eq!(
        items.as_slice(),
        &[Lit::Int(1_152_921_504_606_847_000), Lit::Number(2.5)]
    );

    // The fallthrough edges stay `Number`, exactly as serde_json's untagged parse
    // of their JS tokens would: non-integral; ±2^63 (whose shortest token
    // "±9223372036854776000" is OUTSIDE i64). Non-finite numbers have NO decimal
    // token at all — `JSON.stringify(NaN)` emits `null` — so their wire identity
    // is `Lit::Null`, and the walker lands there too. Small integrals re-key
    // (serde-wasm-bindgen already visits those as i64, so this is a no-op in vivo).
    for (input, expect) in [
        (Lit::Number(2.5), Lit::Number(2.5)),
        (
            Lit::Number(9_223_372_036_854_775_808.0), // 2^63
            Lit::Number(9_223_372_036_854_775_808.0),
        ),
        (
            Lit::Number(-9_223_372_036_854_775_808.0), // -2^63: token also out of range
            Lit::Number(-9_223_372_036_854_775_808.0),
        ),
        (Lit::Number(f64::NAN), Lit::Null),
        (Lit::Number(f64::INFINITY), Lit::Null),
        (Lit::Number(f64::NEG_INFINITY), Lit::Null),
        (Lit::Number(5.0), Lit::Int(5)),
        (Lit::Number(-0.0), Lit::Int(0)), // JSON.stringify(-0) === "0"
    ] {
        let mut ast = Ast {
            table: "t".into(),
            r#where: lit_filter(input),
            ..Default::default()
        };
        canonicalize_wire_number_lits(&mut ast);
        let Some(Condition::Simple(SimpleCondition {
            right: ValuePosition::Literal { value },
            ..
        })) = &ast.r#where
        else {
            panic!("shape preserved")
        };
        match (value, &expect) {
            // NaN != NaN under PartialEq — compare bitwise for that row.
            (Lit::Number(a), Lit::Number(b)) => assert_eq!(a.to_bits(), b.to_bits()),
            (got, want) => assert_eq!(got, want),
        }
    }
}

/// The walker covers `having` too — at the root AND inside a related subquery.
/// `having` addresses the aggregate's OUTPUT columns (the `group_by` columns plus
/// `count`), and >2^53 BIGINT ids in group columns are exactly what design 226
/// exists for, so a skipped `having` selects different groups on the wasm home
/// than on the JSON authority.
#[test]
fn having_lits_canonicalize_at_every_level() {
    use rindle::ast::canonicalize_wire_number_lits;

    let binary_2_60 = 1_152_921_504_606_846_976_f64;
    let group_pred = |value: Lit| {
        Some(Condition::Simple(SimpleCondition {
            op: Op::Eq,
            left: ValuePosition::Column { name: "grp".into() },
            right: ValuePosition::Literal { value },
        }))
    };

    let mut ast = Ast {
        table: "t".into(),
        group_by: vec!["grp".into()],
        having: group_pred(Lit::Number(binary_2_60)),
        related: vec![CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["t_id".into()],
            },
            subquery: Box::new(Ast {
                table: "c".into(),
                group_by: vec!["grp".into()],
                having: group_pred(Lit::Number(f64::NAN)),
                ..Default::default()
            }),
            system: None,
        }],
        ..Default::default()
    };
    canonicalize_wire_number_lits(&mut ast);

    assert_eq!(
        ast.having,
        group_pred(Lit::Int(1_152_921_504_606_847_000)),
        "root having must land on the JSON home's literal identity"
    );
    assert_eq!(
        ast.related[0].subquery.having,
        group_pred(Lit::Null),
        "nested having walks too, and non-finite lands on the wire's null"
    );
}
