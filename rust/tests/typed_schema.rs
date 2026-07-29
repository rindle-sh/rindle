//! Typed engine schemas (design 226 §4.1, Stages C1–C2): `column_types` parallel
//! to `columns` on [`SourceSchema`]/[`Schema`], propagated structurally through
//! the view-schema derivation and the reduce's synthetic output schema. Still
//! **dark** from the product's view — no decltype maps to `Int` until C4 lifts
//! the `BIGINT`/`INT8` refusal, and the builder gate lands in C3 — so these
//! tests pin the plumbing: defaults, the `with_column_types` builder, and every
//! derivation path preserving/deriving types exactly as §4.1 specifies
//! (projection keeps the full-width types; join children retain theirs; group
//! keys preserve input types; `count`/`avg` output `Number`; `sum` follows its
//! input column — `Int` over the declared exact plane, `Number` otherwise).

use rindle::value::{Schema, SourceSchema, ValueType};

// --- the structs -------------------------------------------------------------

#[test]
fn constructors_default_every_column_to_number() {
    let s = Schema::new(vec!["id", "title", "open"], vec![0], vec![(0, true)]);
    assert_eq!(s.column_types, vec![ValueType::Number; 3]);
    let ss = SourceSchema::new(vec!["id", "title"], vec![0], vec![(0, true)]);
    assert_eq!(ss.column_types, vec![ValueType::Number; 2]);
}

#[test]
fn with_column_types_installs_declared_types_and_into_schema_preserves_them() {
    let types = vec![ValueType::Number, ValueType::String, ValueType::Boolean];
    let ss = SourceSchema::new(vec!["id", "title", "open"], vec![0], vec![(0, true)])
        .with_column_types(types.clone());
    assert_eq!(ss.column_types, types);
    let s = ss.into_schema();
    assert_eq!(s.column_types, types, "into_schema carries the types");

    let s2 = Schema::new(vec!["id", "meta"], vec![0], vec![(0, true)])
        .with_column_types(vec![ValueType::Number, ValueType::Json]);
    assert_eq!(s2.column_types, vec![ValueType::Number, ValueType::Json]);
}

#[test]
#[should_panic(expected = "column_types arity")]
fn schema_with_column_types_rejects_wrong_arity() {
    let _ = Schema::new(vec!["id", "title"], vec![0], vec![(0, true)])
        .with_column_types(vec![ValueType::Number]);
}

#[test]
#[should_panic(expected = "column_types arity")]
fn source_schema_with_column_types_rejects_wrong_arity() {
    let _ = SourceSchema::new(vec!["id"], vec![0], vec![(0, true)])
        .with_column_types(vec![ValueType::Number, ValueType::String]);
}

// --- propagation through the view-schema derivation --------------------------

#[cfg(feature = "testkit")]
mod propagation {
    use rindle::op::Reduce;
    use rindle::testkit::{ast_view_schema, TableSpec};
    use rindle::value::{Schema, SourceSchema, ValueType};
    use rindle::{table, Ast};

    /// issue = (id: Number, title: String, open: Boolean, meta: Json).
    fn issues_schema() -> SourceSchema {
        SourceSchema::new(
            vec!["id", "title", "open", "meta"],
            vec![0],
            vec![(0, true)],
        )
        .with_column_types(vec![
            ValueType::Number,
            ValueType::String,
            ValueType::Boolean,
            ValueType::Json,
        ])
    }
    /// comment = (id: Number, issue: Number, body: String, val: Number) — the
    /// aggregate child; `body` proves a non-Number group key propagates.
    fn comments_schema() -> SourceSchema {
        SourceSchema::new(vec!["id", "issue", "body", "val"], vec![0], vec![(0, true)])
            .with_column_types(vec![
                ValueType::Number,
                ValueType::Number,
                ValueType::String,
                ValueType::Number,
            ])
    }
    fn sources() -> Vec<TableSpec> {
        vec![
            TableSpec::new("issue", issues_schema(), vec![]),
            TableSpec::new("comment", comments_schema(), vec![]),
        ]
    }
    fn derive(ast: &Ast) -> Schema {
        ast_view_schema(&sources(), ast).expect("view schema derives")
    }

    #[test]
    fn base_types_flow_into_the_view_schema() {
        let schema = derive(&table("issue").build());
        assert_eq!(schema.column_types, issues_schema().column_types);
    }

    #[test]
    fn projection_keeps_the_full_width_types() {
        // §2.1: `columns` stays the full positional list under a projection, and so
        // must `column_types` — the PK/sort/type indices remain valid in the full
        // column space.
        let schema = derive(&table("issue").select("title").build());
        assert_eq!(schema.projection, Some(vec![1]));
        assert_eq!(schema.column_types, issues_schema().column_types);
    }

    #[test]
    fn related_child_schema_carries_the_child_types() {
        let ast = table("issue")
            .sub_as("comments", |r| {
                table("comment").r#where("issue", r.col("id"))
            })
            .build();
        let schema = derive(&ast);
        let child = schema.relationships[0]
            .child
            .as_deref()
            .expect("in-view child");
        assert_eq!(child.column_types, comments_schema().column_types);
    }

    #[test]
    fn relationship_count_group_key_preserves_and_count_is_number() {
        // Correlate on the String column so the group key visibly PRESERVES the
        // child's declared type instead of defaulting.
        let ast = table("issue")
            .count_as("byBody", |r| {
                table("comment").r#where("body", r.col("title"))
            })
            .build();
        let schema = derive(&ast);
        let child = schema.relationships[0]
            .child
            .as_deref()
            .expect("aggregate child");
        assert_eq!(
            child.columns.iter().map(|c| &**c).collect::<Vec<_>>(),
            vec!["body", "count"]
        );
        assert_eq!(
            child.column_types,
            vec![ValueType::String, ValueType::Number]
        );
    }

    #[test]
    fn relationship_sum_and_avg_output_number_today() {
        for ast in [
            table("issue")
                .sum_as("valTotal", "val", |r| {
                    table("comment").r#where("issue", r.col("id"))
                })
                .build(),
            table("issue")
                .avg_as("valAvg", "val", |r| {
                    table("comment").r#where("issue", r.col("id"))
                })
                .build(),
        ] {
            let schema = derive(&ast);
            let child = schema.relationships[0]
                .child
                .as_deref()
                .expect("aggregate child");
            // Group key `issue` is Number; the aggregate column is Number (`sum`
            // follows its input column — see the `int64` section for the Int case).
            assert_eq!(
                child.column_types,
                vec![ValueType::Number, ValueType::Number]
            );
        }
    }

    #[test]
    fn top_level_aggregate_types_group_keys_from_the_source() {
        let grouped = derive(&table("issue").group_by("title").count().build());
        assert_eq!(
            grouped.columns.iter().map(|c| &**c).collect::<Vec<_>>(),
            vec!["title", "count"]
        );
        assert_eq!(
            grouped.column_types,
            vec![ValueType::String, ValueType::Number]
        );

        let global = derive(&table("issue").count().build());
        assert_eq!(global.column_types, vec![ValueType::Number]);
    }

    /// The MUST-mirror invariant (`REDUCE-DESIGN.md` §8/§9) now covers types: the
    /// reduce's synthetic output schema derives the same `column_types` as the
    /// view-schema twin, via [`Reduce::with_input_types`].
    #[test]
    fn reduce_output_types_mirror_the_view_schema_twin() {
        use rindle::graph::{NodeId, StorageId};
        use rindle::op::AggSpec;
        let input = issues_schema().into_schema();
        let node = NodeId::new(0, 0);
        let storage = StorageId { idx: 0, gen: 0 };
        let reduce = Reduce::grouped_agg(node, storage, vec![1], vec!["title"], AggSpec::Count)
            .with_input_types(&input);
        let twin = derive(&table("issue").group_by("title").count().build());
        assert_eq!(reduce.schema.column_types, twin.column_types);
        assert_eq!(
            reduce.schema.column_types,
            vec![ValueType::String, ValueType::Number]
        );

        let global = Reduce::global_agg(node, storage, AggSpec::Sum(3))
            .with_input_types(&comments_schema().into_schema());
        assert_eq!(global.schema.column_types, vec![ValueType::Number]);
    }

    // --- the exact-i64 plane (design 226, Stage C2) ---------------------------

    /// ledger = (id: Number, ref: String, amount: Int) — a declared `int64` column.
    fn ledger_schema() -> SourceSchema {
        SourceSchema::new(vec!["id", "ref", "amount"], vec![0], vec![(0, true)])
            .with_column_types(vec![ValueType::Number, ValueType::String, ValueType::Int])
    }
    fn ledger_sources() -> Vec<TableSpec> {
        vec![
            TableSpec::new("issue", issues_schema(), vec![]),
            TableSpec::new("ledger", ledger_schema(), vec![]),
        ]
    }
    fn derive_ledger(ast: &Ast) -> Schema {
        ast_view_schema(&ledger_sources(), ast).expect("view schema derives")
    }

    #[test]
    fn int_columns_flow_into_the_view_schema() {
        let schema = derive_ledger(&table("ledger").build());
        assert_eq!(schema.column_types, ledger_schema().column_types);
    }

    /// §4.1: `sum` over an `Int` input derives `Int`; `count`/`avg` over the same
    /// input stay `Number` — top-level and relationship shapes agree, and the
    /// reduce twin mirrors.
    #[test]
    fn sum_over_an_int_column_derives_int_and_count_avg_stay_number() {
        let sum = derive_ledger(&table("ledger").group_by("ref").sum("amount").build());
        assert_eq!(sum.column_types, vec![ValueType::String, ValueType::Int]);
        let avg = derive_ledger(&table("ledger").group_by("ref").avg("amount").build());
        assert_eq!(avg.column_types, vec![ValueType::String, ValueType::Number]);
        let count = derive_ledger(&table("ledger").group_by("ref").count().build());
        assert_eq!(
            count.column_types,
            vec![ValueType::String, ValueType::Number]
        );

        // The relationship shape: issue { total: sum(ledger.amount) }.
        let rel = derive_ledger(
            &table("issue")
                .sum_as("total", "amount", |r| {
                    table("ledger").r#where("id", r.col("id"))
                })
                .build(),
        );
        let child = rel.relationships[0]
            .child
            .as_deref()
            .expect("aggregate child");
        assert_eq!(child.column_types, vec![ValueType::Number, ValueType::Int]);

        // The dataflow twin (MUST-mirror, now for Int): sum(amount)=col 2 grouped
        // by ref=col 1.
        use rindle::graph::{NodeId, StorageId};
        use rindle::op::AggSpec;
        let reduce = Reduce::grouped_agg(
            NodeId::new(0, 0),
            StorageId { idx: 0, gen: 0 },
            vec![1],
            vec!["ref"],
            AggSpec::Sum(2),
        )
        .with_input_types(&ledger_schema().into_schema());
        let twin = derive_ledger(&table("ledger").group_by("ref").sum("amount").build());
        assert_eq!(reduce.schema.column_types, twin.column_types);
    }
}

// --- the §8 sync-boundary gate (design 226, Stage C3) -------------------------

#[cfg(feature = "testkit")]
mod gate {
    use rindle::builder::BuildError;
    use rindle::testkit::{run_fetch_test_ast, TableSpec};
    use rindle::value::{SourceSchema, ValueType};
    use rindle::{table, Ast};

    /// ledger = (id: Number PK, ref: String, amount: Int).
    fn ledger_schema() -> SourceSchema {
        SourceSchema::new(vec!["id", "ref", "amount"], vec![0], vec![(0, true)])
            .with_column_types(vec![ValueType::Number, ValueType::String, ValueType::Int])
    }
    /// account = (key: Int PK, name: String) — an int64 PRIMARY KEY.
    fn account_schema() -> SourceSchema {
        SourceSchema::new(vec!["key", "name"], vec![0], vec![(0, true)])
            .with_column_types(vec![ValueType::Int, ValueType::String])
    }
    /// issue = (id: Number PK, title: String) — no int64 anywhere.
    fn issue_schema() -> SourceSchema {
        SourceSchema::new(vec!["id", "title"], vec![0], vec![(0, true)])
            .with_column_types(vec![ValueType::Number, ValueType::String])
    }
    fn sources() -> Vec<TableSpec> {
        vec![
            TableSpec::new("ledger", ledger_schema(), vec![]),
            TableSpec::new("account", account_schema(), vec![]),
            TableSpec::new("issue", issue_schema(), vec![]),
        ]
    }

    fn refused(ast: &Ast) -> bool {
        matches!(
            run_fetch_test_ast(sources(), ast),
            Err(BuildError::Int64ColumnUnsupported { .. })
        )
    }
    fn admissible(ast: &Ast) {
        run_fetch_test_ast(sources(), ast).expect("query outside the int64 footprint builds");
    }

    #[test]
    fn full_row_selection_of_an_int64_table_is_refused() {
        assert!(refused(&table("ledger").build()));
        // The refusal names the offending column.
        match run_fetch_test_ast(sources(), &table("ledger").build()) {
            Err(BuildError::Int64ColumnUnsupported { table, column }) => {
                assert_eq!(&*table, "ledger");
                assert_eq!(&*column, "amount");
            }
            other => panic!("expected the int64 refusal, got {other:?}"),
        }
    }

    #[test]
    fn a_projection_excluding_the_int64_column_stays_admissible() {
        admissible(&table("ledger").select("id").select("ref").build());
        // …and a query on an int64-free table is untouched by the gate.
        admissible(&table("issue").build());
    }

    #[test]
    fn every_footprint_shape_counts_not_just_the_projection() {
        // Selected field.
        assert!(refused(&table("ledger").select("amount").build()));
        // Predicate operand (projection excludes it).
        assert!(refused(
            &table("ledger")
                .select("ref")
                .where_op("amount", ">", 5)
                .build()
        ));
        // orderBy key.
        assert!(refused(
            &table("ledger")
                .select("ref")
                .order_by("amount", "asc")
                .build()
        ));
        // Grouping key.
        assert!(refused(
            &table("ledger")
                .select("ref")
                .group_by("amount")
                .count()
                .build()
        ));
        // Aggregate input.
        assert!(refused(
            &table("ledger").select("ref").sum("amount").build()
        ));
        // Paging bound.
        assert!(refused(
            &table("ledger")
                .select("ref")
                .order_by("ref", "asc")
                .start_after("amount", 5)
                .build()
        ));
        // PK auto-inclusion: the int64 PRIMARY KEY rides along under any projection.
        assert!(refused(&table("account").select("name").build()));
    }

    #[test]
    fn relationship_and_exists_frames_are_walked() {
        // A related child materializing full int64 rows.
        assert!(refused(
            &table("issue")
                .sub_as("entries", |r| table("ledger").r#where("id", r.col("id")))
                .build()
        ));
        // An EXISTS child ships full rows on the normalized plane — footprint too.
        assert!(refused(
            &table("issue")
                .where_exists(|r| table("ledger").r#where("id", r.col("id")))
                .build()
        ));
        // A correlation ON the int64 column refuses even under a child projection.
        assert!(refused(
            &table("issue")
                .sub_as("entries", |r| {
                    table("ledger").select("ref").r#where("amount", r.col("id"))
                })
                .build()
        ));
        // An int64-free child keeps the parent admissible.
        admissible(
            &table("issue")
                .sub_as("more", |r| table("issue").r#where("id", r.col("id")))
                .build(),
        );
    }

    #[test]
    fn a_count_relationship_onto_an_int64_table_is_refused() {
        // The relationship aggregate's child frame (select None) is in footprint.
        assert!(refused(
            &table("issue")
                .count_as("entryCount", |r| table("ledger").r#where("id", r.col("id")))
                .build()
        ));
    }
}
