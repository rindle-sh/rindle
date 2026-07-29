#![cfg(feature = "testkit")]

//! Builder / AST lowering of a **top-level aggregate** — `SELECT count(*) FROM t`
//! (global) and `… GROUP BY k` / `… HAVING <pred>` (`REDUCE-DESIGN.md` §4/§8). The
//! fluent [`Query::count`](rindle::query::Query) marks the root `aggregate: Count`,
//! [`group_by`](rindle::query::Query::group_by) records the grouping columns, and
//! [`having`](rindle::query::Query::having) a filter over the post-aggregation rows.
//! The builder lowers it to `source → reduce → [HAVING filter] → View`; the reduce is
//! global (one immortal `[count]` row) or eager-grouped (one `[group…, count]` row per
//! group), and the `HAVING` `Filter`'s edit-split turns a group crossing the predicate
//! threshold into an `Add`/`Remove`, maintaining it incrementally.
//!
//! These tests pin the lowering through the **production `View`**: hydrate, then push.

use rindle::testkit::{ast_view_schema, cleaf, run_push_test_ast_view, TableSpec};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{table, Aggregate, BuildError, SourceChange};

// comment = (id PK, issueID); sort by id. Group by issueID:
//   group 10 → 3 (c1,c2,c3), group 20 → 2 (c4,c5), group 30 → 1 (c6).
fn comments_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}
fn c(id: i64, issue: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue)]
}
fn sources() -> Vec<TableSpec> {
    vec![TableSpec::new(
        "comment",
        comments_schema(),
        vec![c(1, 10), c(2, 10), c(3, 10), c(4, 20), c(5, 20), c(6, 30)],
    )]
}

/// `[issueID, count]` aggregate row.
fn g(issue: i64, count: i64) -> Vec<V> {
    vec![V::Int(issue), V::Int(count)]
}

// --- AST ---------------------------------------------------------------------

#[test]
fn count_marks_root_aggregate_with_group_by_and_having() {
    let ast = table("comment")
        .group_by("issueID")
        .count()
        .having(|h| h.where_op("count", ">", 1))
        .build();
    assert_eq!(ast.aggregate, Some(Aggregate::Count));
    assert_eq!(
        ast.group_by.iter().map(|s| &**s).collect::<Vec<_>>(),
        vec!["issueID"]
    );
    assert!(ast.having.is_some(), "having recorded on the AST");
    // The aggregate reshapes the query itself — no materialized relationships.
    assert!(ast.related.is_empty());
}

// --- view schema (the reduce's synthetic output row) -------------------------

#[test]
fn view_schema_global_is_single_count_column() {
    let ast = table("comment").count().build();
    let s = ast_view_schema(&sources(), &ast).expect("view schema derives");
    assert_eq!(
        s.columns.iter().map(|c| &**c).collect::<Vec<_>>(),
        vec!["count"]
    );
    // A singleton: empty PK + sort (REDUCE-DESIGN §3), no relationships.
    assert!(s.primary_key.is_empty());
    assert!(s.sort.is_empty());
    assert!(s.relationships.is_empty());
}

#[test]
fn view_schema_grouped_is_group_cols_then_count() {
    let ast = table("comment").group_by("issueID").count().build();
    let s = ast_view_schema(&sources(), &ast).expect("view schema derives");
    assert_eq!(
        s.columns.iter().map(|c| &**c).collect::<Vec<_>>(),
        vec!["issueID", "count"]
    );
    // The group column is the PK and sort (REDUCE-DESIGN §8.2).
    assert_eq!(s.primary_key, vec![0]);
    assert_eq!(s.sort, vec![(0, true)]);
    assert!(s.relationships.is_empty());
}

#[test]
fn view_schema_rejects_unknown_group_by_column() {
    let ast = table("comment").group_by("nope").count().build();
    assert!(matches!(
        ast_view_schema(&sources(), &ast),
        Err(BuildError::UnknownColumn(_))
    ));
}

// --- global count through the production View --------------------------------

#[test]
fn global_count_materializes_and_maintains() {
    // count(*) = 6, one row; adding a comment edits the immortal row in place to 7.
    let r = run_push_test_ast_view(
        sources(),
        &table("comment").count().build(),
        vec![("comment", SourceChange::Add(row(c(7, 40))))],
        false,
    )
    .unwrap();
    assert_eq!(r.initial, vec![cleaf(vec![V::Int(6)])]);
    assert_eq!(r.data, vec![cleaf(vec![V::Int(7)])]);
}

// --- grouped count through the production View -------------------------------

#[test]
fn grouped_count_materializes_one_row_per_group() {
    let r = run_push_test_ast_view(
        sources(),
        &table("comment").group_by("issueID").count().build(),
        vec![],
        false,
    )
    .unwrap();
    assert_eq!(
        r.initial,
        vec![cleaf(g(10, 3)), cleaf(g(20, 2)), cleaf(g(30, 1))]
    );
}

// --- HAVING: a filter over the post-aggregation rows ------------------------

fn having_gt1_ast() -> rindle::Ast {
    table("comment")
        .group_by("issueID")
        .count()
        .having(|h| h.where_op("count", ">", 1))
        .build()
}

#[test]
fn having_filters_groups_on_hydrate() {
    // count > 1 excludes group 30 (count 1) at hydrate.
    let r = run_push_test_ast_view(sources(), &having_gt1_ast(), vec![], false).unwrap();
    assert_eq!(r.initial, vec![cleaf(g(10, 3)), cleaf(g(20, 2))]);
}

#[test]
fn having_add_crossing_threshold_appears() {
    // Group 30 (count 1, excluded) gains a comment → 2: the reduce emits Edit{1→2},
    // the HAVING Filter edit-splits (false→true) into an Add, so group 30 appears.
    let r = run_push_test_ast_view(
        sources(),
        &having_gt1_ast(),
        vec![("comment", SourceChange::Add(row(c(7, 30))))],
        false,
    )
    .unwrap();
    assert_eq!(
        r.data,
        vec![cleaf(g(10, 3)), cleaf(g(20, 2)), cleaf(g(30, 2))]
    );
}

#[test]
fn having_remove_crossing_threshold_disappears() {
    // Group 20 (count 2, included) loses a comment → 1: the reduce emits Edit{2→1},
    // the HAVING Filter edit-splits (true→false) into a Remove, so group 20 leaves.
    let r = run_push_test_ast_view(
        sources(),
        &having_gt1_ast(),
        vec![("comment", SourceChange::Remove(row(c(4, 20))))],
        false,
    )
    .unwrap();
    assert_eq!(r.data, vec![cleaf(g(10, 3))]);
}

// --- WHERE filters rows BELOW the reduce -------------------------------------

#[test]
fn where_filters_rows_before_aggregating() {
    // count(comment WHERE issueID >= 20) grouped: only groups 20 and 30 see any rows.
    let ast = table("comment")
        .where_op("issueID", ">=", 20)
        .group_by("issueID")
        .count()
        .build();
    let r = run_push_test_ast_view(sources(), &ast, vec![], false).unwrap();
    assert_eq!(r.initial, vec![cleaf(g(20, 2)), cleaf(g(30, 1))]);
}

// --- guards: v1 rejects ordering / limiting / relating an aggregate root -----

fn assert_unsupported(ast: &rindle::Ast) {
    match run_push_test_ast_view(sources(), ast, vec![], false) {
        Err(BuildError::Unsupported(_)) => {}
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

#[test]
fn aggregate_with_order_by_is_rejected() {
    // Ordering groups (ORDER BY agg) is Tier 2.
    assert_unsupported(
        &table("comment")
            .group_by("issueID")
            .count()
            .order_by("count", "desc")
            .build(),
    );
}

#[test]
fn aggregate_with_limit_is_rejected() {
    // Top-N over groups is Tier 2 (ORDER BY agg + Take).
    assert_unsupported(
        &table("comment")
            .group_by("issueID")
            .count()
            .limit(2)
            .build(),
    );
}

#[test]
fn aggregate_with_related_is_rejected() {
    // The synthetic aggregate row has no relationships.
    let ast = table("comment")
        .group_by("issueID")
        .count()
        .sub_as("self", |r| {
            table("comment").r#where("issueID", r.col("issueID"))
        })
        .build();
    assert_unsupported(&ast);
}

#[test]
fn aggregate_with_select_is_rejected() {
    // A base-column projection has nothing to project on the `[group…, count]` output;
    // rejected rather than silently dropped.
    assert_unsupported(&table("comment").select("id").count().build());
}

#[test]
fn aggregate_with_one_is_rejected() {
    // `.one()` (singular output) is Tier 2 even for a global count (which is already a
    // single row); rejected with its own message, not the limit guard's.
    assert_unsupported(&table("comment").count().one().build());
}

// =============================================================================
// Additional coverage (added during review) — the incremental-maintenance paths
// the committed suite left untested: group birth/death and the cross-group
// split-edit through the production View, empty / down-to-zero global counts,
// compound GROUP BY, global HAVING, and HAVING over a group column.
// =============================================================================

fn empty_comments() -> Vec<TableSpec> {
    vec![TableSpec::new("comment", comments_schema(), vec![])]
}

// --- global count: empty input and maintenance down to zero ------------------

#[test]
fn global_count_empty_is_zero_row() {
    // count(*) over an empty table is one row, value 0 (SQL semantics, §3).
    let r = run_push_test_ast_view(
        empty_comments(),
        &table("comment").count().build(),
        vec![],
        false,
    )
    .unwrap();
    assert_eq!(r.initial, vec![cleaf(vec![V::Int(0)])]);
}

#[test]
fn global_count_maintained_down_to_zero_keeps_the_row() {
    // One row in (count 1); removing it edits the immortal row to 0 — it stays present,
    // it does not disappear (a global count is always exactly one row).
    let r = run_push_test_ast_view(
        vec![TableSpec::new("comment", comments_schema(), vec![c(1, 10)])],
        &table("comment").count().build(),
        vec![("comment", SourceChange::Remove(row(c(1, 10))))],
        false,
    )
    .unwrap();
    assert_eq!(r.initial, vec![cleaf(vec![V::Int(1)])]);
    assert_eq!(r.data, vec![cleaf(vec![V::Int(0)])]);
}

// --- grouped count: group birth / death / cross-group move through the View ---

#[test]
fn grouped_count_group_birth_through_view() {
    // An Add to a brand-new group (40) is born as a fresh `[40, 1]` row (eager §8).
    let r = run_push_test_ast_view(
        sources(),
        &table("comment").group_by("issueID").count().build(),
        vec![("comment", SourceChange::Add(row(c(7, 40))))],
        false,
    )
    .unwrap();
    assert_eq!(
        r.data,
        vec![
            cleaf(g(10, 3)),
            cleaf(g(20, 2)),
            cleaf(g(30, 1)),
            cleaf(g(40, 1))
        ]
    );
}

#[test]
fn grouped_count_group_death_through_view() {
    // Removing the only row of group 30 drains it to 0; the eager reduce deletes the
    // slot and the group leaves the View (no lingering count-0 row).
    let r = run_push_test_ast_view(
        sources(),
        &table("comment").group_by("issueID").count().build(),
        vec![("comment", SourceChange::Remove(row(c(6, 30))))],
        false,
    )
    .unwrap();
    assert_eq!(r.data, vec![cleaf(g(10, 3)), cleaf(g(20, 2))]);
}

#[test]
fn grouped_count_cross_group_edit_moves_count() {
    // Move comment 1 from issue 10 to issue 20. The connection split-edits on the
    // group-by column (§8): the reduce sees Remove(@10)+Add(@20), so group 10 drops
    // 3→2 and group 20 rises 2→3. This is the exact path the `connect(.., group_cols)`
    // split-edit argument exists for — and the one the committed suite never exercised.
    let r = run_push_test_ast_view(
        sources(),
        &table("comment").group_by("issueID").count().build(),
        vec![(
            "comment",
            SourceChange::Edit {
                row: row(c(1, 20)),
                old: row(c(1, 10)),
            },
        )],
        false,
    )
    .unwrap();
    assert_eq!(
        r.data,
        vec![cleaf(g(10, 2)), cleaf(g(20, 3)), cleaf(g(30, 1))]
    );
}

// --- compound GROUP BY (k = 2) -----------------------------------------------

#[test]
fn compound_group_by_two_columns() {
    // post(id PK, board, author); GROUP BY (board, author) → `[board, author, count]`,
    // keyed and sorted by (board, author). Exercises k = 2 partitioning + view schema.
    fn posts_schema() -> SourceSchema {
        SourceSchema::new(vec!["id", "board", "author"], vec![0], vec![(0, true)])
    }
    fn p(id: i64, board: i64, author: i64) -> Vec<V> {
        vec![V::Int(id), V::Int(board), V::Int(author)]
    }
    let srcs = vec![TableSpec::new(
        "post",
        posts_schema(),
        vec![p(1, 1, 100), p(2, 1, 100), p(3, 1, 200), p(4, 2, 100)],
    )];
    let ast = table("post")
        .group_by("board")
        .group_by("author")
        .count()
        .build();
    let r = run_push_test_ast_view(srcs, &ast, vec![], false).unwrap();
    // (board=1, author=100)→2, (1, 200)→1, (2, 100)→1; sorted by (board, author).
    assert_eq!(
        r.initial,
        vec![
            cleaf(vec![V::Int(1), V::Int(100), V::Int(2)]),
            cleaf(vec![V::Int(1), V::Int(200), V::Int(1)]),
            cleaf(vec![V::Int(2), V::Int(100), V::Int(1)]),
        ]
    );
}

// --- HAVING: global, and over a group column ---------------------------------

#[test]
fn global_count_having_appears_then_disappears() {
    // `count(*) HAVING count > 0`: empty → no row; a 0→1 Add crosses the threshold so
    // the row appears, and a 1→0 Remove crosses back so it disappears — the global
    // counterpart of the grouped HAVING edit-split.
    let ast = table("comment")
        .count()
        .having(|h| h.where_op("count", ">", 0))
        .build();
    let (initial, steps) = rindle::testkit::run_push_walk_ast_view(
        empty_comments(),
        &ast,
        vec![
            ("comment", SourceChange::Add(row(c(1, 10)))),
            ("comment", SourceChange::Remove(row(c(1, 10)))),
        ],
    )
    .unwrap();
    assert!(
        initial.is_empty(),
        "count 0 is filtered by HAVING at hydrate"
    );
    assert_eq!(
        steps[0],
        vec![cleaf(vec![V::Int(1)])],
        "0→1 crosses, row appears"
    );
    assert!(steps[1].is_empty(), "1→0 crosses, row disappears");
}

#[test]
fn having_over_a_group_column_filters_groups() {
    // HAVING may also predicate a GROUP BY column (it addresses the reduce's output row
    // `[issueID, count]`): issueID >= 20 keeps groups 20 and 30 at hydrate.
    let ast = table("comment")
        .group_by("issueID")
        .count()
        .having(|h| h.where_op("issueID", ">=", 20))
        .build();
    let r = run_push_test_ast_view(sources(), &ast, vec![], false).unwrap();
    assert_eq!(r.initial, vec![cleaf(g(20, 2)), cleaf(g(30, 1))]);
}

// =============================================================================
// Top-level sum / avg (REDUCE-DESIGN.md §8) — a valued fixture with a `GROUP BY`
// column so both the global and grouped shapes are exercised.
// =============================================================================

// sale = (id PK, cat, amt); sort by id.
//   cat 1 → amts 10, 20  (sum 30, avg 15.0); cat 2 → amt 100 (sum 100, avg 100.0).
fn sales_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "cat", "amt"], vec![0], vec![(0, true)])
}
fn s(id: i64, cat: i64, amt: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(cat), V::Int(amt)]
}
fn sales() -> Vec<TableSpec> {
    vec![TableSpec::new(
        "sale",
        sales_schema(),
        vec![s(1, 1, 10), s(2, 1, 20), s(3, 2, 100)],
    )]
}

#[test]
fn view_schema_global_sum_is_single_sum_column() {
    let ast = table("sale").sum("amt").build();
    let sc = ast_view_schema(&sales(), &ast).expect("view schema derives");
    assert_eq!(
        sc.columns.iter().map(|c| &**c).collect::<Vec<_>>(),
        vec!["sum"]
    );
    assert!(sc.primary_key.is_empty() && sc.sort.is_empty());
}

#[test]
fn view_schema_grouped_avg_is_group_then_avg() {
    let ast = table("sale").group_by("cat").avg("amt").build();
    let sc = ast_view_schema(&sales(), &ast).expect("view schema derives");
    assert_eq!(
        sc.columns.iter().map(|c| &**c).collect::<Vec<_>>(),
        vec!["cat", "avg"]
    );
    assert_eq!(sc.primary_key, vec![0]);
}

#[test]
fn view_schema_rejects_unknown_summed_column() {
    let ast = table("sale").sum("nope").build();
    assert!(matches!(
        ast_view_schema(&sales(), &ast),
        Err(BuildError::UnknownColumn(_))
    ));
}

#[test]
fn global_sum_materializes_and_maintains() {
    // sum(amt) = 130; adding a 5-amt sale edits the immortal row to 135.
    let r = run_push_test_ast_view(
        sales(),
        &table("sale").sum("amt").build(),
        vec![("sale", SourceChange::Add(row(s(4, 2, 5))))],
        false,
    )
    .unwrap();
    assert_eq!(r.initial, vec![cleaf(vec![V::Int(130)])]);
    assert_eq!(r.data, vec![cleaf(vec![V::Int(135)])]);
}

#[test]
fn global_sum_of_empty_is_null() {
    let r = run_push_test_ast_view(
        vec![TableSpec::new("sale", sales_schema(), vec![])],
        &table("sale").sum("amt").build(),
        vec![],
        false,
    )
    .unwrap();
    assert_eq!(r.initial, vec![cleaf(vec![V::Null])]);
}

#[test]
fn global_avg_is_mean_and_maintained_by_in_place_edit() {
    // avg(amt) = 130/3 ≈ 43.333…; editing sale 3's amt 100 → 130 (count steady) moves
    // the mean to 160/3 — the in-place-edit path a top-level `count` would not observe.
    let r = run_push_test_ast_view(
        sales(),
        &table("sale").avg("amt").build(),
        vec![(
            "sale",
            SourceChange::Edit {
                old: row(s(3, 2, 100)),
                row: row(s(3, 2, 130)),
            },
        )],
        false,
    )
    .unwrap();
    assert_eq!(r.initial, vec![cleaf(vec![V::Float(130.0 / 3.0)])]);
    assert_eq!(r.data, vec![cleaf(vec![V::Float(160.0 / 3.0)])]);
}

#[test]
fn grouped_sum_materializes_one_row_per_group() {
    // cat 1 → 30, cat 2 → 100.
    let r = run_push_test_ast_view(
        sales(),
        &table("sale").group_by("cat").sum("amt").build(),
        vec![],
        false,
    )
    .unwrap();
    assert_eq!(
        r.initial,
        vec![
            cleaf(vec![V::Int(1), V::Int(30)]),
            cleaf(vec![V::Int(2), V::Int(100)]),
        ]
    );
}

#[test]
fn grouped_avg_materializes_one_row_per_group() {
    // cat 1 → (10+20)/2 = 15.0, cat 2 → 100.0.
    let r = run_push_test_ast_view(
        sales(),
        &table("sale").group_by("cat").avg("amt").build(),
        vec![],
        false,
    )
    .unwrap();
    assert_eq!(
        r.initial,
        vec![
            cleaf(vec![V::Int(1), V::Float(15.0)]),
            cleaf(vec![V::Int(2), V::Float(100.0)]),
        ]
    );
}

#[test]
fn grouped_sum_having_filters_on_the_sum_column() {
    // HAVING sum > 50 keeps only cat 2 (sum 100) at hydrate.
    let ast = table("sale")
        .group_by("cat")
        .sum("amt")
        .having(|h| h.where_op("sum", ">", 50))
        .build();
    let r = run_push_test_ast_view(sales(), &ast, vec![], false).unwrap();
    assert_eq!(r.initial, vec![cleaf(vec![V::Int(2), V::Int(100)])]);
}
