//! Exact-i64 differential parity (design 226 Stage D — the §9 un-masking).
//!
//! These lanes grade Stage B's exact comparators/keys and Stage C5's i128 `sum`
//! **against SQLite**, over boundary integers the pre-226 harness had to exclude:
//! `2^53 ± 1`, `i64::MAX`, `i64::MIN`. The fixtures carry `Int` *cells* in untyped
//! (`Number`-declared) columns — the engine has always admitted those on the memory
//! plane, and §8's gate keys on **declared** `int64` columns, so this is exactly the
//! differential surface reachable before Stage E lifts the gate. The oracle side
//! seeds bare-affinity SQLite columns (INTEGER storage for integer literals) and
//! compares exactly, so every equality, ordering, join-key, and group decision here
//! is graded by SQLite's own exact numeric semantics.
//!
//! The overflow lane resolves the design's OQ3: rindle-d2s surfaces SQLite's
//! integer-overflow **distinctly** (`is_integer_overflow`), so a sum whose set total
//! leaves i64 grades as *vacuously passing* — both sides refuse — rather than as a
//! divergence or a harness panic.

use rindle::change::SourceChange;
use rindle::testkit::{
    ast_view_schema, run_fetch_test_ast_view, run_fetch_test_ast_view_fallible,
    run_push_test_ast_view, TableSpec,
};
use rindle::value::owned_row as row;
use rindle::{table, Ast, Lit, OwnedValue as V, SourceSchema};
use rindle_d2s::{Cardinality, Catalog};
use rindle_testfix::parity::{caught_tree, is_integer_overflow, run_oracle_result};
use serde_json::Value;

const P53: i64 = 9_007_199_254_740_992; // 2^53
const UNSAFE: i64 = 9_007_199_254_740_993; // 2^53 + 1: first integer with no exact f64

// ── fixture: ledger ─many(entries)→ entry ───────────────────────────────────────
//
// `amount` carries the boundary integers; `entry.ref_amount` mirrors them so joins
// key on exact values. All columns are UNDECLARED (`Number`-typed) — the cells are
// `Int`, which is the point: exact semantics must hold for the values themselves.

fn sources() -> Vec<TableSpec> {
    vec![
        TableSpec::new(
            "ledger",
            SourceSchema::new(vec!["id", "ref", "amount"], vec![0], vec![(0, true)]),
            vec![
                vec![V::Int(1), V::str("a"), V::Int(UNSAFE)],
                vec![V::Int(2), V::str("b"), V::Int(P53)], // the f64-neighbor of row 1
                vec![V::Int(3), V::str("c"), V::Int(i64::MAX)],
                vec![V::Int(4), V::str("d"), V::Int(i64::MIN)],
                vec![V::Int(5), V::str("e"), V::Int(-1)],
                vec![V::Int(6), V::str("a"), V::Int(1)],
            ],
        ),
        TableSpec::new(
            "entry",
            SourceSchema::new(vec!["eid", "ref_amount"], vec![0], vec![(0, true)]),
            vec![
                vec![V::Int(10), V::Int(UNSAFE)],
                vec![V::Int(11), V::Int(P53)],
                vec![V::Int(12), V::Int(UNSAFE)],
            ],
        ),
    ]
}

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.table("ledger", &["id", "ref", "amount"], &["id"]);
    c.table("entry", &["eid", "ref_amount"], &["eid"]);
    c.relationship("ledger", "entries", Cardinality::Many);
    c
}

/// Hydrate `ast` on the memory IVM and against the d2s-compiled SQLite oracle, and
/// assert the JSON trees are equal. Deliberately NO `canon` collapse here: every value
/// in these fixtures must already render identically on both sides — an exact i64 is
/// a JSON integer whichever side produced it, and hiding a rendering difference is
/// exactly what this lane exists to stop doing.
fn assert_exact_parity(ast: &Ast) {
    let schema = ast_view_schema(&sources(), ast).expect("derive view schema");
    let nodes = run_fetch_test_ast_view(sources(), ast).expect("ivm hydrate");
    let ivm = caught_tree(&nodes, &schema);
    let (oracle, sql) = match run_oracle_result(&sources(), &catalog(), ast, &[]) {
        (Ok(v), sql) => (v, sql),
        (Err(e), sql) => panic!("oracle: {e}\nSQL:\n{sql}"),
    };
    assert_eq!(
        ivm, oracle,
        "IVM != oracle (exact, uncanonicalized)\n--- IVM ---\n{ivm:#}\n--- ORACLE ---\n{oracle:#}\n--- SQL ---\n{sql}"
    );
}

/// The projected `amount` cells of the IVM tree, in view order.
fn amounts(tree: &Value) -> Vec<Value> {
    tree.as_array()
        .expect("root array")
        .iter()
        .map(|row| row["amount"].clone())
        .collect()
}

// ── ordering across the f64 ceiling ─────────────────────────────────────────────

#[test]
fn parity_order_by_boundary_amounts() {
    // i64::MIN < -1 < 1 < 2^53 < 2^53+1 < i64::MAX — the two middle neighbors
    // collapse to the same f64, so any widened comparator ties them arbitrarily;
    // SQLite and the exact engine must both produce this exact order.
    let ast = table("ledger").order_by("amount", "asc").build();
    assert_exact_parity(&ast);

    let schema = ast_view_schema(&sources(), &ast).expect("schema");
    let nodes = run_fetch_test_ast_view(sources(), &ast).expect("ivm");
    let got = amounts(&caught_tree(&nodes, &schema));
    let want: Vec<Value> = [i64::MIN, -1, 1, P53, UNSAFE, i64::MAX]
        .iter()
        .map(|&i| serde_json::json!(i))
        .collect();
    assert_eq!(got, want, "exact ascending order across the 2^53 boundary");
}

// ── predicate equality at the boundary ──────────────────────────────────────────

#[test]
fn parity_exact_int_literal_matches_only_its_row() {
    // WHERE amount = 2^53+1 (exact Lit::Int) must match row 1 and NOT its
    // f64-neighbor row 2 — under the old widened literal both engines' agreement
    // was vacuous because the generator could never even write this literal.
    let ast = table("ledger")
        .r#where("amount", Lit::Int(UNSAFE))
        .order_by("id", "asc")
        .build();
    assert_exact_parity(&ast);

    let schema = ast_view_schema(&sources(), &ast).expect("schema");
    let nodes = run_fetch_test_ast_view(sources(), &ast).expect("ivm");
    assert_eq!(
        amounts(&caught_tree(&nodes, &schema)),
        vec![serde_json::json!(UNSAFE)],
        "exactly the 2^53+1 row"
    );
}

#[test]
fn parity_float_literal_matches_its_exact_int_twin() {
    // WHERE amount = 9007199254740992.0 (a float literal): SQLite's exact numeric
    // compare and §5.4's canonical keys both say the REAL equals INTEGER 2^53 and
    // nothing else — in particular not 2^53+1.
    let ast = table("ledger")
        .r#where("amount", Lit::Number(P53 as f64))
        .order_by("id", "asc")
        .build();
    assert_exact_parity(&ast);

    let schema = ast_view_schema(&sources(), &ast).expect("schema");
    let nodes = run_fetch_test_ast_view(sources(), &ast).expect("ivm");
    assert_eq!(
        amounts(&caught_tree(&nodes, &schema)),
        vec![serde_json::json!(P53)],
        "the float literal hits only its exactly-equal integer twin"
    );
}

#[test]
fn parity_range_predicates_at_the_boundary() {
    // Strict bounds around the ceiling: > 2^53 keeps exactly {2^53+1, i64::MAX};
    // >= i64::MAX keeps exactly the MAX row; < i64::MIN + 1 keeps exactly MIN.
    for (op, lit) in [
        (">", Lit::Int(P53)),
        (">=", Lit::Int(i64::MAX)),
        ("<", Lit::Int(i64::MIN + 1)),
    ] {
        let ast = table("ledger")
            .where_op("amount", op, lit)
            .order_by("id", "asc")
            .build();
        assert_exact_parity(&ast);
    }
}

// ── joins keyed on exact values ─────────────────────────────────────────────────

#[test]
fn parity_related_join_on_exact_boundary_key() {
    // ledger.amount = entry.ref_amount: the 2^53+1 parent joins its two exact
    // entries, the 2^53 parent joins exactly one — a widened join key would fuse
    // the two partner sets.
    let ast = table("ledger")
        .sub_as("entries", |row| {
            table("entry").r#where("ref_amount", row.col("amount"))
        })
        .order_by("id", "asc")
        .build();
    assert_exact_parity(&ast);
}

#[test]
fn parity_exists_on_exact_boundary_key() {
    // EXISTS over the same exact correlation: rows 1 (2^53+1) and 2 (2^53) qualify;
    // MAX/MIN/±1 do not.
    let ast = table("ledger")
        .where_exists(|row| {
            table("entry")
                .alias("hasEntry")
                .r#where("ref_amount", row.col("amount"))
        })
        .order_by("id", "asc")
        .build();
    assert_exact_parity(&ast);
}

// ── the i128 sum plane, graded against SQLite (§5.3 + OQ3) ─────────────────────

#[test]
fn parity_sum_exact_above_the_f64_ceiling() {
    // sum(entries.ref_amount) for the 2^53+1 parent is 2·(2^53+1) = 2^54+2 — an
    // all-integer total no f64 can hold. The i128 accumulator and SQLite's integer
    // sum must agree exactly.
    let ast = table("ledger")
        .sum_as("total", "ref_amount", |row| {
            table("entry").r#where("ref_amount", row.col("amount"))
        })
        .order_by("id", "asc")
        .build();
    assert_exact_parity(&ast);
}

#[test]
fn sum_overflow_grades_vacuously_on_both_sides() {
    // A set total outside i64 (i64::MAX + 1): the engine raises §5.3's typed error at
    // the hydrate boundary and SQLite raises its own "integer overflow" — the design's
    // OQ3 required proof that d2s surfaces the latter DISTINCTLY, so the harness can
    // grade "both refused" as a vacuous pass instead of a divergence.
    let sources = vec![
        TableSpec::new(
            "ledger",
            SourceSchema::new(vec!["id", "ref", "amount"], vec![0], vec![(0, true)]),
            vec![vec![V::Int(1), V::str("a"), V::Int(0)]],
        ),
        TableSpec::new(
            "entry",
            SourceSchema::new(vec!["eid", "ref_amount", "val"], vec![0], vec![(0, true)]),
            vec![
                vec![V::Int(10), V::Int(0), V::Int(i64::MAX)],
                vec![V::Int(11), V::Int(0), V::Int(1)],
                vec![V::Int(12), V::Int(0), V::Int(0)],
            ],
        ),
    ];
    let mut catalog = Catalog::new();
    catalog.table("ledger", &["id", "ref", "amount"], &["id"]);
    catalog.table("entry", &["eid", "ref_amount", "val"], &["eid"]);
    catalog.relationship("ledger", "entries", Cardinality::Many);
    let ast = table("ledger")
        .sum_as("total", "val", |row| {
            table("entry").r#where("ref_amount", row.col("amount"))
        })
        .build();

    let ivm = run_fetch_test_ast_view_fallible(sources.clone(), &ast).expect("build");
    let ivm_err = ivm.expect_err("the engine must refuse the unrepresentable sum");
    assert!(
        ivm_err.to_string().contains("integer sum overflow"),
        "typed §5.3 error, got: {ivm_err}"
    );

    let (oracle, sql) = run_oracle_result(&sources, &catalog, &ast, &[]);
    let oracle_err = oracle.expect_err("SQLite must also refuse the unrepresentable sum");
    assert!(
        is_integer_overflow(&oracle_err),
        "d2s must surface SQLite's integer overflow distinctly (OQ3); got: {oracle_err}\nSQL:\n{sql}"
    );

    // The recognizer is specific: an ordinary failure is NOT graded vacuous.
    let bogus = rusqlite::Connection::open_in_memory()
        .unwrap()
        .query_row("SELECT * FROM missing_table", [], |_| Ok(()))
        .expect_err("bogus query errors");
    assert!(!is_integer_overflow(&bogus));
}

// ── the push path (§9 gap closed): maintained-view parity over incremental writes ──

/// The push-path twin of [`assert_exact_parity`]: hydrate over the base fixture,
/// apply `pushes` INCREMENTALLY through the maintained pipeline (the production
/// `ArrayView`), and grade the FINAL tree against a fresh oracle over base+pushes —
/// view-after-write == fresh-query, uncanonicalized, at the boundary values. A
/// defect confined to the push/probe path (a wrong `CanonVal` probe, a stale state
/// key on Remove, a mis-keyed same-PK edit) escapes every hydrate-only lane above;
/// these lanes close that gap.
fn assert_exact_push_parity(ast: &Ast, pushes: Vec<(&'static str, SourceChange)>) {
    let schema = ast_view_schema(&sources(), ast).expect("derive view schema");
    let result = run_push_test_ast_view(sources(), ast, pushes.clone(), false).expect("ivm build");
    let ivm = caught_tree(&result.data, &schema);
    let (oracle, sql) = match run_oracle_result(&sources(), &catalog(), ast, &pushes) {
        (Ok(v), sql) => (v, sql),
        (Err(e), sql) => panic!("oracle: {e}\nSQL:\n{sql}"),
    };
    assert_eq!(
        ivm, oracle,
        "push-path IVM != oracle (exact, uncanonicalized)\n--- IVM ---\n{ivm:#}\n--- ORACLE ---\n{oracle:#}\n--- SQL ---\n{sql}"
    );
}

#[test]
fn parity_push_join_probe_at_the_ceiling() {
    // Incremental Adds/Removes of child rows keyed at 2^53/2^53+1: every push
    // probes the join through `CanonVal` (fetch-during-push), where the hydrate
    // lane only ever scanned. The Remove must find — and only find — the exact
    // partner's state.
    let ast = table("ledger")
        .sub_as("entries", |row| {
            table("entry").r#where("ref_amount", row.col("amount"))
        })
        .order_by("id", "asc")
        .build();
    assert_exact_push_parity(
        &ast,
        vec![
            (
                "entry",
                SourceChange::Add(row(vec![V::Int(13), V::Int(UNSAFE)])),
            ),
            (
                "entry",
                SourceChange::Add(row(vec![V::Int(14), V::Int(P53)])),
            ),
            (
                "entry",
                SourceChange::Add(row(vec![V::Int(15), V::Int(i64::MAX)])),
            ),
            (
                "entry",
                SourceChange::Remove(row(vec![V::Int(10), V::Int(UNSAFE)])),
            ),
        ],
    );
}

#[test]
fn parity_push_order_take_displacement_across_the_ceiling() {
    // ORDER BY amount DESC LIMIT 3 maintained across boundary displacement: adding
    // 2^53+2 (between the neighbors) and removing i64::MAX both re-rank rows whose
    // sort keys are f64-indistinguishable — the Take backfill probe and the
    // displacement compare must both run exact.
    let ast = table("ledger").order_by("amount", "desc").limit(3).build();
    assert_exact_push_parity(
        &ast,
        vec![
            (
                "ledger",
                SourceChange::Add(row(vec![V::Int(7), V::str("g"), V::Int(UNSAFE + 1)])),
            ),
            (
                "ledger",
                SourceChange::Remove(row(vec![V::Int(3), V::str("c"), V::Int(i64::MAX)])),
            ),
        ],
    );
}

#[test]
fn parity_push_same_pk_edit_crosses_the_ceiling() {
    // A same-PK Edit moving amount between the f64-equal neighbors 2^53+1 → 2^53:
    // the maintained order must relocate the row exactly (a widened comparator sees
    // no move at all), and the join partners must re-key.
    let ast = table("ledger")
        .sub_as("entries", |row| {
            table("entry").r#where("ref_amount", row.col("amount"))
        })
        .order_by("amount", "asc")
        .build();
    assert_exact_push_parity(
        &ast,
        vec![(
            "ledger",
            SourceChange::Edit {
                old: row(vec![V::Int(1), V::str("a"), V::Int(UNSAFE)]),
                row: row(vec![V::Int(1), V::str("a"), V::Int(P53)]),
            },
        )],
    );
}
