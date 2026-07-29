//! Differential **parity harness** — drive one [`Ast`] through the real IVM `View`
//! and through `rindle-d2s` + SQLite over the *same* data, then assert the two
//! materialized JSON trees are equal. Two flavours:
//!
//! * [`assert_hydrate_parity`] — the initial materialize (`View::hydrate`) matches
//!   the query compiled to a single SQLite `SELECT`.
//!
//! * [`assert_push_parity`] — hydrate, then apply a batch of [`SourceChange`]s
//!   *incrementally* to the `View` **and** as plain SQL mutations to the oracle DB,
//!   re-run the compiled `SELECT`, and assert the incrementally-maintained
//!   `view.data` equals the freshly-recomputed oracle (it also re-checks the
//!   pre-push hydrate, so each push test is self-contained).
//!
//! The pushes drive both sides from the identical seed, so the oracle is a
//! *recompute-from-scratch* ground truth for the engine's incremental maintenance.
//!
//! The assertions are fixture-agnostic (any `Vec<TableSpec>` + matching `Catalog`),
//! but the bundled [`crate::chinook`] module is the intended source of realistic data.

#![allow(dead_code)] // a shared harness exposes more than any single test file uses.

use rindle::testkit::{
    ast_view_schema, run_fetch_test_ast_view, run_push_test_ast_view, run_push_walk_ast_view,
    TableSpec,
};
use rindle::{
    resolve_scalars, Ast, CaughtNode, Condition, ExistsOp, MemorySource, OwnedRow, OwnedValue as V,
    RelId, ScalarCatalog, ScalarSource, Schema, SourceChange, SourceSchema,
};
use rindle_d2s::{compile, Catalog};
use rusqlite::Connection;
use serde_json::{Map, Value};

// ── the SUT seam (FUZZER-SCOPE-LIFT-DESIGN.md) ─────────────────────────────────────
//
// The differential asserts compare an IVM result tree against the SQLite recompute
// oracle. *What produces that tree* is the only thing that changes as the fuzzer is
// lifted up the stack: the in-process `View` (scope 1, [`ViewSut`]) → `Db` → `Cluster`
// → `rindled` → replicator. Everything else here (the oracle, `assert_trees_eq`, the
// generators upstream) is scope-invariant. A driver returns the materialized tree as
// the oracle-comparable JSON [`caught_tree`] produces.

/// A system-under-test the parity asserts can drive. Implementors seed `sources`,
/// run `ast`, and return its materialized result tree **after the relevant transaction
/// has fully propagated** — so the intra-transaction ordering non-determinism of the
/// concurrent/networked tiers is invisible (lock-step at transaction boundaries,
/// design §4).
pub trait Sut {
    /// Seed `sources`, hydrate `ast`, and return the converged hydrate tree.
    fn hydrate(&mut self, sources: &[TableSpec], ast: &Ast) -> Value;

    /// Hydrate, then apply each push as one committed transaction, returning the hydrate
    /// tree (`.0`) and the converged tree after each push (`.1[k]`). Lock-step: each tx's
    /// effects are fully propagated before its snapshot is taken and before the next push.
    fn push_walk(
        &mut self,
        sources: &[TableSpec],
        ast: &Ast,
        pushes: &[(&str, SourceChange)],
    ) -> (Value, Vec<Value>);
}

/// Scope 1: the in-process synchronous `View` (memory backend), i.e. today's
/// `run_*_ast_view` runners. The default the free `assert_*` functions drive.
pub struct ViewSut;

impl Sut for ViewSut {
    fn hydrate(&mut self, sources: &[TableSpec], ast: &Ast) -> Value {
        let schema = ast_view_schema(sources, ast).expect("derive view schema");
        let nodes = run_fetch_test_ast_view(sources.to_vec(), ast).expect("ivm hydrate");
        tree(&nodes, &schema)
    }

    fn push_walk(
        &mut self,
        sources: &[TableSpec],
        ast: &Ast,
        pushes: &[(&str, SourceChange)],
    ) -> (Value, Vec<Value>) {
        let schema = ast_view_schema(sources, ast).expect("derive view schema");
        let (initial, steps) =
            run_push_walk_ast_view(sources.to_vec(), ast, clone_pushes(pushes)).expect("ivm walk");
        let steps = steps.iter().map(|s| tree(s, &schema)).collect();
        (tree(&initial, &schema), steps)
    }
}

/// Serialize an IVM result forest (`View`/change-sink `CaughtNode`s) to the name-keyed
/// nested JSON the oracle emits — the canonical oracle-comparable shape. Exposed so
/// out-of-crate SUT drivers (`Db`/`Cluster` `read_snapshot`) render their snapshot the
/// SAME way (cells flat by name, in-view relationships nested, projection/`.one()`/count
/// honored), and so a `read_snapshot`'s `Vec<CaughtChange>` of `Add`s compares apples-to-
/// apples against this same function over the scope-1 forest.
pub fn caught_tree(nodes: &[CaughtNode], schema: &Schema) -> Value {
    tree(nodes, schema)
}

// ── public assertions ────────────────────────────────────────────────────────────

/// Materialize `ast` through the IVM `View` (memory backend) and through the
/// compiled SQLite oracle over the same seed data; assert the JSON trees are equal.
pub fn assert_hydrate_parity(sources: &[TableSpec], catalog: &Catalog, ast: &Ast) {
    assert_hydrate_parity_with(&mut ViewSut, sources, catalog, ast);
}

/// [`assert_hydrate_parity`] generic over the [`Sut`]: hydrate `ast` through `sut`, then
/// assert it equals the SQLite oracle over the same seed. The oracle side is unchanged at
/// every scope — only `sut` differs.
pub fn assert_hydrate_parity_with(
    sut: &mut dyn Sut,
    sources: &[TableSpec],
    catalog: &Catalog,
    ast: &Ast,
) {
    let ivm = sut.hydrate(sources, ast);
    let (oracle, sql) = run_oracle(sources, catalog, ast, &[]);
    assert_trees_eq(&ivm, &oracle, "hydrate", &sql);
}

/// **Scalar-subquery fetch differential** (`SCALAR-SUBQUERY-DESIGN.md`): resolve
/// `ast`'s `scalar`-flagged `EXISTS` folds at build time against the seed (via a
/// memory-backed [`ScalarCatalog`]), hydrate the *folded* AST through the IVM, and
/// assert it equals the **plain-EXISTS** oracle over the same seed. The d2s oracle
/// ignores `scalar` and runs the live `EXISTS`, so equality proves the fold is
/// result-preserving at the snapshot. **Fetch-only by construction** — the inlined
/// value is a snapshot (design §3), so it would diverge from a live oracle under
/// mutation; that path is deliberately not exercised here.
pub fn assert_scalar_hydrate_parity(sources: &[TableSpec], catalog: &Catalog, ast: &Ast) {
    let scat = SeedScalarCatalog::new(sources);
    let resolved = resolve_scalars(ast, &scat).expect("resolve scalar subqueries");
    let schema = ast_view_schema(sources, &resolved).expect("derive view schema");
    let nodes = run_fetch_test_ast_view(sources.to_vec(), &resolved).expect("ivm hydrate (folded)");
    let ivm = tree(&nodes, &schema);
    // Oracle runs the ORIGINAL plain-EXISTS query (d2s ignores `scalar`).
    let (oracle, sql) = run_oracle(sources, catalog, ast, &[]);
    assert_trees_eq(&ivm, &oracle, "scalar hydrate", &sql);
}

/// A memory-backed [`ScalarCatalog`] over the seed [`TableSpec`]s (PK-only unique
/// keys) — the build-time read seam the scalar resolver looks up child rows through.
struct SeedScalarCatalog {
    sources: Vec<(String, MemorySource)>,
}
impl SeedScalarCatalog {
    fn new(sources: &[TableSpec]) -> SeedScalarCatalog {
        SeedScalarCatalog {
            sources: sources
                .iter()
                .map(|ts| {
                    let schema = ts.schema.clone().into_schema();
                    (ts.name.clone(), MemorySource::new(schema, ts.rows.clone()))
                })
                .collect(),
        }
    }
}
impl ScalarCatalog for SeedScalarCatalog {
    fn source(&self, table: &str) -> Option<&dyn ScalarSource> {
        self.sources
            .iter()
            .find(|(n, _)| n == table)
            .map(|(_, s)| s as &dyn ScalarSource)
    }
}

/// Hydrate, then run `pushes` through the IVM `View` (incremental) and against the
/// oracle DB (SQL mutations + recompute); assert parity both *before* the pushes
/// (the hydrate) and *after* (the maintained view vs. the recomputed query).
pub fn assert_push_parity(
    sources: &[TableSpec],
    catalog: &Catalog,
    ast: &Ast,
    pushes: Vec<(&str, SourceChange)>,
) {
    let schema = ast_view_schema(sources, ast).expect("derive view schema");

    // IVM side: hydrate + apply every push incrementally, reading `initial`/`data`.
    let ivm_pushes: Vec<(&str, SourceChange)> =
        pushes.iter().map(|(n, c)| (*n, c.clone())).collect();
    let result =
        run_push_test_ast_view(sources.to_vec(), ast, ivm_pushes, false).expect("ivm push");

    // 1) Pre-push hydrate parity (oracle over the untouched seed).
    let initial = tree(&result.initial, &schema);
    let (oracle0, sql) = run_oracle(sources, catalog, ast, &[]);
    assert_trees_eq(&initial, &oracle0, "pre-push hydrate", &sql);

    // 2) Post-push parity (oracle recomputed over the seed + the same mutations).
    let data = tree(&result.data, &schema);
    let (oracle1, _) = run_oracle(sources, catalog, ast, &pushes);
    assert_trees_eq(&data, &oracle1, "post-push", &sql);
}

/// Apply `pushes` to **one** long-lived `View`, snapshotting after each, and assert
/// every intermediate tree matches the SQLite oracle recomputed over the seed + that
/// prefix of mutations. Where [`assert_push_parity`] checks only the final state, this
/// pins the maintained view at *every* step of a long history — surfacing accumulation
/// drift (stale storage/version state, refill bookkeeping, COW reuse) that a single
/// end-of-batch comparison can mask.
pub fn assert_push_walk_parity(
    sources: &[TableSpec],
    catalog: &Catalog,
    ast: &Ast,
    pushes: Vec<(&str, SourceChange)>,
) {
    assert_push_walk_parity_with(&mut ViewSut, sources, catalog, ast, &pushes);
}

/// [`assert_push_walk_parity`] generic over the [`Sut`]: drive the whole push history
/// through `sut` (which snapshots after each fully-propagated transaction), then assert
/// the hydrate tree and every per-step tree equal the oracle recomputed over the seed +
/// that prefix of mutations. The oracle is unchanged at every scope.
pub fn assert_push_walk_parity_with(
    sut: &mut dyn Sut,
    sources: &[TableSpec],
    catalog: &Catalog,
    ast: &Ast,
    pushes: &[(&str, SourceChange)],
) {
    let (initial, steps) = sut.push_walk(sources, ast, pushes);

    // Post-hydrate parity (oracle over the untouched seed).
    let (oracle0, sql) = run_oracle(sources, catalog, ast, &[]);
    assert_trees_eq(&initial, &oracle0, "walk hydrate", &sql);

    // A driver must snapshot once per push (lock-step); a length mismatch is a driver bug,
    // not a divergence — surface it as such rather than silently checking a short prefix.
    let n = pushes.len();
    assert_eq!(
        steps.len(),
        n,
        "sut returned {} per-step snapshots for {n} pushes",
        steps.len()
    );

    // Per-step parity: after k pushes, the maintained view equals the oracle recomputed
    // over the seed + the first k mutations.
    for (k, step) in steps.iter().enumerate() {
        let (oracle_k, _) = run_oracle(sources, catalog, ast, &pushes[..=k]);
        assert_trees_eq(step, &oracle_k, &format!("walk step {}/{n}", k + 1), &sql);
    }
}

// ── flip invariance (metamorphic) ────────────────────────────────────────────────
//
// `flip` is a result-INVARIANT routing flag on an EXISTS condition: the engine may
// evaluate `EXISTS` parent-driven (`flip=None/Some(false)`) or child-driven
// (`Some(true)`), but the *answer* must be identical, and the d2s compiler ignores
// flip entirely. So for a query with k EXISTS nodes, every one of the 2^k flip
// assignments must equal the SAME oracle — a metamorphic check needing no
// hand-computed expectation, stressing exactly the divergent dataflow.
//
// One engine constraint: a *flipped* `NOT EXISTS` is `BuildError::Unsupported`
// (the anti-join isn't built). Those variants are asserted to REJECT, not to match.

/// Assert hydrate parity for `ast` under EVERY flip assignment of its EXISTS nodes.
pub fn assert_flip_invariant(sources: &[TableSpec], catalog: &Catalog, ast: &Ast) {
    let (oracle, sql) = run_oracle(sources, catalog, ast, &[]);
    for (variant, label) in flip_variants(ast) {
        if has_flipped_not_exists(&variant) {
            assert!(
                run_fetch_test_ast_view(sources.to_vec(), &variant).is_err(),
                "expected flipped NOT EXISTS to be rejected [{label}]"
            );
            continue;
        }
        let schema = ast_view_schema(sources, &variant).expect("derive view schema");
        let nodes = run_fetch_test_ast_view(sources.to_vec(), &variant)
            .unwrap_or_else(|e| panic!("ivm hydrate [{label}]: {e:?}"));
        assert_trees_eq(
            &tree(&nodes, &schema),
            &oracle,
            &format!("hydrate {label}"),
            &sql,
        );
    }
}

/// Assert hydrate + push parity for `ast` under EVERY flip assignment (the oracle is
/// flip-blind, so it is computed once and every variant is checked against it).
pub fn assert_flip_invariant_push(
    sources: &[TableSpec],
    catalog: &Catalog,
    ast: &Ast,
    pushes: Vec<(&str, SourceChange)>,
) {
    let (oracle0, sql) = run_oracle(sources, catalog, ast, &[]);
    let (oracle1, _) = run_oracle(sources, catalog, ast, &pushes);
    for (variant, label) in flip_variants(ast) {
        if has_flipped_not_exists(&variant) {
            assert!(
                run_push_test_ast_view(sources.to_vec(), &variant, clone_pushes(&pushes), false)
                    .is_err(),
                "expected flipped NOT EXISTS to be rejected [{label}]"
            );
            continue;
        }
        let schema = ast_view_schema(sources, &variant).expect("derive view schema");
        let res = run_push_test_ast_view(sources.to_vec(), &variant, clone_pushes(&pushes), false)
            .unwrap_or_else(|e| panic!("ivm push [{label}]: {e:?}"));
        assert_trees_eq(
            &tree(&res.initial, &schema),
            &oracle0,
            &format!("pre-push {label}"),
            &sql,
        );
        assert_trees_eq(
            &tree(&res.data, &schema),
            &oracle1,
            &format!("post-push {label}"),
            &sql,
        );
    }
}

/// Enumerate the 2^k flip assignments over `ast`'s k EXISTS nodes (k==0 ⇒ the AST
/// unchanged). Bit i (DFS order: where-tree before `related`, a node before its own
/// subquery) sets the i-th EXISTS node's `flip`. Capped to keep the fan-out sane.
pub fn flip_variants(ast: &Ast) -> Vec<(Ast, String)> {
    let k = count_exists(ast);
    assert!(
        k <= 6,
        "flip_variants: {k} EXISTS nodes is too many to enumerate"
    );
    if k == 0 {
        return vec![(ast.clone(), "flip[]".to_string())];
    }
    (0..(1u32 << k))
        .map(|bits| {
            let mut v = ast.clone();
            let mut idx = 0;
            apply_flips_ast(&mut v, bits, &mut idx);
            (v, format!("flip[{bits:0width$b}]", width = k))
        })
        .collect()
}

fn count_exists(ast: &Ast) -> usize {
    let w = ast.r#where.as_ref().map_or(0, count_exists_cond);
    w + ast
        .related
        .iter()
        .map(|r| count_exists(&r.subquery))
        .sum::<usize>()
}

fn count_exists_cond(c: &Condition) -> usize {
    match c {
        Condition::Simple(_) => 0,
        Condition::CorrelatedSubquery(csq) => 1 + count_exists(&csq.related.subquery),
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().map(count_exists_cond).sum()
        }
    }
}

fn apply_flips_ast(ast: &mut Ast, bits: u32, idx: &mut u32) {
    if let Some(w) = ast.r#where.as_mut() {
        apply_flips_cond(w, bits, idx);
    }
    for r in ast.related.iter_mut() {
        apply_flips_ast(&mut r.subquery, bits, idx);
    }
}

fn apply_flips_cond(c: &mut Condition, bits: u32, idx: &mut u32) {
    match c {
        Condition::Simple(_) => {}
        Condition::CorrelatedSubquery(csq) => {
            csq.flip = Some((bits >> *idx) & 1 == 1);
            *idx += 1;
            apply_flips_ast(&mut csq.related.subquery, bits, idx);
        }
        Condition::And { conditions } | Condition::Or { conditions } => {
            for cc in conditions.iter_mut() {
                apply_flips_cond(cc, bits, idx);
            }
        }
    }
}

fn has_flipped_not_exists(ast: &Ast) -> bool {
    ast.r#where
        .as_ref()
        .is_some_and(has_flipped_not_exists_cond)
        || ast
            .related
            .iter()
            .any(|r| has_flipped_not_exists(&r.subquery))
}

fn has_flipped_not_exists_cond(c: &Condition) -> bool {
    match c {
        Condition::Simple(_) => false,
        Condition::CorrelatedSubquery(csq) => {
            (csq.op == ExistsOp::NotExists && csq.flip == Some(true))
                || has_flipped_not_exists(&csq.related.subquery)
        }
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().any(has_flipped_not_exists_cond)
        }
    }
}

fn clone_pushes<'a>(pushes: &[(&'a str, SourceChange)]) -> Vec<(&'a str, SourceChange)> {
    pushes.iter().map(|(n, c)| (*n, c.clone())).collect()
}

// ── the SQLite oracle ────────────────────────────────────────────────────────────

/// Seed a fresh in-memory SQLite DB from `sources`, apply `pushes` as SQL mutations,
/// run the compiled query, and parse the single nested-JSON result cell. Returns the
/// JSON tree and the SQL (for failure messages).
pub fn run_oracle(
    sources: &[TableSpec],
    catalog: &Catalog,
    ast: &Ast,
    pushes: &[(&str, SourceChange)],
) -> (Value, String) {
    let conn = Connection::open_in_memory().expect("open oracle db");
    conn.execute_batch("PRAGMA case_sensitive_like = ON;")
        .unwrap();
    // One transaction for the bulk seed — the chinook tables are several thousand rows.
    conn.execute_batch(&format!("BEGIN;\n{}\nCOMMIT;", seed_sql(sources)))
        .expect("seed oracle db");
    apply_pushes_sql(&conn, sources, pushes);

    let sql = compile(catalog, ast).unwrap_or_else(|e| panic!("compile: {e}"));
    let text: String = conn
        .query_row(&sql, [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("run oracle: {e}\nSQL:\n{sql}"));
    (serde_json::from_str(&text).expect("parse oracle json"), sql)
}

/// Fallible twin of [`run_oracle`] for lanes that must grade the oracle's *failure* too
/// (design 226 §5.3/OQ3): seeding and pushes still panic on error (a broken fixture is a
/// harness bug), but the compiled query's execution comes back as a `Result`, with the SQL
/// alongside for failure messages. Pair with [`is_integer_overflow`] to grade a sum whose
/// set total leaves i64 as **vacuously passing** — both the engine (the §5.3 typed error)
/// and SQLite refuse — instead of panicking the walk.
pub fn run_oracle_result(
    sources: &[TableSpec],
    catalog: &Catalog,
    ast: &Ast,
    pushes: &[(&str, SourceChange)],
) -> (Result<Value, rusqlite::Error>, String) {
    let conn = Connection::open_in_memory().expect("open oracle db");
    conn.execute_batch("PRAGMA case_sensitive_like = ON;")
        .unwrap();
    conn.execute_batch(&format!("BEGIN;\n{}\nCOMMIT;", seed_sql(sources)))
        .expect("seed oracle db");
    apply_pushes_sql(&conn, sources, pushes);

    let sql = compile(catalog, ast).unwrap_or_else(|e| panic!("compile: {e}"));
    let result = conn
        .query_row(&sql, [], |r| r.get::<_, String>(0))
        .map(|text| serde_json::from_str(&text).expect("parse oracle json"));
    (result, sql)
}

/// Recognize SQLite's integer-overflow runtime error — the design 226 OQ3 question
/// ("can d2s surface it distinctly, vs a generic step failure?") answered: rusqlite maps
/// it to a `SqliteFailure` whose message is exactly `integer overflow`, so a differential
/// lane can grade it specifically rather than swallowing every oracle error.
pub fn is_integer_overflow(e: &rusqlite::Error) -> bool {
    matches!(e, rusqlite::Error::SqliteFailure(_, Some(msg)) if msg == "integer overflow")
}

/// The push→SQL mapping, shared by the oracle (`apply_pushes_sql`) and the SQLite-backed
/// SUT drivers (`Db`/`Cluster`), so both sides apply a [`SourceChange`] **identically** to
/// real SQLite — `Add` inserts, `Remove` deletes the key, `Edit` deletes the old key then
/// inserts the new row (so a primary-key edit moves the row, matching the engine). Returns
/// the statements for one change (an `Edit` is two). The driver runs these through its
/// tracked write path (so the preupdate-hook CDC fires); the oracle runs them directly.
pub fn change_to_sql(table: &str, schema: &SourceSchema, change: &SourceChange) -> Vec<String> {
    match change {
        SourceChange::Add(row) => vec![insert_sql(table, row)],
        SourceChange::Remove(row) => vec![delete_sql(table, schema, row)],
        SourceChange::Edit { row, old } => {
            vec![delete_sql(table, schema, old), insert_sql(table, row)]
        }
    }
}

/// Apply each [`SourceChange`] to the oracle DB via the shared [`change_to_sql`] mapping.
fn apply_pushes_sql(conn: &Connection, sources: &[TableSpec], pushes: &[(&str, SourceChange)]) {
    for (name, change) in pushes {
        let spec = sources
            .iter()
            .find(|s| s.name.as_str() == *name)
            .unwrap_or_else(|| panic!("push targets unknown source {name:?}"));
        for s in change_to_sql(name, &spec.schema, change) {
            conn.execute_batch(&s)
                .unwrap_or_else(|e| panic!("apply push to {name}: {e}\nSQL:\n{s}"));
        }
    }
}

/// `INSERT INTO "t" VALUES (v0, v1, …)` — positional, all columns, matching `seed_sql`.
fn insert_sql(table: &str, row: &rindle::value::OwnedRow) -> String {
    let vals: Vec<String> = row.cells().map(|v| v_to_sql(&v.to_owned())).collect();
    format!("INSERT INTO {} VALUES ({});", quote(table), vals.join(", "))
}

/// `DELETE FROM "t" WHERE "pk0" = v0 AND …` — identify the row by its primary key.
fn delete_sql(table: &str, schema: &SourceSchema, row: &rindle::value::OwnedRow) -> String {
    let conds: Vec<String> = schema
        .primary_key
        .iter()
        .map(|&i| {
            format!(
                "{} = {}",
                quote(&schema.columns[i]),
                v_to_sql(&row.col(i).to_owned())
            )
        })
        .collect();
    format!(
        "DELETE FROM {} WHERE {};",
        quote(table),
        conds.join(" AND ")
    )
}

// ── (de)serialization, shared with the SQL side ──────────────────────────────────

/// Serialize a `View`/`Catch` `CaughtNode` forest to the same name-keyed JSON shape
/// the oracle emits.
fn tree(nodes: &[CaughtNode], schema: &Schema) -> Value {
    Value::Array(nodes.iter().map(|n| node_to_json(n, schema)).collect())
}

/// One node → `{col: v, …, relName: [child, …], …}`: columns by name, then each
/// **in-view** relationship by name (gating/EXISTS slots carry no child schema and
/// are omitted, exactly as `view.data` omits them).
fn node_to_json(node: &CaughtNode, schema: &Schema) -> Value {
    let mut obj = Map::new();
    // Projection (PROJECTION-SUPPORT-DESIGN.md §6): a projected level reports only its selected
    // columns; a `'*'` level reports every column. The differential oracle compares against d2s,
    // which projects identically (key SET, order-independent — the JSON map is key-sorted).
    match &schema.projection {
        Some(cols) => {
            for &i in cols {
                obj.insert(
                    schema.columns[i].to_string(),
                    owned_to_json(&node.row.col(i).to_owned()),
                );
            }
        }
        None => {
            for (i, col) in schema.columns.iter().enumerate() {
                obj.insert(col.to_string(), owned_to_json(&node.row.col(i).to_owned()));
            }
        }
    }
    for (slot, rd) in schema.relationships.iter().enumerate() {
        let Some(child_schema) = rd.child.as_deref() else {
            continue; // gating (EXISTS) relationship — not part of the output tree.
        };
        let children = node
            .relationships
            .get(&RelId(slot as u32))
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        // A scalar-projected relationship aggregate (`count(child)`, REDUCE-DESIGN.md §9):
        // unwrap the one-row child to a bare scalar (its projected column), substituting
        // the aggregate identity (`0` for count) when the relationship is empty — matching
        // the oracle's `count(*)` column.
        let v = if let Some(proj) = &rd.project {
            match children.first() {
                Some(c) => owned_to_json(&c.row.col(proj.col).to_owned()),
                None => owned_to_json(&proj.identity),
            }
        } else {
            Value::Array(
                children
                    .iter()
                    .map(|c| node_to_json(c, child_schema))
                    .collect(),
            )
        };
        obj.insert(rd.name.to_string(), v);
    }
    Value::Object(obj)
}

fn owned_to_json(v: &V) -> Value {
    match v {
        V::Absent => Value::Null, // parity fixtures use full rows; never `Absent`
        V::Null => Value::Null,
        V::Bool(b) => Value::Bool(*b),
        V::Int(i) => Value::Number((*i).into()),
        V::Float(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        V::Str(s) => Value::String(s.to_string()),
        V::Json(s) => serde_json::from_str(s).unwrap_or(Value::Null),
    }
}

/// `CREATE TABLE`/`INSERT` SQL built from the SAME sources the IVM consumes, so both
/// sides see identical data (no separate fixture to drift). Bare columns (no type
/// affinity) mirror `ivm_parity.rs`.
fn seed_sql(sources: &[TableSpec]) -> String {
    let mut s = String::new();
    for t in sources {
        let cols: Vec<String> = t.schema.columns.iter().map(|c| quote(c)).collect();
        s.push_str(&format!(
            "CREATE TABLE {} ({});\n",
            quote(&t.name),
            cols.join(", ")
        ));
        for r in &t.rows {
            let vals: Vec<String> = r.cells().map(|v| v_to_sql(&v.to_owned())).collect();
            s.push_str(&format!(
                "INSERT INTO {} VALUES ({});\n",
                quote(&t.name),
                vals.join(", ")
            ));
        }
    }
    s
}

/// Infer a SQLite **column type** for column `col` from the seed rows. The real SUTs'
/// `register_table` rejects untyped/BLOB columns (`schema.rs` `value_type_of`), so the
/// generator's typeless schema must be given a declared type — chosen so it round-trips
/// through that discovery to the matching engine `ValueType` (the §9 schema-discovery seam):
/// `Int`→`INTEGER`, `Float`→`REAL`, `Str`→`TEXT`, `Bool`→`BOOLEAN`, `Json`→`JSON`. An
/// all-null column has no type evidence; `TEXT` is a safe non-BLOB default that registers.
fn infer_sql_type(rows: &[OwnedRow], col: usize) -> &'static str {
    for r in rows {
        match &r.col(col).to_owned() {
            V::Null | V::Absent => continue,
            V::Bool(_) => return "BOOLEAN",
            V::Int(_) => return "INTEGER",
            V::Float(_) => return "REAL",
            V::Str(_) => return "TEXT",
            V::Json(_) => return "JSON",
        }
    }
    "TEXT"
}

/// The **typed** `CREATE TABLE`s (with the primary key declared), no rows — for SUT drivers
/// that create empty tables at open and seed through their own write path (the cluster's
/// tracked writes, the master's ingress). [`create_and_seed_sql`] is this plus the seed
/// `INSERT`s. Column types
/// are inferred from the data (`infer_sql_type`); this is the §9 schema-discovery seam:
/// the generator's typeless `(columns, primary_key)` becomes a real schema that a live
/// `register_table` discovers (affinity + PK index) to key CDC + the IVM source.
pub fn create_tables_sql(sources: &[TableSpec]) -> String {
    let mut s = String::new();
    for t in sources {
        let cols: Vec<String> = t
            .schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{} {}", quote(c), infer_sql_type(&t.rows, i)))
            .collect();
        let pk: Vec<String> = t
            .schema
            .primary_key
            .iter()
            .map(|&i| quote(&t.schema.columns[i]))
            .collect();
        let pk_clause = if pk.is_empty() {
            String::new()
        } else {
            format!(", PRIMARY KEY ({})", pk.join(", "))
        };
        s.push_str(&format!(
            "CREATE TABLE {} ({}{});\n",
            quote(&t.name),
            cols.join(", "),
            pk_clause
        ));
    }
    s
}

/// Like `seed_sql` but **typed and with the primary key declared** — for the real
/// SQLite-backed SUTs (`Db`/`Cluster`), whose `register_table` discovers both from the live
/// schema to key CDC + the IVM source. The oracle's `seed_sql` needs neither (it uses bare
/// affinity and identifies rows by PK manually in its DELETEs); a real source requires both.
/// The typed `CREATE`s come from [`create_tables_sql`]; this appends the seed `INSERT`s.
pub fn create_and_seed_sql(sources: &[TableSpec]) -> String {
    let mut s = create_tables_sql(sources);
    for t in sources {
        for r in &t.rows {
            let vals: Vec<String> = r.cells().map(|v| v_to_sql(&v.to_owned())).collect();
            s.push_str(&format!(
                "INSERT INTO {} VALUES ({});\n",
                quote(&t.name),
                vals.join(", ")
            ));
        }
    }
    s
}

/// `DELETE FROM` then re-`INSERT` the seed for every source (no `CREATE`) — run by a reused
/// SUT through its **tracked write path** to reset the data to the baseline between cases, so
/// the IVM re-maintains back to the seed (design §9 reset hygiene). Deletes precede inserts so
/// the tables are empty before reseeding.
pub fn reseed_sql(sources: &[TableSpec]) -> String {
    let mut s = String::new();
    for t in sources {
        s.push_str(&format!("DELETE FROM {};\n", quote(&t.name)));
    }
    for t in sources {
        for r in &t.rows {
            let vals: Vec<String> = r.cells().map(|v| v_to_sql(&v.to_owned())).collect();
            s.push_str(&format!(
                "INSERT INTO {} VALUES ({});\n",
                quote(&t.name),
                vals.join(", ")
            ));
        }
    }
    s
}

/// [`reseed_sql`] as **individual** statements — a `DELETE FROM` per table, then one `INSERT`
/// per seed row (no trailing `;`) — for SUT write paths that take a single prepared statement
/// at a time. The write hosts run one `SqlStatement` per `write.exec`, so the
/// single-string [`reseed_sql`] (an `exec_batch` of many statements, as `Db`/`Cluster` use)
/// will not do; reset hygiene (design §9) is otherwise identical — empty the tables, replay
/// the seed, so the IVM re-maintains to the baseline between cases.
pub fn reset_statements(sources: &[TableSpec]) -> Vec<String> {
    let mut out = Vec::new();
    for t in sources {
        out.push(format!("DELETE FROM {}", quote(&t.name)));
    }
    for t in sources {
        for r in &t.rows {
            out.push(insert_sql(&t.name, r).trim_end_matches(';').to_string());
        }
    }
    out
}

fn v_to_sql(v: &V) -> String {
    match v {
        V::Absent => "NULL".into(), // parity fixtures use full rows; never `Absent`
        V::Null => "NULL".into(),
        V::Bool(true) => "1".into(),
        V::Bool(false) => "0".into(),
        V::Int(i) => i.to_string(),
        V::Float(f) => format!("{f}"),
        V::Str(s) | V::Json(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Collapse integral floats to integers on both sides (the JS `number` int/float
/// non-distinction) so e.g. a column read as `5.0` compares equal to SQLite's `5`.
fn canon(v: &Value) -> Value {
    match v {
        Value::Array(a) => Value::Array(a.iter().map(canon).collect()),
        Value::Object(m) => Value::Object(m.iter().map(|(k, x)| (k.clone(), canon(x))).collect()),
        Value::Number(n) => match n.as_f64() {
            Some(f) if f.is_finite() && f.fract() == 0.0 && f.abs() < 9e15 => {
                Value::Number((f as i64).into())
            }
            _ => v.clone(),
        },
        _ => v.clone(),
    }
}

fn assert_trees_eq(ivm: &Value, oracle: &Value, phase: &str, sql: &str) {
    assert_eq!(
        canon(ivm),
        canon(oracle),
        "IVM != oracle [{phase}]\n--- IVM ---\n{ivm:#}\n--- ORACLE ---\n{oracle:#}\n--- SQL ---\n{sql}"
    );
}

#[cfg(test)]
mod canon_tests {
    use super::canon;
    use serde_json::json;

    /// Design 226 Stage D (§9): the collapse is **already exact-safe** — a boundary i64
    /// rides JSON as an integer whose f64 image is `≥ 9e15`, so `canon` passes it through
    /// untouched while still integrating out the int/float non-distinction below the bound.
    /// Pinned so a future "simplification" can't quietly widen the exact plane.
    #[test]
    fn canon_is_identity_on_exact_boundary_i64() {
        for i in [9_007_199_254_740_993_i64, i64::MAX, i64::MIN] {
            assert_eq!(canon(&json!(i)), json!(i));
        }
        assert_eq!(
            canon(&json!(5.0)),
            json!(5),
            "the collapse below 9e15 stays"
        );
    }
}
