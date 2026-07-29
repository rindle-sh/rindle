//! `build_select_query` — the `FetchRequest` → parameterized `SELECT` lowering
//! (spec `05` §4.4). A faithful port of `packages/zqlite/src/query-builder.ts`.
//!
//! **All identifiers come from the schema** (resolved from a [`ColId`] to a
//! [`ColumnDef::name`]); **all values are bound `?` params**, never interpolated
//! — the JS uses `@databases/sql` tagged templates, we emit `?` placeholders and a
//! parallel [`SqliteParam`] vec. The `?` slots and the param vec are built in
//! **lockstep**, so the emission order IS the bind order (`05` §4.4): constraint →
//! multiConstraints → start → filters (ORDER BY has no params).
//!
//! Every cell value crosses the `to_sqlite_param` boundary (`toSQLiteType`,
//! query-builder.ts:278): `boolean`→0/1 (a null boolean stays NULL), `json`→
//! serialized TEXT (a null json becomes the 4-char TEXT `"null"` — the three SQL
//! nulls are distinct, `05` §3.13 / §13 Q11). Strings/numbers/null pass through.
//!
//! Scope (per `05` §1.2): this lowers the *already-subquery-stripped*
//! `SqlCondition`. Statics must have been substituted upstream — a `static`
//! operand is a builder bug, not a runtime path (`debug_assert!`-style
//! `unreachable`, foundations §10). Here statics simply don't exist in the
//! [`Operand`] enum, which makes that invariant structural.

use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::ToSql;

use rindle::change::{Basis, Constraint, MultiConstraint, Start};
use rindle::source_common::{Operand, SqlCondition, SqlOp};
use rindle::value::{ColId, OwnedValue, Sort, Value, ValueType};

/// Static, build-time column metadata (spec `05` §4.1). The ONLY place a column
/// *name* lives; the index into a `&[ColumnDef]` IS the [`ColId`]. `optional`
/// drives the start-bound nullable-aware `=` vs `IS` / `<`,`>` vs `(… IS NULL OR
/// …)` lowering (§3.7); `ty` drives the value boundary (`to_sqlite_param` and the
/// leaf's `col()` conversion).
#[derive(Clone, Debug)]
pub struct ColumnDef {
    pub name: Box<str>,
    pub ty: ValueType,
    pub optional: bool,
}

/// A value already converted to its SQLite storage form (`toSQLiteType`, §3.13):
/// `bool`→`Int(0|1)`, `json`→`Text(serialized)`, else passthrough. Carries a
/// distinct `Null` (the SQL NULL) separate from a `Text("null")` (a serialized
/// json null) — the two SQL nulls the JS keeps distinct (`05` §13 Q11).
#[derive(Clone, Debug)]
pub enum SqliteParam {
    Null,
    Int(i64),
    Real(f64),
    /// Strings AND serialized json (both TEXT).
    Text(Box<str>),
}

impl ToSql for SqliteParam {
    #[inline]
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            SqliteParam::Null => ToSqlOutput::Borrowed(ValueRef::Null),
            SqliteParam::Int(i) => ToSqlOutput::Borrowed(ValueRef::Integer(*i)),
            SqliteParam::Real(f) => ToSqlOutput::Borrowed(ValueRef::Real(*f)),
            SqliteParam::Text(s) => ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes())),
        })
    }
}

/// The compiled statement: SQL text (the statement-cache key) + ordered bind
/// params (the `?` slots, in textual order).
#[derive(Debug)]
pub struct CompiledQuery {
    pub sql: String,
    pub params: Vec<SqliteParam>,
}

// ---------------------------------------------------------------------------
// Identifier quoting + value conversion
// ---------------------------------------------------------------------------

/// Quote a SQL identifier (`sql.ident`): wrap in double quotes, doubling any
/// embedded quote. The schema's column/table names, never user values. Shared
/// with `table_source` for the write-statement (INSERT/DELETE/UPDATE) SQL.
pub(crate) fn ident(name: &str) -> String {
    let mut s = String::with_capacity(name.len() + 2);
    s.push('"');
    for ch in name.chars() {
        if ch == '"' {
            s.push('"');
        }
        s.push(ch);
    }
    s.push('"');
    s
}

/// `toSQLiteType` (query-builder.ts:278): convert a cell value to its
/// SQLite bind form, ty-directed. `boolean` → `null` stays NULL else `1`/`0`;
/// `number`/`string`/`null` pass through; `json` → serialized TEXT (a null json
/// serializes to the TEXT `"null"`). Takes the borrowed [`Value`] (a flat row's
/// cell form); owned callers bridge with `.as_ref()`.
pub fn to_sqlite_param(value: Value<'_>, ty: ValueType) -> SqliteParam {
    use Value as V;
    match ty {
        ValueType::Boolean => match value {
            // The server holds full rows and never constructs `Absent`
            // (PROJECTION-SUPPORT-DESIGN.md §3.1 / OQ-2); reaching the bind boundary with
            // one is a bug. Assert in debug, bind NULL defensively in release.
            V::Absent => {
                debug_assert!(false, "Value::Absent must never reach SQLite bind");
                SqliteParam::Null
            }
            V::Null => SqliteParam::Null, // a null boolean stays NULL (NOT coerced to 0)
            V::Bool(b) => SqliteParam::Int(if b { 1 } else { 0 }),
            // A boolean column only holds Bool/Null; mirror JS truthiness defensively.
            V::Int(i) => SqliteParam::Int(if i != 0 { 1 } else { 0 }),
            V::Float(f) => SqliteParam::Int(if f != 0.0 { 1 } else { 0 }),
            V::Str(s) => SqliteParam::Int(if s.is_empty() { 0 } else { 1 }),
            V::Json(_) => SqliteParam::Int(1),
        },
        // number/string/null all "pass through" in the JS (`return v`); the value
        // already carries the storage class. `int64` binds the same way — an exact
        // `Int` stays INTEGER; fidelity of what lands in the column is enforced at
        // the capture and hydrate/scan boundaries (design 226 §4.2), not at bind.
        ValueType::Number | ValueType::Int | ValueType::String | ValueType::Null => match value {
            V::Absent => {
                debug_assert!(false, "Value::Absent must never reach SQLite bind");
                SqliteParam::Null
            }
            V::Null => SqliteParam::Null,
            V::Bool(b) => SqliteParam::Int(if b { 1 } else { 0 }),
            V::Int(i) => SqliteParam::Int(i),
            V::Float(f) => SqliteParam::Real(f),
            // Row text is UTF-8-validated at construction (value.rs), so the lossy
            // decode is a straight copy.
            V::Str(b) => SqliteParam::Text(String::from_utf8_lossy(b).into()),
            V::Json(b) => SqliteParam::Text(String::from_utf8_lossy(b).into()),
        },
        // `json` → `JSON.stringify(v)`. Our json cell is ALREADY the raw text, so
        // bind it verbatim (more faithful than JS's parse→re-stringify, which can
        // re-normalize whitespace/key-order). A null json → TEXT `"null"`.
        ValueType::Json => SqliteParam::Text(json_serialize(value)),
    }
}

/// `JSON.stringify` for an owned value. In practice a json column only ever holds
/// `Json(text)` (bound verbatim) or `Null` (→ `"null"`); the scalar arms exist for
/// totality (e.g. a json *literal* in a filter, though `getJsType` only classifies
/// objects/arrays as json).
fn json_serialize(value: Value<'_>) -> Box<str> {
    use Value as V;
    match value {
        V::Absent => {
            debug_assert!(false, "Value::Absent must never reach SQLite json bind");
            "null".into()
        }
        V::Null => "null".into(),
        V::Bool(b) => if b { "true" } else { "false" }.into(),
        V::Int(i) => i.to_string().into(),
        V::Float(f) => f.to_string().into(),
        V::Json(b) => String::from_utf8_lossy(b).into(),
        V::Str(b) => {
            let s = String::from_utf8_lossy(b);
            let mut out = String::with_capacity(s.len() + 2);
            out.push('"');
            for ch in s.chars() {
                match ch {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    c if (c as u32) < 0x20 => {
                        out.push_str(&format!("\\u{:04x}", c as u32));
                    }
                    c => out.push(c),
                }
            }
            out.push('"');
            out.into()
        }
    }
}

/// Resolve a literal operand's logical type the way the JS `getJsType` does — from
/// the *value's own* runtime shape, NOT a column type (query-builder.ts:265).
fn js_type_of(value: &OwnedValue) -> ValueType {
    match value {
        OwnedValue::Absent => {
            debug_assert!(false, "OwnedValue::Absent has no SQLite logical type");
            ValueType::Null
        }
        OwnedValue::Null => ValueType::Null,
        OwnedValue::Str(_) => ValueType::String,
        OwnedValue::Int(_) | OwnedValue::Float(_) => ValueType::Number,
        OwnedValue::Bool(_) => ValueType::Boolean,
        OwnedValue::Json(_) => ValueType::Json,
    }
}

// ---------------------------------------------------------------------------
// The top-level builder
// ---------------------------------------------------------------------------

/// Lower a `FetchRequest`'s pushed-down parts (+ connection filter + sort) to a
/// parameterized `SELECT`. The arg grouping is regrouped from the JS for
/// readability; the **clause-emission order is fixed** (constraint →
/// multiConstraints → start → filters → ORDER BY) so the `?` slots and `params`
/// stay aligned (`05` §4.4).
#[allow(clippy::too_many_arguments)]
pub fn build_select_query(
    table: &str,
    columns: &[ColumnDef],
    constraint: Option<&Constraint>,
    multi_constraints: &[MultiConstraint],
    filters: Option<&SqlCondition>,
    order: Option<&Sort>,
    reverse: bool,
    start: Option<&Start>,
) -> CompiledQuery {
    // SELECT <declared cols> FROM <table> — column order == ColId order, which is
    // what makes the leaf `col(i)` an O(1) array index (§8.1).
    let mut sql = String::from("SELECT ");
    for (i, c) in columns.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&ident(&c.name));
    }
    sql.push_str(" FROM ");
    sql.push_str(&ident(table));

    // WHERE fragments, each `(text, params)`, joined by AND in emission order.
    let mut terms: Vec<(String, Vec<SqliteParam>)> = Vec::new();
    if let Some(c) = constraint {
        terms.extend(constraints_to_sql(c, columns));
    }
    for mc in multi_constraints {
        if !mc.is_empty() {
            terms.push(multi_constraint_to_sql(mc, columns));
        }
    }
    if let Some(s) = start {
        let order = order.expect("start requires ordering (query-builder.ts:51)");
        terms.push(gather_start_constraints(s, reverse, order, columns));
    }
    if let Some(f) = filters {
        terms.push(filters_to_sql(f, columns));
    }

    let mut params: Vec<SqliteParam> = Vec::new();
    if !terms.is_empty() {
        sql.push_str(" WHERE ");
        for (i, (text, ps)) in terms.into_iter().enumerate() {
            if i > 0 {
                sql.push_str(" AND ");
            }
            sql.push_str(&text);
            params.extend(ps);
        }
    }

    if let Some(order) = order {
        if !order.is_empty() {
            sql.push(' ');
            sql.push_str(&order_by_to_sql(order, reverse, columns));
        }
    }

    CompiledQuery { sql, params }
}

// ---------------------------------------------------------------------------
// constraint / multiConstraint
// ---------------------------------------------------------------------------

/// `constraintsToSQL` (query-builder.ts:69): one bare `"col" = ?` per
/// `(ColId, value)`, returned as separate AND terms. **Always `=`** (NOT
/// nullable-aware — §3.3): constraints come from join keys, never null.
fn constraints_to_sql(c: &Constraint, columns: &[ColumnDef]) -> Vec<(String, Vec<SqliteParam>)> {
    c.iter()
        .map(|(col, v)| {
            let text = format!("{} = ?", ident(&columns[*col].name));
            (text, vec![to_sqlite_param(v.as_ref(), columns[*col].ty)])
        })
        .collect()
}

/// `multiConstraintToSQL` (query-builder.ts:98): a batched `IN`. Single-column →
/// `"col" IN (?, …)`; compound → `("a","b",…) IN (VALUES (?,…), …)`. The key
/// shape is taken from the first entry (entries share it, an upstream invariant).
fn multi_constraint_to_sql(
    mc: &MultiConstraint,
    columns: &[ColumnDef],
) -> (String, Vec<SqliteParam>) {
    debug_assert!(!mc.is_empty(), "multiConstraint must be non-empty");
    // Key columns, in the first entry's order.
    let keys: Vec<ColId> = mc[0].iter().map(|(col, _)| *col).collect();
    debug_assert!(!keys.is_empty(), "multiConstraint entries need >=1 key");

    // Look up an entry's value for a key column (entries are keyed by ColId).
    let value_for = |entry: &Constraint, key: ColId| -> SqliteParam {
        let v = entry
            .iter()
            .find(|(c, _)| *c == key)
            .map(|(_, v)| v)
            .expect("multiConstraint entries share the first entry's keys");
        to_sqlite_param(v.as_ref(), columns[key].ty)
    };

    let mut params = Vec::with_capacity(mc.len() * keys.len());

    if keys.len() == 1 {
        let key = keys[0];
        let mut text = format!("{} IN (", ident(&columns[key].name));
        for (i, entry) in mc.iter().enumerate() {
            if i > 0 {
                text.push_str(", ");
            }
            text.push('?');
            params.push(value_for(entry, key));
        }
        text.push(')');
        return (text, params);
    }

    // Compound: (a, b, …) IN (VALUES (?, ?, …), …)
    let col_list = keys
        .iter()
        .map(|k| ident(&columns[*k].name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut text = format!("({col_list}) IN (VALUES ");
    for (i, entry) in mc.iter().enumerate() {
        if i > 0 {
            text.push_str(", ");
        }
        text.push('(');
        for (j, key) in keys.iter().enumerate() {
            if j > 0 {
                text.push_str(", ");
            }
            text.push('?');
            params.push(value_for(entry, *key));
        }
        text.push(')');
    }
    text.push(')');
    (text, params)
}

// ---------------------------------------------------------------------------
// start bound (the only nullable-aware part — §3.7)
// ---------------------------------------------------------------------------

/// `nullableAwareEquality` (query-builder.ts:291): `"col" IS ?` for an optional
/// column (so `IS NULL` matches), else bare `"col" = ?` (avoids the NULL+OR
/// full-scan gotcha on a column that can't be null). One bound param.
fn nullable_aware_equality(col: &ColumnDef, value: SqliteParam) -> (String, Vec<SqliteParam>) {
    let op = if col.optional { "IS" } else { "=" };
    (format!("{} {op} ?", ident(&col.name)), vec![value])
}

/// `nullableAwareRangeComparison` (query-builder.ts:303). Non-optional → bare
/// `"col" <op> ?`. Optional `>` → `(? IS NULL OR "col" > ?)` (value bound TWICE).
/// Optional `<` → `("col" IS NULL OR "col" < ?)` (value bound once). The asymmetry
/// (which side gets `IS NULL`) encodes Zero's `null < everything` ordering.
fn nullable_aware_range_comparison(
    col: &ColumnDef,
    value: SqliteParam,
    op_gt: bool, // true => '>', false => '<'
) -> (String, Vec<SqliteParam>) {
    let op = if op_gt { ">" } else { "<" };
    let id = ident(&col.name);
    let comparison = format!("{id} {op} ?");
    if !col.optional {
        return (comparison, vec![value]);
    }
    if op_gt {
        // value appears in BOTH `? IS NULL` and `"col" > ?` → two params.
        (
            format!("(? IS NULL OR {comparison})"),
            vec![value.clone(), value],
        )
    } else {
        (format!("({id} IS NULL OR {comparison})"), vec![value])
    }
}

/// `gatherStartConstraints` (query-builder.ts:341): the OR-of-ANDs lexicographic
/// start bound. For order `o[0..n]`, OR over `i` of (AND over `j<i` of `o[j] = v_j`
/// (nullable-aware equality), then `o[i] <op> v_i` (nullable-aware range)), where
/// `<op>` = `>` for asc / `<` for desc, **flipped by `reverse`**. `basis = At`
/// appends a final all-equality term so the start row itself is included.
///
/// **Deviation from JS (perf):** when the leading sort column is non-nullable we
/// prepend a redundant, sargable `o[0] <op>= v0` term so SQLite can seek the index
/// instead of filter-scanning the partition. It is entailed by the disjunction (so
/// result-preserving); see the inline note at the emission site.
fn gather_start_constraints(
    start: &Start,
    reverse: bool,
    order: &Sort,
    columns: &[ColumnDef],
) -> (String, Vec<SqliteParam>) {
    let mut groups: Vec<(String, Vec<SqliteParam>)> = Vec::new();

    for i in 0..order.len() {
        let mut frags: Vec<String> = Vec::new();
        let mut params: Vec<SqliteParam> = Vec::new();
        for (j, &(j_col, _j_asc)) in order.iter().enumerate().take(i + 1) {
            let col = &columns[j_col];
            let value = to_sqlite_param(start.row.col(j_col), col.ty);
            if j == i {
                let (_, i_asc) = order[i];
                // asc ? (reverse ? '<' : '>') : (reverse ? '>' : '<')
                let op_gt = if i_asc { !reverse } else { reverse };
                let (text, ps) = nullable_aware_range_comparison(col, value, op_gt);
                frags.push(text);
                params.extend(ps);
            } else {
                let (text, ps) = nullable_aware_equality(col, value);
                frags.push(text);
                params.extend(ps);
            }
        }
        groups.push((format!("({})", frags.join(" AND ")), params));
    }

    if matches!(start.basis, Basis::At) {
        let mut frags: Vec<String> = Vec::new();
        let mut params: Vec<SqliteParam> = Vec::new();
        for &(col_id, _) in order {
            let col = &columns[col_id];
            let value = to_sqlite_param(start.row.col(col_id), col.ty);
            let (text, ps) = nullable_aware_equality(col, value);
            frags.push(text);
            params.extend(ps);
        }
        groups.push((format!("({})", frags.join(" AND ")), params));
    }

    let mut disj = String::from("(");
    let mut disj_params: Vec<SqliteParam> = Vec::new();
    for (i, (frag, ps)) in groups.into_iter().enumerate() {
        if i > 0 {
            disj.push_str(" OR ");
        }
        disj.push_str(&frag);
        disj_params.extend(ps);
    }
    disj.push(')');

    // Redundant sargable leading-column bound (§3.7 perf). Every disjunct pins the
    // leading sort column `o[0]` to either `<op> v0` (group i=0) or `= v0` (every
    // later group + the `At` all-equality term), so the inclusive bound
    // `o[0] <op>= v0` is *entailed* by the whole OR-of-ANDs — ANDing it in removes no
    // rows. But SQLite can **seek** an index on it, turning a keyset start-fetch from
    // a full-partition filter-scan (O(partition)) into an index range (O(log n)):
    // the OR-of-ANDs alone is not sargable, so a `Take` displacement fetch anchored
    // at the window boundary scans the whole partition up to the boundary (measured
    // ~35k VM steps on a 3k-row partition vs ~62 with this term).
    //
    // Only emitted when the leading column is NOT NULL: a nullable one needs the
    // `(… IS NULL OR …)` wrap (which defeats the seek anyway), and an ascending NULL
    // *bound value* makes group 0 match every row (`? IS NULL OR …`), so there is no
    // sound bare bound to add. `op_gt = asc XOR reverse` (matches group 0's operator);
    // the inclusive form (`>=`/`<=`) keeps the boundary row, which every disjunct
    // admits.
    if let Some(&(col_id, asc)) = order.first() {
        let col = &columns[col_id];
        if !col.optional {
            let op = if asc != reverse { ">=" } else { "<=" };
            let value = to_sqlite_param(start.row.col(col_id), col.ty);
            let text = format!("{} {op} ? AND {disj}", ident(&col.name));
            let mut params = Vec::with_capacity(disj_params.len() + 1);
            params.push(value);
            params.extend(disj_params);
            return (text, params);
        }
    }

    (disj, disj_params)
}

// ---------------------------------------------------------------------------
// ORDER BY
// ---------------------------------------------------------------------------

/// `orderByToSQL` (query-builder.ts:144): per-column direction, flipped under
/// `reverse`. Effective direction = `asc XOR reverse`.
fn order_by_to_sql(order: &Sort, reverse: bool, columns: &[ColumnDef]) -> String {
    let mut s = String::from("ORDER BY ");
    for (i, &(col, asc)) in order.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        let asc_eff = asc != reverse; // XOR
        s.push_str(&ident(&columns[col].name));
        s.push(' ');
        s.push_str(if asc_eff { "asc" } else { "desc" });
    }
    s
}

// ---------------------------------------------------------------------------
// filters (the residual is owned by 07; here we lower the pushed-down condition)
// ---------------------------------------------------------------------------

/// `filtersToSQL` (query-builder.ts:169): `and`/`or` recurse (empty `and` → `TRUE`,
/// empty `or` → `FALSE`); `simple` → `simpleConditionToSQL`.
fn filters_to_sql(c: &SqlCondition, columns: &[ColumnDef]) -> (String, Vec<SqliteParam>) {
    match c {
        SqlCondition::Simple { left, op, right } => {
            simple_condition_to_sql(left, *op, right, columns)
        }
        SqlCondition::And(conds) => {
            if conds.is_empty() {
                return ("TRUE".to_string(), Vec::new());
            }
            join_bool(conds, " AND ", columns)
        }
        SqlCondition::Or(conds) => {
            if conds.is_empty() {
                return ("FALSE".to_string(), Vec::new());
            }
            join_bool(conds, " OR ", columns)
        }
    }
}

fn join_bool(
    conds: &[SqlCondition],
    sep: &str,
    columns: &[ColumnDef],
) -> (String, Vec<SqliteParam>) {
    let mut text = String::from("(");
    let mut params: Vec<SqliteParam> = Vec::new();
    for (i, c) in conds.iter().enumerate() {
        if i > 0 {
            text.push_str(sep);
        }
        let (t, ps) = filters_to_sql(c, columns);
        text.push_str(&t);
        params.extend(ps);
    }
    text.push(')');
    (text, params)
}

/// `simpleConditionToSQL` (query-builder.ts:194). `IN`/`NOT IN` → a
/// `json_each(?)` subquery over the JSON-array literal; the `LIKE` family →
/// `likeConditionToSQL`; everything else emits the op **verbatim**.
fn simple_condition_to_sql(
    left: &Operand,
    op: SqlOp,
    right: &Operand,
    columns: &[ColumnDef],
) -> (String, Vec<SqliteParam>) {
    match op {
        SqlOp::In | SqlOp::NotIn => {
            let (ltext, mut params) = value_position_to_sql(left, columns);
            // The right side is a JSON-array literal; bind its text and expand it
            // via json_each (query-builder.ts:196-205).
            let arr = match right {
                Operand::Literal(v) => json_array_param(v),
                Operand::Column(_) => panic!("IN right side must be a literal array"),
            };
            params.push(arr);
            let op_str = if matches!(op, SqlOp::In) {
                "IN"
            } else {
                "NOT IN"
            };
            (
                format!("{ltext} {op_str} (SELECT value FROM json_each(?))"),
                params,
            )
        }
        SqlOp::Like | SqlOp::NotLike | SqlOp::Ilike | SqlOp::NotIlike => {
            like_condition_to_sql(left, op, right, columns)
        }
        _ => {
            let (ltext, mut params) = value_position_to_sql(left, columns);
            let (rtext, rparams) = value_position_to_sql(right, columns);
            params.extend(rparams);
            (format!("{ltext} {} {rtext}", sql_op_raw(op)), params)
        }
    }
}

/// `likeConditionToSQL` (query-builder.ts:226): `ILIKE` lowers both sides through
/// `lower(...)` (ICU, matching the in-memory matcher); the bare `LIKE` relies on
/// the connection's `PRAGMA case_sensitive_like = ON`. Always `ESCAPE '\'`.
fn like_condition_to_sql(
    left: &Operand,
    op: SqlOp,
    right: &Operand,
    columns: &[ColumnDef],
) -> (String, Vec<SqliteParam>) {
    let case_insensitive = matches!(op, SqlOp::Ilike | SqlOp::NotIlike);
    let negated = matches!(op, SqlOp::NotLike | SqlOp::NotIlike);
    let like_op = if negated { "NOT LIKE" } else { "LIKE" };

    let (ltext, mut params) = value_position_to_sql(left, columns);
    let (rtext, rparams) = value_position_to_sql(right, columns);
    params.extend(rparams);

    let text = if case_insensitive {
        format!("lower({ltext}) {like_op} lower({rtext}) ESCAPE '\\'")
    } else {
        format!("{ltext} {like_op} {rtext} ESCAPE '\\'")
    };
    (text, params)
}

/// `valuePositionToSQL` (query-builder.ts:252): a column → its quoted identifier;
/// a literal → a bound `?` param typed by its own runtime shape (`getJsType`).
fn value_position_to_sql(operand: &Operand, columns: &[ColumnDef]) -> (String, Vec<SqliteParam>) {
    match operand {
        Operand::Column(col) => (ident(&columns[*col].name), Vec::new()),
        Operand::Literal(v) => (
            "?".to_string(),
            vec![to_sqlite_param(v.as_ref(), js_type_of(v))],
        ),
    }
}

/// Bind the JSON-array literal for an `IN` list (the JS `JSON.stringify(right.value)`).
/// A `Json` operand already carries the array text; anything else is serialized.
fn json_array_param(v: &OwnedValue) -> SqliteParam {
    SqliteParam::Text(json_serialize(v.as_ref()))
}

/// The raw SQL spelling for the verbatim-emitted operators
/// (`sql.__dangerous__rawValue(filter.op)`). A free function rather than an inherent
/// method: `SqlOp` is defined in the core `rindle` crate, so an inherent `impl` here is
/// not allowed (orphan rule).
fn sql_op_raw(op: SqlOp) -> &'static str {
    match op {
        SqlOp::Eq => "=",
        SqlOp::Ne => "!=",
        SqlOp::Lt => "<",
        SqlOp::Le => "<=",
        SqlOp::Gt => ">",
        SqlOp::Ge => ">=",
        SqlOp::Is => "IS",
        SqlOp::IsNot => "IS NOT",
        SqlOp::In => "IN",
        SqlOp::NotIn => "NOT IN",
        SqlOp::Like => "LIKE",
        SqlOp::NotLike => "NOT LIKE",
        SqlOp::Ilike => "ILIKE",
        SqlOp::NotIlike => "NOT ILIKE",
    }
}
