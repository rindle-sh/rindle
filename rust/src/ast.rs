//! The query **AST** — the wire-format, target-agnostic query input the
//! [`crate::query`] fluent builder crafts and the pipeline builder (spec `08`)
//! lowers into an arena `Graph`.
//!
//! This is the **wire shape** of spec `02-ast.md`: the types deserialize from the
//! *same JSON* the TypeScript `astSchema` (`zero-protocol/src/ast.ts`) produces,
//! so a JS-emitted query can drive the Rust builder unchanged — the shared-AST
//! differential corpus (spec `11` §3.6). The earlier trimmed spike
//! (`Condition::Simple { field, op, value }`, separate `Exists`/`NotExists`,
//! `Int`/`Float` literals) is gone; what replaced it:
//!
//! - **`SimpleCondition { op, left, right }`** over [`ValuePosition`] (a column or a
//!   literal), so a column on either side is representable. The fluent builder still
//!   only emits `Column <op> Literal`.
//! - **One [`Condition::CorrelatedSubquery`]** carrying a [`CorrelatedSubqueryCondition`]
//!   (`op: EXISTS | NOT EXISTS`, plus `flip`/`scalar`/`plan_id`) — replacing the two
//!   `Exists`/`NotExists` variants — matching the wire `correlatedSubquery` condition.
//! - **[`Lit::Number`] is one `f64`**, with an exact [`Lit::Int`] beside it (design
//!   226 Stage B): an integer JSON token parses as `Int` (all 64 bits), a float
//!   token as `Number`; both lower identically for integral values in ±2^53.
//!
//! **serde derives are feature-gated** behind `any(testkit, serde)`. The AST is the
//! wire format, so two consumers need it: the test-only
//! differential corpus (`testkit`) and — per productionization decision **D1** —
//! the wasm client, which deserializes JS-emitted query JSON in the *shipping*
//! artifact (via `serde-wasm-bindgen`). D1 deliberately overrides the spec `11`
//! §1.2 "never in the shipping wasm artifact" rule for the wasm path. `testkit`
//! depends on the `serde` feature transitively, so its behavior is unchanged. When
//! neither feature is on, these are plain owned-data types with no serde.
//!
//! **Not represented:** *static parameters* (`{type:'static',…}`) — deprecated and
//! being removed from the upstream engine, so a `ValuePosition` is a column or a
//! literal, never a parameter. **Deferred (the agreed plan):** the canonicalization
//! passes `normalize_ast`/`cmp_condition` and the client↔server `map_ast` remap —
//! the output differential does not need them; `complete_ordering` (the
//! always-includes-PK invariant) is a builder (`08`) concern. `plan_id` is a
//! planner annotation, parsed-but-skipped and never set until the planner lands.
//!
//! **One sanctioned divergence from canonical Zero:** [`Ast::select`] — `None` ⇒
//! *select all columns*, `Some(cols)` ⇒ project. Canonical Zero returns whole rows;
//! this field is the fluent builder's `.select(..)`, is not wire data, and does
//! nothing at this layer (spec `12` owns projection). It serializes only when set.
//!
//! Every type is `Clone + Debug + PartialEq` (so hand-written "expected" ASTs in
//! tests compare with `assert_eq!`). [`Ast`] is `Default` for struct-update syntax:
//! `Ast { table: "issue".into(), ..Default::default() }`.

use std::collections::BTreeMap;

/// Sort direction for an [`OrderPart`]. Wire `'asc' | 'desc'` (`ast.ts:24`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "lowercase")
)]
pub enum Dir {
    Asc,
    Desc,
}

/// A simple comparison operator — the wire `SimpleOperator` set (`ast.ts:211-215`).
/// Serializes as the exact SQL-ish string (`"="`, `"!="`, `"IS NOT"`, `"NOT IN"`, …).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
pub enum Op {
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "="))]
    Eq,
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "!="))]
    Ne,
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "<"))]
    Lt,
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "<="))]
    Le,
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = ">"))]
    Gt,
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = ">="))]
    Ge,
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "IS"))]
    Is,
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "IS NOT"))]
    IsNot,
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "LIKE"))]
    Like,
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(rename = "NOT LIKE")
    )]
    NotLike,
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "ILIKE"))]
    ILike,
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(rename = "NOT ILIKE")
    )]
    NotILike,
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "IN"))]
    In,
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "NOT IN"))]
    NotIn,
}

/// An AST literal value (`LiteralValue`, `ast.ts:284-289`). Its own type — distinct
/// from the runtime [`crate::value::OwnedValue`] — so the AST derives `PartialEq`
/// (`OwnedValue` forbids derived comparison; you must pick `compare_values` vs
/// `values_equal`). The two are bridged at lowering time (builder, `08`).
///
/// serde-`untagged`: a JSON `null`/`bool`/`number`/`string`/`array` round-trips to
/// the matching variant. An **integer** JSON token deserializes into `Int` (exact,
/// all 64 bits — design 226 Stage B); a float token into `Number` (`f64`). The two
/// lower identically for every integral value in ±2^53 (`number_to_owned` already
/// produced `OwnedValue::Int` there), so a JS client serializing `5.0` as `5`
/// changes nothing; what `Int` adds is exactness ABOVE 2^53, where the old
/// `Number(f64)` round-trip rounded (the fluent API's `i64` impl was lossy).
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize),
    serde(untagged)
)]
pub enum Lit {
    /// JSON `null`.
    Null,
    Bool(bool),
    /// An exact integer literal (a JSON integer token). Ordered BEFORE `Number` so
    /// untagged deserialization tries i64 first; a non-integral or out-of-i64-range
    /// number falls through to `Number`.
    Int(i64),
    /// JS `number` — a single `f64`.
    Number(f64),
    Str(Box<str>),
    /// The right-hand side of an `IN` / `NOT IN` — a list of scalars.
    Array(Vec<Lit>),
}

/// Re-key every [`Lit::Number`] in `ast` onto the wire-token rule — the identity a
/// JSON home derives (design 226 §6). serde_json sees `JSON.stringify`'s
/// SHORTEST-round-trip decimal token and parses an integer-form token in i64 range
/// as [`Lit::Int`]; an `Ast` that entered through a non-JSON deserializer
/// (serde-wasm-bindgen visits every non-safe-integer JS number as f64) instead
/// carries the BINARY value. For `2**60` that is `Int(1152921504606846976)` where
/// every JSON home parses `Int(1152921504606847000)` — two exact literals for one
/// query text, so a predicate or resume cursor matches on one home and misses on
/// the other. Rust's `Display` emits the same shortest-round-trip digits as
/// `JSON.stringify` across the integer-form range, so formatting the f64 and
/// re-parsing the token converges the homes on one identity. Non-integral and
/// out-of-i64-token values stay [`Lit::Number`] — exactly serde_json's
/// fallthrough — and NaN/±Infinity become [`Lit::Null`], because that IS their
/// wire token (`JSON.stringify` emits `null` for non-finite numbers). Call at
/// every non-JSON AST entry (the wasm boundary).
pub fn canonicalize_wire_number_lits(ast: &mut Ast) {
    if let Some(cond) = ast.r#where.as_mut() {
        canonicalize_condition(cond);
    }
    if let Some(cond) = ast.having.as_mut() {
        canonicalize_condition(cond);
    }
    if let Some(bound) = ast.start.as_mut() {
        for lit in bound.row.values_mut() {
            canonicalize_lit(lit);
        }
    }
    for rel in &mut ast.related {
        canonicalize_wire_number_lits(&mut rel.subquery);
    }
}

fn canonicalize_condition(cond: &mut Condition) {
    match cond {
        Condition::Simple(s) => {
            canonicalize_value_position(&mut s.left);
            canonicalize_value_position(&mut s.right);
        }
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter_mut().for_each(canonicalize_condition)
        }
        Condition::CorrelatedSubquery(c) => canonicalize_wire_number_lits(&mut c.related.subquery),
    }
}

fn canonicalize_value_position(vp: &mut ValuePosition) {
    if let ValuePosition::Literal { value } = vp {
        canonicalize_lit(value);
    }
}

fn canonicalize_lit(lit: &mut Lit) {
    match lit {
        Lit::Number(f) => {
            if !f.is_finite() {
                // `JSON.stringify` has no token for NaN/±Infinity — it emits `null`,
                // so every JSON home receives `Lit::Null`. Same identity here.
                *lit = Lit::Null;
            } else if f.fract() == 0.0 {
                if let Ok(i) = format!("{f}").parse::<i64>() {
                    *lit = Lit::Int(i);
                }
            }
        }
        Lit::Array(items) => items.iter_mut().for_each(canonicalize_lit),
        _ => {}
    }
}

/// A value position in a [`SimpleCondition`] — a column reference or a literal
/// (`ValuePosition`, `ast.ts:267`, minus the deprecated `static` parameter form).
/// Wire-tagged by `"type"`.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize),
    serde(tag = "type")
)]
pub enum ValuePosition {
    /// `{ type: "literal", value }` (`ast.ts:279-282`).
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "literal"))]
    Literal { value: Lit },
    /// `{ type: "column", name }` (`ast.ts:269-277`). Name stays a `String` — name
    /// → `ColId` lowering is the builder's job (`08` §5.5).
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "column"))]
    Column { name: Box<str> },
}

/// The filter tree (`Condition`, `ast.ts:296-300`). Wire-tagged by `"type"`;
/// recursive through the `Vec`s (heap-boxed elements) and the boxed subquery.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize),
    serde(tag = "type")
)]
pub enum Condition {
    /// `field <op> value`.
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "simple"))]
    Simple(SimpleCondition),
    /// All children must hold (`{ type: "and", conditions }`).
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "and"))]
    And { conditions: Vec<Condition> },
    /// At least one child must hold (`{ type: "or", conditions }`).
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "or"))]
    Or { conditions: Vec<Condition> },
    /// `(NOT) EXISTS (<correlated subquery>)`.
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(rename = "correlatedSubquery")
    )]
    CorrelatedSubquery(CorrelatedSubqueryCondition),
}

/// A single comparison (`SimpleCondition`, `ast.ts:302-312`). `right` is wire-typed
/// to exclude a column (`Exclude<ValuePosition, ColumnReference>`); the builder
/// validates "not a column" at its boundary.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
pub struct SimpleCondition {
    pub op: Op,
    /// LHS — a value position (the fluent builder always emits a `Column`).
    pub left: ValuePosition,
    /// RHS — a `Literal`, never a `Column` (enforced in the builder).
    pub right: ValuePosition,
}

/// A `(NOT) EXISTS` condition (`CorrelatedSubqueryCondition`, `ast.ts:324-331`).
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
pub struct CorrelatedSubqueryCondition {
    /// The subquery + how it correlates to the parent.
    pub related: CorrelatedSubquery,
    /// `EXISTS` | `NOT EXISTS`.
    pub op: ExistsOp,
    /// Flipped-join routing flag (read by the builder / planner). Deferred path.
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub flip: Option<bool>,
    /// Scalar-subquery flag. Deferred path.
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub scalar: Option<bool>,
    /// Build-time planner annotation — **not** wire data (`#[serde(skip)]`), set by
    /// the planner (deferred), and always `None` today. Once the planner sets it, it
    /// must be excluded from `PartialEq`/canonical ordering (spec `02` §4.4); it is
    /// `None`-only now, so the derived `PartialEq` is correct in the interim.
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(skip))]
    pub plan_id: Option<u32>,
}

/// `'EXISTS' | 'NOT EXISTS'` (`CorrelatedSubqueryConditionOperator`, `ast.ts:333`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
pub enum ExistsOp {
    #[cfg_attr(any(feature = "testkit", feature = "serde"), serde(rename = "EXISTS"))]
    Exists,
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(rename = "NOT EXISTS")
    )]
    NotExists,
}

/// A child query joined to its parent by a key [`Correlation`]
/// (`CorrelatedSubquery`, `ast.ts:250-265`). Used both as a materialized
/// relationship ([`Ast::related`]) and inside a [`CorrelatedSubqueryCondition`].
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
pub struct CorrelatedSubquery {
    /// How the child relates to the parent (the join key pair).
    pub correlation: Correlation,
    /// The child query. Boxed to break the `Ast`→`CorrelatedSubquery`→`Ast` cycle.
    pub subquery: Box<Ast>,
    /// Origin: `'client' | 'permissions' | 'test'`. Wire-optional; the builder
    /// defaults absent ⇒ `client`.
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub system: Option<System>,
}

/// Subquery provenance (`System`, `ast.ts:28`). Shared with the runtime layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "lowercase")
)]
pub enum System {
    Permissions,
    Client,
    Test,
}

/// The join key pair (`Correlation`, `ast.ts:245-248`). `parent_field[i]` on the
/// parent correlates with `child_field[i]` on the child; both non-empty, same length.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct Correlation {
    /// Column name(s) on the **parent** table.
    pub parent_field: Vec<Box<str>>,
    /// Column name(s) on the **child** (subquery) table.
    pub child_field: Vec<Box<str>>,
}

/// One `(field, direction)` of an ordering (`OrderPart`, `ast.ts:208`). A wire
/// 2-tuple — serializes as the JSON array `["field", "asc"]`.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
pub struct OrderPart(pub Box<str>, pub Dir);

impl OrderPart {
    /// The order-by column name.
    pub fn field(&self) -> &str {
        &self.0
    }
    /// The sort direction.
    pub fn dir(&self) -> Dir {
        self.1
    }
}

/// A paging lower bound (`Bound`, `ast.ts:199-202`). `row` is a *partial* wire row —
/// the bound columns by name (a `BTreeMap` for deterministic key order, matching the
/// wire object). `exclusive` maps to the runtime `Basis` (`false` ⇒ `At`/inclusive,
/// `true` ⇒ `After`/exclusive) at lowering, in the builder/`Skip` (spec `03` §3.5).
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
pub struct Bound {
    pub row: BTreeMap<Box<str>, Lit>,
    pub exclusive: bool,
}

/// An aggregate over a (correlated) subquery's rows (`REDUCE-DESIGN.md`).
///
/// Set on a [`Ast`] that is a `related` subquery, it marks that relationship as a
/// **relationship aggregate** — `issue { commentCount: count(comments) }` — which the
/// builder (`08`) lowers to a grouped `reduce` + a scalar-projected singular relationship
/// (§9) instead of a row-materializing join. Set on the **root** `Ast`, it is a top-level
/// aggregate (`SELECT count(*) FROM …`, optionally with `group_by`/`having`).
///
/// `Sum`/`Avg` carry the **column name** they aggregate (resolved to a child/source
/// [`ColId`](crate::value::ColId) by the builder). All three are *invertible*, so
/// `reduce` maintains them from a pure delta stream. Serde is externally tagged: `Count`
/// ⇒ the bare string `"count"`; `Sum(c)` ⇒ `{"sum": c}`; `Avg(c)` ⇒ `{"avg": c}`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "lowercase")
)]
pub enum Aggregate {
    /// `count(*)` over the subquery's (filtered) rows.
    Count,
    /// `sum(col)` over the subquery's (filtered) rows — Σ of the non-`NULL` values.
    Sum(Box<str>),
    /// `avg(col)` over the subquery's (filtered) rows — `sum / count` of the non-`NULL`
    /// values (`NULL` when there are none).
    Avg(Box<str>),
}

/// `serde(skip_serializing_if)` predicate for a `bool` field that defaults to `false`
/// (so the wire JSON omits it unless set) — used by [`Ast::one`].
#[cfg(any(feature = "testkit", feature = "serde"))]
fn is_false(b: &bool) -> bool {
    !*b
}

/// The query AST (`Ast`, `ast.ts:217-243`). `table` is the only required field;
/// every other wire field is optional. `Default` enables struct-update syntax for
/// hand-written test expectations. Deserializes from the JS wire JSON (camelCase
/// keys; absent ⇒ `None`/empty).
#[derive(Clone, Debug, PartialEq, Default)]
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct Ast {
    /// Postgres schema namespace — opaque pass-through (`ast.ts:218`).
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub schema: Option<Box<str>>,
    /// Source table name. The only required field.
    pub table: Box<str>,
    /// Subquery alias (the relationship name, when this is a `sub`/`exists` child).
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub alias: Option<Box<str>>,
    /// Projection. `None` ⇒ **select all columns**; `Some(cols)` ⇒ just `cols`.
    /// Sanctioned non-wire extension (see module docs); serialized only when set.
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub select: Option<Vec<Box<str>>>,
    /// Filter tree.
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub r#where: Option<Condition>,
    /// Child subqueries (materialized relationships). Empty ⇒ none.
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub related: Vec<CorrelatedSubquery>,
    /// Paging lower bound.
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub start: Option<Bound>,
    /// Row limit.
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub limit: Option<u32>,
    /// `.one()` — return a **single** row: the result is presented as one object (or
    /// `null`/absent) instead of an array. Query *intent* recorded on the AST; the
    /// engine stays plural internally and the single-element unwrap happens at the
    /// result boundary (the builder lowers `one` onto the view
    /// [`Schema`](crate::value::Schema)'s `singular` flag). A `.one()` query also sets
    /// `limit = 1`. On a `related` subquery, `one` makes **that** relationship singular.
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "is_false")
    )]
    pub one: bool,
    /// Aggregate this (sub)query's rows instead of materializing them
    /// (`REDUCE-DESIGN.md`). `None` ⇒ ordinary row output; `Some` on a `related`
    /// subquery ⇒ a relationship aggregate the builder lowers to a `reduce` + a
    /// scalar-projected singular relationship (§9). Absent on the wire ⇒ `None`.
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub aggregate: Option<Aggregate>,
    /// The [`aggregate`](Ast::aggregate) is **precomputed** — its `(group_key…, value)`
    /// rows are supplied as a (synthetic) source table rather than reduced from child
    /// rows. Set by the normalized client's AST rewrite (`AGGREGATE-SYNC-DESIGN.md` §3.3):
    /// the server ships the reduce's output as a base table, and the client reads it with
    /// a plain singular join + the *same* scalar projection — **not** a `reduce`, which
    /// would recount the already-aggregated rows. Only meaningful alongside `aggregate`;
    /// absent on the wire ⇒ `false` (the ordinary reduce-backed relationship aggregate).
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "is_false")
    )]
    pub aggregate_precomputed: bool,
    /// Top-level `GROUP BY` columns (names, not ColIds), meaningful only alongside a
    /// root [`aggregate`](Ast::aggregate) (`REDUCE-DESIGN.md` §8). Empty + `aggregate`
    /// set ⇒ a **global** aggregate (one `[count]` row, no grouping); non-empty ⇒ one
    /// `[group…, count]` row per distinct value-tuple, which the builder lowers to an
    /// **eager** grouped `reduce` feeding the `View`. Distinct from a relationship
    /// aggregate's grouping, which is implicit (the correlation child key). Absent on
    /// the wire ⇒ empty.
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub group_by: Vec<Box<str>>,
    /// `HAVING` — a filter over the **post-aggregation** rows of a root
    /// [`aggregate`](Ast::aggregate) (`REDUCE-DESIGN.md` §4: a `HAVING` filter sits
    /// directly above the `reduce`). The condition addresses the aggregate's *output*
    /// columns — the [`group_by`](Ast::group_by) columns and the synthetic `count`
    /// column — not base-table columns. The builder lowers it to a `Filter` sub-graph
    /// **above** the reduce (whereas [`where`](Ast::where) filters rows *below* it);
    /// the `Filter` edit-split turns a group crossing the predicate threshold into an
    /// `Add`/`Remove`, so it is maintained incrementally for free. Absent ⇒ `None`.
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub having: Option<Condition>,
    /// Sort spec (names, not ColIds). Empty ⇒ none. Wire key `orderBy`.
    #[cfg_attr(
        any(feature = "testkit", feature = "serde"),
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub order_by: Vec<OrderPart>,
}

impl Ast {
    /// A bare query over `table` with every other field absent. Equivalent to
    /// `Ast { table: table.into(), ..Default::default() }`.
    pub fn new(table: &str) -> Ast {
        Ast {
            table: table.into(),
            ..Ast::default()
        }
    }
}

/// Design 226 Stage B: the untagged `Lit` wire behavior around the new `Int`
/// variant. Serde-gated like the derives themselves (runs in the `testkit` lane).
#[cfg(all(test, any(feature = "testkit", feature = "serde")))]
mod lit_serde_tests {
    use super::Lit;

    #[test]
    fn integer_tokens_parse_exact_and_float_tokens_stay_number() {
        // An integer JSON token → Int, all 64 bits exact.
        assert_eq!(serde_json::from_str::<Lit>("5").unwrap(), Lit::Int(5));
        assert_eq!(
            serde_json::from_str::<Lit>("9007199254740993").unwrap(),
            Lit::Int(9_007_199_254_740_993) // 2^53 + 1 — unrepresentable as f64
        );
        assert_eq!(
            serde_json::from_str::<Lit>("9223372036854775807").unwrap(),
            Lit::Int(i64::MAX)
        );
        assert_eq!(
            serde_json::from_str::<Lit>("-9223372036854775808").unwrap(),
            Lit::Int(i64::MIN)
        );
        // A float token (or an out-of-i64 magnitude) falls through to Number.
        assert_eq!(
            serde_json::from_str::<Lit>("5.5").unwrap(),
            Lit::Number(5.5)
        );
        assert_eq!(
            serde_json::from_str::<Lit>("1e300").unwrap(),
            Lit::Number(1e300)
        );
        assert_eq!(
            serde_json::from_str::<Lit>("18446744073709551615").unwrap(),
            Lit::Number(18_446_744_073_709_551_615.0) // > i64::MAX → f64
        );
        // Round-trips: Int serializes as a bare integer token, exactly.
        assert_eq!(
            serde_json::to_string(&Lit::Int(9_007_199_254_740_993)).unwrap(),
            "9007199254740993"
        );
        // In an IN-list the elements behave identically.
        assert_eq!(
            serde_json::from_str::<Lit>("[1, 2.5]").unwrap(),
            Lit::Array(vec![Lit::Int(1), Lit::Number(2.5)])
        );
    }

    #[test]
    fn int_and_number_spellings_lower_to_the_same_scalar_below_2_53() {
        // `5` (now Int) and `5.0` (Number) must build the SAME pipeline scalar —
        // `number_to_owned` already lowered integral f64s to `OwnedValue::Int`, so
        // the new wire parse changes nothing an operator can observe.
        let a = crate::builder::lit_to_scalar(&Lit::Int(5)).unwrap();
        let b = crate::builder::lit_to_scalar(&Lit::Number(5.0)).unwrap();
        assert!(matches!(a, crate::value::OwnedValue::Int(5)));
        assert!(matches!(b, crate::value::OwnedValue::Int(5)));
    }
}
