//! A small, loosely-typed **fluent query builder** that crafts [`Ast`]s — the
//! Rust analogue of the TS `zql/src/query/query-impl.ts`, for use in tests and
//! such. (The other "query builder", `query_builder`, is a different
//! thing: the `FetchRequest` → SQL lowering, mirroring TS `zqlite/query-builder.ts`.)
//!
//! It is **schema-free**: unlike the TS builder, which looks up a relationship's
//! correlation from a typed schema, this derives the correlation from the
//! subquery closure itself. Instead of `issue.related("creator")` you write:
//!
//! ```
//! use rindle::query::table;
//!
//! let q = table("issue")
//!     .select("title")
//!     .sub(|row| table("user")
//!         .r#where("id", row.col("creatorID"))   // ← `id` (child) ↔ `creatorID` (parent)
//!         .select("name"))
//!     .build();
//! ```
//!
//! The trick: inside a `sub` closure, `row.col("creatorID")` is a reference to a
//! **parent** column. When such a reference is used as the value of a child
//! `where`, it is *not* a filter — it defines the [`Correlation`] between parent
//! and child (here `parentField = ["creatorID"]`, `childField = ["id"]`). Several
//! such `where`s build a compound key. Everything else (`where` with a real
//! literal, `select`, `limit`, …) behaves as you'd expect.
//!
//! The builder is move-chaining: each method takes `self` and returns `Self`;
//! [`Query::build`] consumes it and yields the [`Ast`].
//!
//! ## Boolean groups (`AND` / `OR`, nestable)
//!
//! Chained `where`s `AND` together. For an `OR` — or any nested mix — use
//! [`Query::where_any`] (an `OR` group) / [`Query::where_all`] (an explicit `AND`
//! group). Each takes a closure handed a fresh [`Cond`]; inside it you chain the
//! same `where`/`where_op`/`where_in`/`where_exists` clauses, plus [`Cond::any`] /
//! [`Cond::all`] to nest to any depth:
//!
//! ```
//! use rindle::query::table;
//!
//! // (priority > 3 OR kind = 'bug') AND NOT EXISTS(a blocking issue)
//! let q = table("issue")
//!     .where_any(|c| c.where_op("priority", ">", 3).r#where("kind", "bug"))
//!     .where_not_exists(|row| table("link").r#where("blocks", row.col("id")))
//!     .build();
//!
//! // (a = 1 AND b = 2) OR c = 3   — nesting an AND inside an OR
//! let q = table("t")
//!     .where_any(|c| c
//!         .all(|c| c.r#where("a", 1).r#where("b", 2))
//!         .r#where("c", 3))
//!     .build();
//! ```
//!
//! ## Surface
//! - [`table`] — start a query.
//! - [`Query::select`] — project a column (chain for several; omit for all).
//! - [`Query::where`] — `field = value`, or a parent correlation if `value` is a
//!   [`Parent`]; [`Query::where_op`] — explicit operator; [`Query::where_in`].
//! - [`Query::sub`] / [`Query::sub_as`] — add a correlated child (a relationship).
//! - [`Query::where_exists`] / [`Query::where_not_exists`] — `(NOT) EXISTS`.
//! - [`Query::where_any`] / [`Query::where_all`] — an `OR` / `AND` group of
//!   conditions, nestable via [`Cond::any`] / [`Cond::all`].
//! - [`Query::limit`], [`Query::order_by`], [`Query::start_at`] /
//!   [`Query::start_after`] / [`Query::start_row`], [`Query::alias`].

use crate::ast::{
    Aggregate, Ast, Bound, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation,
    Dir, ExistsOp, Lit, Op, OrderPart, SimpleCondition, System, ValuePosition,
};

/// Start a query for `table`. The entry point: `table("issue").select("title")…`.
pub fn table(name: &str) -> Query {
    Query::new(name)
}

/// The fluent builder. Holds the [`Ast`] under construction plus a *pending
/// correlation* — the `(child, parent)` field pairs siphoned from
/// `where(child, row.col(parent))` calls, which the enclosing `sub` /
/// `where_exists` drains.
pub struct Query {
    ast: Ast,
    /// Parent-side correlation columns, parallel to `corr_child` (see module docs).
    corr_parent: Vec<Box<str>>,
    /// Child-side correlation columns, parallel to `corr_parent`.
    corr_child: Vec<Box<str>>,
}

/// A handle to the **parent** row, handed to a `sub` / `where_exists` closure.
/// `row.col("creatorID")` yields a [`Parent`] reference to that parent column.
#[derive(Clone, Copy)]
pub struct ParentRow;

impl ParentRow {
    /// Reference parent column `name`. Use it as a `where` value to correlate:
    /// `child.r#where("id", row.col("creatorID"))`.
    pub fn col(&self, name: &str) -> Parent {
        Parent(name.into())
    }
}

/// A reference to a parent-row column (see [`ParentRow::col`]). As a `where`
/// value it defines a [`Correlation`], not a filter.
pub struct Parent(Box<str>);

/// The right-hand side of a `where`: a literal, or a [`Parent`] correlation ref.
/// You rarely name this — pass a literal (`5`, `"bob"`, `true`, `None::<i64>`) or
/// a `row.col(..)` and the [`IntoRhs`] conversions build it.
pub enum Rhs {
    Lit(Lit),
    Parent(Box<str>),
}

// ---------------------------------------------------------------------------
// Value / operator / direction conversions — the "loosely typed" surface
// ---------------------------------------------------------------------------

/// Convert a Rust value into an AST [`Lit`]. Implemented for the obvious scalars
/// (`&str`, `String`, `bool`, `i32`, `i64`, `f64`, `Lit` itself) and for
/// `Option<T>` (where `None` ⇒ [`Lit::Null`]).
pub trait IntoLit {
    fn into_lit(self) -> Lit;
}

/// Convert a Rust value into a [`Rhs`]. Every [`IntoLit`] type is an `Rhs::Lit`;
/// a [`Parent`] is an `Rhs::Parent` (a correlation).
pub trait IntoRhs {
    fn into_rhs(self) -> Rhs;
}

/// Convert into an [`Op`]. Implemented for `Op` and for the usual SQL spellings
/// as `&str` (`"="`, `"!="`, `"<"`, `">="`, `"LIKE"`, `"IN"`, …) — handy in tests;
/// an unknown spelling panics.
pub trait IntoOp {
    fn into_op(self) -> Op;
}

/// Convert into a [`Dir`]. Implemented for `Dir`, and for `"asc"`/`"desc"`.
pub trait IntoDir {
    fn into_dir(self) -> Dir;
}

macro_rules! into_lit_scalar {
    ($($t:ty => $ctor:expr),* $(,)?) => {
        $(
            impl IntoLit for $t {
                fn into_lit(self) -> Lit { ($ctor)(self) }
            }
            impl IntoRhs for $t {
                fn into_rhs(self) -> Rhs { Rhs::Lit(self.into_lit()) }
            }
        )*
    };
}

into_lit_scalar! {
    Lit    => |v: Lit| v,
    bool   => Lit::Bool,
    i32    => |v: i32| Lit::Int(v as i64),
    // Exact (design 226 Stage B): the former `v as f64` rounded above 2^53.
    i64    => Lit::Int,
    f64    => Lit::Number,
    &str   => |v: &str| Lit::Str(v.into()),
    String => |v: String| Lit::Str(v.into_boxed_str()),
}

impl<T: IntoLit> IntoLit for Option<T> {
    fn into_lit(self) -> Lit {
        self.map_or(Lit::Null, IntoLit::into_lit)
    }
}
impl<T: IntoLit> IntoRhs for Option<T> {
    fn into_rhs(self) -> Rhs {
        Rhs::Lit(self.into_lit())
    }
}

impl IntoRhs for Rhs {
    fn into_rhs(self) -> Rhs {
        self
    }
}
impl IntoRhs for Parent {
    fn into_rhs(self) -> Rhs {
        Rhs::Parent(self.0)
    }
}

impl IntoOp for Op {
    fn into_op(self) -> Op {
        self
    }
}
impl IntoOp for &str {
    fn into_op(self) -> Op {
        match self {
            "=" | "==" => Op::Eq,
            "!=" | "<>" => Op::Ne,
            "<" => Op::Lt,
            "<=" => Op::Le,
            ">" => Op::Gt,
            ">=" => Op::Ge,
            "IS" => Op::Is,
            "IS NOT" => Op::IsNot,
            "LIKE" => Op::Like,
            "NOT LIKE" => Op::NotLike,
            "ILIKE" => Op::ILike,
            "NOT ILIKE" => Op::NotILike,
            "IN" => Op::In,
            "NOT IN" => Op::NotIn,
            other => panic!("unknown operator: {other:?}"),
        }
    }
}

impl IntoDir for Dir {
    fn into_dir(self) -> Dir {
        self
    }
}
impl IntoDir for &str {
    fn into_dir(self) -> Dir {
        match self {
            "asc" | "ASC" => Dir::Asc,
            "desc" | "DESC" => Dir::Desc,
            other => panic!("unknown sort direction: {other:?} (want \"asc\"/\"desc\")"),
        }
    }
}

// ---------------------------------------------------------------------------
// The builder
// ---------------------------------------------------------------------------

/// Build the `Column <op> Literal` simple condition the fluent builder emits.
fn col_op_lit(field: &str, op: Op, value: Lit) -> Condition {
    Condition::Simple(SimpleCondition {
        op,
        left: ValuePosition::Column { name: field.into() },
        right: ValuePosition::Literal { value },
    })
}

/// Options for a `where_exists` / `where_not_exists` correlated subquery — an
/// extensible struct so the common case stays `where_exists(f)` and opt-in behaviors
/// ride a `..Default::default()` struct via the `_with` variants.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExistsOpts {
    /// Fold this `EXISTS` as a build-time **scalar** subquery
    /// (`SCALAR-SUBQUERY-DESIGN.md`): when the child binds a statically-unique key, the
    /// resolver reads it once, inlines the correlation value as a literal, and deletes
    /// the join. **Snapshot semantics** — the inlined value does not react to later
    /// child changes (design §3). Default `false` (a live `EXISTS` join).
    pub scalar: bool,
}

/// Build a `(NOT) EXISTS` condition over a correlated subquery.
fn exists_cond(related: CorrelatedSubquery, op: ExistsOp, opts: ExistsOpts) -> Condition {
    Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related,
        op,
        flip: None,
        scalar: opts.scalar.then_some(true),
        plan_id: None,
    })
}

/// Fold a group's clauses into one `Or` (a lone clause needs no wrapper). Empty is
/// a build-time mistake — an `OR` of nothing is never true — so it fails loudly.
fn fold_or(mut conditions: Vec<Condition>) -> Condition {
    assert!(
        !conditions.is_empty(),
        "a `where_any`/`any` group is empty — add at least one condition"
    );
    if conditions.len() == 1 {
        conditions.pop().expect("len checked == 1")
    } else {
        Condition::Or { conditions }
    }
}

/// Fold a group's clauses into one `And` (a lone clause needs no wrapper). Empty is
/// rejected for symmetry with [`fold_or`].
fn fold_and(mut conditions: Vec<Condition>) -> Condition {
    assert!(
        !conditions.is_empty(),
        "a `where_all`/`all` group is empty — add at least one condition"
    );
    if conditions.len() == 1 {
        conditions.pop().expect("len checked == 1")
    } else {
        Condition::And { conditions }
    }
}

impl Query {
    fn new(table: &str) -> Query {
        Query {
            ast: Ast::new(table),
            corr_parent: Vec::new(),
            corr_child: Vec::new(),
        }
    }

    /// Set this query's alias (the relationship name when it is a `sub`/`exists`
    /// child). Usually left to [`Query::sub_as`].
    pub fn alias(mut self, alias: &str) -> Query {
        self.ast.alias = Some(alias.into());
        self
    }

    /// Project a column. Chain for several (`.select("a").select("b")`); omit
    /// entirely to select **all** columns.
    pub fn select(mut self, col: &str) -> Query {
        self.ast
            .select
            .get_or_insert_with(Vec::new)
            .push(col.into());
        self
    }

    /// `field = value` — **or**, if `value` is a `row.col(..)` [`Parent`] ref, a
    /// correlation to the parent (child `field` ↔ parent column). See module docs.
    #[allow(clippy::should_implement_trait)] // intentional: mirrors the TS `.where`
    pub fn r#where(mut self, field: &str, value: impl IntoRhs) -> Query {
        match value.into_rhs() {
            Rhs::Parent(parent_col) => {
                // Siphon into the pending correlation rather than the filter tree.
                self.corr_child.push(field.into());
                self.corr_parent.push(parent_col);
            }
            Rhs::Lit(value) => self.push_condition(col_op_lit(field, Op::Eq, value)),
        }
        self
    }

    /// `field <op> value` with an explicit operator (`"<"`, `">="`, `"LIKE"`, …
    /// or an [`Op`]). The value is always a literal here — correlations go
    /// through [`Query::where`].
    pub fn where_op(mut self, field: &str, op: impl IntoOp, value: impl IntoLit) -> Query {
        self.push_condition(col_op_lit(field, op.into_op(), value.into_lit()));
        self
    }

    /// `field IN (values…)`.
    pub fn where_in<V: IntoLit>(
        mut self,
        field: &str,
        values: impl IntoIterator<Item = V>,
    ) -> Query {
        let arr = Lit::Array(values.into_iter().map(IntoLit::into_lit).collect());
        self.push_condition(col_op_lit(field, Op::In, arr));
        self
    }

    /// Add a correlated child relationship. The closure receives the parent
    /// [`ParentRow`] and returns the child query; its `where(child, row.col(parent))`
    /// calls define the [`Correlation`]. (Generic `related`; the closure picks the
    /// child table.)
    #[allow(clippy::should_implement_trait)] // intentional API name (not `std::ops::Sub`)
    pub fn sub(self, f: impl FnOnce(ParentRow) -> Query) -> Query {
        self.sub_inner(None, f)
    }

    /// Like [`Query::sub`], but names the relationship (sets the child's alias).
    pub fn sub_as(self, alias: &str, f: impl FnOnce(ParentRow) -> Query) -> Query {
        self.sub_inner(Some(alias.into()), f)
    }

    fn sub_inner(mut self, alias: Option<Box<str>>, f: impl FnOnce(ParentRow) -> Query) -> Query {
        let mut child = f(ParentRow);
        if alias.is_some() {
            child.ast.alias = alias;
        }
        let csq = child.into_correlated("sub");
        self.ast.related.push(csq);
        self
    }

    /// Add a **relationship aggregate** — `issue { commentCount: count(comments) }`
    /// (`REDUCE-DESIGN.md` §9). Same closure/correlation mechanism as [`Query::sub`]
    /// (the child query relates to the parent via `row.col(..)`), but instead of
    /// materializing the child rows the relationship surfaces a single scalar `count(*)`
    /// of them, named `alias`. The builder lowers it to a grouped `reduce` + a
    /// scalar-projected singular relationship; an empty (childless) parent reads `0`.
    pub fn count_as(mut self, alias: &str, f: impl FnOnce(ParentRow) -> Query) -> Query {
        let mut child = f(ParentRow);
        child.ast.alias = Some(alias.into());
        child.ast.aggregate = Some(Aggregate::Count);
        let csq = child.into_correlated("count_as");
        self.ast.related.push(csq);
        self
    }

    /// Add a **relationship aggregate** surfacing `sum(col)` of the child rows —
    /// `issue { totalEstimate: sum(subtasks.estimate) }` (`REDUCE-DESIGN.md` §9). Like
    /// [`count_as`](Query::count_as) but the scalar is the Σ of the child column `col`
    /// (non-`NULL` values); a childless parent reads `NULL` (SQL's `sum` of no rows).
    pub fn sum_as(mut self, alias: &str, col: &str, f: impl FnOnce(ParentRow) -> Query) -> Query {
        let mut child = f(ParentRow);
        child.ast.alias = Some(alias.into());
        child.ast.aggregate = Some(Aggregate::Sum(col.into()));
        let csq = child.into_correlated("sum_as");
        self.ast.related.push(csq);
        self
    }

    /// Add a **relationship aggregate** surfacing `avg(col)` of the child rows —
    /// `issue { avgEstimate: avg(subtasks.estimate) }` (`REDUCE-DESIGN.md` §9). Like
    /// [`count_as`](Query::count_as) but the scalar is the mean of the child column `col`
    /// over its non-`NULL` values; a childless parent reads `NULL`.
    pub fn avg_as(mut self, alias: &str, col: &str, f: impl FnOnce(ParentRow) -> Query) -> Query {
        let mut child = f(ParentRow);
        child.ast.alias = Some(alias.into());
        child.ast.aggregate = Some(Aggregate::Avg(col.into()));
        let csq = child.into_correlated("avg_as");
        self.ast.related.push(csq);
        self
    }

    /// Aggregate **this** query's own rows into a top-level `count(*)`
    /// (`REDUCE-DESIGN.md` §8) — the SQL `SELECT count(*) FROM table`. Without
    /// [`group_by`](Query::group_by) it is a **global** count (one `[count]` row, value
    /// `0` even on empty input); with it, one `[group…, count]` row per group. Distinct
    /// from [`count_as`](Query::count_as), which counts a *child relationship*; this
    /// reshapes the query itself into the aggregate. Combine with
    /// [`having`](Query::having) to filter the post-aggregation rows.
    pub fn count(mut self) -> Query {
        self.ast.aggregate = Some(Aggregate::Count);
        self
    }

    /// Aggregate **this** query's own rows into a top-level `sum(col)`
    /// (`REDUCE-DESIGN.md` §8) — the SQL `SELECT sum(col) FROM table`. Global by default
    /// (one `[sum]` row, `NULL` on empty input); with [`group_by`](Query::group_by), one
    /// `[group…, sum]` row per group. Combine with [`having`](Query::having) to filter
    /// the post-aggregation rows.
    pub fn sum(mut self, col: &str) -> Query {
        self.ast.aggregate = Some(Aggregate::Sum(col.into()));
        self
    }

    /// Aggregate **this** query's own rows into a top-level `avg(col)`
    /// (`REDUCE-DESIGN.md` §8) — the SQL `SELECT avg(col) FROM table`. Global by default
    /// (one `[avg]` row, `NULL` on empty input); with [`group_by`](Query::group_by), one
    /// `[group…, avg]` row per group.
    pub fn avg(mut self, col: &str) -> Query {
        self.ast.aggregate = Some(Aggregate::Avg(col.into()));
        self
    }

    /// Add a top-level `GROUP BY` column (chain for a compound key). Only meaningful
    /// alongside [`count`](Query::count); the grouped result is one `[group…, count]`
    /// row per distinct value-tuple, keyed and sorted by the group columns.
    pub fn group_by(mut self, col: &str) -> Query {
        self.ast.group_by.push(col.into());
        self
    }

    /// `HAVING (…)` — filter the **post-aggregation** rows of a [`count`](Query::count)
    /// query (`REDUCE-DESIGN.md` §4: a filter directly above the reduce). The closure
    /// receives a fresh [`Cond`]; its clauses address the aggregate's *output* columns —
    /// the [`group_by`](Query::group_by) columns and the synthetic `count` column — and
    /// `AND` together (nest via [`Cond::any`] / [`Cond::all`]). E.g.
    /// `.group_by("status").count().having(|c| c.where_op("count", ">", 3))`.
    pub fn having(mut self, f: impl FnOnce(Cond) -> Cond) -> Query {
        self.ast.having = Some(fold_and(f(Cond::new()).conditions));
        self
    }

    /// Filter this parent by a **child relationship aggregate's count** —
    /// `issue WHERE count(comments) > 10` (`PARENT-AGGREGATE-FILTER-DESIGN.md`). `alias`
    /// must name a [`count_as`](Query::count_as) relationship already attached to this
    /// query; this drops parents whose child count fails `<op> <val>`, maintained
    /// incrementally (a child add/remove crossing the threshold adds/removes the parent).
    /// The display `count_as` is untouched — the parent row still shows the real count.
    ///
    /// Distinct from [`having`](Query::having), which filters a **top-level**
    /// [`count`](Query::count)'s own output rows; this gates a *parent* by a *child*
    /// aggregate (lowered to an `EXISTS` over a `HAVING`-filtered reduce, design §3).
    ///
    /// **v1: high-pass predicates only.** A childless parent forms no group, so the engine
    /// rejects (at build, [`BuildError::Unsupported`](crate::builder::BuildError)) a
    /// predicate *true* at count 0 (`<= n`, `< n` for `n ≥ 1`, `= 0`, `>= 0`); those need
    /// row-widening (deferred Tier 2, design §5). `> n` (`n ≥ 0`), `>= n`/`= n`/`!= n`
    /// (`n ≥ 1`) are in the safe set. Panics if `alias` is not a `count_as` relationship.
    pub fn having_count(mut self, alias: &str, op: impl IntoOp, val: i64) -> Query {
        let gate = {
            let display = self
                .ast
                .related
                .iter()
                .find(|csq| {
                    csq.subquery.aggregate == Some(Aggregate::Count)
                        && csq.subquery.alias.as_deref() == Some(alias)
                })
                .unwrap_or_else(|| {
                    panic!(
                        "having_count({alias:?}, …): this query has no `count_as({alias:?}, …)` \
                         relationship to filter on — attach the child count aggregate first"
                    )
                });
            // Clone the display aggregate's {correlation, child, where, aggregate}, hide it
            // under a slot-distinct alias (never colliding with the display `related`), and
            // attach the post-aggregation `count <op> val` HAVING. The builder lowers this
            // EXISTS over a HAVING-filtered reduce (design §4); the gate's reduce is a
            // second fold over the same child rows (A1, design §9 — dedupe is a follow-up).
            let mut gate = display.clone();
            gate.subquery.alias = Some(format!("__having_{alias}").into());
            gate.subquery.having = Some(col_op_lit("count", op.into_op(), val.into_lit()));
            gate
        };
        self.push_condition(exists_cond(gate, ExistsOp::Exists, ExistsOpts::default()));
        self
    }

    /// `WHERE EXISTS (<correlated child>)`. Same closure/correlation mechanism as
    /// [`Query::sub`], but the child becomes an `EXISTS` filter rather than a
    /// materialized relationship.
    pub fn where_exists(self, f: impl FnOnce(ParentRow) -> Query) -> Query {
        self.where_exists_with(f, ExistsOpts::default())
    }

    /// [`Query::where_exists`] with [`ExistsOpts`] — e.g. `ExistsOpts { scalar: true }`
    /// to request a build-time scalar fold (`SCALAR-SUBQUERY-DESIGN.md`).
    pub fn where_exists_with(
        mut self,
        f: impl FnOnce(ParentRow) -> Query,
        opts: ExistsOpts,
    ) -> Query {
        let csq = f(ParentRow).into_correlated("where_exists");
        self.push_condition(exists_cond(csq, ExistsOp::Exists, opts));
        self
    }

    /// `WHERE NOT EXISTS (<correlated child>)`.
    pub fn where_not_exists(self, f: impl FnOnce(ParentRow) -> Query) -> Query {
        self.where_not_exists_with(f, ExistsOpts::default())
    }

    /// [`Query::where_not_exists`] with [`ExistsOpts`].
    pub fn where_not_exists_with(
        mut self,
        f: impl FnOnce(ParentRow) -> Query,
        opts: ExistsOpts,
    ) -> Query {
        let csq = f(ParentRow).into_correlated("where_not_exists");
        self.push_condition(exists_cond(csq, ExistsOp::NotExists, opts));
        self
    }

    /// `WHERE EXISTS (<correlated child>)` as a **server-only, non-syncing** gate
    /// (`exists_noSync`, `EXISTS-NOSYNC-DESIGN.md`). Stamps the subquery `system:
    /// Permissions`, which (a) gates parent visibility server-side exactly like
    /// [`where_exists`](Query::where_exists), but (b) marks the gate so the normalized
    /// serializer prunes its witnesses from the footprint — the permission table's rows are
    /// never synced to the client, and the client never re-evaluates the gate. Build this on
    /// the **server's** query; the client holds its own un-gated query.
    pub fn where_exists_no_sync(mut self, f: impl FnOnce(ParentRow) -> Query) -> Query {
        let mut csq = f(ParentRow).into_correlated("where_exists_no_sync");
        csq.system = Some(System::Permissions);
        self.push_condition(exists_cond(csq, ExistsOp::Exists, ExistsOpts::default()));
        self
    }

    /// `WHERE NOT EXISTS (<correlated child>)` as a **server-only, non-syncing** gate — the
    /// `NOT EXISTS` form of [`where_exists_no_sync`](Query::where_exists_no_sync) (a deny-style
    /// permission rule). A `NOT EXISTS` gate passes on zero children, so it carries no
    /// witnesses to sync; the `system: Permissions` stamp is recorded for symmetry and to keep
    /// the gate off the client.
    pub fn where_not_exists_no_sync(mut self, f: impl FnOnce(ParentRow) -> Query) -> Query {
        let mut csq = f(ParentRow).into_correlated("where_not_exists_no_sync");
        csq.system = Some(System::Permissions);
        self.push_condition(exists_cond(csq, ExistsOp::NotExists, ExistsOpts::default()));
        self
    }

    /// `WHERE (c1 OR c2 OR …)` — an **OR** group. The closure receives a fresh
    /// [`Cond`] to which it adds clauses (`where`/`where_op`/`where_in`/
    /// `where_exists`, or nested `any`/`all`). The group `AND`-combines with any
    /// other top-level `where`s, exactly like the simple forms.
    pub fn where_any(mut self, f: impl FnOnce(Cond) -> Cond) -> Query {
        self.push_condition(fold_or(f(Cond::new()).conditions));
        self
    }

    /// `WHERE (c1 AND c2 AND …)` — an explicit **AND** group. Redundant at the top
    /// level (chained `where`s already `AND`), but the way to express a grouped
    /// `AND` *nested inside* a [`Query::where_any`], e.g. `(a AND b) OR c`.
    pub fn where_all(mut self, f: impl FnOnce(Cond) -> Cond) -> Query {
        self.push_condition(fold_and(f(Cond::new()).conditions));
        self
    }

    /// Cap the number of rows.
    pub fn limit(mut self, n: u32) -> Query {
        self.ast.limit = Some(n);
        self
    }

    /// Return a **single** row: the result is presented as one object (or
    /// `null`/absent) instead of an array. Records the intent on the AST
    /// ([`Ast::one`]) and caps the query to one row (`limit = 1`). The engine stays
    /// plural internally; the single-element unwrap happens at the result boundary.
    /// Used on a `sub`/`sub_as` child query, it makes **that** relationship singular.
    pub fn one(mut self) -> Query {
        self.ast.one = true;
        self.ast.limit = Some(1);
        self
    }

    /// Append an ordering term (`"asc"`/`"desc"` or a [`Dir`]). Chain for a
    /// compound sort.
    pub fn order_by(mut self, field: &str, dir: impl IntoDir) -> Query {
        self.ast
            .order_by
            .push(OrderPart(field.into(), dir.into_dir()));
        self
    }

    /// Page from `col = val`, **inclusive** of that row.
    pub fn start_at(self, col: &str, val: impl IntoLit) -> Query {
        self.start_row(vec![(col.into(), val.into_lit())], false)
    }

    /// Page from `col = val`, **exclusive** of that row.
    pub fn start_after(self, col: &str, val: impl IntoLit) -> Query {
        self.start_row(vec![(col.into(), val.into_lit())], true)
    }

    /// Set a (possibly multi-column) paging bound directly. `exclusive` ⇒ skip the
    /// bound row.
    pub fn start_row(mut self, row: Vec<(Box<str>, Lit)>, exclusive: bool) -> Query {
        self.ast.start = Some(Bound {
            row: row.into_iter().collect(),
            exclusive,
        });
        self
    }

    /// Finish building and yield the [`Ast`].
    pub fn build(self) -> Ast {
        debug_assert!(
            self.corr_child.is_empty(),
            "build() called on a query holding an unconsumed parent correlation — a \
             `row.col(..)` reference escaped its `sub`/`where_exists` closure"
        );
        self.ast
    }

    /// AND a new condition into the `where` tree, flattening a top-level `And`.
    fn push_condition(&mut self, cond: Condition) {
        self.ast.r#where = Some(match self.ast.r#where.take() {
            None => cond,
            Some(Condition::And { mut conditions }) => {
                conditions.push(cond);
                Condition::And { conditions }
            }
            Some(existing) => Condition::And {
                conditions: vec![existing, cond],
            },
        });
    }

    /// Drain the pending correlation into a [`CorrelatedSubquery`]. Panics if no
    /// correlation was established (a `sub`/`exists` child must relate to its
    /// parent) — a loud, test-time guard against the easy mistake of forgetting
    /// the `row.col(..)` link.
    fn into_correlated(self, what: &str) -> CorrelatedSubquery {
        assert!(
            !self.corr_child.is_empty(),
            "{what}() child has no correlation — relate it to the parent with \
             `.r#where(childCol, row.col(parentCol))`"
        );
        debug_assert_eq!(
            self.corr_parent.len(),
            self.corr_child.len(),
            "correlation key halves must be the same length"
        );
        CorrelatedSubquery {
            correlation: Correlation {
                parent_field: self.corr_parent,
                child_field: self.corr_child,
            },
            subquery: Box::new(self.ast),
            system: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Cond — a nestable boolean group (the `AND`/`OR` tree builder)
// ---------------------------------------------------------------------------

/// A boolean **condition group** under construction — the building block for
/// nested `AND`/`OR` filter trees. You don't make one directly; a fresh `Cond` is
/// handed to the closure of [`Query::where_any`] / [`Query::where_all`] (and the
/// nested [`Cond::any`] / [`Cond::all`]). Add clauses by chaining, just like
/// [`Query`].
///
/// **No correlations here.** A correlation (`row.col(..)`) is a property of an
/// EXISTS subquery, never of a free-standing condition, so [`Cond::where`] takes a
/// plain literal (`impl IntoLit`). To correlate, nest a [`Cond::where_exists`]
/// whose *child* query carries the `row.col(..)` link.
pub struct Cond {
    /// The clauses accumulated so far; folded into one `And`/`Or` by the enclosing
    /// `where_all`/`where_any` (or `all`/`any`).
    conditions: Vec<Condition>,
}

impl Cond {
    fn new() -> Cond {
        Cond {
            conditions: Vec::new(),
        }
    }

    /// `field = value` (a literal — see the type docs on why correlations don't
    /// belong in a group).
    #[allow(clippy::should_implement_trait)] // mirrors `Query::where`
    pub fn r#where(mut self, field: &str, value: impl IntoLit) -> Cond {
        self.conditions
            .push(col_op_lit(field, Op::Eq, value.into_lit()));
        self
    }

    /// `field <op> value` with an explicit operator (`"<"`, `">="`, `"LIKE"`, … or
    /// an [`Op`]).
    pub fn where_op(mut self, field: &str, op: impl IntoOp, value: impl IntoLit) -> Cond {
        self.conditions
            .push(col_op_lit(field, op.into_op(), value.into_lit()));
        self
    }

    /// `field IN (values…)`.
    pub fn where_in<V: IntoLit>(
        mut self,
        field: &str,
        values: impl IntoIterator<Item = V>,
    ) -> Cond {
        let arr = Lit::Array(values.into_iter().map(IntoLit::into_lit).collect());
        self.conditions.push(col_op_lit(field, Op::In, arr));
        self
    }

    /// `EXISTS (<correlated subquery>)` — same closure/correlation mechanism as
    /// [`Query::where_exists`].
    pub fn where_exists(self, f: impl FnOnce(ParentRow) -> Query) -> Cond {
        self.where_exists_with(f, ExistsOpts::default())
    }

    /// [`Cond::where_exists`] with [`ExistsOpts`] (e.g. `{ scalar: true }`).
    pub fn where_exists_with(
        mut self,
        f: impl FnOnce(ParentRow) -> Query,
        opts: ExistsOpts,
    ) -> Cond {
        let csq = f(ParentRow).into_correlated("where_exists");
        self.conditions
            .push(exists_cond(csq, ExistsOp::Exists, opts));
        self
    }

    /// `NOT EXISTS (<correlated subquery>)`.
    pub fn where_not_exists(self, f: impl FnOnce(ParentRow) -> Query) -> Cond {
        self.where_not_exists_with(f, ExistsOpts::default())
    }

    /// [`Cond::where_not_exists`] with [`ExistsOpts`].
    pub fn where_not_exists_with(
        mut self,
        f: impl FnOnce(ParentRow) -> Query,
        opts: ExistsOpts,
    ) -> Cond {
        let csq = f(ParentRow).into_correlated("where_not_exists");
        self.conditions
            .push(exists_cond(csq, ExistsOp::NotExists, opts));
        self
    }

    /// `EXISTS (<correlated subquery>)` as a **server-only, non-syncing** gate — the nestable
    /// form of [`Query::where_exists_no_sync`].
    pub fn where_exists_no_sync(mut self, f: impl FnOnce(ParentRow) -> Query) -> Cond {
        let mut csq = f(ParentRow).into_correlated("where_exists_no_sync");
        csq.system = Some(System::Permissions);
        self.conditions
            .push(exists_cond(csq, ExistsOp::Exists, ExistsOpts::default()));
        self
    }

    /// `NOT EXISTS (<correlated subquery>)` as a **server-only, non-syncing** gate — the
    /// nestable form of [`Query::where_not_exists_no_sync`].
    pub fn where_not_exists_no_sync(mut self, f: impl FnOnce(ParentRow) -> Query) -> Cond {
        let mut csq = f(ParentRow).into_correlated("where_not_exists_no_sync");
        csq.system = Some(System::Permissions);
        self.conditions
            .push(exists_cond(csq, ExistsOp::NotExists, ExistsOpts::default()));
        self
    }

    /// Nest an **OR** sub-group: `(… OR …)`.
    pub fn any(mut self, f: impl FnOnce(Cond) -> Cond) -> Cond {
        self.conditions.push(fold_or(f(Cond::new()).conditions));
        self
    }

    /// Nest an **AND** sub-group: `(… AND …)`.
    pub fn all(mut self, f: impl FnOnce(Cond) -> Cond) -> Cond {
        self.conditions.push(fold_and(f(Cond::new()).conditions));
        self
    }
}
