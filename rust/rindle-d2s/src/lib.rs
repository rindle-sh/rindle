//! Compile a deltic [`Ast`] into **one** SQLite `SELECT` whose single column
//! (`"rindle_result"`) is the entire query result as a nested-JSON document — the
//! exact hierarchical shape the IVM [`View`](rindle::View) materializes. This is the
//! SQLite analogue of mono's `z2s` package (`packages/z2s/src/compiler.ts`), which
//! compiles the same AST for **Postgres**.
//!
//! # Why a separate compiler
//!
//! The IVM engine answers a query by *decomposing* it into per-table fetches wired
//! into a dataflow graph. A SQL database answers the *same* query *declaratively*
//! in one statement. Running both over the same data and comparing the results is
//! the strongest correctness oracle there is: it is the only check that grounds the
//! engine's join / EXISTS / ordering / limit *logic* against an outside source of
//! truth (the engine's own two same-graph oracles — memory-leaf-vs-sqlite-leaf and
//! view-self-consistency — cannot catch a *consistently* wrong dataflow). mono uses
//! Postgres for this; we use SQLite (no Node, deterministic, links the same vendored
//! engine the IVM reads from, so value/collation/LIKE semantics agree for free).
//!
//! # Shape (mirrors `data_node_to_json`)
//!
//! Each row becomes `json_object('col', t."col", …, 'relName', <child>, …)`: every
//! column by name, then every materialized relationship by name (a nested object for
//! a `one`, a nested array for a `many`). A plural level is wrapped in
//! `json_group_array(... ORDER BY …)`; a singular level / the `LIMIT 1` of a `one`
//! takes the first row or JSON `null`. There is **no `row_to_json`** in SQLite, so
//! the columns are enumerated explicitly. EXISTS/NOT-EXISTS conditions compile to
//! correlated `EXISTS (SELECT 1 …)` subqueries (they filter, they do not project).
//!
//! # Values (minimal — no casting)
//!
//! SQLite is dynamically typed, so literals are **inlined as native SQLite values**
//! (`5`, `0.99`, `'text'`, `1`/`0` for bools, `NULL`) with no `::text::<type>`
//! coercion dance. Identifiers are double-quoted. `LIKE`/`ILIKE` reuse the engine's
//! `ESCAPE '\'` + `lower(...)` lowering so they match the in-memory matcher
//! (`tests/like_parity.rs`); the executing connection must set
//! `PRAGMA case_sensitive_like = ON` (the IVM's `TableSource` connection already does).
//!
//! # Ordering
//!
//! [`compile`] runs [`rindle::complete_ordering`] (append the full primary key to every
//! `order_by`, at every level) before emitting — the same pass the IVM builder runs —
//! so both sides produce a total order and identical row/child order. SQLite's default
//! `NULL`-first-on-`ASC` / `NULL`-last-on-`DESC` matches the engine's `null < everything`.

use std::collections::{HashMap, HashSet};

use rindle::{
    Aggregate, Bound, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir,
    ExistsOp, Lit, Op, OrderPart, SimpleCondition, ValuePosition,
};
// `Ast` is re-exported from the crate root.
use rindle::Ast;

/// Whether a relationship yields a single child object (`one`) or an array (`many`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}

/// Per-table metadata the compiler needs: the column list (for `json_object`
/// enumeration, in the same order the View's `Schema` projects), the primary key
/// (for [`rindle::complete_ordering`]), and each declared relationship's cardinality
/// (the only fact not already carried by the [`Ast`]). The relationship *structure*
/// (correlation keys, nesting, `where`/`order`/`limit`) lives in the `Ast`.
#[derive(Clone, Debug, Default)]
pub struct Catalog {
    tables: HashMap<Box<str>, CatalogTable>,
}

#[derive(Clone, Debug)]
struct CatalogTable {
    columns: Vec<Box<str>>,
    primary_key: Vec<Box<str>>,
    relationships: HashMap<Box<str>, Cardinality>,
}

impl Catalog {
    pub fn new() -> Catalog {
        Catalog::default()
    }

    /// Declare a table: its columns (in projection order) and primary-key columns.
    pub fn table(&mut self, name: &str, columns: &[&str], primary_key: &[&str]) -> &mut Catalog {
        self.tables.insert(
            name.into(),
            CatalogTable {
                columns: columns.iter().map(|c| (*c).into()).collect(),
                primary_key: primary_key.iter().map(|c| (*c).into()).collect(),
                relationships: HashMap::new(),
            },
        );
        self
    }

    /// Declare a relationship's cardinality on `table` (the relationship name is the
    /// child subquery's alias in the `Ast`).
    pub fn relationship(&mut self, table: &str, name: &str, card: Cardinality) -> &mut Catalog {
        if let Some(t) = self.tables.get_mut(table) {
            t.relationships.insert(name.into(), card);
        }
        self
    }

    fn columns(&self, table: &str) -> Result<&[Box<str>], CompileError> {
        self.tables
            .get(table)
            .map(|t| t.columns.as_slice())
            .ok_or_else(|| CompileError::UnknownTable(table.into()))
    }

    fn pk(&self, table: &str) -> Vec<Box<str>> {
        self.tables
            .get(table)
            .map(|t| t.primary_key.clone())
            .unwrap_or_default()
    }

    fn cardinality(&self, table: &str, rel: &str) -> Result<Cardinality, CompileError> {
        self.tables
            .get(table)
            .and_then(|t| t.relationships.get(rel))
            .copied()
            .ok_or_else(|| CompileError::UnknownRelationship {
                table: table.into(),
                rel: rel.into(),
            })
    }
}

/// Why a query could not be compiled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompileError {
    /// A referenced table is not in the [`Catalog`].
    UnknownTable(String),
    /// A `related`/EXISTS relationship's cardinality is not declared in the [`Catalog`].
    UnknownRelationship { table: String, rel: String },
    /// A subquery is missing an alias (every `related` child must be named).
    MissingAlias { table: String },
    /// An AST feature the compiler does not lower in the position it appears (rather
    /// than silently emit wrong SQL). Currently: a `start` paging bound *inside* an
    /// EXISTS subquery — `start` is lowered on materialized collections, but existence
    /// paths ignore paging, so a bound there is rejected loudly.
    Unsupported(&'static str),
}

impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompileError::UnknownTable(t) => write!(f, "unknown table `{t}`"),
            CompileError::UnknownRelationship { table, rel } => {
                write!(f, "unknown relationship `{rel}` on table `{table}`")
            }
            CompileError::MissingAlias { table } => {
                write!(f, "related subquery on `{table}` has no alias")
            }
            CompileError::Unsupported(what) => write!(f, "unsupported AST feature: {what}"),
        }
    }
}

impl std::error::Error for CompileError {}

/// Compile `ast` into a SQLite `SELECT` returning the **plural** result (a JSON
/// array) in the column `"rindle_result"`. Run it on a SQLite connection holding the
/// query's data; the one returned cell is the whole nested result tree as JSON text.
pub fn compile(catalog: &Catalog, ast: &Ast) -> Result<String, CompileError> {
    compile_with(catalog, ast, false)
}

/// Like [`compile`], but the root is **singular** (a single JSON object or `null`),
/// i.e. the equivalent of a `.one()` query.
pub fn compile_one(catalog: &Catalog, ast: &Ast) -> Result<String, CompileError> {
    compile_with(catalog, ast, true)
}

fn compile_with(catalog: &Catalog, ast: &Ast, singular: bool) -> Result<String, CompileError> {
    reject_unsupported(ast)?;
    let mut ast = ast.clone();
    rindle::complete_ordering(&mut ast, &|t| catalog.pk(t));
    let mut cx = Cx { catalog, n: 0 };
    let expr = cx.collection(&ast, singular, None)?;
    Ok(format!("SELECT {expr} AS \"rindle_result\""))
}

/// Reject AST features the compiler does not lower, anywhere in the tree — so an
/// unhandled field fails loudly rather than producing silently-wrong SQL.
///
/// `start` (a paging bound) is lowered as a keyset predicate, but only on the
/// **materialized collections** the compiler emits via [`Cx::collection`]: the root and
/// each `related` child (recursively). It is *not* honored inside an `EXISTS`/`NOT EXISTS`
/// subquery (existence ignores paging), so a `start` there is rejected here rather than
/// silently dropped.
fn reject_unsupported(ast: &Ast) -> Result<(), CompileError> {
    // `ast.start` IS honored at this (collection) node — do not reject it.
    for rel in &ast.related {
        if rel.subquery.aggregate.is_some() {
            // A `count(child)` aggregate reduces its rows away: a `start`/`limit`/nested
            // `related` on the subquery would change the IVM count (the builder applies
            // them in the reduce's input pipeline) but `count_expr` ignores them — reject
            // rather than silently diverge. (`order_by` is count-irrelevant — allowed.)
            reject_aggregate_subquery(&rel.subquery)?;
        } else {
            reject_unsupported(&rel.subquery)?; // collection: start honored, recurse
        }
    }
    if let Some(w) = &ast.r#where {
        reject_unsupported_cond(w)?;
    }
    Ok(())
}

/// Reject the AST features a `count`/`sum`/`avg` aggregate subquery does not honor (its
/// rows are reduced, not materialized): a paging `start`, a `limit`, or nested `related`.
fn reject_aggregate_subquery(ast: &Ast) -> Result<(), CompileError> {
    if ast.start.is_some() {
        return Err(CompileError::Unsupported("start inside an aggregate"));
    }
    if ast.limit.is_some() {
        return Err(CompileError::Unsupported("limit inside an aggregate"));
    }
    if !ast.related.is_empty() {
        return Err(CompileError::Unsupported(
            "nested related inside an aggregate",
        ));
    }
    if let Some(w) = &ast.r#where {
        reject_unsupported_cond(w)?;
    }
    Ok(())
}

fn reject_unsupported_cond(cond: &Condition) -> Result<(), CompileError> {
    match cond {
        Condition::Simple(_) => Ok(()),
        // EXISTS subquery: its `start` (and any nested) is not honored.
        Condition::CorrelatedSubquery(c) => reject_start_anywhere(&c.related.subquery),
        Condition::And { conditions } | Condition::Or { conditions } => {
            for c in conditions {
                reject_unsupported_cond(c)?;
            }
            Ok(())
        }
    }
}

/// Reject a `start` bound *anywhere* in `ast` (used for the EXISTS subtrees the compiler
/// does not page). Conservative: it rejects even positions a clever lowering might honor,
/// keeping the oracle sound rather than maximal.
fn reject_start_anywhere(ast: &Ast) -> Result<(), CompileError> {
    if ast.start.is_some() {
        return Err(CompileError::Unsupported(
            "start (paging) inside an EXISTS subquery",
        ));
    }
    for rel in &ast.related {
        reject_start_anywhere(&rel.subquery)?;
    }
    if let Some(w) = &ast.r#where {
        reject_start_anywhere_cond(w)?;
    }
    Ok(())
}

fn reject_start_anywhere_cond(cond: &Condition) -> Result<(), CompileError> {
    match cond {
        Condition::Simple(_) => Ok(()),
        Condition::CorrelatedSubquery(c) => reject_start_anywhere(&c.related.subquery),
        Condition::And { conditions } | Condition::Or { conditions } => {
            for c in conditions {
                reject_start_anywhere_cond(c)?;
            }
            Ok(())
        }
    }
}

/// Compile context: the catalog plus a monotonic counter for unique table aliases.
struct Cx<'a> {
    catalog: &'a Catalog,
    n: usize,
}

impl Cx<'_> {
    fn alias(&mut self, table: &str) -> String {
        let a = format!("{table}_{}", self.n);
        self.n += 1;
        a
    }

    /// A scalar-subquery expression producing the JSON for `ast` as either a single
    /// object (`singular`) or a `json_group_array` of objects, correlated to a parent
    /// when `correlate` is set (`parent_alias` + the `Correlation` key pair).
    fn collection(
        &mut self,
        ast: &Ast,
        singular: bool,
        correlate: Option<(&str, &Correlation)>,
    ) -> Result<String, CompileError> {
        let table = &*ast.table;
        let cols = self.catalog.columns(table)?.to_vec();
        let alias = self.alias(table);
        let row = self.row_object(ast, &alias, &cols)?;

        let mut conds: Vec<String> = Vec::new();
        if let Some((parent_alias, corr)) = correlate {
            conds.push(correlate_sql(parent_alias, &alias, corr));
        }
        if let Some(w) = &ast.r#where {
            conds.push(self.where_sql(w, &alias)?);
        }
        // Paging bound: keep rows at/after `start` under the (already PK-completed)
        // sort. `ast.order_by` here is the full sort key (`compile_with` ran
        // `complete_ordering`), so it is exactly the comparator the engine's `Skip` uses.
        if let Some(start) = &ast.start {
            conds.push(start_sql(start, &ast.order_by, &alias));
        }
        let where_clause = where_clause(&conds);
        let from = format!("{} AS {}", quote_ident(table), quote_ident(&alias));

        Ok(plural_or_singular(
            singular,
            &row,
            &from,
            &where_clause,
            &ast.order_by,
            &alias,
            ast.limit,
        ))
    }

    /// `json_object('c', a."c", …, 'rel', <child expr>, …)`. A column shadowed by a
    /// same-named relationship is dropped (SQLite `json_object` rejects duplicate keys;
    /// this matches d2s and the View, where the relationship wins).
    fn row_object(
        &mut self,
        ast: &Ast,
        alias: &str,
        cols: &[Box<str>],
    ) -> Result<String, CompileError> {
        let rel_names: HashSet<&str> = ast
            .related
            .iter()
            .filter_map(|r| r.subquery.alias.as_deref())
            .collect();

        let mut parts: Vec<String> = Vec::new();
        // Projection (PROJECTION-SUPPORT-DESIGN.md §6): when `select` is set, emit only the
        // selected columns (relationships are not columns and are always emitted below). This
        // mirrors the engine's view projection so the differential oracle agrees.
        let selected: Option<HashSet<&str>> = ast
            .select
            .as_ref()
            .map(|s| s.iter().map(|c| &**c).collect());
        for col in cols {
            if rel_names.contains(&**col) {
                continue;
            }
            if let Some(sel) = &selected {
                if !sel.contains(&**col) {
                    continue;
                }
            }
            parts.push(format!(
                "{}, {}.{}",
                json_key(col),
                quote_ident(alias),
                quote_ident(col)
            ));
        }
        for rel in &ast.related {
            let name = rel
                .subquery
                .alias
                .as_deref()
                .ok_or_else(|| CompileError::MissingAlias {
                    table: rel.subquery.table.to_string(),
                })?;
            // A relationship **aggregate** (`count`/`sum`/`avg` of a child,
            // REDUCE-DESIGN.md §9) is a scalar column, not a nested collection.
            let expr = match &rel.subquery.aggregate {
                Some(agg) => self.agg_expr(rel, alias, agg)?,
                None => self.relation_expr(&ast.table, rel, alias)?,
            };
            parts.push(format!("{}, {}", json_key(name), expr));
        }
        Ok(format!("json_object({})", parts.join(", ")))
    }

    /// The JSON expression for one materialized relationship (a column of the parent's
    /// `json_object`).
    fn relation_expr(
        &mut self,
        parent_table: &str,
        rel: &CorrelatedSubquery,
        parent_alias: &str,
    ) -> Result<String, CompileError> {
        let name = rel
            .subquery
            .alias
            .as_deref()
            .ok_or_else(|| CompileError::MissingAlias {
                table: rel.subquery.table.to_string(),
            })?;
        let singular = self.catalog.cardinality(parent_table, name)? == Cardinality::One;
        self.collection(
            &rel.subquery,
            singular,
            Some((parent_alias, &rel.correlation)),
        )
    }

    /// A scalar aggregate for a relationship aggregate (`REDUCE-DESIGN.md` §9): a
    /// correlated `(SELECT <agg> FROM child AS a WHERE <correlation> [AND <child.where>])`.
    /// The SQL aggregate matches the IVM's empty-relationship identity exactly: SQLite
    /// `count(*)` of no rows is `0` (the projection's `count` identity); `sum`/`avg` of no
    /// rows are `NULL` (the `sum`/`avg` identity). The subquery's order/limit/start/nested-
    /// `related` do not affect an aggregate and are rejected upstream ([`reject_unsupported`]).
    fn agg_expr(
        &mut self,
        rel: &CorrelatedSubquery,
        parent_alias: &str,
        agg: &Aggregate,
    ) -> Result<String, CompileError> {
        let sub = &rel.subquery;
        let alias = self.alias(&sub.table);
        let mut conds = vec![correlate_sql(parent_alias, &alias, &rel.correlation)];
        if let Some(w) = &sub.r#where {
            conds.push(self.where_sql(w, &alias)?);
        }
        let agg_sql = match agg {
            Aggregate::Count => "count(*)".to_string(),
            Aggregate::Sum(col) => {
                format!("sum({}.{})", quote_ident(&alias), quote_ident(col))
            }
            Aggregate::Avg(col) => {
                format!("avg({}.{})", quote_ident(&alias), quote_ident(col))
            }
        };
        Ok(format!(
            "(SELECT {} FROM {} AS {} WHERE {})",
            agg_sql,
            quote_ident(&sub.table),
            quote_ident(&alias),
            conds.join(" AND ")
        ))
    }

    /// A boolean SQL expression for a `where` condition tree.
    fn where_sql(&mut self, cond: &Condition, alias: &str) -> Result<String, CompileError> {
        Ok(match cond {
            Condition::And { conditions } => {
                if conditions.is_empty() {
                    "1".into()
                } else {
                    let mut parts = Vec::with_capacity(conditions.len());
                    for c in conditions {
                        parts.push(self.where_sql(c, alias)?);
                    }
                    format!("({})", parts.join(" AND "))
                }
            }
            Condition::Or { conditions } => {
                if conditions.is_empty() {
                    "0".into()
                } else {
                    let mut parts = Vec::with_capacity(conditions.len());
                    for c in conditions {
                        parts.push(self.where_sql(c, alias)?);
                    }
                    format!("({})", parts.join(" OR "))
                }
            }
            Condition::CorrelatedSubquery(c) => self.exists_sql(c, alias)?,
            Condition::Simple(sc) => self.simple_sql(sc, alias),
        })
    }

    /// `[NOT] EXISTS (SELECT 1 FROM child AS a WHERE <correlation> [AND <child.where>])`.
    /// `flip`/`scalar` are IVM routing flags with no effect on the SQL. Ordering, limit,
    /// and materialized children of the subquery do not affect existence and are dropped.
    fn exists_sql(
        &mut self,
        c: &CorrelatedSubqueryCondition,
        parent_alias: &str,
    ) -> Result<String, CompileError> {
        let sub = &c.related.subquery;
        let alias = self.alias(&sub.table);
        let mut conds = vec![correlate_sql(parent_alias, &alias, &c.related.correlation)];
        if let Some(w) = &sub.r#where {
            conds.push(self.where_sql(w, &alias)?);
        }
        let inner = format!(
            "SELECT 1 FROM {} AS {} WHERE {}",
            quote_ident(&sub.table),
            quote_ident(&alias),
            conds.join(" AND ")
        );
        Ok(match c.op {
            ExistsOp::Exists => format!("EXISTS ({inner})"),
            ExistsOp::NotExists => format!("NOT EXISTS ({inner})"),
        })
    }

    /// A single comparison. Operators render verbatim except: `IS`/`IS NOT` use
    /// SQLite's native null-safe forms, `ILIKE` folds via `lower()`, and `IN`/`NOT IN`
    /// inline the literal list.
    fn simple_sql(&self, sc: &SimpleCondition, alias: &str) -> String {
        let l = value_pos(&sc.left, alias);
        match sc.op {
            Op::Eq => format!("{l} = {}", value_pos(&sc.right, alias)),
            Op::Ne => format!("{l} != {}", value_pos(&sc.right, alias)),
            Op::Lt => format!("{l} < {}", value_pos(&sc.right, alias)),
            Op::Le => format!("{l} <= {}", value_pos(&sc.right, alias)),
            Op::Gt => format!("{l} > {}", value_pos(&sc.right, alias)),
            Op::Ge => format!("{l} >= {}", value_pos(&sc.right, alias)),
            Op::Is => format!("{l} IS {}", value_pos(&sc.right, alias)),
            Op::IsNot => format!("{l} IS NOT {}", value_pos(&sc.right, alias)),
            Op::Like => format!("{l} LIKE {} ESCAPE '\\'", value_pos(&sc.right, alias)),
            Op::NotLike => format!("{l} NOT LIKE {} ESCAPE '\\'", value_pos(&sc.right, alias)),
            Op::ILike => format!(
                "lower({l}) LIKE lower({}) ESCAPE '\\'",
                value_pos(&sc.right, alias)
            ),
            Op::NotILike => format!(
                "lower({l}) NOT LIKE lower({}) ESCAPE '\\'",
                value_pos(&sc.right, alias)
            ),
            Op::In => in_sql(&l, &sc.right, alias, false),
            Op::NotIn => in_sql(&l, &sc.right, alias, true),
        }
    }
}

/// Emit the plural (`json_group_array`) or singular (`json_object … LIMIT 1`) form.
/// When a `LIMIT` is present the rows are selected (ordered + limited) in an inner
/// subquery and then array-aggregated in the declared order, so both the top-N
/// selection and the array order are correct.
fn plural_or_singular(
    singular: bool,
    row: &str,
    from: &str,
    where_clause: &str,
    order: &[OrderPart],
    alias: &str,
    limit: Option<u32>,
) -> String {
    if singular {
        let order_clause = order_clause(order, alias);
        return format!("(SELECT {row} FROM {from}{where_clause}{order_clause} LIMIT 1)");
    }
    match limit {
        Some(n) => {
            let inner_order = order_clause(order, alias);
            let proj = order_proj(order, alias);
            let agg = agg_order(order);
            // `json(__r)` re-asserts the JSON subtype: SQLite drops it when a
            // `json_object(...)` value round-trips through the inner subquery's
            // `AS __r` column, which would otherwise re-encode the object as a string.
            format!(
                "COALESCE((SELECT json_group_array(json(__r){agg}) FROM \
                 (SELECT {row} AS __r{proj} FROM {from}{where_clause}{inner_order} LIMIT {n})), '[]')"
            )
        }
        None => {
            let agg = agg_order_inline(order, alias);
            format!(
                "COALESCE((SELECT json_group_array({row}{agg}) FROM {from}{where_clause}), '[]')"
            )
        }
    }
}

/// ` ORDER BY a."c0" ASC, a."c1" DESC` (empty when `order` is empty).
fn order_clause(order: &[OrderPart], alias: &str) -> String {
    if order.is_empty() {
        return String::new();
    }
    format!(" ORDER BY {}", order_terms(order, alias))
}

/// In-aggregate ordering referencing the FROM columns: ` ORDER BY a."c0" ASC, …`.
fn agg_order_inline(order: &[OrderPart], alias: &str) -> String {
    if order.is_empty() {
        return String::new();
    }
    format!(" ORDER BY {}", order_terms(order, alias))
}

/// In-aggregate ordering over the inner-subquery's projected order columns:
/// ` ORDER BY __o0 ASC, __o1 DESC`.
fn agg_order(order: &[OrderPart]) -> String {
    if order.is_empty() {
        return String::new();
    }
    let terms: Vec<String> = order
        .iter()
        .enumerate()
        .map(|(i, op)| format!("__o{i} {}", dir_sql(op.dir())))
        .collect();
    format!(" ORDER BY {}", terms.join(", "))
}

/// Project the order columns into the inner subquery: `, a."c0" AS __o0, …`.
fn order_proj(order: &[OrderPart], alias: &str) -> String {
    let mut s = String::new();
    for (i, op) in order.iter().enumerate() {
        s.push_str(&format!(
            ", {}.{} AS __o{i}",
            quote_ident(alias),
            quote_ident(op.field())
        ));
    }
    s
}

fn order_terms(order: &[OrderPart], alias: &str) -> String {
    order
        .iter()
        .map(|op| {
            format!(
                "{}.{} {}",
                quote_ident(alias),
                quote_ident(op.field()),
                dir_sql(op.dir())
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn dir_sql(d: Dir) -> &'static str {
    match d {
        Dir::Asc => "ASC",
        Dir::Desc => "DESC",
    }
}

/// `parent."p0" = child."c0" AND parent."p1" = child."c1" …`.
fn correlate_sql(parent_alias: &str, child_alias: &str, corr: &Correlation) -> String {
    corr.parent_field
        .iter()
        .zip(&corr.child_field)
        .map(|(p, c)| {
            format!(
                "{}.{} = {}.{}",
                quote_ident(parent_alias),
                quote_ident(p),
                quote_ident(child_alias),
                quote_ident(c)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn where_clause(conds: &[String]) -> String {
    if conds.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conds.join(" AND "))
    }
}

/// The keyset paging predicate for a `start` bound: keep a row iff the bound sorts
/// before it (`B < R`), or sorts equal with an inclusive bound (`B <= R`), under the
/// already-PK-completed sort `order`. Faithful to the engine's `Skip` (`compare_rows`
/// over the `Basis`): the comparison is lexicographic over the sort columns with
/// **null-low** ordering — NULL sorts before any value on ASC, after every value on DESC.
///
/// Every bound value is known at compile time, so each column folds to a small SQL
/// fragment; a sort column the bound omits (or sets to NULL) compares as null-low.
fn start_sql(start: &Bound, order: &[OrderPart], alias: &str) -> String {
    // `B < R` = OR_i ( AND_{j<i} eq_j AND before_i ); `B <= R` adds the all-equal term.
    let mut or_terms: Vec<String> = Vec::new();
    let mut prefix_eq: Vec<String> = Vec::new();
    for op in order {
        let asc = matches!(op.dir(), Dir::Asc);
        let x = format!("{}.{}", quote_ident(alias), quote_ident(op.field()));
        // A missing or explicitly-NULL bound value compares null-low.
        let b = start
            .row
            .get(op.field())
            .filter(|l| !matches!(l, Lit::Null));

        let mut conj = prefix_eq.clone();
        conj.push(before_sql(&x, asc, b));
        or_terms.push(format!("({})", conj.join(" AND ")));
        prefix_eq.push(eq_sql(&x, b));
    }
    if or_terms.is_empty() {
        return "1".into(); // no sort columns (unreachable post-completion) ⇒ keep all
    }
    let strict_lt = format!("({})", or_terms.join(" OR "));
    if start.exclusive {
        strict_lt
    } else {
        format!("({strict_lt} OR ({}))", prefix_eq.join(" AND "))
    }
}

/// `x` strictly after the bound value `b` in this column's direction, null-low. `b` is
/// `None` for an omitted/NULL bound value.
fn before_sql(x: &str, asc: bool, b: Option<&Lit>) -> String {
    match b {
        // NULL bound: before every non-null `x` on ASC; never before on DESC.
        None => {
            if asc {
                format!("{x} IS NOT NULL")
            } else {
                "0".into()
            }
        }
        Some(lit) => {
            let v = inline_lit(lit);
            if asc {
                // `v < x`; a NULL `x` yields NULL ⇒ false (a null row sorts before the
                // non-null bound on ASC — i.e. not after it).
                format!("{v} < {x}")
            } else {
                // DESC: a null `x` sorts last, so it is after the non-null bound; else `v > x`.
                format!("({x} IS NULL OR {v} > {x})")
            }
        }
    }
}

/// `x` equal to the bound value `b` under the sort's null-identical equality.
fn eq_sql(x: &str, b: Option<&Lit>) -> String {
    match b {
        None => format!("{x} IS NULL"),
        // `b` is non-null, so `x = v` (NULL `x` ⇒ NULL ⇒ false) is the null-low equality.
        Some(lit) => format!("{x} = {}", inline_lit(lit)),
    }
}

fn value_pos(vp: &ValuePosition, alias: &str) -> String {
    match vp {
        ValuePosition::Column { name } => format!("{}.{}", quote_ident(alias), quote_ident(name)),
        ValuePosition::Literal { value } => inline_lit(value),
    }
}

/// `lhs [NOT] IN (v0, v1, …)`. An empty list compiles to the constant truth value
/// (`x IN ()` is a SQLite syntax error). A non-array RHS falls back to `=`/`!=`.
fn in_sql(lhs: &str, right: &ValuePosition, alias: &str, negate: bool) -> String {
    if let ValuePosition::Literal {
        value: Lit::Array(items),
    } = right
    {
        if items.is_empty() {
            return if negate { "1".into() } else { "0".into() };
        }
        let list = items.iter().map(inline_lit).collect::<Vec<_>>().join(", ");
        let op = if negate { "NOT IN" } else { "IN" };
        return format!("{lhs} {op} ({list})");
    }
    let r = value_pos(right, alias);
    if negate {
        format!("{lhs} != {r}")
    } else {
        format!("{lhs} = {r}")
    }
}

/// Inline a literal as a native SQLite value (no type casts).
fn inline_lit(lit: &Lit) -> String {
    match lit {
        Lit::Null => "NULL".into(),
        Lit::Bool(true) => "1".into(),
        Lit::Bool(false) => "0".into(),
        // Exact i64 decimal (design 226 Stage B) — for values in ±2^53 identical to
        // what `fmt_number` renders for the same literal arriving as `Number`.
        Lit::Int(i) => i.to_string(),
        Lit::Number(n) => fmt_number(*n),
        Lit::Str(s) => format!("'{}'", s.replace('\'', "''")),
        // Only reachable as an IN/NOT-IN RHS (handled by `in_sql`); inlined defensively.
        Lit::Array(items) => format!(
            "({})",
            items.iter().map(inline_lit).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// Format a JS `number` (`f64`) as a SQLite numeric literal: an integral, in-`i64`,
/// finite value prints as an integer (matching the engine's `Int` widening and so
/// `canon`-equal to the View); other finite values print as a round-trippable float.
fn fmt_number(n: f64) -> String {
    if !n.is_finite() {
        return "NULL".into();
    }
    if n.fract() == 0.0 && n.abs() < 9_007_199_254_740_992.0 {
        return format!("{}", n as i64);
    }
    let s = format!("{n}");
    if s.contains('.') || s.contains('e') || s.contains('E') {
        s
    } else {
        format!("{s}.0")
    }
}

/// A `json_object` key: a single-quoted string literal (keys are column/relationship
/// names, `'`-escaped defensively).
fn json_key(name: &str) -> String {
    format!("'{}'", name.replace('\'', "''"))
}

/// Double-quote an identifier, doubling any embedded `"`.
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
