//! The pipeline builder (spec `08`) — lowers an [`Ast`] into a wired arena
//! [`Graph`](crate::graph). This module holds the build-time **AST passes** the
//! builder runs *before* lowering, the recursive `build_pipeline` spine (which walks
//! the AST, allocates the arena nodes, and calls [`create_predicate`] per `where`
//! leaf), and [`create_predicate`] itself (the leaf-condition →
//! [`CompiledPredicate`](crate::predicate) compiler).
//!
//! What lives here today:
//! - [`complete_ordering`] — the always-includes-PK invariant (foundations §4).
//! - [`normalize_pipeline_ast`] — the builder's pre-lowering AST normalization: the
//!   `flattened` structural subset of `normalizeAST` (splice same-op AND/OR, drop
//!   empties, unwrap singletons) followed by JS-parity correlated-subquery alias
//!   uniquification.
//! - [`transform_filters`] — strip correlated-subquery conditions so the source
//!   connection sees only a leaf-condition tree, and report whether anything was
//!   removed (gates the in-memory filter sub-graph, spec `08` §5.4).
//! - [`create_predicate`] — lower one AST [`SimpleCondition`] to a
//!   [`CompiledPredicate`] (name→`ColId`, literal→`OwnedValue` under the
//!   number-coercion rule, three-valued-null parity with JS `createPredicate`).
//!   AND/OR/NOT are the Filter sub-graph's job, so this lowers a single *leaf*.
//! - [`BuildError`] — the lowering error type (first used by [`create_predicate`]).
//! - [`schema_primary_key_names`] — the `getPrimaryKey` bridge from a `Schema`
//!   (whose PK is `ColId`s) to the column *names* the name-based passes need.

use crate::ast::{
    Aggregate, Ast, Bound, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Dir,
    ExistsOp, Lit, Op, OrderPart, SimpleCondition, System, ValuePosition,
};
use crate::change::{Basis, Constraint, OutEdge, Port, Start};
use crate::graph::{Graph, NodeId};
use crate::predicate::{CmpOp, CompiledPredicate, LikeMatcher, ValueSet};
use crate::push_index::PushGuard;
use crate::source_common::{ConnectionFilters, Operand, RowPredicate, SqlCondition, SqlOp};
use crate::value::{
    owned_row, values_identical, ColId, OwnedRow, OwnedValue, RelDef, Schema, Sort, SourceSchema,
    Value, ValueType,
};
use crate::{metric_build_err, metric_inc, metric_timer};
use std::rc::Rc;
use std::sync::Arc;

/// The row bound EXISTS child pipelines are built with (`builder.ts:224`). Exists
/// only needs "`> 0`" vs "`== 0`", so the counted size never needs to exceed this.
const EXISTS_LIMIT: u32 = 3;
/// The tighter bound for permission-system subqueries (`builder.ts:225`).
const PERMISSIONS_EXISTS_LIMIT: u32 = 1;

/// A table's primary-key column **names**, in primary-key order. Bridges a source
/// [`Schema`] (whose `primary_key` is resolved `ColId`s) back to the names the
/// name-based AST passes ([`complete_ordering`]) operate on — the analogue of the
/// JS `getPrimaryKey(tableName)` (`complete-ordering.ts:8`).
pub fn schema_primary_key_names(schema: &Schema) -> Vec<Box<str>> {
    schema
        .primary_key
        .iter()
        .map(|&c| schema.columns[c].clone())
        .collect()
}

/// Derive the production-[`View`](crate::view::View) **hierarchical schema** for `ast`,
/// resolving table names through the same `resolve` closure that [`build_pipeline`] uses.
/// `build_pipeline` returns only the top `NodeId`, so the View's tree shape is
/// reconstructed here from the `Ast`. The schema *is* the view shape: a relationship slot
/// carries a child schema iff it is in view (a join-only `RelDef::new` is out of view).
///
/// - the **slot order** is the source schema's declared relationship order (the same
///   order `build_pipeline` resolves aliases against via `rel_slot`), so dataflow slots
///   line up with the view's;
/// - a slot whose name is a `related` alias of *this* frame is **in view**: its child
///   schema is built recursively (its sort PK-completed by `resolve_sort`) — a
///   `RelDef::related`. On a duplicate alias the **last** writer wins, matching the
///   dataflow's `dedup_related_by_alias`, so the View's child schema/sort is derived from
///   the same subquery the Join is built from;
/// - every other declared slot — an EXISTS gating relationship, or one unused by this
///   `Ast` — is **out of view**: a join-only `RelDef::new` (no child schema), which the
///   View's `apply_change` skips (`rel_child(slot)` is `None`).
///
/// The top frame's sort is the resolved `order_by`; top-level is plural.
///
/// This is the production seam graduated out of `testkit`: the wasm client (WS01) and the
/// testkit runners both derive the view shape here, so there is one derivation, not two.
pub fn view_schema(
    ast: &Ast,
    resolve: &impl Fn(&str) -> Option<(NodeId, SourceSchema)>,
) -> Result<Schema, BuildError> {
    let ast = normalize_pipeline_ast(ast);
    let base = resolve(&ast.table)
        .map(|(_, s)| s.into_schema())
        .ok_or_else(|| BuildError::UnknownTable(ast.table.clone()))?;

    // A top-level aggregate's view shape is the reduce's synthetic output row
    // (`[group…, count]`), not the base table — a `having` filter is schema-preserving,
    // so it does not change this. Mirror `Reduce`'s output schema (§8).
    if ast.aggregate.is_some() {
        return aggregate_view_schema(&ast, &base);
    }

    let sort = resolve_sort(&ast.order_by, &base)?;

    // The slot layout is **query-derived** (see [`query_local_slot_names`]), identical to
    // the order `build_pipeline` resolves against, so the View's `RelId`s line up with the
    // dataflow's by construction (the load-bearing "one tree, three consumers" invariant —
    // not the source schema's *declared* relationships, which a query cannot pre-declare
    // for synthesized EXISTS-gate aliases like `comments_0`).
    let slot_names = query_local_slot_names(&ast);
    let mut rel_defs: Vec<RelDef> = Vec::with_capacity(slot_names.len());
    for name in &slot_names {
        // In view iff this slot's name is a `related` alias of THIS frame's `Ast`.
        // LAST-writer-wins on a duplicate alias (`.rev().find`), matching the dataflow's
        // `dedup_related_by_alias` so the child schema + sort come from the SAME subquery
        // the Join is built from.
        let related = ast
            .related
            .iter()
            .rev()
            .find(|c| c.subquery.alias.as_deref() == Some(name.as_ref()));
        match related {
            // A relationship **aggregate** (`count(child)`): a scalar-projected singular
            // relationship over the synthetic `[group…, count]` row (§9), NOT a recursive
            // row-materializing child schema.
            Some(sub) if sub.subquery.aggregate.is_some() => {
                let agg = sub.subquery.aggregate.as_ref().expect("aggregate present");
                // Resolve the child SOURCE (the rows the reduce folds). For a `sum`/`avg`
                // this is mandatory — its column must exist there (mirroring
                // `apply_related`'s `agg_spec_for`). For a `count` it is best-effort,
                // types only: this schema-only path never resolved a count's child
                // before, and an unresolvable one keeps the legacy `Number` default
                // rather than growing a new error.
                let child_source: Option<Schema> =
                    resolve(&sub.subquery.table).map(|(_, s)| s.into_schema());
                if matches!(agg, Aggregate::Sum(_) | Aggregate::Avg(_)) {
                    let cs = child_source
                        .as_ref()
                        .ok_or_else(|| BuildError::UnknownTable(sub.subquery.table.clone()))?;
                    let _ = agg_spec_for(agg, cs)?;
                }
                rel_defs.push(agg_relationship_reldef(
                    name,
                    &sub.correlation.child_field,
                    agg,
                    child_source.as_ref(),
                ));
            }
            Some(sub) => {
                let child_schema = view_schema(&sub.subquery, resolve)?;
                rel_defs.push(RelDef::related(name, child_schema));
            }
            None => {
                // Gating (EXISTS) or unused: declared join-only, excluded from the view.
                rel_defs.push(RelDef::new(name));
            }
        }
    }

    // Lower the query's projection (`Ast::select`) onto this level (§6). `None` ⇒ the
    // view reports every column; `Some(names)` ⇒ resolve to base `ColId`s, in select
    // order. `columns` stays the full positional list (§2.1) — only the *reported* set
    // narrows. An unknown selected column is a build error, like any name lowering.
    let projection: Option<Vec<ColId>> = match &ast.select {
        None => None,
        Some(names) => Some(
            names
                .iter()
                .map(|n| col_id(&base, n))
                .collect::<Result<Vec<_>, _>>()?,
        ),
    };

    Ok(Schema {
        sort,
        relationships: rel_defs,
        // Lower the query's `.one()` intent onto this level's view shape. The child
        // levels get theirs from the recursive `view_schema(&sub.subquery, ..)` above
        // (each reads its own subquery's `one`).
        singular: ast.one,
        projection,
        ..base
    })
}

/// The synthetic output column name and empty-group **identity** for an aggregate
/// (`REDUCE-DESIGN.md` §9). The name mirrors [`AggSpec::col_name`](crate::op::AggSpec) so
/// the reduce's output schema and the view schema agree; the identity is what the scalar
/// projection substitutes for a childless parent — `0` for `count` (SQL `LEFT JOIN …
/// count → 0`), `NULL` for `sum`/`avg`.
fn agg_output(agg: &Aggregate) -> (&'static str, OwnedValue) {
    match agg {
        Aggregate::Count => ("count", OwnedValue::Int(0)),
        Aggregate::Sum(_) => ("sum", OwnedValue::Null),
        Aggregate::Avg(_) => ("avg", OwnedValue::Null),
    }
}

/// Resolve an [`Aggregate`] (column *names*) to an [`AggSpec`] (column *indices*) against
/// the schema the reduce folds over (the child source for a relationship aggregate, the
/// table source for a top-level one). An unknown summed column is a build error.
fn agg_spec_for(agg: &Aggregate, schema: &Schema) -> Result<crate::op::AggSpec, BuildError> {
    Ok(match agg {
        Aggregate::Count => crate::op::AggSpec::Count,
        Aggregate::Sum(col) => crate::op::AggSpec::Sum(col_id(schema, col)?),
        Aggregate::Avg(col) => crate::op::AggSpec::Avg(col_id(schema, col)?),
    })
}

/// The view child schema + scalar projection for a relationship aggregate
/// (`REDUCE-DESIGN.md` §9). The synthetic aggregate row is `[child_field…, <agg>]`,
/// keyed and sorted by the group (correlation child) columns and marked `singular`; the
/// trailing aggregate column is **scalar-projected** with the aggregate's empty-group
/// identity. This MUST mirror the reduce's output schema
/// ([`Reduce::grouped_agg`](crate::op::Reduce)) so the production `View` differ locates
/// the aggregate row by the same sort/PK.
fn agg_relationship_reldef(
    name: &str,
    child_field: &[Box<str>],
    agg: &Aggregate,
    child_source: Option<&Schema>,
) -> RelDef {
    let (col_name, identity) = agg_output(agg);
    let k = child_field.len();
    let mut cols: Vec<&str> = child_field.iter().map(|c| &**c).collect();
    cols.push(col_name);
    let key: Vec<ColId> = (0..k).collect();
    let sort: Sort = (0..k).map(|i| (i, true)).collect();
    // Column types (design 226 §4.1): each group column preserves the child source's
    // declared type; the aggregate column carries `AggSpec::output_type` — the same
    // derivation `Reduce::with_input_types` applies to the dataflow twin. An
    // unresolvable child source / column keeps the legacy `Number` default.
    let mut types: Vec<ValueType> = child_field
        .iter()
        .map(|f| {
            child_source
                .and_then(|cs| cs.col_id(f).and_then(|c| cs.column_types.get(c).copied()))
                .unwrap_or(ValueType::Number)
        })
        .collect();
    types.push(
        child_source
            .and_then(|cs| agg_spec_for(agg, cs).ok().map(|s| s.output_type(cs)))
            .unwrap_or(ValueType::Number),
    );
    let mut child = Schema::new(cols, key, sort).with_column_types(types);
    child.singular = true;
    // The aggregate column sits just past the k group columns; empty group → identity.
    RelDef::related(name, child).project_scalar(k, identity)
}

/// The View result schema for a **top-level aggregate** (`REDUCE-DESIGN.md` §8): the
/// reduce's synthetic output row. Global (no `group_by`) ⇒ `[count]` with empty PK/sort
/// (a singleton, §3); grouped ⇒ `[group…, count]` keyed and sorted by the group
/// columns. MUST mirror [`Reduce::count`](crate::op::Reduce::count) /
/// [`Reduce::count_by`](crate::op::Reduce::count_by) so the production `View` differ
/// locates rows by the same sort/PK and a `having` Filter (built against the reduce's
/// schema) resolves identical `ColId`s. Group-by names are validated against the source.
fn aggregate_view_schema(ast: &Ast, base: &Schema) -> Result<Schema, BuildError> {
    // Validate the group-by names exist in the source (unknown column → BuildError) — the
    // same columns the reduce's partition key resolves.
    let group_cols = resolve_cols(&ast.group_by, base)?;
    let agg = ast
        .aggregate
        .as_ref()
        .expect("aggregate_view_schema on a non-aggregate AST");
    // Validate the summed column (Sum/Avg) exists in the source, mirroring the reduce.
    let spec = agg_spec_for(agg, base)?;
    let (agg_col, _identity) = agg_output(agg);
    let k = ast.group_by.len();
    let mut cols: Vec<&str> = ast.group_by.iter().map(|c| &**c).collect();
    cols.push(agg_col);
    let (key, sort): (Vec<ColId>, Sort) = if k == 0 {
        (Vec::new(), Vec::new())
    } else {
        ((0..k).collect(), (0..k).map(|i| (i, true)).collect())
    };
    // Column types (design 226 §4.1): group columns preserve the source's declared
    // types; the aggregate column carries `AggSpec::output_type` — mirroring
    // `Reduce::with_input_types` on the dataflow twin.
    let mut types: Vec<ValueType> = group_cols
        .iter()
        .map(|&c| {
            base.column_types
                .get(c)
                .copied()
                .unwrap_or(ValueType::Number)
        })
        .collect();
    types.push(spec.output_type(base));
    Ok(Schema::new(cols, key, sort).with_column_types(types))
}

/// `completeOrdering` (`complete-ordering.ts:6`). Append every missing primary-key
/// column (as `asc`) to `order_by`, recursively — the root, **every** `related`
/// subquery, and **every** correlated-subquery's subquery — so each `order_by`
/// ends with the full PK (foundations §4; the builder later lowers it to a `Sort`).
/// `get_pk(table)` yields a table's PK column names in PK order
/// ([`schema_primary_key_names`]). Mutates in place — the builder owns the `Ast`.
///
/// The whole-tree recursion is load-bearing: a `related`-Join child fetches in its
/// *own* completed order, so skipping a subquery here would mis-sort the child.
pub fn complete_ordering(ast: &mut Ast, get_pk: &impl Fn(&str) -> Vec<Box<str>>) {
    let pk = get_pk(&ast.table);
    for csq in &mut ast.related {
        complete_ordering(&mut csq.subquery, get_pk);
    }
    if let Some(cond) = &mut ast.r#where {
        complete_ordering_in_condition(cond, get_pk);
    }
    add_primary_keys(&pk, &mut ast.order_by);
}

/// Recurse `completeOrdering` into a condition tree's subqueries
/// (`completeOrderingInCondition`, `complete-ordering.ts:46`). A `Simple` is
/// untouched; a correlated subquery and `and`/`or` branches recurse.
fn complete_ordering_in_condition(cond: &mut Condition, get_pk: &impl Fn(&str) -> Vec<Box<str>>) {
    match cond {
        Condition::Simple(_) => {}
        Condition::CorrelatedSubquery(c) => complete_ordering(&mut c.related.subquery, get_pk),
        Condition::And { conditions } | Condition::Or { conditions } => {
            for c in conditions {
                complete_ordering_in_condition(c, get_pk);
            }
        }
    }
}

/// `addPrimaryKeys` (`complete-ordering.ts:74`). Append each PK column **not
/// already present** in `order_by` (matched by name), as `asc`, in PK order.
/// Already-present PK columns keep their existing position and direction.
fn add_primary_keys(pk: &[Box<str>], order_by: &mut Vec<OrderPart>) {
    for pk_col in pk {
        if !order_by.iter().any(|op| op.field() == pk_col.as_ref()) {
            order_by.push(OrderPart(pk_col.clone(), Dir::Asc));
        }
    }
}

/// Builder pre-lowering normalization.
///
/// Two passes:
/// 1. `flatten_condition` — the structural `flattened` subset (`ast.ts:564`): splice
///    same-op AND/OR inline, drop empty conjunctions, unwrap singletons. This is a
///    **Rust-side pre-pass the JS builder itself does not run** — it is the analogue of
///    the JS *fluent* layer's `simplifyCondition` (`expression.ts:249`, run by every
///    `.where(...)`), which the JS builder assumes has already shaped the AST. We run a
///    pass here because the builder accepts raw (un-simplified) ASTs directly. The
///    `cmp_condition` sort + `sortedRelated` are deferred (byte-identical canonical
///    output only; they would reorder the conditions the alias pass numbers — see
///    PRODUCTIONIZATION 05.4 / spec `02` §3.2). NB `flattened` is a *one-level* splice
///    (not the bottom-up full flatten of `simplifyCondition`), so a residual same-op
///    nest can survive linear 3+-deep input; that is handled downstream by recursion.
/// 2. `uniquifyCorrelatedSubqueryConditionAliases` (`builder.ts:763`): when the
///    *flattened* top-level `where` is an `and`/`or`, every correlated subquery
///    condition nested inside that tree has its subquery alias suffixed in pre-order
///    (`comments` -> `comments_0`, next -> `_1`, ...). A bare top-level EXISTS — or a
///    singleton `and[EXISTS]` that flattening unwrapped to one — is left unchanged.
///    (This deliberately differs from the JS builder run on a *raw* singleton AST,
///    which would still see `and[…]` and number it `_0`; it instead matches the JS
///    fluent path, which `simplifyCondition`-unwraps the singleton before the builder.
///    The alias only names an EXISTS gate's operator/storage — it is excluded from the
///    view — so this is row-output-invisible.)
///
/// Flattening here is what unblocks the AND-within-AND flipped-EXISTS shape (the
/// nesting it removes was the only reason that shape was rejected) without reordering
/// the existing EXISTS-under-OR / union-fan layouts (flatten preserves left-to-right
/// leaf order, so the alias numbers are stable).
///
/// This is frame-local. Child ASTs are normalized when their own
/// `build_pipeline_internal` frame runs.
pub fn normalize_pipeline_ast(ast: &Ast) -> Ast {
    let Some(where_clause) = ast.r#where.as_ref() else {
        return ast.clone();
    };

    let Some(flattened) = flatten_condition(where_clause) else {
        // The whole `where` flattened to empty (e.g. `and[]`) → drop it.
        let mut out = ast.clone();
        out.r#where = None;
        return out;
    };

    if !matches!(flattened, Condition::And { .. } | Condition::Or { .. }) {
        let mut out = ast.clone();
        out.r#where = Some(flattened);
        return out;
    }

    let mut next_alias = 0u32;
    let mut out = ast.clone();
    out.r#where = Some(uniquify_condition_aliases(&flattened, &mut next_alias));
    out
}

/// `flattened` (`ast.ts:564`) — the structural subset of `normalizeAST` the builder
/// needs. For an `and`/`or` node: splice each **same-op** child's conditions inline
/// (one level, mirroring the JS `c.conditions.map(flattened)`), recurse into
/// different-op / leaf children, drop the children that flatten away, then collapse
/// (`case 0 → None`, `case 1 → the sole child`, else the rebuilt node). A `Simple` /
/// `CorrelatedSubquery` returns unchanged.
///
/// Faithful port, quirk included: the splice maps `flattened` over a same-op child's
/// *children*, so a same-op grandchild surfaced by that map is **not** re-spliced —
/// linear nesting deeper than two levels (`and[a, and[b, and[c,d]]]`) flattens to
/// `and[a, b, and[c,d]]`, not fully flat. That residual same-op nest is handled
/// downstream by recursion (`apply_flips_and` → [`apply_where_with_flips`] →
/// `apply_flips_and`, exactly as JS `applyFilterWithFlips` recurses), so the result is
/// correctness-preserving regardless.
fn flatten_condition(cond: &Condition) -> Option<Condition> {
    let (is_and, conditions) = match cond {
        Condition::Simple(_) | Condition::CorrelatedSubquery(_) => return Some(cond.clone()),
        Condition::And { conditions } => (true, conditions),
        Condition::Or { conditions } => (false, conditions),
    };

    let mut flat: Vec<Condition> = Vec::with_capacity(conditions.len());
    for c in conditions {
        let same_op = matches!(
            (is_and, c),
            (true, Condition::And { .. }) | (false, Condition::Or { .. })
        );
        if same_op {
            // Splice the same-op child's children inline, each flattened once
            // (`c.conditions.map(flattened)`), dropping any that flatten to empty
            // (the JS `defined(...)`).
            let (Condition::And { conditions: kids } | Condition::Or { conditions: kids }) = c
            else {
                unreachable!("same_op implies and/or")
            };
            flat.extend(kids.iter().filter_map(flatten_condition));
        } else if let Some(f) = flatten_condition(c) {
            flat.push(f);
        }
    }

    match flat.len() {
        0 => None,
        1 => Some(flat.into_iter().next().expect("len == 1")),
        _ if is_and => Some(Condition::And { conditions: flat }),
        _ => Some(Condition::Or { conditions: flat }),
    }
}

fn uniquify_condition_aliases(cond: &Condition, next_alias: &mut u32) -> Condition {
    match cond {
        Condition::Simple(c) => Condition::Simple(c.clone()),
        Condition::CorrelatedSubquery(c) => {
            let mut c = c.clone();
            let base = c.related.subquery.alias.as_deref().unwrap_or("");
            c.related.subquery.alias = Some(format!("{base}_{}", *next_alias).into_boxed_str());
            *next_alias += 1;
            Condition::CorrelatedSubquery(c)
        }
        Condition::And { conditions } => Condition::And {
            conditions: conditions
                .iter()
                .map(|c| uniquify_condition_aliases(c, next_alias))
                .collect(),
        },
        Condition::Or { conditions } => Condition::Or {
            conditions: conditions
                .iter()
                .map(|c| uniquify_condition_aliases(c, next_alias))
                .collect(),
        },
    }
}

/// `transformFilters` (`filter.ts:171`) — the **core**: strip every
/// correlated-subquery condition so what remains is a leaf-only condition tree the
/// source connection can apply, and report whether anything was removed.
///
/// Returns `(stripped, conditions_removed)`. The builder threads `conditions_removed`
/// into `fully_applied = !conditions_removed` (spec `08` §5.4) and feeds `stripped`
/// to the in-memory predicate / SQL lowering.
///
/// The rules (ported verbatim):
/// - `None` ⇒ `(None, false)`; a bare `Simple` ⇒ `(Some(clone), false)`.
/// - a `CorrelatedSubquery` condition ⇒ `(None, true)` (removed).
/// - `and`/`or` recurse each branch; **a vanished branch under an `or` collapses
///   the whole `or`** to `(None, true)` (`filter.ts:191`) — the surviving branches
///   would otherwise admit rows the original rejected.
///
/// **Post-strip simplify, intentionally skipped here:** the JS wraps the rebuilt tree
/// in `simplifyCondition` (flatten / singleton-unwrap / constant-fold). The structural
/// flatten now runs in [`normalize_pipeline_ast`] on the *whole* `where` before this
/// strip; re-flattening the stripped leaf tree would only collapse a single-branch
/// `and`/`or` left behind by a removed subquery, which is downstream-equivalent (one
/// `Filter` either way) and would churn the pushed-down SQL text the source-connection
/// tests pin. So the stripped tree is returned un-unwrapped, by design.
pub fn transform_filters(filters: Option<&Condition>) -> (Option<Condition>, bool) {
    let Some(cond) = filters else {
        return (None, false);
    };
    match cond {
        Condition::Simple(_) => (Some(cond.clone()), false),
        Condition::CorrelatedSubquery(_) => (None, true),
        Condition::And { conditions } | Condition::Or { conditions } => {
            let is_or = matches!(cond, Condition::Or { .. });
            let mut transformed = Vec::with_capacity(conditions.len());
            let mut removed = false;
            for c in conditions {
                let (t, r) = transform_filters(Some(c));
                if t.is_none() && is_or {
                    // A removed OR branch collapses the whole OR (filter.ts:191).
                    return (None, true);
                }
                removed |= r;
                if let Some(t) = t {
                    transformed.push(t);
                }
            }
            let rebuilt = if is_or {
                Condition::Or {
                    conditions: transformed,
                }
            } else {
                Condition::And {
                    conditions: transformed,
                }
            };
            (Some(rebuilt), removed)
        }
    }
}

// ---------------------------------------------------------------------------
// create_predicate — one AST `SimpleCondition` → one `CompiledPredicate`
// ---------------------------------------------------------------------------

/// Errors raised while lowering an [`Ast`] into pipeline pieces (spec `08`).
/// Introduced for [`create_predicate`]; the `build_pipeline` spine grows it.
#[derive(Clone, Debug, PartialEq)]
pub enum BuildError {
    /// A column name in the condition is not declared on the table [`Schema`].
    /// (JS reads `row[name]` and silently gets `undefined`; the index-addressed
    /// engine needs the `ColId`, so a missing column is a hard build error.)
    UnknownColumn(Box<str>),
    /// A condition shape the builder does not yet lower. The text is the reason —
    /// e.g. a flipped `NOT EXISTS` (anti-join) or an EXISTS subquery carrying a
    /// shape this builder cannot lower yet.
    Unsupported(&'static str),
    /// A malformed condition: a literal's type does not fit the operator — `IN`
    /// without an array, `LIKE` without a string pattern, a column on the RHS, or
    /// an ordering comparison between two mismatched literal types.
    Invalid(&'static str),
    /// A table name in the AST has no registered source (`resolve` returned `None`).
    UnknownTable(Box<str>),
    /// A relationship alias is not declared on the parent table's [`Schema`].
    UnknownRelationship(Box<str>),
    /// Design 226 §8 — the sync-boundary gate: the query's footprint touches a
    /// declared `int64` column, which the browser/room IVM value plane cannot
    /// carry until Stage E lands the bigint lane. The footprint is deliberately
    /// broader than the projection: a selected field, PK auto-inclusion, a
    /// predicate operand, a join/correlation key, an `orderBy`/grouping key, an
    /// aggregate input, or a paging-bound column all count. A query on the same
    /// table whose required-column set excludes the column stays admissible; the
    /// 222 SQL plane never enters `build_pipeline` and is unaffected.
    Int64ColumnUnsupported { table: Box<str>, column: Box<str> },
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::UnknownColumn(c) => write!(f, "unknown column {c:?}"),
            BuildError::Unsupported(why) => write!(f, "unsupported condition: {why}"),
            BuildError::Invalid(why) => write!(f, "invalid condition: {why}"),
            BuildError::UnknownTable(t) => write!(f, "unknown table {t:?}"),
            BuildError::UnknownRelationship(r) => write!(f, "unknown relationship {r:?}"),
            BuildError::Int64ColumnUnsupported { table, column } => write!(
                f,
                "int64 column {table}.{column} is in this query's footprint; \
                 int64 columns are not yet supported in IVM queries (design 226 — \
                 the refusal lifts at Stage E; the SQL plane is unaffected)"
            ),
        }
    }
}

impl std::error::Error for BuildError {}

/// Lower one AST [`SimpleCondition`] to a [`CompiledPredicate`] — the port of JS
/// `createPredicate` for a single **leaf** (`filter.ts:27`). AND/OR/NOT are *not*
/// here: they are realized in the Filter sub-graph, so the `build_pipeline` spine
/// walks the condition tree and calls this once per leaf.
///
/// Faithful to `createPredicate`'s shape and its three-valued-null rule:
/// - **`IS` / `IS NOT`** (`filter.ts:64-72`, `createIsPredicate`): identity
///   equality, including `null`. A `null` RHS takes the compact
///   [`IsNull`](CompiledPredicate::IsNull) path; other RHS values lower to
///   [`Is`](CompiledPredicate::Is).
/// - **A `null` RHS on any other op** (`filter.ts:75`) folds to
///   [`Const(false)`](CompiledPredicate::Const) — `col = null` never matches; that
///   is `IS NULL`'s job. So a `Cmp`/`In`/`Like` is *never* built over a null
///   literal, which is exactly the invariant [`CompiledPredicate::eval`] relies on.
/// - **A literal LHS** (`filter.ts:80-86`) is row-independent and folds to a
///   `Const`: the column-form predicate is built over a one-cell row and evaluated
///   at build time, so the fold matches runtime semantics exactly. (The fluent
///   builder only emits a column LHS; this exists for wire-AST faithfulness.)
/// - **A column LHS** resolves the name to a [`ColId`] against `schema` (→
///   [`BuildError::UnknownColumn`]) and builds the per-row predicate.
///
/// **Number coercion** (JS's single `number` vs the runtime `Int`/`Float` split):
/// an integral, in-`i64`-range literal lowers to `Int`, else `Float`
/// (`number_to_owned`). Since design 226 Stage B the exact comparators place
/// `Int(5)` and `Float(5.0)` in ONE equivalence class, so the lit-vs-cell plane no
/// longer decides equality below 2^53; the shared rule still matters because it
/// fixes WHICH exact value a >2^53 literal denotes (`number_to_owned` truncates the
/// f64's binary value — the wasm entry canonicalizes to the wire token first,
/// `canonicalize_wire_number_lits`).
///
/// **Known fidelity gaps (deferred, documented):** `LIKE` is byte-level and
/// case-sensitive for `LIKE` / ASCII-case-insensitive for `ILIKE`, and does **not**
/// honour the Postgres `\` pattern escape that JS `getLikePredicate` applies
/// (`like.ts`); an ordering const-fold between mismatched numeric literals (`5 < 5.5`) is
/// [`BuildError::Invalid`] pending the numeric widening deferred in
/// [`values_identical`].
pub fn create_predicate(
    cond: &SimpleCondition,
    schema: &Schema,
) -> Result<CompiledPredicate, BuildError> {
    // RHS is always a literal in a `SimpleCondition` (the wire type excludes a
    // column on the right); read it, erroring if a column slipped through.
    let right = match &cond.right {
        ValuePosition::Literal { value } => value,
        ValuePosition::Column { .. } => {
            return Err(BuildError::Invalid(
                "right-hand side of a condition must be a literal",
            ))
        }
    };

    // IS / IS NOT first (matching `createPredicate`'s switch order): identity
    // equality, including null.
    if let Op::Is | Op::IsNot = cond.op {
        let negated = matches!(cond.op, Op::IsNot);
        let rhs = lit_to_scalar(right)?;
        return match &cond.left {
            ValuePosition::Column { name } => {
                let col = col_id(schema, name)?;
                if matches!(right, Lit::Null) {
                    Ok(CompiledPredicate::IsNull { col, negated })
                } else {
                    Ok(CompiledPredicate::Is {
                        col,
                        value: rhs,
                        negated,
                    })
                }
            }
            // `<lit> IS [NOT] <lit>` is constant (`filter.ts:67-70`).
            ValuePosition::Literal { value } => {
                let lhs = lit_to_scalar(value)?;
                Ok(CompiledPredicate::Const(
                    values_identical(lhs.as_ref(), rhs.as_ref()) ^ negated,
                ))
            }
        };
    }

    // Any other op with a null RHS is `UNKNOWN` for every row → drop
    // (`filter.ts:75`). Folded here so a `Cmp`/`In`/`Like` never carries a null.
    if matches!(right, Lit::Null) {
        return Ok(CompiledPredicate::Const(false));
    }

    match &cond.left {
        // Constant condition (literal LHS) — fold by evaluating the column-form
        // predicate against a one-cell row (`filter.ts:80-86`).
        ValuePosition::Literal { value } => {
            // A null literal LHS is `false` for every non-IS op (`filter.ts:81`).
            if matches!(value, Lit::Null) {
                return Ok(CompiledPredicate::Const(false));
            }
            let lhs = lit_to_scalar(value)?;
            guard_ordering_const(cond.op, &lhs, right)?;
            let probe = lower_op(cond.op, 0, right)?;
            Ok(CompiledPredicate::Const(probe.eval(&owned_row(vec![lhs]))))
        }
        ValuePosition::Column { name } => lower_op(cond.op, col_id(schema, name)?, right),
    }
}

/// Resolve a column name to its [`ColId`], or [`BuildError::UnknownColumn`].
fn col_id(schema: &Schema, name: &str) -> Result<ColId, BuildError> {
    schema
        .col_id(name)
        .ok_or_else(|| BuildError::UnknownColumn(name.into()))
}

/// Build the per-row predicate for a **column** LHS (non-`IS` op, non-null RHS).
fn lower_op(op: Op, col: ColId, right: &Lit) -> Result<CompiledPredicate, BuildError> {
    let cmp = |o| -> Result<CompiledPredicate, BuildError> {
        Ok(CompiledPredicate::Cmp {
            col,
            op: o,
            value: lit_to_scalar(right)?,
        })
    };
    match op {
        Op::Eq => cmp(CmpOp::Eq),
        Op::Ne => cmp(CmpOp::Ne),
        Op::Lt => cmp(CmpOp::Lt),
        Op::Le => cmp(CmpOp::Le),
        Op::Gt => cmp(CmpOp::Gt),
        Op::Ge => cmp(CmpOp::Ge),
        Op::Like => like_pred(col, right, false),
        Op::NotLike => like_pred(col, right, true),
        Op::ILike => ilike_pred(col, right, false),
        Op::NotILike => ilike_pred(col, right, true),
        Op::In => in_pred(col, right, false),
        Op::NotIn => in_pred(col, right, true),
        Op::Is | Op::IsNot => unreachable!("IS / IS NOT handled before lower_op"),
    }
}

/// `col LIKE pattern` / `col NOT LIKE pattern`. The pattern must be a string
/// literal. **Note:** the compiled matcher is byte-level/case-sensitive and does
/// not apply the `\` pattern escape (see [`create_predicate`] fidelity gaps).
fn like_pred(col: ColId, right: &Lit, negated: bool) -> Result<CompiledPredicate, BuildError> {
    match right {
        Lit::Str(pat) => Ok(CompiledPredicate::Like {
            col,
            matcher: LikeMatcher::compile(pat.as_bytes()),
            negated,
        }),
        _ => Err(BuildError::Invalid("LIKE pattern must be a string literal")),
    }
}

/// `col ILIKE pattern` / `col NOT ILIKE pattern`. ASCII-case-insensitive variant
/// of [`like_pred`].
fn ilike_pred(col: ColId, right: &Lit, negated: bool) -> Result<CompiledPredicate, BuildError> {
    match right {
        Lit::Str(pat) => Ok(CompiledPredicate::Like {
            col,
            matcher: LikeMatcher::compile_case_insensitive(pat.as_bytes()),
            negated,
        }),
        _ => Err(BuildError::Invalid(
            "ILIKE pattern must be a string literal",
        )),
    }
}

/// `col IN (list)` / `col NOT IN (list)`. The RHS must be an array literal of
/// scalars (each coerced with the number rule, like any other literal).
fn in_pred(col: ColId, right: &Lit, negated: bool) -> Result<CompiledPredicate, BuildError> {
    let Lit::Array(elems) = right else {
        return Err(BuildError::Invalid("IN / NOT IN requires an array literal"));
    };
    let mut values = Vec::with_capacity(elems.len());
    for e in elems {
        values.push(lit_to_scalar(e)?);
    }
    Ok(CompiledPredicate::In {
        col,
        set: ValueSet::new(values),
        negated,
    })
}

/// A literal-LHS ordering const-fold (`5 < 6`) reaches [`compare_values`], which
/// **panics** on a cross-type pair (as JS `compareValues` throws). Surface that as
/// a build error rather than a panic. Only ordering ops are at risk; `=`/`!=`/`IN`/
/// `LIKE` are total. Both operands are already known non-null here.
///
/// [`compare_values`]: crate::value::compare_values
fn guard_ordering_const(op: Op, lhs: &OwnedValue, right: &Lit) -> Result<(), BuildError> {
    if !matches!(op, Op::Lt | Op::Le | Op::Gt | Op::Ge) {
        return Ok(());
    }
    let rhs = lit_to_scalar(right)?;
    if same_class(lhs.as_ref(), rhs.as_ref()) {
        Ok(())
    } else {
        Err(BuildError::Invalid(
            "ordering comparison between mismatched literal types",
        ))
    }
}

/// True if two **non-null** values share a storage class (so [`compare_values`]
/// won't panic on them). Used only to pre-screen ordering const-folds.
///
/// [`compare_values`]: crate::value::compare_values
fn same_class(a: Value<'_>, b: Value<'_>) -> bool {
    use Value::*;
    matches!(
        (a, b),
        (Bool(_), Bool(_))
            | (Int(_), Int(_))
            | (Float(_), Float(_))
            | (Str(_), Str(_))
            | (Json(_), Json(_))
    )
}

/// Lower an AST scalar [`Lit`] to a runtime [`OwnedValue`]. `pub(crate)` because
/// the row loader must use the SAME number coercion (see [`create_predicate`]). An
/// array is not a scalar — callers handle `IN`/`NOT IN` lists element-by-element.
pub(crate) fn lit_to_scalar(lit: &Lit) -> Result<OwnedValue, BuildError> {
    Ok(match lit {
        Lit::Null => OwnedValue::Null,
        Lit::Bool(b) => OwnedValue::Bool(*b),
        Lit::Int(i) => OwnedValue::Int(*i),
        Lit::Number(n) => number_to_owned(*n),
        Lit::Str(s) => OwnedValue::str(s),
        Lit::Array(_) => {
            return Err(BuildError::Invalid(
                "expected a scalar literal, found an array",
            ))
        }
    })
}

/// The number-coercion rule (`create_predicate` docs): an integral, in-`i64`-range
/// `f64` → [`OwnedValue::Int`], else [`OwnedValue::Float`]. The bound is `[-2^63,
/// 2^63)` — `i64::MIN` is exact as `f64`, and `2^63` (`-(i64::MIN as f64)`) is the
/// first value too large for `i64`, so `n as i64` never saturates-then-misclassifies.
///
/// `-0.0` note: `fract() == 0.0` admits `-0.0` → `Int(0)`, which sits in the
/// `0`/`0.0` class while `float_int_class` deliberately EXCLUDES `-0.0` (§5.1's
/// total order keeps `-0.0 < 0`). Consequence: an embedded literal `= -0.0` matches
/// `Int(0)`/`Float(0.0)` cells but not `Float(-0.0)` cells, where SQLite says all
/// three are equal — the §9 pinned `-0.0` oracle divergence. A JSON home can never
/// send the literal (`JSON.stringify(-0)` is `"0"`).
pub(crate) fn number_to_owned(n: f64) -> OwnedValue {
    let lo = i64::MIN as f64; // -2^63, exact
    let hi = -(i64::MIN as f64); //  2^63, one past i64::MAX
    if n.fract() == 0.0 && n >= lo && n < hi {
        OwnedValue::Int(n as i64)
    } else {
        OwnedValue::Float(n)
    }
}

// ---------------------------------------------------------------------------
// build_pipeline — lower an `Ast` into a wired arena `Graph` (spec 08)
// ---------------------------------------------------------------------------

/// Lower an [`Ast`] into a wired pipeline in `graph`, returning the **top**
/// operator (the one a sink attaches to via [`Graph::set_sink_edge`]). The port of
/// JS `buildPipeline`/`buildPipelineInternal` (`builder.ts:256`) for the **built
/// operator subset**: a source connection carrying its pushed-down `where`
/// ([`ConnectionFilters`]) plus a chain of parent-driven relationship joins.
///
/// `resolve` maps a table name to its already-created source [`NodeId`] and a clone
/// of that source's [`Schema`] — the builder seeds no sources (the test harness /
/// view layer owns them, mirroring JS `delegate.getSource`). A relationship child
/// is resolved the same way.
///
/// **In scope:** `table`; `where` (AND/OR/leaves lowered to one connection
/// [`RowPredicate`] via [`create_predicate`], the memory leaf's filter — no Filter
/// operators are built because a subquery-free `where` is fully applied at the
/// source, matching JS `fullyAppliedFilters`); `order_by` (PK-completed at lowering
/// so the source's connect assertion holds); and `related` relationships —
/// **sibling** (multiple relationships on one row, each a [`Join`](crate::graph)
/// stacked on the prior) and **nested** (a relationship whose child has its own
/// `related`, lowered to a Join feeding the parent join's child port). Joins carry
/// a port-tagged [`OutEdge`] so they can feed each other
/// ([`Graph::set_out_edge`]).
///
/// `start` lowers to a [`Skip`](crate::op::Skip) and `limit` to a
/// [`Take`](crate::op::Take) (after `start`, before `related`), each carried on a
/// port-tagged edge so it can feed a relationship join.
///
/// EXISTS (`where` correlated subqueries) is fully lowered — flipped + non-flipped,
/// nested, under `OR` (the union fan), and AND-within-AND (the `where` is structurally
/// flattened by [`normalize_pipeline_ast`] first) — as is `related`, `limit`/`start`,
/// and the deepest-nested `Child` push (the keystone). `select` is ignored (projection
/// is select-all, spec 12).
///
/// **Out of scope → [`BuildError::Unsupported`]:** flipped `NOT EXISTS`, an EXISTS
/// subquery carrying `start` or nested `related`, and a bare EXISTS subquery alias
/// colliding with a materialized `related` of the same name (a genuine one-slot-per-name
/// limitation, not a normalization artifact — see `normalize_pipeline_ast` / WS05.4).
pub fn build_pipeline(
    graph: &mut Graph,
    ast: &Ast,
    resolve: &impl Fn(&str) -> Option<(NodeId, SourceSchema)>,
) -> Result<NodeId, BuildError> {
    // Query compile latency (208.2): time the whole build, dropped on every return path.
    let _timer = metric_timer!(query_build);
    // Design 226 §8: the int64 sync-boundary gate, at the one chokepoint every IVM
    // query passes (sync registration, one-shot daemon-client queries, and mutator
    // `tx.query` alike). Runs on the raw AST — normalization restructures the
    // condition tree and aliases but never changes which tables/columns a query
    // touches — and before any node is allocated, so a refusal leaves no debris.
    if let Err(err) = reject_int64_footprint(ast, resolve) {
        metric_build_err!(&err);
        return Err(err);
    }
    // A top-level aggregate (a bare `count(table)` feeding the View as a scalar/grouped
    // row) is a different result shape than the §9 relationship aggregate (a reduce as a
    // join child): the reduce feeds the View directly, optionally through a `HAVING`
    // filter. `build_aggregate_pipeline` lowers it; everything else takes the row spine.
    // Count build outcomes so a rejection *rate* (`rindle.build.errors{kind}` over
    // `rindle.build.ok`) is computable; no-op when the `metrics` feature is off.
    let result = if ast.aggregate.is_some() {
        build_aggregate_pipeline(graph, ast, resolve)
    } else {
        build_pipeline_internal(graph, ast, None, false, resolve)
    };
    match result {
        Ok(node) => {
            metric_inc!(build_ok);
            Ok(node)
        }
        Err(err) => {
            metric_build_err!(&err);
            Err(err)
        }
    }
}

/// Design 226 §8 — the whole-footprint `int64` gate (see
/// [`BuildError::Int64ColumnUnsupported`]). Walks every frame of the query (the
/// root, each `related` child, and each `where`-embedded EXISTS child,
/// recursively), collects the columns that frame requires of its own table, and
/// refuses if any is a declared [`ValueType::Int`] column.
///
/// A frame's required set: the projection (`select: None` ⇒ **every** column),
/// PK auto-inclusion, `order_by` keys, predicate operand columns, `group_by`
/// keys, a `sum`/`avg` input column, paging-bound (`start`) columns, and the
/// correlation fields of every attached subquery (parent side on this frame,
/// child side on the child frame). `having` addresses the reduce's *output*
/// columns, whose base inputs are already counted. Unknown tables and columns
/// are skipped here — their own lowering surfaces the right error downstream.
fn reject_int64_footprint(
    ast: &Ast,
    resolve: &impl Fn(&str) -> Option<(NodeId, SourceSchema)>,
) -> Result<(), BuildError> {
    // Column names this frame's own `where` tree references (Simple operands and
    // EXISTS parent-side correlation fields); subquery frames are walked separately.
    fn cond_cols<'a>(cond: &'a Condition, out: &mut Vec<&'a str>) {
        match cond {
            Condition::Simple(s) => {
                if let ValuePosition::Column { name } = &s.left {
                    out.push(name);
                }
            }
            Condition::And { conditions } | Condition::Or { conditions } => {
                for c in conditions {
                    cond_cols(c, out);
                }
            }
            Condition::CorrelatedSubquery(csq) => {
                out.extend(csq.related.correlation.parent_field.iter().map(|f| &**f));
            }
        }
    }

    fn check_frame(
        ast: &Ast,
        extra: &[Box<str>],
        resolve: &impl Fn(&str) -> Option<(NodeId, SourceSchema)>,
    ) -> Result<(), BuildError> {
        if let Some((_, schema)) = resolve(&ast.table) {
            let refuse = |col: ColId| -> Result<(), BuildError> {
                Err(BuildError::Int64ColumnUnsupported {
                    table: ast.table.clone(),
                    column: schema.columns[col].clone(),
                })
            };
            match &ast.select {
                // No projection ⇒ the full row is in the footprint.
                None => {
                    if let Some(c) = schema
                        .column_types
                        .iter()
                        .position(|t| *t == ValueType::Int)
                    {
                        return refuse(c);
                    }
                }
                Some(sel) => {
                    let mut names: Vec<&str> = sel.iter().map(|s| &**s).collect();
                    names.extend(extra.iter().map(|f| &**f));
                    names.extend(ast.order_by.iter().map(|op| op.field()));
                    names.extend(ast.group_by.iter().map(|g| &**g));
                    match &ast.aggregate {
                        Some(Aggregate::Sum(col)) | Some(Aggregate::Avg(col)) => names.push(col),
                        _ => {}
                    }
                    if let Some(bound) = &ast.start {
                        names.extend(bound.row.keys().map(|k| &**k));
                    }
                    if let Some(w) = &ast.r#where {
                        cond_cols(w, &mut names);
                    }
                    for csq in &ast.related {
                        names.extend(csq.correlation.parent_field.iter().map(|f| &**f));
                    }
                    for c in names.into_iter().filter_map(|n| schema.col_id(n)) {
                        if schema.column_types.get(c) == Some(&ValueType::Int) {
                            return refuse(c);
                        }
                    }
                    // PK auto-inclusion: row identity/ordering always crosses the
                    // boundary, projected or not (§2.1).
                    for &pk in &schema.primary_key {
                        if schema.column_types.get(pk) == Some(&ValueType::Int) {
                            return refuse(pk);
                        }
                    }
                }
            }
        }

        for csq in &ast.related {
            check_frame(&csq.subquery, &csq.correlation.child_field, resolve)?;
        }
        for csq in gather_csq_conditions(ast.r#where.as_ref()) {
            check_frame(
                &csq.related.subquery,
                &csq.related.correlation.child_field,
                resolve,
            )?;
        }
        Ok(())
    }

    check_frame(ast, &[], resolve)
}

/// Lower a **top-level aggregate** query (`ast.aggregate` set on the root) to
/// `source → reduce → [HAVING filter] → View` (`REDUCE-DESIGN.md` §4/§8). The reduce
/// is **eager** — the `View` hydrates with one unconstrained fetch that folds every
/// group — and is **global** when [`group_by`](Ast::group_by) is empty (one immortal
/// `[count]` row, §3) or **grouped** otherwise (one `[group…, count]` row per group,
/// born/dying with its count, §8).
///
/// `where` filters rows **below** the reduce (which rows are counted), pushed into the
/// source leaf exactly as for a non-aggregate query; [`having`](Ast::having) filters
/// groups **above** it. The grouped input connection split-edits on the group-by
/// columns, so a row changing its group arrives as Remove(old)+Add(new) before the
/// reduce (§8); the global case never splits (§7).
///
/// **v1 scope.** Rejects, on an aggregate root: `related` (the aggregate row has no
/// relationships), `order_by` / `limit` / `start` (ordering or limiting *groups* is
/// Tier 2 — ORDER BY agg + Take, §9), and a correlated subquery in either `where` or
/// `having`. The `having` predicate addresses the reduce's **output** columns
/// (`[group…, count]`), so it is built against the reduce's own [`Schema`].
fn build_aggregate_pipeline(
    graph: &mut Graph,
    ast: &Ast,
    resolve: &impl Fn(&str) -> Option<(NodeId, SourceSchema)>,
) -> Result<NodeId, BuildError> {
    let ast = normalize_pipeline_ast(ast);
    if !ast.related.is_empty() {
        return Err(BuildError::Unsupported(
            "a top-level aggregate must not carry `related` (the aggregate row has no relationships)",
        ));
    }
    // The aggregate's output is the synthetic `[group…, count]` row, so a base-column
    // `select` projection has nothing to project — reject it rather than silently
    // dropping it (the row spine honours `select`; this path cannot).
    if ast.select.is_some() {
        return Err(BuildError::Unsupported(
            "`select` does not apply to a top-level aggregate (its output is [group…, count])",
        ));
    }
    // `.one()` (singular output) sets both `one` and `limit = 1`; check `one` first so
    // its own message wins over the limit guard below, and so a wire AST carrying
    // `one: true` without a `limit` is still rejected rather than silently ignored.
    if ast.one {
        return Err(BuildError::Unsupported(
            "`.one()` on a top-level aggregate is not yet supported \
             (a global count is already a single row; Tier 2)",
        ));
    }
    if !ast.order_by.is_empty() || ast.limit.is_some() || ast.start.is_some() {
        return Err(BuildError::Unsupported(
            "ordering/limiting/paging a top-level aggregate is not yet supported \
             (Tier 2: ORDER BY an aggregate + Take)",
        ));
    }
    if !gather_csq_conditions(ast.r#where.as_ref()).is_empty() {
        return Err(BuildError::Unsupported(
            "a correlated subquery in a top-level aggregate's `where` is not yet supported",
        ));
    }

    let (source_id, source_schema) =
        resolve(&ast.table).ok_or_else(|| BuildError::UnknownTable(ast.table.clone()))?;
    let schema = source_schema.into_schema();

    // Group-by columns in input (source) coordinates; empty ⇒ a global aggregate.
    let group_cols = resolve_cols(&ast.group_by, &schema)?;
    // The aggregate spec in source coordinates (a `Sum`/`Avg` column must exist).
    let spec = agg_spec_for(ast.aggregate.as_ref().expect("aggregate present"), &schema)?;

    // The reduce folds regardless of input order, but the connection still needs a
    // valid ordering — the source's PK sort (`resolve_sort` of an empty `order_by`).
    let sort = resolve_sort(&[], &schema)?;
    // `where` is pushed into the source leaf (fully applied — no subqueries here, so the
    // `fully_applied` flag is always true and no Filter sub-graph is needed below).
    let (filters, _fully) = build_connection_filters(ast.r#where.as_ref(), None, &schema)?;
    // Split-edit on the group-by columns so a group-changing edit decomposes into
    // Remove(old)+Add(new) before the reduce (§8). Global (empty) never splits (§7).
    let conn = graph.connect(source_id, Some(sort), filters, group_cols.clone());

    // The reduce: global (single row), or grouped **eager** (top-level GROUP BY).
    let storage = graph.alloc_storage();
    let reduce_op = if group_cols.is_empty() {
        crate::op::Reduce::global_agg(conn, storage, spec).with_input_types(&schema)
    } else {
        let key_cols: Vec<&str> = ast.group_by.iter().map(|c| &**c).collect();
        crate::op::Reduce::grouped_agg(conn, storage, group_cols, key_cols, spec)
            .with_input_types(&schema)
    };
    // The reduce's output schema (`[group…, count]`) is what a `having` filter and the
    // View resolve against; clone it before the op is moved into the graph.
    let reduce_schema = reduce_op.schema.clone();
    let reduce = graph.add_reduce(reduce_op);
    graph.set_conn_output(
        conn,
        OutEdge {
            node: reduce,
            port: Port::Single,
        },
    );

    // `having` → a Filter sub-graph ABOVE the reduce, predicating on its output columns.
    // The Filter edit-split (graph.rs `filter_chain_push`) turns a group crossing the
    // predicate threshold into an `Add`/`Remove`, maintaining `HAVING` incrementally.
    let end = match &ast.having {
        None => reduce,
        Some(having) => {
            if !gather_csq_conditions(Some(having)).is_empty() {
                return Err(BuildError::Unsupported(
                    "a correlated subquery in `having` is not supported \
                     (HAVING predicates over the aggregate's output columns)",
                ));
            }
            build_filter_pipeline(graph, reduce, having, &reduce_schema)?
        }
    };
    Ok(end)
}

/// The recursive spine. `partition_key` is the correlation **child** field names
/// when this AST is a relationship child (it seeds `split_edit_keys`, mirroring JS
/// `buildPipelineInternal`'s `partitionKey`); `None` at the root. `is_exists_child`
/// is true when this AST is a non-flipped EXISTS subquery (`isNonFlippedExistsChild`)
/// — it makes the frame's `limit` lower to an unordered [`Cap`](crate::op::Cap)
/// instead of a [`Take`](crate::op::Take).
fn build_pipeline_internal(
    graph: &mut Graph,
    ast: &Ast,
    partition_key: Option<&[Box<str>]>,
    is_exists_child: bool,
    resolve: &impl Fn(&str) -> Option<(NodeId, SourceSchema)>,
) -> Result<NodeId, BuildError> {
    let ast = normalize_pipeline_ast(ast);
    let (source_id, schema) =
        resolve(&ast.table).ok_or_else(|| BuildError::UnknownTable(ast.table.clone()))?;

    // Gather the non-flipped EXISTS/NOT EXISTS conditions of `where`: each becomes a
    // relationship Join (built below) + an `Exists` gate (in `apply_where_exists`).
    // Flipped NOT EXISTS needs an anti-join operator and is still rejected in the
    // flipped lowering path.
    let csq_conditions = gather_csq_conditions(ast.r#where.as_ref());
    // A `where` carrying a **flipped** EXISTS anywhere lowers its `applyWhere` through
    // `apply_where_with_flips` (the `applyFilterWithFlips` port, `builder.ts:414`): the
    // recursion builds the flipped `FlippedJoin`s + the non-flipped `Exists` gates (which
    // count the SPINE Joins, built unconditionally below, through the rel-preserving
    // broadcast). A flip-free `where` takes the spine filter / gate-chain path instead.
    let where_has_flip = ast
        .r#where
        .as_ref()
        .is_some_and(condition_has_flipped_subquery);

    // EXISTS aliases that build a Join (everything except a `limit 0` subquery — a
    // constant-false gate, see `apply_where_exists`) must be distinct from each other
    // and from `related` aliases after JS-parity alias normalization: the slot model
    // attaches one relationship per slot. Reject a collision rather than mis-count it.
    //
    // WS05.4 verified this is a GENUINE one-slot-per-name limitation, NOT an artifact
    // of un-flattened nesting: `flatten_condition` cannot remove it (a bare top-level
    // EXISTS isn't uniquified, by JS-parity guard, so an EXISTS sharing a `related`
    // alias still clashes). Two EXISTS under a top-level and/or are uniquified to
    // distinct `_N` slots and never reach here; only a bare EXISTS vs a materialized
    // `related` of the same name does. Kept (narrowed), with a covering test.
    {
        let mut join_aliases: Vec<&str> = ast
            .related
            .iter()
            .filter_map(|c| c.subquery.alias.as_deref())
            .collect();
        for c in &csq_conditions {
            if c.related.subquery.limit == Some(0) {
                continue;
            }
            let alias = c.related.subquery.alias.as_deref().unwrap_or("");
            if join_aliases.contains(&alias) {
                return Err(BuildError::Unsupported(
                    "an EXISTS subquery alias collides with a materialized `related` of \
                     the same name (one relationship per slot)",
                ));
            }
            join_aliases.push(alias);
        }
    }

    // Decouple the **slot layout** from the source schema's declared relationships: the
    // slot tree is a pure function of THIS query (see [`query_local_slot_names`]), so a
    // production-shaped source schema (which declares only its real relationship names,
    // or none) builds a multi-EXISTS `where` whose uniquifier mints synthesized gate
    // aliases (`comments_0`, …) the source never declared. Every downstream `rel_slot`
    // resolution, the union fan's `add_empty_relationships` count (it clones this
    // schema), and [`view_schema`] all resolve against this one tree, so the Join
    // output tag, the `Exists` count, and the View materialization share identical
    // `RelId`s by construction. (Columns / PK / sort are untouched — only `relationships`
    // is replaced — so the column/PK-based helpers below are unaffected.)
    let schema = Schema {
        relationships: query_local_slot_names(&ast)
            .into_iter()
            .map(|name| RelDef {
                name,
                child: None,
                project: None,
            })
            .collect(),
        ..schema.into_schema()
    };

    let sort = resolve_sort(&ast.order_by, &schema)?;
    let split_edit_keys =
        compute_split_edit_keys(partition_key, &ast.related, &csq_conditions, &schema)?;
    // A projected query (`select` set) gets a connection presence predicate over the
    // columns it structurally reads (§3.2–3.3); a `'*'` query gets `None` and is
    // unchanged (§7).
    let presence: Option<Vec<ColId>> = if ast.select.is_some() {
        Some(required_cols(&ast, &schema, &sort, &split_edit_keys)?)
    } else {
        None
    };
    let (filters, fully_applied) =
        build_connection_filters(ast.r#where.as_ref(), presence.as_deref(), &schema)?;
    let conn = graph.connect(source_id, Some(sort.clone()), filters, split_edit_keys);

    // `start` → a `Skip` over the connection (JS `buildPipelineInternal:323`).
    let mut end = conn;
    if let Some(bound) = &ast.start {
        let skip = graph.add_skip(crate::op::Skip::new(end, lower_start(bound, &schema)?));
        graph.set_conn_output(
            conn,
            OutEdge {
                node: skip,
                port: Port::Single,
            },
        );
        end = skip;
    }

    // Non-flipped EXISTS relationship joins on the **spine** (`builder.ts:329-350`,
    // before `applyWhere`) — built **regardless of whether `where` carries a flip**, the
    // JS layout. Each attaches the child relationship (limited to `EXISTS_LIMIT` via a
    // `Cap`) that the matching `Exists` gate counts; the gate may live in a fan-out
    // branch above (the rel-preserving union broadcast carries the relationship down to
    // it). A `limit 0` EXISTS builds no Join (constant-false `Filter` in the gate).
    for csq in &csq_conditions {
        if csq.flip == Some(true) || csq.related.subquery.limit == Some(0) {
            continue;
        }
        // A child-aggregate parent filter (`issue WHERE count(comments) > 10`,
        // `PARENT-AGGREGATE-FILTER-DESIGN.md`) is an EXISTS whose subquery carries a
        // relationship `aggregate`. It must fold the child **uncapped** (the count is the
        // thing being filtered), so it takes a dedicated lowering rather than
        // `apply_exists_join`'s EXISTS_LIMIT-capped child (design §4).
        end = if csq.related.subquery.aggregate.is_some() {
            apply_agg_exists_join(graph, end, csq, &schema, resolve)?
        } else {
            apply_exists_join(graph, end, csq, &schema, resolve)?
        };
    }

    // `applyWhere` (`builder.ts:352`), dispatched by whether `where` carries a flip.
    if let Some(w) = &ast.r#where {
        if where_has_flip {
            // A flipped EXISTS anywhere → `applyFilterWithFlips` (`builder.ts:414`): the
            // flipped `FlippedJoin`s + the non-flipped `Exists` gates (which count the
            // spine Joins above, reached through the rel-preserving fan-out). Returns the
            // branch head edge the spine (connection/skip/Join) pushes the change into.
            let (head, tail) = apply_where_with_flips(graph, end, w, &schema, &sort, resolve)?;
            graph.set_out_edge(end, head);
            end = tail;
        } else if has_subquery_under_or(w) {
            // A non-flipped subquery under `OR` (or nested AND/OR). The source could not
            // push the disjunction (`transform_filters` collapses an `or` carrying a
            // subquery), so the **full** `where` tree is applied here as a filter
            // sub-graph: a `FanOut` of leaf `Filter`s / `Exists` gates, k-way-collapsed
            // by a `FanIn`. The non-flipped EXISTS rels the spine loop attached are read
            // by those gates.
            end = build_filter_pipeline(graph, end, w, &schema)?;
        } else if !fully_applied {
            // Pure AND of leaves + EXISTS: the source fully applied the leaves, so
            // `applyWhere` is only the non-flipped `Exists` gate chain.
            let non_flipped: Vec<&CorrelatedSubqueryCondition> = csq_conditions
                .iter()
                .copied()
                .filter(|c| c.flip != Some(true))
                .collect();
            if !non_flipped.is_empty() {
                end = apply_where_exists(graph, end, &non_flipped, &schema)?;
            }
        }
    }

    // `limit` → a `Take` (root / `related` child) or a `Cap` (EXISTS child), after
    // `start`/`applyWhere`, before `related` (`builder.ts:356`). The `Cap` needs an
    // unordered source connect, but a flipped `where` builds a `UnionFanIn` (ordered
    // merge) — so a flipped-`where` EXISTS child falls back to the ordered `Take`,
    // mirroring JS `useCap` (`builder.ts:306-308`).
    if let Some(limit) = ast.limit {
        let use_cap = is_exists_child && !where_has_flip;
        end = lower_limit(graph, end, limit, partition_key, use_cap, &schema, &sort)?;
    }

    // Chain one Join per `related` relationship, deduped by alias (last-writer-wins,
    // `builder.ts:385-393`).
    for csq in dedup_related_by_alias(&ast.related) {
        end = apply_related(graph, end, csq, &schema, false, resolve)?;
    }
    Ok(end)
}

/// Stack one relationship [`Join`](crate::graph) on `parent_top` (the current
/// pipeline end — the connection or the previous join), mirroring JS
/// `applyCorrelatedSubQuery` (`builder.ts:650`). Builds the child sub-pipeline
/// (itself a bare connection, or a *nested* Join when the child has its own
/// `related`), resolves the correlation keys against each side's schema, wires the
/// parent→join (`JoinParent`) and child→join (`JoinChild`) edges with explicit
/// ports, and returns the Join as the new pipeline end.
fn apply_related(
    graph: &mut Graph,
    parent_top: NodeId,
    csq: &CorrelatedSubquery,
    schema: &Schema,
    is_exists_child: bool,
    resolve: &impl Fn(&str) -> Option<(NodeId, SourceSchema)>,
) -> Result<NodeId, BuildError> {
    let rel_name = csq
        .subquery
        .alias
        .as_deref()
        .ok_or(BuildError::Invalid("a related subquery must have an alias"))?;
    // The relationship name resolves against the frame's **query-local** slot tree
    // (`build_pipeline_internal` installed it as `schema.relationships`), regardless of
    // how many sibling joins are already stacked on `parent_top`. The slot is resolved
    // HERE and handed to the graph, so the join is not re-resolved against the shared
    // source node's declared relationships.
    let rel_slot = schema
        .rel_slot(rel_name)
        .ok_or_else(|| BuildError::UnknownRelationship(rel_name.into()))?;
    let child_top = build_pipeline_internal(
        graph,
        &csq.subquery,
        Some(&csq.correlation.child_field),
        is_exists_child,
        resolve,
    )?;

    let parent_key = resolve_cols(&csq.correlation.parent_field, schema)?;
    let (_, child_source) = resolve(&csq.subquery.table)
        .ok_or_else(|| BuildError::UnknownTable(csq.subquery.table.clone()))?;
    let child_schema = child_source.into_schema();
    // The correlation **child** columns in child-source coordinates (the reduce's
    // partition key, and the row-join's child key).
    let child_corr = resolve_cols(&csq.correlation.child_field, &child_schema)?;
    // The correlation key pair must be non-empty and equal-length (`ast.rs:249`;
    // JS `Join` asserts it). Otherwise `build_join_constraint` zips mismatched
    // keys and panics at push time — reject it here as a build error instead.
    if parent_key.is_empty() || parent_key.len() != child_corr.len() {
        return Err(BuildError::Invalid(
            "relationship correlation parent/child fields must be non-empty and equal-length",
        ));
    }

    // The child of the join is one of three shapes:
    //   - an ordinary materialized child (`None`);
    //   - a **reduce-backed** aggregate (`count`/`sum`/`avg` of a child, §9): a grouped
    //     lazy `reduce` partitioned by the correlation child key, whose group columns
    //     (output positions `0..k`) become the join's child key (the reduce reshapes the
    //     row);
    //   - a **precomputed** aggregate (`AGGREGATE-SYNC-DESIGN.md` §3.3): the synthetic
    //     `(group…, count)` rows already exist as a source table, so the child is joined
    //     directly like an ordinary child and surfaced by the scalar projection
    //     `view_schema` attaches — reducing it would recount the aggregated rows.
    let (join_child, child_key) = match &csq.subquery.aggregate {
        Some(agg) if !csq.subquery.aggregate_precomputed => {
            // An aggregate child reduces its rows away; nested `related` would be
            // materialized into a child the aggregate discards — reject it as confusing.
            if !csq.subquery.related.is_empty() {
                return Err(BuildError::Unsupported(
                    "a relationship aggregate must not carry nested `related`",
                ));
            }
            // The aggregate column (Sum/Avg) resolves against the child source schema.
            let spec = agg_spec_for(agg, &child_schema)?;
            let storage = graph.alloc_storage();
            let key_cols: Vec<&str> = csq.correlation.child_field.iter().map(|c| &**c).collect();
            let reduce = graph.add_reduce(
                crate::op::Reduce::grouped_agg(
                    child_top,
                    storage,
                    child_corr.clone(),
                    key_cols,
                    spec,
                )
                .lazy()
                .with_input_types(&child_schema),
            );
            // child rows → reduce (Single), reduce → join child.
            graph.set_out_edge(
                child_top,
                OutEdge {
                    node: reduce,
                    port: Port::Single,
                },
            );
            let group_key: Vec<ColId> = (0..child_corr.len()).collect();
            (reduce, group_key)
        }
        // Ordinary child OR a precomputed aggregate: join the child source directly on the
        // correlation child columns (resolved against that source's schema). For a
        // precomputed aggregate the source schema is `[group…, count]`, so `child_corr`
        // is the group columns and `view_schema` scalar-projects the trailing value.
        _ => (child_top, child_corr),
    };

    let join = graph.add_join_slot(parent_top, join_child, parent_key, child_key, rel_slot);
    graph.set_out_edge(
        parent_top,
        OutEdge {
            node: join,
            port: Port::JoinParent,
        },
    );
    graph.set_out_edge(
        join_child,
        OutEdge {
            node: join,
            port: Port::JoinChild,
        },
    );
    Ok(join)
}

/// Build the relationship [`Join`](crate::graph) for one EXISTS condition — the
/// analogue of the `csqConditions` loop's `applyCorrelatedSubQuery` with the
/// subquery limit forced to `EXISTS_LIMIT` and `isNonFlippedExistsChild = true`
/// (`builder.ts:329-350`). The child's `limit` therefore lowers to an unordered
/// [`Cap`](crate::op::Cap). EXISTS subqueries must not carry `start`/`related`.
fn apply_exists_join(
    graph: &mut Graph,
    parent_top: NodeId,
    csq: &CorrelatedSubqueryCondition,
    schema: &Schema,
    resolve: &impl Fn(&str) -> Option<(NodeId, SourceSchema)>,
) -> Result<NodeId, BuildError> {
    if csq.related.subquery.start.is_some() {
        return Err(BuildError::Unsupported(
            "EXISTS subquery must not have `start`",
        ));
    }
    if !csq.related.subquery.related.is_empty() {
        return Err(BuildError::Unsupported(
            "EXISTS subquery must not have nested `related`",
        ));
    }
    let limit = match csq.related.system {
        Some(System::Permissions) => PERMISSIONS_EXISTS_LIMIT,
        _ => EXISTS_LIMIT,
    };
    let mut related = csq.related.clone();
    related.subquery.limit = Some(limit);
    apply_related(graph, parent_top, &related, schema, true, resolve)
}

/// Lower a **child-aggregate parent filter** — `issue WHERE count(comments) <op> n`
/// (`PARENT-AGGREGATE-FILTER-DESIGN.md`). The synthesized EXISTS whose subquery carries a
/// relationship `aggregate` (branched in the spine loop on `aggregate.is_some()`): build
/// the correlated child **uncapped**, fold it with a **lazy grouped** `count_by` reduce
/// (one `[child_key…, count]` group per parent), drop the groups failing the `having`
/// predicate, and attach the survivor to the parent join's `rel_slot`. The matching
/// [`Exists`](crate::op::Exists) gate ([`apply_where_exists`]) then counts the ≤1
/// surviving group per parent and keeps the parent iff present.
///
/// **Why not [`apply_exists_join`].** That forces `limit = EXISTS_LIMIT` and builds the
/// child capped (`is_exists_child = true`), truncating the very rows the count folds — a
/// silent miscount (design §4). This path builds the child uncapped.
///
/// **v1 guard.** The `having` must be a **high-pass** count predicate (false at count 0):
/// a childless parent forms no group, so a count-0-satisfying predicate (`<= n`, `= 0`,
/// `>= 0`) would wrongly drop it (design §5). See [`reject_count_zero_satisfiable`].
fn apply_agg_exists_join(
    graph: &mut Graph,
    parent_top: NodeId,
    csq: &CorrelatedSubqueryCondition,
    schema: &Schema,
    resolve: &impl Fn(&str) -> Option<(NodeId, SourceSchema)>,
) -> Result<NodeId, BuildError> {
    let sub = &csq.related.subquery;
    // v1 scope guards.
    if sub.aggregate != Some(Aggregate::Count) {
        return Err(BuildError::Unsupported(
            "only a `count` child aggregate can gate a parent (sum/avg parent filters deferred)",
        ));
    }
    if !matches!(csq.op, ExistsOp::Exists) {
        return Err(BuildError::Unsupported(
            "a child-aggregate parent filter must be EXISTS (NOT EXISTS over a count is deferred)",
        ));
    }
    if !sub.related.is_empty() {
        return Err(BuildError::Unsupported(
            "a `count` child-aggregate filter must not carry nested `related`",
        ));
    }
    if sub.start.is_some() || sub.limit.is_some() || !sub.order_by.is_empty() {
        return Err(BuildError::Unsupported(
            "a child-aggregate parent filter subquery must not page/order/limit the children \
             (the count folds all of them)",
        ));
    }

    let rel_name = sub.alias.as_deref().ok_or(BuildError::Invalid(
        "an aggregate EXISTS subquery must have an alias",
    ))?;
    let rel_slot = schema
        .rel_slot(rel_name)
        .ok_or_else(|| BuildError::UnknownRelationship(rel_name.into()))?;

    // The child is built UNCAPPED (`is_exists_child = false`): the count must fold every
    // child row, not the `EXISTS_LIMIT` cap `apply_exists_join` would impose (design §4).
    // The child `where` (e.g. `count(comments WHERE …)`) lowers below the reduce here.
    let child_top = build_pipeline_internal(
        graph,
        sub,
        Some(&csq.related.correlation.child_field),
        false,
        resolve,
    )?;

    let parent_key = resolve_cols(&csq.related.correlation.parent_field, schema)?;
    let (_, child_schema) =
        resolve(&sub.table).ok_or_else(|| BuildError::UnknownTable(sub.table.clone()))?;
    let child_schema = child_schema.into_schema();
    let child_corr = resolve_cols(&csq.related.correlation.child_field, &child_schema)?;
    if parent_key.is_empty() || parent_key.len() != child_corr.len() {
        return Err(BuildError::Invalid(
            "aggregate EXISTS correlation parent/child fields must be non-empty and equal-length",
        ));
    }

    // The lazy grouped reduce: one `[child_key…, count]` group per parent (the same shape
    // the display relationship aggregate builds, design §3). Lazy so the parent join folds
    // one group at a time on a constrained fetch.
    let storage = graph.alloc_storage();
    let key_cols: Vec<&str> = csq
        .related
        .correlation
        .child_field
        .iter()
        .map(|c| &**c)
        .collect();
    let reduce_op = crate::op::Reduce::count_by(child_top, storage, child_corr.clone(), key_cols)
        .lazy()
        .with_input_types(&child_schema);
    let reduce_schema = reduce_op.schema.clone();
    let reduce = graph.add_reduce(reduce_op);
    graph.set_out_edge(
        child_top,
        OutEdge {
            node: reduce,
            port: Port::Single,
        },
    );

    // `having` → a Filter sub-graph ABOVE the reduce, predicating on its `count` output.
    // The Filter edit-split turns a group crossing the predicate threshold into an
    // `Add`/`Remove`, so the gate is maintained incrementally for free (design §8). Absent
    // `having` ⇒ "EXISTS any group" ⇒ `count ≥ 1` (high-pass), so no filter is needed.
    let gate_in = match &sub.having {
        None => reduce,
        Some(having) => {
            if !gather_csq_conditions(Some(having)).is_empty() {
                return Err(BuildError::Unsupported(
                    "a correlated subquery in a child-aggregate `having` is not supported",
                ));
            }
            reject_count_zero_satisfiable(having, &reduce_schema)?;
            build_filter_pipeline(graph, reduce, having, &reduce_schema)?
        }
    };

    // Attach the (HAVING-filtered) group to the parent's relationship slot. The HAVING
    // Filter is schema-preserving, so the group columns are still `0..k` — the join's child
    // key, matching `parent_key`. The `Exists` gate then counts this slot.
    let group_key: Vec<ColId> = (0..child_corr.len()).collect();
    let join = graph.add_join_slot(parent_top, gate_in, parent_key, group_key, rel_slot);
    graph.set_out_edge(
        parent_top,
        OutEdge {
            node: join,
            port: Port::JoinParent,
        },
    );
    graph.set_out_edge(
        gate_in,
        OutEdge {
            node: join,
            port: Port::JoinChild,
        },
    );
    Ok(join)
}

/// Reject a child-aggregate `having` that is **satisfied at count 0** (design §5). A lazy
/// reduce never fabricates a count-0 group for a childless parent, so the `Exists` gate
/// cannot see "count = 0"; a predicate true at 0 (`<= n`, `= 0`, `>= 0`, `!= n` for `n>0`)
/// would silently drop such parents from the view. v1 therefore accepts only a **single
/// high-pass `count <op> n` comparison** — false at 0 — and rejects everything else (a
/// group-column `having`, a compound tree, a non-numeric RHS) as out of scope.
fn reject_count_zero_satisfiable(
    having: &Condition,
    reduce_schema: &Schema,
) -> Result<(), BuildError> {
    let Condition::Simple(sc) = having else {
        return Err(BuildError::Unsupported(
            "a child-aggregate `having` v1 supports only a single `count <op> n` comparison",
        ));
    };
    let ValuePosition::Column { name } = &sc.left else {
        return Err(BuildError::Unsupported(
            "a child-aggregate `having` must compare the `count` column on its left",
        ));
    };
    // It must address the reduce's trailing synthetic `count` column, not a group column
    // (group-column HAVING is deferred Tier 2).
    let count_id = reduce_schema.columns.len() - 1;
    if col_id(reduce_schema, name)? != count_id {
        return Err(BuildError::Unsupported(
            "a child-aggregate `having` v1 filters only on `count` (group-column HAVING deferred)",
        ));
    }
    // Both numeric spellings: a JSON integer literal parses as `Lit::Int` as of
    // design 226 Stage B (`having count > 5` must keep working). The f64 widening is
    // safe here — the threshold compares against a row COUNT, far below 2^53.
    let n = match &sc.right {
        ValuePosition::Literal {
            value: Lit::Number(n),
        } => *n,
        ValuePosition::Literal { value: Lit::Int(i) } => *i as f64,
        _ => {
            return Err(BuildError::Unsupported(
                "a child-aggregate `having` right-hand side must be a numeric literal",
            ))
        }
    };
    let true_at_zero =
        match sc.op {
            Op::Gt => 0.0 > n,
            Op::Ge => 0.0 >= n,
            Op::Lt => 0.0 < n,
            Op::Le => 0.0 <= n,
            Op::Eq => 0.0 == n,
            Op::Ne => (0.0 - n).abs() > f64::EPSILON,
            _ => return Err(BuildError::Unsupported(
                "a child-aggregate `having` op must be a numeric comparison (=, !=, <, <=, >, >=)",
            )),
        };
    if true_at_zero {
        return Err(BuildError::Unsupported(
            "a count-0-satisfying child-aggregate filter (e.g. `<= n`, `= 0`, `>= 0`) needs \
             row-widening; only high-pass predicates like `> n` are supported (design §5)",
        ));
    }
    Ok(())
}

/// Build the [`FlippedJoin`](crate::op::FlippedJoin) for one **flipped** EXISTS
/// condition (`builder.ts:488-516`) — the child sub-pipeline (no `Cap`,
/// `isExistsChild=false`: it may carry its own `start`/`limit`/`related`), the
/// resolved keys, the `add_flipped_join`, and the **child** input edge — but **not**
/// the parent input edge. The flipped join is a child-driven inner join whose
/// inner-join drop *is* the EXISTS gate (no separate `Exists` operator). The caller
/// wires the parent: [`apply_where_with_flips`] returns `(parent_top → fj, fj)` so the
/// connection (AND spine) or the `UnionFanOut` broadcast (OR branch) reaches `fj` on
/// its `JoinParent` port.
fn build_flipped_join(
    graph: &mut Graph,
    parent_top: NodeId,
    csq: &CorrelatedSubqueryCondition,
    schema: &Schema,
    resolve: &impl Fn(&str) -> Option<(NodeId, SourceSchema)>,
) -> Result<NodeId, BuildError> {
    let rel_name = csq
        .related
        .subquery
        .alias
        .as_deref()
        .ok_or(BuildError::Invalid(
            "a flipped EXISTS subquery must have an alias",
        ))?;
    let rel_slot = schema
        .rel_slot(rel_name)
        .ok_or_else(|| BuildError::UnknownRelationship(rel_name.into()))?;

    // The flipped child is a normal sub-pipeline (NOT an exists-child → no `Cap`):
    // the FlippedJoin fetches all children first and its inner-join drop gates.
    let child_top = build_pipeline_internal(
        graph,
        &csq.related.subquery,
        Some(&csq.related.correlation.child_field),
        false,
        resolve,
    )?;

    let parent_key = resolve_cols(&csq.related.correlation.parent_field, schema)?;
    let (_, child_schema) = resolve(&csq.related.subquery.table)
        .ok_or_else(|| BuildError::UnknownTable(csq.related.subquery.table.clone()))?;
    let child_key = resolve_cols(
        &csq.related.correlation.child_field,
        &child_schema.into_schema(),
    )?;
    if parent_key.is_empty() || parent_key.len() != child_key.len() {
        return Err(BuildError::Invalid(
            "flipped EXISTS correlation parent/child fields must be non-empty and equal-length",
        ));
    }

    let fj = graph.add_flipped_join_slot(parent_top, child_top, parent_key, child_key, rel_slot);
    graph.set_out_edge(
        child_top,
        OutEdge {
            node: fj,
            port: Port::JoinChild,
        },
    );
    Ok(fj)
}

/// `applyFilterWithFlips` (`builder.ts:414`): lower a `where` (sub)tree that carries a
/// **flipped** EXISTS at some level. Returns `(head_edge, tail)` — the upstream
/// (connection/skip, or a `UnionFanOut` broadcast, or a sibling join) pushes the source
/// change into `head_edge` (node + port), and `tail` is the node the downstream / a
/// fan-in consumes. The caller owns the `upstream → head_edge` wiring (it differs by
/// upstream: `set_out_edge` for a port-aware op, `set_output`/`set_union_fan` otherwise),
/// so this never wires its own input.
///
/// Three cases (a bare `Simple` never reaches here: it has no flip, so
/// [`condition_has_flipped_subquery`] is false and it is never partitioned into a
/// with-flip branch):
/// - **`Or`** → [`apply_flips_or`]: a [`UnionFanOut`](crate::op::UnionFanOut) with one
///   branch per OR condition (flipped → recurse, flip-free → a local filter branch).
/// - **`And`** → [`apply_flips_and`]: the conjunctive gate chain.
/// - **`CorrelatedSubquery`** (flipped) → a bare [`FlippedJoin`](crate::op::FlippedJoin).
fn apply_where_with_flips(
    graph: &mut Graph,
    input: NodeId,
    cond: &Condition,
    schema: &Schema,
    sort: &Sort,
    resolve: &impl Fn(&str) -> Option<(NodeId, SourceSchema)>,
) -> Result<(OutEdge, NodeId), BuildError> {
    match cond {
        Condition::Or { conditions } => {
            apply_flips_or(graph, input, conditions, schema, sort, resolve)
        }
        Condition::And { conditions } => {
            apply_flips_and(graph, input, conditions, schema, sort, resolve)
        }
        Condition::CorrelatedSubquery(csq) => {
            debug_assert!(
                csq.flip == Some(true),
                "apply_where_with_flips on a non-flipped CSQ"
            );
            if matches!(csq.op, ExistsOp::NotExists) {
                return Err(BuildError::Unsupported("flipped NOT EXISTS is not lowered"));
            }
            let fj = build_flipped_join(graph, input, csq, schema, resolve)?;
            Ok((
                OutEdge {
                    node: fj,
                    port: Port::JoinParent,
                },
                fj,
            ))
        }
        // A `Simple` has no flip, so it is never a `withFlipped` branch.
        Condition::Simple(_) => unreachable!("apply_where_with_flips on a Simple (no flip)"),
    }
}

/// The `or` case of `applyFilterWithFlips` (`builder.ts:448`): a node-level
/// [`UnionFanOut`](crate::op::UnionFanOut) over `parent_top`, collapsed by a
/// [`UnionFanIn`](crate::op::UnionFanIn). Partition the OR conditions (by
/// [`condition_has_flipped_subquery`]) into with-flip and flip-free, then build
/// **the JS branch set** (`builder.ts:459-483`):
/// - **branch 0** (if any flip-free condition) — ONE combined `withoutFlipped` branch:
///   `FilterStart → applyOr(withoutFlipped) → FilterEnd` over the fan-out. Its `Exists`
///   gates count the **spine** non-flipped EXISTS Joins (below the fan-out), reached
///   through the rel-preserving broadcast — no local Joins;
/// - **branch i** — one per with-flip condition → [`apply_where_with_flips`] (a
///   `FlippedJoin`, or a nested AND-chain / union fan) over the fan-out.
///
/// The combined-`withoutFlipped` branch comes **first**, so the fan-in's PK dedup keeps
/// its relationship attachment over a later flipped branch's (the JS `mergeFetches`
/// first-wins). Returns `(parent_top → ufo, ufi)`.
fn apply_flips_or(
    graph: &mut Graph,
    parent_top: NodeId,
    or_conditions: &[Condition],
    schema: &Schema,
    sort: &Sort,
    resolve: &impl Fn(&str) -> Option<(NodeId, SourceSchema)>,
) -> Result<(OutEdge, NodeId), BuildError> {
    let mut with_flipped: Vec<&Condition> = Vec::new();
    let mut without_flipped: Vec<Condition> = Vec::new();
    for c in or_conditions {
        if condition_has_flipped_subquery(c) {
            with_flipped.push(c);
        } else {
            without_flipped.push(c.clone());
        }
    }
    debug_assert!(
        !with_flipped.is_empty(),
        "apply_flips_or with no flipped branch"
    );

    let ufo = graph.add_union_fan_out(parent_top);
    let mut branch_edges: Vec<OutEdge> = Vec::new();
    let mut branch_tails: Vec<NodeId> = Vec::new();
    // The per-branch pushable constraint (parallel to `branch_tails`): the fan-in merges
    // it into each branch fetch so an `eq(pk)` branch SEEKS the shared connection rather
    // than full-scanning it (see [`pushable_constraint`] / `UnionFanIn::fetch_branch`).
    let mut branch_constraints: Vec<Constraint> = Vec::new();

    // Branch 0: the combined `withoutFlipped` filter pipeline (FIRST → wins the merge).
    if !without_flipped.is_empty() {
        let or_cond = Condition::Or {
            conditions: without_flipped,
        };
        let (edge, tail) = build_filter_branch(graph, ufo, &or_cond, schema)?;
        branch_edges.push(edge);
        branch_tails.push(tail);
        branch_constraints.push(pushable_constraint(&or_cond, schema));
    }

    // One branch per with-flip condition.
    for c in with_flipped {
        let (edge, tail) = apply_where_with_flips(graph, ufo, c, schema, sort, resolve)?;
        branch_edges.push(edge);
        branch_tails.push(tail);
        branch_constraints.push(pushable_constraint(c, schema));
    }

    // The fan-in owns the merged schema — the root schema (its declared relationships
    // already cover the branches' slots) but carrying the connection's **resolved
    // `order_by` sort**, NOT the base table sort: every branch fetch streams in the
    // connection's order, so the k-way merge (and its PK adjacency dedup) must compare in
    // that same order (`union-fan-in.ts:34` reads the connection schema's resolved sort).
    // The resolved sort is PK-completed, so equal-PK rows stay adjacent.
    let fan_in_schema = Schema {
        sort: sort.clone(),
        ..schema.clone()
    };
    let ufi = graph.add_union_fan_in(ufo, branch_tails.clone(), branch_constraints, fan_in_schema);
    for tail in &branch_tails {
        graph.set_output(*tail, ufi);
    }
    graph.set_union_fan(ufo, branch_edges, ufi);
    Ok((
        OutEdge {
            node: ufo,
            port: Port::Single,
        },
        ufi,
    ))
}

/// The `and` case of `applyFilterWithFlips` (`builder.ts:424`): the conjunctive chain
/// over `input`, laid out **exactly as the JS**. Partition (by
/// [`condition_has_flipped_subquery`]) into with-flip and flip-free conditions. After
/// [`flatten_condition`] a with-flip condition is normally a bare flipped EXISTS or an
/// `OR` carrying a flip; a residual same-op `AND` (from the flatten quirk on
/// linear 3+-deep nesting) is handled by recursion through [`apply_where_with_flips`],
/// just as the JS `for (const cond of withFlipped) end = applyFilterWithFlips(end, …)`
/// (`builder.ts:443-445`) recurses — no special case.
///
///   `input → [withoutFlipped filter pipeline] → withFlipped₀ → withFlipped₁ → … → tail`
///
/// The flip-free conjuncts form ONE filter pipeline at the **bottom** (its `Exists` gates
/// count the spine non-flipped EXISTS Joins built below `input`); each with-flip condition
/// is then stacked on top via [`apply_where_with_flips`] (a `FlippedJoin`, a nested
/// union fan, or a recursive AND chain). Gap B lets a `FilterEnd` / `UnionFanIn` tail
/// present a `JoinParent` port, so a `FlippedJoin` can sit directly over the filter
/// pipeline — no port-aware reordering.
fn apply_flips_and(
    graph: &mut Graph,
    input: NodeId,
    and_conditions: &[Condition],
    schema: &Schema,
    sort: &Sort,
    resolve: &impl Fn(&str) -> Option<(NodeId, SourceSchema)>,
) -> Result<(OutEdge, NodeId), BuildError> {
    let mut with_flipped: Vec<&Condition> = Vec::new();
    let mut without_flipped: Vec<Condition> = Vec::new();
    for c in and_conditions {
        if condition_has_flipped_subquery(c) {
            with_flipped.push(c);
        } else {
            without_flipped.push(c.clone());
        }
    }
    debug_assert!(
        !with_flipped.is_empty(),
        "apply_flips_and with no flipped branch"
    );

    let mut head: Option<OutEdge> = None;
    let mut cur = input; // the running pipeline tail (port-capable: a spine op, a
                         // `FilterEnd`, a `FlippedJoin`, or a nested `UnionFanIn`).

    // BOTTOM: the flip-free conjuncts as one filter pipeline (`FilterStart → applyAnd →
    // FilterEnd`). Its `Exists` gates count the spine EXISTS Joins (built below `input`).
    if !without_flipped.is_empty() {
        let and_cond = Condition::And {
            conditions: without_flipped,
        };
        let (lf_head, lf_tail) = build_filter_branch(graph, input, &and_cond, schema)?;
        head = Some(lf_head);
        cur = lf_tail;
    }

    // Each with-flip condition stacked on top (`builder.ts:443-445`).
    for c in &with_flipped {
        let (edge, tail) = apply_where_with_flips(graph, cur, c, schema, sort, resolve)?;
        match head {
            None => head = Some(edge),                // first part → the overall head
            Some(_) => graph.set_out_edge(cur, edge), // wire cur → this head (Gap B tail OK)
        }
        cur = tail;
    }

    Ok((head.expect("apply_flips_and head set"), cur))
}

/// The **pushable constraint** of a `where` (sub)condition: the `col = literal`
/// equalities that every row the condition keeps must satisfy. Used by the OR union
/// fan ([`apply_flips_or`]) to give each branch a fetch constraint, so a branch like
/// `eq(pk)` **seeks** the shared source connection instead of full-scanning it (the OR
/// branches all fetch the *same* connection — without a per-branch constraint, a
/// `eq(pk) OR exists` query scans the whole table on the eq branch even though it
/// resolves to a single PK).
///
/// A constraint here is a *necessary* condition (every kept row satisfies it), so it
/// only narrows the fetch — the branch's own filter chain still applies the full
/// predicate, leaving results unchanged:
/// - `Simple(col = literal)` (non-null literal) → `[(col, value)]`; any other operator
///   or a literal/array LHS → empty (nothing seekable);
/// - `And[..]` → the **union** of the conjuncts' constraints (each is necessary),
///   skipping a later pair that repeats a column already taken (left wins — an
///   unsatisfiable AND still yields no rows via the predicate);
/// - `Or[..]` → the **intersection** across disjuncts (a pair is necessary only if
///   *every* disjunct forces the same `col = value`); a singleton `And`/`Or` thus
///   reduces to its one child, and any non-pushable disjunct empties the whole `Or`;
/// - `CorrelatedSubquery` → empty.
fn pushable_constraint(cond: &Condition, schema: &Schema) -> Constraint {
    match cond {
        Condition::Simple(sc) => simple_eq_constraint(sc, schema),
        Condition::And { conditions } => {
            let mut out: Constraint = Vec::new();
            for c in conditions {
                for (col, v) in pushable_constraint(c, schema) {
                    if !out.iter().any(|(cc, _)| *cc == col) {
                        out.push((col, v));
                    }
                }
            }
            out
        }
        Condition::Or { conditions } => {
            let mut iter = conditions.iter();
            let Some(first) = iter.next() else {
                return Vec::new(); // empty OR is `Const(false)` — nothing to push
            };
            let mut acc = pushable_constraint(first, schema);
            for c in iter {
                if acc.is_empty() {
                    break;
                }
                let next = pushable_constraint(c, schema);
                // Keep a pair only if this disjunct forces the SAME (col, value).
                acc.retain(|(col, v)| {
                    next.iter()
                        .any(|(c2, v2)| c2 == col && values_identical(v.as_ref(), v2.as_ref()))
                });
            }
            acc
        }
        Condition::CorrelatedSubquery(_) => Vec::new(),
    }
}

/// The single seekable equality of a leaf condition, or empty. Only `col = <non-null
/// literal>` is pushable; the value is lowered with the SAME number coercion
/// ([`lit_to_scalar`]) the connection predicate and row loader use, so a constraint
/// value compares identical to the stored cell.
fn simple_eq_constraint(sc: &SimpleCondition, schema: &Schema) -> Constraint {
    if !matches!(sc.op, Op::Eq) {
        return Vec::new();
    }
    let ValuePosition::Column { name } = &sc.left else {
        return Vec::new();
    };
    let ValuePosition::Literal { value } = &sc.right else {
        return Vec::new();
    };
    if matches!(value, Lit::Null) {
        return Vec::new(); // `col = null` never matches (folds to `Const(false)`)
    }
    match (schema.col_id(name), lit_to_scalar(value).ok()) {
        (Some(col), Some(v)) => vec![(col, v)],
        _ => Vec::new(),
    }
}

/// True if `cond` contains a flipped (`flip == Some(true)`) correlated subquery at any
/// level — the `conditionIncludesFlippedSubqueryAtAnyLevel` classifier
/// (`builder.ts:811`).
fn condition_has_flipped_subquery(cond: &Condition) -> bool {
    match cond {
        Condition::CorrelatedSubquery(csq) => csq.flip == Some(true),
        Condition::Simple(_) => false,
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().any(condition_has_flipped_subquery)
        }
    }
}

/// `buildFilterPipeline` (`filter-operators.ts:148`) for the **non-flipped**
/// `applyWhere` (`builder.ts:399`): bracket `input` in a `FilterStart … FilterEnd`
/// sub-graph whose chain is [`apply_filter`]`(where_tree)` — leaf `Filter`s,
/// [`Exists`](crate::op::Exists) gates, and `FanOut`/`FanIn` OR fans, mirroring the
/// JS Filter pipeline. The **full** `where` tree is applied: the source could not
/// push a disjunction carrying a subquery (`transform_filters` collapsed it), so this
/// is the only place that `OR` is evaluated; a leaf that the source *did* apply (a
/// pure-AND conjunct) is re-applied here as a harmless pass-through gate, matching the
/// JS. Returns the `FilterEnd` (the new pipeline `end`).
fn build_filter_pipeline(
    graph: &mut Graph,
    input: NodeId,
    where_tree: &Condition,
    schema: &Schema,
) -> Result<NodeId, BuildError> {
    let (head, tail) = build_filter_branch(graph, input, where_tree, schema)?;
    graph.set_out_edge(input, head);
    Ok(tail)
}

/// Like [`build_filter_pipeline`] but **does not wire its own input** — it returns the
/// `FilterStart` head edge (always `Port::Single`) and the `FilterEnd` tail, leaving the
/// `input → head` wiring to the caller. Used for an `OR`/`AND`'s combined `withoutFlipped`
/// branch over a [`UnionFanOut`](crate::op::UnionFanOut) broadcast (the fan-out wires the
/// edge via [`Graph::set_union_fan`]) and the `AND` chain's bottom filter pipeline.
fn build_filter_branch(
    graph: &mut Graph,
    input: NodeId,
    cond: &Condition,
    schema: &Schema,
) -> Result<(OutEdge, NodeId), BuildError> {
    let fs = graph.add_filter_start(input);
    let (head, tail) = apply_filter(graph, fs, cond, schema)?;
    let fe = graph.add_filter_end(fs);
    graph.set_chain_head(fs, head);
    graph.set_output(tail, fe);
    Ok((
        OutEdge {
            node: fs,
            port: Port::Single,
        },
        fe,
    ))
}

/// `applyFilter` (`builder.ts:523`): dispatch one `where` condition into the filter
/// chain over `input`. Returns `(head, tail)` — `head` is the link the upstream wires
/// **into** (a `FilterStart`'s chain head, or a `FanOut` branch), `tail` is the link
/// whose downstream wires to the **next** chain link (a `FanIn` / `FilterEnd`). For a
/// single-link condition (a leaf `Filter` or an `Exists` gate) `head == tail`. (The
/// caller owns the upstream→`head` wiring, which differs by upstream kind:
/// `set_chain_head` / `set_fan` / `set_output`.)
fn apply_filter(
    graph: &mut Graph,
    input: NodeId,
    cond: &Condition,
    schema: &Schema,
) -> Result<(NodeId, NodeId), BuildError> {
    match cond {
        // `applySimpleCondition` (`builder.ts:625`): one leaf `Filter`.
        Condition::Simple(sc) => {
            let f = graph.add_filter(input, create_predicate(sc, schema)?);
            Ok((f, f))
        }
        Condition::CorrelatedSubquery(csq) => apply_csq_condition(graph, input, csq, schema),
        Condition::And { conditions } => apply_and(graph, input, conditions, schema),
        Condition::Or { conditions } => apply_or(graph, input, conditions, schema),
    }
}

/// `applyAnd` (`builder.ts:541`): chain each sub-condition's filter **in series** —
/// the AND is the chain (every link must pass). Returns the first link's head and the
/// last link's tail. An empty `AND` is `true` (a `Const(true)` pass-through).
fn apply_and(
    graph: &mut Graph,
    input: NodeId,
    conditions: &[Condition],
    schema: &Schema,
) -> Result<(NodeId, NodeId), BuildError> {
    let mut iter = conditions.iter();
    let Some(first) = iter.next() else {
        let f = graph.add_filter(input, CompiledPredicate::Const(true));
        return Ok((f, f));
    };
    let (head, mut tail) = apply_filter(graph, input, first, schema)?;
    for c in iter {
        let (h, t) = apply_filter(graph, tail, c, schema)?;
        graph.set_output(tail, h); // wire the previous tail → this head
        tail = t;
    }
    Ok((head, tail))
}

/// `applyOr` (`builder.ts:553`): a `FanOut` over `input`, **one branch per
/// condition**, k-way-collapsed by a `FanIn` — the disjunction (the `FanOut` ORs the
/// branches, `chain_filter` / `fan_out_push`). The JS groups the pure-predicate
/// branches into a single `Filter` over their `Or`; the flat [`CompiledPredicate`]
/// cannot hold an `OR`, so each branch fans separately instead — equivalent, since the
/// `FanOut` is the disjunction. Returns `(fan_out, fan_in)`. An empty `OR` is `false`.
fn apply_or(
    graph: &mut Graph,
    input: NodeId,
    conditions: &[Condition],
    schema: &Schema,
) -> Result<(NodeId, NodeId), BuildError> {
    if conditions.is_empty() {
        let f = graph.add_filter(input, CompiledPredicate::Const(false));
        return Ok((f, f));
    }
    let fan_out = graph.add_fan_out(input);
    let fan_in = graph.add_fan_in(fan_out);
    let mut branch_heads: Vec<NodeId> = Vec::with_capacity(conditions.len());
    for c in conditions {
        let (head, tail) = apply_filter(graph, fan_out, c, schema)?;
        graph.set_output(tail, fan_in); // each branch tail → the fan-in
        branch_heads.push(head);
    }
    graph.set_fan(fan_out, branch_heads, fan_in);
    Ok((fan_out, fan_in))
}

/// `applyCorrelatedSubqueryCondition` (`builder.ts:689`) for a **non-flipped** EXISTS:
/// one [`Exists`](crate::op::Exists) gate counting the relationship its `Join` (built
/// in the `csq_conditions` loop) attached. An EXISTS over a `limit 0` (empty) subquery
/// is constant-false (`builder.ts:699`) — constant-true for `NOT EXISTS` — so it
/// becomes a `Const` `Filter`, no Join. Returns `(node, node)` (a single chain link).
/// A flipped subquery never reaches here.
fn apply_csq_condition(
    graph: &mut Graph,
    input: NodeId,
    csq: &CorrelatedSubqueryCondition,
    schema: &Schema,
) -> Result<(NodeId, NodeId), BuildError> {
    let node = if csq.related.subquery.limit == Some(0) {
        let pass = matches!(csq.op, ExistsOp::NotExists);
        graph.add_filter(input, CompiledPredicate::Const(pass))
    } else {
        let rel_name = csq
            .related
            .subquery
            .alias
            .as_deref()
            .ok_or(BuildError::Invalid("an EXISTS subquery must have an alias"))?;
        let rel_slot = schema
            .rel_slot(rel_name)
            .ok_or_else(|| BuildError::UnknownRelationship(rel_name.into()))?;
        let parent_key = resolve_cols(&csq.related.correlation.parent_field, schema)?;
        let not = matches!(csq.op, ExistsOp::NotExists);
        graph.add_exists(crate::op::Exists::new(
            input,
            rel_slot,
            parent_key,
            not,
            &schema.primary_key,
        ))
    };
    Ok((node, node))
}

/// `applyWhere` for the EXISTS subset (`builder.ts:399`): bracket `end` in a
/// `FilterStart … FilterEnd` filter sub-graph whose chain is one
/// [`Exists`](crate::op::Exists) gate per gathered condition (in tree order). The
/// leaf filters are not re-applied here — the source already applied them fully — so
/// the chain is purely the EXISTS gates (AND-composed by chaining; OR-with-subquery
/// is rejected upstream). Returns the `FilterEnd` (the new pipeline `end`).
fn apply_where_exists(
    graph: &mut Graph,
    end: NodeId,
    csq_conditions: &[&CorrelatedSubqueryCondition],
    schema: &Schema,
) -> Result<NodeId, BuildError> {
    debug_assert!(
        !csq_conditions.is_empty(),
        "apply_where_exists with no EXISTS"
    );
    let fs = graph.add_filter_start(end);
    let fe = graph.add_filter_end(fs);

    let mut chain: Vec<NodeId> = Vec::with_capacity(csq_conditions.len());
    let mut prev = fs;
    for csq in csq_conditions {
        let node = if csq.related.subquery.limit == Some(0) {
            // EXISTS over a `limit 0` (empty) subquery is constant false; NOT EXISTS
            // over the same empty subquery is constant true. No Join is built for
            // either case (`applyCorrelatedSubqueryCondition`'s `limit === 0`
            // short-circuit, `builder.ts:699-704`).
            let pass = matches!(csq.op, ExistsOp::NotExists);
            graph.add_filter(prev, CompiledPredicate::Const(pass))
        } else {
            let rel_name = csq
                .related
                .subquery
                .alias
                .as_deref()
                .ok_or(BuildError::Invalid("an EXISTS subquery must have an alias"))?;
            let rel_slot = schema
                .rel_slot(rel_name)
                .ok_or_else(|| BuildError::UnknownRelationship(rel_name.into()))?;
            let parent_key = resolve_cols(&csq.related.correlation.parent_field, schema)?;
            let not = matches!(csq.op, ExistsOp::NotExists);
            graph.add_exists(crate::op::Exists::new(
                prev,
                rel_slot,
                parent_key,
                not,
                &schema.primary_key,
            ))
        };
        chain.push(node);
        prev = node;
    }

    // Wire FilterStart → Exists* → FilterEnd.
    graph.set_chain_head(fs, chain[0]);
    for (i, &ex) in chain.iter().enumerate() {
        let downstream = chain.get(i + 1).copied().unwrap_or(fe);
        graph.set_output(ex, downstream);
    }
    graph.set_out_edge(
        end,
        OutEdge {
            node: fs,
            port: Port::Single,
        },
    );
    Ok(fe)
}

/// Gather every correlated-subquery condition in `where`, in tree order
/// (`gatherCorrelatedSubqueryQueryConditions`, `builder.ts:720`).
fn gather_csq_conditions(where_clause: Option<&Condition>) -> Vec<&CorrelatedSubqueryCondition> {
    fn go<'a>(cond: &'a Condition, out: &mut Vec<&'a CorrelatedSubqueryCondition>) {
        match cond {
            Condition::CorrelatedSubquery(c) => out.push(c),
            Condition::And { conditions } | Condition::Or { conditions } => {
                for c in conditions {
                    go(c, out);
                }
            }
            Condition::Simple(_) => {}
        }
    }
    let mut out = Vec::new();
    if let Some(w) = where_clause {
        go(w, &mut out);
    }
    out
}

/// The **query-local relationship slot layout** for one pipeline frame: the ordered,
/// de-duplicated relationship names this query references, in the order that backs each
/// [`RelId`](crate::value::RelId) (slot = list index). The slot layout is a pure function
/// of the (already **normalized**) query AST — *not* the source [`Schema`]'s declared
/// relationships, which the source cannot pre-declare for the synthesized EXISTS-gate
/// aliases (`comments_0`, …) the alias-uniquifier mints for a multi-EXISTS `where`.
///
/// Order (the convention the oracle-backed `diff_fixtures::collect_rel_aliases` test
/// harness already encodes):
/// 1. **materialized `related`** aliases first, in `ast.related` order (first-seen),
/// 2. then **EXISTS gating** aliases in `where`-tree pre-order (`gather_csq_conditions` —
///    the same order `uniquify_condition_aliases` assigns the `_N` suffixes), skipping a
///    `limit 0` subquery (a constant-false gate that builds **no** Join, so it claims no
///    slot).
///
/// De-duped by name across both passes. The result is the single source of truth shared
/// by the three slot consumers — the build path (`build_pipeline_internal` resolves
/// `RelId`s against it), the [`View`](crate::view) shape ([`view_schema`]), and
/// the union fan's `add_empty_relationships` count — so they agree on `RelId`/order by
/// construction.
///
/// **Input must be normalized** ([`normalize_pipeline_ast`]); calling it on a raw AST
/// would read un-uniquified aliases and could double-uniquify on re-normalization.
pub fn query_local_slot_names(normalized: &Ast) -> Vec<Box<str>> {
    let mut names: Vec<Box<str>> = Vec::new();
    let push_unique = |names: &mut Vec<Box<str>>, alias: &str| {
        if !names.iter().any(|n| n.as_ref() == alias) {
            names.push(alias.into());
        }
    };
    // (1) materialized `related` (in-view), first-seen order.
    for csq in &normalized.related {
        if let Some(a) = csq.subquery.alias.as_deref() {
            push_unique(&mut names, a);
        }
    }
    // (2) EXISTS gating (out-of-view), where-tree pre-order; a `limit 0` gate builds no
    //     Join, so it claims no slot.
    for c in gather_csq_conditions(normalized.r#where.as_ref()) {
        if c.related.subquery.limit == Some(0) {
            continue;
        }
        if let Some(a) = c.related.subquery.alias.as_deref() {
            push_unique(&mut names, a);
        }
    }
    names
}

/// True if any correlated subquery sits under an `or` — the UnionFanOut path.
fn has_subquery_under_or(cond: &Condition) -> bool {
    fn go(cond: &Condition, under_or: bool) -> bool {
        match cond {
            Condition::CorrelatedSubquery(_) => under_or,
            Condition::Simple(_) => false,
            Condition::And { conditions } => conditions.iter().any(|c| go(c, under_or)),
            Condition::Or { conditions } => conditions.iter().any(|c| go(c, true)),
        }
    }
    go(cond, false)
}

/// Dedup `related` by subquery alias, last-writer-wins (`builder.ts:385-388`).
fn dedup_related_by_alias(related: &[CorrelatedSubquery]) -> Vec<&CorrelatedSubquery> {
    let mut order: Vec<&str> = Vec::new();
    let mut chosen: Vec<&CorrelatedSubquery> = Vec::new();
    for csq in related {
        let alias = csq.subquery.alias.as_deref().unwrap_or("");
        match order.iter().position(|a| *a == alias) {
            Some(i) => chosen[i] = csq,
            None => {
                order.push(alias);
                chosen.push(csq);
            }
        }
    }
    chosen
}

/// Lower `ast.limit` to a [`Take`](crate::op::Take) over `parent` (the current
/// pipeline end) — the analogue of JS `applyLimit`'s ordered branch
/// (`builder.ts:373`). `partition_key` (names) is this frame's correlation **child**
/// field when this AST is a limited relationship child, or `None` at the root; it
/// resolves to the Take's partition columns against `schema`. The Take carries the
/// connection's resolved `sort` (PK-completed) and a fresh [`Graph::alloc_storage`]
/// slot, and forwards on a port-carrying [`OutEdge`] so a limited relationship can
/// feed a parent join's parent port.
fn lower_limit(
    graph: &mut Graph,
    parent: NodeId,
    limit: u32,
    partition_key: Option<&[Box<str>]>,
    use_cap: bool,
    schema: &Schema,
    sort: &Sort,
) -> Result<NodeId, BuildError> {
    let pk_cols = match partition_key {
        Some(names) => Some(resolve_cols(names, schema)?),
        None => None,
    };
    let storage = graph.alloc_storage();
    // An EXISTS child whose `where` carries no flip uses an unordered `Cap` (count-only,
    // PK-set membership, JS `useCap`); every other limit — and a flipped-`where` EXISTS
    // child, whose union-fan tail needs the ordered merge — uses an ordered `Take`.
    let node = if use_cap {
        graph.add_cap(crate::op::Cap::new(
            parent,
            storage,
            limit,
            pk_cols,
            schema.primary_key.clone(),
        ))
    } else {
        graph.add_take(crate::op::Take::new(
            parent,
            storage,
            limit,
            pk_cols,
            sort.clone(),
        ))
    };
    // `wire_single`, not `set_out_edge`: a flipped-`where` lowering ends in a
    // `UnionFanIn`/`FilterEnd` tail (not port-aware), over which the `Take`/`Cap` sits.
    graph.wire_single(parent, node);
    Ok(node)
}

/// Lower the AST `start` bound — a **name-keyed** partial row + `exclusive` flag —
/// into the positional [`Start`] the [`Skip`](crate::op::Skip) operator wants. Each
/// `(name, lit)` is placed at its [`ColId`]; unmentioned columns stay `Null` (the
/// Skip comparator only reads the sort columns, which a start bound carries).
/// `exclusive` ⇒ [`Basis::After`] (drop the bound row), else [`Basis::At`] (keep it).
fn lower_start(bound: &Bound, schema: &Schema) -> Result<Start, BuildError> {
    let mut row = vec![OwnedValue::Null; schema.columns.len()];
    for (name, lit) in &bound.row {
        row[col_id(schema, name)?] = lit_to_scalar(lit)?;
    }
    Ok(Start {
        row: owned_row(row),
        basis: if bound.exclusive {
            Basis::After
        } else {
            Basis::At
        },
    })
}

/// Resolve `order_by` to a [`Sort`], appending every missing primary-key column
/// (asc) so the result includes the PK — `complete_ordering`'s invariant applied at
/// `ColId` level (which the source's connect asserts). Done per frame, so the whole
/// tree (root + each child) is completed.
pub(crate) fn resolve_sort(order_by: &[OrderPart], schema: &Schema) -> Result<Sort, BuildError> {
    let mut sort: Sort = Vec::with_capacity(order_by.len() + schema.primary_key.len());
    for op in order_by {
        sort.push((col_id(schema, op.field())?, matches!(op.dir(), Dir::Asc)));
    }
    for &pk in &schema.primary_key {
        if !sort.iter().any(|&(c, _)| c == pk) {
            sort.push((pk, true));
        }
    }
    Ok(sort)
}

/// Resolve a list of column names to [`ColId`]s.
fn resolve_cols(names: &[Box<str>], schema: &Schema) -> Result<Vec<ColId>, BuildError> {
    names.iter().map(|n| col_id(schema, n)).collect()
}

/// Assemble the connection's `split_edit_keys` (`builder.ts:275-292`):
/// `partition_key` (the correlation **child** field, when this AST is a
/// relationship child) plus every `related` correlation **parent** field. No PK
/// (matching JS). Names resolve against THIS source's schema; deduped.
fn compute_split_edit_keys(
    partition_key: Option<&[Box<str>]>,
    related: &[CorrelatedSubquery],
    csq_conditions: &[&CorrelatedSubqueryCondition],
    schema: &Schema,
) -> Result<Vec<ColId>, BuildError> {
    let mut cols: Vec<ColId> = Vec::new();
    let add = |cols: &mut Vec<ColId>, names: &[Box<str>]| -> Result<(), BuildError> {
        for n in names {
            let c = col_id(schema, n)?;
            if !cols.contains(&c) {
                cols.push(c);
            }
        }
        Ok(())
    };
    if let Some(pk) = partition_key {
        add(&mut cols, pk)?;
    }
    // The EXISTS conditions' parent fields (`builder.ts:280-285`)…
    for csq in csq_conditions {
        add(&mut cols, &csq.related.correlation.parent_field)?;
    }
    // …and the `related` parent fields (`builder.ts:287-291`).
    for csq in related {
        add(&mut cols, &csq.correlation.parent_field)?;
    }
    Ok(cols)
}

/// The set of columns a built query structurally touches — the input to the
/// connection presence predicate (`PROJECTION-SUPPORT-DESIGN.md` §3.2). It is the
/// query's projection plus every column it reads to resolve a row:
/// `select` (or **all** columns when `None`) ∪ `where`-leaf columns ∪ the resolved
/// `sort` (order_by + completed PK) ∪ `start`-bound columns ∪ correlation parent
/// fields (already gathered as `split_edit_keys`). A partial union row missing any of
/// these is dropped from this query by the presence predicate (§3.3). A pure function
/// of the `Ast`, computed once at build time; deduped, output order is incidental.
fn required_cols(
    ast: &Ast,
    schema: &Schema,
    sort: &Sort,
    split_edit_keys: &[ColId],
) -> Result<Vec<ColId>, BuildError> {
    let mut cols: Vec<ColId> = Vec::new();
    // `select` (or every column when select-all).
    match &ast.select {
        Some(names) => {
            for n in names {
                push_unique(&mut cols, col_id(schema, n)?);
            }
        }
        None => {
            for c in 0..schema.columns.len() {
                push_unique(&mut cols, c);
            }
        }
    }
    // `where`-leaf columns (a column on either side of a `Simple`).
    if let Some(w) = &ast.r#where {
        where_leaf_columns(w, schema, &mut cols)?;
    }
    // Resolved sort (order_by + PK-completion).
    for &(c, _) in sort {
        push_unique(&mut cols, c);
    }
    // `start`-bound columns (the cursor's named cells; `b.row` is a positional list of
    // `(name, literal)`, not a map).
    if let Some(b) = &ast.start {
        for cell in &b.row {
            push_unique(&mut cols, col_id(schema, cell.0.as_ref())?);
        }
    }
    // Correlation parent fields a `related`/EXISTS child reads (already resolved).
    for &c in split_edit_keys {
        push_unique(&mut cols, c);
    }
    Ok(cols)
}

/// Push `c` into `cols` iff absent (small set; linear scan is fine).
fn push_unique(cols: &mut Vec<ColId>, c: ColId) {
    if !cols.contains(&c) {
        cols.push(c);
    }
}

/// Collect the leaf column [`ColId`]s referenced by a `where` tree (either side of a
/// `Simple` comparison; recursing `and`/`or`). A `CorrelatedSubquery`'s parent fields
/// are *not* gathered here — they arrive via `split_edit_keys` — so this stays a pure
/// scan of the leaf comparisons.
fn where_leaf_columns(
    cond: &Condition,
    schema: &Schema,
    out: &mut Vec<ColId>,
) -> Result<(), BuildError> {
    match cond {
        Condition::Simple(sc) => {
            for vp in [&sc.left, &sc.right] {
                if let ValuePosition::Column { name } = vp {
                    push_unique(out, col_id(schema, name)?);
                }
            }
            Ok(())
        }
        Condition::And { conditions } | Condition::Or { conditions } => {
            for c in conditions {
                where_leaf_columns(c, schema, out)?;
            }
            Ok(())
        }
        Condition::CorrelatedSubquery(_) => Ok(()),
    }
}

/// Deduplicate an owned-value set under **predicate identity**
/// ([`values_identical`]) — the same equality the guard's buckets key on, so a
/// literal repeated as `IN (1, 1)` or unioned across OR branches indexes once. O(n²)
/// over a tiny per-guard set.
fn dedup_guard_values(values: Vec<OwnedValue>) -> Vec<OwnedValue> {
    let mut out: Vec<OwnedValue> = Vec::with_capacity(values.len());
    for v in values {
        if !out.iter().any(|u| values_identical(u.as_ref(), v.as_ref())) {
            out.push(v);
        }
    }
    out
}

/// Extract an equality [`PushGuard`] from a connection's **stripped** `where` tree —
/// a single-column finite implication `predicate(row) ⇒ row[col] ∈ values` used to
/// prune the source push fan-out (`designs/205-GUARDED-PUSH-FANOUT-DESIGN.md`).
///
/// **Fail closed:** any shape not listed yields `None` (→ the connection joins the
/// always-visited scan list, today's behavior). The guard is only ever a *weakening*
/// of the predicate, so `None` and any over-approximation are safe; the exact
/// predicate still runs on every candidate.
///
/// Extraction mirrors [`create_predicate`]'s lowering exactly — literals go through
/// [`lit_to_scalar`] (the same number coercion), so a guard value lands in the same
/// [`GuardKey`](crate::push_index) bucket the matching row cell will look up:
///
/// - `col = <non-null lit>` → `{lit}`. `col = NULL` folds to never-matching
///   (SQL `= NULL` is UNKNOWN, as [`create_predicate`] const-folds to false) →
///   **empty** values (indexed nowhere, exact).
/// - `col IS <lit>` (incl. `col IS NULL`, non-negated) → `{lit}` — the NULL-aware
///   identity the predicate uses.
/// - `col IN (l₁…lₖ)` (non-negated) → `{l₁…lₖ}`; empty `IN ()` → empty values.
/// - `And(children)` → the child guard with the **smallest** value set (ties → first);
///   a conjunction only narrows, so any one child's guard is a valid weakening. A
///   never-matching child (empty values) wins as smallest → the whole `And` indexes
///   nowhere, which is exact.
/// - `Or(children)` → a guard only if **every** child yields one; never-matching
///   children are dropped (a false disjunct cannot change the result), and the
///   survivors must guard the **same** column — `values` is their union. One
///   guard-less or different-column survivor ⇒ `None`.
/// - Everything else (`!=`/`IS NOT`/`NOT IN`, ordering, LIKE family, correlated
///   subqueries, a literal LHS, a column RHS) ⇒ `None`.
fn extract_push_guard(cond: &Condition, schema: &Schema) -> Option<PushGuard> {
    match cond {
        Condition::Simple(sc) => extract_simple_guard(sc, schema),
        Condition::And { conditions } => conditions
            .iter()
            .filter_map(|c| extract_push_guard(c, schema))
            .min_by_key(|g| g.values.len()),
        Condition::Or { conditions } => {
            // Every branch must be guardable; never-matching branches (empty values)
            // are dropped, and the rest must share one column.
            let mut col: Option<ColId> = None;
            let mut union: Vec<OwnedValue> = Vec::new();
            for c in conditions {
                let g = extract_push_guard(c, schema)?; // any None branch ⇒ no guard
                if g.values.is_empty() {
                    continue; // false disjunct: cannot broaden the result
                }
                match col {
                    None => col = Some(g.col),
                    Some(existing) if existing != g.col => return None,
                    Some(_) => {}
                }
                union.extend(g.values);
            }
            // All branches never-matching ⇒ the whole OR is never-matching. Emit an
            // empty guard (indexed nowhere) on any branch's column; `col` is unused
            // when values is empty, so fall back to column 0.
            Some(PushGuard {
                col: col.unwrap_or(0),
                values: dedup_guard_values(union),
            })
        }
        Condition::CorrelatedSubquery(_) => None,
    }
}

/// The leaf case of [`extract_push_guard`]: a `col <op> lit` comparison.
fn extract_simple_guard(sc: &SimpleCondition, schema: &Schema) -> Option<PushGuard> {
    // Guard only a `Column <op> Literal` shape (the dominant `where col = ?`). A
    // literal LHS is a constant fold, a column RHS is invalid — neither guards.
    let ValuePosition::Column { name } = &sc.left else {
        return None;
    };
    let ValuePosition::Literal { value: rhs } = &sc.right else {
        return None;
    };
    let col = schema.col_id(name)?;
    let one = |v: OwnedValue| {
        Some(PushGuard {
            col,
            values: vec![v],
        })
    };
    match sc.op {
        // `col = NULL` is UNKNOWN for every row (`create_predicate` folds to false):
        // never-matching, so an empty guard (indexed nowhere) is exact.
        Op::Eq if matches!(rhs, Lit::Null) => Some(PushGuard {
            col,
            values: Vec::new(),
        }),
        // `col = lit` / `col IS lit` (incl. `IS NULL`): identity on one literal.
        Op::Eq | Op::Is => one(lit_to_scalar(rhs).ok()?),
        // `col IN (list)`: identity membership over the (deduped) scalar list.
        Op::In => {
            let Lit::Array(elems) = rhs else {
                return None;
            };
            let mut values = Vec::with_capacity(elems.len());
            for e in elems {
                values.push(lit_to_scalar(e).ok()?);
            }
            Some(PushGuard {
                col,
                values: dedup_guard_values(values),
            })
        }
        // Negated equality, ordering, and the LIKE family imply no finite value set.
        _ => None,
    }
}

/// Build the connection's pushed-down filter from `where` (+ an optional projection
/// **presence** clause). Runs [`transform_filters`] (strip subqueries); for the built
/// subset nothing is removed, so `fully_applied = true` and no Filter operators are
/// needed above. A stripped subquery ⇒ [`BuildError::Unsupported`] (EXISTS not yet
/// built).
///
/// `presence` is `Some(required_cols)` **only for a projected query** (§3.3): a clause
/// requiring every column the query reads to be present is AND-ed into the predicate,
/// so a partial union row missing one is dropped (turned into an `Add`/`Remove` by
/// `filter_push` as the union widens/narrows). For a `'*'` query it is `None` and the
/// behavior is byte-identical to before (§7). The presence test is **client-only** —
/// it is never lowered to `sql_condition` (the server holds full rows and never
/// constructs `Absent`).
fn build_connection_filters(
    where_clause: Option<&Condition>,
    presence: Option<&[ColId]>,
    schema: &Schema,
) -> Result<(Option<ConnectionFilters>, bool), BuildError> {
    let (stripped, removed) = transform_filters(where_clause);
    // `fullyAppliedFilters` is false iff a subquery was stripped — the source then
    // applies only the surviving leaf tree, and the builder adds an `Exists` gate
    // for each stripped subquery (`builder.ts:321`,`:352`).
    let fully_applied = !removed;
    let presence_pred: Option<RowPredicate> = presence.map(|cols| {
        let cols = cols.to_vec();
        Rc::new(move |row: &OwnedRow| cols.iter().all(|&c| !row.col(c).is_absent())) as RowPredicate
    });
    match stripped {
        None => match presence_pred {
            None => Ok((None, fully_applied)),
            // A projected query with no `where`: a presence-only connection filter.
            // No `where` tree ⇒ no equality guard, so this connection lands in the
            // push index's always-visited scan list (`push_guard: None`).
            Some(pres) => Ok((
                Some(ConnectionFilters {
                    predicate: pres,
                    pk_constraint: None,
                    fully_applied,
                    sql_condition: None,
                    push_guard: None,
                }),
                fully_applied,
            )),
        },
        Some(cond) => {
            let base = create_row_predicate(&cond, schema)?;
            // Guard derives from the *stripped* leaf tree — exactly what `predicate`
            // (below) evaluates — so `predicate ⇒ guard` holds; the presence AND-clause
            // only narrows further (`designs/205` §1 soundness).
            let push_guard = extract_push_guard(&cond, schema);
            let predicate: RowPredicate = match presence_pred {
                None => base,
                Some(pres) => Rc::new(move |row: &OwnedRow| pres(row) && base(row)),
            };
            Ok((
                Some(ConnectionFilters {
                    predicate,
                    // Hoisting the PK constraint from the filters is a deferred fetch
                    // optimization (the predicate already filters correctly).
                    pk_constraint: None,
                    fully_applied,
                    // SQL pushdown is the sqlite leaf's path; the memory leaf ignores this
                    // and filters via `predicate`. Presence is intentionally NOT lowered.
                    sql_condition: Some(create_sql_condition(&cond, schema)?),
                    push_guard,
                }),
                fully_applied,
            ))
        }
    }
}

/// Lower the same leaf-only condition tree used for [`create_row_predicate`] into
/// the backend-neutral SQL condition consumed by the SQLite `TableSource` (in the
/// `rindle-sqlite` crate). Column names are resolved to [`ColId`] once here; literal
/// values are bound later by the SQL query builder, never interpolated into SQL text.
fn create_sql_condition(cond: &Condition, schema: &Schema) -> Result<SqlCondition, BuildError> {
    match cond {
        Condition::Simple(sc) => create_simple_sql_condition(sc, schema),
        Condition::And { conditions } => Ok(SqlCondition::And(
            conditions
                .iter()
                .map(|c| create_sql_condition(c, schema))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Condition::Or { conditions } => Ok(SqlCondition::Or(
            conditions
                .iter()
                .map(|c| create_sql_condition(c, schema))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Condition::CorrelatedSubquery(_) => Err(BuildError::Unsupported(
            "correlated subquery in source SQL condition",
        )),
    }
}

fn create_simple_sql_condition(
    cond: &SimpleCondition,
    schema: &Schema,
) -> Result<SqlCondition, BuildError> {
    Ok(SqlCondition::Simple {
        left: value_position_to_sql_operand(&cond.left, schema)?,
        op: sql_op(cond.op),
        right: match &cond.right {
            ValuePosition::Literal { .. } => value_position_to_sql_operand(&cond.right, schema)?,
            ValuePosition::Column { .. } => {
                return Err(BuildError::Invalid(
                    "right-hand side of a condition must be a literal",
                ))
            }
        },
    })
}

fn value_position_to_sql_operand(
    value: &ValuePosition,
    schema: &Schema,
) -> Result<Operand, BuildError> {
    match value {
        ValuePosition::Column { name } => Ok(Operand::Column(col_id(schema, name)?)),
        ValuePosition::Literal { value } => Ok(Operand::Literal(lit_to_sql_value(value)?)),
    }
}

fn lit_to_sql_value(lit: &Lit) -> Result<OwnedValue, BuildError> {
    Ok(match lit {
        Lit::Array(_) => OwnedValue::Json(Arc::from(lit_to_json(lit))),
        _ => lit_to_scalar(lit)?,
    })
}

fn sql_op(op: Op) -> SqlOp {
    match op {
        Op::Eq => SqlOp::Eq,
        Op::Ne => SqlOp::Ne,
        Op::Lt => SqlOp::Lt,
        Op::Le => SqlOp::Le,
        Op::Gt => SqlOp::Gt,
        Op::Ge => SqlOp::Ge,
        Op::Is => SqlOp::Is,
        Op::IsNot => SqlOp::IsNot,
        Op::In => SqlOp::In,
        Op::NotIn => SqlOp::NotIn,
        Op::Like => SqlOp::Like,
        Op::NotLike => SqlOp::NotLike,
        Op::ILike => SqlOp::Ilike,
        Op::NotILike => SqlOp::NotIlike,
    }
}

fn lit_to_json(lit: &Lit) -> String {
    match lit {
        Lit::Null => "null".to_string(),
        Lit::Bool(b) => b.to_string(),
        Lit::Int(i) => i.to_string(),
        Lit::Number(n) => {
            if n.fract() == 0.0 {
                format!("{n:.0}")
            } else {
                n.to_string()
            }
        }
        Lit::Str(s) => json_quote(s),
        Lit::Array(values) => {
            let mut out = String::from("[");
            for (i, v) in values.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&lit_to_json(v));
            }
            out.push(']');
            out
        }
    }
}

fn json_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Lower a leaf-only `where` condition tree to one [`RowPredicate`] — the
/// connection's in-memory filter. AND/OR fold to `all`/`any` over the child
/// predicates (an empty AND ⇒ `true`, empty OR ⇒ `false`); each leaf is one
/// [`create_predicate`]. This is the memory analogue of building the Filter
/// sub-graph: same gate, evaluated inside the source instead of as operators.
fn create_row_predicate(cond: &Condition, schema: &Schema) -> Result<RowPredicate, BuildError> {
    match cond {
        Condition::Simple(sc) => {
            let pred = create_predicate(sc, schema)?;
            Ok(Rc::new(move |row: &OwnedRow| pred.eval(row)))
        }
        Condition::And { conditions } => {
            let preds = conditions
                .iter()
                .map(|c| create_row_predicate(c, schema))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Rc::new(move |row: &OwnedRow| preds.iter().all(|p| p(row))))
        }
        Condition::Or { conditions } => {
            let preds = conditions
                .iter()
                .map(|c| create_row_predicate(c, schema))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Rc::new(move |row: &OwnedRow| preds.iter().any(|p| p(row))))
        }
        Condition::CorrelatedSubquery(_) => Err(BuildError::Unsupported(
            "correlated subquery in `where` (EXISTS not yet built)",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{
        CorrelatedSubqueryCondition, ExistsOp, Lit, Op, SimpleCondition, ValuePosition,
    };
    use crate::query::table;
    use crate::value::{RelId, Schema, SourceSchema};

    // --- complete_ordering ---------------------------------------------------

    /// PK names keyed by table — a stub source registry for `get_pk`.
    fn pk_of(table: &str) -> Vec<Box<str>> {
        match table {
            "issue" => vec!["id".into()],
            "comment" => vec!["id".into()],
            "member" => vec!["org".into(), "id".into()], // compound PK
            other => panic!("no PK for table {other:?}"),
        }
    }

    fn fields(ast: &Ast) -> Vec<(String, Dir)> {
        ast.order_by
            .iter()
            .map(|op| (op.field().to_string(), op.dir()))
            .collect()
    }

    #[test]
    fn appends_missing_pk_as_asc() {
        let mut ast = table("issue").order_by("created", "desc").build();
        complete_ordering(&mut ast, &pk_of);
        assert_eq!(
            fields(&ast),
            vec![("created".into(), Dir::Desc), ("id".into(), Dir::Asc)]
        );
    }

    #[test]
    fn empty_order_by_becomes_the_pk() {
        let mut ast = table("member").build();
        complete_ordering(&mut ast, &pk_of);
        assert_eq!(
            fields(&ast),
            vec![("org".into(), Dir::Asc), ("id".into(), Dir::Asc)]
        );
    }

    #[test]
    fn present_pk_keeps_its_position_and_direction() {
        // `id` is the PK and is already in order_by (descending) — keep it as-is,
        // append nothing.
        let mut ast = table("issue").order_by("id", "desc").build();
        complete_ordering(&mut ast, &pk_of);
        assert_eq!(fields(&ast), vec![("id".into(), Dir::Desc)]);
    }

    #[test]
    fn compound_pk_appends_only_the_missing_half_in_pk_order() {
        // PK = [org, id]; order_by already has `id` → append only `org`, but in PK
        // order it would come first; addPrimaryKeys appends missing ones AFTER the
        // existing order_by entries (it only ever appends).
        let mut ast = table("member").order_by("id", "asc").build();
        complete_ordering(&mut ast, &pk_of);
        assert_eq!(
            fields(&ast),
            vec![("id".into(), Dir::Asc), ("org".into(), Dir::Asc)]
        );
    }

    #[test]
    fn completes_related_and_where_subqueries_whole_tree() {
        let mut ast = table("issue")
            .order_by("created", "desc")
            .sub_as("comments", |row| {
                table("comment").r#where("issueID", row.col("id"))
            })
            .where_exists(|row| table("comment").r#where("issueID", row.col("id")))
            .build();
        complete_ordering(&mut ast, &pk_of);

        // root
        assert_eq!(
            fields(&ast),
            vec![("created".into(), Dir::Desc), ("id".into(), Dir::Asc)]
        );
        // related[0].subquery (comment) — PK appended
        assert_eq!(
            fields(&ast.related[0].subquery),
            vec![("id".into(), Dir::Asc)]
        );
        // where EXISTS subquery (comment) — PK appended
        match ast.r#where.as_ref().unwrap() {
            Condition::CorrelatedSubquery(c) => {
                assert_eq!(fields(&c.related.subquery), vec![("id".into(), Dir::Asc)]);
            }
            other => panic!("expected EXISTS, got {other:?}"),
        }
    }

    // --- transform_filters ---------------------------------------------------

    fn simple(field: &str) -> Condition {
        Condition::Simple(SimpleCondition {
            op: Op::Eq,
            left: ValuePosition::Column { name: field.into() },
            right: ValuePosition::Literal {
                value: Lit::Bool(true),
            },
        })
    }

    fn exists() -> Condition {
        // A bare correlated-subquery condition (the subquery shape is irrelevant to
        // stripping); built directly since the fluent builder only emits it inside
        // a where.
        Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: crate::ast::CorrelatedSubquery {
                correlation: crate::ast::Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueID".into()],
                },
                subquery: Box::new(Ast::new("comment")),
                system: None,
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        })
    }
    fn exists_alias(alias: &str, table: &str) -> Condition {
        let mut cond = exists();
        if let Condition::CorrelatedSubquery(c) = &mut cond {
            c.related.subquery.table = table.into();
            c.related.subquery.alias = Some(alias.into());
        }
        cond
    }
    fn csq_alias(cond: &Condition) -> &str {
        match cond {
            Condition::CorrelatedSubquery(c) => c
                .related
                .subquery
                .alias
                .as_deref()
                .expect("normalized EXISTS has an alias"),
            other => panic!("expected EXISTS condition, got {other:?}"),
        }
    }

    #[test]
    fn normalize_pipeline_ast_uniquifies_exists_aliases_under_top_level_and_or() {
        let ast = Ast {
            table: "issue".into(),
            r#where: Some(Condition::Or {
                conditions: vec![
                    simple("a"),
                    Condition::And {
                        conditions: vec![
                            exists_alias("comments", "comment"),
                            exists_alias("labels", "label"),
                        ],
                    },
                ],
            }),
            ..Default::default()
        };
        let got = normalize_pipeline_ast(&ast);
        let Some(Condition::Or { conditions }) = got.r#where.as_ref() else {
            panic!("top-level OR preserved");
        };
        let Condition::And { conditions: nested } = &conditions[1] else {
            panic!("nested AND preserved");
        };
        assert_eq!(csq_alias(&nested[0]), "comments_0");
        assert_eq!(csq_alias(&nested[1]), "labels_1");
    }

    #[test]
    fn normalize_pipeline_ast_leaves_bare_top_level_exists_alias_unchanged() {
        let ast = Ast {
            table: "issue".into(),
            r#where: Some(exists_alias("comments", "comment")),
            ..Default::default()
        };
        let got = normalize_pipeline_ast(&ast);
        assert_eq!(got, ast);
    }

    // --- flatten_condition (the `flattened` subset of normalizeAST) -----------

    fn and(conditions: Vec<Condition>) -> Condition {
        Condition::And { conditions }
    }
    fn or(conditions: Vec<Condition>) -> Condition {
        Condition::Or { conditions }
    }

    #[test]
    fn flatten_leaf_is_identity() {
        assert_eq!(flatten_condition(&simple("a")), Some(simple("a")));
        assert_eq!(flatten_condition(&exists()), Some(exists()));
    }

    #[test]
    fn flatten_splices_same_op_one_level() {
        // and[a, and[b, c]] → and[a, b, c]
        let cond = and(vec![simple("a"), and(vec![simple("b"), simple("c")])]);
        assert_eq!(
            flatten_condition(&cond),
            Some(and(vec![simple("a"), simple("b"), simple("c")]))
        );
    }

    #[test]
    fn flatten_keeps_different_op_nested() {
        // and[a, or[b, c]] → and[a, or[b, c]] (or is a different op → stays nested)
        let cond = and(vec![simple("a"), or(vec![simple("b"), simple("c")])]);
        assert_eq!(flatten_condition(&cond), Some(cond));
    }

    #[test]
    fn flatten_unwraps_singleton_regardless_of_operator() {
        // and[a] → a ; or[and[a]] → a (singleton unwrap at both levels)
        assert_eq!(
            flatten_condition(&and(vec![simple("a")])),
            Some(simple("a"))
        );
        assert_eq!(
            flatten_condition(&or(vec![and(vec![simple("a")])])),
            Some(simple("a"))
        );
    }

    #[test]
    fn flatten_drops_empty_conjunctions() {
        assert_eq!(flatten_condition(&and(vec![])), None);
        // and[ or[] ] → an empty child is dropped → and[] → None.
        assert_eq!(flatten_condition(&and(vec![or(vec![])])), None);
    }

    #[test]
    fn flatten_is_one_level_splice_not_a_fixpoint() {
        // The faithful JS quirk (ast.ts:564): the splice maps `flattened` over a
        // same-op child's *children*, so a same-op grandchild surfaced by that map is
        // NOT re-spliced. Linear 3-deep nesting flattens only one level:
        //   and[a, and[b, and[c, d]]] → and[a, b, and[c, d]]  (NOT fully flat).
        let cond = and(vec![
            simple("a"),
            and(vec![simple("b"), and(vec![simple("c"), simple("d")])]),
        ]);
        assert_eq!(
            flatten_condition(&cond),
            Some(and(vec![
                simple("a"),
                simple("b"),
                and(vec![simple("c"), simple("d")]),
            ]))
        );
    }

    #[test]
    fn flatten_matches_the_spec_mixed_example() {
        // flatten: ((a AND b) AND (c AND (d OR (e OR f))))
        //          → (a AND b AND c AND (d OR e OR f))
        let cond = and(vec![
            and(vec![simple("a"), simple("b")]),
            and(vec![
                simple("c"),
                or(vec![simple("d"), or(vec![simple("e"), simple("f")])]),
            ]),
        ]);
        assert_eq!(
            flatten_condition(&cond),
            Some(and(vec![
                simple("a"),
                simple("b"),
                simple("c"),
                or(vec![simple("d"), simple("e"), simple("f")]),
            ]))
        );
    }

    #[test]
    fn normalize_flattens_and_within_and_before_uniquify() {
        // and[ simple(a), and[ EXISTS(comments), EXISTS(labels) ] ]
        //   flatten → and[ simple(a), EXISTS(comments), EXISTS(labels) ]
        //   uniquify (top-level AND) → comments_0, labels_1
        let ast = Ast {
            table: "issue".into(),
            r#where: Some(and(vec![
                simple("a"),
                and(vec![
                    exists_alias("comments", "comment"),
                    exists_alias("labels", "label"),
                ]),
            ])),
            ..Default::default()
        };
        let got = normalize_pipeline_ast(&ast);
        let Some(Condition::And { conditions }) = got.r#where.as_ref() else {
            panic!("flattened to a single top-level AND");
        };
        assert_eq!(conditions.len(), 3, "inner AND spliced inline");
        assert_eq!(csq_alias(&conditions[1]), "comments_0");
        assert_eq!(csq_alias(&conditions[2]), "labels_1");
    }

    #[test]
    fn normalize_drops_a_fully_empty_where() {
        let ast = Ast {
            table: "issue".into(),
            r#where: Some(and(vec![])),
            ..Default::default()
        };
        assert_eq!(normalize_pipeline_ast(&ast).r#where, None);
    }

    #[test]
    fn normalize_unwraps_singleton_and_then_leaves_the_bare_exists_alias() {
        // and[ EXISTS(comments) ] → (flatten) bare EXISTS → (guard) NOT uniquified
        // (alias stays `comments`). This matches the JS *fluent* path (which
        // simplifyCondition-unwraps the singleton before the builder), and is a
        // deliberate, row-invisible divergence from the JS builder on a *raw*
        // singleton AST (which would number it `comments_0`). See normalize_pipeline_ast.
        let ast = Ast {
            table: "issue".into(),
            r#where: Some(and(vec![exists_alias("comments", "comment")])),
            ..Default::default()
        };
        let got = normalize_pipeline_ast(&ast);
        assert_eq!(csq_alias(got.r#where.as_ref().unwrap()), "comments");
    }

    #[test]
    fn none_and_simple_pass_through() {
        assert_eq!(transform_filters(None), (None, false));
        let (t, r) = transform_filters(Some(&simple("a")));
        assert_eq!((t, r), (Some(simple("a")), false));
    }

    #[test]
    fn bare_subquery_is_removed() {
        let (t, r) = transform_filters(Some(&exists()));
        assert_eq!((t, r), (None, true));
    }

    #[test]
    fn and_drops_subquery_branches_keeps_simples() {
        let cond = Condition::And {
            conditions: vec![simple("a"), exists(), simple("b")],
        };
        let (t, r) = transform_filters(Some(&cond));
        assert_eq!(
            (t, r),
            (
                Some(Condition::And {
                    conditions: vec![simple("a"), simple("b")]
                }),
                true
            )
        );
    }

    #[test]
    fn or_with_a_removed_branch_collapses_entirely() {
        // An OR branch that vanishes makes the whole OR unsound to keep.
        let cond = Condition::Or {
            conditions: vec![simple("a"), exists()],
        };
        assert_eq!(transform_filters(Some(&cond)), (None, true));
    }

    #[test]
    fn or_of_simples_is_unchanged() {
        let cond = Condition::Or {
            conditions: vec![simple("a"), simple("b")],
        };
        let (t, r) = transform_filters(Some(&cond));
        assert_eq!((t, r), (Some(cond), false));
    }

    #[test]
    fn nested_and_inside_and_strips_subqueries() {
        // and[ simple(a), and[ exists, simple(b) ] ] → and[ simple(a), and[ simple(b) ] ]
        let cond = Condition::And {
            conditions: vec![
                simple("a"),
                Condition::And {
                    conditions: vec![exists(), simple("b")],
                },
            ],
        };
        let (t, r) = transform_filters(Some(&cond));
        assert_eq!(
            (t, r),
            (
                Some(Condition::And {
                    conditions: vec![
                        simple("a"),
                        Condition::And {
                            conditions: vec![simple("b")]
                        }
                    ]
                }),
                true
            )
        );
    }

    // --- schema_primary_key_names --------------------------------------------

    #[test]
    fn primary_key_names_from_schema() {
        // columns id=0, org=1, name=2; PK = [org, id] (ColIds 1, 0) in PK order.
        let schema = Schema::new(
            vec!["id", "org", "name"],
            vec![1, 0],
            vec![(1, true), (0, true)],
        );
        assert_eq!(
            schema_primary_key_names(&schema),
            vec!["org".into(), "id".into()] as Vec<Box<str>>
        );
    }

    // --- create_predicate ----------------------------------------------------

    /// columns: id=0 (Int PK), name=1 (Str), age=2 (Int).
    fn t_schema() -> Schema {
        Schema::new(vec!["id", "name", "age"], vec![0], vec![(0, true)])
    }

    fn t_row(id: i64, name: &str, age: i64) -> crate::value::OwnedRow {
        owned_row(vec![
            OwnedValue::Int(id),
            OwnedValue::str(name),
            OwnedValue::Int(age),
        ])
    }

    fn colp(name: &str) -> ValuePosition {
        ValuePosition::Column { name: name.into() }
    }
    fn litp(value: Lit) -> ValuePosition {
        ValuePosition::Literal { value }
    }
    fn sc(op: Op, left: ValuePosition, right: ValuePosition) -> SimpleCondition {
        SimpleCondition { op, left, right }
    }

    // --- extract_push_guard (design 205 §Testing.1) --------------------------

    fn gsimp(op: Op, col: &str, rhs: Lit) -> Condition {
        Condition::Simple(sc(op, colp(col), litp(rhs)))
    }
    /// Assert the guard's column and that its values equal `vals` as a set under
    /// predicate identity (union/dedup do not preserve a fixed order).
    fn assert_guard(g: Option<PushGuard>, col: ColId, vals: &[OwnedValue]) {
        let g = g.expect("expected a guard");
        assert_eq!(g.col, col, "guard column");
        assert_eq!(g.values.len(), vals.len(), "value count: {:?}", g.values);
        for want in vals {
            assert!(
                g.values
                    .iter()
                    .any(|got| values_identical(got.as_ref(), want.as_ref())),
                "missing guard value {want:?} in {:?}",
                g.values
            );
        }
    }

    #[test]
    fn guard_eq_column_literal() {
        let s = t_schema();
        // age = 30  ->  {2: [30]}  (Number(30.0) coerces to Int(30), like the predicate)
        assert_guard(
            extract_push_guard(&gsimp(Op::Eq, "age", Lit::Number(30.0)), &s),
            2,
            &[OwnedValue::Int(30)],
        );
    }

    #[test]
    fn guard_eq_null_is_never_matching() {
        let s = t_schema();
        // name = NULL folds to false -> empty guard (indexed nowhere).
        let g = extract_push_guard(&gsimp(Op::Eq, "name", Lit::Null), &s).unwrap();
        assert_eq!(g.col, 1);
        assert!(g.values.is_empty());
    }

    #[test]
    fn guard_is_null_matches_null_cell() {
        let s = t_schema();
        // name IS NULL -> {1: [Null]} (NULL-aware identity, not never-matching).
        assert_guard(
            extract_push_guard(&gsimp(Op::Is, "name", Lit::Null), &s),
            1,
            &[OwnedValue::Null],
        );
    }

    #[test]
    fn guard_is_value() {
        let s = t_schema();
        assert_guard(
            extract_push_guard(&gsimp(Op::Is, "name", Lit::Str("x".into())), &s),
            1,
            &[OwnedValue::str("x")],
        );
    }

    #[test]
    fn guard_in_list_deduped() {
        let s = t_schema();
        // id IN (1, 2, 1) -> {0: [1, 2]}  (dedup under predicate identity)
        let list = Lit::Array(vec![Lit::Number(1.0), Lit::Number(2.0), Lit::Number(1.0)]);
        assert_guard(
            extract_push_guard(&gsimp(Op::In, "id", list), &s),
            0,
            &[OwnedValue::Int(1), OwnedValue::Int(2)],
        );
    }

    #[test]
    fn guard_in_int_float_dedup() {
        let s = t_schema();
        // IN (1, 1.0): the two literals are predicate-identical -> one bucket.
        let list = Lit::Array(vec![Lit::Number(1.0), Lit::Number(1.0)]);
        let g = extract_push_guard(&gsimp(Op::In, "id", list), &s).unwrap();
        assert_eq!(g.values.len(), 1);
    }

    #[test]
    fn guard_empty_in_is_never_matching() {
        let s = t_schema();
        let g = extract_push_guard(&gsimp(Op::In, "id", Lit::Array(vec![])), &s).unwrap();
        assert!(g.values.is_empty());
    }

    #[test]
    fn no_guard_for_unguardable_ops() {
        let s = t_schema();
        for cond in [
            gsimp(Op::Ne, "age", Lit::Number(1.0)),
            gsimp(Op::IsNot, "name", Lit::Null),
            gsimp(Op::Lt, "age", Lit::Number(1.0)),
            gsimp(Op::Ge, "age", Lit::Number(1.0)),
            gsimp(Op::Like, "name", Lit::Str("a%".into())),
            gsimp(Op::ILike, "name", Lit::Str("a%".into())),
            gsimp(Op::NotIn, "id", Lit::Array(vec![Lit::Number(1.0)])),
        ] {
            assert!(
                extract_push_guard(&cond, &s).is_none(),
                "expected None for {cond:?}"
            );
        }
    }

    #[test]
    fn no_guard_for_literal_lhs_or_column_rhs() {
        let s = t_schema();
        // literal = literal (constant fold) -> None
        let lit_lhs = Condition::Simple(sc(Op::Eq, litp(Lit::Number(1.0)), litp(Lit::Number(1.0))));
        assert!(extract_push_guard(&lit_lhs, &s).is_none());
        // column = column -> None
        let col_rhs = Condition::Simple(sc(Op::Eq, colp("id"), colp("age")));
        assert!(extract_push_guard(&col_rhs, &s).is_none());
    }

    #[test]
    fn no_guard_for_unknown_column() {
        let s = t_schema();
        assert!(extract_push_guard(&gsimp(Op::Eq, "missing", Lit::Number(1.0)), &s).is_none());
    }

    #[test]
    fn and_picks_smallest_value_set() {
        let s = t_schema();
        // (id IN (1,2,3)) AND (age = 30) -> the age=30 child (1 value < 3).
        let cond = Condition::And {
            conditions: vec![
                gsimp(
                    Op::In,
                    "id",
                    Lit::Array(vec![Lit::Number(1.0), Lit::Number(2.0), Lit::Number(3.0)]),
                ),
                gsimp(Op::Eq, "age", Lit::Number(30.0)),
            ],
        };
        assert_guard(extract_push_guard(&cond, &s), 2, &[OwnedValue::Int(30)]);
    }

    #[test]
    fn and_ignores_unguardable_child() {
        let s = t_schema();
        // (age = 30) AND (name LIKE 'a%') -> guard on age (LIKE child yields None).
        let cond = Condition::And {
            conditions: vec![
                gsimp(Op::Eq, "age", Lit::Number(30.0)),
                gsimp(Op::Like, "name", Lit::Str("a%".into())),
            ],
        };
        assert_guard(extract_push_guard(&cond, &s), 2, &[OwnedValue::Int(30)]);
    }

    #[test]
    fn and_all_unguardable_is_none() {
        let s = t_schema();
        let cond = Condition::And {
            conditions: vec![
                gsimp(Op::Lt, "age", Lit::Number(30.0)),
                gsimp(Op::Like, "name", Lit::Str("a%".into())),
            ],
        };
        assert!(extract_push_guard(&cond, &s).is_none());
    }

    #[test]
    fn or_same_column_unions() {
        let s = t_schema();
        // id = 1 OR id = 2 -> {0: [1, 2]}
        let cond = Condition::Or {
            conditions: vec![
                gsimp(Op::Eq, "id", Lit::Number(1.0)),
                gsimp(Op::Eq, "id", Lit::Number(2.0)),
            ],
        };
        assert_guard(
            extract_push_guard(&cond, &s),
            0,
            &[OwnedValue::Int(1), OwnedValue::Int(2)],
        );
    }

    #[test]
    fn or_different_columns_is_none() {
        let s = t_schema();
        let cond = Condition::Or {
            conditions: vec![
                gsimp(Op::Eq, "id", Lit::Number(1.0)),
                gsimp(Op::Eq, "age", Lit::Number(2.0)),
            ],
        };
        assert!(extract_push_guard(&cond, &s).is_none());
    }

    #[test]
    fn or_with_unguardable_branch_is_none() {
        let s = t_schema();
        let cond = Condition::Or {
            conditions: vec![
                gsimp(Op::Eq, "id", Lit::Number(1.0)),
                gsimp(Op::Like, "name", Lit::Str("a%".into())),
            ],
        };
        assert!(extract_push_guard(&cond, &s).is_none());
    }

    #[test]
    fn or_drops_never_matching_branch() {
        let s = t_schema();
        // id = 1 OR id = NULL  ->  {0: [1]}  (id = NULL is a false disjunct)
        let cond = Condition::Or {
            conditions: vec![
                gsimp(Op::Eq, "id", Lit::Number(1.0)),
                gsimp(Op::Eq, "id", Lit::Null),
            ],
        };
        assert_guard(extract_push_guard(&cond, &s), 0, &[OwnedValue::Int(1)]);
    }

    #[test]
    fn or_of_and_unions_same_column() {
        let s = t_schema();
        // (id = 1 AND age = 5) OR (id = 2): the AND picks its first smallest (id=1,
        // col 0); the other branch is id=2 (col 0) -> union {1, 2} on col 0.
        let cond = Condition::Or {
            conditions: vec![
                Condition::And {
                    conditions: vec![
                        gsimp(Op::Eq, "id", Lit::Number(1.0)),
                        gsimp(Op::Eq, "age", Lit::Number(5.0)),
                    ],
                },
                gsimp(Op::Eq, "id", Lit::Number(2.0)),
            ],
        };
        assert_guard(
            extract_push_guard(&cond, &s),
            0,
            &[OwnedValue::Int(1), OwnedValue::Int(2)],
        );
    }

    #[test]
    fn eq_resolves_column_and_coerces_integral_number() {
        // `age = 30`: Number(30.0) coerces to Int(30); matches an Int(30) cell.
        let s = t_schema();
        let p = create_predicate(&sc(Op::Eq, colp("age"), litp(Lit::Number(30.0))), &s).unwrap();
        assert!(matches!(
            &p,
            CompiledPredicate::Cmp {
                col: 2,
                op: CmpOp::Eq,
                value: OwnedValue::Int(30)
            }
        ));
        assert!(p.eval(&t_row(1, "a", 30)));
        assert!(!p.eval(&t_row(1, "a", 31)));
    }

    #[test]
    fn non_integral_number_stays_float() {
        let s = t_schema();
        let p = create_predicate(&sc(Op::Eq, colp("age"), litp(Lit::Number(30.5))), &s).unwrap();
        assert!(matches!(
            &p,
            CompiledPredicate::Cmp {
                value: OwnedValue::Float(_),
                ..
            }
        ));
    }

    #[test]
    fn null_rhs_folds_to_const_false() {
        // `name = null` is UNKNOWN for every row → drop (use IS NULL instead).
        let s = t_schema();
        let p = create_predicate(&sc(Op::Eq, colp("name"), litp(Lit::Null)), &s).unwrap();
        assert!(matches!(p, CompiledPredicate::Const(false)));
    }

    #[test]
    fn is_null_and_is_not_null() {
        let s = t_schema();
        let isn = create_predicate(&sc(Op::Is, colp("name"), litp(Lit::Null)), &s).unwrap();
        assert!(matches!(
            isn,
            CompiledPredicate::IsNull {
                col: 1,
                negated: false
            }
        ));
        let isnn = create_predicate(&sc(Op::IsNot, colp("name"), litp(Lit::Null)), &s).unwrap();
        assert!(matches!(
            isnn,
            CompiledPredicate::IsNull {
                col: 1,
                negated: true
            }
        ));
    }

    #[test]
    fn is_against_non_null_uses_identity() {
        let s = t_schema();
        let p =
            create_predicate(&sc(Op::Is, colp("name"), litp(Lit::Str("a".into()))), &s).unwrap();
        assert!(matches!(
            &p,
            CompiledPredicate::Is {
                col: 1,
                negated: false,
                ..
            }
        ));
        assert!(p.eval(&t_row(1, "a", 0)));
        assert!(!p.eval(&t_row(1, "b", 0)));

        let np =
            create_predicate(&sc(Op::IsNot, colp("name"), litp(Lit::Str("a".into()))), &s).unwrap();
        assert!(!np.eval(&t_row(1, "a", 0)));
        assert!(np.eval(&t_row(1, "b", 0)));
    }

    #[test]
    fn unknown_column_errors() {
        let s = t_schema();
        let r = create_predicate(&sc(Op::Eq, colp("missing"), litp(Lit::Number(1.0))), &s);
        assert_eq!(r.err(), Some(BuildError::UnknownColumn("missing".into())));
    }

    #[test]
    fn in_and_not_in() {
        let s = t_schema();
        let set = Lit::Array(vec![Lit::Number(1.0), Lit::Number(3.0)]);
        let p = create_predicate(&sc(Op::In, colp("id"), litp(set.clone())), &s).unwrap();
        assert!(matches!(
            &p,
            CompiledPredicate::In {
                col: 0,
                negated: false,
                ..
            }
        ));
        assert!(p.eval(&t_row(1, "a", 0)));
        assert!(!p.eval(&t_row(2, "a", 0)));

        let np = create_predicate(&sc(Op::NotIn, colp("id"), litp(set)), &s).unwrap();
        assert!(np.eval(&t_row(2, "a", 0)));
        assert!(!np.eval(&t_row(3, "a", 0)));
    }

    #[test]
    fn in_requires_array() {
        let s = t_schema();
        let r = create_predicate(&sc(Op::In, colp("id"), litp(Lit::Number(1.0))), &s);
        assert!(matches!(r, Err(BuildError::Invalid(_))));
    }

    #[test]
    fn like_and_not_like() {
        let s = t_schema();
        let p =
            create_predicate(&sc(Op::Like, colp("name"), litp(Lit::Str("a%".into()))), &s).unwrap();
        assert!(matches!(
            &p,
            CompiledPredicate::Like {
                col: 1,
                negated: false,
                ..
            }
        ));
        assert!(p.eval(&t_row(1, "abc", 0)));
        assert!(!p.eval(&t_row(1, "xyz", 0)));

        let np = create_predicate(
            &sc(Op::NotLike, colp("name"), litp(Lit::Str("a%".into()))),
            &s,
        )
        .unwrap();
        assert!(np.eval(&t_row(1, "xyz", 0)));
        assert!(!np.eval(&t_row(1, "abc", 0)));
    }

    #[test]
    fn ilike_and_not_ilike() {
        let s = t_schema();
        let p = create_predicate(
            &sc(Op::ILike, colp("name"), litp(Lit::Str("a%".into()))),
            &s,
        )
        .unwrap();
        assert!(p.eval(&t_row(1, "abc", 0)));
        assert!(p.eval(&t_row(1, "ABC", 0)));
        assert!(!p.eval(&t_row(1, "xbc", 0)));

        let np = create_predicate(
            &sc(Op::NotILike, colp("name"), litp(Lit::Str("a%".into()))),
            &s,
        )
        .unwrap();
        assert!(!np.eval(&t_row(1, "ABC", 0)));
        assert!(np.eval(&t_row(1, "xbc", 0)));
    }

    #[test]
    fn ordering_uses_compare_semantics() {
        let s = t_schema();
        let p = create_predicate(&sc(Op::Gt, colp("age"), litp(Lit::Number(18.0))), &s).unwrap();
        assert!(p.eval(&t_row(1, "a", 21)));
        assert!(!p.eval(&t_row(1, "a", 18)));
        assert!(!p.eval(&t_row(1, "a", 10)));
    }

    #[test]
    fn null_cell_drops_for_every_op() {
        // A null cell is UNKNOWN → dropped, even for the negated ops (parity with
        // JS createPredicate's LHS null guard).
        let s = t_schema();
        let ne =
            create_predicate(&sc(Op::Ne, colp("name"), litp(Lit::Str("x".into()))), &s).unwrap();
        let null_name = owned_row(vec![
            OwnedValue::Int(1),
            OwnedValue::Null,
            OwnedValue::Int(0),
        ]);
        assert!(!ne.eval(&null_name)); // null != "x" → UNKNOWN → drop
    }

    #[test]
    fn literal_lhs_folds_to_const() {
        let s = t_schema();
        // `5 = 5` → true; `5 = 6` → false.
        let t = create_predicate(
            &sc(Op::Eq, litp(Lit::Number(5.0)), litp(Lit::Number(5.0))),
            &s,
        )
        .unwrap();
        assert!(matches!(t, CompiledPredicate::Const(true)));
        let f = create_predicate(
            &sc(Op::Eq, litp(Lit::Number(5.0)), litp(Lit::Number(6.0))),
            &s,
        )
        .unwrap();
        assert!(matches!(f, CompiledPredicate::Const(false)));
        // `5 < 6` → true (same numeric class folds via compare).
        let lt = create_predicate(
            &sc(Op::Lt, litp(Lit::Number(5.0)), litp(Lit::Number(6.0))),
            &s,
        )
        .unwrap();
        assert!(matches!(lt, CompiledPredicate::Const(true)));
        // `'a' IN ['a','b']` → true.
        let inset = Lit::Array(vec![Lit::Str("a".into()), Lit::Str("b".into())]);
        let m = create_predicate(&sc(Op::In, litp(Lit::Str("a".into())), litp(inset)), &s).unwrap();
        assert!(matches!(m, CompiledPredicate::Const(true)));
        // null literal LHS → false for non-IS ops.
        let n = create_predicate(&sc(Op::Eq, litp(Lit::Null), litp(Lit::Number(5.0))), &s).unwrap();
        assert!(matches!(n, CompiledPredicate::Const(false)));
    }

    #[test]
    fn literal_lhs_is_null_folds() {
        let s = t_schema();
        let yes = create_predicate(&sc(Op::Is, litp(Lit::Null), litp(Lit::Null)), &s).unwrap();
        assert!(matches!(yes, CompiledPredicate::Const(true))); // null IS null
        let no = create_predicate(&sc(Op::IsNot, litp(Lit::Null), litp(Lit::Null)), &s).unwrap();
        assert!(matches!(no, CompiledPredicate::Const(false))); // null IS NOT null
        let nn =
            create_predicate(&sc(Op::IsNot, litp(Lit::Number(5.0)), litp(Lit::Null)), &s).unwrap();
        assert!(matches!(nn, CompiledPredicate::Const(true))); // 5 IS NOT null
    }

    #[test]
    fn mismatched_ordering_const_is_invalid() {
        // `5 < 5.5` would need Int/Float coercion in compare (deferred) → build
        // error instead of a panic.
        let s = t_schema();
        let r = create_predicate(
            &sc(Op::Lt, litp(Lit::Number(5.0)), litp(Lit::Number(5.5))),
            &s,
        );
        assert!(matches!(r, Err(BuildError::Invalid(_))));
    }

    #[test]
    fn column_on_right_is_invalid() {
        let s = t_schema();
        let r = create_predicate(&sc(Op::Eq, colp("id"), colp("age")), &s);
        assert!(matches!(r, Err(BuildError::Invalid(_))));
    }

    #[test]
    fn bool_vs_number_ordering_const_is_invalid() {
        // `5 < true` — Bool and Int are different storage classes; guarded to a
        // build error rather than a compare_values panic.
        let s = t_schema();
        let r = create_predicate(
            &sc(Op::Lt, litp(Lit::Number(5.0)), litp(Lit::Bool(true))),
            &s,
        );
        assert!(matches!(r, Err(BuildError::Invalid(_))));
    }

    #[test]
    fn in_list_with_null_element_drops_null_cell() {
        // `id IN [1, null]`: a null in the set is harmless — a null CELL is
        // UNKNOWN and dropped before membership is checked (parity with JS's
        // lhs-null guard), and a non-null cell matches only the real entries.
        let s = t_schema();
        let list = Lit::Array(vec![Lit::Number(1.0), Lit::Null]);
        let p = create_predicate(&sc(Op::In, colp("id"), litp(list)), &s).unwrap();
        assert!(p.eval(&t_row(1, "a", 0))); // 1 ∈ {1, null}
        assert!(!p.eval(&t_row(2, "a", 0))); // 2 ∉ {1, null}
        let null_id = owned_row(vec![
            OwnedValue::Null,
            OwnedValue::str("a"),
            OwnedValue::Int(0),
        ]);
        assert!(!p.eval(&null_id)); // null cell → UNKNOWN → drop
    }

    #[test]
    fn literal_lhs_like_folds_to_const() {
        // `'abc' LIKE 'a%'` → true; `'xyz' LIKE 'a%'` → false (folded at build).
        let s = t_schema();
        let yes = create_predicate(
            &sc(
                Op::Like,
                litp(Lit::Str("abc".into())),
                litp(Lit::Str("a%".into())),
            ),
            &s,
        )
        .unwrap();
        assert!(matches!(yes, CompiledPredicate::Const(true)));
        let no = create_predicate(
            &sc(
                Op::Like,
                litp(Lit::Str("xyz".into())),
                litp(Lit::Str("a%".into())),
            ),
            &s,
        )
        .unwrap();
        assert!(matches!(no, CompiledPredicate::Const(false)));
    }

    // --- build_pipeline ------------------------------------------------------

    /// issue(id=0, priority=1) source schema. A source carries no relationships
    /// (the `comments` slot is query-derived); a view that materializes `comments`
    /// builds its own `Schema` from [`issue_view_schema`].
    fn issue_schema() -> SourceSchema {
        SourceSchema::new(vec!["id", "priority"], vec![0], vec![(0, true)])
    }
    /// comment(id=0, issueID=1) source schema.
    fn comment_schema() -> SourceSchema {
        SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
    }
    /// issue view schema declaring the `comments` relationship (its leaf child
    /// schema lets the production `View` sort/build the materialized comments).
    fn issue_view_schema() -> Schema {
        Schema::new(vec!["id", "priority"], vec![0], vec![(0, true)]).with_relationships(vec![
            crate::value::RelDef::related("comments", comment_schema().into_schema()),
        ])
    }
    fn order_id() -> Vec<OrderPart> {
        vec![OrderPart("id".into(), Dir::Asc)]
    }
    fn irow(a: i64, b: i64) -> crate::value::OwnedRow {
        owned_row(vec![OwnedValue::Int(a), OwnedValue::Int(b)])
    }
    /// Column-0 (`id`) of a caught/owned row as `i64` — for asserting nested trees.
    fn col0(r: &OwnedRow) -> i64 {
        match r.col(0) {
            crate::value::Value::Int(i) => i,
            other => panic!("expected Int in col 0, got {other:?}"),
        }
    }
    fn related_csq(alias: &str, table: &str, parent_f: &str, child_f: &str) -> CorrelatedSubquery {
        related_csq2(alias, table, &[parent_f], &[child_f])
    }
    fn related_csq2(
        alias: &str,
        table: &str,
        parent_f: &[&str],
        child_f: &[&str],
    ) -> CorrelatedSubquery {
        CorrelatedSubquery {
            correlation: crate::ast::Correlation {
                parent_field: parent_f.iter().map(|s| (*s).into()).collect(),
                child_field: child_f.iter().map(|s| (*s).into()).collect(),
            },
            subquery: Box::new(Ast {
                table: table.into(),
                alias: Some(alias.into()),
                // No explicit order — resolve_sort completes it from the child's PK
                // (comment PK is `id`, so this matches the prior `order_by("id")`).
                ..Default::default()
            }),
            system: None,
        }
    }
    /// issue ⋈ comments (issue.id = comment.issueID).
    fn issue_with_comments() -> Ast {
        Ast {
            table: "issue".into(),
            order_by: order_id(),
            related: vec![related_csq("comments", "comment", "id", "issueID")],
            ..Default::default()
        }
    }

    #[test]
    fn pipeline_source_where_filters_on_fetch() {
        let mut g = Graph::new();
        let schema = issue_schema();
        let issue = g.add_source(schema.clone(), vec![irow(1, 5), irow(2, 1), irow(3, 9)]);
        let resolve = |t: &str| (t == "issue").then(|| (issue, schema.clone()));
        let ast = Ast {
            table: "issue".into(),
            r#where: Some(Condition::Simple(sc(
                Op::Gt,
                colp("priority"),
                litp(Lit::Number(2.0)),
            ))),
            order_by: order_id(),
            ..Default::default()
        };
        let top = build_pipeline(&mut g, &ast, &resolve).unwrap();
        let view = g.add_view(top, schema.clone().into_schema());
        g.hydrate(view);
        // priority > 2 keeps id 1 (5) and 3 (9), drops id 2 (1); sorted by id.
        assert_eq!(
            g.dump_view_rows(view),
            vec![(vec![1, 5], vec![]), (vec![3, 9], vec![])]
        );
    }

    // --- projection (PROJECTION-SUPPORT-DESIGN.md §3, §6) --------------------

    #[test]
    fn view_schema_lowers_select_to_projection() {
        let src = issue_schema();
        let resolve = |t: &str| (t == "issue").then(|| (NodeId::new(0, 0), src.clone()));

        // `'*'` → no projection.
        let star = table("issue").order_by("id", "asc").build();
        let s = view_schema(&star, &resolve).unwrap();
        assert_eq!(s.projection, None);
        assert_eq!(s.columns.len(), 2); // columns stay full width

        // `.select("priority")` → projection over base ColId 1, columns still full.
        let proj = table("issue")
            .select("priority")
            .order_by("id", "asc")
            .build();
        let s = view_schema(&proj, &resolve).unwrap();
        assert_eq!(s.projection, Some(vec![1]));
        assert_eq!(s.columns.len(), 2);

        // Selection order is preserved; an unknown column is a build error.
        let proj2 = table("issue").select("priority").select("id").build();
        let s = view_schema(&proj2, &resolve).unwrap();
        assert_eq!(s.projection, Some(vec![1, 0]));
        let bad = table("issue").select("nope").build();
        assert!(matches!(
            view_schema(&bad, &resolve),
            Err(BuildError::UnknownColumn(_))
        ));
    }

    #[test]
    fn required_cols_unions_select_where_and_sort() {
        let schema = issue_schema().into_schema(); // cols: id=0, priority=1
                                                   // select id; where priority > 2; order by id → required = {id, priority}.
        let ast = table("issue")
            .select("id")
            .where_op("priority", ">", 2)
            .order_by("id", "asc")
            .build();
        let sort = resolve_sort(&ast.order_by, &schema).unwrap();
        let mut req = required_cols(&ast, &schema, &sort, &[]).unwrap();
        req.sort_unstable();
        assert_eq!(req, vec![0, 1]);

        // `'*'`-style required (select all) is every column.
        let star = table("issue").order_by("id", "asc").build();
        let sort = resolve_sort(&star.order_by, &schema).unwrap();
        let mut req = required_cols(&star, &schema, &sort, &[]).unwrap();
        req.sort_unstable();
        assert_eq!(req, vec![0, 1]);
    }

    #[test]
    fn projection_presence_predicate_drops_partial_rows() {
        // A projected query drops a shared row whose *required* column is `Absent`,
        // while keeping rows that carry it (§3.3). Row id=2 has an Absent `priority`.
        let mut g = Graph::new();
        let schema = issue_schema();
        let issue = g.add_source(
            schema.clone(),
            vec![
                owned_row(vec![OwnedValue::Int(1), OwnedValue::Int(5)]),
                owned_row(vec![OwnedValue::Int(2), OwnedValue::Absent]),
                owned_row(vec![OwnedValue::Int(3), OwnedValue::Int(9)]),
            ],
        );
        let resolve = |t: &str| (t == "issue").then(|| (issue, schema.clone()));
        let ast = table("issue")
            .select("priority")
            .order_by("id", "asc")
            .build();
        let top = build_pipeline(&mut g, &ast, &resolve).unwrap();
        let view = g.add_view(top, view_schema(&ast, &resolve).unwrap());
        g.hydrate(view);
        // id=2 is dropped (its required `priority` is Absent); id=1 and id=3 remain.
        assert_eq!(
            g.dump_view_rows(view),
            vec![(vec![1, 5], vec![]), (vec![3, 9], vec![])]
        );
    }

    #[test]
    fn star_query_keeps_full_rows_unchanged() {
        // A `'*'` query installs no presence predicate (§7): behavior is identical to
        // before projection existed — every full row surfaces.
        let mut g = Graph::new();
        let schema = issue_schema();
        let issue = g.add_source(schema.clone(), vec![irow(1, 5), irow(2, 1), irow(3, 9)]);
        let resolve = |t: &str| (t == "issue").then(|| (issue, schema.clone()));
        let ast = table("issue").order_by("id", "asc").build();
        let top = build_pipeline(&mut g, &ast, &resolve).unwrap();
        let view = g.add_view(top, view_schema(&ast, &resolve).unwrap());
        g.hydrate(view);
        assert_eq!(
            g.dump_view_rows(view),
            vec![
                (vec![1, 5], vec![]),
                (vec![2, 1], vec![]),
                (vec![3, 9], vec![])
            ]
        );
    }

    #[test]
    fn pipeline_source_where_and_or() {
        let mut g = Graph::new();
        let schema = issue_schema();
        let issue = g.add_source(schema.clone(), vec![irow(1, 5), irow(2, 1), irow(3, 9)]);
        let resolve = |t: &str| (t == "issue").then(|| (issue, schema.clone()));

        // (priority > 2) AND (id < 3) → only id 1.
        let and = Condition::And {
            conditions: vec![
                Condition::Simple(sc(Op::Gt, colp("priority"), litp(Lit::Number(2.0)))),
                Condition::Simple(sc(Op::Lt, colp("id"), litp(Lit::Number(3.0)))),
            ],
        };
        let ast = Ast {
            table: "issue".into(),
            r#where: Some(and),
            order_by: order_id(),
            ..Default::default()
        };
        let top = build_pipeline(&mut g, &ast, &resolve).unwrap();
        let v1 = g.add_view(top, schema.clone().into_schema());
        g.hydrate(v1);
        assert_eq!(g.dump_view_rows(v1), vec![(vec![1, 5], vec![])]);

        // (priority > 8) OR (id = 2) → id 2 and id 3.
        let or = Condition::Or {
            conditions: vec![
                Condition::Simple(sc(Op::Gt, colp("priority"), litp(Lit::Number(8.0)))),
                Condition::Simple(sc(Op::Eq, colp("id"), litp(Lit::Number(2.0)))),
            ],
        };
        let ast2 = Ast {
            table: "issue".into(),
            r#where: Some(or),
            order_by: order_id(),
            ..Default::default()
        };
        let top2 = build_pipeline(&mut g, &ast2, &resolve).unwrap();
        let v2 = g.add_view(top2, schema.clone().into_schema());
        g.hydrate(v2);
        assert_eq!(
            g.dump_view_rows(v2),
            vec![(vec![2, 1], vec![]), (vec![3, 9], vec![])]
        );
    }

    #[test]
    fn pipeline_source_where_push_gates() {
        let mut g = Graph::new();
        let schema = issue_schema();
        let issue = g.add_source(schema.clone(), vec![irow(1, 5), irow(2, 1), irow(3, 9)]);
        let resolve = |t: &str| (t == "issue").then(|| (issue, schema.clone()));
        let ast = Ast {
            table: "issue".into(),
            r#where: Some(Condition::Simple(sc(
                Op::Gt,
                colp("priority"),
                litp(Lit::Number(2.0)),
            ))),
            order_by: order_id(),
            ..Default::default()
        };
        let top = build_pipeline(&mut g, &ast, &resolve).unwrap();
        let view = g.add_view(top, schema.clone().into_schema());
        g.set_sink_edge(top, view);
        g.hydrate(view);
        assert_eq!(
            g.dump_view_rows(view),
            vec![(vec![1, 5], vec![]), (vec![3, 9], vec![])]
        );

        // A passing add (priority 7 > 2) appears; a filtered add (priority 1) does not.
        g.source_push(issue, crate::change::SourceChange::Add(irow(4, 7)));
        g.source_push(issue, crate::change::SourceChange::Add(irow(5, 1)));
        assert_eq!(
            g.dump_view_rows(view),
            vec![
                (vec![1, 5], vec![]),
                (vec![3, 9], vec![]),
                (vec![4, 7], vec![])
            ]
        );
    }

    #[test]
    fn pipeline_single_relationship_join_on_fetch() {
        let mut g = Graph::new();
        let issue_s = issue_schema();
        let comment_s = comment_schema();
        let issue = g.add_source(issue_s.clone(), vec![irow(1, 5), irow(2, 1)]);
        let comment = g.add_source(
            comment_s.clone(),
            vec![irow(10, 1), irow(11, 1), irow(12, 2)],
        );
        let resolve = |t: &str| match t {
            "issue" => Some((issue, issue_s.clone())),
            "comment" => Some((comment, comment_s.clone())),
            _ => None,
        };
        let top = build_pipeline(&mut g, &issue_with_comments(), &resolve).unwrap();
        let view = g.add_view(top, issue_view_schema());
        g.hydrate(view);
        assert_eq!(
            g.dump_view_rows(view),
            vec![
                (vec![1, 5], vec![vec![10, 1], vec![11, 1]]),
                (vec![2, 1], vec![vec![12, 2]]),
            ]
        );
    }

    #[test]
    fn pipeline_join_push_adds_child() {
        let mut g = Graph::new();
        let issue_s = issue_schema();
        let comment_s = comment_schema();
        let issue = g.add_source(issue_s.clone(), vec![irow(1, 5), irow(2, 1)]);
        let comment = g.add_source(
            comment_s.clone(),
            vec![irow(10, 1), irow(11, 1), irow(12, 2)],
        );
        let resolve = |t: &str| match t {
            "issue" => Some((issue, issue_s.clone())),
            "comment" => Some((comment, comment_s.clone())),
            _ => None,
        };
        let top = build_pipeline(&mut g, &issue_with_comments(), &resolve).unwrap();
        let view = g.add_view(top, issue_view_schema());
        g.set_sink_edge(top, view);
        g.hydrate(view);

        // A new comment for issue 2 lands as a child via the join's child port.
        g.source_push(comment, crate::change::SourceChange::Add(irow(13, 2)));
        assert_eq!(
            g.dump_view_rows(view),
            vec![
                (vec![1, 5], vec![vec![10, 1], vec![11, 1]]),
                (vec![2, 1], vec![vec![12, 2], vec![13, 2]]),
            ]
        );
    }

    #[test]
    fn pipeline_lowers_start() {
        // `start_after id 2` → a Skip over the connection. The bound is pushed into
        // the source's fetch start (hydrate drops ids 1,2) and gates pushes (an add
        // before the bound is dropped, one after is kept).
        let mut g = Graph::new();
        let schema = issue_schema(); // (id, priority); PK id
        let issue = g.add_source(
            schema.clone(),
            vec![irow(1, 5), irow(2, 1), irow(3, 9), irow(4, 2)],
        );
        let resolve = |t: &str| (t == "issue").then(|| (issue, schema.clone()));
        let ast = table("issue").start_after("id", 2).build();

        let top = build_pipeline(&mut g, &ast, &resolve).unwrap();
        let view = g.add_view(top, schema.clone().into_schema());
        g.set_sink_edge(top, view);
        g.hydrate(view);
        // ids after 2 survive; sorted by the PK-completed sort.
        assert_eq!(
            g.dump_view_rows(view),
            vec![(vec![3, 9], vec![]), (vec![4, 2], vec![])]
        );

        // a push before the bound is dropped; one after is kept.
        g.source_push(issue, crate::change::SourceChange::Add(irow(0, 7)));
        g.source_push(issue, crate::change::SourceChange::Add(irow(5, 7)));
        assert_eq!(
            g.dump_view_rows(view),
            vec![
                (vec![3, 9], vec![]),
                (vec![4, 2], vec![]),
                (vec![5, 7], vec![])
            ]
        );
    }

    #[test]
    fn pipeline_lowers_limit() {
        // `.limit(3)` → a Take over the connection. Hydrate keeps the first 3 by the
        // PK-completed sort; an add that displaces the boundary emits a
        // Remove(bound) + Add(new); an add past the (full) window is dropped.
        let mut g = Graph::new();
        let schema = issue_schema(); // (id, priority); PK id; sort id asc
        let issue = g.add_source(
            schema.clone(),
            vec![irow(1, 5), irow(2, 1), irow(3, 9), irow(4, 2)],
        );
        let resolve = |t: &str| (t == "issue").then(|| (issue, schema.clone()));
        let ast = table("issue").limit(3).build();

        let top = build_pipeline(&mut g, &ast, &resolve).unwrap();
        let view = g.add_view(top, schema.clone().into_schema());
        g.set_sink_edge(top, view);
        g.hydrate(view);
        assert_eq!(
            g.dump_view_rows(view),
            vec![
                (vec![1, 5], vec![]),
                (vec![2, 1], vec![]),
                (vec![3, 9], vec![])
            ]
        );

        // Add id 0 (sorts before the bound, id 3): displaces the boundary out.
        g.source_push(issue, crate::change::SourceChange::Add(irow(0, 7)));
        assert_eq!(
            g.dump_view_rows(view),
            vec![
                (vec![0, 7], vec![]),
                (vec![1, 5], vec![]),
                (vec![2, 1], vec![])
            ]
        );

        // Add id 5 (after the new bound, id 2): the window is full → dropped.
        g.source_push(issue, crate::change::SourceChange::Add(irow(5, 7)));
        assert_eq!(
            g.dump_view_rows(view),
            vec![
                (vec![0, 7], vec![]),
                (vec![1, 5], vec![]),
                (vec![2, 1], vec![])
            ]
        );
    }

    #[test]
    fn pipeline_rejects_unsupported_shapes() {
        let mut g = Graph::new();
        let schema = issue_schema();
        let issue = g.add_source(schema.clone(), vec![]);
        let resolve = |t: &str| (t == "issue").then(|| (issue, schema.clone()));
        let base = || Ast {
            table: "issue".into(),
            order_by: order_id(),
            ..Default::default()
        };

        // (`start`/`limit`/`where`-EXISTS now lower — see `pipeline_lowers_*` and the
        // EXISTS end-to-end tests.) Flipped NOT EXISTS still needs an anti-join.
        let flipped_not_exists = match exists() {
            Condition::CorrelatedSubquery(mut c) => {
                c.op = ExistsOp::NotExists;
                c.flip = Some(true);
                Condition::CorrelatedSubquery(c)
            }
            other => other,
        };
        let with_flipped_not_exists = Ast {
            r#where: Some(flipped_not_exists),
            ..base()
        };
        assert!(matches!(
            build_pipeline(&mut g, &with_flipped_not_exists, &resolve),
            Err(BuildError::Unsupported(_))
        ));

        let unknown = Ast {
            table: "nope".into(),
            ..Default::default()
        };
        assert!(matches!(
            build_pipeline(&mut g, &unknown, &resolve),
            Err(BuildError::UnknownTable(_))
        ));
    }

    #[test]
    fn pipeline_multiple_relationships_fetch_and_push() {
        // issue { comments, labels } — two SIBLING relationships, lowered to two
        // stacked joins: join(labels, parent = join(comments, parent = conn)). The
        // inner join's output edge carries `Port::JoinParent` so parent changes flow
        // up the chain (each join attaching its own relationship); a child add on
        // either relationship routes a Child change to the view.
        let mut g = Graph::new();
        let comment_s = comment_schema(); // (id, issueID)
        let label_s = SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)]);
        let issue_s = SourceSchema::new(vec!["id", "priority"], vec![0], vec![(0, true)]);
        // The View materializes both relationships, so the view schema's RelDefs
        // carry their (leaf) child schemas (the comment/label sort + pk).
        let issue_view = Schema::new(vec!["id", "priority"], vec![0], vec![(0, true)])
            .with_relationships(vec![
                crate::value::RelDef::related("comments", comment_s.clone().into_schema()),
                crate::value::RelDef::related("labels", label_s.clone().into_schema()),
            ]);
        let issue = g.add_source(issue_s.clone(), vec![irow(1, 5), irow(2, 1)]);
        let comment = g.add_source(comment_s.clone(), vec![irow(10, 1), irow(11, 2)]);
        let label = g.add_source(label_s.clone(), vec![irow(20, 1), irow(21, 2)]);
        let resolve = |t: &str| match t {
            "issue" => Some((issue, issue_s.clone())),
            "comment" => Some((comment, comment_s.clone())),
            "label" => Some((label, label_s.clone())),
            _ => None,
        };
        let ast = Ast {
            table: "issue".into(),
            order_by: order_id(),
            related: vec![
                related_csq("comments", "comment", "id", "issueID"),
                related_csq("labels", "label", "id", "issueID"),
            ],
            ..Default::default()
        };
        let top = build_pipeline(&mut g, &ast, &resolve).unwrap();
        let view = g.add_view(top, issue_view);
        g.set_sink_edge(top, view);
        g.hydrate(view);
        // `dump_view` flattens BOTH relationships into `children`: comments first
        // (its join is innermost), then labels. issue 1 → [10, 20]; issue 2 → [11, 21].
        assert_eq!(
            g.dump_view(view),
            vec![(1, vec![10, 20]), (2, vec![11, 21])]
        );

        // Add a comment for issue 1: a Child(comments) on the inner join's child
        // port that the labels join forwards untouched (parent-port passthrough).
        g.source_push(comment, crate::change::SourceChange::Add(irow(12, 1)));
        assert_eq!(
            g.dump_view(view),
            vec![(1, vec![10, 12, 20]), (2, vec![11, 21])]
        );

        // Add a label for issue 2: a Child(labels) on the OUTER join's own child
        // port, routed straight to the view.
        g.source_push(label, crate::change::SourceChange::Add(irow(22, 2)));
        assert_eq!(
            g.dump_view(view),
            vec![(1, vec![10, 12, 20]), (2, vec![11, 21, 22])]
        );
    }

    #[test]
    fn pipeline_nested_relationship_on_fetch() {
        // issue { comments { authors } } — a NESTED relationship: the comments
        // child sub-pipeline is itself a Join (authors), wired into the issue join's
        // child port (the inner join's output carries `Port::JoinChild`). The
        // pipeline builds and fetches the full three-level tree. (The deepest *push*
        // path — an author change — stays item-4, so this asserts fetch only.)
        let mut g = Graph::new();
        let issue_s = issue_schema(); // "comments" is query-local (slot 0)
        let comment_s = SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)]);
        let author_s = SourceSchema::new(vec!["id", "commentID"], vec![0], vec![(0, true)]);
        let issue = g.add_source(issue_s.clone(), vec![irow(1, 5)]);
        let comment = g.add_source(comment_s.clone(), vec![irow(10, 1)]);
        let author = g.add_source(author_s.clone(), vec![irow(100, 10)]);
        let resolve = |t: &str| match t {
            "issue" => Some((issue, issue_s.clone())),
            "comment" => Some((comment, comment_s.clone())),
            "author" => Some((author, author_s.clone())),
            _ => None,
        };
        // issue ⋈ comments(issue.id = comment.issueID) ⋈ authors(comment.id = author.commentID)
        let mut ast = issue_with_comments();
        ast.related[0].subquery.related = vec![related_csq("authors", "author", "id", "commentID")];

        let top = build_pipeline(&mut g, &ast, &resolve).unwrap();
        let catch = g.add_catch(top, false);
        let tree = g.catch_fetch(catch, &crate::change::FetchRequest::all());

        // One issue → one comment → one author, nested. Slots are query-local: the
        // issue frame's only related is "comments" (slot 0); the comment frame's only
        // related is "authors" (slot 0).
        let comments_slot = RelId(0);
        let authors_slot = RelId(0);
        assert_eq!(tree.len(), 1);
        let issue_node = &tree[0];
        assert_eq!(col0(&issue_node.row), 1);
        let comments = &issue_node.relationships[&comments_slot];
        assert_eq!(comments.len(), 1);
        assert_eq!(col0(&comments[0].row), 10);
        let authors = &comments[0].relationships[&authors_slot];
        assert_eq!(authors.len(), 1);
        assert_eq!(col0(&authors[0].row), 100);
    }

    #[test]
    fn pipeline_compound_key_join() {
        // issue(org, id) ⋈ comment(org, issueID, cid) on (org, id)=(org, issueID).
        let mut g = Graph::new();
        let comment_s = SourceSchema::new(vec!["org", "issueID", "cid"], vec![2], vec![(2, true)]);
        let issue_s = SourceSchema::new(vec!["org", "id"], vec![0, 1], vec![(0, true), (1, true)]);
        // The View materializes "comments", so the view schema carries its child schema.
        let issue_view = Schema::new(vec!["org", "id"], vec![0, 1], vec![(0, true), (1, true)])
            .with_relationships(vec![crate::value::RelDef::related(
                "comments",
                comment_s.clone().into_schema(),
            )]);
        let issue = g.add_source(issue_s.clone(), vec![irow(1, 10), irow(1, 20), irow(2, 10)]);
        let comment = g.add_source(
            comment_s.clone(),
            vec![
                owned_row(vec![
                    OwnedValue::Int(1),
                    OwnedValue::Int(10),
                    OwnedValue::Int(100),
                ]),
                owned_row(vec![
                    OwnedValue::Int(1),
                    OwnedValue::Int(10),
                    OwnedValue::Int(101),
                ]),
                owned_row(vec![
                    OwnedValue::Int(1),
                    OwnedValue::Int(20),
                    OwnedValue::Int(102),
                ]),
                owned_row(vec![
                    OwnedValue::Int(2),
                    OwnedValue::Int(10),
                    OwnedValue::Int(103),
                ]),
            ],
        );
        let resolve = |t: &str| match t {
            "issue" => Some((issue, issue_s.clone())),
            "comment" => Some((comment, comment_s.clone())),
            _ => None,
        };
        let ast = Ast {
            table: "issue".into(),
            related: vec![related_csq2(
                "comments",
                "comment",
                &["org", "id"],
                &["org", "issueID"],
            )],
            ..Default::default()
        };
        let top = build_pipeline(&mut g, &ast, &resolve).unwrap();
        let view = g.add_view(top, issue_view);
        g.hydrate(view);
        assert_eq!(
            g.dump_view_rows(view),
            vec![
                (vec![1, 10], vec![vec![1, 10, 100], vec![1, 10, 101]]),
                (vec![1, 20], vec![vec![1, 20, 102]]),
                (vec![2, 10], vec![vec![2, 10, 103]]),
            ]
        );
    }

    #[test]
    fn pipeline_rejects_mismatched_key_lengths() {
        let mut g = Graph::new();
        let issue_s = issue_schema();
        let comment_s = comment_schema();
        let issue = g.add_source(issue_s.clone(), vec![]);
        let comment = g.add_source(comment_s.clone(), vec![]);
        let resolve = |t: &str| match t {
            "issue" => Some((issue, issue_s.clone())),
            "comment" => Some((comment, comment_s.clone())),
            _ => None,
        };
        // parent_field has 1 column, child_field has 2 → invalid correlation.
        let mut ast = issue_with_comments();
        ast.related[0].correlation.child_field = vec!["issueID".into(), "id".into()];
        assert!(matches!(
            build_pipeline(&mut g, &ast, &resolve),
            Err(BuildError::Invalid(_))
        ));
    }

    #[test]
    fn pipeline_query_local_relationship_needs_no_schema_declaration() {
        // Query-local slots: a relationship defined BY THE QUERY (a `related`/EXISTS CSQ)
        // builds even when the source schema does NOT declare it — the slot layout is
        // derived from the AST, not the source's declared `relationships`. This is the
        // production payoff (the wasm `schema_from_js` declares no synthesized gate slots).
        // Pre-refactor this raised `BuildError::UnknownRelationship("comments")`; that
        // "schema must pre-declare the relationship" contract is retired here.
        let mut g = Graph::new();
        // issue source schema (a source never declares relationships anyway).
        let issue_s = SourceSchema::new(vec!["id", "priority"], vec![0], vec![(0, true)]);
        let comment_s = comment_schema();
        let issue = g.add_source(issue_s.clone(), vec![]);
        let comment = g.add_source(comment_s.clone(), vec![]);
        let resolve = |t: &str| match t {
            "issue" => Some((issue, issue_s.clone())),
            "comment" => Some((comment, comment_s.clone())),
            _ => None,
        };
        assert!(build_pipeline(&mut g, &issue_with_comments(), &resolve).is_ok());
    }

    #[test]
    fn bare_exists_alias_colliding_with_a_materialized_related_is_rejected() {
        // WS05.4: a GENUINE one-slot-per-name clash that `flatten_condition` does not
        // remove. A bare top-level EXISTS('comments') is not uniquified (JS-parity
        // guard), so its alias collides with the materialized related('comments').
        let mut g = Graph::new();
        let issue_s = issue_schema();
        let comment_s = comment_schema();
        let issue = g.add_source(issue_s.clone(), vec![]);
        let comment = g.add_source(comment_s.clone(), vec![]);
        let resolve = |t: &str| match t {
            "issue" => Some((issue, issue_s.clone())),
            "comment" => Some((comment, comment_s.clone())),
            _ => None,
        };
        // The query both materializes `related("comments", …)` and gates on a bare
        // EXISTS('comments') → a one-slot-per-name clash.
        let ast = Ast {
            table: "issue".into(),
            order_by: order_id(),
            related: vec![related_csq("comments", "comment", "id", "issueID")],
            r#where: Some(exists_alias("comments", "comment")),
            ..Default::default()
        };
        assert!(matches!(
            build_pipeline(&mut g, &ast, &resolve),
            Err(BuildError::Unsupported(_))
        ));
    }
}
