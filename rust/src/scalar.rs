//! Build-time **scalar-subquery resolution** (`SCALAR-SUBQUERY-DESIGN.md`).
//!
//! A correlated `EXISTS` / `NOT EXISTS` subquery whose matched row is **statically
//! unique** — the subquery's `WHERE` binds every column of a unique key of the child
//! table to a build-time-known literal — can be resolved *before lowering*: we read
//! the one row from the source, inline its correlation value into the parent query as
//! a literal, and **delete the join entirely**. The parent pipeline then never
//! subscribes to the child table.
//!
//! This is an opt-in, snapshot operation. It fires only on a condition the user
//! flagged `scalar: true` ([`CorrelatedSubqueryCondition::scalar`](crate::ast)), and
//! the inlined value is frozen for the life of the pipeline (design §3). The pass is
//! a pure `Ast → Ast` transform, like the planner, but with one extra capability: a
//! read seam over the source ([`ScalarCatalog`]).
//!
//! **Cross-level recursion (design §6).** Resolution is **inner-first**: before
//! folding a scalar condition, its child subquery is resolved first, so an inner fold
//! that turns the child `WHERE` into a unique-key binding can *unlock* the outer fold
//! (`project ← issue ← comment` collapses to a single `project.id = …`). The recursion
//! is the fixpoint — a chain of nested scalar `EXISTS` resolves bottom-up in one
//! traversal. It stays **user-driven**: only conditions the user flagged `scalar` are
//! folded, so each fold (and the staleness hop it adds, §3) is opt-in per level.
//!
//! **Precondition is exact (design §4).** A scalar child's `WHERE` must bind *exactly*
//! a unique key — no extra filters. A `WHERE` like `id = 7 AND status = 'open'` is
//! **not** folded (returns [`BuildError::Unsupported`]): folding to the correlation
//! value would silently drop the `status` filter. The slice's canonical shape (a child
//! `WHERE` that is exactly the unique-key equality, including the one an inner fold
//! produces) satisfies this.

use std::collections::BTreeMap;

use crate::ast::{
    Ast, Condition, CorrelatedSubqueryCondition, ExistsOp, Lit, Op, SimpleCondition, ValuePosition,
};
use crate::builder::{lit_to_scalar, BuildError};
use crate::value::{ColId, OwnedRow, OwnedValue, Schema};

/// The per-table read seam the resolver uses to fold a statically-unique subquery.
/// One impl per source backend; [`MemorySource`](crate::memory_source::MemorySource)
/// provides the PK-only slice impl.
pub trait ScalarSource {
    /// The child table's schema (column names → [`ColId`], primary key).
    fn schema(&self) -> &Schema;

    /// The statically-unique key column sets, **PK first**. Empty ⇒ nothing here is
    /// foldable. (Slice: PK only; SQLite `pragma_index_xinfo` discovery is a
    /// follow-up — design §4.1.)
    fn unique_keys(&self) -> Vec<Vec<ColId>>;

    /// The single row in which every `(ColId, value)` of `bound` holds. The caller
    /// guarantees `bound` covers one of [`Self::unique_keys`], so the result is
    /// unique. `None` ⇒ no row matches (the empty-result fold).
    fn lookup_unique(&self, bound: &[(ColId, OwnedValue)]) -> Option<OwnedRow>;
}

/// Maps a table name to its [`ScalarSource`]. The resolver only ever looks up the
/// **child** (subquery) table — the parent is never read.
pub trait ScalarCatalog {
    /// The source for `table`, or `None` if this catalog does not back it.
    fn source(&self, table: &str) -> Option<&dyn ScalarSource>;
}

/// Resolve every `scalar`-flagged correlated subquery in `ast` (its `WHERE` tree and,
/// recursively, every nested subquery and `related` child) against `catalog`,
/// returning a rewritten AST in which each fold has replaced its `EXISTS`/`NOT EXISTS`
/// condition with a plain `Simple` condition (so the builder emits no join for it).
/// Conditions without the flag are untouched, so an AST with no `scalar: true`
/// anywhere round-trips unchanged.
///
/// Errors (design §7: a flagged scalar that cannot be proven single-row is loud, not
/// a silent live fallback):
/// - [`BuildError::UnknownTable`] — the child table is not in `catalog`.
/// - [`BuildError::Unsupported`] — the precondition fails (the child `WHERE` does not
///   bind *exactly* a unique key), or this shape is not yet folded (e.g. a
///   compound-correlation `NOT EXISTS`).
/// - [`BuildError::UnknownColumn`] — a correlation/key column is absent from the schema.
pub fn resolve_scalars(ast: &Ast, catalog: &dyn ScalarCatalog) -> Result<Ast, BuildError> {
    resolve_ast(ast, catalog)
}

/// True if `ast` contains any `scalar`-flagged correlated subquery (anywhere in its
/// `WHERE` tree or a nested/`related` subquery). A cheap gate so a caller can skip the
/// [`resolve_scalars`] clone when there is nothing to fold — the peer of the planner's
/// `has_flippable_exists`.
pub fn has_scalar_subquery(ast: &Ast) -> bool {
    ast.r#where.as_ref().is_some_and(cond_has_scalar)
        || ast.related.iter().any(|r| has_scalar_subquery(&r.subquery))
}

fn cond_has_scalar(cond: &Condition) -> bool {
    match cond {
        Condition::Simple(_) => false,
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().any(cond_has_scalar)
        }
        Condition::CorrelatedSubquery(csq) => {
            csq.scalar == Some(true) || has_scalar_subquery(&csq.related.subquery)
        }
    }
}

/// Resolve scalars throughout one AST level: its `WHERE` tree and each materialized
/// `related` child (recursively). Pure clone-and-rewrite.
fn resolve_ast(ast: &Ast, catalog: &dyn ScalarCatalog) -> Result<Ast, BuildError> {
    let mut out = ast.clone();
    if let Some(w) = &ast.r#where {
        out.r#where = Some(resolve_condition(w, catalog)?);
    }
    out.related = ast
        .related
        .iter()
        .map(|csq| {
            let mut c = csq.clone();
            c.subquery = Box::new(resolve_ast(&csq.subquery, catalog)?);
            Ok(c)
        })
        .collect::<Result<_, BuildError>>()?;
    Ok(out)
}

/// Recurse the `WHERE` tree, folding each `scalar`-flagged correlated subquery. The
/// `And`/`Or` arms recurse structurally; a `Simple` is inert; a `CorrelatedSubquery`
/// is resolved **inner-first** (its child subquery is resolved before this level is
/// folded — the cross-level unlock, §6).
fn resolve_condition(
    cond: &Condition,
    catalog: &dyn ScalarCatalog,
) -> Result<Condition, BuildError> {
    match cond {
        Condition::Simple(_) => Ok(cond.clone()),
        Condition::And { conditions } => Ok(Condition::And {
            conditions: conditions
                .iter()
                .map(|c| resolve_condition(c, catalog))
                .collect::<Result<_, _>>()?,
        }),
        Condition::Or { conditions } => Ok(Condition::Or {
            conditions: conditions
                .iter()
                .map(|c| resolve_condition(c, catalog))
                .collect::<Result<_, _>>()?,
        }),
        Condition::CorrelatedSubquery(csq) => {
            // Inner-first: resolve the child subquery so an inner scalar fold can turn
            // this child's WHERE into a unique-key binding that unlocks the fold here.
            let child = resolve_ast(&csq.related.subquery, catalog)?;
            if csq.scalar == Some(true) {
                fold_scalar(csq, &child, catalog)
            } else {
                // A non-flagged EXISTS stays a join, but carries its resolved child
                // (deeper scalars are still folded).
                let mut c = csq.clone();
                c.related.subquery = Box::new(child);
                Ok(Condition::CorrelatedSubquery(c))
            }
        }
    }
}

/// Fold one statically-unique `scalar` subquery into a parent-side `Simple`
/// condition (or a constant when the row is absent). `child` is the **already
/// inner-resolved** subquery (§6). See the design §5 table.
fn fold_scalar(
    csq: &CorrelatedSubqueryCondition,
    child: &Ast,
    catalog: &dyn ScalarCatalog,
) -> Result<Condition, BuildError> {
    // Inner resolution may have collapsed the child WHERE to a constant. A
    // constant-**false** child ⇒ the subquery is empty ⇒ decide without a lookup (the
    // inner-absent cross-level cascade, §6). A constant-**true** child ⇒ "all rows",
    // which is not single-row — it falls through to the unique-key precondition, which
    // fails loudly (correct: a `WHERE true` EXISTS is a real semijoin, not scalar).
    if child.r#where.as_ref().and_then(const_bool) == Some(false) {
        return Ok(empty_fold(csq.op));
    }

    let corr = &csq.related.correlation;
    let source = catalog
        .source(&child.table)
        .ok_or_else(|| BuildError::UnknownTable(child.table.clone()))?;

    // 1. The child's WHERE must be a conjunction of `col = <literal>` bindings.
    let bindings =
        child
            .r#where
            .as_ref()
            .and_then(collect_eq_bindings)
            .ok_or(BuildError::Unsupported(
                "scalar subquery: child WHERE must be a conjunction of `column = literal`",
            ))?;

    // 2. The bindings must cover **exactly** a unique key (no extra filters — folding
    //    away an extra filter would be unsound, §4).
    let bound = bind_unique_key(source, &bindings)?.ok_or(BuildError::Unsupported(
        "scalar subquery: child WHERE does not bind exactly a unique key",
    ))?;

    // 3. Read the (at most one) row and rewrite the parent condition.
    match source.lookup_unique(&bound) {
        Some(row) => present_fold(csq.op, corr, source.schema(), &row),
        None => Ok(empty_fold(csq.op)),
    }
}

/// The matched row exists: the `EXISTS` holds exactly for parents whose
/// `parent_field` equals the row's `child_field` value. Rewrite to that equality
/// (`EXISTS`) or its null-aware negation (`NOT EXISTS`, the `field IS NOT literal`
/// rewrite — design §5).
fn present_fold(
    op: ExistsOp,
    corr: &crate::ast::Correlation,
    child_schema: &Schema,
    row: &OwnedRow,
) -> Result<Condition, BuildError> {
    // Each correlation pair `parent_field[i] (=) child_field[i]` becomes an equality
    // of the parent column against the row's child-field value, read as a literal.
    let mut eqs: Vec<SimpleCondition> = Vec::with_capacity(corr.parent_field.len());
    for (pf, cf) in corr.parent_field.iter().zip(&corr.child_field) {
        let col = child_schema
            .col_id(cf)
            .ok_or_else(|| BuildError::UnknownColumn(cf.clone()))?;
        let lit = scalar_to_lit(&row.col(col).to_owned())?;
        eqs.push(SimpleCondition {
            op: Op::Eq,
            left: ValuePosition::Column { name: pf.clone() },
            right: ValuePosition::Literal { value: lit },
        });
    }

    match op {
        ExistsOp::Exists => Ok(and_of(eqs)),
        // `NOT EXISTS` over a single correlation column is `parent IS NOT literal`.
        // Compound correlation would be `OR` of `IS NOT`s — deferred (slice).
        ExistsOp::NotExists => {
            let [eq] = <[SimpleCondition; 1]>::try_from(eqs).map_err(|_| {
                BuildError::Unsupported(
                    "scalar NOT EXISTS with a compound correlation is not yet folded",
                )
            })?;
            Ok(Condition::Simple(SimpleCondition {
                op: Op::IsNot,
                ..eq
            }))
        }
    }
}

/// No matching row: `EXISTS` is constant-false, `NOT EXISTS` constant-true. Encoded
/// as a literal-vs-literal `Simple`, which the builder folds to
/// `CompiledPredicate::Const` (`builder.rs:490`) — no new AST variant needed.
fn empty_fold(op: ExistsOp) -> Condition {
    let always = matches!(op, ExistsOp::NotExists);
    // `1 = 1` ⇒ true, `0 = 1` ⇒ false.
    Condition::Simple(SimpleCondition {
        op: Op::Eq,
        left: ValuePosition::Literal {
            value: Lit::Number(if always { 1.0 } else { 0.0 }),
        },
        right: ValuePosition::Literal {
            value: Lit::Number(1.0),
        },
    })
}

/// Wrap a list of equalities as a single condition: the bare `Simple` for one,
/// an `And` for several. (Always non-empty — a correlation has ≥1 pair.)
fn and_of(mut eqs: Vec<SimpleCondition>) -> Condition {
    if eqs.len() == 1 {
        Condition::Simple(eqs.pop().unwrap())
    } else {
        Condition::And {
            conditions: eqs.into_iter().map(Condition::Simple).collect(),
        }
    }
}

/// A literal-vs-literal `Eq` `Simple` is a constant (the shape [`empty_fold`] emits,
/// and what an inner fold leaves behind). Returns its truth value, else `None`.
fn const_bool(cond: &Condition) -> Option<bool> {
    if let Condition::Simple(SimpleCondition {
        op: Op::Eq,
        left: ValuePosition::Literal { value: a },
        right: ValuePosition::Literal { value: b },
    }) = cond
    {
        return Some(lit_eq(a, b));
    }
    None
}

/// Structural literal equality over the scalar arms (enough for the constants the
/// resolver produces and re-reads).
fn lit_eq(a: &Lit, b: &Lit) -> bool {
    use crate::value::compare_int_f64;
    use std::cmp::Ordering;
    match (a, b) {
        (Lit::Null, Lit::Null) => true,
        (Lit::Bool(x), Lit::Bool(y)) => x == y,
        (Lit::Int(x), Lit::Int(y)) => x == y,
        (Lit::Number(x), Lit::Number(y)) => x == y,
        // Numerics compare exactly across the Int/Number spelling (design 226 Stage B):
        // the resolver may have inlined `Int(5)` where an older constant said `5.0`.
        (Lit::Int(x), Lit::Number(y)) | (Lit::Number(y), Lit::Int(x)) => {
            compare_int_f64(*x, *y) == Ordering::Equal
        }
        (Lit::Str(x), Lit::Str(y)) => x == y,
        _ => false,
    }
}

/// Collect a `column = literal` conjunction into a `name → literal` map. Returns
/// `None` (⇒ not a foldable shape) on any other condition — an `Or`, a non-`Eq` op,
/// a column RHS, a nested subquery, etc.
fn collect_eq_bindings(cond: &Condition) -> Option<BTreeMap<&str, &Lit>> {
    let mut out = BTreeMap::new();
    if collect_eq_into(cond, &mut out) {
        Some(out)
    } else {
        None
    }
}

fn collect_eq_into<'a>(cond: &'a Condition, out: &mut BTreeMap<&'a str, &'a Lit>) -> bool {
    match cond {
        Condition::And { conditions } => conditions.iter().all(|c| collect_eq_into(c, out)),
        Condition::Simple(SimpleCondition {
            op: Op::Eq,
            left: ValuePosition::Column { name },
            right: ValuePosition::Literal { value },
        }) => {
            out.insert(name, value);
            true
        }
        _ => false,
    }
}

/// If `bindings` covers **exactly** some unique key of `source` (the same columns, no
/// more), return that key's `(ColId, value)` lookup pairs (first match wins — PK is
/// first). `None` ⇒ no unique key is matched exactly. The exact-cover requirement is a
/// soundness rule: a binding *beyond* the key is an extra filter that folding to the
/// correlation value would silently drop (§4).
fn bind_unique_key(
    source: &dyn ScalarSource,
    bindings: &BTreeMap<&str, &Lit>,
) -> Result<Option<Vec<(ColId, OwnedValue)>>, BuildError> {
    let schema = source.schema();
    for key in source.unique_keys() {
        // Same arity + every key column bound ⇒ the binding set equals the key set.
        if key.len() != bindings.len() {
            continue;
        }
        let mut bound = Vec::with_capacity(key.len());
        let mut covered = true;
        for &col in &key {
            let name: &str = &schema.columns[col];
            match bindings.get(name) {
                Some(lit) => bound.push((col, lit_to_scalar(lit)?)),
                None => {
                    covered = false;
                    break;
                }
            }
        }
        if covered {
            return Ok(Some(bound));
        }
    }
    Ok(None)
}

/// Bridge a runtime [`OwnedValue`] (read from the source row) back to an AST [`Lit`]
/// for inlining. The inverse of [`lit_to_scalar`] over the scalar arms; `Int` keeps
/// all 64 bits via `Lit::Int` (design 226 Stage B — the old `as f64` collapse
/// rounded above 2^53), and a `Json` cell — never a correlation key — is unsupported.
fn scalar_to_lit(v: &OwnedValue) -> Result<Lit, BuildError> {
    Ok(match v {
        OwnedValue::Null => Lit::Null,
        OwnedValue::Bool(b) => Lit::Bool(*b),
        OwnedValue::Int(i) => Lit::Int(*i),
        OwnedValue::Float(f) => Lit::Number(*f),
        OwnedValue::Str(s) => Lit::Str(s.as_ref().into()),
        OwnedValue::Json(_) => {
            return Err(BuildError::Unsupported(
                "scalar subquery: a JSON correlation value cannot be inlined",
            ))
        }
        // A correlation key is read from a full engine row, so the projection sentinel should
        // never reach here; if it does, it cannot be inlined as a literal.
        OwnedValue::Absent => {
            return Err(BuildError::Unsupported(
                "scalar subquery: an absent (unprojected) correlation value cannot be inlined",
            ))
        }
    })
}
