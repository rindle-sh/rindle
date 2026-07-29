//! Spec `07` §4 — the compiled `where` **leaf predicate**: a [`ColId`]-addressed,
//! string-free gate evaluated per row on the hot path.
//!
//! The boolean *structure* of a `where` (AND / OR / NOT) is **not** here — it is
//! realized in the **Filter sub-graph** (one [`Filter`](crate::graph) link per
//! leaf for AND, `FanOut`/`FanIn` for OR; specs `06`/`07`). A
//! [`CompiledPredicate`] is therefore exactly one **leaf** condition: `col <cmp>
//! value`, `col IN set`, `col LIKE pattern`, or `col IS [NOT] literal`. The builder
//! (`08`) lowers each AST simple-condition — resolving the column *name* to a
//! [`ColId`] against the `Schema` — into one of these.
//!
//! This is the **shared** predicate layer: the `Filter` operator (`07` §3) and any
//! membership gate consume it, so the comparator choices (the three-way split in
//! [`crate::value`]: [`compare_values`](crate::value::compare_values) for ordering, [`values_identical`] for
//! `=`/`!=`/`IN`, never [`values_equal`](crate::value::values_equal) — `07` §8.1)
//! are made **once, here**, not reinvented per operator. It replaces the spike's
//! 2-variant `FilterPred` (`graph.rs`), which used the wrong (`values_equal`)
//! comparator for `=`.

use crate::value::{values_identical, ColId, OwnedRow, OwnedValue, Value};
use std::cmp::Ordering;

/// A scalar comparison operator (`07` §4.1). Ordering ops (`Lt`/`Le`/`Gt`/`Ge`)
/// evaluate via [`compare_values`](crate::value::compare_values); `Eq`/`Ne` via [`values_identical`] (the
/// predicate identity comparator, null≡null).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// One compiled leaf condition (`07` §4.1). Built once by the builder, evaluated
/// per row with no name lookup. AND/OR/NOT live in the Filter sub-graph, so this
/// is deliberately *flat* — no recursive `And`/`Or` variant.
pub enum CompiledPredicate {
    /// `col <op> value`. A NULL cell drops the row (SQL `UNKNOWN`) for every op.
    /// Non-null `Eq`/`Ne` use [`values_identical`]; the ordering ops use
    /// [`compare_values`](crate::value::compare_values).
    Cmp {
        col: ColId,
        op: CmpOp,
        value: OwnedValue,
    },
    /// `col IN (set)` (or `NOT IN` when `negated`). A NULL cell drops the row (even
    /// for `NOT IN`). Membership via [`values_identical`] ([`ValueSet::contains`]).
    In {
        col: ColId,
        set: ValueSet,
        negated: bool,
    },
    /// `col LIKE pattern` / `col ILIKE pattern` (or the negated forms). Non-text
    /// cells (incl. `null`) never match (so `NOT LIKE` on a null is still `false`
    /// — SQL's three-valued `UNKNOWN`, collapsed to "drop").
    Like {
        col: ColId,
        matcher: LikeMatcher,
        negated: bool,
    },
    /// `col IS literal` (or `IS NOT` when `negated`). Identity equality, including
    /// null (`null IS null` is true).
    Is {
        col: ColId,
        value: OwnedValue,
        negated: bool,
    },
    /// Fast path for `col IS NULL` (or `IS NOT NULL` when `negated`).
    IsNull { col: ColId, negated: bool },
    /// A constant, independent of the row — the builder folds a literal-only
    /// condition (`5 = 5`) or any non-`IS` `col <op> NULL` (always false) to this
    /// (`08`; `filter.ts:69,76,82,85`).
    Const(bool),
}

impl CompiledPredicate {
    /// Evaluate against one owned row. Index-addressed; no allocation. Cells are
    /// borrowed via `as_ref()` and fed to the appropriate comparator.
    ///
    /// **Null handling (the SQL three-valued rule).** Except for [`IsNull`] — which
    /// inspects null-ness directly — a NULL cell makes the comparison `UNKNOWN`, so
    /// the row is dropped (`false`) for **every** operator, including the negated
    /// ones (`!=` / `NOT IN` / `NOT LIKE`). This mirrors the JS `createPredicate`
    /// LHS null guard (`filter.ts:88-94`). The builder never produces a `Cmp`/`In`/
    /// `Like` over a NULL literal (those fold to `Const(false)`), so only the *cell*
    /// can be null here.
    ///
    /// [`IsNull`]: CompiledPredicate::IsNull
    pub fn eval(&self, row: &OwnedRow) -> bool {
        match self {
            CompiledPredicate::Const(b) => *b,
            CompiledPredicate::Is {
                col,
                value,
                negated,
            } => values_identical(row.col(*col), value.as_ref()) ^ negated,
            CompiledPredicate::IsNull { col, negated } => row.col(*col).is_null() ^ negated,
            CompiledPredicate::Cmp { col, op, value } => {
                let cell = row.col(*col);
                if cell.is_null() {
                    return false;
                }
                let v = value.as_ref();
                match op {
                    CmpOp::Eq => values_identical(cell, v),
                    CmpOp::Ne => !values_identical(cell, v),
                    // Ordering ops: a null RHS has no order vs the (non-null) cell
                    // (SQL `UNKNOWN`); guard it out rather than letting
                    // `compare_values`'s null-is-least order leak in. In practice the
                    // builder folds a null RHS to `Const(false)`, so this is belt-
                    // and-suspenders (`07` §3.1: ordering predicates are non-null).
                    CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => {
                        if v.is_null() {
                            return false;
                        }
                        let ord = compare_predicate_values(cell, v);
                        matches!(
                            (op, ord),
                            (CmpOp::Lt, Ordering::Less)
                                | (CmpOp::Le, Ordering::Less | Ordering::Equal)
                                | (CmpOp::Gt, Ordering::Greater)
                                | (CmpOp::Ge, Ordering::Greater | Ordering::Equal)
                        )
                    }
                }
            }
            CompiledPredicate::In { col, set, negated } => {
                let cell = row.col(*col);
                if cell.is_null() {
                    return false;
                }
                set.contains(cell) ^ negated
            }
            CompiledPredicate::Like {
                col,
                matcher,
                negated,
            } => {
                let cell = row.col(*col);
                if cell.is_null() {
                    return false;
                }
                let hit = match cell {
                    Value::Str(b) | Value::Json(b) => matcher.matches(b),
                    _ => false, // non-text never matches
                };
                hit ^ negated
            }
        }
    }
}

/// Ordering comparator for the predicate path (`<`/`<=`/`>`/`>=`). Like
/// [`compare_values`](crate::value::compare_values) but **never panics on a cross-type pair** — a `where` filter
/// must not crash the pipeline on a literal/column type mismatch (`07` §8.1; the
/// `values.rs` doc-contract). Numeric `Int`/`Float` compare EXACTLY (design 226
/// §5.1, `compare_int_f64` — all 64 bits significant, no f64 widening); a genuinely
/// incomparable pair (e.g. string vs number) orders by a stable
/// type rank, so a range op is total and an equality over mismatched types is false.
/// (WS02.4 — this replaces the former `_ => compare_values` panic fallthrough.)
///
/// `pub(crate)` so the push-guard reverse index (`push_index::GuardKey`)
/// can key a `BTreeMap` with the SAME total order the `=`/`IN` predicate identity
/// (`values_identical`) uses — coarser-than-identical is safe (a false-positive
/// candidate re-checks the exact predicate), finer would drop a delta.
pub(crate) fn compare_predicate_values(a: Value<'_>, b: Value<'_>) -> Ordering {
    use Value::*;
    match (a, b) {
        // `Absent` < everything (mirrors `compare_values`); never met on a predicate
        // column in practice (presence-required), but kept total here.
        (Absent, Absent) => Ordering::Equal,
        (Absent, _) => Ordering::Less,
        (_, Absent) => Ordering::Greater,
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Int(x), Float(y)) => crate::value::compare_int_f64(x, y),
        (Float(x), Int(y)) => crate::value::compare_int_f64(y, x).reverse(),
        (Bool(x), Bool(y)) => x.cmp(&y),
        (Int(x), Int(y)) => x.cmp(&y),
        (Float(x), Float(y)) => x.total_cmp(&y),
        (Str(x), Str(y)) => x.cmp(y),
        (Json(x), Json(y)) => x.cmp(y),
        _ => predicate_type_rank(a).cmp(&predicate_type_rank(b)),
    }
}

/// Stable type ordering for incomparable cross-type predicate operands (numerics
/// share a rank so `Int`/`Float` never reach here as a mismatch).
fn predicate_type_rank(v: Value<'_>) -> u8 {
    match v {
        Value::Absent => 0,
        Value::Null => 1,
        Value::Bool(_) => 2,
        Value::Int(_) | Value::Float(_) => 3,
        Value::Str(_) => 4,
        Value::Json(_) => 5,
    }
}

// ---------------------------------------------------------------------------
// ValueSet — the `IN (...)` membership set
// ---------------------------------------------------------------------------

/// The right-hand side of an `IN`. A small owned set; membership is
/// [`values_identical`] (null≡null, the predicate comparator). Backed by a `Vec`
/// with a linear scan — `IN` lists are typically short, and a hashed/sorted form
/// is a measured optimization deferred until profiling shows it matters (`07`
/// OQ on `ValueSet` representation).
pub struct ValueSet {
    values: Vec<OwnedValue>,
}

impl ValueSet {
    pub fn new(values: Vec<OwnedValue>) -> ValueSet {
        ValueSet { values }
    }

    /// True if `needle` is identical to some member (`values_identical`).
    pub fn contains(&self, needle: Value<'_>) -> bool {
        self.values
            .iter()
            .any(|v| values_identical(v.as_ref(), needle))
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

// ---------------------------------------------------------------------------
// LikeMatcher — compiled SQL `LIKE` pattern
// ---------------------------------------------------------------------------

/// One token of a compiled `LIKE` pattern.
#[derive(Clone, PartialEq, Eq, Debug)]
enum LikeTok {
    /// A run of literal bytes that must match exactly.
    Lit(Box<[u8]>),
    /// `_` — exactly one byte.
    One,
    /// `%` — any run of bytes (including empty).
    Any,
}

/// A compiled SQL `LIKE` pattern (`07` §4 / `filter.ts`). `%` matches any byte
/// run, `_` matches exactly one byte. Compiled once (the builder lowers the
/// literal pattern) and matched per row with a classic backtracking glob walk.
///
/// **Collation / escape policy (WS05.1):**
/// - **`\` escape** is honoured, matching the SQL the builder emits (`ESCAPE '\'`):
///   `\%`/`\_`/`\\` are the literal `%`/`_`/`\`. So memory and SQLite agree for
///   patterns that escape a wildcard.
/// - **Case-sensitive `LIKE`** is byte-level; the SQLite leaf sets `PRAGMA
///   case_sensitive_like = ON` so its bare `LIKE` agrees (both case-sensitive).
/// - **`ILIKE`** folds ASCII-only (`eq_ignore_ascii_case`), which matches the
///   bundled SQLite's `lower()` (no ICU) — so memory and SQLite agree on ASCII.
///   **Non-ASCII case folding is a documented limitation** (both leave non-ASCII
///   bytes unfolded); revisit only if a corpus needs Unicode `ILIKE`.
/// - `_` is **byte-level**, not UTF-8-character-level (a deferred edge for non-ASCII
///   single-char matches; the byte glob structure is correct and tested).
pub struct LikeMatcher {
    toks: Box<[LikeTok]>,
    case_insensitive: bool,
}

impl LikeMatcher {
    /// Compile a `LIKE` pattern. Adjacent literal bytes coalesce into one `Lit`
    /// run; runs of `%` collapse to a single `Any`.
    pub fn compile(pattern: &[u8]) -> LikeMatcher {
        LikeMatcher::compile_with_case(pattern, false)
    }

    /// Compile a case-insensitive `LIKE` pattern (`ILIKE`).
    pub fn compile_case_insensitive(pattern: &[u8]) -> LikeMatcher {
        LikeMatcher::compile_with_case(pattern, true)
    }

    fn compile_with_case(pattern: &[u8], case_insensitive: bool) -> LikeMatcher {
        let mut toks: Vec<LikeTok> = Vec::new();
        let mut lit: Vec<u8> = Vec::new();
        let flush = |lit: &mut Vec<u8>, toks: &mut Vec<LikeTok>| {
            if !lit.is_empty() {
                toks.push(LikeTok::Lit(std::mem::take(lit).into_boxed_slice()));
            }
        };
        // Honour the `\` escape, matching the SQL the builder emits (`ESCAPE '\'`,
        // `query_builder.rs`) and Postgres/SQLite semantics (WS05.1): `\%`/`\_`/`\\`
        // are the literal `%`/`_`/`\`. An escaped wildcard goes into the `Lit` run, so
        // it never becomes `Any`/`One` and never collapses with an adjacent real `%`.
        let mut i = 0;
        while i < pattern.len() {
            let b = pattern[i];
            match b {
                b'\\' => match pattern.get(i + 1) {
                    Some(&n @ (b'%' | b'_' | b'\\')) => {
                        lit.push(n);
                        i += 2;
                    }
                    // `\x` for any other `x`: SQLite leaves this undefined; be lenient
                    // and keep both bytes literal.
                    Some(&n) => {
                        lit.push(b'\\');
                        lit.push(n);
                        i += 2;
                    }
                    // A trailing `\` is a literal backslash.
                    None => {
                        lit.push(b'\\');
                        i += 1;
                    }
                },
                b'%' => {
                    flush(&mut lit, &mut toks);
                    // Collapse `%%`→`%`.
                    if toks.last() != Some(&LikeTok::Any) {
                        toks.push(LikeTok::Any);
                    }
                    i += 1;
                }
                b'_' => {
                    flush(&mut lit, &mut toks);
                    toks.push(LikeTok::One);
                    i += 1;
                }
                other => {
                    lit.push(other);
                    i += 1;
                }
            }
        }
        flush(&mut lit, &mut toks);
        LikeMatcher {
            toks: toks.into_boxed_slice(),
            case_insensitive,
        }
    }

    fn lit_matches(&self, text: &[u8], start: usize, run: &[u8]) -> bool {
        let end = start.saturating_add(run.len());
        if end > text.len() {
            return false;
        }
        if self.case_insensitive {
            text[start..end].eq_ignore_ascii_case(run)
        } else {
            text[start..].starts_with(run)
        }
    }

    /// Match `text` against the compiled pattern. Backtracking glob: `Any` records
    /// a restart point so a later mismatch can re-anchor `%` one byte further.
    pub fn matches(&self, text: &[u8]) -> bool {
        let toks = &self.toks;
        let (mut ti, mut si) = (0usize, 0usize); // token / string indices
        let mut star: Option<(usize, usize)> = None; // (token idx after %, string idx)

        loop {
            match toks.get(ti) {
                Some(LikeTok::Lit(run)) => {
                    if self.lit_matches(text, si, run) {
                        si += run.len();
                        ti += 1;
                    } else if let Some((st, ss)) = star {
                        ti = st;
                        si = ss + 1;
                        star = Some((st, ss + 1));
                        if si > text.len() {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                Some(LikeTok::One) => {
                    if si < text.len() {
                        si += 1;
                        ti += 1;
                    } else if let Some((st, ss)) = star {
                        ti = st;
                        si = ss + 1;
                        star = Some((st, ss + 1));
                        if si > text.len() {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                Some(LikeTok::Any) => {
                    ti += 1;
                    star = Some((ti, si));
                }
                None => {
                    // Pattern exhausted: match iff string is too.
                    if si == text.len() {
                        return true;
                    } else if let Some((st, ss)) = star {
                        ti = st;
                        si = ss + 1;
                        star = Some((st, ss + 1));
                        if si > text.len() {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::owned_row;

    fn row(vals: Vec<OwnedValue>) -> OwnedRow {
        owned_row(vals)
    }

    /// Design 226 §5.1: the predicate comparator's mixed Int/Float arms are exact —
    /// the third comparator moves in lockstep with `compare_values`/`values_identical`
    /// (a `GuardKey` BTreeMap keys on this order, so all three must agree).
    #[test]
    fn predicate_mixed_numeric_arms_are_exact() {
        use crate::value::Value;
        use std::cmp::Ordering;
        const TWO_53: i64 = 1 << 53;
        // Above 2^53: distinct Ints stay distinct against the same Float.
        assert_eq!(
            compare_predicate_values(Value::Int(TWO_53), Value::Float(TWO_53 as f64)),
            Ordering::Equal
        );
        assert_eq!(
            compare_predicate_values(Value::Int(TWO_53 + 1), Value::Float(TWO_53 as f64)),
            Ordering::Greater
        );
        assert_eq!(
            compare_predicate_values(Value::Float(TWO_53 as f64), Value::Int(TWO_53 + 1)),
            Ordering::Less
        );
        // Below 2^53 the order is the old widened order (signed zero included).
        assert_eq!(
            compare_predicate_values(Value::Int(0), Value::Float(-0.0)),
            Ordering::Greater
        );
        assert_eq!(
            compare_predicate_values(Value::Int(3), Value::Float(3.5)),
            Ordering::Less
        );
    }

    #[test]
    fn cmp_eq_compares_non_null_and_drops_null_cells() {
        // Filter `=`: non-null cells compare by identity; a NULL cell is SQL
        // UNKNOWN → dropped (NOT "null = null" — that is `IS NULL`'s job).
        let p = CompiledPredicate::Cmp {
            col: 0,
            op: CmpOp::Eq,
            value: OwnedValue::Int(7),
        };
        assert!(p.eval(&row(vec![OwnedValue::Int(7)])));
        assert!(!p.eval(&row(vec![OwnedValue::Int(8)])));
        assert!(!p.eval(&row(vec![OwnedValue::Null])));
    }

    #[test]
    fn negated_ops_drop_null_cells() {
        // The fix: `!=` / `NOT IN` / `NOT LIKE` on a NULL cell must DROP (false),
        // not pass — SQL three-valued UNKNOWN, matching JS `createPredicate`.
        let ne = CompiledPredicate::Cmp {
            col: 0,
            op: CmpOp::Ne,
            value: OwnedValue::Int(7),
        };
        assert!(ne.eval(&row(vec![OwnedValue::Int(8)]))); // 8 != 7 → true
        assert!(!ne.eval(&row(vec![OwnedValue::Null]))); // null != 7 → UNKNOWN → drop

        let not_in = CompiledPredicate::In {
            col: 0,
            set: ValueSet::new(vec![OwnedValue::Int(1)]),
            negated: true,
        };
        assert!(not_in.eval(&row(vec![OwnedValue::Int(2)])));
        assert!(!not_in.eval(&row(vec![OwnedValue::Null])));

        let not_like = CompiledPredicate::Like {
            col: 0,
            matcher: LikeMatcher::compile(b"x%"),
            negated: true,
        };
        assert!(not_like.eval(&row(vec![OwnedValue::str("yz")]))); // not like "x%" → true
        assert!(!not_like.eval(&row(vec![OwnedValue::Null]))); // null → drop
    }

    #[test]
    fn const_is_row_independent() {
        assert!(CompiledPredicate::Const(true).eval(&row(vec![OwnedValue::Null])));
        assert!(!CompiledPredicate::Const(false).eval(&row(vec![OwnedValue::Int(1)])));
    }

    #[test]
    fn cmp_ordering_guards_nulls() {
        let p = CompiledPredicate::Cmp {
            col: 0,
            op: CmpOp::Gt,
            value: OwnedValue::Int(5),
        };
        assert!(p.eval(&row(vec![OwnedValue::Int(6)])));
        assert!(!p.eval(&row(vec![OwnedValue::Int(5)])));
        assert!(!p.eval(&row(vec![OwnedValue::Int(4)])));
        // null vs a real value has no order in a predicate → false, NOT "null < 5".
        assert!(!p.eval(&row(vec![OwnedValue::Null])));
    }

    #[test]
    fn in_and_not_in() {
        let set = ValueSet::new(vec![OwnedValue::Int(1), OwnedValue::Int(3)]);
        let p = CompiledPredicate::In {
            col: 0,
            set,
            negated: false,
        };
        assert!(p.eval(&row(vec![OwnedValue::Int(1)])));
        assert!(!p.eval(&row(vec![OwnedValue::Int(2)])));

        let set = ValueSet::new(vec![OwnedValue::Int(1), OwnedValue::Int(3)]);
        let np = CompiledPredicate::In {
            col: 0,
            set,
            negated: true,
        };
        assert!(!np.eval(&row(vec![OwnedValue::Int(1)])));
        assert!(np.eval(&row(vec![OwnedValue::Int(2)])));
    }

    #[test]
    fn is_null() {
        let p = CompiledPredicate::IsNull {
            col: 0,
            negated: false,
        };
        assert!(p.eval(&row(vec![OwnedValue::Null])));
        assert!(!p.eval(&row(vec![OwnedValue::Int(0)])));
        let np = CompiledPredicate::IsNull {
            col: 0,
            negated: true,
        };
        assert!(!np.eval(&row(vec![OwnedValue::Null])));
        assert!(np.eval(&row(vec![OwnedValue::Int(0)])));
    }

    fn like(pat: &str, text: &str) -> bool {
        LikeMatcher::compile(pat.as_bytes()).matches(text.as_bytes())
    }

    #[test]
    fn like_glob_structure() {
        assert!(like("abc", "abc"));
        assert!(!like("abc", "abd"));
        assert!(like("a%", "abcdef"));
        assert!(like("%f", "abcdef"));
        assert!(like("a%f", "abcdef"));
        assert!(like("a%f", "af"));
        assert!(!like("a%f", "afx"));
        assert!(like("%", ""));
        assert!(like("%%", "anything"));
        assert!(like("a_c", "abc"));
        assert!(!like("a_c", "ac"));
        assert!(!like("a_c", "abbc"));
        assert!(like("%a%b%", "xxaxxbxx"));
        assert!(!like("%a%b%", "xxbxxaxx"));
        // backtracking: the first `%` must give bytes back to satisfy the literal.
        assert!(like("%abc", "zzabc"));
        assert!(!like("%abc", "zzab"));
    }

    #[test]
    fn like_predicate_non_text_never_matches() {
        let p = CompiledPredicate::Like {
            col: 0,
            matcher: LikeMatcher::compile(b"%"),
            negated: false,
        };
        // `%` matches any *text*, but a null/number cell is not text → false.
        assert!(!p.eval(&row(vec![OwnedValue::Null])));
        assert!(!p.eval(&row(vec![OwnedValue::Int(0)])));
        assert!(p.eval(&row(vec![OwnedValue::str("hi")])));
    }
}
