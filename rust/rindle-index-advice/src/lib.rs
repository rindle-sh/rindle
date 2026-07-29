//! Index-need derivation: from a query's wire AST, derive the SQLite `TableSource` indexes it
//! needs to be served well, mechanically applying `docs/INDEXING.md`'s rules.
//!
//! This is the **pure, std-only core** shared by two consumers:
//!   - `rindle indices suggest` (the CLI, `rindle-cli/src/indices.rs`) — offline derivation over
//!     a JS-produced shapes document, diffed against a live daemon;
//!   - the `rindled` daemon's index-coverage advisories (see
//!     `designs/INDEX-COVERAGE-ADVISORY-DESIGN.md`) — a per-query check at materialize time.
//!
//! It operates on the engine's typed [`rindle::ast::Ast`], so the derivation walks exactly the
//! shape every tier executes; neither consumer re-defines the wire format. No serde, no I/O — the
//! serde-deserialized shapes document and the SQL emission live in the CLI's I/O shell.
//!
//! Rules applied (INDEXING.md):
//!   - **R1/R2** — every `related`/`EXISTS` correlation becomes a composite on the CHILD table:
//!     correlation column(s) first, then the child's equality-filter columns (the R2 two-equality
//!     probe), then the child's `order_by` columns (the per-parent fetch is
//!     `WHERE corr = ? [AND filter = ?] ORDER BY sort`). No PK suffix — the in-engine sort
//!     tiebreak (design 204) re-applies it, and the oracle indices in shipped apps agree.
//!   - **R3** — a ROOT `order_by` gets `(sortColumns…, pk…)`, PK suffixed per the cheat-sheet (a
//!     top-level feed's boundary re-fetch is the hot O(n)-per-write case).
//!   - **R4** — selective top-level `where` columns, emitted as single-column candidates and
//!     marked selectivity-dependent: whether they earn their write cost depends on data
//!     distribution the AST can't see, so they rank below the structural R1–R3 set.
//!   - **R0/R5** are schema-level, not per-query: the PK index exists via `PRIMARY KEY`, and the
//!     report reminds about `ANALYZE`.
//!
//! Out of scope (v1): `group_by`/`having` reduce paths, and selectivity measurement — see the R4
//! marking above.

use std::collections::BTreeMap;

use rindle::ast::{Ast, Condition, CorrelatedSubquery, Dir, Op, ValuePosition};

// ----------------------------------- derived indexes -----------------------------------

/// One ordered index column. `desc` matters only for sort columns (an equality probe is
/// direction-blind), but generation is deterministic so strict comparison stays sound.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct IndexCol {
    pub name: String,
    pub desc: bool,
}

/// Which INDEXING.md rule produced a [`Suggestion`] — carried structurally (not parsed back out
/// of the reason text) so the advisory tiering in [`check`] can apply each rule's real threshold.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum NeedKind {
    /// R1/R2 correlation — a child-fetch composite or a parent-push key. The first `corr` columns
    /// are the correlation key that MUST be led, or the per-parent fetch scans the whole child
    /// table (the O(n²) hydration case, INDEXING.md:74); any trailing columns (an R2 equality
    /// filter, the child's sort) are refinement whose absence is at most `consider`.
    Correlation { corr: usize },
    /// R3 — a ROOT `order_by`. `limited` marks the hot boundary-refetch case (`order_by` + `limit`,
    /// INDEXING.md:82). The first `sort` columns are the order; the trailing PK tiebreak is
    /// optional because the in-engine sort tiebreak (design 204) re-applies it, so a bare
    /// leading-sort-column index is adequate.
    RootOrder { limited: bool, sort: usize },
    /// R4 — a selective top-level filter column; worth its write cost only if the column is
    /// selective, which the AST can't know, so it is never more than `consider`.
    Filter,
}

impl NeedKind {
    /// How many LEADING columns of the suggestion are structurally required (vs a refinement or
    /// tiebreak suffix) — the prefix an existing index must serve to keep the query off the
    /// pathological path.
    fn critical(&self, total: usize) -> usize {
        match *self {
            NeedKind::Correlation { corr } => corr.min(total),
            NeedKind::RootOrder { sort, .. } => sort.min(total),
            NeedKind::Filter => total,
        }
    }
}

/// One derived index suggestion for a table.
#[derive(Debug)]
pub struct Suggestion {
    pub table: String,
    pub cols: Vec<IndexCol>,
    /// Provenance lines, `"<query>: <rule> — <detail>"`.
    pub reasons: Vec<String>,
    /// The rule that produced this need — drives both the CLI's `?` marking and [`check`]'s tier.
    pub kind: NeedKind,
}

impl Suggestion {
    /// R4 candidates are the only selectivity-dependent ones (INDEXING.md R4): whether they earn
    /// their write cost depends on data distribution the AST can't see, so the CLI marks them `?`.
    pub fn selectivity_dependent(&self) -> bool {
        matches!(self.kind, NeedKind::Filter)
    }
}

/// Derive the raw (pre-subsumption) suggestions for ONE query's root AST: R3 + R4 apply at the
/// root, then descend into every correlation. Appends to `out`; a caller with many queries loops
/// this and then [`subsume`]s the union. `query` is the provenance label used in `reasons`.
pub fn derive_query(
    query: &str,
    ast: &Ast,
    pks: &BTreeMap<&str, &[String]>,
    out: &mut Vec<Suggestion>,
) {
    // R3 — root order_by gets the PK tiebreaker suffix (cheat-sheet: `(sortColumn, …, pk)`).
    if !ast.order_by.is_empty() {
        let mut cols: Vec<IndexCol> = ast
            .order_by
            .iter()
            .map(|p| IndexCol {
                name: p.field().to_string(),
                desc: matches!(p.dir(), Dir::Desc),
            })
            .collect();
        for pk in pks.get(&*ast.table).copied().unwrap_or_default() {
            if !cols.iter().any(|c| c.name == *pk) {
                cols.push(IndexCol {
                    name: pk.clone(),
                    desc: false,
                });
            }
        }
        out.push(Suggestion {
            table: ast.table.to_string(),
            cols,
            reasons: vec![format!(
                "{query}: R3 — root order_by{} (boundary re-fetch on every windowed write)",
                if ast.limit.is_some() { " + limit" } else { "" }
            )],
            kind: NeedKind::RootOrder {
                limited: ast.limit.is_some(),
                sort: ast.order_by.len(),
            },
        });
    }
    // R4 — selective top-level filter columns (single-column candidates).
    for col in equality_and_range_columns(ast.r#where.as_ref()) {
        out.push(Suggestion {
            table: ast.table.to_string(),
            cols: vec![IndexCol {
                name: col.clone(),
                desc: false,
            }],
            reasons: vec![format!(
                "{query}: R4 — top-level where on `{col}` (worth it only if selective)"
            )],
            kind: NeedKind::Filter,
        });
    }
    descend(query, ast, pks, out);
}

/// Shared descent: every correlation (materialized `related` or an `EXISTS` gate in the filter
/// tree) becomes an R1/R2 composite on its child, then the child descends in turn.
fn descend(query: &str, ast: &Ast, pks: &BTreeMap<&str, &[String]>, out: &mut Vec<Suggestion>) {
    for rel in &ast.related {
        correlation_composite(query, ast, rel, "related", pks, out);
    }
    for rel in exists_subqueries(ast.r#where.as_ref()) {
        correlation_composite(query, ast, rel, "exists", pks, out);
    }
}

/// Both directions of one correlation (the oracle apps index "both directions", and IVM needs
/// both):
///   - **fetch** (R1/R2/child-R3): `(childCorrelation…, childEqualityFilters…, childSort…)` on the
///     CHILD — hydration fetches the child once per parent,
///     `WHERE corr = ? [AND f = ?] ORDER BY sort`;
///   - **push**: the parent-side correlation column(s) on the PARENT — when a CHILD row changes,
///     the join pushes the delta up by fetching the parents it belongs to,
///     `WHERE parentField = ?`. Without it every child write scans the parent table.
fn correlation_composite(
    query: &str,
    parent: &Ast,
    rel: &CorrelatedSubquery,
    kind: &str,
    pks: &BTreeMap<&str, &[String]>,
    out: &mut Vec<Suggestion>,
) {
    let child = &rel.subquery;
    let mut cols: Vec<IndexCol> = rel
        .correlation
        .child_field
        .iter()
        .map(|c| IndexCol {
            name: c.to_string(),
            desc: false,
        })
        .collect();
    let mut detail = format!("correlation ({})", cols_list(&cols));
    for f in equality_columns(child.r#where.as_ref()) {
        if !cols.iter().any(|c| c.name == f) {
            cols.push(IndexCol {
                name: f.clone(),
                desc: false,
            });
            detail.push_str(&format!(" + filter `{f}` (R2)"));
        }
    }
    for p in &child.order_by {
        if !cols.iter().any(|c| c.name == p.field()) {
            cols.push(IndexCol {
                name: p.field().to_string(),
                desc: matches!(p.dir(), Dir::Desc),
            });
            detail.push_str(&format!(" + sort `{}`", p.field()));
        }
    }
    let alias = child.alias.as_deref().unwrap_or(&child.table);
    out.push(Suggestion {
        table: child.table.to_string(),
        cols,
        reasons: vec![format!("{query}: R1 — {kind} `{alias}` {detail}")],
        kind: NeedKind::Correlation {
            corr: rel.correlation.child_field.len(),
        },
    });
    // Push direction: a child-row change looks up its parents by the parent-side column(s).
    let parent_cols: Vec<IndexCol> = rel
        .correlation
        .parent_field
        .iter()
        .map(|c| IndexCol {
            name: c.to_string(),
            desc: false,
        })
        .collect();
    let list = cols_list(&parent_cols);
    let corr = parent_cols.len();
    out.push(Suggestion {
        table: parent.table.to_string(),
        cols: parent_cols,
        reasons: vec![format!(
            "{query}: R1 push — a `{}` change propagates up through `{alias}` by parent ({list})",
            child.table
        )],
        kind: NeedKind::Correlation { corr },
    });
    descend(query, child, pks, out);
}

/// Column names in equality positions (`=`, `IN`) of a filter tree — the probes an index prefix
/// can serve. Descends `and`/`or` (an OR branch's column is still probed).
fn equality_columns(cond: Option<&Condition>) -> Vec<String> {
    filter_columns(cond, /* equality_only= */ true)
}

/// Equality + range (`<`,`<=`,`>`,`>=`,`IS`) columns — the R4 candidate set.
fn equality_and_range_columns(cond: Option<&Condition>) -> Vec<String> {
    filter_columns(cond, /* equality_only= */ false)
}

fn filter_columns(cond: Option<&Condition>, equality_only: bool) -> Vec<String> {
    let mut cols = Vec::new();
    collect_filter_columns(cond, equality_only, &mut cols);
    cols
}

fn collect_filter_columns(cond: Option<&Condition>, equality_only: bool, out: &mut Vec<String>) {
    match cond {
        None => {}
        Some(Condition::Simple(s)) => {
            let indexable = match s.op {
                Op::Eq | Op::In => true,
                Op::Lt | Op::Le | Op::Gt | Op::Ge | Op::Is => !equality_only,
                // LIKE-family and negations don't seek an index prefix; NOT IN/!=/IS NOT scan.
                _ => false,
            };
            if indexable {
                if let ValuePosition::Column { name } = &s.left {
                    if !out.iter().any(|c| c == &**name) {
                        out.push(name.to_string());
                    }
                }
            }
        }
        Some(Condition::And { conditions }) | Some(Condition::Or { conditions }) => {
            for c in conditions {
                collect_filter_columns(Some(c), equality_only, out);
            }
        }
        // A nested EXISTS is handled by `descend` (it indexes the CHILD, not this table).
        Some(Condition::CorrelatedSubquery(_)) => {}
    }
}

/// `EXISTS`/`NOT EXISTS` subqueries anywhere in a filter tree.
fn exists_subqueries(cond: Option<&Condition>) -> Vec<&CorrelatedSubquery> {
    let mut found = Vec::new();
    collect_exists(cond, &mut found);
    found
}

fn collect_exists<'a>(cond: Option<&'a Condition>, out: &mut Vec<&'a CorrelatedSubquery>) {
    match cond {
        None | Some(Condition::Simple(_)) => {}
        Some(Condition::And { conditions }) | Some(Condition::Or { conditions }) => {
            for c in conditions {
                collect_exists(Some(c), out);
            }
        }
        Some(Condition::CorrelatedSubquery(cs)) => out.push(&cs.related),
    }
}

// ----------------------------------- subsumption -----------------------------------

/// Dedupe + subsume: a suggestion is dropped when another's columns carry it as a PREFIX (its
/// probes/sort are served by the wider index's left edge). A single-column equality/R4 candidate
/// is direction-blind, so any index LEADING with that column covers it. Reasons merge into the
/// survivor; structural (R1–R3) suggestions absorb selectivity-dependent ones, never the reverse.
pub fn subsume(mut raw: Vec<Suggestion>) -> Vec<Suggestion> {
    // Widest first within a table so survivors absorb the narrower ones deterministically.
    raw.sort_by(|a, b| {
        (a.table.as_str(), std::cmp::Reverse(a.cols.len()))
            .cmp(&(b.table.as_str(), std::cmp::Reverse(b.cols.len())))
    });
    let mut kept: Vec<Suggestion> = Vec::new();
    'next: for s in raw {
        for k in kept.iter_mut() {
            if k.table == s.table && covers(&k.cols, &s.cols) {
                for r in s.reasons {
                    if !k.reasons.contains(&r) {
                        k.reasons.push(r);
                    }
                }
                // A structural need (R1/R3) absorbed by an R4 survivor makes the survivor
                // structural: the wider index serves the structural probe, so the merged need is
                // no longer merely selectivity-dependent (mirrors the old `&=` on the bool field).
                if matches!(k.kind, NeedKind::Filter) && !matches!(s.kind, NeedKind::Filter) {
                    k.kind = s.kind.clone();
                }
                continue 'next;
            }
        }
        kept.push(s);
    }
    kept.sort_by(|a, b| (&a.table, &a.cols).cmp(&(&b.table, &b.cols)));
    kept
}

/// Does index `wide` serve everything `narrow` does? True when `narrow` is a prefix of `wide`
/// (direction-blind for a 1-column narrow — an equality probe works either way).
pub fn covers(wide: &[IndexCol], narrow: &[IndexCol]) -> bool {
    if narrow.len() > wide.len() {
        return false;
    }
    if narrow.len() == 1 {
        return wide[0].name == narrow[0].name;
    }
    narrow == &wide[..narrow.len()]
}

/// Render an index's ordered key columns as `"a, b desc"` — used in provenance reasons and in the
/// CLI's human output.
pub fn cols_list(cols: &[IndexCol]) -> String {
    cols.iter()
        .map(|c| {
            if c.desc {
                format!("{} desc", c.name)
            } else {
                c.name.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

// ----------------------------------- advisory tiers -----------------------------------

/// How loud a missing index is (design INDEX-COVERAGE-ADVISORY-DESIGN.md §4). Only [`Degraded`]
/// drives a dev-mode warning / CI failure; [`Consider`] is info that stays silent by default.
///
/// [`Degraded`]: Severity::Degraded
/// [`Consider`]: Severity::Consider
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Severity {
    /// A measured superlinear miss: the pathological per-parent scan (R1/R2) or the O(n)-per-write
    /// boundary re-fetch (R3 `order_by` + `limit`). The threshold is deliberately looser than the
    /// suggester's ideal set — it fires only where INDEXING.md measured a real blow-up.
    Degraded,
    /// Selectivity- or stats-dependent, or a refinement the engine already tolerates: an R4 filter,
    /// an unlimited sort, or a composite whose correlation prefix is already indexed (the R2 ANALYZE
    /// case) / whose bare sort-column index is adequate under the design-204 tiebreak.
    Consider,
}

/// One classified advisory: a need [`check`] found not fully served by the live indexes.
#[derive(Clone, Debug)]
pub struct Advisory {
    pub table: String,
    /// The ideal index to create (feed to [`create_index_sql`] for a paste-ready `CREATE INDEX`).
    pub cols: Vec<IndexCol>,
    pub severity: Severity,
    /// The originating [`Suggestion`]'s first provenance line (`"<query>: <rule> — <detail>"`).
    pub reason: String,
    /// An existing index already leads with the need's first column but doesn't cover the whole
    /// thing (a `Consider` refinement, never a `Degraded` — the seek prefix works). `false` when
    /// nothing even leads with the first column.
    pub partial: bool,
}

/// Classify each subsumed need against the columns actually present, at the warn threshold (§4).
/// `existing` maps a table to its live indexes, each as ordered key columns (as
/// `pragma_index_xinfo` reports them). Fully-covered needs yield nothing.
pub fn check(
    needs: &[Suggestion],
    existing: &BTreeMap<String, Vec<Vec<IndexCol>>>,
) -> Vec<Advisory> {
    let mut out = Vec::new();
    for need in needs {
        if need.cols.is_empty() {
            continue;
        }
        let idxs: &[Vec<IndexCol>] = existing.get(&need.table).map(Vec::as_slice).unwrap_or(&[]);
        let crit = &need.cols[..need.kind.critical(need.cols.len()).max(1)];
        let crit_covered = idxs.iter().any(|e| covers(e, crit));

        if crit_covered {
            // The structurally-required prefix is served, so the pathological path is avoided.
            match need.kind {
                // R3 under design-204 / R4: leading the critical prefix IS adequate — no advisory.
                NeedKind::RootOrder { .. } | NeedKind::Filter => continue,
                // A correlation whose corr prefix is indexed but whose full composite is not: the
                // per-parent fetch already seeks (no O(n²)); the R2 filter / child sort is a
                // planner/ANALYZE-dependent refinement → `Consider`, not `Degraded`.
                NeedKind::Correlation { .. } => {
                    if idxs.iter().any(|e| covers(e, &need.cols)) {
                        continue; // full composite present
                    }
                    out.push(advisory(need, Severity::Consider, true));
                }
            }
            continue;
        }

        // The critical prefix is NOT even led by any index.
        let severity = match need.kind {
            NeedKind::Filter => Severity::Consider, // selectivity-dependent (R4)
            // An unlimited root sort is worth an index but not the hot boundary-refetch case.
            NeedKind::RootOrder { limited: false, .. } => Severity::Consider,
            NeedKind::RootOrder { limited: true, .. } | NeedKind::Correlation { .. } => {
                Severity::Degraded
            }
        };
        out.push(advisory(need, severity, false));
    }
    out
}

fn advisory(need: &Suggestion, severity: Severity, partial: bool) -> Advisory {
    Advisory {
        table: need.table.clone(),
        cols: need.cols.clone(),
        severity,
        reason: need.reasons.first().cloned().unwrap_or_default(),
        partial,
    }
}

// ----------------------------------- SQL emit -----------------------------------

/// A stable, collision-resistant index name for `table (cols…)` — `ix_<table>__<col>[_desc]_…`.
/// Shared by the CLI's emitted DDL and the daemon advisory so both name an index identically.
pub fn index_name(table: &str, cols: &[IndexCol]) -> String {
    let cols = cols
        .iter()
        .map(|c| {
            let mut n = c.name.replace(|ch: char| !ch.is_alphanumeric(), "_");
            if c.desc {
                n.push_str("_desc");
            }
            n
        })
        .collect::<Vec<_>>()
        .join("_");
    format!("ix_{table}__{cols}")
}

/// A paste-ready `CREATE INDEX IF NOT EXISTS … ON "table" ("col" [DESC], …);` for `table (cols…)`.
pub fn create_index_sql(table: &str, cols: &[IndexCol]) -> String {
    let spec = cols
        .iter()
        .map(|c| {
            if c.desc {
                format!("\"{}\" DESC", c.name)
            } else {
                format!("\"{}\"", c.name)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE INDEX IF NOT EXISTS \"{}\" ON \"{table}\" ({spec});",
        index_name(table, cols)
    )
}

// ----------------------------------- tests -----------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    /// Build an `Ast` from wire JSON (the CLI oracle's pattern), then derive + subsume its
    /// suggestions — the same path a caller runs. `pk_pairs` is the table→primary-key catalog.
    fn suggest(query: &str, ast: Value, pk_pairs: &[(&str, &[String])]) -> Vec<Suggestion> {
        let pks: BTreeMap<&str, &[String]> = pk_pairs.iter().copied().collect();
        let ast: Ast = serde_json::from_value(ast).expect("valid wire ast");
        let mut out = Vec::new();
        derive_query(query, &ast, &pks, &mut out);
        subsume(out)
    }

    fn find<'a>(s: &'a [Suggestion], table: &str, cols: &str) -> Option<&'a Suggestion> {
        s.iter()
            .find(|x| x.table == table && cols_list(&x.cols) == cols)
    }

    /// A `col <op> literal` simple condition.
    fn simple(op: &str, col: &str) -> Value {
        json!({ "type": "simple", "op": op,
            "left": { "type": "column", "name": col },
            "right": { "type": "literal", "value": "x" } })
    }

    // -------------------------------- covers (unit) --------------------------------

    #[test]
    fn subsume_is_direction_blind_only_for_single_column_candidates() {
        // (createdAt desc, id) covers a bare (createdAt) equality candidate…
        let wide = vec![
            IndexCol {
                name: "createdAt".into(),
                desc: true,
            },
            IndexCol {
                name: "id".into(),
                desc: false,
            },
        ];
        assert!(covers(
            &wide,
            &[IndexCol {
                name: "createdAt".into(),
                desc: false
            }]
        ));
        // …but not a two-column prefix with mismatched direction.
        assert!(!covers(
            &wide,
            &[
                IndexCol {
                    name: "createdAt".into(),
                    desc: false
                },
                IndexCol {
                    name: "id".into(),
                    desc: false
                },
            ]
        ));
    }

    // -------------------------------- R3: root order_by --------------------------------

    #[test]
    fn r3_multicolumn_sort_appends_pk_tiebreak_and_notes_limit() {
        let id = vec!["id".to_string()];
        let got = suggest(
            "q",
            json!({ "table": "t", "orderBy": [["a", "asc"], ["b", "desc"]], "limit": 10 }),
            &[("t", &id)],
        );
        let s = find(&got, "t", "a, b desc, id").expect("multi-col sort + pk tiebreak");
        assert!(!s.selectivity_dependent());
        assert!(s.reasons[0].contains("R3") && s.reasons[0].contains("+ limit"));
    }

    #[test]
    fn r3_pk_already_in_sort_is_not_duplicated() {
        let id = vec!["id".to_string()];
        let got = suggest(
            "q",
            json!({ "table": "t", "orderBy": [["id", "asc"]] }),
            &[("t", &id)],
        );
        assert!(
            find(&got, "t", "id").is_some(),
            "sort key IS the pk — no suffix"
        );
        assert!(
            find(&got, "t", "id, id").is_none(),
            "pk must not be appended twice"
        );
    }

    #[test]
    fn r3_without_pk_in_catalog_emits_bare_sort_and_omits_limit() {
        // No PK entry for `t` → `unwrap_or_default()` → no tiebreak suffix; no limit → no "+ limit".
        let got = suggest(
            "q",
            json!({ "table": "t", "orderBy": [["a", "desc"]] }),
            &[],
        );
        let s = find(&got, "t", "a desc").expect("bare sort index");
        assert!(
            !s.reasons[0].contains("+ limit"),
            "order_by without limit omits the limit note"
        );
    }

    // -------------------------------- R4: top-level where --------------------------------

    #[test]
    fn r4_range_operator_yields_selectivity_dependent_candidate() {
        for op in [">", "<", ">=", "<=", "IS"] {
            let got = suggest(
                "q",
                json!({ "table": "t", "where": simple(op, "score") }),
                &[],
            );
            let s = find(&got, "t", "score")
                .unwrap_or_else(|| panic!("range op {op} should yield an R4 candidate"));
            assert!(
                s.selectivity_dependent(),
                "R4 candidates are selectivity-dependent ({op})"
            );
            assert!(s.reasons[0].contains("R4"));
        }
    }

    #[test]
    fn r4_surviving_equality_candidate_stays_marked_selectivity_dependent() {
        // Nothing structural touches `status`, so the candidate survives subsumption as a "?".
        let got = suggest(
            "q",
            json!({ "table": "t", "where": simple("=", "status") }),
            &[],
        );
        let s = find(&got, "t", "status").expect("equality R4 candidate");
        assert!(
            s.selectivity_dependent(),
            "a surviving R4 candidate is emitted as selectivity-dependent"
        );
    }

    #[test]
    fn non_indexable_ops_produce_no_candidate() {
        for op in ["!=", "NOT IN", "IS NOT", "LIKE", "NOT LIKE", "ILIKE"] {
            let got = suggest("q", json!({ "table": "t", "where": simple(op, "x") }), &[]);
            assert!(
                got.is_empty(),
                "op `{op}` must not seek an index prefix, got {got:?}"
            );
        }
    }

    // -------------------------------- OR trees --------------------------------

    #[test]
    fn or_branches_are_walked_for_both_r4_columns_and_exists_gates() {
        let id = vec!["id".to_string()];
        let got = suggest(
            "q",
            json!({
                "table": "parent",
                "where": { "type": "or", "conditions": [
                    simple("=", "flag"),
                    { "type": "correlatedSubquery", "op": "EXISTS", "related": {
                        "correlation": { "parentField": ["id"], "childField": ["parentId"] },
                        "subquery": { "table": "child" } } }
                ]}
            }),
            &[("parent", &id)],
        );
        assert!(
            find(&got, "parent", "flag").is_some(),
            "R4 column collected from an OR branch"
        );
        assert!(
            find(&got, "child", "parentId").is_some(),
            "EXISTS inside an OR indexes the child"
        );
        assert!(
            find(&got, "parent", "id").is_some(),
            "…and its push-direction parent index"
        );
    }

    // -------------------------------- composite correlation --------------------------------

    #[test]
    fn composite_correlation_indexes_all_key_columns_both_directions() {
        let got = suggest(
            "q",
            json!({
                "table": "parent",
                "related": [ {
                    "correlation": { "parentField": ["a", "b"], "childField": ["x", "y"] },
                    "subquery": { "table": "child" }
                } ]
            }),
            &[],
        );
        assert!(
            find(&got, "child", "x, y").is_some(),
            "fetch side: child composite (x, y)"
        );
        assert!(
            find(&got, "parent", "a, b").is_some(),
            "push side: parent composite (a, b)"
        );
    }

    #[test]
    fn exists_with_nonindexable_child_filter_indexes_correlation_only() {
        let id = vec!["id".to_string()];
        // The child filter is `!=` (non-indexable) so it must NOT extend the R2 composite.
        let got = suggest(
            "q",
            json!({
                "table": "parent",
                "where": { "type": "correlatedSubquery", "op": "EXISTS", "related": {
                    "correlation": { "parentField": ["id"], "childField": ["pid"] },
                    "subquery": { "table": "child", "where": simple("!=", "deleted") } } }
            }),
            &[("parent", &id)],
        );
        assert!(
            find(&got, "child", "pid").is_some(),
            "correlation-only composite"
        );
        assert!(
            find(&got, "child", "pid, deleted").is_none(),
            "non-indexable filter excluded"
        );
    }

    #[test]
    fn exists_with_indexable_child_filter_extends_the_composite_r2() {
        let id = vec!["id".to_string()];
        let got = suggest(
            "q",
            json!({
                "table": "parent",
                "where": { "type": "correlatedSubquery", "op": "EXISTS", "related": {
                    "correlation": { "parentField": ["id"], "childField": ["pid"] },
                    "subquery": { "table": "child", "where": simple("=", "kind") } } }
            }),
            &[("parent", &id)],
        );
        let s = find(&got, "child", "pid, kind").expect("R2 two-equality composite");
        assert!(
            s.reasons.iter().any(|r| r.contains("R2")),
            "reason names the R2 filter"
        );
    }

    // -------------------------------- check(): advisory tiers (§4) --------------------------------

    /// An index spec `[(col, desc), …]` → its ordered key columns.
    fn ix(cols: &[(&str, bool)]) -> Vec<IndexCol> {
        cols.iter()
            .map(|(n, d)| IndexCol {
                name: n.to_string(),
                desc: *d,
            })
            .collect()
    }

    /// The per-table live-index catalog `check` consumes.
    fn live(pairs: Vec<(&str, Vec<Vec<IndexCol>>)>) -> BTreeMap<String, Vec<Vec<IndexCol>>> {
        pairs.into_iter().map(|(t, v)| (t.to_string(), v)).collect()
    }

    fn advice_for<'a>(a: &'a [Advisory], table: &str, cols: &str) -> Option<&'a Advisory> {
        a.iter()
            .find(|x| x.table == table && cols_list(&x.cols) == cols)
    }

    fn album_tracks_related() -> Value {
        json!({ "table": "album", "related": [ {
            "correlation": { "parentField": ["id"], "childField": ["albumId"] },
            "subquery": { "table": "track" } } ] })
    }

    #[test]
    fn check_flags_an_unindexed_correlation_as_degraded() {
        let id = vec!["id".to_string()];
        let needs = suggest(
            "q",
            album_tracks_related(),
            &[("album", &id), ("track", &id)],
        );
        // album has its (rowid) PK; track has nothing.
        let existing = live(vec![
            ("album", vec![ix(&[("id", false)])]),
            ("track", vec![]),
        ]);
        let adv = check(&needs, &existing);
        let track = advice_for(&adv, "track", "albumId").expect("child-fetch advisory");
        assert_eq!(track.severity, Severity::Degraded);
        assert!(!track.partial, "nothing leads with albumId");
        // The push side album(id) is served by the PK → no advisory.
        assert!(
            advice_for(&adv, "album", "id").is_none(),
            "the PK covers the push-side parent lookup"
        );
    }

    #[test]
    fn check_stays_silent_when_the_correlation_is_indexed() {
        let id = vec!["id".to_string()];
        let needs = suggest(
            "q",
            album_tracks_related(),
            &[("album", &id), ("track", &id)],
        );
        let existing = live(vec![
            ("album", vec![ix(&[("id", false)])]),
            ("track", vec![ix(&[("albumId", false)])]),
        ]);
        assert!(
            check(&needs, &existing).is_empty(),
            "fully covered → no advisory"
        );
    }

    #[test]
    fn check_bare_sort_column_index_is_adequate_for_r3_limit() {
        // design-204: (createdAt) alone serves ORDER BY createdAt DESC LIMIT n (the engine
        // re-applies the pk tiebreak), so NO advisory despite the ideal being (createdAt desc, id).
        let id = vec!["id".to_string()];
        let needs = suggest(
            "feed",
            json!({ "table": "issue", "orderBy": [["createdAt", "desc"]], "limit": 50 }),
            &[("issue", &id)],
        );
        let existing = live(vec![("issue", vec![ix(&[("createdAt", true)])])]);
        assert!(
            check(&needs, &existing).is_empty(),
            "a bare (createdAt) index is adequate under the design-204 tiebreak"
        );
    }

    #[test]
    fn check_unindexed_r3_limit_is_degraded() {
        let id = vec!["id".to_string()];
        let needs = suggest(
            "feed",
            json!({ "table": "issue", "orderBy": [["createdAt", "desc"]], "limit": 50 }),
            &[("issue", &id)],
        );
        // Only the PK exists — it leads with id, not createdAt, so it doesn't serve the order.
        let adv = check(&needs, &live(vec![("issue", vec![ix(&[("id", false)])])]));
        let feed = advice_for(&adv, "issue", "createdAt desc, id").expect("R3 advisory");
        assert_eq!(feed.severity, Severity::Degraded);
    }

    #[test]
    fn check_unlimited_root_sort_is_only_consider() {
        let id = vec!["id".to_string()];
        let needs = suggest(
            "list",
            json!({ "table": "issue", "orderBy": [["createdAt", "desc"]] }),
            &[("issue", &id)],
        );
        let adv = check(&needs, &live(vec![("issue", vec![ix(&[("id", false)])])]));
        let s = advice_for(&adv, "issue", "createdAt desc, id").expect("R3 advisory");
        assert_eq!(
            s.severity,
            Severity::Consider,
            "no limit → not the hot boundary-refetch case"
        );
    }

    #[test]
    fn check_single_column_correlation_index_downgrades_r2_to_consider() {
        // The EXISTS gate wants (albumId, genreId); (albumId) alone already avoids the O(n²) scan,
        // so the missing composite is `Consider`, not `Degraded` (the ANALYZE case, §4).
        let id = vec!["id".to_string()];
        let needs = suggest(
            "q",
            json!({ "table": "album",
                "where": { "type": "correlatedSubquery", "op": "EXISTS", "related": {
                    "correlation": { "parentField": ["id"], "childField": ["albumId"] },
                    "subquery": { "table": "track", "where": {
                        "type": "simple", "op": "=",
                        "left": { "type": "column", "name": "genreId" },
                        "right": { "type": "literal", "value": 1 } } } } } }),
            &[("album", &id), ("track", &id)],
        );
        let existing = live(vec![
            ("album", vec![ix(&[("id", false)])]),
            ("track", vec![ix(&[("albumId", false)])]),
        ]);
        let adv = check(&needs, &existing);
        let track = advice_for(&adv, "track", "albumId, genreId").expect("R2 composite advisory");
        assert_eq!(track.severity, Severity::Consider);
        assert!(
            track.partial,
            "corr prefix indexed, filter refinement missing"
        );
    }

    #[test]
    fn check_r4_filter_is_consider() {
        let needs = suggest(
            "q",
            json!({ "table": "t", "where": simple("=", "status") }),
            &[],
        );
        let adv = check(&needs, &BTreeMap::new());
        let s = advice_for(&adv, "t", "status").expect("R4 advisory");
        assert_eq!(s.severity, Severity::Consider);
    }

    // -------------------------------- create_index_sql --------------------------------

    #[test]
    fn create_index_sql_quotes_and_orders() {
        let cols = ix(&[("createdAt", true), ("id", false)]);
        assert_eq!(
            create_index_sql("issue", &cols),
            "CREATE INDEX IF NOT EXISTS \"ix_issue__createdAt_desc_id\" ON \"issue\" (\"createdAt\" DESC, \"id\");"
        );
    }
}
