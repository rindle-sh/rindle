//! **Plan quality: does the cost-based planner pick plans that make SQLite do less
//! work?**
//!
//! The differential parity suite proves the Rust planner makes the *same* flip
//! decisions as JS; the result-preservation suite proves a flipped plan returns the
//! *same rows*. Neither answers the question the planner exists to answer: *is the
//! chosen plan actually cheaper to execute?* This suite does, by measuring SQLite's
//! own per-statement work counters (`sqlite3_stmt_status`: VM steps, full-scan steps,
//! sorts) — the Rust analogue of the JS planner's "scanned rows" tracking — for the
//! planned query vs every alternative, over a real SQLite `TableSource` pipeline.
//!
//! Two kinds of check:
//!
//! 1. **Directional** — on a fixture where flipping is the clear win (tiny child,
//!    huge parent) the planner flips AND the flipped plan does strictly less work;
//!    on the mirror fixture (huge child, tiny parent) it keeps the semi-join AND
//!    semi does strictly less work. This proves it is not just "always flip".
//! 2. **Exhaustive min-scan oracle** — the flip space of a query with K correlated
//!    EXISTS is only 2^K plans. We run *all* of them, measure each, and assert the
//!    planner's choice is a measured-minimum plan. No hand-labelled "best plan" — the
//!    executor decides what is best and we check the planner agreed.
//!
//! Deterministic (counts are a function of data + plan, not wall-clock), so it gates
//! in CI. Run with `--features scan-stats,testkit`; off by default so no
//! instrumentation ships in production.
#![cfg(all(feature = "scan-stats", feature = "testkit"))]

use std::rc::Rc;

use rindle::testkit::{run_fetch_test_ast_with_backend, TableSpec};
use rindle::value::{OwnedValue as V, SourceSchema};
use rindle::{
    Ast, CaughtNode, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir,
    ExistsOp, OrderPart,
};
use rindle_planner::plan_ast;
use rindle_sqlite::testkit::{cost_model_db, sqlite_backend_with_scan_sink, ScanSink, ScanStats};
use rindle_sqlite::SqliteCostModel;

// ---------------------------------------------------------------------------
// Schemas & AST builders
// ---------------------------------------------------------------------------

// issue(id, x); PK id. (The EXISTS relationships "comments"/"labels" are query-local,
// derived from the AST at build time — a source schema declares none.)
fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
// A child(id, issueID); PK id; issueID is NOT indexed (so a semi-probe full-scans it).
fn child_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}

/// One `EXISTS(child WHERE child.issueID = issue.id)` condition with a chosen flip.
fn exists_cond(rel: &str, child_table: &str, flip: Option<bool>) -> Condition {
    Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                table: child_table.into(),
                alias: Some(rel.into()),
                order_by: vec![OrderPart("id".into(), Dir::Asc)],
                ..Default::default()
            }),
            system: None,
        },
        op: ExistsOp::Exists,
        flip,
        scalar: None,
        plan_id: None,
    })
}

/// `issue WHERE EXISTS(comments)`.
fn single_exists_ast(flip: Option<bool>) -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        r#where: Some(exists_cond("comments", "comment", flip)),
        ..Default::default()
    }
}

/// `issue WHERE EXISTS(comments) AND EXISTS(labels)`.
fn two_exists_ast(flip_comments: Option<bool>, flip_labels: Option<bool>) -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        r#where: Some(Condition::And {
            conditions: vec![
                exists_cond("comments", "comment", flip_comments),
                exists_cond("labels", "label", flip_labels),
            ],
        }),
        ..Default::default()
    }
}

/// The plan-quality correctness invariant: which **parent rows pass the filter**,
/// with the EXISTS child subtree stripped off before comparison.
///
/// We deliberately compare only the top-level rows, NOT the full tree. A non-flipped
/// EXISTS materializes at most `EXISTS_LIMIT` (= 3) children per parent as an
/// existence witness (faithful to JS), whereas a flipped EXISTS drives off the child
/// and materializes all of them — so the EXISTS *child subtree* legitimately differs
/// between plans once a parent has >3 matches. What every plan must agree on is the
/// set of surviving parents, which is exactly what the planner's flip cannot change.
/// (`CaughtNode` carries a value-aware `PartialEq`; `OwnedValue` has none, so we
/// compare via stripped `CaughtNode`s rather than the raw rows.)
fn parent_rows(nodes: &[CaughtNode]) -> Vec<CaughtNode> {
    nodes
        .iter()
        .map(|n| CaughtNode {
            row: n.row.clone(),
            relationships: Default::default(),
        })
        .collect()
}

fn single_flip(ast: &Ast) -> Option<bool> {
    match ast.r#where.as_ref().unwrap() {
        Condition::CorrelatedSubquery(c) => c.flip,
        _ => panic!("expected an EXISTS where"),
    }
}

fn and_flips(ast: &Ast) -> (Option<bool>, Option<bool>) {
    match ast.r#where.as_ref().unwrap() {
        Condition::And { conditions } => {
            let flip = |i: usize| match &conditions[i] {
                Condition::CorrelatedSubquery(c) => c.flip,
                other => panic!("expected EXISTS at AND[{i}], got {other:?}"),
            };
            (flip(0), flip(1))
        }
        _ => panic!("expected an AND where"),
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn issues(n: i64) -> Vec<Vec<V>> {
    (1..=n).map(|i| vec![V::Int(i), V::Int(i * 10)]).collect()
}
fn children_on(issue_ids: &[i64]) -> Vec<Vec<V>> {
    issue_ids
        .iter()
        .enumerate()
        .map(|(k, &iss)| vec![V::Int(1_000 + k as i64), V::Int(iss)])
        .collect()
}

fn specs_single(n_issues: i64, comment_issue_ids: &[i64]) -> Vec<TableSpec> {
    vec![
        TableSpec::new("issue", issue_schema(), issues(n_issues)),
        TableSpec::new("comment", child_schema(), children_on(comment_issue_ids)),
    ]
}

fn specs_two(n_issues: i64, comment_ids: &[i64], label_ids: &[i64]) -> Vec<TableSpec> {
    vec![
        TableSpec::new("issue", issue_schema(), issues(n_issues)),
        TableSpec::new("comment", child_schema(), children_on(comment_ids)),
        TableSpec::new("label", child_schema(), children_on(label_ids)),
    ]
}

// ---------------------------------------------------------------------------
// Measurement & planning
// ---------------------------------------------------------------------------

/// Hydrate `ast` over a real SQLite `TableSource` pipeline and return the result tree
/// plus the total SQLite work it caused (summed across every leaf `SELECT`).
fn measure(specs: Vec<TableSpec>, ast: &Ast) -> (Vec<CaughtNode>, ScanStats) {
    let sink: ScanSink = Rc::new(std::cell::Cell::new(ScanStats::default()));
    let result =
        run_fetch_test_ast_with_backend(specs, sqlite_backend_with_scan_sink(sink.clone()), ast)
            .expect("AST must be buildable");
    (result, sink.get())
}

/// Run the real `SqliteCostModel` (over an ANALYZE'd DB matching `specs`) to plan the
/// flips for `unplanned`.
fn planned(specs: &[TableSpec], unplanned: &Ast) -> Ast {
    let model = SqliteCostModel::new(cost_model_db(specs));
    plan_ast(unplanned, Rc::new(model))
}

// ---------------------------------------------------------------------------
// 1. Directional: flip is the clear win — planner flips AND it scans less.
// ---------------------------------------------------------------------------

#[test]
fn flips_huge_parent_tiny_child_and_scans_less() {
    // 200 issues, only issues 1/2/3 have comments (6 rows). The semi-join full-scans
    // the (un-indexed) comment table once per *non-matching* issue → ~197 wasted
    // scans; the flipped plan scans the 6 comments once and probes issue by PK.
    let comment_ids = [1, 1, 2, 2, 3, 3];

    let plan = planned(&specs_single(200, &comment_ids), &single_exists_ast(None));
    assert_eq!(
        single_flip(&plan),
        Some(true),
        "planner should flip: tiny child (6) vs huge parent (200)"
    );

    let (semi_res, semi) = measure(
        specs_single(200, &comment_ids),
        &single_exists_ast(Some(false)),
    );
    let (flip_res, flip) = measure(
        specs_single(200, &comment_ids),
        &single_exists_ast(Some(true)),
    );

    assert_eq!(
        parent_rows(&semi_res),
        parent_rows(&flip_res),
        "flipping must preserve which parents pass the filter"
    );
    assert_eq!(semi_res.len(), 3, "only issues 1/2/3 have comments");
    eprintln!("[flip-wins] semi {semi:?} | flipped {flip:?} (planner chose flipped)");
    assert!(
        flip.vm_steps < semi.vm_steps,
        "flipped plan should do strictly less SQLite work: flip {flip:?} vs semi {semi:?}"
    );
}

// ---------------------------------------------------------------------------
// 2. Directional: flip would lose — planner keeps semi AND it scans less.
// ---------------------------------------------------------------------------

#[test]
fn keeps_semi_tiny_parent_huge_child_and_scans_less() {
    // 4 issues, 400 comments (100 per issue, every issue matches). The semi-join
    // probes the child 4 times (each finds a match immediately → early termination);
    // the flipped plan scans all 400 comments and probes issue 400 times.
    let comment_ids: Vec<i64> = (0..400).map(|k| 1 + (k % 4)).collect();

    let plan = planned(&specs_single(4, &comment_ids), &single_exists_ast(None));
    assert_eq!(
        single_flip(&plan),
        Some(false),
        "planner should keep the semi-join: huge child (400) vs tiny parent (4)"
    );

    let (semi_res, semi) = measure(
        specs_single(4, &comment_ids),
        &single_exists_ast(Some(false)),
    );
    let (flip_res, flip) = measure(
        specs_single(4, &comment_ids),
        &single_exists_ast(Some(true)),
    );

    assert_eq!(
        parent_rows(&semi_res),
        parent_rows(&flip_res),
        "flipping must preserve which parents pass the filter"
    );
    assert_eq!(semi_res.len(), 4, "every issue has comments");
    eprintln!("[semi-wins] semi {semi:?} | flipped {flip:?} (planner kept semi)");
    assert!(
        semi.vm_steps < flip.vm_steps,
        "semi-join should do strictly less SQLite work here: semi {semi:?} vs flip {flip:?}"
    );
}

// ---------------------------------------------------------------------------
// 3. Exhaustive min-scan oracle, K=1: enumerate both plans, assert the planner
//    picked a measured minimum. Covers several data shapes.
// ---------------------------------------------------------------------------

fn assert_planner_picks_min_single(label: &str, n_issues: i64, comment_ids: &[i64]) {
    let (semi_res, semi) = measure(
        specs_single(n_issues, comment_ids),
        &single_exists_ast(Some(false)),
    );
    let (flip_res, flip) = measure(
        specs_single(n_issues, comment_ids),
        &single_exists_ast(Some(true)),
    );
    assert_eq!(
        parent_rows(&semi_res),
        parent_rows(&flip_res),
        "[{label}] all plans must select the same parent rows"
    );

    let chosen = single_flip(&planned(
        &specs_single(n_issues, comment_ids),
        &single_exists_ast(None),
    ));
    let chosen_cost = if chosen == Some(true) {
        flip.vm_steps
    } else {
        semi.vm_steps
    };
    let min_cost = semi.vm_steps.min(flip.vm_steps);

    eprintln!(
        "[{label}] semi {} vm / {} fullscan | flipped {} vm / {} fullscan | planner chose flip={chosen:?}",
        semi.vm_steps, semi.fullscan_steps, flip.vm_steps, flip.fullscan_steps
    );
    assert_eq!(
        chosen_cost, min_cost,
        "[{label}] planner chose flip={chosen:?} (cost {chosen_cost}) but the measured minimum is \
         {min_cost} (semi {}, flip {})",
        semi.vm_steps, flip.vm_steps
    );
}

#[test]
fn planner_picks_measured_minimum_single_exists() {
    // Flip wins by a mile.
    assert_planner_picks_min_single("flip-wins", 200, &[1, 1, 2, 2, 3, 3]);
    // Semi wins by a mile.
    let many: Vec<i64> = (0..400).map(|k| 1 + (k % 4)).collect();
    assert_planner_picks_min_single("semi-wins", 4, &many);
    // Moderate asymmetry, flip still favoured.
    assert_planner_picks_min_single("flip-moderate", 60, &[1, 2, 3, 4, 5, 6, 7, 8]);
}

// ---------------------------------------------------------------------------
// 4. Exhaustive min-scan oracle, K=2: `EXISTS(comments) AND EXISTS(labels)`.
//    All 4 flip assignments enumerated; planner's choice must be a measured min.
// ---------------------------------------------------------------------------

#[test]
fn planner_picks_measured_minimum_two_exists() {
    // 100 issues; comments are highly selective (only issues 1/2/3); every issue has
    // a label. The AND result is {1,2,3}. The cheap plan drives off the selective
    // comment side; the planner must land on a measured-minimum flip assignment.
    let n = 100;
    let comment_ids = [1, 1, 2, 2, 3, 3];
    let label_ids: Vec<i64> = (1..=n).collect(); // one label per issue

    let variants = [(false, false), (false, true), (true, false), (true, true)];
    let mut results = Vec::new();
    let mut min_cost = u64::MAX;
    for &(fc, fl) in &variants {
        let (res, stats) = measure(
            specs_two(n, &comment_ids, &label_ids),
            &two_exists_ast(Some(fc), Some(fl)),
        );
        min_cost = min_cost.min(stats.vm_steps);
        results.push((fc, fl, res, stats.vm_steps));
    }

    // All assignments must agree on which parents pass (correctness across the whole
    // plan space; the EXISTS child subtree may differ — see `parent_rows`).
    let first = parent_rows(&results[0].2);
    assert_eq!(first.len(), 3, "AND result should be issues 1/2/3");
    for (fc, fl, res, _) in &results {
        assert_eq!(
            parent_rows(res),
            first,
            "plan ({fc},{fl}) changed which parents pass"
        );
    }

    // The planner's chosen assignment must be a measured minimum.
    let (cf, lf) = and_flips(&planned(
        &specs_two(n, &comment_ids, &label_ids),
        &two_exists_ast(None, None),
    ));
    let chosen = (cf == Some(true), lf == Some(true));
    let chosen_cost = results
        .iter()
        .find(|(fc, fl, _, _)| (*fc, *fl) == chosen)
        .map(|(_, _, _, c)| *c)
        .expect("planner choice must be one of the enumerated variants");

    assert_eq!(
        chosen_cost,
        min_cost,
        "planner chose {chosen:?} (cost {chosen_cost}) but the measured minimum over all 4 plans \
         is {min_cost}; costs were {:?}",
        results
            .iter()
            .map(|(fc, fl, _, c)| ((*fc, *fl), *c))
            .collect::<Vec<_>>()
    );
}
