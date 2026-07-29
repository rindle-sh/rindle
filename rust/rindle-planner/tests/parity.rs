//! **JS↔Rust planner differential parity gate.**
//!
//! Each `tests/fixtures/planner/*.json` is `{name, ast, costModel, flips}` where the
//! REAL JS planner (`planQuery`) is the ORACLE: `flips` is the flip boolean of every
//! correlated-subquery condition in a fixed traversal order. We deserialize `ast`, run
//! `plan_ast` with the SAME cost model, collect flips with the SAME traversal, and
//! assert equality. Recorded (not a live bridge) → Rust CI stays Node-free.
//!
//! The cost model is a serializable spec replicated identically here and in the JS
//! generator (`tools/gen_planner_fixtures.test.ts`): `simple` = the verbatim
//! `SimpleCostModel`; `perTable` = per-table rows/startup. Both use `fanout = 1`, so the
//! differential exercises no transcendental (exact parity). Regenerate the fixtures per
//! the generator's header comment.

use std::collections::BTreeMap;
use std::rc::Rc;

use rindle::{Ast, Condition};
use rindle_planner::{
    plan_ast, ConnectionCostModel, CostModelCost, FanoutConfidence, FanoutEst, PlannerConstraint,
    SimpleCostModel,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    name: String,
    ast: Ast,
    #[serde(rename = "costModel")]
    cost_model: CostSpec,
    flips: Vec<bool>,
}

#[derive(Deserialize)]
#[serde(tag = "kind")]
enum CostSpec {
    #[serde(rename = "simple")]
    Simple,
    #[serde(rename = "perTable")]
    PerTable {
        rows: BTreeMap<String, f64>,
        #[serde(default)]
        startup: BTreeMap<String, f64>,
        #[serde(rename = "defaultRows", default = "default_rows")]
        default_rows: f64,
    },
}

fn default_rows() -> f64 {
    100.0
}

struct PerTableModel {
    rows: BTreeMap<String, f64>,
    startup: BTreeMap<String, f64>,
    default_rows: f64,
}

impl ConnectionCostModel for PerTableModel {
    fn estimate(
        &self,
        table: &str,
        _sort: &[rindle::OrderPart],
        _filters: Option<&Condition>,
        _constraint: Option<&PlannerConstraint>,
    ) -> CostModelCost {
        CostModelCost {
            startup_cost: self.startup.get(table).copied().unwrap_or(0.0),
            rows: self.rows.get(table).copied().unwrap_or(self.default_rows),
            fanout: Rc::new(|_| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None,
            }),
        }
    }
}

fn build_model(spec: &CostSpec) -> Rc<dyn ConnectionCostModel> {
    match spec {
        CostSpec::Simple => Rc::new(SimpleCostModel),
        CostSpec::PerTable {
            rows,
            startup,
            default_rows,
        } => Rc::new(PerTableModel {
            rows: rows.clone(),
            startup: startup.clone(),
            default_rows: *default_rows,
        }),
    }
}

/// Collect the flip boolean of every correlated-subquery condition, in the SAME order
/// as the JS generator: a CSQ's own child `where` first (post-order), then this
/// condition; AND/OR left-to-right; finally each related subquery's tree.
fn collect_flips(ast: &Ast, out: &mut Vec<bool>) {
    if let Some(w) = &ast.r#where {
        walk_cond(w, out);
    }
    for csq in &ast.related {
        collect_flips(&csq.subquery, out);
    }
}

fn walk_cond(cond: &Condition, out: &mut Vec<bool>) {
    match cond {
        Condition::Simple(_) => {}
        Condition::CorrelatedSubquery(c) => {
            if let Some(w) = &c.related.subquery.r#where {
                walk_cond(w, out);
            }
            out.push(c.flip == Some(true));
        }
        Condition::And { conditions } | Condition::Or { conditions } => {
            for c in conditions {
                walk_cond(c, out);
            }
        }
    }
}

#[test]
fn planner_differential_parity() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/planner");
    let mut fixtures: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read fixtures dir {dir:?}: {e}"))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    fixtures.sort();
    assert!(!fixtures.is_empty(), "no planner fixtures found in {dir:?}");

    let mut checked = 0;
    for path in &fixtures {
        let text = std::fs::read_to_string(path).unwrap();
        let fx: Fixture =
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {path:?}: {e}"));

        let planned = plan_ast(&fx.ast, build_model(&fx.cost_model));
        let mut got = Vec::new();
        collect_flips(&planned, &mut got);

        assert_eq!(
            got, fx.flips,
            "fixture `{}`: Rust plan_ast flips {got:?} != JS planQuery flips {:?}",
            fx.name, fx.flips
        );
        checked += 1;
    }
    // Guard against the corpus silently shrinking.
    assert!(
        checked >= 15,
        "expected the planner corpus, only checked {checked}"
    );
}
