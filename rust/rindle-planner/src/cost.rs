//! The cost-model seam (port of the cost types in `planner-connection.ts` + the
//! `simpleCostModel` test oracle in `planner/test/helpers.ts`).
//!
//! [`ConnectionCostModel`] is the planner's only injection point: given a table scan
//! (table, sort, filters, optional correlation constraint) it returns an estimated
//! `{startup_cost, rows, fanout}`. The planner builds its whole cost picture from
//! these. The production SQLite estimator (`sqlite-cost-model.ts`) is a Stage-8 /
//! `rindle-sqlite` concern; the core ports + tests against [`SimpleCostModel`], which
//! both JS and Rust run verbatim so the differential sees identical inputs.

use std::rc::Rc;

use rindle::{Condition, OrderPart};

use crate::constraint::PlannerConstraint;

/// Confidence in a fanout estimate (`'high' | 'med' | 'none'`,
/// `planner-connection.ts:329-332`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FanoutConfidence {
    High,
    Med,
    None,
}

/// One fanout estimate: the average number of child rows per distinct constrained key
/// (`FanoutEst`, `planner-connection.ts:329`).
#[derive(Clone, Debug, PartialEq)]
pub struct FanoutEst {
    pub fanout: f64,
    pub confidence: FanoutConfidence,
}

/// `(columns) -> FanoutEst` (`FanoutCostModel`, `planner-connection.ts:333`).
///
/// `Rc` so it threads cheaply through the per-node `CostEstimate`s and planning
/// snapshots (Stages 1–4) without cloning the closure — the JS object shares the
/// function by reference.
pub type FanoutCostModel = Rc<dyn Fn(&[&str]) -> FanoutEst>;

/// The cost model's per-scan return value (`CostModelCost`,
/// `planner-connection.ts:335`).
///
/// Not `Debug`/`PartialEq` (the `fanout` closure is neither); compare `startup_cost` /
/// `rows` and invoke `fanout` directly in tests.
#[derive(Clone)]
pub struct CostModelCost {
    pub startup_cost: f64,
    pub rows: f64,
    pub fanout: FanoutCostModel,
}

/// The planner's sole cost injection point (`ConnectionCostModel`,
/// `planner-connection.ts:340`). Object-safe so the graph can hold a `&dyn`.
///
/// NB: the real JS returns `{startupCost, rows, fanout}` — richer than spec 08 §4.5's
/// pinned `-> {startup_cost, rows}` (which described doc-06's *heuristic*). The
/// faithful port needs `fanout` for the Stage-2 semi-join selectivity scaling, so we
/// use the real shape. See `PLANNER-PORT-DESIGN.md` §6.
pub trait ConnectionCostModel {
    fn estimate(
        &self,
        table: &str,
        sort: &[OrderPart],
        filters: Option<&Condition>,
        constraint: Option<&PlannerConstraint>,
    ) -> CostModelCost;
}

/// Base scan cost with no constraints applied (`BASE_COST`, `helpers.ts:21`).
pub const BASE_COST: f64 = 100.0;
/// Row reduction per applied constraint (`CONSTRAINT_REDUCTION`, `helpers.ts:26`).
pub const CONSTRAINT_REDUCTION: f64 = 10.0;

/// The shared deterministic test oracle (`simpleCostModel`, `helpers.ts:56`): base
/// `100` rows, minus `10` per applied constraint column, floored at `1`; zero startup
/// cost; constant `fanout = 1`. Ignores `sort`/`filters` (constraint *count* only).
///
/// This is THE cost model the JS↔Rust differential runs on both sides — ported
/// verbatim, preserving operation order, so flip decisions match bit-for-bit. Because
/// `fanout = 1`, the planner's one transcendental (`pow(1 - sel, fanout)`) is trivial
/// (`pow(x, 1) = x`), so the differential never exercises a non-exact op.
pub struct SimpleCostModel;

impl ConnectionCostModel for SimpleCostModel {
    fn estimate(
        &self,
        _table: &str,
        _sort: &[OrderPart],
        _filters: Option<&Condition>,
        constraint: Option<&PlannerConstraint>,
    ) -> CostModelCost {
        // JS: const rows = Math.max(1, 100 - constraintCount * 10);
        let constraint_count = constraint.map_or(0, |c| c.len());
        let rows = (100.0 - constraint_count as f64 * 10.0).max(1.0);
        CostModelCost {
            startup_cost: 0.0,
            rows,
            fanout: Rc::new(|_columns| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cset(items: &[&str]) -> PlannerConstraint {
        items.iter().map(|s| Box::from(*s)).collect()
    }

    #[test]
    fn rows_decrease_per_constraint_floored_at_one() {
        let m = SimpleCostModel;

        let none = m.estimate("t", &[], None, None);
        assert_eq!(none.rows, 100.0);
        assert_eq!(none.startup_cost, 0.0);

        let one = cset(&["a"]);
        assert_eq!(m.estimate("t", &[], None, Some(&one)).rows, 90.0);

        let three = cset(&["a", "b", "c"]);
        assert_eq!(m.estimate("t", &[], None, Some(&three)).rows, 70.0);

        // 12 constraints => 100 - 120 = -20, floored to 1.
        let many = cset(&["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"]);
        assert_eq!(m.estimate("t", &[], None, Some(&many)).rows, 1.0);
    }

    #[test]
    fn fanout_is_constant_one() {
        let m = SimpleCostModel;
        let c = m.estimate("t", &[], None, None);
        let f = (c.fanout)(&["x", "y"]);
        assert_eq!(
            f,
            FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None
            }
        );
    }
}
