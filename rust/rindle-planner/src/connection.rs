//! `PlannerConnection` — a table scan with filters + ordering (port of
//! `planner-connection.ts`).
//!
//! ## Dual-state pattern (faithful to JS)
//! - **Immutable structure** (set at construction): `table`/`name`, `sort`,
//!   `filters`, the cost `model`, `base_constraints`, `base_limit`, `selectivity`,
//!   `is_root`.
//! - **Mutable planning state** (changes during the plan search): `limit` (cleared
//!   when a parent join flips), per-branch `constraints`, and a memoization cache.
//!
//! A connection is a **leaf** of both flows: backward constraint propagation ends
//! here (it stores the constraint), and forward cost estimation starts here (it has
//! no inputs to recurse into). That is why it is fully testable in isolation, ahead
//! of the graph. The cache is transparent memoization (interior-mutable, like JS's
//! free mutation), so [`estimate_cost`](PlannerConnection::estimate_cost) takes
//! `&self`.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use rindle::{Condition, OrderPart};

use crate::constraint::{merge_constraints, PlannerConstraint};
use crate::cost::ConnectionCostModel;
use crate::node::{branch_key, CostEstimate, NodeId};

pub struct PlannerConnection {
    // ---- immutable structure ----
    pub table: Box<str>,
    /// Human-readable name for debugging (defaults to `table`).
    pub name: Box<str>,
    sort: Vec<OrderPart>,
    filters: Option<Condition>,
    model: Rc<dyn ConnectionCostModel>,
    base_constraints: Option<PlannerConstraint>,
    base_limit: Option<f64>,
    /// Fraction of rows passing filters (`1.0` = no filtering). Computed once, only
    /// for an EXISTS child (`limit` set) that actually has filters.
    pub selectivity: f64,
    is_root: bool,
    /// Downstream node, set during graph wiring (`setOutput`). Not read by the cost /
    /// constraint flows (a connection is a leaf), but kept for graph completeness.
    output: Option<NodeId>,

    // ---- mutable planning state ----
    /// Current limit; can be cleared by a flipped parent join ([`Self::unlimit`]).
    pub limit: Option<f64>,
    /// Per-branch propagated constraints (key = [`branch_key`]). `None` value is a
    /// real state (a UFO branch can report an absent constraint).
    constraints: BTreeMap<String, Option<PlannerConstraint>>,
    /// Cached per-branch cost to avoid redundant model calls; invalidated when
    /// constraints change. Interior-mutable so cost estimation is `&self`.
    cached_constraint_costs: RefCell<BTreeMap<String, CostEstimate>>,
}

impl PlannerConnection {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table: Box<str>,
        model: Rc<dyn ConnectionCostModel>,
        sort: Vec<OrderPart>,
        filters: Option<Condition>,
        is_root: bool,
        base_constraints: Option<PlannerConstraint>,
        limit: Option<f64>,
        name: Option<Box<str>>,
    ) -> Self {
        // Selectivity = fraction of rows passing filters; computed only for an EXISTS
        // child (`limit` set) that has filters, exactly as the JS constructor.
        let selectivity = if limit.is_some() && filters.is_some() {
            let with = model.estimate(&table, &sort, filters.as_ref(), None);
            let without = model.estimate(&table, &sort, None, None);
            if without.rows > 0.0 {
                with.rows / without.rows
            } else {
                1.0
            }
        } else {
            1.0
        };
        let name = name.unwrap_or_else(|| table.clone());
        Self {
            table,
            name,
            sort,
            filters,
            model,
            base_constraints,
            base_limit: limit,
            selectivity,
            is_root,
            output: None,
            limit,
            constraints: BTreeMap::new(),
            cached_constraint_costs: RefCell::new(BTreeMap::new()),
        }
    }

    pub fn kind(&self) -> &'static str {
        "connection"
    }

    pub fn set_output(&mut self, node: NodeId) {
        self.output = Some(node);
    }

    pub fn output(&self) -> NodeId {
        self.output.expect("Output not set")
    }

    /// Store the constraint for an OR branch path (`propagateConstraints`). Backward
    /// flow ends here; invalidates the cost cache.
    pub fn propagate_constraints(&mut self, path: &[usize], c: Option<PlannerConstraint>) {
        self.constraints.insert(branch_key(path), c);
        self.cached_constraint_costs.borrow_mut().clear();
    }

    /// Estimate the scan cost for a branch (`estimateCost`). Merges base + propagated
    /// constraints, asks the cost model, and applies the limit/selectivity cap.
    /// Memoized per branch key.
    pub fn estimate_cost(
        &self,
        downstream_child_selectivity: f64,
        branch_pattern: &[usize],
    ) -> CostEstimate {
        let key = branch_key(branch_pattern);
        if let Some(cached) = self.cached_constraint_costs.borrow().get(&key) {
            return cached.clone();
        }

        let constraint = self.constraints.get(&key).cloned().flatten();
        let merged = merge_constraints(self.base_constraints.as_ref(), constraint.as_ref());
        let m = self.model.estimate(
            &self.table,
            &self.sort,
            self.filters.as_ref(),
            merged.as_ref(),
        );

        // scanEst = limit === undefined ? rows : Math.min(rows, limit / downstreamSel)
        let scan_est = match self.limit {
            None => m.rows,
            Some(limit) => m.rows.min(limit / downstream_child_selectivity),
        };
        let cost = CostEstimate {
            startup_cost: m.startup_cost,
            scan_est,
            cost: 0.0,
            returned_rows: m.rows,
            selectivity: self.selectivity,
            limit: self.limit,
            fanout: m.fanout,
        };
        self.cached_constraint_costs
            .borrow_mut()
            .insert(key, cost.clone());
        cost
    }

    /// Remove the limit (a parent join flipped → this connection drives an outer loop
    /// and must produce all rows). Root connections cannot be unlimited. Limit is
    /// applied at the join level, so this does not invalidate the cost cache — exactly
    /// as JS.
    pub fn unlimit(&mut self) {
        if self.is_root {
            return;
        }
        if self.limit.is_some() {
            self.limit = None;
        }
    }

    /// Clear mutable planning state back to the constructed baseline (`reset`).
    pub fn reset(&mut self) {
        self.constraints.clear();
        self.limit = self.base_limit;
        self.cached_constraint_costs.borrow_mut().clear();
    }

    /// Snapshot the per-branch constraint map (`captureConstraints`), for the planner's
    /// save/restore of the best plan.
    pub(crate) fn capture_constraints(&self) -> BTreeMap<String, Option<PlannerConstraint>> {
        self.constraints.clone()
    }

    /// Restore the per-branch constraint map from a snapshot (`restoreConstraints`);
    /// invalidates the cost cache.
    pub(crate) fn restore_constraints(
        &mut self,
        constraints: BTreeMap<String, Option<PlannerConstraint>>,
    ) {
        self.constraints = constraints;
        self.cached_constraint_costs.borrow_mut().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost::{SimpleCostModel, BASE_COST};
    use crate::node::CostNoFanout;
    use rindle::Dir;

    fn cset(items: &[&str]) -> PlannerConstraint {
        items.iter().map(|s| Box::from(*s)).collect()
    }

    /// Mirrors `helpers.ts` `createConnection()`: table `users`, sort `[['id','asc']]`,
    /// no filters, no limit.
    fn create_connection() -> PlannerConnection {
        PlannerConnection::new(
            Box::from("users"),
            Rc::new(SimpleCostModel),
            vec![OrderPart(Box::from("id"), Dir::Asc)],
            None,
            false,
            None,
            None,
            None,
        )
    }

    #[test]
    fn estimate_cost_no_constraints_is_base() {
        let c = create_connection();
        assert_eq!(
            c.estimate_cost(1.0, &[]).omit_fanout(),
            CostNoFanout {
                startup_cost: 0.0,
                scan_est: BASE_COST,
                cost: 0.0,
                returned_rows: BASE_COST,
                selectivity: 1.0,
                limit: None,
            }
        );
    }

    #[test]
    fn estimate_cost_with_constraint_reduces() {
        let mut c = create_connection();
        c.propagate_constraints(&[0], Some(cset(&["userId"])));
        let r = c.estimate_cost(1.0, &[0]);
        assert_eq!(r.scan_est, 90.0); // BASE_COST - 1 * 10
        assert_eq!(r.returned_rows, 90.0);
    }

    #[test]
    fn multiple_constraints_reduce_further() {
        let mut c = create_connection();
        c.propagate_constraints(&[0], Some(cset(&["userId", "postId"])));
        let r = c.estimate_cost(1.0, &[0]);
        assert_eq!(r.scan_est, 80.0); // BASE_COST - 2 * 10
        assert_eq!(r.returned_rows, 80.0);
    }

    #[test]
    fn branch_patterns_are_independent() {
        let mut c = create_connection();
        c.propagate_constraints(&[0], Some(cset(&["userId"])));
        c.propagate_constraints(&[1], Some(cset(&["postId"])));
        assert_eq!(c.estimate_cost(1.0, &[0]).scan_est, 90.0);
        assert_eq!(c.estimate_cost(1.0, &[1]).scan_est, 90.0);
    }

    #[test]
    fn reset_clears_constraints() {
        let mut c = create_connection();
        c.propagate_constraints(&[0], Some(cset(&["userId"])));
        assert_eq!(c.estimate_cost(1.0, &[0]).scan_est, 90.0);
        c.reset();
        assert_eq!(c.estimate_cost(1.0, &[0]).scan_est, BASE_COST);
    }

    #[test]
    fn limit_caps_scan_est_via_downstream_selectivity() {
        // limit=1, downstream selectivity 0.5 => scanEst = min(rows=100, 1/0.5=2) = 2.
        let c = PlannerConnection::new(
            Box::from("users"),
            Rc::new(SimpleCostModel),
            vec![OrderPart(Box::from("id"), Dir::Asc)],
            None,
            false,
            None,
            Some(1.0),
            None,
        );
        let r = c.estimate_cost(0.5, &[]);
        assert_eq!(r.scan_est, 2.0);
        assert_eq!(r.returned_rows, 100.0);
        assert_eq!(r.limit, Some(1.0));
        assert_eq!(r.selectivity, 1.0); // SimpleCostModel ignores filters
    }

    #[test]
    fn unlimit_clears_nonroot_but_not_root() {
        let mut nonroot = PlannerConnection::new(
            Box::from("users"),
            Rc::new(SimpleCostModel),
            vec![],
            None,
            false,
            None,
            Some(1.0),
            None,
        );
        nonroot.unlimit();
        assert_eq!(nonroot.limit, None);

        let mut root = PlannerConnection::new(
            Box::from("users"),
            Rc::new(SimpleCostModel),
            vec![],
            None,
            true,
            None,
            Some(1.0),
            None,
        );
        root.unlimit();
        assert_eq!(root.limit, Some(1.0)); // root cannot be unlimited
    }
}
