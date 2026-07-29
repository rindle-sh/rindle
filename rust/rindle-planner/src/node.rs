//! The forward-flow cost estimate + small shared node helpers (port of
//! `planner-node.ts`).
//!
//! Cost flows *forward* (connection → … → terminus); [`CostEstimate`] is the value
//! threaded along that flow. The node enum / arena (`PlannerNode`, `PlannerGraph`)
//! and the polymorphic dispatch land with the graph (Stage 4); a connection is a leaf
//! of both the cost and constraint flows, so Stage 1 exercises it directly.

use crate::cost::FanoutCostModel;

/// An index into the [`PlannerGraph`](crate::PlannerGraph) arena. The graph is a DAG
/// (a FanOut is the shared parent of several joins), so nodes reference each other by
/// id rather than by ownership — mirroring `rindle`'s own arena `Graph`/`NodeId`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct NodeId(pub usize);

/// The forward cost estimate (`CostEstimate`, `planner-node.ts:17`).
///
/// Carries a `fanout` closure, so it is neither `Debug` nor `PartialEq`; use
/// [`CostEstimate::omit_fanout`] for assertions / serialization.
#[derive(Clone)]
pub struct CostEstimate {
    /// One-time setup cost (e.g. sorting).
    pub startup_cost: f64,
    /// Estimated rows scanned (bounded by `limit / downstream_selectivity`).
    pub scan_est: f64,
    /// Cumulative pipeline cost so far. `0` at a connection; accrues at joins.
    pub cost: f64,
    /// Rows output by this node.
    pub returned_rows: f64,
    /// Fraction of input rows passing this node (`1.0` = no filtering).
    pub selectivity: f64,
    /// Current limit (`number | undefined`), if any.
    pub limit: Option<f64>,
    /// The carried fanout estimator (used by the join cost in Stage 2).
    pub fanout: FanoutCostModel,
}

/// [`CostEstimate`] without the (non-comparable) `fanout` closure — the JS
/// `omitFanout` (`planner-node.ts:60`), for assertions / debug serialization.
#[derive(Clone, Debug, PartialEq)]
pub struct CostNoFanout {
    pub startup_cost: f64,
    pub scan_est: f64,
    pub cost: f64,
    pub returned_rows: f64,
    pub selectivity: f64,
    pub limit: Option<f64>,
}

impl CostEstimate {
    /// Strip the `fanout` closure (`omitFanout`, `planner-node.ts:60`).
    pub fn omit_fanout(&self) -> CostNoFanout {
        CostNoFanout {
            startup_cost: self.startup_cost,
            scan_est: self.scan_est,
            cost: self.cost,
            returned_rows: self.returned_rows,
            selectivity: self.selectivity,
            limit: self.limit,
        }
    }
}

/// Join a branch pattern into the constraint-map key (`path.join(',')`): `[]` → `""`,
/// `[0]` → `"0"`, `[0,1]` → `"0,1"`. The key identifies an OR branch's path through
/// the graph (a connection keeps a separate constraint + cost per branch).
pub(crate) fn branch_key(pattern: &[usize]) -> String {
    pattern
        .iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// Prepend a branch index onto a pattern (`[head, ...rest]`): a FanIn extends the
/// pattern with the branch its input belongs to (`[0,…]` for FI, `[i,…]` for UFI).
pub(crate) fn prepend(head: usize, rest: &[usize]) -> Vec<usize> {
    let mut out = Vec::with_capacity(rest.len() + 1);
    out.push(head);
    out.extend_from_slice(rest);
    out
}
