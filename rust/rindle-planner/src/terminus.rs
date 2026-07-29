//! `PlannerTerminus` — the final output node where both flows begin (port of
//! `planner-terminus.ts`). It wraps the graph's top node and kicks off cost
//! estimation (`estimateCost(1, [])`) and constraint propagation
//! (`propagateConstraints([], undefined)`). It is itself inert: it does not
//! participate in unlimiting. The actual dispatch lives on
//! [`PlannerGraph`](crate::graph).

use crate::node::NodeId;

pub struct PlannerTerminus {
    input: NodeId,
}

impl PlannerTerminus {
    pub fn new(input: NodeId) -> Self {
        Self { input }
    }

    pub fn kind(&self) -> &'static str {
        "terminus"
    }

    pub fn input(&self) -> NodeId {
        self.input
    }
}
