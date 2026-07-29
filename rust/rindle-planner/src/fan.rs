//! `PlannerFanOut` / `PlannerFanIn` — the OR-branch fan nodes (port of
//! `planner-fan-out.ts` + `planner-fan-in.ts`).
//!
//! An `OR` of correlated subqueries splits the flow at a FanOut and merges it at a
//! FanIn, with one join per branch in between:
//!
//! ```text
//!        input            (FO is a passthrough for cost/constraints)
//!          │
//!         FO
//!        /  \
//!      J0    J1           branch patterns: J0 → [0,…], J1 → [1,…] under UFI
//!        \  /
//!         FI              FI = max across branches; UFI = sum across branches
//!          │
//!        output
//! ```
//!
//! - **FanOut** is a pure passthrough: cost = its input's cost, constraints forward to
//!   the input. The `FO`/`UFO` flag is just tracked (the graph's FOFI conversion sets
//!   it; it does not change FanOut's own behavior).
//! - **FanIn** does the real work — `FI` takes the **max** of its branches, `UFI` the
//!   **sum** — and both combine branch selectivities as `1 − ∏(1 − sᵢ)` (OR of
//!   independent events). It assigns each input a branch pattern (`[0,…]` for FI,
//!   `[i,…]` for UFI) so a connection costs each OR path separately.
//!
//! The cost/constraint recursion over inputs lives on
//! [`PlannerGraph`](crate::graph); these structs hold the node data + the
//! type transitions.

use crate::node::NodeId;

/// FanOut kind: `FO` (shared single path) or `UFO` (one path per branch).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FanOutType {
    Fo,
    Ufo,
}

/// FanIn kind: `FI` (single fetch, max cost) or `UFI` (fetch per branch, summed cost).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FanInType {
    Fi,
    Ufi,
}

pub struct PlannerFanOut {
    type_: FanOutType,
    input: NodeId,
    outputs: Vec<NodeId>,
}

impl PlannerFanOut {
    pub fn new(input: NodeId) -> Self {
        Self {
            type_: FanOutType::Fo,
            input,
            outputs: Vec::new(),
        }
    }

    pub fn kind(&self) -> &'static str {
        "fan-out"
    }

    pub fn type_(&self) -> FanOutType {
        self.type_
    }

    pub fn input(&self) -> NodeId {
        self.input
    }

    pub fn add_output(&mut self, node: NodeId) {
        self.outputs.push(node);
    }

    pub fn outputs(&self) -> &[NodeId] {
        &self.outputs
    }

    pub fn convert_to_ufo(&mut self) {
        self.type_ = FanOutType::Ufo;
    }

    pub fn reset(&mut self) {
        self.type_ = FanOutType::Fo;
    }
}

pub struct PlannerFanIn {
    type_: FanInType,
    inputs: Vec<NodeId>,
    output: Option<NodeId>,
}

impl PlannerFanIn {
    pub fn new(inputs: Vec<NodeId>) -> Self {
        Self {
            type_: FanInType::Fi,
            inputs,
            output: None,
        }
    }

    pub fn kind(&self) -> &'static str {
        "fan-in"
    }

    pub fn type_(&self) -> FanInType {
        self.type_
    }

    pub fn inputs(&self) -> &[NodeId] {
        &self.inputs
    }

    pub fn set_output(&mut self, node: NodeId) {
        self.output = Some(node);
    }

    pub fn output(&self) -> NodeId {
        self.output.expect("Output not set")
    }

    pub fn convert_to_ufi(&mut self) {
        self.type_ = FanInType::Ufi;
    }

    pub fn reset(&mut self) {
        self.type_ = FanInType::Fi;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost::{
        ConnectionCostModel, CostModelCost, FanoutConfidence, FanoutEst, SimpleCostModel,
    };
    use crate::graph::PlannerGraph;
    use crate::node::CostNoFanout;
    use crate::source::PlannerSource;
    use rindle::{Condition, Dir, Lit, Op, OrderPart, SimpleCondition, ValuePosition};
    use std::rc::Rc;

    fn cset(items: &[&str]) -> crate::PlannerConstraint {
        items.iter().map(|s| Box::from(*s)).collect()
    }

    fn default_sort() -> Vec<OrderPart> {
        vec![OrderPart(Box::from("id"), Dir::Asc)]
    }

    fn add_simple_connection(g: &mut PlannerGraph, table: &str) -> NodeId {
        let model: Rc<dyn ConnectionCostModel> = Rc::new(SimpleCostModel);
        let src = PlannerSource::new(table, model);
        g.add_connection(src.connect(default_sort(), None, false, None, None))
    }

    fn create_fan_out() -> (PlannerGraph, NodeId, NodeId) {
        let mut g = PlannerGraph::new();
        let input = add_simple_connection(&mut g, "users");
        let fo = g.add_fan_out(PlannerFanOut::new(input));
        (g, input, fo)
    }

    fn create_fan_in(count: usize) -> (PlannerGraph, Vec<NodeId>, NodeId) {
        let mut g = PlannerGraph::new();
        let inputs: Vec<NodeId> = (0..count)
            .map(|i| add_simple_connection(&mut g, &format!("table{i}")))
            .collect();
        let fi = g.add_fan_in(PlannerFanIn::new(inputs.clone()));
        (g, inputs, fi)
    }

    fn unconstrained() -> CostNoFanout {
        CostNoFanout {
            startup_cost: 0.0,
            scan_est: 100.0,
            cost: 0.0,
            returned_rows: 100.0,
            selectivity: 1.0,
            limit: None,
        }
    }

    // ---- FanOut ----

    #[test]
    fn fan_out_initial_state_is_fo() {
        let (g, _input, fo) = create_fan_out();
        assert_eq!(g.fan_out(fo).kind(), "fan-out");
        assert_eq!(g.fan_out(fo).type_(), FanOutType::Fo);
    }

    #[test]
    fn fan_out_can_add_outputs() {
        let (mut g, _input, fo) = create_fan_out();
        let out1 = add_simple_connection(&mut g, "posts");
        let out2 = add_simple_connection(&mut g, "comments");
        g.fan_out_mut(fo).add_output(out1);
        g.fan_out_mut(fo).add_output(out2);
        assert_eq!(g.fan_out(fo).outputs(), &[out1, out2]);
    }

    #[test]
    fn fan_out_convert_to_ufo() {
        let (mut g, _input, fo) = create_fan_out();
        assert_eq!(g.fan_out(fo).type_(), FanOutType::Fo);
        g.fan_out_mut(fo).convert_to_ufo();
        assert_eq!(g.fan_out(fo).type_(), FanOutType::Ufo);
    }

    #[test]
    fn fan_out_propagate_forwards_to_input() {
        let (mut g, input, fo) = create_fan_out();
        g.propagate_constraints(fo, &[0], Some(cset(&["userId"])));
        // The constraint went to branch [0]; the input's [] branch is still base cost.
        assert_eq!(
            g.estimate_cost(input, 1.0, &[]).omit_fanout(),
            unconstrained()
        );
    }

    #[test]
    fn fan_out_reset_restores_fo() {
        let (mut g, _input, fo) = create_fan_out();
        g.fan_out_mut(fo).convert_to_ufo();
        assert_eq!(g.fan_out(fo).type_(), FanOutType::Ufo);
        g.fan_out_mut(fo).reset();
        assert_eq!(g.fan_out(fo).type_(), FanOutType::Fo);
    }

    // ---- FanIn ----

    #[test]
    fn fan_in_initial_state_is_fi() {
        let (g, _inputs, fi) = create_fan_in(2);
        assert_eq!(g.fan_in(fi).kind(), "fan-in");
        assert_eq!(g.fan_in(fi).type_(), FanInType::Fi);
    }

    #[test]
    fn fan_in_convert_and_reset() {
        let (mut g, _inputs, fi) = create_fan_in(2);
        g.fan_in_mut(fi).convert_to_ufi();
        assert_eq!(g.fan_in(fi).type_(), FanInType::Ufi);
        g.fan_in_mut(fi).reset();
        assert_eq!(g.fan_in(fi).type_(), FanInType::Fi);
    }

    #[test]
    fn fan_in_set_and_get_output() {
        let (mut g, _inputs, fi) = create_fan_in(2);
        let output = add_simple_connection(&mut g, "comments");
        g.fan_in_mut(fi).set_output(output);
        assert_eq!(g.fan_in(fi).output(), output);
    }

    #[test]
    fn fan_in_fi_propagate_same_pattern_to_all_inputs() {
        let (mut g, inputs, fi) = create_fan_in(2);
        g.propagate_constraints(fi, &[], Some(cset(&["userId"])));
        // FI sends [0] to all; the [] branch on each input stays unconstrained.
        for &input in &inputs {
            assert_eq!(
                g.estimate_cost(input, 1.0, &[]).omit_fanout(),
                unconstrained()
            );
        }
    }

    #[test]
    fn fan_in_ufi_propagate_unique_patterns() {
        let (mut g, inputs, fi) = create_fan_in(3);
        g.fan_in_mut(fi).convert_to_ufi();
        g.propagate_constraints(fi, &[], Some(cset(&["userId"])));
        for &input in &inputs {
            assert_eq!(
                g.estimate_cost(input, 1.0, &[]).omit_fanout(),
                unconstrained()
            );
        }
    }

    // ---- OR-selectivity (also exercises the connection selectivity path) ----

    /// Cost model whose row count drops to `selectivity_percent` when a filter is
    /// present (else 100) — so a filtered, limited connection has
    /// `selectivity = selectivity_percent / 100`.
    struct SelectiveModel {
        selectivity_percent: f64,
    }

    impl ConnectionCostModel for SelectiveModel {
        fn estimate(
            &self,
            _table: &str,
            _sort: &[OrderPart],
            filters: Option<&Condition>,
            _constraint: Option<&crate::PlannerConstraint>,
        ) -> CostModelCost {
            let rows = if filters.is_some() {
                self.selectivity_percent
            } else {
                100.0
            };
            CostModelCost {
                startup_cost: 0.0,
                rows,
                fanout: Rc::new(|_| FanoutEst {
                    fanout: 1.0,
                    confidence: FanoutConfidence::None,
                }),
            }
        }
    }

    fn selective_connection(g: &mut PlannerGraph, table: &str, pct: f64) -> NodeId {
        let model: Rc<dyn ConnectionCostModel> = Rc::new(SelectiveModel {
            selectivity_percent: pct,
        });
        let src = PlannerSource::new(table, model);
        let filter = Condition::Simple(SimpleCondition {
            op: Op::Eq,
            left: ValuePosition::Column {
                name: Box::from("x"),
            },
            right: ValuePosition::Literal {
                value: Lit::Number(1.0),
            },
        });
        // limit=1 triggers the selectivity calculation in the connection ctor.
        g.add_connection(src.connect(default_sort(), Some(filter), false, None, Some(1.0)))
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-10
    }

    #[test]
    fn or_selectivity_two_branches_fi_and_ufi() {
        for ufi in [false, true] {
            let mut g = PlannerGraph::new();
            let a = selective_connection(&mut g, "branchA", 50.0); // 50% selective
            let b = selective_connection(&mut g, "branchB", 30.0); // 30% selective
            assert_eq!(g.connection(a).selectivity, 0.5);
            assert_eq!(g.connection(b).selectivity, 0.3);

            let fi = g.add_fan_in(PlannerFanIn::new(vec![a, b]));
            if ufi {
                g.fan_in_mut(fi).convert_to_ufi();
            }
            // P(A OR B) = 1 - (1-0.5)(1-0.3) = 0.65
            let sel = g.estimate_cost(fi, 1.0, &[]).selectivity;
            assert!(close(sel, 0.65), "expected ~0.65, got {sel}");
        }
    }

    #[test]
    fn or_selectivity_three_branches() {
        let mut g = PlannerGraph::new();
        let a = selective_connection(&mut g, "branchA", 50.0);
        let b = selective_connection(&mut g, "branchB", 40.0);
        let c = selective_connection(&mut g, "branchC", 60.0);
        let fi = g.add_fan_in(PlannerFanIn::new(vec![a, b, c]));
        // P = 1 - (1-0.5)(1-0.4)(1-0.6) = 1 - 0.12 = 0.88
        let sel = g.estimate_cost(fi, 1.0, &[]).selectivity;
        assert!(close(sel, 0.88), "expected ~0.88, got {sel}");
    }

    #[test]
    fn or_selectivity_never_exceeds_one() {
        let mut g = PlannerGraph::new();
        let a = selective_connection(&mut g, "branchA", 99.0);
        let b = selective_connection(&mut g, "branchB", 99.0);
        let fi = g.add_fan_in(PlannerFanIn::new(vec![a, b]));
        // P = 1 - (1-0.99)(1-0.99) = 0.9999
        let sel = g.estimate_cost(fi, 1.0, &[]).selectivity;
        assert!(close(sel, 0.9999), "expected ~0.9999, got {sel}");
        assert!(sel <= 1.0);
    }
}
