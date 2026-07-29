//! `PlannerGraph` — the node arena + the polymorphic dispatch for the cost and
//! constraint flows (the node-method dispatch half of `planner-graph.ts`; the
//! enumeration algorithm itself lands in Stage 4).
//!
//! The planner graph is a DAG — a FanOut is the shared parent of several joins — so
//! nodes reference each other by [`NodeId`] index into `nodes`, mirroring `rindle`'s own
//! arena `Graph`. The JS node methods (`node.estimateCost(...)`,
//! `node.propagateConstraints(...)`) become dispatch methods here that match on the
//! node kind and recurse through the arena.
//!
//! Borrow discipline:
//! - **Forward cost** ([`estimate_cost`](PlannerGraph::estimate_cost)) is `&self`: it
//!   only reads structure and mutates the connection memo cache (a `RefCell`), so the
//!   recursion composes through shared borrows.
//! - **Backward constraint** ([`propagate_constraints`](PlannerGraph::propagate_constraints))
//!   is `&mut self`: each arm first reads the node immutably to extract the
//!   `Copy`/cloned data it needs, releasing the borrow before recursing.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::rc::Rc;

use crate::connection::PlannerConnection;
use crate::constraint::PlannerConstraint;
use crate::cost::ConnectionCostModel;
use crate::fan::{FanInType, FanOutType, PlannerFanIn, PlannerFanOut};
use crate::join::{
    flipped_parent_constraint, translate_constraints_for_flipped_join, JoinType, PlannerJoin,
};
use crate::node::{prepend, CostEstimate, NodeId};
use crate::source::PlannerSource;
use crate::terminus::PlannerTerminus;

/// Max flippable joins for exhaustive `2^n` enumeration (`MAX_FLIPPABLE_JOINS`). Above
/// this the planner skips optimization and the query runs as authored.
const MAX_FLIPPABLE_JOINS: usize = 9;

/// A saved snapshot of the mutable planning state (`PlanState`) — kept for the best
/// plan during enumeration and restored at the end. Captured/restored per the graph's
/// insertion-ordered kind lists.
pub struct PlanState {
    connections: Vec<Option<f64>>,
    joins: Vec<JoinType>,
    fan_outs: Vec<FanOutType>,
    fan_ins: Vec<FanInType>,
    connection_constraints: Vec<BTreeMap<String, Option<PlannerConstraint>>>,
}

enum PlannerNode {
    Connection(PlannerConnection),
    Join(PlannerJoin),
    FanOut(PlannerFanOut),
    FanIn(PlannerFanIn),
    Terminus(PlannerTerminus),
}

#[derive(Default)]
pub struct PlannerGraph {
    nodes: Vec<PlannerNode>,
    // Per-kind ordered id lists (mirroring JS `graph.connections/joins/fanOuts/fanIns`),
    // walked in insertion order by reset / snapshot / restore / plan.
    connections: Vec<NodeId>,
    joins: Vec<NodeId>,
    fan_outs: Vec<NodeId>,
    fan_ins: Vec<NodeId>,
    terminus: Option<NodeId>,
    // Sources keyed by table name (reused for self-joins during construction).
    sources: BTreeMap<Box<str>, PlannerSource>,
}

impl PlannerGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_connection(&mut self, connection: PlannerConnection) -> NodeId {
        let id = self.push(PlannerNode::Connection(connection));
        self.connections.push(id);
        id
    }

    pub fn add_join(&mut self, join: PlannerJoin) -> NodeId {
        let id = self.push(PlannerNode::Join(join));
        self.joins.push(id);
        id
    }

    pub fn add_fan_out(&mut self, fan_out: PlannerFanOut) -> NodeId {
        let id = self.push(PlannerNode::FanOut(fan_out));
        self.fan_outs.push(id);
        id
    }

    pub fn add_fan_in(&mut self, fan_in: PlannerFanIn) -> NodeId {
        let id = self.push(PlannerNode::FanIn(fan_in));
        self.fan_ins.push(id);
        id
    }

    /// Add the terminus node wrapping `input`. Call [`set_terminus`](Self::set_terminus)
    /// to make it the graph's output (where the flows start).
    pub fn add_terminus(&mut self, input: NodeId) -> NodeId {
        self.push(PlannerNode::Terminus(PlannerTerminus::new(input)))
    }

    pub fn set_terminus(&mut self, id: NodeId) {
        self.terminus = Some(id);
    }

    /// Register a source for `name` (`addSource`); returns a clone to `connect` against.
    pub fn add_source(&mut self, name: &str, model: Rc<dyn ConnectionCostModel>) -> PlannerSource {
        assert!(
            !self.sources.contains_key(name),
            "Source {name} already exists in the graph"
        );
        let source = PlannerSource::new(name, model);
        self.sources.insert(Box::from(name), source.clone());
        source
    }

    pub fn get_source(&self, name: &str) -> PlannerSource {
        self.sources
            .get(name)
            .unwrap_or_else(|| panic!("Source {name} not found in the graph"))
            .clone()
    }

    pub fn has_source(&self, name: &str) -> bool {
        self.sources.contains_key(name)
    }

    /// Wire a node's downstream output (`wireOutput`): connection/join/fan-in set their
    /// single output; a fan-out appends a branch; a terminus has none.
    pub fn wire_output(&mut self, from: NodeId, to: NodeId) {
        match &mut self.nodes[from.0] {
            PlannerNode::Connection(c) => c.set_output(to),
            PlannerNode::Join(j) => j.set_output(to),
            PlannerNode::FanIn(f) => f.set_output(to),
            PlannerNode::FanOut(f) => f.add_output(to),
            PlannerNode::Terminus(_) => panic!("Terminus nodes cannot have outputs"),
        }
    }

    /// The plan ids of all flipped joins (`applyPlansToAST`'s `flippedIds`).
    pub fn flipped_join_plan_ids(&self) -> HashSet<u32> {
        self.joins
            .iter()
            .filter(|&&id| self.join(id).type_() == JoinType::Flipped)
            .map(|&id| self.join(id).plan_id)
            .collect()
    }

    pub fn connection_ids(&self) -> &[NodeId] {
        &self.connections
    }

    pub fn join_ids(&self) -> &[NodeId] {
        &self.joins
    }

    pub fn fan_out_ids(&self) -> &[NodeId] {
        &self.fan_outs
    }

    pub fn fan_in_ids(&self) -> &[NodeId] {
        &self.fan_ins
    }

    fn push(&mut self, node: PlannerNode) -> NodeId {
        let id = NodeId(self.nodes.len());
        self.nodes.push(node);
        id
    }

    pub fn join(&self, id: NodeId) -> &PlannerJoin {
        match &self.nodes[id.0] {
            PlannerNode::Join(j) => j,
            _ => panic!("node {} is not a join", id.0),
        }
    }

    pub fn join_mut(&mut self, id: NodeId) -> &mut PlannerJoin {
        match &mut self.nodes[id.0] {
            PlannerNode::Join(j) => j,
            _ => panic!("node {} is not a join", id.0),
        }
    }

    pub fn connection(&self, id: NodeId) -> &PlannerConnection {
        match &self.nodes[id.0] {
            PlannerNode::Connection(c) => c,
            _ => panic!("node {} is not a connection", id.0),
        }
    }

    fn connection_mut(&mut self, id: NodeId) -> &mut PlannerConnection {
        match &mut self.nodes[id.0] {
            PlannerNode::Connection(c) => c,
            _ => panic!("node {} is not a connection", id.0),
        }
    }

    pub fn fan_out(&self, id: NodeId) -> &PlannerFanOut {
        match &self.nodes[id.0] {
            PlannerNode::FanOut(f) => f,
            _ => panic!("node {} is not a fan-out", id.0),
        }
    }

    pub fn fan_out_mut(&mut self, id: NodeId) -> &mut PlannerFanOut {
        match &mut self.nodes[id.0] {
            PlannerNode::FanOut(f) => f,
            _ => panic!("node {} is not a fan-out", id.0),
        }
    }

    pub fn fan_in(&self, id: NodeId) -> &PlannerFanIn {
        match &self.nodes[id.0] {
            PlannerNode::FanIn(f) => f,
            _ => panic!("node {} is not a fan-in", id.0),
        }
    }

    pub fn fan_in_mut(&mut self, id: NodeId) -> &mut PlannerFanIn {
        match &mut self.nodes[id.0] {
            PlannerNode::FanIn(f) => f,
            _ => panic!("node {} is not a fan-in", id.0),
        }
    }

    /// Forward cost estimation (`estimateCost`). Cost flows from connections up toward
    /// the terminus; a join combines its child + parent estimates.
    pub fn estimate_cost(
        &self,
        id: NodeId,
        downstream_child_sel: f64,
        branch: &[usize],
    ) -> CostEstimate {
        match &self.nodes[id.0] {
            PlannerNode::Connection(c) => c.estimate_cost(downstream_child_sel, branch),
            PlannerNode::Join(j) => {
                // Child cost uses downstreamChildSelectivity = 1 (child chains are
                // independent sub-graphs); only parent rows scale with it.
                let child = self.estimate_cost(j.child, 1.0, branch);

                let keys: Vec<&str> = j.child_constraint().iter().map(|s| s.as_ref()).collect();
                let fanout_factor = (child.fanout)(&keys);
                // 1 - (1 - childSelectivity)^fanout. With fanout == 1 this is exactly
                // childSelectivity (pow(x, 1) == x), so SimpleCostModel never exercises
                // a non-trivial transcendental.
                let scaled_child_selectivity =
                    1.0 - (1.0 - child.selectivity).powf(fanout_factor.fanout);

                // Selectivity flows up from child to parent (a flipped join does not
                // scale the parent by child selectivity — flipping already accounts for
                // it).
                let parent_sel = if j.type_() == JoinType::Flipped {
                    1.0 * downstream_child_sel
                } else {
                    scaled_child_selectivity * downstream_child_sel
                };
                let parent = self.estimate_cost(j.parent, parent_sel, branch);

                j.combine_cost(downstream_child_sel, &child, &parent)
            }
            // Terminus starts cost estimation fresh: selectivity 1, empty pattern.
            PlannerNode::Terminus(t) => self.estimate_cost(t.input(), 1.0, &[]),
            // FanOut is a passthrough: its cost is its input's cost.
            PlannerNode::FanOut(f) => self.estimate_cost(f.input(), downstream_child_sel, branch),
            PlannerNode::FanIn(f) => match f.type_() {
                // FI: a single fetch — take the MAX cost across branches; selectivity
                // is the OR of the branches (1 - ∏(1 - sᵢ)). All inputs share pattern
                // [0, ...branch].
                FanInType::Fi => {
                    let updated = prepend(0, branch);
                    let (mut max_rows, mut max_cost, mut max_startup, mut max_scan) =
                        (0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64);
                    let mut no_match_prob = 1.0_f64;
                    let mut fanout = None;
                    let mut limit = None;
                    for &input in f.inputs() {
                        let c = self.estimate_cost(input, downstream_child_sel, &updated);
                        fanout = Some(c.fanout.clone());
                        if c.returned_rows > max_rows {
                            max_rows = c.returned_rows;
                        }
                        if c.cost > max_cost {
                            max_cost = c.cost;
                        }
                        if c.startup_cost > max_startup {
                            max_startup = c.startup_cost;
                        }
                        if c.scan_est > max_scan {
                            max_scan = c.scan_est;
                        }
                        no_match_prob *= 1.0 - c.selectivity;
                        assert!(
                            limit.is_none() || c.limit == limit,
                            "All FanIn inputs should have the same limit"
                        );
                        limit = c.limit;
                    }
                    CostEstimate {
                        startup_cost: max_startup,
                        scan_est: max_scan,
                        cost: max_cost,
                        returned_rows: max_rows,
                        selectivity: 1.0 - no_match_prob,
                        limit,
                        fanout: fanout.expect("Failed to set fanout model"),
                    }
                }
                // UFI: a fetch per branch — SUM the costs; each input gets a unique
                // pattern [i, ...branch]. Selectivity is still the OR of the branches.
                FanInType::Ufi => {
                    let (mut rows, mut cost, mut scan, mut startup) =
                        (0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64);
                    let mut no_match_prob = 1.0_f64;
                    let mut fanout = None;
                    let mut limit = None;
                    for (i, &input) in f.inputs().iter().enumerate() {
                        let updated = prepend(i, branch);
                        let c = self.estimate_cost(input, downstream_child_sel, &updated);
                        fanout = Some(c.fanout.clone());
                        rows += c.returned_rows;
                        cost += c.cost;
                        scan += c.scan_est;
                        startup += c.startup_cost;
                        no_match_prob *= 1.0 - c.selectivity;
                        assert!(
                            limit.is_none() || c.limit == limit,
                            "All FanIn inputs should have the same limit"
                        );
                        limit = c.limit;
                    }
                    CostEstimate {
                        startup_cost: startup,
                        scan_est: scan,
                        cost,
                        returned_rows: rows,
                        selectivity: 1.0 - no_match_prob,
                        limit,
                        fanout: fanout.expect("Failed to set fanout model"),
                    }
                }
            },
        }
    }

    /// Backward constraint propagation (`propagateConstraints`). Constraints flow from
    /// the terminus down to connections; a join routes them per semi/flipped.
    pub fn propagate_constraints(
        &mut self,
        id: NodeId,
        branch: &[usize],
        constraint: Option<PlannerConstraint>,
    ) {
        enum Action {
            Connection,
            Join {
                parent: NodeId,
                child: NodeId,
                type_: JoinType,
                parent_constraint: PlannerConstraint,
                child_constraint: PlannerConstraint,
            },
            FanOut(NodeId),
            FanIn {
                type_: FanInType,
                inputs: Vec<NodeId>,
            },
            Terminus(NodeId),
        }

        // Read immutably; extract everything we need so the borrow ends before we
        // recurse with `&mut self`.
        let action = match &self.nodes[id.0] {
            PlannerNode::Connection(_) => Action::Connection,
            PlannerNode::Join(j) => Action::Join {
                parent: j.parent,
                child: j.child,
                type_: j.type_(),
                parent_constraint: j.parent_constraint().clone(),
                child_constraint: j.child_constraint().clone(),
            },
            PlannerNode::FanOut(f) => Action::FanOut(f.input()),
            PlannerNode::FanIn(f) => Action::FanIn {
                type_: f.type_(),
                inputs: f.inputs().to_vec(),
            },
            PlannerNode::Terminus(t) => Action::Terminus(t.input()),
        };

        match action {
            Action::Connection => {
                if let PlannerNode::Connection(conn) = &mut self.nodes[id.0] {
                    conn.propagate_constraints(branch, constraint);
                }
            }
            Action::Join {
                parent,
                child,
                type_,
                parent_constraint,
                child_constraint,
            } => match type_ {
                JoinType::Semi => {
                    // Semi: send the correlation constraint to the child, forward the
                    // received constraint to the parent.
                    self.propagate_constraints(child, branch, Some(child_constraint));
                    self.propagate_constraints(parent, branch, constraint);
                }
                JoinType::Flipped => {
                    // Flipped: translate the received constraint into child space; send
                    // the received-merged-with-own constraint to the parent.
                    let translated = translate_constraints_for_flipped_join(
                        constraint.as_ref(),
                        &parent_constraint,
                        &child_constraint,
                    );
                    self.propagate_constraints(child, branch, translated);
                    let merged = flipped_parent_constraint(constraint.as_ref(), &parent_constraint);
                    self.propagate_constraints(parent, branch, merged);
                }
            },
            // FanOut forwards to its single input, unchanged.
            Action::FanOut(input) => self.propagate_constraints(input, branch, constraint),
            // FanIn sends the constraint to every input — same pattern [0,…] for FI,
            // a unique [i,…] per input for UFI.
            Action::FanIn { type_, inputs } => match type_ {
                FanInType::Fi => {
                    let updated = prepend(0, branch);
                    for input in inputs {
                        self.propagate_constraints(input, &updated, constraint.clone());
                    }
                }
                FanInType::Ufi => {
                    for (i, input) in inputs.into_iter().enumerate() {
                        let updated = prepend(i, branch);
                        self.propagate_constraints(input, &updated, constraint.clone());
                    }
                }
            },
            // Terminus starts the flow fresh: empty pattern, no constraint.
            Action::Terminus(input) => self.propagate_constraints(input, &[], None),
        }
    }

    /// Propagate unlimiting from a freshly flipped join (`propagateUnlimit`): the child
    /// becomes the outer loop and must produce all rows, so its chain is unlimited.
    pub fn propagate_unlimit(&mut self, id: NodeId) {
        let child = match &self.nodes[id.0] {
            PlannerNode::Join(j) => {
                assert!(
                    j.type_() == JoinType::Flipped,
                    "Can only unlimit a flipped join"
                );
                j.child
            }
            _ => panic!("propagate_unlimit expects a join"),
        };
        self.propagate_unlimit_from_flipped_join(child);
    }

    /// `propagateUnlimitFromFlippedJoin`: connections unlimit; joins continue up the
    /// parent (outer-loop) chain; fans propagate to their input(s).
    fn propagate_unlimit_from_flipped_join(&mut self, id: NodeId) {
        enum Action {
            Connection,
            Join(NodeId),
            FanOut(NodeId),
            FanIn(Vec<NodeId>),
            // Terminus does not participate in unlimiting.
            Terminus,
        }
        let action = match &self.nodes[id.0] {
            PlannerNode::Connection(_) => Action::Connection,
            PlannerNode::Join(j) => Action::Join(j.parent),
            PlannerNode::FanOut(f) => Action::FanOut(f.input()),
            PlannerNode::FanIn(f) => Action::FanIn(f.inputs().to_vec()),
            PlannerNode::Terminus(_) => Action::Terminus,
        };
        match action {
            Action::Connection => {
                if let PlannerNode::Connection(conn) = &mut self.nodes[id.0] {
                    conn.unlimit();
                }
            }
            Action::Join(parent) => self.propagate_unlimit_from_flipped_join(parent),
            Action::FanOut(input) => self.propagate_unlimit_from_flipped_join(input),
            Action::FanIn(inputs) => {
                for input in inputs {
                    self.propagate_unlimit_from_flipped_join(input);
                }
            }
            Action::Terminus => {}
        }
    }

    // ===================================================================
    // The enumeration algorithm (the `planner-graph.ts` algorithm half).
    // ===================================================================

    /// Reset all nodes' mutable planning state to their constructed baseline
    /// (`resetPlanningState`).
    pub fn reset_planning_state(&mut self) {
        for id in self.joins.clone() {
            self.join_mut(id).reset();
        }
        for id in self.fan_outs.clone() {
            self.fan_out_mut(id).reset();
        }
        for id in self.fan_ins.clone() {
            self.fan_in_mut(id).reset();
        }
        for id in self.connections.clone() {
            self.connection_mut(id).reset();
        }
    }

    /// Start constraint propagation from the terminus (`graph.propagateConstraints`).
    pub fn propagate_constraints_from_terminus(&mut self) {
        let terminus = self
            .terminus
            .expect("Cannot propagate constraints without a terminus");
        self.propagate_constraints(terminus, &[], None);
    }

    /// Total plan cost = `estimate.cost + estimate.startupCost` at the terminus
    /// (`getTotalCost`).
    pub fn get_total_cost(&self) -> f64 {
        let terminus = self
            .terminus
            .expect("Cannot get total cost without a terminus");
        let estimate = self.estimate_cost(terminus, 1.0, &[]);
        estimate.cost + estimate.startup_cost
    }

    /// Snapshot the mutable planning state (`capturePlanningSnapshot`).
    pub fn capture_planning_snapshot(&self) -> PlanState {
        PlanState {
            connections: self
                .connections
                .iter()
                .map(|&id| self.connection(id).limit)
                .collect(),
            joins: self.joins.iter().map(|&id| self.join(id).type_()).collect(),
            fan_outs: self
                .fan_outs
                .iter()
                .map(|&id| self.fan_out(id).type_())
                .collect(),
            fan_ins: self
                .fan_ins
                .iter()
                .map(|&id| self.fan_in(id).type_())
                .collect(),
            connection_constraints: self
                .connections
                .iter()
                .map(|&id| self.connection(id).capture_constraints())
                .collect(),
        }
    }

    /// Restore a snapshot (`restorePlanningSnapshot`). Joins are reset then re-flipped
    /// to the target; fans are only *upgraded* to Union if the target is Union (faithful
    /// to JS — it never downgrades a fan here).
    pub fn restore_planning_snapshot(&mut self, state: &PlanState) {
        assert_eq!(
            self.connections.len(),
            state.connections.len(),
            "Plan state mismatch: connections"
        );
        assert_eq!(
            self.joins.len(),
            state.joins.len(),
            "Plan state mismatch: joins"
        );
        assert_eq!(
            self.fan_outs.len(),
            state.fan_outs.len(),
            "Plan state mismatch: fanOuts"
        );
        assert_eq!(
            self.fan_ins.len(),
            state.fan_ins.len(),
            "Plan state mismatch: fanIns"
        );
        assert_eq!(
            self.connections.len(),
            state.connection_constraints.len(),
            "Plan state mismatch: connectionConstraints"
        );

        for (i, id) in self.connections.clone().into_iter().enumerate() {
            let limit = state.connections[i];
            let constraints = state.connection_constraints[i].clone();
            let c = self.connection_mut(id);
            c.limit = limit;
            c.restore_constraints(constraints);
        }
        for (i, id) in self.joins.clone().into_iter().enumerate() {
            let target = state.joins[i];
            let j = self.join_mut(id);
            j.reset();
            if target == JoinType::Flipped && j.type_() != JoinType::Flipped {
                j.flip();
            }
            assert_eq!(
                target,
                self.join(id).type_(),
                "join is not in the correct state after reset"
            );
        }
        for (i, id) in self.fan_outs.clone().into_iter().enumerate() {
            if state.fan_outs[i] == FanOutType::Ufo && self.fan_out(id).type_() == FanOutType::Fo {
                self.fan_out_mut(id).convert_to_ufo();
            }
        }
        for (i, id) in self.fan_ins.clone().into_iter().enumerate() {
            if state.fan_ins[i] == FanInType::Ufi && self.fan_in(id).type_() == FanInType::Fi {
                self.fan_in_mut(id).convert_to_ufi();
            }
        }
    }

    /// Main planning algorithm (`plan`): exhaustively enumerate the `2^n` flip
    /// patterns over the flippable joins, cost each, and restore the cheapest. The
    /// tie-break is **strict `<`** with patterns enumerated `0..2^n`, so pattern 0 (all
    /// semi) is retained on any cost tie — the result is deterministic and matches JS.
    pub fn plan(&mut self) {
        let flippable: Vec<NodeId> = self
            .joins
            .iter()
            .copied()
            .filter(|&id| self.join(id).is_flippable())
            .collect();

        // Too many flippable joins — skip optimization (the query runs as authored).
        if flippable.len() > MAX_FLIPPABLE_JOINS {
            return;
        }

        let fofi_cache = self.build_fofi_cache();
        let num_patterns: u64 = if flippable.is_empty() {
            0
        } else {
            1u64 << flippable.len()
        };

        let mut best_cost = f64::INFINITY;
        let mut best_plan: Option<PlanState> = None;

        for pattern in 0..num_patterns {
            self.reset_planning_state();

            // Apply the flip pattern (bit i set ⇒ flip flippable join i).
            for (i, &id) in flippable.iter().enumerate() {
                if pattern & (1u64 << i) != 0 {
                    self.join_mut(id).flip();
                }
            }

            self.check_and_convert_fofi(&fofi_cache);
            self.propagate_unlimit_for_flipped_joins();
            self.propagate_constraints_from_terminus();

            let total_cost = self.get_total_cost();
            if total_cost < best_cost {
                best_cost = total_cost;
                best_plan = Some(self.capture_planning_snapshot());
            }
        }

        if let Some(plan) = best_plan {
            self.restore_planning_snapshot(&plan);
            // Re-propagate so all derived state is consistent with the chosen plan.
            self.propagate_constraints_from_terminus();
        } else {
            assert_eq!(
                num_patterns, 0,
                "no plan was found but flippable joins did exist!"
            );
        }
    }

    /// Build the FO→FI cache once (`buildFOFICache`): for each FanOut, the matching
    /// FanIn and the joins between them.
    fn build_fofi_cache(&self) -> Vec<(NodeId, Option<NodeId>, Vec<NodeId>)> {
        self.fan_outs
            .iter()
            .map(|&fo| {
                let (fi, joins_between) = self.find_fi_and_joins(fo);
                (fo, fi, joins_between)
            })
            .collect()
    }

    /// BFS from a FanOut's outputs to its FanIn, collecting the joins along the way
    /// (`findFIAndJoins`).
    fn find_fi_and_joins(&self, fo: NodeId) -> (Option<NodeId>, Vec<NodeId>) {
        let mut joins_between = Vec::new();
        let mut fi = None;
        let mut queue: VecDeque<NodeId> = self.fan_out(fo).outputs().iter().copied().collect();
        let mut visited: HashSet<NodeId> = HashSet::new();

        while let Some(node) = queue.pop_front() {
            if !visited.insert(node) {
                continue;
            }
            match &self.nodes[node.0] {
                PlannerNode::Join(j) => {
                    joins_between.push(node);
                    queue.push_back(j.output());
                }
                PlannerNode::FanOut(f) => {
                    queue.extend(f.outputs().iter().copied());
                }
                PlannerNode::FanIn(_) => {
                    // Found the boundary FanIn — do not traverse past it.
                    fi = Some(node);
                }
                PlannerNode::Connection(_) | PlannerNode::Terminus(_) => {}
            }
        }

        (fi, joins_between)
    }

    /// If any join between a FO and its FI is flipped, upgrade FO→UFO and FI→UFI
    /// (`checkAndConvertFOFI`). Must run after flipping, before constraint propagation.
    fn check_and_convert_fofi(&mut self, cache: &[(NodeId, Option<NodeId>, Vec<NodeId>)]) {
        for (fo, fi, joins_between) in cache {
            let has_flipped = joins_between
                .iter()
                .any(|&j| self.join(j).type_() == JoinType::Flipped);
            if let Some(fi) = fi {
                if has_flipped {
                    self.fan_out_mut(*fo).convert_to_ufo();
                    self.fan_in_mut(*fi).convert_to_ufi();
                }
            }
        }
    }

    /// Unlimit every flipped join's child chain (`propagateUnlimitForFlippedJoins`).
    fn propagate_unlimit_for_flipped_joins(&mut self) {
        for id in self.joins.clone() {
            if self.join(id).type_() == JoinType::Flipped {
                self.propagate_unlimit(id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost::{
        ConnectionCostModel, CostModelCost, FanoutConfidence, FanoutEst, SimpleCostModel,
    };
    use crate::source::PlannerSource;
    use rindle::{Condition, Dir, OrderPart};
    use std::rc::Rc;

    fn cset(items: &[&str]) -> PlannerConstraint {
        items.iter().map(|s| Box::from(*s)).collect()
    }

    fn default_sort() -> Vec<OrderPart> {
        vec![OrderPart(Box::from("id"), Dir::Asc)]
    }

    /// Cost model keyed on table name: `parent` vs `child` rows/startup.
    struct FixedModel {
        parent_rows: f64,
        parent_startup: f64,
        child_rows: f64,
        child_startup: f64,
    }

    impl ConnectionCostModel for FixedModel {
        fn estimate(
            &self,
            table: &str,
            _sort: &[OrderPart],
            _filters: Option<&Condition>,
            _constraint: Option<&PlannerConstraint>,
        ) -> CostModelCost {
            let (startup, rows) = if table == "parent" {
                (self.parent_startup, self.parent_rows)
            } else {
                (self.child_startup, self.child_rows)
            };
            CostModelCost {
                startup_cost: startup,
                rows,
                fanout: Rc::new(|_| FanoutEst {
                    fanout: 1.0,
                    confidence: FanoutConfidence::None,
                }),
            }
        }
    }

    fn conn(g: &mut PlannerGraph, model: Rc<dyn ConnectionCostModel>, table: &str) -> NodeId {
        let src = PlannerSource::new(table, model);
        g.add_connection(src.connect(default_sort(), None, false, None, None))
    }

    /// child startup 50 / rows 1 vs parent rows 1000 ⇒ flipping (scan the small child)
    /// is far cheaper than the per-parent probe.
    fn flip_favoring_model() -> Rc<dyn ConnectionCostModel> {
        Rc::new(FixedModel {
            parent_rows: 1000.0,
            parent_startup: 0.0,
            child_rows: 1.0,
            child_startup: 50.0,
        })
    }

    fn single_join_graph(
        model: Rc<dyn ConnectionCostModel>,
        flippable: bool,
    ) -> (PlannerGraph, NodeId) {
        let mut g = PlannerGraph::new();
        let parent = conn(&mut g, model.clone(), "parent");
        let child = conn(&mut g, model, "child");
        let join = g.add_join(PlannerJoin::new(
            parent,
            child,
            cset(&["userId"]),
            cset(&["id"]),
            flippable,
            0,
        ));
        let terminus = g.add_terminus(join);
        g.set_terminus(terminus);
        (g, join)
    }

    #[test]
    fn plan_flips_join_when_flipped_is_cheaper() {
        let (mut g, join) = single_join_graph(flip_favoring_model(), true);
        g.plan();
        assert_eq!(g.join(join).type_(), JoinType::Flipped);
    }

    #[test]
    fn plan_keeps_semi_on_cost_tie() {
        // SimpleCostModel ⇒ semi and flipped cost the same ⇒ pattern 0 (semi) wins.
        let (mut g, join) = single_join_graph(Rc::new(SimpleCostModel), true);
        g.plan();
        assert_eq!(g.join(join).type_(), JoinType::Semi);
    }

    #[test]
    fn plan_is_noop_for_unflippable_join() {
        // NOT EXISTS (flippable=false) ⇒ 0 flippable joins ⇒ plan does nothing.
        let (mut g, join) = single_join_graph(flip_favoring_model(), false);
        g.plan();
        assert_eq!(g.join(join).type_(), JoinType::Semi);
    }

    #[test]
    fn snapshot_restore_round_trips_join_type() {
        let (mut g, join) = single_join_graph(Rc::new(SimpleCostModel), true);

        let semi = g.capture_planning_snapshot();
        g.join_mut(join).flip();
        assert_eq!(g.join(join).type_(), JoinType::Flipped);
        g.restore_planning_snapshot(&semi);
        assert_eq!(g.join(join).type_(), JoinType::Semi);

        g.join_mut(join).flip();
        let flipped = g.capture_planning_snapshot();
        g.join_mut(join).reset();
        assert_eq!(g.join(join).type_(), JoinType::Semi);
        g.restore_planning_snapshot(&flipped);
        assert_eq!(g.join(join).type_(), JoinType::Flipped);
    }

    #[test]
    fn plan_converts_fofi_when_branch_flips() {
        // OR with one branch: FO → join → FI. Flipping the join (flip-favoring model)
        // must upgrade FO→UFO and FI→UFI.
        let model = flip_favoring_model();
        let mut g = PlannerGraph::new();
        let parent = conn(&mut g, model.clone(), "parent");
        let fo = g.add_fan_out(PlannerFanOut::new(parent));
        let child = conn(&mut g, model, "child");
        let join = g.add_join(PlannerJoin::new(
            fo,
            child,
            cset(&["userId"]),
            cset(&["id"]),
            true,
            0,
        ));
        let fi = g.add_fan_in(PlannerFanIn::new(vec![join]));
        let terminus = g.add_terminus(fi);
        g.set_terminus(terminus);
        // Down-direction wiring for the FOFI BFS.
        g.fan_out_mut(fo).add_output(join);
        g.join_mut(join).set_output(fi);

        g.plan();
        assert_eq!(g.join(join).type_(), JoinType::Flipped);
        assert_eq!(g.fan_out(fo).type_(), FanOutType::Ufo);
        assert_eq!(g.fan_in(fi).type_(), FanInType::Ufi);
    }

    #[test]
    fn fan_in_ufi_total_cost_is_sum_vs_fi_max() {
        // Three symmetric OR branches: FI cost = max(branch) = one branch;
        // UFI cost = sum = 3 branches.
        let model: Rc<dyn ConnectionCostModel> = Rc::new(SimpleCostModel);
        let mut g = PlannerGraph::new();
        let parent = conn(&mut g, model.clone(), "parent");
        let fo = g.add_fan_out(PlannerFanOut::new(parent));
        let mut joins = Vec::new();
        for i in 0..3u32 {
            let child = conn(&mut g, model.clone(), &format!("child{i}"));
            let j = g.add_join(PlannerJoin::new(
                fo,
                child,
                cset(&["userId"]),
                cset(&["id"]),
                true,
                i,
            ));
            joins.push(j);
        }
        let fi = g.add_fan_in(PlannerFanIn::new(joins.clone()));
        let terminus = g.add_terminus(fi);
        g.set_terminus(terminus);
        for &j in &joins {
            g.fan_out_mut(fo).add_output(j);
            g.join_mut(j).set_output(fi);
        }

        let fi_cost = g.get_total_cost();
        g.fan_out_mut(fo).convert_to_ufo();
        g.fan_in_mut(fi).convert_to_ufi();
        let ufi_cost = g.get_total_cost();

        assert!(fi_cost > 0.0);
        assert_eq!(ufi_cost, 3.0 * fi_cost);
    }
}
