//! `PlannerJoin` — a join between a parent and child stream, i.e. one EXISTS /
//! NOT EXISTS check (port of `planner-join.ts`). This is the cost epicenter.
//!
//! ## Dual state
//! - **Immutable structure:** `parent`/`child` node ids, the parent/child correlation
//!   constraints, `flippable` (false for NOT EXISTS), `plan_id`, `initial_type`.
//! - **Mutable planning state:** `type_` (`semi` ⇄ `flipped`), reset between attempts.
//!
//! A semi-join scans the parent and probes the child per row; a flipped join scans the
//! child and batch-fetches parents. The recursion over parent/child lives on
//! [`PlannerGraph`](crate::graph) (the arena); this module holds the node data plus the
//! *pure* pieces: [`PlannerJoin::combine_cost`] (the cost formula given the two child /
//! parent estimates), [`translate_constraints_for_flipped_join`], `flip`/`reset`.
//!
//! The cost formulas are transcribed verbatim from JS — operation order is load-bearing
//! (IEEE-754 is not associative), so the parenthesization is preserved exactly.

use std::cell::Cell;

use crate::constraint::{merge_constraints, PlannerConstraint};
use crate::node::{CostEstimate, NodeId};

/// A join's execution direction (`'semi' | 'flipped'`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    /// Parent is the outer loop, child the inner (the default EXISTS semi-join).
    Semi,
    /// Child is the outer loop, parent batch-fetched (the flipped fast path).
    Flipped,
}

thread_local! {
    /// The flipped-join IN-batch chunk size the planner costs against. Mirrors the
    /// runtime `getMultiConstraintChunkSize()` (default 256). Thread-local so the
    /// test seam ([`set_multi_constraint_chunk_size`]) can override it without racing
    /// other parallel tests; production never sets it.
    static MULTI_CONSTRAINT_CHUNK_SIZE: Cell<usize> = const { Cell::new(256) };
}

/// The chunk size used by the flipped-join cost (`getMultiConstraintChunkSize`).
pub fn multi_constraint_chunk_size() -> f64 {
    MULTI_CONSTRAINT_CHUNK_SIZE.with(|c| c.get()) as f64
}

/// Test seam: override the chunk size for the current thread; the returned guard
/// restores the previous value on drop (`setMultiConstraintChunkSizeForTest`, which
/// returns a restore fn). Keeps the planner cost and the IVM chunking in sync.
pub fn set_multi_constraint_chunk_size(size: usize) -> ChunkSizeGuard {
    let prev = MULTI_CONSTRAINT_CHUNK_SIZE.with(|c| c.replace(size));
    ChunkSizeGuard { prev }
}

/// RAII restore guard for [`set_multi_constraint_chunk_size`].
pub struct ChunkSizeGuard {
    prev: usize,
}

impl Drop for ChunkSizeGuard {
    fn drop(&mut self) {
        MULTI_CONSTRAINT_CHUNK_SIZE.with(|c| c.set(self.prev));
    }
}

pub struct PlannerJoin {
    // ---- immutable structure ----
    pub parent: NodeId,
    pub child: NodeId,
    parent_constraint: PlannerConstraint,
    child_constraint: PlannerConstraint,
    flippable: bool,
    /// Sequential plan id (assigned during graph construction), used to map this join
    /// back to its AST condition when applying the plan.
    pub plan_id: u32,
    initial_type: JoinType,
    /// Downstream node (set during graph wiring). Used by the FOFI BFS to walk
    /// FanOut → … → FanIn.
    output: Option<NodeId>,
    // ---- mutable planning state ----
    type_: JoinType,
}

impl PlannerJoin {
    pub fn new(
        parent: NodeId,
        child: NodeId,
        parent_constraint: PlannerConstraint,
        child_constraint: PlannerConstraint,
        flippable: bool,
        plan_id: u32,
    ) -> Self {
        Self::with_initial_type(
            parent,
            child,
            parent_constraint,
            child_constraint,
            flippable,
            plan_id,
            JoinType::Semi,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_initial_type(
        parent: NodeId,
        child: NodeId,
        parent_constraint: PlannerConstraint,
        child_constraint: PlannerConstraint,
        flippable: bool,
        plan_id: u32,
        initial_type: JoinType,
    ) -> Self {
        Self {
            parent,
            child,
            parent_constraint,
            child_constraint,
            flippable,
            plan_id,
            initial_type,
            output: None,
            type_: initial_type,
        }
    }

    pub fn kind(&self) -> &'static str {
        "join"
    }

    pub fn set_output(&mut self, node: NodeId) {
        self.output = Some(node);
    }

    pub fn output(&self) -> NodeId {
        self.output.expect("Output not set")
    }

    pub fn type_(&self) -> JoinType {
        self.type_
    }

    pub fn is_flippable(&self) -> bool {
        self.flippable
    }

    pub(crate) fn parent_constraint(&self) -> &PlannerConstraint {
        &self.parent_constraint
    }

    pub(crate) fn child_constraint(&self) -> &PlannerConstraint {
        &self.child_constraint
    }

    /// `flip()` — semi → flipped. Panics (the JS `UnflippableJoinError` / assert) on a
    /// non-flippable or already-flipped join; the enumeration only ever flips
    /// flippable semi-joins, so this is unreachable in normal flow.
    pub fn flip(&mut self) {
        assert!(self.type_ == JoinType::Semi, "Can only flip a semi-join");
        assert!(
            self.flippable,
            "Cannot flip a non-flippable join (e.g., NOT EXISTS)"
        );
        self.type_ = JoinType::Flipped;
    }

    /// Flip iff the driving `input` is the child (`flipIfNeeded`); otherwise it must be
    /// the parent.
    pub fn flip_if_needed(&mut self, input: NodeId) {
        if input == self.child {
            self.flip();
        } else {
            assert!(
                input == self.parent,
                "Can only flip a join from one of its inputs"
            );
        }
    }

    pub fn reset(&mut self) {
        self.type_ = self.initial_type;
    }

    /// Combine the child + parent cost estimates into this join's cost — the semi /
    /// flipped formulas (`estimateCost`), transcribed verbatim. `downstream_child_sel`
    /// is the selectivity flowing *down* into this join (it bounds how many parent
    /// rows we scan against the limit).
    pub(crate) fn combine_cost(
        &self,
        downstream_child_sel: f64,
        child: &CostEstimate,
        parent: &CostEstimate,
    ) -> CostEstimate {
        match self.type_ {
            JoinType::Semi => CostEstimate {
                startup_cost: parent.startup_cost,
                scan_est: match parent.limit {
                    None => parent.returned_rows,
                    Some(limit) => parent.returned_rows.min(if downstream_child_sel == 0.0 {
                        0.0
                    } else {
                        limit / downstream_child_sel
                    }),
                },
                cost: parent.cost
                    + parent.scan_est * (child.startup_cost + child.cost + child.scan_est),
                returned_rows: parent.returned_rows * child.selectivity,
                selectivity: child.selectivity * parent.selectivity,
                limit: parent.limit,
                fanout: parent.fanout.clone(),
            },
            JoinType::Flipped => CostEstimate {
                startup_cost: child.startup_cost,
                scan_est: match parent.limit {
                    None => parent.returned_rows * child.returned_rows,
                    Some(limit) => (parent.returned_rows * child.returned_rows).min(
                        if downstream_child_sel == 0.0 {
                            0.0
                        } else {
                            limit / downstream_child_sel
                        },
                    ),
                },
                // child.cost + ceil(child.scanEst / chunk) * parent.startupCost
                //            + child.scanEst * (parent.cost + parent.scanEst)
                cost: child.cost
                    + (child.scan_est / multi_constraint_chunk_size()).ceil() * parent.startup_cost
                    + child.scan_est * (parent.cost + parent.scan_est),
                returned_rows: parent.returned_rows * child.returned_rows,
                selectivity: parent.selectivity * child.selectivity,
                limit: parent.limit,
                fanout: parent.fanout.clone(),
            },
        }
    }
}

/// Translate constraints across a flipped join, parent space → child space
/// (`translateConstraintsForFlippedJoin`). The parent's i-th constraint key maps to
/// the child's i-th key — positions mirror the correlation `parent_field[i]` ↔
/// `child_field[i]`, which is why [`PlannerConstraint`] must preserve insertion order.
pub(crate) fn translate_constraints_for_flipped_join(
    incoming: Option<&PlannerConstraint>,
    parent_constraint: &PlannerConstraint,
    child_constraint: &PlannerConstraint,
) -> Option<PlannerConstraint> {
    let incoming = incoming?;
    let parent_keys: Vec<&str> = parent_constraint.iter().map(|s| s.as_ref()).collect();
    let child_keys: Vec<&str> = child_constraint.iter().map(|s| s.as_ref()).collect();

    let mut translated = PlannerConstraint::new();
    for key in incoming {
        if let Some(index) = parent_keys.iter().position(|k| *k == key.as_ref()) {
            // Map to the child key at the same position (well-formed correlations are
            // equal length, so the child key is always present).
            if let Some(child_key) = child_keys.get(index) {
                translated.insert(Box::from(*child_key));
            }
        }
    }

    if translated.is_empty() {
        None
    } else {
        Some(translated)
    }
}

/// The flipped-join constraint routing for the parent side (`mergeConstraints(constraint,
/// this.#parentConstraint)`): the output's constraints merged with the child's own.
pub(crate) fn flipped_parent_constraint(
    incoming: Option<&PlannerConstraint>,
    parent_constraint: &PlannerConstraint,
) -> Option<PlannerConstraint> {
    merge_constraints(incoming, Some(parent_constraint))
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
    use rindle::{Dir, OrderPart};
    use std::rc::Rc;

    fn cset(items: &[&str]) -> PlannerConstraint {
        items.iter().map(|s| Box::from(*s)).collect()
    }

    fn default_sort() -> Vec<OrderPart> {
        vec![OrderPart(Box::from("id"), Dir::Asc)]
    }

    /// Mirrors `helpers.ts` `createJoin()`: parent `users`, child `posts`, both via
    /// `simpleCostModel`; parentConstraint `userId`, childConstraint `id`; flippable.
    /// Returns the graph plus the parent/child/join ids.
    fn create_join(flippable: bool) -> (PlannerGraph, NodeId, NodeId, NodeId) {
        create_join_with(flippable, cset(&["userId"]), cset(&["id"]))
    }

    fn create_join_with(
        flippable: bool,
        parent_constraint: PlannerConstraint,
        child_constraint: PlannerConstraint,
    ) -> (PlannerGraph, NodeId, NodeId, NodeId) {
        let model: Rc<dyn ConnectionCostModel> = Rc::new(SimpleCostModel);
        let parent_src = PlannerSource::new("users", model.clone());
        let child_src = PlannerSource::new("posts", model);
        let mut g = PlannerGraph::new();
        let parent = g.add_connection(parent_src.connect(default_sort(), None, false, None, None));
        let child = g.add_connection(child_src.connect(default_sort(), None, false, None, None));
        let join = g.add_join(PlannerJoin::new(
            parent,
            child,
            parent_constraint,
            child_constraint,
            flippable,
            0,
        ));
        (g, parent, child, join)
    }

    #[test]
    fn initial_state_is_semi() {
        let (g, _p, _c, join) = create_join(true);
        assert_eq!(g.join(join).kind(), "join");
        assert_eq!(g.join(join).type_(), JoinType::Semi);
    }

    #[test]
    fn can_flip_when_flippable() {
        let (mut g, _p, _c, join) = create_join(true);
        g.join_mut(join).flip();
        assert_eq!(g.join(join).type_(), JoinType::Flipped);
    }

    #[test]
    #[should_panic(expected = "non-flippable")]
    fn cannot_flip_when_not_flippable() {
        let (mut g, _p, _c, join) = create_join(false);
        g.join_mut(join).flip();
    }

    #[test]
    #[should_panic(expected = "Can only flip a semi-join")]
    fn cannot_flip_when_already_flipped() {
        let (mut g, _p, _c, join) = create_join(true);
        g.join_mut(join).flip();
        g.join_mut(join).flip();
    }

    #[test]
    fn flip_if_needed_flips_on_child_input() {
        let (mut g, _p, child, join) = create_join(true);
        g.join_mut(join).flip_if_needed(child);
        assert_eq!(g.join(join).type_(), JoinType::Flipped);
    }

    #[test]
    fn flip_if_needed_no_flip_on_parent_input() {
        let (mut g, parent, _c, join) = create_join(true);
        g.join_mut(join).flip_if_needed(parent);
        assert_eq!(g.join(join).type_(), JoinType::Semi);
    }

    #[test]
    fn reset_clears_flipped() {
        let (mut g, _p, _c, join) = create_join(true);
        g.join_mut(join).flip();
        assert_eq!(g.join(join).type_(), JoinType::Flipped);
        g.join_mut(join).reset();
        assert_eq!(g.join(join).type_(), JoinType::Semi);
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

    #[test]
    fn semi_propagate_leaves_other_branch_unconstrained() {
        let (mut g, _p, child, join) = create_join(true);
        g.propagate_constraints(join, &[0], None);
        // Branch [] was never constrained → still base cost.
        assert_eq!(
            g.estimate_cost(child, 1.0, &[]).omit_fanout(),
            unconstrained()
        );
    }

    #[test]
    fn flipped_propagate_sends_undefined_to_child() {
        let (mut g, _p, child, join) = create_join(true);
        g.join_mut(join).flip();
        g.propagate_constraints(join, &[0], None);
        assert_eq!(
            g.estimate_cost(child, 1.0, &[]).omit_fanout(),
            unconstrained()
        );
    }

    #[test]
    fn flipped_propagate_merges_for_parent() {
        let (mut g, parent, _c, join) =
            create_join_with(true, cset(&["userId"]), cset(&["postId"]));
        g.join_mut(join).flip();
        g.propagate_constraints(join, &[0], Some(cset(&["name"])));
        // Branch [] on the parent is unconstrained.
        assert_eq!(
            g.estimate_cost(parent, 1.0, &[]).omit_fanout(),
            unconstrained()
        );
    }

    #[test]
    fn semi_and_flipped_have_equal_cost_in_base_case() {
        let (mut g, _p, _c, join) = create_join(true);
        let semi_cost = g.estimate_cost(join, 1.0, &[]).cost;
        g.join_mut(join).reset();
        g.join_mut(join).flip();
        let flipped_cost = g.estimate_cost(join, 1.0, &[]).cost;
        assert_eq!(semi_cost, flipped_cost); // both 10000
        assert_eq!(semi_cost / flipped_cost, 1.0);
    }

    #[test]
    fn propagate_unlimit_clears_child_limit_when_flipped() {
        let model: Rc<dyn ConnectionCostModel> = Rc::new(SimpleCostModel);
        let parent_src = PlannerSource::new("users", model.clone());
        let child_src = PlannerSource::new("posts", model);
        let mut g = PlannerGraph::new();
        let parent = g.add_connection(parent_src.connect(default_sort(), None, false, None, None));
        // Child has an EXISTS limit of 1; flipping makes it the outer loop → unlimited.
        let child =
            g.add_connection(child_src.connect(default_sort(), None, false, None, Some(1.0)));
        let join = g.add_join(PlannerJoin::new(
            parent,
            child,
            cset(&["userId"]),
            cset(&["id"]),
            true,
            0,
        ));
        g.join_mut(join).flip();
        g.propagate_unlimit(join);
        assert_eq!(g.connection(child).limit, None);
    }

    // ---- flipped-join chunk-boundary cost ----

    const PARENT_STARTUP: f64 = 100.0;

    /// Cost model: `parent` has startup 100 / 1 row; `child` returns `child_rows`.
    struct ChunkModel {
        child_rows: f64,
    }

    impl ConnectionCostModel for ChunkModel {
        fn estimate(
            &self,
            table: &str,
            _sort: &[OrderPart],
            _filters: Option<&rindle::Condition>,
            _constraint: Option<&PlannerConstraint>,
        ) -> CostModelCost {
            let fanout = Rc::new(|_: &[&str]| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None,
            });
            if table == "parent" {
                CostModelCost {
                    startup_cost: PARENT_STARTUP,
                    rows: 1.0,
                    fanout,
                }
            } else {
                CostModelCost {
                    startup_cost: 0.0,
                    rows: self.child_rows,
                    fanout,
                }
            }
        }
    }

    fn flipped_cost(child_rows: f64) -> f64 {
        let model: Rc<dyn ConnectionCostModel> = Rc::new(ChunkModel { child_rows });
        let parent_src = PlannerSource::new("parent", model.clone());
        let child_src = PlannerSource::new("child", model);
        let mut g = PlannerGraph::new();
        let parent = g.add_connection(parent_src.connect(default_sort(), None, false, None, None));
        let child = g.add_connection(child_src.connect(default_sort(), None, false, None, None));
        let join = g.add_join(PlannerJoin::new(
            parent,
            child,
            cset(&["userId"]),
            cset(&["id"]),
            true,
            0,
        ));
        g.join_mut(join).flip();
        g.estimate_cost(join, 1.0, &[]).cost
    }

    #[test]
    fn cost_jumps_by_parent_startup_at_each_chunk_boundary() {
        // chunk size 2: ceil(N/2)*100 + N*(parent.cost + parent.scanEst) = ceil(N/2)*100 + N
        let _g = set_multi_constraint_chunk_size(2);
        assert_eq!(flipped_cost(1.0), 1.0 * PARENT_STARTUP + 1.0);
        assert_eq!(flipped_cost(2.0), 1.0 * PARENT_STARTUP + 2.0);
        assert_eq!(flipped_cost(3.0), 2.0 * PARENT_STARTUP + 3.0);
        assert_eq!(flipped_cost(4.0), 2.0 * PARENT_STARTUP + 4.0);
        assert_eq!(flipped_cost(5.0), 3.0 * PARENT_STARTUP + 5.0);
    }

    #[test]
    fn cost_respects_default_chunk_size_at_256_boundary() {
        let c = multi_constraint_chunk_size(); // 256
        assert_eq!(flipped_cost(1.0), 1.0 * PARENT_STARTUP + 1.0);
        assert_eq!(flipped_cost(c), 1.0 * PARENT_STARTUP + c);
        assert_eq!(flipped_cost(c + 1.0), 2.0 * PARENT_STARTUP + (c + 1.0));
        assert_eq!(flipped_cost(2.0 * c), 2.0 * PARENT_STARTUP + 2.0 * c);
        assert_eq!(
            flipped_cost(2.0 * c + 1.0),
            3.0 * PARENT_STARTUP + (2.0 * c + 1.0)
        );
    }

    #[test]
    fn chunk_size_seam_is_observed_and_restored() {
        let default_cost = flipped_cost(256.0);
        {
            let _g = set_multi_constraint_chunk_size(64);
            // 256 rows / 64 = 4 chunks.
            assert_eq!(flipped_cost(256.0), 4.0 * PARENT_STARTUP + 256.0);
        }
        // Guard dropped → back to the default (1 chunk for 256).
        assert_eq!(flipped_cost(256.0), default_cost);
    }

    // ---- constraint translation (the IndexSet order fix) ----

    #[test]
    fn translate_uses_correlation_order_not_sorted() {
        // parent_field [b, a] correlates child_field [x, y] (b↔x, a↔y).
        let parent_c = cset(&["b", "a"]);
        let child_c = cset(&["x", "y"]);
        let out = translate_constraints_for_flipped_join(Some(&cset(&["b"])), &parent_c, &child_c)
            .unwrap();
        let keys: Vec<&str> = out.iter().map(|s| s.as_ref()).collect();
        // b is parent index 0 → child index 0 = "x". A sorted set would give "y".
        assert_eq!(keys, vec!["x"]);
    }

    #[test]
    fn translate_none_incoming_is_none() {
        let parent_c = cset(&["a"]);
        let child_c = cset(&["x"]);
        assert_eq!(
            translate_constraints_for_flipped_join(None, &parent_c, &child_c),
            None
        );
    }
}
