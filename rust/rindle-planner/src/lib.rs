//! # `rindle-planner` — Zero's cost-based query planner, ported to Rust
//!
//! A faithful port of `mono/packages/zql/src/planner/`. The planner is a pure
//! transform
//!
//! ```text
//! plan_ast(ast, cost_model) -> ast
//! ```
//!
//! whose only effect is annotating `WHERE EXISTS` correlated-subquery conditions
//! with `flip: true` when the cost model says the *child-driven* (flipped) join is
//! cheaper than the default *parent-driven* (semi) join. It runs **before lowering**
//! — `rindle::build_pipeline` already consumes `flip` end-to-end (the `FlippedJoin`
//! operator). The planner is the missing producer of `flip`; the consumer exists.
//!
//! The algorithm builds an in-memory `PlannerGraph` from the AST, exhaustively
//! enumerates the `2^n` join-flip patterns (`n` flippable joins, capped at
//! `MAX_FLIPPABLE_JOINS = 9`), propagates constraints backward + estimates cost
//! forward for each, and rewrites the AST for the cheapest plan.
//!
//! ## Staging (see `PLANNER-PORT-DESIGN.md`)
//!
//! - **Stage 0 (this module):** the cost-model seam ([`ConnectionCostModel`],
//!   [`CostModelCost`], [`FanoutEst`]) + [`PlannerConstraint`]/[`merge_constraints`]
//!   + the shared deterministic [`SimpleCostModel`] oracle.
//! - **Stages 1–5:** the graph nodes, the enumeration, and `plan_ast` /
//!   `apply_plans_to_ast`.
//! - **Stage 6:** the JS↔Rust differential (the parity gate).
//!
//! The public seam is [`plan_ast`] itself (a pure AST→AST transform, the `planQuery`
//! port): a consumer lowers with `rindle::build_pipeline(&plan_ast(&ast, cost))`. The
//! planner never depends on the build pipeline.
//!
//! Parity is validated against the JS planner running the *same* `SimpleCostModel`
//! over the *same* ASTs; because that model returns `fanout = 1` the one
//! transcendental in the core (`pow(x, 1) = x`) is trivial, so the differential is
//! exact. See `PLANNER-PORT-DESIGN.md` §4.

mod builder;
mod caching;
mod connection;
mod constraint;
mod cost;
mod fan;
mod graph;
mod join;
mod node;
mod source;
mod terminus;

pub use builder::{apply_plans_to_ast, build_plan_graph, has_flippable_exists, plan_ast, Plans};
pub use caching::CachingCostModel;
pub use connection::PlannerConnection;
pub use constraint::{merge_constraints, PlannerConstraint};
pub use cost::{
    ConnectionCostModel, CostModelCost, FanoutConfidence, FanoutCostModel, FanoutEst,
    SimpleCostModel, BASE_COST, CONSTRAINT_REDUCTION,
};
pub use fan::{FanInType, FanOutType, PlannerFanIn, PlannerFanOut};
pub use graph::{PlanState, PlannerGraph};
pub use join::{
    multi_constraint_chunk_size, set_multi_constraint_chunk_size, ChunkSizeGuard, JoinType,
    PlannerJoin,
};
pub use node::{CostEstimate, CostNoFanout, NodeId};
pub use source::PlannerSource;
pub use terminus::PlannerTerminus;
