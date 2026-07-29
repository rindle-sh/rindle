//! The AST↔graph bridge + the public entry (`planner-builder.ts`):
//! [`build_plan_graph`] walks an AST into a [`PlannerGraph`], [`plan_ast`] plans it
//! and rewrites the AST's `flip` flags, and [`apply_plans_to_ast`] applies a planned
//! graph back onto its AST.
//!
//! ## `plan_id`
//! Each `WHERE EXISTS` condition gets a sequential `plan_id`, assigned in
//! [`Builder::process_correlated_subquery`] **after** recursing the child `where` (so
//! nested EXISTS get lower ids), in depth-first left-to-right order. Rather than stamp
//! the AST (the JS uses a Symbol annotation), [`apply_to_condition`] **re-derives** the
//! same ids by walking the AST with the identical traversal — only correlated-subquery
//! nodes advance the counter, so `process_or`'s filtering of non-subquery branches does
//! not perturb the sequence.

use std::collections::HashSet;
use std::rc::Rc;

use rindle::{Ast, Condition, CorrelatedSubqueryCondition, ExistsOp};

use crate::constraint::PlannerConstraint;
use crate::cost::ConnectionCostModel;
use crate::fan::{PlannerFanIn, PlannerFanOut};
use crate::graph::PlannerGraph;
use crate::join::{JoinType, PlannerJoin};
use crate::node::NodeId;

/// A built plan graph plus one independently-planned sub-graph per `related` alias
/// (`Plans`). `sub_plans` is in `ast.related` order (matching JS Object insertion order).
pub struct Plans {
    pub plan: PlannerGraph,
    pub sub_plans: Vec<(Box<str>, Plans)>,
}

struct Builder {
    graph: PlannerGraph,
    model: Rc<dyn ConnectionCostModel>,
    next_plan_id: u32,
}

impl Builder {
    fn process_condition(
        &mut self,
        condition: &Condition,
        input: NodeId,
        parent_table: &str,
    ) -> NodeId {
        match condition {
            Condition::Simple(_) => input,
            Condition::And { conditions } => self.process_and(conditions, input, parent_table),
            Condition::Or { conditions } => self.process_or(conditions, input, parent_table),
            Condition::CorrelatedSubquery(csq) => {
                self.process_correlated_subquery(csq, input, parent_table)
            }
        }
    }

    fn process_and(
        &mut self,
        conditions: &[Condition],
        input: NodeId,
        parent_table: &str,
    ) -> NodeId {
        let mut end = input;
        for c in conditions {
            end = self.process_condition(c, end, parent_table);
        }
        end
    }

    fn process_or(
        &mut self,
        conditions: &[Condition],
        input: NodeId,
        parent_table: &str,
    ) -> NodeId {
        // Only branches that contain a correlated subquery participate in the fan.
        let subquery_conditions: Vec<&Condition> = conditions
            .iter()
            .filter(|c| matches!(c, Condition::CorrelatedSubquery(_)) || has_correlated_subquery(c))
            .collect();
        if subquery_conditions.is_empty() {
            return input;
        }

        let fan_out = self.graph.add_fan_out(PlannerFanOut::new(input));
        self.graph.wire_output(input, fan_out);

        let mut branches = Vec::new();
        for c in subquery_conditions {
            let branch = self.process_condition(c, fan_out, parent_table);
            branches.push(branch);
            self.graph.fan_out_mut(fan_out).add_output(branch);
        }

        let fan_in = self.graph.add_fan_in(PlannerFanIn::new(branches.clone()));
        for &branch in &branches {
            self.graph.wire_output(branch, fan_in);
        }
        fan_in
    }

    fn process_correlated_subquery(
        &mut self,
        condition: &CorrelatedSubqueryCondition,
        input: NodeId,
        _parent_table: &str,
    ) -> NodeId {
        let related = &condition.related;
        let child_table = &related.subquery.table;

        let child_source = if self.graph.has_source(child_table) {
            self.graph.get_source(child_table)
        } else {
            self.graph.add_source(child_table, self.model.clone())
        };

        // EXISTS child connections are limited to 1; NOT EXISTS are unlimited.
        let child_limit = if condition.op == ExistsOp::Exists {
            Some(1.0)
        } else {
            None
        };
        let child_connection = child_source.connect(
            related.subquery.order_by.clone(),
            related.subquery.r#where.clone(),
            false,
            None, // no base constraints for EXISTS / NOT EXISTS
            child_limit,
        );
        let mut child_end = self.graph.add_connection(child_connection);

        if let Some(child_where) = &related.subquery.r#where {
            child_end = self.process_condition(child_where, child_end, child_table);
        }

        let parent_constraint = extract_constraint(&related.correlation.parent_field);
        let child_constraint = extract_constraint(&related.correlation.child_field);

        // Assigned AFTER the child recursion (nested EXISTS get lower ids).
        let plan_id = self.next_plan_id;
        self.next_plan_id += 1;

        let (flippable, initial_type) = if condition.op == ExistsOp::NotExists {
            (false, JoinType::Semi) // NOT EXISTS can never flip
        } else {
            match condition.flip {
                Some(true) => (false, JoinType::Flipped), // user forced flip
                Some(false) => (false, JoinType::Semi),   // user forced semi
                None => (true, JoinType::Semi),           // planner decides
            }
        };

        let join = self.graph.add_join(PlannerJoin::with_initial_type(
            input,
            child_end,
            parent_constraint,
            child_constraint,
            flippable,
            plan_id,
            initial_type,
        ));
        self.graph.wire_output(input, join);
        self.graph.wire_output(child_end, join);
        join
    }
}

/// Build a plan graph from an AST (`buildPlanGraph`), recursing `related` into
/// independent sub-plans.
pub fn build_plan_graph(
    ast: &Ast,
    model: Rc<dyn ConnectionCostModel>,
    is_root: bool,
    base_constraints: Option<PlannerConstraint>,
) -> Plans {
    let mut b = Builder {
        graph: PlannerGraph::new(),
        model: model.clone(),
        next_plan_id: 0,
    };

    let source = b.graph.add_source(&ast.table, model.clone());
    let connection = source.connect(
        ast.order_by.clone(),
        ast.r#where.clone(),
        is_root,
        base_constraints,
        ast.limit.map(|l| l as f64),
    );
    let conn_id = b.graph.add_connection(connection);

    let mut end = conn_id;
    if let Some(where_) = &ast.r#where {
        end = b.process_condition(where_, conn_id, &ast.table);
    }

    let terminus = b.graph.add_terminus(end);
    b.graph.wire_output(end, terminus);
    b.graph.set_terminus(terminus);

    let mut sub_plans = Vec::new();
    for csq in &ast.related {
        let alias = csq
            .subquery
            .alias
            .clone()
            .expect("Related subquery must have alias");
        let child_constraints = extract_constraint(&csq.correlation.child_field);
        let sub = build_plan_graph(&csq.subquery, model.clone(), true, Some(child_constraints));
        sub_plans.push((alias, sub));
    }

    Plans {
        plan: b.graph,
        sub_plans,
    }
}

fn has_correlated_subquery(condition: &Condition) -> bool {
    match condition {
        Condition::CorrelatedSubquery(_) => true,
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().any(has_correlated_subquery)
        }
        Condition::Simple(_) => false,
    }
}

/// `extractConstraint`: the correlation field names as an insertion-ordered key-set.
fn extract_constraint(fields: &[Box<str>]) -> PlannerConstraint {
    fields.iter().cloned().collect()
}

fn plan_recursively(plans: &mut Plans) {
    for (_, sub) in &mut plans.sub_plans {
        plan_recursively(sub);
    }
    plans.plan.plan();
}

/// Plan a query (`planQuery`): build the graph, plan it (sub-plans first), and return a
/// new AST with the chosen `flip` flags. Pure — the input AST is not mutated.
pub fn plan_ast(ast: &Ast, model: Rc<dyn ConnectionCostModel>) -> Ast {
    let mut plans = build_plan_graph(ast, model, true, None);
    plan_recursively(&mut plans);
    apply_plans_to_ast(ast, &plans)
}

/// Whether `plan_ast` could change anything — i.e. the AST contains at least one
/// `EXISTS` whose `flip` the planner gets to decide (`op == Exists && flip.is_none()`).
/// If this is `false`, `plan_ast` is a guaranteed no-op (it would build a graph with no
/// flippable joins, enumerate nothing, and clone the AST unchanged), so callers can skip
/// it and lower the AST directly — avoiding the plan-graph build, the cost-model
/// construction, and the clone on every plain query.
///
/// **Over-approximation (deliberately safe):** this recurses into *every* subquery and
/// `related` sub-plan, so it returns `true` for some EXISTS the planner ultimately leaves
/// alone (e.g. one buried in an EXISTS child's own `related`, which the planner does not
/// flip). That only costs a wasted no-op plan — it never returns `false` when the planner
/// would actually flip something. `NotExists` and already-`flip`-set conditions are
/// correctly excluded (the planner is a no-op for them: the builder honors a user-set
/// `flip` without the planner, and NOT EXISTS never flips). Mirrors the flippability rule
/// in `Builder::process_correlated_subquery` (a private item); keep the two in sync.
pub fn has_flippable_exists(ast: &Ast) -> bool {
    fn condition_has(c: &Condition) -> bool {
        match c {
            Condition::Simple(_) => false,
            Condition::And { conditions } | Condition::Or { conditions } => {
                conditions.iter().any(condition_has)
            }
            Condition::CorrelatedSubquery(csq) => {
                (csq.op == ExistsOp::Exists && csq.flip.is_none())
                    || has_flippable_exists(&csq.related.subquery)
            }
        }
    }
    ast.r#where.as_ref().is_some_and(condition_has)
        || ast
            .related
            .iter()
            .any(|r| has_flippable_exists(&r.subquery))
}

fn apply_to_condition(
    condition: &Condition,
    flipped_ids: &HashSet<u32>,
    next_plan_id: &mut u32,
) -> Condition {
    match condition {
        Condition::Simple(_) => condition.clone(),
        Condition::CorrelatedSubquery(csq) => {
            // Recurse the child `where` FIRST (consuming nested ids), then assign this
            // condition's id — mirroring build_plan_graph's traversal exactly.
            let new_child_where = csq
                .related
                .subquery
                .r#where
                .as_ref()
                .map(|w| apply_to_condition(w, flipped_ids, next_plan_id));
            let plan_id = *next_plan_id;
            *next_plan_id += 1;

            let mut new_csq = csq.clone();
            new_csq.flip = Some(flipped_ids.contains(&plan_id));
            new_csq.related.subquery.r#where = new_child_where;
            Condition::CorrelatedSubquery(new_csq)
        }
        Condition::And { conditions } => {
            let mut new = Vec::with_capacity(conditions.len());
            for c in conditions {
                new.push(apply_to_condition(c, flipped_ids, next_plan_id));
            }
            Condition::And { conditions: new }
        }
        Condition::Or { conditions } => {
            let mut new = Vec::with_capacity(conditions.len());
            for c in conditions {
                new.push(apply_to_condition(c, flipped_ids, next_plan_id));
            }
            Condition::Or { conditions: new }
        }
    }
}

/// Apply a planned graph back onto its AST (`applyPlansToAST`): set `flip` on each
/// correlated-subquery condition whose join was flipped, recursing `related` into its
/// sub-plan. Each (sub-)plan re-derives its own `plan_id` counter from 0.
pub fn apply_plans_to_ast(ast: &Ast, plans: &Plans) -> Ast {
    let flipped_ids = plans.plan.flipped_join_plan_ids();
    let mut new_ast = ast.clone();

    let mut counter = 0u32;
    new_ast.r#where = ast
        .r#where
        .as_ref()
        .map(|w| apply_to_condition(w, &flipped_ids, &mut counter));

    new_ast.related = ast
        .related
        .iter()
        .map(|csq| {
            let mut new_csq = csq.clone();
            let sub = csq
                .subquery
                .alias
                .as_deref()
                .and_then(|a| plans.sub_plans.iter().find(|(k, _)| k.as_ref() == a));
            if let Some((_, sub_plan)) = sub {
                new_csq.subquery = Box::new(apply_plans_to_ast(&csq.subquery, sub_plan));
            }
            new_csq
        })
        .collect();

    new_ast
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost::{
        ConnectionCostModel, CostModelCost, FanoutConfidence, FanoutEst, SimpleCostModel,
    };
    use rindle::{CorrelatedSubquery, Correlation, OrderPart, SimpleCondition, ValuePosition};
    use rindle::{Lit, Op};

    fn model_simple() -> Rc<dyn ConnectionCostModel> {
        Rc::new(SimpleCostModel)
    }

    /// users large; any child table tiny with high startup ⇒ flipping wins.
    struct FlipModel;
    impl ConnectionCostModel for FlipModel {
        fn estimate(
            &self,
            table: &str,
            _sort: &[OrderPart],
            _filters: Option<&Condition>,
            _constraint: Option<&PlannerConstraint>,
        ) -> CostModelCost {
            let (startup, rows) = if table == "users" {
                (0.0, 1000.0)
            } else {
                (50.0, 1.0)
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

    fn child_ast(table: &str, where_: Option<Condition>) -> Ast {
        Ast {
            table: Box::from(table),
            alias: Some(Box::from(table)),
            r#where: where_,
            ..Default::default()
        }
    }

    /// An EXISTS / NOT EXISTS over `child_table`, correlation id ↔ userId.
    fn exists_cond(
        child_table: &str,
        op: ExistsOp,
        flip: Option<bool>,
        child_where: Option<Condition>,
    ) -> Condition {
        Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec![Box::from("id")],
                    child_field: vec![Box::from("userId")],
                },
                subquery: Box::new(child_ast(child_table, child_where)),
                system: None,
            },
            op,
            flip,
            scalar: None,
            plan_id: None,
        })
    }

    fn simple_cond() -> Condition {
        Condition::Simple(SimpleCondition {
            op: Op::Eq,
            left: ValuePosition::Column {
                name: Box::from("active"),
            },
            right: ValuePosition::Literal {
                value: Lit::Bool(true),
            },
        })
    }

    fn root(where_: Option<Condition>) -> Ast {
        Ast {
            table: Box::from("users"),
            r#where: where_,
            ..Default::default()
        }
    }

    fn tables(g: &PlannerGraph) -> Vec<String> {
        g.connection_ids()
            .iter()
            .map(|&id| g.connection(id).table.to_string())
            .collect()
    }

    fn flip_of(condition: &Condition) -> Option<bool> {
        match condition {
            Condition::CorrelatedSubquery(c) => c.flip,
            _ => panic!("not a correlated subquery"),
        }
    }

    // ---- buildPlanGraph structure ----

    #[test]
    fn simple_table_query() {
        let plans = build_plan_graph(&root(None), model_simple(), true, None);
        assert_eq!(tables(&plans.plan), vec!["users"]);
        assert!(plans.sub_plans.is_empty());
        assert!(plans.plan.has_source("users"));
    }

    #[test]
    fn exists_creates_one_flippable_join() {
        let ast = root(Some(exists_cond("posts", ExistsOp::Exists, None, None)));
        let plans = build_plan_graph(&ast, model_simple(), true, None);
        assert_eq!(tables(&plans.plan), vec!["users", "posts"]);
        assert_eq!(plans.plan.join_ids().len(), 1);
        let join = plans.plan.join(plans.plan.join_ids()[0]);
        assert!(join.is_flippable());
        assert_eq!(join.type_(), JoinType::Semi);
    }

    #[test]
    fn not_exists_join_is_unflippable() {
        let ast = root(Some(exists_cond("posts", ExistsOp::NotExists, None, None)));
        let plans = build_plan_graph(&ast, model_simple(), true, None);
        let join = plans.plan.join(plans.plan.join_ids()[0]);
        assert!(!join.is_flippable());
        assert_eq!(join.type_(), JoinType::Semi);
    }

    #[test]
    fn and_assigns_sequential_plan_ids() {
        let ast = root(Some(Condition::And {
            conditions: vec![
                exists_cond("posts", ExistsOp::Exists, None, None),
                exists_cond("comments", ExistsOp::Exists, None, None),
            ],
        }));
        let plans = build_plan_graph(&ast, model_simple(), true, None);
        assert_eq!(plans.plan.join_ids().len(), 2);
        assert_eq!(plans.plan.join(plans.plan.join_ids()[0]).plan_id, 0);
        assert_eq!(plans.plan.join(plans.plan.join_ids()[1]).plan_id, 1);
        assert_eq!(tables(&plans.plan), vec!["users", "posts", "comments"]);
    }

    #[test]
    fn or_creates_fan_out_in() {
        let ast = root(Some(Condition::Or {
            conditions: vec![
                exists_cond("posts", ExistsOp::Exists, None, None),
                exists_cond("comments", ExistsOp::Exists, None, None),
            ],
        }));
        let plans = build_plan_graph(&ast, model_simple(), true, None);
        assert_eq!(plans.plan.fan_out_ids().len(), 1);
        assert_eq!(plans.plan.fan_in_ids().len(), 1);
        assert_eq!(plans.plan.join_ids().len(), 2);
    }

    #[test]
    fn or_with_only_simple_has_no_fan() {
        let ast = root(Some(Condition::Or {
            conditions: vec![simple_cond(), simple_cond()],
        }));
        let plans = build_plan_graph(&ast, model_simple(), true, None);
        assert_eq!(plans.plan.fan_out_ids().len(), 0);
        assert_eq!(plans.plan.fan_in_ids().len(), 0);
        assert_eq!(plans.plan.join_ids().len(), 0);
    }

    #[test]
    fn exists_child_has_limit_one_root_has_query_limit() {
        let ast = Ast {
            table: Box::from("users"),
            limit: Some(10),
            r#where: Some(exists_cond("posts", ExistsOp::Exists, None, None)),
            ..Default::default()
        };
        let plans = build_plan_graph(&ast, model_simple(), true, None);
        let ids = plans.plan.connection_ids();
        let users = plans.plan.connection(ids[0]);
        let posts = plans.plan.connection(ids[1]);
        assert_eq!(&*users.table, "users");
        assert_eq!(users.limit, Some(10.0)); // root gets ast.limit
        assert_eq!(&*posts.table, "posts");
        assert_eq!(posts.limit, Some(1.0)); // EXISTS child limit = 1
    }

    #[test]
    fn not_exists_child_has_no_limit() {
        let ast = root(Some(exists_cond("posts", ExistsOp::NotExists, None, None)));
        let plans = build_plan_graph(&ast, model_simple(), true, None);
        let posts = plans.plan.connection(plans.plan.connection_ids()[1]);
        assert_eq!(posts.limit, None);
    }

    #[test]
    fn related_creates_subplan() {
        let ast = Ast {
            table: Box::from("users"),
            related: vec![CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec![Box::from("id")],
                    child_field: vec![Box::from("userId")],
                },
                subquery: Box::new(child_ast("posts", None)),
                system: None,
            }],
            ..Default::default()
        };
        let plans = build_plan_graph(&ast, model_simple(), true, None);
        assert_eq!(tables(&plans.plan), vec!["users"]); // related is NOT in the main graph
        assert_eq!(plans.sub_plans.len(), 1);
        assert_eq!(&*plans.sub_plans[0].0, "posts");
        assert_eq!(tables(&plans.sub_plans[0].1.plan), vec!["posts"]);
    }

    #[test]
    fn manual_flip_settings() {
        for (flip, want_flippable, want_type) in [
            (None, true, JoinType::Semi),
            (Some(true), false, JoinType::Flipped),
            (Some(false), false, JoinType::Semi),
        ] {
            let ast = root(Some(exists_cond("posts", ExistsOp::Exists, flip, None)));
            let plans = build_plan_graph(&ast, model_simple(), true, None);
            let join = plans.plan.join(plans.plan.join_ids()[0]);
            assert_eq!(join.is_flippable(), want_flippable, "flip={flip:?}");
            assert_eq!(join.type_(), want_type, "flip={flip:?}");
        }
    }

    #[test]
    fn terminus_propagation_does_not_panic() {
        let ast = root(Some(exists_cond("posts", ExistsOp::Exists, None, None)));
        let mut plans = build_plan_graph(&ast, model_simple(), true, None);
        plans.plan.propagate_constraints_from_terminus();
    }

    // ---- plan_ast end-to-end ----

    #[test]
    fn plan_query_flips_when_child_is_cheap() {
        let ast = root(Some(exists_cond("posts", ExistsOp::Exists, None, None)));
        let out = plan_ast(&ast, Rc::new(FlipModel));
        assert_eq!(flip_of(out.r#where.as_ref().unwrap()), Some(true));
    }

    #[test]
    fn plan_query_keeps_semi_under_simple_model() {
        // EXISTS child limit=1 makes the semi-probe cheap, so flipping loses.
        let ast = root(Some(exists_cond("posts", ExistsOp::Exists, None, None)));
        let out = plan_ast(&ast, model_simple());
        assert_eq!(flip_of(out.r#where.as_ref().unwrap()), Some(false));
    }

    #[test]
    fn plan_query_preserves_manual_flips() {
        for flip in [Some(true), Some(false)] {
            let ast = root(Some(exists_cond("posts", ExistsOp::Exists, flip, None)));
            let out = plan_ast(&ast, model_simple());
            assert_eq!(flip_of(out.r#where.as_ref().unwrap()), flip);
        }
    }

    #[test]
    fn plan_query_does_not_mutate_input() {
        let ast = root(Some(exists_cond("posts", ExistsOp::Exists, None, None)));
        let _ = plan_ast(&ast, Rc::new(FlipModel));
        // Input's flip is still None (output is a fresh AST).
        assert_eq!(flip_of(ast.r#where.as_ref().unwrap()), None);
    }

    #[test]
    fn plan_query_applies_flips_to_nested_exists() {
        // users WHERE EXISTS(posts WHERE EXISTS(comments)): both nested conditions get
        // an explicit flip (proves plan_id re-derivation descends correctly).
        let inner = exists_cond("comments", ExistsOp::Exists, None, None);
        let ast = root(Some(exists_cond(
            "posts",
            ExistsOp::Exists,
            None,
            Some(inner),
        )));
        let out = plan_ast(&ast, Rc::new(FlipModel));

        let outer = out.r#where.as_ref().unwrap();
        assert!(flip_of(outer).is_some());
        // Descend into posts' where to the inner EXISTS(comments).
        let Condition::CorrelatedSubquery(outer_csq) = outer else {
            panic!()
        };
        let inner_cond = outer_csq.related.subquery.r#where.as_ref().unwrap();
        assert!(flip_of(inner_cond).is_some());
    }

    #[test]
    fn plan_query_applies_subplan_flips_to_related() {
        // A related subquery whose own where has an EXISTS gets planned + applied.
        let related_sub = Ast {
            table: Box::from("posts"),
            alias: Some(Box::from("posts")),
            r#where: Some(exists_cond("comments", ExistsOp::Exists, None, None)),
            ..Default::default()
        };
        let ast = Ast {
            table: Box::from("users"),
            related: vec![CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec![Box::from("id")],
                    child_field: vec![Box::from("userId")],
                },
                subquery: Box::new(related_sub),
                system: None,
            }],
            ..Default::default()
        };
        let out = plan_ast(&ast, Rc::new(FlipModel));
        // The related subquery's nested EXISTS got an explicit flip.
        let sub_where = out.related[0].subquery.r#where.as_ref().unwrap();
        assert!(flip_of(sub_where).is_some());
    }

    // ---- has_flippable_exists (the engine's skip-planning guard) ----

    #[test]
    fn flippable_guard_true_only_when_planner_can_decide() {
        // No where ⇒ nothing to plan.
        assert!(!has_flippable_exists(&root(None)));
        // A plain filter ⇒ nothing to plan.
        assert!(!has_flippable_exists(&root(Some(simple_cond()))));
        // EXISTS with undecided flip ⇒ the planner decides ⇒ true.
        assert!(has_flippable_exists(&root(Some(exists_cond(
            "posts",
            ExistsOp::Exists,
            None,
            None,
        )))));
        // NOT EXISTS never flips ⇒ false.
        assert!(!has_flippable_exists(&root(Some(exists_cond(
            "posts",
            ExistsOp::NotExists,
            None,
            None,
        )))));
        // A user-forced flip (Some(_)) is honored by the builder without the planner ⇒
        // the planner is a no-op ⇒ false.
        assert!(!has_flippable_exists(&root(Some(exists_cond(
            "posts",
            ExistsOp::Exists,
            Some(true),
            None,
        )))));
        assert!(!has_flippable_exists(&root(Some(exists_cond(
            "posts",
            ExistsOp::Exists,
            Some(false),
            None,
        )))));
    }

    #[test]
    fn flippable_guard_finds_nested_and_related_exists() {
        // Flippable EXISTS nested inside an EXISTS child's where.
        let nested = root(Some(exists_cond(
            "posts",
            ExistsOp::Exists,
            Some(false), // outer is user-pinned (not flippable itself)
            Some(exists_cond("comments", ExistsOp::Exists, None, None)),
        )));
        assert!(has_flippable_exists(&nested));

        // Flippable EXISTS hiding in a `related` sub-plan (empty top-level where).
        let mut sub = child_ast(
            "posts",
            Some(exists_cond("comments", ExistsOp::Exists, None, None)),
        );
        sub.alias = Some(Box::from("posts"));
        let with_related = Ast {
            table: Box::from("users"),
            related: vec![CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec![Box::from("id")],
                    child_field: vec![Box::from("userId")],
                },
                subquery: Box::new(sub),
                system: None,
            }],
            ..Default::default()
        };
        assert!(has_flippable_exists(&with_related));
    }
}
