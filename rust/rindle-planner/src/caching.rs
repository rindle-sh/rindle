//! [`CachingCostModel`] — memoize [`ConnectionCostModel::estimate`] across a single
//! `plan()` call.
//!
//! ## Why
//! The planner enumerates `2^n` flip patterns, and `PlannerGraph::plan` calls
//! `reset_planning_state` + `propagate_constraints_from_terminus` **per pattern** —
//! both of which clear `PlannerConnection`'s per-branch cost cache (`connection.rs`).
//! So the inner cache only ever helps *within* one pattern's walk; `estimate` is
//! re-invoked for every connection on every one of the `2^n` patterns. With a cheap
//! model (`SimpleCostModel`) that is merely wasteful; with [`SqliteCostModel`] each
//! `estimate` is a `sqlite3_prepare_v2` + scanstatus read, so the worst case (`n = 9`)
//! is thousands of prepares for one registration — see
//! `rindle-planner/examples/bench_planner.rs` and `PLANNER-INTEGRATION-DESIGN.md` §4.
//!
//! ## What
//! This wrapper memoizes the *whole-plan* estimate, keyed on the full estimate inputs
//! `(table, sort, filters, constraint)`. Those inputs are deterministic for a given DB
//! snapshot and invariant across flip patterns (only *which* constraints reach a
//! connection changes, and the set of distinct input tuples is small — tens, not
//! thousands), so the memo is exact: it returns the identical [`CostModelCost`] the
//! inner model would have. Wrapping changes the planner's decisions in **no** way (the
//! parity test below pins flip-set equality); it only collapses the redundant calls.
//!
//! ## Soundness / scope
//! The estimate depends only on its four arguments (plus the inner model's view of the
//! DB), so memoizing on those arguments is exact **for a fixed snapshot**. Because
//! stats can drift, a `CachingCostModel` is meant to be **constructed fresh per
//! `plan_ast` call** (per query registration) and dropped after — never shared across
//! registrations. The plan is frozen at registration anyway (see the design doc), so a
//! per-plan cache lifetime is exactly right.
//!
//! ## Keying
//! `Condition` is `PartialEq` but not `Hash`/`Eq` (it carries float literals), so the
//! cache buckets by table name (a cheap `HashMap` probe) and linear-scans the bucket
//! comparing `(sort, filters, constraint)` by `PartialEq`. A table usually has one or
//! two distinct entries, so the scan is short — and, crucially, this needs no new trait
//! bounds on the AST and cannot return a wrong cost on a hash collision (there is no
//! hash of the structural part).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rindle::{Condition, OrderPart};

use crate::constraint::PlannerConstraint;
use crate::cost::{ConnectionCostModel, CostModelCost};

/// A blanket impl so a `CachingCostModel` (or the planner) can wrap a boxed/`Rc`'d
/// model: `CachingCostModel<Rc<dyn ConnectionCostModel>>` and friends just work.
impl<T: ConnectionCostModel + ?Sized> ConnectionCostModel for Rc<T> {
    fn estimate(
        &self,
        table: &str,
        sort: &[OrderPart],
        filters: Option<&Condition>,
        constraint: Option<&PlannerConstraint>,
    ) -> CostModelCost {
        (**self).estimate(table, sort, filters, constraint)
    }
}

/// One memoized estimate: the structural inputs (for equality) + the cached result.
struct CacheEntry {
    sort: Vec<OrderPart>,
    filters: Option<Condition>,
    constraint: Option<PlannerConstraint>,
    cost: CostModelCost,
}

/// Memoizing [`ConnectionCostModel`] wrapper. Construct fresh per plan; see the module
/// docs for lifetime/soundness.
pub struct CachingCostModel<M: ConnectionCostModel> {
    inner: M,
    /// table name → distinct `(sort, filters, constraint)` entries for that table.
    cache: RefCell<HashMap<Box<str>, Vec<CacheEntry>>>,
}

impl<M: ConnectionCostModel> CachingCostModel<M> {
    pub fn new(inner: M) -> Self {
        Self {
            inner,
            cache: RefCell::new(HashMap::new()),
        }
    }

    /// Borrow the wrapped model (e.g. to read a call counter in a benchmark).
    pub fn inner(&self) -> &M {
        &self.inner
    }

    /// Unwrap, dropping the cache.
    pub fn into_inner(self) -> M {
        self.inner
    }

    /// Drop all memoized estimates (e.g. to reuse the wrapper after a stat change).
    pub fn clear(&self) {
        self.cache.borrow_mut().clear();
    }

    fn lookup(
        &self,
        table: &str,
        sort: &[OrderPart],
        filters: Option<&Condition>,
        constraint: Option<&PlannerConstraint>,
    ) -> Option<CostModelCost> {
        let cache = self.cache.borrow();
        let bucket = cache.get(table)?;
        bucket
            .iter()
            .find(|e| {
                e.sort.as_slice() == sort
                    && e.filters.as_ref() == filters
                    && e.constraint.as_ref() == constraint
            })
            .map(|e| e.cost.clone())
    }
}

impl<M: ConnectionCostModel> ConnectionCostModel for CachingCostModel<M> {
    fn estimate(
        &self,
        table: &str,
        sort: &[OrderPart],
        filters: Option<&Condition>,
        constraint: Option<&PlannerConstraint>,
    ) -> CostModelCost {
        if let Some(hit) = self.lookup(table, sort, filters, constraint) {
            return hit;
        }
        // Miss: compute (no cache borrow held across the inner call), then store.
        let cost = self.inner.estimate(table, sort, filters, constraint);
        self.cache
            .borrow_mut()
            .entry(Box::from(table))
            .or_default()
            .push(CacheEntry {
                sort: sort.to_vec(),
                filters: filters.cloned(),
                constraint: constraint.cloned(),
                cost: cost.clone(),
            });
        cost
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::plan_ast;
    use crate::cost::{FanoutConfidence, FanoutEst, SimpleCostModel};
    use rindle::{
        Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir,
        ExistsOp, OrderPart,
    };

    /// Counts `estimate` calls; cost favors flipping so enumeration is non-degenerate.
    struct Counting(RefCell<u64>);
    impl Counting {
        fn new() -> Self {
            Counting(RefCell::new(0))
        }
        fn calls(&self) -> u64 {
            *self.0.borrow()
        }
    }
    impl ConnectionCostModel for Counting {
        fn estimate(
            &self,
            table: &str,
            _sort: &[OrderPart],
            _filters: Option<&Condition>,
            constraint: Option<&PlannerConstraint>,
        ) -> CostModelCost {
            *self.0.borrow_mut() += 1;
            let (startup, rows) = if table == "t0" {
                (0.0, 1000.0)
            } else {
                (50.0, 1.0)
            };
            // Distinguish by constraint count so different constraints get different costs
            // (proves the cache keys on the constraint, not just the table).
            let rows = rows - constraint.map_or(0, |c| c.len()) as f64;
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

    fn order_id() -> Vec<OrderPart> {
        vec![OrderPart("id".into(), Dir::Asc)]
    }

    fn exists_cond(child: &str, inner: Option<Condition>) -> Condition {
        Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["parentID".into()],
                },
                subquery: Box::new(Ast {
                    table: child.into(),
                    order_by: order_id(),
                    r#where: inner,
                    ..Default::default()
                }),
                system: None,
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        })
    }

    /// `t0 WHERE EXISTS(t1 WHERE EXISTS(t2 ...))` — `n` nested flippable joins.
    fn nested(n: usize) -> Ast {
        let mut inner = None;
        for d in (1..=n).rev() {
            inner = Some(exists_cond(&format!("t{d}"), inner));
        }
        Ast {
            table: "t0".into(),
            order_by: order_id(),
            r#where: inner,
            ..Default::default()
        }
    }

    fn flip_set(ast: &Ast) -> Vec<Option<bool>> {
        // Collect the `flip` flag of every correlated subquery, depth-first.
        fn walk(c: &Condition, out: &mut Vec<Option<bool>>) {
            match c {
                Condition::CorrelatedSubquery(csq) => {
                    if let Some(w) = &csq.related.subquery.r#where {
                        walk(w, out);
                    }
                    out.push(csq.flip);
                }
                Condition::And { conditions } | Condition::Or { conditions } => {
                    conditions.iter().for_each(|c| walk(c, out))
                }
                Condition::Simple(_) => {}
            }
        }
        let mut out = Vec::new();
        if let Some(w) = &ast.r#where {
            walk(w, &mut out);
        }
        out
    }

    #[test]
    fn caching_collapses_calls_but_preserves_the_plan() {
        for n in 1..=6 {
            let ast = nested(n);

            // Bare: count calls + record the flip decisions.
            let bare = Rc::new(Counting::new());
            let bare_plan = plan_ast(&ast, bare.clone());
            let bare_calls = bare.calls();

            // Cached: wrap the *same kind* of model.
            let cached = Rc::new(CachingCostModel::new(Counting::new()));
            let cached_plan = plan_ast(&ast, cached.clone());
            let inner_calls = cached.inner().calls();

            // 1. The cache must not change the planner's output.
            assert_eq!(
                flip_set(&bare_plan),
                flip_set(&cached_plan),
                "n={n}: caching changed the flip decisions"
            );
            // 2. The cache must never increase inner-model calls, and must strictly
            //    reduce them once enumeration repeats input tuples (n >= 2). At n == 1
            //    (2 patterns) the semi/flipped constraint tuples are all distinct, so
            //    there is genuinely nothing to reuse.
            if n >= 2 {
                assert!(
                    inner_calls < bare_calls,
                    "n={n}: expected fewer inner calls ({inner_calls}) than bare ({bare_calls})"
                );
            } else {
                assert!(inner_calls <= bare_calls, "n={n}: caching increased calls");
            }
        }
    }

    #[test]
    fn cached_estimate_matches_inner() {
        let m = CachingCostModel::new(SimpleCostModel);
        let c1: PlannerConstraint = ["userId"].iter().map(|s| Box::from(*s)).collect();
        let first = m.estimate("t", &order_id(), None, Some(&c1));
        let second = m.estimate("t", &order_id(), None, Some(&c1)); // hit
        assert_eq!(first.rows, second.rows);
        assert_eq!(first.startup_cost, second.startup_cost);
        // A different constraint is a distinct key (not served the cached value).
        let c2: PlannerConstraint = ["a", "b"].iter().map(|s| Box::from(*s)).collect();
        let third = m.estimate("t", &order_id(), None, Some(&c2));
        assert_eq!(third.rows, 80.0); // SimpleCostModel: 100 - 2*10
        assert_eq!(first.rows, 90.0); // 100 - 1*10
    }
}
