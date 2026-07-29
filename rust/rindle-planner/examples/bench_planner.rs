//! `bench_planner` — measure `plan_ast` cost as the flippable-join count grows.
//!
//! The planner enumerates `2^n` flip patterns (`n` flippable EXISTS joins, capped at
//! `MAX_FLIPPABLE_JOINS = 9`). This harness builds synthetic ASTs with a tunable `n`
//! in three graph shapes and reports, per `(shape, n)`:
//!
//! - **`estimate` calls** — how many times the planner invoked the cost model. This is
//!   the portable, machine-independent signal. With the *real* `SqliteCostModel` each
//!   call is a `sqlite3_prepare_v2` + scanstatus read, so this count is a direct proxy
//!   for registration latency.
//! - **wall-time** of `plan_ast` (pure enumeration arithmetic, since the cost model
//!   here is trivial).
//!
//! It runs every shape twice: once against a bare counting cost model, and once with
//! that model wrapped in the shipping [`rindle_planner::CachingCostModel`]. The A/B
//! columns show the call-count collapse the cache buys — see
//! `PLANNER-INTEGRATION-DESIGN.md` §4. For the same A/B against the *real*
//! `SqliteCostModel` (measured prepares, not a simulated spin), see
//! `rindle-sqlite/examples/bench_planner_sqlite.rs`.
//!
//! Run: `cargo run -p rindle-planner --example bench_planner --release`

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    OrderPart,
};
use rindle_planner::{
    plan_ast, CachingCostModel, ConnectionCostModel, CostModelCost, FanoutConfidence, FanoutEst,
    PlannerConstraint,
};

// ---------------------------------------------------------------------------
// Cost models
// ---------------------------------------------------------------------------

/// A trivial deterministic cost model that *counts* every `estimate` call. The
/// returned cost favors flipping (large parent, tiny child) so the enumeration is
/// non-degenerate, but the numbers are arbitrary — we measure call counts and timing,
/// not flip decisions.
///
/// `sim_prep` busy-spins for a fixed duration per call to emulate a real
/// `SqliteCostModel` prepare + scanstatus read, so the wall-time reflects the regime
/// where `estimate` is *not* free (the production regime). `0` = pure enumeration.
struct CountingModel {
    calls: RefCell<u64>,
    sim_prep: Duration,
}

impl CountingModel {
    fn new() -> Self {
        Self::with_sim(Duration::ZERO)
    }
    fn with_sim(sim_prep: Duration) -> Self {
        Self {
            calls: RefCell::new(0),
            sim_prep,
        }
    }
    fn calls(&self) -> u64 {
        *self.calls.borrow()
    }
}

impl ConnectionCostModel for CountingModel {
    fn estimate(
        &self,
        table: &str,
        _sort: &[OrderPart],
        _filters: Option<&Condition>,
        _constraint: Option<&PlannerConstraint>,
    ) -> CostModelCost {
        *self.calls.borrow_mut() += 1;
        if !self.sim_prep.is_zero() {
            let spin_until = Instant::now() + self.sim_prep;
            while Instant::now() < spin_until {
                std::hint::spin_loop();
            }
        }
        // Root table ("t0") is the large parent; every EXISTS child is tiny + costly to
        // start, so flipping is favored and the search explores real cost differences.
        let (startup, rows) = if table == "t0" {
            (0.0, 10_000.0)
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

// The cache under test is the shipping `rindle_planner::CachingCostModel`.

// ---------------------------------------------------------------------------
// Synthetic AST shapes (tunable flippable-join count `n`)
// ---------------------------------------------------------------------------

fn order_id() -> Vec<OrderPart> {
    vec![OrderPart("id".into(), Dir::Asc)]
}

/// One flippable `EXISTS(child)` condition against table `child_table`, correlated
/// parent.id == child.parentID.
fn exists_cond(child_table: &str, subquery_where: Option<Condition>) -> Condition {
    Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["parentID".into()],
            },
            subquery: Box::new(Ast {
                table: child_table.into(),
                order_by: order_id(),
                r#where: subquery_where,
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

/// **Linear-nested**: `t0 WHERE EXISTS(t1 WHERE EXISTS(t2 WHERE EXISTS(...)))`.
/// `n` nested EXISTS → `n` flippable joins down a single spine.
fn shape_nested(n: usize) -> Ast {
    // Build from the innermost child outward.
    let mut inner_where: Option<Condition> = None;
    for depth in (1..=n).rev() {
        inner_where = Some(exists_cond(&format!("t{depth}"), inner_where));
    }
    Ast {
        table: "t0".into(),
        order_by: order_id(),
        r#where: inner_where,
        ..Default::default()
    }
}

/// Combine a list of conditions under a single n-ary `And`/`Or` node. A lone
/// condition is returned bare (no wrapping), matching how the builder canonicalizes.
fn combine(op_and: bool, conds: Vec<Condition>) -> Option<Condition> {
    match conds.len() {
        0 => None,
        1 => conds.into_iter().next(),
        _ => Some(if op_and {
            Condition::And { conditions: conds }
        } else {
            Condition::Or { conditions: conds }
        }),
    }
}

/// **Flat AND**: `t0 WHERE EXISTS(t1) AND EXISTS(t2) AND ... EXISTS(tn)`.
fn shape_and(n: usize) -> Ast {
    let conds: Vec<Condition> = (1..=n)
        .map(|i| exists_cond(&format!("t{i}"), None))
        .collect();
    Ast {
        table: "t0".into(),
        order_by: order_id(),
        r#where: combine(true, conds),
        ..Default::default()
    }
}

/// **Flat OR**: `t0 WHERE EXISTS(t1) OR EXISTS(t2) OR ... EXISTS(tn)` — exercises the
/// FanOut/FanIn (FOFI) path.
fn shape_or(n: usize) -> Ast {
    let conds: Vec<Condition> = (1..=n)
        .map(|i| exists_cond(&format!("t{i}"), None))
        .collect();
    Ast {
        table: "t0".into(),
        order_by: order_id(),
        r#where: combine(false, conds),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

const ITERS: u32 = 50;

fn bench_one(name: &str, build: &dyn Fn(usize) -> Ast, max_n: usize) {
    println!("\n## shape: {name}");
    println!(
        "{:>3} | {:>12} {:>10} | {:>12} {:>10} | {:>8}",
        "n", "calls", "ms", "calls(cache)", "ms(cache)", "speedup"
    );
    println!("{}", "-".repeat(70));

    for n in 1..=max_n {
        let ast = build(n);

        // --- bare counting model ---
        let bare = Rc::new(CountingModel::new());
        // warm + correctness: ensure it actually plans (no panic / shape rejection).
        let _ = plan_ast(&ast, bare.clone());
        let calls_bare = bare.calls();

        let t = Instant::now();
        for _ in 0..ITERS {
            let m = Rc::new(CountingModel::new());
            let _ = plan_ast(&ast, m);
        }
        let ms_bare = t.elapsed().as_secs_f64() * 1000.0 / ITERS as f64;

        // --- cached model ---
        let cached = Rc::new(CachingCostModel::new(CountingModel::new()));
        let _ = plan_ast(&ast, cached.clone());
        let calls_cached = cached.inner().calls();

        let t = Instant::now();
        for _ in 0..ITERS {
            let m = Rc::new(CachingCostModel::new(CountingModel::new()));
            let _ = plan_ast(&ast, m);
        }
        let ms_cached = t.elapsed().as_secs_f64() * 1000.0 / ITERS as f64;

        let speedup = if ms_cached > 0.0 {
            ms_bare / ms_cached
        } else {
            f64::NAN
        };

        println!(
            "{n:>3} | {calls_bare:>12} {ms_bare:>10.4} | {calls_cached:>12} {ms_cached:>10.4} | {speedup:>7.2}x"
        );
    }
}

fn main() {
    println!("rindle-planner enumeration benchmark");
    println!(
        "  ITERS={ITERS} per cell; MAX_FLIPPABLE_JOINS=9 (n>9 ⇒ planner skips, runs as authored)"
    );
    println!(
        "  'calls' = cost-model estimate() invocations (≈ SQLite prepares under SqliteCostModel)"
    );

    // Push to 10 to show the n>9 cliff (the planner bails and does zero enumeration).
    bench_one("nested EXISTS", &shape_nested, 10);
    bench_one("flat AND-of-EXISTS", &shape_and, 10);
    bench_one("flat OR-of-EXISTS (FOFI)", &shape_or, 10);

    // Real-world regime: each estimate() emulates a SQLite prepare + scanstatus read.
    // This is the regime that actually matters for registration latency — the tables
    // above (free estimate) only measure enumeration arithmetic.
    real_world_regime(Duration::from_micros(20));
}

/// Worst case (`n = 9`) with a non-free `estimate`, the production regime. Shows that
/// once a call costs a real prepare, the bare per-pattern re-estimation dominates and
/// the cache is the difference between a stalled registration and a fast one.
fn real_world_regime(sim_prep: Duration) {
    println!(
        "\n## real-world regime — n=9, each estimate() spins ~{}us (≈ a SQLite prepare)",
        sim_prep.as_micros()
    );
    println!(
        "{:>26} | {:>10} {:>10} | {:>10} {:>10} | {:>8}",
        "shape", "calls", "ms", "calls(cache)", "ms(cache)", "speedup"
    );
    println!("{}", "-".repeat(86));

    type ShapeFn = fn(usize) -> Ast;
    let shapes: [(&str, ShapeFn); 3] = [
        ("nested EXISTS", shape_nested),
        ("flat AND-of-EXISTS", shape_and),
        ("flat OR-of-EXISTS (FOFI)", shape_or),
    ];
    // Few iterations: each plan now costs real wall-time.
    const N: usize = 9;
    const REPS: u32 = 5;

    for (name, build) in shapes {
        let ast = build(N);

        let t = Instant::now();
        let mut calls_bare = 0;
        for _ in 0..REPS {
            let m = Rc::new(CountingModel::with_sim(sim_prep));
            let _ = plan_ast(&ast, m.clone());
            calls_bare = m.calls();
        }
        let ms_bare = t.elapsed().as_secs_f64() * 1000.0 / REPS as f64;

        let t = Instant::now();
        let mut calls_cached = 0;
        for _ in 0..REPS {
            let m = Rc::new(CachingCostModel::new(CountingModel::with_sim(sim_prep)));
            let _ = plan_ast(&ast, m.clone());
            calls_cached = m.inner().calls();
        }
        let ms_cached = t.elapsed().as_secs_f64() * 1000.0 / REPS as f64;

        let speedup = ms_bare / ms_cached;
        println!(
            "{name:>26} | {calls_bare:>10} {ms_bare:>10.2} | {calls_cached:>10} {ms_cached:>10.2} | {speedup:>7.1}x"
        );
    }
}
