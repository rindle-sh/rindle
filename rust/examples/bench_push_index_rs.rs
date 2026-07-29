//! `bench_push_index` — the guarded push fan-out win, measured on the component in
//! isolation (`designs/205-GUARDED-PUSH-FANOUT-DESIGN.md`, §Complexity / §Testing.7).
//!
//! Every source write today fans out to **all N** connections on the table and runs
//! each one's compiled `where` predicate against the changed row — O(N) predicate
//! evaluations, almost all concluding "drop" for the `where col = ?` shapes that
//! dominate `rindled`. This harness reproduces that inner loop two ways, at growing
//! N, so the scaling is visible without the engine wiring (that lands in the 205
//! push-path integration):
//!
//! - **`scan_all`** — the baseline: evaluate every connection's predicate, exactly
//!   what `filter_push` does per connection today.
//! - **`indexed`** — [`PushIndex::push_candidates`] first, then evaluate the exact
//!   predicate on the (superset) candidate slots only.
//!
//! Both arms run the *same* predicates on the candidates they visit, so `indexed`
//! never changes the answer — it only skips connections the index proves cannot
//! match. Two workloads bracket the behavior:
//!
//! - **`hits_one`** — N connections each `col = k` for distinct k; the write hits
//!   exactly one. The headline: baseline O(N), indexed one `BTreeMap` descent + one
//!   eval, flat in N.
//! - **`hits_none`** — N connections `col = k`; the write targets a value no
//!   connection guards (the dominant case: a write to project X while most live
//!   queries watch other projects). Baseline still pays N all-dropping evals;
//!   indexed returns zero candidates.
//!
//! Emits `workload/N|ns-per-push` lines (lower is better), matching the other
//! `bench_*` harnesses so `./orient.sh benchmarks --record` can track them.
//!
//! Run: `cargo run --release --example bench_push_index_rs`

use std::hint::black_box;
use std::time::Instant;

use rindle::change::SourceChange;
use rindle::push_index::{PushGuard, PushIndex};
use rindle::value::{owned_row, OwnedValue};
use rindle::{CmpOp, CompiledPredicate};

/// Sizes that make the O(N) baseline vs flat index visually obvious.
const SIZES: &[u32] = &[100, 1_000, 10_000];

/// Warmup, then the min ns/op over rounds — the identical harness shape the other
/// `bench_*` examples use.
fn bench(name: &str, mut body: impl FnMut()) {
    let warm = Instant::now();
    while warm.elapsed().as_nanos() < 200_000_000 {
        body();
    }
    let mut best = f64::INFINITY;
    for _ in 0..6 {
        let mut iters: u64 = 1;
        loop {
            let start = Instant::now();
            for _ in 0..iters {
                body();
            }
            let dt = start.elapsed().as_nanos() as f64;
            if dt >= 100_000_000.0 {
                best = best.min(dt / iters as f64);
                break;
            }
            iters *= 2;
        }
    }
    println!("{name}|{best:.1}");
}

/// One connection's compiled `col0 = value` predicate — the exact form
/// `create_predicate` lowers `where col = ?` into.
fn eq_pred(value: i64) -> CompiledPredicate {
    CompiledPredicate::Cmp {
        col: 0,
        op: CmpOp::Eq,
        value: OwnedValue::Int(value),
    }
}

/// Build N connections each guarding `col0 = k` (k = 0..N), registered into a
/// [`PushIndex`], with the matching compiled predicates in slot order.
fn build(n: u32) -> (PushIndex, Vec<CompiledPredicate>) {
    let mut idx = PushIndex::new();
    let mut preds = Vec::with_capacity(n as usize);
    for slot in 0..n {
        let value = slot as i64;
        let guard = PushGuard {
            col: 0,
            values: vec![OwnedValue::Int(value)],
        };
        idx.insert(slot, Some(&guard), &[]);
        preds.push(eq_pred(value));
    }
    (idx, preds)
}

/// The baseline inner loop: evaluate every connection's predicate against the row.
#[inline]
fn scan_all(preds: &[CompiledPredicate], row: &rindle::value::OwnedRow) -> usize {
    let mut hits = 0usize;
    for p in preds {
        if p.eval(row) {
            hits += 1;
        }
    }
    hits
}

/// The indexed inner loop: candidate slots first, then the exact predicate on each.
#[inline]
fn indexed(
    idx: &PushIndex,
    preds: &[CompiledPredicate],
    change: &SourceChange,
    row: &rindle::value::OwnedRow,
) -> usize {
    let mut hits = 0usize;
    for &slot in &idx.push_candidates(change) {
        if preds[slot as usize].eval(row) {
            hits += 1;
        }
    }
    hits
}

fn main() {
    println!("rindle guarded-push-fan-out benchmark (component, memory backend)");
    println!("# workload/N | ns per simulated push (lower is better)");

    for &n in SIZES {
        let (idx, preds) = build(n);

        // hits_one: write a value exactly one connection guards.
        let target = (n / 2) as i64;
        let row = owned_row(vec![OwnedValue::Int(target)]);
        let change = SourceChange::Add(row.clone());
        bench(&format!("scan_all/hits_one/{n}"), || {
            black_box(scan_all(&preds, &row));
        });
        bench(&format!("indexed/hits_one/{n}"), || {
            black_box(indexed(&idx, &preds, &change, &row));
        });

        // hits_none: write a value no connection guards (the dominant rindled case).
        let miss = n as i64 + 1;
        let row_miss = owned_row(vec![OwnedValue::Int(miss)]);
        let change_miss = SourceChange::Add(row_miss.clone());
        bench(&format!("scan_all/hits_none/{n}"), || {
            black_box(scan_all(&preds, &row_miss));
        });
        bench(&format!("indexed/hits_none/{n}"), || {
            black_box(indexed(&idx, &preds, &change_miss, &row_miss));
        });
    }
}
