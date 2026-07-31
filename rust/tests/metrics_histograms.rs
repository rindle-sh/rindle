//! The build-gated latency histograms (the `metrics` feature, 208.2): the `Histogram`
//! primitive's shape (cumulative monotonicity, `_count == Σ` per-bucket, `+Inf` catch),
//! and that the `apply_batch` / `hydrate` seams actually observe into the process-global
//! registry. Only compiled/run with `--features metrics`; a no-op otherwise.

#![cfg(feature = "metrics")]

use rindle::metrics::{Histogram, BUCKET_LE_SECONDS, N_BUCKETS};
use rindle::{owned_row, Graph, OwnedValue, Schema, SourceChange, SourceSchema};

fn id_schema() -> SourceSchema {
    SourceSchema::new(vec!["id"], vec![0], vec![(0, true)])
}
fn row(n: i64) -> rindle::OwnedRow {
    owned_row(vec![OwnedValue::Int(n)])
}

// -- primitive: LOCAL histograms only (never the global registry), so these are exact and
//    safe to run in parallel with the seam test below. --------------------------------

#[test]
fn histogram_shape_and_sum() {
    let h = Histogram::new();
    // One sample per bucket, the last (10s) deliberately over the 5s top boundary → +Inf.
    let samples: [u64; N_BUCKETS] = [
        10, 75, 200, 400, 900, 2_000, 4_000, 9_000, 20_000, 40_000, 90_000, 200_000, 400_000,
        900_000, 2_000_000, 4_500_000, 10_000_000,
    ];
    for &s in &samples {
        h.observe(s);
    }
    let snap = h.snapshot();

    assert_eq!(
        snap.count,
        samples.len() as u64,
        "one _count per observation"
    );
    assert_eq!(
        snap.sum,
        samples.iter().sum::<u64>(),
        "_sum is Σ observed µs"
    );
    assert_eq!(
        snap.buckets.iter().sum::<u64>(),
        snap.count,
        "per-bucket counts sum to _count"
    );

    // Cumulative rendering is monotone non-decreasing and ends at _count.
    let mut cum = 0u64;
    for b in snap.buckets {
        cum += b;
        assert!(cum <= snap.count, "cumulative never exceeds _count");
    }
    assert_eq!(cum, snap.count, "+Inf cumulative == _count");

    // The 10s sample is over-range → it lands in the final (+Inf) slot.
    assert_eq!(
        *snap.buckets.last().unwrap(),
        1,
        "+Inf catches the over-range sample"
    );

    assert_eq!(BUCKET_LE_SECONDS.len(), N_BUCKETS);
    assert_eq!(BUCKET_LE_SECONDS[N_BUCKETS - 1], "+Inf");
}

#[test]
fn observe_le_boundary_is_inclusive() {
    // Prometheus `le` is inclusive: a sample equal to a boundary falls in THAT bucket.
    let h = Histogram::new();
    h.observe(50); // == the first boundary (50µs)
    let snap = h.snapshot();
    assert_eq!(
        snap.buckets[0], 1,
        "50µs lands in le=0.00005, not the next bucket"
    );
    assert_eq!(snap.buckets[1], 0);
}

// -- seam: the ONLY test in this binary that touches the process-global registry, so its
//    before/after delta is exact (per-binary statics; nothing else races them). The seams are
//    backend-agnostic (memory and SQLite sources flow through the same `try_source_push` /
//    `try_hydrate`), so counting the memory path proves both. --

/// Latency is per BATCH, not per push. This is the load-bearing assertion of the batch-scope
/// design: N pushes inside one scope must produce ONE latency observation carrying N rows.
///
/// The inverse — `push_count == N` — is what this deliberately no longer does. A per-push
/// timer meant a 100k-row write paid 100k `Instant::now()` pairs (~15ns each, ~13% of a
/// minimal push) for samples that all landed in the first bucket anyway, since the µs bounds
/// start at 50µs and a push takes ~150ns.
///
/// What a bare `Graph` CANNOT show is the other half of the contract — that a batch is not a
/// transaction, because `rindle-replica` chunks and broadcasts. That is pinned one layer up,
/// in `rindle-server`'s `net_metrics.rs`, where a real multi-worker cluster is running.
#[test]
fn apply_batch_latency_is_per_batch_and_carries_its_row_count() {
    let batch_before = rindle::metrics::engine_hist().apply_batch.snapshot();
    let rows_before = rindle::metrics::engine_hist().apply_batch_rows.snapshot();
    let hydrate_before = rindle::metrics::engine_hist().hydrate.snapshot();

    let mut g = Graph::new();
    let src = g.add_source(id_schema(), Vec::new());
    const N: u64 = 5;
    {
        let _batch = rindle::metrics::apply_batch(N);
        for i in 0..N as i64 {
            g.source_push(src, SourceChange::Add(row(i)));
        }
    } // scope closes here — one observation, not N
      // A view over a source connection, hydrated once.
    let conn = g.connect(src, None, None, Vec::new());
    let view = g.add_view(conn, Schema::new(vec!["id"], vec![0], vec![(0, true)]));
    g.hydrate(view);

    let batch_after = rindle::metrics::engine_hist().apply_batch.snapshot();
    let rows_after = rindle::metrics::engine_hist().apply_batch_rows.snapshot();
    let hydrate_after = rindle::metrics::engine_hist().hydrate.snapshot();

    assert_eq!(
        batch_after.count - batch_before.count,
        1,
        "ONE apply_batch_seconds observation per batch, regardless of how many rows it pushed"
    );
    assert_eq!(
        rows_after.count - rows_before.count,
        1,
        "the size companion observes exactly once per batch, from the same guard drop"
    );
    assert_eq!(
        rows_after.sum - rows_before.sum,
        N,
        "apply_batch_rows _sum accumulates ROWS (this is the denominator that keeps mean \
         per-row cost recoverable), not microseconds"
    );
    assert_eq!(
        hydrate_after.count - hydrate_before.count,
        1,
        "one hydrate_seconds observation per hydrate"
    );
    // Bucket totals stay consistent with the count on the live registry too.
    assert_eq!(
        batch_after.buckets.iter().sum::<u64>(),
        batch_after.count,
        "live registry: per-bucket Σ == _count"
    );

    // -- and the other half of the contract, asserted HERE rather than in its own #[test]:
    //    the registry is a per-binary static and `cargo test` runs tests in parallel, so a
    //    second test taking before/after deltas would race this one (it did — it saw this
    //    test's 5 pushes in its own delta). One registry-touching test per binary.
    //
    //    A caller that declares no batch is NOT timed, but its rows are still counted — so
    //    an embedder with no batch boundary keeps working, and the counter that makes mean
    //    per-row cost recoverable never silently stops.
    let unscoped_batch_before = rindle::metrics::engine_hist().apply_batch.snapshot();
    let changes_before = rindle::metrics::snapshot().changes_add;
    const M: i64 = 3;
    for i in 100..100 + M {
        g.source_push(src, SourceChange::Add(row(i)));
    }
    assert_eq!(
        rindle::metrics::engine_hist().apply_batch.snapshot().count,
        unscoped_batch_before.count,
        "an unscoped push records NO latency observation (no clock read on that path)"
    );
    assert_eq!(
        rindle::metrics::snapshot().changes_add - changes_before,
        M as u64,
        "...but every change is still counted — that counter is clock-free and per-change"
    );
}
