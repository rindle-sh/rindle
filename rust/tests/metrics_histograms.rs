//! The build-gated latency histograms (the `metrics` feature, 208.2): the `Histogram`
//! primitive's shape (cumulative monotonicity, `_count == Σ` per-bucket, `+Inf` catch),
//! and that the `source_push` / `hydrate` seams actually observe into the process-global
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
        snap.sum_micros,
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
//    before/after delta is exact (per-binary statics; nothing else races them). The timer
//    wraps `Graph::try_source_push` / `try_hydrate`, which are backend-agnostic (memory and
//    SQLite sources flow through the same seam), so counting the memory path proves both. --

#[test]
fn source_push_and_hydrate_latency_observed() {
    let push_before = rindle::metrics::engine_hist().source_push.snapshot();
    let hydrate_before = rindle::metrics::engine_hist().hydrate.snapshot();

    let mut g = Graph::new();
    let src = g.add_source(id_schema(), Vec::new());
    const N: u64 = 5;
    for i in 0..N as i64 {
        g.source_push(src, SourceChange::Add(row(i)));
    }
    // A view over a source connection, hydrated once.
    let conn = g.connect(src, None, None, Vec::new());
    let view = g.add_view(conn, Schema::new(vec!["id"], vec![0], vec![(0, true)]));
    g.hydrate(view);

    let push_after = rindle::metrics::engine_hist().source_push.snapshot();
    let hydrate_after = rindle::metrics::engine_hist().hydrate.snapshot();

    assert_eq!(
        push_after.count - push_before.count,
        N,
        "one source_push_seconds observation per push"
    );
    assert_eq!(
        hydrate_after.count - hydrate_before.count,
        1,
        "one hydrate_seconds observation per hydrate"
    );
    // Bucket totals stay consistent with the count on the live registry too.
    assert_eq!(
        push_after.buckets.iter().sum::<u64>(),
        push_after.count,
        "live registry: per-bucket Σ == _count"
    );
}
