//! Build-gated process metrics (the `metrics` feature) — the *scrape-path* sibling
//! of the [`observe`](crate::observe) shim.
//!
//! `observe` routes seams to `tracing` spans/events for **sampled diagnostics**.
//! Counters do not belong there: a Prometheus counter is just a monotonic number
//! read at scrape time, so routing each increment through an event (callsite check,
//! field capture, subscriber dispatch) is all cost and no benefit — and sampling a
//! counter only trades that cost for an undercount you then have to scale back up.
//! These macros instead fold each seam into a single relaxed atomic add on a
//! process-global registry the metrics endpoint reads directly.
//!
//! Like `observe`, this is **off by default**: with the feature off every macro
//! expands to an argument-consuming no-op and **no global is linked** (the wasm
//! client and any embedder that doesn't want the counters pay nothing — verify with
//! `cargo tree` / a symbol check). The daemon (`rindle-server`) opts in through its
//! own `metrics` feature, which enables `rindle/metrics`.
//!
//! # Taxonomy (WS03 metric contract; counters unless noted)
//!
//! | Name                              | Labels | Seam |
//! |-----------------------------------|--------|------|
//! | `rindle.changes.processed`        | `kind` = add/remove/edit | `Graph::try_source_push` |
//! | `rindle.build.ok`                 | —      | `builder::build_pipeline` (Ok) |
//! | `rindle.build.errors`             | `kind` | `builder::build_pipeline` (Err) |
//! | `rindle.push.visited`             | —      | `source_common::gen_push` (index candidates) |
//! | `rindle.push.skipped`             | —      | `source_common::gen_push` (index-pruned slots) |
//!
//! The `push.skipped / (push.visited + push.skipped)` ratio is the guarded-fan-out
//! win (`designs/205-GUARDED-PUSH-FANOUT-DESIGN.md` §Observability); a `visited` that
//! tracks total connections flags guard extraction silently failing for a workload.
//!
//! Labels are `&'static str` only, so the series count is bounded (no per-row/-query
//! cardinality) — the discipline that keeps a cardinality-billed backend cheap.

#[cfg(feature = "metrics")]
mod imp {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    /// Process-global engine counters. All monotonic; read via [`snapshot`].
    pub struct EngineMetrics {
        pub changes_add: AtomicU64,
        pub changes_remove: AtomicU64,
        pub changes_edit: AtomicU64,
        pub build_ok: AtomicU64,
        pub build_err_unknown_column: AtomicU64,
        pub build_err_unsupported: AtomicU64,
        pub build_err_invalid: AtomicU64,
        pub build_err_unknown_table: AtomicU64,
        pub build_err_unknown_relationship: AtomicU64,
        /// Slots the push index selected to visit (candidates), summed per push.
        pub push_visited: AtomicU64,
        /// Slots the push index pruned (total slots − candidates), summed per push.
        pub push_skipped: AtomicU64,
        /// Per-mutation panics contained by [`Graph::source_push_isolated`](crate::graph)'s
        /// `catch_unwind` (the `release-server` `panic=unwind` net). A rising value is the
        /// single best "a bad mutation is loose" signal (208.5).
        pub mutation_panics: AtomicU64,
    }

    impl EngineMetrics {
        const fn new() -> Self {
            EngineMetrics {
                changes_add: AtomicU64::new(0),
                changes_remove: AtomicU64::new(0),
                changes_edit: AtomicU64::new(0),
                build_ok: AtomicU64::new(0),
                build_err_unknown_column: AtomicU64::new(0),
                build_err_unsupported: AtomicU64::new(0),
                build_err_invalid: AtomicU64::new(0),
                build_err_unknown_table: AtomicU64::new(0),
                build_err_unknown_relationship: AtomicU64::new(0),
                push_visited: AtomicU64::new(0),
                push_skipped: AtomicU64::new(0),
                mutation_panics: AtomicU64::new(0),
            }
        }
    }

    static ENGINE: EngineMetrics = EngineMetrics::new();

    /// The process-global registry the instrumentation macros bump.
    #[inline]
    pub fn engine() -> &'static EngineMetrics {
        &ENGINE
    }

    /// A plain-data copy of the registry for the metrics endpoint — keeps atomics
    /// out of the public surface and reads every counter once (relaxed).
    #[derive(Clone, Copy, Debug, Default)]
    pub struct EngineSnapshot {
        pub changes_add: u64,
        pub changes_remove: u64,
        pub changes_edit: u64,
        pub build_ok: u64,
        pub build_err_unknown_column: u64,
        pub build_err_unsupported: u64,
        pub build_err_invalid: u64,
        pub build_err_unknown_table: u64,
        pub build_err_unknown_relationship: u64,
        pub push_visited: u64,
        pub push_skipped: u64,
        pub mutation_panics: u64,
    }

    /// Snapshot every engine counter for rendering.
    pub fn snapshot() -> EngineSnapshot {
        EngineSnapshot {
            changes_add: ENGINE.changes_add.load(Relaxed),
            changes_remove: ENGINE.changes_remove.load(Relaxed),
            changes_edit: ENGINE.changes_edit.load(Relaxed),
            build_ok: ENGINE.build_ok.load(Relaxed),
            build_err_unknown_column: ENGINE.build_err_unknown_column.load(Relaxed),
            build_err_unsupported: ENGINE.build_err_unsupported.load(Relaxed),
            build_err_invalid: ENGINE.build_err_invalid.load(Relaxed),
            build_err_unknown_table: ENGINE.build_err_unknown_table.load(Relaxed),
            build_err_unknown_relationship: ENGINE.build_err_unknown_relationship.load(Relaxed),
            push_visited: ENGINE.push_visited.load(Relaxed),
            push_skipped: ENGINE.push_skipped.load(Relaxed),
            mutation_panics: ENGINE.mutation_panics.load(Relaxed),
        }
    }

    // -----------------------------------------------------------------------
    // Histograms (208.2): a std-only fixed-bucket latency histogram, hand-rolled
    // to keep the root crate dep-free (no `prometheus`/`metrics` crate — wasm-clean,
    // no C toolchain). Durations are captured as `u64` MICROSECONDS so the running
    // sum stays integer (no float atomics); `_sum` is rendered `micros / 1e6` at
    // scrape time only.
    // -----------------------------------------------------------------------
    use std::time::Instant;

    /// One shared bucket table for every latency histogram, so the renderer stays
    /// uniform. **These boundaries are a metric contract — choose once.** Upper bounds
    /// for the first `N_BUCKETS - 1` buckets, in µs; the final bucket is `+Inf` (catches
    /// any over-range sample).
    pub const BUCKET_BOUNDS_MICROS: [u64; N_BUCKETS - 1] = [
        50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
        1_000_000, 2_500_000, 5_000_000,
    ];

    /// Total bucket count (16 finite bounds + `+Inf`).
    pub const N_BUCKETS: usize = 17;

    /// The `le` label for each bucket, in **seconds** (histograms are exposed in seconds
    /// per Prometheus convention, though stored in µs). Kept next to `BUCKET_BOUNDS_MICROS`
    /// so the two can't drift; the server renderer reads it directly.
    pub const BUCKET_LE_SECONDS: [&str; N_BUCKETS] = [
        "0.00005", "0.0001", "0.00025", "0.0005", "0.001", "0.0025", "0.005", "0.01", "0.025",
        "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "+Inf",
    ];

    /// Bounds for histograms that count **rows**, not time — today just the apply-batch
    /// size. A separate table because the µs bounds above start at 50µs, which would put
    /// every realistic batch in one bucket.
    ///
    /// Deliberately dense at the bottom: the interesting question for an app is "are my
    /// normal writes 1 row or 20?", and the answer lives between 1 and 64.
    ///
    /// The top bound is **1024 on purpose** — it is `rindle-replica`'s `PUSH_CHUNK_ROWS`,
    /// the size at which the coordinator cuts a transaction into apply batches, so 1024 is
    /// the largest value this histogram can observe on the production path. Bounds beyond
    /// it would be dead buckets that read like headroom. `+Inf` is therefore not slack but
    /// a **signal**: anything landing there means an unchunked caller appeared and the cap
    /// assumption behind this table no longer holds. See [`ApplyBatch`].
    pub const ROW_BUCKET_BOUNDS: [u64; N_BUCKETS - 1] = [
        1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64, 128, 256, 512, 1024,
    ];

    /// The `le` label for each [`ROW_BUCKET_BOUNDS`] entry. Kept beside it so the two
    /// cannot drift, exactly like [`BUCKET_LE_SECONDS`].
    pub const ROW_BUCKET_LE: [&str; N_BUCKETS] = [
        "1", "2", "3", "4", "6", "8", "12", "16", "24", "32", "48", "64", "128", "256", "512",
        "1024", "+Inf",
    ];

    /// A Prometheus-shaped histogram over [`BUCKET_BOUNDS_MICROS`], in microseconds.
    /// All lock-free: `observe` is one linear scan (≤ `N_BUCKETS` compares) + three
    /// relaxed adds. Per-bucket counts are **not** cumulative — the renderer sums them
    /// cumulatively at scrape time.
    pub struct Histogram {
        buckets: [AtomicU64; N_BUCKETS],
        /// Σ observed values → `_sum`, in whatever unit was observed: µs for a latency
        /// histogram (the renderer divides by 1e6), raw rows for a count histogram.
        /// Integer either way, so no float atomic.
        sum: AtomicU64,
        /// Total observations → `_count`.
        count: AtomicU64,
    }

    impl Histogram {
        pub const fn new() -> Self {
            Histogram {
                buckets: [const { AtomicU64::new(0) }; N_BUCKETS],
                sum: AtomicU64::new(0),
                count: AtomicU64::new(0),
            }
        }

        /// Record one observation of `micros` µs. Linear scan for the first bucket whose
        /// upper bound is `>= micros` (Prometheus `le` is inclusive); over-range falls to
        /// the `+Inf` slot.
        #[inline]
        pub fn observe(&self, micros: u64) {
            self.observe_in(micros, &BUCKET_BOUNDS_MICROS);
        }

        /// Record one observation against an explicit bound table — the same linear scan,
        /// for histograms whose unit is not µs (see [`ROW_BUCKET_BOUNDS`]). The caller
        /// must pass the SAME table the renderer labels with, or `le` would lie.
        #[inline]
        pub fn observe_in(&self, value: u64, bounds: &[u64; N_BUCKETS - 1]) {
            let idx = bounds
                .iter()
                .position(|&b| value <= b)
                .unwrap_or(N_BUCKETS - 1);
            self.buckets[idx].fetch_add(1, Relaxed);
            self.sum.fetch_add(value, Relaxed);
            self.count.fetch_add(1, Relaxed);
        }

        /// A plain-data copy for rendering — reads every atomic once (relaxed).
        pub fn snapshot(&self) -> HistogramSnapshot {
            let mut buckets = [0u64; N_BUCKETS];
            for (dst, src) in buckets.iter_mut().zip(self.buckets.iter()) {
                *dst = src.load(Relaxed);
            }
            HistogramSnapshot {
                buckets,
                sum: self.sum.load(Relaxed),
                count: self.count.load(Relaxed),
            }
        }
    }

    impl Default for Histogram {
        fn default() -> Self {
            Self::new()
        }
    }

    /// A rendering-ready copy of a [`Histogram`]: per-bucket (non-cumulative) counts,
    /// the summed observations (µs for latency, rows for a count histogram), and the
    /// total count.
    #[derive(Clone, Copy, Debug)]
    pub struct HistogramSnapshot {
        pub buckets: [u64; N_BUCKETS],
        pub sum: u64,
        pub count: u64,
    }

    /// RAII latency timer (208): mirrors the codebase's `CursorGuard`/`PooledStmt` habit
    /// so early returns and `?` still record — it observes elapsed µs into its histogram
    /// on drop, on every return path. `Instant::now()` (~tens of ns) is compiled only
    /// under `metrics`.
    pub struct Timed {
        hist: &'static Histogram,
        start: Instant,
    }

    impl Timed {
        #[inline]
        pub fn new(hist: &'static Histogram) -> Self {
            Timed {
                hist,
                start: Instant::now(),
            }
        }
    }

    impl Drop for Timed {
        #[inline]
        fn drop(&mut self) {
            self.hist.observe(self.start.elapsed().as_micros() as u64);
        }
    }

    /// Process-global Tier-1 latency histograms (208.2), read via [`engine_hist`].
    pub struct EngineHistograms {
        /// One caller-declared BATCH of source pushes — on the replica path, one worker's
        /// apply of one bounded chunk (fan-out + sink drain). See [`ApplyBatch`] for why
        /// this is neither per-push nor per-transaction.
        pub apply_batch: Histogram,
        /// Rows in that batch, over [`ROW_BUCKET_BOUNDS`]. The companion that keeps the
        /// latency reading interpretable: it separates "slow because it was 1000 rows"
        /// from "slow because something is wrong".
        pub apply_batch_rows: Histogram,
        /// `builder::build_pipeline` — query compile latency.
        pub query_build: Histogram,
        /// `Graph::try_hydrate` — subscription cold-start latency.
        pub hydrate: Histogram,
    }

    impl EngineHistograms {
        const fn new() -> Self {
            EngineHistograms {
                apply_batch: Histogram::new(),
                apply_batch_rows: Histogram::new(),
                query_build: Histogram::new(),
                hydrate: Histogram::new(),
            }
        }
    }

    static ENGINE_HIST: EngineHistograms = EngineHistograms::new();

    /// The process-global histogram registry the [`metric_timer!`] macro observes into.
    #[inline]
    pub fn engine_hist() -> &'static EngineHistograms {
        &ENGINE_HIST
    }

    /// RAII scope for ONE batch of source pushes: times the whole batch and records how
    /// many rows it carried, both on drop.
    ///
    /// **Why this is not a per-push timer.** It used to be — `try_source_push` opened a
    /// [`Timed`] — and that was wrong twice over. The apply path pushes a batch's rows ONE
    /// AT A TIME (`rindle-replica`'s `push_and_drain` loops over the captured slice), so a
    /// 100k-row write paid 100k `Instant::now()` pairs: measured at ~15ns per push, ~13%
    /// of a minimal in-memory push. And it bought nothing, because [`BUCKET_BOUNDS_MICROS`]
    /// starts at 50µs while a push takes ~150ns — every sample landed in bucket 0, so the
    /// 17 buckets described no distribution at all.
    ///
    /// **Why it is not per-TRANSACTION either.** Naming matters here, because the obvious
    /// reading of "batch" is "transaction" and it is wrong on the replica path in two
    /// independent ways:
    ///
    /// 1. **Chunking.** `rindle-replica`'s coordinator cuts a transaction into `TxPush`
    ///    chunks of at most `PUSH_CHUNK_ROWS` (1024) rows, so a transaction bigger than
    ///    that is several batches. A 50k-row backfill is 49 observations, never one.
    /// 2. **Broadcast.** Each chunk goes to EVERY worker (one shared `Arc`), and each
    ///    worker opens its own scope over it. With `n_workers = W` a single chunk yields W
    ///    observations of the same row count — so `_count` and `_sum` are ×W. Quantiles
    ///    are unaffected (identical duplicates do not move a distribution), which is why
    ///    the dashboard reads quantiles and not rates.
    ///
    /// So one observation is **one worker's apply of one bounded chunk** — real, useful
    /// latency, but not a transaction. Do not label it as one. (An earlier revision of this
    /// metric did, under the name `push_batch`; see the write-latency revision note in
    /// `designs-implemented/208-METRICS-EXPANSION-DESIGN.md`.)
    ///
    /// Per-ROW cost stays recoverable, because `rindle_changes_processed_total` still
    /// counts every change (one relaxed add, no clock) and scales with W the same way:
    ///
    /// ```text
    /// rate(rindle_apply_batch_seconds_sum) / rate(rindle_changes_processed_total)
    ///     = mean seconds per row
    /// ```
    ///
    /// A caller that declares no batch is simply not timed — its rows are still counted.
    /// That is deliberate: only a caller that knows its own batch boundary can draw one,
    /// and inventing a batch-of-1 would reintroduce the per-push clock.
    pub struct ApplyBatch {
        rows: u64,
        start: Instant,
    }

    impl Drop for ApplyBatch {
        #[inline]
        fn drop(&mut self) {
            let h = engine_hist();
            h.apply_batch
                .observe(self.start.elapsed().as_micros() as u64);
            h.apply_batch_rows.observe_in(self.rows, &ROW_BUCKET_BOUNDS);
        }
    }

    /// Open an [`ApplyBatch`] scope for a batch of `rows` changes. Bind it (`let _batch =
    /// …`) so it closes on every return path, `?` included.
    #[inline]
    pub fn apply_batch(rows: u64) -> ApplyBatch {
        ApplyBatch {
            rows,
            start: Instant::now(),
        }
    }
}

// `ROW_BUCKET_BOUNDS` ships alongside `ROW_BUCKET_LE` deliberately: `Histogram::observe_in`
// is public and its contract is "pass the SAME table the renderer labels with", which an
// out-of-crate caller cannot honour if only the label half is reachable.
#[cfg(feature = "metrics")]
pub use imp::{
    apply_batch, engine, engine_hist, snapshot, ApplyBatch, EngineHistograms, EngineMetrics,
    EngineSnapshot, Histogram, HistogramSnapshot, Timed, BUCKET_LE_SECONDS, N_BUCKETS,
    ROW_BUCKET_BOUNDS, ROW_BUCKET_LE,
};

// A no-op RAII timer TYPE for the feature-off build, so a `metric_timer!` site binds a
// (zero-sized) value rather than unit — `let _t = ()` would trip `clippy::let_unit_value`
// under `-D warnings`. No `Instant`, no Drop work: pays nothing.
#[cfg(not(feature = "metrics"))]
mod imp_off {
    pub struct Timed;
    impl Timed {
        #[inline]
        pub fn noop() -> Self {
            Timed
        }
    }

    /// Feature-off twin of the batch scope: a ZST with no `Drop`, no `Instant`, no
    /// global. `apply_batch` is a plain `fn` (not a macro) so a caller in ANOTHER crate —
    /// `rindle-replica`'s apply loop is the one that matters — writes one unconditional
    /// line with no `#[cfg]` of its own and pays nothing here.
    pub struct ApplyBatch;

    #[inline]
    pub fn apply_batch(_rows: u64) -> ApplyBatch {
        ApplyBatch
    }
}

#[cfg(not(feature = "metrics"))]
pub use imp_off::{apply_batch, ApplyBatch, Timed};

// ---------------------------------------------------------------------------
// metrics ON: fold each seam into a relaxed atomic add on the global registry.
// ---------------------------------------------------------------------------

/// Bump a named [`EngineMetrics`] counter by one.
#[cfg(feature = "metrics")]
macro_rules! metric_inc {
    ($field:ident) => {
        $crate::metrics::engine()
            .$field
            .fetch_add(1, ::std::sync::atomic::Ordering::Relaxed)
    };
}

/// Add `n` to a named [`EngineMetrics`] counter (for per-batch seams like the push
/// fan-out's visited/skipped slot counts). `n` is evaluated once.
#[cfg(feature = "metrics")]
macro_rules! metric_add {
    ($field:ident, $n:expr) => {
        $crate::metrics::engine()
            .$field
            .fetch_add($n, ::std::sync::atomic::Ordering::Relaxed)
    };
}

/// Classify a `SourceChange` into a small kind token carried across the move into the
/// push (then consumed by [`metric_changes_inc!`]). Unconditional — always a `u8`, so the
/// caller's binding is never unit (which would trip `clippy::let_unit_value` in the OFF
/// build); when the feature is off the token feeds a no-op and the match is DCE'd. Pass
/// `&change` so the original value is still movable into the push afterwards.
macro_rules! metric_change_kind {
    ($change:expr) => {
        match $change {
            $crate::change::SourceChange::Add(_) => 0u8,
            $crate::change::SourceChange::Remove(_) => 1u8,
            $crate::change::SourceChange::Edit { .. } => 2u8,
        }
    };
}

/// Bump `rindle.changes.processed` for the kind token from [`metric_change_kind!`].
#[cfg(feature = "metrics")]
macro_rules! metric_changes_inc {
    ($kind:expr) => {{
        use ::std::sync::atomic::Ordering::Relaxed;
        let m = $crate::metrics::engine();
        match $kind {
            0u8 => &m.changes_add,
            1u8 => &m.changes_remove,
            _ => &m.changes_edit,
        }
        .fetch_add(1, Relaxed);
    }};
}

/// Bump the per-`kind` `rindle.build.errors` counter for a `&BuildError`.
#[cfg(feature = "metrics")]
macro_rules! metric_build_err {
    ($err:expr) => {{
        use ::std::sync::atomic::Ordering::Relaxed;
        let m = $crate::metrics::engine();
        match $err {
            $crate::builder::BuildError::UnknownColumn(_) => &m.build_err_unknown_column,
            // The 226 §8 int64 gate is an unsupported-shape rejection; no separate
            // counter until the gate is load-bearing enough to want one.
            $crate::builder::BuildError::Unsupported(_)
            | $crate::builder::BuildError::Int64ColumnUnsupported { .. } => {
                &m.build_err_unsupported
            }
            $crate::builder::BuildError::Invalid(_) => &m.build_err_invalid,
            $crate::builder::BuildError::UnknownTable(_) => &m.build_err_unknown_table,
            $crate::builder::BuildError::UnknownRelationship(_) => {
                &m.build_err_unknown_relationship
            }
        }
        .fetch_add(1, Relaxed);
    }};
}

/// Start an RAII latency timer for a named [`EngineHistograms`](imp::EngineHistograms)
/// field. Bind it (`let _t = metric_timer!(hydrate);`) so it observes elapsed µs into
/// the histogram when it drops — on every return path, `?` included. One timer per
/// call, never per row — and "never per row" has to hold for the CALL GRAPH, not just
/// this function body: check whether the caller loops before deciding a seam is coarse
/// (that is exactly how the retired `source_push` timer became a per-row clock). A seam
/// whose caller owns the batch wants [`ApplyBatch`](imp::ApplyBatch), not this.
#[cfg(feature = "metrics")]
macro_rules! metric_timer {
    ($field:ident) => {
        $crate::metrics::Timed::new(&$crate::metrics::engine_hist().$field)
    };
}

// ---------------------------------------------------------------------------
// metrics OFF: consume the args, emit nothing, link no global.
// ---------------------------------------------------------------------------

#[cfg(not(feature = "metrics"))]
macro_rules! metric_inc {
    ($field:ident) => {{}};
}

#[cfg(not(feature = "metrics"))]
macro_rules! metric_add {
    ($field:ident, $n:expr) => {{
        let _ = $n;
    }};
}

#[cfg(not(feature = "metrics"))]
macro_rules! metric_changes_inc {
    ($kind:expr) => {{
        let _ = $kind;
    }};
}

#[cfg(not(feature = "metrics"))]
macro_rules! metric_build_err {
    ($err:expr) => {{
        let _ = $err;
    }};
}

// A no-op timer guard (ZST) so the binding is never unit (`clippy::let_unit_value`).
#[cfg(not(feature = "metrics"))]
macro_rules! metric_timer {
    ($field:ident) => {
        $crate::metrics::Timed::noop()
    };
}

#[allow(unused_imports)]
pub(crate) use {
    metric_add, metric_build_err, metric_change_kind, metric_changes_inc, metric_inc, metric_timer,
};
