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

    /// A Prometheus-shaped histogram over [`BUCKET_BOUNDS_MICROS`], in microseconds.
    /// All lock-free: `observe` is one linear scan (≤ `N_BUCKETS` compares) + three
    /// relaxed adds. Per-bucket counts are **not** cumulative — the renderer sums them
    /// cumulatively at scrape time.
    pub struct Histogram {
        buckets: [AtomicU64; N_BUCKETS],
        /// Σ observed µs → rendered as `_sum` seconds (`/ 1e6`). Integer, so no float atomic.
        sum_micros: AtomicU64,
        /// Total observations → `_count`.
        count: AtomicU64,
    }

    impl Histogram {
        pub const fn new() -> Self {
            Histogram {
                buckets: [const { AtomicU64::new(0) }; N_BUCKETS],
                sum_micros: AtomicU64::new(0),
                count: AtomicU64::new(0),
            }
        }

        /// Record one observation of `micros` µs. Linear scan for the first bucket whose
        /// upper bound is `>= micros` (Prometheus `le` is inclusive); over-range falls to
        /// the `+Inf` slot.
        #[inline]
        pub fn observe(&self, micros: u64) {
            let idx = BUCKET_BOUNDS_MICROS
                .iter()
                .position(|&b| micros <= b)
                .unwrap_or(N_BUCKETS - 1);
            self.buckets[idx].fetch_add(1, Relaxed);
            self.sum_micros.fetch_add(micros, Relaxed);
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
                sum_micros: self.sum_micros.load(Relaxed),
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
    /// the µs sum, and the total count.
    #[derive(Clone, Copy, Debug)]
    pub struct HistogramSnapshot {
        pub buckets: [u64; N_BUCKETS],
        pub sum_micros: u64,
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
        /// `Graph::try_source_push` — the write-path headline latency.
        pub source_push: Histogram,
        /// `builder::build_pipeline` — query compile latency.
        pub query_build: Histogram,
        /// `Graph::try_hydrate` — subscription cold-start latency.
        pub hydrate: Histogram,
    }

    impl EngineHistograms {
        const fn new() -> Self {
            EngineHistograms {
                source_push: Histogram::new(),
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
}

#[cfg(feature = "metrics")]
pub use imp::{
    engine, engine_hist, snapshot, EngineHistograms, EngineMetrics, EngineSnapshot, Histogram,
    HistogramSnapshot, Timed, BUCKET_LE_SECONDS, N_BUCKETS,
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
}

#[cfg(not(feature = "metrics"))]
pub use imp_off::Timed;

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
/// field. Bind it (`let _t = metric_timer!(source_push);`) so it observes elapsed µs into
/// the histogram when it drops — on every return path, `?` included. One timer per
/// call, never per row.
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
