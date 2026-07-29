//! Observability shim (productionization WS03.1).
//!
//! This is the **only** module that references the `tracing` crate. Every other
//! seam calls the `obs_span!`, `obs_event!`, `obs_counter_inc!`, and
//! `obs_gauge_set!` macros defined here, which have two `#[cfg]` bodies:
//!
//! - **`feature = "observe"` ON** — they forward to `tracing::span!`/`event!`.
//! - **`observe` OFF (the default, incl. the wasm client)** — they expand to a
//!   block that only *consumes* the field expressions (`let _ = &expr;`) so the
//!   arguments are still type-checked but **no code and no `tracing` dependency**
//!   reach the artifact. `cargo tree` shows no `tracing` in the default build.
//!
//! # House style (so the off-arm can consume args without choking on sigils)
//!
//! Unlike raw `tracing`, this shim's field grammar is **sigil-free**: write
//! `key = expr`, never `key = %expr` / `key = ?expr`. The value `expr` must be a
//! `tracing`-recordable `tracing::Value` — an integer, `bool`, `f64`, or `&str`.
//! For a `Box<str>`/`String` pass `&*s` / `s.as_str()`; for a `Display`/`Debug`
//! type format it at the call site only behind a level the subscriber enabled.
//! The level is a **bare ident** (`TRACE`/`DEBUG`/`INFO`/`WARN`/`ERROR`), never
//! `tracing::Level::…`, so the off-build never names the absent crate.
//!
//! Keep field expressions **side-effect-free**: the off-arm references them but
//! the on-arm may skip them when the level is statically disabled.
//!
//! # Taxonomy (the metric/span contract — WS03.2–03.5 fill these in)
//!
//! | Kind  | Name                      | Level | Fields |
//! |-------|---------------------------|-------|--------|
//! | span  | `build_pipeline`          | DEBUG | `table` |
//! | span  | `hydrate`                 | DEBUG | `view`, `rows` |
//! | span  | `source_push`             | TRACE | `src` |
//! | span  | `leaf_fetch`              | TRACE | `table`, `ordered` |
//! | span  | `view_flush`              | DEBUG | `view_entries` |
//! | count | `rindle.build.ok`            | DEBUG | — |
//! | count | `rindle.build.errors`        | WARN  | `kind` |
//! | count | `rindle.changes.processed`   | TRACE | `kind` |
//! | count | `rindle.leaf.fetch_errors`   | ERROR | — |
//! | count | `rindle.view.changes`        | DEBUG | `kind` |
//! | count | `rindle.view.flushes`        | DEBUG | — |
//! | gauge | `rindle.cursors.open`        | DEBUG | — |
//! | gauge | `rindle.stmt_cache.capacity` | DEBUG | — |
//!
//! Naming rule: dotted `rindle.<area>.<metric>`, `snake_case` fields, labels are
//! `&'static str` only. Counters/gauges are emitted as `tracing` events carrying a
//! `metric = "<name>"` field plus a `counter`/`gauge` value field — an
//! exporter-agnostic convention (wiring a Prometheus/OTLP exporter is the
//! embedding server's choice; see WS03 non-goals).

/// RAII guard returned by `obs_span!` when `observe` is **off** — a zero-sized
/// stand-in for `tracing`'s `EnteredSpan` so `let _span = obs_span!(…);` binds and
/// drops uniformly in both builds.
#[cfg(not(feature = "observe"))]
#[derive(Debug)]
pub struct DisabledSpan;

#[cfg(not(feature = "observe"))]
impl DisabledSpan {
    #[inline(always)]
    pub fn new() -> Self {
        DisabledSpan
    }
}

#[cfg(not(feature = "observe"))]
impl Default for DisabledSpan {
    #[inline(always)]
    fn default() -> Self {
        DisabledSpan
    }
}

// ---------------------------------------------------------------------------
// observe ON: forward to `tracing`.
// ---------------------------------------------------------------------------

/// Open a span over a seam: `let _s = obs_span!(DEBUG, "name", key = expr, …);`.
/// Hold the returned guard for the scope you want spanned.
#[cfg(feature = "observe")]
macro_rules! obs_span {
    ($level:ident, $name:expr $(, $key:ident = $val:expr)* $(,)?) => {
        ::tracing::span!(::tracing::Level::$level, $name $(, $key = $val)*).entered()
    };
}

/// Emit a structured event: `obs_event!(WARN, reason = msg, kind = k);`.
#[cfg(feature = "observe")]
macro_rules! obs_event {
    ($level:ident $(, $key:ident = $val:expr)* $(,)?) => {
        ::tracing::event!(::tracing::Level::$level $(, $key = $val)*)
    };
}

/// Increment a named counter: `obs_counter_inc!("rindle.build.errors", kind = k);`.
#[cfg(feature = "observe")]
macro_rules! obs_counter_inc {
    ($name:expr $(, $key:ident = $val:expr)* $(,)?) => {
        ::tracing::event!(::tracing::Level::DEBUG, metric = $name, counter = 1u64 $(, $key = $val)*)
    };
}

/// Set a named gauge: `obs_gauge_set!("rindle.cursors.open", n);`.
#[cfg(feature = "observe")]
macro_rules! obs_gauge_set {
    ($name:expr, $value:expr $(, $key:ident = $val:expr)* $(,)?) => {
        ::tracing::event!(::tracing::Level::DEBUG, metric = $name, gauge = $value $(, $key = $val)*)
    };
}

// ---------------------------------------------------------------------------
// observe OFF: consume the field expressions, emit nothing, link no `tracing`.
// ---------------------------------------------------------------------------

#[cfg(not(feature = "observe"))]
macro_rules! obs_span {
    ($level:ident, $name:expr $(, $key:ident = $val:expr)* $(,)?) => {{
        let _ = &$name;
        $( let _ = &$val; )*
        $crate::observe::DisabledSpan::new()
    }};
}

#[cfg(not(feature = "observe"))]
macro_rules! obs_event {
    ($level:ident $(, $key:ident = $val:expr)* $(,)?) => {{
        $( let _ = &$val; )*
    }};
}

#[cfg(not(feature = "observe"))]
macro_rules! obs_counter_inc {
    ($name:expr $(, $key:ident = $val:expr)* $(,)?) => {{
        let _ = &$name;
        $( let _ = &$val; )*
    }};
}

#[cfg(not(feature = "observe"))]
macro_rules! obs_gauge_set {
    ($name:expr, $value:expr $(, $key:ident = $val:expr)* $(,)?) => {{
        let _ = &$name;
        let _ = &$value;
        $( let _ = &$val; )*
    }};
}

// Consumed by the instrumented seams in WS03.2–03.5; until they land the
// re-export itself is unreferenced (the macros are exercised by `_shim_compiles`).
#[allow(unused_imports)]
pub(crate) use {obs_counter_inc, obs_event, obs_gauge_set, obs_span};

/// Compile-time proof that **both** `#[cfg]` arms of every macro type-check and
/// consume their arguments. Never called — its sole purpose is to be compiled
/// under each feature config (CI runs both `--features observe` and the default).
#[allow(dead_code)]
fn _shim_compiles() {
    let table: Box<str> = "issue".into();
    let view_id = 7usize;
    let rows = 3usize;
    let ordered = true;
    let kind = "add";
    let n = 42i64;

    let _s = obs_span!(DEBUG, "build_pipeline", table = &*table);
    let _s2 = obs_span!(TRACE, "hydrate", view = view_id, rows = rows);
    obs_event!(WARN, reason = "rejected", kind = kind);
    obs_event!(DEBUG, ordered = ordered);
    obs_counter_inc!("rindle.build.ok");
    obs_counter_inc!("rindle.build.errors", kind = kind);
    obs_gauge_set!("rindle.cursors.open", n);
}
