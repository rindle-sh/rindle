//! Panic taxonomy + the `invariant!` macro (productionization WS02.1).
//!
//! # The one rule
//!
//! Every panic-family site in the engine (`panic!`, `unreachable!`, `assert!`,
//! `.unwrap()`, `.expect()`, `debug_assert!`) belongs to **exactly one** of two
//! classes, following foundations §10:
//!
//! - **Keep-as-panic — build-time wiring & internal-misuse invariants.** These can
//!   only be violated by an *engine bug* (a mis-wired graph, an internal accessor
//!   called on the wrong node, a serialized blob produced by our own codec failing
//!   to round-trip). They are never reachable from *data* — no source row, no
//!   pushed change, no user filter, and no transient backend error can trip them.
//!   A panic here is correct: it is the fail-fast signal of a programming error,
//!   and converting it to a `Result` would only add error arms the JS engine
//!   doesn't have (foundations §10 warns against exactly this over-conversion).
//!   **Use `invariant!`** for these so the keep-bucket is one grep.
//!
//! - **Convert-to-error — data/contract reachable failures.** These *can* be
//!   reached by an input the engine does not control: a malformed change stream
//!   (ADD of an existing row, REMOVE of an absent row, an EDIT mutating a join
//!   key), a wrong-width / wrong-type ingest row, a cross-type predicate compare,
//!   or a transient SQLite I/O error. In a server these must surface as
//!   [`crate::error::RindleError`] — never abort the process, never silently corrupt
//!   view state in a release build. These are converted by WS02.2–02.4; this
//!   module only establishes the *macro and the rule* they sort against.
//!
//! # How to classify a site
//!
//! Ask: *"Can any sequence of `source_push` / `hydrate` / user-supplied AST /
//! backend hiccup reach this, with the graph correctly built?"*
//!   - **No** → build-time/internal invariant → `invariant!` (or a documented
//!     `panic!`/`unreachable!` annotated `// PANIC-CLASS: build-time|internal`).
//!   - **Yes** → data/contract error → return `RindleError` (WS02.2–02.4).
//!
//! # The five data-reachable classes (the convert-bucket — for reference)
//!
//! 1. change consistency (ADD-exists / REMOVE-absent / EDIT-old-absent) — WS02.2
//! 2. join child-edit that mutates the join key — WS02.2
//! 3. SQLite operator-storage I/O (`OpStorage` `.expect`s) — WS02.3
//! 4. cross-type predicate ordering compare — WS02.4
//! 5. wrong-width / wrong-type ingest row — WS02.4
//!
//! Anything *not* in that list and reachable only via a builder/internal bug stays
//! a panic.
//!
//! # `// PANIC-CLASS:` annotations
//!
//! Sites that stay panics carry a one-line `// PANIC-CLASS: build-time` or
//! `// PANIC-CLASS: internal` comment so an auditor can confirm the keep-bucket by
//! grep without re-reasoning each site:
//!
//! ```text
//! rg -n "PANIC-CLASS|invariant!" src/      # the audited keep-bucket
//! rg -n "rindle internal invariant violated"  # every invariant! message at runtime
//! ```

/// Stable prefix prepended to every `invariant!` panic message. Grepping this
/// string (in source or in a captured panic payload) enumerates every internal
/// invariant in the engine.
pub const INVARIANT_PREFIX: &str = "rindle internal invariant violated";

/// Assert a **build-time / internal-misuse invariant** — a condition that can only
/// be false because of an engine bug, never because of data or a transient
/// backend error. Prefer this over a bare `panic!`/`assert!` for the keep-bucket
/// (see the [module docs](self) for the taxonomy).
///
/// Unlike [`debug_assert!`], an `invariant!` is **compiled in every profile** — it
/// is *not* stripped in release. It reports the **call-site** location (the
/// expanded `panic!` captures `Location::caller()` at the invocation site) and
/// prefixes its message with [`INVARIANT_PREFIX`] for greppability.
///
/// Do **not** use this for anything a `source_push` / `hydrate` / user AST / DB
/// error can reach — those must return [`crate::error::RindleError`] (WS02.2–02.4).
///
/// # Examples
///
/// ```ignore
/// invariant!(slot < self.relationships.len());
/// invariant!(node.is_join(), "expected a Join at {id:?}, found {node:?}");
/// ```
macro_rules! invariant {
    ($cond:expr $(,)?) => {{
        if !$cond {
            ::core::panic!(::core::concat!(
                "rindle internal invariant violated: ",
                ::core::stringify!($cond)
            ));
        }
    }};
    ($cond:expr, $($arg:tt)+) => {{
        if !$cond {
            ::core::panic!(
                "rindle internal invariant violated: {}",
                ::core::format_args!($($arg)+)
            );
        }
    }};
}

// Re-exported for crate-wide use (`use crate::invariant::invariant;`). The
// data-reachable conversions that consume it land in WS02.2–02.4; until then the
// re-export itself is unreferenced (the macro is exercised by `_invariant_compiles`).
#[allow(unused_imports)]
pub(crate) use invariant;

/// Compile-time proof that **both** arms of `invariant!` type-check (the
/// condition-only arm and the formatted-message arm). Never called — it exists so
/// a plain `cargo check`/`build` compiles the macro even before any seam uses it.
#[allow(dead_code)]
fn _invariant_compiles(ok: bool, n: usize, limit: usize) {
    invariant!(ok);
    invariant!(n <= limit, "index {n} out of bounds (limit {limit})");
}
