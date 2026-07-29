//! Shared **test fixtures** + the IVM↔SQLite **differential parity harness**.
//!
//! This is the `rindle-d2s/tests/harness/` code, promoted verbatim into a library so
//! a second crate — the `rindle-fuzz` coverage-driven generator — can drive the same
//! SQLite oracle over the same data without reaching into another crate's `tests/`
//! tree. `rindle-d2s`'s own tests now re-export this under their original `harness::`
//! path (a thin shim), so the move is behaviour-preserving.
//!
//! - [`parity`] — the asserts: [`parity::assert_hydrate_parity`],
//!   [`parity::assert_push_parity`], [`parity::assert_push_walk_parity`], and the
//!   flip-invariance metamorphic checks. Each materializes an [`rindle::Ast`] through
//!   the real `View` *and* through `rindle-d2s` + SQLite, then asserts the two
//!   nested-JSON trees are equal.
//! - [`chinook`] — the realistic-scale fixture (the canonical chinook subset loaded
//!   from `$CHINOOK_SQL`) plus the [`rindle_d2s::Catalog`] both sides share.
//! - [`mini`] — a small, hand-shaped (~40-row) fixture on the same schema, tuned so
//!   EXISTS gates / limit boundaries / null groups *partition* the rows (the home
//!   the generator's directed matrices run over).

pub mod chinook;
pub mod mini;
pub mod parity;
