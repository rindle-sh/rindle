//! Thin shim — the parity harness + chinook/mini fixtures graduated to the reusable
//! [`rindle_testfix`] library crate (so the `rindle-fuzz` generator can drive the same
//! SQLite oracle over the same data). Re-exported here under the original
//! `harness::` path so this crate's existing integration tests are unchanged
//! (`mod harness; use harness::{assert_*, chinook, mini};`).

// A catch-all re-export shim: any given test binary uses only a subset of these
// names, so an unconditional re-export of the rest is "unused" in that binary.
#![allow(unused_imports)]

pub use rindle_testfix::parity::*;
pub use rindle_testfix::{chinook, mini};
