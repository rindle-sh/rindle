//! # rindle-sqlite — the SQLite backend for the [`rindle`] IVM engine
//!
//! Split out of the core `rindle` crate so the engine stays std-only / wasm-clean (no
//! `rusqlite`, no C toolchain). This crate owns the native-server leaf and its
//! supporting machinery:
//!
//! - [`table_source::TableSource`] — a [`rindle::Source`] backed by a SQLite table
//!   (`05`): lazy zero-copy cursor reads + write-through pushes, plugged into a graph
//!   via [`GraphTableSourceExt::add_table_source`].
//! - [`query_builder`] — the `FetchRequest` → parameterized `SELECT` lowering (`05`).
//! - [`sqlite`] — the zero-copy `RowStream` cursor + value marshalling.
//! - [`stmt_cache`] — a prepared-statement cache.
//! - [`storage`] — the spill-to-SQLite operator storage (`DatabaseStorage` /
//!   `OpStorage`), plugged into a graph via [`rindle::StorageFactory::custom`].
//!
//! It links the SAME vendored bedrock SQLite as the rest of the workspace through the
//! root `[patch.crates-io]` redirect, so no `build.rs` is needed here.

pub mod cost_model;
pub mod query_builder;
pub mod sqlite;
mod stat_fanout;
pub mod stmt_cache;
pub mod storage;
pub mod table_source;
#[cfg(feature = "testkit")]
pub mod testkit;
mod tiebreak;

use rindle::{Graph, NodeId};

pub use cost_model::{btree_cost, SqliteCostModel};
pub use query_builder::{build_select_query, ColumnDef, CompiledQuery, SqliteParam};
pub use storage::{DatabaseStorage, DatabaseStorageOptions, OpStorage};
pub use table_source::TableSource;
// The per-statement SQLite work counters + their shared sink — the `scanned` (work-proxy)
// number the `rindle analyze query` diagnostic reads back (`ANALYZE-QUERY-DESIGN.md` §3.3).
// `PlanSink` captures each leaf's fetch SQL and `explain_plan` re-`EXPLAIN`s it for the
// per-leaf access-path text. Feature-gated so a bare build links no instrumentation.
#[cfg(feature = "scan-stats")]
pub use cost_model::explain_plan;
#[cfg(feature = "scan-stats")]
pub use table_source::{PlanSink, ScanSink, ScanStats};

/// Adds `add_table_source` to [`rindle::Graph`] — the ergonomic equivalent of the
/// former inherent `Graph::add_table_source`, now that `TableSource` lives in this
/// crate. Delegates to the core [`Graph::add_dyn_source`].
///
/// ```ignore
/// use rindle_sqlite::GraphTableSourceExt;
/// let id = graph.add_table_source(table_source);
/// ```
pub trait GraphTableSourceExt {
    /// Add a SQLite [`TableSource`] as a graph leaf and return its `NodeId`.
    fn add_table_source(&mut self, source: TableSource) -> NodeId;
}

impl GraphTableSourceExt for Graph {
    fn add_table_source(&mut self, source: TableSource) -> NodeId {
        self.add_dyn_source(Box::new(source))
    }
}
