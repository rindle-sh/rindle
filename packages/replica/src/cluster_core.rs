//! The **cluster-backed async core** for `@rindle/replica`'s optimistic server path (the
//! `CLUSTER-FOLD-IN-DESIGN.md` WS6). The engine composition itself lives in
//! [`rindle_replica::ClusterConsumer`] (shared with the Rust daemon's network front); this
//! module keeps only the napi-specific glue: the temp-file lifecycle and the JSON sink
//! (`set_sink` installs the `ThreadsafeFunction` shim AFTER open — events produced with no
//! sink installed are dropped, which is safe because `onEvent` registers before any
//! subscription or commit).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rindle::Ast;
use rindle_replica::{
    wire_json, ClusterConsumer, ConnId, DrainSink, Mutation, NormalizedBatch, NormalizedHello,
    ProgressFrame, QueryId, ReplicaError,
};
use serde_json::{json, Value};

pub use rindle_replica::ClusterMutationWrite;

use crate::replica_core::ColType;

/// The sink closure the napi layer installs (a `ThreadsafeFunction` shim). Shared between
/// the coordinator (which sets it on `onEvent`) and the drain thread (which calls it).
type SinkCb = Arc<Mutex<Option<Box<dyn Fn(Value) + Send>>>>;

static TMP_CTR: AtomicU64 = AtomicU64::new(0);

/// How many IVM worker threads the cluster shards queries across.
fn worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().clamp(2, 8))
        .unwrap_or(4)
}

/// The drain sink that forwards each finished output to the napi `ThreadsafeFunction` as a
/// tagged JSON object (`{ t, conn, queryId?, batch|frame|reason }`).
struct CbSink {
    cb: SinkCb,
}

impl CbSink {
    fn emit(&self, v: Value) {
        if let Some(cb) = self.cb.lock().unwrap().as_ref() {
            cb(v);
        }
    }
}

impl DrainSink for CbSink {
    fn batch(&mut self, conn: ConnId, query_id: QueryId, batch: NormalizedBatch) {
        // The 09.8 `strict_i64` boundary check, mandatory since design 226 Stage C:
        // a batch carrying an `Int` outside `Number.MAX_SAFE_INTEGER` is delivered
        // as a `faulted` event (typed, per-query) instead of a silently rounded
        // batch. (This async surface cannot throw into the caller.)
        if let Some(i) = rindle_replica::unsafe_int_in_normalized_batch(&batch) {
            self.emit(json!({
                "t": "faulted",
                "conn": conn as f64,
                "queryId": query_id.0 as f64,
                "reason": format!(
                    "unsupported_value: i64 {i} exceeds the JS safe-integer range (strict_i64)"
                ),
            }));
            return;
        }
        self.emit(json!({
            "t": "batch",
            "conn": conn as f64,
            "queryId": query_id.0 as f64,
            "batch": wire_json::normalized_batch_to_json(&batch),
        }));
    }
    fn progress(&mut self, conn: ConnId, frame: ProgressFrame) {
        self.emit(json!({
            "t": "progress",
            "conn": conn as f64,
            "frame": { "cvMin": frame.cv_min as f64 },
        }));
    }
    fn faulted(&mut self, conn: ConnId, query_id: QueryId, reason: String) {
        self.emit(json!({
            "t": "faulted",
            "conn": conn as f64,
            "queryId": query_id.0 as f64,
            "reason": reason,
        }));
    }
}

/// The cluster-backed replica (coordinator side). Lives on the Node main thread.
pub struct ClusterReplica {
    consumer: ClusterConsumer,
    tmp: PathBuf,
    cb: SinkCb,
}

impl ClusterReplica {
    /// Open a fresh cluster-backed replica over a unique temp wal2 file. Spawns the worker
    /// pool + the drain thread (with a not-yet-installed sink — set later via `set_sink`).
    pub fn open() -> Result<ClusterReplica, ReplicaError> {
        let n = TMP_CTR.fetch_add(1, Ordering::Relaxed);
        let tmp = std::env::temp_dir().join(format!(
            "rindle-cluster-node-{}-{}.db",
            std::process::id(),
            n
        ));
        for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
            let _ = std::fs::remove_file(format!("{}{}", tmp.display(), suffix));
        }
        let cb: SinkCb = Arc::new(Mutex::new(None));
        let sink = CbSink { cb: cb.clone() };
        let consumer = ClusterConsumer::open(&tmp, worker_count(), sink)?;
        Ok(ClusterReplica { consumer, tmp, cb })
    }

    /// Install the async-event sink (the napi `ThreadsafeFunction` shim). Call once, before
    /// any subscription or commit, so no event is produced before it is set.
    pub fn set_sink(&self, cb: Box<dyn Fn(Value) + Send>) {
        *self.cb.lock().unwrap() = Some(cb);
    }

    /// Release the async-event sink (so its held `ThreadsafeFunction` ref drops and the Node
    /// event loop can exit). The drain thread keeps running but delivers nowhere; its
    /// threads are joined when this `ClusterReplica` is dropped. Idempotent.
    pub fn close(&self) {
        *self.cb.lock().unwrap() = None;
    }

    /// Define + register a base table (`CREATE TABLE` + cluster source). Idempotent.
    pub fn register_table(
        &self,
        table: &str,
        columns: &[String],
        pk: &[usize],
        col_types: &[ColType],
    ) -> Result<(), ReplicaError> {
        self.consumer.register_table(table, columns, pk, col_types)
    }

    /// One-time setup for client mutations (lmid-as-data; see the consumer).
    pub fn enable_client_mutations(&self) -> Result<(), ReplicaError> {
        self.consumer.enable_client_mutations()
    }

    /// The durably stored high-water mutation id for `client_id` (0 if new).
    pub fn client_lmid(&self, client_id: &str) -> Result<u64, ReplicaError> {
        self.consumer.client_lmid(client_id)
    }

    /// Register a connection (drain-side progress bookkeeping).
    pub fn connect(&self, conn: ConnId) {
        self.consumer.connect(conn);
    }

    /// Drop a connection's drain-side progress bookkeeping.
    pub fn disconnect(&self, conn: ConnId) {
        self.consumer.disconnect(conn);
    }

    /// Register a NORMALIZED live query for `conn`; `hello` returns synchronously, the
    /// snapshot + batches arrive through the sink. See the consumer.
    pub fn query_normalized(
        &self,
        conn: ConnId,
        server_qid: u64,
        ast: Ast,
        epoch: u64,
    ) -> Result<NormalizedHello, ReplicaError> {
        self.consumer.query_normalized(conn, server_qid, ast, epoch)
    }

    /// Tear down a registered query (cluster pipeline + drain bookkeeping).
    pub fn destroy_query(&self, server_qid: u64) {
        self.consumer.destroy_query(server_qid);
    }

    /// Apply a batch of positional mutations as one raw foreign write (no `lmid`).
    pub fn commit_normalized(&self, muts: &[Mutation]) -> Result<u64, ReplicaError> {
        self.consumer.commit_normalized(muts)
    }

    /// Open one mutation's write transaction (effects + lmid commit atomically).
    pub fn begin_mutation(&self) -> Result<ClusterMutationWrite, ReplicaError> {
        self.consumer.begin_mutation()
    }
}

impl Drop for ClusterReplica {
    fn drop(&mut self) {
        // The drain stops on its own Drop; remove the temp file + siblings.
        for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
            let _ = std::fs::remove_file(format!("{}{}", self.tmp.display(), suffix));
        }
    }
}
