//! The pure-Rust core of the `@rindle/replica` addon: a thin façade over `rindle_replica::Db`
//! that mirrors the wasm `Db`'s register/query/write/commit/destroy surface but is
//! SQLite-backed. It turns the engine's `CaughtChange` stream into **flat changes** (the
//! exact shape the JS `ArrayView` folds) + a **wire schema**, and translates the Store's
//! positional add/remove/edit mutations into SQL against the controlled writer.
//!
//! No napi here — this is plain Rust, unit-tested against the [`Receiver`] oracle. The
//! napi glue (value marshalling, the `#[napi]` class) wraps this in `lib.rs`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use rindle::{flatten_all, to_wire, Ast, FlatChange, OwnedValue, WireSchema, COMPARATOR_VERSION};
use rindle_replica::{
    agg_table_schemas, table_tree, AggTable, Db, NormalizedBatch, NormalizedHello,
    NormalizedPublisher, Query, QueryId, ReplicaError, TableNode, TableWireSchema, Update,
};

/// The comparator-contract version the engine ships (re-exported for the `hello`).
pub const COMPARATOR_VERSION_OUT: u32 = COMPARATOR_VERSION;

// `ColType`/`TableMeta`/`Mutation` + the DDL/SQL builders live in `rindle_replica` now
// (lifted so the cluster consumer and this single-thread core share one implementation).
pub(crate) use rindle_replica::{build_mutation_sql, create_table_ddl, TableMeta};
pub use rindle_replica::{ColType, Mutation};

/// The `hello` + hydrate snapshot for one registered query.
pub struct QueryInit {
    pub schema: WireSchema,
    pub comparator_version: u32,
    pub snapshot: Vec<FlatChange>,
}

/// The normalized `hello` + the seq-0 snapshot batch for one registered normalized query
/// (NORMALIZED-CHANGES-DESIGN.md §3). The path-free twin of [`QueryInit`].
pub struct NormalizedInit {
    pub hello: NormalizedHello,
    pub snapshot: NormalizedBatch,
}

static TMP_CTR: AtomicU64 = AtomicU64::new(0);

/// Collect every distinct base-table name a query's tree can surface (root + descendants).
fn collect_table_names(node: &TableNode, out: &mut std::collections::BTreeSet<String>) {
    out.insert(node.table.to_string());
    // Pruned (`exists_noSync`) slots are `None`: their permission tables are never synced.
    for c in node.children.iter().flatten() {
        collect_table_names(c, out);
    }
}

/// The SQLite-backed replica engine, façaded for the JS backend. All methods take `&self`
/// (interior mutability) so the napi class can expose them on a shared handle.
pub struct Replica {
    db: Db,
    tmp: PathBuf,
    tables: RefCell<HashMap<String, TableMeta>>,
    /// Live query handles (kept alive; dropped/destroyed on `destroy_query`).
    queries: RefCell<HashMap<u64, Query>>,
    /// Per-query flat changes accumulated by each query's subscribe callback during the
    /// most recent commit; drained (and returned) by [`Replica::commit`].
    pending: Rc<RefCell<HashMap<u64, Vec<FlatChange>>>>,
    /// Per-normalized-query batches accumulated during the most recent commit; drained
    /// (and returned) by [`Replica::commit_normalized`]. A normalized query's subscribe
    /// callback folds its `CaughtChange`s through its own [`NormalizedPublisher`] (which it
    /// owns) and pushes the resulting batch here.
    pending_norm: Rc<RefCell<HashMap<u64, Vec<NormalizedBatch>>>>,
}

impl Replica {
    /// Open a fresh replica over a unique temp wal2 file (the engine requires a file-backed
    /// path). The file (and its -wal/-wal2/-shm siblings) is removed on drop.
    pub fn open() -> Result<Replica, ReplicaError> {
        let n = TMP_CTR.fetch_add(1, Ordering::Relaxed);
        let tmp = std::env::temp_dir().join(format!(
            "rindle-replica-node-{}-{}.db",
            std::process::id(),
            n
        ));
        // A stale file from a crashed run would carry old state — start clean.
        for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
            let _ = std::fs::remove_file(format!("{}{}", tmp.display(), suffix));
        }
        let db = Db::open(&tmp)?;
        Ok(Replica {
            db,
            tmp,
            tables: RefCell::new(HashMap::new()),
            queries: RefCell::new(HashMap::new()),
            pending: Rc::new(RefCell::new(HashMap::new())),
            pending_norm: Rc::new(RefCell::new(HashMap::new())),
        })
    }

    /// Define + register a base table: `CREATE TABLE` from the typed column spec, then teach
    /// the engine about it. Idempotent (`CREATE TABLE IF NOT EXISTS`). `pk` indexes into
    /// `columns`.
    pub fn register_table(
        &self,
        table: &str,
        columns: &[String],
        pk: &[usize],
        col_types: &[ColType],
    ) -> Result<(), ReplicaError> {
        if self.tables.borrow().contains_key(table) {
            return Ok(());
        }
        self.db
            .exec_ddl(&create_table_ddl(table, columns, pk, col_types))?;
        self.db.register_table(table)?;
        self.tables.borrow_mut().insert(
            table.to_string(),
            TableMeta {
                columns: columns.to_vec(),
                pk: pk.to_vec(),
                col_types: col_types.to_vec(),
                // `create_table_ddl` emits no `NOT NULL`, so every column of a typed registration is
                // nullable in the generated DDL — which is exactly what `schema::discover` reads back.
                nullable: vec![true; columns.len()],
            },
        );
        Ok(())
    }

    /// Register a live query under `qid`: build its wire schema, hydrate, and capture the
    /// snapshot as flat changes. A subscribe callback accumulates every later commit's
    /// changes into `pending` (keyed by `qid`) for [`commit`](Replica::commit) to return.
    pub fn query(&self, qid: u64, ast: Ast) -> Result<QueryInit, ReplicaError> {
        let schema = to_wire(&self.db.view_schema(&ast)?);
        let q = self.db.query(QueryId(qid), ast)?;

        // `subscribe` fires `Hydrated` synchronously now (→ the snapshot), then `Changed`
        // per future commit (→ `pending[qid]`).
        let snapshot_slot: Rc<RefCell<Vec<FlatChange>>> = Rc::new(RefCell::new(Vec::new()));
        let snap = snapshot_slot.clone();
        let pending = self.pending.clone();
        q.subscribe(move |update: &Update| match update {
            Update::Hydrated { changes, .. } => {
                *snap.borrow_mut() = flatten_all(changes);
            }
            Update::Changed { changes, .. } => {
                pending
                    .borrow_mut()
                    .entry(qid)
                    .or_default()
                    .extend(flatten_all(changes));
            }
        });

        let snapshot = snapshot_slot.borrow().clone();
        self.queries.borrow_mut().insert(qid, q);
        Ok(QueryInit {
            schema,
            comparator_version: COMPARATOR_VERSION_OUT,
            snapshot,
        })
    }

    /// Tear down a registered query (reclaims its pipeline + stops delivery). No-op for an
    /// unknown id.
    pub fn destroy_query(&self, qid: u64) {
        if let Some(q) = self.queries.borrow_mut().remove(&qid) {
            q.destroy();
        }
        self.pending.borrow_mut().remove(&qid);
    }

    /// Register a NORMALIZED live query under `qid` (NORMALIZED-CHANGES-DESIGN.md §4/§5): the
    /// path-free twin of [`query`](Replica::query). The query owns a [`NormalizedPublisher`]
    /// (folding `CaughtChange`s → table-tagged `NormalizedOp`s, server-side intra-query
    /// dedup); its subscribe callback captures the seq-0 snapshot synchronously and pushes
    /// each later commit's batch into `pending_norm`. Returns the slim `hello` + the snapshot.
    pub fn query_normalized(
        &self,
        qid: u64,
        ast: Ast,
        epoch: u64,
    ) -> Result<NormalizedInit, ReplicaError> {
        let tables = self.normalized_table_schemas(&ast)?;
        let mut publisher = NormalizedPublisher::new(epoch, &ast, tables);
        let hello = publisher.hello().clone();
        let q = self.db.query(QueryId(qid), ast)?;

        // The publisher is moved INTO the subscribe callback (it owns the stateful fold).
        let snap_slot: Rc<RefCell<Option<NormalizedBatch>>> = Rc::new(RefCell::new(None));
        let snap = snap_slot.clone();
        let pending = self.pending_norm.clone();
        q.subscribe(move |update: &Update| match update {
            Update::Hydrated { changes, tx_id } => {
                *snap.borrow_mut() = Some(publisher.snapshot(changes, tx_id.0));
            }
            Update::Changed { changes, tx_id } => {
                if let Some(batch) = publisher.commit(changes, tx_id.0) {
                    pending.borrow_mut().entry(qid).or_default().push(batch);
                }
            }
        });
        let snapshot = snap_slot
            .borrow_mut()
            .take()
            .expect("subscribe fires Hydrated synchronously");
        self.queries.borrow_mut().insert(qid, q);
        Ok(NormalizedInit { hello, snapshot })
    }

    /// Apply a batch of mutations as one transaction, then return each affected query's
    /// derived flat changes (`[(queryId, events)]`), mirroring the wasm `Db.commit`. An
    /// empty batch still opens+commits a (no-op) transaction and returns nothing.
    pub fn commit(&self, muts: &[Mutation]) -> Result<Vec<(u64, Vec<FlatChange>)>, ReplicaError> {
        self.pending.borrow_mut().clear();
        self.apply_txn(muts)?; // fires the per-query subscribe callbacks → fills `pending`
        Ok(self.pending.borrow_mut().drain().collect())
    }

    /// The normalized twin of [`commit`](Replica::commit): apply the txn and return each
    /// affected NORMALIZED query's batch (`[(queryId, batch)]`). One commit is one tick, so a
    /// query emits at most one batch.
    pub fn commit_normalized(
        &self,
        muts: &[Mutation],
    ) -> Result<Vec<(u64, NormalizedBatch)>, ReplicaError> {
        self.pending_norm.borrow_mut().clear();
        self.apply_txn(muts)?;
        let mut out = Vec::new();
        for (qid, batches) in self.pending_norm.borrow_mut().drain() {
            for b in batches {
                out.push((qid, b));
            }
        }
        Ok(out)
    }

    /// Apply one transaction's mutations against the controlled writer (shared by the flat
    /// and normalized commit paths). Firing `commit` runs every registered query's subscribe
    /// callback, filling whichever `pending*` map applies.
    fn apply_txn(&self, muts: &[Mutation]) -> Result<(), ReplicaError> {
        let mut txn = self.db.write()?;
        for m in muts {
            let (sql, params) = self.build_sql(m)?;
            txn.exec(&sql, &params)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// The flat schema of every base table the query's tree can surface (root + each
    /// `related`/EXISTS child table) — what the `NormalizedPublisher` needs for the slim
    /// hello + the `NormalizeFold`'s PK map. Derived from the registered table metadata.
    fn normalized_table_schemas(&self, ast: &Ast) -> Result<Vec<TableWireSchema>, ReplicaError> {
        // A relationship aggregate surfaces a SYNTHETIC table (`AGGREGATE-SYNC-DESIGN.md` §3.2)
        // whose schema is derived from the AST (`agg_table_schemas`), not the table registry —
        // the child rows it replaces are never synced. Mirror of
        // `ClusterConsumer::normalized_table_schemas` for the single-thread napi core.
        let synth: HashMap<Box<str>, AggTable> = agg_table_schemas(ast)
            .into_iter()
            .map(|t| (t.name.clone(), t))
            .collect();
        let mut names = std::collections::BTreeSet::new();
        collect_table_names(&table_tree(ast), &mut names);
        let tables = self.tables.borrow();
        let mut out = Vec::with_capacity(names.len());
        for name in &names {
            if let Some(at) = synth.get(name.as_str()) {
                // Synthetic aggregate table: schema from the AST, PK = the leading group cols.
                out.push(TableWireSchema {
                    name: at.name.clone(),
                    columns: at.columns.clone(),
                    primary_key: (0..at.key_len as u32).collect(),
                });
            } else {
                let meta = tables.get(name.as_str()).ok_or_else(|| {
                    ReplicaError::Schema(format!("unknown table in query tree: {name}"))
                })?;
                out.push(TableWireSchema {
                    name: name.as_str().into(),
                    columns: meta.columns.iter().map(|c| c.as_str().into()).collect(),
                    primary_key: meta.pk.iter().map(|&i| i as u32).collect(),
                });
            }
        }
        Ok(out)
    }

    /// Build the SQL + bound params for one positional mutation against its table's meta.
    fn build_sql(&self, m: &Mutation) -> Result<(String, Vec<OwnedValue>), ReplicaError> {
        let tables = self.tables.borrow();
        let meta = tables
            .get(m.table())
            .ok_or_else(|| ReplicaError::Schema(format!("unknown table: {}", m.table())))?;
        build_mutation_sql(&meta.columns, &meta.pk, m)
    }

    // --- the optimistic-writes upstream (OPTIMISTIC-WRITES-DESIGN.md §8.1–§8.2) -----

    /// One-time (idempotent) setup for client mutations: the `_rindle_client_mutations`
    /// table is created, captured, AND engine-hosted (lmid-as-data) — its meta joins the
    /// table map so the per-client system query's `hello` resolves like any table's.
    pub fn enable_client_mutations(&self) -> Result<(), ReplicaError> {
        self.db.enable_client_mutations()?;
        self.tables.borrow_mut().insert(
            rindle_replica::CLIENT_MUTATIONS_TABLE.to_string(),
            TableMeta {
                columns: vec!["client_id".to_string(), "last_mutation_id".to_string()],
                pk: vec![0],
                col_types: vec![ColType::String, ColType::Number],
                // Bookkeeping table (both columns NOT NULL); excluded from `base_table_schemas`.
                nullable: vec![false, false],
            },
        );
        Ok(())
    }

    /// The durably stored high-water mutation id for `client_id` (0 for a new client).
    pub fn client_lmid(&self, client_id: &str) -> Result<u64, ReplicaError> {
        self.db.client_lmid(client_id)
    }

    /// Open one mutation's write transaction. The JS server registry runs its mutator
    /// against it (`exec`/`query`), then [`MutationWrite::commit_with_lmid`] lands the
    /// effects + the lmid upsert atomically and returns the commit's `cv` + every
    /// affected NORMALIZED query's batch (already `cv`-stamped by its publisher).
    pub fn begin_mutation(&self) -> Result<MutationWrite, ReplicaError> {
        self.pending_norm.borrow_mut().clear();
        Ok(MutationWrite {
            txn: Some(self.db.write()?),
            pending_norm: self.pending_norm.clone(),
        })
    }
}

/// One mutation's open transaction (see [`Replica::begin_mutation`]). Dropping without
/// committing rolls back (the inner `WriteTxn`'s contract).
pub struct MutationWrite {
    txn: Option<rindle_replica::WriteTxn>,
    pending_norm: Rc<RefCell<HashMap<u64, Vec<NormalizedBatch>>>>,
}

impl MutationWrite {
    fn txn(&mut self) -> Result<&mut rindle_replica::WriteTxn, ReplicaError> {
        self.txn
            .as_mut()
            .ok_or_else(|| ReplicaError::Mutation("transaction already finished".into()))
    }

    /// Run one statement with positional parameters (the mutator's write path).
    pub fn exec(&mut self, sql: &str, params: &[OwnedValue]) -> Result<usize, ReplicaError> {
        self.txn()?.exec(sql, params)
    }

    /// Run a read through the open transaction (sees its own uncommitted writes — the
    /// read-dependent mutator contract, §4.1). Raw SQLite storage classes.
    pub fn query(
        &mut self,
        sql: &str,
        params: &[OwnedValue],
    ) -> Result<Vec<Vec<OwnedValue>>, ReplicaError> {
        use rindle_replica::MutationSql;
        self.txn()?.query(sql, params)
    }

    /// Upsert `last_mutation_id = mid` for `client_id` **in this transaction**, commit,
    /// and return `(cv, per-query normalized batches)`. The lmid rides the same commit
    /// as the mutator's effects (§8.2) — and therefore the same CDC capture, which the
    /// engine partitions off before the push.
    pub fn commit_with_lmid(
        &mut self,
        client_id: &str,
        mid: u64,
    ) -> Result<(u64, Vec<(u64, NormalizedBatch)>), ReplicaError> {
        let mut txn = self
            .txn
            .take()
            .ok_or_else(|| ReplicaError::Mutation("transaction already finished".into()))?;
        txn.exec(
            &format!(
                "INSERT INTO {}(client_id, last_mutation_id) VALUES(?1, ?2) \
                 ON CONFLICT(client_id) DO UPDATE \
                 SET last_mutation_id = excluded.last_mutation_id",
                rindle_replica::CLIENT_MUTATIONS_TABLE
            ),
            &[OwnedValue::str(client_id), OwnedValue::Int(mid as i64)],
        )?;
        let info = txn.commit_with_info()?;
        let mut batches = Vec::new();
        for (qid, bs) in self.pending_norm.borrow_mut().drain() {
            for b in bs {
                batches.push((qid, b));
            }
        }
        Ok((info.tx_id.0, batches))
    }

    /// Roll back: effects discarded, nothing delivered.
    pub fn rollback(&mut self) {
        if let Some(txn) = self.txn.take() {
            txn.rollback();
        }
    }
}

impl Drop for Replica {
    fn drop(&mut self) {
        // Drop live query handles + the Db first (closes connections), then remove the file.
        self.queries.borrow_mut().clear();
        for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
            let _ = std::fs::remove_file(format!("{}{}", self.tmp.display(), suffix));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rindle::{to_schema, OwnedValue as V, Receiver};

    fn ids(top: &[rindle::RecvNode]) -> Vec<i64> {
        // id stored as REAL → reads back Float; accept Int or Float.
        top.iter()
            .map(|n| match &n.row.col(0).to_owned() {
                V::Int(i) => *i,
                V::Float(f) => *f as i64,
                o => panic!("unexpected id cell: {o:?}"),
            })
            .collect()
    }

    fn title(node: &rindle::RecvNode) -> String {
        match &node.row.col(1).to_owned() {
            V::Str(s) => s.to_string(),
            o => panic!("unexpected title cell: {o:?}"),
        }
    }

    #[test]
    fn register_query_write_reconstructs_via_receiver() {
        let r = Replica::open().expect("open");
        r.register_table(
            "issue",
            &["id".into(), "title".into()],
            &[0],
            &[ColType::Number, ColType::String],
        )
        .expect("register issue");

        // Register BEFORE any data → empty snapshot.
        let init = r.query(1, Ast::new("issue")).expect("query");
        assert!(init.snapshot.is_empty(), "snapshot empty before any write");
        assert_eq!(init.comparator_version, COMPARATOR_VERSION_OUT);

        // The Receiver is the Rust twin of the JS ArrayView — fold the same stream.
        let mut recv = Receiver::new(to_schema(&init.schema));
        recv.apply_all(&init.snapshot);
        assert!(recv.top().is_empty());

        // Commit two adds (out of order) in one txn.
        let batches = r
            .commit(&[
                Mutation::Add {
                    table: "issue".into(),
                    row: vec![V::Float(2.0), V::Str("b".into())],
                },
                Mutation::Add {
                    table: "issue".into(),
                    row: vec![V::Float(1.0), V::Str("a".into())],
                },
            ])
            .expect("commit");
        assert_eq!(batches.len(), 1, "one affected query");
        let (qid, events) = &batches[0];
        assert_eq!(*qid, 1);
        recv.apply_all(events);
        assert_eq!(ids(recv.top()), vec![1, 2], "sorted by pk id asc");
        assert_eq!(title(&recv.top()[0]), "a");

        // A second commit: edit + add.
        let batches = r
            .commit(&[
                Mutation::Edit {
                    table: "issue".into(),
                    old: vec![V::Float(1.0), V::Str("a".into())],
                    new: vec![V::Float(1.0), V::Str("A".into())],
                },
                Mutation::Add {
                    table: "issue".into(),
                    row: vec![V::Float(3.0), V::Str("c".into())],
                },
            ])
            .expect("commit 2");
        for (_, events) in &batches {
            recv.apply_all(events);
        }
        assert_eq!(ids(recv.top()), vec![1, 2, 3]);
        assert_eq!(title(&recv.top()[0]), "A", "edit applied");

        // Remove the middle row.
        let batches = r
            .commit(&[Mutation::Remove {
                table: "issue".into(),
                row: vec![V::Float(2.0), V::Str("b".into())],
            }])
            .expect("commit 3");
        for (_, events) in &batches {
            recv.apply_all(events);
        }
        assert_eq!(ids(recv.top()), vec![1, 3]);
    }

    #[test]
    fn empty_commit_yields_no_batches() {
        let r = Replica::open().expect("open");
        r.register_table("t", &["id".into()], &[0], &[ColType::Number])
            .expect("reg");
        r.query(7, Ast::new("t")).expect("query");
        let batches = r.commit(&[]).expect("empty commit");
        assert!(batches.is_empty());
    }

    #[test]
    fn destroyed_query_stops_receiving() {
        let r = Replica::open().expect("open");
        r.register_table("t", &["id".into()], &[0], &[ColType::Number])
            .expect("reg");
        r.query(1, Ast::new("t")).expect("q1");
        r.query(2, Ast::new("t")).expect("q2");
        r.destroy_query(1);
        let batches = r
            .commit(&[Mutation::Add {
                table: "t".into(),
                row: vec![V::Float(5.0)],
            }])
            .expect("commit");
        // Only the surviving query (2) gets the change.
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0, 2);
    }
}
