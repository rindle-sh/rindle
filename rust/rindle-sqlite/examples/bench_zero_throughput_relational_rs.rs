//! Rindle port of Zero's `zql-benchmarks/src/zero-throughput-relational.bench.ts`.
//!
//! It measures how fast one *logical write* pushes through **300 standing
//! pipelines** — the core IVM engine wired to a leaf source (here both the SQLite
//! `TableSource` and the in-memory COW-B+tree `MemorySource`, so the two leaves can
//! be diffed like the other `bench_*_rs.rs` examples).
//!
//!   cargo run -p rindle-sqlite --release --example bench_zero_throughput_relational_rs --features fast-alloc
//!
//! ## Shape (mirrors the upstream profile op-for-op)
//!
//!   - 100 users × 3 standing query shapes = 300 query instances (pipelines).
//!   - 50 rows per top-level query window.
//!   - Seed: 1 org, 64 accounts, 4 contacts/account (256 contacts), 512 activities.
//!   - One logical write = add relActivity + edit relAccount + edit relOrg.
//!
//! The three query shapes are the deeply-relational ones from the upstream file
//! (org→accounts→{contacts, activities→contact} + org→activities; account→{org,
//! contacts, activities→contact}; activity→{org, account→contacts, contact}), built
//! with the fluent [`table`] query builder, lowered by [`build_pipeline`], and fed
//! by [`graph.source_push`](rindle::Graph::source_push). All 300 pipelines read from
//! the SAME 4 shared source nodes, so a single `source_push` fans out to every
//! pipeline that references that table — exactly the upstream "one write, N standing
//! queries" fan-out, without zero-cache / CVR / WebSockets.
//!
//! ## Push vs. flush — an architectural note
//!
//! Upstream (Zero's `ArrayView`) *accumulates* changes on `push` and applies +
//! notifies on `flush`. Rindle's `View` folds each change into the materialized tree
//! *during* `source_push` (marking the view dirty); `flush_view` only fires
//! listeners and closes the txn generation. So the Rindle "push only" arm already
//! includes the full IVM through to the materialized rows, and the "+ flush" arm adds
//! only the (listener-less here) notify pass. The two arms are still the faithful
//! `push` / `push + flush` split — just weighted differently than upstream by design.

use std::collections::HashMap;
use std::hint::black_box;
use std::rc::Rc;
use std::time::Instant;

use rindle::{
    build_pipeline, owned_row, table, view_schema, Ast, Graph, MemorySource, NodeId, OwnedRow,
    OwnedValue, Schema, SourceChange, SourceSchema, ValueType,
};
use rindle_sqlite::{ColumnDef, GraphTableSourceExt, TableSource};
use rusqlite::Connection as SqliteConnection;

use rindle_d2s::{compile, Cardinality, Catalog};

#[cfg(feature = "scan-stats")]
use rindle_sqlite::table_source::{ScanSink, ScanStats};
#[cfg(feature = "scan-stats")]
use std::cell::Cell;

#[cfg(feature = "fast-alloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const USERS: usize = 100;
const QUERIES_PER_USER: usize = 3;
const ROWS_PER_QUERY: u32 = 50;
const QUERY_INSTANCES: usize = USERS * QUERIES_PER_USER; // 300

const REL_ORG_ID: &str = "rel-org-0";
const REL_ACCOUNT_COUNT: i64 = 64;
const REL_CONTACTS_PER_ACCOUNT: i64 = 4;
const INITIAL_ACTIVITY_COUNT: i64 = 512;
const PAYLOAD_LEN: usize = 256;

// ---------------------------------------------------------------------------
// Timing harness — identical to the other `bench_*_rs.rs` examples
// (~200 ms warmup, then min ns/op over 6 rounds; emits `name|ns`).
// ---------------------------------------------------------------------------

fn bench(name: &str, mut body: impl FnMut()) {
    let warm = Instant::now();
    while warm.elapsed().as_nanos() < 200_000_000 {
        body();
    }
    let mut best = f64::INFINITY;
    for _ in 0..6 {
        let mut iters: u64 = 1;
        loop {
            let start = Instant::now();
            for _ in 0..iters {
                body();
            }
            let dt = start.elapsed().as_nanos() as f64;
            if dt >= 100_000_000.0 {
                best = best.min(dt / iters as f64);
                break;
            }
            iters *= 2;
        }
    }
    println!("{name}|{best:.1}");
}

// ---------------------------------------------------------------------------
// Value / row helpers
// ---------------------------------------------------------------------------

fn s(v: &str) -> OwnedValue {
    OwnedValue::str(v)
}

fn f(v: i64) -> OwnedValue {
    OwnedValue::Float(v as f64)
}

// ---------------------------------------------------------------------------
// Table definitions (columns in `ColId` order; column 0 is the PK).
// ---------------------------------------------------------------------------

type ColSpec = (&'static str, ValueType);

const ORG_COLS: &[ColSpec] = &[
    ("id", ValueType::String),
    ("name", ValueType::String),
    ("region", ValueType::String),
    ("seq", ValueType::Number),
    ("writtenAt", ValueType::Number),
    ("updatedAt", ValueType::Number),
];

const ACCOUNT_COLS: &[ColSpec] = &[
    ("id", ValueType::String),
    ("orgID", ValueType::String),
    ("ownerID", ValueType::String),
    ("name", ValueType::String),
    ("status", ValueType::String),
    ("seq", ValueType::Number),
    ("writtenAt", ValueType::Number),
    ("updatedAt", ValueType::Number),
];

const CONTACT_COLS: &[ColSpec] = &[
    ("id", ValueType::String),
    ("accountID", ValueType::String),
    ("name", ValueType::String),
    ("role", ValueType::String),
    ("seq", ValueType::Number),
    ("writtenAt", ValueType::Number),
    ("updatedAt", ValueType::Number),
];

const ACTIVITY_COLS: &[ColSpec] = &[
    ("id", ValueType::String),
    ("orgID", ValueType::String),
    ("accountID", ValueType::String),
    ("contactID", ValueType::String),
    ("kind", ValueType::String),
    ("body", ValueType::String),
    ("seq", ValueType::Number),
    ("writtenAt", ValueType::Number),
    ("updatedAt", ValueType::Number),
];

struct TableDef {
    name: &'static str,
    cols: &'static [ColSpec],
    /// Secondary index DDL, matching the upstream profile (DESC on `seq`).
    indexes: &'static [&'static str],
}

fn table_defs() -> [TableDef; 4] {
    [
        TableDef {
            name: "relOrg",
            cols: ORG_COLS,
            indexes: &[],
        },
        TableDef {
            name: "relAccount",
            cols: ACCOUNT_COLS,
            indexes: &["CREATE INDEX relAccount_org_seq_idx ON relAccount (orgID, seq DESC, id ASC);"],
        },
        TableDef {
            name: "relContact",
            cols: CONTACT_COLS,
            indexes: &["CREATE INDEX relContact_account_idx ON relContact (accountID, id ASC);"],
        },
        TableDef {
            name: "relActivity",
            cols: ACTIVITY_COLS,
            indexes: &[
                "CREATE INDEX relActivity_org_seq_idx ON relActivity (orgID, seq DESC, id ASC);",
                "CREATE INDEX relActivity_account_seq_idx ON relActivity (accountID, seq DESC, id ASC);",
                "CREATE INDEX relActivity_contact_seq_idx ON relActivity (contactID, seq DESC, id ASC);",
            ],
        },
    ]
}

fn column_defs(cols: &[ColSpec]) -> Vec<ColumnDef> {
    cols.iter()
        .map(|(name, ty)| ColumnDef {
            name: (*name).into(),
            ty: *ty,
            optional: false,
        })
        .collect()
}

fn col_names(cols: &[ColSpec]) -> Vec<&'static str> {
    cols.iter().map(|(name, _)| *name).collect()
}

/// The source-registration schema (columns + PK + primary/PK sort, no
/// relationships) — what `build_pipeline`'s `resolve` and the memory `add_source`
/// boundary speak.
fn source_schema_of(tdef: &TableDef) -> SourceSchema {
    SourceSchema::new(col_names(tdef.cols), vec![0], vec![(0, true)])
}

fn source_row_schema(tdef: &TableDef) -> Schema {
    Schema::new(col_names(tdef.cols), vec![0], vec![(0, true)])
}

// ---------------------------------------------------------------------------
// Row builders — column order matches the `*_COLS` specs above.
// ---------------------------------------------------------------------------

fn org_row(seq: i64) -> OwnedRow {
    owned_row(vec![
        s(REL_ORG_ID),
        s("Throughput Org"),
        s("na"),
        f(seq), // seq
        f(seq), // writtenAt
        f(seq), // updatedAt
    ])
}

fn account_row(index: i64, seq: i64) -> OwnedRow {
    owned_row(vec![
        s(&format!("rel-account-{index}")),
        s(REL_ORG_ID),
        s(&format!("owner-{}", index % 8)),
        s(&format!("Account {index}")),
        s(if index % 3 == 0 { "risk" } else { "active" }),
        f(seq), // seq
        f(seq), // writtenAt
        f(seq), // updatedAt
    ])
}

fn contact_row(account_index: i64, contact_index: i64) -> OwnedRow {
    owned_row(vec![
        s(&format!(
            "rel-account-{account_index}-contact-{contact_index}"
        )),
        s(&format!("rel-account-{account_index}")),
        s(&format!(
            "Contact {contact_index} at Account {account_index}"
        )),
        s(if contact_index == 0 {
            "buyer"
        } else {
            "stakeholder"
        }),
        f(0),
        f(0),
        f(0),
    ])
}

fn activity_row(seq: i64, payload: &str) -> OwnedRow {
    let account_index = seq % REL_ACCOUNT_COUNT;
    let contact_index = seq % REL_CONTACTS_PER_ACCOUNT;
    let account_id = format!("rel-account-{account_index}");
    owned_row(vec![
        s(&format!("bench-rel-activity-{seq}")),
        s(REL_ORG_ID),
        s(&account_id),
        s(&format!("{account_id}-contact-{contact_index}")),
        s(if seq % 5 == 0 { "meeting" } else { "note" }),
        s(payload),
        f(seq), // seq
        f(seq), // writtenAt
        f(seq), // updatedAt
    ])
}

/// Clone `row`, overwriting the `seq`, `writtenAt`, `updatedAt` triple (which always
/// sits at `seq_col, seq_col + 1, seq_col + 2`) with `seq`.
fn bump_seq(row: &OwnedRow, seq_col: usize, seq: i64) -> OwnedRow {
    let mut values: Vec<OwnedValue> = row.to_value_vec();
    let v = OwnedValue::Float(seq as f64);
    values[seq_col] = v.clone();
    values[seq_col + 1] = v.clone();
    values[seq_col + 2] = v;
    owned_row(values)
}

const ORG_SEQ_COL: usize = 3;
const ACCOUNT_SEQ_COL: usize = 5;

// ---------------------------------------------------------------------------
// Seed data (mirrors upstream `makeSeedData`)
// ---------------------------------------------------------------------------

struct SeedData {
    org: OwnedRow,
    accounts: Vec<OwnedRow>,
    contacts: Vec<OwnedRow>,
    activities: Vec<OwnedRow>,
    next_seq: i64,
}

fn make_seed_data(payload: &str) -> SeedData {
    // Each account / the org carries the last `seq` value that touched it (the
    // upstream loop bumps them as it emits the seed activities).
    let mut account_seq = vec![0i64; REL_ACCOUNT_COUNT as usize];
    let mut org_seq = 0i64;
    let mut activities = Vec::with_capacity(INITIAL_ACTIVITY_COUNT as usize);
    for seq in 1..=INITIAL_ACTIVITY_COUNT {
        let account_index = (seq % REL_ACCOUNT_COUNT) as usize;
        account_seq[account_index] = seq;
        org_seq = seq;
        activities.push(activity_row(seq, payload));
    }

    let accounts = (0..REL_ACCOUNT_COUNT)
        .map(|i| account_row(i, account_seq[i as usize]))
        .collect();

    let mut contacts = Vec::with_capacity((REL_ACCOUNT_COUNT * REL_CONTACTS_PER_ACCOUNT) as usize);
    for account_index in 0..REL_ACCOUNT_COUNT {
        for contact_index in 0..REL_CONTACTS_PER_ACCOUNT {
            contacts.push(contact_row(account_index, contact_index));
        }
    }

    SeedData {
        org: org_row(org_seq),
        accounts,
        contacts,
        activities,
        next_seq: INITIAL_ACTIVITY_COUNT + 1,
    }
}

// ---------------------------------------------------------------------------
// The three standing query shapes (built with the fluent query builder).
// The schema-free builder derives each relationship's correlation from the
// `where(childCol, row.col(parentCol))` link inside the `sub_as` closure.
// ---------------------------------------------------------------------------

fn relational_query(query_index: usize) -> Ast {
    match query_index % QUERIES_PER_USER {
        // org → { accounts → { contacts, activities → contact }, activities }
        0 => table("relOrg")
            .r#where("id", REL_ORG_ID)
            .sub_as("accounts", |org| {
                table("relAccount")
                    .r#where("orgID", org.col("id"))
                    .order_by("seq", "desc")
                    .limit(ROWS_PER_QUERY)
                    .sub_as("contacts", |acct| {
                        table("relContact").r#where("accountID", acct.col("id"))
                    })
                    .sub_as("activities", |acct| {
                        table("relActivity")
                            .r#where("accountID", acct.col("id"))
                            .order_by("seq", "desc")
                            .limit(5)
                            .sub_as("contact", |act| {
                                table("relContact")
                                    .r#where("id", act.col("contactID"))
                                    .one()
                            })
                    })
            })
            .sub_as("activities", |org| {
                table("relActivity")
                    .r#where("orgID", org.col("id"))
                    .order_by("seq", "desc")
                    .limit(ROWS_PER_QUERY)
            })
            .build(),

        // account → { org, contacts, activities → contact }  (top 50 by seq)
        1 => table("relAccount")
            .r#where("orgID", REL_ORG_ID)
            .sub_as("org", |acct| {
                table("relOrg").r#where("id", acct.col("orgID")).one()
            })
            .sub_as("contacts", |acct| {
                table("relContact").r#where("accountID", acct.col("id"))
            })
            .sub_as("activities", |acct| {
                table("relActivity")
                    .r#where("accountID", acct.col("id"))
                    .order_by("seq", "desc")
                    .limit(5)
                    .sub_as("contact", |act| {
                        table("relContact")
                            .r#where("id", act.col("contactID"))
                            .one()
                    })
            })
            .order_by("seq", "desc")
            .limit(ROWS_PER_QUERY)
            .build(),

        // activity → { org, account → contacts, contact }  (top 50 by seq)
        2 => table("relActivity")
            .r#where("orgID", REL_ORG_ID)
            .sub_as("org", |act| {
                table("relOrg").r#where("id", act.col("orgID")).one()
            })
            .sub_as("account", |act| {
                table("relAccount")
                    .r#where("id", act.col("accountID"))
                    .one()
                    .sub_as("contacts", |acct| {
                        table("relContact").r#where("accountID", acct.col("id"))
                    })
            })
            .sub_as("contact", |act| {
                table("relContact")
                    .r#where("id", act.col("contactID"))
                    .one()
            })
            .order_by("seq", "desc")
            .limit(ROWS_PER_QUERY)
            .build(),

        _ => unreachable!("query_index % {QUERIES_PER_USER} is 0..{QUERIES_PER_USER}"),
    }
}

// ---------------------------------------------------------------------------
// Backends
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Backend {
    Memory,
    Table,
}

impl Backend {
    fn label(self) -> &'static str {
        match self {
            Backend::Memory => "memory",
            Backend::Table => "table",
        }
    }
}

fn create_table_sql(tdef: &TableDef) -> String {
    let defs = tdef
        .cols
        .iter()
        .enumerate()
        .map(|(i, (name, ty))| {
            let sqlty = match ty {
                ValueType::Boolean | ValueType::Int => "INTEGER",
                ValueType::Number => "REAL",
                ValueType::String | ValueType::Json | ValueType::Null => "TEXT",
            };
            let pk = if i == 0 { " PRIMARY KEY" } else { "" };
            format!("\"{name}\" {sqlty} NOT NULL{pk}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE TABLE \"{}\" ({defs});", tdef.name)
}

// Shared scan-stats sink (`--features scan-stats`): every `TableSource` cursor
// accumulates its `sqlite3_stmt_status` work here on drop, so a read after a
// single sub-push tells us how many leaf statements ran (`runs` ≈ fetch/probe
// calls) and how much SQLite work they cost (`vm_steps`) — i.e. whether one push
// fans out to a huge number of source fetches.
#[cfg(feature = "scan-stats")]
thread_local! {
    static SCAN_SINK: ScanSink = Rc::new(Cell::new(ScanStats::default()));
}

/// Read and zero the shared scan-stats accumulator.
#[cfg(feature = "scan-stats")]
fn take_scan_stats() -> ScanStats {
    SCAN_SINK.with(|s| s.replace(ScanStats::default()))
}

/// Build a SQLite `TableSource` over `tdef`'s table in `db`, seeded with `rows`.
fn build_table_source(
    db: &Rc<SqliteConnection>,
    tdef: &TableDef,
    rows: &[OwnedRow],
) -> TableSource {
    db.execute_batch(&create_table_sql(tdef))
        .expect("create bench table");
    for ddl in tdef.indexes {
        db.execute_batch(ddl).expect("create bench index");
    }
    let source = TableSource::new_with_schema(
        Rc::clone(db),
        tdef.name,
        column_defs(tdef.cols),
        vec![0],
        source_schema_of(tdef),
    );
    for row in rows {
        source.push(SourceChange::Add(row.clone()), &|_, _| {});
    }
    // Attach the shared work-counter AFTER seeding so the seed writes aren't counted.
    #[cfg(feature = "scan-stats")]
    SCAN_SINK.with(|s| source.set_scan_sink(Rc::clone(s)));
    source
}

// ---------------------------------------------------------------------------
// Bench state — one graph, 4 shared sources, 300 pipelines/views.
// ---------------------------------------------------------------------------

struct BenchState {
    graph: Graph,
    org_src: NodeId,
    account_src: NodeId,
    activity_src: NodeId,
    views: Vec<NodeId>,
    org: OwnedRow,
    accounts: Vec<OwnedRow>,
    next_seq: i64,
    payload: String,
    /// The shared SQLite connection (Table backend only; `None` for Memory). All 4
    /// sources read + write through it, so an explicit `BEGIN`/`COMMIT` here wraps
    /// every write in one transaction — see [`run_one_tx`].
    db: Option<Rc<SqliteConnection>>,
}

// When set (via `--scan-count <shape>`), `setup` builds USERS pipelines of *only*
// that one query shape, so scan-count can attribute the per-sub-push fetch fan-out to
// a single shape (0=org root, 1=account root, 2=activity root).
thread_local! {
    static SHAPE_FILTER: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

fn setup(backend: Backend) -> BenchState {
    let payload = "x".repeat(PAYLOAD_LEN);
    let seed = make_seed_data(&payload);
    let defs = table_defs();

    let mut graph = Graph::new();
    // One in-memory db holds all 4 tables (the `Table` backend shares one connection,
    // matching the upstream single-db setup).
    let db = Rc::new(SqliteConnection::open_in_memory().expect("open bench sqlite db"));
    if let Backend::Table = backend {
        db.execute_batch("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;")
            .expect("set bench pragmas");
    }

    let rows_for = |name: &str| -> Vec<OwnedRow> {
        match name {
            "relOrg" => vec![seed.org.clone()],
            "relAccount" => seed.accounts.clone(),
            "relContact" => seed.contacts.clone(),
            "relActivity" => seed.activities.clone(),
            other => panic!("no seed rows for {other:?}"),
        }
    };

    let mut by_name: HashMap<&'static str, (NodeId, SourceSchema)> = HashMap::new();
    for tdef in &defs {
        let rows = rows_for(tdef.name);
        let node = match backend {
            Backend::Memory => {
                graph.add_memory_source(MemorySource::new(source_row_schema(tdef), rows))
            }
            Backend::Table => graph.add_table_source(build_table_source(&db, tdef, &rows)),
        };
        by_name.insert(tdef.name, (node, source_schema_of(tdef)));
    }

    // Build the 300 pipelines. Every `build_pipeline` call attaches fresh
    // connections to the same 4 source nodes, so one `source_push` later fans out to
    // all of them.
    let asts: Vec<Ast> = match SHAPE_FILTER.with(|f| f.get()) {
        Some(s) => vec![relational_query(s)],
        None => (0..QUERIES_PER_USER).map(relational_query).collect(),
    };
    let resolve = |name: &str| by_name.get(name).cloned();
    let mut views = Vec::with_capacity(QUERY_INSTANCES);
    for _user in 0..USERS {
        for ast in &asts {
            let top = build_pipeline(&mut graph, ast, &resolve).expect("build bench pipeline");
            let schema = view_schema(ast, &resolve).expect("build bench view schema");
            let view = graph.add_view(top, schema);
            graph.set_sink_edge(top, view);
            graph.hydrate(view);
            views.push(view);
        }
    }

    let org_src = by_name["relOrg"].0;
    let account_src = by_name["relAccount"].0;
    let activity_src = by_name["relActivity"].0;

    BenchState {
        graph,
        org_src,
        account_src,
        activity_src,
        views,
        org: seed.org,
        accounts: seed.accounts,
        next_seq: seed.next_seq,
        payload,
        db: match backend {
            Backend::Table => Some(db),
            Backend::Memory => None,
        },
    }
}

/// Advance to the next logical-write `seq`, returning `(seq, account_index)`.
fn next_write(state: &mut BenchState) -> (i64, usize) {
    let seq = state.next_seq;
    state.next_seq += 1;
    (seq, (seq % REL_ACCOUNT_COUNT) as usize)
}

/// Sub-push 1: add the new activity (fans to every pipeline reading relActivity).
fn push_activity(state: &mut BenchState, seq: i64) {
    let activity = activity_row(seq, &state.payload);
    state
        .graph
        .source_push(state.activity_src, SourceChange::Add(activity));
}

/// Sub-push 2: bump the touched account's `seq` (re-sorts it wherever it is ordered).
fn push_account_edit(state: &mut BenchState, seq: i64, account_index: usize) {
    let old_account = state.accounts[account_index].clone();
    let new_account = bump_seq(&old_account, ACCOUNT_SEQ_COL, seq);
    state.graph.source_push(
        state.account_src,
        SourceChange::Edit {
            row: new_account.clone(),
            old: old_account,
        },
    );
    state.accounts[account_index] = new_account;
}

/// Sub-push 3: bump the org's `seq` (a plain column edit that cascades to every
/// `related('org')` child across all pipelines).
fn push_org_edit(state: &mut BenchState, seq: i64) {
    let old_org = state.org.clone();
    let new_org = bump_seq(&old_org, ORG_SEQ_COL, seq);
    state.graph.source_push(
        state.org_src,
        SourceChange::Edit {
            row: new_org.clone(),
            old: old_org,
        },
    );
    state.org = new_org;
}

/// One logical write: add an activity, edit the touched account's seq, edit the org's
/// seq — each fanning out to every pipeline that reads that table.
fn push_logical_write(state: &mut BenchState) {
    let (seq, account_index) = next_write(state);
    push_activity(state, seq);
    push_account_edit(state, seq, account_index);
    push_org_edit(state, seq);
    black_box(state.next_seq);
}

fn flush_views(state: &BenchState) {
    for &view in &state.views {
        state.graph.flush_view(view);
    }
}

// ---------------------------------------------------------------------------
// Profiling modes (`--breakdown`, `--profile <table|memory|both>`)
// ---------------------------------------------------------------------------

/// Attribute `push_only`'s cost across its three sub-pushes, so we know which one
/// dominates before reaching for a flamegraph. `Instant` overhead (~20 ns) is
/// negligible against the ms-scale sub-pushes.
fn run_breakdown() {
    const WARMUP: usize = 100;
    const N: usize = 2000;
    println!("# breakdown: mean ns per sub-push over {N} logical writes (after {WARMUP} warmup)");
    for backend in [Backend::Table, Backend::Memory] {
        let mut state = setup(backend);
        for _ in 0..WARMUP {
            push_logical_write(&mut state);
        }
        let (mut act, mut acc, mut org) = (0u128, 0u128, 0u128);
        for _ in 0..N {
            let (seq, account_index) = next_write(&mut state);
            let t = Instant::now();
            push_activity(&mut state, seq);
            act += t.elapsed().as_nanos();
            let t = Instant::now();
            push_account_edit(&mut state, seq, account_index);
            acc += t.elapsed().as_nanos();
            let t = Instant::now();
            push_org_edit(&mut state, seq);
            org += t.elapsed().as_nanos();
        }
        let n = N as u128;
        println!(
            "{}.breakdown|activity_add={} account_edit={} org_edit={} total={} (ns/op)",
            backend.label(),
            act / n,
            acc / n,
            org / n,
            (act + acc + org) / n,
        );
    }
}

/// The d2s [`Catalog`] for the 4 bench tables: columns (in projection order) + PK,
/// plus each relationship's cardinality (the one fact the [`Ast`] doesn't carry —
/// `.one()` ⇒ [`One`](Cardinality::One), otherwise [`Many`](Cardinality::Many)). The
/// relationship *names* are the `sub_as("name", …)` aliases in [`relational_query`].
fn build_catalog() -> Catalog {
    use Cardinality::{Many, One};
    let mut cat = Catalog::new();
    for tdef in &table_defs() {
        cat.table(tdef.name, &col_names(tdef.cols), &["id"]);
    }
    cat.relationship("relOrg", "accounts", Many)
        .relationship("relOrg", "activities", Many)
        .relationship("relAccount", "org", One)
        .relationship("relAccount", "contacts", Many)
        .relationship("relAccount", "activities", Many)
        .relationship("relActivity", "org", One)
        .relationship("relActivity", "account", One)
        .relationship("relActivity", "contact", One);
    cat
}

/// Apply one logical write straight to a plain SQLite db (no IVM graph): the 3
/// mutations pushed through their [`TableSource`]s with a **no-op driver**, so
/// `write_change` hits the table but nothing fans out. This is the write half of the
/// raw-SQL app in [`run_sql_baseline`].
#[allow(clippy::too_many_arguments)]
fn raw_write(
    activity_src: &TableSource,
    account_src: &TableSource,
    org_src: &TableSource,
    accounts: &mut [OwnedRow],
    org: &mut OwnedRow,
    next_seq: &mut i64,
    payload: &str,
) {
    let seq = *next_seq;
    *next_seq += 1;
    let ai = (seq % REL_ACCOUNT_COUNT) as usize;
    activity_src.push(SourceChange::Add(activity_row(seq, payload)), &|_, _| {});
    let old = accounts[ai].clone();
    let new = bump_seq(&old, ACCOUNT_SEQ_COL, seq);
    account_src.push(
        SourceChange::Edit {
            row: new.clone(),
            old,
        },
        &|_, _| {},
    );
    accounts[ai] = new;
    let old_org = org.clone();
    let new_org = bump_seq(&old_org, ORG_SEQ_COL, seq);
    org_src.push(
        SourceChange::Edit {
            row: new_org.clone(),
            old: old_org,
        },
        &|_, _| {},
    );
    *org = new_org;
}

/// The `re-execution baseline`: what an app that just **re-queries SQLite on every
/// write** gets, instead of maintaining views incrementally. Each of the 300 standing
/// queries is compiled by [`rindle_d2s`] into one nested-JSON `SELECT` — the exact
/// result the IVM view materializes — and re-run in full on each write. We give raw
/// SQL its best case: the 3 distinct shapes are prepared/plan-cached and executed
/// `USERS` (100) times each, so SQLite parses + plans once and only pays execution.
///
/// Measured at the same warmed table size against IVM's `push_logical_write`, so the
/// ratio is the concrete "incremental vs. re-run-from-scratch" win on the worst-case
/// (every query sees every write) fan-out.
fn run_sql_baseline(warmup: usize, n: usize) {
    let (warmup, n) = (warmup.max(1), n.max(1));

    // Compile the 3 distinct shapes to one JSON-building SELECT each (literals inlined,
    // so no bind params). All roots are plural (a plain array result).
    let catalog = build_catalog();
    let sqls: Vec<String> = (0..QUERIES_PER_USER)
        .map(|i| compile(&catalog, &relational_query(i)).expect("d2s compile bench query"))
        .collect();

    // ---- Raw-SQL app: a plain seeded db, warmed with no-fan-out writes. ----
    let payload = "x".repeat(PAYLOAD_LEN);
    let seed = make_seed_data(&payload);
    let defs = table_defs();
    let db = Rc::new(SqliteConnection::open_in_memory().expect("open sql-baseline db"));
    db.execute_batch("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;")
        .expect("set sql-baseline pragmas");
    let org_src = build_table_source(&db, &defs[0], std::slice::from_ref(&seed.org));
    let account_src = build_table_source(&db, &defs[1], &seed.accounts);
    let _contact_src = build_table_source(&db, &defs[2], &seed.contacts);
    let activity_src = build_table_source(&db, &defs[3], &seed.activities);

    let mut accounts = seed.accounts.clone();
    let mut org = seed.org.clone();
    let mut next_seq = seed.next_seq;
    for _ in 0..warmup {
        raw_write(
            &activity_src,
            &account_src,
            &org_src,
            &mut accounts,
            &mut org,
            &mut next_seq,
            &payload,
        );
    }
    let table_rows = INITIAL_ACTIVITY_COUNT as usize + warmup;

    // Min over N writes of [apply write + re-run all 300 queries as SQL].
    let mut sql_best = f64::INFINITY;
    let mut sink = 0usize;
    for _ in 0..n {
        raw_write(
            &activity_src,
            &account_src,
            &org_src,
            &mut accounts,
            &mut org,
            &mut next_seq,
            &payload,
        );
        let t = Instant::now();
        for sql in &sqls {
            let mut stmt = db.prepare_cached(sql).expect("prepare compiled query");
            for _ in 0..USERS {
                let json: String = stmt
                    .query_row([], |r| r.get(0))
                    .expect("run compiled query");
                sink = sink.wrapping_add(json.len());
            }
        }
        sql_best = sql_best.min(t.elapsed().as_nanos() as f64);
    }
    black_box(sink);

    // ---- IVM at the same warmed table size, for the ratio. ----
    let mut ivm_state = setup(Backend::Table);
    for _ in 0..warmup {
        push_logical_write(&mut ivm_state);
    }
    let mut ivm_best = f64::INFINITY;
    for _ in 0..n {
        let t = Instant::now();
        push_logical_write(&mut ivm_state);
        ivm_best = ivm_best.min(t.elapsed().as_nanos() as f64);
    }

    let wps = |ns: f64| 1e9 / ns;
    println!(
        "# sql-baseline: raw SQL re-query (d2s-compiled) vs IVM, ~{table_rows} relActivity rows"
    );
    println!(
        "#   one write = 3 mutations; re-query = all {QUERY_INSTANCES} standing queries re-run as SQL"
    );
    println!(
        "#   compiled SQL sizes (chars): shape0={} shape1={} shape2={}",
        sqls[0].len(),
        sqls[1].len(),
        sqls[2].len(),
    );
    println!(
        "sql_reexec | {sql_best:>13.1} ns/write  {:>7.2} writes/sec  ({:.0} ns per single SELECT)",
        wps(sql_best),
        sql_best / QUERY_INSTANCES as f64,
    );
    println!(
        "ivm        | {ivm_best:>13.1} ns/write  {:>7.2} writes/sec",
        wps(ivm_best),
    );
    println!(
        "=> IVM is {:.0}× faster per write than re-running the queries as SQL",
        sql_best / ivm_best,
    );
}

/// Answer "is each write its own txn, and what does one enclosing txn buy?" for the
/// Table backend. Each `source_push` writes via a bare `execute()` in SQLite
/// autocommit, so one logical write = 3 implicit transactions. Here we time N logical
/// writes (a) in that default autocommit mode and (b) all wrapped in a single
/// `BEGIN…COMMIT`, on a fresh `setup()` each, and report ns/op + writes/sec for both.
///
/// The interleaved fetches run on the SAME connection, so uncommitted writes stay
/// visible to them — correctness is identical; only the per-statement transaction
/// bookkeeping differs. (It's an in-memory db, so there's no journal/fsync either
/// way — this isolates the pure autocommit-open/close cost.)
fn run_one_tx() {
    const WARMUP: usize = 100;
    const N: usize = 2000;
    println!(
        "# one-tx: {N} logical writes (= {} source pushes), autocommit-per-write vs one BEGIN/COMMIT",
        N * 3
    );

    let time_run = |wrap_in_txn: bool| -> f64 {
        let mut state = setup(Backend::Table);
        for _ in 0..WARMUP {
            push_logical_write(&mut state);
        }
        let db = state.db.clone().expect("table backend has a connection");
        let start = Instant::now();
        if wrap_in_txn {
            db.execute_batch("BEGIN").expect("begin");
        }
        for _ in 0..N {
            push_logical_write(&mut state);
        }
        if wrap_in_txn {
            db.execute_batch("COMMIT").expect("commit");
        }
        start.elapsed().as_nanos() as f64 / N as f64
    };

    // Best-of-3 each, to cut machine noise.
    let mut auto = f64::INFINITY;
    let mut onetx = f64::INFINITY;
    for _ in 0..3 {
        auto = auto.min(time_run(false));
        onetx = onetx.min(time_run(true));
    }
    let wps = |ns: f64| 1e9 / ns;
    println!(
        "table.autocommit | {auto:>12.1} ns/op  {:>7.1} writes/sec",
        wps(auto)
    );
    println!(
        "table.one_txn    | {onetx:>12.1} ns/op  {:>7.1} writes/sec  ({:.2}× vs autocommit)",
        wps(onetx),
        auto / onetx,
    );
}

#[cfg(feature = "scan-stats")]
fn add_stats(a: ScanStats, b: ScanStats) -> ScanStats {
    ScanStats {
        fullscan_steps: a.fullscan_steps + b.fullscan_steps,
        sorts: a.sorts + b.sorts,
        vm_steps: a.vm_steps + b.vm_steps,
        runs: a.runs + b.runs,
    }
}

/// Count SQLite leaf work per sub-push (`--features scan-stats`). Directly answers
/// "does a push fan out to a huge number of source fetches?" by reading the shared
/// scan-stats sink around each of the three sub-pushes. `runs` ≈ number of leaf
/// `SELECT`s executed; `vm_steps` ≈ total SQLite work; `fullscan_steps` ≈ rows
/// visited by un-indexed scans.
#[cfg(feature = "scan-stats")]
fn run_scan_count(shape: Option<usize>) {
    const WARMUP: usize = 100;
    const N: usize = 200;
    SHAPE_FILTER.with(|f| f.set(shape));
    let n_pipelines = if shape.is_some() {
        USERS
    } else {
        QUERY_INSTANCES
    };
    let mut state = setup(Backend::Table);
    for _ in 0..WARMUP {
        push_logical_write(&mut state);
    }
    let (mut act, mut acc, mut org) = (
        ScanStats::default(),
        ScanStats::default(),
        ScanStats::default(),
    );
    for _ in 0..N {
        let (seq, account_index) = next_write(&mut state);
        let _ = take_scan_stats(); // discard anything since last read
        push_activity(&mut state, seq);
        act = add_stats(act, take_scan_stats());
        push_account_edit(&mut state, seq, account_index);
        acc = add_stats(acc, take_scan_stats());
        push_org_edit(&mut state, seq);
        org = add_stats(org, take_scan_stats());
    }
    println!(
        "# scan-count: SQLite leaf work per sub-push (mean over {N} logical writes, after {WARMUP} warmup)"
    );
    println!(
        "# {n_pipelines} pipelines ({}) share 4 sources; activity table ~{} rows during the run",
        match shape {
            Some(0) => "shape 0: org root",
            Some(1) => "shape 1: account root",
            Some(2) => "shape 2: activity root",
            _ => "all 3 shapes",
        },
        INITIAL_ACTIVITY_COUNT as usize + WARMUP + N
    );
    let n = N as u64;
    let show = |label: &str, s: ScanStats| {
        println!(
            "{label:14}| runs={:>6} vm_steps={:>8} fullscan_steps={:>7} sorts={:>5}  (per logical write)",
            s.runs / n,
            s.vm_steps / n,
            s.fullscan_steps / n,
            s.sorts / n,
        );
    };
    show("activity_add", act);
    show("account_edit", acc);
    show("org_edit", org);
}

/// Sample `push_only` under pprof for `secs` and write `flamegraphs/…svg`.
/// Requires `--features profile`.
#[cfg(feature = "profile")]
fn run_profile(which: &str) {
    let backends: &[Backend] = match which {
        "table" => &[Backend::Table],
        "memory" => &[Backend::Memory],
        "both" | "all" => &[Backend::Table, Backend::Memory],
        other => panic!("unknown --profile target {other:?} (want table|memory|both)"),
    };
    std::fs::create_dir_all("flamegraphs").ok();
    for &backend in backends {
        let mut state = setup(backend);
        for _ in 0..100 {
            push_logical_write(&mut state); // reach steady state before sampling
        }
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(4000)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .expect("build pprof guard");
        let start = Instant::now();
        let mut iters: u64 = 0;
        while start.elapsed().as_secs() < 5 {
            push_logical_write(&mut state);
            iters += 1;
        }
        eprintln!(
            "# profiled push_only.{}: {iters} iters / 5s",
            backend.label()
        );
        let report = guard.report().build().expect("build pprof report");
        print_top_frames(backend.label(), &report);
        let path = format!(
            "flamegraphs/zero_throughput_push_only.{}.svg",
            backend.label()
        );
        let f = std::fs::File::create(&path).expect("create flamegraph file");
        report.flamegraph(f).expect("write flamegraph svg");
        println!("# flamegraph -> {path}");
    }
}

/// Aggregate a pprof report into readable top-N tables: self-time (samples whose
/// innermost frame is this fn) and cumulative (samples with this fn anywhere in the
/// stack, deduped per sample). `frames.frames` is leaf-first (index 0 = innermost).
#[cfg(feature = "profile")]
fn print_top_frames(label: &str, report: &pprof::Report) {
    use std::collections::{HashMap, HashSet};
    let mut self_time: HashMap<String, isize> = HashMap::new();
    let mut cumulative: HashMap<String, isize> = HashMap::new();
    let mut total: isize = 0;
    for (frames, count) in &report.data {
        total += *count;
        if let Some(leaf) = frames.frames.first().and_then(|f| f.first()) {
            *self_time.entry(leaf.name()).or_default() += *count;
        }
        let mut seen = HashSet::new();
        for frame in &frames.frames {
            for sym in frame {
                let name = sym.name();
                if seen.insert(name.clone()) {
                    *cumulative.entry(name).or_default() += *count;
                }
            }
        }
    }
    let pct = |c: isize| 100.0 * c as f64 / total.max(1) as f64;
    let dump = |title: &str, map: HashMap<String, isize>, take: usize| {
        let mut v: Vec<_> = map.into_iter().collect();
        v.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
        println!("## {label} — {title} (total {total} samples)");
        for (name, c) in v.into_iter().take(take) {
            let name = if name.len() > 96 {
                format!("{}…", &name[..96])
            } else {
                name
            };
            println!("  {:6.2}%  {name}", pct(c));
        }
    };
    dump("self-time (leaf)", self_time, 25);
    dump("cumulative (any frame)", cumulative, 30);
    // NOTE: pprof's signal-based self-time over-attributes to the `madvise` syscall on
    // macOS (truncated system-lib stacks). Cross-check the leaf table against macOS
    // `sample` (`sample <pid> …`), which showed the real leaders are `sqlite3VdbeExec`
    // (query execution) + allocation + row memmoves — a fetch-bound profile.
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--breakdown") {
        run_breakdown();
        return;
    }
    if args.iter().any(|a| a == "--one-tx") {
        run_one_tx();
        return;
    }
    if let Some(pos) = args.iter().position(|a| a == "--sql-baseline") {
        // Optional `--sql-baseline [warmup] [n]` to sweep table size (defaults 200 20).
        let warmup = args
            .get(pos + 1)
            .and_then(|a| a.parse().ok())
            .unwrap_or(200);
        let n = args.get(pos + 2).and_then(|a| a.parse().ok()).unwrap_or(20);
        run_sql_baseline(warmup, n);
        return;
    }
    if let Some(pos) = args.iter().position(|a| a == "--scan-count") {
        // Optional `--scan-count <0|1|2>` to isolate a single query shape.
        let _shape = args.get(pos + 1).and_then(|a| a.parse::<usize>().ok());
        #[cfg(feature = "scan-stats")]
        run_scan_count(_shape.filter(|s| *s < QUERIES_PER_USER));
        #[cfg(not(feature = "scan-stats"))]
        eprintln!("--scan-count needs `--features scan-stats`; rebuild with it");
        return;
    }
    if let Some(pos) = args.iter().position(|a| a == "--profile") {
        let which = args.get(pos + 1).map(String::as_str).unwrap_or("table");
        #[cfg(feature = "profile")]
        run_profile(which);
        #[cfg(not(feature = "profile"))]
        {
            let _ = which;
            eprintln!("--profile needs `--features profile` (pprof); rebuild with it");
        }
        return;
    }

    println!(
        "# zero-throughput-relational: {QUERY_INSTANCES} pipelines \
         (one logical write = add relActivity + edit relAccount + edit relOrg)"
    );
    for backend in [Backend::Table, Backend::Memory] {
        {
            let mut state = setup(backend);
            bench(&format!("{}.push_only", backend.label()), || {
                push_logical_write(&mut state);
            });
        }
        {
            let mut state = setup(backend);
            bench(&format!("{}.push_flush", backend.label()), || {
                push_logical_write(&mut state);
                flush_views(&state);
            });
        }
    }
}
