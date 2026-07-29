//! Large-scale E2E **EXISTS discard** bench: a big parent table scanned in full
//! where an `EXISTS` / `NOT EXISTS` gate throws away almost every row, so the result
//! set is tiny but every parent row still had to be read out of the leaf.
//!
//! This is the "large table scan, near-empty result" case. A plain indexed filter
//! lets the `TableSource` leaf push the predicate into SQLite and read only the
//! surviving rows out; an `EXISTS` gate cannot be pushed into the leaf `SELECT`, so
//! the engine streams **every** parent row out of SQLite and the EXISTS operator
//! discards ~99.9% of them in the dataflow. The Memory-vs-Table delta isolates the
//! per-row SQLite step/statement floor paid for those discarded rows; the
//! `exists_rare` vs `filter_rare_indexed` delta shows the cost of *not* being able to
//! push the predicate down.
//!
//! Like the other `bench_*_rs.rs` examples this is a hand-rolled `name|ns` bench so it
//! diffs directly against them.
//!
//!   cargo run -p rindle-sqlite --release --features fast-alloc --example bench_large_exists_rs
//!
//! Env knobs:
//!   LARGE_EXISTS_SCALES=50000,200000   parent row counts to sweep (default 50000,200000)
//!   LARGE_EXISTS_ANALYZE=1             run SQLite ANALYZE after load (planner stats for
//!                                      the two-equality EXISTS child probe; see chinook bench)
//!
//! Output is one line per measured cell: `backend|pattern|rowsN|ns` (lower better),
//! plus a `# rows` banner and a `# select` report (Memory vs Table surviving counts —
//! which doubles as a parity check AND shows how few rows make it past each gate).

use std::collections::BTreeMap;
use std::env;
use std::hint::black_box;
use std::rc::Rc;
use std::time::Instant;

use rusqlite::types::Value as RValue;
use rusqlite::{params_from_iter, Connection as SqliteConnection};

use rindle::view::ViewData;
use rindle::{
    build_pipeline, owned_row, table, view_schema, Ast, Graph, MemorySource, NodeId, OwnedRow,
    OwnedValue as V, SourceChange, SourceSchema, ValueType,
};
use rindle_sqlite::{ColumnDef, GraphTableSourceExt, TableSource};

#[cfg(feature = "fast-alloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// Selectivity strides over the parent's row index `i`:
//   - `i % RARE_STRIDE == 0`   gets a `rare` tag  (~0.1% of parents match `exists_rare`)
//   - `i % COMMON_STRIDE == 0` gets a `common` tag (~50%  match `exists_common`)
//   - the parent's `bucket = i % RARE_STRIDE`, so `bucket = RARE_BUCKET` selects the
//     same ~0.1% as the rare EXISTS but via an indexable column the leaf CAN push down.
const RARE_STRIDE: usize = 1000;
const COMMON_STRIDE: usize = 2;
const RARE_BUCKET: f64 = 7.0;

// ---------------------------------------------------------------------------
// Timing harness — identical methodology to the other bench_*_rs examples:
// ~200ms warmup, then the min ns/op over 6 rounds (min = most noise-robust).
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
// Schema / data generation
// ---------------------------------------------------------------------------

fn col(name: &str, ty: ValueType, optional: bool) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ty,
        optional,
    }
}

fn schema_of(cols: &[ColumnDef], pk: &[usize]) -> SourceSchema {
    let names: Vec<&str> = cols.iter().map(|c| &*c.name).collect();
    let sort: Vec<(usize, bool)> = pk.iter().map(|&c| (c, true)).collect();
    SourceSchema::new(names, pk.to_vec(), sort)
}

fn event_columns() -> Vec<ColumnDef> {
    vec![
        col("id", ValueType::Number, false),
        col("bucket", ValueType::Number, false),
        col("title", ValueType::String, false),
        col("body", ValueType::String, false),
    ]
}

fn tag_columns() -> Vec<ColumnDef> {
    vec![
        col("id", ValueType::Number, false),
        col("eventID", ValueType::Number, false),
        col("kind", ValueType::String, false),
    ]
}

fn event_row(i: usize) -> OwnedRow {
    owned_row(vec![
        V::Float(i as f64),
        V::Float((i % RARE_STRIDE) as f64),
        V::str(&format!("Event {i}")),
        V::str(&format!("Body for event {i}: lorem ipsum dolor sit amet")),
    ])
}

fn events(n: usize) -> Vec<OwnedRow> {
    (0..n).map(event_row).collect()
}

/// One `tag` row per (event, kind) match, with sequential ids. Only ~50% of events get
/// a `common` tag and only ~0.1% get a `rare` tag, so the EXISTS gates over these
/// throw away most of the parent scan.
fn tags(n: usize) -> Vec<OwnedRow> {
    let mut out = Vec::new();
    let mut id = 0.0_f64;
    for i in 0..n {
        if i % COMMON_STRIDE == 0 {
            out.push(owned_row(vec![
                V::Float(id),
                V::Float(i as f64),
                V::str("common"),
            ]));
            id += 1.0;
        }
        if i % RARE_STRIDE == 0 {
            out.push(owned_row(vec![
                V::Float(id),
                V::Float(i as f64),
                V::str("rare"),
            ]));
            id += 1.0;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Leaves (Memory + Table) for one source table, built once per scale and forked
// per graph build.
// ---------------------------------------------------------------------------

struct Loaded {
    mem: MemorySource,
    tab: TableSource,
    schema: SourceSchema,
    rows: usize,
}

struct Catalog {
    rows: usize,
    tables: BTreeMap<&'static str, Loaded>,
}

fn load(
    name: &'static str,
    cols: Vec<ColumnDef>,
    pk: Vec<usize>,
    indexes: Vec<Vec<usize>>,
    rows: Vec<OwnedRow>,
) -> Loaded {
    let schema = schema_of(&cols, &pk);
    let mem = MemorySource::new(schema.clone().into_schema(), rows.clone());
    let tab = make_table_source(name, &cols, &pk, &indexes, &rows);
    Loaded {
        mem,
        tab,
        schema,
        rows: rows.len(),
    }
}

fn build_catalog(n: usize) -> Catalog {
    let mut tables = BTreeMap::new();
    tables.insert(
        "event",
        load(
            "event",
            event_columns(),
            vec![0],
            vec![vec![1]], // index on `bucket` — the pushdown-able filter
            events(n),
        ),
    );
    tables.insert(
        "tag",
        load(
            "tag",
            tag_columns(),
            vec![0],
            // FK join-key index + the (tempting, non-selective) `kind` index, so the
            // planner has the same wrong-index choice the chinook bench's ANALYZE knob
            // controls for the two-equality EXISTS child probe.
            vec![vec![1], vec![2]],
            tags(n),
        ),
    );
    Catalog { rows: n, tables }
}

fn to_rusqlite(v: &V) -> RValue {
    match v {
        V::Absent | V::Null => RValue::Null,
        V::Bool(b) => RValue::Integer(if *b { 1 } else { 0 }),
        V::Int(i) => RValue::Integer(*i),
        V::Float(f) => RValue::Real(*f),
        V::Str(s) => RValue::Text(s.to_string()),
        V::Json(s) => RValue::Text(s.to_string()),
    }
}

fn q(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Create + bulk-load a fresh in-memory SQLite table (one prepared INSERT in a
/// transaction, indexes built after the load), and wrap it in a `TableSource`.
fn make_table_source(
    name: &str,
    cols: &[ColumnDef],
    pk: &[usize],
    indexes: &[Vec<usize>],
    rows: &[OwnedRow],
) -> TableSource {
    let conn = Rc::new(SqliteConnection::open_in_memory().expect("open table-source db"));
    let coldefs = cols
        .iter()
        .map(|c| {
            let ty = match c.ty {
                ValueType::Boolean => "INTEGER",
                ValueType::Number => "REAL",
                _ => "TEXT",
            };
            let nn = if c.optional { "" } else { " NOT NULL" };
            format!("{} {ty}{nn}", q(&c.name))
        })
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!("CREATE TABLE {} ({coldefs});", q(name)))
        .expect("create table");

    let colnames = cols
        .iter()
        .map(|c| q(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = vec!["?"; cols.len()].join(", ");
    let insert = format!(
        "INSERT INTO {} ({colnames}) VALUES ({placeholders})",
        q(name)
    );
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn.prepare(&insert).expect("prepare insert");
        for row in rows {
            let params: Vec<RValue> = row.cells().map(|v| to_rusqlite(&v.to_owned())).collect();
            stmt.execute(params_from_iter(params.iter()))
                .expect("insert row");
        }
    }
    conn.execute_batch("COMMIT").unwrap();

    let pkcols = pk
        .iter()
        .map(|&c| q(&cols[c].name))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!(
        "CREATE UNIQUE INDEX {} ON {} ({pkcols});",
        q(&format!("pk_{name}")),
        q(name)
    ))
    .expect("create pk index");
    for (k, idx) in indexes.iter().enumerate() {
        let icols = idx
            .iter()
            .map(|&c| q(&cols[c].name))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!(
            "CREATE INDEX {} ON {} ({icols});",
            q(&format!("ix_{name}_{k}")),
            q(name)
        ))
        .expect("create index");
    }

    // See chinook bench: ANALYZE gives the planner column-selectivity stats so the
    // two-equality EXISTS child probe (eventID=? AND kind=?) picks the selective
    // join-key index instead of the non-selective `kind` index.
    if env::var("LARGE_EXISTS_ANALYZE").as_deref() == Ok("1") {
        conn.execute_batch("ANALYZE").expect("analyze");
    }

    TableSource::new_with_schema(conn, name, cols.to_vec(), pk.to_vec(), schema_of(cols, pk))
}

// ---------------------------------------------------------------------------
// Query ladder — all over the large `event` parent.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
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

struct Fixture {
    name: &'static str,
    ast: Ast,
    needs: &'static [&'static str],
}

fn hydration_fixtures() -> Vec<Fixture> {
    vec![
        // Baseline: full scan, everything survives — the cost of reading ALL rows out.
        Fixture {
            name: "scan",
            ast: table("event").build(),
            needs: &["event"],
        },
        // Indexable filter that keeps ~0.1%: the leaf pushes `bucket = ?` into SQLite
        // and reads only the survivors out. The contrast to `exists_rare`.
        Fixture {
            name: "filter_rare_indexed",
            ast: table("event").where_op("bucket", "=", RARE_BUCKET).build(),
            needs: &["event"],
        },
        // EXISTS keeping ~50% — moderate discard.
        Fixture {
            name: "exists_common",
            ast: table("event")
                .where_exists(|r| {
                    table("tag")
                        .r#where("eventID", r.col("id"))
                        .where_op("kind", "=", "common")
                        .alias("has_common")
                })
                .build(),
            needs: &["event", "tag"],
        },
        // THE headline case: EXISTS keeping ~0.1%. Every parent row is streamed out of
        // the leaf, the gate discards ~99.9%, the result set is tiny.
        Fixture {
            name: "exists_rare",
            ast: table("event")
                .where_exists(|r| {
                    table("tag")
                        .r#where("eventID", r.col("id"))
                        .where_op("kind", "=", "rare")
                        .alias("has_rare")
                })
                .build(),
            needs: &["event", "tag"],
        },
        // NOT EXISTS keeping ~99.9% — the inverse gate (almost everything survives).
        Fixture {
            name: "not_exists_rare",
            ast: table("event")
                .where_not_exists(|r| {
                    table("tag")
                        .r#where("eventID", r.col("id"))
                        .where_op("kind", "=", "rare")
                        .alias("no_rare")
                })
                .build(),
            needs: &["event", "tag"],
        },
        // NOT EXISTS keeping ~50%.
        Fixture {
            name: "not_exists_common",
            ast: table("event")
                .where_not_exists(|r| {
                    table("tag")
                        .r#where("eventID", r.col("id"))
                        .where_op("kind", "=", "common")
                        .alias("no_common")
                })
                .build(),
            needs: &["event", "tag"],
        },
    ]
}

// ---------------------------------------------------------------------------
// Graph build / hydrate
// ---------------------------------------------------------------------------

struct BuiltGraph {
    graph: Graph,
    view: NodeId,
    sources: BTreeMap<&'static str, NodeId>,
}

fn build_view_graph(fx: &Fixture, backend: Backend, cat: &Catalog) -> BuiltGraph {
    let mut graph = Graph::new();
    let mut ids: BTreeMap<&'static str, (NodeId, SourceSchema)> = BTreeMap::new();
    let mut sources: BTreeMap<&'static str, NodeId> = BTreeMap::new();
    for &t in fx.needs {
        let loaded = cat
            .tables
            .get(t)
            .unwrap_or_else(|| panic!("missing table {t}"));
        let id = match backend {
            Backend::Memory => graph.add_memory_source(loaded.mem.fork()),
            Backend::Table => graph.add_table_source(loaded.tab.fork()),
        };
        ids.insert(t, (id, loaded.schema.clone()));
        sources.insert(t, id);
    }
    let resolve = |t: &str| ids.get(t).cloned();
    let vschema = view_schema(&fx.ast, &resolve).expect("derive view schema");
    let top = build_pipeline(&mut graph, &fx.ast, &resolve).expect("build pipeline");
    let view = graph.add_view(top, vschema);
    graph.set_sink_edge(top, view);
    graph.hydrate(view);
    BuiltGraph {
        graph,
        view,
        sources,
    }
}

fn count_view_data(data: &ViewData) -> usize {
    data.items.len()
}

fn hydrate_count(fx: &Fixture, backend: Backend, cat: &Catalog) -> usize {
    let built = build_view_graph(fx, backend, cat);
    count_view_data(&built.graph.view_data(built.view))
}

// ---------------------------------------------------------------------------
// Drivers
// ---------------------------------------------------------------------------

/// Report each fixture's surviving row count on both leaves: doubles as a parity check
/// (Memory must equal Table) AND shows just how few rows make it past each gate.
fn report_selectivity(fixtures: &[Fixture], cat: &Catalog) {
    for fx in fixtures {
        let m = hydrate_count(fx, Backend::Memory, cat);
        let t = hydrate_count(fx, Backend::Table, cat);
        let mark = if m == t { "ok" } else { "MISMATCH" };
        let pct = 100.0 * m as f64 / cat.rows.max(1) as f64;
        println!(
            "# select rows{:<7} {:<22} memory={m} table={t} ({pct:.2}% kept) {mark}",
            cat.rows, fx.name
        );
    }
}

fn run_hydration(fixtures: &[Fixture], cat: &Catalog) {
    for fx in fixtures {
        for backend in [Backend::Memory, Backend::Table] {
            let name = format!("{}|{}|rows{}", backend.label(), fx.name, cat.rows);
            bench(&name, || {
                black_box(hydrate_count(fx, backend, cat));
            });
        }
    }
}

/// Steady-state IVM cost: a reversible add+remove of one row. `push_event` adds/removes
/// a parent under the full scan; `push_exists_flip` adds/removes a `rare` tag for a
/// parent that currently has none, flipping it into and out of the near-empty result.
fn run_push(fixtures: &[Fixture], cat: &Catalog) {
    let by_name: BTreeMap<&str, &Fixture> = fixtures.iter().map(|f| (f.name, f)).collect();

    // (push-label, base-fixture, target source table, reversible row)
    let event_id = 1_000_000_000.0;
    let flip_event = 3.0; // not a multiple of RARE_STRIDE, so it has no `rare` tag yet
    let specs: Vec<(&str, &str, &str, OwnedRow)> = vec![
        (
            "push_event_scan",
            "scan",
            "event",
            owned_row(vec![
                V::Float(event_id),
                V::Float(RARE_BUCKET),
                V::str("Pushed event"),
                V::str("Pushed body"),
            ]),
        ),
        (
            "push_exists_flip",
            "exists_rare",
            "tag",
            owned_row(vec![
                V::Float(2_000_000_000.0),
                V::Float(flip_event),
                V::str("rare"),
            ]),
        ),
    ];

    for (label, fxname, target, row) in &specs {
        let fx = by_name[fxname];
        for backend in [Backend::Memory, Backend::Table] {
            let built = build_view_graph(fx, backend, cat);
            let src = built.sources[target];
            let name = format!("{}|{}|rows{}", backend.label(), label, cat.rows);
            bench(&name, || {
                built.graph.source_push(src, SourceChange::Add(row.clone()));
                built
                    .graph
                    .source_push(src, SourceChange::Remove(row.clone()));
                black_box(count_view_data(&built.graph.view_data(built.view)));
            });
        }
    }
}

fn scales() -> Vec<usize> {
    match env::var("LARGE_EXISTS_SCALES") {
        Ok(s) => s
            .split(',')
            .filter_map(|x| x.trim().parse::<usize>().ok())
            .collect(),
        Err(_) => vec![50_000, 200_000],
    }
}

fn main() {
    let fixtures = hydration_fixtures();
    for n in scales() {
        let cat = build_catalog(n);
        let total: usize = cat.tables.values().map(|l| l.rows).sum();
        println!(
            "# rows {n}: {} source rows across {} tables",
            total,
            cat.tables.len()
        );
        report_selectivity(&fixtures, &cat);
        run_hydration(&fixtures, &cat);
        run_push(&fixtures, &cat);
    }
}
