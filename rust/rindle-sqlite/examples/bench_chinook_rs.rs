//! End-to-end **profiling** bench over the real **chinook** dataset (v1.4.5),
//! driving the fluent query builder through `build_pipeline` → production `View`,
//! over BOTH leaves (`MemorySource`, `TableSource`) at multiple data scales.
//!
//! Unlike `bench_graph_sources_rs` (a JS-vs-Rust competitive bench on synthetic
//! data), this is an **internal** profile: it sweeps a simple→complex query ladder
//! to find where time goes and how each pattern scales with row count. The
//! Memory-vs-Table delta isolates the SQLite step/statement floor from pure engine
//! (operator/view) cost.
//!
//!   # build the chinook db source once (downloaded SQL dump):
//!   #   curl -fsSL -o /tmp/Chinook_Sqlite.sql \
//!   #     https://github.com/lerocha/chinook-database/releases/download/v1.4.5/Chinook_Sqlite.sql
//!   cargo run -p rindle-sqlite --release --features fast-alloc --example bench_chinook_rs
//!
//! Env knobs:
//!   CHINOOK_SQL=/path/to/Chinook_Sqlite.sql   (default /tmp/Chinook_Sqlite.sql)
//!   CHINOOK_SCALES=1,10,30                     (row-count multipliers for the music closure)
//!
//! Output is one line per measured cell: `backend|pattern|scaleN|ns` (lower better),
//! plus a `# rows` banner per scale and a `# parity` check (Memory vs Table counts).

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::hint::black_box;
use std::rc::Rc;
use std::time::Instant;

use rusqlite::types::{Value as RValue, ValueRef};
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

/// Offset added to scalable id columns per replicated copy. Must exceed the max id
/// in any scalable table (Track ≤ 3503) so copies never collide; exact in f64 up to
/// ~9e15, so safe for scale × 1e7 up to thousands of copies.
const ID_STEP: f64 = 10_000_000.0;

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
// Table metadata
// ---------------------------------------------------------------------------

/// Static metadata for one chinook table we drive: the engine column defs, the PK,
/// secondary indexes (FK columns, mirroring chinook's `IFK_*`), the id columns to
/// offset when replicating for scale, and whether the table replicates at all.
struct TableDef {
    name: &'static str,
    cols: Vec<ColumnDef>,
    pk: Vec<usize>,
    indexes: Vec<Vec<usize>>,
    /// PK + FK columns pointing at *scalable* tables — offset per replicated copy so
    /// intra-copy joins still resolve. FK columns to shared dimension tables (Genre,
    /// MediaType) are NOT listed, so every copy references the same dimension rows.
    scale_cols: Vec<usize>,
    scalable: bool,
    /// `true` ⇒ only built at scale 1 (the invoice closure, not part of the scaled set).
    extra: bool,
}

fn col(name: &str, ty: ValueType, optional: bool) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ty,
        optional,
    }
}
fn num(name: &str, optional: bool) -> ColumnDef {
    col(name, ValueType::Number, optional)
}
fn txt(name: &str, optional: bool) -> ColumnDef {
    col(name, ValueType::String, optional)
}

fn table_defs() -> Vec<TableDef> {
    vec![
        // ---- music closure (scaled) ----
        TableDef {
            name: "Artist",
            cols: vec![num("ArtistId", false), txt("Name", true)],
            pk: vec![0],
            indexes: vec![],
            scale_cols: vec![0],
            scalable: true,
            extra: false,
        },
        TableDef {
            name: "Album",
            cols: vec![
                num("AlbumId", false),
                txt("Title", false),
                num("ArtistId", false),
            ],
            pk: vec![0],
            indexes: vec![vec![2]], // IFK_AlbumArtistId
            scale_cols: vec![0, 2], // AlbumId (PK), ArtistId (FK→Artist, scaled)
            scalable: true,
            extra: false,
        },
        TableDef {
            name: "Track",
            cols: vec![
                num("TrackId", false),
                txt("Name", false),
                num("AlbumId", true),
                num("MediaTypeId", false),
                num("GenreId", true),
                txt("Composer", true),
                num("Milliseconds", false),
                num("Bytes", true),
                num("UnitPrice", false),
            ],
            pk: vec![0],
            indexes: vec![vec![2], vec![4]], // IFK_TrackAlbumId, IFK_TrackGenreId
            scale_cols: vec![0, 2], // TrackId (PK), AlbumId (FK→Album, scaled). Genre/Media shared.
            scalable: true,
            extra: false,
        },
        // ---- shared dimension (never scaled) ----
        TableDef {
            name: "Genre",
            cols: vec![num("GenreId", false), txt("Name", true)],
            pk: vec![0],
            indexes: vec![],
            scale_cols: vec![],
            scalable: false,
            extra: false,
        },
        // ---- invoice closure (scale 1 only) ----
        TableDef {
            name: "Customer",
            cols: vec![
                num("CustomerId", false),
                txt("FirstName", false),
                txt("LastName", false),
                txt("Country", true),
                num("SupportRepId", true),
            ],
            pk: vec![0],
            indexes: vec![],
            scale_cols: vec![],
            scalable: false,
            extra: true,
        },
        TableDef {
            name: "Invoice",
            cols: vec![
                num("InvoiceId", false),
                num("CustomerId", false),
                txt("InvoiceDate", false),
                num("Total", false),
            ],
            pk: vec![0],
            indexes: vec![vec![1]], // IFK_InvoiceCustomerId
            scale_cols: vec![],
            scalable: false,
            extra: true,
        },
        TableDef {
            name: "InvoiceLine",
            cols: vec![
                num("InvoiceLineId", false),
                num("InvoiceId", false),
                num("TrackId", false),
                num("UnitPrice", false),
                num("Quantity", false),
            ],
            pk: vec![0],
            indexes: vec![vec![1], vec![2]], // IFK_InvoiceLineInvoiceId, IFK_InvoiceLineTrackId
            scale_cols: vec![],
            scalable: false,
            extra: true,
        },
    ]
}

fn base_schema(def: &TableDef) -> SourceSchema {
    let names: Vec<&str> = def.cols.iter().map(|c| &*c.name).collect();
    let sort: Vec<(usize, bool)> = def.pk.iter().map(|&c| (c, true)).collect();
    SourceSchema::new(names, def.pk.clone(), sort)
}

// ---------------------------------------------------------------------------
// Loading chinook
// ---------------------------------------------------------------------------

fn q(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Map a SQLite cell to an `OwnedValue` the SAME way the `TableSource` leaf does:
/// a `Number` column is `ColType::Float`, so an INTEGER cell becomes `V::Float`
/// (table_source.rs / sqlite.rs). Loading the MemorySource identically keeps the two
/// leaves bit-for-bit comparable (and numeric query literals are passed as f64).
fn cell_to_owned(vr: ValueRef, ty: ValueType) -> V {
    match ty {
        ValueType::String | ValueType::Json | ValueType::Null => match vr {
            ValueRef::Null => V::Null,
            ValueRef::Text(b) => V::str(std::str::from_utf8(b).unwrap_or("")),
            ValueRef::Integer(i) => V::str(&i.to_string()),
            ValueRef::Real(f) => V::str(&f.to_string()),
            ValueRef::Blob(_) => V::Null,
        },
        // An exact-i64 column (design 226): INTEGER stays exact, like the leaf's
        // `ColType::Int` read arm. (No Chinook column declares it; kept for parity.)
        ValueType::Int => match vr {
            ValueRef::Null => V::Null,
            ValueRef::Integer(i) => V::Int(i),
            ValueRef::Real(f) => V::Float(f),
            ValueRef::Text(b) => V::str(std::str::from_utf8(b).unwrap_or("")),
            ValueRef::Blob(_) => V::Null,
        },
        ValueType::Number | ValueType::Boolean => match vr {
            ValueRef::Null => V::Null,
            ValueRef::Integer(i) => V::Float(i as f64),
            ValueRef::Real(f) => V::Float(f),
            ValueRef::Text(b) => V::str(std::str::from_utf8(b).unwrap_or("")),
            ValueRef::Blob(_) => V::Null,
        },
    }
}

fn read_table(conn: &SqliteConnection, def: &TableDef) -> Vec<OwnedRow> {
    let cols = def
        .cols
        .iter()
        .map(|c| q(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {cols} FROM {}", q(def.name));
    let mut stmt = conn.prepare(&sql).expect("prepare chinook read");
    let rows = stmt
        .query_map([], |row| {
            let mut vals = Vec::with_capacity(def.cols.len());
            for (i, c) in def.cols.iter().enumerate() {
                vals.push(cell_to_owned(row.get_ref(i)?, c.ty));
            }
            Ok(owned_row(vals))
        })
        .expect("query chinook read");
    rows.map(|r| r.expect("chinook row")).collect()
}

fn load_base() -> BTreeMap<&'static str, Vec<OwnedRow>> {
    let path = env::var("CHINOOK_SQL").unwrap_or_else(|_| "/tmp/Chinook_Sqlite.sql".to_string());
    let sql = fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!("read chinook SQL at {path}: {e}\n  set CHINOOK_SQL or download the dump")
    });
    let conn = SqliteConnection::open_in_memory().expect("open chinook scratch db");
    conn.execute_batch(&sql).expect("load chinook SQL");
    let mut out = BTreeMap::new();
    for def in table_defs() {
        out.insert(def.name, read_table(&conn, &def));
    }
    out
}

/// Replicate a base table `scale` times, offsetting its scalable id columns so each
/// copy is an independent island whose intra-copy joins resolve and whose shared-dim
/// FKs (Genre) all point at the single dimension copy.
fn scale_rows(base: &[OwnedRow], def: &TableDef, scale: usize) -> Vec<OwnedRow> {
    let factor = if def.scalable { scale } else { 1 };
    if factor == 1 {
        return base.to_vec();
    }
    let mut out = Vec::with_capacity(base.len() * factor);
    for k in 0..factor {
        let off = k as f64 * ID_STEP;
        for row in base {
            let mut vals: Vec<V> = row.to_value_vec();
            for &c in &def.scale_cols {
                if let V::Float(x) = vals[c] {
                    vals[c] = V::Float(x + off);
                }
            }
            out.push(owned_row(vals));
        }
    }
    out
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

/// Create + bulk-load a fresh in-memory SQLite table (one prepared INSERT in a
/// transaction, indexes built after the load), and wrap it in a `TableSource`.
fn make_table_source(def: &TableDef, rows: &[OwnedRow]) -> TableSource {
    let conn = Rc::new(SqliteConnection::open_in_memory().expect("open table-source db"));
    let coldefs = def
        .cols
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
    conn.execute_batch(&format!("CREATE TABLE {} ({coldefs});", q(def.name)))
        .expect("create table");

    let colnames = def
        .cols
        .iter()
        .map(|c| q(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = vec!["?"; def.cols.len()].join(", ");
    let insert = format!(
        "INSERT INTO {} ({colnames}) VALUES ({placeholders})",
        q(def.name)
    );
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn.prepare(&insert).expect("prepare insert");
        for row in rows {
            let params: Vec<RValue> = row.cells().map(|v| to_rusqlite(&v.to_owned())).collect();
            stmt.execute(params_from_iter(params.iter()))
                .expect("insert chinook row");
        }
    }
    conn.execute_batch("COMMIT").unwrap();

    let pk = def
        .pk
        .iter()
        .map(|&c| q(&def.cols[c].name))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!(
        "CREATE UNIQUE INDEX {} ON {} ({pk});",
        q(&format!("pk_{}", def.name)),
        q(def.name)
    ))
    .expect("create pk index");
    for (k, idx) in def.indexes.iter().enumerate() {
        let cols = idx
            .iter()
            .map(|&c| q(&def.cols[c].name))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!(
            "CREATE INDEX {} ON {} ({cols});",
            q(&format!("ix_{}_{k}", def.name)),
            q(def.name)
        ))
        .expect("create index");
    }

    // CHINOOK_ANALYZE=1 runs ANALYZE after the bulk load so SQLite's planner has
    // column-selectivity stats. This is the controlled A/B for the EXISTS O(n²)
    // finding: without stats the planner mis-picks the non-selective filter index
    // for a two-equality EXISTS child probe (join-key=? AND filter=?) and scans the
    // whole matching set per parent; ANALYZE lets it pick the selective join-key index.
    if env::var("CHINOOK_ANALYZE").as_deref() == Ok("1") {
        conn.execute_batch("ANALYZE").expect("analyze");
    }

    TableSource::new_with_schema(
        conn,
        def.name,
        def.cols.clone(),
        def.pk.clone(),
        base_schema(def),
    )
}

struct Loaded {
    mem: MemorySource,
    tab: TableSource,
    schema: SourceSchema,
    rows: usize,
}

struct Catalog {
    scale: usize,
    tables: BTreeMap<&'static str, Loaded>,
}

fn build_catalog(base: &BTreeMap<&'static str, Vec<OwnedRow>>, scale: usize) -> Catalog {
    let mut tables = BTreeMap::new();
    for def in table_defs() {
        if def.extra && scale != 1 {
            continue; // invoice closure: scale-1 only
        }
        let rows = scale_rows(&base[def.name], &def, scale);
        let schema = base_schema(&def);
        let mem = MemorySource::new(schema.clone().into_schema(), rows.clone());
        let tab = make_table_source(&def, &rows);
        tables.insert(
            def.name,
            Loaded {
                mem,
                tab,
                schema,
                rows: rows.len(),
            },
        );
    }
    Catalog { scale, tables }
}

// ---------------------------------------------------------------------------
// Query ladder (fluent builder + hand-built OR / OR-EXISTS ASTs)
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
    /// Run at every scale (music closure) vs scale-1 only (invoice closure).
    all_scales: bool,
}

/// `Track where (GenreId = 1 OR Milliseconds > 400000)` — top-level OR.
fn or_simple_ast() -> Ast {
    table("Track")
        .where_any(|c| {
            c.r#where("GenreId", 1.0)
                .where_op("Milliseconds", ">", 400_000.0)
        })
        .build()
}

/// `Album where (Title LIKE '%Greatest%' OR EXISTS(Track where AlbumId=.. AND GenreId=1))`
/// — the EXISTS-under-OR lowering path.
fn or_exists_ast() -> Ast {
    table("Album")
        .where_any(|c| {
            c.where_op("Title", "LIKE", "%Greatest%")
                .where_exists(|row| {
                    table("Track")
                        .r#where("AlbumId", row.col("AlbumId"))
                        .r#where("GenreId", 1.0)
                })
        })
        .build()
}

fn hydration_fixtures() -> Vec<Fixture> {
    vec![
        // --- L0: raw hydration ---
        Fixture {
            name: "scan",
            ast: table("Track").build(),
            needs: &["Track"],
            all_scales: true,
        },
        // --- L1: filters ---
        Fixture {
            name: "filter_eq_indexed",
            ast: table("Track").r#where("GenreId", 1.0).build(),
            needs: &["Track"],
            all_scales: true,
        },
        Fixture {
            name: "filter_range",
            ast: table("Track")
                .where_op("Milliseconds", ">", 300_000.0)
                .build(),
            needs: &["Track"],
            all_scales: true,
        },
        Fixture {
            name: "filter_like",
            ast: table("Track").where_op("Name", "LIKE", "%Love%").build(),
            needs: &["Track"],
            all_scales: true,
        },
        // --- L2/L3: order, limit ---
        Fixture {
            name: "order_by_name",
            ast: table("Track").order_by("Name", "asc").build(),
            needs: &["Track"],
            all_scales: true,
        },
        Fixture {
            name: "order_limit_50",
            ast: table("Track")
                .order_by("Milliseconds", "desc")
                .limit(50)
                .build(),
            needs: &["Track"],
            all_scales: true,
        },
        // --- L4: pagination ---
        // Start value is intentionally fractional (1750.5): the engine coerces an
        // *integral* `Lit::Number` to `OwnedValue::Int` (builder::number_to_owned),
        // which would mismatch the uniformly-`Float` numeric data under the start
        // bound's strict `compare_values`. A fractional bound stays `Float` and
        // compares cleanly on both leaves (≡ `TrackId >= 1751`).
        Fixture {
            name: "start_at_mid",
            ast: table("Track")
                .order_by("TrackId", "asc")
                .start_at("TrackId", 1750.5)
                .build(),
            needs: &["Track"],
            all_scales: true,
        },
        // --- L5: one-to-one sub ---
        Fixture {
            name: "sub_1to1_album",
            ast: table("Track")
                .sub_as("album", |r| {
                    table("Album").r#where("AlbumId", r.col("AlbumId"))
                })
                .build(),
            needs: &["Track", "Album"],
            all_scales: true,
        },
        Fixture {
            name: "sub_1to1_genre",
            ast: table("Track")
                .sub_as("genre", |r| {
                    table("Genre").r#where("GenreId", r.col("GenreId"))
                })
                .build(),
            needs: &["Track", "Genre"],
            all_scales: true,
        },
        // --- L6: one-to-many sub ---
        Fixture {
            name: "sub_1many_album_tracks",
            ast: table("Album")
                .sub_as("tracks", |r| {
                    table("Track").r#where("AlbumId", r.col("AlbumId"))
                })
                .build(),
            needs: &["Album", "Track"],
            all_scales: true,
        },
        // --- L7: nested sub (Artist → Album → Track) ---
        Fixture {
            name: "nested_artist_album_track",
            ast: table("Artist")
                .sub_as("albums", |r| {
                    table("Album")
                        .r#where("ArtistId", r.col("ArtistId"))
                        .sub_as("tracks", |r2| {
                            table("Track").r#where("AlbumId", r2.col("AlbumId"))
                        })
                })
                .build(),
            needs: &["Artist", "Album", "Track"],
            all_scales: true,
        },
        // --- L8: EXISTS / NOT EXISTS ---
        // A *bare* top-level EXISTS isn't auto-aliased (the alias pass only fires under
        // and/or), and `apply_related` requires an alias, so name the gate child
        // explicitly. The name only labels the EXISTS gate's storage; it's excluded
        // from the view output.
        Fixture {
            name: "exists_album_has_genre1",
            ast: table("Album")
                .where_exists(|r| {
                    table("Track")
                        .r#where("AlbumId", r.col("AlbumId"))
                        .where_op("GenreId", "=", 1.0)
                        .alias("has_genre1")
                })
                .build(),
            needs: &["Album", "Track"],
            all_scales: true,
        },
        Fixture {
            name: "not_exists_album_no_genre2",
            ast: table("Album")
                .where_not_exists(|r| {
                    table("Track")
                        .r#where("AlbumId", r.col("AlbumId"))
                        .where_op("GenreId", "=", 2.0)
                        .alias("no_genre2")
                })
                .build(),
            needs: &["Album", "Track"],
            all_scales: true,
        },
        // --- L9: OR, OR+EXISTS ---
        Fixture {
            name: "or_simple",
            ast: or_simple_ast(),
            needs: &["Track"],
            all_scales: true,
        },
        Fixture {
            name: "or_exists",
            ast: or_exists_ast(),
            needs: &["Album", "Track"],
            all_scales: true,
        },
        // --- L10: combined realistic mix ---
        Fixture {
            name: "combined_filter_order_limit_sub",
            ast: table("Track")
                .r#where("GenreId", 1.0)
                .order_by("Name", "asc")
                .limit(50)
                .sub_as("album", |r| {
                    table("Album").r#where("AlbumId", r.col("AlbumId"))
                })
                .build(),
            needs: &["Track", "Album"],
            all_scales: true,
        },
        // --- extras (scale 1 only): deeper invoice-closure nesting ---
        Fixture {
            name: "deep_nest_customer_invoice_line_track",
            ast: table("Customer")
                .sub_as("invoices", |r| {
                    table("Invoice")
                        .r#where("CustomerId", r.col("CustomerId"))
                        .sub_as("lines", |r2| {
                            table("InvoiceLine")
                                .r#where("InvoiceId", r2.col("InvoiceId"))
                                .sub_as("track", |r3| {
                                    table("Track").r#where("TrackId", r3.col("TrackId"))
                                })
                        })
                })
                .build(),
            needs: &["Customer", "Invoice", "InvoiceLine", "Track"],
            all_scales: false,
        },
        Fixture {
            name: "not_exists_unsold_tracks",
            ast: table("Track")
                .where_not_exists(|r| {
                    table("InvoiceLine")
                        .r#where("TrackId", r.col("TrackId"))
                        .alias("sold")
                })
                .build(),
            needs: &["Track", "InvoiceLine"],
            all_scales: false,
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
// Push row builders
// ---------------------------------------------------------------------------

/// A Track row in this bench's declared column order. `album`/`genre` accept
/// `None` for SQL NULL. `id` should be unique & outside the scaled id ranges.
fn pushed_track(id: f64, album: Option<f64>, genre: Option<f64>, ms: f64) -> OwnedRow {
    owned_row(vec![
        V::Float(id),
        V::str("Pushed Track"),
        album.map_or(V::Null, V::Float),
        V::Float(1.0),
        genre.map_or(V::Null, V::Float),
        V::Null,
        V::Float(ms),
        V::Null,
        V::Float(0.99),
    ])
}

// ---------------------------------------------------------------------------
// Drivers
// ---------------------------------------------------------------------------

fn fixture_runs(fx: &Fixture, cat: &Catalog) -> bool {
    (fx.all_scales || cat.scale == 1) && fx.needs.iter().all(|t| cat.tables.contains_key(t))
}

/// Verify the two leaves produce the SAME row count for each fixture at this scale —
/// catches value-representation parity bugs before trusting the timings.
fn verify_parity(fixtures: &[Fixture], cat: &Catalog) {
    for fx in fixtures {
        if !fixture_runs(fx, cat) {
            continue;
        }
        let m = hydrate_count(fx, Backend::Memory, cat);
        let t = hydrate_count(fx, Backend::Table, cat);
        let mark = if m == t { "ok" } else { "MISMATCH" };
        println!(
            "# parity scale{} {:<38} memory={m} table={t} {mark}",
            cat.scale, fx.name
        );
    }
}

fn run_hydration(fixtures: &[Fixture], cat: &Catalog) {
    for fx in fixtures {
        if !fixture_runs(fx, cat) {
            continue;
        }
        for backend in [Backend::Memory, Backend::Table] {
            let name = format!("{}|{}|scale{}", backend.label(), fx.name, cat.scale);
            bench(&name, || {
                black_box(hydrate_count(fx, backend, cat));
            });
        }
    }
}

/// The incremental-push ladder: `(push-label, hydration-fixture reused, source table
/// pushed, the reversible row)`. A reversible add+remove of one row measures
/// steady-state IVM cost (not re-hydration). `push_id` sits in copy-0's id gap so it
/// never collides with any scaled copy.
fn push_specs() -> Vec<(&'static str, &'static str, &'static str, OwnedRow)> {
    let id = 9_999_999.0;
    vec![
        (
            "push_scan",
            "scan",
            "Track",
            pushed_track(id, Some(1.0), Some(1.0), 200_000.0),
        ),
        (
            "push_filter_match",
            "filter_eq_indexed",
            "Track",
            pushed_track(id, Some(1.0), Some(1.0), 200_000.0),
        ),
        (
            "push_limit_inside",
            "order_limit_50",
            "Track",
            pushed_track(id, Some(1.0), Some(1.0), 900_000_000.0),
        ),
        (
            "push_limit_outside",
            "order_limit_50",
            "Track",
            pushed_track(id, Some(1.0), Some(1.0), 1.0),
        ),
        (
            "push_sub_child",
            "sub_1many_album_tracks",
            "Track",
            pushed_track(id, Some(1.0), Some(1.0), 200_000.0),
        ),
        (
            "push_nested_child",
            "nested_artist_album_track",
            "Track",
            pushed_track(id, Some(1.0), Some(1.0), 200_000.0),
        ),
        (
            "push_exists_flip",
            "exists_album_has_genre1",
            "Track",
            pushed_track(id, Some(1.0), Some(1.0), 200_000.0),
        ),
    ]
}

/// Build the view once, then time the reversible add+remove for each push spec.
fn run_push(fixtures: &[Fixture], cat: &Catalog) {
    let by_name: BTreeMap<&str, &Fixture> = fixtures.iter().map(|f| (f.name, f)).collect();
    let specs = push_specs();

    for (label, fxname, target, row) in &specs {
        let fx = match by_name.get(fxname) {
            Some(f) if fixture_runs(f, cat) => *f,
            _ => continue,
        };
        for backend in [Backend::Memory, Backend::Table] {
            let built = build_view_graph(fx, backend, cat);
            let src = built.sources[target];
            let name = format!("{}|{}|scale{}", backend.label(), label, cat.scale);
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

// ---------------------------------------------------------------------------
// Flamegraph profiling mode (`--profile <pattern>`, feature = "profile")
// ---------------------------------------------------------------------------

/// Sample `body` under pprof for `secs` seconds and return the report. 4 kHz is a
/// good CPU-sampling rate for sub-ms operations run in a tight loop.
#[cfg(feature = "profile")]
fn profile_loop(label: &str, secs: u64, mut body: impl FnMut()) -> pprof::Report {
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(4000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("build pprof guard");
    let start = Instant::now();
    let mut iters: u64 = 0;
    while start.elapsed().as_secs() < secs {
        body();
        iters += 1;
    }
    eprintln!("# profiled {label}: {iters} iters / {secs}s");
    guard.report().build().expect("build pprof report")
}

#[cfg(feature = "profile")]
fn write_flamegraph(report: &pprof::Report, path: &str) {
    let f = std::fs::File::create(path).expect("create flamegraph file");
    report.flamegraph(f).expect("write flamegraph svg");
    println!("# flamegraph -> {path}");
}

/// Profile one pattern (hydration, or push if the name starts with `push_`) on both
/// leaves, writing `flamegraphs/<pattern>.<backend>.svg`.
#[cfg(feature = "profile")]
fn run_profile(pattern: &str, cat: &Catalog, fixtures: &[Fixture]) {
    std::fs::create_dir_all("flamegraphs").ok();
    let by_name: BTreeMap<&str, &Fixture> = fixtures.iter().map(|f| (f.name, f)).collect();

    if pattern.starts_with("push_") {
        let specs = push_specs();
        let (label, fxname, target, row) = specs
            .iter()
            .find(|(l, _, _, _)| *l == pattern)
            .unwrap_or_else(|| panic!("unknown push pattern {pattern}"));
        let fx = by_name[fxname];
        for backend in [Backend::Memory, Backend::Table] {
            let built = build_view_graph(fx, backend, cat);
            let src = built.sources[*target];
            let report = profile_loop(&format!("{label}.{}", backend.label()), 5, || {
                built.graph.source_push(src, SourceChange::Add(row.clone()));
                built
                    .graph
                    .source_push(src, SourceChange::Remove(row.clone()));
                black_box(count_view_data(&built.graph.view_data(built.view)));
            });
            write_flamegraph(
                &report,
                &format!("flamegraphs/{pattern}.{}.svg", backend.label()),
            );
        }
    } else {
        let fx = *by_name
            .get(pattern)
            .unwrap_or_else(|| panic!("unknown hydration pattern {pattern}"));
        for backend in [Backend::Memory, Backend::Table] {
            let report = profile_loop(&format!("{pattern}.{}", backend.label()), 5, || {
                black_box(hydrate_count(fx, backend, cat));
            });
            write_flamegraph(
                &report,
                &format!("flamegraphs/{pattern}.{}.svg", backend.label()),
            );
        }
    }
}

fn scales() -> Vec<usize> {
    match env::var("CHINOOK_SCALES") {
        Ok(s) => s
            .split(',')
            .filter_map(|x| x.trim().parse::<usize>().ok())
            .collect(),
        Err(_) => vec![1, 10, 30],
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    // `--profile <pattern>`: flamegraph one pattern (both leaves) instead of the matrix.
    if let Some(pos) = args.iter().position(|a| a == "--profile") {
        let pattern = args
            .get(pos + 1)
            .expect("--profile needs a <pattern>")
            .clone();
        #[cfg(feature = "profile")]
        {
            let base = load_base();
            let fixtures = hydration_fixtures();
            let scale = scales().first().copied().unwrap_or(1);
            let cat = build_catalog(&base, scale);
            eprintln!("# profiling {pattern} at scale {scale}");
            run_profile(&pattern, &cat, &fixtures);
            return;
        }
        #[cfg(not(feature = "profile"))]
        {
            eprintln!(
                "--profile ({pattern}) requires `--features profile` (pprof). Rebuild with it."
            );
            std::process::exit(2);
        }
    }

    let base = load_base();
    let fixtures = hydration_fixtures();

    print!("# base rows:");
    for (name, rows) in &base {
        print!(" {name}={}", rows.len());
    }
    println!();

    for scale in scales() {
        let cat = build_catalog(&base, scale);
        let total: usize = cat.tables.values().map(|l| l.rows).sum();
        println!(
            "# scale {scale}: {} source rows across {} tables",
            total,
            cat.tables.len()
        );
        verify_parity(&fixtures, &cat);
        run_hydration(&fixtures, &cat);
        run_push(&fixtures, &cat);
    }
}
