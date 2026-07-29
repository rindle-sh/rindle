//! The bundled **chinook** fixture for the parity harness.
//!
//! Loads a fixed subset of the canonical chinook tables (the same columns
//! `bench_chinook_rs` drives) from the SQL dump into the [`TableSpec`]s the IVM
//! consumes, plus a matching [`Catalog`] that declares each relationship's
//! cardinality for the `rindle-d2s` SQLite compiler. Numbers are loaded as
//! [`V::Float`] *exactly* as the `TableSource`/`MemorySource` leaves do, so the IVM
//! view and the SQL oracle compare equal under the harness's `canon`.
//!
//! The dump path is `$CHINOOK_SQL` (default `/tmp/Chinook_Sqlite.sql`); download it
//! once with:
//!
//! ```text
//! curl -fsSL -o /tmp/Chinook_Sqlite.sql \
//!   https://github.com/lerocha/chinook-database/releases/download/v1.4.5/Chinook_Sqlite.sql
//! ```
//!
//! Tests call [`sources_or_skip`] and quietly no-op when the dump is absent, so a CI
//! lane without the dataset stays green.

use std::path::Path;
use std::sync::OnceLock;

use rindle::testkit::TableSpec;
use rindle::{owned_row, OwnedRow, OwnedValue as V, SourceSchema, ValueType};
use rindle_d2s::{Cardinality, Catalog};
use rusqlite::types::ValueRef;
use rusqlite::Connection;

// ── table metadata ───────────────────────────────────────────────────────────────

struct ColDef {
    name: &'static str,
    ty: ValueType,
}

struct TableDef {
    name: &'static str,
    cols: Vec<ColDef>,
    /// Primary-key column indices into `cols`.
    pk: Vec<usize>,
}

fn num(name: &'static str) -> ColDef {
    ColDef {
        name,
        ty: ValueType::Number,
    }
}
fn txt(name: &'static str) -> ColDef {
    ColDef {
        name,
        ty: ValueType::String,
    }
}

/// The chinook subset we model — identical column choice/order to `bench_chinook_rs`
/// (so a `track(..)` push row lines up with the seeded schema position-for-position).
fn defs() -> Vec<TableDef> {
    vec![
        TableDef {
            name: "Artist",
            cols: vec![num("ArtistId"), txt("Name")],
            pk: vec![0],
        },
        TableDef {
            name: "Album",
            cols: vec![num("AlbumId"), txt("Title"), num("ArtistId")],
            pk: vec![0],
        },
        TableDef {
            name: "Track",
            cols: vec![
                num("TrackId"),
                txt("Name"),
                num("AlbumId"),
                num("MediaTypeId"),
                num("GenreId"),
                txt("Composer"),
                num("Milliseconds"),
                num("Bytes"),
                num("UnitPrice"),
            ],
            pk: vec![0],
        },
        TableDef {
            name: "Genre",
            cols: vec![num("GenreId"), txt("Name")],
            pk: vec![0],
        },
        TableDef {
            name: "Customer",
            cols: vec![
                num("CustomerId"),
                txt("FirstName"),
                txt("LastName"),
                txt("Country"),
                num("SupportRepId"),
            ],
            pk: vec![0],
        },
        TableDef {
            name: "Invoice",
            cols: vec![
                num("InvoiceId"),
                num("CustomerId"),
                txt("InvoiceDate"),
                num("Total"),
            ],
            pk: vec![0],
        },
        TableDef {
            name: "InvoiceLine",
            cols: vec![
                num("InvoiceLineId"),
                num("InvoiceId"),
                num("TrackId"),
                num("UnitPrice"),
                num("Quantity"),
            ],
            pk: vec![0],
        },
        // Self-referential (ReportsTo → EmployeeId) — for self-join EXISTS/nesting.
        TableDef {
            name: "Employee",
            cols: vec![
                num("EmployeeId"),
                txt("LastName"),
                txt("FirstName"),
                txt("Title"),
                num("ReportsTo"),
                txt("City"),
                txt("Country"),
            ],
            pk: vec![0],
        },
        TableDef {
            name: "MediaType",
            cols: vec![num("MediaTypeId"), txt("Name")],
            pk: vec![0],
        },
        TableDef {
            name: "Playlist",
            cols: vec![num("PlaylistId"), txt("Name")],
            pk: vec![0],
        },
        // Composite-PK junction. We do NOT hide it: a Playlist↔Track many-to-many is
        // expressed as ordinary two-level nesting (Playlist → playlistTracks → track),
        // so this level appears in the output tree.
        TableDef {
            name: "PlaylistTrack",
            cols: vec![num("PlaylistId"), num("TrackId")],
            pk: vec![0, 1],
        },
    ]
}

fn schema_for(def: &TableDef) -> SourceSchema {
    let names: Vec<&str> = def.cols.iter().map(|c| c.name).collect();
    let sort: Vec<(usize, bool)> = def.pk.iter().map(|&c| (c, true)).collect();
    SourceSchema::new(names, def.pk.clone(), sort)
}

// ── loading the dump ─────────────────────────────────────────────────────────────

fn dump_path() -> String {
    std::env::var("CHINOOK_SQL").unwrap_or_else(|_| "/tmp/Chinook_Sqlite.sql".to_string())
}

/// Whether the chinook dump is present (so a test can skip rather than fail when the
/// dataset has not been downloaded).
pub fn available() -> bool {
    Path::new(&dump_path()).exists()
}

/// Map a SQLite cell to an `OwnedValue` the SAME way the `TableSource` leaf does — a
/// `Number` column reads an INTEGER cell as `V::Float`, so the in-memory and SQLite
/// sides stay bit-for-bit comparable (mirrors `bench_chinook_rs::cell_to_owned`).
fn cell_to_owned(vr: ValueRef, ty: ValueType) -> V {
    match ty {
        ValueType::Number | ValueType::Boolean => match vr {
            ValueRef::Null => V::Null,
            ValueRef::Integer(i) => V::Float(i as f64),
            ValueRef::Real(f) => V::Float(f),
            ValueRef::Text(b) => V::str(std::str::from_utf8(b).unwrap_or("")),
            ValueRef::Blob(_) => V::Null,
        },
        _ => match vr {
            ValueRef::Null => V::Null,
            ValueRef::Text(b) => V::str(std::str::from_utf8(b).unwrap_or("")),
            ValueRef::Integer(i) => V::str(&i.to_string()),
            ValueRef::Real(f) => V::str(&f.to_string()),
            ValueRef::Blob(_) => V::Null,
        },
    }
}

fn load() -> Vec<TableSpec> {
    let sql = std::fs::read_to_string(dump_path())
        .unwrap_or_else(|e| panic!("read chinook dump at {}: {e}", dump_path()));
    let conn = Connection::open_in_memory().expect("open chinook scratch db");
    conn.execute_batch(&sql).expect("load chinook dump");

    defs()
        .iter()
        .map(|def| {
            let cols = def
                .cols
                .iter()
                .map(|c| quote(c.name))
                .collect::<Vec<_>>()
                .join(", ");
            let select = format!("SELECT {cols} FROM {}", quote(def.name));
            let mut stmt = conn.prepare(&select).expect("prepare chinook read");
            let rows: Vec<Vec<V>> = stmt
                .query_map([], |row| {
                    let mut vals = Vec::with_capacity(def.cols.len());
                    for (i, c) in def.cols.iter().enumerate() {
                        vals.push(cell_to_owned(row.get_ref(i)?, c.ty));
                    }
                    Ok(vals)
                })
                .expect("query chinook read")
                .map(|r| r.expect("chinook row"))
                .collect();
            TableSpec::new(def.name, schema_for(def), rows)
        })
        .collect()
}

// The dataset is large and immutable; load + parse it once per test binary and hand
// out cheap clones (rows are `Arc<[OwnedValue]>`). `OnceLock` is `Sync`, so this is
// safe across the parallel test threads.
static CACHE: OnceLock<Vec<TableSpec>> = OnceLock::new();

/// The chinook tables as IVM [`TableSpec`]s (panics if the dump is missing — prefer
/// [`sources_or_skip`] in tests that should tolerate its absence).
pub fn sources() -> Vec<TableSpec> {
    CACHE.get_or_init(load).clone()
}

/// The chinook sources, or `None` (with a skip note) when the dump is not present.
pub fn sources_or_skip() -> Option<Vec<TableSpec>> {
    if available() {
        Some(sources())
    } else {
        eprintln!(
            "skip: chinook dump not found at {} \
             (set CHINOOK_SQL or download chinook v1.4.5)",
            dump_path()
        );
        None
    }
}

/// The [`Catalog`] for the chinook subset: every modeled table plus the relationship
/// cardinalities the queries reference (relationship *name* → `one`/`many`). The
/// correlation keys themselves live in each query's AST, not here.
///
/// **Every relationship is declared `Many`** — the production `View` materializes
/// every relationship as a plural array (`view_schema` is plural at every level;
/// relationship-level `.one()` singular projection is a separate, not-yet-implemented
/// feature), so a `Many` oracle is what matches the View. A relationship that is
/// *semantically* one-per-parent (e.g. `Album → artist`, the junction's inner
/// `PlaylistTrack → track`) still materializes as a one-element array today, so it is
/// declared `Many` here too; the true cardinality is noted in a trailing comment for
/// when singular projection lands.
pub fn catalog() -> Catalog {
    let mut c = Catalog::new();
    for def in defs() {
        let cols: Vec<&str> = def.cols.iter().map(|col| col.name).collect();
        let pk: Vec<&str> = def.pk.iter().map(|&i| def.cols[i].name).collect();
        c.table(def.name, &cols, &pk);
    }
    use Cardinality::Many;
    c.relationship("Artist", "albums", Many);
    c.relationship("Album", "tracks", Many);
    c.relationship("Album", "artist", Many); // semantically one
    c.relationship("Track", "album", Many); // semantically one
    c.relationship("Track", "genre", Many); // semantically one
    c.relationship("Genre", "tracks", Many);
    c.relationship("Customer", "invoices", Many);
    c.relationship("Invoice", "lines", Many);
    c.relationship("Invoice", "customer", Many); // semantically one
    c.relationship("InvoiceLine", "track", Many); // semantically one
    c.relationship("Track", "mediaType", Many); // semantically one
    c.relationship("MediaType", "tracks", Many);
    // self-referential employee hierarchy
    c.relationship("Employee", "manager", Many); // semantically one (ReportsTo → EmployeeId)
    c.relationship("Employee", "reports", Many);
    c.relationship("Customer", "supportRep", Many); // semantically one (SupportRepId → EmployeeId)
                                                    // junction (NOT hidden): plain two-level nesting through PlaylistTrack
    c.relationship("Playlist", "playlistTracks", Many);
    c.relationship("PlaylistTrack", "track", Many); // semantically one
    c.relationship("PlaylistTrack", "playlist", Many); // semantically one
    c.relationship("Track", "playlistTracks", Many);
    c
}

// ── row builders (for pushes) ────────────────────────────────────────────────────

/// Look up an existing row by its single integer primary key — the exact, complete
/// row to feed a `Remove`/`Edit` push (the engine matches by key; the oracle deletes
/// by key, so the non-key columns must round-trip faithfully).
pub fn find(table: &str, id: i64) -> Option<OwnedRow> {
    let srcs = sources();
    let spec = srcs.iter().find(|s| s.name == table)?;
    let pk = spec.schema.primary_key[0];
    spec.rows
        .iter()
        .find(|r| matches!(r.col(pk), rindle::value::Value::Float(f) if f == id as f64))
        .cloned()
}

pub fn artist(id: i64, name: &str) -> OwnedRow {
    owned_row(vec![V::Float(id as f64), V::str(name)])
}

pub fn album(id: i64, title: &str, artist_id: i64) -> OwnedRow {
    owned_row(vec![
        V::Float(id as f64),
        V::str(title),
        V::Float(artist_id as f64),
    ])
}

#[allow(clippy::too_many_arguments)]
pub fn track(
    id: i64,
    name: &str,
    album_id: i64,
    media_type_id: i64,
    genre_id: i64,
    composer: Option<&str>,
    milliseconds: i64,
    bytes: i64,
    unit_price: f64,
) -> OwnedRow {
    owned_row(vec![
        V::Float(id as f64),
        V::str(name),
        V::Float(album_id as f64),
        V::Float(media_type_id as f64),
        V::Float(genre_id as f64),
        composer.map(V::str).unwrap_or(V::Null),
        V::Float(milliseconds as f64),
        V::Float(bytes as f64),
        V::Float(unit_price),
    ])
}

pub fn genre(id: i64, name: &str) -> OwnedRow {
    owned_row(vec![V::Float(id as f64), V::str(name)])
}

pub fn employee(
    id: i64,
    last: &str,
    first: &str,
    title: Option<&str>,
    reports_to: Option<i64>,
    city: Option<&str>,
    country: Option<&str>,
) -> OwnedRow {
    owned_row(vec![
        V::Float(id as f64),
        V::str(last),
        V::str(first),
        title.map(V::str).unwrap_or(V::Null),
        reports_to.map(|r| V::Float(r as f64)).unwrap_or(V::Null),
        city.map(V::str).unwrap_or(V::Null),
        country.map(V::str).unwrap_or(V::Null),
    ])
}

pub fn media_type(id: i64, name: &str) -> OwnedRow {
    owned_row(vec![V::Float(id as f64), V::str(name)])
}

pub fn playlist(id: i64, name: &str) -> OwnedRow {
    owned_row(vec![V::Float(id as f64), V::str(name)])
}

pub fn playlist_track(playlist_id: i64, track_id: i64) -> OwnedRow {
    owned_row(vec![
        V::Float(playlist_id as f64),
        V::Float(track_id as f64),
    ])
}

pub fn customer(
    id: i64,
    first: &str,
    last: &str,
    country: Option<&str>,
    support_rep: Option<i64>,
) -> OwnedRow {
    owned_row(vec![
        V::Float(id as f64),
        V::str(first),
        V::str(last),
        country.map(V::str).unwrap_or(V::Null),
        support_rep.map(|r| V::Float(r as f64)).unwrap_or(V::Null),
    ])
}

pub fn invoice(id: i64, customer_id: i64, date: &str, total: f64) -> OwnedRow {
    owned_row(vec![
        V::Float(id as f64),
        V::Float(customer_id as f64),
        V::str(date),
        V::Float(total),
    ])
}

pub fn invoice_line(
    id: i64,
    invoice_id: i64,
    track_id: i64,
    unit_price: f64,
    qty: i64,
) -> OwnedRow {
    owned_row(vec![
        V::Float(id as f64),
        V::Float(invoice_id as f64),
        V::Float(track_id as f64),
        V::Float(unit_price),
        V::Float(qty as f64),
    ])
}

/// The [`SourceSchema`] for one modeled table (columns + PK + PK-sort), exposed so a
/// synthetic fixture (`harness::mini`) can reuse the chinook shape with its own rows.
pub fn table_schema(name: &str) -> SourceSchema {
    defs()
        .iter()
        .find(|d| d.name == name)
        .map(schema_for)
        .unwrap_or_else(|| panic!("unknown chinook table {name:?}"))
}

/// Double-quote a SQL identifier, doubling any embedded `"`.
fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
