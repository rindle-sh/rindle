//! A small, hand-authored fixture on the **chinook schema** for the combinatorial
//! generator (`tests/chinook_generated.rs`). ~40 FK-consistent rows, deliberately
//! shaped to exercise transitions the generator relies on:
//!
//! * an artist with **no albums** (Gamma / id 3), a genre with **no tracks**
//!   (Empty / id 4), an **empty playlist** (P-two / id 2), a customer with **no
//!   invoices** (id 3) — for EXISTS/NOT-EXISTS flips and empty children;
//! * albums with **exactly 2–3 tracks** (10→3, 11→2, 20→3) — for limit boundaries;
//! * **NULL**s (some `Composer`, customer 3's `Country`, the boss's `ReportsTo`) —
//!   for IS NULL / null ordering;
//! * a **self-referential** employee chain (1←2←3) and a **junction** (PlaylistTrack).
//!
//! Reuses [`crate::chinook::table_schema`] and the `chinook` row builders, so the rows
//! line up with the same `Catalog` ([`crate::chinook::catalog`]) the SQL compiler uses.

use rindle::testkit::TableSpec;
use rindle::OwnedRow;

use crate::chinook::{
    album, artist, customer, employee, genre, invoice, invoice_line, media_type, playlist,
    playlist_track, table_schema, track,
};

fn spec(name: &str, rows: Vec<OwnedRow>) -> TableSpec {
    TableSpec {
        name: name.to_string(),
        schema: table_schema(name),
        rows,
    }
}

// ── exact-row lookup (for Remove/Edit pushes) ─────────────────────────────────────
//
// A `Remove`/`Edit{old}` push must carry the row *exactly* as seeded — the engine
// matches by key, the oracle deletes by key, but the non-key columns still have to
// round-trip. Rather than hand-copy the magic numbers from `sources()` (and risk
// drift when the fixture is edited), pull the live seeded row straight back out.

/// The exact seeded row of `table` whose single integer PK equals `id`.
pub fn find(table: &str, id: i64) -> OwnedRow {
    let srcs = sources();
    let spec = srcs
        .iter()
        .find(|s| s.name == table)
        .unwrap_or_else(|| panic!("mini::find: unknown table {table:?}"));
    let pk = spec.schema.primary_key[0];
    spec.rows
        .iter()
        .find(|r| matches!(r.col(pk), rindle::value::Value::Float(f) if f == id as f64))
        .cloned()
        .unwrap_or_else(|| panic!("mini::find: no {table} row with PK {id}"))
}

/// The exact seeded row of a composite-PK `table` (e.g. `PlaylistTrack`) whose two
/// PK columns equal `(k0, k1)`, in PK-column order.
pub fn find2(table: &str, k0: i64, k1: i64) -> OwnedRow {
    let srcs = sources();
    let spec = srcs
        .iter()
        .find(|s| s.name == table)
        .unwrap_or_else(|| panic!("mini::find2: unknown table {table:?}"));
    let (c0, c1) = (spec.schema.primary_key[0], spec.schema.primary_key[1]);
    spec.rows
        .iter()
        .find(|r| {
            matches!(r.col(c0), rindle::value::Value::Float(f) if f == k0 as f64)
                && matches!(r.col(c1), rindle::value::Value::Float(f) if f == k1 as f64)
        })
        .cloned()
        .unwrap_or_else(|| panic!("mini::find2: no {table} row with PK ({k0}, {k1})"))
}

/// The mini dataset as IVM [`TableSpec`]s (all 11 modeled tables, so any shape the
/// generator builds resolves). Pair with [`crate::chinook::catalog`].
pub fn sources() -> Vec<TableSpec> {
    vec![
        spec(
            "Artist",
            vec![
                artist(1, "Alpha"),
                artist(2, "Beta"),
                artist(3, "Gamma"), // no albums
            ],
        ),
        spec(
            "Album",
            vec![
                album(10, "A-one", 1),
                album(11, "A-two", 1),
                album(20, "B-one", 2),
            ],
        ),
        spec(
            "Genre",
            vec![
                genre(1, "Rock"),
                genre(2, "Jazz"),
                genre(3, "Metal"),
                genre(4, "Empty"), // no tracks
            ],
        ),
        spec(
            "MediaType",
            vec![media_type(1, "MP3"), media_type(2, "AAC")],
        ),
        spec(
            "Track",
            vec![
                // (id, name, album, media, genre, composer, ms, bytes, price)
                track(
                    100,
                    "t-a",
                    10,
                    1,
                    1,
                    Some("Smith"),
                    200_000,
                    2_000_000,
                    0.99,
                ),
                track(101, "t-b", 10, 1, 1, None, 100_000, 1_000_000, 0.99),
                track(
                    102,
                    "t-c",
                    10,
                    2,
                    2,
                    Some("Jones"),
                    300_000,
                    3_000_000,
                    1.29,
                ),
                track(
                    103,
                    "t-d",
                    11,
                    1,
                    3,
                    Some("Smith"),
                    150_000,
                    1_500_000,
                    0.99,
                ),
                track(104, "t-e", 11, 2, 1, None, 250_000, 2_500_000, 0.99),
                track(105, "t-f", 20, 1, 2, Some("Lee"), 50_000, 500_000, 0.99),
                track(
                    106,
                    "t-g",
                    20,
                    1,
                    3,
                    Some("Smith"),
                    400_000,
                    4_000_000,
                    1.29,
                ),
                track(107, "t-h", 20, 2, 1, None, 350_000, 3_500_000, 0.99),
            ],
        ),
        spec("Playlist", vec![playlist(1, "P-one"), playlist(2, "P-two")]),
        spec(
            "PlaylistTrack",
            vec![
                playlist_track(1, 100),
                playlist_track(1, 101),
                playlist_track(1, 102),
            ],
        ),
        spec(
            "Employee",
            vec![
                employee(
                    1,
                    "Adams",
                    "Andrew",
                    Some("GM"),
                    None,
                    Some("NYC"),
                    Some("USA"),
                ),
                employee(
                    2,
                    "Mills",
                    "Margaret",
                    Some("Sales"),
                    Some(1),
                    Some("LA"),
                    Some("USA"),
                ),
                employee(
                    3,
                    "Park",
                    "Peter",
                    Some("Support"),
                    Some(2),
                    Some("SF"),
                    Some("USA"),
                ),
            ],
        ),
        spec(
            "Customer",
            vec![
                customer(1, "Ann", "Aaronson", Some("USA"), Some(2)),
                customer(2, "Bo", "Becker", Some("UK"), Some(2)),
                customer(3, "Cy", "Carter", None, None), // no invoices, null country
            ],
        ),
        spec(
            "Invoice",
            vec![
                invoice(1000, 1, "2020-01-01", 10.0),
                invoice(1001, 1, "2020-02-01", 5.0),
                invoice(1002, 2, "2020-03-01", 7.0),
            ],
        ),
        spec(
            "InvoiceLine",
            vec![
                invoice_line(5000, 1000, 100, 0.99, 1),
                invoice_line(5001, 1000, 101, 0.99, 2),
                invoice_line(5002, 1002, 105, 0.99, 1),
            ],
        ),
    ]
}
