//! **Cross-table push interleaving** parity.
//!
//! The push runner applies a batch one change at a time, flushing the `View` after
//! *each* push (mirroring the JS per-commit flush — see `testkit::run_push_test_ast_view`).
//! So a batch that interleaves changes to *different* source tables genuinely exercises
//! the engine's incremental maintenance under out-of-order and cross-join arrival —
//! a child landing before its parent, a parent removed while its children remain, two
//! reparents at different join levels in the same batch, an EXISTS gate opening on one
//! table while another closes on a second. The SQLite oracle recomputes from the final
//! state, so whatever interleaving the engine takes, the maintained tree must converge
//! to the declarative answer.
//!
//! Runs on the small synthetic [`mini`] fixture (chinook schema, ~40 controlled rows),
//! so every transition the pushes depend on is authored, not hunted for, and failure
//! diffs stay tiny. EXISTS-bearing cases route through [`assert_flip_invariant_push`]
//! for 2^k flip coverage; the rest through [`assert_push_parity`].

mod harness;

use harness::{assert_flip_invariant_push, assert_push_parity, chinook, mini};
use rindle::{owned_row, table, OwnedRow, OwnedValue as V, SourceChange};

type Push = (&'static str, SourceChange);

// Column indices for the single-field edits below.
const T_ALBUM: usize = 2; // Track.AlbumId
const AL_ARTIST: usize = 2; // Album.ArtistId

/// `SourceChange::Edit` from an exact seeded row, replacing one column.
fn edit(old: OwnedRow, idx: usize, v: V) -> SourceChange {
    let mut vals: Vec<V> = old.to_value_vec();
    vals[idx] = v;
    SourceChange::Edit {
        old,
        row: owned_row(vals),
    }
}

// ── query shapes reused across the cases ──────────────────────────────────────────

/// Artist → albums → tracks, every level PK-ordered (deterministic for positional
/// JSON comparison).
fn artist_albums_tracks() -> rindle::Ast {
    table("Artist")
        .sub_as("albums", |ar| {
            table("Album")
                .r#where("ArtistId", ar.col("ArtistId"))
                .sub_as("tracks", |al| {
                    table("Track")
                        .r#where("AlbumId", al.col("AlbumId"))
                        .order_by("TrackId", "asc")
                })
                .order_by("AlbumId", "asc")
        })
        .order_by("ArtistId", "asc")
        .build()
}

/// Customer → invoices → lines, every level PK-ordered.
fn customer_invoices_lines() -> rindle::Ast {
    table("Customer")
        .sub_as("invoices", |c| {
            table("Invoice")
                .r#where("CustomerId", c.col("CustomerId"))
                .sub_as("lines", |i| {
                    table("InvoiceLine")
                        .r#where("InvoiceId", i.col("InvoiceId"))
                        .order_by("InvoiceLineId", "asc")
                })
                .order_by("InvoiceId", "asc")
        })
        .order_by("CustomerId", "asc")
        .build()
}

/// Playlist → playlistTracks → track (junction; three tables), PK-ordered.
fn playlist_junction() -> rindle::Ast {
    table("Playlist")
        .sub_as("playlistTracks", |p| {
            table("PlaylistTrack")
                .r#where("PlaylistId", p.col("PlaylistId"))
                .sub_as("track", |pt| {
                    table("Track").r#where("TrackId", pt.col("TrackId"))
                })
                .order_by("TrackId", "asc")
        })
        .order_by("PlaylistId", "asc")
        .build()
}

// ── out-of-order arrival across a two-level join ──────────────────────────────────

#[test]
fn child_track_before_parent_album() {
    let src = mini::sources();
    // Gamma (artist 3) has no albums. A brand-new track lands FIRST (orphaned — its
    // album isn't in view yet), then its album arrives under Gamma. The maintained
    // tree must end with Gamma → album 40 → track 300.
    let pushes = vec![
        (
            "Track",
            SourceChange::Add(chinook::track(
                300, "t-new", 40, 1, 1, None, 120_000, 1_200_000, 0.99,
            )),
        ),
        ("Album", SourceChange::Add(chinook::album(40, "G-new", 3))),
    ];
    assert_push_parity(&src, &chinook::catalog(), &artist_albums_tracks(), pushes);
}

#[test]
fn parent_album_before_child_track() {
    let src = mini::sources();
    // Same net mutation as `child_track_before_parent_album`, reversed order: the album
    // arrives first (empty), then the track fills it. Both orders must converge.
    let pushes = vec![
        ("Album", SourceChange::Add(chinook::album(40, "G-new", 3))),
        (
            "Track",
            SourceChange::Add(chinook::track(
                300, "t-new", 40, 1, 1, None, 120_000, 1_200_000, 0.99,
            )),
        ),
    ];
    assert_push_parity(&src, &chinook::catalog(), &artist_albums_tracks(), pushes);
}

// ── parent/child removal order across a two-level join ────────────────────────────

#[test]
fn remove_parent_invoice_then_its_lines() {
    let src = mini::sources();
    // Invoice 1000 (customer 1) owns lines 5000 & 5001. Drop the parent invoice first
    // (orphaning its lines in the view), then drop the lines. Customer 1 keeps the
    // line-less invoice 1001.
    let pushes = vec![
        ("Invoice", SourceChange::Remove(mini::find("Invoice", 1000))),
        (
            "InvoiceLine",
            SourceChange::Remove(mini::find("InvoiceLine", 5000)),
        ),
        (
            "InvoiceLine",
            SourceChange::Remove(mini::find("InvoiceLine", 5001)),
        ),
    ];
    assert_push_parity(
        &src,
        &chinook::catalog(),
        &customer_invoices_lines(),
        pushes,
    );
}

#[test]
fn remove_child_lines_then_parent_invoice() {
    let src = mini::sources();
    // Reverse removal order: lines first, then the now-empty invoice. Must converge to
    // the same tree as `remove_parent_invoice_then_its_lines`.
    let pushes = vec![
        (
            "InvoiceLine",
            SourceChange::Remove(mini::find("InvoiceLine", 5000)),
        ),
        (
            "InvoiceLine",
            SourceChange::Remove(mini::find("InvoiceLine", 5001)),
        ),
        ("Invoice", SourceChange::Remove(mini::find("Invoice", 1000))),
    ];
    assert_push_parity(
        &src,
        &chinook::catalog(),
        &customer_invoices_lines(),
        pushes,
    );
}

// ── simultaneous reparents at two different join levels ───────────────────────────

#[test]
fn reparent_track_and_album_same_batch() {
    let src = mini::sources();
    // Two reparents in one batch, at different join levels and on different tables:
    //   * Track 105 moves album 20 → 10  (a Track-level reparent), and
    //   * Album 20 moves artist 2 → 1     (an Album-level reparent).
    // After both: artist 1 owns albums 10 (now +105), 11, 20 (tracks 106/107); artist
    // 2 owns nothing. The join must settle both moves regardless of interleaving.
    let pushes = vec![
        (
            "Track",
            edit(mini::find("Track", 105), T_ALBUM, V::Float(10.0)),
        ),
        (
            "Album",
            edit(mini::find("Album", 20), AL_ARTIST, V::Float(1.0)),
        ),
    ];
    assert_push_parity(&src, &chinook::catalog(), &artist_albums_tracks(), pushes);
}

// ── a sibling fan whose two relationships both change in one batch ─────────────────

#[test]
fn sibling_fan_album_artist_and_tracks() {
    let src = mini::sources();
    // Album → { tracks, artist } sibling fan. In one batch, album 20's *both* children
    // change on different tables: its `artist` rel flips (ArtistId 2 → 3) while its
    // `tracks` set gains 306 and loses 105.
    let ast = table("Album")
        .sub_as("tracks", |a| {
            table("Track")
                .r#where("AlbumId", a.col("AlbumId"))
                .order_by("TrackId", "asc")
        })
        .sub_as("artist", |a| {
            table("Artist")
                .r#where("ArtistId", a.col("ArtistId"))
                .order_by("ArtistId", "asc")
        })
        .order_by("AlbumId", "asc")
        .build();
    let pushes = vec![
        (
            "Album",
            edit(mini::find("Album", 20), AL_ARTIST, V::Float(3.0)),
        ),
        (
            "Track",
            SourceChange::Add(chinook::track(
                306, "t-add", 20, 1, 1, None, 222_000, 2_220_000, 0.99,
            )),
        ),
        ("Track", SourceChange::Remove(mini::find("Track", 105))),
    ];
    assert_push_parity(&src, &chinook::catalog(), &ast, pushes);
}

// ── junction (three tables) with out-of-order and mixed add/remove ────────────────

#[test]
fn junction_link_before_leaf_track() {
    let src = mini::sources();
    // Link playlist 2 → track 307 BEFORE track 307 exists: the junction row lands first
    // (its `track` child empty), then the leaf track arrives and must attach.
    let pushes = vec![
        (
            "PlaylistTrack",
            SourceChange::Add(chinook::playlist_track(2, 307)),
        ),
        (
            "Track",
            SourceChange::Add(chinook::track(
                307, "t-pl", 10, 1, 1, None, 130_000, 1_300_000, 0.99,
            )),
        ),
    ];
    assert_push_parity(&src, &chinook::catalog(), &playlist_junction(), pushes);
}

#[test]
fn junction_mixed_add_and_remove_three_tables() {
    let src = mini::sources();
    // One batch touching Track + PlaylistTrack: a new track is created and linked into
    // the empty playlist 2; existing track 105 is linked into playlist 1; and the
    // existing (1,100) link is removed — all interleaved.
    let pushes = vec![
        (
            "Track",
            SourceChange::Add(chinook::track(
                308, "t-mix", 10, 1, 1, None, 140_000, 1_400_000, 0.99,
            )),
        ),
        (
            "PlaylistTrack",
            SourceChange::Add(chinook::playlist_track(2, 308)),
        ),
        (
            "PlaylistTrack",
            SourceChange::Add(chinook::playlist_track(1, 105)),
        ),
        (
            "PlaylistTrack",
            SourceChange::Remove(mini::find2("PlaylistTrack", 1, 100)),
        ),
    ];
    assert_push_parity(&src, &chinook::catalog(), &playlist_junction(), pushes);
}

// ── EXISTS gates opening on one table while closing on another (interleaved) ──────

#[test]
fn album_exists_gate_open_and_close_two_tables() {
    let src = mini::sources();
    // Album WHERE EXISTS(its tracks). One batch, two tables, two opposite gate flips:
    //   * a fresh empty album 41 is added (gate closed) then a track fills it (opens), and
    //   * album 11's only tracks (103, 104) are both removed (closes its gate).
    let ast = table("Album")
        .where_exists(|a| {
            table("Track")
                .alias("hasTrack")
                .r#where("AlbumId", a.col("AlbumId"))
        })
        .order_by("AlbumId", "asc")
        .build();
    let pushes = vec![
        ("Album", SourceChange::Add(chinook::album(41, "A-empty", 1))),
        (
            "Track",
            SourceChange::Add(chinook::track(
                309, "t-41", 41, 1, 1, None, 160_000, 1_600_000, 0.99,
            )),
        ),
        ("Track", SourceChange::Remove(mini::find("Track", 103))),
        ("Track", SourceChange::Remove(mini::find("Track", 104))),
    ];
    assert_flip_invariant_push(&src, &chinook::catalog(), &ast, pushes);
}

#[test]
fn customer_exists_gate_two_tables() {
    let src = mini::sources();
    // Customer WHERE EXISTS(an invoice). Interleaved across Customer + Invoice:
    //   * a fresh customer 4 is added (no invoices → out) then given an invoice (enters), and
    //   * customer 2's only invoice (1002) is removed (customer 2 leaves).
    let ast = table("Customer")
        .where_exists(|c| {
            table("Invoice")
                .alias("hasInvoice")
                .r#where("CustomerId", c.col("CustomerId"))
        })
        .order_by("CustomerId", "asc")
        .build();
    let pushes: Vec<Push> = vec![
        (
            "Customer",
            SourceChange::Add(chinook::customer(4, "Dee", "Dorn", Some("USA"), None)),
        ),
        (
            "Invoice",
            SourceChange::Add(chinook::invoice(2000, 4, "2020-09-09", 4.0)),
        ),
        ("Invoice", SourceChange::Remove(mini::find("Invoice", 1002))),
    ];
    assert_flip_invariant_push(&src, &chinook::catalog(), &ast, pushes);
}
