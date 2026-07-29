//! Chinook differential parity — drive real-data queries through the IVM `View` and
//! the `rindle-d2s` SQLite oracle and assert they agree, for both **hydration** and
//! **incremental push** maintenance (see [`harness`]).
//!
//! This is the seed of what will grow into a broad combinatorial suite over the
//! operator/relationship matrix; the cases here are deliberately simple and scoped
//! (tight `where`s keep each result tree small and the diffs readable on failure).
//!
//! Requires the chinook v1.4.5 SQL dump (see `harness::chinook`); each test no-ops
//! when it is absent.

mod harness;

use harness::{assert_hydrate_parity, assert_push_parity, chinook};
use rindle::{table, SourceChange};

// ── hydration ────────────────────────────────────────────────────────────────────

#[test]
fn hydrate_flat_genres() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // A whole small table, PK-ordered.
    let ast = table("Genre").order_by("GenreId", "asc").build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

#[test]
fn hydrate_flat_filtered_tracks() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // Range filter + ordering on a large table.
    let ast = table("Track")
        .where_op("AlbumId", "<=", 5)
        .where_op("Milliseconds", ">", 200_000)
        .order_by("TrackId", "asc")
        .build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

#[test]
fn hydrate_artist_albums() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // One level of nesting: artists 1..=5, each with their albums.
    let ast = table("Artist")
        .where_op("ArtistId", "<=", 5)
        .sub_as("albums", |a| {
            table("Album")
                .r#where("ArtistId", a.col("ArtistId"))
                .order_by("AlbumId", "asc")
        })
        .order_by("ArtistId", "asc")
        .build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

#[test]
fn hydrate_album_tracks_two_levels() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // Two levels: artist 1 → albums → tracks.
    let ast = table("Artist")
        .r#where("ArtistId", 1)
        .sub_as("albums", |a| {
            table("Album")
                .r#where("ArtistId", a.col("ArtistId"))
                .sub_as("tracks", |al| {
                    table("Track")
                        .r#where("AlbumId", al.col("AlbumId"))
                        .order_by("TrackId", "asc")
                })
                .order_by("AlbumId", "asc")
        })
        .build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

#[test]
fn hydrate_child_filter_and_limit() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // Child-level filter + order + limit (top-2 longest tracks per album).
    let ast = table("Album")
        .where_op("AlbumId", "<=", 10)
        .sub_as("tracks", |al| {
            table("Track")
                .r#where("AlbumId", al.col("AlbumId"))
                .order_by("Milliseconds", "desc")
                .limit(2)
        })
        .order_by("AlbumId", "asc")
        .build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

#[test]
fn hydrate_exists_genres_with_tracks() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // EXISTS gates the parent and is excluded from the output tree.
    let ast = table("Genre")
        .where_exists(|g| {
            table("Track")
                .alias("hasTrack")
                .r#where("GenreId", g.col("GenreId"))
        })
        .order_by("GenreId", "asc")
        .build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

// ── push ─────────────────────────────────────────────────────────────────────────

#[test]
fn push_add_album_to_artist() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    let ast = table("Artist")
        .r#where("ArtistId", 1)
        .sub_as("albums", |a| {
            table("Album")
                .r#where("ArtistId", a.col("ArtistId"))
                .order_by("AlbumId", "asc")
        })
        .build();
    // A brand-new album appears under artist 1 (high id avoids any PK collision).
    let pushes = vec![(
        "Album",
        SourceChange::Add(chinook::album(9_000_001, "Brand New Record", 1)),
    )];
    assert_push_parity(&src, &chinook::catalog(), &ast, pushes);
}

#[test]
fn push_remove_existing_track() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    let ast = table("Album")
        .r#where("AlbumId", 1)
        .sub_as("tracks", |al| {
            table("Track")
                .r#where("AlbumId", al.col("AlbumId"))
                .order_by("TrackId", "asc")
        })
        .build();
    // Remove a real track that belongs to album 1 (track 1 is on album 1 in chinook).
    let track = chinook::find("Track", 1).expect("track 1 exists");
    let pushes = vec![("Track", SourceChange::Remove(track))];
    assert_push_parity(&src, &chinook::catalog(), &ast, pushes);
}

#[test]
fn push_edit_track_moves_albums() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // Two parents in view; an edit reparents a track from album 1 to album 2.
    let ast = table("Album")
        .where_op("AlbumId", "<=", 2)
        .sub_as("tracks", |al| {
            table("Track")
                .r#where("AlbumId", al.col("AlbumId"))
                .order_by("TrackId", "asc")
        })
        .order_by("AlbumId", "asc")
        .build();
    let old = chinook::find("Track", 1).expect("track 1 exists");
    let mut new_vals: Vec<_> = old.to_value_vec();
    new_vals[2] = rindle::OwnedValue::Float(2.0); // AlbumId 1 -> 2
    let new = rindle::owned_row(new_vals);
    let pushes = vec![("Track", SourceChange::Edit { row: new, old })];
    assert_push_parity(&src, &chinook::catalog(), &ast, pushes);
}

#[test]
fn push_exists_flip_on_add() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // A genre with no tracks is absent from an EXISTS query; adding a track makes it
    // appear. Genre 25 ("Opera") has very few/zero tracks — use a fresh high-id genre
    // we add a track to, so the flip is unambiguous.
    let ast = table("Genre")
        .where_exists(|g| {
            table("Track")
                .alias("hasTrack")
                .r#where("GenreId", g.col("GenreId"))
        })
        .order_by("GenreId", "asc")
        .build();
    let pushes = vec![
        // New empty genre (does not satisfy EXISTS yet) ...
        (
            "Genre",
            SourceChange::Add(chinook::genre(9001, "Synthwave")),
        ),
        // ... then a track in it (now it should appear).
        (
            "Track",
            SourceChange::Add(chinook::track(
                9_000_002,
                "Neon Drive",
                1,
                1,
                9001,
                Some("Anon"),
                210_000,
                5_000_000,
                0.99,
            )),
        ),
    ];
    assert_push_parity(&src, &chinook::catalog(), &ast, pushes);
}
