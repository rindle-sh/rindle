//! **Multi-nested EXISTS** parity — an EXISTS whose subquery itself contains an
//! EXISTS (two and three levels deep), plus the riskier compositions: an inner filter
//! beside the inner EXISTS, two *sibling* EXISTS at the inner level, a nested EXISTS
//! buried under an `OR`, and an outer `NOT EXISTS` wrapping an inner `EXISTS`.
//!
//! Every case is EXISTS-bearing, so it routes through the flip harness: each query is
//! re-run under all 2^k assignments of its EXISTS nodes' result-invariant `flip` flag
//! and checked against the (flip-blind) SQLite oracle — a metamorphic check that needs
//! no hand-computed expectation and stresses exactly the parent-driven vs. child-driven
//! dataflow at every nesting level. A *flipped* `NOT EXISTS` is unsupported; the harness
//! asserts those variants reject rather than match, so the outer-NOT cases need no
//! special handling here.
//!
//! Runs on the small synthetic [`mini`] fixture so each gate transition the pushes flip
//! is authored, not hunted for. The push cases drive a gate at depth: opening/closing
//! the *outer* membership by adding/removing the *deepest* correlated row, and moving an
//! inner correlation so two parents flip in opposite directions at once.

mod harness;

use harness::{assert_flip_invariant, assert_flip_invariant_push, chinook, mini};
use rindle::{table, Ast, SourceChange};

// ── reused shapes ─────────────────────────────────────────────────────────────────

/// Genre → has a Track → on a Playlist. Two chained EXISTS. In-view genres are those
/// with at least one track that appears on some playlist: {1 (track 100), 2 (track 102)}.
fn genre_track_playlist() -> Ast {
    table("Genre")
        .where_exists(|g| {
            table("Track")
                .alias("t")
                .r#where("GenreId", g.col("GenreId"))
                .where_exists(|t| {
                    table("PlaylistTrack")
                        .alias("pt")
                        .r#where("TrackId", t.col("TrackId"))
                })
        })
        .order_by("GenreId", "asc")
        .build()
}

/// Artist → has an Album → has a Track → on a Playlist. Three chained EXISTS ⇒ 8 flip
/// variants. Only artist 1 qualifies (album 10's tracks are on playlist 1).
fn artist_album_track_playlist() -> Ast {
    table("Artist")
        .where_exists(|ar| {
            table("Album")
                .alias("a")
                .r#where("ArtistId", ar.col("ArtistId"))
                .where_exists(|al| {
                    table("Track")
                        .alias("t")
                        .r#where("AlbumId", al.col("AlbumId"))
                        .where_exists(|t| {
                            table("PlaylistTrack")
                                .alias("pt")
                                .r#where("TrackId", t.col("TrackId"))
                        })
                })
        })
        .order_by("ArtistId", "asc")
        .build()
}

/// Genre with NO track that is on a playlist — outer `NOT EXISTS` wrapping an inner
/// `EXISTS`. The complement of [`genre_track_playlist`]: {3 (tracks 103/106, neither
/// playlisted), 4 (no tracks at all)}.
fn genre_not_exists_playlisted_track() -> Ast {
    table("Genre")
        .where_not_exists(|g| {
            table("Track")
                .alias("t")
                .r#where("GenreId", g.col("GenreId"))
                .where_exists(|t| {
                    table("PlaylistTrack")
                        .alias("pt")
                        .r#where("TrackId", t.col("TrackId"))
                })
        })
        .order_by("GenreId", "asc")
        .build()
}

// ── hydrate: EXISTS inside EXISTS ─────────────────────────────────────────────────

#[test]
fn hydrate_artist_album_track() {
    let src = mini::sources();
    // Artists that have an album that has a track. {1,2} qualify; Gamma (3) has no album.
    let ast = table("Artist")
        .where_exists(|ar| {
            table("Album")
                .alias("hasAlbum")
                .r#where("ArtistId", ar.col("ArtistId"))
                .where_exists(|al| {
                    table("Track")
                        .alias("hasTrack")
                        .r#where("AlbumId", al.col("AlbumId"))
                })
        })
        .order_by("ArtistId", "asc")
        .build();
    assert_flip_invariant(&src, &chinook::catalog(), &ast);
}

#[test]
fn hydrate_genre_track_playlist() {
    let src = mini::sources();
    assert_flip_invariant(&src, &chinook::catalog(), &genre_track_playlist());
}

#[test]
fn hydrate_three_level_chain() {
    let src = mini::sources();
    assert_flip_invariant(&src, &chinook::catalog(), &artist_album_track_playlist());
}

#[test]
fn hydrate_inner_filter_beside_inner_exists() {
    let src = mini::sources();
    // Album that has a track which is BOTH > 100k ms AND on a playlist. Only album 10
    // (track 100: 200k, playlisted) qualifies — track 101 is on a playlist but exactly
    // 100k (not > 100k), so the filter and the inner EXISTS must both bite.
    let ast = table("Album")
        .where_exists(|a| {
            table("Track")
                .alias("t")
                .r#where("AlbumId", a.col("AlbumId"))
                .where_op("Milliseconds", ">", 100_000)
                .where_exists(|t| {
                    table("PlaylistTrack")
                        .alias("pt")
                        .r#where("TrackId", t.col("TrackId"))
                })
        })
        .order_by("AlbumId", "asc")
        .build();
    assert_flip_invariant(&src, &chinook::catalog(), &ast);
}

#[test]
fn hydrate_two_sibling_inner_exists() {
    let src = mini::sources();
    // Album that has a track which (has a genre) AND (is on a playlist) — two sibling
    // EXISTS ANDed at the inner level (3 EXISTS total). Every track has a genre, so this
    // reduces to "album with a playlisted track": only album 10.
    let ast = table("Album")
        .where_exists(|a| {
            table("Track")
                .alias("t")
                .r#where("AlbumId", a.col("AlbumId"))
                .where_exists(|t| {
                    table("Genre")
                        .alias("g")
                        .r#where("GenreId", t.col("GenreId"))
                })
                .where_exists(|t| {
                    table("PlaylistTrack")
                        .alias("pt")
                        .r#where("TrackId", t.col("TrackId"))
                })
        })
        .order_by("AlbumId", "asc")
        .build();
    assert_flip_invariant(&src, &chinook::catalog(), &ast);
}

#[test]
fn hydrate_nested_exists_under_or() {
    let src = mini::sources();
    // (Name LIKE 'G%') OR (has an album that has a track) — a nested EXISTS buried under
    // an OR, the riskiest routing position. The LIKE pulls in Gamma (3), which the EXISTS
    // branch excludes (no albums); the EXISTS branch yields {1,2}. Union: {1,2,3}.
    let ast = table("Artist")
        .where_any(|c| {
            c.where_op("Name", "LIKE", "G%").where_exists(|ar| {
                table("Album")
                    .alias("a")
                    .r#where("ArtistId", ar.col("ArtistId"))
                    .where_exists(|al| {
                        table("Track")
                            .alias("t")
                            .r#where("AlbumId", al.col("AlbumId"))
                    })
            })
        })
        .order_by("ArtistId", "asc")
        .build();
    assert_flip_invariant(&src, &chinook::catalog(), &ast);
}

#[test]
fn hydrate_outer_not_exists_wraps_inner_exists() {
    let src = mini::sources();
    // Genres with no playlisted track: {3,4}. The flipped-outer-NOT variants reject
    // (anti-join not built); the harness asserts that automatically.
    assert_flip_invariant(
        &src,
        &chinook::catalog(),
        &genre_not_exists_playlisted_track(),
    );
}

// ── push: flipping a gate from the deepest level ──────────────────────────────────

#[test]
fn push_open_deep_gate() {
    let src = mini::sources();
    // 3-level chain; artist 2 is OUT (album 20's tracks aren't on any playlist). Linking
    // track 105 onto a playlist lights up the whole chain → artist 2 enters.
    let pushes = vec![(
        "PlaylistTrack",
        SourceChange::Add(chinook::playlist_track(2, 105)),
    )];
    assert_flip_invariant_push(
        &src,
        &chinook::catalog(),
        &artist_album_track_playlist(),
        pushes,
    );
}

#[test]
fn push_close_outer_via_deep_removes() {
    let src = mini::sources();
    // 3-level chain; artist 1 is IN via album 10's playlisted tracks (100/101/102), and
    // its other album 11 has none. Removing all of playlist 1's entries empties the
    // deepest gate for every one of artist 1's tracks → artist 1 leaves.
    let pushes = vec![
        (
            "PlaylistTrack",
            SourceChange::Remove(mini::find2("PlaylistTrack", 1, 100)),
        ),
        (
            "PlaylistTrack",
            SourceChange::Remove(mini::find2("PlaylistTrack", 1, 101)),
        ),
        (
            "PlaylistTrack",
            SourceChange::Remove(mini::find2("PlaylistTrack", 1, 102)),
        ),
    ];
    assert_flip_invariant_push(
        &src,
        &chinook::catalog(),
        &artist_album_track_playlist(),
        pushes,
    );
}

#[test]
fn push_grow_chain_two_tables() {
    let src = mini::sources();
    // genre→track→playlist; genre 4 (Empty) is OUT. Add a track in genre 4 AND link it
    // onto a playlist, in one batch across Track + PlaylistTrack → genre 4 enters.
    let pushes = vec![
        (
            "Track",
            SourceChange::Add(chinook::track(
                320, "t-g4", 10, 1, 4, None, 170_000, 1_700_000, 0.99,
            )),
        ),
        (
            "PlaylistTrack",
            SourceChange::Add(chinook::playlist_track(1, 320)),
        ),
    ];
    assert_flip_invariant_push(&src, &chinook::catalog(), &genre_track_playlist(), pushes);
}

#[test]
fn push_move_inner_correlation_flips_two() {
    let src = mini::sources();
    // genre→track→playlist. Move a playlist membership from track 102 (genre 2) to track
    // 106 (genre 3) via a composite-PK Edit. Genre 2's only other track (105) isn't
    // playlisted, so genre 2 LEAVES; genre 3 gains a playlisted track, so it ENTERS —
    // one inner-correlation move flips two outer parents in opposite directions.
    let old = mini::find2("PlaylistTrack", 1, 102);
    let pushes = vec![(
        "PlaylistTrack",
        SourceChange::Edit {
            old,
            row: chinook::playlist_track(1, 106),
        },
    )];
    assert_flip_invariant_push(&src, &chinook::catalog(), &genre_track_playlist(), pushes);
}

#[test]
fn push_outer_not_exists_gains_member() {
    let src = mini::sources();
    // Genre NOT EXISTS(a playlisted track). Genre 1 (Rock) is OUT — tracks 100/101 are
    // on playlist 1. Removing both leaves Rock with no playlisted track (its other
    // tracks 104/107 were never on one) → Rock ENTERS the NOT-EXISTS view. Flipped-outer
    // variants reject (handled by the harness).
    let pushes = vec![
        (
            "PlaylistTrack",
            SourceChange::Remove(mini::find2("PlaylistTrack", 1, 100)),
        ),
        (
            "PlaylistTrack",
            SourceChange::Remove(mini::find2("PlaylistTrack", 1, 101)),
        ),
    ];
    assert_flip_invariant_push(
        &src,
        &chinook::catalog(),
        &genre_not_exists_playlisted_track(),
        pushes,
    );
}
