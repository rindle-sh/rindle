//! Flip-invariance parity — every EXISTS-bearing query is run under ALL 2^k flip
//! assignments of its EXISTS nodes and checked against the (flip-blind) SQLite
//! oracle, for both hydration and push. `flip` is a result-invariant routing choice,
//! so this is a metamorphic check that needs no hand-computed expectation and
//! stresses exactly the divergent parent-driven vs. child-driven dataflow.
//!
//! Flipped `NOT EXISTS` is `BuildError::Unsupported`; the harness asserts those
//! variants reject rather than match (no special-casing needed in the tests).
//!
//! Requires the chinook v1.4.5 dump (see `harness::chinook`); tests no-op without it.

mod harness;

use harness::{assert_flip_invariant, assert_flip_invariant_push, chinook};
use rindle::{table, SourceChange};

// ── hydrate flip-invariance ──────────────────────────────────────────────────────

#[test]
fn flip_exists_top_level() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    let ast = table("Genre")
        .where_exists(|g| {
            table("Track")
                .alias("hasTrack")
                .r#where("GenreId", g.col("GenreId"))
        })
        .order_by("GenreId", "asc")
        .build();
    assert_flip_invariant(&src, &chinook::catalog(), &ast);
}

#[test]
fn flip_exists_order_limit_chunked_merge() {
    // REGRESSION (chinook sweep): a flipped EXISTS over a child set LARGER than the
    // FlippedJoin IN-batch chunk size (256) — chinook has 347 albums — takes the
    // chunked-fetch k-way-merge path. That merge must compare on the parent's RESOLVED
    // order (`order by Milliseconds`), not the base-table PK sort, or a top-N `limit`
    // keeps the wrong rows. The non-flipped variant (and the oracle) pin the answer.
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    let cat = chinook::catalog();

    // Minimal triggering shape: flipped EXISTS + order + limit, >256 distinct keys.
    let bare = table("Track")
        .where_exists(|t| {
            table("Album")
                .alias("onAlbum")
                .r#where("AlbumId", t.col("AlbumId"))
        })
        .order_by("Milliseconds", "asc")
        .limit(2)
        .build();
    assert_flip_invariant(&src, &cat, &bare);

    // The shape the sweep originally flagged: a sibling AND-conjunct shifts the
    // qualifying set so the broken merge surfaces a visibly wrong row.
    let with_conjunct = table("Track")
        .where_op("Composer", "IS NOT", None::<&str>)
        .where_exists(|t| {
            table("Album")
                .alias("onAlbum")
                .r#where("AlbumId", t.col("AlbumId"))
        })
        .order_by("Milliseconds", "asc")
        .limit(2)
        .build();
    assert_flip_invariant(&src, &cat, &with_conjunct);
}

#[test]
fn flip_not_exists_top_level() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // Genres with NO tracks. The flipped variant must REJECT (anti-join not built);
    // the non-flipped variant must match the oracle.
    let ast = table("Genre")
        .where_not_exists(|g| {
            table("Track")
                .alias("noTrack")
                .r#where("GenreId", g.col("GenreId"))
        })
        .order_by("GenreId", "asc")
        .build();
    assert_flip_invariant(&src, &chinook::catalog(), &ast);
}

#[test]
fn flip_exists_with_inner_filter() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // Albums that have at least one "long" track (> 5 min).
    let ast = table("Album")
        .where_op("AlbumId", "<=", 50)
        .where_exists(|al| {
            table("Track")
                .alias("longTrack")
                .r#where("AlbumId", al.col("AlbumId"))
                .where_op("Milliseconds", ">", 300_000)
        })
        .order_by("AlbumId", "asc")
        .build();
    assert_flip_invariant(&src, &chinook::catalog(), &ast);
}

#[test]
fn flip_exists_under_or() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // (Name LIKE 'R%') OR EXISTS(a track in this genre) — EXISTS nested under OR, the
    // riskiest routing position. Both flip states must equal the declarative oracle.
    let ast = table("Genre")
        .where_any(|c| {
            c.where_op("Name", "LIKE", "R%").where_exists(|g| {
                table("Track")
                    .alias("hasTrack")
                    .r#where("GenreId", g.col("GenreId"))
            })
        })
        .order_by("GenreId", "asc")
        .build();
    assert_flip_invariant(&src, &chinook::catalog(), &ast);
}

#[test]
fn flip_two_exists_anded() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // Two EXISTS AND'd ⇒ 4 flip assignments. Tracks that appear on some album AND in
    // some playlist.
    let ast = table("Track")
        .where_op("TrackId", "<=", 100)
        .where_exists(|t| {
            table("Album")
                .alias("onAlbum")
                .r#where("AlbumId", t.col("AlbumId"))
        })
        .where_exists(|t| {
            table("PlaylistTrack")
                .alias("onPlaylist")
                .r#where("TrackId", t.col("TrackId"))
        })
        .order_by("TrackId", "asc")
        .build();
    assert_flip_invariant(&src, &chinook::catalog(), &ast);
}

#[test]
fn flip_self_join_exists() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // Self-referential: employees who manage someone (an employee reports to them).
    let ast = table("Employee")
        .where_exists(|e| {
            table("Employee")
                .alias("report")
                .r#where("ReportsTo", e.col("EmployeeId"))
        })
        .order_by("EmployeeId", "asc")
        .build();
    assert_flip_invariant(&src, &chinook::catalog(), &ast);
}

#[test]
fn flip_junction_exists() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // Playlists that contain track 1, via the (non-hidden) junction table.
    let ast = table("Playlist")
        .where_exists(|p| {
            table("PlaylistTrack")
                .alias("hasTrack")
                .r#where("PlaylistId", p.col("PlaylistId"))
                .where_op("TrackId", "==", 1)
        })
        .order_by("PlaylistId", "asc")
        .build();
    assert_flip_invariant(&src, &chinook::catalog(), &ast);
}

// ── push flip-invariance (gate transitions) ──────────────────────────────────────

#[test]
fn flip_push_exists_gate_opens() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // A fresh genre with no tracks does not satisfy EXISTS; adding a track opens the
    // gate. Both flip states must track the transition identically.
    let ast = table("Genre")
        .where_exists(|g| {
            table("Track")
                .alias("hasTrack")
                .r#where("GenreId", g.col("GenreId"))
        })
        .order_by("GenreId", "asc")
        .build();
    let pushes = vec![
        (
            "Genre",
            SourceChange::Add(chinook::genre(9001, "Synthwave")),
        ),
        (
            "Track",
            SourceChange::Add(chinook::track(
                9_000_010,
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
    assert_flip_invariant_push(&src, &chinook::catalog(), &ast, pushes);
}

#[test]
fn flip_push_exists_gate_opens_then_closes() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // Gate 0→1→0: add a genre, add its only track, then remove that track. Net result:
    // the genre exists but has no track, so it is absent from the EXISTS view.
    let ast = table("Genre")
        .where_exists(|g| {
            table("Track")
                .alias("hasTrack")
                .r#where("GenreId", g.col("GenreId"))
        })
        .order_by("GenreId", "asc")
        .build();
    let t = chinook::track(
        9_000_011,
        "Transient",
        1,
        1,
        9002,
        Some("Anon"),
        180_000,
        4_000_000,
        0.99,
    );
    let pushes = vec![
        (
            "Genre",
            SourceChange::Add(chinook::genre(9002, "Vaporwave")),
        ),
        ("Track", SourceChange::Add(t.clone())),
        ("Track", SourceChange::Remove(t)),
    ];
    assert_flip_invariant_push(&src, &chinook::catalog(), &ast, pushes);
}

#[test]
fn flip_push_not_exists_loses_membership() {
    let Some(src) = chinook::sources_or_skip() else {
        return;
    };
    // NOT EXISTS: a fresh empty genre is in the view; adding a track removes it.
    // (The flipped variant rejects; the harness asserts that automatically.)
    let ast = table("Genre")
        .where_not_exists(|g| {
            table("Track")
                .alias("noTrack")
                .r#where("GenreId", g.col("GenreId"))
        })
        .order_by("GenreId", "asc")
        .build();
    let pushes = vec![
        ("Genre", SourceChange::Add(chinook::genre(9003, "Drone"))),
        (
            "Track",
            SourceChange::Add(chinook::track(
                9_000_012,
                "Long Tone",
                1,
                1,
                9003,
                None,
                600_000,
                9_000_000,
                0.99,
            )),
        ),
    ];
    assert_flip_invariant_push(&src, &chinook::catalog(), &ast, pushes);
}
