//! **Targeted gap** parity — specific corners the combinatorial matrix and the
//! cross-table / nested-EXISTS suites don't reach, each grounded against the SQLite
//! oracle on the small synthetic [`mini`] fixture:
//!
//! * **long mutation history** — one long-lived `View` driven through ~15 interleaved
//!   adds/removes/reparents, with parity asserted after *every* step (accumulation
//!   drift, not just the end state);
//! * **multi-column correlation** — a relationship whose join key is two columns
//!   (`AlbumId` *and* `MediaTypeId`), which every existing test's single-FK correlation
//!   never exercises;
//! * **root-level `LIMIT` refill on push** — a visible top-N row evicted (must refill
//!   from below) and a new row added above the window (must evict the tail);
//! * **an edit that moves a row's ORDER-BY key** — reordering it within its sibling
//!   list (the `View`'s copy-on-write reorder path);
//! * **NULL ordering under push** — a sort key edited *to* and *from* NULL, moving rows
//!   in and out of the null group (`null < everything`).
//!
//! (`select` projection is intentionally absent: d2s projects all catalog columns and
//! never consults `ast.select`, and the `View` materializes all columns too — both
//! sides ignore it, so there is no projection divergence to pin until that changes.)

mod harness;

use harness::{assert_hydrate_parity, assert_push_parity, assert_push_walk_parity, chinook, mini};
use rindle::{owned_row, table, OwnedRow, OwnedValue as V, SourceChange};
use rindle_d2s::{Cardinality, Catalog};

// Track column indices for the single-field edits below.
const T_ALBUM: usize = 2;
const T_COMPOSER: usize = 5;
const T_MS: usize = 6;

/// Copy `row`, replacing column `idx`.
fn set_col(row: &OwnedRow, idx: usize, v: V) -> OwnedRow {
    let mut vals: Vec<V> = row.to_value_vec();
    vals[idx] = v;
    owned_row(vals)
}

/// `SourceChange::Edit` from an exact seeded/known row + a single-field change.
fn edit(old: OwnedRow, idx: usize, v: V) -> SourceChange {
    let row = set_col(&old, idx, v);
    SourceChange::Edit { old, row }
}

/// Album → tracks, both levels PK-ordered.
fn album_tracks() -> rindle::Ast {
    table("Album")
        .sub_as("tracks", |a| {
            table("Track")
                .r#where("AlbumId", a.col("AlbumId"))
                .order_by("TrackId", "asc")
        })
        .order_by("AlbumId", "asc")
        .build()
}

// ── long mutation history (per-step parity) ───────────────────────────────────────

#[test]
fn long_history_album_tracks() {
    let src = mini::sources();
    // Brand-new tracks created along the way; their exact rows are bound so the later
    // Remove/Edit of each matches by value, not just key.
    let t300 = chinook::track(300, "t-300", 40, 1, 1, None, 120_000, 1_200_000, 0.99);
    let t301 = chinook::track(301, "t-301", 40, 1, 1, None, 121_000, 1_210_000, 0.99);
    let t302 = chinook::track(302, "t-302", 11, 1, 3, None, 122_000, 1_220_000, 0.99);
    let t303 = chinook::track(303, "t-303", 10, 1, 1, None, 123_000, 1_230_000, 0.99);

    // Each entry is one flush; the view is checked against the oracle after every one.
    // No row is edited twice, so each Edit/Remove carries a row that is exact at that
    // point in the history.
    let pushes = vec![
        ("Album", SourceChange::Add(chinook::album(40, "X", 3))), // empty new album
        ("Track", SourceChange::Add(t300.clone())),               // alb40 = {300}
        ("Track", SourceChange::Add(t301.clone())),               // alb40 = {300,301}
        ("Track", SourceChange::Remove(mini::find("Track", 105))), // alb20 = {106,107}
        ("Track", SourceChange::Add(t302.clone())),               // alb11 = {103,104,302}
        // reparent mini track 106 from album 20 to the new album 40
        (
            "Track",
            edit(mini::find("Track", 106), T_ALBUM, V::Float(40.0)),
        ),
        ("Track", SourceChange::Remove(t301.clone())), // alb40 = {106,300}
        ("Album", SourceChange::Add(chinook::album(41, "Y", 1))), // empty new album
        ("Track", edit(t300.clone(), T_ALBUM, V::Float(41.0))), // 300 → alb41
        ("Album", SourceChange::Remove(chinook::album(40, "X", 3))), // alb40 gone (106 orphaned)
        ("Track", SourceChange::Remove(mini::find("Track", 103))), // alb11 = {104,302}
        ("Track", SourceChange::Add(t303.clone())),    // alb10 = {100,101,102,303}
        (
            "Album",
            SourceChange::Edit {
                old: chinook::album(41, "Y", 1),
                row: chinook::album(41, "Y2", 1),
            },
        ),
        ("Track", SourceChange::Remove(mini::find("Track", 100))), // alb10 = {101,102,303}
        ("Track", SourceChange::Remove(t302.clone())),             // alb11 = {104}
    ];
    assert_push_walk_parity(&src, &chinook::catalog(), &album_tracks(), pushes);
}

// ── multi-column correlation (two-column join key) ────────────────────────────────

/// The chinook catalog plus a synthetic `Track.twins` relationship (declared `Many`).
/// The two correlation columns themselves live in the query AST; the catalog only needs
/// the relationship *name*.
fn twins_catalog() -> Catalog {
    let mut c = chinook::catalog();
    c.relationship("Track", "twins", Cardinality::Many);
    c
}

/// Tracks on album 10, each with its `twins`: the tracks sharing BOTH its album and its
/// media type (a 2-column correlation). On album 10: 100(MP3),101(MP3),102(AAC) — so
/// 100/101 are mutual twins and 102 stands alone.
fn track_twins() -> rindle::Ast {
    table("Track")
        .where_op("AlbumId", "==", 10)
        .sub_as("twins", |t| {
            table("Track")
                .r#where("AlbumId", t.col("AlbumId"))
                .r#where("MediaTypeId", t.col("MediaTypeId"))
                .order_by("TrackId", "asc")
        })
        .order_by("TrackId", "asc")
        .build()
}

#[test]
fn multi_col_correlation_hydrate() {
    let src = mini::sources();
    assert_hydrate_parity(&src, &twins_catalog(), &track_twins());
}

#[test]
fn multi_col_correlation_push() {
    let src = mini::sources();
    // Add another MP3 on album 10: it must join into 100's and 101's twin lists (and
    // carry them in its own), while the lone AAC track 102 is untouched.
    let pushes = vec![(
        "Track",
        SourceChange::Add(chinook::track(
            310, "t-310", 10, 1, 1, None, 111_000, 1_110_000, 0.99,
        )),
    )];
    assert_push_parity(&src, &twins_catalog(), &track_twins(), pushes);
}

// ── root-level LIMIT refill on push ───────────────────────────────────────────────

/// Tracks, longest-first, top 3. mini ms ⇒ {106:400k, 107:350k, 102:300k} are visible;
/// {104:250k, 100:200k, 103:150k, 101:100k, 105:50k} sit below the window.
fn top3_longest() -> rindle::Ast {
    table("Track")
        .order_by("Milliseconds", "desc")
        .limit(3)
        .build()
}

#[test]
fn root_limit_remove_top_refills() {
    let src = mini::sources();
    // Remove the current #1 (106). The window must drop it and pull 104 up from below:
    // {107, 102, 104}.
    let pushes = vec![("Track", SourceChange::Remove(mini::find("Track", 106)))];
    assert_push_parity(&src, &chinook::catalog(), &top3_longest(), pushes);
}

#[test]
fn root_limit_add_above_evicts() {
    let src = mini::sources();
    // A longer-than-anything track enters at the top and must evict the tail (102):
    // {330, 106, 107}.
    let pushes = vec![(
        "Track",
        SourceChange::Add(chinook::track(
            330, "t-330", 10, 1, 1, None, 999_000, 9_990_000, 0.99,
        )),
    )];
    assert_push_parity(&src, &chinook::catalog(), &top3_longest(), pushes);
}

// ── an edit that moves a row's ORDER-BY key (intra-sibling reorder) ───────────────

#[test]
fn edit_moves_child_sort_position() {
    let src = mini::sources();
    // Album → tracks ordered by Milliseconds. Album 20 by ms asc is [105:50k, 107:350k,
    // 106:400k]; bumping 105 to 999k must reorder it to the end of its sibling list.
    let ast = table("Album")
        .where_op("AlbumId", "==", 20)
        .sub_as("tracks", |a| {
            table("Track")
                .r#where("AlbumId", a.col("AlbumId"))
                .order_by("Milliseconds", "asc")
                .order_by("TrackId", "asc")
        })
        .build();
    let pushes = vec![(
        "Track",
        edit(mini::find("Track", 105), T_MS, V::Float(999_000.0)),
    )];
    assert_push_parity(&src, &chinook::catalog(), &ast, pushes);
}

// ── NULL ordering under push (rows crossing the null group) ───────────────────────

#[test]
fn null_ordering_under_push() {
    let src = mini::sources();
    // Order by the nullable Composer (nulls first on ASC), TrackId as a stable tiebreak.
    // One edit pushes a row INTO the null group (105: "Lee" → NULL) and another pulls one
    // OUT of it (101: NULL → "Zed", sorting to the very end).
    let ast = table("Track")
        .order_by("Composer", "asc")
        .order_by("TrackId", "asc")
        .build();
    let pushes = vec![
        ("Track", edit(mini::find("Track", 105), T_COMPOSER, V::Null)),
        (
            "Track",
            edit(mini::find("Track", 101), T_COMPOSER, V::str("Zed")),
        ),
    ];
    assert_push_parity(&src, &chinook::catalog(), &ast, pushes);
}
