//! **Pagination (`start`) parity** — the keyset paging bound, newly lowered by the
//! d2s compiler, checked against the IVM `Skip` operator over the SQLite oracle.
//!
//! The engine keeps a row iff the bound sorts before it (`B < R`), or equal with an
//! inclusive bound (`B <= R`), under the PK-completed sort, null-low. These cases pin
//! that the compiled keyset predicate reproduces it across: inclusive/exclusive bounds,
//! ASC/DESC, a partial bound (sort key's PK column omitted) vs. a full multi-column
//! bound, the `start` + `LIMIT` window, a nullable sort column, a paged nested child,
//! and a push whose row crosses the bound. The final case pins that a `start` the
//! compiler does **not** lower (inside an EXISTS) is rejected, not silently dropped.

mod harness;

use harness::{assert_hydrate_parity, assert_push_parity, chinook, mini};
use rindle::{table, Lit, SourceChange};
use rindle_d2s::compile;

// ── root paging by the primary key ────────────────────────────────────────────────

#[test]
fn start_at_pk_inclusive() {
    let src = mini::sources();
    let ast = table("Track")
        .start_at("TrackId", 103)
        .order_by("TrackId", "asc")
        .build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

#[test]
fn start_after_pk_exclusive() {
    let src = mini::sources();
    let ast = table("Track")
        .start_after("TrackId", 103)
        .order_by("TrackId", "asc")
        .build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

#[test]
fn start_pk_descending() {
    let src = mini::sources();
    // DESC sort: rows "after" the bound are the ones with a *smaller* key.
    let ast = table("Track")
        .start_after("TrackId", 105)
        .order_by("TrackId", "desc")
        .build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

// ── paging by an order_by column (partial vs full sort key) ───────────────────────

#[test]
fn start_after_order_col_partial_key() {
    let src = mini::sources();
    // Bound names only the order column; the completed sort also has the PK (TrackId),
    // whose bound value is therefore NULL (null-low). Exercises the omitted-column path.
    let ast = table("Track")
        .start_after("Milliseconds", 300_000)
        .order_by("Milliseconds", "desc")
        .build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

#[test]
fn start_full_multi_col_key_exclusive() {
    let src = mini::sources();
    // Full sort key (Milliseconds + TrackId) ⇒ the bound lands exactly on track 102;
    // exclusive must drop it and keep everything sorting after.
    let ast = table("Track")
        .start_row(
            vec![
                ("Milliseconds".into(), Lit::Number(300_000.0)),
                ("TrackId".into(), Lit::Number(102.0)),
            ],
            true,
        )
        .order_by("Milliseconds", "desc")
        .build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

#[test]
fn start_full_multi_col_key_inclusive() {
    let src = mini::sources();
    let ast = table("Track")
        .start_row(
            vec![
                ("Milliseconds".into(), Lit::Number(300_000.0)),
                ("TrackId".into(), Lit::Number(102.0)),
            ],
            false,
        )
        .order_by("Milliseconds", "desc")
        .build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

// ── start + limit (the paging window) ─────────────────────────────────────────────

#[test]
fn start_then_limit_window() {
    let src = mini::sources();
    let ast = table("Track")
        .start_after("Milliseconds", 150_000)
        .order_by("Milliseconds", "desc")
        .limit(3)
        .build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

// ── paging by a nullable column (null group boundary) ─────────────────────────────

#[test]
fn start_after_nullable_column() {
    let src = mini::sources();
    // Composer is nullable (101/104/107 are NULL). ASC ⇒ nulls first; a bound at "Lee"
    // must skip the null group and "Jones", keeping the "Lee" and "Smith" rows.
    let ast = table("Track")
        .start_after("Composer", "Lee")
        .order_by("Composer", "asc")
        .order_by("TrackId", "asc")
        .build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

// ── paging a nested child relationship ────────────────────────────────────────────

#[test]
fn start_on_nested_child() {
    let src = mini::sources();
    // The paging bound is applied per parent's child list (the same `start` filters each
    // album's tracks independently).
    let ast = table("Album")
        .where_in("AlbumId", [10, 20])
        .sub_as("tracks", |a| {
            table("Track")
                .r#where("AlbumId", a.col("AlbumId"))
                .start_after("TrackId", 100)
                .order_by("TrackId", "asc")
        })
        .order_by("AlbumId", "asc")
        .build();
    assert_hydrate_parity(&src, &chinook::catalog(), &ast);
}

// ── push: a row crosses the bound ─────────────────────────────────────────────────

#[test]
fn push_rows_cross_bound() {
    let src = mini::sources();
    // Window = tracks with Milliseconds at/after 200k, longest-first. Edits and adds move
    // rows across the bound in both directions; a new row lands inside the window.
    let ast = table("Track")
        .start_at("Milliseconds", 200_000)
        .order_by("Milliseconds", "desc")
        .order_by("TrackId", "asc")
        .build();
    let bump = |id: i64, ms: i64| {
        let old = mini::find("Track", id);
        let mut vals: Vec<_> = old.to_value_vec();
        vals[6] = rindle::OwnedValue::Float(ms as f64); // Milliseconds
        (
            "Track",
            SourceChange::Edit {
                old,
                row: rindle::owned_row(vals),
            },
        )
    };
    let pushes = vec![
        bump(105, 250_000), // 50k → 250k: enters the window
        bump(100, 100_000), // 200k → 100k: leaves the window
        (
            "Track",
            SourceChange::Add(chinook::track(
                208, "t-208", 10, 1, 1, None, 500_000, 5_000_000, 0.99,
            )),
        ), // new top row, inside the window
    ];
    assert_push_parity(&src, &chinook::catalog(), &ast, pushes);
}

// ── unsupported position is rejected, not silently dropped ────────────────────────

#[test]
fn start_inside_exists_is_rejected() {
    // A `start` on an EXISTS child is not lowered (existence ignores paging). The
    // compiler must reject it rather than emit SQL that ignores the bound.
    let ast = table("Genre")
        .where_exists(|g| {
            table("Track")
                .alias("hasTrack")
                .r#where("GenreId", g.col("GenreId"))
                .start_after("TrackId", 100)
        })
        .order_by("GenreId", "asc")
        .build();
    assert!(
        compile(&chinook::catalog(), &ast).is_err(),
        "expected `start` inside EXISTS to be rejected as Unsupported"
    );
}
