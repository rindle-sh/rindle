//! Directed combinatorial generator (Stage 3+). Orthogonal axes —
//! **shape** × **filter** × **order/limit** and **shape** × **push-scenario** ×
//! **filter** — are crossed into a matrix of queries; each query+push is routed
//! through [`assert_flip_invariant_push`], so every generated shape is checked for
//! both hydrate and push parity against the SQLite oracle **under all flip
//! assignments**.
//!
//! Two sub-matrices instead of one 4-way product (which would explode): matrix **A**
//! crosses structure with root filter/order (using each shape's *primary* push);
//! matrix **B** crosses structure with the full **push-scenario** list against a
//! representative filter subset. Together they cover (filter×order) and
//! (push×filter) pairwise without the full cross.
//!
//! Runs over the small synthetic [`mini`] fixture (chinook schema, ~40 controlled
//! rows) so the matrix stays fast and failure diffs stay tiny — and so the
//! membership / limit / exists / reparent transitions the pushes depend on are
//! authored, not hunted for. Realistic-scale data is covered by
//! `chinook_parity`/`chinook_flip`.
//!
//! All failures are collected and reported together (with the case label) rather than
//! failing on the first, so a generator regression surfaces the whole blast radius.

mod harness;

use std::panic::{catch_unwind, AssertUnwindSafe};

use harness::{assert_flip_invariant_push, chinook, mini};
use rindle::{owned_row, table, OwnedRow, OwnedValue as V, Query, SourceChange};

type Push = (&'static str, SourceChange);

// ── exact-row helpers (keep Remove/Edit rows identical to the `mini` fixture) ─────

/// The exact `mini` Track row for `id` — so a `Remove`/`Edit{old}` matches by key
/// *and* value (the engine matches by key; the oracle deletes by key).
fn mtrack(id: i64) -> OwnedRow {
    match id {
        100 => chinook::track(
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
        101 => chinook::track(101, "t-b", 10, 1, 1, None, 100_000, 1_000_000, 0.99),
        102 => chinook::track(
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
        103 => chinook::track(
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
        104 => chinook::track(104, "t-e", 11, 2, 1, None, 250_000, 2_500_000, 0.99),
        105 => chinook::track(105, "t-f", 20, 1, 2, Some("Lee"), 50_000, 500_000, 0.99),
        106 => chinook::track(
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
        107 => chinook::track(107, "t-h", 20, 2, 1, None, 350_000, 3_500_000, 0.99),
        _ => panic!("mtrack: no mini track {id}"),
    }
}

/// A fresh "generated" track (defaults for the columns a scenario doesn't care about).
fn nt(id: i64, album: i64, genre: i64, ms: i64) -> OwnedRow {
    chinook::track(id, "t-gen", album, 1, genre, None, ms, ms * 10, 0.99)
}

/// Copy `row`, replacing one column — so an `Edit` changes exactly one field.
fn edit_field(row: OwnedRow, idx: usize, v: V) -> OwnedRow {
    let mut vals: Vec<V> = row.to_value_vec();
    vals[idx] = v;
    owned_row(vals)
}

// Track column indices, for `edit_field`.
const T_ALBUM: usize = 2;
const T_GENRE: usize = 4;
const T_COMPOSER: usize = 5;
const T_MS: usize = 6;

// ── axis 1: shape ────────────────────────────────────────────────────────────────

/// Columns on a shape's ROOT table that the filter/order axes parameterize over,
/// with concrete values tuned to the `mini` data so the filters actually partition.
struct RootCols {
    num: &'static str,
    num_mid: i64,
    in_set: [i64; 2],
    txt: &'static str,
    like_prefix: &'static str,
    ilike_prefix: &'static str,
    nullable: &'static str,
    order_col: &'static str,
}

struct PushScenario {
    label: &'static str,
    changes: fn() -> Vec<Push>,
}

struct Shape {
    label: &'static str,
    /// The structural query (nesting / EXISTS), WITHOUT root filter/order/limit.
    root: fn() -> Query,
    cols: RootCols,
    /// Labeled transition scenarios. `[0]` is the *primary*, used by matrix A.
    scenarios: fn() -> Vec<PushScenario>,
}

fn track_col() -> RootCols {
    RootCols {
        num: "TrackId",
        num_mid: 104,
        in_set: [100, 102],
        txt: "Name",
        like_prefix: "t-",
        ilike_prefix: "T-",
        nullable: "Composer",
        order_col: "Milliseconds",
    }
}
fn album_col() -> RootCols {
    RootCols {
        num: "AlbumId",
        num_mid: 11,
        in_set: [10, 20],
        txt: "Title",
        like_prefix: "A-",
        ilike_prefix: "a-",
        nullable: "Title",
        order_col: "ArtistId",
    }
}
fn artist_col() -> RootCols {
    RootCols {
        num: "ArtistId",
        num_mid: 2,
        in_set: [1, 3],
        txt: "Name",
        like_prefix: "A",
        ilike_prefix: "a",
        nullable: "Name",
        order_col: "Name",
    }
}
fn genre_col() -> RootCols {
    RootCols {
        num: "GenreId",
        num_mid: 2,
        in_set: [1, 4],
        txt: "Name",
        like_prefix: "R",
        ilike_prefix: "r",
        nullable: "Name",
        order_col: "Name",
    }
}
fn playlist_col() -> RootCols {
    RootCols {
        num: "PlaylistId",
        num_mid: 1,
        in_set: [1, 2],
        txt: "Name",
        like_prefix: "P",
        ilike_prefix: "p",
        nullable: "Name",
        order_col: "Name",
    }
}
fn employee_col() -> RootCols {
    RootCols {
        num: "EmployeeId",
        num_mid: 2,
        in_set: [1, 3],
        txt: "LastName",
        like_prefix: "A",
        ilike_prefix: "a",
        nullable: "ReportsTo",
        order_col: "ReportsTo",
    }
}
fn customer_col() -> RootCols {
    RootCols {
        num: "CustomerId",
        num_mid: 2,
        in_set: [1, 3],
        txt: "Country",
        like_prefix: "U",
        ilike_prefix: "u",
        nullable: "Country",
        order_col: "Country",
    }
}

fn shapes() -> Vec<Shape> {
    vec![
        Shape {
            label: "flat_track",
            root: || table("Track"),
            cols: track_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "addrm",
                        changes: || {
                            vec![
                                ("Track", SourceChange::Add(nt(200, 10, 1, 120_000))),
                                ("Track", SourceChange::Remove(mtrack(107))),
                            ]
                        },
                    },
                    PushScenario {
                        label: "edit_inplace",
                        changes: || {
                            vec![("Track", edit(mtrack(100), T_COMPOSER, V::str("Edited")))]
                        },
                    },
                ]
            },
        },
        Shape {
            label: "album_tracks",
            root: || {
                table("Album").sub_as("tracks", |a| {
                    table("Track")
                        .r#where("AlbumId", a.col("AlbumId"))
                        .order_by("TrackId", "asc")
                })
            },
            cols: album_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "child_addrm",
                        changes: || {
                            vec![
                                ("Track", SourceChange::Add(nt(201, 11, 1, 90_000))),
                                ("Track", SourceChange::Remove(mtrack(100))),
                            ]
                        },
                    },
                    PushScenario {
                        label: "child_reparent",
                        changes: || vec![("Track", edit(mtrack(105), T_ALBUM, V::Float(10.0)))],
                    },
                ]
            },
        },
        Shape {
            label: "artist_album_track",
            root: || {
                table("Artist").sub_as("albums", |a| {
                    table("Album")
                        .r#where("ArtistId", a.col("ArtistId"))
                        .sub_as("tracks", |al| {
                            table("Track")
                                .r#where("AlbumId", al.col("AlbumId"))
                                .order_by("TrackId", "asc")
                        })
                        .order_by("AlbumId", "asc")
                })
            },
            cols: artist_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "grow_empty",
                        changes: || {
                            vec![
                                ("Album", SourceChange::Add(chinook::album(30, "G-one", 3))),
                                ("Track", SourceChange::Add(nt(202, 30, 1, 120_000))),
                            ]
                        },
                    },
                    PushScenario {
                        label: "remove_mid",
                        changes: || {
                            vec![(
                                "Album",
                                SourceChange::Remove(chinook::album(11, "A-two", 1)),
                            )]
                        },
                    },
                ]
            },
        },
        Shape {
            label: "album_artist_one",
            root: || {
                table("Album").sub_as("artist", |a| {
                    table("Artist").r#where("ArtistId", a.col("ArtistId"))
                })
            },
            cols: album_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "reparent",
                        changes: || {
                            vec![(
                                "Album",
                                SourceChange::Edit {
                                    old: chinook::album(20, "B-one", 2),
                                    row: chinook::album(20, "B-one", 3),
                                },
                            )]
                        },
                    },
                    PushScenario {
                        label: "edit_one_child",
                        changes: || {
                            vec![(
                                "Artist",
                                SourceChange::Edit {
                                    old: chinook::artist(2, "Beta"),
                                    row: chinook::artist(2, "Beta-2"),
                                },
                            )]
                        },
                    },
                ]
            },
        },
        Shape {
            label: "album_sibling_fan",
            root: || {
                table("Album")
                    .sub_as("tracks", |a| {
                        table("Track")
                            .r#where("AlbumId", a.col("AlbumId"))
                            .order_by("TrackId", "asc")
                    })
                    .sub_as("artist", |a| {
                        table("Artist").r#where("ArtistId", a.col("ArtistId"))
                    })
            },
            cols: album_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "remove_track",
                        changes: || vec![("Track", SourceChange::Remove(mtrack(105)))],
                    },
                    PushScenario {
                        label: "add_track",
                        changes: || vec![("Track", SourceChange::Add(nt(205, 20, 1, 60_000)))],
                    },
                ]
            },
        },
        Shape {
            label: "playlist_junction",
            root: || {
                table("Playlist").sub_as("playlistTracks", |p| {
                    table("PlaylistTrack")
                        .r#where("PlaylistId", p.col("PlaylistId"))
                        .sub_as("track", |pt| {
                            table("Track").r#where("TrackId", pt.col("TrackId"))
                        })
                        .order_by("TrackId", "asc")
                })
            },
            cols: playlist_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "junction_addrm",
                        changes: || {
                            vec![
                                (
                                    "PlaylistTrack",
                                    SourceChange::Add(chinook::playlist_track(2, 105)),
                                ),
                                (
                                    "PlaylistTrack",
                                    SourceChange::Remove(chinook::playlist_track(1, 100)),
                                ),
                            ]
                        },
                    },
                    PushScenario {
                        label: "leaf_edit",
                        changes: || vec![("Track", edit(mtrack(100), T_COMPOSER, V::str("Re")))],
                    },
                ]
            },
        },
        Shape {
            label: "album_exists_track",
            root: || {
                table("Album").where_exists(|a| {
                    table("Track")
                        .alias("hasTrack")
                        .r#where("AlbumId", a.col("AlbumId"))
                })
            },
            cols: album_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "gate_open",
                        changes: || {
                            vec![
                                ("Album", SourceChange::Add(chinook::album(31, "A-new", 1))),
                                ("Track", SourceChange::Add(nt(203, 31, 1, 120_000))),
                            ]
                        },
                    },
                    PushScenario {
                        label: "gate_close",
                        changes: || {
                            // album 11 has exactly tracks 103,104 — remove both → it leaves.
                            vec![
                                ("Track", SourceChange::Remove(mtrack(103))),
                                ("Track", SourceChange::Remove(mtrack(104))),
                            ]
                        },
                    },
                ]
            },
        },
        Shape {
            label: "genre_not_exists_track",
            root: || {
                table("Genre").where_not_exists(|g| {
                    table("Track")
                        .alias("noTrack")
                        .r#where("GenreId", g.col("GenreId"))
                })
            },
            cols: genre_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "lose_membership",
                        changes: || vec![("Track", SourceChange::Add(nt(204, 10, 4, 120_000)))],
                    },
                    PushScenario {
                        label: "gain_membership",
                        changes: || {
                            // genre 2 has exactly tracks 102,105 — remove both → it enters.
                            vec![
                                ("Track", SourceChange::Remove(mtrack(102))),
                                ("Track", SourceChange::Remove(mtrack(105))),
                            ]
                        },
                    },
                ]
            },
        },
        Shape {
            label: "track_exists_under_or",
            root: || {
                table("Track").where_any(|c| {
                    c.where_op("Milliseconds", ">", 300_000).where_exists(|t| {
                        table("PlaylistTrack")
                            .alias("onPl")
                            .r#where("TrackId", t.col("TrackId"))
                    })
                })
            },
            cols: track_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "enter_via_exists",
                        changes: || {
                            vec![(
                                "PlaylistTrack",
                                SourceChange::Add(chinook::playlist_track(1, 103)),
                            )]
                        },
                    },
                    PushScenario {
                        label: "enter_via_ms",
                        changes: || vec![("Track", edit(mtrack(103), T_MS, V::Float(350_000.0)))],
                    },
                ]
            },
        },
        Shape {
            label: "employee_self_exists",
            root: || {
                table("Employee").where_exists(|e| {
                    table("Employee")
                        .alias("rep")
                        .r#where("ReportsTo", e.col("EmployeeId"))
                })
            },
            cols: employee_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "gain_report",
                        changes: || {
                            vec![(
                                "Employee",
                                SourceChange::Add(chinook::employee(
                                    4,
                                    "Quan",
                                    "Quinn",
                                    Some("Intern"),
                                    Some(3),
                                    Some("SF"),
                                    Some("USA"),
                                )),
                            )]
                        },
                    },
                    PushScenario {
                        label: "lose_report",
                        changes: || {
                            // remove employee 3 (the only report of 2) → 2 leaves the EXISTS view.
                            vec![(
                                "Employee",
                                SourceChange::Remove(chinook::employee(
                                    3,
                                    "Park",
                                    "Peter",
                                    Some("Support"),
                                    Some(2),
                                    Some("SF"),
                                    Some("USA"),
                                )),
                            )]
                        },
                    },
                ]
            },
        },
        Shape {
            label: "customer_invoice_line",
            root: || {
                table("Customer").sub_as("invoices", |c| {
                    table("Invoice")
                        .r#where("CustomerId", c.col("CustomerId"))
                        .sub_as("lines", |i| {
                            table("InvoiceLine")
                                .r#where("InvoiceId", i.col("InvoiceId"))
                                .order_by("InvoiceLineId", "asc")
                        })
                        .order_by("InvoiceId", "asc")
                })
            },
            cols: customer_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "grow_empty",
                        changes: || {
                            vec![
                                (
                                    "Invoice",
                                    SourceChange::Add(chinook::invoice(2000, 3, "2020-09-09", 3.0)),
                                ),
                                (
                                    "InvoiceLine",
                                    SourceChange::Add(chinook::invoice_line(
                                        6000, 2000, 105, 0.99, 1,
                                    )),
                                ),
                            ]
                        },
                    },
                    PushScenario {
                        label: "remove_invoice",
                        changes: || {
                            vec![(
                                "Invoice",
                                SourceChange::Remove(chinook::invoice(1001, 1, "2020-02-01", 5.0)),
                            )]
                        },
                    },
                ]
            },
        },
        // ── new shapes: child-level filter / order / limit + more EXISTS ──────────
        Shape {
            label: "album_tracks_childfilter",
            root: || {
                table("Album").sub_as("tracks", |a| {
                    table("Track")
                        .r#where("AlbumId", a.col("AlbumId"))
                        .where_op("Milliseconds", ">", 150_000)
                        .order_by("TrackId", "asc")
                })
            },
            cols: album_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "child_add_above",
                        changes: || vec![("Track", SourceChange::Add(nt(206, 10, 1, 500_000)))],
                    },
                    PushScenario {
                        label: "child_add_below_filter",
                        changes: || vec![("Track", SourceChange::Add(nt(207, 10, 1, 50_000)))],
                    },
                ]
            },
        },
        Shape {
            label: "album_tracks_childlimit",
            root: || {
                table("Album").sub_as("tracks", |a| {
                    table("Track")
                        .r#where("AlbumId", a.col("AlbumId"))
                        .order_by("Milliseconds", "desc")
                        .limit(2)
                })
            },
            cols: album_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "add_over_limit",
                        changes: || vec![("Track", SourceChange::Add(nt(208, 20, 1, 999_999)))],
                    },
                    PushScenario {
                        label: "remove_visible_refill",
                        // album 20 desc-by-ms: [106(400k),107(350k)] visible, 105(50k) hidden.
                        // removing the visible 106 must refill 105.
                        changes: || vec![("Track", SourceChange::Remove(mtrack(106)))],
                    },
                ]
            },
        },
        Shape {
            label: "album_exists_inner_filter",
            root: || {
                table("Album").where_exists(|a| {
                    table("Track")
                        .alias("longTrack")
                        .r#where("AlbumId", a.col("AlbumId"))
                        .where_op("Milliseconds", ">", 300_000)
                })
            },
            cols: album_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "gate_open_inner",
                        // album 10 has no track >300k; add one → it qualifies.
                        changes: || vec![("Track", SourceChange::Add(nt(209, 10, 1, 350_000)))],
                    },
                    PushScenario {
                        label: "gate_close_inner",
                        // album 20's only >300k tracks are 106,107; remove both → it leaves.
                        changes: || {
                            vec![
                                ("Track", SourceChange::Remove(mtrack(106))),
                                ("Track", SourceChange::Remove(mtrack(107))),
                            ]
                        },
                    },
                ]
            },
        },
        Shape {
            label: "track_two_one_rels",
            root: || {
                table("Track")
                    .sub_as("album", |t| {
                        table("Album").r#where("AlbumId", t.col("AlbumId"))
                    })
                    .sub_as("genre", |t| {
                        table("Genre").r#where("GenreId", t.col("GenreId"))
                    })
            },
            cols: track_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "edit_genre_rel",
                        changes: || vec![("Track", edit(mtrack(100), T_GENRE, V::Float(2.0)))],
                    },
                    PushScenario {
                        label: "edit_album_rel",
                        changes: || vec![("Track", edit(mtrack(100), T_ALBUM, V::Float(20.0)))],
                    },
                ]
            },
        },
        Shape {
            label: "artist_albums_tracks_leaflimit",
            root: || {
                table("Artist").sub_as("albums", |a| {
                    table("Album")
                        .r#where("ArtistId", a.col("ArtistId"))
                        .sub_as("tracks", |al| {
                            table("Track")
                                .r#where("AlbumId", al.col("AlbumId"))
                                .order_by("Milliseconds", "desc")
                                .limit(2)
                        })
                        .order_by("AlbumId", "asc")
                })
            },
            cols: artist_col(),
            scenarios: || {
                vec![
                    PushScenario {
                        label: "leaf_add_over",
                        changes: || vec![("Track", SourceChange::Add(nt(210, 20, 1, 999_999)))],
                    },
                    PushScenario {
                        label: "leaf_remove_visible",
                        changes: || vec![("Track", SourceChange::Remove(mtrack(106)))],
                    },
                ]
            },
        },
    ]
}

/// `SourceChange::Edit` from an exact old row + a single-field change.
fn edit(old: OwnedRow, idx: usize, v: V) -> SourceChange {
    let row = edit_field(old.clone(), idx, v);
    SourceChange::Edit { old, row }
}

// ── axis 2: filter (on the root) ─────────────────────────────────────────────────

type FilterMod = (&'static str, fn(Query, &RootCols) -> Query);

fn filters() -> Vec<FilterMod> {
    vec![
        ("none", |q, _| q),
        ("eq", |q, c| q.where_op(c.num, "==", c.num_mid)),
        ("ne", |q, c| q.where_op(c.num, "!=", c.num_mid)),
        ("lt", |q, c| q.where_op(c.num, "<", c.num_mid)),
        ("le", |q, c| q.where_op(c.num, "<=", c.num_mid)),
        ("gt", |q, c| q.where_op(c.num, ">", c.num_mid)),
        ("ge", |q, c| q.where_op(c.num, ">=", c.num_mid)),
        ("in", |q, c| q.where_in(c.num, c.in_set)),
        ("like", |q, c| {
            q.where_op(c.txt, "LIKE", format!("{}%", c.like_prefix))
        }),
        ("ilike", |q, c| {
            q.where_op(c.txt, "ILIKE", format!("{}%", c.ilike_prefix))
        }),
        ("not_like", |q, c| {
            q.where_op(c.txt, "NOT LIKE", format!("{}%", c.like_prefix))
        }),
        ("is_null", |q, c| q.where_op(c.nullable, "IS", None::<&str>)),
        ("is_not_null", |q, c| {
            q.where_op(c.nullable, "IS NOT", None::<&str>)
        }),
        ("and2", |q, c| {
            q.where_op(c.num, "!=", c.num_mid).where_op(
                c.txt,
                "LIKE",
                format!("{}%", c.like_prefix),
            )
        }),
        ("or2", |q, c| {
            q.where_any(|cc| {
                cc.where_op(c.num, "==", c.num_mid)
                    .where_op(c.num, ">", c.num_mid)
            })
        }),
        ("nested3", |q, c| {
            // num != in_set[0]  AND  (num == mid  OR  txt LIKE prefix%)
            q.where_op(c.num, "!=", c.in_set[0]).where_any(|cc| {
                cc.where_op(c.num, "==", c.num_mid).where_op(
                    c.txt,
                    "LIKE",
                    format!("{}%", c.like_prefix),
                )
            })
        }),
    ]
}

/// Representative filters for matrix B (push × filter) — kept small to bound the cross.
const B_FILTERS: &[&str] = &["none", "gt", "like", "is_null"];

// ── axis 3: order / limit (on the root) ──────────────────────────────────────────

type OrderMod = (&'static str, fn(Query, &RootCols) -> Query);

fn order_limits() -> Vec<OrderMod> {
    vec![
        ("pk", |q, _| q), // PK order via complete_ordering
        ("desc", |q, c| q.order_by(c.order_col, "desc")),
        ("limit3", |q, _| q.limit(3)),
        ("ord_limit2", |q, c| q.order_by(c.order_col, "asc").limit(2)),
        ("multi", |q, c| {
            q.order_by(c.order_col, "asc").order_by(c.num, "desc")
        }),
    ]
}

// ── the matrix ───────────────────────────────────────────────────────────────────

const SHOW_FAILURES: usize = 12;

#[test]
fn generated_matrix() {
    let src = mini::sources();
    let cat = chinook::catalog();

    let mut total = 0usize;
    let mut failures: Vec<(String, String)> = Vec::new();

    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let mut run = |label: String, ast: &rindle::Ast, pushes: Vec<Push>| {
        total += 1;
        let res = catch_unwind(AssertUnwindSafe(|| {
            assert_flip_invariant_push(&src, &cat, ast, pushes)
        }));
        if let Err(payload) = res {
            let msg: String = panic_message(&*payload).chars().take(900).collect();
            failures.push((label, msg));
        }
    };

    // Matrix A: shape × filter × order (each shape's primary push).
    for shape in shapes() {
        let primary = (shape.scenarios)().swap_remove(0);
        for (flabel, fmod) in filters() {
            for (olabel, omod) in order_limits() {
                let ast = omod(fmod((shape.root)(), &shape.cols), &shape.cols).build();
                run(
                    format!("A|{}|{flabel}|{olabel}|{}", shape.label, primary.label),
                    &ast,
                    (primary.changes)(),
                );
            }
        }
    }

    // Matrix B: shape × push-scenario × filter-subset (PK order).
    for shape in shapes() {
        for scen in (shape.scenarios)() {
            for (flabel, fmod) in filters() {
                if !B_FILTERS.contains(&flabel) {
                    continue;
                }
                let ast = fmod((shape.root)(), &shape.cols).build();
                run(
                    format!("B|{}|{}|{flabel}", shape.label, scen.label),
                    &ast,
                    (scen.changes)(),
                );
            }
        }
    }

    std::panic::set_hook(prev);

    if !failures.is_empty() {
        let all_labels = failures
            .iter()
            .map(|(l, _)| l.as_str())
            .collect::<Vec<_>>()
            .join("\n  ");
        let shown = failures
            .iter()
            .take(SHOW_FAILURES)
            .map(|(l, m)| format!("CASE {l}\n{m}"))
            .collect::<Vec<_>>()
            .join("\n\n========================================\n\n");
        let extra = failures.len().saturating_sub(SHOW_FAILURES);
        let more = if extra > 0 {
            format!("\n\n... and {extra} more (see the label list above)")
        } else {
            String::new()
        };
        panic!(
            "{}/{} generated cases failed.\n\nALL FAILING LABELS:\n  {all_labels}\n\n\
             FIRST {SHOW_FAILURES} DIFFS:\n\n{shown}{more}",
            failures.len(),
            total
        );
    }

    eprintln!("generated_matrix: {total} cases passed");
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else {
        "<non-string panic payload>".to_string()
    }
}
