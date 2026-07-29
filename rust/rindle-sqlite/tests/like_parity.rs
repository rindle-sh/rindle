//! WS05.1 — LIKE/ILIKE escape parity between the in-memory `LikeMatcher` and real
//! SQLite. For each (text, pattern) pair we compare `LikeMatcher::matches` against
//! what SQLite returns for `text LIKE pattern ESCAPE '\'` (the exact SQL the builder
//! emits), under both case-sensitive `LIKE` and ASCII-folded `ILIKE`. They must
//! agree for every escaped-wildcard pattern — otherwise the same query gives
//! different results on the memory vs sqlite backend (a silent correctness bug for
//! an IVM engine whose contract is view == fresh query).

use rindle::LikeMatcher;
use rusqlite::{params, Connection};

/// Ground truth: SQLite's own `LIKE`/`ILIKE` with the builder's `ESCAPE '\'`.
fn sqlite_like(conn: &Connection, text: &str, pattern: &str, case_insensitive: bool) -> bool {
    // `ILIKE` is lowered as `lower(col) LIKE lower(pat)` (query_builder.rs); bare
    // `LIKE` runs case-sensitive because the leaf sets `case_sensitive_like = ON`.
    let sql = if case_insensitive {
        r"SELECT lower(?1) LIKE lower(?2) ESCAPE '\'"
    } else {
        r"SELECT ?1 LIKE ?2 ESCAPE '\'"
    };
    let n: i64 = conn
        .query_row(sql, params![text, pattern], |r| r.get(0))
        .expect("sqlite LIKE");
    n == 1
}

fn mem_like(text: &str, pattern: &str, case_insensitive: bool) -> bool {
    let m = if case_insensitive {
        LikeMatcher::compile_case_insensitive(pattern.as_bytes())
    } else {
        LikeMatcher::compile(pattern.as_bytes())
    };
    m.matches(text.as_bytes())
}

#[test]
fn like_escape_parity_memory_vs_sqlite() {
    let conn = Connection::open_in_memory().expect("open sqlite");
    // Mirror the TableSource leaf's collation (src/table_source.rs:172).
    conn.execute_batch("PRAGMA case_sensitive_like = ON;")
        .expect("set case_sensitive_like");

    // Every pattern uses only the WELL-DEFINED escapes (`\%`, `\_`, `\\`) + plain
    // wildcards (sqlite leaves `\x` for other x undefined, so we don't assert on it).
    let cases: &[(&str, &str)] = &[
        // escaped `%` is a literal percent
        ("100%", r"100\%"),
        ("100x", r"100\%"),
        ("a%b", r"a\%b"),
        ("axb", r"a\%b"),
        // escaped `_` is a literal underscore
        ("a_b", r"a\_b"),
        ("axb", r"a\_b"),
        // escaped backslash is a literal backslash
        (r"a\b", r"a\\b"),
        (r"a\c", r"a\\b"),
        // escaped wildcard next to a real one must NOT collapse
        ("a%b", r"%\%%"),
        ("ab", r"%\%%"),
        ("50% off!", r"%\%%"),
        ("literal 100% then more", r"100\%%"),
        ("100%", r"100\%%"),
        // plain wildcards (unchanged behavior)
        ("hello", "h%o"),
        ("hello", "h_llo"),
        ("hello", "hell_"),
        ("hello", "%"),
        ("", "%"),
        ("hello", "x%"),
        // case sensitivity: differs between LIKE and ILIKE
        ("HELLO", "hello"),
        ("HeLLo", "h%o"),
        ("ABC", r"abc\%"),
        ("ABC%", r"abc\%"),
    ];

    let mut checked = 0;
    for &(text, pat) in cases {
        for case_insensitive in [false, true] {
            let want = sqlite_like(&conn, text, pat, case_insensitive);
            let got = mem_like(text, pat, case_insensitive);
            assert_eq!(
                got, want,
                "LIKE parity mismatch: text={text:?} pattern={pat:?} ci={case_insensitive} \
                 (sqlite={want}, memory={got})"
            );
            checked += 1;
        }
    }
    assert_eq!(checked, cases.len() * 2);
}
