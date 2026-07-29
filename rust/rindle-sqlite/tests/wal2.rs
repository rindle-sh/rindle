//! Bedrock backend regression guard (wal2 + BEGIN CONCURRENT + hct_isolation).
//!
//! The crate links a vendored **`bedrock`-branch** SQLite **3.54.0** amalgamation
//! in place of rusqlite's stock 3.46 `bundled` source (see `docs/SQLITE_WAL2.md`
//! and the `[patch.crates-io]` in `Cargo.toml`). `bedrock` combines **wal2**
//! (dual-WAL) and **BEGIN CONCURRENT** (optimistic concurrent writers — required
//! by the rindle-replica write-then-abort model). These tests fail loudly if that
//! wiring regresses — e.g. the patch is dropped, or the amalgamation is
//! regenerated from a plain `wal2` (no begin-concurrent) or wrong-version SQLite.
//! They run on every `cargo test --features sqlite` lane, so CI needs no special
//! configuration.

use rusqlite::Connection;

/// `PRAGMA journal_mode=wal2` is only honored by a wal2-branch build. Stock SQLite
/// does not recognize the `wal2` mode and leaves the connection in its prior mode,
/// so the returned mode string would be `delete`, not `wal2`.
#[test]
fn journal_mode_wal2_is_available() {
    let path = std::env::temp_dir().join(format!("zql_wal2_{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let mode: String = {
        let conn = Connection::open(&path).unwrap();
        // A fresh DB defaults to rollback (`delete`) mode, so the documented
        // `wal -> delete -> wal2` dance is unnecessary here: switch straight in.
        conn.query_row("PRAGMA journal_mode=wal2;", [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(
        mode, "wal2",
        "vendored SQLite is not a wal2 build — the `[patch.crates-io]` redirect to \
         vendor/libsqlite3-sys is missing or the amalgamation was regenerated from \
         a non-wal2 source"
    );

    // wal2 keeps two write-ahead logs alongside the db; clean them all up.
    for suffix in ["", "-wal", "-wal2", "-shm"] {
        let _ = std::fs::remove_file(
            std::env::temp_dir().join(format!("zql_wal2_{}.db{suffix}", std::process::id())),
        );
    }
}

/// The runtime library is the 3.54 wal2 line, not the stock 3.46 amalgamation it
/// replaced. (Note: `rusqlite::version()` reports 3.46 because we deliberately
/// keep the ABI-compatible prebuilt bindings — only the *runtime* `sqlite_version()`
/// reflects the linked C source. See `docs/SQLITE_WAL2.md`.)
#[test]
fn links_wal2_3_54_runtime() {
    let conn = Connection::open_in_memory().unwrap();
    let v: String = conn
        .query_row("SELECT sqlite_version()", [], |r| r.get(0))
        .unwrap();
    let mut parts = v.split('.').map(|p| p.parse::<u32>().unwrap_or(0));
    let (major, minor) = (parts.next().unwrap_or(0), parts.next().unwrap_or(0));
    assert!(
        (major, minor) >= (3, 54),
        "expected the vendored wal2 3.54+ amalgamation, got runtime sqlite_version() = {v}"
    );
}

/// `BEGIN CONCURRENT` is only recognized by a begin-concurrent (`bedrock`) build.
/// A plain `wal2` build (the previous vendored source) raises a syntax error at
/// `CONCURRENT`, so this `execute_batch` would fail. This is the M0 regression
/// guard for the rindle-replica write-then-abort model, which requires overlapping
/// concurrent write transactions (the pipeline writes throwaway rows on a
/// pre-commit snapshot, then rolls back).
#[test]
fn begin_concurrent_is_available() {
    let base = format!("rindle_bc_{}.db", std::process::id());
    let cleanup = || {
        for suffix in ["", "-wal", "-wal2", "-shm"] {
            let _ = std::fs::remove_file(std::env::temp_dir().join(format!("{base}{suffix}")));
        }
    };
    cleanup();
    let path = std::env::temp_dir().join(&base);

    // BEGIN CONCURRENT requires WAL/wal2 mode; a file-backed wal2 DB provides it.
    let conn = Connection::open(&path).unwrap();
    let mode: String = conn
        .query_row("PRAGMA journal_mode=wal2;", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal2");
    conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);")
        .unwrap();

    // The proof: the statement parses and runs. On a non-begin-concurrent build
    // this errors with "near \"CONCURRENT\": syntax error".
    conn.execute_batch("BEGIN CONCURRENT;\nINSERT INTO t(id, v) VALUES (1, 'a');\nCOMMIT;")
        .expect(
            "BEGIN CONCURRENT must parse + execute — the vendored amalgamation is not a \
             begin-concurrent (`bedrock`) build; regenerate via \
             scripts/regen-bedrock-amalgamation.sh from a `bedrock` checkout",
        );

    let n: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);
    drop(conn);
    cleanup();
}

/// `PRAGMA hct_isolation` (snapshot isolation / repeatable read) exists only
/// because the pinned fossil branch (`hct-repeatable-read-2`) carries the two
/// cherry-picked repeatable-read commits on top of upstream `hctree-bedrock` —
/// see `scripts/regen-hctree-bedrock-amalgamation.sh`. This fails loudly if the
/// amalgamation is ever regenerated from a checkout without them. The pragma is
/// hctree-specific, so the db is created in hctree format (`hctree=1`), which
/// also keeps `-pagemap`/`-log-N` sidecars — hence the per-test directory.
#[test]
fn hct_isolation_pragma_is_available() {
    let dir = std::env::temp_dir().join(format!("rindle_hctiso_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    {
        let path = dir.join("iso.db");
        let conn = Connection::open(format!("file:{}?hctree=1", path.display())).unwrap();

        let level: String = conn
            .query_row("PRAGMA hct_isolation", [], |r| r.get(0))
            .expect(
                "PRAGMA hct_isolation must exist — the vendored amalgamation was \
                 regenerated without the repeatable-read commits; re-run \
                 scripts/regen-hctree-bedrock-amalgamation.sh against the pinned \
                 hct-repeatable-read-2 checkout",
            );
        assert_eq!(
            level, "serializable",
            "connection default must be serializable (SQLITE_HCT_DEFAULT_ISOLATION_SNAPSHOT unset)"
        );

        let level: String = conn
            .query_row("PRAGMA hct_isolation = snapshot", [], |r| r.get(0))
            .unwrap();
        assert_eq!(level, "snapshot");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
