//! Smoke test for the SQLite `scanstatus` API (planner Stage 8 infra de-risk).
//!
//! `sqlite3_stmt_scanstatus_v2` only returns data when the amalgamation is compiled
//! with `-DSQLITE_ENABLE_STMT_SCANSTATUS` (added to the vendored `build.rs`). This
//! proves, via raw FFI through the rusqlite db handle, that we can read the planner's
//! per-loop row estimates + EXPLAIN text from a prepared single-table SELECT — the
//! exact inputs the cost model needs.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};

use rusqlite::{ffi, Connection};

#[derive(Debug)]
struct Loop {
    select_id: i32,
    parent_id: i32,
    est: f64,
    explain: String,
}

/// Raw-prepare `sql`, read all scanstatus loops (SQLITE_SCANSTAT_COMPLEX), finalize.
unsafe fn read_scanstatus_loops(conn: &Connection, sql: &str) -> Vec<Loop> {
    let db = conn.handle();
    let csql = CString::new(sql).unwrap();
    let mut stmt: *mut ffi::sqlite3_stmt = std::ptr::null_mut();
    let rc = ffi::sqlite3_prepare_v2(db, csql.as_ptr(), -1, &mut stmt, std::ptr::null_mut());
    assert_eq!(rc, ffi::SQLITE_OK, "prepare failed for {sql}");

    let flags = ffi::SQLITE_SCANSTAT_COMPLEX as c_int;
    let mut out = Vec::new();
    let mut idx: c_int = 0;
    loop {
        let mut select_id: c_int = 0;
        let rc = ffi::sqlite3_stmt_scanstatus_v2(
            stmt,
            idx,
            ffi::SQLITE_SCANSTAT_SELECTID as c_int,
            flags,
            &mut select_id as *mut c_int as *mut c_void,
        );
        if rc != 0 {
            break; // idx out of range → end of loops
        }
        let mut parent_id: c_int = 0;
        ffi::sqlite3_stmt_scanstatus_v2(
            stmt,
            idx,
            ffi::SQLITE_SCANSTAT_PARENTID as c_int,
            flags,
            &mut parent_id as *mut c_int as *mut c_void,
        );
        let mut est: f64 = 0.0;
        ffi::sqlite3_stmt_scanstatus_v2(
            stmt,
            idx,
            ffi::SQLITE_SCANSTAT_EST as c_int,
            flags,
            &mut est as *mut f64 as *mut c_void,
        );
        let mut explain_ptr: *const c_char = std::ptr::null();
        ffi::sqlite3_stmt_scanstatus_v2(
            stmt,
            idx,
            ffi::SQLITE_SCANSTAT_EXPLAIN as c_int,
            flags,
            &mut explain_ptr as *mut *const c_char as *mut c_void,
        );
        let explain = if explain_ptr.is_null() {
            String::new()
        } else {
            CStr::from_ptr(explain_ptr).to_string_lossy().into_owned()
        };

        out.push(Loop {
            select_id,
            parent_id,
            est,
            explain,
        });
        idx += 1;
    }
    ffi::sqlite3_finalize(stmt);
    out
}

#[test]
fn scanstatus_returns_planner_estimates() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v INTEGER);
         CREATE INDEX t_k ON t(k);",
    )
    .unwrap();
    // ~200 rows, ~10 distinct k values.
    for i in 0..200 {
        conn.execute("INSERT INTO t (id, k, v) VALUES (?, ?, ?)", [i, i % 10, i])
            .unwrap();
    }
    conn.execute_batch("ANALYZE;").unwrap();

    // A scan on the indexed column + an ORDER BY (so a sort loop appears).
    let mut loops =
        unsafe { read_scanstatus_loops(&conn, "SELECT id, k, v FROM t WHERE k = 5 ORDER BY v") };
    // The cost model processes loops in execution order (the same sort it uses).
    loops.sort_by_key(|l| l.select_id);

    assert!(
        !loops.is_empty(),
        "scanstatus returned no loops — is SQLITE_ENABLE_STMT_SCANSTATUS compiled in?"
    );
    // At least one top-level loop with a positive row estimate.
    let top: Vec<&Loop> = loops.iter().filter(|l| l.parent_id == 0).collect();
    assert!(
        !top.is_empty(),
        "no top-level (parentId=0) loops: {loops:?}"
    );
    assert!(
        top.iter().any(|l| l.est > 0.0),
        "expected a positive row estimate: {loops:?}"
    );
    // The ORDER BY should surface as a sort loop in the EXPLAIN text.
    assert!(
        loops.iter().any(|l| l.explain.contains("ORDER BY")),
        "expected an ORDER BY loop in {loops:?}"
    );
}
