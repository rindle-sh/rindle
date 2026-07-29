//! Throwaway diagnostic: a persistent prepared statement reprepares on EVERY step
//! here, with no schema change. Nail the trigger: compile options + a ladder that
//! discriminates SELECT-1 / literal-WHERE / bound-param / varying-param / post-ANALYZE.
//!   cargo run -p rindle-sqlite --release --example reprepare_repro

use std::time::Instant;

use rusqlite::config::DbConfig;
use rusqlite::{Connection, StatementStatus};

fn open_wal2(path: &str) -> Connection {
    let c = Connection::open(path).unwrap();
    let m: String = c
        .query_row("PRAGMA journal_mode=wal2", [], |r| r.get(0))
        .unwrap();
    assert!(m.eq_ignore_ascii_case("wal2"), "got {m:?}");
    c.execute_batch("PRAGMA synchronous=NORMAL; PRAGMA case_sensitive_like=ON;")
        .unwrap();
    c
}

fn conn(name: &str) -> (Connection, tempdir::Guard) {
    let dir = std::env::temp_dir().join(format!("rp_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ps = dir.join("db.sqlite").to_str().unwrap().to_string();
    let r = open_wal2(&ps);
    r.execute_batch(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, k INTEGER, v INTEGER);
         CREATE INDEX ix_t_k ON t(k);
         INSERT INTO t(id,k,v) VALUES (1,10,0),(2,20,0),(3,30,0),(4,40,0);",
    )
    .unwrap();
    (r, tempdir::Guard(dir))
}
mod tempdir {
    pub struct Guard(pub std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

/// Persistent statement, stepped `n` times; bind value from `key(i)` (None ⇒ no params).
fn persistent(
    label: &str,
    c: &Connection,
    sql: &str,
    n: usize,
    key: impl Fn(usize) -> Option<i64>,
) {
    let mut st = c.prepare(sql).unwrap();
    let r0 = st.get_status(StatementStatus::RePrepare);
    let t = Instant::now();
    for i in 0..n {
        match key(i) {
            Some(k) => {
                let mut rows = st.query([k]).unwrap();
                while rows.next().unwrap().is_some() {}
            }
            None => {
                let mut rows = st.query([]).unwrap();
                while rows.next().unwrap().is_some() {}
            }
        }
    }
    let us = t.elapsed().as_micros() as f64 / n as f64;
    let dr = st.get_status(StatementStatus::RePrepare) - r0;
    println!("{label:<40} reprepares={dr:<6} {us:7.2} us/iter");
}

fn main() {
    let n = 4000;
    let (c, _g) = conn("main");
    let opts: Vec<String> = {
        let mut s = c.prepare("PRAGMA compile_options").unwrap();
        let rows = s.query_map([], |r| r.get::<_, String>(0)).unwrap();
        rows.filter_map(Result::ok).collect()
    };
    let interesting: Vec<&String> = opts
        .iter()
        .filter(|o| {
            let u = o.to_uppercase();
            u.contains("STAT")
                || u.contains("REPREPARE")
                || u.contains("CONCURRENT")
                || u.contains("WAL")
        })
        .collect();
    println!("compile_options (STAT/WAL/CONCURRENT): {interesting:?}\n");
    println!("iters={n}\n");

    // THE FIX: QPSG on ⇒ planner ignores bound-param values ⇒ no expmask ⇒ no reprepare.
    {
        let (q, _g) = conn("qpsg");
        q.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_QPSG, true)
            .unwrap();
        persistent(
            "QPSG_bound_param_same",
            &q,
            "SELECT id,k,v FROM t WHERE k=?1",
            n,
            |_| Some(20),
        );
        persistent(
            "QPSG_bound_param_varying",
            &q,
            "SELECT id,k,v FROM t WHERE k=?1",
            n,
            |i| Some([10, 20, 30, 40][i % 4]),
        );
    }

    persistent("A_SELECT_1_no_table", &c, "SELECT 1", n, |_| None);
    persistent(
        "B_literal_where_no_param",
        &c,
        "SELECT id,k,v FROM t WHERE k=20",
        n,
        |_| None,
    );
    persistent(
        "C_bound_param_same_value",
        &c,
        "SELECT id,k,v FROM t WHERE k=?1",
        n,
        |_| Some(20),
    );
    persistent(
        "D_bound_param_varying",
        &c,
        "SELECT id,k,v FROM t WHERE k=?1",
        n,
        |i| Some([10, 20, 30, 40][i % 4]),
    );
    persistent(
        "E_pk_lookup_param",
        &c,
        "SELECT id,k,v FROM t WHERE id=?1",
        n,
        |_| Some(2),
    );
    persistent(
        "F_full_scan_no_where",
        &c,
        "SELECT id,k,v FROM t",
        n,
        |_| None,
    );

    // Now ANALYZE (populate stats) and repeat the param seek — does stat data change it?
    c.execute_batch("ANALYZE").unwrap();
    println!("\n-- after ANALYZE --");
    persistent(
        "C2_bound_param_same_after_analyze",
        &c,
        "SELECT id,k,v FROM t WHERE k=?1",
        n,
        |_| Some(20),
    );
    persistent(
        "D2_bound_param_varying_after_analyze",
        &c,
        "SELECT id,k,v FROM t WHERE k=?1",
        n,
        |i| Some([10, 20, 30, 40][i % 4]),
    );
    persistent("A2_SELECT_1_after_analyze", &c, "SELECT 1", n, |_| None);
    persistent(
        "G_nonindexed_col_param",
        &c,
        "SELECT id,k,v FROM t WHERE v=?1",
        n,
        |_| Some(0),
    );
    // Literal-embedded seek via prepare_cached, varying key -> cache-miss parse per distinct key.
    {
        let keys = [10i64, 20, 30, 40];
        let mut misses = 0u64;
        let t = std::time::Instant::now();
        for i in 0..n {
            let k = keys[i % 4];
            let sql = format!("SELECT id,k,v FROM t WHERE k={k}");
            let before = c.prepare_cached(&sql).is_ok();
            let mut st = c.prepare_cached(&sql).unwrap();
            let _ = before;
            let mut rows = st.query([]).unwrap();
            while rows.next().unwrap().is_some() {}
            let _ = &mut misses;
        }
        let us = t.elapsed().as_micros() as f64 / n as f64;
        println!(
            "{:<40} (literal+cached, 4 distinct keys)         {us:7.2} us/iter",
            "H_literal_cached_varying"
        );
    }
}
