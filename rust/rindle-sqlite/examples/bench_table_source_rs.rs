//! Rust side of the **TableSource** (SQLite-backed leaf) Rust-vs-JS head-to-head.
//! Peer of `examples/bench_memory_source_rs.rs`, but over the `rusqlite`-backed
//! [`TableSource`] (`src/table_source.rs` + `src/sqlite.rs`) instead of the COW
//! B+tree `MemorySource`. Mirrors `tools/bench_table_source_js.ts`
//! operation-for-operation, with the IDENTICAL timing harness as the other
//! benches (~200 ms warmup, then min ns/op over 6 rounds).
//!
//!   cargo run -p rindle-sqlite --release --example bench_table_source_rs --features fast-alloc
//!
//! Both contestants are backed by an **in-memory** SQLite db (`:memory:`) over the
//! SAME schema + indexes, and issue equivalent SQL, so the comparison isolates the
//! leaf layer: rusqlite's zero-copy lending cursor vs the better-sqlite3 binding +
//! the JS `fromSQLiteTypes` retyping pass.
//!
//! Data: 1000 rows `(id, owner, priority)`, number columns (vended as f64 — the JS
//! `number` is f64), PK = id, `owner = id % 10` (one owner ~100 rows; `owner IN
//! {1,3,5}` ~300). Rows are seeded ONCE outside the timed loops.

use std::hint::black_box;
use std::rc::Rc;
use std::time::Instant;

use rindle::change::{Basis, Start};
use rindle::{
    owned_row as orow, ColId, ConnId, FetchRequest, Node, NodeId, OutEdge, OwnedValue::Float, Port,
    Source, SourceChange, ValueType,
};
use rindle_sqlite::{ColumnDef, TableSource};
use rusqlite::{params, Connection};

#[cfg(feature = "fast-alloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const N: i64 = 1000;
const ID: ColId = 0;
const OWNER: ColId = 1;
const PRIORITY: ColId = 2;

/// (id, owner, priority) — all number columns, built as f64 (the leaf vends a
/// `number` column as `Float`; mixing `Int`/`Float` for one column would trip the
/// `compare_values` type-mismatch guard).
fn row3(id: i64) -> rindle::OwnedRow {
    orow(vec![
        Float(id as f64),
        Float((id % 10) as f64),
        Float(id as f64),
    ])
}

fn id_of(row: &rindle::OwnedRow) -> i64 {
    match row.col(ID) {
        rindle::value::Value::Float(f) => f as i64,
        _ => 0,
    }
}

fn columns() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: "id".into(),
            ty: ValueType::Number,
            optional: false,
        },
        ColumnDef {
            name: "owner".into(),
            ty: ValueType::Number,
            optional: true,
        },
        ColumnDef {
            name: "priority".into(),
            ty: ValueType::Number,
            optional: false,
        },
    ]
}

/// Build an in-memory SQLite db with the shared schema + indexes and seed `N`
/// rows, returning a `TableSource` over it.
fn build_source() -> TableSource {
    let conn = Rc::new(Connection::open_in_memory().unwrap());
    conn.execute_batch(
        "CREATE TABLE t (id REAL NOT NULL, owner REAL, priority REAL NOT NULL);
         CREATE UNIQUE INDEX t_pk ON t (id);
         CREATE INDEX t_owner ON t (owner, id);",
    )
    .unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO t (id, owner, priority) VALUES (?1, ?2, ?3)")
            .unwrap();
        for id in 0..N {
            stmt.execute(params![id as f64, (id % 10) as f64, id as f64])
                .unwrap();
        }
    }
    TableSource::new(conn, "t", columns(), vec![ID])
}

fn bench(name: &str, mut body: impl FnMut()) {
    let warm = Instant::now();
    while warm.elapsed().as_nanos() < 200_000_000 {
        body();
    }
    let mut best = f64::INFINITY;
    for _ in 0..6 {
        let mut iters: u64 = 1;
        loop {
            let start = Instant::now();
            for _ in 0..iters {
                body();
            }
            let dt = start.elapsed().as_nanos() as f64;
            if dt >= 100_000_000.0 {
                best = best.min(dt / iters as f64);
                break;
            }
            iters *= 2;
        }
    }
    println!("{name}|{best:.1}");
}

/// Drain a connection-level fetch (rows wrapped into leaf nodes, as the
/// `SourceConn` boundary does) and sum the id column — touches one cell per row.
fn fetch_sum(ts: &TableSource, conn: ConnId, req: &FetchRequest) -> i64 {
    ts.fetch(conn, req)
        .map(Node::leaf)
        .map(|n| id_of(&n.row))
        .sum()
}

fn main() {
    // Read source: connect once (sorted by id). The constraint/multi benches reuse
    // the (owner, id) index via SQL.
    let read = build_source();
    let conn = read.connect(Some(vec![(ID, true)]), None, vec![]);
    let pconn = read.connect(Some(vec![(PRIORITY, false), (ID, true)]), None, vec![]);

    let owner5 = FetchRequest::with_constraint(vec![(OWNER, Float(5.0))]);
    let rev = FetchRequest {
        reverse: true,
        ..Default::default()
    };
    let from_mid = FetchRequest {
        start: Some(Start {
            row: row3(500),
            basis: Basis::At,
        }),
        ..Default::default()
    };
    let multi = FetchRequest {
        multi_constraints: vec![vec![
            vec![(OWNER, Float(1.0))],
            vec![(OWNER, Float(3.0))],
            vec![(OWNER, Float(5.0))],
        ]],
        ..Default::default()
    };

    bench("fetch_scan_full", || {
        black_box(fetch_sum(&read, conn, &FetchRequest::all()));
    });
    bench("fetch_constraint_100", || {
        black_box(fetch_sum(&read, conn, &owner5));
    });
    bench("fetch_reverse_full", || {
        black_box(fetch_sum(&read, conn, &rev));
    });
    bench("fetch_start_mid", || {
        black_box(fetch_sum(&read, conn, &from_mid));
    });
    bench("fetch_multi_in3", || {
        black_box(fetch_sum(&read, conn, &multi));
    });
    // Ad-hoc secondary sort (priority desc, id asc) — no covering index, so SQLite
    // sorts. Peer of the memory bench's "new sort" op.
    bench("fetch_sort_priority", || {
        black_box(fetch_sum(&read, pconn, &FetchRequest::all()));
    });

    // Write source: a fresh source, one connection with a no-op output (so the
    // fan-out + per-connection node build run, mirroring the JS write bench).
    let write = build_source();
    let wconn = write.connect(Some(vec![(ID, true)]), None, vec![]);
    write.set_conn_output(
        wconn,
        OutEdge {
            node: NodeId::new(0, 0),
            port: Port::Single,
        },
    );
    let push_one = |_c: &rindle::Connection, sc: SourceChange| {
        let node: Node<'static> = match sc {
            SourceChange::Add(r) | SourceChange::Remove(r) => Node::leaf(r),
            SourceChange::Edit { row, .. } => Node::leaf(row),
        };
        black_box(id_of(&node.row));
    };

    let sentinel = row3(N); // id = 1000, not in the source
    bench("push_add_remove", || {
        write.push(SourceChange::Add(sentinel.clone()), &push_one);
        write.push(SourceChange::Remove(sentinel.clone()), &push_one);
    });

    let id1_a = row3(1);
    let id1_b = orow(vec![Float(1.0), Float(1.0), Float(999.0)]);
    bench("push_edit_roundtrip", || {
        write.push(
            SourceChange::Edit {
                row: id1_b.clone(),
                old: id1_a.clone(),
            },
            &push_one,
        );
        write.push(
            SourceChange::Edit {
                row: id1_a.clone(),
                old: id1_b.clone(),
            },
            &push_one,
        );
    });
}
