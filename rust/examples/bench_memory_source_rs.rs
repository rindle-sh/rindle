//! Rust side of the **MemorySource** Rust-vs-JS head-to-head. Mirrors
//! `tools/bench_memory_source_js.ts` operation-for-operation, with the IDENTICAL
//! timing harness as the btree bench (`examples/bench_rs.rs`): ~200 ms warmup,
//! then the min ns/op over 6 rounds.
//!
//!   cargo run --release --example bench_memory_source_rs --features fast-alloc
//!
//! Data: 1000 rows `(id, owner, priority)`, PK = id, `owner = id % 10` (so a
//! single owner selects ~100 rows; `owner IN {1,3,5}` ~300). Rows are pre-built
//! ONCE outside the timed loops, so we measure *source* work, not row allocation.
//!
//! Fairness: the read path is compared at the **connection level** — the Rust
//! source emits rows, so each bench wraps them into leaf `Node`s (`Node::leaf`)
//! exactly as the `SourceConn` boundary does, matching the JS source which yields
//! nodes. The write path fans out to one connection with a no-op output (so
//! `filter_push` + the per-connection node build run on both sides), writing to a
//! source that holds only the primary index.

use std::hint::black_box;
use std::time::Instant;

use rindle::change::{Basis, Start};
use rindle::{
    owned_row as orow, ConnId, FetchRequest, MemorySource, Node, NodeId, OutEdge, OwnedValue::Int,
    Port, Schema, Source, SourceChange,
};

#[cfg(feature = "fast-alloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const N: i64 = 1000;

fn schema() -> Schema {
    Schema::new(vec!["id", "owner", "priority"], vec![0], vec![(0, true)])
}

fn row3(id: i64) -> rindle::OwnedRow {
    orow(vec![Int(id), Int(id % 10), Int(id)])
}

fn id_of(row: &rindle::OwnedRow) -> i64 {
    match row.col(0) {
        rindle::value::Value::Int(i) => i,
        _ => 0,
    }
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
fn fetch_sum(ms: &MemorySource, conn: ConnId, req: &FetchRequest) -> i64 {
    ms.fetch(conn, req)
        .map(Node::leaf)
        .map(|n| id_of(&n.row))
        .sum()
}

fn main() {
    let rows: Vec<rindle::OwnedRow> = (0..N).map(row3).collect();

    // Read source: connect once; the constraint/multi benches lazily build the
    // (owner, id) index, then reuse it.
    let read = MemorySource::new(schema(), rows.clone());
    let conn = read.connect(Some(vec![(0, true)]), None, vec![]);

    let owner5 = FetchRequest::with_constraint(vec![(1, Int(5))]);
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
            vec![(1, Int(1))],
            vec![(1, Int(3))],
            vec![(1, Int(5))],
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

    // Index build: fork (O(1) primary share) + connect a NEW sort + fetch, so the
    // lazy getOrCreateIndex builds a fresh secondary from the primary each round.
    let desc_sort = vec![(2, false), (0, true)]; // priority desc, id asc
    bench("fetch_new_sort_build", || {
        let f = read.fork();
        let c = f.connect(Some(desc_sort.clone()), None, vec![]);
        black_box(fetch_sum(&f, c, &FetchRequest::all()));
    });

    // Write source: a fresh source holding ONLY the primary index, one connection
    // with a no-op output (so the fan-out + per-connection node build run).
    let write = MemorySource::new(schema(), rows.clone());
    let wconn = write.connect(Some(vec![(0, true)]), None, vec![]);
    write.set_conn_output(
        wconn,
        OutEdge {
            node: NodeId::new(0, 0),
            port: Port::Single,
        },
    );
    // The connection-boundary node build the graph's push_one performs.
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

    let id1_a = orow(vec![Int(1), Int(1), Int(1)]);
    let id1_b = orow(vec![Int(1), Int(1), Int(999)]);
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
