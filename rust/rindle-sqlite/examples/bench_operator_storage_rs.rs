//! Side-by-side benchmark of the two **operator scratch-state** backends a server-side
//! pipeline can run on, measured at the [`rindle::storage::Storage`] trait surface —
//! the exact primitives stateful operators (`take` / `cap` / `reduce`) call on every
//! push:
//!
//!   - `btree`  — [`rindle::storage::MemoryStorage`] (an in-process `BTreeMap`).
//!   - `sqlite` — [`rindle_sqlite::DatabaseStorage::new_temp_file`]'s `OpStorage`: a
//!     private on-disk temporary SQLite DB with `journal_mode = OFF`,
//!     `synchronous = OFF`, `locking_mode = EXCLUSIVE`. The spill-to-disk path — no
//!     journal writes and no `fsync`s, so writes never block on durability; pages only
//!     reach the disk when the OS page cache evicts them.
//!
//! Both backends are result-equivalent (the cross-backend test in `tests/storage.rs`
//! pins this), so this isolates pure storage cost. We measure point ops over a large
//! keyspace (so the SQLite side is a real on-disk B-tree, not a toy) plus a full scan.
//!
//!   cargo run -p rindle-sqlite --release --example bench_operator_storage_rs --features fast-alloc

use std::hint::black_box;
use std::time::Instant;

use rindle::storage::{MemoryStorage, Storage, StorageValue};
use rindle_sqlite::DatabaseStorage;

#[cfg(feature = "fast-alloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Distinct operator-scratch keys (e.g. one grouped-`reduce` count per group). Large
/// enough that the SQLite backend is an honest on-disk B-tree.
const KEYS: usize = 200_000;

fn val(i: usize) -> StorageValue {
    StorageValue::Reduce {
        count: i as i64,
        accs: Vec::new(),
    }
}

/// Operator keys are short, sorted strings (group ids); mirror that shape.
fn keys() -> Vec<String> {
    (0..KEYS).map(|i| format!("g:{i:08}")).collect()
}

// --- bench harness (hand-rolled `name|ns`, matching the other bench_*_rs examples) ---

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

fn run_backend(label: &str, store: &dyn Storage, keys: &[String]) {
    let n = keys.len();

    // Bulk fill: insert every key once. One-shot (re-running would just overwrite), so
    // time it directly and report ns per inserted key.
    let t = Instant::now();
    for (i, k) in keys.iter().enumerate() {
        store.set(k, val(i));
    }
    println!(
        "set_fill/{label}|{:.1}",
        t.elapsed().as_nanos() as f64 / n as f64
    );

    // Point read of an existing key (rotating across the whole keyspace).
    let mut i = 0usize;
    bench(&format!("get_hit/{label}"), || {
        i = if i + 1 == n { 0 } else { i + 1 };
        black_box(store.get(&keys[i]));
    });

    // Point read of an absent key (the miss path).
    let miss = "g:zzzzzzzz".to_string();
    bench(&format!("get_miss/{label}"), || {
        black_box(store.get(&miss));
    });

    // Overwrite an existing key (the steady-state `reduce` count update).
    let mut i = 0usize;
    bench(&format!("set_update/{label}"), || {
        i = if i + 1 == n { 0 } else { i + 1 };
        store.set(&keys[i], val(i));
    });

    // Delete + re-insert a key (group churn; keyspace stays at `KEYS`). Two writes/iter.
    let mut i = 0usize;
    bench(&format!("del+set/{label}"), || {
        i = if i + 1 == n { 0 } else { i + 1 };
        store.del(&keys[i]);
        store.set(&keys[i], val(i));
    });

    // Full ascending scan (what a `reduce`/`cap` does to enumerate its groups). One-shot
    // throughput: ns per yielded pair.
    let t = Instant::now();
    let count = store.scan("").count();
    assert_eq!(count, n, "scan must see every key");
    println!(
        "scan_all/{label}|{:.1}",
        t.elapsed().as_nanos() as f64 / n as f64
    );
}

fn main() {
    println!("# operator-storage primitives — {KEYS} keys");
    println!("# name|ns_per_op  (lower is better; del+set is two writes)");

    let ks = keys();

    let mem = MemoryStorage::new();
    run_backend("btree", &mem, &ks);

    // The temp-file scratch DB lives as long as the `OpStorage` (it holds a clone of the
    // connection); it is deleted when that drops at the end of `main`.
    let db = DatabaseStorage::new_temp_file().expect("open temp scratch db");
    let op = db.create_storage();
    run_backend("sqlite", &op, &ks);
}
