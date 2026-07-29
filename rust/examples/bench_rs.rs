//! Rust side of the Rust-vs-JS head-to-head (spec `04` §10). Mirrors
//! `tools/bench_js.ts` operation-for-operation over single-`Int` `Row`s, with an
//! IDENTICAL timing harness (warmup, then min ns/op over rounds). Build for real:
//!   cargo run --release --example bench_rs
use std::hint::black_box;
use std::time::Instant;

use rindle::btree::{row_bound_of, BTree, RowStream};
use rindle::value::{compare_rows, owned_row as row, OwnedRow as Row, OwnedValue as Value, Sort};

// A production-grade allocator, opt-in via `--features fast-alloc`. The native
// server uses a tuned allocator (mimalloc/jemalloc) and the wasm client uses a
// fast bump allocator — comparing Rust's general-purpose system `malloc` against
// V8's highly-tuned GC would handicap the alloc-bound ops (node splits, fresh
// builds). mimalloc is the fair, production-representative choice for the native
// head-to-head. Off by default so plain `cargo test`/`build` stays std-only.
#[cfg(feature = "fast-alloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const N: i64 = 1000;
const MID: i64 = 500;

fn s_asc() -> Sort {
    vec![(0, true)]
}

fn rk(k: i64) -> Row {
    row(vec![Value::Int(k)])
}

fn bench(name: &str, mut body: impl FnMut()) {
    // Warmup ~200ms.
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

fn build(n: i64, sort: &Sort) -> BTree {
    let mut t = BTree::new();
    for k in 0..n {
        t.add(rk(k), sort);
    }
    t
}

fn sum_forward(t: &BTree, bound: Option<&rindle::btree::RowBound>, sort: &Sort) -> i64 {
    let mut c = t.values_from(bound, true, sort);
    let mut s = 0;
    while let Some(r) = c.next_row() {
        if let Value::Int(i) = &r.col(0).to_owned() {
            s += *i;
        }
    }
    s
}

fn sum_reverse(t: &BTree, bound: Option<&rindle::btree::RowBound>, sort: &Sort) -> i64 {
    let mut c = t.values_from_reversed(bound, true, sort);
    let mut s = 0;
    while let Some(r) = c.next_row() {
        if let Value::Int(i) = &r.col(0).to_owned() {
            s += *i;
        }
    }
    s
}

fn main() {
    let asc = s_asc();
    // Descending sort spec (mirrors the JS reverseComparator) for the
    // getOrCreateIndex pattern.
    let desc: Sort = vec![(0, false)];

    let tree = build(N, &asc);
    let mid = row_bound_of(&rk(MID), &asc);

    // Pre-built keys/rows (constructed ONCE, outside the timed loops) so the
    // benchmark measures TREE work, not `Arc<[Value]>` key allocation — exactly as
    // the JS side reuses pre-built `[k]` arrays. Row construction (Arc vs JS array)
    // is a separate value-representation axis, held constant here.
    let k_hit = [rk(100), rk(500), rk(900)];
    let k_miss = [rk(N + 1), rk(N + 2), rk(N + 3)];
    let k_addel = rk(N + 1);
    let k_path = rk(N + 7);
    let rows: Vec<Row> = (0..N).map(rk).collect(); // reused by the build benches

    bench("scan_full", || {
        black_box(sum_forward(&tree, None, &asc));
    });
    bench("scan_from_mid", || {
        black_box(sum_forward(&tree, Some(&mid), &asc));
    });
    bench("scan_reverse_full", || {
        black_box(sum_reverse(&tree, None, &asc));
    });
    bench("scan_reverse_from_mid", || {
        black_box(sum_reverse(&tree, Some(&mid), &asc));
    });
    bench("has_hit_x3", || {
        black_box(tree.has(&k_hit[0], &asc));
        black_box(tree.has(&k_hit[1], &asc));
        black_box(tree.has(&k_hit[2], &asc));
    });
    bench("has_miss_x3", || {
        black_box(tree.has(&k_miss[0], &asc));
        black_box(tree.has(&k_miss[1], &asc));
        black_box(tree.has(&k_miss[2], &asc));
    });
    bench("get_hit_x3", || {
        black_box(tree.get(&k_hit[0], &asc));
        black_box(tree.get(&k_hit[1], &asc));
        black_box(tree.get(&k_hit[2], &asc));
    });
    {
        let mut t = tree.fork();
        bench("add_then_delete_1", || {
            t.add(k_addel.clone(), &asc);
            t.delete(&k_addel, &asc);
        });
    }
    bench("add_1000_sequential", || {
        let mut t = BTree::new();
        for r in &rows {
            t.add(r.clone(), &asc);
        }
        black_box(t.len());
    });
    bench("from_sorted_1000", || {
        let t = BTree::from_sorted(rows.iter().cloned(), &asc);
        black_box(t.len());
    });
    bench("get_or_create_index_old", || {
        let mut sorted = rows.clone();
        sorted.sort_by(|a, b| compare_rows(&desc, a, b));
        let mut t = BTree::new();
        for k in sorted {
            t.add(k, &desc);
        }
        black_box(t.len());
    });
    bench("get_or_create_index_new", || {
        let mut sorted = rows.clone();
        sorted.sort_by(|a, b| compare_rows(&desc, a, b));
        let t = BTree::from_sorted(sorted.into_iter(), &desc);
        black_box(t.len());
    });
    bench("fork_clone", || {
        black_box(tree.fork().len());
    });
    bench("clone_then_insert_pathcopy", || {
        let mut w = tree.fork();
        w.add(k_path.clone(), &asc);
        black_box(w.len());
    });

    // --- String-key variants: a realistic key type where JS has no SMI fast path.
    let srow = |k: i64| row(vec![Value::str(&format!("item{k:06}"))]);
    let stree = {
        let mut t = BTree::new();
        for k in 0..N {
            t.add(srow(k), &asc);
        }
        t
    };
    let s_hit = [srow(100), srow(500), srow(900)];
    let s_miss = [srow(N + 1), srow(N + 2), srow(N + 3)];
    bench("str_scan_full", || {
        let mut c = stree.values_from(None, true, &asc);
        let mut n = 0;
        while c.next_row().is_some() {
            n += 1;
        }
        black_box(n);
    });
    bench("str_has_hit_x3", || {
        black_box(stree.has(&s_hit[0], &asc));
        black_box(stree.has(&s_hit[1], &asc));
        black_box(stree.has(&s_hit[2], &asc));
    });
    bench("str_has_miss_x3", || {
        black_box(stree.has(&s_miss[0], &asc));
        black_box(stree.has(&s_miss[1], &asc));
        black_box(stree.has(&s_miss[2], &asc));
    });
    bench("str_get_hit_x3", || {
        black_box(stree.get(&s_hit[0], &asc));
        black_box(stree.get(&s_hit[1], &asc));
        black_box(stree.get(&s_hit[2], &asc));
    });
}
