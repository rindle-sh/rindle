//! COW B+tree allocation proofs (spec `04` §8, §10; the M3 milestone numbers).
//!
//! A counting global allocator turns the headline claims into assertions:
//!   1. `fork()` is O(1)            → ZERO heap allocations, independent of size.
//!   2. pass-through scan vends      → ZERO heap allocations per row (README Step-5
//!      headline: "~0 allocations per row on the transient pass-through path").
//!   3. `to_owned_row()` owns a row  → an Arc bump, still ZERO heap allocations.
//!   4. a write to a SHARED tree     → allocates (it path-copies); structural
//!      proof that it copies ONLY the path lives in the unit test
//!      `make_mut_copies_only_the_path`.
//!
//! The counter is PER-THREAD, not process-global: cargo runs `#[test]`s
//! concurrently, and even within this one binary the libtest harness runs the
//! test on a worker thread while its main thread does bookkeeping (which can
//! allocate). A process-global counter would let those cross-thread allocations
//! land inside a measurement window and pollute the count — a rare but real
//! flake. Counting only the measuring thread's own allocations makes the proofs
//! deterministic. (Measurements start AFTER `values_from`, so the cursor's
//! one-time O(log n) spine `Vec` allocation is excluded; the claim is about the
//! per-row vend cost.)

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use rindle::btree::{BTree, RowRef, RowStream};
use rindle::value::{owned_row as row, OwnedRow as Row, OwnedValue as Value, Sort};

thread_local! {
    // const-initialized ⇒ no lazy heap init, so it is safe to touch from inside
    // the global allocator without re-entrant allocation.
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

fn bump() {
    // `try_with` so an allocation during thread teardown (after the TLS is
    // destroyed) is silently ignored rather than panicking inside the allocator.
    let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
}

struct Counting;

// SAFETY: a thin pass-through to the system allocator that only bumps a
// per-thread counter.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        bump();
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        bump();
        System.alloc_zeroed(l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        bump();
        System.realloc(p, l, n)
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn allocs() -> usize {
    ALLOCS.with(|c| c.get())
}

fn s1() -> Sort {
    vec![(0, true)]
}

fn rk(k: i64) -> Row {
    row(vec![Value::Int(k)])
}

fn build(n: i64) -> BTree {
    let sort = s1();
    let mut t = BTree::new();
    for k in 0..n {
        t.add(rk(k), &sort);
    }
    t
}

#[test]
fn allocation_proofs() {
    let sort = s1();

    // (1) fork() allocates nothing, at every size.
    for n in [1, 100, 2000, 50_000] {
        let t = build(n);
        let before = allocs();
        let forked = t.fork();
        let after = allocs();
        std::hint::black_box(&forked);
        assert_eq!(
            after - before,
            0,
            "fork() of a {n}-row tree must allocate nothing (got {})",
            after - before
        );
    }

    // (2) Pass-through scan vends with ZERO heap allocations per row.
    {
        let t = build(5000);
        let mut cur = t.values_from(None, true, &sort); // one-time spine Vec alloc happens here
        let before = allocs();
        let mut sum: i64 = 0;
        let mut n = 0;
        while let Some(r) = cur.next_row() {
            sum += match &r.col(0).to_owned() {
                Value::Int(i) => *i,
                _ => 0,
            };
            n += 1;
        }
        let after = allocs();
        std::hint::black_box(sum);
        assert_eq!(n, 5000);
        assert_eq!(
            after - before,
            0,
            "pass-through scan of 5000 rows must vend with ZERO heap allocations (got {})",
            after - before
        );
    }

    // (3) Owning each row via `to_owned_row()` is an Arc bump — still ZERO allocs.
    // (The cursor vends `&OwnedRow`, so `RowRef::to_owned_row` is `Arc::clone`.)
    {
        let t = build(5000);
        let mut cur = t.values_from(None, true, &sort);
        let before = allocs();
        let mut last = None;
        let mut n = 0;
        while let Some(r) = cur.next_row() {
            last = Some(r.to_owned_row()); // &OwnedRow ⇒ refcount bump, no heap alloc
            n += 1;
        }
        let after = allocs();
        std::hint::black_box(&last);
        assert_eq!(n, 5000);
        assert_eq!(
            after - before,
            0,
            "owning each row via to_owned_row (Arc bump) must not heap-allocate (got {})",
            after - before
        );
    }

    // (4) A write to a SHARED tree path-copies (allocates); an unshared write of
    // the same shape allocates no more (it mutates in place).
    {
        let mut a = build(5000);
        let snapshot = a.fork(); // share the whole tree

        let before = allocs();
        a.add(rk(1_000_000), &sort); // larger than all ⇒ rightmost path only
        let shared_cost = allocs() - before;
        std::hint::black_box(&snapshot);
        assert!(
            shared_cost > 0,
            "writing a shared tree must path-copy (allocate); got 0"
        );

        drop(snapshot); // now `a` is unshared
        let before = allocs();
        a.add(rk(1_000_001), &sort);
        let unshared_cost = allocs() - before;
        eprintln!(
            "path-copy cost: shared write = {shared_cost} allocs, unshared write = {unshared_cost} allocs"
        );
        assert!(
            unshared_cost <= shared_cost,
            "unshared write ({unshared_cost}) should not exceed shared path-copy ({shared_cost})"
        );
    }
}
