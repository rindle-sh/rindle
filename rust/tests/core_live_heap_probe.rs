#![cfg(feature = "testkit")]

//! Decisive separation of the two candidate causes of the wiki-demo RSS growth, at the
//! **core `Graph`** level (single thread, deterministic, no SQLite, no channels): is heap
//! *genuinely retained* (live bytes = allocations − frees climbing per edit → a real leak in
//! a live data structure), or is live heap flat while only RSS climbs (allocator
//! fragmentation of cleanly-freed churn)?
//!
//! A counting allocator tracks BOTH the gross churn (alloc count) AND the **net live** bytes
//! (`alloc − free`). We churn `Edit`s over a bounded key set (the live set never grows) and
//! report net-live drift per edit. If net-live climbs, the engine is holding growing live
//! state per push (the op-state probes miss it because `storage_snapshot` only scans operator
//! storage, not the source / cursors / overlay). If net-live is flat, the growth the RSS
//! probes see is purely the allocator not returning freed memory.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{build_pipeline, Ast, Dir, Graph, OrderPart, SourceChange};

struct Counting;
static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
static LIVE_BLOCKS: AtomicI64 = AtomicI64::new(0);
static GROSS_ALLOCS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = System.alloc(l);
        if !p.is_null() {
            LIVE_BYTES.fetch_add(l.size() as i64, Ordering::Relaxed);
            LIVE_BLOCKS.fetch_add(1, Ordering::Relaxed);
            GROSS_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE_BYTES.fetch_sub(l.size() as i64, Ordering::Relaxed);
        LIVE_BLOCKS.fetch_sub(1, Ordering::Relaxed);
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let np = System.realloc(p, l, new);
        if !np.is_null() {
            LIVE_BYTES.fetch_add(new as i64 - l.size() as i64, Ordering::Relaxed);
            GROSS_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        np
    }
}

#[global_allocator]
static A: Counting = Counting;

fn page_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "last_ts"], vec![0], vec![(1, false), (0, true)])
}

/// `page.orderBy(last_ts desc).limit(20)` — the flat board shape.
fn flat_board() -> Ast {
    Ast {
        table: "page".into(),
        order_by: vec![OrderPart("last_ts".into(), Dir::Desc)],
        limit: Some(20),
        ..Default::default()
    }
}

fn snapshot() -> (i64, i64) {
    (
        LIVE_BYTES.load(Ordering::Relaxed),
        LIVE_BLOCKS.load(Ordering::Relaxed),
    )
}

#[test]
fn core_graph_net_live_heap_under_edit_churn() {
    let mut g = Graph::new();
    let page_id = g.add_source(page_schema(), vec![]);
    let ast = flat_board();
    let top = {
        let resolve = |t: &str| match t {
            "page" => Some((page_id, page_schema())),
            _ => None,
        };
        build_pipeline(&mut g, &ast, &resolve).expect("build")
    };
    let sink = g.add_change_sink(top);
    g.set_sink_edge(top, sink);
    let _ = g.try_hydrate_change_sink(sink).unwrap();

    const N_PAGES: i64 = 40;
    let mut last_ts: Vec<i64> = vec![0; N_PAGES as usize];
    for k in 0..N_PAGES {
        last_ts[k as usize] = k;
        g.source_push_isolated(page_id, SourceChange::Add(row(vec![V::Int(k), V::Int(k)])))
            .expect("seed add");
        let _ = g.take_sink_changes(sink);
    }

    // Warm up so first-touch lazy structures settle, then sample net-live every 10k edits.
    let mut ts = N_PAGES;
    let churn = |g: &Graph, last_ts: &mut [i64], ts: &mut i64, n: i64| {
        for k in 0..n {
            let pk = k % N_PAGES;
            let old = row(vec![V::Int(pk), V::Int(last_ts[pk as usize])]);
            *ts += 1;
            last_ts[pk as usize] = *ts;
            let new = row(vec![V::Int(pk), V::Int(*ts)]);
            g.source_push_isolated(page_id, SourceChange::Edit { row: new, old })
                .expect("edit");
            let _ = g.take_sink_changes(sink);
        }
    };
    churn(&g, &mut last_ts, &mut ts, 2000);

    let (base_bytes, base_blocks) = snapshot();
    let gross0 = GROSS_ALLOCS.load(Ordering::Relaxed);
    const STEP: i64 = 10_000;
    const STEPS: i64 = 6;
    for s in 1..=STEPS {
        churn(&g, &mut last_ts, &mut ts, STEP);
        let (b, blk) = snapshot();
        println!(
            "[net-live] after {:>6} edits: net-live = {:+} bytes / {:+} blocks vs warm baseline  ({:+.3} bytes/edit, {:+.4} blocks/edit)",
            s * STEP,
            b - base_bytes,
            blk - base_blocks,
            (b - base_bytes) as f64 / (s * STEP) as f64,
            (blk - base_blocks) as f64 / (s * STEP) as f64,
        );
    }
    let gross = GROSS_ALLOCS.load(Ordering::Relaxed) - gross0;
    println!(
        "[net-live] gross allocs over {} edits = {} ({:.1}/edit)",
        STEP * STEPS,
        gross,
        gross as f64 / (STEP * STEPS) as f64
    );

    // Regression guard for the change-sink leak (`Graph::collector_push`): a production
    // change-sink used to record one un-drained `CollectedChange` (pinning its row `Arc`s)
    // per push into the testkit `changes` buffer, FOREVER — net-live heap climbing exactly
    // +1 block/edit over a bounded key set, freed only on teardown (the wiki-demo "leak").
    // With the leak fixed, churning a fixed key set must leave net-live heap FLAT. A tiny
    // slack absorbs incidental lazy-structure settling; the bug was +60k blocks here.
    let (final_bytes, final_blocks) = snapshot();
    let blocks_drift = final_blocks - base_blocks;
    let bytes_drift = final_bytes - base_bytes;
    assert!(
        blocks_drift.abs() <= 64,
        "net-live heap leaked under bounded edit churn: {blocks_drift:+} live blocks over {} edits \
         ({bytes_drift:+} bytes) — expected ~0 (the change-sink `changes`-buffer leak regressed)",
        STEP * STEPS,
    );
}
