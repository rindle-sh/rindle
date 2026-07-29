//! Memory probe for the guarded push fan-out (`designs/205-GUARDED-PUSH-FANOUT-DESIGN.md`,
//! `src/push_index.rs`): churn guarded **pipelines** — not rows — through connect/destroy
//! and prove the reverse predicate index leaves *no live state* behind per teardown.
//!
//! The push index (`PushIndex` inside `ConnTable`) grows and shrinks on the connection
//! axis: `connect` inserts a slot into `eq[col][value]` (or `scan`) and refcounts its
//! `split_edit_keys`; `destroy` is the exact inverse, pruning emptied buckets, column
//! maps, and zeroed refcounts (`source_common.rs` `connect`/`destroy`). The unit tests
//! prove each inverse in isolation; the existing churn/leak probes
//! (`wiki_recency_churn_leak.rs`, `core_live_heap_probe.rs`) churn *rows through operator
//! state* and never exercise this axis. This probe closes that gap.
//!
//! A counting global allocator tracks **net-live** blocks (`alloc − free`), exactly as
//! `core_live_heap_probe.rs` does. We build a batch of guarded connections, drive a wave
//! of writes through the populated index (so `push_candidates` + the debug soundness net
//! run against a non-trivial index), then tear the whole batch down — repeatedly, over a
//! **fixed, bounded** seed row set and a **fixed** guard-value space, so a correct
//! `remove` returns the index to byte-identical state each round and net-live heap stays
//! flat. A `remove` that failed to prune a bucket / column map, leaked a `split_keys`
//! refcount, or left a stale slot in the `conns`/free vectors would climb net-live blocks
//! monotonically per round — the signal this asserts against. A persistent probe pipeline
//! kept alive across every round confirms churn never corrupts a surviving connection's
//! index entry. Every write also runs `debug_assert_index_sound` (debug test build), so a
//! churn that left a stale guard mis-routing a delta would panic here, not drop silently.

use std::alloc::{GlobalAlloc, Layout, System};
use std::rc::Rc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use rindle::push_index::PushGuard;
use rindle::value::{owned_row as row, ColId, OwnedRow, OwnedValue as V, SourceSchema};
use rindle::{
    CmpOp, CompiledPredicate, ConnectionFilters, Graph, NodeId, PipelineManifest, SourceChange,
};

// -- net-live counting allocator (per-binary; this file is its own test crate) ----------

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

fn snapshot() -> (i64, i64) {
    (
        LIVE_BYTES.load(Ordering::Relaxed),
        LIVE_BLOCKS.load(Ordering::Relaxed),
    )
}

// -- schema / guarded-connection helpers ------------------------------------------------

const ID: ColId = 0;
const PROJ: ColId = 1;
const VER: ColId = 2;

/// `issue(id, proj, ver)`; PK = id, ordered by id asc.
fn schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "proj", "ver"], vec![ID], vec![(ID, true)])
}
fn r(id: i64, proj: i64, ver: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(proj), V::Int(ver)]
}

/// A `where proj == val` connection filter with the matching equality guard — exactly the
/// pair `build_connection_filters` produces, hand-built (like `guarded_push.rs`). When
/// `split` is set, the connection lists `ver` as a split-edit key, so a `ver` edit churns
/// the index's refcounted `split_keys` union too.
fn guarded_filters(val: i64) -> ConnectionFilters {
    let cp = CompiledPredicate::Cmp {
        col: PROJ,
        op: CmpOp::Eq,
        value: V::Int(val),
    };
    ConnectionFilters {
        predicate: Rc::new(move |row: &OwnedRow| cp.eval(row)),
        pk_constraint: None,
        fully_applied: true,
        sql_condition: None,
        push_guard: Some(PushGuard {
            col: PROJ,
            values: vec![V::Int(val)],
        }),
    }
}

/// Build a recorded `source → guarded conn → change-sink` pipeline over `src`, hydrate it,
/// and return `(manifest, sink)`. `destroy_pipeline(&manifest)` later un-indexes the
/// connection's guard from the `ConnTable` push index — the teardown path under test.
fn build_guarded_sink(
    g: &mut Graph,
    src: NodeId,
    proj: i64,
    split: bool,
) -> (PipelineManifest, NodeId) {
    let split_keys = if split { vec![VER] } else { Vec::new() };
    g.begin_recording();
    let conn = g.connect(
        src,
        Some(schema().sort),
        Some(guarded_filters(proj)),
        split_keys,
    );
    let sink = g.add_change_sink(conn);
    g.set_sink_edge(conn, sink);
    let manifest = g.take_recording();
    let _ = g.try_hydrate_change_sink(sink).unwrap();
    (manifest, sink)
}

// -- the probe --------------------------------------------------------------------------

/// Guard-value space churned each round. Fixed and bounded (the same values every round),
/// so a correct `remove` returns the index to identical state — the strongest leak signal.
const BATCH: i64 = 24;
/// Proj for the persistent probe pipeline — outside `0..BATCH`, so no churned pipeline ever
/// guards it (only the probe does).
const SENTINEL: i64 = 10_000;
/// Proj that NO connection ever guards — its writes must find zero candidates. Distinct
/// from every guarded value above.
const MISS_PROJ: i64 = 20_000;

/// Per-row `ver` counters, so every Edit carries the correct `old`. Threaded through the
/// churn rounds by `&mut` to keep the source's row set byte-stable (Edits, never Adds).
struct Vers {
    guarded: Vec<i64>,
    sentinel: i64,
    miss: i64,
}

/// Bump a single row's ver and push the resulting Edit. Returns nothing — callers drain the
/// sinks they care about.
fn bump(g: &mut Graph, src: NodeId, id: i64, proj: i64, ver: &mut i64) {
    let old = r(id, proj, *ver);
    *ver += 1;
    let new = r(id, proj, *ver);
    g.source_push(
        src,
        SourceChange::Edit {
            row: row(new),
            old: row(old),
        },
    );
}

#[test]
fn guarded_pipeline_churn_leaves_no_live_index_state() {
    let mut g = Graph::new();
    // One shared, persistent source seeded ONCE with a bounded row set: one row per guard
    // value, the sentinel row, and an unguarded miss row. Writes below are Edits over these
    // rows (ver bumps), so the source B+tree stays bounded — isolating connection-axis churn
    // from row growth.
    let mut seed: Vec<Vec<V>> = (0..BATCH).map(|p| r(p, p, 0)).collect();
    seed.push(r(SENTINEL, SENTINEL, 0));
    seed.push(r(MISS_PROJ, MISS_PROJ, 0));
    let src = g.add_source(schema(), seed.into_iter().map(row).collect());

    let mut vers = Vers {
        guarded: vec![0i64; BATCH as usize],
        sentinel: 0,
        miss: 0,
    };

    // A persistent guarded pipeline that survives every round; churn must not corrupt it.
    let (_probe_m, probe_sink) = build_guarded_sink(&mut g, src, SENTINEL, false);
    let _ = g.take_sink_changes(probe_sink);

    // One churn round: build BATCH guarded pipelines, drive a write wave through the
    // populated index, then destroy them all. Returns the node_count observed while the
    // batch was fully torn down (used to prove the arena recycles rather than grows).
    let round = |g: &mut Graph, vers: &mut Vers| -> usize {
        let mut built: Vec<(PipelineManifest, NodeId)> = Vec::with_capacity(BATCH as usize);
        for j in 0..BATCH {
            // Even pipelines list `ver` as a split key, so the split_keys refcount union
            // churns up and back to empty across the round.
            built.push(build_guarded_sink(g, src, j, j % 2 == 0));
        }

        // Write wave: bump every guarded row's ver by an Edit. Each Edit is routed by
        // push_candidates to exactly the one matching guard bucket (plus, while any even-j
        // pipeline is live, split into Remove+Add table-wide). Drain each sink so no
        // change-sink buffer accumulates.
        for j in 0..BATCH {
            bump(g, src, j, j, &mut vers.guarded[j as usize]);
        }
        for (_m, sink) in &built {
            let _ = g.take_sink_changes(*sink);
        }

        // A write to a proj NO connection guards: push_candidates must return empty and the
        // debug soundness net walks every wired connection (the whole batch + the probe) as a
        // non-candidate, asserting each rejects both rows. No sink receives it.
        bump(g, src, MISS_PROJ, MISS_PROJ, &mut vers.miss);

        // Tear the whole batch down: each destroy un-indexes its guard.
        for (m, _sink) in &built {
            g.destroy_pipeline(m);
        }
        let nc = g.node_count();

        // The persistent probe still routes: with the batch (and its split keys) gone, a
        // matching sentinel Edit reaches the probe as one change — proving its index entry
        // survived a full round of neighbour churn intact.
        bump(g, src, SENTINEL, SENTINEL, &mut vers.sentinel);
        assert!(
            !g.take_sink_changes(probe_sink).is_empty(),
            "persistent probe pipeline stopped receiving its matching write after churn — \
             the push index corrupted a surviving connection's entry"
        );
        nc
    };

    // Warm up so first-touch lazy structures (BTree headers, sink buffers, arena vectors)
    // settle, then take the baseline. node_count must be identical every round thereafter —
    // torn-down slots and nodes are recycled, never appended.
    const WARM: i64 = 6;
    for _ in 0..WARM {
        round(&mut g, &mut vers);
    }
    let (base_bytes, base_blocks) = snapshot();
    let base_nodes = g.node_count();
    let gross0 = GROSS_ALLOCS.load(Ordering::Relaxed);

    const ROUNDS: i64 = 30;
    for s in 1..=ROUNDS {
        let nc = round(&mut g, &mut vers);
        assert_eq!(
            nc, base_nodes,
            "arena grew under pipeline churn: {base_nodes} -> {nc} after round {s} teardown \
             (torn-down connection slots / nodes were leaked, not recycled)"
        );
        if s % 10 == 0 {
            let (b, blk) = snapshot();
            println!(
                "[conn-churn] after {:>4} rounds ({} build+destroy cycles): net-live {:+} bytes / {:+} blocks \
                 ({:+.3} blocks/round)",
                s,
                s * BATCH,
                b - base_bytes,
                blk - base_blocks,
                (blk - base_blocks) as f64 / s as f64,
            );
        }
    }

    let (final_bytes, final_blocks) = snapshot();
    let gross = GROSS_ALLOCS.load(Ordering::Relaxed) - gross0;
    let blocks_drift = final_blocks - base_blocks;
    let bytes_drift = final_bytes - base_bytes;
    println!(
        "[conn-churn] {ROUNDS} rounds x {BATCH} guarded pipelines: net-live drift = {blocks_drift:+} blocks \
         / {bytes_drift:+} bytes; gross allocs = {gross}"
    );

    // A correct `remove` is the exact inverse of `insert`, so churning a fixed guard-value
    // space over a bounded source must leave net-live heap FLAT. A leaked bucket / column
    // map / split_keys refcount / connection slot would climb ~1+ block per destroy —
    // ROUNDS*BATCH = 720 destroys here, an order of magnitude above this slack, which only
    // absorbs incidental steady-state settling (the deterministic `node_count` equality
    // above is the exact companion check for a leaked slot/node).
    assert!(
        blocks_drift.abs() <= 128,
        "guarded push index leaked live state under pipeline churn: {blocks_drift:+} net-live blocks \
         over {} connect/destroy cycles ({bytes_drift:+} bytes) — expected ~0 (a `PushIndex::remove` \
         that failed to prune an emptied bucket / column map / split_keys refcount, or a leaked \
         connection slot)",
        ROUNDS * BATCH,
    );
}
