#![cfg(feature = "testkit")]

//! Final bisection for the wiki-demo materialized-query leak. The Cluster-level probe
//! (`rindle-replica/tests/wiki_cluster_rss_probe.rs`) shows that pinning even a bare
//! `page.orderBy(last_ts desc).limit(20)` (no nesting/Join/Filter) leaks ~0.8 KB per
//! committed transaction, while the same churn with NO materialized query is flat. The
//! demo's ingester UPSERTs — every commit bumps a page's `last_ts`/`edits`, i.e. an
//! `Edit` to a row already in the top-N — a pattern the earlier `Add`-only churn probes
//! (`wiki_recency_churn_leak.rs`) never exercised.
//!
//! This drives exactly that against the **core operator `Graph`** (no SQLite, no Cluster,
//! no drain): seed a bounded set of pages, then push thousands of `Edit`s bumping their
//! sort key so they reshuffle inside a `limit(20)` Take. It reports BOTH the operator
//! storage entry count and process RSS, so we can tell whether the leak lives in the core
//! IVM graph (`src/`) or above it in the replica layer (`rindle-replica/`).

use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{build_pipeline, Ast, Dir, Graph, OrderPart, SourceChange};

// Run under mimalloc with `--features fast-alloc` to tell a true core-engine leak apart from
// glibc fragmentation: if RSS still climbs per Edit under a returning allocator, the core graph
// genuinely retains memory per push.
#[cfg(feature = "fast-alloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn page_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "last_ts"], vec![0], vec![(1, false), (0, true)])
}

/// `page.orderBy(last_ts desc).limit(20)` — flat, no nested relationship.
fn flat_board() -> Ast {
    Ast {
        table: "page".into(),
        order_by: vec![OrderPart("last_ts".into(), Dir::Desc)],
        limit: Some(20),
        ..Default::default()
    }
}

fn rss_kb() -> i64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    statm
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
        * 4
}

/// `page` — plain select-all, NO orderBy/limit (no Take operator).
fn plain_board() -> Ast {
    Ast {
        table: "page".into(),
        ..Default::default()
    }
}

fn churn(ast: Ast, label: &str) {
    let mut g = Graph::new();
    let page_id = g.add_source(page_schema(), vec![]);
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

    let report = |g: &Graph| -> usize { g.storage_snapshot().iter().map(Vec::len).sum() };

    // Seed a bounded set of pages (more than the limit so the Take has to drop some).
    const N_PAGES: i64 = 40;
    let mut last_ts: Vec<i64> = vec![0; N_PAGES as usize];
    for k in 0..N_PAGES {
        last_ts[k as usize] = k;
        g.source_push_isolated(page_id, SourceChange::Add(row(vec![V::Int(k), V::Int(k)])))
            .expect("seed add");
        let _ = g.take_sink_changes(sink);
    }

    let state_warm = report(&g);
    let rss_warm = rss_kb();

    // Churn: repeatedly bump a page's last_ts (an Edit to an existing row) so it jumps to
    // the front of the recency order — the upsert pattern. Bounded key set, so the live
    // data never grows; only the number of Edits processed does.
    const ITERS: i64 = 60_000;
    let mut ts = N_PAGES;
    for k in 0..ITERS {
        let pk = k % N_PAGES;
        let old = row(vec![V::Int(pk), V::Int(last_ts[pk as usize])]);
        ts += 1;
        last_ts[pk as usize] = ts;
        let new = row(vec![V::Int(pk), V::Int(ts)]);
        g.source_push_isolated(page_id, SourceChange::Edit { row: new, old })
            .expect("edit");
        let _ = g.take_sink_changes(sink);
        if (k + 1) % 10_000 == 0 {
            println!(
                "[{label}] after {:>6} edits: rss={} KB (+{} KB)",
                k + 1,
                rss_kb(),
                rss_kb() - rss_warm
            );
        }
    }

    let state_after = report(&g);
    let rss_after = rss_kb();
    println!(
        "[{label}] op-state {state_warm} -> {state_after} entries | rss {rss_warm} -> {rss_after} KB \
         (+{} KB over {ITERS} edits = {:.3} KB/edit)",
        rss_after - rss_warm,
        (rss_after - rss_warm) as f64 / ITERS as f64
    );

    // The operator graph must NOT accumulate state per Edit over a bounded key set.
    assert!(
        state_after <= state_warm + 4,
        "[{label}] core operator-graph state grew under repeated Edits: {state_warm} -> {state_after}"
    );
}

#[test]
fn flat_edit_churn_state_and_rss() {
    churn(flat_board(), "take-limit-20");
}

#[test]
fn plain_edit_churn_state_and_rss() {
    churn(plain_board(), "plain-select-all");
}

/// Is the per-Edit RSS climb a *logical* leak (memory never freed) or allocator
/// high-water / fragmentation (freed but not returned to the OS)? Churn hard, record peak
/// RSS, DROP the whole graph, then re-measure: if RSS falls back near baseline the memory
/// was reclaimable (allocator-retention, not a leak); if it stays at the peak it leaked.
#[test]
fn pure_graph_reclaim_after_drop() {
    let base = rss_kb();
    {
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
        let mut last_ts = vec![0i64; N_PAGES as usize];
        for k in 0..N_PAGES {
            last_ts[k as usize] = k;
            g.source_push_isolated(page_id, SourceChange::Add(row(vec![V::Int(k), V::Int(k)])))
                .unwrap();
            let _ = g.take_sink_changes(sink);
        }
        let mut ts = N_PAGES;
        for k in 0..60_000i64 {
            let pk = k % N_PAGES;
            let old = row(vec![V::Int(pk), V::Int(last_ts[pk as usize])]);
            ts += 1;
            last_ts[pk as usize] = ts;
            g.source_push_isolated(
                page_id,
                SourceChange::Edit {
                    row: row(vec![V::Int(pk), V::Int(ts)]),
                    old,
                },
            )
            .unwrap();
            let _ = g.take_sink_changes(sink);
        }
        let peak = rss_kb();
        println!(
            "[reclaim] base={base} KB  peak(after 60k edits)={peak} KB  (+{} KB)",
            peak - base
        );
        // graph + sink dropped at end of this block
    }
    let after_drop = rss_kb();
    println!("[reclaim] after dropping the graph: rss={after_drop} KB (vs base {base} KB)");
}

/// Discriminator: is the per-push retention specific to `Edit`, or does plain `Add`+`Remove`
/// churn (net-zero, fully bounded) leak too? Plain select-all so there is NO operator state.
#[test]
fn plain_add_remove_churn_rss() {
    let mut g = Graph::new();
    let page_id = g.add_source(page_schema(), vec![]);
    let ast = plain_board();
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

    let rss_warm = rss_kb();
    const ITERS: i64 = 60_000;
    for k in 0..ITERS {
        // Add a fresh key then immediately remove it — the live set never exceeds 1 row.
        g.source_push_isolated(page_id, SourceChange::Add(row(vec![V::Int(k), V::Int(k)])))
            .expect("add");
        let _ = g.take_sink_changes(sink);
        g.source_push_isolated(
            page_id,
            SourceChange::Remove(row(vec![V::Int(k), V::Int(k)])),
        )
        .expect("remove");
        let _ = g.take_sink_changes(sink);
    }
    let rss_after = rss_kb();
    println!(
        "[plain-add-remove] rss {rss_warm} -> {rss_after} KB (+{} KB over {ITERS} add/remove pairs = {:.3} KB/pair)",
        rss_after - rss_warm,
        (rss_after - rss_warm) as f64 / ITERS as f64
    );
}
