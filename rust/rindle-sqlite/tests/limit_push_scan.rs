//! **How much SQLite work does one in-window insert into an `ORDER BY … LIMIT n` view cost —
//! O(n) (bounded) or O(N) (full table)?**
//!
//! Context: a loadgen "latest-N feed" measured ~O(base size) wall-time per insert and the schema
//! sort index didn't help. But `Take`'s displacement fetch is lazy (`take.rs:204`, `.take(n)` over
//! a streaming leaf cursor), so it should pull ~1–2 rows, not N. This test settles it
//! deterministically: it counts SQLite's own `sqlite3_stmt_status` work counters
//! (`fullscan_steps` = rows visited by un-indexed scans; `vm_steps` = total VM ops) for ONE push,
//! over a seeded base of N rows, sweeping {score index} × {ANALYZE}.
//!
//! If `fullscan_steps`/`vm_steps` per push are flat in N → the Take is bounded (engine fine, the
//! wall-time O(N) lives elsewhere). If they grow with N → the leaf full-sorts every push, and the
//! index/ANALYZE columns show what fixes it.
//!
//! Run: `cargo test -p rindle-sqlite --features scan-stats,testkit --test limit_push_scan -- --nocapture`
#![cfg(all(feature = "scan-stats", feature = "testkit"))]

use std::rc::Rc;

use rindle::value::{owned_row, OwnedValue as V, SourceSchema, ValueType};
use rindle::{build_pipeline, table, FetchRequest, Graph, SourceChange};
use rindle_sqlite::table_source::TableSource;
use rindle_sqlite::testkit::{ScanSink, ScanStats};
use rindle_sqlite::ColumnDef;

const LIMIT: u32 = 50;
const PUSHES: i64 = 20; // average per-push work over a handful of inserts

/// One measured cell: per-push SQLite work for a recency feed over `n` seeded rows.
struct Cell {
    n: i64,
    index: bool,
    analyze: bool,
    fullscan_per_push: f64,
    vm_per_push: f64,
    runs_per_push: f64,
}

fn col(name: &str) -> ColumnDef {
    // Int-valued columns: leave affinity blank so SQLite keeps INTEGER storage (matches the
    // testkit's `infer_sqlite_columns` for `OwnedValue::Int`).
    ColumnDef {
        name: name.into(),
        ty: ValueType::Null,
        optional: false,
    }
}

fn measure(n: i64, index: bool, analyze: bool) -> Cell {
    let conn = Rc::new(rusqlite::Connection::open_in_memory().expect("open sqlite"));
    // items(id, score); PK id. Recency feed ⇒ score == id (monotone), so every new row is the
    // new max and lands in the top-N window — the in-window churn that drives the displacement.
    conn.execute_batch(
        "CREATE TABLE items (id, score); CREATE UNIQUE INDEX __zql_items_pk ON items (id);",
    )
    .unwrap();
    if index {
        // Cover the full PK-completed sort `score DESC, id ASC` (builder.rs::resolve_sort).
        conn.execute_batch("CREATE INDEX items_score_idx ON items (score DESC, id ASC);")
            .unwrap();
    }

    let schema = SourceSchema::new(vec!["id", "score"], vec![0], vec![(0, true)]);
    let source = TableSource::new_with_schema(
        conn.clone(),
        "items",
        vec![col("id"), col("score")],
        vec![0],
        schema.clone(),
    );
    // Seed N rows through the tested write path (BEFORE attaching the sink, so the seed isn't
    // counted).
    for id in 1..=n {
        source.push(
            SourceChange::Add(owned_row(vec![V::Int(id), V::Int(id)])),
            &|_, _| {},
        );
    }
    if analyze {
        conn.execute_batch("ANALYZE;").unwrap();
    }

    let sink: ScanSink = Rc::new(std::cell::Cell::new(ScanStats::default()));
    source.set_scan_sink(sink.clone());

    // Build `table("items").order_by("score","desc").limit(50)` and hydrate it.
    let mut g = Graph::new();
    let src_id = g.add_dyn_source(Box::new(source));
    let by_name = [("items".to_string(), (src_id, schema))];
    let resolve = |t: &str| by_name.iter().find(|(n, _)| n == t).map(|(_, v)| v.clone());
    let ast = table("items")
        .order_by("score", "desc")
        .limit(LIMIT)
        .build();
    let top = build_pipeline(&mut g, &ast, &resolve).unwrap();
    let catch = g.add_catch(top, false);
    g.set_sink_edge(top, catch);
    let _initial = g.catch_fetch(catch, &FetchRequest::all());
    let _ = g.catch_pushes(catch); // drain hydration diff

    // Per-push measurement: insert new max rows (id = n+1, n+2, …), each enters the window.
    sink.set(ScanStats::default());
    for k in 1..=PUSHES {
        let id = n + k;
        g.source_push(
            src_id,
            SourceChange::Add(owned_row(vec![V::Int(id), V::Int(id)])),
        );
        let _ = g.catch_pushes(catch);
    }
    let acc = sink.get();
    Cell {
        n,
        index,
        analyze,
        fullscan_per_push: acc.fullscan_steps as f64 / PUSHES as f64,
        vm_per_push: acc.vm_steps as f64 / PUSHES as f64,
        runs_per_push: acc.runs as f64 / PUSHES as f64,
    }
}

#[test]
fn limit_push_scan_work_vs_base_size() {
    println!(
        "\n{:>8} {:>6} {:>8} | {:>12} {:>10} {:>8}",
        "N", "index", "analyze", "fullscan/push", "vm/push", "runs/push"
    );
    let mut cells = Vec::new();
    for &n in &[1_000i64, 10_000, 100_000] {
        for &index in &[false, true] {
            for &analyze in &[false, true] {
                let c = measure(n, index, analyze);
                println!(
                    "{:>8} {:>6} {:>8} | {:>12.1} {:>10.1} {:>8.1}",
                    c.n, c.index, c.analyze, c.fullscan_per_push, c.vm_per_push, c.runs_per_push
                );
                cells.push(c);
            }
        }
    }

    // The diagnostic question: with the covering index + ANALYZE, is per-push scan work bounded
    // (flat in N) rather than O(N)? Compare 10k vs 100k for the indexed+analyzed cell.
    let cell = |n: i64, index: bool, analyze: bool| {
        cells
            .iter()
            .find(|c| c.n == n && c.index == index && c.analyze == analyze)
            .unwrap()
    };
    let lo = cell(10_000, true, true);
    let hi = cell(100_000, true, true);
    println!(
        "\nindexed+analyzed: vm/push 10k={:.0} 100k={:.0} (ratio {:.2}); fullscan/push 10k={:.0} 100k={:.0}",
        lo.vm_per_push,
        hi.vm_per_push,
        hi.vm_per_push / lo.vm_per_push.max(1.0),
        lo.fullscan_per_push,
        hi.fullscan_per_push,
    );
    // Bounded ⇒ a 10× base should NOT cost ~10× the VM work. Allow generous slack (3×) so this is
    // a regression gate on "O(N) per push", not a tight microbenchmark.
    assert!(
        hi.vm_per_push <= lo.vm_per_push * 3.0,
        "indexed+analyzed LIMIT-push looks O(N): vm/push went {:.0} (10k) → {:.0} (100k)",
        lo.vm_per_push,
        hi.vm_per_push
    );
}
