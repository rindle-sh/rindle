//! Scaffold tests for `203-MUTATOR-READS-DESIGN.md` — reads (one-shot queries) inside a
//! mutator, served by a lazy read-cache **fork** seeded from the staged buffer (§4), while
//! commit stays plain buffer-replay (§6).
//!
//! `WriteTxn::query` / `MutationTx.query` do not exist yet, so these tests exercise the
//! **mechanism** the new primitive will be built from, at the `Graph` / `MemorySource`
//! level, with today's public API:
//!
//!   subject  = a COW `fork` of the live source + the buffer replayed onto it, queried
//!              through a transient pipeline over a *scratch* source node (exactly §5.2);
//!   oracle   = a parallel store that *committed* the same buffer, queried normally
//!              (the project's standard fresh-hydrate oracle — `optimistic_driver.rs`).
//!
//! The design contract (§4.1 / §12.1) is that these are **equal**: the seed-replay and the
//! commit apply the identical buffer, so any divergence is a real fork/query bug. The
//! `#[ignore]` skeletons at the bottom are the API-level cases that land with the
//! implementation.

use std::collections::BTreeMap;

use rindle::changes::CaughtChange;
use rindle::graph::{Graph, NodeId};
use rindle::value::{owned_row, OwnedRow, OwnedValue, SourceSchema};
use rindle::{build_pipeline, table, Ast, SourceChange};

use OwnedValue::Int;

// --- the `issue(id, owner, score)` table under test --------------------------------

fn schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "owner", "score"], vec![0], vec![(0, true)])
}
fn irow(id: i64, owner: i64, score: i64) -> OwnedRow {
    owned_row(vec![Int(id), Int(owner), Int(score)])
}
fn select_all() -> Ast {
    rindle::table("issue").build()
}

/// Hydrate a sink's snapshot into bare integer rows, sorted by pk — the comparable shape
/// for the differential (sidesteps `OwnedValue` equality: every cell here is an `Int`).
fn hydrate_ints(graph: &Graph, sink: NodeId) -> Vec<Vec<i64>> {
    let mut rows: Vec<Vec<i64>> = graph
        .try_hydrate_change_sink(sink)
        .unwrap()
        .into_iter()
        .map(|c| match c {
            CaughtChange::Add(n) => (0..3)
                .map(|i| match &n.row.col(i).to_owned() {
                    Int(v) => *v,
                    other => panic!("non-int cell {other:?}"),
                })
                .collect(),
            other => panic!("hydrate emitted a non-add: {other:?}"),
        })
        .collect();
    rows.sort();
    rows
}

/// ORACLE: a fresh store that *commits* `base` then `buffer`, queried as an ordinary
/// registered query (§12.1).
fn oracle_rows(base: Vec<OwnedRow>, buffer: &[SourceChange], ast: &Ast) -> Vec<Vec<i64>> {
    let mut g = Graph::new();
    let sid = g.try_add_source(schema(), base).unwrap();
    for c in buffer {
        g.try_source_push(sid, c.clone()).unwrap();
    }
    let resolve = |t: &str| (t == "issue").then(|| (sid, schema()));
    let top = build_pipeline(&mut g, ast, &resolve).unwrap();
    let sink = g.add_change_sink(top);
    g.set_sink_edge(top, sink);
    hydrate_ints(&g, sink)
}

/// SUBJECT: fork the live source, replay `buffer` onto the fork, query it through a
/// transient pipeline over a scratch source node — the §5.2 mechanism, minus the
/// `WriteTxn` wrapper.
fn subject_rows_over_fork(
    base: Vec<OwnedRow>,
    buffer: &[SourceChange],
    ast: &Ast,
) -> Vec<Vec<i64>> {
    let mut g = Graph::new();
    let live = g.try_add_source(schema(), base).unwrap();
    let fork = g.memory_source(live).unwrap().fork(); // O(1) COW (§4.1)
    let scratch = g.add_memory_source(fork);
    for c in buffer {
        g.try_source_push(scratch, c.clone()).unwrap(); // replay the buffer onto the fork
    }
    let resolve = |t: &str| (t == "issue").then(|| (scratch, schema()));
    let top = build_pipeline(&mut g, ast, &resolve).unwrap();
    let sink = g.add_change_sink(top);
    g.set_sink_edge(top, sink);
    hydrate_ints(&g, sink)
}

// --- runnable mechanism tests (green today) ----------------------------------------

/// Core differential: a query over `fork + replay(buffer)` equals a query over
/// `commit(buffer)` — read-your-writes for add / edit / remove (§4.1, §12.1).
#[test]
fn fork_with_replayed_buffer_matches_committed() {
    let base = vec![irow(1, 10, 100), irow(2, 20, 200), irow(3, 30, 300)];
    let buffer = vec![
        SourceChange::Add(irow(4, 40, 400)),
        SourceChange::Edit {
            old: irow(2, 20, 200),
            row: irow(2, 20, 250),
        },
        SourceChange::Remove(irow(3, 30, 300)),
    ];
    let q = select_all();

    let subject = subject_rows_over_fork(base.clone(), &buffer, &q);
    let oracle = oracle_rows(base.clone(), &buffer, &q);
    assert_eq!(
        subject, oracle,
        "in-mutator read (fork+buffer) must equal read-after-commit (oracle)"
    );

    // The read genuinely reflects the buffer (would differ from the untouched base).
    let base_only = oracle_rows(base, &[], &q);
    assert_ne!(
        subject, base_only,
        "the query must observe the mutator's own writes-in-progress"
    );
    assert_eq!(
        subject,
        vec![vec![1, 10, 100], vec![2, 20, 250], vec![4, 40, 400]]
    );
}

/// Isolation + no-leak (§12.2): a scratch query must not disturb the live query, and must
/// fully reclaim its pipeline + scratch source — `node_count` stays flat across two
/// build/teardown cycles (the slot-reuse idiom, `graph.rs:767`).
#[test]
fn scratch_query_is_isolated_and_reuses_slots() {
    let mut g = Graph::new();
    let live = g
        .try_add_source(schema(), vec![irow(1, 10, 100), irow(2, 20, 200)])
        .unwrap();
    let q = select_all();
    let resolve_live = |t: &str| (t == "issue").then(|| (live, schema()));
    let live_top = build_pipeline(&mut g, &q, &resolve_live).unwrap();
    let live_sink = g.add_change_sink(live_top);
    g.set_sink_edge(live_top, live_sink);
    let live_before = hydrate_ints(&g, live_sink);

    let after1 = run_scratch_query_and_teardown(&mut g, live, &q);
    let after2 = run_scratch_query_and_teardown(&mut g, live, &q);
    assert_eq!(
        after1, after2,
        "scratch query slots must be reclaimed/reused — node_count flat across cycles"
    );

    let live_after = hydrate_ints(&g, live_sink);
    assert_eq!(
        live_before, live_after,
        "a scratch query must not perturb the live query"
    );
}

/// Build a scratch query over a fork of `live`, hydrate, then tear it all down (the §5.2
/// success-path teardown). Returns the graph's slot count after teardown.
fn run_scratch_query_and_teardown(g: &mut Graph, live: NodeId, ast: &Ast) -> usize {
    let fork = g.memory_source(live).unwrap().fork();
    let scratch = g.add_memory_source(fork);
    g.begin_recording();
    let resolve = |t: &str| (t == "issue").then(|| (scratch, schema()));
    let top = build_pipeline(g, ast, &resolve).unwrap();
    let sink = g.add_change_sink(top);
    g.set_sink_edge(top, sink);
    let _ = g.try_hydrate_change_sink(sink).unwrap();
    let manifest = g.take_recording();
    g.destroy_pipeline(&manifest);
    g.remove_source(scratch).unwrap(); // the scratch source is gone; its slot returns to the recycler
    g.node_count()
}

/// Seeded property test of the §12.3 differential relation over random valid buffers — the
/// thing to promote into `rindle-fuzz::sut_db` once `WriteTxn::query` exists.
#[test]
fn differential_property_over_random_buffers() {
    // A tiny deterministic LCG (no `Math.random` / no dev-dep): reproducible across runs.
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = |n: u64| -> u64 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) % n
    };
    let q = select_all();

    for _ in 0..400 {
        // Random base: pks 1..=8 present with prob 1/2.
        let mut present: BTreeMap<i64, OwnedRow> = BTreeMap::new();
        for id in 1..=8i64 {
            if next(2) == 0 {
                present.insert(id, irow(id, next(5) as i64, next(100) as i64));
            }
        }
        let base: Vec<OwnedRow> = present.values().cloned().collect();

        // Random VALID buffer (the present-set keeps each op legal against the prior state).
        let mut buffer = Vec::new();
        for _ in 0..next(6) {
            let id = 1 + next(8) as i64;
            if let Some(old) = present.get(&id).cloned() {
                if next(2) == 0 {
                    let new = irow(id, next(5) as i64, next(100) as i64);
                    buffer.push(SourceChange::Edit {
                        old,
                        row: new.clone(),
                    });
                    present.insert(id, new);
                } else {
                    buffer.push(SourceChange::Remove(old));
                    present.remove(&id);
                }
            } else {
                let new = irow(id, next(5) as i64, next(100) as i64);
                buffer.push(SourceChange::Add(new.clone()));
                present.insert(id, new);
            }
        }

        let subject = subject_rows_over_fork(base.clone(), &buffer, &q);
        let oracle = oracle_rows(base, &buffer, &q);
        assert_eq!(subject, oracle, "differential failed for buffer {buffer:?}");
    }
}

/// §7.3 (A): the read-cache fork COW-carries the live source's existing secondary indexes, so a
/// one-shot query that sorts in an order a live query already built **reuses** that index (a one-
/// `Rc` bump) instead of rebuilding it. Observable via `get_index_keys`: querying the carrying
/// fork-of-fork adds **no** new index, whereas a primary-only `fork()` rebuilds it.
#[test]
fn read_cache_fork_carries_secondary_indexes() {
    let mut g = Graph::new();
    let live = g
        .try_add_source(
            schema(),
            vec![irow(1, 10, 30), irow(2, 20, 10), irow(3, 30, 20)],
        )
        .unwrap();

    // A live query sorted by `score` builds that secondary index on the live source.
    let by_score = table("issue").order_by("score", "asc").build();
    let resolve_live = |t: &str| (t == "issue").then(|| (live, schema()));
    let live_top = build_pipeline(&mut g, &by_score, &resolve_live).unwrap();
    let live_sink = g.add_change_sink(live_top);
    g.set_sink_edge(live_top, live_sink);
    let _ = g.try_hydrate_change_sink(live_sink).unwrap(); // forces the score index to build on live
    let live_keys = g.memory_source(live).unwrap().get_index_keys().len();
    assert_eq!(
        live_keys, 2,
        "live holds the primary + the orderBy(score) secondary index"
    );

    // CARRYING fork: the cache fork-of-fork inherits every index (incl. the score one) via COW,
    // so the same query reuses it — no rebuild (index count is flat across the query).
    let carried = g.memory_source(live).unwrap().fork_with_indexes();
    assert_eq!(
        carried.get_index_keys().len(),
        live_keys,
        "fork_with_indexes carries every secondary index (COW), not just the primary"
    );
    let carried_id = g.add_memory_source(carried);
    let before = g.memory_source(carried_id).unwrap().get_index_keys().len();
    let resolve = |t: &str| (t == "issue").then(|| (carried_id, schema()));
    let top = build_pipeline(&mut g, &by_score, &resolve).unwrap();
    let sink = g.add_change_sink(top);
    g.set_sink_edge(top, sink);
    let _ = g.try_hydrate_change_sink(sink).unwrap();
    let after = g.memory_source(carried_id).unwrap().get_index_keys().len();
    assert_eq!(
        before, after,
        "the carried score index was reused — no rebuild on the read-cache fork"
    );

    // CONTRAST: a primary-only `fork()` carries just the primary, so the same query must REBUILD.
    let plain = g.memory_source(live).unwrap().fork();
    assert_eq!(
        plain.get_index_keys().len(),
        1,
        "plain fork carries only the primary index"
    );
    let plain_id = g.add_memory_source(plain);
    let before_plain = g.memory_source(plain_id).unwrap().get_index_keys().len();
    let resolve2 = |t: &str| (t == "issue").then(|| (plain_id, schema()));
    let top2 = build_pipeline(&mut g, &by_score, &resolve2).unwrap();
    let sink2 = g.add_change_sink(top2);
    g.set_sink_edge(top2, sink2);
    let _ = g.try_hydrate_change_sink(sink2).unwrap();
    let after_plain = g.memory_source(plain_id).unwrap().get_index_keys().len();
    assert_eq!(
        after_plain,
        before_plain + 1,
        "a primary-only fork rebuilds the score index on query"
    );
}

// --- API-level skeletons (un-ignore as `WriteTxn::query` lands) ---------------------

/// §12.2 read-your-writes through the real primitive. Belongs at the wasm/TS layer where
/// `WriteTxn::query` / `MutationTx.query` live (the wasm `Db` is `#[wasm_bindgen]`, awkward
/// from a native test); the mechanism above is the stand-in until then.
#[test]
#[ignore = "203: needs WriteTxn::query (see packages/optimistic/test/mutator-reads.test.ts)"]
fn read_your_writes_via_writetxn_query() {
    // let tx = db.write();
    // tx.add("issue", [4, 40, 400]);
    // assert_eq!(tx.query(select_all()), oracle_rows(base, &[add(4..)], &select_all()));
    todo!("203: WriteTxn::query")
}

/// §12.2 query semantics: the differential extended to `where` / `orderBy` / `limit` ASTs
/// — same `oracle_rows` / `subject_rows_over_fork` helpers, just a richer `Ast`. The fork
/// must produce the same rows as a fresh commit for a filtered / limited query.
#[test]
fn where_orderby_limit_over_fork() {
    let base = vec![irow(1, 10, 100), irow(2, 20, 200), irow(3, 30, 300)];
    let buffer = vec![
        SourceChange::Add(irow(4, 40, 400)),
        SourceChange::Edit {
            old: irow(2, 20, 200),
            row: irow(2, 20, 250),
        },
    ];

    // Filtered: score >= 250. Post-buffer the matches are 2@250, 3@300, 4@400. `hydrate_ints`
    // sorts both sides, so the differential is order-independent; the golden anchors the rows.
    let filtered = table("issue").where_op("score", ">=", 250).build();
    let subject = subject_rows_over_fork(base.clone(), &buffer, &filtered);
    let oracle = oracle_rows(base.clone(), &buffer, &filtered);
    assert_eq!(
        subject, oracle,
        "filtered query over the fork must equal read-after-commit"
    );
    assert_eq!(
        subject,
        vec![vec![2, 20, 250], vec![3, 30, 300], vec![4, 40, 400]]
    );

    // Ordered + limited: top 2 by score desc. Subject and oracle share the AST + ordering, so
    // the kept set is identical; the golden pins which rows the limit keeps (4@400, 3@300).
    let limited = table("issue").order_by("score", "desc").limit(2).build();
    let subject = subject_rows_over_fork(base.clone(), &buffer, &limited);
    let oracle = oracle_rows(base, &buffer, &limited);
    assert_eq!(
        subject, oracle,
        "ordered+limited query over the fork must equal read-after-commit"
    );
    assert_eq!(subject, vec![vec![3, 30, 300], vec![4, 40, 400]]);
}

// --- the untouched `comment(id, issueID)` table for the join test ------------------

fn comment_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}
fn crow(id: i64, issue_id: i64) -> OwnedRow {
    owned_row(vec![Int(id), Int(issue_id)])
}

/// Build the `issue WHERE EXISTS(comment ON issue.id = comment.issueID)` join over two
/// sources, hydrate the issue sink, and return its int rows. `issue_init`/`comment_init`
/// seed the sources; `buffer` is replayed onto the issue source only.
fn join_rows(
    issue_init: Vec<OwnedRow>,
    comment_init: Vec<OwnedRow>,
    buffer: &[SourceChange],
    fork_issue: bool,
) -> Vec<Vec<i64>> {
    let ast = table("issue")
        .where_exists(|row| {
            table("comment")
                .r#where("issueID", row.col("id"))
                .alias("hasComment") // the non-flipped EXISTS semi-join needs an aliased child
        })
        .build();
    let mut g = Graph::new();
    let ilive = g.try_add_source(schema(), issue_init).unwrap();
    let clive = g.try_add_source(comment_schema(), comment_init).unwrap();
    // SUBJECT forks both sources (issue written via the buffer; comment untouched — O(1) COW off
    // live, §4.1) and replays the buffer onto the issue fork; ORACLE commits the buffer directly.
    let (issue_src, comment_src) = if fork_issue {
        let ifork = g.memory_source(ilive).unwrap().fork();
        let cfork = g.memory_source(clive).unwrap().fork();
        (g.add_memory_source(ifork), g.add_memory_source(cfork))
    } else {
        (ilive, clive)
    };
    for c in buffer {
        g.try_source_push(issue_src, c.clone()).unwrap();
    }
    let resolve = |t: &str| match t {
        "issue" => Some((issue_src, schema())),
        "comment" => Some((comment_src, comment_schema())),
        _ => None,
    };
    let top = build_pipeline(&mut g, &ast, &resolve).unwrap();
    let sink = g.add_change_sink(top);
    g.set_sink_edge(top, sink);
    hydrate_ints(&g, sink)
}

/// §12.2 join a written table to an untouched one (untouched forked O(1) off live): only the
/// in-mutator-written issue that has a matching base comment survives the EXISTS join, and the
/// fork path agrees with read-after-commit.
#[test]
fn join_written_table_to_untouched() {
    // Untouched base: a comment for issue 1 only. Issue table starts empty.
    let comment_base = vec![crow(100, 1)];
    // In-mutator buffer: add issues 1 and 2 — only 1 has a matching comment.
    let buffer = vec![
        SourceChange::Add(irow(1, 10, 100)),
        SourceChange::Add(irow(2, 20, 200)),
    ];

    let subject = join_rows(vec![], comment_base.clone(), &buffer, true);
    let oracle = join_rows(vec![], comment_base, &buffer, false);
    assert_eq!(
        subject, oracle,
        "the join over the fork must equal read-after-commit"
    );
    assert_eq!(
        subject,
        vec![vec![1, 10, 100]],
        "only the written issue with a matching untouched-base comment survives the EXISTS join"
    );
}

/// §12.2 rebase re-invocation: drive `OptimisticTables::rewind` + re-invoke a
/// `query`-reading mutator, assert the prediction matches a fresh-hydrate of the server
/// state (the `optimistic_driver.rs` cycle harness, extended with an in-mutator query).
#[test]
#[ignore = "203: needs WriteTxn::query in the re-invocation path"]
fn rebase_reinvocation_query_prediction() {
    todo!("203: cycle oracle with a query-reading mutator")
}
