#![cfg(feature = "testkit")]

//! Regression: `whereExists(child filter).limit(n)` over many parents that SHARE one
//! child, when a single source change to that child flips `EXISTS` for all of them in
//! one push (the "cascading backfill / runaway push" scenario — renaming a project
//! that 240k issues filter on).
//!
//! **Post root-fix behavior (see `RUNAWAY-PUSH-FINDINGS.md`).** A reentrant mid-push
//! backfill now reads **pre-change membership** via the Zero-faithful in-flight child
//! overlay: `Join` publishes the change LIVE (`inprogress_overlay`/`inprogress_position`,
//! `graph.rs`) and a reentrant refetch re-adds the not-yet-removed siblings, so the
//! backfill does NOT mask them. `Take` then refills the window one row at a time and the
//! removes **cascade** — exactly as upstream Zero does. The end state is correct (the
//! view empties), and the change stream is the cascade: ~one Remove per parent, each
//! (until the window can no longer refill) paired with an Add. This is *correct but
//! unbounded* — bounding it back to `O(limit)` is the separate `TakeGate` perf work
//! (findings §3/§6 step 2), NOT a correctness concern.
//!
//! Ground truth: after removing the only matching child, NO parent satisfies `EXISTS`,
//! so the view MUST be empty.

use rindle::change::FetchRequest;
use rindle::graph::Graph;
use rindle::testkit::{run_push_test_ast_view, CaughtChange, TableSpec};
use rindle::value::{owned_row, OwnedValue as V, SourceSchema};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    Lit, Op, OrderPart, SimpleCondition, SourceChange, ValuePosition,
};

fn issue_schema() -> SourceSchema {
    // issue(id, projectID); PK id; sort by id.
    SourceSchema::new(vec!["id", "projectID"], vec![0], vec![(0, true)])
}
fn project_schema() -> SourceSchema {
    // project(id, name); PK id; sort by id.
    SourceSchema::new(vec!["id", "name"], vec![0], vec![(0, true)])
}

/// `issue.whereExists('project', p => p.name == 'zero').orderBy('id').limit(limit)`.
fn query(limit: u32, with_order: bool) -> Ast {
    Ast {
        table: "issue".into(),
        order_by: if with_order {
            vec![OrderPart("id".into(), Dir::Asc)]
        } else {
            vec![]
        },
        limit: Some(limit),
        r#where: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["projectID".into()],
                    child_field: vec!["id".into()],
                },
                subquery: Box::new(Ast {
                    table: "project".into(),
                    alias: Some("project".into()),
                    order_by: vec![OrderPart("id".into(), Dir::Asc)],
                    r#where: Some(Condition::Simple(SimpleCondition {
                        op: Op::Eq,
                        left: ValuePosition::Column {
                            name: "name".into(),
                        },
                        right: ValuePosition::Literal {
                            value: Lit::Str("zero".into()),
                        },
                    })),
                    ..Default::default()
                }),
                system: None,
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        })),
        ..Default::default()
    }
}

fn sources(n: i64) -> Vec<TableSpec> {
    let issue_rows: Vec<Vec<V>> = (1..=n).map(|i| vec![V::Int(i), V::Int(1)]).collect();
    vec![
        TableSpec::new("issue", issue_schema(), issue_rows),
        TableSpec::new(
            "project",
            project_schema(),
            vec![vec![V::Int(1), V::str("zero")]],
        ),
    ]
}

fn rename_change() -> SourceChange {
    SourceChange::Edit {
        row: owned_row(vec![V::Int(1), V::str("zero2")]),
        old: owned_row(vec![V::Int(1), V::str("zero")]),
    }
}
fn delete_change() -> SourceChange {
    SourceChange::Remove(owned_row(vec![V::Int(1), V::str("zero")]))
}

/// The view must be EMPTY after the flip, for every (limit, N, variant).
#[test]
fn view_empties_when_shared_child_flip_removes_all_parents() {
    for (variant, change) in [("rename", rename_change()), ("delete", delete_change())] {
        for &limit in &[1u32, 2, 3, 10] {
            for &n in &[2i64, 5, 50] {
                if (n as u32) < limit {
                    continue;
                }
                let res = run_push_test_ast_view(
                    sources(n),
                    &query(limit, true),
                    vec![("project", change.clone())],
                    false,
                )
                .expect("build+run");
                assert_eq!(
                    res.initial.len(),
                    limit.min(n as u32) as usize,
                    "{variant} limit={limit} N={n}: hydrate should hold min(limit, N)"
                );
                assert_eq!(
                    res.data.len(),
                    0,
                    "{variant} limit={limit} N={n}: after the only matching child is removed, \
                     NO parent satisfies EXISTS, so the view must be empty — got stale rows {:?}",
                    res.data
                        .iter()
                        .map(|nd| match &nd.row.col(0).to_owned() {
                            V::Int(i) => *i,
                            _ => -1,
                        })
                        .collect::<Vec<_>>()
                );
            }
        }
    }
}

/// The same bug without an explicit `order_by` (Take falls back to PK order) — the
/// fuzzer only excludes the `exists + order + limit` triple, so this plain
/// `exists + limit` shape is in the supported space.
#[test]
fn view_empties_without_explicit_order_by() {
    let res = run_push_test_ast_view(
        sources(5),
        &query(2, false),
        vec![("project", delete_change())],
        false,
    )
    .expect("build+run");
    assert_eq!(res.initial.len(), 2);
    assert_eq!(
        res.data.len(),
        0,
        "exists+limit (no order): view must empty"
    );
}

/// `Take` cascades to an empty view: every parent's removal is delivered, the window
/// refills one committed row at a time (so each Remove until the window can no longer
/// refill is paired with an Add), and the end state is empty. This is the correct
/// (Zero-faithful) cascade; its O(N) size — not O(limit) — is the documented perf
/// followup (`TakeGate`), not a bug.
#[test]
fn take_cascades_removes_to_empty_view() {
    let limit = 10u32;
    let n = 5_000i64;
    let res = run_push_test_ast_view(
        sources(n),
        &query(limit, true),
        vec![("project", rename_change())],
        false,
    )
    .expect("build+run");

    let (adds, removes, others) =
        res.pushes
            .iter()
            .fold((0usize, 0usize, 0usize), |(a, r, o), c| match c {
                CaughtChange::Add(_) => (a + 1, r, o),
                CaughtChange::Remove(_) => (a, r + 1, o),
                _ => (a, r, o + 1),
            });
    assert_eq!(others, 0, "only Adds/Removes are forwarded");
    assert_eq!(
        removes, n as usize,
        "every parent that lost EXISTS is removed exactly once"
    );
    // The window refills until the last `limit` rows have nothing past them to pull,
    // so exactly `limit` of the removes are unpaired.
    assert_eq!(
        adds,
        n as usize - limit as usize,
        "each remove refills except the final `limit` that exhaust the window"
    );
    assert_eq!(res.data.len(), 0, "view empties");
}

/// End-to-end: the rename push drives the pipeline to a consistent empty state via the
/// cascade, and a fresh re-fetch (no overlay) confirms it. The forwarded change stream
/// is now the O(N) cascade (every doomed parent removed, the window refilled until it
/// can't) — bounding it back to O(limit) is the `TakeGate` perf followup, so this test
/// asserts CORRECTNESS (the empty end state), not a stream bound.
#[test]
fn rename_push_drives_pipeline_to_empty_via_cascade() {
    let limit = 10u32;
    for &n in &[1_000i64, 8_000] {
        let mut g = Graph::new();
        let issue_rows: Vec<_> = (1..=n)
            .map(|i| owned_row(vec![V::Int(i), V::Int(1)]))
            .collect();
        let issue_src = g.add_source(issue_schema(), issue_rows);
        let project_src = g.add_source(
            project_schema(),
            vec![owned_row(vec![V::Int(1), V::str("zero")])],
        );
        let (is, ps) = (issue_schema(), project_schema());
        let resolve = |t: &str| match t {
            "issue" => Some((issue_src, is.clone())),
            "project" => Some((project_src, ps.clone())),
            _ => None,
        };
        let top = rindle::build_pipeline(&mut g, &query(limit, true), &resolve).expect("build");
        let catch = g.add_catch(top, false);
        g.set_sink_edge(top, catch);
        let initial = g.catch_fetch(catch, &FetchRequest::all());
        assert_eq!(initial.len(), limit as usize);

        g.source_push(project_src, rename_change());

        // The view is consistent: a fresh re-fetch (no overlay) sees no matches.
        let after = g.catch_fetch(catch, &FetchRequest::all());
        assert_eq!(
            after.len(),
            0,
            "N={n}: fresh re-fetch must see an empty result"
        );
    }
}

/// FOLLOWER-LAG-SHED §6.6 — the runaway-push bail. The exact shape the ladder cannot see
/// (backlog may be ONE change whose derive fans out unboundedly): with a push deadline armed,
/// the join child fan-out's checkpoint parks a `PushDeadlineExceeded` and stops iterating, and
/// `try_source_push` surfaces it — instead of spinning the full cascade. Torn operator state
/// is the contract (the host discards the engine, abort≙epoch-rehydrate). An UNARMED graph —
/// the wasm client's permanent state — is untouched and never consults the clock.
#[test]
fn armed_push_deadline_bails_the_runaway_cascade() {
    let limit = 10u32;
    let n = 8_000i64;
    let build = || {
        let mut g = Graph::new();
        let issue_rows: Vec<_> = (1..=n)
            .map(|i| owned_row(vec![V::Int(i), V::Int(1)]))
            .collect();
        let issue_src = g.add_source(issue_schema(), issue_rows);
        let project_src = g.add_source(
            project_schema(),
            vec![owned_row(vec![V::Int(1), V::str("zero")])],
        );
        let (is, ps) = (issue_schema(), project_schema());
        let resolve = move |t: &str| match t {
            "issue" => Some((issue_src, is.clone())),
            "project" => Some((project_src, ps.clone())),
            _ => None,
        };
        let top = rindle::build_pipeline(&mut g, &query(limit, true), &resolve).expect("build");
        let catch = g.add_catch(top, false);
        g.set_sink_edge(top, catch);
        let initial = g.catch_fetch(catch, &FetchRequest::all());
        assert_eq!(initial.len(), limit as usize);
        (g, project_src, catch)
    };

    // Armed with an already-expired deadline: the first amortized checkpoint (after ~1024
    // fan-out iterations, far short of the 8000-parent cascade) must bail.
    let (g, project_src, _catch) = build();
    g.set_push_deadline(Some(std::time::Instant::now()));
    let err = g
        .try_source_push(project_src, rename_change())
        .expect_err("an expired deadline must bail the runaway fan-out");
    assert!(
        matches!(err, rindle::RindleError::PushDeadlineExceeded { .. }),
        "the bail must be classified, not a generic error: {err}"
    );

    // Unarmed (the wasm client's state): the same push runs the full cascade to the correct
    // empty view — the checkpoint never consults the clock and changes nothing.
    let (g, project_src, catch) = build();
    g.try_source_push(project_src, rename_change())
        .expect("unarmed push must complete");
    assert_eq!(
        g.catch_fetch(catch, &FetchRequest::all()).len(),
        0,
        "unarmed cascade still converges to the empty view"
    );
}

/// FOLLOWER-LAG-SHED §6.6 — the armed deadline must bound the WHOLE armed derive, not merely a
/// single fan-out loop. The worker arms the deadline ONCE around a whole `apply_and_drain(batch)`
/// (`parallel.rs`), which pushes the batch change-by-change. A batch of many SMALL-fan-out changes
/// — here one project rename per issue, each flipping EXISTS for its ONE parent — can sum past the
/// deadline while every individual `push_child_change` loop runs just ONCE, far short of
/// `DEADLINE_CHECK_EVERY` (1024). The checkpoint tick counter must therefore accumulate across the
/// armed window so the aggregate crosses the checkpoint and the (already-expired) deadline bails.
///
/// A counter reset per fan-out loop never accumulates across small changes, so it NEVER consults
/// the clock for this shape — the derive runs unbounded past the deadline, and the coordinator's
/// next begin-barrier catches the wedged worker bluntly (`ACK_TIMEOUT` → whole-worker detach +
/// leaked thread), the exact outcome §6.6 exists to preempt. This test is the regression guard.
#[test]
fn armed_push_deadline_bails_spread_work_across_many_small_changes() {
    // > DEADLINE_CHECK_EVERY (1024) projects, each matched by EXACTLY ONE issue (issue k has
    // projectID k), so a rename of project k fans out to the single issue k — one checkpoint tick
    // per change. A limit that holds every match means no `Take` backfill runs: each change's only
    // checkpointed loop is that one-parent join fan-out.
    let n = 4_000i64;
    let mut g = Graph::new();
    let issue_rows: Vec<_> = (1..=n)
        .map(|i| owned_row(vec![V::Int(i), V::Int(i)]))
        .collect();
    let issue_src = g.add_source(issue_schema(), issue_rows);
    let project_rows: Vec<_> = (1..=n)
        .map(|i| owned_row(vec![V::Int(i), V::str("zero")]))
        .collect();
    let project_src = g.add_source(project_schema(), project_rows);
    let (is, ps) = (issue_schema(), project_schema());
    let resolve = |t: &str| match t {
        "issue" => Some((issue_src, is.clone())),
        "project" => Some((project_src, ps.clone())),
        _ => None,
    };
    let top = rindle::build_pipeline(&mut g, &query(n as u32, true), &resolve).expect("build");
    let catch = g.add_catch(top, false);
    g.set_sink_edge(top, catch);
    assert_eq!(
        g.catch_fetch(catch, &FetchRequest::all()).len(),
        n as usize,
        "every issue matches initially"
    );

    // One armed window (the `apply_and_drain` analog): arm an already-expired deadline ONCE, then
    // push the batch change-by-change. The only variable is whether the checkpoint counter reaches
    // DEADLINE_CHECK_EVERY — deterministic, no wall-clock race.
    g.set_push_deadline(Some(std::time::Instant::now()));
    let mut bailed_after: Option<i64> = None;
    for k in 1..=n {
        match g.try_source_push(
            project_src,
            SourceChange::Edit {
                old: owned_row(vec![V::Int(k), V::str("zero")]),
                row: owned_row(vec![V::Int(k), V::str("zero2")]),
            },
        ) {
            Ok(()) => {}
            Err(rindle::RindleError::PushDeadlineExceeded { .. }) => {
                bailed_after = Some(k);
                break;
            }
            Err(e) => panic!("unexpected error on change {k}: {e}"),
        }
    }

    let bailed_after = bailed_after.expect(
        "the armed deadline must bail once the AGGREGATE fan-out crosses the checkpoint; a tick \
         counter reset per loop never accumulates across small changes, so the derive runs \
         unbounded past the deadline",
    );
    // It must bail near the checkpoint interval (~1024, one tick per small change), NOT near the
    // full 4000 — proving the bound is over the whole armed window, not a single fan-out loop.
    assert!(
        bailed_after <= 1100,
        "bail should land near DEADLINE_CHECK_EVERY (1024) — got {bailed_after}, i.e. the deadline \
         is being enforced per-change instead of across the batch"
    );
}
