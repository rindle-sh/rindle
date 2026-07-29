//! Unit tests for the backend-agnostic `source_common` seam — the utilities the
//! memory leaf AND the SQLite `TableSource` share. Ported from the JS
//! `memory-source.test.ts` suites that exercise the free functions directly
//! (`generateWithOverlayInner`, `overlaysFor*`, `generateWithOverlayUnordered`,
//! `mergeSortedStreams`). Rows are index-addressed (`col 0` is the id / sort key).

use std::cell::Cell;
use std::rc::Rc;

use rindle::change::{Basis, RowFlow, SourceChange, Start};
use rindle::source_common::{
    compute_overlays, generate_with_constraint, generate_with_filter, generate_with_overlay,
    generate_with_overlay_inner, generate_with_overlay_inner_unordered,
    generate_with_overlay_unordered, generate_with_start, merge_sorted_streams,
    overlays_for_constraint, overlays_for_multi_constraint, overlays_for_start_at, Overlay,
    Overlays, RowPredicate,
};
use rindle::value::{owned_row as orow, OwnedRow, OwnedValue::Int, RowStream, Sort};
use rindle::Constraint;

// --- helpers -------------------------------------------------------------
// The source pipeline emits ROWS, not nodes — these helpers stay row-level.

fn r1(id: i64) -> OwnedRow {
    orow(vec![Int(id)])
}
/// (id, s, n) — col 0 is the sort key, cols 1/2 are payload / constraint targets.
fn r3(id: i64, s: i64, n: i64) -> OwnedRow {
    orow(vec![Int(id), Int(s), Int(n)])
}
fn id_of(row: &OwnedRow) -> i64 {
    match &row.col(0).to_owned() {
        Int(i) => *i,
        other => panic!("expected Int id, got {other:?}"),
    }
}
fn ids(stream: RowFlow) -> Vec<i64> {
    stream.map(|r| id_of(&r)).collect()
}
fn sort1() -> Sort {
    vec![(0, true)]
}
fn row_vec(ids: &[i64]) -> Vec<OwnedRow> {
    ids.iter().map(|&i| r1(i)).collect()
}
fn stream(ids: &[i64]) -> RowFlow<'static> {
    Box::new(row_vec(ids).into_iter())
}

/// A minimal lending [`RowStream`] over an owned Vec — a backend-neutral leaf for
/// exercising the generic `generate_with_overlay*` functions without a B+tree.
struct VecCursor {
    rows: Vec<OwnedRow>,
    i: usize,
}
impl VecCursor {
    fn new(ids: &[i64]) -> VecCursor {
        VecCursor {
            rows: ids.iter().map(|&i| r1(i)).collect(),
            i: 0,
        }
    }
    fn rows(rows: Vec<OwnedRow>) -> VecCursor {
        VecCursor { rows, i: 0 }
    }
}
impl RowStream for VecCursor {
    type Row<'a> = &'a OwnedRow;
    fn next_row(&mut self) -> Option<&OwnedRow> {
        if self.i >= self.rows.len() {
            return None;
        }
        let idx = self.i;
        self.i += 1;
        Some(&self.rows[idx])
    }
}

// =========================================================================
// generateWithOverlayInner — the ordered splice matrix
// =========================================================================

fn splice(rows: &[i64], add: Option<i64>, remove: Option<i64>, reverse: bool) -> Vec<i64> {
    let overlays = Overlays {
        add: add.map(r1),
        remove: remove.map(r1),
    };
    ids(generate_with_overlay_inner(
        row_vec(rows).into_iter(),
        overlays,
        sort1(),
        reverse,
    ))
}

#[test]
fn overlay_inner_no_overlay_passthrough() {
    assert_eq!(splice(&[10, 20, 30], None, None, false), vec![10, 20, 30]);
}

#[test]
fn overlay_inner_add_before_all() {
    assert_eq!(
        splice(&[10, 20, 30], Some(5), None, false),
        vec![5, 10, 20, 30]
    );
}

#[test]
fn overlay_inner_add_in_middle() {
    assert_eq!(
        splice(&[10, 20, 30], Some(15), None, false),
        vec![10, 15, 20, 30]
    );
}

#[test]
fn overlay_inner_add_after_all_emitted_at_end() {
    // The "never emitted ⇒ yield at end" branch (memory-source.ts:875).
    assert_eq!(
        splice(&[10, 20, 30], Some(40), None, false),
        vec![10, 20, 30, 40]
    );
}

#[test]
fn overlay_inner_add_into_empty() {
    assert_eq!(splice(&[], Some(7), None, false), vec![7]);
}

#[test]
fn overlay_inner_remove_suppresses_match() {
    assert_eq!(splice(&[10, 20, 30], None, Some(20), false), vec![10, 30]);
}

#[test]
fn overlay_inner_remove_no_match_is_noop() {
    assert_eq!(
        splice(&[10, 20, 30], None, Some(25), false),
        vec![10, 20, 30]
    );
}

#[test]
fn overlay_inner_add_and_remove_together() {
    // add=25 splices before 30; remove=20 suppressed.
    assert_eq!(
        splice(&[10, 20, 30], Some(25), Some(20), false),
        vec![10, 25, 30]
    );
}

#[test]
fn overlay_inner_reverse_splices_descending() {
    // Rows already descending; add=25 lands between 30 and 20 under the reverse
    // comparator; remove=20 suppressed.
    assert_eq!(
        splice(&[30, 20, 10], Some(25), Some(20), true),
        vec![30, 25, 10]
    );
}

// =========================================================================
// overlaysFor{Constraint,StartAt,MultiConstraint} — pure narrowers
// =========================================================================

#[test]
fn overlays_for_constraint_keeps_match_drops_nonmatch() {
    // constraint: col 1 == 5.
    let c: Constraint = vec![(1, Int(5))];
    let o = Overlays {
        add: Some(r3(1, 5, 0)),    // matches
        remove: Some(r3(2, 9, 0)), // does not
    };
    let out = overlays_for_constraint(o, &c);
    assert!(out.add.is_some(), "matching add kept");
    assert!(out.remove.is_none(), "non-matching remove dropped");
}

#[test]
fn overlays_for_constraint_independent_add_remove() {
    let c: Constraint = vec![(1, Int(5))];
    let o = Overlays {
        add: Some(r3(1, 9, 0)),    // no match -> dropped
        remove: Some(r3(2, 5, 0)), // match    -> kept
    };
    let out = overlays_for_constraint(o, &c);
    assert!(out.add.is_none());
    assert!(out.remove.is_some());
}

#[test]
fn overlays_for_start_at_drops_before_start() {
    let o = Overlays {
        add: Some(r1(10)),    // before start 20 -> dropped
        remove: Some(r1(30)), // at/after        -> kept
    };
    let out = overlays_for_start_at(o, &r1(20), &sort1(), false);
    assert!(out.add.is_none());
    assert_eq!(out.remove.map(|r| id_of(&r)), Some(30));
}

#[test]
fn overlays_for_start_at_inclusive_at_boundary() {
    // A row exactly at startAt is kept (`< 0` drops, `== 0` keeps).
    let o = Overlays {
        add: Some(r1(20)),
        remove: None,
    };
    let out = overlays_for_start_at(o, &r1(20), &sort1(), false);
    assert_eq!(out.add.map(|r| id_of(&r)), Some(20));
}

#[test]
fn overlays_for_multi_constraint_keeps_if_any_entry_matches() {
    // col 1 IN {5, 7}.
    let mc: rindle::MultiConstraint = vec![vec![(1, Int(5))], vec![(1, Int(7))]];
    let o = Overlays {
        add: Some(r3(1, 7, 0)),    // matches the 2nd entry -> kept
        remove: Some(r3(2, 9, 0)), // matches none          -> dropped
    };
    let out = overlays_for_multi_constraint(o, &mc);
    assert!(out.add.is_some());
    assert!(out.remove.is_none());
}

#[test]
fn overlays_for_multi_constraint_compound_entry_full_match() {
    // Each entry is a conjunction: (col1==5 AND col2==1).
    let mc: rindle::MultiConstraint = vec![vec![(1, Int(5)), (2, Int(1))]];
    let full = Overlays {
        add: Some(r3(1, 5, 1)),
        remove: None,
    };
    assert!(overlays_for_multi_constraint(full, &mc).add.is_some());
    let partial = Overlays {
        add: Some(r3(1, 5, 9)), // col2 differs -> no match
        remove: None,
    };
    assert!(overlays_for_multi_constraint(partial, &mc).add.is_none());
}

#[test]
fn compute_overlays_runs_full_pipeline() {
    // Edit overlay -> {add: new, remove: old}; startAt drops the add (before),
    // constraint keeps the remove; predicate then drops the remove.
    let overlay = Overlay {
        epoch: 1,
        change: SourceChange::Edit {
            row: r3(10, 5, 0), // add  (id 10, col1 5)
            old: r3(30, 5, 0), // remove (id 30, col1 5)
        },
    };
    let constraint: Constraint = vec![(1, Int(5))];
    // predicate: keep only id != 30.
    let pred: RowPredicate = Rc::new(|row: &OwnedRow| id_of(row) != 30);
    let out = compute_overlays(
        Some(&r1(20)), // startAt 20 -> add(10) before -> dropped
        Some(&constraint),
        Some(&overlay),
        &sort1(),
        false,
        Some(&pred),
        &[],
    );
    assert!(out.add.is_none(), "add dropped by startAt");
    assert!(out.remove.is_none(), "remove dropped by predicate");
}

// =========================================================================
// generateWithOverlayUnordered + …InnerUnordered — eager add, PK suppress
// =========================================================================

#[test]
fn unordered_epoch_gating() {
    let overlay = Overlay {
        epoch: 5,
        change: SourceChange::Add(r1(3)),
    };
    // last_pushed_epoch < overlay.epoch -> overlay NOT applied.
    let skipped = generate_with_overlay_unordered(
        VecCursor::new(&[1, 2]),
        None,
        Some(&overlay),
        4,
        &[0],
        None,
        &[],
    );
    assert_eq!(ids(skipped), vec![1, 2]);

    // last_pushed_epoch >= overlay.epoch -> applied (add eager at head).
    let applied = generate_with_overlay_unordered(
        VecCursor::new(&[1, 2]),
        None,
        Some(&overlay),
        5,
        &[0],
        None,
        &[],
    );
    assert_eq!(ids(applied), vec![3, 1, 2]);
}

#[test]
fn unordered_remove_suppressed_by_pk() {
    let overlay = Overlay {
        epoch: 1,
        change: SourceChange::Remove(r1(2)),
    };
    let s = generate_with_overlay_unordered(
        VecCursor::new(&[1, 2, 3]),
        None,
        Some(&overlay),
        1,
        &[0],
        None,
        &[],
    );
    assert_eq!(ids(s), vec![1, 3]);
}

#[test]
fn unordered_edit_adds_new_suppresses_old() {
    // Edit keeps PK (col 0) the same; add new at head, suppress old by PK.
    let overlay = Overlay {
        epoch: 1,
        change: SourceChange::Edit {
            row: r3(2, 99, 0),
            old: r3(2, 0, 0),
        },
    };
    let rows = VecCursor::rows(vec![r3(1, 0, 0), r3(2, 0, 0), r3(3, 0, 0)]);
    let s = generate_with_overlay_unordered(rows, None, Some(&overlay), 1, &[0], None, &[]);
    // Eager add (id 2 new) at head, then 1, then old id 2 suppressed, then 3.
    assert_eq!(ids(s), vec![2, 1, 3]);
}

#[test]
fn unordered_constraint_and_predicate_narrow_overlay() {
    // Add overlay fails the constraint -> not injected.
    let overlay = Overlay {
        epoch: 1,
        change: SourceChange::Add(r3(9, 0, 0)), // col1 == 0
    };
    let c: Constraint = vec![(1, Int(5))];
    let s = generate_with_overlay_unordered(
        VecCursor::rows(vec![r3(1, 5, 0)]),
        Some(&c),
        Some(&overlay),
        1,
        &[0],
        None,
        &[],
    );
    assert_eq!(ids(s), vec![1]);
}

#[test]
fn unordered_inner_compound_pk_partial_no_suppress() {
    // PK = (col0, col1). remove = (1, 5). A row (1, 9) partially matches PK and
    // must NOT be suppressed; (1, 5) must be.
    let overlays = Overlays {
        add: None,
        remove: Some(r3(1, 5, 0)),
    };
    let inner = vec![r3(1, 9, 0), r3(1, 5, 0)].into_iter();
    let out: Vec<(i64, i64)> = generate_with_overlay_inner_unordered(inner, overlays, vec![0, 1])
        .map(|r| match (&r.col(0).to_owned(), &r.col(1).to_owned()) {
            (Int(a), Int(b)) => (*a, *b),
            _ => panic!(),
        })
        .collect();
    assert_eq!(out, vec![(1, 9)], "only the full-PK match is suppressed");
}

// =========================================================================
// generate_with_start / generate_with_constraint / generate_with_filter
// =========================================================================

#[test]
fn start_gate_at_and_after() {
    let at = generate_with_start(
        stream(&[1, 2, 3, 4]),
        Some(Start {
            row: r1(2),
            basis: Basis::At,
        }),
        sort1(),
        false,
    );
    assert_eq!(ids(at), vec![2, 3, 4]);

    let after = generate_with_start(
        stream(&[1, 2, 3, 4]),
        Some(Start {
            row: r1(2),
            basis: Basis::After,
        }),
        sort1(),
        false,
    );
    assert_eq!(ids(after), vec![3, 4]);
}

#[test]
fn constraint_breaks_not_filters() {
    // generate_with_constraint is a take_while: it stops at the first non-match,
    // even if later rows would match again (they can't, given a sorted prefix).
    let c: Constraint = vec![(1, Int(5))];
    let inner = vec![
        r3(1, 5, 0),
        r3(2, 5, 0),
        r3(3, 9, 0), // first non-match -> break here
        r3(4, 5, 0), // would match, but never reached
    ]
    .into_iter();
    let s = generate_with_constraint(Box::new(inner), Some(c));
    assert_eq!(ids(s), vec![1, 2]);
}

#[test]
fn filter_drops_non_matching() {
    let pred: RowPredicate = Rc::new(|row: &OwnedRow| id_of(row) % 2 == 0);
    let s = generate_with_filter(stream(&[1, 2, 3, 4]), pred);
    assert_eq!(ids(s), vec![2, 4]);
}

// =========================================================================
// mergeSortedStreams — k-way min-heap merge, reverse, Drop-clean
// =========================================================================

#[test]
fn merge_empty_inputs() {
    assert_eq!(
        ids(merge_sorted_streams(vec![], sort1(), false)),
        Vec::<i64>::new()
    );
    assert_eq!(
        ids(merge_sorted_streams(vec![stream(&[])], sort1(), false)),
        Vec::<i64>::new()
    );
    assert_eq!(
        ids(merge_sorted_streams(
            vec![stream(&[]), stream(&[])],
            sort1(),
            false
        )),
        Vec::<i64>::new()
    );
}

#[test]
fn merge_single_stream_passthrough() {
    assert_eq!(
        ids(merge_sorted_streams(
            vec![stream(&[1, 2, 3])],
            sort1(),
            false
        )),
        vec![1, 2, 3]
    );
}

#[test]
fn merge_two_streams_interleave() {
    assert_eq!(
        ids(merge_sorted_streams(
            vec![stream(&[1, 3, 5]), stream(&[2, 4, 6])],
            sort1(),
            false
        )),
        vec![1, 2, 3, 4, 5, 6]
    );
}

#[test]
fn merge_three_streams_varying_lengths() {
    assert_eq!(
        ids(merge_sorted_streams(
            vec![stream(&[1, 10]), stream(&[2, 5, 8, 11]), stream(&[3])],
            sort1(),
            false
        )),
        vec![1, 2, 3, 5, 8, 10, 11]
    );
}

#[test]
fn merge_equal_keys_all_emitted() {
    let out = ids(merge_sorted_streams(
        vec![stream(&[1, 2]), stream(&[2, 3])],
        sort1(),
        false,
    ));
    assert_eq!(out, vec![1, 2, 2, 3]);
}

#[test]
fn merge_reverse_via_comparator() {
    // Each input is descending; the merge under reverse yields global descending.
    assert_eq!(
        ids(merge_sorted_streams(
            vec![stream(&[5, 3, 1]), stream(&[6, 4, 2])],
            sort1(),
            true
        )),
        vec![6, 5, 4, 3, 2, 1]
    );
}

#[test]
fn merge_is_drop_clean_on_early_termination() {
    // Wrap each sub-stream in a Drop-counting guard. Take a couple of rows from
    // the merge, then drop it: every un-exhausted sub-stream must be dropped
    // (the Rust analogue of the JS `finally` -> `.return()`).
    let open = Rc::new(Cell::new(0i64));

    struct Guard(Rc<Cell<i64>>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.set(self.0.get() - 1);
        }
    }
    struct Guarded {
        inner: RowFlow<'static>,
        _g: Guard,
    }
    impl Iterator for Guarded {
        type Item = OwnedRow;
        fn next(&mut self) -> Option<OwnedRow> {
            self.inner.next()
        }
    }
    let guarded = |ids: &[i64]| -> RowFlow<'static> {
        open.set(open.get() + 1);
        Box::new(Guarded {
            inner: stream(ids),
            _g: Guard(open.clone()),
        })
    };

    let streams = vec![
        guarded(&[1, 3, 5, 7]),
        guarded(&[2, 4, 6, 8]),
        guarded(&[100]),
    ];
    assert_eq!(open.get(), 3);

    let mut merged = merge_sorted_streams(streams, sort1(), false);
    assert_eq!(merged.next().map(|r| id_of(&r)), Some(1));
    assert_eq!(merged.next().map(|r| id_of(&r)), Some(2));
    drop(merged); // early termination

    assert_eq!(open.get(), 0, "all sub-streams dropped on early close");
}

#[test]
fn merge_randomized_preserves_global_sort() {
    // Three pre-sorted streams of pseudo-random values; the merge must be globally
    // sorted and contain exactly the union (with multiplicity).
    let mk = |seed: u64, n: usize| -> Vec<i64> {
        let mut s = seed;
        let mut v: Vec<i64> = (0..n)
            .map(|_| {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
                (s >> 33) as i64 % 100
            })
            .collect();
        v.sort();
        v
    };
    let a = mk(1, 30);
    let b = mk(2, 40);
    let c = mk(3, 20);
    let mut expected: Vec<i64> = a.iter().chain(&b).chain(&c).copied().collect();
    expected.sort();
    let got = ids(merge_sorted_streams(
        vec![stream(&a), stream(&b), stream(&c)],
        sort1(),
        false,
    ));
    assert_eq!(got, expected);
    assert!(got.windows(2).all(|w| w[0] <= w[1]), "globally sorted");
}

// =========================================================================
// Laziness: the overlay/constraint chain is pull-driven. An early break does
// NOT scan the whole source — we never "fully map the source to apply an
// overlay". (The only eager work is compute_overlays over the <=2 overlay rows.)
// =========================================================================

/// A leaf RowStream that counts how many rows the pipeline pulls from it.
struct CountingCursor {
    rows: Vec<OwnedRow>,
    i: usize,
    pulled: Rc<Cell<usize>>,
}
impl RowStream for CountingCursor {
    type Row<'a> = &'a OwnedRow;
    fn next_row(&mut self) -> Option<&OwnedRow> {
        if self.i >= self.rows.len() {
            return None;
        }
        self.pulled.set(self.pulled.get() + 1);
        let idx = self.i;
        self.i += 1;
        Some(&self.rows[idx])
    }
}

#[test]
fn overlay_fetch_is_lazy_on_early_break() {
    // 1000-row leaf + a LIVE overlay add that sorts LAST (the worst case for the
    // "trailing add" branch — it is only emitted once the inner stream is
    // exhausted). A consumer that takes only 3 rows must pull only ~3 from the
    // leaf: the overlay is spliced lazily, never mapped over the whole stream.
    let pulled = Rc::new(Cell::new(0));
    let cursor = CountingCursor {
        rows: (0..1000).map(r1).collect(),
        i: 0,
        pulled: pulled.clone(),
    };
    let overlay = Overlay {
        epoch: 1,
        change: SourceChange::Add(r1(1_000_000)), // sorts after everything
    };
    let stream = generate_with_overlay(
        None,
        cursor,
        None,
        Some(&overlay),
        1,
        &sort1(),
        false,
        None,
        &[],
    );
    let first3: Vec<i64> = stream.take(3).map(|r| id_of(&r)).collect();
    assert_eq!(first3, vec![0, 1, 2]);
    assert!(
        pulled.get() <= 4,
        "lazy overlay fetch should pull ~3 of 1000 rows, pulled {}",
        pulled.get()
    );
}

#[test]
fn constraint_break_short_circuits_the_leaf() {
    // generate_with_constraint is a take_while: once the constraint stops matching
    // it stops pulling. The matching group (col 1 == 0) is a 50-row prefix; the
    // leaf must not be drained past the first non-match.
    let pulled = Rc::new(Cell::new(0));
    let mut rows: Vec<OwnedRow> = (0..50).map(|i| r3(i, 0, 0)).collect();
    rows.extend((50..1000).map(|i| r3(i, 1, 0))); // col 1 == 1, won't match
    let cursor = CountingCursor {
        rows,
        i: 0,
        pulled: pulled.clone(),
    };
    let overlaid = generate_with_overlay(None, cursor, None, None, 0, &sort1(), false, None, &[]);
    let c: Constraint = vec![(1, Int(0))];
    let got = ids(generate_with_constraint(overlaid, Some(c)));
    assert_eq!(got.len(), 50, "only the matching prefix is emitted");
    assert!(
        pulled.get() <= 52,
        "constraint break stops pulling at the first non-match, pulled {}",
        pulled.get()
    );
}

#[test]
fn full_chain_is_lazy_on_early_break() {
    // The whole fetch chain (overlay -> start -> constraint -> filter), taken
    // partially, pulls only as far as the consumer reads.
    let pulled = Rc::new(Cell::new(0));
    let cursor = CountingCursor {
        rows: (0..1000).map(r1).collect(),
        i: 0,
        pulled: pulled.clone(),
    };
    let overlay = Overlay {
        epoch: 1,
        change: SourceChange::Add(r1(5)), // lands at its sorted position, mid-stream
    };
    let pred: RowPredicate = Rc::new(|_r: &OwnedRow| true);
    let mut s = generate_with_overlay(
        None,
        cursor,
        None,
        Some(&overlay),
        1,
        &sort1(),
        false,
        Some(&pred),
        &[],
    );
    s = generate_with_start(s, None, sort1(), false);
    s = generate_with_constraint(s, None);
    s = generate_with_filter(s, pred);
    let first2: Vec<i64> = s.take(2).map(|r| id_of(&r)).collect();
    assert_eq!(first2, vec![0, 1]);
    assert!(
        pulled.get() <= 3,
        "full chain stays lazy under take(2), pulled {}",
        pulled.get()
    );
}
