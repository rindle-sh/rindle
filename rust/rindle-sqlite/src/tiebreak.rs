//! In-engine sort tiebreak for the SQLite source (`designs-implemented/204-IN-ENGINE-SORT-TIEBREAK-DESIGN.md`).
//!
//! The engine appends the primary key to every `order_by` so the sort is a *total*
//! order — a query ordered by `ord` is really ordered by `(ord, pk)`. On a rowid table
//! with a non-rowid PK (TEXT/UUID, or a composite PK) a user's index on `(ord)` is
//! physically `(ord, rowid)`, which cannot serve `(ord, pk)`; SQLite then builds a temp
//! B-tree over the whole result before yielding row 1.
//!
//! This adapter keeps the PK tiebreak a *logical* concern: SQL orders by the user
//! `prefix` (which an `(ord)` index *can* serve) and the appended-PK suffix is
//! re-applied in-engine by buffering each maximal run of rows that share `prefix` and
//! stable-sorting that run by the full `(prefix, pk)` sort. Memory is bounded by the
//! largest equal-prefix run, not the table.
//!
//! It sits *below* the overlay splice (it wraps the leaf cursor), so the stream reaching
//! `generate_with_overlay_checked` is fully `(prefix, pk)`-ordered and every downstream
//! consumer (the overlay splice, the constraint `take_while`, `generate_with_start`) is
//! unchanged. Because it owns the buffered rows below `LeafRows`, it also inherits
//! `LeafRows`' error duty: a `try_to_owned_row` failure is parked into the shared fetch
//! error sink (never silently swallowed).

use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;

use rindle::error::RindleError;
use rindle::source_common::compare_rows_rev;
use rindle::value::{compare_rows, OwnedRow as Row, RowRef, RowStream, Sort};

/// Wraps a leaf [`RowStream`] whose rows arrive ordered by a *prefix* of the full
/// sort, and vends them fully ordered by the whole `(prefix, pk)` sort. See the module
/// docs and design 204.
pub(crate) struct TiebreakStream<S: RowStream> {
    inner: S,
    /// The full `(prefix ++ appended-PK)` order.
    sort: Sort,
    /// The leading `prefix` columns SQLite ordered by (`sort[..prefix_len]`),
    /// materialized once for the run-boundary comparator.
    prefix: Sort,
    reverse: bool,
    /// The current equal-prefix run, sorted by the full `sort`.
    run: Vec<Row>,
    /// Cursor into `run`.
    pos: usize,
    /// First row of the *next* run, already pulled and owned (owned so it survives
    /// across `next_row` calls — the lending contract forbids holding a borrow).
    peeked: Option<Row>,
    /// Set once the inner stream is exhausted (or parked an error).
    done: bool,
    /// Shared with `TableSource::fetch_error`: a `try_to_owned_row` failure is parked
    /// here (the same channel `LeafRows` uses) and resurfaced after the drain.
    error_sink: Rc<RefCell<Option<RindleError>>>,
}

impl<S: RowStream> TiebreakStream<S> {
    /// `prefix_len` must be in `1..sort.len()` (the caller — `TableSource::fetch` via
    /// `TableMeta::tiebreak_prefix_len` — guarantees a non-empty prefix and a non-empty
    /// appended-PK suffix, so no run can span the whole table for a pure-PK order).
    pub(crate) fn new(
        inner: S,
        sort: Sort,
        prefix_len: usize,
        reverse: bool,
        error_sink: Rc<RefCell<Option<RindleError>>>,
    ) -> TiebreakStream<S> {
        debug_assert!(
            prefix_len >= 1 && prefix_len < sort.len(),
            "tiebreak prefix_len {prefix_len} out of range for sort len {}",
            sort.len()
        );
        let prefix = sort[..prefix_len].to_vec();
        TiebreakStream {
            inner,
            sort,
            prefix,
            reverse,
            run: Vec::new(),
            pos: 0,
            peeked: None,
            done: false,
            error_sink,
        }
    }

    /// Pull one row from the inner stream and own it. Parks a materialization error
    /// into the shared sink and reports end-of-stream (matching `LeafRows::next`).
    fn pull(&mut self) -> Option<Row> {
        if self.done {
            return None;
        }
        match self.inner.next_row() {
            None => {
                self.done = true;
                None
            }
            Some(r) => match r.try_to_owned_row() {
                Ok(row) => Some(row),
                Err(err) => {
                    *self.error_sink.borrow_mut() = Some(err);
                    self.done = true;
                    None
                }
            },
        }
    }

    /// Refill `run` with the next maximal equal-prefix run, sorted by the full sort.
    fn refill(&mut self) {
        self.run.clear();
        self.pos = 0;

        let Some(seed) = self.peeked.take().or_else(|| self.pull()) else {
            return; // stream exhausted (or errored — parked in the sink)
        };
        self.run.push(seed);

        while let Some(cand) = self.pull() {
            // Run boundary: prefix equality is direction-independent, so compare the
            // candidate's prefix against the run seed with the (unreversed) prefix
            // comparator.
            if compare_rows(&self.prefix, &self.run[0], &cand) == Ordering::Equal {
                self.run.push(cand);
            } else {
                self.peeked = Some(cand);
                break;
            }
        }

        // Finish the total order within the run. Stable, so rows equal under the full
        // `sort` — which, since `sort` includes the whole PK, are identical — keep their
        // arrival order.
        let sort = &self.sort;
        let reverse = self.reverse;
        self.run
            .sort_by(|a, b| compare_rows_rev(sort, reverse, a, b));
    }
}

impl<S: RowStream> RowStream for TiebreakStream<S> {
    type Row<'a>
        = &'a Row
    where
        Self: 'a;

    fn next_row(&mut self) -> Option<&Row> {
        if self.pos == self.run.len() {
            self.refill();
            if self.run.is_empty() {
                return None;
            }
        }
        let i = self.pos;
        self.pos += 1;
        Some(&self.run[i])
    }
}
