//! Shared harness support for the optimistic-writes slice tests
//! (`OPTIMISTIC-WRITES-DESIGN.md`): the §3 `Coalescer`, the `issue` table row
//! helpers, a deterministic PRNG, and a passthrough engine view for cross-checks.
//!
//! Lives in a subdirectory so it is NOT compiled as its own test binary; included
//! by each slice test via `mod optimistic_support;`.
#![allow(dead_code)]

use std::cmp::Ordering;

use rindle::btree::{BTree, RowRef, RowStream};
use rindle::graph::{Graph, NodeId};
use rindle::value::{compare_values, owned_row as row, OwnedRow, OwnedValue, Sort, SourceSchema};
use rindle::{build_pipeline, Ast, Dir, OrderPart, SourceChange};

use OwnedValue::Int;

// ---------------------------------------------------------------------------
// the `issue(id, owner, score)` table + row helpers
// ---------------------------------------------------------------------------

pub fn source_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "owner", "score"], vec![0], vec![(0, true)])
}
pub fn primary_sort() -> Sort {
    vec![(0, true)]
}
pub fn irow(id: i64, owner: i64, score: i64) -> OwnedRow {
    row(vec![Int(id), Int(owner), Int(score)])
}
pub fn key_of(r: &OwnedRow) -> i64 {
    match &r.col(0).to_owned() {
        Int(v) => *v,
        other => panic!("non-int key {other:?}"),
    }
}
pub fn col(r: &OwnedRow, i: usize) -> i64 {
    match &r.col(i).to_owned() {
        Int(v) => *v,
        other => panic!("non-int col {i} {other:?}"),
    }
}
pub fn as_ints(r: &OwnedRow) -> Vec<i64> {
    (0..r.len()).map(|i| col(r, i)).collect()
}
/// Full-width row equality (`null == null`) — the coalescer's "did it really
/// change" test, independent of `Arc` identity.
pub fn rows_equal(a: &OwnedRow, b: &OwnedRow) -> bool {
    a.len() == b.len()
        && a.cells()
            .zip(b.cells())
            .all(|(x, y)| compare_values(x, y) == Ordering::Equal)
}
pub fn apply_to_bt(t: &mut BTree, c: &SourceChange, sort: &Sort) {
    match c {
        SourceChange::Add(r) => {
            t.add(r.clone(), sort);
        }
        SourceChange::Remove(r) => {
            t.delete(r, sort);
        }
        SourceChange::Edit { old, row } => {
            t.delete(old, sort);
            t.add(row.clone(), sort);
        }
    }
}
pub fn drain(t: &BTree, sort: &Sort) -> Vec<OwnedRow> {
    let mut c = t.values_from(None, true, sort);
    let mut v = Vec::new();
    while let Some(r) = c.next_row() {
        v.push(r.to_owned_row());
    }
    v
}

/// Passthrough ordered-by-id view of `rows` through the REAL engine — the
/// cross-check that a coalesced delivery reconstructs the same rows the pipeline
/// materializes.
pub fn engine_view_rows(rows: Vec<OwnedRow>) -> Vec<Vec<i64>> {
    let mut g = Graph::new();
    let schema = source_schema();
    let src = g.add_source(schema.clone(), rows);
    let ast = Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        ..Default::default()
    };
    let resolve = |t: &str| (t == "issue").then(|| (src, schema.clone()));
    let top = build_pipeline(&mut g, &ast, &resolve).unwrap();
    let view: NodeId = g.add_view(top, schema.clone().into_schema());
    g.wire_single(top, view);
    g.hydrate(view);
    g.dump_view_rows(view).into_iter().map(|(r, _)| r).collect()
}

// ---------------------------------------------------------------------------
// deterministic PRNG
// ---------------------------------------------------------------------------

pub struct Lcg(pub u64);
impl Lcg {
    pub fn new(s: u64) -> Lcg {
        Lcg(s)
    }
    pub fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    pub fn range(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// ---------------------------------------------------------------------------
// the delivery boundary — RAW (the §3 coalescer was removed; see OPTIMISTIC notes)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum Delivered {
    Add(OwnedRow),
    Remove(OwnedRow),
    Edit { old: OwnedRow, new: OwnedRow },
}

/// Map one engine change to one delivered op, 1:1 — the RAW delivery the production
/// `Db::server_batch_end` now forwards (the §3 `Coalescer` was removed). No netting:
/// a consumer folds these into a pk-keyed view that settles to the same state
/// regardless of order, so `remove R` + `add R` is a no-op there.
pub fn delivered_of(c: &SourceChange) -> Delivered {
    match c {
        SourceChange::Add(r) => Delivered::Add(r.clone()),
        SourceChange::Remove(r) => Delivered::Remove(r.clone()),
        SourceChange::Edit { old, row } => Delivered::Edit {
            old: old.clone(),
            new: row.clone(),
        },
    }
}

/// Flatten a batch of engine changes to raw delivered ops, in order.
pub fn raw_deliver(changes: &[SourceChange]) -> Vec<Delivered> {
    changes.iter().map(delivered_of).collect()
}

/// `(op-tag, key)` per delivered change, for compact assertions.
pub fn delivered_keys(batch: &[Delivered]) -> Vec<(char, i64)> {
    batch
        .iter()
        .map(|d| match d {
            Delivered::Add(r) => ('A', key_of(r)),
            Delivered::Remove(r) => ('R', key_of(r)),
            Delivered::Edit { new, .. } => ('E', key_of(new)),
        })
        .collect()
}
