//! `Reduce` — incremental invertible aggregates (`REDUCE-DESIGN.md`): `count(*)`,
//! `sum(col)`, and `avg(col)`, each maintained from a fixed-size accumulator so a
//! `Remove` never re-reads the inputs (the [`StorageValue::Reduce`] state carries the
//! row `count` plus one `(int_sum, float_sum, non_null, float_count)`
//! [`ReduceAcc`](crate::storage::ReduceAcc) per `Sum`/`Avg`). Two regimes:
//!
//! - **Global** (`partition_key = None`): collapses the whole input into **one**
//!   synthetic `[<agg>]` row, born once at hydrate and then **immortal** (`count` of
//!   empty input is `0`, `sum`/`avg` are `NULL`; the row never dies → steady state is a
//!   pure `Edit` stream).
//! - **Grouped** (`partition_key = Some`, the top-level `GROUP BY` case): one
//!   `[group_key…, <agg>]` row **per group**, keyed in storage per group. A group is
//!   **born** (`Add`) on its first row and **dies** (`Remove`) when its row count hits
//!   `0`. Unlike `count`, an in-place `Edit` within a group can move a `sum`/`avg`, so
//!   the operator emits whenever the aggregate row actually changes (§6).
//!
//! This is the first operator that *originates* `Edit`s mid-graph and the first that
//! **reshapes** its row (output is synthetic, not an input row), so — unlike
//! [`Skip`](crate::op::Skip)/[`Take`](crate::op::Take), which forward input nodes
//! unchanged — it carries its own output [`Schema`] and reads/writes a
//! [`StorageValue::Reduce`] accumulator. Like `Take` it is **stateful** (state in the
//! [`Graph`] storage arena, read through a shared `&Graph` borrow, so the reentrant
//! first-sight fold composes for free) and, in the grouped case, relies on the input
//! connection **split-editing the partition key** so an `Edit` never crosses groups.
//!
//! ## Hydration regime (grouped)
//!
//! This file implements the **eager / top-level `GROUP BY`** regime: the consumer
//! (`View`) hydrates with one unconstrained `fetch`, which folds the entire input and
//! materializes *every* group, so after hydrate a `push` `Add` to a group with no
//! state is a genuine **birth**. The **lazy** regime — a relationship aggregate whose
//! groups are fetched one at a time (constrained) by a parent join — needs the
//! opposite "drop-and-fold-on-fetch" rule (and persisting count-0 groups for the 0→1
//! transition); it lands with the join-attachment milestone (`REDUCE-DESIGN.md` §8/§9)
//! and is **not** handled here. Until then `fetch` ignores the request constraint and
//! always full-folds.

use std::cell::Cell;
use std::collections::HashMap;

use crate::change::{Change, Constraint, FetchRequest, Node, NodeStream, OutEdge};
use crate::graph::{Graph, NodeId, StorageId};
use crate::op::partition::{
    constraint_matches_partition_key, encode_state_key, partition_key_unchanged,
};
use crate::storage::{ReduceAcc, StorageValue};
use crate::value::{
    compare_rows, owned_row, ColId, OwnedRow as Row, OwnedValue, Schema, Value, ValueType,
};

/// Which aggregate a [`Reduce`] column computes, in output-column order
/// (`REDUCE-DESIGN.md` §5). All three are **invertible** — a `Remove` folds into a
/// fixed-size accumulator without re-reading the inputs — which is what lets `Reduce`
/// maintain them from a pure delta stream.
#[derive(Clone, Debug)]
pub enum AggSpec {
    /// `count(*)` — counts input rows (`NULL`s included). `Add` `+1`, `Remove` `-1`.
    Count,
    /// `sum(col)` — Σ of the column's non-`NULL` values. `Add` `+v`, `Remove` `-v`.
    Sum(ColId),
    /// `avg(col)` — `sum(col) / count(col)` over non-`NULL` values (SQL semantics:
    /// the denominator is the non-`NULL` count, not `count(*)`); `NULL` on empty.
    Avg(ColId),
}

impl AggSpec {
    /// The synthetic output column name this aggregate contributes (`count`/`sum`/`avg`).
    /// The builder's view schema mirrors these so the production `View` differ and the
    /// scalar projection resolve the same column.
    fn col_name(&self) -> &'static str {
        match self {
            AggSpec::Count => "count",
            AggSpec::Sum(_) => "sum",
            AggSpec::Avg(_) => "avg",
        }
    }

    /// The declared [`ValueType`] of this aggregate's output column (design 226
    /// §4.1): `count` and `avg` are `Number`; `sum` follows its input column —
    /// `Int` over a declared `int64` input (the exact-i64 sum plane), `Number`
    /// otherwise. Shared by [`Reduce::with_input_types`] and the builder's
    /// view-schema twins so the two derivations cannot drift.
    pub fn output_type(&self, input: &Schema) -> ValueType {
        match self {
            AggSpec::Count | AggSpec::Avg(_) => ValueType::Number,
            AggSpec::Sum(col) => match input.column_types.get(*col) {
                Some(ValueType::Int) => ValueType::Int,
                _ => ValueType::Number,
            },
        }
    }
}

/// Fold one row's cell into an accumulator with `sign` `+1` (an `Add`) or `-1`
/// (a `Remove`). `NULL`/`Absent` contribute nothing; a non-numeric non-`NULL` value
/// counts toward `non_null` with a `0` sum contribution (matching SQLite's coerce-to-0).
fn acc_delta(acc: &mut ReduceAcc, v: Value, sign: i64) {
    match v {
        Value::Int(i) => {
            // i128 accumulation (design 226 §5.3): a delta-order transient past
            // i64::MAX must neither wrap nor error, and `-1 × i64::MIN` must not
            // overflow on a Remove. The i128 cannot overflow at any real
            // cardinality; only the EMITTED total is bounded (`sum_value`).
            acc.int_sum += i128::from(sign) * i128::from(i);
            acc.non_null += sign;
        }
        Value::Float(f) => {
            acc.float_sum += sign as f64 * f;
            acc.float_count += sign;
            acc.non_null += sign;
        }
        Value::Null | Value::Absent => {}
        Value::Bool(_) | Value::Str(_) | Value::Json(_) => acc.non_null += sign,
    }
}

/// The emitted `sum` cell: `NULL` for an empty (all-`NULL`) input, otherwise `Int` while
/// every contributing value was an integer and `Float` once any float contributed.
///
/// `Err(total)` when the **set total** of an all-integer sum falls outside i64
/// (design 226 §5.3) — SQLite's *result* contract, not its scan mechanics: the
/// i128 accumulator has already absorbed any delta-order transient; only what
/// would be emitted is bounded. [`Reduce::agg_row`] renders the error case as
/// `NULL`; the typed error itself is raised at the `try_*` boundary, where
/// `Graph::take_runtime_error` polls [`Reduce::overflow_count`].
fn sum_value(a: &ReduceAcc) -> Result<OwnedValue, i128> {
    if a.non_null == 0 {
        Ok(OwnedValue::Null)
    } else if a.float_count == 0 {
        match i64::try_from(a.int_sum) {
            Ok(total) => Ok(OwnedValue::Int(total)),
            Err(_) => Err(a.int_sum),
        }
    } else {
        Ok(OwnedValue::Float(a.int_sum as f64 + a.float_sum))
    }
}

/// The emitted `avg` cell: SQL `avg` is always real (or `NULL` when there are no
/// non-`NULL` values).
fn avg_value(a: &ReduceAcc) -> OwnedValue {
    if a.non_null == 0 {
        OwnedValue::Null
    } else {
        OwnedValue::Float((a.int_sum as f64 + a.float_sum) / a.non_null as f64)
    }
}

/// Whole-row equality for a synthetic aggregate row, treating `NULL == NULL` and — unlike
/// [`compare_values`](crate::value::compare_values) — an `Int`/`Float` type change as a
/// *change* (so a `sum` that flips storage class re-emits). Drives the "emit only if the
/// row actually changed" rule (`REDUCE-DESIGN.md` §6).
fn agg_rows_equal(a: &Row, b: &Row) -> bool {
    a.len() == b.len() && (0..a.len()).all(|i| agg_val_eq(a.col(i), b.col(i)))
}

fn agg_val_eq(a: Value, b: Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) | (Value::Absent, Value::Absent) => true,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => x == y,
        (Value::Str(x), Value::Str(y)) => x == y,
        (Value::Json(x), Value::Json(y)) => x == y,
        _ => false,
    }
}

/// `Reduce`: an invertible aggregate, global or grouped. The running accumulator(s)
/// live in [`Graph`] storage under [`Reduce::storage`], not in the struct, so they
/// survive across `fetch`/`push`.
pub struct Reduce {
    /// Upstream input operator (the source connection, or a sub-pipeline tail).
    pub input: NodeId,
    /// Scratch-state slot holding the per-group [`StorageValue::Reduce`] counters.
    pub storage: StorageId,
    /// The aggregates this node computes, in output-column order. v1: `[Count]`.
    pub aggs: Vec<AggSpec>,
    /// The grouping columns, **in input-row coordinates**. `None` = global (a single
    /// immortal row); `Some` = one row per distinct value-tuple (top-level `GROUP BY`).
    /// In the output row the group columns occupy positions `0..partition_key.len()`.
    pub partition_key: Option<Vec<ColId>>,
    /// Hydration regime for the grouped case (`REDUCE-DESIGN.md` §8.1; moot when
    /// `partition_key` is `None`). `true` = **eager** (top-level `GROUP BY` → `View`,
    /// full-fold hydrate; an `Add` to a no-state group births it, death deletes the
    /// slot). `false` = **lazy** (relationship aggregate → per-group constrained
    /// `fetch`; an `Add` to a no-state group is dropped and folded on the next fetch,
    /// death keeps the count-0 slot so the next `Add` re-births).
    pub eager: bool,
    /// The **synthetic** output schema (`REDUCE-DESIGN.md` §3): `[group_cols…,
    /// agg_cols…]`. Global ⇒ just `[count]` with empty PK/sort (singleton-only, §8);
    /// grouped ⇒ the group columns are the PK and sort, then the aggregate columns.
    pub schema: Schema,
    /// The single downstream edge, wired two-phase like every fan-out-seam operator
    /// (via [`Graph::set_output`](crate::graph::Graph::set_output)).
    pub output: Cell<Option<OutEdge>>,
    /// How many **stored** groups currently hold an all-integer `sum` whose set
    /// total is outside i64 (design 226 §5.3). Maintained by `write_at`/`del_at`
    /// against the stored state, so it is **state-based, not event-based**: a
    /// delta-order transient (an Add past `i64::MAX` compensated by a Remove
    /// before the boundary is consulted) nets back to `0` and does not error,
    /// while a genuinely-unrepresentable stored total raises the typed error at
    /// every `try_*` boundary until the state shrinks back into range —
    /// SQLite's *result* contract (a fresh `SELECT sum(…)` there errors every
    /// time too). Read by `Graph::take_runtime_error`.
    pub(crate) overflowed: Cell<i64>,
}

impl Reduce {
    /// A **global** single-aggregate reducer over `input` (`spec`). Output is a
    /// one-column `[<agg>]` row (no key, no sort — a singleton, §8).
    pub fn global_agg(input: NodeId, storage: StorageId, spec: AggSpec) -> Reduce {
        let cols = vec![spec.col_name()];
        Reduce {
            input,
            storage,
            aggs: vec![spec],
            partition_key: None,
            eager: true,
            schema: Schema::new(cols, Vec::new(), Vec::new()),
            output: Cell::new(None),
            overflowed: Cell::new(0),
        }
    }

    /// A **grouped** single-aggregate reducer: one `[group_key…, <agg>]` row per distinct
    /// value of `partition_key` (input-row column indices). `key_cols` are the output
    /// names of the group columns (same arity/order as `partition_key`). The output
    /// schema's PK and sort are the group columns (`0..k`, ascending), then the aggregate.
    pub fn grouped_agg(
        input: NodeId,
        storage: StorageId,
        partition_key: Vec<ColId>,
        key_cols: Vec<&str>,
        spec: AggSpec,
    ) -> Reduce {
        assert_eq!(
            partition_key.len(),
            key_cols.len(),
            "reduce: partition_key arity must match key_cols arity"
        );
        let k = partition_key.len();
        let mut cols = key_cols;
        cols.push(spec.col_name());
        let key_ids: Vec<ColId> = (0..k).collect();
        let sort = (0..k).map(|i| (i, true)).collect();
        Reduce {
            input,
            storage,
            aggs: vec![spec],
            partition_key: Some(partition_key),
            eager: true,
            schema: Schema::new(cols, key_ids, sort),
            output: Cell::new(None),
            overflowed: Cell::new(0),
        }
    }

    /// A **global** `count(*)` reducer over `input` — [`global_agg`](Self::global_agg)
    /// with [`AggSpec::Count`].
    pub fn count(input: NodeId, storage: StorageId) -> Reduce {
        Self::global_agg(input, storage, AggSpec::Count)
    }

    /// A **global** `sum(col)` reducer (input-row column index).
    pub fn sum(input: NodeId, storage: StorageId, col: ColId) -> Reduce {
        Self::global_agg(input, storage, AggSpec::Sum(col))
    }

    /// A **global** `avg(col)` reducer (input-row column index).
    pub fn avg(input: NodeId, storage: StorageId, col: ColId) -> Reduce {
        Self::global_agg(input, storage, AggSpec::Avg(col))
    }

    /// A **grouped** `count(*)` reducer — [`grouped_agg`](Self::grouped_agg) with
    /// [`AggSpec::Count`].
    pub fn count_by(
        input: NodeId,
        storage: StorageId,
        partition_key: Vec<ColId>,
        key_cols: Vec<&str>,
    ) -> Reduce {
        Self::grouped_agg(input, storage, partition_key, key_cols, AggSpec::Count)
    }

    /// A **grouped** `sum(col)` reducer (`col` in input-row coordinates).
    pub fn sum_by(
        input: NodeId,
        storage: StorageId,
        partition_key: Vec<ColId>,
        key_cols: Vec<&str>,
        col: ColId,
    ) -> Reduce {
        Self::grouped_agg(input, storage, partition_key, key_cols, AggSpec::Sum(col))
    }

    /// A **grouped** `avg(col)` reducer (`col` in input-row coordinates).
    pub fn avg_by(
        input: NodeId,
        storage: StorageId,
        partition_key: Vec<ColId>,
        key_cols: Vec<&str>,
        col: ColId,
    ) -> Reduce {
        Self::grouped_agg(input, storage, partition_key, key_cols, AggSpec::Avg(col))
    }

    /// Switch a grouped reducer to the **lazy** regime (`REDUCE-DESIGN.md` §8.1) — for
    /// a relationship aggregate whose groups a parent join fetches one at a time. A
    /// constrained `fetch` folds just that group (persisting even a count-0 group); a
    /// `push` to a group with no state is dropped (the next fetch folds it), and a
    /// group that drains to `0` keeps its slot so the next `Add` re-births it.
    pub fn lazy(mut self) -> Reduce {
        debug_assert!(
            self.partition_key.is_some(),
            "Reduce::lazy is only meaningful for a grouped (partitioned) reducer"
        );
        self.eager = false;
        self
    }

    /// Derive the synthetic output schema's `column_types` from the **input**
    /// schema (design 226 §4.1): each group column preserves its input column's
    /// declared type (`partition_key[i]` in input coordinates → output position
    /// `i`), and each aggregate column carries [`AggSpec::output_type`]. The
    /// builder chains this on every reduce it constructs; the constructors alone
    /// leave [`Schema::new`]'s all-`Number` default, which direct-op tests keep.
    pub fn with_input_types(mut self, input: &Schema) -> Reduce {
        let mut types: Vec<ValueType> = Vec::with_capacity(self.schema.columns.len());
        if let Some(pk) = &self.partition_key {
            types.extend(pk.iter().map(|&c| {
                input
                    .column_types
                    .get(c)
                    .copied()
                    .unwrap_or(ValueType::Number)
            }));
        }
        types.extend(self.aggs.iter().map(|a| a.output_type(input)));
        debug_assert_eq!(types.len(), self.schema.columns.len());
        self.schema.column_types = types;
        self
    }

    /// The group's value-tuple for a row (input coordinates). Global ⇒ `[]`.
    fn group_vals(&self, row: &Row) -> Vec<OwnedValue> {
        match &self.partition_key {
            None => Vec::new(),
            Some(pk) => pk.iter().map(|&c| row.col(c).to_owned()).collect(),
        }
    }

    /// The group's value-tuple from a fetch **constraint** (which is in *output*
    /// coordinates — the group columns are output positions `0..k`), in key order.
    fn group_vals_from_constraint(&self, c: &Constraint, k: usize) -> Vec<OwnedValue> {
        (0..k)
            .map(|i| {
                c.iter()
                    .find(|(col, _)| *col == i)
                    .map(|(_, v)| v.clone())
                    .unwrap_or(OwnedValue::Null)
            })
            .collect()
    }

    /// Translate a group's output-coordinate value-tuple into an *input* constraint
    /// (`partition_key[i] → group_vals[i]`) to fold just that group's source rows.
    fn input_constraint(&self, group_vals: &[OwnedValue]) -> Constraint {
        let pk = self
            .partition_key
            .as_ref()
            .expect("input_constraint on a global reducer");
        pk.iter()
            .zip(group_vals)
            .map(|(&col, v)| (col, v.clone()))
            .collect()
    }

    /// The storage key for a group value-tuple. Global's empty tuple encodes to the
    /// single fixed key, so the global and grouped paths share one keyspace.
    fn group_key(vals: &[OwnedValue]) -> String {
        encode_state_key("reduce", vals)
    }

    /// One zeroed accumulator per `Sum`/`Avg` aggregate (a plain `count` yields `[]`).
    fn new_accs(&self) -> Vec<ReduceAcc> {
        self.aggs
            .iter()
            .filter(|a| matches!(a, AggSpec::Sum(_) | AggSpec::Avg(_)))
            .map(|_| ReduceAcc::default())
            .collect()
    }

    /// Fold `row`'s summed cells into the per-aggregate accumulators with `sign` (`+1`
    /// for an `Add`, `-1` for a `Remove`). `accs` is parallel to the `Sum`/`Avg` specs.
    fn acc_apply(&self, accs: &mut [ReduceAcc], row: &Row, sign: i64) {
        let mut ai = 0;
        for spec in &self.aggs {
            match spec {
                AggSpec::Count => {}
                AggSpec::Sum(c) | AggSpec::Avg(c) => {
                    acc_delta(&mut accs[ai], row.col(*c), sign);
                    ai += 1;
                }
            }
        }
    }

    /// Read a group's stored `(count, accs)`, or `None` if it has no state yet.
    fn read_at(&self, g: &Graph, key: &str) -> Option<(i64, Vec<ReduceAcc>)> {
        match g.storage(self.storage).get(key) {
            None => None,
            Some(StorageValue::Reduce { count, accs }) => Some((count, accs)),
            Some(other) => unreachable!("reduce state slot held {other:?}"),
        }
    }

    /// Whether any **`Sum`-spec** accumulator's current all-integer set total is
    /// outside i64 (design 226 §5.3). `Avg` accs never emit an integer, so an
    /// out-of-range `int_sum` under an `Avg` is fine — it emits as f64.
    fn sum_total_out_of_range(&self, accs: &[ReduceAcc]) -> bool {
        let mut ai = 0;
        for spec in &self.aggs {
            match spec {
                AggSpec::Count => {}
                AggSpec::Sum(_) => {
                    let a = &accs[ai];
                    if a.non_null > 0 && a.float_count == 0 && i64::try_from(a.int_sum).is_err() {
                        return true;
                    }
                    ai += 1;
                }
                AggSpec::Avg(_) => ai += 1,
            }
        }
        false
    }

    /// How many stored groups currently hold an unrepresentable `sum` total —
    /// non-zero makes [`Graph::take_runtime_error`] raise the §5.3 typed error.
    pub(crate) fn overflow_count(&self) -> i64 {
        self.overflowed.get()
    }

    /// Adjust the [`Reduce::overflowed`] counter for a stored-state transition
    /// `old → new` of one group. `None` = the group is absent (birth/death).
    fn track_overflow(&self, old: Option<&[ReduceAcc]>, new: Option<&[ReduceAcc]>) {
        let was = old.is_some_and(|a| self.sum_total_out_of_range(a));
        let is = new.is_some_and(|a| self.sum_total_out_of_range(a));
        match (was, is) {
            (false, true) => self.overflowed.set(self.overflowed.get() + 1),
            (true, false) => self.overflowed.set(self.overflowed.get() - 1),
            _ => {}
        }
    }

    fn write_at(&self, g: &Graph, key: &str, count: i64, accs: Vec<ReduceAcc>) {
        let old = self.read_at(g, key);
        self.track_overflow(old.as_ref().map(|(_, a)| a.as_slice()), Some(&accs));
        g.storage(self.storage)
            .set(key, StorageValue::Reduce { count, accs });
    }

    fn del_at(&self, g: &Graph, key: &str) {
        let old = self.read_at(g, key);
        self.track_overflow(old.as_ref().map(|(_, a)| a.as_slice()), None);
        g.storage(self.storage).del(key);
    }

    /// The synthetic aggregate row `[group_vals…, agg_0, …]` from the `(count, accs)`
    /// accumulator. Global passes `&[]` ⇒ just the aggregate columns. `accs` is parallel
    /// to the `Sum`/`Avg` specs (a `Count` consumes none).
    ///
    /// A `sum` whose set total leaves i64 (design 226 §5.3) substitutes `NULL` in
    /// the cell; the error surfaces at the operation's `try_*` boundary
    /// (`try_source_push` / `try_fetch_all` / `try_hydrate`), where
    /// [`Graph::take_runtime_error`] polls [`Reduce::overflow_count`] — state-based,
    /// so a transient corrected before the boundary never errors. The `NULL` is
    /// deliberately invalid (SQL `sum` over a non-empty set is never `NULL`), so a
    /// swallowed error cannot masquerade as a plausible total.
    fn agg_row(&self, group_vals: &[OwnedValue], count: i64, accs: &[ReduceAcc]) -> Row {
        let mut v = group_vals.to_vec();
        let mut ai = 0;
        for spec in &self.aggs {
            v.push(match spec {
                AggSpec::Count => OwnedValue::Int(count),
                AggSpec::Sum(_) => {
                    let out = sum_value(&accs[ai]).unwrap_or(OwnedValue::Null);
                    ai += 1;
                    out
                }
                AggSpec::Avg(_) => {
                    let out = avg_value(&accs[ai]);
                    ai += 1;
                    out
                }
            });
        }
        owned_row(v)
    }

    /// Fold `input` rows into an accumulator, optionally constrained to one group's rows.
    /// Reads only each row's own cells (relationship thunks stay uncalled), so folding a
    /// `count`/`sum`/`avg` never materializes children.
    fn fold(&self, g: &Graph, req: &FetchRequest) -> (i64, Vec<ReduceAcc>) {
        let mut count = 0i64;
        let mut accs = self.new_accs();
        for node in g.fetch(self.input, req) {
            count += 1;
            self.acc_apply(&mut accs, &node.row, 1);
        }
        (count, accs)
    }

    /// Lazy pull. The request is ignored for the eager regimes (see the module note):
    /// global serves/folds the single accumulator; grouped folds the whole input into
    /// per-group accumulators, persists each, and emits one row per group in output-sort
    /// order. On first sight the fold seeds state; thereafter `push` maintains it.
    pub fn fetch<'g>(&'g self, g: &'g Graph, req: &FetchRequest) -> NodeStream<'g> {
        match &self.partition_key {
            None => {
                let _ = req; // a global aggregate is one row regardless of constraint
                let (count, accs) = match self.read_at(g, GLOBAL_KEY) {
                    Some(s) => s,
                    None => {
                        let s = self.fold(g, &FetchRequest::all());
                        self.write_at(g, GLOBAL_KEY, s.0, s.1.clone());
                        s
                    }
                };
                Box::new(std::iter::once(Node::leaf(self.agg_row(&[], count, &accs))))
            }
            Some(pk) => {
                let k = pk.len();
                // Lazy + a partition-key constraint ⇒ serve/fold just that one group.
                let single = (!self.eager)
                    .then_some(req.constraint.as_ref())
                    .flatten()
                    .filter(|c| constraint_matches_partition_key(c, &(0..k).collect::<Vec<_>>()));
                if let Some(c) = single {
                    return self.fetch_one_group(g, self.group_vals_from_constraint(c, k));
                }
                // Eager (always) or lazy-unconstrained: full-fold every group.
                let mut groups: HashMap<String, (Vec<OwnedValue>, i64, Vec<ReduceAcc>)> =
                    HashMap::new();
                for node in g.fetch(self.input, &FetchRequest::all()) {
                    let vals = self.group_vals(&node.row);
                    let entry = groups
                        .entry(Self::group_key(&vals))
                        .or_insert_with(|| (vals, 0, self.new_accs()));
                    entry.1 += 1;
                    self.acc_apply(&mut entry.2, &node.row, 1);
                }
                let mut rows: Vec<Row> = Vec::with_capacity(groups.len());
                for (key, (vals, count, accs)) in &groups {
                    self.write_at(g, key, *count, accs.clone());
                    rows.push(self.agg_row(vals, *count, accs));
                }
                rows.sort_by(|a, b| compare_rows(&self.schema.sort, a, b));
                Box::new(rows.into_iter().map(Node::leaf))
            }
        }
    }

    /// Lazy single-group fetch: serve the group's accumulator from state, or — on first
    /// sight — fold *only that group's* source rows (constraint pushed into the input),
    /// **persist even a count-0 group** (so a later `Add` finds state and fires the
    /// `0 → 1` birth, §8.1), and emit one row iff the group is non-empty.
    fn fetch_one_group<'g>(&'g self, g: &'g Graph, group_vals: Vec<OwnedValue>) -> NodeStream<'g> {
        let key = Self::group_key(&group_vals);
        let (count, accs) = match self.read_at(g, &key) {
            Some(s) => s,
            None => {
                let s = self.fold(
                    g,
                    &FetchRequest::with_constraint(self.input_constraint(&group_vals)),
                );
                self.write_at(g, &key, s.0, s.1.clone());
                s
            }
        };
        if count > 0 {
            Box::new(std::iter::once(Node::leaf(self.agg_row(
                &group_vals,
                count,
                &accs,
            ))))
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Eager push. Global maintains the one immortal accumulator (always an `Edit`);
    /// grouped maintains per-group accumulators with birth (`Add`), shift (`Edit`), and
    /// death (`Remove` at row `count → 0`). A same-group `Edit` leaves `count(*)`
    /// unchanged but can still move a `sum`/`avg`, so it re-folds the accumulator and
    /// emits iff the aggregate row actually changed (`REDUCE-DESIGN.md` §6); a `Child`
    /// never changes the input-row population, so it emits nothing.
    pub fn push<'g>(&'g self, g: &'g Graph, change: Change<'g>) {
        let out = self.output.get().expect("Reduce output not wired");
        match &self.partition_key {
            None => self.push_global(g, out, change),
            Some(pk) => match change {
                Change::Add(node) => self.grouped_delta(g, out, &node.row, 1),
                Change::Remove(node) => self.grouped_delta(g, out, &node.row, -1),
                Change::Edit { node, old } => {
                    debug_assert!(
                        partition_key_unchanged(&old.row, &node.row, pk),
                        "reduce: Edit crossed a partition; the input connection must \
                         split-edit the partition key (REDUCE-DESIGN.md §6)"
                    );
                    self.grouped_edit(g, out, &old.row, &node.row);
                }
                Change::Child { .. } => {}
            },
        }
    }

    /// Global push: the single immortal accumulator. Bails if not hydrated (mirrors
    /// `Take`'s unhydrated-partition guard); a dropped push still lands in the source,
    /// so the next hydrate fold folds it. Emits an `Edit` only if the aggregate row
    /// changed — a `count` always moves, but a `sum`/`avg` of a row that is `NULL` in the
    /// summed column does not.
    fn push_global<'g>(&'g self, g: &'g Graph, out: OutEdge, change: Change<'g>) {
        let Some((count, accs)) = self.read_at(g, GLOBAL_KEY) else {
            return;
        };
        if matches!(change, Change::Child { .. }) {
            return;
        }
        let old_row = self.agg_row(&[], count, &accs);
        let mut new_accs = accs;
        let new_count = match &change {
            Change::Add(node) => {
                self.acc_apply(&mut new_accs, &node.row, 1);
                count + 1
            }
            Change::Remove(node) => {
                self.acc_apply(&mut new_accs, &node.row, -1);
                count - 1
            }
            Change::Edit { node, old } => {
                self.acc_apply(&mut new_accs, &old.row, -1);
                self.acc_apply(&mut new_accs, &node.row, 1);
                count
            }
            Change::Child { .. } => unreachable!("Child returned above"),
        };
        let new_row = self.agg_row(&[], new_count, &new_accs);
        self.write_at(g, GLOBAL_KEY, new_count, new_accs);
        if !agg_rows_equal(&old_row, &new_row) {
            g.push(
                out.node,
                Change::Edit {
                    node: Node::leaf(new_row),
                    old: Node::leaf(old_row),
                },
                out.port,
            );
        }
    }

    /// Apply an `Add`/`Remove` (`delta` `±1`) of `row` to its group, emitting by the
    /// `(old_count > 0, new_count > 0)` transition: birth (`Add`), shift (`Edit`), death
    /// (`Remove`), or nothing. The two regime-dependent points (`REDUCE-DESIGN.md` §8.1):
    ///
    /// - **No state.** Eager treats it as a genuinely-new group (count `0`): a `+1`
    ///   births it (seeding the accumulator from `row`); a `-1` is impossible and is
    ///   dropped. Lazy drops *both* — the group is merely un-folded, and the next
    ///   constrained `fetch` will fold it from source.
    /// - **Death** (`new_count == 0`). Eager **deletes** the slot; Lazy **keeps** it (at
    ///   count `0`, accumulator drained to zero) so a future `Add` re-births (`0 → 1`).
    fn grouped_delta<'g>(&'g self, g: &'g Graph, out: OutEdge, row: &Row, delta: i64) {
        let group_vals = self.group_vals(row);
        let key = Self::group_key(&group_vals);
        let (old_count, old_accs) = match self.read_at(g, &key) {
            Some(s) => s,
            None => {
                if self.eager && delta > 0 {
                    let mut accs = self.new_accs();
                    self.acc_apply(&mut accs, row, 1);
                    let born = self.agg_row(&group_vals, delta, &accs);
                    self.write_at(g, &key, delta, accs);
                    g.push(out.node, Change::Add(Node::leaf(born)), out.port);
                }
                return; // lazy no-state, or eager `-1` to absent: nothing to do.
            }
        };
        let new_count = (old_count + delta).max(0);
        let mut new_accs = old_accs.clone();
        self.acc_apply(&mut new_accs, row, delta);
        let change = match (old_count > 0, new_count > 0) {
            (false, true) => {
                Change::Add(Node::leaf(self.agg_row(&group_vals, new_count, &new_accs)))
            }
            (true, true) => {
                let old_row = self.agg_row(&group_vals, old_count, &old_accs);
                let new_row = self.agg_row(&group_vals, new_count, &new_accs);
                if agg_rows_equal(&old_row, &new_row) {
                    // Row count moved but the emitted aggregate did not (e.g. `sum` of a
                    // row that is `NULL` in the summed column) — persist, emit nothing.
                    self.write_at(g, &key, new_count, new_accs);
                    return;
                }
                Change::Edit {
                    node: Node::leaf(new_row),
                    old: Node::leaf(old_row),
                }
            }
            (true, false) => {
                Change::Remove(Node::leaf(self.agg_row(&group_vals, old_count, &old_accs)))
            }
            (false, false) => {
                // Stays empty (lazy slot kept at 0); nothing emitted.
                self.write_at(g, &key, new_count, new_accs);
                return;
            }
        };
        if matches!(change, Change::Remove(_)) && self.eager {
            self.del_at(g, &key);
        } else {
            self.write_at(g, &key, new_count, new_accs);
        }
        g.push(out.node, change, out.port);
    }

    /// Apply a same-group in-place `Edit` (`old` → `new`, same partition key). The row
    /// count is unchanged, so this never births or kills a group; it re-folds the
    /// accumulator (`Remove(old) + Add(new)`) and emits an `Edit` iff the aggregate row
    /// moved — a `count` never does (nothing emitted), a `sum`/`avg` does when the summed
    /// cell changes. A group with no state (lazy, un-folded) drops the edit; the next
    /// constrained `fetch` folds the updated row.
    fn grouped_edit<'g>(&'g self, g: &'g Graph, out: OutEdge, old_in: &Row, new_in: &Row) {
        let group_vals = self.group_vals(new_in);
        let key = Self::group_key(&group_vals);
        let Some((count, old_accs)) = self.read_at(g, &key) else {
            return;
        };
        let mut new_accs = old_accs.clone();
        self.acc_apply(&mut new_accs, old_in, -1);
        self.acc_apply(&mut new_accs, new_in, 1);
        let old_row = self.agg_row(&group_vals, count, &old_accs);
        let new_row = self.agg_row(&group_vals, count, &new_accs);
        self.write_at(g, &key, count, new_accs);
        if !agg_rows_equal(&old_row, &new_row) {
            g.push(
                out.node,
                Change::Edit {
                    node: Node::leaf(new_row),
                    old: Node::leaf(old_row),
                },
                out.port,
            );
        }
    }
}

/// The single fixed state key for the global accumulator — the encoding of the empty
/// group tuple (`encode_state_key("reduce", &[])`), so the global and grouped paths
/// share one keyspace.
const GLOBAL_KEY: &str = "reduce";
