//! Spec `11` — the shared **operator test harness** (the "testkit").
//!
//! This is the substrate every operator agent (specs `06`/`07`) asserts against:
//! instead of each operator inventing ad-hoc `Graph` wiring + a bespoke sink, it
//! sinks its pipeline into `Catch` (the output oracle), optionally taps the
//! message stream with `Snitch`, and drives the whole thing through
//! `run_push_test`/`run_fetch_test`.
//!
//! ## What lives here (spec `11` §3 — built)
//!
//! - `Catch` (§3.2) — the output oracle. A sink [`Operator`](crate::graph) that
//!   records the emitted `Change` stream as a fully-materialized
//!   `CaughtChange`/`CaughtNode` tree and can re-`fetch` the whole pipeline
//!   into the same comparable tree. Port of `zql/src/ivm/catch.ts`. It is the
//!   **generalization of the spike's `graph::Collector`**: where `Collector`
//!   records a *flat* `Add/Remove/Edit` and `return`s on `Child`, `Catch`
//!   eagerly drains every relationship thunk into a nested tree and records the
//!   `Child` arm. (`Collector` is left in place — it is the minimal chassis sink
//!   the 24-test filter-chain proof asserts against; `Catch` is what new operator
//!   suites use.)
//! - `Snitch` (§3.3) — the message-log oracle. A transparent pass-through
//!   operator that records every `fetch`/`push`/`fetchCount` it sees. Port of
//!   `zql/src/ivm/snitch.ts`. `fetchCount` is recorded on stream **`Drop`** (the
//!   RAII analogue of the JS `finally`, `snitch.ts:81-83`).
//! - `run_push_test`/`run_fetch_test` (§3.1) — the fixture-driven runners.
//! - `fingerprint` (§3.5) — the cheap cross-build equality probe.
//! - The **diff predicate** (§3.8): `CaughtChange`/`CaughtNode` implement a
//!   structural `PartialEq` (full-cell content equality via
//!   [`compare_values`](crate::value::compare_values); relationship child order
//!   preserved as the operator's sort order, never re-sorted).
//! - Constructor helpers (`add`/`remove`/`edit`/`child`/`cnode`/
//!   `cleaf`/`rel`) so an inline-snapshot expectation reads like the JS
//!   snapshot but is structurally typed.
//!
//! ## Deviations from the original spec'd test strategy (deliberate; the code
//! is the source of truth)
//!
//! 1. **Two runner flavours: build-closure AND `Ast`.** The build-closure runners
//!    `run_push_test`/`run_fetch_test` take a
//!    `build: FnOnce(&mut Graph, &[NodeId]) -> NodeId` that wires the pipeline by
//!    hand (as today's `tests/*.rs` do) — the only option for operators
//!    [`build_pipeline`](crate::builder::build_pipeline) cannot yet lower
//!    (`Skip`/`Take`/`Exists`/the OR fan). Now that the builder has landed
//!    (builder-port steps 0–4), the **`Ast`-driven** runners `run_push_test_ast`/
//!    `run_fetch_test_ast` lower a JS-shaped `Ast` through `build_pipeline`
//!    instead — the in-memory half of §3.1's `PushTest{ ast, .. }` model, so the JS
//!    suites become a Rust corpus for the **built subset** (a source's pushed-down
//!    `where`/`order_by` + sibling/nested relationship joins). Still deferred: the
//!    production `ArrayView`/`ViewData` (the sink stays `Catch`, not a
//!    `View`), the serde JSON boundary that *parses* a JS fixture into these types
//!    (§3.6), and the live-JS fuzzer (§3.7). (`query_builder` is **not** the
//!    builder: it lowers a single connection's `FetchRequest` — pushed-down filters
//!    + constraint/start/order — to a per-table `SELECT` for the SQLite leaf; it
//!    never sees an AST, and joins/Take/Exists run as in-memory operators above the
//!    leaf.)
//! 2. **Index-addressed rows / `RelId`-keyed relationships — §3.2's name-keyed
//!    `OwnedRowJson` is SUPERSEDED, not deferred.** The engine is index-addressed
//!    end to end ("never a string on the hot path") and so is the oracle:
//!    `CaughtNode.row` is an `OwnedRow`, and relationships are a `BTreeMap<RelId,
//!    _>` (slot-keyed → slot-stable diffs). §3.2's column-name→value map was a
//!    JS-object isomorphism the Rust port deliberately rejects — the
//!    index-addressed form is the intended design (**the spec is wrong here**,
//!    confirmed with the owner 2026-06-04). If the serde/JS differential (§3.6) is
//!    ever built, names are re-attached *only at the JSON boundary* for the JS
//!    comparison — never in the in-memory oracle.
//! 3. **FNV-1a fingerprint, not sha256 (by design).** A fingerprint wants a *fast*
//!    hash, not a crypto one; `fingerprint` is a deterministic 64-bit FNV-1a over
//!    a length-framed canonical encoding — a fine *in-repo* cross-build /
//!    chunk-equivalence probe. If the cross-*engine* probe
//!    (`flipped-join-merge.perf.test.ts:190`) is ever wired up, both sides need
//!    only agree on *a* fast hash + the same canonical bytes — sha2 is not
//!    required.
//! 4. **No build-twice storage compare yet.** §3.1's "build with `Catch`, build
//!    with `View`, assert `actualStorage == actualStorage2`" is a no-op until a
//!    stateful operator (`Take`/`Cap`/`Exists`, spec `07`) actually writes scratch
//!    storage — none do today (`Graph::alloc_storage` is exercised only by the
//!    arena test). `Storage::scan("")` makes the snapshot trivially buildable when
//!    they land; wired in then.
//!
//! ## Gating
//!
//! The oracle is exported only under `cfg(test)` or the `testkit` cargo feature.
//! That keeps `Catch`, `Snitch`, and serde-gated AST helpers out of production
//! library builds and the shipping wasm. Operator suites that consume
//! `rindle::testkit::*` run with `cargo test --features testkit`; the SQLite-backend
//! cross-checks (a `SourceBackend` from `rindle_sqlite::testkit::sqlite_backend()`) run
//! with `cargo test -p rindle-sqlite --features testkit`.

use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::ast::Ast;
use crate::builder::{build_pipeline, BuildError};
use crate::change::{Basis, Change, FetchRequest, MultiConstraint, Node, NodeStream, Port};
use crate::change::{Constraint, SourceChange};
use crate::graph::{Graph, NodeId};
use crate::storage::StorageValue;
use crate::value::{compare_values, owned_row, OwnedRow, OwnedValue, RelId, Schema, SourceSchema};
use crate::view::{Entry, ResultType, ViewData};
use crate::RindleError;

// ---------------------------------------------------------------------------
// The comparison tree: CaughtNode / CaughtChange (catch.ts `CaughtNode` /
// `CaughtChange`, with the `'yield'` variant gone — Primitive #6)
// ---------------------------------------------------------------------------

// The owned change tree itself now lives in `crate::changes` (graduated out of the
// testkit so production + the `rindle-replica` wrapper can consume it outside
// `cfg(test)`/`testkit`). Re-export the names so `rindle::testkit::{CaughtChange,
// CaughtNode, expand_change, expand_node}` and the oracle code below are unchanged.
pub use crate::changes::{expand_change, expand_node, CaughtChange, CaughtNode};

// ---------------------------------------------------------------------------
// The diff predicate (§3.8): structural equality with full-cell content
// comparison. `OwnedValue` has no derived `PartialEq` (it would invite the wrong
// comparator), so we spell row equality out via `compare_values` (null == null,
// floats via total_cmp — `11` §3.8).
// ---------------------------------------------------------------------------

#[inline]
fn val_eq(a: &OwnedValue, b: &OwnedValue) -> bool {
    compare_values(a.as_ref(), b.as_ref()) == Ordering::Equal
}

#[inline]
fn row_eq(a: &OwnedRow, b: &OwnedRow) -> bool {
    a.len() == b.len()
        && a.cells()
            .zip(b.cells())
            .all(|(x, y)| crate::value::compare_values(x, y) == std::cmp::Ordering::Equal)
}

fn constraint_eq(a: &Constraint, b: &Constraint) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|((c1, v1), (c2, v2))| c1 == c2 && val_eq(v1, v2))
}

fn multi_eq(a: &MultiConstraint, b: &MultiConstraint) -> bool {
    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| constraint_eq(x, y))
}

// `PartialEq`/`Eq` for `CaughtNode`/`CaughtChange` moved with the types into
// `crate::changes` (the structural cell-content comparator).

/// Assert two caught change streams are structurally identical, with a readable
/// failure that includes both trees and their [`fingerprint`]s. Operator suites
/// can also use a bare `assert_eq!` (both `PartialEq` and `Debug` are derived/impl'd).
#[track_caller]
pub fn assert_caught_eq(actual: &[CaughtChange], expected: &[CaughtChange]) {
    if actual != expected {
        panic!(
            "caught change streams differ\n  actual ({}): {actual:#?}\n  expected: {expected:#?}",
            actual.len(),
        );
    }
}

// ---------------------------------------------------------------------------
// fingerprint (§3.5) — canonical serialization + FNV-1a (see deviation #3)
// ---------------------------------------------------------------------------

/// A cheap, deterministic equality probe over a [`Catch`] fetch tree. Two builds
/// (or two `chunk_size`s, `11` §5.8) agree iff their fingerprints match.
///
/// The tree is encoded into a **length-framed** binary buffer (every string/blob
/// prefixed with its byte length, every list with its element count, every cell
/// tagged with a type byte) so the encoding is unambiguous *by construction* — no
/// in-band delimiter a cell value could forge. Per §3.5 the **top-level nodes are
/// sorted** (by their encoding) so the probe is insensitive to top-level emission
/// order; child order within a relationship is **preserved** (it is the operator's
/// sort order). FNV-1a over the buffer; first 16 hex (64 bits). Not sha256 — see
/// the module deviation note.
pub fn fingerprint(nodes: &[CaughtNode]) -> String {
    let mut encs: Vec<Vec<u8>> = nodes
        .iter()
        .map(|n| {
            let mut b = Vec::new();
            enc_node(&mut b, n);
            b
        })
        .collect();
    encs.sort(); // top-level order-independent (§3.5)
    let mut buf = Vec::new();
    enc_len(&mut buf, encs.len());
    for e in &encs {
        enc_bytes(&mut buf, e);
    }
    format!("{:016x}", fnv1a64(&buf))
}

fn enc_node(buf: &mut Vec<u8>, n: &CaughtNode) {
    enc_row(buf, &n.row);
    enc_len(buf, n.relationships.len());
    for (slot, children) in &n.relationships {
        buf.extend_from_slice(&slot.0.to_le_bytes());
        enc_len(buf, children.len());
        for c in children {
            enc_node(buf, c);
        }
    }
}

fn enc_row(buf: &mut Vec<u8>, r: &OwnedRow) {
    enc_len(buf, r.len());
    for v in r.cells() {
        enc_val(buf, &v.to_owned());
    }
}

fn enc_val(buf: &mut Vec<u8>, v: &OwnedValue) {
    match v {
        OwnedValue::Absent => buf.push(6),
        OwnedValue::Null => buf.push(0),
        OwnedValue::Bool(b) => {
            buf.push(1);
            buf.push(*b as u8);
        }
        OwnedValue::Int(i) => {
            buf.push(2);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        // bit pattern is stable (no `-0.0`/NaN formatting ambiguity).
        OwnedValue::Float(f) => {
            buf.push(3);
            buf.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        OwnedValue::Str(s) => {
            buf.push(4);
            enc_bytes(buf, s.as_bytes());
        }
        OwnedValue::Json(s) => {
            buf.push(5);
            enc_bytes(buf, s.as_bytes());
        }
    }
}

fn enc_len(buf: &mut Vec<u8>, n: usize) {
    buf.extend_from_slice(&(n as u64).to_le_bytes());
}

fn enc_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    enc_len(buf, bytes.len());
    buf.extend_from_slice(bytes);
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

// ---------------------------------------------------------------------------
// Catch (catch.ts) — the output oracle operator
// ---------------------------------------------------------------------------

/// The output oracle. Sink it on the operator under test (the runner does this
/// for you; or wire it by hand with [`Graph::add_catch`](crate::graph::Graph) +
/// [`Graph::set_sink_edge`](crate::graph::Graph)). Read what it caught with
/// [`Graph::catch_pushes`](crate::graph::Graph) /
/// [`Graph::catch_fetch`](crate::graph::Graph).
pub struct Catch {
    pub input: NodeId,
    fetch_on_push: Cell<bool>,
    pushes: RefCell<Vec<CaughtChange>>,
    pushes_with_fetch: RefCell<Vec<(CaughtChange, Vec<CaughtNode>)>>,
}

impl Catch {
    pub fn new(input: NodeId, fetch_on_push: bool) -> Catch {
        Catch {
            input,
            fetch_on_push: Cell::new(fetch_on_push),
            pushes: RefCell::new(Vec::new()),
            pushes_with_fetch: RefCell::new(Vec::new()),
        }
    }

    /// `Catch.push` (`catch.ts:68-81`): record `expandChange(change)`, and when
    /// `fetch_on_push` is set, a pre-recorded re-`fetch` of the whole input
    /// alongside it (`{change, fetch}`). Called from the graph's push dispatch.
    pub(crate) fn record<'g>(&self, g: &'g Graph, change: &Change<'g>) {
        let fetched: Option<Vec<CaughtNode>> = if self.fetch_on_push.get() {
            Some(
                g.fetch(self.input, &FetchRequest::all())
                    .map(|n| expand_node(&n))
                    .collect(),
            )
        } else {
            None
        };
        let cc = expand_change(change);
        if let Some(f) = fetched {
            self.pushes_with_fetch.borrow_mut().push((cc.clone(), f));
        }
        self.pushes.borrow_mut().push(cc);
    }

    pub(crate) fn pushes(&self) -> Vec<CaughtChange> {
        self.pushes.borrow().clone()
    }

    pub(crate) fn pushes_with_fetch(&self) -> Vec<(CaughtChange, Vec<CaughtNode>)> {
        self.pushes_with_fetch.borrow().clone()
    }
}

// ---------------------------------------------------------------------------
// Snitch (snitch.ts) — the message-log oracle operator
// ---------------------------------------------------------------------------

/// A serialized `FetchRequest` for the log. Names are NOT re-attached (the engine
/// is index-addressed; see module deviation #2) — `constraint` columns are
/// [`ColId`](crate::value::ColId)s. `start` is `(row, is_at)` where `is_at` is
/// true for [`Basis::At`], false for [`Basis::After`].
#[derive(Clone, Debug)]
pub struct FetchRequestRecord {
    pub constraint: Option<Constraint>,
    pub start: Option<(OwnedRow, bool)>,
    pub reverse: bool,
    pub multi_constraints: Vec<MultiConstraint>,
}

impl FetchRequestRecord {
    fn capture(req: &FetchRequest) -> FetchRequestRecord {
        FetchRequestRecord {
            constraint: req.constraint.clone(),
            start: req
                .start
                .as_ref()
                .map(|s| (s.row.clone(), matches!(s.basis, Basis::At))),
            reverse: req.reverse,
            multi_constraints: req.multi_constraints.clone(),
        }
    }
}

impl PartialEq for FetchRequestRecord {
    fn eq(&self, o: &Self) -> bool {
        let c_eq = match (&self.constraint, &o.constraint) {
            (None, None) => true,
            (Some(a), Some(b)) => constraint_eq(a, b),
            _ => false,
        };
        let s_eq = match (&self.start, &o.start) {
            (None, None) => true,
            (Some((r1, a1)), Some((r2, a2))) => a1 == a2 && row_eq(r1, r2),
            _ => false,
        };
        let m_eq = self.multi_constraints.len() == o.multi_constraints.len()
            && self
                .multi_constraints
                .iter()
                .zip(&o.multi_constraints)
                .all(|(a, b)| multi_eq(a, b));
        c_eq && s_eq && self.reverse == o.reverse && m_eq
    }
}
impl Eq for FetchRequestRecord {}

/// A serialized [`Change`] for the push log (`snitch.ts` `toChangeRecord`): rows
/// only, recursive for `Child`. Faithful to the JS log, which captures **neither**
/// relationships on an Add (`snitch.ts:200-205`) **nor the relationship name on a
/// Child** (`toChangeRecord`, `snitch.ts:106-111`, emits `{type,row,child}` only) —
/// so `Child` here carries no `rel` slot (unlike the *output*-oracle
/// [`CaughtChange::Child`], whose `catch.ts` counterpart does carry it).
#[derive(Clone, Debug)]
pub enum ChangeRecord {
    Add(OwnedRow),
    Remove(OwnedRow),
    Edit {
        row: OwnedRow,
        old: OwnedRow,
    },
    Child {
        row: OwnedRow,
        child: Box<ChangeRecord>,
    },
}

impl ChangeRecord {
    fn capture(c: &Change<'_>) -> ChangeRecord {
        match c {
            Change::Add(n) => ChangeRecord::Add(n.row.clone()),
            Change::Remove(n) => ChangeRecord::Remove(n.row.clone()),
            Change::Edit { node, old } => ChangeRecord::Edit {
                row: node.row.clone(),
                old: old.row.clone(),
            },
            // The relationship slot is intentionally dropped (matches snitch.ts).
            Change::Child { node, child, .. } => ChangeRecord::Child {
                row: node.row.clone(),
                child: Box::new(ChangeRecord::capture(child)),
            },
        }
    }
}

impl PartialEq for ChangeRecord {
    fn eq(&self, o: &Self) -> bool {
        use ChangeRecord::*;
        match (self, o) {
            (Add(a), Add(b)) | (Remove(a), Remove(b)) => row_eq(a, b),
            (Edit { row: r1, old: o1 }, Edit { row: r2, old: o2 }) => {
                row_eq(r1, r2) && row_eq(o1, o2)
            }
            (Child { row: r1, child: c1 }, Child { row: r2, child: c2 }) => {
                row_eq(r1, r2) && c1 == c2
            }
            _ => false,
        }
    }
}
impl Eq for ChangeRecord {}

/// One recorded message. Mirrors `snitch.ts` `SnitchMessage`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnitchMessage {
    Fetch {
        name: String,
        req: FetchRequestRecord,
    },
    /// Recorded on stream **`Drop`** (`snitch.ts:81-83`): `count` is exactly how
    /// many rows were pulled before the stream was abandoned (a Take/`first`
    /// early-termination signal, and a cursor-balance probe).
    FetchCount {
        name: String,
        req: FetchRequestRecord,
        count: usize,
    },
    Push {
        name: String,
        change: ChangeRecord,
    },
}

/// A transparent pass-through operator that records every `fetch`/`push`/
/// `fetchCount` it relays. Insert it between two operators (its `input` is the
/// upstream; wire its single output with
/// [`Graph::set_output`](crate::graph::Graph)). Port-transparent: it forwards a
/// push on whichever [`Port`] it arrived on, so it may sit before a join port.
pub struct Snitch {
    pub input: NodeId,
    name: String,
    log: RefCell<Vec<SnitchMessage>>,
    output: Cell<Option<NodeId>>,
}

impl Snitch {
    pub fn new(input: NodeId, name: impl Into<String>) -> Snitch {
        Snitch {
            input,
            name: name.into(),
            log: RefCell::new(Vec::new()),
            output: Cell::new(None),
        }
    }

    /// `Snitch.fetch` (`snitch.ts:65-84`): log the `fetch`, then vend the upstream
    /// stream wrapped so a `fetchCount` is logged on `Drop`.
    pub(crate) fn fetch<'g>(&'g self, g: &'g Graph, req: &FetchRequest) -> NodeStream<'g> {
        let rec = FetchRequestRecord::capture(req);
        self.log.borrow_mut().push(SnitchMessage::Fetch {
            name: self.name.clone(),
            req: rec.clone(),
        });
        let inner = g.fetch(self.input, req);
        Box::new(CountingStream {
            inner,
            log: &self.log,
            name: self.name.as_str(),
            req: rec,
            count: 0,
        })
    }

    /// `Snitch.push` (`snitch.ts:86-91`): log the change, then forward it on the
    /// arriving port.
    pub(crate) fn push<'g>(&'g self, g: &'g Graph, change: Change<'g>, port: Port) {
        self.log.borrow_mut().push(SnitchMessage::Push {
            name: self.name.clone(),
            change: ChangeRecord::capture(&change),
        });
        let out = self.output.get().expect("Snitch output not wired");
        g.push(out, change, port);
    }

    pub(crate) fn set_output(&self, out: NodeId) {
        self.output.set(Some(out));
    }

    pub(crate) fn log(&self) -> Vec<SnitchMessage> {
        self.log.borrow().clone()
    }

    pub(crate) fn clear_log(&self) {
        self.log.borrow_mut().clear();
    }
}

/// The fetch-stream wrapper that records a `fetchCount` on `Drop` — the RAII
/// rendering of the JS `try { .. } finally { log fetchCount }` (`snitch.ts:70-84`).
/// Holds a borrow into the owning `Snitch`'s log (valid for `'g`, the graph borrow).
struct CountingStream<'g> {
    inner: NodeStream<'g>,
    log: &'g RefCell<Vec<SnitchMessage>>,
    name: &'g str,
    req: FetchRequestRecord,
    count: usize,
}

impl<'g> Iterator for CountingStream<'g> {
    type Item = Node<'g>;
    fn next(&mut self) -> Option<Node<'g>> {
        let n = self.inner.next();
        if n.is_some() {
            self.count += 1;
        }
        n
    }
}

impl Drop for CountingStream<'_> {
    fn drop(&mut self) {
        self.log.borrow_mut().push(SnitchMessage::FetchCount {
            name: self.name.to_string(),
            req: self.req.clone(),
            count: self.count,
        });
    }
}

// ---------------------------------------------------------------------------
// Inline-snapshot constructor helpers (§4 "Layout rules")
// ---------------------------------------------------------------------------

/// A [`RelId`] slot literal (`rel(0)` is the first declared relationship).
pub fn rel(slot: u32) -> RelId {
    RelId(slot)
}

/// A [`CaughtNode`] with relationships: `cnode(vec![Int(1)], vec![(rel(0), vec![..])])`.
pub fn cnode(row: Vec<OwnedValue>, rels: Vec<(RelId, Vec<CaughtNode>)>) -> CaughtNode {
    CaughtNode {
        row: owned_row(row),
        relationships: rels.into_iter().collect(),
    }
}

/// A leaf [`CaughtNode`] (no relationships).
pub fn cleaf(row: Vec<OwnedValue>) -> CaughtNode {
    cnode(row, Vec::new())
}

/// `CaughtChange::Add` over a node.
pub fn add(node: CaughtNode) -> CaughtChange {
    CaughtChange::Add(node)
}

/// `CaughtChange::Remove` over a node.
pub fn remove(node: CaughtNode) -> CaughtChange {
    CaughtChange::Remove(node)
}

/// `CaughtChange::Edit` over the old/new rows.
pub fn edit(old: Vec<OwnedValue>, row: Vec<OwnedValue>) -> CaughtChange {
    CaughtChange::Edit {
        old: owned_row(old),
        row: owned_row(row),
    }
}

/// `CaughtChange::Child`: a parent row, the relationship slot, and the nested change.
pub fn child(row: Vec<OwnedValue>, rel: RelId, change: CaughtChange) -> CaughtChange {
    CaughtChange::Child {
        row: owned_row(row),
        rel,
        change: Box::new(change),
    }
}

// ---------------------------------------------------------------------------
// The runners (§3.1) — build-closure flavoured (see module deviation #1)
// ---------------------------------------------------------------------------

/// One source's shape + seed rows. Mirrors a `Sources[name]` + `SourceContents[name]`.
#[derive(Clone)]
pub struct SourceSpec {
    pub schema: SourceSchema,
    pub rows: Vec<OwnedRow>,
}

impl SourceSpec {
    pub fn new(schema: SourceSchema, rows: Vec<Vec<OwnedValue>>) -> SourceSpec {
        SourceSpec {
            schema,
            rows: rows.into_iter().map(owned_row).collect(),
        }
    }
}

/// The injected leaf-builder behind [`SourceBackend::Custom`]: build one source from
/// (graph, table name, schema, initial rows, extra rows the fixture will later push).
pub type SourceBuilder =
    dyn Fn(&mut Graph, String, SourceSchema, Vec<OwnedRow>, &[OwnedRow]) -> NodeId;

/// Which leaf backend a runner should seed. Existing runner functions use
/// [`SourceBackend::Memory`]; the `*_with_backend` variants let an alternate-backend
/// crate reuse the same fixtures against a custom leaf builder (e.g. `rindle-sqlite`'s
/// SQLite `TableSource` seeder, via `rindle_sqlite::testkit::sqlite_backend()`).
#[derive(Clone)]
pub enum SourceBackend {
    Memory,
    /// A custom leaf builder injected by a backend crate — keeps the SQLite seeding
    /// (and its `TableSource` dependency) out of the core testkit.
    Custom(Rc<SourceBuilder>),
}

/// The `run_push_test` observable output (the buildable subset of §3.1's
/// `PushTestResult`). `data`/`log`/`storage` are deferred — see module
/// deviations #1/#4; an operator suite that wants the message log places a
/// [`Snitch`] and reads [`Graph::snitch_log`](crate::graph::Graph).
pub struct PushTestResult {
    /// The pipeline materialized once at hydrate (`Catch.fetch()`), before any push.
    pub initial: Vec<CaughtNode>,
    /// The downstream change stream caught during the push phase (`Catch.pushes`).
    pub pushes: Vec<CaughtChange>,
    /// Populated only when `fetch_on_push` is set: each push paired with a
    /// re-fetch of the whole pipeline taken alongside it.
    pub pushes_with_fetch: Vec<(CaughtChange, Vec<CaughtNode>)>,
}

/// The fixture-driven push runner (`runPushTest`, `fetch-and-push-tests.ts:63`).
///
/// Steps (faithful to the JS, modulo the deviations): build N sources from
/// `sources`; let `build` wire the operator pipeline (it receives the source
/// `NodeId`s, creates its own connections, wires every edge **except** the final
/// one into the sink, and returns the op that feeds the sink); attach a [`Catch`]
/// sink and wire it; **hydrate** with one `Catch.fetch()` (recorded as `initial`,
/// `fetch-and-push-tests.ts:100`); **clear** any [`Snitch`] logs so they reflect
/// only the push phase (the `clearLog` of `:81`); run the `pushes` in order via
/// `Graph::source_push`; return what `Catch` caught.
///
/// `pushes` is `(source_index, change)` — the index into `sources`.
pub fn run_push_test(
    sources: Vec<SourceSpec>,
    build: impl FnOnce(&mut Graph, &[NodeId]) -> NodeId,
    pushes: Vec<(usize, SourceChange)>,
    fetch_on_push: bool,
) -> PushTestResult {
    run_push_test_with_backend(sources, SourceBackend::Memory, build, pushes, fetch_on_push)
}

pub fn run_push_test_with_backend(
    sources: Vec<SourceSpec>,
    backend: SourceBackend,
    build: impl FnOnce(&mut Graph, &[NodeId]) -> NodeId,
    pushes: Vec<(usize, SourceChange)>,
    fetch_on_push: bool,
) -> PushTestResult {
    let mut g = Graph::new();
    let extras = source_push_rows(sources.len(), &pushes);
    let src_ids: Vec<NodeId> = sources
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            add_test_source(
                &mut g,
                backend.clone(),
                format!("source_{i}"),
                s.schema,
                s.rows,
                &extras[i],
            )
        })
        .collect();

    let sink_input = build(&mut g, &src_ids);
    let catch = g.add_catch(sink_input, fetch_on_push);
    g.set_sink_edge(sink_input, catch);

    let initial = g.catch_fetch(catch, &FetchRequest::all());
    g.clear_all_snitch_logs();

    for (idx, change) in pushes {
        g.source_push(src_ids[idx], change);
    }

    PushTestResult {
        initial,
        pushes: g.catch_pushes(catch),
        pushes_with_fetch: g.catch_pushes_with_fetch(catch),
    }
}

/// The fetch-only runner (`runFetchTest`, `fetch-and-push-tests.ts:161`): build +
/// hydrate, return the `Catch.fetch()` tree. No pushes.
pub fn run_fetch_test(
    sources: Vec<SourceSpec>,
    build: impl FnOnce(&mut Graph, &[NodeId]) -> NodeId,
) -> Vec<CaughtNode> {
    run_fetch_test_with_backend(sources, SourceBackend::Memory, build)
}

pub fn run_fetch_test_with_backend(
    sources: Vec<SourceSpec>,
    backend: SourceBackend,
    build: impl FnOnce(&mut Graph, &[NodeId]) -> NodeId,
) -> Vec<CaughtNode> {
    let mut g = Graph::new();
    let src_ids: Vec<NodeId> = sources
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            add_test_source(
                &mut g,
                backend.clone(),
                format!("source_{i}"),
                s.schema,
                s.rows,
                &[],
            )
        })
        .collect();
    let sink_input = build(&mut g, &src_ids);
    let catch = g.add_catch(sink_input, false);
    g.set_sink_edge(sink_input, catch);
    g.catch_fetch(catch, &FetchRequest::all())
}

// ---------------------------------------------------------------------------
// The AST-driven runners (§3.1, deviation #1 lifted) — drive the SAME pipeline
// through `build_pipeline` instead of a hand-wired build closure. This is the
// payoff of the builder port: a JS-emitted `Ast` + named source/push fixtures
// run against the Rust engine, so the JS push/fetch suites become a Rust
// differential corpus for the built subset. (The serde JSON boundary that
// *parses* a JS fixture into these types is still deferred — §3.6.)
// ---------------------------------------------------------------------------

/// One **named** source's shape + seed rows — the `Ast`-runner analogue of
/// [`SourceSpec`], adding the table name that the `Ast`'s `table`/relationship
/// references and the name-keyed pushes resolve against (mirrors a JS
/// `Sources[name]` + `SourceContents[name]`).
#[derive(Clone)]
pub struct TableSpec {
    pub name: String,
    pub schema: SourceSchema,
    pub rows: Vec<OwnedRow>,
}

impl TableSpec {
    pub fn new(
        name: impl Into<String>,
        schema: SourceSchema,
        rows: Vec<Vec<OwnedValue>>,
    ) -> TableSpec {
        TableSpec {
            name: name.into(),
            schema,
            rows: rows.into_iter().map(owned_row).collect(),
        }
    }
}

/// `table name -> (source NodeId, schema clone)` — the input to `build_pipeline`'s
/// `resolve` closure (the [`TestBuilderDelegate::getSource`] analogue) and to
/// name-keyed pushes / the view-schema derivation.
type SourcesByName = BTreeMap<String, (NodeId, SourceSchema)>;

fn add_named_sources_with_backend(
    g: &mut Graph,
    sources: Vec<TableSpec>,
    backend: SourceBackend,
    extra_rows: &BTreeMap<String, Vec<OwnedRow>>,
) -> SourcesByName {
    let mut by_name = BTreeMap::new();
    for s in sources {
        let extras = extra_rows.get(&s.name).map(Vec::as_slice).unwrap_or(&[]);
        let id = add_test_source(
            g,
            backend.clone(),
            s.name.clone(),
            s.schema.clone(),
            s.rows,
            extras,
        );
        by_name.insert(s.name, (id, s.schema));
    }
    by_name
}

fn add_test_source(
    g: &mut Graph,
    backend: SourceBackend,
    _table_name: String,
    schema: SourceSchema,
    rows: Vec<OwnedRow>,
    _extra_rows: &[OwnedRow],
) -> NodeId {
    match backend {
        SourceBackend::Memory => g.add_source(schema, rows),
        SourceBackend::Custom(build) => build(g, _table_name, schema, rows, _extra_rows),
    }
}

fn source_push_rows(n: usize, pushes: &[(usize, SourceChange)]) -> Vec<Vec<OwnedRow>> {
    let mut rows = vec![Vec::new(); n];
    for (idx, change) in pushes {
        for row in change_rows(change) {
            rows[*idx].push(row.clone());
        }
    }
    rows
}

fn table_push_rows(pushes: &[(&str, SourceChange)]) -> BTreeMap<String, Vec<OwnedRow>> {
    let mut rows: BTreeMap<String, Vec<OwnedRow>> = BTreeMap::new();
    for (name, change) in pushes {
        let entry = rows.entry((*name).to_string()).or_default();
        for row in change_rows(change) {
            entry.push(row.clone());
        }
    }
    rows
}

fn change_rows(change: &SourceChange) -> Vec<&OwnedRow> {
    match change {
        SourceChange::Add(r) | SourceChange::Remove(r) => vec![r],
        SourceChange::Edit { row, old } => vec![row, old],
    }
}

/// The `Ast`-driven push runner — [`run_push_test`] with the pipeline lowered by
/// [`build_pipeline`] instead of a build closure
/// (`runPushTest`, `fetch-and-push-tests.ts:63`).
///
/// Builds the named sources, lowers `ast` against them (a table name resolves to
/// its source `NodeId` + a clone of its [`Schema`]), sinks the top operator into a
/// [`Catch`], **hydrates** (`initial`), **clears** any [`Snitch`] logs, then runs
/// `pushes` in order — each `(table_name, change)` applied to that source via
/// [`Graph::source_push`](crate::graph::Graph). Returns `Err` if the `Ast` is
/// outside the built subset, surfacing `build_pipeline`'s [`BuildError`] where JS
/// would throw (so a test can assert the rejection too).
pub fn run_push_test_ast(
    sources: Vec<TableSpec>,
    ast: &Ast,
    pushes: Vec<(&str, SourceChange)>,
    fetch_on_push: bool,
) -> Result<PushTestResult, BuildError> {
    run_push_test_ast_with_backend(sources, SourceBackend::Memory, ast, pushes, fetch_on_push)
}

pub fn run_push_test_ast_with_backend(
    sources: Vec<TableSpec>,
    backend: SourceBackend,
    ast: &Ast,
    pushes: Vec<(&str, SourceChange)>,
    fetch_on_push: bool,
) -> Result<PushTestResult, BuildError> {
    let mut g = Graph::new();
    let extras = table_push_rows(&pushes);
    let (by_name, top) = seed_and_build_with_backend(&mut g, sources, backend, &extras, ast)?;
    let catch = g.add_catch(top, fetch_on_push);
    g.set_sink_edge(top, catch);

    let initial = g.catch_fetch(catch, &FetchRequest::all());
    g.clear_all_snitch_logs();

    for (name, change) in pushes {
        let (src_id, _) = by_name
            .get(name)
            .unwrap_or_else(|| panic!("push targets unknown source {name:?}"));
        g.source_push(*src_id, change);
    }

    Ok(PushTestResult {
        initial,
        pushes: g.catch_pushes(catch),
        pushes_with_fetch: g.catch_pushes_with_fetch(catch),
    })
}

/// The `Ast`-driven fetch-only runner (`runFetchTest`) — build + hydrate, return
/// the `Catch.fetch()` tree. The [`build_pipeline`]
/// analogue of [`run_fetch_test`].
pub fn run_fetch_test_ast(
    sources: Vec<TableSpec>,
    ast: &Ast,
) -> Result<Vec<CaughtNode>, BuildError> {
    run_fetch_test_ast_with_backend(sources, SourceBackend::Memory, ast)
}

pub fn run_fetch_test_ast_with_backend(
    sources: Vec<TableSpec>,
    backend: SourceBackend,
    ast: &Ast,
) -> Result<Vec<CaughtNode>, BuildError> {
    let mut g = Graph::new();
    let (_by_name, top) =
        seed_and_build_with_backend(&mut g, sources, backend, &BTreeMap::new(), ast)?;
    let catch = g.add_catch(top, false);
    g.set_sink_edge(top, catch);
    Ok(g.catch_fetch(catch, &FetchRequest::all()))
}

/// Seed `g` with the named sources and lower `ast` through
/// [`build_pipeline`](crate::builder::build_pipeline), returning the `table name ->
/// (source NodeId, schema)` map and the top operator. The shared front half of every
/// `Ast`-driven runner; the `resolve` borrow of `by_name` is scoped to the build so
/// callers read `by_name` again afterwards.
fn seed_and_build_with_backend(
    g: &mut Graph,
    sources: Vec<TableSpec>,
    backend: SourceBackend,
    extra_rows: &BTreeMap<String, Vec<OwnedRow>>,
    ast: &Ast,
) -> Result<(SourcesByName, NodeId), BuildError> {
    let by_name = add_named_sources_with_backend(g, sources, backend, extra_rows);
    let top = {
        let resolve = |t: &str| by_name.get(t).cloned();
        build_pipeline(g, ast, &resolve)?
    };
    Ok((by_name, top))
}

// ---------------------------------------------------------------------------
// The View-based AST runners (§3.1, deviations #1 + #4 lifted) — the same
// `build_pipeline` pipeline driven into the **production `ArrayView`** (spec `09`),
// not the `Catch` oracle, so a test reads the materialized `view.data` tree exactly
// as JS `runPushTest` does. This is the e2e payoff: build pipeline → ArrayView →
// push → read `.data`, with the JS build-twice storage compare.
// ---------------------------------------------------------------------------

/// The View-based runner's output (the `Ast` analogue of JS `runPushTest`'s
/// `{data, pushes}`). `data` is the production [`View`](crate::view::View)'s
/// **materialized tree** (`view.data`) after the pushes — the headline e2e artifact
/// the `Catch`-sink runners could not produce. `initial` is that tree right after
/// hydrate (pre-push); `pushes` is the downstream change stream the parallel `Catch`
/// build recorded (push phase only). Both trees are converted to the comparable
/// [`CaughtNode`] form and carry **only in-view relationship slots** (gating EXISTS
/// slots are excluded — the JS `view.data` shape).
///
/// The build-twice storage compare (spec `11` §3.1.1) runs inside the runner: it
/// builds the pipeline once with a `Catch` sink and once with the `View`, runs the
/// same pushes through both, and asserts the operator scratch storage is identical
/// (a free sink-independence check).
#[derive(Debug)]
pub struct ViewPushTestResult {
    /// The `ArrayView` tree after hydrate, before any push (in-view slots only).
    pub initial: Vec<CaughtNode>,
    /// The `ArrayView` tree after all pushes + per-push flush — the JS `view.data`.
    pub data: Vec<CaughtNode>,
    /// The downstream change stream the parallel `Catch` build caught (push phase).
    pub pushes: Vec<CaughtChange>,
}

/// Derive the production-`View` **hierarchical schema** for `ast` from the seeded
/// sources. The schema *is* the view shape: a relationship slot is in-view iff its
/// `RelDef` carries a child schema (a gating EXISTS / unused slot is a join-only
/// `RelDef::new`, which `apply_change` skips). This is the testkit adapter over the
/// `by_name` map; the real derivation lives in
/// [`builder::view_schema`](crate::builder::view_schema).
fn view_schema(ast: &Ast, by_name: &SourcesByName) -> Result<Schema, BuildError> {
    crate::builder::view_schema(ast, &|t| by_name.get(t).cloned())
}

/// Convert a production [`View`](crate::view::View) snapshot ([`ViewData`]) into the
/// comparable [`CaughtNode`] tree the `Catch`/AST runners use — **only the in-view
/// relationship slots** (`schema.rel_child(slot).is_some()`), so the result is the JS
/// `view.data` shape (gating EXISTS slots excluded). Reuses `CaughtNode`'s
/// `PartialEq`/[`fingerprint`]/[`cnode`]/[`cleaf`] machinery, so a View result
/// compares directly against an inline-snapshot expectation or a `Catch` fetch tree.
pub fn view_data_to_caught(data: &ViewData, schema: &Schema) -> Vec<CaughtNode> {
    data.items.iter().map(|e| entry_caught(e, schema)).collect()
}

fn entry_caught(e: &Entry, schema: &Schema) -> CaughtNode {
    let mut relationships: BTreeMap<RelId, Vec<CaughtNode>> = BTreeMap::new();
    for (i, rel) in e.rels.iter().enumerate() {
        // In-view slots only (a slot is in-view iff its `RelDef` carries a child schema;
        // gating / out-of-view slots are skipped).
        let child_schema = match schema.rel_child(RelId(i as u32)) {
            Some(s) => s,
            None => continue,
        };
        let children = rel
            .items
            .iter()
            .map(|c| entry_caught(c, child_schema))
            .collect();
        relationships.insert(RelId(i as u32), children);
    }
    CaughtNode {
        row: e.row.clone(),
        relationships,
    }
}

/// The `Ast`-driven **View** push runner — [`run_push_test_ast`] with the pipeline
/// materialized into the production [`View`](crate::view::View) instead of a `Catch`
/// (`runPushTest`, `fetch-and-push-tests.ts:63`). Seeds the named sources, lowers
/// `ast`, sinks the top operator into an `ArrayView` (hierarchical schema derived by
/// [`view_schema`](crate::builder::view_schema)), **hydrates** (`initial`), runs `pushes` in
/// order — each `(table_name, change)` applied via
/// [`Graph::source_push`](crate::graph::Graph) then [`Graph::flush_view`] (one
/// transaction per push) — and reads the materialized `view.data`.
///
/// In parallel it builds the **same** pipeline into a `Catch` (for the `pushes`
/// change stream) and asserts the two builds' operator storage snapshots are equal —
/// the JS build-twice sink-independence check (spec `11` §3.1.1). Returns `Err` if
/// `ast` is outside `build_pipeline`'s subset (where JS would throw).
pub fn run_push_test_ast_view(
    sources: Vec<TableSpec>,
    ast: &Ast,
    pushes: Vec<(&str, SourceChange)>,
    fetch_on_push: bool,
) -> Result<ViewPushTestResult, BuildError> {
    run_push_test_ast_view_with_backend(sources, SourceBackend::Memory, ast, pushes, fetch_on_push)
}

pub fn run_push_test_ast_view_with_backend(
    sources: Vec<TableSpec>,
    backend: SourceBackend,
    ast: &Ast,
    pushes: Vec<(&str, SourceChange)>,
    fetch_on_push: bool,
) -> Result<ViewPushTestResult, BuildError> {
    let extras = table_push_rows(&pushes);
    // --- Build #1: Catch sink (the downstream change stream + storage snapshot). ---
    let mut gc = Graph::new();
    let (bn_c, top_c) =
        seed_and_build_with_backend(&mut gc, sources.clone(), backend.clone(), &extras, ast)?;
    let catch = gc.add_catch(top_c, fetch_on_push);
    gc.set_sink_edge(top_c, catch);
    let _ = gc.catch_fetch(catch, &FetchRequest::all()); // hydrate (writes Take/Cap state)
    gc.clear_all_snitch_logs();
    for (name, change) in &pushes {
        let (sid, _) = bn_c
            .get(*name)
            .unwrap_or_else(|| panic!("push targets unknown source {name:?}"));
        gc.source_push(*sid, change.clone());
    }
    let pushes_caught = gc.catch_pushes(catch);

    // --- Build #2: production View sink (the materialized tree + storage snapshot). ---
    let mut gv = Graph::new();
    let (bn_v, top_v) = seed_and_build_with_backend(&mut gv, sources, backend, &extras, ast)?;
    let vschema = view_schema(ast, &bn_v)?;
    let view = gv.add_view_with(top_v, vschema.clone(), false, ResultType::Complete);
    gv.set_sink_edge(top_v, view);
    gv.hydrate(view);
    let initial = view_data_to_caught(&gv.view_data(view), &vschema);
    for (name, change) in &pushes {
        let (sid, _) = bn_v
            .get(*name)
            .unwrap_or_else(|| panic!("push targets unknown source {name:?}"));
        gv.source_push(*sid, change.clone());
        gv.flush_view(view); // close each push's transaction (the JS per-commit flush)
    }
    let data = view_data_to_caught(&gv.view_data(view), &vschema);

    // --- Build twice, compare storage (spec 11 §3.1.1, adapted). ---
    // The spec frames this as "operator scratch storage is identical regardless of
    // sink." That literal equality does NOT hold in this port for a query with an
    // **out-of-view (gating EXISTS) relationship**: the `Catch` oracle's `expand_node`
    // eagerly drains *every* relationship thunk — including the gating ones the
    // production `View` never materializes — and a `Cap`/`Exists` fetch persists its
    // per-partition state (`cap.rs` `initial_fetch` → `set_state`). So the Catch build
    // writes scratch entries for partitions the View build never touches: Catch's
    // storage is a strict **superset** of the View's. The check below is the SUBSET
    // form (every View-sink entry present + equal in the Catch build). Note this holds
    // *by construction* today — the View's fold writes no operator storage, and it can
    // only ever drain a subset of the thunks Catch drains — so it is a structural
    // invariant + regression guard, NOT a routine bug-catcher. The View runner's real
    // fold-correctness coverage is the explicit `.data`/`.initial` assertions each test
    // writes (this storage check never inspects the materialized tree).
    assert_view_storage_subset_of_catch(&gv.storage_snapshot(), &gc.storage_snapshot());

    Ok(ViewPushTestResult {
        initial,
        data,
        pushes: pushes_caught,
    })
}

/// A stepwise variant of [`run_push_test_ast_view`]: apply each push to **one**
/// long-lived production `View` and snapshot the materialized tree after *every* push,
/// so a differential harness can assert parity at each point of a long mutation history
/// (catching state-accumulation drift a single end-of-batch check would miss). Returns
/// the post-hydrate tree plus one snapshot per push, in order. (Memory backend; no
/// Catch oracle or storage cross-check — the external SQLite oracle is the ground truth.)
pub fn run_push_walk_ast_view(
    sources: Vec<TableSpec>,
    ast: &Ast,
    pushes: Vec<(&str, SourceChange)>,
) -> Result<(Vec<CaughtNode>, Vec<Vec<CaughtNode>>), BuildError> {
    let extras = table_push_rows(&pushes);
    let mut gv = Graph::new();
    let (bn_v, top_v) =
        seed_and_build_with_backend(&mut gv, sources, SourceBackend::Memory, &extras, ast)?;
    let vschema = view_schema(ast, &bn_v)?;
    let view = gv.add_view_with(top_v, vschema.clone(), false, ResultType::Complete);
    gv.set_sink_edge(top_v, view);
    gv.hydrate(view);
    let initial = view_data_to_caught(&gv.view_data(view), &vschema);

    let mut steps = Vec::with_capacity(pushes.len());
    for (name, change) in &pushes {
        let (sid, _) = bn_v
            .get(*name)
            .unwrap_or_else(|| panic!("push targets unknown source {name:?}"));
        gv.source_push(*sid, change.clone());
        gv.flush_view(view); // close each push's transaction (the JS per-commit flush)
        steps.push(view_data_to_caught(&gv.view_data(view), &vschema));
    }
    Ok((initial, steps))
}

/// Like [`run_push_walk_ast_view`] but, instead of the per-step trees, returns the
/// **total operator scratch-state entry count** (summed across every storage slot) after
/// the whole walk. A stateful operator (`Take`/`Cap`/`Reduce`) keys its bookkeeping by
/// partition/group, so a state leak — a partition slot kept alive after its rows are gone
/// — shows up here as this count growing without bound under churn that should reach a
/// bounded steady state. Memory backend (operator state is the in-RAM `BTreeMap`).
pub fn run_push_walk_ast_view_state_entries(
    sources: Vec<TableSpec>,
    ast: &Ast,
    pushes: Vec<(&str, SourceChange)>,
) -> Result<usize, BuildError> {
    let extras = table_push_rows(&pushes);
    let mut gv = Graph::new();
    let (bn_v, top_v) =
        seed_and_build_with_backend(&mut gv, sources, SourceBackend::Memory, &extras, ast)?;
    let vschema = view_schema(ast, &bn_v)?;
    let view = gv.add_view_with(top_v, vschema.clone(), false, ResultType::Complete);
    gv.set_sink_edge(top_v, view);
    gv.hydrate(view);

    for (name, change) in &pushes {
        let (sid, _) = bn_v
            .get(*name)
            .unwrap_or_else(|| panic!("push targets unknown source {name:?}"));
        gv.source_push(*sid, change.clone());
        gv.flush_view(view);
    }
    Ok(gv.storage_snapshot().iter().map(Vec::len).sum())
}

/// The production-`View` hierarchical [`Schema`] the View runners derive for `ast` from
/// the named sources — exposed so a differential harness can serialize a [`CaughtNode`]
/// result back to **name-keyed** JSON (the schema carries column + relationship *names*
/// and the in-view shape: a relationship is in-view iff its `RelDef` has a child schema).
/// Pure derivation: it touches no graph, so the source `NodeId`s are immaterial.
pub fn ast_view_schema(sources: &[TableSpec], ast: &Ast) -> Result<Schema, BuildError> {
    let by_name: SourcesByName = sources
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.clone(), (NodeId::new(i as u32, 0), s.schema.clone())))
        .collect();
    view_schema(ast, &by_name)
}

/// The `Ast`-driven **View** fetch-only runner (`runFetchTest`): seed + lower +
/// hydrate, return the materialized `view.data` tree. The
/// [`View`](crate::view::View) analogue of [`run_fetch_test_ast`].
pub fn run_fetch_test_ast_view(
    sources: Vec<TableSpec>,
    ast: &Ast,
) -> Result<Vec<CaughtNode>, BuildError> {
    run_fetch_test_ast_view_with_backend(sources, SourceBackend::Memory, ast)
}

pub fn run_fetch_test_ast_view_with_backend(
    sources: Vec<TableSpec>,
    backend: SourceBackend,
    ast: &Ast,
) -> Result<Vec<CaughtNode>, BuildError> {
    let mut g = Graph::new();
    let (by_name, top) =
        seed_and_build_with_backend(&mut g, sources, backend, &BTreeMap::new(), ast)?;
    let vschema = view_schema(ast, &by_name)?;
    let view = g.add_view_with(top, vschema.clone(), false, ResultType::Complete);
    g.set_sink_edge(top, view);
    g.hydrate(view);
    Ok(view_data_to_caught(&g.view_data(view), &vschema))
}

/// [`run_fetch_test_ast_view`] with a fallible hydrate: build errors stay the outer
/// `Result` (a broken fixture/AST is a harness bug), while a parked **runtime** error —
/// e.g. design 226 §5.3's integer-sum overflow, raised at every `try_*` boundary while a
/// stored group's all-integer total is outside i64 — comes back as the inner
/// `Err(RindleError)` instead of panicking, so a differential lane can grade it against
/// the oracle's own refusal (`rindle-testfix`'s `is_integer_overflow`).
pub fn run_fetch_test_ast_view_fallible(
    sources: Vec<TableSpec>,
    ast: &Ast,
) -> Result<Result<Vec<CaughtNode>, RindleError>, BuildError> {
    let mut g = Graph::new();
    let (by_name, top) = seed_and_build_with_backend(
        &mut g,
        sources,
        SourceBackend::Memory,
        &BTreeMap::new(),
        ast,
    )?;
    let vschema = view_schema(ast, &by_name)?;
    let view = g.add_view_with(top, vschema.clone(), false, ResultType::Complete);
    g.set_sink_edge(top, view);
    Ok(match g.try_hydrate(view) {
        Ok(()) => Ok(view_data_to_caught(&g.view_data(view), &vschema)),
        Err(e) => Err(e),
    })
}

/// The build-twice storage check (subset-agreement form): assert every entry in the
/// `View`-sink snapshot ([`Graph::storage_snapshot`]) appears **identically** in the
/// `Catch`-sink snapshot. The production `View`'s fold writes **no** operator storage
/// (only `Take`/`Cap` do, during fetch/push), and the View can only ever drain a
/// *subset* of the relationship thunks the `Catch` oracle's `expand_node` drains —
/// Catch's eager drain only *adds* scratch entries (for out-of-view rels the View
/// never fetches). So View-storage ⊆ Catch-storage holds **by construction** today:
/// this is a structural invariant + a regression guard against a future change that
/// makes the View write storage or alter its fetch-forcing, **not** a routine
/// bug-catcher (it never inspects the materialized tree — fold correctness is covered
/// by each test's explicit `.data`/`.initial` assertions). A failure means a View
/// entry Catch lacks or a shared key whose value disagrees. `StorageValue` has no
/// derived `PartialEq` (it carries [`OwnedValue`]), so equality is spelled out via
/// [`row_eq`]/[`compare_values`]. Both graphs lower the same `Ast`, so storage is
/// allocated in the same order and snapshot index `i` denotes the same operator's
/// store in both.
#[track_caller]
fn assert_view_storage_subset_of_catch(
    view: &[Vec<(Box<str>, StorageValue)>],
    catch: &[Vec<(Box<str>, StorageValue)>],
) {
    assert_eq!(
        view.len(),
        catch.len(),
        "build-twice: the Catch and View builds allocated a different number of operator \
         stores (a structural divergence between the two lowerings of the same Ast)",
    );
    for (i, (vstore, cstore)) in view.iter().zip(catch).enumerate() {
        for (k, v) in vstore {
            let agrees = cstore
                .iter()
                .find(|(ck, _)| ck == k)
                .is_some_and(|(_, cv)| storage_value_eq(v, cv));
            assert!(
                agrees,
                "build-twice storage divergence at store #{i}, key {k:?}: the View-sink build \
                 has a scratch entry the Catch-sink build lacks or disagrees on — a real \
                 sink-dependence bug (Catch only ever ADDS over-drain entries, so every View \
                 entry must be present + equal in Catch).\n  view entry: {v:?}\n  catch store: {cstore:#?}",
            );
        }
    }
}

/// Content equality for a [`StorageValue`] (no derived `PartialEq` — see its docs).
fn storage_value_eq(a: &StorageValue, b: &StorageValue) -> bool {
    use StorageValue::*;
    let bound_eq = |x: &Option<OwnedRow>, y: &Option<OwnedRow>| match (x, y) {
        (None, None) => true,
        (Some(r1), Some(r2)) => row_eq(r1, r2),
        _ => false,
    };
    match (a, b) {
        (
            Take {
                size: s1,
                bound: b1,
            },
            Take {
                size: s2,
                bound: b2,
            },
        ) => s1 == s2 && bound_eq(b1, b2),
        (Bound(r1), Bound(r2)) => row_eq(r1, r2),
        (Cap { size: s1, pks: p1 }, Cap { size: s2, pks: p2 }) => s1 == s2 && p1 == p2,
        (
            Reduce {
                count: c1,
                accs: a1,
            },
            Reduce {
                count: c2,
                accs: a2,
            },
        ) => c1 == c2 && a1 == a2,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change::OutEdge;
    use crate::value::{OwnedValue as V, RelId, SourceSchema};

    // issue(id, val); PK id. The "comments" relationship is query-local (slot 0).
    fn issues_schema() -> SourceSchema {
        SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
    }
    // comment(id, issue, val); PK id.
    fn comments_schema() -> SourceSchema {
        SourceSchema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)])
    }

    /// Wire issues ⟕ comments (rel "comments", slot 0) off two source conns; return the join.
    fn build_join(g: &mut Graph, srcs: &[NodeId]) -> NodeId {
        let pconn = g.connect(srcs[0], Some(vec![(0, true)]), None, vec![]);
        let cconn = g.connect(srcs[1], Some(vec![(0, true)]), None, vec![]);
        let join = g.add_join_slot(pconn, cconn, vec![0], vec![1], RelId(0));
        g.set_conn_output(
            pconn,
            OutEdge {
                node: join,
                port: Port::JoinParent,
            },
        );
        g.set_conn_output(
            cconn,
            OutEdge {
                node: join,
                port: Port::JoinChild,
            },
        );
        join
    }

    #[test]
    fn catch_fetch_materializes_the_nested_relationship_tree() {
        let caught = run_fetch_test(
            vec![
                SourceSpec::new(issues_schema(), vec![vec![V::Int(1), V::Int(100)]]),
                SourceSpec::new(
                    comments_schema(),
                    vec![vec![V::Int(10), V::Int(1), V::Int(500)]],
                ),
            ],
            build_join,
        );
        // issue 1 with one comment child under slot 0 ("comments").
        assert_eq!(
            caught,
            vec![cnode(
                vec![V::Int(1), V::Int(100)],
                vec![(
                    rel(0),
                    vec![cleaf(vec![V::Int(10), V::Int(1), V::Int(500)])]
                )],
            )]
        );
    }

    #[test]
    fn catch_records_child_change_on_child_add() {
        let r = run_push_test(
            vec![
                SourceSpec::new(issues_schema(), vec![vec![V::Int(1), V::Int(100)]]),
                SourceSpec::new(comments_schema(), vec![]),
            ],
            build_join,
            // add a comment for issue 1 → join emits a Child change (slot 0).
            vec![(
                1,
                SourceChange::Add(owned_row(vec![V::Int(10), V::Int(1), V::Int(500)])),
            )],
            false,
        );
        assert_caught_eq(
            &r.pushes,
            &[child(
                vec![V::Int(1), V::Int(100)],
                rel(0),
                add(cleaf(vec![V::Int(10), V::Int(1), V::Int(500)])),
            )],
        );
    }

    #[test]
    fn catch_records_flat_add_remove_through_a_plain_source() {
        // source -> catch directly (build returns the conn; set_sink_edge handles it).
        let r = run_push_test(
            vec![SourceSpec::new(comments_schema(), vec![])],
            |g, srcs| g.connect(srcs[0], Some(vec![(0, true)]), None, vec![]),
            vec![
                (
                    0,
                    SourceChange::Add(owned_row(vec![V::Int(7), V::Int(1), V::Int(0)])),
                ),
                (
                    0,
                    SourceChange::Remove(owned_row(vec![V::Int(7), V::Int(1), V::Int(0)])),
                ),
            ],
            false,
        );
        assert_caught_eq(
            &r.pushes,
            &[
                add(cleaf(vec![V::Int(7), V::Int(1), V::Int(0)])),
                remove(cleaf(vec![V::Int(7), V::Int(1), V::Int(0)])),
            ],
        );
    }

    #[test]
    fn snitch_logs_fetch_fetchcount_and_push() {
        let mut g = Graph::new();
        let src = g.add_source(
            comments_schema(),
            vec![owned_row(vec![V::Int(1), V::Int(1), V::Int(0)])],
        );
        let conn = g.connect(src, Some(vec![(0, true)]), None, vec![]);
        let snitch = g.add_snitch(conn, ":source(comment)");
        let catch = g.add_catch(snitch, false);
        g.set_conn_output(
            conn,
            OutEdge {
                node: snitch,
                port: Port::Single,
            },
        );
        g.set_output(snitch, catch);

        // a fetch through the snitch logs Fetch + (on drop) FetchCount.
        let _ = g.catch_fetch(catch, &FetchRequest::all());
        let log = g.snitch_log(snitch);
        assert!(matches!(log.first(), Some(SnitchMessage::Fetch { .. })));
        match log.last() {
            Some(SnitchMessage::FetchCount { count, .. }) => assert_eq!(*count, 1),
            other => panic!("expected FetchCount last, got {other:?}"),
        }

        // a push through the snitch logs Push.
        g.clear_all_snitch_logs();
        g.source_push(
            src,
            SourceChange::Add(owned_row(vec![V::Int(2), V::Int(1), V::Int(0)])),
        );
        let log = g.snitch_log(snitch);
        assert_eq!(
            log,
            vec![SnitchMessage::Push {
                name: ":source(comment)".to_string(),
                change: ChangeRecord::Add(owned_row(vec![V::Int(2), V::Int(1), V::Int(0)])),
            }]
        );
    }

    #[test]
    fn fingerprint_is_stable_and_order_insensitive_at_top_level() {
        let a = vec![cleaf(vec![V::Int(1)]), cleaf(vec![V::Int(2)])];
        let b = vec![cleaf(vec![V::Int(2)]), cleaf(vec![V::Int(1)])];
        // top-level order does not affect the fingerprint (§3.5 sorts top-level).
        assert_eq!(fingerprint(&a), fingerprint(&b));
        // a content change does.
        let c = vec![cleaf(vec![V::Int(1)]), cleaf(vec![V::Int(3)])];
        assert_ne!(fingerprint(&a), fingerprint(&c));
    }

    #[test]
    fn fingerprint_distinguishes_relationship_shape() {
        let with_child = vec![cnode(
            vec![V::Int(1)],
            vec![(rel(0), vec![cleaf(vec![V::Int(9)])])],
        )];
        let without = vec![cnode(vec![V::Int(1)], vec![(rel(0), vec![])])];
        assert_ne!(fingerprint(&with_child), fingerprint(&without));
    }

    #[test]
    fn diff_predicate_catches_a_mismatch() {
        let actual = vec![add(cleaf(vec![V::Int(1)]))];
        let expected = vec![add(cleaf(vec![V::Int(2)]))];
        assert_ne!(actual, expected);
        // null == null under the content comparator.
        assert_eq!(
            vec![add(cleaf(vec![V::Null]))],
            vec![add(cleaf(vec![V::Null]))]
        );
    }
}
