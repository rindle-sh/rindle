//! Spec `10` — **operator scratch state**: the small, sorted, string-keyed
//! side-table a *stateful* operator (`Take`, `Cap`) keeps bookkeeping in across
//! `fetch`/`push` calls. Ports `zql/src/ivm/operator.ts:132` `Storage` and
//! `zql/src/ivm/memory-storage.ts`.
//!
//! It is **not** a row store, **not** the source index ([`crate::btree`]), and
//! **not** the view tree. It is the IVM analogue of a per-operator hash-map,
//! hoisted behind a trait so the server can later spill it to SQLite while the
//! client keeps it in RAM (spec `10` §1.1).
//!
//! ## What's here vs. the SQLite backend
//!
//! This module is the backend-agnostic core: the [`Storage`] trait, the tight
//! [`StorageValue`] enum, [`MemoryStorage`] for the client/test path, the
//! [`StorageProvider`] seam, and the [`StorageFactory`] the builder calls. The
//! server spill-to-SQLite backend (`DatabaseStorage`/`OpStorage` over rusqlite, spec
//! `10` §4.4) lives in the `rindle-sqlite` crate, plugged in via
//! `StorageFactory::custom(Rc::new(database_storage))` — so the core crate carries no
//! `rusqlite` dependency.
//!
//! ## Backing structure — `BTreeMap` for now (a measured decision)
//!
//! [`MemoryStorage`] is backed by a `std::collections::BTreeMap`, kept **private**
//! behind the [`Storage`] trait. This is the spec `10` §4.3 choice. The crate's
//! COW B+tree ([`crate::btree`]) was considered to avoid `BTreeMap`'s wasm
//! codegen, but it is an `OwnedRow` *set* keyed by a `Sort`, not a key→value map —
//! adapting it costs an encode/decode adapter for a bundle win that is currently
//! *unmeasured*. Since the backing is invisible behind the trait, we ship the
//! simplest correct thing now and revisit only if `twiggy` (M9) shows the
//! `BTreeMap` delta actually matters. Two properties make `BTreeMap` a good fit:
//! it stores [`StorageValue`] directly (so `get` of a `Take{bound}` is an `Arc`
//! refcount bump, not a row copy — spec `10` §3.2/§8.1), and it gives the
//! sorted-key invariant + a native `range` for the prefix [`Storage::scan`].

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::rc::Rc;

use crate::error::RindleError;
use crate::value::OwnedRow;

// ---------------------------------------------------------------------------
// The value model — a tight enum, NOT serde_json::Value (spec 10 §4.2)
// ---------------------------------------------------------------------------

/// The value stored in operator scratch state. A **tight enum**, not generic
/// JSON: the only writers are `Take` and `Cap` (spec `10` §1.3), and their
/// payloads are known. This (a) keeps `serde_json` *out* of the wasm client,
/// (b) makes every value a flat, cheaply-clonable struct instead of a heap JSON
/// tree, and (c) gives operators type-safe state instead of the JS
/// `storage as TakeStorage` casts (`take.ts:78`).
///
/// **No `PartialEq`.** It carries [`OwnedRow`]s ([`crate::value::OwnedValue`]
/// deliberately has no `PartialEq` — you must choose `compare_values` vs
/// `values_equal`), so comparison is spelled out where needed (tests compare via
/// `compare_values`, mirroring `graph::CollectedChange`).
///
/// Borrowed and owned forms coincide for this enum (no `&str` fields — `bound` is
/// already an `OwnedRow`, `pks` are `Box<str>`), so [`OwnedStorageValue`] is just
/// an alias; it leaves room for a future borrowed `set` form (spec `10` §4.2).
#[derive(Clone, Debug)]
pub enum StorageValue {
    /// `take.ts` `TakeState`: a count + an optional boundary row. `bound` is an
    /// `OwnedRow` because it outlives the cursor that produced it (foundations
    /// §3.3 — operator-buffered state).
    Take { size: u32, bound: Option<OwnedRow> },
    /// `take.ts` `MAX_BOUND_KEY` slot: a bare boundary row.
    Bound(OwnedRow),
    /// `cap.ts` `CapState`: a count + the membership pk-set (each pk
    /// pre-serialized to a string exactly as JS `serializePK`, `cap.ts:315`).
    Cap { size: u32, pks: Vec<Box<str>> },
    /// `op/reduce.rs` accumulator (`REDUCE-DESIGN.md` §5). `count` is the running row
    /// count backing `count(*)` (birth/death is driven by it, `NULL`s included). `accs`
    /// holds one [`ReduceAcc`] per `Sum`/`Avg` aggregate in output-column order (empty for
    /// a plain `count`), so a `Remove` never re-reads the inputs.
    Reduce { count: i64, accs: Vec<ReduceAcc> },
}

/// One per-column running accumulator for a `Sum`/`Avg` aggregate inside a
/// [`StorageValue::Reduce`] (`REDUCE-DESIGN.md` §5). The integer and float sums are kept
/// **apart** so the emitted `sum` matches SQLite's typing — integer iff every summed
/// value was an integer, real once any float contributes — and stays fully invertible
/// (a later `Remove` of a float value demotes the result back to an integer).
///
/// - `int_sum` / `float_sum` — running Σ of the column's integer- and float-typed
///   non-`NULL` values. `int_sum` is **i128** (design 226 §5.3): the accumulator
///   folds deltas in arrival order (Removes included), so a transient past
///   `i64::MAX` that SQLite's scan order would never see must not error or wrap —
///   and `-1 × i64::MIN` must not overflow on a Remove. Only the **emitted** total
///   is bounded: `sum` raises a typed error at emit when the set total leaves the
///   i64 range (`op/reduce.rs`'s `sum_value`).
/// - `non_null` — count of non-`NULL` values; this is `avg`'s denominator (SQL
///   `count(col)`, **not** the row `count`), and `avg` emits `NULL` when it is `0`.
/// - `float_count` — how many contributing values were float; `0` ⇒ `sum` emits `Int`,
///   else `Float`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReduceAcc {
    pub int_sum: i128,
    pub float_sum: f64,
    pub non_null: i64,
    pub float_count: i64,
}

/// Owned counterpart returned by [`Storage::get`]/[`Storage::scan`]. Identical to
/// [`StorageValue`] (the enum has no borrowed fields); aliased for clarity at the
/// trait surface and to leave room for a future borrowed `set` form (spec `10`
/// §4.2 / Appendix A).
pub type OwnedStorageValue = StorageValue;

// ---------------------------------------------------------------------------
// The trait (spec 10 §4.1 / Appendix A)
// ---------------------------------------------------------------------------

/// Per-operator scratch state: a sorted string-keyed map with a prefix range
/// scan. Ports `zql/src/ivm/operator.ts:132` `Storage`.
///
/// **Object-safe by design.** The builder (`08`) stores the chosen backend
/// behind `Box<dyn Storage>` so the same operator code (`Take`, `Cap`) links
/// against either backend with no per-backend monomorphization. Operators are
/// already dyn-dispatched at the graph boundary (foundations §6.2), so the extra
/// vtable here is free. The boxed, lending-free `scan` return keeps it object-safe.
///
/// **`&self`, not `&mut self`** (foundations §6: no `&mut` on operator-held state
/// during a shared-borrowed reentrant push). Backends use interior mutability.
/// Storage ops are synchronous and non-reentrant — they never call back into the
/// graph — so none of the "no borrow across a vend" hazards apply *within* the
/// store (spec `10` §5, E12).
///
/// **The trait is intentionally infallible** (spec `10` §4.1, E14). A backend that
/// can fail (the SQLite `OpStorage`) reports errors **out of band** — it parks a
/// [`RindleError`] on the graph's runtime-error sink and returns a safe sentinel
/// (`None` / empty / no-op); the graph drains it via `take_runtime_error` at the
/// mutation boundary (WS02.3). The in-RAM [`MemoryStorage`] never fails.
pub trait Storage {
    /// Insert or **overwrite** `key`'s value (spec `10` §3.1, E3).
    fn set(&self, key: &str, value: StorageValue);

    /// Owned lookup. `None` if absent (E1). The JS `get(key, def)` default is a
    /// caller concern — use `get(k).unwrap_or(default)` (spec `10` §6 deviation
    /// D1); we do not bake it into the signature. The returned value is **owned**
    /// (never a borrow into the store), so the operator may hold/mutate it past a
    /// later `set` regardless of backend (spec `10` §3.2 inv.3, E11).
    fn get(&self, key: &str) -> Option<OwnedStorageValue>;

    /// Remove `key`. No-op if absent (E4).
    fn del(&self, key: &str);

    /// Ascending prefix scan. Yields owned `(key, value)` pairs in **byte-ascending
    /// key order**, starting at the first key `>= prefix` and **stopping at the
    /// first key not starting with `prefix`** (spec `10` §3.2). `prefix == ""`
    /// scans the whole keyspace in order (E5/E7).
    ///
    /// Returns a boxed iterator of OWNED pairs (the future SQLite backend cannot
    /// lend across a `step()`; we keep both backends signature-identical — spec
    /// `10` §4.1, §6).
    fn scan<'s>(
        &'s self,
        prefix: &str,
    ) -> Box<dyn Iterator<Item = (Box<str>, OwnedStorageValue)> + 's>;

    /// Drop **every** key in this store, leaving it an empty namespace. Called by
    /// [`Graph::destroy_pipeline`](crate::graph::Graph::destroy_pipeline) to reclaim a
    /// torn-down operator's scratch state while keeping the slot reusable (the slot —
    /// and, for a SQLite-backed store, its `op_id` namespace — is recycled by the next
    /// `alloc_storage`, so we must *clear contents*, not drop the store object).
    ///
    /// Default: collect the full keyspace then `del` each (the keys are collected
    /// first so we are not iterating the store while mutating it). A backend with a
    /// cheaper bulk delete (e.g. SQLite `DELETE … WHERE op_id = ?`) should override.
    fn clear(&self) {
        let keys: Vec<Box<str>> = self.scan("").map(|(k, _)| k).collect();
        for k in keys {
            self.del(&k);
        }
    }
}

// ---------------------------------------------------------------------------
// MemoryStorage (client / tests) — spec 10 §4.3, §5.1
// ---------------------------------------------------------------------------

/// In-RAM [`Storage`]. A `BTreeMap<Box<str>, StorageValue>` (kept private — see
/// the module docs) gives the sorted-key invariant, O(log n) point ops, and a
/// native `range` for the prefix scan. `RefCell` provides the interior mutability
/// the `&self` trait methods need (single-threaded per pipeline — foundations
/// §1.3, so no `Sync` required).
pub struct MemoryStorage {
    data: RefCell<BTreeMap<Box<str>, StorageValue>>,
}

impl MemoryStorage {
    pub fn new() -> MemoryStorage {
        MemoryStorage {
            data: RefCell::new(BTreeMap::new()),
        }
    }

    /// Test/debug snapshot (ports `cloneData`, `memory-storage.ts:47`). Returns an
    /// owned copy of the whole map; the `BTreeMap` type is intentionally **not**
    /// part of the [`Storage`] trait surface, so this leaks only from the concrete
    /// type, keeping the backing swappable (module docs).
    pub fn clone_data(&self) -> BTreeMap<Box<str>, StorageValue> {
        self.data.borrow().clone()
    }
}

impl Default for MemoryStorage {
    fn default() -> MemoryStorage {
        MemoryStorage::new()
    }
}

impl Storage for MemoryStorage {
    fn set(&self, key: &str, value: StorageValue) {
        // BTreeMap::insert overwrites an equal key (E3).
        self.data.borrow_mut().insert(key.into(), value);
    }

    fn get(&self, key: &str) -> Option<StorageValue> {
        // `.cloned()` is the owned-copy point (E11). For `Take{bound}`/`Bound` the
        // "copy" is an `Arc` refcount bump on the `OwnedRow` (spec 10 §3.2).
        self.data.borrow().get(key).cloned()
    }

    fn del(&self, key: &str) {
        // remove returns None for an absent key — no-op, no error (E4).
        self.data.borrow_mut().remove(key);
    }

    fn scan<'s>(&'s self, prefix: &str) -> Box<dyn Iterator<Item = (Box<str>, StorageValue)> + 's> {
        // Collect into a Vec so the RefCell borrow is dropped before the iterator
        // is handed out: another operator-state op could `set` while the operator
        // drains the scan, and we must not pin a borrow across that. Scratch-state
        // scans are tiny (a handful of partition keys), so the copy is negligible;
        // this mirrors the JS generator over a not-mutated-mid-scan structure
        // (spec 10 §5.1 borrow discipline, §8.1).
        let data = self.data.borrow();
        // `BTreeMap<Box<str>, _>::range` over `Q = str` (since `Box<str>:
        // Borrow<str>`): the bound is `&str`, no alloc. `Included(prefix)` seeks
        // `>= prefix` inclusively so an exact prefix key is the first yielded
        // (E8); `prefix == ""` matches every key (E5). `take_while(starts_with)`
        // is the exact port of `memory-storage.ts:40` (`if (!key.startsWith(
        // prefix)) return;`) and bounds the scan to O(matches) (E6).
        let out: Vec<(Box<str>, StorageValue)> = data
            .range::<str, _>((Bound::Included(prefix), Bound::Unbounded))
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Box::new(out.into_iter())
    }
}

// ---------------------------------------------------------------------------
// Builder-facing factory (spec 10 §4.5)
// ---------------------------------------------------------------------------

/// A backend's per-operator store factory, erased behind a trait object so the core
/// graph's [`StorageFactory`] can vend SQLite-backed operator storage (the
/// `rindle-sqlite` `DatabaseStorage`) without the core crate naming a `rusqlite`
/// type. The in-memory path bypasses this (it builds [`MemoryStorage`] directly).
pub trait StorageProvider {
    /// Allocate one isolated operator keyspace, parking any backend errors into
    /// `error_sink` (the graph's `runtime_error`, WS02.3) so they surface via
    /// `take_runtime_error` instead of aborting.
    fn create_storage_with_sink(
        &self,
        error_sink: Rc<RefCell<Option<RindleError>>>,
    ) -> Box<dyn Storage>;
}

pub struct StorageFactory {
    backend: StorageFactoryBackend,
}

enum StorageFactoryBackend {
    Memory,
    Custom(Rc<dyn StorageProvider>),
}

impl StorageFactory {
    pub fn memory() -> StorageFactory {
        StorageFactory {
            backend: StorageFactoryBackend::Memory,
        }
    }

    /// Build a factory from a custom [`StorageProvider`] (e.g. `rindle-sqlite`'s
    /// `DatabaseStorage`). Each `alloc_storage` vends one namespaced store.
    pub fn custom(provider: Rc<dyn StorageProvider>) -> StorageFactory {
        StorageFactory {
            backend: StorageFactoryBackend::Custom(provider),
        }
    }

    pub fn create_storage(&self) -> Box<dyn Storage> {
        self.create_storage_with_sink(Rc::default())
    }

    /// Like [`create_storage`](Self::create_storage) but threads the graph's
    /// `runtime_error` sink into the backend so operator-storage failures
    /// surface as `RindleError` via `take_runtime_error` instead of aborting (WS02.3).
    /// The memory backend ignores the sink (it is infallible).
    pub(crate) fn create_storage_with_sink(
        &self,
        error_sink: Rc<RefCell<Option<RindleError>>>,
    ) -> Box<dyn Storage> {
        match &self.backend {
            StorageFactoryBackend::Memory => {
                let _ = &error_sink;
                Box::new(MemoryStorage::new())
            }
            StorageFactoryBackend::Custom(provider) => {
                provider.create_storage_with_sink(error_sink)
            }
        }
    }
}

impl Default for StorageFactory {
    fn default() -> StorageFactory {
        StorageFactory::memory()
    }
}

/// What the builder (`08`) calls to give a stateful operator its own store. On
/// the client/test path a *fresh object is the namespace* (spec `10` §3.3, §4.5):
/// each `Take`/`Cap` gets a disjoint keyspace because it gets a distinct
/// `MemoryStorage`. The JS `name` argument is not needed by the memory backend
/// (uniqueness is object identity — spec `10` §6 deviation D2), so it is not taken
/// here; the builder keeps `name` at its own boundary for debug/logging.
///
/// The server path should inject a [`StorageFactory`] built from
/// `DatabaseStorage`; this compatibility helper stays memory-backed.
pub fn create_storage() -> Box<dyn Storage> {
    Box::new(MemoryStorage::new())
}
