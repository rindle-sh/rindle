//! The materialization sink — the production `ArrayView` (`09`).
//!
//! Ports `packages/zql/src/ivm/array-view.ts` (the sink shell) **plus**
//! `packages/zql/src/ivm/view-apply-change.ts` (the 916-line immutable tree
//! differ `applyChange`). This module holds:
//!
//! - the **`Arc`-shared entry tree** ([`Entry`]/[`EntryList`]) with
//!   per-entry [`rc`](Entry::rc) refcounts and a transaction generation stamp
//!   ([`Entry::created`]);
//! - the immutable, **reference-stable** [`apply_change`] (add/remove/child/edit;
//!   plural at every level);
//! - the transaction-scoped **copy-on-write** machinery ([`Mutate`]/[`TxnDirty`]/
//!   [`TxnGen`]) — the Rust analogue of the JS `#txnDirty` `WeakSet`, replaced by
//!   a per-object generation stamp (`09` §4.8 approach A);
//! - the [`View`] shell (`Arc` root, [`Schema`], listeners, [`ResultType`],
//!   flush) — its `hydrate`/`push` graph-touching halves live in `graph.rs`.
//!
//! ## Reference stability (the headline property, `09` §1.1)
//!
//! `apply_change` is *immutable*: it produces a new root that **preserves the
//! `Arc` pointer identity of every subtree it did not touch**, so a UI framework
//! can skip unchanged subtrees via a cheap [`Arc::ptr_eq`](std::sync::Arc::ptr_eq). Off-spine siblings are
//! shared (an `Arc` clone is a refcount bump, not a deep copy); only the changed
//! ancestor path is rebuilt.
//!
//! ## The recursion shape (deviation from the JS, called out)
//!
//! The JS recursion is `applyChangeInternal(parentEntry, …) -> Entry` (returns the
//! new-or-same object). Rust cannot mutate an `Arc` in place through a shared
//! `&Arc`, so the recursion here threads **`&mut Arc<Entry>`** and returns a
//! `bool changed` (the [`Arc::ptr_eq`](std::sync::Arc::ptr_eq) short-circuit of the JS `newExisting ===
//! existing`). The COW decision is driven by the generation stamp, not
//! `Arc::strong_count` (`09` §4.8 rejects the strong-count approach): a *committed*
//! object (`created < dirty.gen`) is **always cloned** on first touch (never
//! `Arc::make_mut`, which would mutate a count-1 deep committed node in place and
//! corrupt a listener's snapshot); an *owned* object (`created == dirty.gen`, or
//! [`Mutate::InPlace`]) is mutated via [`Arc::make_mut`](std::sync::Arc::make_mut) (in place when uniquely
//! held — which the push/hydrate take-out guarantees for the spine).

use std::cell::{Cell, RefCell};
use std::sync::{Arc, OnceLock};

use crate::change::Node;
use crate::value::{compare_rows, ColId, OwnedRow, RelId, Schema, Sort, Value};
use std::cmp::Ordering;

/// The synthetic root reaches the top-level result (`root[""]`) through its one
/// relationship slot. Mirrors the JS `''` relationship (`array-view.ts:88`).
pub const REL_ROOT: RelId = RelId(0);

// ---------------------------------------------------------------------------
// Transaction / copy-on-write machinery (`09` §4.8)
// ---------------------------------------------------------------------------

/// Per-transaction "created this txn" marker — the Rust analogue of the JS
/// `#txnDirty` `WeakSet`. A monotonically increasing stamp; bumped on `flush` so
/// all prior marks go stale "for free" (no `WeakSet::clear`).
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct TxnGen(pub u64);

/// The COW tracker threaded (by `&`) through one `apply_change` call. `owns(x) ⇔
/// x.created == gen`.
#[derive(Clone, Copy)]
pub struct TxnDirty {
    pub gen: TxnGen,
}

/// Update strategy passed down the recursion. Ports the JS `mutate` argument.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mutate {
    /// Fully immutable: always path-copy (JS `false`). Used by `NO_MUTATE` tests.
    Immutable,
    /// Mutate everything in place: only safe when the tree is unobserved
    /// (hydration). (JS `true`.)
    InPlace,
    /// Transaction-scoped copy-on-write (JS `WeakSet`). Copy on first touch of a
    /// committed object, in place after.
    Cow,
}

/// `mutate || owns(x)` (`view-apply-change.ts:588` etc): may we mutate the object
/// whose creation stamp is `created` in place?
#[inline]
fn can_mut(created: TxnGen, mode: Mutate, dirty: &TxnDirty) -> bool {
    match mode {
        Mutate::Immutable => false,
        Mutate::InPlace => true,
        Mutate::Cow => created == dirty.gen,
    }
}

// ---------------------------------------------------------------------------
// The entry tree (`09` §4.2)
// ---------------------------------------------------------------------------

/// The stable id: the JSON-stringified PK (`makeID`, `view-apply-change.ts:847`).
/// Present iff `with_ids`. `ArrayView` always passes `with_ids=false`, so this is
/// dead for it; kept so a future `SolidView` reuses the same differ (`09` §1.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryId {
    /// `JSON.stringify(row[pk0])` — the single-column PK fast path.
    Scalar(Box<str>),
    /// `JSON.stringify([row[pk0], …])` — compound PK.
    Compound(Box<str>),
}

/// One materialized node in the view tree (ports `MetaEntry`). `Arc`-shared so
/// unchanged subtrees keep pointer identity across an immutable [`apply_change`].
#[derive(Clone, Debug)]
pub struct Entry {
    /// The row's columns, index-addressed (never a `HashMap<String,_>`).
    pub row: OwnedRow,
    /// How many query paths reach this row within its containing relationship.
    /// Add increments, remove decrements; physical removal at `rc == 0`.
    pub rc: u32,
    /// "Created/cloned this transaction" stamp (`09` §4.8-A). `owns(e) ⇔
    /// e.created == dirty.gen`. Bumped-stale on every flush.
    pub created: TxnGen,
    /// Stable identity (present iff `with_ids`). Mirrors `idSymbol`. Boxed: the
    /// field is dead for `ArrayView` (`with_ids=false` everywhere today), so it
    /// costs 8 bytes in-line instead of 24; a future `SolidView` pays one alloc
    /// per entry for it, in the same breath as its id string allocation.
    pub id: Option<Box<EntryId>>,
    /// Child relationships, **index-addressed by [`RelId`]** (slot = position),
    /// in the same order as `Schema::relationships`. Each slot is a sorted list:
    /// the view is plural at every level (singular `.one()` is applied at the
    /// presentation boundary, not in the materialized tree).
    pub rels: Box<[EntryList]>,
}

/// A sorted list of child entries (ports `MetaEntryList`). Carries its own
/// `created` stamp so `owns(list)` works under COW (the JS tracks arrays in
/// `#txnDirty` exactly like entries). The LIST has stable identity when
/// unchanged, independent of its elements.
#[derive(Clone, Debug)]
pub struct EntryListInner {
    pub created: TxnGen,
    pub items: Vec<Arc<Entry>>,
}

/// The reference-counted, copy-on-write child list.
pub type EntryList = Arc<EntryListInner>;

/// The top-level result the consumer sees (`root[""]`): the sorted list of root
/// entries. An `Arc`-shared [`EntryList`], so a consumer can `Arc::ptr_eq` the
/// snapshot to detect an unchanged top level.
///
/// The view is **plural at every level** — which relationships appear in the view
/// is carried by the hierarchical [`Schema`] (`rel_child(slot).is_some()` ⇔
/// in-view); a join-only / gating slot has no child schema and is excluded.
pub type ViewData = EntryList;

// ---------------------------------------------------------------------------
// ViewChange (`09` §4.5) — the View-local change shape
// ---------------------------------------------------------------------------

/// View-local change (ports `ViewChange`, `view-apply-change.ts:61-92`). `Add`/
/// `Remove` keep the full [`Node`] (their relationship thunks are consumed on a
/// plural/singular insert, `09` §4.5);
/// `Child`/`Edit` are row-only (their relationships are never consumed).
pub enum ViewChange<'g> {
    Add {
        node: Node<'g>,
    },
    Remove {
        node: Node<'g>,
    },
    Child {
        row: OwnedRow,
        rel: RelId,
        change: Box<ViewChange<'g>>,
    },
    Edit {
        row: OwnedRow,
        old: OwnedRow,
    },
}

impl<'g> ViewChange<'g> {
    /// Convert a dataflow [`Change`](crate::change::Change) into a `ViewChange`
    /// (ports `changeToViewChange`, `array-view.ts:15-37`). Strips relationships
    /// from the `Child`/`Edit` nodes the View never consumes; recurses for nested
    /// child changes.
    pub fn from_change(c: crate::change::Change<'g>) -> ViewChange<'g> {
        use crate::change::Change;
        match c {
            Change::Add(node) => ViewChange::Add { node },
            Change::Remove(node) => ViewChange::Remove { node },
            Change::Edit { node, old } => ViewChange::Edit {
                row: node.row,
                old: old.row,
            },
            Change::Child { node, rel, child } => ViewChange::Child {
                row: node.row,
                rel,
                change: Box::new(ViewChange::from_change(*child)),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// COW accessors: in-place if owned, else clone-and-stamp (`09` §6.3)
// ---------------------------------------------------------------------------

/// Get `&mut Entry`, copy-on-writing as needed. A committed object (`!can_mut`)
/// is **always cloned** (never `make_mut`) so a count-1 deep committed node is
/// never mutated in place (which would corrupt a listener's snapshot).
#[inline]
fn entry_cow<'a>(arc: &'a mut Arc<Entry>, mode: Mutate, dirty: &TxnDirty) -> &'a mut Entry {
    if can_mut(arc.created, mode, dirty) {
        let e = Arc::make_mut(arc); // in place (count 1) or clones a transient alias
        e.created = dirty.gen;
        e
    } else {
        let mut new = (**arc).clone();
        new.created = dirty.gen;
        *arc = Arc::new(new);
        Arc::get_mut(arc).expect("fresh Arc is uniquely held")
    }
}

/// Get `&mut EntryListInner`, copy-on-writing as needed (mirror of [`entry_cow`]).
#[inline]
fn list_cow<'a>(arc: &'a mut EntryList, mode: Mutate, dirty: &TxnDirty) -> &'a mut EntryListInner {
    if can_mut(arc.created, mode, dirty) {
        let l = Arc::make_mut(arc);
        l.created = dirty.gen;
        l
    } else {
        let mut new = (**arc).clone();
        new.created = dirty.gen;
        *arc = Arc::new(new);
        Arc::get_mut(arc).expect("fresh Arc is uniquely held")
    }
}

/// `&mut` the (plural) list slot of an entry.
#[inline]
fn list_slot(e: &mut Entry, rel: RelId) -> &mut EntryList {
    &mut e.rels[rel.ix()]
}

#[inline]
fn binary_search(items: &[Arc<Entry>], row: &OwnedRow, sort: &Sort) -> Result<usize, usize> {
    items.binary_search_by(|e| compare_rows(sort, &e.row, row))
}

// ---------------------------------------------------------------------------
// Entry construction
// ---------------------------------------------------------------------------

/// Encode a cell as `JSON.stringify` would (for [`make_id`]).
fn json_value(v: Value<'_>) -> String {
    match v {
        // A view row's PK is always present; mirror JS `JSON.stringify(undefined)`-in-array
        // (→ `null`) for totality should an `Absent` ever reach here.
        Value::Absent => "null".to_string(),
        Value::Null => "null".to_string(),
        Value::Bool(b) => {
            if b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            // JSON has no NaN/Inf; JS JSON.stringify emits `null` for them.
            if f.is_finite() {
                let mut s = f.to_string();
                if s == "-0" {
                    s = "0".to_string();
                }
                s
            } else {
                "null".to_string()
            }
        }
        Value::Str(b) | Value::Json(b) => {
            // Row text is UTF-8-validated at construction.
            let s = String::from_utf8_lossy(b);
            let mut out = String::with_capacity(s.len() + 2);
            out.push('"');
            for ch in s.chars() {
                match ch {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    c => out.push(c),
                }
            }
            out.push('"');
            out
        }
    }
}

/// `makeID` (`view-apply-change.ts:847`): the JSON-stringified PK.
fn make_id(row: &OwnedRow, pk: &[ColId]) -> EntryId {
    if pk.len() == 1 {
        EntryId::Scalar(json_value(row.col(pk[0])).into_boxed_str())
    } else {
        let parts: Vec<String> = pk.iter().map(|&c| json_value(row.col(c))).collect();
        EntryId::Compound(format!("[{}]", parts.join(",")).into_boxed_str())
    }
}

/// The one shared empty [`EntryList`]: every empty relationship slot is a
/// refcount bump on this, not a private 48-byte heap block. Safe to share:
/// its `created` stamp is permanently stale ("committed"), and both COW
/// branches ([`list_cow`]) physically clone a shared `Arc` before mutating —
/// the `OnceLock` keeps the count ≥ 2 forever, so no path can mutate it.
fn empty_entry_list() -> EntryList {
    static EMPTY: OnceLock<EntryList> = OnceLock::new();
    EMPTY
        .get_or_init(|| {
            Arc::new(EntryListInner {
                created: TxnGen::default(),
                items: Vec::new(),
            })
        })
        .clone()
}

/// Build a fresh entry with the given rc, all relationship slots empty (plural,
/// the shared empty list). Stamped `created = dirty.gen` (owned this txn).
/// Mirrors `makeNewMetaEntry` (`view-apply-change.ts:830`) + the empty-slot init.
fn make_new_entry(
    row: OwnedRow,
    schema: &Schema,
    with_ids: bool,
    rc: u32,
    dirty: &TxnDirty,
) -> Entry {
    let id = if with_ids {
        Some(Box::new(make_id(&row, &schema.primary_key)))
    } else {
        None
    };
    let rels = (0..schema.relationships.len())
        .map(|_| empty_entry_list())
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Entry {
        row,
        rc,
        created: dirty.gen,
        id,
        rels,
    }
}

/// The synthetic root entry: one relationship slot (`""`) holding the top-level
/// result list. Mirrors `#root = {'': []}` (`array-view.ts:88`).
fn synthetic_root() -> Entry {
    let slot = Arc::new(EntryListInner {
        created: TxnGen::default(),
        items: Vec::new(),
    });
    Entry {
        row: OwnedRow::empty(),
        rc: 0,
        created: TxnGen::default(),
        id: None,
        rels: Box::new([slot]),
    }
}

/// Read the [`ViewData`] out of a root entry (the `data` getter, `array-view.ts:111`).
fn view_data(root: &Arc<Entry>) -> ViewData {
    root.rels[REL_ROOT.ix()].clone()
}

// ---------------------------------------------------------------------------
// The differ (`09` §5.4-§5.10)
// ---------------------------------------------------------------------------

/// Immutable view update. Mutates `*root` to fold in `change` (`*root` becomes the
/// new — possibly same — root `Arc`); unchanged subtrees keep `Arc` identity.
/// Ports `applyChange` (`view-apply-change.ts:184`). `rel` is the relationship by
/// which `*root` reaches the level the change targets ([`REL_ROOT`] at the top).
#[allow(clippy::too_many_arguments)]
pub fn apply_change(
    root: &mut Arc<Entry>,
    change: &ViewChange<'_>,
    schema: &Schema,
    rel: RelId,
    with_ids: bool,
    mode: Mutate,
    dirty: &TxnDirty,
) {
    apply(root, change, schema, rel, with_ids, mode, dirty);
}

/// The recursive core (`applyChangeInternal`, `view-apply-change.ts:212`). Returns
/// `true` iff the relationship slot's content changed (the [`Arc::ptr_eq`]
/// short-circuit). On the owned/`InPlace` path the parent `Arc` keeps its pointer
/// (uniquely held → in place) even when this returns `false`.
#[allow(clippy::too_many_arguments)]
fn apply(
    parent: &mut Arc<Entry>,
    change: &ViewChange<'_>,
    schema: &Schema,
    rel: RelId,
    with_ids: bool,
    mode: Mutate,
    dirty: &TxnDirty,
) -> bool {
    match change {
        ViewChange::Add { node } => apply_add(parent, node, schema, rel, with_ids, mode, dirty),
        ViewChange::Remove { node } => apply_remove(parent, &node.row, schema, rel, mode, dirty),
        ViewChange::Child {
            row,
            rel: child_rel,
            change,
        } => apply_child(
            parent, row, *child_rel, change, schema, rel, with_ids, mode, dirty,
        ),
        ViewChange::Edit { row, old } => {
            apply_edit_change(parent, row, old, schema, rel, with_ids, mode, dirty)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_add(
    parent: &mut Arc<Entry>,
    node: &Node<'_>,
    schema: &Schema,
    rel: RelId,
    with_ids: bool,
    mode: Mutate,
    dirty: &TxnDirty,
) -> bool {
    let pos = binary_search(&parent.rels[rel.ix()].items, &node.row, &schema.sort);
    match pos {
        Ok(p) => {
            let pe = entry_cow(parent, mode, dirty);
            let list = list_cow(list_slot(pe, rel), mode, dirty);
            entry_cow(&mut list.items[p], mode, dirty).rc += 1;
        }
        Err(ins) => {
            let mut e = make_new_entry(node.row.clone(), schema, with_ids, 1, dirty);
            init_rels_for_new_entry(&mut e, node, schema, with_ids, dirty);
            let pe = entry_cow(parent, mode, dirty);
            let list = list_cow(list_slot(pe, rel), mode, dirty);
            list.items.insert(ins, Arc::new(e));
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
fn apply_remove(
    parent: &mut Arc<Entry>,
    row: &OwnedRow,
    schema: &Schema,
    rel: RelId,
    mode: Mutate,
    dirty: &TxnDirty,
) -> bool {
    let pos = match binary_search(&parent.rels[rel.ix()].items, row, &schema.sort) {
        Ok(p) => p,
        Err(_) => panic!("node does not exist"),
    };
    let rc = parent.rels[rel.ix()].items[pos].rc;
    let pe = entry_cow(parent, mode, dirty);
    let list = list_cow(list_slot(pe, rel), mode, dirty);
    if rc == 1 {
        list.items.remove(pos);
    } else {
        entry_cow(&mut list.items[pos], mode, dirty).rc -= 1;
    }
    true
}

#[allow(clippy::too_many_arguments)]
fn apply_child(
    parent: &mut Arc<Entry>,
    row: &OwnedRow,
    child_rel: RelId,
    change: &ViewChange<'_>,
    schema: &Schema,
    rel: RelId,
    with_ids: bool,
    mode: Mutate,
    dirty: &TxnDirty,
) -> bool {
    // The in-view gate — the JS `format.relationships[relationship] === undefined`
    // check (`view-apply-change.ts:360`) — is now carried by the hierarchical schema:
    // a slot with no child schema (`rel_child` is `None`) is declared join-only / out
    // of the view shape, so it is ignored with the parent unchanged *before* anything
    // else. This matters for an out-of-view relationship that still delivers `Child`
    // changes: a non-flipped `Exists` gate forwards a `Child` on its own (gating,
    // out-of-view) slot when membership does not flip (`exists.ts` `#pushWithFilter`).
    // Checking `rel_child` first short-circuits before the (absent) child lookup —
    // matching the JS, which never reaches the lookup because the format check returns
    // early. (The builder makes in-view ⇔ child-schema-present by construction:
    // `RelDef::related` for a `related` alias, `RelDef::new` for a gating slot.)
    let child_schema = match schema.rel_child(child_rel) {
        Some(s) => s,
        None => return false, // relationship not in view → parent unchanged
    };

    let pos = match binary_search(&parent.rels[rel.ix()].items, row, &schema.sort) {
        Ok(p) => p,
        Err(_) => panic!("node does not exist"),
    };
    // Gate on the LIST's mutability, NOT the parent's. The danger case is an
    // *owned* parent whose target list is still *committed* (it reached the
    // parent un-touched while a sibling slot was COW'd earlier this txn): the
    // in-place branch would `list_cow`-clone that committed list **before**
    // recursing, losing its `Arc` identity even if the recursion is a no-op.
    // When the list is mutable-in-place (`InPlace`, or a `Cow`-owned list) we
    // recurse on the real slot (the JS in-place identity contract — §9.3 #4);
    // when it is committed we recurse on a clone and only commit (clone the
    // list) if the child actually changed (the JS `newExisting === existing`
    // short-circuit, `view-apply-change.ts:396-398`).
    let list_owned = can_mut(parent.rels[rel.ix()].created, mode, dirty);
    if list_owned {
        let pe = entry_cow(parent, mode, dirty);
        let list = list_cow(list_slot(pe, rel), mode, dirty);
        apply(
            &mut list.items[pos],
            change,
            child_schema,
            child_rel,
            with_ids,
            mode,
            dirty,
        )
    } else {
        let mut child = parent.rels[rel.ix()].items[pos].clone();
        let changed = apply(
            &mut child,
            change,
            child_schema,
            child_rel,
            with_ids,
            mode,
            dirty,
        );
        if !changed {
            return false;
        }
        let pe = entry_cow(parent, mode, dirty);
        let list = list_cow(list_slot(pe, rel), mode, dirty);
        list.items[pos] = child;
        true
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_edit_change(
    parent: &mut Arc<Entry>,
    new_row: &OwnedRow,
    old_row: &OwnedRow,
    schema: &Schema,
    rel: RelId,
    with_ids: bool,
    mode: Mutate,
    dirty: &TxnDirty,
) -> bool {
    let sort = &schema.sort;
    if compare_rows(sort, old_row, new_row) != Ordering::Equal {
        // Sort key changed → the row may move.
        let (old_pos, raw, old_rc) = {
            let items = &parent.rels[rel.ix()].items;
            let old_pos = match binary_search(items, old_row, sort) {
                Ok(p) => p,
                Err(_) => panic!("old node does not exist"),
            };
            let raw = binary_search(items, new_row, sort);
            (old_pos, raw, items[old_pos].rc)
        };
        let found = raw.is_ok();
        let pos = raw.unwrap_or_else(|e| e);

        // Fast path: rc==1 and the row lands in the same slot after removing old.
        if old_rc == 1 && (pos == old_pos || pos.checked_sub(1) == Some(old_pos)) {
            let pe = entry_cow(parent, mode, dirty);
            let list = list_cow(list_slot(pe, rel), mode, dirty);
            apply_edit(
                &mut list.items[old_pos],
                new_row,
                old_row,
                schema,
                with_ids,
                mode,
                dirty,
            );
            return true;
        }

        // General move (rc may be > 1).
        let pe = entry_cow(parent, mode, dirty);
        let list = list_cow(list_slot(pe, rel), mode, dirty);
        let old_entry = list.items[old_pos].clone(); // capture original before mutating
        let new_rc = old_rc - 1;
        let adjusted_pos;
        if new_rc == 0 {
            list.items.remove(old_pos);
            adjusted_pos = if old_pos < pos { pos - 1 } else { pos };
        } else {
            entry_cow(&mut list.items[old_pos], mode, dirty).rc = new_rc; // ghost
            adjusted_pos = pos;
        }
        if found {
            // Merge into the existing entry at the destination, bump its rc.
            let existing_rc = list.items[adjusted_pos].rc;
            apply_edit(
                &mut list.items[adjusted_pos],
                new_row,
                old_row,
                schema,
                with_ids,
                mode,
                dirty,
            );
            entry_cow(&mut list.items[adjusted_pos], mode, dirty).rc = existing_rc + 1;
        } else {
            // Move: edit the (captured) old entry, set rc=1, insert at the new pos.
            let mut moved = old_entry;
            apply_edit(&mut moved, new_row, old_row, schema, with_ids, mode, dirty);
            entry_cow(&mut moved, mode, dirty).rc = 1;
            list.items.insert(adjusted_pos, moved);
        }
        true
    } else {
        // Sort key unchanged → edit in place at the located position.
        let pos = match binary_search(&parent.rels[rel.ix()].items, old_row, sort) {
            Ok(p) => p,
            Err(_) => panic!("node does not exist"),
        };
        let pe = entry_cow(parent, mode, dirty);
        let list = list_cow(list_slot(pe, rel), mode, dirty);
        apply_edit(
            &mut list.items[pos],
            new_row,
            old_row,
            schema,
            with_ids,
            mode,
            dirty,
        );
        true
    }
}

/// `applyEdit` (`view-apply-change.ts:578`): field-merge in place when allowed and
/// the sort key is unchanged, else clone-and-track. A PK/sort change always forces
/// a fresh entry (so identity tracks the new key). Recomputes `id` (unconditional,
/// matching the JS) when `with_ids`.
#[allow(clippy::too_many_arguments)]
fn apply_edit(
    existing: &mut Arc<Entry>,
    new_row: &OwnedRow,
    old_row: &OwnedRow,
    schema: &Schema,
    with_ids: bool,
    mode: Mutate,
    dirty: &TxnDirty,
) {
    let can = can_mut(existing.created, mode, dirty);
    if can && compare_rows(&schema.sort, old_row, new_row) == Ordering::Equal {
        let e = entry_cow(existing, mode, dirty);
        e.row = new_row.clone(); // edits carry the full row (`09` §5.8 / §12-Q6)
        if with_ids {
            e.id = Some(Box::new(make_id(new_row, &schema.primary_key)));
        }
    } else {
        let mut e = (**existing).clone();
        e.row = new_row.clone();
        e.created = dirty.gen;
        if with_ids {
            e.id = Some(Box::new(make_id(new_row, &schema.primary_key)));
        }
        *existing = Arc::new(e);
    }
}

/// Build a freshly-added entry's children **in place** (it is unobserved).
/// Ports `initializeRelationshipsForNewEntryIfAny` (`view-apply-change.ts:624`).
/// Drains each present relationship thunk; each plural child is binary-search-
/// inserted directly. A slot with no child schema is join-only / out of the view
/// shape and is skipped.
fn init_rels_for_new_entry(
    entry: &mut Entry,
    node: &Node<'_>,
    schema: &Schema,
    with_ids: bool,
    dirty: &TxnDirty,
) {
    for r in &node.rels {
        let slot = r.slot;
        let child_schema = match schema.rel_child(slot) {
            Some(s) => s,
            None => continue, // join-only / out of view
        };

        // Plural: build the sorted list directly.
        let mut items: Vec<Arc<Entry>> = Vec::new();
        for child in (r.thunk)() {
            match binary_search(&items, &child.row, &child_schema.sort) {
                Ok(p) => {
                    Arc::make_mut(&mut items[p]).rc += 1;
                }
                Err(ins) => {
                    let mut ce =
                        make_new_entry(child.row.clone(), child_schema, with_ids, 1, dirty);
                    init_rels_for_new_entry(&mut ce, &child, child_schema, with_ids, dirty);
                    items.insert(ins, Arc::new(ce));
                }
            }
        }
        // A drained-empty slot keeps the shared empty list from `make_new_entry`
        // (no per-slot allocation); only a populated slot gets its own list.
        if !items.is_empty() {
            entry.rels[slot.ix()] = Arc::new(EntryListInner {
                created: dirty.gen,
                items,
            });
        }
    }
}

/// A throwaway entry for a `std::mem::replace` take-then-put-back (the View flush).
fn placeholder_entry() -> Entry {
    Entry {
        row: OwnedRow::empty(),
        rc: 0,
        created: TxnGen::default(),
        id: None,
        rels: Box::new([]),
    }
}

// ---------------------------------------------------------------------------
// ResultType / listeners (`09` §4.7)
// ---------------------------------------------------------------------------

/// The query's completion state (`typed-view.ts`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ResultType {
    Unknown,
    Complete,
    Error,
}

/// A flush listener (`Listener`, `typed-view.ts:9`). Fired on flush and once
/// immediately on registration.
pub type Listener = Box<dyn FnMut(&ViewData, ResultType)>;

// ---------------------------------------------------------------------------
// The View sink (`09` §4.1) — the arena operator
// ---------------------------------------------------------------------------

/// The materialization sink (ports `ArrayView`). Lives in the arena as
/// `Operator::View`; its graph-touching halves (`hydrate`/`view_push`) are
/// `Graph` methods that delegate the pure folding to [`apply_change`].
pub struct View {
    /// Upstream operator we fetch/receive pushes from.
    pub input: crate::graph::NodeId,
    /// Output schema of `input` (hierarchical; names resolved to `ColId`/`RelId`).
    /// This is also the view shape: a relationship slot is in-view iff its `RelDef`
    /// carries a child schema (`rel_child(slot).is_some()`).
    pub schema: Arc<Schema>,
    /// Always `false` for `ArrayView` (`09` §1.2); kept for a future `SolidView`.
    pub with_ids: bool,
    /// The synthetic root. `root[""]` is the top-level result; `Arc`-shared
    /// subtrees give reference stability.
    pub root: RefCell<Arc<Entry>>,
    listeners: RefCell<Vec<Listener>>,
    dirty: Cell<bool>,
    result_type: Cell<ResultType>,
    /// Transaction generation (the `#txnDirty` analogue, bumped on flush).
    txn: Cell<TxnGen>,
}

impl View {
    /// Construct a View over `input` with the given hierarchical `schema` (which
    /// also carries the view shape). The caller (`Graph::add_array_view`) then
    /// `hydrate`s it.
    pub fn new(
        input: crate::graph::NodeId,
        schema: Schema,
        with_ids: bool,
        result_type: ResultType,
    ) -> View {
        let root = synthetic_root();
        View {
            input,
            schema: Arc::new(schema),
            with_ids,
            root: RefCell::new(Arc::new(root)),
            listeners: RefCell::new(Vec::new()),
            dirty: Cell::new(false),
            result_type: Cell::new(result_type),
            txn: Cell::new(TxnGen::default()),
        }
    }

    /// The top-level result snapshot (`data` getter). Cheap `Arc` clone.
    pub fn data(&self) -> ViewData {
        view_data(&self.root.borrow())
    }

    /// Current result type.
    pub fn result_type(&self) -> ResultType {
        self.result_type.get()
    }

    /// Set the result type (the async `queryComplete` resolution, `09` §3.8). Fires
    /// listeners out of band (it does not touch the txn dirty state).
    pub fn set_result_type(&self, rt: ResultType) {
        self.result_type.set(rt);
        self.fire_listeners();
    }

    /// Register a listener; fires it once immediately with the current snapshot
    /// (`addListener`, `array-view.ts:115`). Returns the index (for removal).
    pub fn add_listener(&self, mut l: Listener) -> usize {
        // Fire once immediately (no View borrow held across the call).
        let (data, rt) = (self.data(), self.result_type.get());
        l(&data, rt);
        let mut ls = self.listeners.borrow_mut();
        ls.push(l);
        ls.len() - 1
    }

    /// Number of registered listeners (test/inspection helper).
    pub fn listener_count(&self) -> usize {
        self.listeners.borrow().len()
    }

    /// Mark the view dirty (a push happened). `view_push` (in `graph.rs`) calls
    /// this before folding the change.
    pub fn mark_dirty(&self) {
        self.dirty.set(true);
    }

    /// `flush` (`array-view.ts:173`): if dirty, fire listeners with the snapshot,
    /// then bump the txn generation (so the next txn copy-on-writes again — the
    /// fresh-`WeakSet` analogue). No-op when not dirty.
    pub fn flush(&self) {
        if !self.dirty.get() {
            return;
        }
        self.dirty.set(false);
        self.fire_listeners();
        self.txn.set(TxnGen(self.txn.get().0 + 1));
    }

    /// Fire listeners with the current snapshot. Holds **no** `RefCell` borrow
    /// across a listener call (a listener may re-enter the View): snapshot the
    /// data, `take` the listener vec, fire, then splice the vec back.
    fn fire_listeners(&self) {
        let data = self.data();
        let rt = self.result_type.get();
        let mut ls = std::mem::take(&mut *self.listeners.borrow_mut());
        for l in ls.iter_mut() {
            l(&data, rt);
        }
        // Re-attach (a listener may have registered more during the fire).
        self.listeners.borrow_mut().splice(0..0, ls);
    }

    /// The current transaction generation (used by `view_push`/`hydrate`).
    pub fn txn_gen(&self) -> TxnGen {
        self.txn.get()
    }

    // --- graph-driven hydrate / push (the `array-view.ts` halves) ---------

    /// Take the root `Arc` out (replacing it with a placeholder), so the returned
    /// local is the **sole** strong holder — letting `InPlace`/owned `make_mut`
    /// genuinely mutate in place (`09` §5.1). No `RefCell` borrow is held across
    /// the subsequent fetch/`apply_change`.
    fn take_root(&self) -> Arc<Entry> {
        std::mem::replace(&mut *self.root.borrow_mut(), Arc::new(placeholder_entry()))
    }
    fn put_root(&self, root: Arc<Entry>) {
        *self.root.borrow_mut() = root;
    }

    /// Hydrate the tree from the input's `fetch` stream (the caller supplies the
    /// already-fetched node iterator so the `Graph` borrow stays with the caller).
    /// Builds `InPlace` (the root is unobserved), then flushes once
    /// (`array-view.ts:140-157`).
    pub fn hydrate_from<'g>(&self, nodes: impl Iterator<Item = Node<'g>>) {
        self.mark_dirty();
        let mut root = self.take_root();
        let dirty = TxnDirty {
            gen: self.txn.get(),
        };
        for node in nodes {
            apply_change(
                &mut root,
                &ViewChange::Add { node },
                &self.schema,
                REL_ROOT,
                self.with_ids,
                Mutate::InPlace,
                &dirty,
            );
        }
        self.put_root(root);
        self.flush();
    }

    /// Fold one dataflow [`Change`](crate::change::Change) into the tree
    /// (`array-view.ts:159-171`). Transaction-scoped COW: takes the root out (so
    /// owned spine objects are uniquely held and mutate in place), applies, writes
    /// back. Does **not** flush — the caller flushes at the transaction boundary
    /// (the legacy `source_push` tests read the tree directly without flushing,
    /// which is fine: the change is applied to `root` synchronously).
    pub fn push_change<'g>(&self, change: crate::change::Change<'g>) {
        self.mark_dirty();
        let vc = ViewChange::from_change(change);
        let mut root = self.take_root();
        let dirty = TxnDirty {
            gen: self.txn.get(),
        };
        apply_change(
            &mut root,
            &vc,
            &self.schema,
            REL_ROOT,
            self.with_ids,
            Mutate::Cow,
            &dirty,
        );
        self.put_root(root);
    }

    // --- test/inspection readback (the spike `dump_view` shape) -----------

    /// `[(col0_id, [child_col0_id, …]), …]` — top rows + their children's col-0
    /// ids, children flattened across all relationship slots in slot order (the
    /// spike `dump_view` contract). Assumes col 0 is `Int`.
    pub fn dump_col0(&self) -> Vec<(i64, Vec<i64>)> {
        fn id_of(row: &OwnedRow) -> i64 {
            match row.col(0) {
                Value::Int(i) => i,
                other => panic!("dump_view expects Int id in col 0, got {other:?}"),
            }
        }
        self.top_entries()
            .iter()
            .map(|e| {
                let mut kids = Vec::new();
                for r in e.rels.iter() {
                    kids.extend(r.items.iter().map(|c| id_of(&c.row)));
                }
                (id_of(&e.row), kids)
            })
            .collect()
    }

    /// `[(full_int_row, [child_full_int_row, …]), …]` — like [`View::dump_col0`]
    /// but every column as `i64` (so an edit's non-key value is observable).
    pub fn dump_rows(&self) -> Vec<(Vec<i64>, Vec<Vec<i64>>)> {
        fn ints(row: &OwnedRow) -> Vec<i64> {
            row.cells()
                .map(|v| match v {
                    Value::Int(i) => i,
                    other => panic!("dump_view_rows expects Int columns, got {other:?}"),
                })
                .collect()
        }
        self.top_entries()
            .iter()
            .map(|e| {
                let mut kids = Vec::new();
                for r in e.rels.iter() {
                    kids.extend(r.items.iter().map(|c| ints(&c.row)));
                }
                (ints(&e.row), kids)
            })
            .collect()
    }

    /// Recursive col-0 dump: each entry's col-0 id plus its children's (recursively,
    /// across all relationship slots in slot order). The deep counterpart of
    /// [`View::dump_col0`] — surfaces grandchildren, so a nested relationship
    /// (`issue{comments{reactions}}`) is fully observable. Assumes col 0 is `Int`.
    pub fn dump_col0_deep(&self) -> Vec<Col0Node> {
        fn id_of(row: &OwnedRow) -> i64 {
            match row.col(0) {
                Value::Int(i) => i,
                other => panic!("dump_col0_deep expects Int id in col 0, got {other:?}"),
            }
        }
        fn walk(e: &Entry) -> Col0Node {
            let mut children = Vec::new();
            for r in e.rels.iter() {
                children.extend(r.items.iter().map(|c| walk(c)));
            }
            Col0Node {
                id: id_of(&e.row),
                children,
            }
        }
        self.top_entries().iter().map(|e| walk(e)).collect()
    }

    /// The top-level entries (`root[""]`), collected into an owned `Vec` of `Arc`
    /// clones so the `RefCell` borrow is released. Used by the dump helpers.
    fn top_entries(&self) -> Vec<Arc<Entry>> {
        self.root.borrow().rels[REL_ROOT.ix()].items.clone()
    }
}

/// A node of [`View::dump_col0_deep`]: a col-0 id and its recursive children.
#[derive(Debug, PartialEq, Eq)]
pub struct Col0Node {
    pub id: i64,
    pub children: Vec<Col0Node>,
}

#[cfg(test)]
mod tests;
