//! **Primitive #4 — the COW B+tree (the clean win).** Spike for spec
//! `04-sources-memory-and-btree.md` §4.4 (the M3 milestone) and the two open
//! questions it flags as the #1 implementation risk:
//!
//! - **OQ-1 — `BTreeCursor` self-reference / snapshot wiring** (§4.4.4, §12 Q1).
//!   The cursor must hold its `Rc<BNode>` snapshot *and* vend a borrowed
//!   `&OwnedRow` out of it. The naive `&'t BNode` field that borrows another
//!   field of the same struct is a self-referential struct — not expressible in
//!   safe Rust. This spike locks in the **recommended shape**: the cursor owns
//!   `Rc<BNode>` clones along the spine (no `&'t` self-borrows) and `next_row`
//!   returns `&self.leaf.keys[pos]`, a borrow of `self` valid only until the
//!   next call — exactly the lending [`RowStream`] contract. *Result: the cursor
//!   needs no lifetime parameter at all.*
//!
//! - **OQ-2 — owning a row at the node boundary without a deep copy** (§8.2,
//!   §12 Q2). Node construction needs the stored `Arc<[OwnedValue]>` to *bump*
//!   (refcount++), not deep-copy. A bare borrowed slice has lost its `Arc`
//!   identity, so `Arc::from(slice)` would re-allocate. **Resolution — folded
//!   into the shared trait:** the cursor's `RowStream::Row<'a>` is `&'a OwnedRow`
//!   (not a slice), so `RowRef::to_owned_row` is just `Arc::clone` of the stored
//!   row — a refcount bump, never a copy (proven by `Arc::ptr_eq` in the tests).
//!   No concrete `current_arc` escape hatch is needed: the *same* `to_owned_row`
//!   the SQLite leaf uses to *copy* out of its step buffer is, here, a *bump*.
//!
//! **What `Rc<BNode>` + `Rc::make_mut` deletes** (§4.4.1): the JS
//! `BNode.isShared` transitive flag (`btree-set.ts:384-396`) — the hardest
//! hand-port artifact — *vanishes*. Sharing is `Arc::strong_count > 1` (tracked
//! by the runtime); "clone before mutate if shared" is precisely `Rc::make_mut`;
//! transitivity falls out because `make_mut` is called lazily per node as the
//! mutation descends, copying only the root→leaf path and only the nodes that are
//! actually aliased. `fork()`/`clone()` is one `Arc` bump — O(1).
//!
//! Scope note: this uses the spike crate's existing **owned** `Value`/`Row`
//! (`value.rs`); the production `Value<'a>`/`OwnedValue` split is Step 2 (a
//! mechanical type swap — the algorithm here is the production one). `add`,
//! `delete` (with the JS opportunistic `tryMerge` rebalancing), `from_sorted`, the
//! forward/reverse bounded cursor, and `fork`/`make_mut` COW are all faithful
//! ports of `shared/src/btree-set.ts` and differential-tested against it
//! (`tests/btree_diff.rs`). The one **deliberate** divergence from JS is the
//! `set` "shift-to-avoid-split" sibling rebalance (`btree-set.ts:558-583`): it is
//! a pure micro-optimization (never changes contents, and is never triggered by
//! ascending inserts or `from_sorted`, so it affects no benchmark), and it carries
//! the most bug surface of any path — omitted on purpose, documented in
//! `node_set`. In production this module is `#[cfg(feature = "memory")]`.

use std::cmp::Ordering;
use std::rc::Rc; // node pointer — single-threaded client default (README §7.1)

use crate::change::SourceChange;
use crate::value::{compare_rows, compare_values, ColId, OwnedRow, OwnedValue, Sort};

// Re-export the lending traits so existing `rindle::btree::{RowStream, RowRef}`
// import paths (and the integration tests) keep working now that the canonical
// definitions live in `value.rs` — the union proven against BOTH backends.
pub use crate::value::{RowRef, RowStream};

/// The btree's element type is the canonical [`OwnedRow`]. The COW tree stores
/// owned rows; the lending cursor vends `&OwnedRow`, so owning a row at the node
/// boundary ([`RowRef::to_owned_row`]) is an `Arc` bump, not a copy.
type Row = OwnedRow;

/// Max keys per leaf / max children per internal node before a split. The JS uses
/// 32 (`btree-set.ts:3`, tuned for V8); 64 is the Rust measured optimum (a swept
/// benchmark — `examples/bench_rs.rs`): fewer levels → fewer pointer-chases per
/// lookup and better scan locality, at a small cost to single-key path-copy. Node
/// size changes no observable contents, so the JS differential still holds. Tests
/// insert well past it to force multi-level trees.
const MAX_NODE_SIZE: usize = 64;

/// Pre-allocated cursor spine depth. A fanout-`MAX_NODE_SIZE` tree of this depth
/// holds `32^24` rows — astronomically beyond any real dataset (real depth is
/// < 8), so the spine `Vec` is allocated ONCE at cursor creation and **never
/// reallocates during the scan**. This is what makes the per-row vend cost a
/// clean 0 (the spine's O(log n) cost is paid once, not per row).
const SPINE_CAP: usize = 24;

// The lending `RowStream`/`RowRef` traits now live in `value.rs` (the canonical
// union, refined by the SQLite step-buffer lifetime) and are re-exported above.
// OQ-1 — "does the lending trait compose with a cursor that *owns* its `Rc`
// snapshot?" — is answered by the `impl RowStream for BTreeCursor` at the bottom
// of this file.

// ---------------------------------------------------------------------------
// Nodes — a Rust enum replaces the JS BNode / BNodeInternal subclass pair.
// NO `isShared` field (§4.4.1): the whole reason this port is "the clean win".
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum BNode {
    Leaf {
        /// Sorted rows.
        keys: Vec<Row>,
    },
    Internal {
        /// `keys[i] == children[i].max_key()` — the cached subtree-max invariant
        /// (`btree-set.ts:501-503`). Lets `max_key()` be O(1) and navigation a
        /// binary search over child maxes.
        keys: Vec<Row>,
        children: Vec<Rc<BNode>>,
    },
}

impl BNode {
    fn empty_leaf() -> BNode {
        BNode::Leaf { keys: Vec::new() }
    }

    fn is_leaf(&self) -> bool {
        matches!(self, BNode::Leaf { .. })
    }

    /// The keys vec for either variant (rows for a leaf; cached child-maxes for an
    /// internal node).
    fn keys(&self) -> &[Row] {
        match self {
            BNode::Leaf { keys } | BNode::Internal { keys, .. } => keys,
        }
    }

    fn num_children(&self) -> usize {
        match self {
            BNode::Internal { children, .. } => children.len(),
            BNode::Leaf { .. } => 0,
        }
    }

    /// Clone (Arc bump) of child `i`. Panics on a leaf — callers guard with
    /// `is_leaf`.
    fn child(&self, i: usize) -> Rc<BNode> {
        match self {
            BNode::Internal { children, .. } => Rc::clone(&children[i]),
            BNode::Leaf { .. } => unreachable!("child() on a leaf"),
        }
    }

    /// O(1) max key of this subtree, via the cache invariant: a leaf's max is its
    /// last key; an internal node's max is `keys.last()` (which *is* the cached max
    /// of its last child, recursively). Caller guarantees non-empty.
    fn max_key(&self) -> &Row {
        self.keys().last().expect("max_key on an empty node")
    }

    /// An empty leaf (0 keys) or an empty internal node (0 children) — pruned by
    /// `delete` so the tree never contains empty interior nodes.
    fn is_empty_node(&self) -> bool {
        match self {
            BNode::Leaf { keys } => keys.is_empty(),
            BNode::Internal { children, .. } => children.is_empty(),
        }
    }
}

// ---------------------------------------------------------------------------
// Seek bounds — the min/max sentinels (§4.3)
// ---------------------------------------------------------------------------

/// A scan-start cell: a concrete value or a type-extreme sentinel that sorts
/// below/above *every* real value (including `Null`, the lowest real value).
/// Mirrors JS `Bound = Value | minValue | maxValue` (`memory-source.ts:967`). The
/// spike uses one owned form (the production `Bound<'a>`/`OwnedBound` split is the
/// zero-copy concern of Step 2).
#[derive(Clone)]
pub enum Bound {
    Min,
    Max,
    Val(OwnedValue),
}

/// A scan-start key: each index-sort column pinned to a [`Bound`]. Empty / absent
/// columns are treated as `Min` (a safety default — a real seek pins every sort
/// column). Index-addressed; a small `Vec`, not a map (`04` §4.3).
pub type RowBound = Vec<(ColId, Bound)>;

/// Build an all-`Val` `RowBound` from a concrete lower-bound row.
pub fn row_bound_of(row: &Row, sort: &Sort) -> RowBound {
    sort.iter()
        .map(|&(c, _)| (c, Bound::Val(row.col(c).to_owned())))
        .collect()
}

/// `compareBounds`-over-a-sort (`memory-source.ts:1000-1015`): compare a
/// (possibly-sentinel) bound against a stored row under `sort`. Reverse is *not*
/// baked in here — the caller negates as needed (the one-place convention of
/// `04` §4.6 lives in the source, not the tree).
fn cmp_bound_row(sort: &Sort, bound: &RowBound, row: &Row) -> Ordering {
    for &(col, asc) in sort {
        let b = bound.iter().find(|(c, _)| *c == col).map(|(_, b)| b);
        let c = match b {
            None | Some(Bound::Min) => Ordering::Less,
            Some(Bound::Max) => Ordering::Greater,
            Some(Bound::Val(v)) => compare_values(v.as_ref(), row.col(col)),
        };
        if c != Ordering::Equal {
            return if asc { c } else { c.reverse() };
        }
    }
    Ordering::Equal
}

// ---------------------------------------------------------------------------
// Search / navigation helpers (free fns over `&Sort`, no boxed comparator —
// foundations §11.6)
// ---------------------------------------------------------------------------

/// Binary search a sorted `keys` slice for `key`. `Ok(i)` = exact match at `i`;
/// `Err(i)` = insertion point.
fn search(keys: &[Row], key: &Row, sort: &Sort) -> Result<usize, usize> {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        match compare_rows(sort, &keys[mid], key) {
            Ordering::Less => lo = mid + 1,
            Ordering::Greater => hi = mid,
            Ordering::Equal => return Ok(mid),
        }
    }
    Err(lo)
}

/// First index `i` with `keys[i] >= key` (lower bound). In `[0, keys.len()]` —
/// unlike [`child_index`] it is NOT clamped, so `== keys.len()` signals "key is
/// greater than every entry" (used by `node_delete` to detect a key past the end).
fn lower_bound(keys: &[Row], key: &Row, sort: &Sort) -> usize {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if compare_rows(sort, &keys[mid], key) == Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Child index whose subtree should contain `key`: the first child whose cached
/// max is `>= key`, else the last child (for a key past the end). `keys` are the
/// internal node's child-maxes; non-empty by the no-empty-node invariant.
fn child_index(keys: &[Row], key: &Row, sort: &Sort) -> usize {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if compare_rows(sort, &keys[mid], key) == Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == keys.len() {
        keys.len() - 1
    } else {
        lo
    }
}

/// Child index for a seek bound: first child whose cached max is `>= bound`, else
/// the last child.
fn child_index_bound(keys: &[Row], bound: &RowBound, sort: &Sort) -> usize {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if cmp_bound_row(sort, bound, &keys[mid]) == Ordering::Greater {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == keys.len() {
        keys.len() - 1
    } else {
        lo
    }
}

/// First leaf index `>= bound` (lower bound). May equal `keys.len()` (the element
/// is in the next leaf — the cursor normalizes).
fn leaf_lower_bound(keys: &[Row], bound: &RowBound, sort: &Sort) -> usize {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if cmp_bound_row(sort, bound, &keys[mid]) == Ordering::Greater {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// First leaf index strictly `> bound` (upper bound). Used by the reverse seek:
/// the largest element `<= bound` sits at `leaf_upper_bound - 1`.
fn leaf_upper_bound(keys: &[Row], bound: &RowBound, sort: &Sort) -> usize {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if cmp_bound_row(sort, bound, &keys[mid]) == Ordering::Less {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

/// Recursive read descent (`btree-set.ts:415-423`, `537-547`). Borrows the stored
/// row out of the tree — zero allocation and zero refcount traffic (unlike an
/// iterative `Arc`-cloning walk). Recursion (rather than a loop) sidesteps the
/// borrow-reassign trap while keeping the descent by reference; depth is
/// `log_32 n` so the call chain is tiny.
fn find<'a>(node: &'a BNode, key: &Row, sort: &Sort) -> Option<&'a Row> {
    match node {
        BNode::Leaf { keys } => match search(keys, key, sort) {
            Ok(i) => Some(&keys[i]),
            Err(_) => None,
        },
        BNode::Internal { keys, children } => {
            if children.is_empty() {
                return None;
            }
            find(&children[child_index(keys, key, sort)], key, sort)
        }
    }
}

// ---------------------------------------------------------------------------
// The COW B+tree
// ---------------------------------------------------------------------------

/// A copy-on-write B+tree set of [`OwnedRow`]s, ordered by a `Sort` comparator passed
/// per call (no stored closure — foundations §11.6). Replaces `BTreeSet<Row>`
/// (`btree-set.ts:7`) with `Rc<BNode>` + `Rc::make_mut`. The comparator lives
/// with the enclosing `Index` in the real source; the spike threads `&Sort`.
///
/// **Preconditions** (schema invariants the builder/source guarantee; a violation
/// is a programming error and panics, mirroring the JS oracle's `compareValues`
/// throw — `data.ts`):
/// - every `Row` is at least as wide as `max(ColId in sort/bound) + 1` (so column
///   indexing never goes out of bounds);
/// - within any one sort column, all rows carry the **same** `Value` variant
///   (so `compare_values` never hits a cross-type pair — risk-register R10; the
///   builder coerces literals to the column type at build time).
pub struct BTree {
    root: Rc<BNode>,
    size: usize,
}

impl Default for BTree {
    fn default() -> Self {
        BTree::new()
    }
}

/// Outcome of an internal `node_set` descent.
enum InsertResult {
    /// An equal key already existed; its slot was overwritten. Size unchanged.
    Existed,
    /// A new key was inserted; no split propagated.
    Inserted,
    /// The node overflowed and split; this is the new right sibling to graft into
    /// the parent (or grow a new root).
    Split(Rc<BNode>),
}

impl BTree {
    /// Empty tree. O(1).
    pub fn new() -> BTree {
        BTree {
            root: Rc::new(BNode::empty_leaf()),
            size: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// **The headline: O(1) structural clone — one `Arc` bump + a `usize` copy.**
    /// JS `clone()` (`btree-set.ts:28-34`) sets `root.isShared = true` and shares
    /// the root; Rust just clones the `Arc`. `fork()` and `#getOrCreateIndex`'s
    /// `data.clone()` both become this. NO flag mutation, NO node allocation.
    /// (Not `impl Clone` so the O(1)-ness is named at every call site.)
    pub fn fork(&self) -> BTree {
        BTree {
            root: Rc::clone(&self.root),
            size: self.size,
        }
    }

    /// `has` (`btree-set.ts:56-58`). O(log n), read-only — never calls `make_mut`,
    /// never bumps a refcount (descends by reference).
    pub fn has(&self, key: &Row, sort: &Sort) -> bool {
        find(&self.root, key, sort).is_some()
    }

    /// `get` (`btree-set.ts:36-38`): the stored row equal to `key` under the
    /// comparator (rows may carry payload beyond the sort columns), borrowed out of
    /// the tree. O(log n), read-only, zero allocation/refcount traffic — the caller
    /// `Arc::clone`s only if it needs to own (one bump, OQ-2).
    pub fn get(&self, key: &Row, sort: &Sort) -> Option<&Row> {
        find(&self.root, key, sort)
    }

    /// `add` (`btree-set.ts:40-47`). Inserts `key`; returns `true` iff newly
    /// inserted (`false` overwrites the equal slot, like the JS). **Path-copying
    /// COW:** `make_mut` the root, then each child as the insert descends — only
    /// the root→leaf path is copied, and only nodes that are actually shared.
    pub fn add(&mut self, key: Row, sort: &Sort) -> bool {
        // `make_mut` IS the JS `if (root.isShared) root = root.clone()` — but it
        // clones *iff* `strong_count > 1`, else mutates in place.
        let root = Rc::make_mut(&mut self.root);
        match node_set(root, key, sort) {
            InsertResult::Existed => false,
            InsertResult::Inserted => {
                self.size += 1;
                true
            }
            InsertResult::Split(right) => {
                // Root split: grow a new root one level taller.
                self.size += 1;
                let left = std::mem::replace(&mut self.root, Rc::new(BNode::empty_leaf()));
                let lk = left.max_key().clone();
                let rk = right.max_key().clone();
                self.root = Rc::new(BNode::Internal {
                    keys: vec![lk, rk],
                    children: vec![left, right],
                });
                true
            }
        }
    }

    /// `delete` (`btree-set.ts:66-89`). Removes `key`; returns `true` iff present.
    /// Path-copying COW (same `make_mut` descent as `add`) + the JS opportunistic
    /// rebalancing: after removing from a child, an emptied child is dropped and an
    /// underfull child (`<= MAX/2`) is `tryMerge`d with its right neighbour when the
    /// combined size fits (`btree-set.ts:639-721`). Each merged-into sibling is
    /// `make_mut`'d before mutation (the `isShared`→`Arc` substitution, §4.4.5).
    /// Then the root-collapse loop runs. This matches the JS `BTreeSet` occupancy
    /// behaviour exactly (it too tolerates underfull nodes that can't merge — it is
    /// *not* a strict B-tree).
    pub fn delete(&mut self, key: &Row, sort: &Sort) -> bool {
        let root = Rc::make_mut(&mut self.root);
        let removed = node_delete(root, key, sort);
        if removed {
            self.size -= 1;
            self.collapse_root();
        }
        removed
    }

    /// `btree-set.ts:79-88`: while the root is an internal node with a single
    /// child, descend it (the tree got shorter). A fully-emptied internal root
    /// becomes an empty leaf. `make_mut` already copied the path; no `isShared`
    /// re-propagation needed (`Arc` handles it).
    fn collapse_root(&mut self) {
        loop {
            let only_child = match &*self.root {
                BNode::Internal { children, .. } if children.len() == 1 => {
                    Some(Rc::clone(&children[0]))
                }
                _ => None,
            };
            match only_child {
                Some(c) => self.root = c,
                None => break,
            }
        }
        if matches!(&*self.root, BNode::Internal { children, .. } if children.is_empty()) {
            self.root = Rc::new(BNode::empty_leaf());
        }
    }

    /// `fromSorted` (`btree-set.ts:140-188`): O(N) bottom-up bulk load from a
    /// PRE-SORTED iterator (caller guarantees order under `sort`). Build leaves of
    /// `MAX_NODE_SIZE`, then internal levels bottom-up until one root. NO per-key
    /// descent — this is the O(N)-vs-O(N log n) win the lazy index build needs
    /// (`04` §5.4).
    pub fn from_sorted(iter: impl Iterator<Item = Row>, sort: &Sort) -> BTree {
        let rows: Vec<Row> = iter.collect();
        let size = rows.len();
        debug_assert!(
            rows.windows(2)
                .all(|w| compare_rows(sort, &w[0], &w[1]) != Ordering::Greater),
            "from_sorted: input not sorted under `sort`"
        );
        if rows.is_empty() {
            return BTree::new();
        }
        // Move rows into leaves (no per-row clone): drain the owned Vec in chunks.
        let mut level: Vec<Rc<BNode>> = Vec::with_capacity(size / MAX_NODE_SIZE + 1);
        let mut it = rows.into_iter();
        loop {
            let chunk: Vec<Row> = it.by_ref().take(MAX_NODE_SIZE).collect();
            if chunk.is_empty() {
                break;
            }
            level.push(Rc::new(BNode::Leaf { keys: chunk }));
        }
        while level.len() > 1 {
            level = level
                .chunks(MAX_NODE_SIZE)
                .map(|chunk| {
                    let children: Vec<Rc<BNode>> = chunk.to_vec();
                    let keys: Vec<Row> = children.iter().map(|c| c.max_key().clone()).collect();
                    Rc::new(BNode::Internal { keys, children })
                })
                .collect();
        }
        BTree {
            root: level.into_iter().next().unwrap(),
            size,
        }
    }

    /// Validate structural invariants — a test/debug helper (cheap to keep `pub`;
    /// it walks the whole tree, so call it in tests/property checks, not the hot
    /// path). Returns `Err(reason)` on the first violation. Checks: balanced (every
    /// leaf at the same depth), per-node strict sortedness, the cached-max invariant
    /// (`keys[i] == children[i].max_key()`), `keys.len() == children.len()` for
    /// internal nodes, node occupancy `<= MAX_NODE_SIZE`, no empty non-root node,
    /// and `size` equals the actual key count.
    pub fn check_invariants(&self, sort: &Sort) -> Result<(), String> {
        fn walk(node: &BNode, sort: &Sort, is_root: bool) -> Result<(usize, usize), String> {
            match node {
                BNode::Leaf { keys } => {
                    if !is_root && keys.is_empty() {
                        return Err("empty non-root leaf".into());
                    }
                    if keys.len() > MAX_NODE_SIZE {
                        return Err(format!("overfull leaf: {} keys", keys.len()));
                    }
                    for w in keys.windows(2) {
                        if compare_rows(sort, &w[0], &w[1]) != Ordering::Less {
                            return Err("leaf keys not strictly sorted".into());
                        }
                    }
                    Ok((0, keys.len()))
                }
                BNode::Internal { keys, children } => {
                    if children.is_empty() {
                        return Err("empty internal node".into());
                    }
                    if keys.len() != children.len() {
                        return Err(format!(
                            "keys.len {} != children.len {}",
                            keys.len(),
                            children.len()
                        ));
                    }
                    if children.len() > MAX_NODE_SIZE {
                        return Err(format!("overfull internal: {} children", children.len()));
                    }
                    let mut total = 0;
                    let mut depth: Option<usize> = None;
                    for (i, child) in children.iter().enumerate() {
                        // The cached separator must be the SAME Arc as the child's
                        // max (every refresh does `child.max_key().clone()`, and
                        // Arc clones are ptr-equal). This is stronger than a
                        // sort-column compare: it also catches a stale *payload* in
                        // a separator after an overwrite (the JS-divergence bug the
                        // adversarial workflow found).
                        if !Row::ptr_eq(&keys[i], child.max_key()) {
                            return Err(format!("cached-max not Arc-identical at child {i}"));
                        }
                        let (d, c) = walk(child, sort, false)?;
                        total += c;
                        match depth {
                            None => depth = Some(d),
                            Some(dd) if dd != d => return Err("unbalanced subtree depths".into()),
                            _ => {}
                        }
                    }
                    for w in keys.windows(2) {
                        if compare_rows(sort, &w[0], &w[1]) != Ordering::Less {
                            return Err("separator keys not strictly increasing".into());
                        }
                    }
                    Ok((depth.unwrap() + 1, total))
                }
            }
        }
        let (_, count) = walk(&self.root, sort, true)?;
        if count != self.size {
            return Err(format!("size {} != actual count {count}", self.size));
        }
        Ok(())
    }

    /// Forward ordered iteration from an optional lower bound (`valuesFrom`,
    /// `btree-set.ts:99-101`). `bound = None` ⇒ from the start. `inclusive` selects
    /// `>= bound` (true) vs `> bound` (false). Returns the LENDING [`BTreeCursor`]
    /// — zero per-row alloc to vend.
    pub fn values_from(
        &self,
        bound: Option<&RowBound>,
        inclusive: bool,
        sort: &Sort,
    ) -> BTreeCursor {
        if self.size == 0 {
            return BTreeCursor::empty(false, Rc::clone(&self.root));
        }
        let mut c = BTreeCursor::empty(false, Rc::clone(&self.root));
        match bound {
            None => c.descend_first(Rc::clone(&self.root)),
            Some(b) => c.descend_seek(Rc::clone(&self.root), b, inclusive, sort),
        }
        c.normalize_forward();
        c
    }

    /// Reverse ordered (descending) iteration from an optional upper bound
    /// (`valuesFromReversed`, `btree-set.ts:113-124`). `bound = None` ⇒ from the
    /// maximum. `inclusive` selects `<= bound` (true) vs `< bound` (false). The
    /// first row yielded is the largest qualifying row; iteration then descends.
    pub fn values_from_reversed(
        &self,
        bound: Option<&RowBound>,
        inclusive: bool,
        sort: &Sort,
    ) -> BTreeCursor {
        if self.size == 0 {
            return BTreeCursor::empty(true, Rc::clone(&self.root));
        }
        let mut c = BTreeCursor::empty(true, Rc::clone(&self.root));
        match bound {
            None => c.descend_last(Rc::clone(&self.root)),
            Some(b) => c.descend_seek_reverse(Rc::clone(&self.root), b, inclusive, sort),
        }
        c.normalize_reverse();
        c
    }
}

/// Insert descent (`btree-set.ts:425-453` / `549-605`). `node` is already
/// `make_mut`'d by the caller; this `make_mut`s each child it descends — that ONE
/// substitution (`if (child.isShared) clone` → `Rc::make_mut`) is the entire
/// `isShared`→`Arc` port (§4.4.5).
///
/// DELIBERATE DIVERGENCE FROM JS: the JS `BNodeInternal.set` shifts an element to
/// a non-full sibling to *avoid* a split when descending into a full child
/// (`btree-set.ts:558-583`, `takeFromLeft`/`takeFromRight`). We omit it. It never
/// changes contents (only node occupancy), it is never triggered by ascending
/// inserts or `from_sorted` (so it affects none of the benchmarks), and it is the
/// single most intricate, alias-sensitive mutation in the tree. The plain
/// split-and-grow below is correct and simpler; the differential suite confirms
/// identical contents. Reintroduce it only behind a benchmark that shows an
/// occupancy regression on a real workload.
fn node_set(node: &mut BNode, key: Row, sort: &Sort) -> InsertResult {
    match node {
        BNode::Leaf { keys } => match search(keys, &key, sort) {
            Ok(i) => {
                keys[i] = key; // overwrite the equal slot (btree-set.ts:451)
                InsertResult::Existed
            }
            Err(i) => {
                keys.insert(i, key);
                if keys.len() > MAX_NODE_SIZE {
                    let mid = keys.len() / 2;
                    let right_keys = keys.split_off(mid);
                    InsertResult::Split(Rc::new(BNode::Leaf { keys: right_keys }))
                } else {
                    InsertResult::Inserted
                }
            }
        },
        BNode::Internal { keys, children } => {
            let i = child_index(keys, &key, sort);
            // COW: clone child i iff it is actually shared, then recurse.
            let res = {
                let child = Rc::make_mut(&mut children[i]);
                node_set(child, key, sort)
            };
            // Refresh the cached separator UNCONDITIONALLY, mirroring JS
            // `this.keys[i] = child.maxKey()` (`btree-set.ts:586`) which runs after
            // every `child.set` regardless of result. This matters on the
            // *overwrite* (`Existed`) path: when the overwritten row was the leaf's
            // max, its full Row is this node's separator, so the new payload (a
            // non-sort column may differ) must propagate up — otherwise the cached
            // max goes stale on the payload columns (caught by the adversarial
            // verification workflow; a JS divergence).
            keys[i] = children[i].max_key().clone();
            match res {
                InsertResult::Existed => InsertResult::Existed,
                InsertResult::Inserted => InsertResult::Inserted,
                InsertResult::Split(right) => {
                    keys[i] = children[i].max_key().clone(); // left half's new max
                    let rmax = right.max_key().clone();
                    children.insert(i + 1, right);
                    keys.insert(i + 1, rmax);
                    if children.len() > MAX_NODE_SIZE {
                        let mid = children.len() / 2;
                        let right_children = children.split_off(mid);
                        let right_keys = keys.split_off(mid);
                        InsertResult::Split(Rc::new(BNode::Internal {
                            keys: right_keys,
                            children: right_children,
                        }))
                    } else {
                        InsertResult::Inserted
                    }
                }
            }
        }
    }
}

/// Delete descent — a faithful port of JS `BNode.delete` / `BNodeInternal.delete`
/// (`btree-set.ts:469-674`). `node` is already `make_mut`'d by the caller; this
/// `make_mut`s each child it descends. The JS `try/finally` is straight-line code
/// here (the JS uses `finally` for control flow, not errors).
fn node_delete(node: &mut BNode, key: &Row, sort: &Sort) -> bool {
    match node {
        BNode::Leaf { keys } => match search(keys, key, sort) {
            Ok(i) => {
                keys.remove(i);
                true
            }
            Err(_) => false,
        },
        BNode::Internal { keys, children } => {
            // iLow = first child whose cached max is `>= key`; if past the end the
            // key cannot be present (JS: `i <= iHigh` guard, `btree-set.ts:646`).
            let i = lower_bound(keys, key, sort);
            if i >= children.len() {
                return false;
            }
            let removed = {
                let child = Rc::make_mut(&mut children[i]);
                node_delete(child, key, sort)
            };
            // Refresh the cached max (unless the child emptied — then it is dropped
            // in the merge scan; JS leaves keys[i]=undefined here, fixed below).
            if !children[i].is_empty_node() {
                keys[i] = children[i].max_key().clone();
            }
            // The JS `finally` merge scan over children[i] and its left neighbour
            // (`btree-set.ts:656-671`): drop emptied children; `tryMerge` underfull
            // ones with their right sibling when the combined size fits.
            let half = MAX_NODE_SIZE / 2;
            let lo = i.saturating_sub(1);
            for j in (lo..=i).rev() {
                if j < children.len() && children[j].keys().len() <= half {
                    if children[j].is_empty_node() {
                        children.remove(j);
                        keys.remove(j);
                    } else {
                        try_merge(keys, children, j);
                    }
                }
            }
            removed
        }
    }
}

/// `BNodeInternal.tryMerge` (`btree-set.ts:677-695`): merge child `i` into its
/// right sibling-pair (`i` and `i+1`) when their combined size fits in a node.
/// `make_mut`s the left child before merging into it; drops the right child.
fn try_merge(keys: &mut Vec<Row>, children: &mut Vec<Rc<BNode>>, i: usize) -> bool {
    if i + 1 < children.len()
        && children[i].keys().len() + children[i + 1].keys().len() <= MAX_NODE_SIZE
    {
        let right = children.remove(i + 1);
        keys.remove(i + 1);
        {
            let left = Rc::make_mut(&mut children[i]);
            merge_into(left, &right);
        }
        keys[i] = children[i].max_key().clone();
        return true;
    }
    false
}

/// `BNode.mergeSibling` (`btree-set.ts:494-496`, `702-721`): append `right`'s
/// contents into `left`. For internal nodes this also recursively `tryMerge`s the
/// new seam (the JS `tryMerge(oldLength-1, …)`). Child `Arc`s are cloned in
/// (refcount bumps), so sharing stays correct with zero `isShared` bookkeeping.
fn merge_into(left: &mut BNode, right: &BNode) {
    match (left, right) {
        (BNode::Leaf { keys: lk }, BNode::Leaf { keys: rk }) => {
            lk.extend(rk.iter().cloned());
        }
        (
            BNode::Internal {
                keys: lk,
                children: lc,
            },
            BNode::Internal {
                keys: rk,
                children: rc,
            },
        ) => {
            let seam = lc.len() - 1;
            lk.extend(rk.iter().cloned());
            lc.extend(rc.iter().cloned());
            try_merge(lk, lc, seam);
        }
        _ => unreachable!("merge_into: mixing a leaf with an internal node"),
    }
}

// ---------------------------------------------------------------------------
// The lending cursor (OQ-1) — owns its Arc snapshot, vends a borrowed row.
// ---------------------------------------------------------------------------

/// A forward-or-reverse ordered cursor over a `BTree`. **No lifetime parameter**
/// (the OQ-1 result): it OWNS `Rc<BNode>` clones along the spine + the root
/// snapshot, so it borrows nothing externally; the only borrow is the one
/// `next_row` lends out of `self` per call. Holding the `_snapshot` root `Arc`
/// keeps the *entire pre-write tree* alive and immutable for the cursor's life —
/// a concurrent `BTree::add`/`delete` `make_mut`s a fresh path (because the
/// snapshot pins `strong_count > 1`) and never touches these nodes. That is what
/// makes reentrant fetch-during-push safe (§6, foundations §6.3).
pub struct BTreeCursor {
    /// (internal node, child index we descended into) from root to the leaf's
    /// parent. Used to walk to the next/prev leaf.
    stack: Vec<(Rc<BNode>, usize)>,
    /// Current leaf (an `Arc` clone — NOT a `&'t` self-borrow; that is the OQ-1
    /// fix). `None` once exhausted.
    leaf: Option<Rc<BNode>>,
    /// Index within `leaf` of the element to yield.
    pos: usize,
    reverse: bool,
    /// `false` until the first `next_row`; the cursor is constructed positioned at
    /// the first element, so the first pull yields without stepping.
    started: bool,
    /// Keep-alive for the whole snapshot tree (the immutability guarantee). Held
    /// even after the root is popped off `stack` during iteration.
    _snapshot: Rc<BNode>,
}

impl BTreeCursor {
    fn empty(reverse: bool, snapshot: Rc<BNode>) -> BTreeCursor {
        BTreeCursor {
            // Sized once so the scan never reallocates the spine (see SPINE_CAP).
            stack: Vec::with_capacity(SPINE_CAP),
            leaf: None,
            pos: 0,
            reverse,
            started: false,
            _snapshot: snapshot,
        }
    }

    /// The element currently under the cursor: the stored `&OwnedRow`, borrowed
    /// out of the leaf `Rc<BNode>` held in `self`. The returned borrow is tied to
    /// `&self` (hence to the `next_row` call) and invalidated by the next pull —
    /// the lending contract. Because this is a `&OwnedRow` (not a bare slice),
    /// `RowRef::to_owned_row` on it is an `Arc` bump, not a copy (OQ-2).
    fn current(&self) -> Option<&Row> {
        let leaf = self.leaf.as_deref()?;
        let keys = leaf.keys();
        if self.pos < keys.len() {
            Some(&keys[self.pos])
        } else {
            None
        }
    }

    /// Descend leftmost (forward start / next-leaf): push each internal node taking
    /// child 0; land on the leftmost leaf at pos 0.
    fn descend_first(&mut self, mut node: Rc<BNode>) {
        loop {
            if node.is_leaf() {
                break;
            }
            let child = node.child(0);
            self.stack.push((node, 0));
            node = child;
        }
        self.leaf = Some(node);
        self.pos = 0;
    }

    /// Descend rightmost (reverse start / prev-leaf): take the last child each
    /// level; land on the rightmost leaf at its last index.
    fn descend_last(&mut self, mut node: Rc<BNode>) {
        loop {
            if node.is_leaf() {
                break;
            }
            let idx = node.num_children() - 1;
            let child = node.child(idx);
            self.stack.push((node, idx));
            node = child;
        }
        let len = node.keys().len();
        self.leaf = Some(node);
        self.pos = len.saturating_sub(1);
    }

    /// Descend to the first row `>= bound` (inclusive) / `> bound` (exclusive),
    /// building the spine for continued forward iteration.
    fn descend_seek(
        &mut self,
        mut node: Rc<BNode>,
        bound: &RowBound,
        inclusive: bool,
        sort: &Sort,
    ) {
        loop {
            if node.is_leaf() {
                break;
            }
            let idx = child_index_bound(node.keys(), bound, sort);
            let child = node.child(idx);
            self.stack.push((node, idx));
            node = child;
        }
        let keys = node.keys();
        let mut pos = leaf_lower_bound(keys, bound, sort);
        // Exclusive: skip the single equal element (a set has at most one).
        if !inclusive
            && pos < keys.len()
            && cmp_bound_row(sort, bound, &keys[pos]) == Ordering::Equal
        {
            pos += 1;
        }
        self.pos = pos;
        self.leaf = Some(node);
    }

    /// Descend to position the reverse cursor at the largest row `<= bound`
    /// (inclusive) / `< bound` (exclusive). Mirrors JS `valuesFromReversed`
    /// (`btree-set.ts:319-349`). The descent picks the same child as the forward
    /// seek (first child whose max `>= bound`); if that leaf has no qualifying row
    /// (all of it `> bound`), the answer is the previous leaf's max — which is
    /// `< bound` because the chosen child is the *first* with max `>= bound`, so the
    /// previous child's max is `< bound` (`prev_leaf` handles it).
    fn descend_seek_reverse(
        &mut self,
        mut node: Rc<BNode>,
        bound: &RowBound,
        inclusive: bool,
        sort: &Sort,
    ) {
        loop {
            if node.is_leaf() {
                break;
            }
            let idx = child_index_bound(node.keys(), bound, sort);
            let child = node.child(idx);
            self.stack.push((node, idx));
            node = child;
        }
        // `j` = first index NOT qualifying as `<= bound`/`< bound`; the largest
        // qualifying row sits at `j - 1`.
        let keys = node.keys();
        let j = if inclusive {
            leaf_upper_bound(keys, bound, sort) // first > bound
        } else {
            leaf_lower_bound(keys, bound, sort) // first >= bound
        };
        self.leaf = Some(node);
        if j == 0 {
            // Nothing qualifies in this leaf; the previous leaf's max qualifies.
            self.prev_leaf();
        } else {
            self.pos = j - 1;
        }
    }

    /// Walk to the next leaf (forward): pop spine entries until one has an
    /// unvisited right child, descend it leftmost. Exhausts to `leaf = None`.
    fn next_leaf(&mut self) {
        loop {
            match self.stack.pop() {
                None => {
                    self.leaf = None;
                    return;
                }
                Some((node, descended)) => {
                    if descended + 1 < node.num_children() {
                        let next = descended + 1;
                        let child = node.child(next);
                        self.stack.push((node, next));
                        self.descend_first(child);
                        return;
                    }
                    // fully consumed; keep popping
                }
            }
        }
    }

    /// Walk to the previous leaf (reverse): mirror of `next_leaf`.
    fn prev_leaf(&mut self) {
        loop {
            match self.stack.pop() {
                None => {
                    self.leaf = None;
                    return;
                }
                Some((node, descended)) => {
                    if descended > 0 {
                        let next = descended - 1;
                        let child = node.child(next);
                        self.stack.push((node, next));
                        self.descend_last(child);
                        return;
                    }
                }
            }
        }
    }

    /// Ensure (leaf, pos) points at a valid element or `leaf = None` (forward).
    /// Handles a seek landing at end-of-leaf (element is in the next leaf).
    fn normalize_forward(&mut self) {
        while let Some(l) = &self.leaf {
            if self.pos < l.keys().len() {
                return;
            }
            self.next_leaf();
        }
    }

    fn normalize_reverse(&mut self) {
        while let Some(l) = &self.leaf {
            if !l.keys().is_empty() && self.pos < l.keys().len() {
                return;
            }
            self.prev_leaf();
        }
    }

    fn step(&mut self) {
        if self.reverse {
            // step back
            if self.pos == 0 {
                self.prev_leaf();
            } else {
                self.pos -= 1;
            }
        } else {
            // step forward
            self.pos += 1;
            let in_leaf = self
                .leaf
                .as_ref()
                .is_some_and(|l| self.pos < l.keys().len());
            if !in_leaf {
                self.next_leaf();
            }
        }
    }
}

impl RowStream for BTreeCursor {
    // The held-`Rc` backend vends a borrowed `&OwnedRow` straight out of the leaf
    // `Rc<BNode>` it owns — the *opposite* provenance to SQLite's step buffer,
    // yet the same trait. Vending `&OwnedRow` (not a bare slice) is what makes
    // `RowRef::to_owned_row` an O(1) `Arc` bump here (OQ-2), the one place this
    // backend differs in cost from the SQLite leaf (which must copy).
    type Row<'a> = &'a Row;

    fn next_row(&mut self) -> Option<Self::Row<'_>> {
        if self.started {
            self.step();
        } else {
            self.started = true;
        }
        self.current()
    }
}

// ---------------------------------------------------------------------------
// Structural diff — the optimistic-writes "rewind" primitive.
//
// A faithful port of the NIM `diffAgainst` dual-cursor reverse walk
// (`quasar-pulse/lq-query/lq-nivm/src/btree.nim:540`), the algorithm reference
// named in `OPTIMISTIC-WRITES-DESIGN.md` §2.1. It computes the row-level changes
// that turn `self` into `other` by walking both COW trees in lockstep, in
// DESCENDING sort order, using `Rc`/`Arc` pointer identity to skip the shared
// structure a `fork()` leaves behind — so the cost tracks *divergence*, not data
// size.
//
// Two invariants, both from design §2.2:
//   1. **Value-correct without identity.** The pointer skips are a pure
//      optimization: disabling them (or having them never fire) yields the
//      identical emitted change set, just computed by a full value scan. The leaf
//      path therefore value-compares rows (`full_row_equal`) before emitting an
//      edit, rather than trusting "the pointers differ" to mean "the content
//      differs". This makes correctness environment-independent (§2.2.1).
//   2. **Observable minimality.** [`DiffStats`] counts skips / visits / compares
//      so a performance oracle can assert that `fork + N point-mutations` does
//      O(N·height) work — a silent degradation to O(tree) is invisible to a
//      correctness test alone (§2.2.2).
// ---------------------------------------------------------------------------

/// Work counters for the structural diff — the performance oracle (design
/// §2.2.2). A `fork + N point-mutations` diff must show O(N·height) visits and
/// O(N) value compares; a regression (lost node sharing, or `ptr_eq` not firing
/// on some target) surfaces as visits proportional to *tree size* instead.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct DiffStats {
    /// Whole-subtree skips via `Rc::ptr_eq` on a shared internal node (the
    /// O(divergence) win).
    pub internal_node_skips: u64,
    /// Leaf rows skipped via `Arc::ptr_eq` (identical stored row → no value scan).
    pub leaf_row_skips: u64,
    /// Internal nodes actually descended into (NOT skipped).
    pub internal_nodes_visited: u64,
    /// Leaves actually descended into.
    pub leaves_visited: u64,
    /// Full-width row value comparisons (`full_row_equal`) — the O(columns) op.
    pub row_value_compares: u64,
    /// Cursor comparisons (`compare_cursors`).
    pub cursor_compares: u64,
}

/// Sink for [`BTree::diff_visit`]: one callback per difference, emitted in
/// descending sort order. "this" is `self`; "other" is the argument tree.
pub trait DiffSink {
    /// A row present in `self` but not in `other`.
    fn only_this(&mut self, row: &OwnedRow);
    /// A row present in `other` but not in `self`.
    fn only_other(&mut self, row: &OwnedRow);
    /// Equal sort-key but differing content: `this` (in `self`) → `other`.
    fn edit(&mut self, this: &OwnedRow, other: &OwnedRow);
}

impl BTree {
    /// Root-to-leaf edge count (a single-leaf root → 0). Walks the leftmost path;
    /// O(height). Used to set up a [`DiffCursor`]'s height normalization.
    fn height(&self) -> usize {
        let mut h = 0;
        let mut node = Rc::clone(&self.root);
        while let BNode::Internal { children, .. } = &*node {
            let c = Rc::clone(&children[0]);
            node = c;
            h += 1;
        }
        h
    }

    /// Visit the row-level differences that turn `self` into `other`, in
    /// descending sort order. Both trees MUST be ordered by `sort` (the diff is
    /// meaningless otherwise).
    ///
    /// `use_identity` enables the `ptr_eq` short-circuits; production callers pass
    /// `true` (see [`BTree::structural_diff`]). Passing `false` forces the full
    /// value walk — used by the differential test to prove the skips are a pure
    /// optimization (design §2.2.1). `stats` accumulates the work counters
    /// (§2.2.2); pass `&mut DiffStats::default()` if you don't care.
    pub fn diff_visit(
        &self,
        other: &BTree,
        sort: &Sort,
        use_identity: bool,
        stats: &mut DiffStats,
        sink: &mut dyn DiffSink,
    ) {
        let this_empty = self.size == 0;
        let other_empty = other.size == 0;
        if this_empty || other_empty {
            if this_empty && other_empty {
                return;
            }
            if this_empty {
                let mut c = DiffCursor::new(other);
                step_to_end(&mut c, stats, &mut |r| sink.only_other(r));
            } else {
                let mut c = DiffCursor::new(self);
                step_to_end(&mut c, stats, &mut |r| sink.only_this(r));
            }
            return;
        }

        let mut tc = DiffCursor::new(self);
        let mut oc = DiffCursor::new(other);
        let mut tok = true;
        let mut ook = true;
        // The previous step's cursor order; gates emission so converging cursors
        // are not double-counted (NIM `prevCursorOrder`).
        let mut prev = compare_cursors(&tc, &oc, sort, stats);

        while tok && ook {
            let order = compare_cursors(&tc, &oc, sort, stats);
            let tl = tc.leaf.is_some();
            let ol = oc.leaf.is_some();

            // Equal sort-key with BOTH cursors on leaf rows: pair them SYMMETRICALLY —
            // emit an edit iff the full rows differ, then advance BOTH cursors (the
            // linear-merge oracle's `i+=1; j+=1`). The `prev`-gated two-step pairing below
            // (advance `oc`, then advance `tc` next iteration under the gate) assumes ONE
            // row per sort key; on a run of k≥2 equal-key rows it mis-pairs the run and the
            // gate then bleeds into a later `Less` compare, emitting a spurious `Remove`
            // (adversarial-review HIGH #6 — value-identical multiset trees diverged). Handle
            // the leaf-pairing here so a duplicate-key run is consumed pairwise.
            if order == Ordering::Equal && tl && ol {
                let a = &tc.current_key;
                let b = &oc.current_key;
                if use_identity && Row::ptr_eq(a, b) {
                    stats.leaf_row_skips += 1;
                } else {
                    stats.row_value_compares += 1;
                    if !full_row_equal(a, b) {
                        sink.edit(a, b);
                    }
                }
                // Both rows are fully consumed — unlike the two-step pairing below, there is
                // no pending follow-up to suppress, so `prev` must be NON-Equal or the next
                // (independent) comparison's Less/Greater emission would be wrongly gated out.
                prev = Ordering::Less;
                tok = tc.step(false, stats);
                ook = oc.step(false, stats);
                continue;
            }

            if tl || ol {
                if prev != Ordering::Equal {
                    match order {
                        // Equal sort-key but NOT both on leaves (one cursor still on an
                        // internal node): nothing to emit here — the dual-cursor walk
                        // descends below (the leaf pairing is handled by the pre-check above).
                        Ordering::Equal => {}
                        // `other` is behind in the descending walk → it holds a row
                        // `self` does not.
                        Ordering::Greater => {
                            if ol {
                                sink.only_other(&oc.current_key);
                            }
                        }
                        // `self` is behind → it holds a row `other` does not.
                        Ordering::Less => {
                            if tl {
                                sink.only_this(&tc.current_key);
                            }
                        }
                    }
                }
            } else if order == Ordering::Equal
                && use_identity
                && Rc::ptr_eq(tc.current_node(), oc.current_node())
            {
                // Both cursors sit on the SAME internal node by reference: the whole
                // subtree is shared and identical → skip both past it.
                stats.internal_node_skips += 1;
                prev = Ordering::Equal;
                tok = tc.step(true, stats);
                ook = oc.step(true, stats);
                continue;
            }

            prev = order;
            if order == Ordering::Less {
                tok = tc.step(false, stats);
            } else {
                ook = oc.step(false, stats);
            }
        }

        // Exactly one cursor (at most) still has rows: drain it as only-this /
        // only-other.
        if tok {
            finish_walk(&mut tc, &oc, sort, stats, &mut |r| sink.only_this(r));
        }
        if ook {
            finish_walk(&mut oc, &tc, sort, stats, &mut |r| sink.only_other(r));
        }
    }

    /// The optimistic-writes "rewind" primitive: the [`SourceChange`]s that turn
    /// `self` into `other` (apply them to a source holding `self` and it becomes
    /// `other`). `only_this → Remove`, `only_other → Add`, `edit → Edit`. Both
    /// trees must be ordered by `sort`. Identity short-circuits are on.
    pub fn structural_diff(&self, other: &BTree, sort: &Sort) -> Vec<SourceChange> {
        struct Collect(Vec<SourceChange>);
        impl DiffSink for Collect {
            fn only_this(&mut self, row: &OwnedRow) {
                self.0.push(SourceChange::Remove(row.clone()));
            }
            fn only_other(&mut self, row: &OwnedRow) {
                self.0.push(SourceChange::Add(row.clone()));
            }
            fn edit(&mut self, this: &OwnedRow, other: &OwnedRow) {
                self.0.push(SourceChange::Edit {
                    old: this.clone(),
                    row: other.clone(),
                });
            }
        }
        let mut sink = Collect(Vec::new());
        let mut stats = DiffStats::default();
        self.diff_visit(other, sort, true, &mut stats, &mut sink);
        sink.0
    }
}

/// Apply one [`SourceChange`] to a COW tree: `Add` → [`BTree::add`], `Remove` →
/// [`BTree::delete`], `Edit` → delete `old` + add `row`. The inverse direction of
/// [`BTree::structural_diff`] (apply what a diff would produce and the tree becomes
/// the other one).
///
/// The write helper for the off-graph transient tree the optimistic rewind builds
/// ([`crate::optimistic`], `S' = fork(sync) + D`). Behavior is delete-old-then-add-new
/// for `Edit`, tolerating no existence assertions — this is a private transient tree,
/// not the live push path.
pub(crate) fn apply_source_change_to_tree(tree: &mut BTree, c: &SourceChange, sort: &Sort) {
    match c {
        SourceChange::Add(r) => {
            tree.add(r.clone(), sort);
        }
        SourceChange::Remove(r) => {
            tree.delete(r, sort);
        }
        SourceChange::Edit { old, row } => {
            tree.delete(old, sort);
            tree.add(row.clone(), sort);
        }
    }
}

/// Full-width row equality (every column, `null == null`) — the value-correct
/// basis for edit detection. Two rows equal under `sort` (same walk position) but
/// differing here are an EDIT; the `Arc::ptr_eq` leaf skip is merely a fast path
/// over this check (design §2.2.1).
fn full_row_equal(a: &Row, b: &Row) -> bool {
    a.len() == b.len()
        && a.cells()
            .zip(b.cells())
            .all(|(x, y)| compare_values(x, y) == Ordering::Equal)
}

/// A reverse-order (descending) walk cursor for the structural diff. Unlike
/// [`BTreeCursor`] it can pause ON an internal node — which is what lets the diff
/// skip a whole shared subtree — so it gets its own type. Owns `Rc`/`Arc` clones
/// along the spine, borrowing nothing external. Port of NIM `DiffCursor`.
struct DiffCursor {
    /// Root-to-leaf edge count of the tree (for height-normalized comparison).
    height: usize,
    /// `spine[level]` = the sibling array at that level; `spine[0] == [root]`.
    spine: Vec<Vec<Rc<BNode>>>,
    /// `indices[level]` = selected entry within `spine[level]`. When `leaf` is
    /// `Some`, a trailing extra entry indexes the current value within the leaf.
    indices: Vec<usize>,
    /// The leaf the cursor is inside, if any. `None` while sitting on an internal
    /// node (the state that enables the subtree skip).
    leaf: Option<Rc<BNode>>,
    /// `Arc` clone of the row/separator under the cursor — a cheap bump that
    /// preserves identity, so `Arc::ptr_eq` leaf skips work.
    current_key: Row,
}

impl DiffCursor {
    /// Position at the rightmost (maximum) element. Precondition: `tree` non-empty.
    fn new(tree: &BTree) -> DiffCursor {
        DiffCursor {
            height: tree.height(),
            spine: vec![vec![Rc::clone(&tree.root)]],
            indices: vec![0],
            leaf: None,
            current_key: tree.root.max_key().clone(),
        }
    }

    /// The internal node the cursor currently sits on. Valid only when `leaf` is
    /// `None` (the both-internal skip check is the only caller).
    fn current_node(&self) -> &Rc<BNode> {
        let last = self.spine.len() - 1;
        &self.spine[last][self.indices[last]]
    }

    /// Advance one step in the descending walk. `step_to_node = true` forces a jump
    /// past the entire current subtree (the internal-node skip). Returns `false`
    /// when the cursor would walk off the far-left end (no state change then).
    fn step(&mut self, step_to_node: bool, stats: &mut DiffStats) -> bool {
        if step_to_node || self.leaf.is_some() {
            let levels = self.indices.len();
            if step_to_node || self.indices[levels - 1] == 0 {
                // Step to the previous NODE: walk up to the deepest spine level that
                // still has an unvisited left sibling, move to it.
                let node_level = self.spine.len() - 1;
                let mut found = None;
                for k in (0..=node_level).rev() {
                    if self.indices[k] > 0 {
                        found = Some(k);
                        break;
                    }
                }
                match found {
                    None => false, // far-left end reached
                    Some(k) => {
                        // Drop the leaf value-index AND every internal level we
                        // ascended past, restoring `indices.len() == spine.len()`.
                        //
                        // (NIM pops `levelIndices` exactly ONCE here — correct only
                        // for a single-level walk-back. Multi-level ascents in deep
                        // trees need both `spine` and `indices` truncated together;
                        // truncating both to `k + 1` is the faithful generalization,
                        // confirmed by the differential oracle over deep trees.)
                        self.leaf = None;
                        self.spine.truncate(k + 1);
                        self.indices.truncate(k + 1);
                        self.indices[k] -= 1;
                        self.current_key = self.spine[k][self.indices[k]].max_key().clone();
                        true
                    }
                }
            } else {
                // Move to the previous value within the current leaf.
                self.indices[levels - 1] -= 1;
                let vi = self.indices[levels - 1];
                self.current_key = self.leaf.as_ref().unwrap().keys()[vi].clone();
                true
            }
        } else {
            // Descend into the currently-selected node, rightmost child / value
            // first.
            let level = self.spine.len() - 1;
            let node = Rc::clone(&self.spine[level][self.indices[level]]);
            if node.is_leaf() {
                stats.leaves_visited += 1;
                let vi = node.keys().len() - 1;
                self.current_key = node.keys()[vi].clone();
                self.indices.push(vi);
                self.leaf = Some(node);
            } else if let BNode::Internal { children, .. } = &*node {
                stats.internal_nodes_visited += 1;
                let ci = children.len() - 1;
                self.current_key = children[ci].max_key().clone();
                self.spine.push(children.clone());
                self.indices.push(ci);
            }
            true
        }
    }
}

/// Compare two cursors in the descending walk. Equal `current_key` ties break on
/// height-normalized depth: a cursor on a shallower internal node with the same
/// `maxKey` is "behind", so trees of differing heights land on shared nodes at
/// the same time — which is what lets the subtree skip fire (design §2.1).
fn compare_cursors(a: &DiffCursor, b: &DiffCursor, sort: &Sort, stats: &mut DiffStats) -> Ordering {
    stats.cursor_compares += 1;
    // Reversed key order: cursors advance in DESCENDING sort order.
    let key_cmp = compare_rows(sort, &b.current_key, &a.current_key);
    if key_cmp != Ordering::Equal {
        return key_cmp;
    }
    let hmin = a.height.min(b.height);
    let da = a.indices.len() as isize - (a.height - hmin) as isize;
    let db = b.indices.len() as isize - (b.height - hmin) as isize;
    da.cmp(&db)
}

/// Drive `c` to its far-left end, invoking `f` on every leaf row it passes
/// (NIM `stepToEnd`).
fn step_to_end(c: &mut DiffCursor, stats: &mut DiffStats, f: &mut dyn FnMut(&OwnedRow)) {
    loop {
        if c.leaf.is_some() {
            f(&c.current_key);
        }
        if !c.step(false, stats) {
            break;
        }
    }
}

/// Drain the still-running cursor `c` once the other (`finished`) has run off its
/// end (NIM `finishCursorWalk`): align past any shared boundary point, then emit
/// the remainder.
fn finish_walk(
    c: &mut DiffCursor,
    finished: &DiffCursor,
    sort: &Sort,
    stats: &mut DiffStats,
    f: &mut dyn FnMut(&OwnedRow),
) {
    match compare_cursors(c, finished, sort, stats) {
        Ordering::Equal => {
            if !c.step(false, stats) {
                return;
            }
        }
        // NIM raises here ("cursor walk terminated early") — an invariant
        // violation. Be defensive: assert in debug, drain in release.
        Ordering::Less => debug_assert!(false, "diff: cursor walk terminated early"),
        Ordering::Greater => {}
    }
    step_to_end(c, stats, f);
}

// ---------------------------------------------------------------------------
// Test-only COW structural-sharing introspection (rebase invariant Q1). `#[cfg(test)]`
// so it compiles ONLY for the crate's own unit tests (`btree.rs` / `optimistic.rs`) — never in a
// release, wasm, or separate integration-test build; zero production API surface or overhead.
// ---------------------------------------------------------------------------

#[cfg(test)]
impl BTree {
    /// Do `self` and `other` share the SAME COW root allocation (`Rc::ptr_eq`)? True iff one is a
    /// fork of the other with no intervening structural mutation — the maximal-sharing signal.
    pub(crate) fn shares_root(&self, other: &BTree) -> bool {
        Rc::ptr_eq(&self.root, &other.root)
    }

    /// The number of nodes in `self`'s tree NOT `Rc::ptr_eq`-shared with `other` — the rigorous
    /// structural-divergence measure. A COW fork + K path-mutations diverges by ≈ K·height nodes
    /// (the copied root→leaf paths); a from-scratch rebuild diverges by ALL of `self`'s nodes.
    pub(crate) fn unshared_node_count(&self, other: &BTree) -> usize {
        let mut shared = std::collections::HashSet::new();
        collect_node_ptrs(&other.root, &mut shared);
        count_unshared_nodes(&self.root, &shared)
    }
}

#[cfg(test)]
fn collect_node_ptrs(node: &Rc<BNode>, out: &mut std::collections::HashSet<usize>) {
    out.insert(Rc::as_ptr(node) as usize);
    if let BNode::Internal { children, .. } = &**node {
        for c in children {
            collect_node_ptrs(c, out);
        }
    }
}

#[cfg(test)]
fn count_unshared_nodes(node: &Rc<BNode>, shared: &std::collections::HashSet<usize>) -> usize {
    // A shared node's ENTIRE subtree is shared (COW: a node not copied ⇒ its children weren't) —
    // prune there.
    if shared.contains(&(Rc::as_ptr(node) as usize)) {
        return 0;
    }
    let mut n = 1;
    if let BNode::Internal { children, .. } = &**node {
        for c in children {
            n += count_unshared_nodes(c, shared);
        }
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change::SourceChange;
    use crate::value::owned_row as row;
    use crate::value::OwnedValue::{Int, Null};
    use crate::value::Value;

    fn s1() -> Sort {
        vec![(0, true)] // single asc key on col 0
    }

    /// Row whose col-0 key is `k` (the only column).
    fn rk(k: i64) -> Row {
        row(vec![Int(k)])
    }

    /// Extract the col-0 int from a borrowed row (vended as `&OwnedRow`).
    fn val(r: &Row) -> i64 {
        match r.col(0) {
            Value::Int(i) => i,
            other => panic!("expected Int, got {other:?}"),
        }
    }

    fn val_row(r: &Row) -> i64 {
        val(r)
    }

    // -- rebase invariant Q2: `structural_diff` is bounded by divergence, not tree size --

    /// A `(key, value)` row: key = col 0 (the sort key), value = col 1 (mutated for an Edit).
    fn kv(k: i64, v: i64) -> Row {
        row(vec![Int(k), Int(v)])
    }

    /// Total nodes the diff DESCENDED into (a `Rc::ptr_eq`-skipped subtree contributes 0 — that is
    /// the whole point). Reads the pre-existing [`DiffStats`] performance-oracle counters (design
    /// §2.2.2); `use_identity = false` is the built-in "skip off" switch (the full value walk).
    fn diff_visits(a: &BTree, b: &BTree, sort: &Sort, use_identity: bool) -> u64 {
        struct NullSink;
        impl DiffSink for NullSink {
            fn only_this(&mut self, _: &Row) {}
            fn only_other(&mut self, _: &Row) {}
            fn edit(&mut self, _: &Row, _: &Row) {}
        }
        let mut stats = DiffStats::default();
        let mut sink = NullSink;
        a.diff_visit(b, sort, use_identity, &mut stats, &mut sink);
        stats.internal_nodes_visited + stats.leaves_visited
    }

    #[test]
    fn q2_structural_diff_is_bounded_by_divergence() {
        let sort = s1();
        let n = 50_000i64;
        let tree = BTree::from_sorted((0..n).map(|k| kv(k, k)), &sort);
        assert_eq!(tree.len(), n as usize);

        // fork + ONE point-edit (same key, new value): path-copies exactly one root→leaf path.
        let mut s_prime = tree.fork();
        let mid = n / 2;
        assert!(s_prime.delete(&kv(mid, mid), &sort));
        assert!(s_prime.add(kv(mid, mid + 777), &sort));

        // (a) value-correct: exactly the one edit.
        let changes = tree.structural_diff(&s_prime, &sort);
        assert_eq!(
            changes.len(),
            1,
            "one changed key ⇒ one change: {changes:?}"
        );
        match &changes[0] {
            SourceChange::Edit { old, row } => {
                assert_eq!(val(old), mid, "the edited key");
                assert_eq!(val_row(row), mid, "key unchanged (value col differs)");
            }
            other => panic!("expected an Edit, got {other:?}"),
        }

        // (b) bounded: the identity walk descends O(log N) nodes, unambiguously << N.
        let visited = diff_visits(&tree, &s_prime, &sort, true);
        assert!(
            visited < 100,
            "fork + 1 change must visit O(log N) nodes, got {visited} at N={n}"
        );

        // The `Rc::ptr_eq` subtree-skip is LOAD-BEARING: the SAME diff with identity OFF (the full
        // value walk) descends EVERY node (≈ 2·N/leaf_size — linear in N, the whole tree), while the
        // identity-on walk stays O(log N). This contrast is the guard the `visited < 100` assertion
        // would catch if the skip ever stopped firing.
        let visited_full = diff_visits(&tree, &s_prime, &sort, false);
        assert!(
            visited_full > (n as u64) / 64,
            "identity OFF descends the whole tree (≈ N/64 nodes per cursor), linear in N: \
             {visited_full} at N={n}"
        );
        assert!(
            visited.saturating_mul(15) < visited_full,
            "the skip must cut visits by more than an order of magnitude: {visited} (on) vs \
             {visited_full} (off)"
        );
    }

    #[test]
    fn q2_diff_against_unmutated_fork_is_o1() {
        // Floor case: an unmutated fork shares the ROOT, so the ptr_eq root-skip fires immediately
        // — the diff is empty and visits O(1) (independent of N).
        let sort = s1();
        let tree = BTree::from_sorted((0..50_000i64).map(|k| kv(k, k)), &sort);
        let twin = tree.fork();
        assert!(tree.shares_root(&twin), "an unmutated fork shares the root");
        assert!(tree.structural_diff(&twin, &sort).is_empty());
        let visited = diff_visits(&tree, &twin, &sort, true);
        assert!(
            visited <= 2,
            "unmutated fork ⇒ root ptr_eq skip ⇒ O(1) visits, got {visited}"
        );
    }

    /// Drain a forward cursor to a Vec of col-0 ints (using the lending API).
    fn drain(cur: &mut BTreeCursor) -> Vec<i64> {
        let mut out = Vec::new();
        while let Some(r) = cur.next_row() {
            out.push(val(r));
        }
        out
    }

    // -- tiny deterministic PRNG (zero-dep) --
    struct Lcg(u64);
    impl Lcg {
        fn new(seed: u64) -> Lcg {
            Lcg(seed)
        }
        fn next(&mut self) -> u64 {
            // numerical recipes LCG
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 16
        }
    }

    #[test]
    fn empty_tree_scans_empty() {
        let t = BTree::new();
        assert_eq!(t.len(), 0);
        let mut cur = t.values_from(None, true, &s1());
        assert!(cur.next_row().is_none());
        let mut rev = t.values_from_reversed(None, true, &s1());
        assert!(rev.next_row().is_none());
    }

    #[test]
    fn add_get_has_basic() {
        let sort = s1();
        let mut t = BTree::new();
        for k in [5, 1, 3, 2, 4] {
            assert!(t.add(rk(k), &sort));
        }
        assert_eq!(t.len(), 5);
        assert!(t.has(&rk(3), &sort));
        assert!(!t.has(&rk(9), &sort));
        assert_eq!(t.get(&rk(4), &sort).map(val_row), Some(4));
        let mut cur = t.values_from(None, true, &sort);
        assert_eq!(drain(&mut cur), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn add_existing_overwrites_not_grows() {
        // key = col 0; col 1 is payload. Equal col-0 ⇒ overwrite the slot.
        let sort = vec![(0, true)];
        let mut t = BTree::new();
        assert!(t.add(row(vec![Int(1), Int(100)]), &sort));
        assert!(!t.add(row(vec![Int(1), Int(200)]), &sort)); // existed
        assert_eq!(t.len(), 1);
        let got = t.get(&row(vec![Int(1), Null]), &sort).unwrap();
        assert_eq!(
            match got.col(1) {
                Value::Int(i) => i,
                _ => panic!(),
            },
            200,
            "payload should be overwritten"
        );
    }

    #[test]
    fn overwrite_of_separator_max_refreshes_cached_payload() {
        // Regression for the JS-divergence the adversarial workflow found: when
        // add() overwrites a row that is a leaf's max (and thus a parent
        // separator), the new payload must propagate into the cached separator
        // (JS refreshes keys[i] unconditionally). Sort by col 0 only; col 1 is
        // payload, so an overwrite changes a non-sort column.
        let sort = vec![(0, true)];
        let mut t = BTree::new();
        for k in 0..=(MAX_NODE_SIZE as i64) {
            t.add(row(vec![Int(k), Int(1)]), &sort); // payload 1; forces a split
        }
        assert!(!t.root.is_leaf(), "need an internal root with separators");
        // Overwrite every key with payload 2 (covers whichever keys are separators).
        for k in 0..=(MAX_NODE_SIZE as i64) {
            assert!(!t.add(row(vec![Int(k), Int(2)]), &sort), "should overwrite");
        }
        // The strengthened invariant (Arc-identical cached max) must hold: pre-fix,
        // a separator kept the old payload-1 Arc while its leaf held payload-2.
        t.check_invariants(&sort)
            .expect("cached separators must be refreshed");
        // And every separator in the root must carry the new payload.
        if let BNode::Internal { keys, .. } = &*t.root {
            for sep in keys {
                assert_eq!(
                    match sep.col(1) {
                        Value::Int(p) => p,
                        _ => panic!(),
                    },
                    2,
                    "separator payload went stale"
                );
            }
        }
    }

    #[test]
    fn split_grows_internal_root() {
        let sort = s1();
        let mut t = BTree::new();
        assert!(t.root.is_leaf());
        for k in 0..=(MAX_NODE_SIZE as i64) {
            t.add(rk(k), &sort); // MAX+1 inserts ⇒ at least one split
        }
        assert!(!t.root.is_leaf(), "root should be internal after overflow");
        assert_eq!(t.len(), MAX_NODE_SIZE + 1);
        let mut cur = t.values_from(None, true, &sort);
        assert_eq!(
            drain(&mut cur),
            (0..=(MAX_NODE_SIZE as i64)).collect::<Vec<_>>()
        );
    }

    #[test]
    fn fork_is_o1_and_independent() {
        let sort = s1();
        let mut a = BTree::new();
        for k in 0..2000 {
            a.add(rk(k), &sort);
        }
        let b = a.fork();
        // Shared root immediately after fork.
        assert!(Rc::ptr_eq(&a.root, &b.root), "fork shares the root Arc");
        // Mutating `a` must not disturb `b` (COW).
        a.add(rk(10_000), &sort);
        a.delete(&rk(0), &sort);
        assert!(!Rc::ptr_eq(&a.root, &b.root), "write forked the root");
        assert_eq!(b.len(), 2000);
        assert!(b.has(&rk(0), &sort) && !b.has(&rk(10_000), &sort));
        assert!(!a.has(&rk(0), &sort) && a.has(&rk(10_000), &sort));
    }

    #[test]
    fn make_mut_copies_only_the_path() {
        let sort = s1();
        let mut a = BTree::new();
        for k in 0..2000 {
            a.add(rk(k), &sort); // multi-level tree
        }
        let mut c = a.fork();
        c.add(rk(10_000), &sort); // larger than all ⇒ rightmost path only
        assert!(!Rc::ptr_eq(&a.root, &c.root), "root copied on write");
        match (&*a.root, &*c.root) {
            (BNode::Internal { children: ac, .. }, BNode::Internal { children: cc, .. }) => {
                assert!(
                    Rc::ptr_eq(&ac[0], &cc[0]),
                    "off-path (leftmost) subtree must stay SHARED"
                );
                assert!(
                    !Rc::ptr_eq(ac.last().unwrap(), cc.last().unwrap()),
                    "on-path (rightmost) subtree must be copied"
                );
            }
            _ => panic!("expected internal roots for a 2000-row tree"),
        }
    }

    #[test]
    fn snapshot_stable_under_mid_iteration_write() {
        // The reentrancy invariant (§6): a cursor opened before a write keeps
        // yielding the pre-write snapshot, because it pins the old root Arc and
        // the writer `make_mut`s a fresh path. This is the COW analogue of the
        // spike's `self_referential_add_needs_source_overlay`.
        let sort = s1();
        let mut t = BTree::new();
        for k in 0..100 {
            t.add(rk(k), &sort);
        }
        let mut cur = t.values_from(None, true, &sort);
        let mut seen = Vec::new();
        for _ in 0..10 {
            seen.push(val(cur.next_row().unwrap())); // read 0..10
        }
        // Mutate the SAME tree mid-iteration.
        for k in 100..150 {
            t.add(rk(k), &sort);
        }
        assert!(t.delete(&rk(0), &sort));
        assert!(t.delete(&rk(50), &sort));
        // The cursor must still see the ORIGINAL 0..100 (incl. 50, excl. 100+).
        while let Some(r) = cur.next_row() {
            seen.push(val(r));
        }
        assert_eq!(
            seen,
            (0..100).collect::<Vec<_>>(),
            "cursor saw the snapshot"
        );
        // The tree itself reflects the writes.
        let after: Vec<i64> = {
            let mut c = t.values_from(None, true, &sort);
            drain(&mut c)
        };
        assert!(after.contains(&149) && !after.contains(&0) && !after.contains(&50));
        assert_eq!(t.len(), 100 + 50 - 2);
    }

    #[test]
    fn to_owned_row_bumps_not_copies() {
        // OQ-2, folded into the shared `RowRef`: the memory cursor vends a
        // `&OwnedRow`, so `to_owned_row()` is a refcount bump (SAME allocation as
        // the stored row); rebuilding from a bare slice deep-copies (different
        // allocation). The SAME trait method that COPIES on the SQLite leaf is a
        // BUMP here — the one-trait-two-costs result.
        let sort = s1();
        let mut t = BTree::new();
        let r = rk(7);
        t.add(r.clone(), &sort);
        let mut cur = t.values_from(None, true, &sort);
        let row_ref: &Row = cur.next_row().unwrap();
        // to_owned_row on the vended `&OwnedRow` bumps the stored Arc.
        let bumped = row_ref.to_owned_row();
        assert!(
            Row::ptr_eq(&bumped, &r),
            "to_owned_row on a `&OwnedRow` must bump the stored Arc, not allocate"
        );
        // For contrast: the `&[OwnedValue]` slice impl builds a fresh flat row
        // (loses buffer identity) — exactly why the cursor vends `&OwnedRow`.
        let cells = r.to_value_vec();
        let slice: &[OwnedValue] = &cells;
        let rebuilt: Row = slice.to_owned_row();
        assert!(
            !Row::ptr_eq(&rebuilt, &r),
            "slice.to_owned_row() rebuilds (loses buffer identity)"
        );
    }

    #[test]
    fn values_from_seeks() {
        let sort = s1();
        let mut t = BTree::new();
        for k in (0..200).map(|i| i * 2) {
            t.add(rk(k), &sort); // evens 0..=398
        }
        // first >= 51 is 52
        let b = row_bound_of(&rk(51), &sort);
        let mut cur = t.values_from(Some(&b), true, &sort);
        assert_eq!(val(cur.next_row().unwrap()), 52);
        assert_eq!(val(cur.next_row().unwrap()), 54);
        // Min sentinel ⇒ from the very start
        let bmin: RowBound = vec![(0, Bound::Min)];
        let mut c2 = t.values_from(Some(&bmin), true, &sort);
        assert_eq!(val(c2.next_row().unwrap()), 0);
        // Max sentinel ⇒ past the end ⇒ empty
        let bmax: RowBound = vec![(0, Bound::Max)];
        let mut c3 = t.values_from(Some(&bmax), true, &sort);
        assert!(c3.next_row().is_none());
        // exact hit is inclusive
        let bhit = row_bound_of(&rk(100), &sort);
        let mut c4 = t.values_from(Some(&bhit), true, &sort);
        assert_eq!(val(c4.next_row().unwrap()), 100);
    }

    #[test]
    fn reverse_scans_descending() {
        let sort = s1();
        let mut t = BTree::new();
        for k in 0..500 {
            t.add(rk(k), &sort);
        }
        let mut cur = t.values_from_reversed(None, true, &sort);
        let got = drain(&mut cur);
        assert_eq!(got, (0..500).rev().collect::<Vec<_>>());
    }

    #[test]
    fn from_sorted_matches_repeated_add() {
        let sort = s1();
        let rows: Vec<Row> = (0..1000).map(rk).collect();
        let t = BTree::from_sorted(rows.into_iter(), &sort);
        assert_eq!(t.len(), 1000);
        let mut cur = t.values_from(None, true, &sort);
        assert_eq!(drain(&mut cur), (0..1000).collect::<Vec<_>>());
        // and lookups work through the bulk-built internal nodes
        assert!(t.has(&rk(0), &sort) && t.has(&rk(999), &sort) && !t.has(&rk(1000), &sort));
    }

    #[test]
    fn differential_against_sorted_vec_oracle() {
        // Random add/delete sequence; assert contents + return-flags match a
        // sorted-Vec set oracle under the same comparator. Full-row key (col 0),
        // so compare==Equal ⇔ identical.
        let sort = s1();
        let mut t = BTree::new();
        let mut oracle: Vec<i64> = Vec::new();
        let mut rng = Lcg::new(0x00C0_FFEE_D00D);
        for _ in 0..20_000 {
            let k = (rng.next() % 600) as i64;
            if rng.next() & 1 == 0 {
                let newly = t.add(rk(k), &sort);
                match oracle.binary_search(&k) {
                    Ok(_) => assert!(!newly, "add of existing {k} should return false"),
                    Err(i) => {
                        assert!(newly, "add of new {k} should return true");
                        oracle.insert(i, k);
                    }
                }
            } else {
                let removed = t.delete(&rk(k), &sort);
                match oracle.binary_search(&k) {
                    Ok(i) => {
                        assert!(removed, "delete of present {k} should return true");
                        oracle.remove(i);
                    }
                    Err(_) => assert!(!removed, "delete of absent {k} should return false"),
                }
            }
            assert_eq!(t.len(), oracle.len());
            // Structural invariants must hold after EVERY mutation.
            t.check_invariants(&sort).expect("invariants after op");
        }
        // In-order traversal equals the oracle.
        let mut cur = t.values_from(None, true, &sort);
        assert_eq!(drain(&mut cur), oracle);
        // Reverse equals reversed oracle.
        let mut rev = t.values_from_reversed(None, true, &sort);
        let mut want_rev = oracle.clone();
        want_rev.reverse();
        assert_eq!(drain(&mut rev), want_rev);
        // has/get parity across the whole key space.
        for k in 0..600 {
            assert_eq!(t.has(&rk(k), &sort), oracle.binary_search(&k).is_ok());
        }
        // Seek parity: for each k, values_from(>=k) first element matches.
        for k in 0..600 {
            let b = row_bound_of(&rk(k), &sort);
            let mut c = t.values_from(Some(&b), true, &sort);
            let got = c.next_row().map(val);
            let want = oracle.iter().find(|&&x| x >= k).copied();
            assert_eq!(got, want, "seek >= {k}");
        }
    }

    #[test]
    fn fork_isolation_both_directions() {
        // Fork, then mutate BOTH copies in different ways; each must reflect ONLY
        // its own mutations (structural sharing + COW isolation).
        let sort = s1();
        let mut a = BTree::new();
        for k in 0..300 {
            a.add(rk(k), &sort);
        }
        let mut b = a.fork();
        // a: remove evens in [0,100); b: add 1000..1100 and remove 200..250.
        for k in (0..100).filter(|k| k % 2 == 0) {
            a.delete(&rk(k), &sort);
        }
        for k in 1000..1100 {
            b.add(rk(k), &sort);
        }
        for k in 200..250 {
            b.delete(&rk(k), &sort);
        }
        a.check_invariants(&sort).unwrap();
        b.check_invariants(&sort).unwrap();

        let a_want: Vec<i64> = (0..300).filter(|k| !(*k < 100 && k % 2 == 0)).collect();
        let mut b_want: Vec<i64> = (0..300).filter(|k| !(200..250).contains(k)).collect();
        b_want.extend(1000..1100);
        let mut ca = a.values_from(None, true, &sort);
        let mut cb = b.values_from(None, true, &sort);
        assert_eq!(drain(&mut ca), a_want);
        assert_eq!(drain(&mut cb), b_want);
    }

    #[test]
    fn reverse_bounded_seek_inclusive_and_exclusive() {
        let sort = s1();
        let mut t = BTree::new();
        for k in (0..400).map(|i| i * 2) {
            t.add(rk(k), &sort); // evens 0..=798
        }
        // inclusive: largest <= 101 is 100, then descending
        let b = row_bound_of(&rk(101), &sort);
        let mut cur = t.values_from_reversed(Some(&b), true, &sort);
        assert_eq!(val(cur.next_row().unwrap()), 100);
        assert_eq!(val(cur.next_row().unwrap()), 98);
        // inclusive at an exact element: includes it
        let bhit = row_bound_of(&rk(100), &sort);
        let mut c2 = t.values_from_reversed(Some(&bhit), true, &sort);
        assert_eq!(val(c2.next_row().unwrap()), 100);
        // exclusive at an exact element: skips it
        let mut c3 = t.values_from_reversed(Some(&bhit), false, &sort);
        assert_eq!(val(c3.next_row().unwrap()), 98);
        // below the minimum ⇒ empty
        let bneg = row_bound_of(&rk(-5), &sort);
        let mut c4 = t.values_from_reversed(Some(&bneg), true, &sort);
        assert!(c4.next_row().is_none());
        // above the maximum ⇒ starts at the max
        let bbig = row_bound_of(&rk(10_000), &sort);
        let mut c5 = t.values_from_reversed(Some(&bbig), true, &sort);
        assert_eq!(val(c5.next_row().unwrap()), 798);
    }

    #[test]
    fn forward_exclusive_seek() {
        let sort = s1();
        let mut t = BTree::new();
        for k in 0..200 {
            t.add(rk(k), &sort);
        }
        let bhit = row_bound_of(&rk(50), &sort);
        // inclusive includes 50
        let mut ci = t.values_from(Some(&bhit), true, &sort);
        assert_eq!(val(ci.next_row().unwrap()), 50);
        // exclusive skips 50 → 51
        let mut ce = t.values_from(Some(&bhit), false, &sort);
        assert_eq!(val(ce.next_row().unwrap()), 51);
    }

    #[test]
    fn reverse_bounded_seek_differential() {
        // Reverse-from-k must equal the oracle's "<= k, descending" for every k,
        // across a tree shaped by random churn (exercises seek + prev-leaf walks).
        let sort = s1();
        let mut t = BTree::new();
        let mut oracle: Vec<i64> = Vec::new();
        let mut rng = Lcg::new(0xBEEF_1234_5678);
        for _ in 0..8_000 {
            let k = (rng.next() % 500) as i64;
            if rng.next() & 1 == 0 {
                if t.add(rk(k), &sort) {
                    let i = oracle.binary_search(&k).unwrap_err();
                    oracle.insert(i, k);
                }
            } else if t.delete(&rk(k), &sort) {
                let i = oracle.binary_search(&k).unwrap();
                oracle.remove(i);
            }
        }
        t.check_invariants(&sort).unwrap();
        for k in -2..502 {
            let b = row_bound_of(&rk(k), &sort);
            // inclusive
            let mut c = t.values_from_reversed(Some(&b), true, &sort);
            let got: Vec<i64> = std::iter::from_fn(|| c.next_row().map(val)).collect();
            let want: Vec<i64> = oracle.iter().rev().filter(|&&x| x <= k).copied().collect();
            assert_eq!(got, want, "reverse inclusive <= {k}");
            // exclusive
            let mut ce = t.values_from_reversed(Some(&b), false, &sort);
            let got_e: Vec<i64> = std::iter::from_fn(|| ce.next_row().map(val)).collect();
            let want_e: Vec<i64> = oracle.iter().rev().filter(|&&x| x < k).copied().collect();
            assert_eq!(got_e, want_e, "reverse exclusive < {k}");
        }
    }

    #[test]
    fn delete_keeps_occupancy_reasonable() {
        // After heavy deletes the JS opportunistic tryMerge keeps the tree compact
        // (the old lazy-prune left one sparse node per surviving key). Note JS does
        // NOT globally minimize — tryMerge is local to each delete site — so we
        // assert a *bound*, not zero-mergeable-pairs (that is not a theorem).
        let sort = s1();
        let mut t = BTree::new();
        for k in 0..5000 {
            t.add(rk(k), &sort);
        }
        for k in 0..5000 {
            if k % 10 != 0 {
                t.delete(&rk(k), &sort); // delete 90%
            }
        }
        assert_eq!(t.len(), 500);
        t.check_invariants(&sort).unwrap();
        // 500 keys: the optimal leaf count is ~16 (500/32). A compact tree stays
        // within a small multiple; a degenerate (un-merged) tree would have
        // hundreds of nodes. Assert well under that.
        let (nodes, leaves, depth) = node_stats(&t.root);
        assert!(
            leaves <= 60,
            "expected compact tree after deletes, got {leaves} leaves ({nodes} nodes, depth {depth})"
        );
        // depth for 500 elems should be 2 (root + leaves) or at most 3.
        assert!(depth <= 3, "tree too deep after deletes: depth {depth}");
    }

    /// (total nodes, leaf count, depth) for occupancy assertions.
    fn node_stats(node: &BNode) -> (usize, usize, usize) {
        match node {
            BNode::Leaf { .. } => (1, 1, 1),
            BNode::Internal { children, .. } => {
                let mut nodes = 1;
                let mut leaves = 0;
                let mut depth = 0;
                for c in children {
                    let (n, l, d) = node_stats(c);
                    nodes += n;
                    leaves += l;
                    depth = depth.max(d);
                }
                (nodes, leaves, depth + 1)
            }
        }
    }
}
