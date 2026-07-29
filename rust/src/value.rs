//! The canonical value/row model — the union that graduates into
//! `01-foundations` §2–§3, refined by the SQLite step-buffer lifetime (the
//! forcing function). One model serves BOTH leaf backends:
//!
//! - the **borrowed-step-buffer** rusqlite cursor (`sqlite.rs`), whose column
//!   pointers are valid only until the next step, and
//! - the **held-`Rc`** [`crate::btree::BTreeCursor`], which vends a row borrowed
//!   from an `Rc<BNode>` snapshot it owns.
//!
//! Three things are encoded structurally:
//!
//! 1. **A borrowed [`Value<'a>`] (zero-copy, `Copy`) split from an owned
//!    [`OwnedValue`].** The transient cursor path flows `Value<'a>`; the only
//!    heap allocation is [`Value::to_owned`], at the well-defined escape points
//!    the borrow checker *forces* (and the JS already copies at).
//! 2. **`Str`/`Json` carry bytes (`&[u8]`), not `&str`.** SQLite's default
//!    `BINARY` collation is a bytewise `memcmp`, so the comparator needs no
//!    UTF-8 validation; the fallible, scanning `&[u8]`→`&str` step is deferred
//!    to the app escape (`to_owned`). This is a deliberate refinement of
//!    foundations §2.2 (which wrote `&'a str`) driven by the SQLite reality:
//!    TEXT is not guaranteed UTF-8, and the validation scan is pure waste on the
//!    pass-through path.
//! 3. **Two distinct, undirivable comparators.** `compare_values` treats
//!    `null == null` (sorting); `values_equal` treats `null != null` (join
//!    matching, SQL semantics). Deriving `Ord`/`PartialEq` would make calling
//!    the wrong one trivially easy; named free functions make that bug hard to
//!    write. (Maps to `zql/src/ivm/data.ts` `compareValues` vs `valuesEqual`.)

use std::cmp::Ordering;
use std::sync::Arc;

use crate::error::RindleError;

/// A column index into a row. Names are resolved to indices once, at
/// pipeline-build time; the hot path never sees a string.
pub type ColId = usize;

/// A resolved **relationship slot** — the index of a relationship in its parent
/// table's [`Schema::relationships`]. Relationship *names* are resolved to a
/// `RelId` once, at build time ([`Schema::rel_slot`]); the hot path
/// ([`Change::Child`](crate::change::Change)`.rel`,
/// [`Relationship`](crate::change::Relationship)`.slot`) carries the index, never
/// a string (foundations §3.4, "never a string map"). A distinct newtype from
/// [`ColId`] so the two index spaces can't be confused.
///
/// `Ord` is derived so a slot can key an ordered map (the testkit's
/// `CaughtNode.relationships` is a `BTreeMap<RelId, _>` for slot-stable diffs);
/// the order is the declared-relationship order, which is meaningful.
///
/// Serde (gated, like [`crate::ast::Ast`]) lets a [`RelId`] slot cross the wire in a
/// flat change (`FLAT-CHANGES-DESIGN.md`); the newtype serializes transparently as its
/// inner `u32`.
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RelId(pub u32);

impl RelId {
    #[inline]
    pub fn ix(self) -> usize {
        self.0 as usize
    }
}

// ---------------------------------------------------------------------------
// Values — the borrowed/owned split
// ---------------------------------------------------------------------------

/// A cell value as seen **while streaming**. `Str`/`Json` borrow raw bytes from
/// the underlying buffer — a SQLite column-text pointer (valid only until the
/// next step) or the bytes inside an owned row — so the whole value is valid
/// only for `'a`.
///
/// `Value<'a>: Copy`: it is just a tag plus a primitive or a fat pointer, so
/// passing it around the hot path is free. `Str`/`Json` are `&[u8]`, not `&str`
/// (see the module docs): bytewise compare on the hot path, validate at the
/// escape.
#[derive(Clone, Copy, Debug)]
pub enum Value<'a> {
    /// **Absent** — the cell does not exist on this (partial / projected) row, as
    /// distinct from present-and-[`Value::Null`]. The borrowed mirror of
    /// [`OwnedValue::Absent`] (`PROJECTION-SUPPORT-DESIGN.md` §3.1). It is the unique
    /// minimum in [`compare_values`] (below `Null`) so the shared source's secondary
    /// indexes stay totally ordered over partial union rows; it is never met on a
    /// column a query sorts/joins/filters by (those are presence-required, §2.1), and
    /// is never produced on the server (full rows) nor delivered to a view.
    Absent,
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    /// Borrowed UTF-8-ish text, zero-copy from the step buffer; compared bytewise.
    Str(&'a [u8]),
    /// Borrowed **unparsed** JSON text; parsed lazily only if compared/needed.
    Json(&'a [u8]),
}

/// Owned counterpart, used wherever a value must outlive the cursor step that
/// produced it: overlays, the memory store (the COW B+tree), the view, and
/// operator-buffered state (Take/Exists, merge-heap heads).
///
/// `Str` is `Arc<str>` (validated UTF-8): sharing an owned string is a refcount
/// bump, and the owned form is the app-facing one, so it is the right place to
/// have paid for validation. (Foundations §2.2 allows `Box<str>`; `Arc<str>`
/// wins here because owned rows are aliased pervasively.)
///
/// **Serde (gated, like [`crate::ast::Ast`]) is the flat-change wire encoding**
/// (`FLAT-CHANGES-DESIGN.md` §5.1). It is **externally tagged** (`"Null"`,
/// `{"Int":5}`, `{"Str":"x"}`, `{"Json":"…"}`) — *not* the lossy JS-marshal collapse
/// (`Int`/`Float`→number, `Str`/`Json`→string). Faithful `ArrayView` reconstruction
/// requires the exact variant: a receiver's [`compare_values`] panics on a cross-type
/// pair (e.g. `Str` vs `Json`) and orders `Int`/`Float` by distinct arms, so the wire
/// must preserve which variant a cell is.
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug)]
pub enum OwnedValue {
    /// **Absent** — the cell does not exist on this (partial / projected) union row.
    /// The source-of-truth presence signal for a positional row
    /// (`PROJECTION-SUPPORT-DESIGN.md` §3.1, D-1): a positional array cannot otherwise
    /// distinguish *present-and-`Null`* from *absent*. Containable — after a
    /// connection's presence predicate (§3.3) admits a row, `Absent` survives only in
    /// columns that query does not read, which are projected away at the view boundary.
    /// **Never constructed on the server** (its rows are always complete) and **never
    /// marshalled to a view** (`owned_value_to_js` asserts this).
    Absent,
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(
        #[cfg_attr(
            any(feature = "testkit", feature = "serde"),
            serde(with = "arc_str_serde")
        )]
        Arc<str>,
    ),
    /// Raw json text (unparsed; a parse cache would be an impl detail).
    Json(
        #[cfg_attr(
            any(feature = "testkit", feature = "serde"),
            serde(with = "arc_str_serde")
        )]
        Arc<str>,
    ),
}

/// serde for an `Arc<str>` cell ([`OwnedValue`]'s `Str`/`Json`): serialize as a plain
/// string, deserialize back into a fresh `Arc<str>`. Hand-rolled so we need not enable
/// serde's crate-wide `rc` feature (with its sharing/DoS caveats) just for these cells.
#[cfg(any(feature = "testkit", feature = "serde"))]
mod arc_str_serde {
    use std::sync::Arc;
    pub fn serialize<S: serde::Serializer>(v: &Arc<str>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(v)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Arc<str>, D::Error> {
        let s = <String as serde::Deserialize>::deserialize(d)?;
        Ok(Arc::from(s.as_str()))
    }
}

impl OwnedValue {
    /// Convenience constructor for tests/util.
    pub fn str(s: &str) -> OwnedValue {
        OwnedValue::Str(Arc::from(s))
    }

    pub fn is_null(&self) -> bool {
        matches!(self, OwnedValue::Null)
    }

    /// Whether this cell is [`OwnedValue::Absent`] — the column-presence test
    /// (`PROJECTION-SUPPORT-DESIGN.md` §3.1). Presence is `!is_absent()`.
    #[inline]
    pub fn is_absent(&self) -> bool {
        matches!(self, OwnedValue::Absent)
    }

    /// Owned → borrowed bridge. **Zero copy:** hands out a byte view into the
    /// owned storage (and a `Copy` scalar otherwise). This is what lets the
    /// memory backend, overlays, and buffered state reach `Value<'_>` (and the
    /// comparators) without re-allocating.
    #[inline]
    pub fn as_ref(&self) -> Value<'_> {
        match self {
            OwnedValue::Absent => Value::Absent,
            OwnedValue::Null => Value::Null,
            OwnedValue::Bool(b) => Value::Bool(*b),
            OwnedValue::Int(i) => Value::Int(*i),
            OwnedValue::Float(f) => Value::Float(*f),
            OwnedValue::Str(s) => Value::Str(s.as_bytes()),
            OwnedValue::Json(s) => Value::Json(s.as_bytes()),
        }
    }
}

impl Value<'_> {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Whether this cell is [`Value::Absent`] (the borrowed presence test).
    #[inline]
    pub fn is_absent(&self) -> bool {
        matches!(self, Value::Absent)
    }

    /// Borrowed → owned. **THE allocation site** — the only place a transient
    /// cell becomes heap-resident. For `Str`/`Json` this also performs the UTF-8
    /// validation the bytes representation deferred (the app-escape boundary).
    #[inline]
    pub fn try_to_owned(&self) -> Result<OwnedValue, RindleError> {
        Ok(match self {
            Value::Absent => OwnedValue::Absent,
            Value::Null => OwnedValue::Null,
            Value::Bool(b) => OwnedValue::Bool(*b),
            Value::Int(i) => OwnedValue::Int(*i),
            Value::Float(f) => OwnedValue::Float(*f),
            Value::Str(b) => {
                OwnedValue::Str(Arc::from(std::str::from_utf8(b).map_err(|source| {
                    RindleError::InvalidUtf8 {
                        value_type: "TEXT",
                        source,
                    }
                })?))
            }
            Value::Json(b) => {
                OwnedValue::Json(Arc::from(std::str::from_utf8(b).map_err(|source| {
                    RindleError::InvalidUtf8 {
                        value_type: "JSON",
                        source,
                    }
                })?))
            }
        })
    }

    /// Infallible compatibility wrapper. Production call sites that handle
    /// external data should use [`Value::try_to_owned`].
    #[inline]
    pub fn to_owned(&self) -> OwnedValue {
        self.try_to_owned()
            .expect("borrowed value could not be materialized")
    }
}

// ---------------------------------------------------------------------------
// The two comparators — over `Value<'_>`, deliberately not derived
// ---------------------------------------------------------------------------

/// SORT / identity-of-storage comparison. `null == null` (Equal). Total order:
/// null < everything. Floats use `total_cmp` (NOT subtraction — avoids the JS
/// `a - b` overflow/NaN trap). Strings/json compare bytewise == SQLite `BINARY`
/// collation. `Int`/`Float` are the **same JS `number` domain** and compare by
/// widening to `f64` (JS has no int/float split — the distinction is a Rust-port
/// artifact, so a mixed pair is not a type error). It arises in practice when an AST
/// paging bound — integral literals lower to `Int` — is sorted against a `Number`
/// column that vends as `Float` (e.g. a SQLite leaf). A pair of *genuinely* different
/// JS types (number vs string, …) is still a builder bug and panics, mirroring the JS
/// oracle's `compareValues` throw.
#[inline]
pub fn compare_values(a: Value<'_>, b: Value<'_>) -> Ordering {
    use Value::*;
    match (a, b) {
        // `Absent` is the unique minimum, below `Null` — keeps the shared source's
        // secondary indexes totally ordered over partial union rows (§2.1, §3.1). An
        // `Absent`-keyed row sorts to the bottom; any query that orders by that column
        // presence-filters the row out of its own output anyway.
        (Absent, Absent) => Ordering::Equal,
        (Absent, _) => Ordering::Less,
        (_, Absent) => Ordering::Greater,
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Bool(x), Bool(y)) => x.cmp(&y),
        (Int(x), Int(y)) => x.cmp(&y),
        (Float(x), Float(y)) => x.total_cmp(&y),
        // One number domain, compared EXACTLY (design 226 §5.1): mathematical order,
        // never `x as f64` widening — two distinct i64s above 2^53 must never compare
        // Equal. Below 2^53 this returns bitwise the same Ordering the old widened
        // comparison did (pinned by `exact_order_matches_widened_below_2_53`).
        (Int(x), Float(y)) => compare_int_f64(x, y),
        (Float(x), Int(y)) => compare_int_f64(y, x).reverse(),
        (Str(x), Str(y)) => x.cmp(y),
        (Json(x), Json(y)) => x.cmp(y),
        _ => panic!("compare_values: type mismatch between {a:?} and {b:?}"),
    }
}

/// Exact `Int`↔`Float` comparison (design 226 §5.1): compare **mathematical values**,
/// with the two `total_cmp` placements preserved bit-for-bit so no existing data
/// reorders — `Int(i)` carries `+0.0`'s sign (`Int(0)` > `Float(-0.0)`), and NaNs
/// keep their `total_cmp` positions (`-NaN` below every `Int`, `+NaN` above).
///
/// Never widens `x` through `as f64`: above 2^53 that rounding made two distinct
/// `Int`s compare Equal to the same `Float` (a non-transitive relation — §3), which
/// is exactly the silent-wrong-rows failure this replaces.
#[inline]
pub fn compare_int_f64(x: i64, y: f64) -> Ordering {
    /// 2^63 — the first f64 too large for i64 (`i64::MIN`'s magnitude; `i64::MAX`
    /// rounds UP to this, so `>=` is the correct exclusion).
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    if y.is_nan() {
        // total_cmp: +NaN above +inf, -NaN below -inf — so above/below every i64.
        return if y.is_sign_positive() {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    // Out-of-i64-range magnitudes (±inf included) order by the float's sign: every
    // i64 lies in [-2^63, 2^63).
    if y >= TWO_POW_63 {
        return Ordering::Less;
    }
    if y < -TWO_POW_63 {
        return Ordering::Greater;
    }
    // y is finite in [-2^63, 2^63): trunc() is integral in the same range, so the
    // cast is exact.
    let t = y.trunc();
    let ty = t as i64;
    match x.cmp(&ty) {
        Ordering::Equal => {
            // Same integer part: the fractional part decides; a true tie is Equal —
            // except `Int(0)` vs `Float(-0.0)`, which keeps total_cmp's Greater.
            if y > t {
                Ordering::Less
            } else if y < t || (x == 0 && y.is_sign_negative()) {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        ord => ord,
    }
}

/// The §5.4 canonical numeric class: `Some(i)` iff `Float(f)` is exactly equal to
/// `Int(i)` under [`compare_int_f64`] — finite, integral, in i64 range, and not
/// `-0.0` (which `total_cmp` orders below `+0.0`/`Int(0)` and so keeps float
/// identity). Joint with the exact comparator this makes numeric equality an
/// equivalence relation a `Hash + Eq` key can represent: `Float(2^53)` shares
/// `Int(2^53)`'s class, while `Int(2^53 + 1)` keeps all 64 bits.
#[inline]
pub fn float_int_class(f: f64) -> Option<i64> {
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    if !f.is_finite() || f.trunc() != f {
        return None;
    }
    if f == 0.0 {
        return if f.is_sign_negative() { None } else { Some(0) };
    }
    if !(-TWO_POW_63..TWO_POW_63).contains(&f) {
        return None;
    }
    Some(f as i64)
}

/// JOIN / equality comparison. `null != null` (any null operand ⇒ not equal) —
/// the opposite of [`compare_values`]. Required for correct join semantics.
#[inline]
pub fn values_equal(a: Value<'_>, b: Value<'_>) -> bool {
    match (a, b) {
        // `Absent` never appears on a join key (presence-required, §2.1); treat it like
        // `Null` for totality — an absent operand is never join-equal to anything.
        (Value::Absent, _) | (_, Value::Absent) => false,
        (Value::Null, _) | (_, Value::Null) => false,
        _ => compare_values(a, b) == Ordering::Equal,
    }
}

/// PREDICATE identity comparison — **the third comparator** (`07` §4.2/§8.1). A
/// compiled `where` predicate's `=` / `!=` / `IN` evaluates with this, NOT with
/// [`values_equal`]: a filter treats **`null` as identical to `null`** (so
/// `col = null` matches a null cell), the opposite of join semantics. It is
/// distinct from all three of:
///
/// - [`compare_values`] (sort): same null-handling (`null == null`) but **total
///   and panicking** on a cross-type pair — wrong for a predicate, which must
///   never crash the pipeline on a literal/column type mismatch.
/// - [`values_equal`] (join): `null != null` — the explicitly-forbidden one here.
///
/// So `values_identical` is the **total, non-panicking, null-identical** equality.
/// Numeric `Int`/`Float` cross-comparison is **exact** (design 226 §5.1) because
/// SQLite `number` columns vend as `Float` while AST literals often lower to
/// integral `Int`; below 2^53 this is indistinguishable from the old f64 widening,
/// and above it two distinct `Int`s no longer collapse onto one `Float`.
#[inline]
pub fn values_identical(a: Value<'_>, b: Value<'_>) -> bool {
    use Value::*;
    match (a, b) {
        // `Absent` is never met on a predicate column (presence-required, §2.1). Define
        // it total and non-panicking: an absent cell is identical only to another absent
        // cell, and distinct from every present value (including `Null`).
        (Absent, Absent) => true,
        (Absent, _) | (_, Absent) => false,
        (Null, Null) => true,
        (Null, _) | (_, Null) => false,
        (Bool(x), Bool(y)) => x == y,
        (Int(x), Int(y)) => x == y,
        (Float(x), Float(y)) => x.total_cmp(&y) == Ordering::Equal,
        (Int(x), Float(y)) => compare_int_f64(x, y) == Ordering::Equal,
        (Float(x), Int(y)) => compare_int_f64(y, x) == Ordering::Equal,
        (Str(x), Str(y)) | (Json(x), Json(y)) => x == y,
        // Other cross-type pairs are simply not identical.
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Rows — index-addressed, owned form + the lending borrowed form
// ---------------------------------------------------------------------------

/// An owned row: an index-addressed, immutable, cheaply-clonable value sequence,
/// stored as **one flat allocation** (`205-FLAT-ROW-SINGLE-BUFFER-DESIGN.md`).
/// Cloning a row into an overlay / the view / a heap is a refcount bump, not a
/// deep copy (shared-row aliasing is pervasive) — exactly as when this was
/// `Arc<[OwnedValue]>`, but the cells now live inline in the row's own buffer
/// (no per-`Str`-cell `Arc<str>` block), so materializing a row is one
/// allocation instead of `2 + k_text`.
///
/// Layout (all integers little-endian):
///
/// ```text
/// [0..4)      n: u32                       cell count
/// [4..4+9n)   directory, 9 bytes per cell: [tag u8][payload 8B]
///               Absent/Null: payload unused          Bool: payload[0]
///               Int: i64                             Float: f64 bits
///               Str/Json: [off u32][len u32]  (absolute into the buffer)
/// [4+9n..)    Str/Json bytes, concatenated in cell order
/// ```
///
/// Cells are read as borrowed [`Value<'_>`]s via [`OwnedRow::col`] — `Str`/`Json`
/// borrow the row's own buffer zero-copy. Text bytes are **UTF-8-validated at
/// construction** (the same eager timing as the old per-cell
/// [`Value::try_to_owned`]), so readers never re-validate.
#[derive(Clone)]
pub struct OwnedRow {
    buf: Arc<[u8]>,
}

const ROW_HDR: usize = 4;
const CELL_STRIDE: usize = 9;

/// Cold-path panic for [`OwnedRow::col`]'s bounds contract. Kept out of line so
/// `col` itself stays small enough to inline into the btree compare loop — with
/// the `assert!` + format machinery inline, the ms-index descents of a top-k
/// push spent ~20% of their time in an outlined `col` (the wtop50 stream
/// regression found by the triangle bench).
#[cold]
#[inline(never)]
fn col_out_of_range(c: ColId) -> ! {
    panic!("column {c} out of range for row");
}

const TAG_ABSENT: u8 = 0;
const TAG_NULL: u8 = 1;
const TAG_BOOL: u8 = 2;
const TAG_INT: u8 = 3;
const TAG_FLOAT: u8 = 4;
const TAG_STR: u8 = 5;
const TAG_JSON: u8 = 6;

impl OwnedRow {
    /// Number of cells.
    #[inline]
    pub fn len(&self) -> usize {
        u32::from_le_bytes(self.buf[0..4].try_into().unwrap()) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Read cell `c`, zero-copy. Panics if `c` is out of range — the same
    /// contract as the old slice index (`ColId`s are resolved against the schema
    /// at build time, so an out-of-range read is a builder bug).
    ///
    /// `#[inline(always)]` + the out-of-line panic are load-bearing: this is the
    /// cell accessor inside `compare_rows`, i.e. inside every btree node compare
    /// on the write path. One fused 9-byte cell slice (a single bounds check)
    /// instead of separate tag/payload indexing keeps it a handful of
    /// instructions.
    #[inline(always)]
    pub fn col(&self, c: ColId) -> Value<'_> {
        let (tag, payload) = self.cell_raw(c);
        self.decode_cell(tag, payload)
    }

    /// Raw `(tag, payload)` of cell `c` — the sort-compare fast path reads this
    /// instead of materializing a `Value`. Same bounds contract as
    /// [`OwnedRow::col`].
    #[inline(always)]
    fn cell_raw(&self, c: ColId) -> (u8, [u8; 8]) {
        if c >= self.len() {
            col_out_of_range(c);
        }
        let base = ROW_HDR + c * CELL_STRIDE;
        let cell: &[u8; CELL_STRIDE] = self.buf[base..base + CELL_STRIDE].try_into().unwrap();
        (cell[0], cell[1..9].try_into().unwrap())
    }

    /// Decode a cell already fetched by [`OwnedRow::cell_raw`] — `Str`/`Json`
    /// borrow this row's buffer zero-copy, so the pair must come from `self`.
    #[inline(always)]
    fn decode_cell(&self, tag: u8, payload: [u8; 8]) -> Value<'_> {
        match tag {
            TAG_ABSENT => Value::Absent,
            TAG_NULL => Value::Null,
            TAG_BOOL => Value::Bool(payload[0] != 0),
            TAG_INT => Value::Int(i64::from_le_bytes(payload)),
            TAG_FLOAT => Value::Float(f64::from_le_bytes(payload)),
            tag @ (TAG_STR | TAG_JSON) => {
                let off = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                let len = u32::from_le_bytes(payload[4..8].try_into().unwrap()) as usize;
                let bytes = &self.buf[off..off + len];
                if tag == TAG_STR {
                    Value::Str(bytes)
                } else {
                    Value::Json(bytes)
                }
            }
            tag => unreachable!("corrupt row cell tag {tag}"),
        }
    }

    /// Read cell `c`, or `None` if out of range (the tolerant peer of
    /// [`OwnedRow::col`] — the wasm marshal reads short/projected rows with it).
    #[inline]
    pub fn get(&self, c: ColId) -> Option<Value<'_>> {
        (c < self.len()).then(|| self.col(c))
    }

    /// Iterate the cells as borrowed [`Value<'_>`]s (replaces `.iter()` over the
    /// old `&[OwnedValue]` view).
    #[inline]
    pub fn cells(&self) -> impl ExactSizeIterator<Item = Value<'_>> + '_ {
        (0..self.len()).map(|i| self.col(i))
    }

    /// Row **identity** (not equality): do two rows share the same buffer? The
    /// btree's COW-refresh and structural-diff fast paths use this exactly as
    /// they used `Arc::ptr_eq` on the old `Arc<[OwnedValue]>`.
    #[inline]
    pub fn ptr_eq(a: &OwnedRow, b: &OwnedRow) -> bool {
        Arc::ptr_eq(&a.buf, &b.buf)
    }

    /// The size of the row's single heap buffer in bytes (header + directory +
    /// text region) — the row's entire heap footprint besides the `Arc` header.
    /// Memory accounting (e.g. `rindle-loadgen`) reads this instead of modeling
    /// per-cell blocks, which no longer exist.
    #[inline]
    pub fn byte_len(&self) -> usize {
        self.buf.len()
    }

    /// The shared zero-column row (ghost/placeholder entries). O(1), allocates
    /// once per process.
    pub fn empty() -> OwnedRow {
        static EMPTY: std::sync::OnceLock<OwnedRow> = std::sync::OnceLock::new();
        EMPTY
            .get_or_init(|| OwnedRow::from_owned_cells(&[]))
            .clone()
    }

    /// Materialize every cell as an [`OwnedValue`] (the wire / compatibility
    /// escape — allocates per `Str`/`Json` cell).
    pub fn to_value_vec(&self) -> Vec<OwnedValue> {
        self.cells().map(|v| v.to_owned()).collect()
    }

    /// Build a row from already-owned cells. Infallible: an
    /// [`OwnedValue::Str`]/[`Json`](OwnedValue::Json) is `Arc<str>`, so its bytes
    /// are valid UTF-8 by construction — no validation pass.
    pub fn from_owned_cells(cells: &[OwnedValue]) -> OwnedRow {
        let mut w = RowWriter::sized(cells.len(), |i| match &cells[i] {
            OwnedValue::Str(s) | OwnedValue::Json(s) => s.len(),
            _ => 0,
        });
        for c in cells {
            w.push(c.as_ref());
        }
        w.finish()
    }

    /// **The leaf-boundary constructor**: materialize a borrowed row (e.g. the
    /// SQLite step buffer) into one flat allocation, validating `Str`/`Json`
    /// UTF-8 exactly once — the same eager timing as the old per-cell
    /// [`Value::try_to_owned`], but with no per-cell allocation. Errors surface
    /// *before* the row allocates.
    pub fn try_from_row_ref<R: RowRef + ?Sized>(r: &R) -> Result<OwnedRow, RindleError> {
        let n = r.len();
        // Pass 1: size + validate (no allocation yet).
        let mut text = 0usize;
        for i in 0..n {
            match r.col(i) {
                Value::Str(b) => {
                    std::str::from_utf8(b).map_err(|source| RindleError::InvalidUtf8 {
                        value_type: "TEXT",
                        source,
                    })?;
                    text += b.len();
                }
                Value::Json(b) => {
                    std::str::from_utf8(b).map_err(|source| RindleError::InvalidUtf8 {
                        value_type: "JSON",
                        source,
                    })?;
                    text += b.len();
                }
                _ => {}
            }
        }
        // Pass 2: write (validated above, so the writer is infallible).
        let mut w = RowWriter::new(n, text);
        for i in 0..n {
            w.push(r.col(i));
        }
        Ok(w.finish())
    }
}

impl std::fmt::Debug for OwnedRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Render Str/Json as strings (the bytes are validated UTF-8), matching
        // the readability of the old `Arc<[OwnedValue]>` debug output.
        struct Cell<'a>(Value<'a>);
        impl std::fmt::Debug for Cell<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self.0 {
                    Value::Str(b) => write!(f, "Str({:?})", String::from_utf8_lossy(b)),
                    Value::Json(b) => write!(f, "Json({:?})", String::from_utf8_lossy(b)),
                    other => write!(f, "{other:?}"),
                }
            }
        }
        f.debug_list().entries(self.cells().map(Cell)).finish()
    }
}

/// The single-allocation row writer: the exact buffer is allocated up front
/// (`Arc::new_uninit_slice`) and cells are written in place — no intermediate
/// `Vec`, no memcpy into the `Arc`. Sizing must be exact; `finish` asserts the
/// buffer was filled completely.
struct RowWriter {
    buf: Arc<[std::mem::MaybeUninit<u8>]>,
    /// Next directory slot (cell index).
    cell: usize,
    /// Total cells (the directory width).
    n: usize,
    /// Next free byte in the text region.
    text_at: usize,
}

impl RowWriter {
    fn new(n: usize, text_bytes: usize) -> RowWriter {
        let dir_end = ROW_HDR + n * CELL_STRIDE;
        let size = dir_end + text_bytes;
        assert!(
            n <= u32::MAX as usize && size <= u32::MAX as usize,
            "row exceeds u32 layout bounds ({n} cells, {size} bytes)"
        );
        let mut w = RowWriter {
            buf: Arc::new_uninit_slice(size),
            cell: 0,
            n,
            text_at: dir_end,
        };
        w.write_at(0, &(n as u32).to_le_bytes());
        w
    }

    /// Convenience: size the text region via a per-cell byte-length closure.
    fn sized(n: usize, text_len: impl Fn(usize) -> usize) -> RowWriter {
        RowWriter::new(n, (0..n).map(text_len).sum())
    }

    #[inline]
    fn write_at(&mut self, at: usize, bytes: &[u8]) {
        let buf = Arc::get_mut(&mut self.buf).expect("RowWriter buffer is unshared");
        for (dst, src) in buf[at..at + bytes.len()].iter_mut().zip(bytes) {
            dst.write(*src);
        }
    }

    /// Append the next cell. Text bytes must already be validated UTF-8 (the
    /// constructors above guarantee it).
    #[inline]
    fn push(&mut self, v: Value<'_>) {
        let base = ROW_HDR + self.cell * CELL_STRIDE;
        debug_assert!(self.cell < self.n, "RowWriter overflow");
        self.cell += 1;
        let (tag, payload): (u8, [u8; 8]) = match v {
            Value::Absent => (TAG_ABSENT, [0; 8]),
            Value::Null => (TAG_NULL, [0; 8]),
            Value::Bool(b) => (TAG_BOOL, [b as u8, 0, 0, 0, 0, 0, 0, 0]),
            Value::Int(i) => (TAG_INT, i.to_le_bytes()),
            Value::Float(f) => (TAG_FLOAT, f.to_le_bytes()),
            Value::Str(b) | Value::Json(b) => {
                let off = self.text_at as u32;
                let len = b.len() as u32;
                self.write_at(self.text_at, b);
                self.text_at += b.len();
                let mut p = [0u8; 8];
                p[0..4].copy_from_slice(&off.to_le_bytes());
                p[4..8].copy_from_slice(&len.to_le_bytes());
                (
                    if matches!(v, Value::Str(_)) {
                        TAG_STR
                    } else {
                        TAG_JSON
                    },
                    p,
                )
            }
        };
        self.write_at(base, &[tag]);
        self.write_at(base + 1, &payload);
    }

    fn finish(self) -> OwnedRow {
        assert_eq!(self.cell, self.n, "RowWriter under-filled directory");
        assert_eq!(self.text_at, self.buf.len(), "RowWriter under-filled text");
        // SAFETY: every byte is initialized — the header in `new`, all `n`
        // directory slots (asserted), and the whole text region (asserted).
        let buf = unsafe { self.buf.assume_init() };
        OwnedRow { buf }
    }
}

/// Build an `OwnedRow` from a vec literal (test/util convenience).
pub fn owned_row(values: Vec<OwnedValue>) -> OwnedRow {
    OwnedRow::from_owned_cells(&values)
}

/// `(column, ascending)` — sort spec resolved to indices at build time.
pub type Sort = Vec<(ColId, bool)>;

/// The logical type of a column (or a literal), mirroring the JS
/// `ValueType` (`zero-schema/table-schema.ts`): the five types Zero stores,
/// plus the opt-in exact-integer plane (design 226). It drives the SQLite value
/// boundary — `to_sqlite_param` (`boolean`→0/1, `json`→serialized TEXT) and
/// `from_sqlite` (ty-directed `col()` conversion, the `number` safe-int bound
/// check). The leaf maps each to a [`crate::value`] storage class.
///
/// `Number` is the f64 plane: an out-of-safe-range integer in a `Number` column
/// is an error, not a silent widening. `Int` is the declared exact-i64 plane
/// (226 §4): its cells are `NULL` or SQLite storage class INTEGER, carried as
/// [`OwnedValue::Int`] end to end with no round-trip bound. No decltype maps to
/// `Int` yet — `BIGINT`/`INT8` stay refused until 226 Stage C4 lifts the
/// refusal — so today it is reachable only by explicit declaration (embedded
/// [`SourceSchema::with_column_types`], typed `ColumnDef`s).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueType {
    String,
    Number,
    Boolean,
    Null,
    Json,
    /// The exact-i64 column plane (design 226): `NULL`-or-INTEGER storage class,
    /// no f64 round-trip requirement.
    Int,
}

/// A **scalar projection** annotation on a relationship slot (`REDUCE-DESIGN.md`
/// §9, Tier 1). A relationship aggregate — `issue { commentCount: count(comments) }`
/// — lives in the engine tree as an ordinary *singular* one-row relationship whose
/// child is the synthetic aggregate row `[group_key…, agg]`. This annotation tells
/// the **presentation boundary** (the `View` marshaller and every wire receiver) to
/// **unwrap** that one-row child into a plain scalar field named after the
/// relationship: instead of `commentCount: [{ issueID: 1, count: 5 }]` the consumer
/// sees `commentCount: 5`. It is purely a result-shape concern — the dataflow and the
/// reconstructed tree stay a plural relationship of one row.
#[derive(Clone, Debug)]
pub struct ScalarProjection {
    /// Which **child** column to surface as the scalar value (the aggregate column —
    /// `count` in `[issueID, count]`). Indexed into the child schema's columns.
    pub col: ColId,
    /// The value to substitute when the relationship is **empty** — a childless
    /// parent has no group in the reduce, so the projection emits the aggregate's
    /// identity (`0` for `count`, `NULL` for `sum`/`avg`), the boundary analogue of
    /// SQL's `LEFT JOIN … count → 0` (`REDUCE-DESIGN.md` §9). Without this an absent
    /// group would render as `null`, which is wrong for `count`.
    pub identity: OwnedValue,
}

/// A declared outgoing relationship on a table [`Schema`] (slot = index in
/// [`Schema::relationships`]). Carries the public **name** the View surfaces
/// (`09`) and, when known, the **target [`Schema`]** the relationship points at —
/// the hierarchical `SourceSchema` the production `ArrayView` (`09`) navigates to
/// sort child entries. `child` is `None` for a join-only `RelDef` (name
/// resolution alone — the join already holds the child schema separately); the
/// View requires `Some`.
#[derive(Clone, Debug)]
pub struct RelDef {
    /// Relationship name. Owned (`Box<str>`) so a runtime-supplied schema (the JS
    /// wasm boundary, WS01.3) can construct it — column/relationship names are not
    /// `'static` when they arrive from JavaScript. The constructors still accept
    /// `&str`, so `&'static str` literals at the existing call sites coerce in.
    pub name: Box<str>,
    /// The relationship's target schema (the child level). `None` for join-only
    /// uses that need name→slot resolution but never feed a View.
    pub child: Option<Box<Schema>>,
    /// When `Some`, this is a **scalar-projected** relationship aggregate
    /// (`REDUCE-DESIGN.md` §9): the presentation boundary unwraps the one-row child
    /// into a scalar field (see [`ScalarProjection`]). `None` for an ordinary
    /// (plural or `.one()` object) relationship.
    pub project: Option<ScalarProjection>,
}

impl RelDef {
    /// A join-only relationship: just a name (no child schema). Used wherever a
    /// parent schema declares a relationship purely so a join can resolve its
    /// `rel_name` to a [`RelId`] slot ([`Schema::rel_slot`]).
    pub fn new(name: &str) -> RelDef {
        RelDef {
            name: name.into(),
            child: None,
            project: None,
        }
    }

    /// A View-facing relationship carrying its target (child) schema.
    pub fn related(name: &str, child: Schema) -> RelDef {
        RelDef {
            name: name.into(),
            child: Some(Box::new(child)),
            project: None,
        }
    }

    /// Mark this relationship as a **scalar-projected** aggregate
    /// (`REDUCE-DESIGN.md` §9): the presentation boundary unwraps the one-row child,
    /// surfacing child column `col` as a scalar field (with `identity` substituted
    /// when the relationship is empty). Builder over [`RelDef::related`] — the child
    /// schema (the synthetic aggregate row) must already be attached.
    pub fn project_scalar(mut self, col: ColId, identity: OwnedValue) -> RelDef {
        debug_assert!(
            self.child.is_some(),
            "project_scalar on a join-only RelDef (no child schema to unwrap)"
        );
        self.project = Some(ScalarProjection { col, identity });
        self
    }
}

/// The **view / pipeline** schema: a table's columns + key + sort *plus* the
/// per-query relationship slots and `.one()` shape the dataflow and materialized
/// view carry. A *source* table is registered with the lighter [`SourceSchema`]
/// (no relationships, no `singular`); the build path widens that to a `Schema` and
/// fills `relationships` from the query AST — never from a source declaration, of
/// which there are none.
#[derive(Clone, Debug)]
pub struct Schema {
    /// Column names. Owned (`Vec<Box<str>>`) so a runtime-supplied schema (the JS
    /// wasm boundary, WS01.3) can construct it without leaking `Box::leak`'d
    /// `'static` strings. [`Schema::new`] still takes `Vec<&str>`, so existing
    /// `vec!["id", …]` literal call sites coerce in unchanged.
    pub columns: Vec<Box<str>>,
    /// Declared logical column types, parallel to `columns` (design 226 §4.1).
    /// [`Schema::new`] defaults every column to [`ValueType::Number`] — the
    /// untyped/legacy declaration; homes that know the real types (the SQLite
    /// replica's decltype discovery, the SQLite leaf's `ColumnDef`s) install them
    /// via [`Schema::with_column_types`]. The engine consults these only for
    /// exact-integer behavior, so a defaulted non-numeric column is inert — the
    /// value boundary keeps its own per-leaf typing channel.
    pub column_types: Vec<ValueType>,
    pub primary_key: Vec<ColId>,
    pub sort: Sort,
    /// Declared outgoing relationships (slot = list index). Empty for a leaf table
    /// that is never a join parent. A join resolves its `rel_name` to a slot here
    /// at build time ([`Schema::rel_slot`]), so the hot path is index-addressed.
    pub relationships: Vec<RelDef>,
    /// `.one()` shape for **this level** of the materialized view — lowered from the
    /// query's [`Ast::one`](crate::ast::Ast) by [`view_schema`](crate::builder::view_schema).
    /// When set, the result boundary presents this level as a single object (or
    /// `null`/absent) instead of an array; the engine tree itself stays plural at
    /// every level. Always `false` for a source / table schema.
    pub singular: bool,
    /// **Projection** (`PROJECTION-SUPPORT-DESIGN.md` §6). `None` ⇒ the view reports
    /// every column (`'*'`); `Some(cols)` ⇒ the view reports only these base
    /// [`ColId`]s, in this order — the lowering of the query's
    /// [`Ast::select`](crate::ast::Ast). **`columns` stays the full, positional column
    /// list** (rows are full width everywhere on the client, §2.1); projection is
    /// applied only at the result boundary (the marshal reads `columns[col]` / `row[col]`
    /// for each `col` in this list). The PK / `sort` indices therefore remain valid in
    /// the full column space regardless of what the view reports.
    pub projection: Option<Vec<ColId>>,
}

impl Schema {
    /// Construct a relationship-free schema (the common case). Use
    /// [`Schema::with_relationships`] to declare relationships for a join parent.
    /// Keeps the 3-field call sites terse now that `relationships` exists.
    pub fn new(columns: Vec<&str>, primary_key: Vec<ColId>, sort: Sort) -> Schema {
        let columns: Vec<Box<str>> = columns.into_iter().map(Box::from).collect();
        Schema {
            column_types: vec![ValueType::Number; columns.len()],
            columns,
            primary_key,
            sort,
            relationships: Vec::new(),
            singular: false,
            projection: None,
        }
    }

    /// Builder: declare this table's outgoing relationships (slot = list index).
    pub fn with_relationships(mut self, relationships: Vec<RelDef>) -> Schema {
        self.relationships = relationships;
        self
    }

    /// Builder: install the declared column types (parallel to `columns`, design
    /// 226 §4.1), replacing [`Schema::new`]'s all-`Number` default.
    pub fn with_column_types(mut self, column_types: Vec<ValueType>) -> Schema {
        assert_eq!(
            column_types.len(),
            self.columns.len(),
            "column_types arity must match columns arity"
        );
        self.column_types = column_types;
        self
    }

    /// The target [`Schema`] of relationship `slot`, or `None` if the slot is
    /// undeclared or join-only (no child schema attached).
    pub fn rel_child(&self, slot: RelId) -> Option<&Schema> {
        self.relationships
            .get(slot.ix())
            .and_then(|r| r.child.as_deref())
    }

    /// Resolve a column **name** to its [`ColId`] — the builder's name→index
    /// lowering (`08`). `None` if the column is not in this schema.
    pub fn col_id(&self, name: &str) -> Option<ColId> {
        self.columns.iter().position(|c| c.as_ref() == name)
    }

    /// Resolve a relationship **name** to its [`RelId`] slot. `None` if undeclared.
    pub fn rel_slot(&self, name: &str) -> Option<RelId> {
        self.relationships
            .iter()
            .position(|r| r.name.as_ref() == name)
            .map(|i| RelId(i as u32))
    }
}

/// Static, user-declared **source table** metadata: columns, primary key, and the
/// table's default sort. This is the table-registration boundary type — what
/// [`Graph::add_source`](crate::graph::Graph::add_source) and the builder's
/// `resolve` callback speak.
///
/// It deliberately carries **no relationships and no `singular`**: a relationship
/// is a property of a *query* (the explicit `sub`/EXISTS correlation in the AST),
/// not of a source table, and the engine derives the per-query relationship slots
/// from the AST at build time (see
/// [`query_local_slot_names`](crate::builder::query_local_slot_names)). The type
/// makes it impossible to declare one on a source. Widen to a [`Schema`] (empty
/// relationships, `singular = false`) via [`From`]/[`SourceSchema::into_schema`]
/// when an operator needs the richer view/pipeline shape; the build path then
/// installs the query-derived slots over the empty `relationships`.
#[derive(Clone, Debug)]
pub struct SourceSchema {
    /// Column names, in order (positional against source rows).
    pub columns: Vec<Box<str>>,
    /// Declared logical column types, parallel to `columns` (design 226 §4.1).
    /// [`SourceSchema::new`] defaults every column to [`ValueType::Number`]; the
    /// homes that know the real declared types (the replica's decltype discovery,
    /// the SQLite leaf) install them via [`SourceSchema::with_column_types`].
    pub column_types: Vec<ValueType>,
    /// Primary-key column indices (into `columns`).
    pub primary_key: Vec<ColId>,
    /// The table's default sort — `(col_idx, ascending)` pairs.
    pub sort: Sort,
}

impl SourceSchema {
    /// Construct a source schema. Same call shape as [`Schema::new`], so existing
    /// `Schema::new(vec!["id", …], …)` source fixtures port over by renaming.
    pub fn new(columns: Vec<&str>, primary_key: Vec<ColId>, sort: Sort) -> SourceSchema {
        let columns: Vec<Box<str>> = columns.into_iter().map(Box::from).collect();
        SourceSchema {
            column_types: vec![ValueType::Number; columns.len()],
            columns,
            primary_key,
            sort,
        }
    }

    /// Builder: install the declared column types (parallel to `columns`, design
    /// 226 §4.1), replacing [`SourceSchema::new`]'s all-`Number` default.
    pub fn with_column_types(mut self, column_types: Vec<ValueType>) -> SourceSchema {
        assert_eq!(
            column_types.len(),
            self.columns.len(),
            "column_types arity must match columns arity"
        );
        self.column_types = column_types;
        self
    }

    /// Resolve a column **name** to its [`ColId`]. `None` if not present.
    pub fn col_id(&self, name: &str) -> Option<ColId> {
        self.columns.iter().position(|c| c.as_ref() == name)
    }

    /// Widen to a [`Schema`] with **no** relationships and `singular = false` — the
    /// view/pipeline shape an operator sees. Moves the columns/key/sort (no clone);
    /// the build path replaces `relationships` with the query-derived slot tree.
    pub fn into_schema(self) -> Schema {
        Schema {
            columns: self.columns,
            column_types: self.column_types,
            primary_key: self.primary_key,
            sort: self.sort,
            relationships: Vec::new(),
            singular: false,
            projection: None,
        }
    }
}

impl From<SourceSchema> for Schema {
    fn from(s: SourceSchema) -> Schema {
        s.into_schema()
    }
}

/// Compare two owned rows under a sort spec (asc/desc per column). The common
/// short sorts (one/two columns) are unrolled to drop iterator overhead on the
/// hottest path. Each cell is borrowed via `as_ref()` and fed to the
/// `Value<'_>` comparator — no allocation.
#[inline]
pub fn compare_rows(sort: &Sort, a: &OwnedRow, b: &OwnedRow) -> Ordering {
    match sort.as_slice() {
        [] => Ordering::Equal,
        [(c0, asc0)] => apply_dir(compare_cell(a, b, *c0), *asc0),
        [(c0, asc0), (c1, asc1)] => {
            let r = apply_dir(compare_cell(a, b, *c0), *asc0);
            if r != Ordering::Equal {
                return r;
            }
            apply_dir(compare_cell(a, b, *c1), *asc1)
        }
        _ => {
            for &(col, asc) in sort {
                let c = apply_dir(compare_cell(a, b, col), asc);
                if c != Ordering::Equal {
                    return c;
                }
            }
            Ordering::Equal
        }
    }
}

/// One sort-column compare between two rows. Int–Int (every pk tiebreak, most
/// sort keys) and Float–Float (the SQLite leaf's number domain) are decided
/// straight from the raw cells — no `Value` is built and the out-of-line
/// [`compare_values`] call is skipped. Anything else decodes the already-read
/// cells and falls back to the full cross-type comparator, unchanged.
#[inline(always)]
fn compare_cell(a: &OwnedRow, b: &OwnedRow, c: ColId) -> Ordering {
    let (ta, pa) = a.cell_raw(c);
    let (tb, pb) = b.cell_raw(c);
    if ta == tb {
        if ta == TAG_INT {
            return i64::from_le_bytes(pa).cmp(&i64::from_le_bytes(pb));
        }
        if ta == TAG_FLOAT {
            return f64::from_le_bytes(pa).total_cmp(&f64::from_le_bytes(pb));
        }
    }
    compare_values(a.decode_cell(ta, pa), b.decode_cell(tb, pb))
}

/// Flip the ordering for a descending column.
#[inline]
fn apply_dir(c: Ordering, asc: bool) -> Ordering {
    if asc {
        c
    } else {
        c.reverse()
    }
}

/// True if rows share the same primary key (PK columns are non-null, so
/// `values_equal` behaves like identity here).
pub fn same_pk(a: &OwnedRow, b: &OwnedRow, pk: &[ColId]) -> bool {
    pk.iter().all(|&c| values_equal(a.col(c), b.col(c)))
}

// ---------------------------------------------------------------------------
// The lending RowRef / RowStream traits (the source-agnostic boundary)
// ---------------------------------------------------------------------------

/// A borrowed view of one row; columns addressed by [`ColId`]. The returned
/// [`Value`] borrows `self`, so it is valid only as long as the row reference —
/// for the SQLite leaf that means until the next `next_row`.
///
/// **Not lifetime-parameterized** (a refinement of foundations' `RowRef<'a>`):
/// `col(&self) -> Value<'_>` ties the cell to the `&self` borrow, which makes
/// the `RowStream` GAT bound (`type Row<'a>: RowRef`) clean — no HRTB. The
/// only cost is that a cell cannot be held past the row reference without
/// `to_owned`, which is exactly the contract we want.
pub trait RowRef {
    /// Read column `c`, zero-copy.
    fn col(&self, c: ColId) -> Value<'_>;
    /// Number of columns.
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Materialize the whole row. For the held-`Rc` memory backend this is an
    /// O(1) `Arc` bump (the impl on `&OwnedRow` below); for the SQLite leaf it
    /// is the one forced per-row copy out of the step buffer (`sqlite.rs`).
    fn to_owned_row(&self) -> OwnedRow;

    /// Fallible materialization for external data. Memory-backed rows override
    /// nothing because owning is an `Arc` bump; SQLite rows override this to
    /// surface invalid text/json as [`RindleError`] instead of panicking.
    fn try_to_owned_row(&self) -> Result<OwnedRow, RindleError> {
        Ok(self.to_owned_row())
    }
}

/// A **lending** stream of rows. `next_row` reborrows `self`, so the row it
/// returns is invalidated by the next call — exactly the SQLite cursor
/// contract, now a compile-time invariant. This is the leaf source's output
/// shape; it is **not** `std::iter::Iterator` (which hands out owned items and
/// cannot express the borrow). GAT-based lending, stable since Rust 1.65.
///
/// **The lending contract is enforced by the borrow checker.** Holding a row
/// (or a borrowed `Value` out of it) across the next `next_row` is a compile
/// error — for SQLite that is precisely "use after `sqlite3_step`," caught at
/// compile time instead of corrupting a read. The same invariant holds for the
/// memory backend (the row borrows the cursor's `Rc` snapshot), so this fails to
/// compile against *either* leaf:
///
/// ```compile_fail
/// use rindle::btree::BTree;
/// use rindle::value::{owned_row, OwnedValue, RowRef, RowStream, Sort};
/// let sort: Sort = vec![(0, true)];
/// let mut t = BTree::new();
/// t.add(owned_row(vec![OwnedValue::Int(1)]), &sort);
/// t.add(owned_row(vec![OwnedValue::Int(2)]), &sort);
/// let mut c = t.values_from(None, true, &sort);
/// let r1 = c.next_row().unwrap();   // borrows `c`
/// let r2 = c.next_row().unwrap();   // ERROR: second &mut borrow of `c` while r1 lives
/// let _ = (r1.col(0), r2.col(0));   // r1 still used here ⇒ borrows overlap
/// ```
///
/// To keep a row past the step, own it — `RowRef::to_owned_row` (a copy on the
/// SQLite leaf, an `Arc` bump on the memory leaf):
///
/// ```
/// use rindle::btree::BTree;
/// use rindle::value::{owned_row, OwnedValue, RowRef, RowStream, Sort};
/// let sort: Sort = vec![(0, true)];
/// let mut t = BTree::new();
/// t.add(owned_row(vec![OwnedValue::Int(1)]), &sort);
/// let mut c = t.values_from(None, true, &sort);
/// let owned = c.next_row().unwrap().to_owned_row(); // escapes the step
/// assert!(matches!(owned.col(0), rindle::value::Value::Int(1)));
/// ```
pub trait RowStream {
    type Row<'a>: RowRef
    where
        Self: 'a;
    fn next_row(&mut self) -> Option<Self::Row<'_>>;
}

/// **THE canonical owned→borrowed bridge for the memory backend.** Vending a
/// `&OwnedRow` (rather than a `&[OwnedValue]` slice) is what makes
/// `to_owned_row` an O(1) `Arc` bump instead of a deep copy — folding the btree
/// spike's `current_arc` escape hatch (`04` OQ-2) into the shared trait. The
/// `BTreeCursor` sets `type Row<'a> = &'a OwnedRow` and gets owning-at-the-node
/// -boundary for free.
impl RowRef for &OwnedRow {
    #[inline]
    fn col(&self, c: ColId) -> Value<'_> {
        OwnedRow::col(self, c)
    }
    #[inline]
    fn len(&self) -> usize {
        OwnedRow::len(self)
    }
    #[inline]
    fn to_owned_row(&self) -> OwnedRow {
        (*self).clone()
    }
}

/// Bridge for a bare **owned-cell slice** (e.g. a constraint/key vector or a
/// synthesized cell list that was never a flat row). Here `to_owned_row` must
/// build a fresh flat row (one allocation; text bytes are copied out of the
/// `Arc<str>` cells, which are valid UTF-8 by construction).
impl RowRef for &[OwnedValue] {
    #[inline]
    fn col(&self, c: ColId) -> Value<'_> {
        self[c].as_ref()
    }
    #[inline]
    fn len(&self) -> usize {
        <[OwnedValue]>::len(self)
    }
    #[inline]
    fn to_owned_row(&self) -> OwnedRow {
        OwnedRow::from_owned_cells(self)
    }
}

#[cfg(test)]
mod absent_tests {
    use super::*;

    #[test]
    fn absent_is_unique_minimum_in_sort_comparator() {
        // Below `Null`, and below every present value.
        assert_eq!(
            compare_values(Value::Absent, Value::Absent),
            Ordering::Equal
        );
        assert_eq!(compare_values(Value::Absent, Value::Null), Ordering::Less);
        assert_eq!(
            compare_values(Value::Null, Value::Absent),
            Ordering::Greater
        );
        assert_eq!(compare_values(Value::Absent, Value::Int(0)), Ordering::Less);
        assert_eq!(
            compare_values(Value::Absent, Value::Str(b"")),
            Ordering::Less
        );
        assert_eq!(
            compare_values(Value::Int(-1), Value::Absent),
            Ordering::Greater
        );
    }

    #[test]
    fn absent_join_equality_is_always_false() {
        // Like `Null`: an absent operand is never join-equal to anything.
        assert!(!values_equal(Value::Absent, Value::Absent));
        assert!(!values_equal(Value::Absent, Value::Null));
        assert!(!values_equal(Value::Absent, Value::Int(0)));
    }

    #[test]
    fn absent_predicate_identity() {
        // Absent ≡ Absent; absent distinct from every present value, including Null.
        assert!(values_identical(Value::Absent, Value::Absent));
        assert!(!values_identical(Value::Absent, Value::Null));
        assert!(!values_identical(Value::Null, Value::Absent));
        assert!(!values_identical(Value::Absent, Value::Int(0)));
    }

    #[test]
    fn presence_roundtrips_owned_borrowed() {
        let a = OwnedValue::Absent;
        assert!(a.is_absent());
        assert!(!a.is_null());
        assert!(a.as_ref().is_absent());
        assert!(matches!(a.as_ref().to_owned(), OwnedValue::Absent));
        // A present cell is not absent.
        assert!(!OwnedValue::Null.is_absent());
        assert!(!OwnedValue::Int(1).is_absent());
    }

    #[test]
    fn absent_sorts_to_bottom_of_a_secondary_index() {
        // The motivating case (§2.1): a partial union row keyed by an Absent cell sorts
        // below all present rows under `compare_rows`, keeping the index total.
        let sort: Sort = vec![(0, true)];
        let absent = owned_row(vec![OwnedValue::Absent]);
        let null = owned_row(vec![OwnedValue::Null]);
        let one = owned_row(vec![OwnedValue::Int(1)]);
        assert_eq!(compare_rows(&sort, &absent, &null), Ordering::Less);
        assert_eq!(compare_rows(&sort, &absent, &one), Ordering::Less);
        assert_eq!(compare_rows(&sort, &one, &absent), Ordering::Greater);
    }
}
