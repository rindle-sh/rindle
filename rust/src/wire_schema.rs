//! The view schema on the wire + a content fingerprint
//! (`FLAT-CHANGES-DESIGN.md` §5.2, §5.5).
//!
//! [`WireSchema`] is the hierarchical view [`Schema`] reduced to exactly what a
//! receiver needs to reconstruct the tree, shipped **once per subscription**: per
//! level the ordered column **names** (wire rows are positional against them), the
//! `primary_key`, the **resolved + PK-completed** `sort` (the comparator input, §4),
//! the `singular` presentation flag, and the relationships **in slot order** — an
//! out-of-view / gating slot (no child schema) has `child: None`.
//!
//! [`schema_fp`] is a content fingerprint (FNV-1a 64 over a canonical, length-prefixed
//! byte stream that resolves the PK and `sort` to column **names** and recurses into
//! child schemas). It is meant to be stamped on every batch so a receiver rejects a
//! batch whose schema drifted from the one it subscribed with (§5.5). FNV-1a is chosen
//! over `std`'s `DefaultHasher` precisely because the fingerprint is a **wire** value:
//! it must be byte-for-byte reproducible across processes, Rust versions, and host
//! languages, which `DefaultHasher` does not guarantee.
//!
//! [`COMPARATOR_VERSION`] is the orthogonal second versioning axis (§5.5): the
//! `compare_values`/`compare_rows` *algorithm* contract (§4). A receiver hard-rejects a
//! mismatch — it is a code contract, not data, so a content hash cannot cover it.

use crate::value::{compare_values, OwnedValue, RelDef, Schema};

/// The `compare_values`/`compare_rows` algorithm-contract version (§4/§5.5). Bump this
/// whenever the total order changes (null handling, float/`total_cmp`, the bytewise
/// `BINARY` string order, cross-type rules). A receiver MUST refuse a subscription
/// whose `comparator_version` differs — its reconstruction would silently corrupt.
///
/// v2 (design 226 Stage B): mixed `Int`/`Float` comparison became exact
/// ([`compare_int_f64`](crate::value::compare_int_f64)) instead of `as f64`
/// widening. Below 2^53 the order is bit-identical to v1; the bump is the accepted
/// hard fleet break (226 §5.2) — no negotiation or compatibility window.
pub const COMPARATOR_VERSION: u32 = 2;

/// The wire form of a [`ScalarProjection`](crate::value::ScalarProjection)
/// (`REDUCE-DESIGN.md` §9): the **child** column to surface as a scalar plus the
/// empty-relationship identity, carried so a receiver reproduces the unwrap at read
/// time. The identity rides the wire for presentation only — it is deliberately **not**
/// in the fingerprint (a bare JS cell can't reproduce a value's `Int`/`Float`/`Json`
/// variant byte-for-byte; the projected-column name and the child schema already capture
/// the structural drift, see `hash_level`).
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug)]
pub struct WireProjection {
    pub col: u32,
    pub identity: OwnedValue,
}

// `OwnedValue` has no derived `PartialEq` (it would invite the wrong comparator), so
// spell projection equality via `compare_values` (the wire comparator). `Eq` is the
// marker over it — identities are concrete cells (no NaN), so it is well-formed.
impl PartialEq for WireProjection {
    fn eq(&self, other: &Self) -> bool {
        self.col == other.col
            && compare_values(self.identity.as_ref(), other.identity.as_ref())
                == std::cmp::Ordering::Equal
    }
}
impl Eq for WireProjection {}

/// A relationship slot on a [`WireSchema`]: its name, its slot index, either the
/// child level's schema (in-view) or `None` (gating / out-of-view — the receiver's
/// in-view gate drops `Child` changes addressed at it, `FLAT-CHANGES-DESIGN.md` §6),
/// and an optional scalar-projection annotation (`REDUCE-DESIGN.md` §9).
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireRel {
    pub name: Box<str>,
    pub slot: u32,
    pub child: Option<WireSchema>,
    /// `Some` ⇒ a scalar-projected relationship aggregate; the receiver unwraps the
    /// one-row child into a scalar field. `None` for an ordinary relationship.
    pub project: Option<WireProjection>,
}

/// One level of the hierarchical view schema, wire-shaped. See the module docs.
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireSchema {
    /// Column names, in order; wire rows are positional against this list.
    pub columns: Vec<Box<str>>,
    /// Primary-key column indices (into `columns`).
    pub primary_key: Vec<u32>,
    /// The resolved, PK-completed sort: `(col_idx, ascending)` pairs — the comparator
    /// input the receiver binary-searches with (`FLAT-CHANGES-DESIGN.md` §4/§5.2).
    pub sort: Vec<(u32, bool)>,
    /// `.one()` presentation flag — single object vs array at the result boundary.
    /// Not used to build the (always-plural) reconstruction tree (§3); carried so a
    /// receiver can reproduce `.one()` unwrapping at read time.
    pub singular: bool,
    /// Relationships in slot order (one entry per declared slot).
    pub relationships: Vec<WireRel>,
}

impl WireSchema {
    /// This schema's content fingerprint (`FLAT-CHANGES-DESIGN.md` §5.5). See [`schema_fp`].
    pub fn fingerprint(&self) -> SchemaFp {
        schema_fp(self)
    }
}

/// Lower an engine [`Schema`] (the hierarchical view schema) to its [`WireSchema`].
/// Recurses into each relationship's child schema; a join-only / gating slot (no child
/// schema) becomes `child: None`.
pub fn to_wire(schema: &Schema) -> WireSchema {
    WireSchema {
        columns: schema.columns.clone(),
        primary_key: schema.primary_key.iter().map(|&c| c as u32).collect(),
        sort: schema
            .sort
            .iter()
            .map(|&(c, asc)| (c as u32, asc))
            .collect(),
        singular: schema.singular,
        relationships: schema
            .relationships
            .iter()
            .enumerate()
            .map(|(slot, rd)| WireRel {
                name: rd.name.clone(),
                slot: slot as u32,
                child: rd.child.as_deref().map(to_wire),
                project: rd.project.as_ref().map(|p| WireProjection {
                    col: p.col as u32,
                    identity: p.identity.clone(),
                }),
            })
            .collect(),
    }
}

/// Rebuild an engine [`Schema`] from a [`WireSchema`] (the receiver side). The inverse
/// of [`to_wire`] for every field the receiver uses (columns, PK, sort, `singular`,
/// each relationship's name + child schema, and any scalar-projection annotation). A
/// `child: None` relationship becomes a join-only [`RelDef::new`] (out-of-view) slot.
pub fn to_schema(ws: &WireSchema) -> Schema {
    let cols: Vec<&str> = ws.columns.iter().map(|c| &**c).collect();
    let pk: Vec<usize> = ws.primary_key.iter().map(|&c| c as usize).collect();
    let sort: Vec<(usize, bool)> = ws.sort.iter().map(|&(c, asc)| (c as usize, asc)).collect();
    let mut schema = Schema::new(cols, pk, sort);
    schema.singular = ws.singular;
    if !ws.relationships.is_empty() {
        let rels: Vec<RelDef> = ws
            .relationships
            .iter()
            .map(|r| match &r.child {
                Some(child) => {
                    let rd = RelDef::related(&r.name, to_schema(child));
                    match &r.project {
                        Some(p) => rd.project_scalar(p.col as usize, p.identity.clone()),
                        None => rd,
                    }
                }
                None => RelDef::new(&r.name),
            })
            .collect();
        schema = schema.with_relationships(rels);
    }
    schema
}

/// A content fingerprint of a resolved [`WireSchema`] (`FLAT-CHANGES-DESIGN.md` §5.5).
/// FNV-1a 64. Serializes as its `u64`; `Display`s as zero-padded hex.
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SchemaFp(pub u64);

impl std::fmt::Display for SchemaFp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// FNV-1a 64 accumulator. The exact, length-prefixed byte protocol below is the wire
/// contract — any receiver must hash identically to compare fingerprints.
struct Fnv(u64);

impl Fnv {
    fn new() -> Fnv {
        Fnv(0xcbf29ce484222325) // FNV-1a 64 offset basis
    }
    #[inline]
    fn byte(&mut self, b: u8) {
        self.0 ^= b as u64;
        self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3); // FNV-1a 64 prime
    }
    fn bytes(&mut self, bs: &[u8]) {
        for &b in bs {
            self.byte(b);
        }
    }
    fn u8(&mut self, v: u8) {
        self.byte(v);
    }
    fn u32(&mut self, v: u32) {
        self.bytes(&v.to_le_bytes());
    }
    /// Length-prefixed string — disambiguates concatenations (`"ab"+"c"` ≠ `"a"+"bc"`).
    fn s(&mut self, s: &str) {
        self.u32(s.len() as u32);
        self.bytes(s.as_bytes());
    }
}

/// Fingerprint a resolved [`WireSchema`]. The PK and `sort` are hashed by column
/// **name** (resolved through `columns`), so the fingerprint is a semantic identity
/// independent of any internal `ColId` numbering (§5.5).
pub fn schema_fp(ws: &WireSchema) -> SchemaFp {
    let mut h = Fnv::new();
    hash_level(&mut h, ws);
    SchemaFp(h.0)
}

fn hash_level(h: &mut Fnv, ws: &WireSchema) {
    h.u8(b'S');
    h.u32(ws.columns.len() as u32);
    for c in &ws.columns {
        h.s(c);
    }
    // PK + sort resolved to NAMES.
    h.u32(ws.primary_key.len() as u32);
    for &pk in &ws.primary_key {
        h.s(&ws.columns[pk as usize]);
    }
    h.u32(ws.sort.len() as u32);
    for &(c, asc) in &ws.sort {
        h.s(&ws.columns[c as usize]);
        h.u8(asc as u8);
    }
    h.u8(ws.singular as u8);
    // Relationships in slot order: name + (child level recursively | gating marker).
    h.u32(ws.relationships.len() as u32);
    for r in &ws.relationships {
        h.s(&r.name);
        match &r.child {
            Some(child) => {
                h.u8(1);
                hash_level(h, child);
                // Scalar-projection marker (`REDUCE-DESIGN.md` §9): a presence byte, then
                // — when present — the projected column's NAME in the child schema
                // (semantic identity, index-independent, matching PK/sort). The empty
                // identity value is NOT hashed: a bare JS cell can't reproduce its type
                // variant byte-for-byte, and the projected-column name + child schema
                // already pin the meaningful drift.
                match &r.project {
                    Some(p) => {
                        h.u8(1);
                        h.s(&child.columns[p.col as usize]);
                    }
                    None => h.u8(0),
                }
            }
            None => h.u8(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{RelDef, Schema};

    fn comment_schema() -> Schema {
        Schema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)])
    }
    fn issue_schema() -> Schema {
        Schema::new(vec!["id", "val"], vec![0], vec![(1, true), (0, true)]).with_relationships(
            vec![
                RelDef::related("comments", comment_schema()),
                RelDef::new("gate"),
            ],
        )
    }

    #[test]
    fn to_wire_shapes_columns_sort_and_relationships() {
        let w = to_wire(&issue_schema());
        assert_eq!(
            w.columns.iter().map(|c| &**c).collect::<Vec<_>>(),
            vec!["id", "val"]
        );
        assert_eq!(w.primary_key, vec![0]);
        assert_eq!(w.sort, vec![(1, true), (0, true)]); // resolved sort: val asc, id asc
        assert!(!w.singular);
        assert_eq!(w.relationships.len(), 2);
        // In-view "comments" carries a child schema; gating "gate" does not.
        assert_eq!(&*w.relationships[0].name, "comments");
        assert_eq!(w.relationships[0].slot, 0);
        assert!(w.relationships[0].child.is_some());
        assert_eq!(&*w.relationships[1].name, "gate");
        assert!(w.relationships[1].child.is_none());
        // Child level resolved.
        let child = w.relationships[0].child.as_ref().unwrap();
        assert_eq!(child.sort, vec![(0, true)]);
        assert!(child.relationships.is_empty());
    }

    #[test]
    fn schema_round_trips_through_wire() {
        let w = to_wire(&issue_schema());
        let rebuilt = to_schema(&w);
        // The wire form of the rebuilt schema is identical (every field the receiver
        // uses survives Schema -> WireSchema -> Schema).
        assert_eq!(to_wire(&rebuilt), w);
    }

    #[test]
    fn fingerprint_is_deterministic_and_round_trip_stable() {
        let w = to_wire(&issue_schema());
        assert_eq!(schema_fp(&w), schema_fp(&w));
        // Stable across the Schema round-trip.
        assert_eq!(schema_fp(&to_wire(&to_schema(&w))), schema_fp(&w));
    }

    #[test]
    fn fingerprint_detects_drift() {
        let base = schema_fp(&to_wire(&issue_schema()));

        // Column rename.
        let renamed = Schema::new(vec!["id", "value"], vec![0], vec![(1, true), (0, true)])
            .with_relationships(vec![
                RelDef::related("comments", comment_schema()),
                RelDef::new("gate"),
            ]);
        assert_ne!(schema_fp(&to_wire(&renamed)), base, "column rename");

        // Sort direction change.
        let resorted = Schema::new(vec!["id", "val"], vec![0], vec![(1, false), (0, true)])
            .with_relationships(vec![
                RelDef::related("comments", comment_schema()),
                RelDef::new("gate"),
            ]);
        assert_ne!(schema_fp(&to_wire(&resorted)), base, "sort dir change");

        // Singular flip.
        let mut singular = issue_schema();
        singular.singular = true;
        assert_ne!(schema_fp(&to_wire(&singular)), base, "singular flip");

        // Relationship in-view → gating (drop the child schema).
        let gated = Schema::new(vec!["id", "val"], vec![0], vec![(1, true), (0, true)])
            .with_relationships(vec![RelDef::new("comments"), RelDef::new("gate")]);
        assert_ne!(schema_fp(&to_wire(&gated)), base, "rel in-view->gating");

        // Child schema drift (rename a child column).
        let child_drift = Schema::new(vec!["id", "val"], vec![0], vec![(1, true), (0, true)])
            .with_relationships(vec![
                RelDef::related(
                    "comments",
                    Schema::new(vec!["id", "issue", "body"], vec![0], vec![(0, true)]),
                ),
                RelDef::new("gate"),
            ]);
        assert_ne!(
            schema_fp(&to_wire(&child_drift)),
            base,
            "child column rename"
        );
    }

    #[cfg(any(feature = "testkit", feature = "serde"))]
    #[test]
    fn wire_schema_serde_round_trips() {
        let w = to_wire(&issue_schema());
        let json = serde_json::to_value(&w).expect("serialize");
        let back: WireSchema = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back, w);
        assert_eq!(schema_fp(&back), schema_fp(&w));
    }

    // --- scalar projection (REDUCE-DESIGN.md §9) ---------------------------------

    use crate::value::OwnedValue as V;

    // issue { commentCount: count(comments) } — the aggregate child [issueID, count],
    // attached as a scalar-projected (.singular) relationship that surfaces col 1
    // (count) with identity 0 for a childless issue.
    fn agg_child() -> Schema {
        let mut s = Schema::new(vec!["issueID", "count"], vec![0], vec![(0, true)]);
        s.singular = true;
        s
    }
    fn projected_schema() -> Schema {
        Schema::new(vec!["id", "val"], vec![0], vec![(0, true)]).with_relationships(vec![
            RelDef::related("commentCount", agg_child()).project_scalar(1, V::Int(0)),
        ])
    }

    #[test]
    fn projection_round_trips_through_wire() {
        let w = to_wire(&projected_schema());
        let p = w.relationships[0]
            .project
            .as_ref()
            .expect("projection on wire");
        assert_eq!(p.col, 1);
        // The whole Schema -> WireSchema -> Schema -> WireSchema is stable.
        assert_eq!(to_wire(&to_schema(&w)), w);
    }

    #[test]
    fn projection_perturbs_fingerprint() {
        // A singular-but-not-projected variant (the `.one()` object shape) vs the same
        // relationship scalar-projected: the projection presence byte + projected column
        // name must perturb the fingerprint (so a sender↔receiver disagreement is caught).
        let singular_only = Schema::new(vec!["id", "val"], vec![0], vec![(0, true)])
            .with_relationships(vec![RelDef::related("commentCount", agg_child())]);
        assert_ne!(
            schema_fp(&to_wire(&projected_schema())),
            schema_fp(&to_wire(&singular_only)),
            "projection marker must perturb the fingerprint"
        );
    }

    #[test]
    fn fingerprint_detects_projected_column_change() {
        let base = schema_fp(&to_wire(&projected_schema()));
        // Project a DIFFERENT child column (issueID, col 0) with the same identity.
        let other_col = Schema::new(vec!["id", "val"], vec![0], vec![(0, true)])
            .with_relationships(vec![
                RelDef::related("commentCount", agg_child()).project_scalar(0, V::Int(0))
            ]);
        assert_ne!(
            schema_fp(&to_wire(&other_col)),
            base,
            "projected column name is part of the fingerprint"
        );
    }

    #[cfg(any(feature = "testkit", feature = "serde"))]
    #[test]
    fn projection_serde_round_trips() {
        let w = to_wire(&projected_schema());
        let json = serde_json::to_value(&w).expect("serialize");
        let back: WireSchema = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back, w);
        assert_eq!(schema_fp(&back), schema_fp(&w));
    }
}
