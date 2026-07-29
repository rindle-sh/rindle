//! `Cap` (`ivm/cap.ts`, spec `07` §3.8) — the **unordered** count-based limiter for
//! non-flipped `EXISTS` child pipelines. Cap is the sibling of [`Take`](crate::op::Take):
//! both bound a partition to `limit` rows and refill on remove, but Cap tracks
//! membership by a **primary-key set** (`CapState{size, pks}`) instead of a sorted
//! bound. Because only the *count* matters to `Exists` (`> 0` vs `== 0` up to the
//! limit), Cap needs no comparator — which lets the SQLite leaf skip `ORDER BY`.
//!
//! Cap asserts **no `start`, no `reverse`** (its only consumer is a join fetching
//! with a constraint that matches the partition key), and does **PK point-lookups**
//! on fetch rather than an ordered scan. Stateful: it reads/writes a [`StorageId`]
//! slot (the shipped [`StorageValue::Cap`] shape — `pks` are serialized strings, so
//! the point-lookup round-trips each through [`serialize_pk`]/[`deserialize_pk`]).

use std::cell::Cell;
use std::sync::Arc;

use crate::change::{Change, Constraint, FetchRequest, Node, NodeStream, OutEdge};
use crate::graph::{Graph, NodeId, StorageId};
use crate::op::partition::{
    constraint_matches_partition_key, encode_state_key, partition_key_unchanged,
};
use crate::storage::StorageValue;
use crate::value::{ColId, OwnedRow as Row, OwnedValue, Value};

/// `Cap`: unordered top-N by PK-set membership, backed by a [`StorageId`] slot.
pub struct Cap {
    /// Upstream input (the EXISTS-child connection, unordered in production).
    pub input: NodeId,
    /// Handle into the [`Graph`] storage arena holding this op's per-partition
    /// `CapState{size, pks}` under `encode_state_key`("cap", …).
    pub storage: StorageId,
    /// The `EXISTS_LIMIT` count. `0` ⇒ yields nothing and stores no state.
    pub limit: u32,
    /// The partition columns (the correlation **child** field), or `None` for a
    /// single global partition.
    pub partition_key: Option<Vec<ColId>>,
    /// The input's primary-key columns — the identity Cap tracks by
    /// (`serializePK`, `cap.ts:315`).
    pub primary_key: Vec<ColId>,
    /// The single downstream edge as a **port-carrying** [`OutEdge`] (Cap feeds a
    /// relationship join's child port). Wired via
    /// [`Graph::set_output`](crate::graph::Graph::set_output) /
    /// [`Graph::set_out_edge`](crate::graph::Graph::set_out_edge).
    pub output: Cell<Option<OutEdge>>,
}

impl Cap {
    pub fn new(
        input: NodeId,
        storage: StorageId,
        limit: u32,
        partition_key: Option<Vec<ColId>>,
        primary_key: Vec<ColId>,
    ) -> Cap {
        Cap {
            input,
            storage,
            limit,
            partition_key,
            primary_key,
            output: Cell::new(None),
        }
    }

    // --- storage helpers ----------------------------------------------------

    fn get_state(&self, g: &Graph, key: &str) -> Option<(u32, Vec<Box<str>>)> {
        match g.storage(self.storage).get(key) {
            None => None,
            Some(StorageValue::Cap { size, pks }) => Some((size, pks)),
            Some(other) => unreachable!("cap state slot held {other:?}"),
        }
    }

    fn set_state(&self, g: &Graph, key: &str, size: u32, pks: Vec<Box<str>>) {
        g.storage(self.storage)
            .set(key, StorageValue::Cap { size, pks });
    }

    /// The cap-state key for a partition identified by a **row** (`getCapStateKey`
    /// over a `Row`, `cap.ts:300`). Unpartitioned ⇒ the single global key.
    fn key_from_row(&self, row: &Row) -> String {
        match &self.partition_key {
            None => encode_state_key("cap", &[]),
            Some(pk) => {
                let vals: Vec<OwnedValue> = pk.iter().map(|&c| row.col(c).to_owned()).collect();
                encode_state_key("cap", &vals)
            }
        }
    }

    /// The cap-state key for a partition identified by a fetch **constraint**.
    fn key_from_constraint(&self, c: Option<&Constraint>) -> String {
        match (&self.partition_key, c) {
            (Some(pk), Some(c)) => {
                let vals: Vec<OwnedValue> = pk
                    .iter()
                    .map(|&col| {
                        c.iter()
                            .find(|(cc, _)| *cc == col)
                            .map(|(_, v)| v.clone())
                            .unwrap_or(OwnedValue::Null)
                    })
                    .collect();
                encode_state_key("cap", &vals)
            }
            _ => encode_state_key("cap", &[]),
        }
    }

    /// The partition constraint for a push on `row` (the reentrant refill fetch),
    /// or `None` when unpartitioned (a bare scan).
    fn constraint_for(&self, row: &Row) -> Option<Constraint> {
        self.partition_key
            .as_ref()
            .map(|pk| pk.iter().map(|&c| (c, row.col(c).to_owned())).collect())
    }

    /// Delete the per-partition slot identified by `constraint`, if this Cap is
    /// partitioned and the constraint covers its partition key — the sibling of
    /// [`Take::evict_partition`](crate::op::Take::evict_partition), called by the
    /// relationship join when a parent leaves a bounded parent view. Safe for the same
    /// reason: an un-hydrated Cap partition is re-hydrated by the join's constrained
    /// `fetch` on parent re-entry, and a push to a missing partition is dropped.
    ///
    /// Note: this addresses the parent-left-the-view source of a zombie partition; the
    /// EXISTS filter's per-parent re-fetch (`follow-ups/04-cap-partition-leak.md`) is a
    /// distinct re-creation path that still needs its own pass.
    pub fn evict_partition(&self, g: &Graph, constraint: &Constraint) {
        let Some(pk) = &self.partition_key else {
            return;
        };
        if !constraint_matches_partition_key(constraint, pk) {
            return;
        }
        let key = self.key_from_constraint(Some(constraint));
        g.storage(self.storage).del(&key);
    }

    // --- fetch --------------------------------------------------------------

    /// Lazy pull (`cap.ts:87`). Asserts no `start`/`reverse`. With state, fetches
    /// each tracked PK by a point-lookup; without state, hydrates.
    pub fn fetch<'g>(&'g self, g: &'g Graph, req: &FetchRequest) -> NodeStream<'g> {
        assert!(req.start.is_none(), "Cap does not support start");
        assert!(!req.reverse, "Cap does not support reverse");
        debug_assert!(
            self.partition_key.as_ref().is_none_or(|pk| req
                .constraint
                .as_ref()
                .is_some_and(|c| constraint_matches_partition_key(c, pk))),
            "Cap fetch: constraint must match partition key when partitioned"
        );

        let key = self.key_from_constraint(req.constraint.as_ref());
        match self.get_state(g, &key) {
            None => self.initial_fetch(g, req),
            Some((0, _)) => Box::new(std::iter::empty()),
            Some((_, pks)) => {
                // PK point-lookups: fetch each tracked row by its PK directly
                // (`cap.ts:113`). Eager — `limit` is tiny (EXISTS_LIMIT = 3).
                let mut out: Vec<Node<'g>> = Vec::new();
                for pk in &pks {
                    let c = deserialize_pk(pk, &self.primary_key);
                    out.extend(g.fetch(self.input, &FetchRequest::with_constraint(c)));
                }
                Box::new(out.into_iter())
            }
        }
    }

    /// `#initialFetch` (`cap.ts:125`): hydrate by recording the first `limit` rows'
    /// PKs. Eager (like [`Take::initial_fetch`](crate::op::Take)) — every real
    /// consumer fully drains a hydrate.
    fn initial_fetch<'g>(&'g self, g: &'g Graph, req: &FetchRequest) -> NodeStream<'g> {
        if self.limit == 0 {
            return Box::new(std::iter::empty());
        }
        let key = self.key_from_constraint(req.constraint.as_ref());
        debug_assert!(
            self.get_state(g, &key).is_none(),
            "Cap initial fetch: state should be undefined"
        );
        let mut size = 0u32;
        let mut pks: Vec<Box<str>> = Vec::new();
        let mut out: Vec<Node<'g>> = Vec::new();
        for node in g.fetch(self.input, req) {
            pks.push(serialize_pk(&node.row, &self.primary_key));
            out.push(node);
            size += 1;
            if size == self.limit {
                break;
            }
        }
        self.set_state(g, &key, size, pks);
        Box::new(out.into_iter())
    }

    // --- push ---------------------------------------------------------------

    /// Eager push (`cap.ts:177`). Edit routes to `Cap::push_edit`; otherwise look
    /// up the partition's state (drop if never hydrated) and dispatch Add / Remove /
    /// Child by PK-set membership.
    pub fn push<'g>(&'g self, g: &'g Graph, change: Change<'g>) {
        if matches!(change, Change::Edit { .. }) {
            return self.push_edit(g, change);
        }

        let row = match &change {
            Change::Add(n) | Change::Remove(n) => &n.row,
            Change::Child { node, .. } => &node.row,
            Change::Edit { .. } => unreachable!(),
        };
        let key = self.key_from_row(row);
        let Some((size, pks)) = self.get_state(g, &key) else {
            return;
        };
        let pk = serialize_pk(row, &self.primary_key);
        let out = self.output.get().expect("Cap output not wired");

        match change {
            Change::Add(node) => {
                if size < self.limit {
                    let mut pks = pks;
                    pks.push(pk);
                    self.set_state(g, &key, size + 1, pks);
                    g.push(out.node, Change::Add(node), out.port);
                }
                // else full ⇒ drop
            }
            Change::Remove(node) => self.push_remove(g, node, out, &key, size, pks, &pk),
            Change::Child { node, rel, child } => {
                if pks.iter().any(|p| **p == *pk) {
                    g.push(out.node, Change::Child { node, rel, child }, out.port);
                }
            }
            Change::Edit { .. } => unreachable!(),
        }
    }

    /// Remove (`cap.ts:203`). Untracked PK ⇒ drop. Else remove the PK and try to
    /// refill from the first partition row whose PK is not in the (post-removal)
    /// set: store WITHOUT the replacement, forward the Remove, then add the
    /// replacement's PK and forward its Add (the state-write ordering that hides the
    /// in-flight change from a reentrant refetch). No refill ⇒ just the Remove.
    #[allow(clippy::too_many_arguments)]
    fn push_remove<'g>(
        &'g self,
        g: &'g Graph,
        node: Node<'g>,
        out: OutEdge,
        key: &str,
        size: u32,
        pks: Vec<Box<str>>,
        pk: &str,
    ) {
        let Some(idx) = pks.iter().position(|p| **p == *pk) else {
            return; // not in our set ⇒ drop
        };
        let mut pks = pks;
        pks.remove(idx);
        let new_size = size - 1;

        // Refill: scan the partition for the first row whose PK is not tracked.
        let req = FetchRequest {
            constraint: self.constraint_for(&node.row),
            ..Default::default()
        };
        let mut replacement: Option<Node<'g>> = None;
        for n in g.fetch(self.input, &req) {
            let npk = serialize_pk(&n.row, &self.primary_key);
            if !pks.iter().any(|p| **p == *npk) {
                replacement = Some(n);
                break;
            }
        }

        match replacement {
            Some(repl) => {
                // Store WITHOUT the replacement, forward the Remove, then add it.
                self.set_state(g, key, new_size, pks.clone());
                g.push(out.node, Change::Remove(node), out.port);
                let repl_pk = serialize_pk(&repl.row, &self.primary_key);
                let mut pks = pks;
                pks.push(repl_pk);
                self.set_state(g, key, new_size + 1, pks);
                g.push(out.node, Change::Add(repl), out.port);
            }
            None => {
                self.set_state(g, key, new_size, pks);
                g.push(out.node, Change::Remove(node), out.port);
            }
        }
    }

    /// `#pushEditChange` (`cap.ts:260`). Asserts the partition key is unchanged. If
    /// the old PK is tracked, swap it for the new PK (when changed) and forward the
    /// Edit; else drop.
    fn push_edit<'g>(&'g self, g: &'g Graph, change: Change<'g>) {
        let Change::Edit { node, old } = change else {
            unreachable!()
        };
        if let Some(pk) = &self.partition_key {
            debug_assert!(
                partition_key_unchanged(&old.row, &node.row, pk),
                "Cap: unexpected change of partition key"
            );
        }
        let key = self.key_from_row(&old.row);
        let Some((size, pks)) = self.get_state(g, &key) else {
            return;
        };
        let old_pk = serialize_pk(&old.row, &self.primary_key);
        let out = self.output.get().expect("Cap output not wired");

        if pks.iter().any(|p| **p == *old_pk) {
            let new_pk = serialize_pk(&node.row, &self.primary_key);
            if new_pk != old_pk {
                let pks: Vec<Box<str>> = pks
                    .into_iter()
                    .map(|p| if *p == *old_pk { new_pk.clone() } else { p })
                    .collect();
                self.set_state(g, &key, size, pks);
            }
            g.push(out.node, Change::Edit { node, old }, out.port);
        }
        // else not tracked ⇒ drop
    }
}

/// `serializePK` (`cap.ts:315`): a reversible, self-describing encoding of the
/// row's primary-key values — the JS `JSON.stringify(pk.map(k => row[k]))`. Each
/// value is `<tag><byte-len>:<content>` (tag selects the type; numeric/bool/null
/// content is ASCII, str/json content is the raw UTF-8), so [`deserialize_pk`]
/// round-trips it exactly without needing the schema's column types.
pub(crate) fn serialize_pk(row: &Row, pk: &[ColId]) -> Box<str> {
    let mut s = String::new();
    for &c in pk {
        let (tag, content): (char, std::borrow::Cow<'_, str>) = match row.col(c) {
            // A PK cell is never `Absent` in practice (presence-required); a distinct
            // round-trippable tag keeps the codec total.
            Value::Absent => ('a', "".into()),
            Value::Null => ('n', "".into()),
            Value::Bool(b) => ('b', if b { "1".into() } else { "0".into() }),
            Value::Int(i) => ('i', i.to_string().into()),
            Value::Float(f) => ('f', f.to_bits().to_string().into()),
            // Row text is UTF-8-validated at construction.
            Value::Str(x) => ('s', String::from_utf8_lossy(x)),
            Value::Json(x) => ('j', String::from_utf8_lossy(x)),
        };
        s.push(tag);
        s.push_str(&content.len().to_string());
        s.push(':');
        s.push_str(&content);
    }
    s.into_boxed_str()
}

/// `deserializePKToConstraint` (`cap.ts:319`): parse a [`serialize_pk`] string back
/// into a [`Constraint`] mapping each primary-key column to its value, for the
/// point-lookup fetch.
pub(crate) fn deserialize_pk(pk: &str, primary_key: &[ColId]) -> Constraint {
    let bytes = pk.as_bytes();
    let mut i = 0;
    let mut out = Vec::with_capacity(primary_key.len());
    for &col in primary_key {
        let tag = bytes[i];
        i += 1;
        let mut len = 0usize;
        while bytes[i] != b':' {
            len = len * 10 + usize::from(bytes[i] - b'0');
            i += 1;
        }
        i += 1; // skip ':'
        let content = &pk[i..i + len];
        i += len;
        let v = match tag {
            b'a' => OwnedValue::Absent,
            b'n' => OwnedValue::Null,
            b'b' => OwnedValue::Bool(content == "1"),
            b'i' => OwnedValue::Int(content.parse().expect("pk int")),
            b'f' => OwnedValue::Float(f64::from_bits(content.parse().expect("pk float bits"))),
            b's' => OwnedValue::str(content),
            b'j' => OwnedValue::Json(Arc::from(content)),
            other => unreachable!("bad pk tag {other}"),
        };
        out.push((col, v));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{deserialize_pk, serialize_pk};
    use crate::change::constraint_matches;
    use crate::value::{owned_row, ColId, OwnedValue};

    #[test]
    fn pk_codec_round_trips() {
        // Each PK shape must deserialize to a constraint that matches its source row
        // (the embedded ':' and multibyte string prove the byte-length prefix holds).
        let cases: Vec<(Vec<OwnedValue>, Vec<ColId>)> = vec![
            (vec![OwnedValue::Int(42)], vec![0]),
            (vec![OwnedValue::Int(-7)], vec![0]),
            (vec![OwnedValue::str("héllo:1")], vec![0]),
            (vec![OwnedValue::str("a"), OwnedValue::Int(2)], vec![0, 1]),
            (vec![OwnedValue::Int(1), OwnedValue::str("x:y")], vec![0, 1]),
        ];
        for (vals, pk) in cases {
            let r = owned_row(vals.clone());
            let s = serialize_pk(&r, &pk);
            let c = deserialize_pk(&s, &pk);
            assert!(
                constraint_matches(&r, &c),
                "round-trip failed for {vals:?} -> {s:?}"
            );
        }
    }

    #[test]
    fn pk_codec_distinguishes_tuples_and_types() {
        // Length-prefixing keeps distinct tuples distinct, and type tags keep
        // `1` (int) apart from `"1"` (str).
        let r2 = owned_row(vec![OwnedValue::str("a"), OwnedValue::str("b")]);
        let r1 = owned_row(vec![OwnedValue::str("ab")]);
        assert_ne!(serialize_pk(&r2, &[0, 1]), serialize_pk(&r1, &[0]));

        let int1 = owned_row(vec![OwnedValue::Int(1)]);
        let str1 = owned_row(vec![OwnedValue::str("1")]);
        assert_ne!(serialize_pk(&int1, &[0]), serialize_pk(&str1, &[0]));
    }
}
