//! The flat-change **subscription protocol**: the batch envelope, epoch/seq framing,
//! and the receiver-side safety rules (`FLAT-CHANGES-DESIGN.md` §5.4/§5.5/§2.3).
//!
//! This is the layer that turns the parts — the flat change stream ([`crate::flat`]),
//! the reference [`Receiver`] ([`crate::flat_receiver`]), and the wire schema +
//! fingerprint ([`crate::wire_schema`]) — into something a transport carries.
//!
//! ## Shape
//!
//! - A subscription opens with a [`Hello`] (sent once): the `epoch`, the
//!   [`COMPARATOR_VERSION`] contract, the [`WireSchema`], and its [`SchemaFp`].
//! - The **hydrate snapshot** establishes the baseline (logical seq 0): either a single
//!   [`Batch`] (seq 0) via [`Publisher::snapshot`], or — for a large `O(full result)`
//!   snapshot — a sequence of [`SnapshotChunk`]s via [`Publisher::snapshot_chunks`]
//!   terminated by a `last` marker (the receiver renders only after it).
//! - Then incremental [`Batch`]es flow at **seq 1, 2, …** (one per non-empty
//!   transaction). Each batch carries `{ epoch, seq, schema_fp, events }`; `events` apply
//!   in array order and the receiver renders only after the whole batch.
//!
//! ## Sender ([`Publisher`])
//!
//! Drives the seq counter and stamps every batch with the subscription's `epoch` +
//! `schema_fp`. **Empty transactions emit no batch and consume no seq** — so `seq` is
//! gap-free *over emitted batches*, which is exactly what lets the receiver treat any
//! gap as a lost batch.
//!
//! ## Receiver ([`Subscriber`])
//!
//! Wraps a [`Receiver`] and enforces (§2.3): the comparator contract (at [`Hello`]),
//! and per batch — epoch match, schema-fingerprint match, and strict in-order seq.
//! A **duplicate** (already-applied seq) is discarded idempotently (`rc` add/remove are
//! NOT idempotent, so re-applying would corrupt). A **gap** (seq beyond the next
//! expected) is unrecoverable from deltas — the sender cannot re-derive a missed one —
//! so the only repair is a full re-hydrate under a **new epoch**; stale in-flight
//! batches from the old epoch are then rejected by [`ProtocolError::EpochMismatch`].

use crate::changes::CaughtChange;
use crate::flat::{flatten_all, FlatChange};
use crate::flat_receiver::{Receiver, RecvNode};
use crate::value::Schema;
use crate::wire_schema::{schema_fp, to_schema, to_wire, SchemaFp, WireSchema, COMPARATOR_VERSION};

/// The subscription handshake, sent once before any [`Batch`].
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug)]
pub struct Hello {
    pub epoch: u64,
    /// The `compare_values` algorithm-contract version the sender used (§4/§5.5).
    pub comparator_version: u32,
    pub schema: WireSchema,
    pub schema_fp: SchemaFp,
}

/// One transaction's flat changes (or the seq-0 hydrate snapshot). `events` apply in
/// order; the receiver renders only after the whole batch (`FLAT-CHANGES-DESIGN.md` §5.4).
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug)]
pub struct Batch {
    pub epoch: u64,
    pub seq: u64,
    pub schema_fp: SchemaFp,
    pub events: Vec<FlatChange>,
}

/// One frame of a **chunked hydrate snapshot** (`FLAT-CHANGES-DESIGN.md` §5.4). The
/// initial snapshot is `O(full result)` — the whole tree as top-level `Add`s (each with
/// its inline subtree) — so it is paged across `SnapshotChunk`s rather than one giant
/// batch. `adds` is a slice of those top-level `Add`s; `index` is gap-free within the
/// snapshot; `last` is the `snapshot_complete` marker (the receiver renders only after
/// it). All chunks carry the subscription `epoch` + `schema_fp` so a mid-snapshot drift
/// is caught. A chunk's `adds` apply atomically (each carries its full subtree).
#[cfg_attr(
    any(feature = "testkit", feature = "serde"),
    derive(serde::Serialize, serde::Deserialize)
)]
#[derive(Clone, Debug)]
pub struct SnapshotChunk {
    pub epoch: u64,
    pub schema_fp: SchemaFp,
    /// 0-based, gap-free within this snapshot.
    pub index: u64,
    /// The `snapshot_complete` marker — `true` on the final chunk.
    pub last: bool,
    /// Top-level `Add`s (each with its inline subtree) for this page.
    pub adds: Vec<FlatChange>,
}

/// The outcome of applying a [`SnapshotChunk`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapStatus {
    /// Chunk applied; more chunks expected.
    Accepted,
    /// The final (`last`) chunk applied — the snapshot is complete; the tree may render
    /// and incremental [`Batch`]es (seq ≥ 1) may follow.
    Complete,
    /// An already-applied (or post-completion) chunk — discarded idempotently.
    Duplicate,
}

/// A protocol violation a [`Subscriber`] surfaces. All but a duplicate are fatal to the
/// current subscription — the consumer must discard its tree and re-subscribe (the
/// sender re-hydrates under a new epoch).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    /// The sender's comparator contract differs — reconstruction would silently corrupt.
    ComparatorMismatch { expected: u32, got: u32 },
    /// A batch from a different (usually stale) subscription epoch.
    EpochMismatch { expected: u64, got: u64 },
    /// The batch (or advertised) schema fingerprint differs from the subscribed one.
    SchemaMismatch { expected: SchemaFp, got: SchemaFp },
    /// A seq beyond the next expected ⇒ a batch was lost. Unrecoverable from deltas;
    /// re-hydrate. (`expected` is the next seq the receiver wanted.)
    Gap { expected: u64, got: u64 },
}

/// The outcome of applying a [`Batch`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applied {
    /// Applied; the reconstructed tree advanced.
    Applied,
    /// An already-applied seq (re-delivery) — discarded idempotently, tree unchanged.
    Duplicate,
}

/// Sender side: stamps batches with the subscription `epoch` + `schema_fp` and drives
/// the gap-free seq. Graph-agnostic — the caller drains the change-sink and hands the
/// [`CaughtChange`]s here.
pub struct Publisher {
    epoch: u64,
    schema: WireSchema,
    schema_fp: SchemaFp,
    next_seq: u64,
}

impl Publisher {
    /// Open a publisher at `epoch` over a hierarchical view [`Schema`]. Bump `epoch`
    /// for each fresh (re-)subscription so the receiver can reject stale batches.
    pub fn new(epoch: u64, view_schema: &Schema) -> Publisher {
        let schema = to_wire(view_schema);
        let fp = schema_fp(&schema);
        Publisher {
            epoch,
            schema,
            schema_fp: fp,
            next_seq: 0,
        }
    }

    /// The handshake to send first.
    pub fn hello(&self) -> Hello {
        Hello {
            epoch: self.epoch,
            comparator_version: COMPARATOR_VERSION,
            schema: self.schema.clone(),
            schema_fp: self.schema_fp,
        }
    }

    /// The hydrate snapshot (the flat `Add`s from
    /// [`Graph::try_hydrate_change_sink`](crate::graph::Graph::try_hydrate_change_sink)) as a
    /// single batch (seq 0). Always emitted — even for an empty result — so the receiver
    /// learns the snapshot is complete. For a large result prefer [`snapshot_chunks`].
    ///
    /// [`snapshot_chunks`]: Publisher::snapshot_chunks
    pub fn snapshot(&mut self, caught: &[CaughtChange]) -> Batch {
        self.emit(caught)
    }

    /// The hydrate snapshot as a sequence of [`SnapshotChunk`]s of at most
    /// `max_per_chunk` top-level entries each (the last chunk carries `last = true`).
    /// Use this instead of [`snapshot`](Publisher::snapshot) when the result is large:
    /// the snapshot is `O(full result)`, so a single batch can blow a transport frame /
    /// pin peak memory. An empty result still yields one terminal (`last`) chunk so the
    /// receiver gets the completion marker.
    ///
    /// Reserves the seq-0 slot for the baseline — incremental [`commit`](Publisher::commit)s
    /// start at seq 1, exactly as after [`snapshot`](Publisher::snapshot). Call this
    /// (or `snapshot`) once, **before** any `commit`.
    pub fn snapshot_chunks(
        &mut self,
        caught: &[CaughtChange],
        max_per_chunk: usize,
    ) -> Vec<SnapshotChunk> {
        assert!(max_per_chunk >= 1, "max_per_chunk must be >= 1");
        // The baseline occupies the logical seq-0 slot; increments start at seq 1.
        self.next_seq = 1;
        let events = flatten_all(caught);
        if events.is_empty() {
            return vec![SnapshotChunk {
                epoch: self.epoch,
                schema_fp: self.schema_fp,
                index: 0,
                last: true,
                adds: Vec::new(),
            }];
        }
        let total = events.len().div_ceil(max_per_chunk);
        events
            .chunks(max_per_chunk)
            .enumerate()
            .map(|(i, slice)| SnapshotChunk {
                epoch: self.epoch,
                schema_fp: self.schema_fp,
                index: i as u64,
                last: i + 1 == total,
                adds: slice.to_vec(),
            })
            .collect()
    }

    /// Wrap one transaction's drained changes
    /// ([`Graph::take_sink_changes`](crate::graph::Graph::take_sink_changes)). Returns
    /// `None` for an empty transaction — **no batch, no seq consumed** — keeping seq
    /// gap-free over emitted batches.
    pub fn commit(&mut self, caught: &[CaughtChange]) -> Option<Batch> {
        if caught.is_empty() {
            return None;
        }
        Some(self.emit(caught))
    }

    fn emit(&mut self, caught: &[CaughtChange]) -> Batch {
        let seq = self.next_seq;
        self.next_seq += 1;
        Batch {
            epoch: self.epoch,
            seq,
            schema_fp: self.schema_fp,
            events: flatten_all(caught),
        }
    }

    /// The subscription epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

/// The subscriber's lifecycle: establishing the baseline, then live increments.
#[derive(Clone, Copy, Debug)]
enum Phase {
    /// Establishing the baseline. `next_chunk` is the next [`SnapshotChunk`] index
    /// expected; `0` also admits a single-shot [`snapshot`](Publisher::snapshot) batch.
    Snapshot { next_chunk: u64 },
    /// Baseline established; only incremental [`Batch`]es (seq ≥ 1) are accepted.
    Live { last_seq: u64 },
}

/// Receiver side: a [`Receiver`] plus the protocol state, enforcing the §2.3/§5.4 rules.
/// The baseline is established by either a single-shot [`apply`](Subscriber::apply) of
/// the seq-0 snapshot batch **or** a sequence of [`apply_snapshot_chunk`] calls; then
/// incremental batches flow.
///
/// [`apply_snapshot_chunk`]: Subscriber::apply_snapshot_chunk
#[derive(Debug)]
pub struct Subscriber {
    epoch: u64,
    schema_fp: SchemaFp,
    recv: Receiver,
    phase: Phase,
}

impl Subscriber {
    /// Open from a [`Hello`]: hard-reject a comparator-contract mismatch (§5.5), verify
    /// the advertised fingerprint matches the shipped schema, and build the receiver
    /// from that schema. Apply the snapshot next (single-shot or chunked).
    pub fn open(hello: &Hello) -> Result<Subscriber, ProtocolError> {
        if hello.comparator_version != COMPARATOR_VERSION {
            return Err(ProtocolError::ComparatorMismatch {
                expected: COMPARATOR_VERSION,
                got: hello.comparator_version,
            });
        }
        // Defensive: the advertised fp must equal the fp of the shipped schema.
        let computed = schema_fp(&hello.schema);
        if computed != hello.schema_fp {
            return Err(ProtocolError::SchemaMismatch {
                expected: hello.schema_fp,
                got: computed,
            });
        }
        Ok(Subscriber {
            epoch: hello.epoch,
            schema_fp: hello.schema_fp,
            recv: Receiver::new(to_schema(&hello.schema)),
            phase: Phase::Snapshot { next_chunk: 0 },
        })
    }

    #[inline]
    fn check_frame(&self, epoch: u64, fp: SchemaFp) -> Result<(), ProtocolError> {
        if epoch != self.epoch {
            return Err(ProtocolError::EpochMismatch {
                expected: self.epoch,
                got: epoch,
            });
        }
        if fp != self.schema_fp {
            return Err(ProtocolError::SchemaMismatch {
                expected: self.schema_fp,
                got: fp,
            });
        }
        Ok(())
    }

    /// Apply one [`SnapshotChunk`] of a chunked hydrate snapshot. Validates epoch +
    /// schema-fp + strict in-order chunk index, applies its `adds`, and on the `last`
    /// chunk transitions to live (snapshot complete — the tree may render).
    /// A re-delivered (or post-completion) chunk is a no-op [`SnapStatus::Duplicate`];
    /// a chunk-index gap is a fatal [`ProtocolError::Gap`] (re-hydrate).
    pub fn apply_snapshot_chunk(
        &mut self,
        chunk: &SnapshotChunk,
    ) -> Result<SnapStatus, ProtocolError> {
        self.check_frame(chunk.epoch, chunk.schema_fp)?;
        match &mut self.phase {
            // Snapshot already complete — any further chunk is a stale re-delivery.
            Phase::Live { .. } => Ok(SnapStatus::Duplicate),
            Phase::Snapshot { next_chunk } => {
                if chunk.index < *next_chunk {
                    return Ok(SnapStatus::Duplicate);
                }
                if chunk.index > *next_chunk {
                    return Err(ProtocolError::Gap {
                        expected: *next_chunk,
                        got: chunk.index,
                    });
                }
                self.recv.apply_all(&chunk.adds);
                if chunk.last {
                    self.phase = Phase::Live { last_seq: 0 };
                    Ok(SnapStatus::Complete)
                } else {
                    *next_chunk += 1;
                    Ok(SnapStatus::Accepted)
                }
            }
        }
    }

    /// Apply one incremental [`Batch`] (or the single-shot seq-0 snapshot batch).
    /// Validates epoch + schema-fp + strict in-order seq, then folds the events in
    /// order. A duplicate (already-applied seq) is a no-op [`Applied::Duplicate`]; a gap
    /// is a fatal [`ProtocolError::Gap`] (re-hydrate).
    pub fn apply(&mut self, batch: &Batch) -> Result<Applied, ProtocolError> {
        self.check_frame(batch.epoch, batch.schema_fp)?;
        match &mut self.phase {
            Phase::Snapshot { next_chunk } => {
                // Single-shot baseline: only valid before any chunk, at seq 0.
                if *next_chunk == 0 && batch.seq == 0 {
                    self.recv.apply_all(&batch.events);
                    self.phase = Phase::Live { last_seq: 0 };
                    Ok(Applied::Applied)
                } else {
                    // Mid chunked-snapshot (last chunk lost) or baseline missing —
                    // unrecoverable from deltas; re-hydrate.
                    Err(ProtocolError::Gap {
                        expected: 0,
                        got: batch.seq,
                    })
                }
            }
            Phase::Live { last_seq } => {
                let expected = *last_seq + 1;
                if batch.seq < expected {
                    // Already applied (re-delivery). `rc` ops are not idempotent — discard.
                    return Ok(Applied::Duplicate);
                }
                if batch.seq > expected {
                    return Err(ProtocolError::Gap {
                        expected,
                        got: batch.seq,
                    });
                }
                self.recv.apply_all(&batch.events);
                *last_seq = batch.seq;
                Ok(Applied::Applied)
            }
        }
    }

    /// The reconstructed top-level result (`root[""]`). Valid to read once the snapshot
    /// is complete ([`snapshot_complete`](Subscriber::snapshot_complete)); mid-snapshot
    /// it is a partial tree (render only after completion — §5.4).
    pub fn top(&self) -> &[RecvNode] {
        self.recv.top()
    }

    /// The subscription epoch this subscriber accepts.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Whether the hydrate snapshot is complete (the tree may render).
    pub fn snapshot_complete(&self) -> bool {
        matches!(self.phase, Phase::Live { .. })
    }

    /// The last applied incremental seq (`None` until the snapshot is complete; `Some(0)`
    /// immediately after the snapshot, then the last applied batch seq).
    pub fn last_seq(&self) -> Option<u64> {
        match self.phase {
            Phase::Live { last_seq } => Some(last_seq),
            Phase::Snapshot { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changes::{CaughtChange, CaughtNode};
    use crate::value::{owned_row, OwnedValue as V, Schema};

    fn schema() -> Schema {
        Schema::new(vec!["id", "val"], vec![0], vec![(0, true)])
    }
    fn add(id: i64, val: i64) -> CaughtChange {
        CaughtChange::Add(CaughtNode {
            row: owned_row(vec![V::Int(id), V::Int(val)]),
            relationships: Default::default(),
        })
    }
    fn ids(top: &[RecvNode]) -> Vec<i64> {
        top.iter()
            .map(|n| match n.row.col(0) {
                crate::value::Value::Int(i) => i,
                o => panic!("{o:?}"),
            })
            .collect()
    }

    #[test]
    fn open_rejects_comparator_mismatch() {
        let pubr = Publisher::new(1, &schema());
        let mut hello = pubr.hello();
        hello.comparator_version = COMPARATOR_VERSION + 1;
        assert_eq!(
            Subscriber::open(&hello).unwrap_err(),
            ProtocolError::ComparatorMismatch {
                expected: COMPARATOR_VERSION,
                got: COMPARATOR_VERSION + 1,
            }
        );
    }

    #[test]
    fn open_rejects_forged_fingerprint() {
        let pubr = Publisher::new(1, &schema());
        let mut hello = pubr.hello();
        hello.schema_fp = SchemaFp(hello.schema_fp.0 ^ 0xdead); // lie about the fp
        assert!(matches!(
            Subscriber::open(&hello),
            Err(ProtocolError::SchemaMismatch { .. })
        ));
    }

    #[test]
    fn snapshot_then_in_order_commits_apply() {
        let mut pubr = Publisher::new(1, &schema());
        let mut sub = Subscriber::open(&pubr.hello()).unwrap();

        let snap = pubr.snapshot(&[add(1, 10), add(2, 20)]);
        assert_eq!(snap.seq, 0);
        assert_eq!(sub.apply(&snap), Ok(Applied::Applied));
        assert_eq!(ids(sub.top()), vec![1, 2]);

        let b1 = pubr.commit(&[add(3, 30)]).expect("non-empty");
        assert_eq!(b1.seq, 1);
        assert_eq!(sub.apply(&b1), Ok(Applied::Applied));
        assert_eq!(ids(sub.top()), vec![1, 2, 3]);
        assert_eq!(sub.last_seq(), Some(1));
    }

    #[test]
    fn empty_commit_emits_no_batch_and_keeps_seq_gap_free() {
        let mut pubr = Publisher::new(1, &schema());
        let mut sub = Subscriber::open(&pubr.hello()).unwrap();
        sub.apply(&pubr.snapshot(&[add(1, 10)])).unwrap(); // seq 0

        assert!(pubr.commit(&[]).is_none(), "empty txn → no batch");
        // The next real commit is still seq 1 (the empty txn consumed nothing).
        let b = pubr.commit(&[add(2, 20)]).expect("non-empty");
        assert_eq!(b.seq, 1);
        assert_eq!(sub.apply(&b), Ok(Applied::Applied));
        assert_eq!(ids(sub.top()), vec![1, 2]);
    }

    #[test]
    fn gap_is_rejected() {
        let mut pubr = Publisher::new(1, &schema());
        let mut sub = Subscriber::open(&pubr.hello()).unwrap();
        sub.apply(&pubr.snapshot(&[add(1, 10)])).unwrap(); // seq 0, expected next = 1

        // Fabricate a seq-2 batch (seq 1 was lost in transit).
        let mut b2 = pubr.commit(&[add(2, 20)]).unwrap(); // really seq 1
        b2.seq = 2;
        assert_eq!(
            sub.apply(&b2),
            Err(ProtocolError::Gap {
                expected: 1,
                got: 2
            })
        );
        // The tree did not advance.
        assert_eq!(ids(sub.top()), vec![1]);
    }

    #[test]
    fn duplicate_is_idempotently_discarded() {
        let mut pubr = Publisher::new(1, &schema());
        let mut sub = Subscriber::open(&pubr.hello()).unwrap();
        let snap = pubr.snapshot(&[add(1, 10)]);
        sub.apply(&snap).unwrap(); // seq 0
        let b1 = pubr.commit(&[add(2, 20)]).unwrap();
        sub.apply(&b1).unwrap(); // seq 1

        // Re-deliver seq 1: must NOT double-apply (rc add is not idempotent).
        assert_eq!(sub.apply(&b1), Ok(Applied::Duplicate));
        assert_eq!(ids(sub.top()), vec![1, 2]);
        assert_eq!(sub.top()[1].rc, 1, "duplicate add must not bump rc");
        // Re-deliver the snapshot (seq 0) too → still a no-op duplicate.
        assert_eq!(sub.apply(&snap), Ok(Applied::Duplicate));
        assert_eq!(ids(sub.top()), vec![1, 2]);
    }

    #[test]
    fn stale_epoch_batch_is_rejected_after_resubscribe() {
        // Epoch-1 subscription.
        let mut pub1 = Publisher::new(1, &schema());
        let mut sub1 = Subscriber::open(&pub1.hello()).unwrap();
        sub1.apply(&pub1.snapshot(&[add(1, 10)])).unwrap();
        let stale = pub1.commit(&[add(2, 20)]).unwrap(); // epoch 1, seq 1

        // A gap forced a re-subscribe at epoch 2 (fresh snapshot of the current state).
        let mut pub2 = Publisher::new(2, &schema());
        let mut sub2 = Subscriber::open(&pub2.hello()).unwrap();
        sub2.apply(&pub2.snapshot(&[add(1, 10), add(2, 20)]))
            .unwrap();
        assert_eq!(ids(sub2.top()), vec![1, 2]);

        // The stale epoch-1 batch is rejected by the epoch-2 subscriber.
        assert_eq!(
            sub2.apply(&stale),
            Err(ProtocolError::EpochMismatch {
                expected: 2,
                got: 1
            })
        );
    }

    #[test]
    fn chunked_snapshot_assembles_then_increments() {
        let mut pubr = Publisher::new(1, &schema());
        let mut sub = Subscriber::open(&pubr.hello()).unwrap();

        // Five entries, two per chunk → 3 chunks (indices 0,1,2; last on index 2).
        let chunks = pubr.snapshot_chunks(
            &[add(1, 10), add(2, 20), add(3, 30), add(4, 40), add(5, 50)],
            2,
        );
        assert_eq!(chunks.len(), 3);
        assert!(!chunks[0].last && !chunks[1].last && chunks[2].last);

        assert_eq!(
            sub.apply_snapshot_chunk(&chunks[0]),
            Ok(SnapStatus::Accepted)
        );
        assert!(!sub.snapshot_complete());
        assert_eq!(
            sub.apply_snapshot_chunk(&chunks[1]),
            Ok(SnapStatus::Accepted)
        );
        assert_eq!(
            sub.apply_snapshot_chunk(&chunks[2]),
            Ok(SnapStatus::Complete)
        );
        assert!(sub.snapshot_complete());
        assert_eq!(ids(sub.top()), vec![1, 2, 3, 4, 5]);
        assert_eq!(sub.last_seq(), Some(0));

        // Increments resume at seq 1 (the baseline reserved seq 0).
        let b = pubr.commit(&[add(6, 60)]).expect("non-empty");
        assert_eq!(b.seq, 1);
        assert_eq!(sub.apply(&b), Ok(Applied::Applied));
        assert_eq!(ids(sub.top()), vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn empty_chunked_snapshot_still_completes() {
        let mut pubr = Publisher::new(1, &schema());
        let mut sub = Subscriber::open(&pubr.hello()).unwrap();
        let chunks = pubr.snapshot_chunks(&[], 4);
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].last);
        assert_eq!(
            sub.apply_snapshot_chunk(&chunks[0]),
            Ok(SnapStatus::Complete)
        );
        assert!(sub.snapshot_complete());
        assert!(sub.top().is_empty());
    }

    #[test]
    fn snapshot_chunk_gap_and_duplicate() {
        let mut pubr = Publisher::new(1, &schema());
        let mut sub = Subscriber::open(&pubr.hello()).unwrap();
        let chunks = pubr.snapshot_chunks(&[add(1, 10), add(2, 20), add(3, 30)], 1);
        assert_eq!(chunks.len(), 3);

        sub.apply_snapshot_chunk(&chunks[0]).unwrap();
        // Skipping chunk 1 (deliver chunk 2) → gap.
        assert_eq!(
            sub.apply_snapshot_chunk(&chunks[2]),
            Err(ProtocolError::Gap {
                expected: 1,
                got: 2
            })
        );
        // Re-deliver chunk 0 → duplicate, no double-apply (rc unchanged).
        assert_eq!(
            sub.apply_snapshot_chunk(&chunks[0]),
            Ok(SnapStatus::Duplicate)
        );
        assert_eq!(sub.top()[0].rc, 1);
        // In order from here completes.
        sub.apply_snapshot_chunk(&chunks[1]).unwrap();
        assert_eq!(
            sub.apply_snapshot_chunk(&chunks[2]),
            Ok(SnapStatus::Complete)
        );
        assert_eq!(ids(sub.top()), vec![1, 2, 3]);
    }

    #[test]
    fn batch_mid_chunked_snapshot_is_a_gap() {
        let mut pubr = Publisher::new(1, &schema());
        let mut sub = Subscriber::open(&pubr.hello()).unwrap();
        let chunks = pubr.snapshot_chunks(&[add(1, 10), add(2, 20)], 1);
        sub.apply_snapshot_chunk(&chunks[0]).unwrap(); // chunk 0 of 2; not complete
                                                       // An incremental batch before the snapshot completes → gap (the `last`
                                                       // chunk was lost); the consumer must re-hydrate.
        let b = pubr.commit(&[add(9, 90)]).expect("non-empty");
        assert!(matches!(sub.apply(&b), Err(ProtocolError::Gap { .. })));
    }
}
