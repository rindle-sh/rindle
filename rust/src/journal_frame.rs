//! The cross-process **journal frame envelope** — the typed header in front of every
//! opaque change payload (`designs-implemented/211-HCTREE-LEADER-CDC-DESIGN.md` §4.1).
//!
//! Today's `_rindle_change_log` carries `kind`, `run_id`, `run_rows`, `rows_cum`, and
//! `committed_at` as **columns**, and the plane reads them as columns: the epoch fence
//! is a `run_id` point lookup at the cursor, C_grace retention reads `committed_at`,
//! and the lag-shed fold groups on `kind` and accounts with the run totals. Under 211
//! the master's log is `hct_journal(cid, query, snapshot, logptr)` — no home for those
//! columns — so they move into this header, encoded in front of the payload bytes
//! handed to `sqlite3_hct_journal_leader_commit`. The same envelope is what the S3
//! shipping plane (`rindle-backup`, `designs-implemented/212-JOURNAL-S3-SHIPPING-DESIGN.md` §2.2)
//! frames into archival segments, so one decode serves the sender, the shipper, and
//! restore replay.
//!
//! Deliberately **not** in the header:
//!
//! - **cid** — assigned by the engine *inside* `leader_commit`, so the producer cannot
//!   know it at encode time. The container carries it (the journal's `cid` column; the
//!   segment framing). Keeping it out also means there is exactly one source of truth.
//! - **chunk_seq** — one HCTree cid is still one atomic transaction, but its opaque payload may
//!   use the versioned chunk container documented at
//!   [`encode_payload_chunks`](crate::journal_frame::encode_payload_chunks). Chunk sequence is
//!   implicit in container order, so it does not belong in the transaction header.
//!
//! Wire layout (all integers little-endian):
//!
//! ```text
//! u8   header format version (1 = legacy, 2 = chunk container — see the constants)
//! u8   kind (0 = rows, 1 = ddl, 2 = empty; 3 reserved — see [`FrameKind`])
//! u8   flags (bit 0: run_id present, bit 1: run totals present; others reserved)
//! i64  committed_at — commit wall clock, unix millis, stamped by the capture path
//!      (identical across a run, like today's column); 0 = unknown (empty frames)
//! [flag bit 0]  u8 run_id byte-length, then that many UTF-8 bytes
//! [flag bit 1]  u64 run_rows, u64 rows_cum — precomputed in the capture path (the
//!      in-txn `stamp_run_totals` UPDATE is impossible once the payload is fixed at
//!      `leader_commit` time, 211 §4.1)
//! …    payload — version-dependent:
//!        v1: ONE opaque legacy blob (in practice a UTF-8 JSON `RowChange[]` array)
//!        v2: the versioned ordered-chunk container below (absent for Empty frames)
//! ```
//!
//! The version byte is **per frame**, not per container: repack re-encodes old frames
//! to the current version (212 §2.2's aging-out policy), and a per-frame stamp is what
//! lets one segment legally carry mixed vintages in between.

/// The LEGACY envelope version: the payload is one opaque blob (in practice a UTF-8 JSON
/// array). Current producers never write it; decode accepts it forever — the archival
/// compatibility policy is "apply engine reads all versions ≥ N" (212 §2.2), so a
/// reader older than a frame must error, never guess. (This policy is exactly why the
/// chunk container gets its own frame version: embedded under v1, a pre-container
/// reader would misreport it as JSON corruption instead of a clean version skew.)
pub const FRAME_FORMAT_V1: u8 = 1;

/// The CURRENT envelope version: the payload IS the versioned ordered-chunk container
/// ([`encode_payload_chunks`]) — mandatory, so a corrupted container fails closed as a
/// container decode error instead of being misread as a legacy payload — or absent for
/// [`FrameKind::Empty`] frames. Written by [`FrameHeader::encode_frame`].
pub const FRAME_FORMAT_V2: u8 = 2;

/// The non-UTF-8 sentinel reserving the versioned payload-container namespace. Legacy journal
/// payloads are UTF-8 JSON arrays, so they can never collide with this prefix.
const PAYLOAD_CONTAINER_SENTINEL: u8 = 0x89;
const PAYLOAD_CONTAINER_MAGIC: &[u8; 4] = b"RJP\0";
const PAYLOAD_CONTAINER_V1: u8 = 1;
const PAYLOAD_CONTAINER_HEADER_LEN: usize = 1 + PAYLOAD_CONTAINER_MAGIC.len() + 1 + 4;

/// Ceiling on the chunks in one container, enforced on BOTH sides. Real producers emit at
/// most a handful (a 64 MiB transaction over an 8 MiB chunk bound), never a fraction of this;
/// the cap exists so a corrupted or hostile chunk count — which decode otherwise has to trust
/// for a `Vec` reservation of one fat pointer per counted chunk — cannot amplify one frame
/// into an unbounded allocation. 65,536 chunks is ≈ a 1 MiB pointer table at most.
pub const MAX_CONTAINER_CHUNKS: usize = 65_536;

/// Encode ordered opaque chunks into one HCTree journal payload without flattening their
/// boundaries. The enclosing [`FrameHeader`] remains the atomic transaction/cid; this inner
/// container only restores the old `(offset, chunk_seq, last)` replay grain.
///
/// Wire layout (container v1, little-endian):
///
/// ```text
/// u8   0x89 (not valid at the start of a UTF-8 JSON payload)
/// u8[4] "RJP\\0"
/// u8   container version (= 1)
/// u32  chunk count (non-zero, at most [`MAX_CONTAINER_CHUNKS`])
/// repeated chunk count times:
///   u32 chunk byte length (non-zero)
///   u8[] chunk bytes
/// ```
///
/// Chunk contents are deliberately opaque here. Rindle currently puts compact JSON
/// `RowChange[]` arrays in them because that is the follower wire codec, but HCTree itself does
/// not require JSON. A zero-length chunk is malformed on both sides: no producer emits one, and
/// admitting them at decode would let a corrupt count region masquerade as millions of chunks.
pub fn encode_payload_chunks(chunks: &[&[u8]]) -> Result<Vec<u8>, PayloadEncodeError> {
    let mut out = Vec::with_capacity(payload_container_len(chunks)?);
    encode_payload_chunks_into(chunks, &mut out).map(|()| out)
}

/// The exact encoded length of the container for `chunks` (validating the same bounds as the
/// encoder, so an `encode_*` caller can size one buffer exactly).
fn payload_container_len(chunks: &[&[u8]]) -> Result<usize, PayloadEncodeError> {
    if chunks.is_empty() {
        return Err(PayloadEncodeError::Empty);
    }
    if chunks.len() > MAX_CONTAINER_CHUNKS {
        return Err(PayloadEncodeError::TooManyChunks(chunks.len()));
    }
    let mut len = PAYLOAD_CONTAINER_HEADER_LEN;
    for chunk in chunks {
        if chunk.is_empty() {
            return Err(PayloadEncodeError::EmptyChunk);
        }
        u32::try_from(chunk.len()).map_err(|_| PayloadEncodeError::ChunkTooLarge(chunk.len()))?;
        len = len
            .checked_add(4)
            .and_then(|n| n.checked_add(chunk.len()))
            .ok_or(PayloadEncodeError::ContainerTooLarge)?;
    }
    Ok(len)
}

/// Append the validated container to `out` (callers size `out` via [`payload_container_len`]).
fn encode_payload_chunks_into(
    chunks: &[&[u8]],
    out: &mut Vec<u8>,
) -> Result<(), PayloadEncodeError> {
    payload_container_len(chunks)?;
    out.push(PAYLOAD_CONTAINER_SENTINEL);
    out.extend_from_slice(PAYLOAD_CONTAINER_MAGIC);
    out.push(PAYLOAD_CONTAINER_V1);
    out.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
    for chunk in chunks {
        out.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
        out.extend_from_slice(chunk);
    }
    Ok(())
}

/// Decode a journal payload into its ordered chunks. The container is REQUIRED here: a payload
/// without the reserved sentinel is a decode error, never a legacy fallback — sniffing would
/// silently misread a container whose first byte was corrupted as one giant opaque chunk.
/// Legacy (pre-container) payloads are a property of the FRAME version, handled by
/// [`FrameHeader::decode_chunks`], not of the bytes.
pub fn decode_payload_chunks(payload: &[u8]) -> Result<Vec<&[u8]>, PayloadDecodeError> {
    if payload.first().copied() != Some(PAYLOAD_CONTAINER_SENTINEL) {
        return Err(PayloadDecodeError::MissingSentinel);
    }
    if payload.len() < PAYLOAD_CONTAINER_HEADER_LEN {
        return Err(PayloadDecodeError::Truncated);
    }
    if &payload[1..1 + PAYLOAD_CONTAINER_MAGIC.len()] != PAYLOAD_CONTAINER_MAGIC {
        return Err(PayloadDecodeError::InvalidMagic);
    }
    let version_at = 1 + PAYLOAD_CONTAINER_MAGIC.len();
    let version = payload[version_at];
    if version != PAYLOAD_CONTAINER_V1 {
        return Err(PayloadDecodeError::UnknownVersion(version));
    }
    let count_at = version_at + 1;
    let count = u32::from_le_bytes(
        payload[count_at..count_at + 4]
            .try_into()
            .expect("four-byte split"),
    ) as usize;
    if count == 0 {
        return Err(PayloadDecodeError::Empty);
    }
    if count > MAX_CONTAINER_CHUNKS {
        return Err(PayloadDecodeError::TooManyChunks(count));
    }

    let mut rest = &payload[PAYLOAD_CONTAINER_HEADER_LEN..];
    // Every chunk needs at least its four-byte length prefix. Check this before reserving from the
    // untrusted count so a tiny corrupt payload cannot request a multi-gigabyte allocation.
    if count > rest.len() / 4 {
        return Err(PayloadDecodeError::Truncated);
    }
    let mut chunks = Vec::with_capacity(count);
    for _ in 0..count {
        if rest.len() < 4 {
            return Err(PayloadDecodeError::Truncated);
        }
        let len = u32::from_le_bytes(rest[..4].try_into().expect("four-byte split")) as usize;
        if len == 0 {
            return Err(PayloadDecodeError::EmptyChunk);
        }
        rest = &rest[4..];
        if rest.len() < len {
            return Err(PayloadDecodeError::Truncated);
        }
        let (chunk, tail) = rest.split_at(len);
        chunks.push(chunk);
        rest = tail;
    }
    if !rest.is_empty() {
        return Err(PayloadDecodeError::TrailingBytes(rest.len()));
    }
    Ok(chunks)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PayloadEncodeError {
    Empty,
    TooManyChunks(usize),
    ChunkTooLarge(usize),
    /// A zero-length chunk — no producer emits one (see [`encode_payload_chunks`]).
    EmptyChunk,
    ContainerTooLarge,
}

impl std::fmt::Display for PayloadEncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PayloadEncodeError::Empty => {
                write!(f, "a chunk container must contain at least one chunk")
            }
            PayloadEncodeError::TooManyChunks(n) => {
                write!(
                    f,
                    "chunk count {n} exceeds the {MAX_CONTAINER_CHUNKS}-chunk container limit"
                )
            }
            PayloadEncodeError::ChunkTooLarge(n) => {
                write!(f, "chunk length {n} exceeds the u32 container limit")
            }
            PayloadEncodeError::EmptyChunk => {
                write!(f, "a chunk container cannot hold a zero-length chunk")
            }
            PayloadEncodeError::ContainerTooLarge => {
                write!(f, "chunk container length overflows addressable memory")
            }
        }
    }
}

impl std::error::Error for PayloadEncodeError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PayloadDecodeError {
    /// The payload does not start with the container sentinel — not a container at all.
    MissingSentinel,
    Truncated,
    InvalidMagic,
    UnknownVersion(u8),
    Empty,
    /// The declared chunk count exceeds [`MAX_CONTAINER_CHUNKS`] — corrupt or hostile.
    TooManyChunks(usize),
    /// A declared zero-length chunk (see [`encode_payload_chunks`]).
    EmptyChunk,
    TrailingBytes(usize),
}

impl std::fmt::Display for PayloadDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PayloadDecodeError::MissingSentinel => {
                write!(f, "payload is not a chunk container (sentinel byte absent)")
            }
            PayloadDecodeError::Truncated => write!(f, "payload chunk container is truncated"),
            PayloadDecodeError::InvalidMagic => {
                write!(f, "payload chunk container has invalid magic")
            }
            PayloadDecodeError::UnknownVersion(v) => {
                write!(f, "unknown payload chunk container version {v}")
            }
            PayloadDecodeError::Empty => write!(f, "payload chunk container has no chunks"),
            PayloadDecodeError::TooManyChunks(n) => {
                write!(
                    f,
                    "payload chunk container declares {n} chunks, over the \
                     {MAX_CONTAINER_CHUNKS}-chunk limit"
                )
            }
            PayloadDecodeError::EmptyChunk => {
                write!(f, "payload chunk container declares a zero-length chunk")
            }
            PayloadDecodeError::TrailingBytes(n) => {
                write!(f, "payload chunk container has {n} trailing byte(s)")
            }
        }
    }
}

impl std::error::Error for PayloadDecodeError {}

/// The entry kind — today's `_rindle_change_log.kind` discriminator plus the shapes
/// the journal substrate adds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameKind {
    /// Row deltas: the payload contains ordered flat-change chunks.
    Rows = 0,
    /// A schema change riding the log in-band (`RELAY-DDL-DESIGN.md`): replay mutates
    /// the schema at exactly this offset. DDL entries are fold barriers (212 §4.2) and
    /// the reason no frame ever meets the wrong schema epoch (212 §3.1).
    Ddl = 1,
    /// An empty entry with **no payload**: the engine itself writes empty journal
    /// records on post-allocation validation failure, and zero-fills crash gaps at
    /// reopen (211 §4). They ship as empty frames so cid contiguity is preserved
    /// end-to-end (212 §2.1). `committed_at` is 0 (unknown) — the engine's records
    /// carry no wall clock.
    Empty = 2,
    // 3 is RESERVED for spill manifests (211 §6): a journal-side payload indirection
    // for giant transactions. Spill manifests are resolved — chunks inlined — at ship
    // time, so they never appear in segments (212 §2.2) and no cross-process reader
    // decodes them yet. Claiming the discriminant now keeps the space unambiguous.
}

/// The run accounting pair (`run_rows`, `rows_cum`) — today's stamped columns, moved
/// into the header and precomputed in the capture path (211 §4.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunTotals {
    /// Total change-row count of this entry's run.
    pub run_rows: u64,
    /// Cumulative change-row count at this entry's head (the master's running total).
    pub rows_cum: u64,
}

/// The decoded envelope header. See the module docs for the wire layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    pub kind: FrameKind,
    /// Commit wall clock, unix millis; 0 = unknown (engine-written [`FrameKind::Empty`]
    /// records). Timestamp→cid PITR resolution scans this field and must skip empties
    /// (212 §3). App-stamped pre-commit, so only *approximately* monotone across cids —
    /// resolvers tolerate local clock steps.
    pub committed_at: i64,
    /// The run identity the epoch fence checks (`subscribe_run_id_matches` becomes a
    /// payload decode under 211 §4.1).
    pub run_id: Option<String>,
    pub run_totals: Option<RunTotals>,
}

/// Encode-side misuse. Every variant is a producer bug, not a data-reachable state —
/// surfaced as errors (not asserts) because the encoder sits on the commit path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameEncodeError {
    /// `run_id` exceeds the u8 length prefix (255 bytes). Run ids are uuid-sized.
    RunIdTooLong(usize),
    /// [`FrameKind::Empty`] with a non-empty payload — an empty entry *means* "no
    /// payload existed" (engine-written record); bytes here would be silently lost
    /// semantics.
    EmptyFrameWithPayload,
    /// The v2 chunk container rejected the chunks (empty set, zero-length chunk, …).
    Payload(PayloadEncodeError),
}

impl std::fmt::Display for FrameEncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameEncodeError::RunIdTooLong(n) => {
                write!(f, "run_id is {n} bytes; the length prefix caps it at 255")
            }
            FrameEncodeError::EmptyFrameWithPayload => {
                write!(f, "an Empty frame must carry no payload")
            }
            FrameEncodeError::Payload(e) => write!(f, "invalid frame payload chunks: {e}"),
        }
    }
}

impl std::error::Error for FrameEncodeError {}

/// Decode-side failures. Fail-closed by construction: unknown versions, kinds, or flag
/// bits are errors, never best-effort skips — a frame will be replayed by a binary
/// years younger than the one that wrote it, and *that* direction must be exact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameDecodeError {
    /// The bytes end before the header (or a length-prefixed field) completes.
    Truncated,
    UnknownFormat(u8),
    UnknownKind(u8),
    /// A reserved flag bit is set — written by a newer producer; refuse rather than
    /// misparse the field section.
    UnknownFlags(u8),
    RunIdNotUtf8,
    /// An [`FrameKind::Empty`] frame carrying payload bytes (see
    /// [`FrameEncodeError::EmptyFrameWithPayload`]).
    EmptyFrameWithPayload,
    /// A v2 frame whose mandatory chunk container failed to decode.
    Payload(PayloadDecodeError),
}

impl std::fmt::Display for FrameDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameDecodeError::Truncated => write!(f, "frame truncated mid-header"),
            FrameDecodeError::UnknownFormat(v) => write!(f, "unknown frame format version {v}"),
            FrameDecodeError::UnknownKind(k) => write!(f, "unknown frame kind {k}"),
            FrameDecodeError::UnknownFlags(b) => write!(f, "unknown frame flag bits {b:#04x}"),
            FrameDecodeError::RunIdNotUtf8 => write!(f, "run_id is not valid UTF-8"),
            FrameDecodeError::EmptyFrameWithPayload => {
                write!(f, "an Empty frame must carry no payload")
            }
            FrameDecodeError::Payload(e) => write!(f, "invalid frame payload container: {e}"),
        }
    }
}

impl std::error::Error for FrameDecodeError {}

const FLAG_RUN_ID: u8 = 1 << 0;
const FLAG_RUN_TOTALS: u8 = 1 << 1;
const KNOWN_FLAGS: u8 = FLAG_RUN_ID | FLAG_RUN_TOTALS;

impl FrameHeader {
    /// The engine-written empty record (post-allocation validation failure, crash
    /// zero-fill) as a frame: kind [`FrameKind::Empty`], no wall clock, no run.
    pub fn empty() -> Self {
        FrameHeader {
            kind: FrameKind::Empty,
            committed_at: 0,
            run_id: None,
            run_totals: None,
        }
    }

    /// The encoded header length (fields only, no payload) — sizes single-allocation buffers.
    fn header_len(&self) -> usize {
        11 + self.run_id.as_ref().map_or(0, |id| 1 + id.len())
            + if self.run_totals.is_some() { 16 } else { 0 }
    }

    /// Append the header fields under `version` to `out` (validating the u8 run_id prefix).
    fn encode_header_into(&self, version: u8, out: &mut Vec<u8>) -> Result<(), FrameEncodeError> {
        if let Some(id) = &self.run_id {
            if id.len() > u8::MAX as usize {
                return Err(FrameEncodeError::RunIdTooLong(id.len()));
            }
        }
        out.push(version);
        out.push(self.kind as u8);
        let mut flags = 0u8;
        if self.run_id.is_some() {
            flags |= FLAG_RUN_ID;
        }
        if self.run_totals.is_some() {
            flags |= FLAG_RUN_TOTALS;
        }
        out.push(flags);
        out.extend_from_slice(&self.committed_at.to_le_bytes());
        if let Some(id) = &self.run_id {
            out.push(id.len() as u8);
            out.extend_from_slice(id.as_bytes());
        }
        if let Some(t) = &self.run_totals {
            out.extend_from_slice(&t.run_rows.to_le_bytes());
            out.extend_from_slice(&t.rows_cum.to_le_bytes());
        }
        Ok(())
    }

    /// Encode `header ‖ payload` as a LEGACY v1 frame — one opaque payload blob. Production
    /// paths write v2 via [`encode_frame`](Self::encode_frame); this stays for the
    /// decode-forever compatibility fixtures and for tests that treat payloads as opaque.
    pub fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, FrameEncodeError> {
        if self.kind == FrameKind::Empty && !payload.is_empty() {
            return Err(FrameEncodeError::EmptyFrameWithPayload);
        }
        let mut out = Vec::with_capacity(self.header_len() + payload.len());
        self.encode_header_into(FRAME_FORMAT_V1, &mut out)?;
        out.extend_from_slice(payload);
        Ok(out)
    }

    /// Encode `header ‖ chunk container` as one v2 frame in a SINGLE buffer — the bytes handed
    /// to `leader_commit` (and, framed under a cid, stored in segments). The container is built
    /// in place, so a maximum-size transaction costs one output allocation, not a container copy
    /// plus a frame copy. [`FrameKind::Empty`] takes NO chunks (an empty entry *means* "no
    /// payload existed"); every other kind takes at least one.
    pub fn encode_frame(&self, chunks: &[&[u8]]) -> Result<Vec<u8>, FrameEncodeError> {
        if self.kind == FrameKind::Empty {
            if !chunks.is_empty() {
                return Err(FrameEncodeError::EmptyFrameWithPayload);
            }
            let mut out = Vec::with_capacity(self.header_len());
            self.encode_header_into(FRAME_FORMAT_V2, &mut out)?;
            return Ok(out);
        }
        let container_len = payload_container_len(chunks).map_err(FrameEncodeError::Payload)?;
        let mut out = Vec::with_capacity(self.header_len() + container_len);
        self.encode_header_into(FRAME_FORMAT_V2, &mut out)?;
        encode_payload_chunks_into(chunks, &mut out).map_err(FrameEncodeError::Payload)?;
        Ok(out)
    }

    /// Decode a full frame into its header and ordered payload chunks — the ONE decode every
    /// chunk consumer shares. A v1 frame's opaque legacy payload is returned as a single chunk;
    /// a v2 frame's chunk container is mandatory (a payload without it is a decode error, never
    /// a legacy guess); an [`FrameKind::Empty`] frame decodes to no chunks.
    pub fn decode_chunks(bytes: &[u8]) -> Result<(FrameHeader, Vec<&[u8]>), FrameDecodeError> {
        let (&version, _) = bytes.split_first().ok_or(FrameDecodeError::Truncated)?;
        let (header, payload) = Self::decode(bytes)?;
        let chunks = if header.kind == FrameKind::Empty {
            Vec::new()
        } else if version == FRAME_FORMAT_V1 {
            vec![payload]
        } else {
            decode_payload_chunks(payload).map_err(FrameDecodeError::Payload)?
        };
        Ok((header, chunks))
    }

    /// Decode a `header ‖ payload` buffer (either frame version); returns the header and the
    /// RAW payload slice. The payload's shape depends on the frame version — chunk consumers
    /// use [`decode_chunks`](Self::decode_chunks); this is for header-only readers and code
    /// that forwards the payload opaquely.
    pub fn decode(bytes: &[u8]) -> Result<(FrameHeader, &[u8]), FrameDecodeError> {
        let (&version, rest) = bytes.split_first().ok_or(FrameDecodeError::Truncated)?;
        if version != FRAME_FORMAT_V1 && version != FRAME_FORMAT_V2 {
            return Err(FrameDecodeError::UnknownFormat(version));
        }
        let (&kind_byte, rest) = rest.split_first().ok_or(FrameDecodeError::Truncated)?;
        let kind = match kind_byte {
            0 => FrameKind::Rows,
            1 => FrameKind::Ddl,
            2 => FrameKind::Empty,
            other => return Err(FrameDecodeError::UnknownKind(other)),
        };
        let (&flags, rest) = rest.split_first().ok_or(FrameDecodeError::Truncated)?;
        if flags & !KNOWN_FLAGS != 0 {
            return Err(FrameDecodeError::UnknownFlags(flags & !KNOWN_FLAGS));
        }
        if rest.len() < 8 {
            return Err(FrameDecodeError::Truncated);
        }
        let (ts, mut rest) = rest.split_at(8);
        let committed_at = i64::from_le_bytes(ts.try_into().expect("8-byte split"));
        let run_id = if flags & FLAG_RUN_ID != 0 {
            let (&len, tail) = rest.split_first().ok_or(FrameDecodeError::Truncated)?;
            if tail.len() < len as usize {
                return Err(FrameDecodeError::Truncated);
            }
            let (id, tail) = tail.split_at(len as usize);
            rest = tail;
            Some(
                std::str::from_utf8(id)
                    .map_err(|_| FrameDecodeError::RunIdNotUtf8)?
                    .to_owned(),
            )
        } else {
            None
        };
        let run_totals = if flags & FLAG_RUN_TOTALS != 0 {
            if rest.len() < 16 {
                return Err(FrameDecodeError::Truncated);
            }
            let (raw, tail) = rest.split_at(16);
            rest = tail;
            Some(RunTotals {
                run_rows: u64::from_le_bytes(raw[..8].try_into().expect("8-byte split")),
                rows_cum: u64::from_le_bytes(raw[8..].try_into().expect("8-byte split")),
            })
        } else {
            None
        };
        if kind == FrameKind::Empty && !rest.is_empty() {
            return Err(FrameDecodeError::EmptyFrameWithPayload);
        }
        Ok((
            FrameHeader {
                kind,
                committed_at,
                run_id,
                run_totals,
            },
            rest,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_header() -> FrameHeader {
        FrameHeader {
            kind: FrameKind::Rows,
            committed_at: 1_752_000_000_123,
            run_id: Some("w:0000000000000abc".to_owned()),
            run_totals: Some(RunTotals {
                run_rows: 42,
                rows_cum: 9_000_000_001,
            }),
        }
    }

    #[test]
    fn round_trips_every_field_combination() {
        let payloads: &[&[u8]] = &[b"", b"[{\"op\":\"add\"}]"];
        for kind in [FrameKind::Rows, FrameKind::Ddl] {
            for run_id in [None, Some("r-1".to_owned())] {
                for run_totals in [
                    None,
                    Some(RunTotals {
                        run_rows: 7,
                        rows_cum: 100,
                    }),
                ] {
                    for payload in payloads {
                        let header = FrameHeader {
                            kind,
                            committed_at: -5, // negative stays intact (i64, pre-epoch clock)
                            run_id: run_id.clone(),
                            run_totals,
                        };
                        let bytes = header.encode(payload).unwrap();
                        let (back, tail) = FrameHeader::decode(&bytes).unwrap();
                        assert_eq!(back, header);
                        assert_eq!(tail, *payload);
                    }
                }
            }
        }
    }

    #[test]
    fn empty_frame_round_trips_and_rejects_payload() {
        let bytes = FrameHeader::empty().encode(b"").unwrap();
        let (back, tail) = FrameHeader::decode(&bytes).unwrap();
        assert_eq!(back, FrameHeader::empty());
        assert!(tail.is_empty());

        assert_eq!(
            FrameHeader::empty().encode(b"x"),
            Err(FrameEncodeError::EmptyFrameWithPayload)
        );
        // Decode side: an Empty header followed by bytes is a torn/forged frame.
        let mut forged = bytes;
        forged.push(0xff);
        assert_eq!(
            FrameHeader::decode(&forged),
            Err(FrameDecodeError::EmptyFrameWithPayload)
        );
    }

    #[test]
    fn truncation_at_every_boundary_is_an_error_not_a_panic() {
        let bytes = full_header().encode(b"payload").unwrap();
        let payload_start = bytes.len() - "payload".len();
        for cut in 0..payload_start {
            assert_eq!(
                FrameHeader::decode(&bytes[..cut]),
                Err(FrameDecodeError::Truncated),
                "cut at {cut}"
            );
        }
        // Any cut at or past the payload start decodes fine (payload is opaque bytes).
        assert!(FrameHeader::decode(&bytes[..payload_start]).is_ok());
    }

    #[test]
    fn unknown_version_kind_and_flags_fail_closed() {
        let bytes = full_header().encode(b"p").unwrap();

        let mut v = bytes.clone();
        v[0] = 3;
        assert_eq!(
            FrameHeader::decode(&v),
            Err(FrameDecodeError::UnknownFormat(3))
        );

        let mut k = bytes.clone();
        k[1] = 3; // the reserved spill-manifest discriminant: not decodable in v1
        assert_eq!(
            FrameHeader::decode(&k),
            Err(FrameDecodeError::UnknownKind(3))
        );

        let mut f = bytes;
        f[2] |= 1 << 5;
        assert_eq!(
            FrameHeader::decode(&f),
            Err(FrameDecodeError::UnknownFlags(1 << 5))
        );
    }

    #[test]
    fn run_id_limits() {
        let header = FrameHeader {
            run_id: Some("x".repeat(256)),
            ..full_header()
        };
        assert_eq!(header.encode(b""), Err(FrameEncodeError::RunIdTooLong(256)));

        let max = FrameHeader {
            run_id: Some("y".repeat(255)),
            ..full_header()
        };
        let bytes = max.encode(b"tail").unwrap();
        let (back, tail) = FrameHeader::decode(&bytes).unwrap();
        assert_eq!(back.run_id.as_deref().map(str::len), Some(255));
        assert_eq!(tail, b"tail");
    }

    #[test]
    fn non_utf8_run_id_is_rejected() {
        let mut bytes = FrameHeader {
            run_id: Some("ab".to_owned()),
            run_totals: None,
            ..full_header()
        }
        .encode(b"")
        .unwrap();
        // Corrupt the run_id bytes in place (they are the last two bytes).
        let n = bytes.len();
        bytes[n - 2] = 0xff;
        bytes[n - 1] = 0xfe;
        assert_eq!(
            FrameHeader::decode(&bytes),
            Err(FrameDecodeError::RunIdNotUtf8)
        );
    }

    #[test]
    fn payload_chunks_round_trip_without_flattening_boundaries() {
        let chunks: &[&[u8]] = &[b"[{\"n\":1}]", b"[]", b"[{\"n\":2},{\"n\":3}]"];
        let encoded = encode_payload_chunks(chunks).unwrap();
        assert_eq!(encoded[0], PAYLOAD_CONTAINER_SENTINEL);
        assert_eq!(decode_payload_chunks(&encoded).unwrap(), chunks);
    }

    #[test]
    fn v2_frames_round_trip_chunks_in_one_buffer() {
        let header = full_header();
        let chunks: &[&[u8]] = &[b"[{\"n\":1}]", b"[{\"n\":2},{\"n\":3}]"];
        let bytes = header.encode_frame(chunks).unwrap();
        assert_eq!(
            bytes[0], FRAME_FORMAT_V2,
            "container frames must be version-stamped so a pre-container reader errors with \
             UnknownFormat, never a JSON parse failure misread as corruption"
        );
        let (back, decoded) = FrameHeader::decode_chunks(&bytes).unwrap();
        assert_eq!(back, header);
        assert_eq!(decoded, chunks);

        // The v2 Empty frame: no chunks in, no chunks out, and no payload bytes on the wire.
        let empty = FrameHeader::empty().encode_frame(&[]).unwrap();
        assert_eq!(empty[0], FRAME_FORMAT_V2);
        let (back, decoded) = FrameHeader::decode_chunks(&empty).unwrap();
        assert_eq!(back, FrameHeader::empty());
        assert!(decoded.is_empty());
    }

    #[test]
    fn v1_legacy_frames_decode_as_one_chunk_forever() {
        // The archival policy: every shipped v1 frame (payload = one opaque JSON array) stays
        // replayable by all future readers.
        let legacy = b"[{\"op\":\"add\"}]";
        let bytes = full_header().encode(legacy).unwrap();
        assert_eq!(bytes[0], FRAME_FORMAT_V1);
        let (_, chunks) = FrameHeader::decode_chunks(&bytes).unwrap();
        assert_eq!(chunks, vec![legacy.as_slice()]);
    }

    #[test]
    fn encode_frame_rejects_producer_misuse() {
        assert_eq!(
            full_header().encode_frame(&[]),
            Err(FrameEncodeError::Payload(PayloadEncodeError::Empty))
        );
        assert_eq!(
            full_header().encode_frame(&[b"ok", b""]),
            Err(FrameEncodeError::Payload(PayloadEncodeError::EmptyChunk))
        );
        assert_eq!(
            FrameHeader::empty().encode_frame(&[b"x"]),
            Err(FrameEncodeError::EmptyFrameWithPayload)
        );
    }

    #[test]
    fn v2_frame_with_a_non_container_payload_fails_closed() {
        // A corrupted sentinel must NOT demote the payload to "legacy" — that guess would hand
        // downstream JSON parsers a binary container body and misreport corruption.
        let mut bytes = full_header().encode_frame(&[b"[{\"n\":1}]"]).unwrap();
        let payload_at = bytes.len() - (PAYLOAD_CONTAINER_HEADER_LEN + 4 + 9);
        assert_eq!(bytes[payload_at], PAYLOAD_CONTAINER_SENTINEL);
        bytes[payload_at] ^= 1;
        assert_eq!(
            FrameHeader::decode_chunks(&bytes),
            Err(FrameDecodeError::Payload(
                PayloadDecodeError::MissingSentinel
            ))
        );
    }

    #[test]
    fn hostile_chunk_counts_cannot_amplify_allocation() {
        // A container body of nothing but zero-length chunk prefixes passes a length-only
        // plausibility check while declaring payload_len/4 chunks — one fat pointer each. The
        // zero-length rejection kills the amplification at the first prefix.
        let mut crafted = Vec::new();
        crafted.push(PAYLOAD_CONTAINER_SENTINEL);
        crafted.extend_from_slice(PAYLOAD_CONTAINER_MAGIC);
        crafted.push(PAYLOAD_CONTAINER_V1);
        crafted.extend_from_slice(&1024u32.to_le_bytes());
        crafted.extend_from_slice(&vec![0u8; 4 * 1024]);
        assert_eq!(
            decode_payload_chunks(&crafted),
            Err(PayloadDecodeError::EmptyChunk)
        );

        // And a count over the absolute cap is rejected before any reservation at all.
        let mut over_cap = Vec::new();
        over_cap.push(PAYLOAD_CONTAINER_SENTINEL);
        over_cap.extend_from_slice(PAYLOAD_CONTAINER_MAGIC);
        over_cap.push(PAYLOAD_CONTAINER_V1);
        over_cap.extend_from_slice(&((MAX_CONTAINER_CHUNKS as u32) + 1).to_le_bytes());
        over_cap.extend_from_slice(&vec![1u8; 5 * (MAX_CONTAINER_CHUNKS + 1)]);
        assert_eq!(
            decode_payload_chunks(&over_cap),
            Err(PayloadDecodeError::TooManyChunks(MAX_CONTAINER_CHUNKS + 1))
        );
    }

    #[test]
    fn malformed_payload_containers_fail_closed() {
        assert_eq!(encode_payload_chunks(&[]), Err(PayloadEncodeError::Empty));
        assert_eq!(
            encode_payload_chunks(&[b"one", b""]),
            Err(PayloadEncodeError::EmptyChunk)
        );

        assert_eq!(
            decode_payload_chunks(b"[{\"op\":\"add\"}]"),
            Err(PayloadDecodeError::MissingSentinel),
            "a non-container payload is an error here — legacy handling is the FRAME version's \
             job (decode_chunks), never a byte-sniffing fallback"
        );

        let valid = encode_payload_chunks(&[b"one", b"two"]).unwrap();
        for cut in 1..valid.len() {
            let decoded = decode_payload_chunks(&valid[..cut]);
            assert!(decoded.is_err(), "cut at {cut} unexpectedly decoded");
        }

        let mut bad_magic = valid.clone();
        bad_magic[1] ^= 1;
        assert_eq!(
            decode_payload_chunks(&bad_magic),
            Err(PayloadDecodeError::InvalidMagic)
        );

        let mut future = valid.clone();
        future[1 + PAYLOAD_CONTAINER_MAGIC.len()] = 2;
        assert_eq!(
            decode_payload_chunks(&future),
            Err(PayloadDecodeError::UnknownVersion(2))
        );

        let mut empty = valid.clone();
        let count_at = 1 + PAYLOAD_CONTAINER_MAGIC.len() + 1;
        empty[count_at..count_at + 4].copy_from_slice(&0u32.to_le_bytes());
        assert_eq!(
            decode_payload_chunks(&empty),
            Err(PayloadDecodeError::Empty)
        );

        let mut impossible_count = valid.clone();
        impossible_count[count_at..count_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            decode_payload_chunks(&impossible_count),
            Err(PayloadDecodeError::TooManyChunks(u32::MAX as usize))
        );

        let mut trailing = valid;
        trailing.push(0);
        assert_eq!(
            decode_payload_chunks(&trailing),
            Err(PayloadDecodeError::TrailingBytes(1))
        );
    }
}
