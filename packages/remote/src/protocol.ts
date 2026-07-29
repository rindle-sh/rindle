// The flat-change subscription protocol, ported from `src/flat_protocol.rs` + the
// `schema_fp` fingerprint from `src/wire_schema.rs`. This is the wire contract shared by the
// server (`@rindle/server`, which frames via {@link Publisher}) and the client RemoteBackend
// (which validates via {@link Subscriber}).
//
// The §2.2 split (WASM-CLIENT-DESIGN.md): the Rust `Subscriber` owns a `Receiver` and does
// validation *and* folding. Here we keep ONLY validation/sequencing in {@link Subscriber} and
// hand the clean `hello`/`snapshot`/`batch` `ChangeEvent`s up to the core `ArrayView` to fold —
// so one `ArrayView` serves both the local and remote backends.

import { COMPARATOR_VERSION } from "@rindle/client";
import type { ChangeEvent, FlatChange, Mutation, MutationEnvelope, ProgressFrame, WireSchema } from "@rindle/client";
import type { NormalizedBatch, NormalizedHello } from "./normalized.ts";

export { COMPARATOR_VERSION };

// ----------------------------- schema fingerprint (FNV-1a 64) -----------------------------

const enc = new TextEncoder();
const FNV_OFFSET = 0xcbf29ce484222325n;
const FNV_PRIME = 0x00000100000001b3n;
const MASK = 0xffffffffffffffffn;

export class Fnv {
  h = FNV_OFFSET;
  private byte(b: number): void {
    this.h = ((this.h ^ BigInt(b & 0xff)) * FNV_PRIME) & MASK;
  }
  u8(v: number): void {
    this.byte(v);
  }
  /** A `u32` little-endian (matching Rust `v.to_le_bytes()`). */
  u32(v: number): void {
    this.byte(v & 0xff);
    this.byte((v >>> 8) & 0xff);
    this.byte((v >>> 16) & 0xff);
    this.byte((v >>> 24) & 0xff);
  }
  /** A length-prefixed string: `u32(byteLen)` then the UTF-8 bytes (disambiguates joins). */
  s(str: string): void {
    const bytes = enc.encode(str);
    this.u32(bytes.length);
    for (const b of bytes) this.byte(b);
  }
}

function hashLevel(f: Fnv, ws: WireSchema): void {
  f.u8(0x53); // 'S'
  f.u32(ws.columns.length);
  for (const c of ws.columns) f.s(c);
  // PK + sort resolved to column NAMES (semantic identity, independent of indices).
  f.u32(ws.primaryKey.length);
  for (const pk of ws.primaryKey) f.s(ws.columns[pk]);
  f.u32(ws.sort.length);
  for (const [c, asc] of ws.sort) {
    f.s(ws.columns[c]);
    f.u8(asc ? 1 : 0);
  }
  f.u8(ws.singular ? 1 : 0);
  f.u32(ws.relationships.length);
  for (const r of ws.relationships) {
    f.s(r.name);
    if (r.child) {
      f.u8(1);
      hashLevel(f, r.child);
      // Scalar-projection marker (REDUCE-DESIGN.md §9): a presence byte, then — when present
      // — the projected column's NAME in the child schema (index-independent, matching
      // PK/sort). The empty-identity value is NOT hashed (a bare cell can't reproduce its
      // type variant). Must match `hash_level` in `src/wire_schema.rs`.
      if (r.project) {
        f.u8(1);
        f.s(r.child.columns[r.project.col]);
      } else {
        f.u8(0);
      }
    } else {
      f.u8(0);
    }
  }
}

/** A `WireSchema`'s content fingerprint — FNV-1a 64 over the canonical, length-prefixed byte
 *  stream of `src/wire_schema.rs`, rendered as 16-char lowercase hex (=== Rust `SchemaFp`
 *  `Display`). A string (not a JS number) so the full 64 bits survive JSON without precision loss. */
export function schemaFp(ws: WireSchema): string {
  const f = new Fnv();
  hashLevel(f, ws);
  return f.h.toString(16).padStart(16, "0");
}

// ----------------------------- the wire frames -----------------------------

/** The subscription handshake, sent once before any {@link Batch}. */
export interface Hello {
  epoch: number;
  comparatorVersion: number;
  schema: WireSchema;
  schemaFp: string;
}

/** One transaction's flat changes (or the seq-0 hydrate snapshot). `events` apply in order. */
export interface Batch {
  epoch: number;
  seq: number;
  schemaFp: string;
  events: FlatChange[];
}

/** The multiplexed wire messages (many queries over one connection, tagged by `queryId`).
 *  `subscribe.mode` selects the serializer (default flat); a normalized subscription gets
 *  `nhello`/`nbatch` back instead of `hello`/`batch` (NORMALIZED-CHANGES-DESIGN.md §6).
 *  Embedded servers receive `{name,args}`. Daemon/serverless deployments can instead receive an
 *  opaque `leaseToken` that the app API server minted after auth + named-query resolution.
 *
 *  The OPTIMISTIC path (OPTIMISTIC-WRITES-DESIGN.md §8) adds `init` (the connection
 *  identifies its stable clientID, so progress frames can carry that client's `lmid`) and
 *  `pushMutation` (one named-mutator envelope up); the server answers with the normalized
 *  frames (`cv`-stamped) plus connection-level `progress` frames. */
export type SubscribeClientMsg =
  | { t: "subscribe"; queryId: number; name: string; args: unknown; mode?: "flat" | "normalized" }
  | { t: "subscribe"; queryId: number; leaseToken: string; mode?: "flat" | "normalized" };

export type ClientMsg =
  | { t: "init"; clientID: string }
  | SubscribeClientMsg
  | { t: "unsubscribe"; queryId: number }
  | { t: "mutate"; mutations: Mutation[] }
  | { t: "pushMutation"; envelope: MutationEnvelope };

export type ServerMsg =
  | { t: "hello"; queryId: number; hello: Hello }
  | { t: "batch"; queryId: number; batch: Batch }
  // `bootId` is the daemon's per-process id (same on every frame of one connection). A change
  // across (re)connections means the daemon restarted — it keeps no durable lease/materialization
  // or `cv` state — so the client must force a clean re-hydrate (reset its `cv` watermark).
  | { t: "nhello"; queryId: number; hello: NormalizedHello; bootId?: string }
  | { t: "nbatch"; queryId: number; batch: NormalizedBatch }
  // `code`/`retryable`/`retryAfterMs` classify the error per 101-QUERY-ERRORS §5 (absent from
  // older servers ⇒ terminal): a retryable error (`code:"shed"` from a load-shedding follower,
  // `code:"faulted"` from a worker fault) schedules a backed-off re-subscribe instead of
  // stranding the subscription (FOLLOWER-LAG-SHED §6.3).
  | { t: "queryError"; queryId: number; message: string; code?: string; retryable?: boolean; retryAfterMs?: number }
  | { t: "progress"; frame: ProgressFrame }
  // The follower-affinity ticket (FOLLOWER-AFFINITY-DESIGN.md §3), minted by the fleet follower as
  // the FIRST frame after the ws opens (`rust/rindle-server/src/net.rs` `serve_ws_conn`). Opaque +
  // connection-level (no `queryId`, no `cv`): the client persists it and offers it as a subprotocol
  // on the next connect + forwards it on the lease POST so both legs pin the same follower. Absent
  // on a single/affinity-off daemon (never sent) — old clients drop the unknown `t`.
  | { t: "affinity"; ticket: string }
  // The room deopt handshake's verdict frame (RINDLE-REALTIME-QUERY-ENABLEMENT §3.3, H-iv-b):
  // sent on the author's socket for every NON-applied mutation, BEFORE the lmid ack that burns
  // the mid; `name`/`args` ride `kind:"deopt"` only (self-contained re-invoke). Connection-level
  // and cv-less — the client dispatches it OUT-OF-BAND, never behind the cv buffer. Additive:
  // old clients drop unknown `t`.
  | { t: "mutationOutcome"; mid: number; kind: "deopt" | "rejected"; reason?: string; name?: string; args?: unknown }
  // A per-connection error reply: the server could not process this client's message (bad
  // table/AST, a commit/derive failure). Sent INSTEAD of crashing the process — the connection
  // stays open and every other connection is unaffected. Existing clients ignore unknown `t`.
  | { t: "error"; queryId?: number; message: string };

// ----------------------------- Publisher (server side) -----------------------------

/** Sender side: stamps batches with the subscription `epoch` + `schemaFp` and drives the
 *  gap-free seq. The hydrate snapshot is seq 0; increments are seq 1, 2, …. An empty
 *  transaction emits no batch and consumes no seq (so a gap always means a lost batch). */
export class Publisher {
  readonly epoch: number;
  readonly schema: WireSchema;
  readonly schemaFp: string;
  private nextSeq = 0;

  constructor(epoch: number, schema: WireSchema) {
    this.epoch = epoch;
    this.schema = schema;
    this.schemaFp = schemaFp(schema);
  }

  hello(): Hello {
    return { epoch: this.epoch, comparatorVersion: COMPARATOR_VERSION, schema: this.schema, schemaFp: this.schemaFp };
  }

  /** The hydrate snapshot (seq 0). Always emitted, even when empty, so the receiver learns it. */
  snapshot(events: FlatChange[]): Batch {
    return this.emit(events);
  }

  /** One transaction's events — `null` for an empty transaction (no batch, no seq consumed). */
  commit(events: FlatChange[]): Batch | null {
    return events.length ? this.emit(events) : null;
  }

  private emit(events: FlatChange[]): Batch {
    const seq = this.nextSeq++;
    return { epoch: this.epoch, seq, schemaFp: this.schemaFp, events };
  }
}

// ----------------------------- Subscriber (client side) -----------------------------

export type ProtocolErrorKind = "comparator-mismatch" | "epoch-mismatch" | "schema-mismatch" | "gap";

/** A protocol violation. All but a duplicate (handled silently) are unrecoverable for the
 *  current subscription — the RemoteBackend re-hydrates under a new epoch. */
export class ProtocolError extends Error {
  readonly kind: ProtocolErrorKind;
  constructor(kind: ProtocolErrorKind, message: string) {
    super(message);
    this.kind = kind;
    this.name = "ProtocolError";
  }
}

/** Receiver side: validates a frame stream (comparator at `hello`; per batch — epoch match,
 *  schema-fp match, strict in-order seq) and emits the clean `hello`/`snapshot`/`batch`
 *  `ChangeEvent`s. It does NOT fold (the core `ArrayView` does) — the §2.2 split. */
export class Subscriber {
  readonly epoch: number;
  readonly schemaFp: string;
  private readonly emit: (ev: ChangeEvent) => void;
  private phase: "snapshot" | "live" = "snapshot";
  private lastSeq = 0;

  constructor(hello: Hello, emit: (ev: ChangeEvent) => void) {
    this.emit = emit;
    if (hello.comparatorVersion !== COMPARATOR_VERSION) {
      throw new ProtocolError(
        "comparator-mismatch",
        `comparator version ${hello.comparatorVersion} != ${COMPARATOR_VERSION}`,
      );
    }
    const computed = schemaFp(hello.schema);
    if (computed !== hello.schemaFp) {
      throw new ProtocolError("schema-mismatch", `advertised fp ${hello.schemaFp} != computed ${computed}`);
    }
    this.epoch = hello.epoch;
    this.schemaFp = hello.schemaFp;
    emit({ type: "hello", schema: hello.schema, comparatorVersion: hello.comparatorVersion });
  }

  /** Apply one incremental batch (or the seq-0 snapshot). Returns `"duplicate"` for an
   *  already-applied seq (discarded — rc ops are not idempotent); throws {@link ProtocolError}
   *  on a gap / epoch / schema mismatch (the caller re-hydrates). */
  apply(batch: Batch): "applied" | "duplicate" {
    if (batch.epoch !== this.epoch) {
      throw new ProtocolError("epoch-mismatch", `expected epoch ${this.epoch}, got ${batch.epoch}`);
    }
    if (batch.schemaFp !== this.schemaFp) {
      throw new ProtocolError("schema-mismatch", `expected fp ${this.schemaFp}, got ${batch.schemaFp}`);
    }
    if (this.phase === "snapshot") {
      if (batch.seq === 0) {
        this.phase = "live";
        this.lastSeq = 0;
        this.emit({ type: "snapshot", adds: batch.events, last: true });
        return "applied";
      }
      throw new ProtocolError("gap", `expected the seq-0 snapshot, got seq ${batch.seq}`);
    }
    const expected = this.lastSeq + 1;
    if (batch.seq < expected) return "duplicate";
    if (batch.seq > expected) {
      throw new ProtocolError("gap", `expected seq ${expected}, got ${batch.seq}`);
    }
    this.lastSeq = batch.seq;
    this.emit({ type: "batch", events: batch.events });
    return "applied";
  }
}
