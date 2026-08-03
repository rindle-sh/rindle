// LM stream checkpointing — the two-plane response path (designs-implemented/LM-STREAM-CHECKPOINT-DESIGN.md).
//
// A model response arrives as hundreds of tiny deltas per second. Every one wants to be on a screen
// immediately; none wants to be a durable write. So the response runs on TWO planes sharing ONE
// monotone coordinate (`seq`, in UTF-16 code units of the response text):
//
//   - the LIVE plane (this module): process-local, volatile, every delta, straight to subscribers;
//   - the DURABLE plane: the app's own tables, advanced at coarse boundaries — N chars, T ms, an
//     explicit `flush()`, and always at close.
//
// The contract both planes stand on (§0):
//
//   S (splice)  durableText(f) ++ concat(frames delivered from f) == producedText, for every join
//               offset f and every join time.
//   P (prefix)  durableText is always a PREFIX of producedText, and durableSeq is monotone.
//
// P is what makes S mechanical — two prefixes of one string merge by taking the longer (see
// {@link spliceStreamText}). Everything else here is bookkeeping in service of those two.
//
// TWO THINGS THIS DELIBERATELY IS NOT (both were wrong in the first cut of this module):
//
//   1. Checkpoints are SYSTEM WRITES, not mutation envelopes. `lmid` is client-authored and exists
//      to release a client's optimistic rebase point; a server-authored checkpoint has no
//      prediction to release, and CDC → IVM fanout needs no envelope at all. Checkpoints therefore
//      go through `outsideSql` with no clientID/mid — the same rule the realtime lifecycle writes
//      follow ("a system write must never advance an lmid").
//   2. A checkpoint INSERTS A CHUNK ROW; it never appends to a growing column. A normalized `edit`
//      carries BOTH the old and the new full row, so advancing a `body` column would re-stream the
//      whole message twice per checkpoint — O(n²) wire to deliver n characters. One row per chunk
//      makes each checkpoint a single `add` carrying only its own slice.
//
// Durability is never overclaimed (§0, the RINDLE-REALTIME §8.1 "ack honesty" rule): a `chunk` frame
// says only "produced", a `durable` frame says "committed", an `end` frame says "sealed". Losing
// this plane entirely costs latency and the un-checkpointed tail — never consistency: a client with
// no live stream still sees the message grow through IVM at checkpoint granularity, which is why
// `absent`/`stale` are ordinary answers rather than errors.

// Type-only (erased at runtime) — no import cycle with ./index.ts.
import { STREAM_STATUS_STREAMING, frameResumePoint } from "@rindle/client";
import type { StreamFrame, StreamStatus } from "@rindle/client";
import type { SqlStatement } from "@rindle/daemon-client";

import type { Authorizer, ServerSql, SqlDialect } from "./index.ts";

// ------------------------------------------------------------------------------- the wire
//
// The frame shapes and the two pure reassembly functions live in `@rindle/client` — BOTH tiers need
// them, and a browser must never pull this package to read a stream. Re-exported here so the
// server-side import path is unchanged.

export {
  STREAM_STATUS_STREAMING,
  assembleDurableText,
  frameResumePoint,
  spliceStreamText,
} from "@rindle/client";
export type { StreamFrame, StreamStatus } from "@rindle/client";

// ------------------------------------------------------------------------------- the durable seam

/** What a checkpoint hands the durable plane when the app supplies its own {@link StreamCommit}. */
export type StreamCommitInput =
  /** The pointer is marked live. The app's own mutator created the row (it owns `chatId`, `role`,
   *  the model name…); this only flips it to `streaming`. */
  | { kind: "open"; streamId: string; meta: unknown; hostId?: string; startedAt: number }
  /** A prefix advance carrying ONLY its own slice. `text.length === seq - from`, and `from` is the
   *  last append the PLANE saw confirmed — so appends are contiguous in the fault-free run, but an
   *  append that committed while its ack was lost makes the next one RE-COVER (its `from` lags what
   *  the app already applied). See {@link StreamCommit} for the two ways to stay idempotent. */
  | { kind: "append"; streamId: string; from: number; seq: number; text: string }
  /** The seal. `body` is the producer's retained text and `bodyFrom` its absolute start offset:
   *  `bodyFrom === 0` — and `body` is the WHOLE response — unless the app opted into trimming via
   *  `retainChars` (`commit` mode only). A compacting app requires `bodyFrom === 0` and writes
   *  `body` wholesale; a non-compacting app appends the outstanding tail —
   *  `body.slice(from - bodyFrom)` — as a final chunk. `seq === bodyFrom + body.length`, always. */
  | { kind: "close"; streamId: string; from: number; seq: number; body: string; bodyFrom: number; status: StreamStatus; error?: string };

/** The escape hatch: persist the checkpoint however the app likes. Retried on throw (§3.3), so it
 *  must be idempotent under a repeated `(from, seq)` — AND under a RE-COVER: an append whose write
 *  committed but whose ack was lost leaves the plane believing less than the store holds, so the
 *  next append's `(from, seq)` can OVERLAP text already applied. Apply only the unseen suffix
 *  (`text.slice(applied - from)` when `applied > from`), or better, return `{seq: applied}` — the
 *  authoritative applied length — and the plane resynchronizes instead of re-covering at all.
 *  Return `{cancelRequested: true}` to tell the producer the reader asked it to stop (§6). */
export type StreamCommit = (
  input: StreamCommitInput,
) => Promise<void | { cancelRequested?: boolean; seq?: number }>;

/** Which columns of the app's own tables the plane reads and writes. Every entry has a default
 *  except `cancel`, `error`, and `host`, which are opt-in BY NAMING: the plane never emits SQL
 *  against a column the app did not ask it to use. */
export interface StreamColumns {
  /** The message row's primary key, matched against `streamId`. Default `id`. */
  key?: string;
  /** The compacted response text. Default `body`. */
  body?: string;
  /** {@link STREAM_STATUS_STREAMING} then a {@link StreamStatus}. Default `status`. */
  status?: string;
  /** Total durable length — `length(body) + Σ chunk lengths`. The CAS column (§3.2). Default `seq`. */
  seq?: string;
  /** Opt-in (§6): a truthy value here stops the generation at the next checkpoint. No default —
   *  naming it is what turns cancellation on. */
  cancel?: string;
  /** Opt-in: where a failed generation's message is recorded. No default. */
  error?: string;
  /** Opt-in: where the open write records this producer's identity ({@link RindleStreamOptions.hostId}).
   *  Naming it upgrades the row-level single-flight guard from check-then-act to a true
   *  compare-and-swap — the open write turns conditional and a read-back names the winner (§5.1) —
   *  and gives multi-instance subscribe routing a column to read (§4). No default. */
  host?: string;
  /** Chunk primary key; the plane writes the deterministic `"<streamId>:<seq>"`. Default `id`. */
  chunkKey?: string;
  /** Chunk → message reference. Default `streamId`. */
  chunkStream?: string;
  /** The chunk's END offset — the ordering key. Default `seq`. */
  chunkSeq?: string;
  /** The chunk's slice of the response. Default `text`. */
  chunkText?: string;
}

/**
 * The app's tables (§5). The app authors and migrates BOTH — the message row is unambiguously
 * app-owned (it has `chatId`, `role`, token counts) and the chunk row must be reachable from the
 * app's own query as a `related` subquery, which a Rindle system table would make awkward. The plane
 * only needs to be told where things live. Use {@link streamChunkTableDdl} for the chunk table's
 * migration.
 *
 * The message table carries WHATEVER ELSE the app wants; the plane's hard requirements are only:
 *
 * | mapped column | requirement | why |
 * | --- | --- | --- |
 * | `key`    | UNIQUE (normally the pk) | every checkpoint targets one row by it |
 * | `seq`    | integer, **NOT NULL DEFAULT 0** | the CAS column — nothing matches NULL (§3.2) |
 * | `body`   | text, empty at open | compaction overwrites it with the whole response |
 * | `status` | text accepting `streaming`/`complete`/`cancelled`/`error`/`interrupted` | a CHECK
 *              constraint that omits one of these turns a seal into an infra failure |
 * | `cancel` | truthy-readable, if mapped | read on the checkpoint round-trip (§6) |
 * | `error`  | nullable text, if mapped | compaction writes `NULL` when there is no error |
 * | `host`   | text, if mapped | written at open with the producer's token; the read-back decides the open race (§5.1) |
 *
 * `body` is PLANE-OWNED and always a bare string. Rich content (an array of content blocks, tool
 * calls, attachments) belongs in SIBLING columns the app's own mutators write — `flush()` orders the
 * text before them. For genuinely multi-block streaming, point `message` at a per-BLOCK table
 * instead: `streamId` is just an app key, so one stream per block needs nothing from this plane.
 */
export interface StreamTables {
  /** The app's message table. Must already contain the row when {@link StreamPlane.open} runs. */
  message: string;
  /** The append-only chunk table. */
  chunks: string;
  columns?: StreamColumns;
}

export type StreamCheckpointTarget = { tables: StreamTables } | { commit: StreamCommit };

// ------------------------------------------------------------------------------- configuration

/** When a checkpoint fires — the FIRST of these wins (§3.1). Checkpoints are serialized, so a slow
 *  store degrades to fewer, larger checkpoints, never to a queue of them. */
export interface StreamCheckpointPolicy {
  /** Produced-but-uncommitted characters that force a checkpoint. Default 512. */
  chars?: number;
  /** Milliseconds since the last checkpoint that force one. Default 750. */
  intervalMs?: number;
  /** Retries for a failing commit before the slice is left for the next trigger. Default 3. */
  retries?: number;
}

export interface AuthorizeStreamInput<User> {
  user: User;
  streamId: string;
  /** Where the subscriber claims to be. */
  from: number;
  /** The `meta` this stream was opened with — `undefined` when the stream is not hosted here, which
   *  is precisely when the app must decide from `streamId` and its own durable state. */
  meta: unknown;
  request?: unknown;
}

/**
 * Optional cross-process transport for the LIVE plane
 * (designs-implemented/LM-STREAM-RELAY-DESIGN.md). Both methods are independently optional; which
 * ones you implement is which topology you built — an addressing adapter (a Durable Object named by
 * `streamId`, `fly-replay`) implements only `attach`; a broadcast adapter (Redis pub/sub, NATS)
 * mirrors with `publish` and subscribes with `attach`; a log adapter (Redis Streams, Kafka) appends
 * and replays. Never consulted for the durable plane — checkpoints are unaffected by any of this.
 *
 * The plane does not trust what `attach` yields: frames are run through the conform pass
 * ({@link StreamRelayConform}) and any contract violation downgrades the subscription to `stale`,
 * which already means "you are on the durable plane now". A broken relay costs a reader smooth
 * tokens, never corrupted text — and can never reach the producer.
 */
export interface StreamRelay {
  /** Producer side: every frame this process's producer fans out (`chunk`, `durable`, and the
   *  terminal `end`), mirrored outward. MUST NOT block; a throw or rejected promise is caught and
   *  routed to {@link RindleStreamOptions.onRelayError} — a relay outage may cost the live leg,
   *  never the generation. Returned promises are observed but never awaited. */
  publish?(streamId: string, frame: StreamFrame): void | PromiseLike<void>;
  /** Subscriber side: this process is not hosting `streamId`. Return a frame source, or `undefined`
   *  for `absent` — exactly the no-relay answer. Consulted only AFTER `authorize` has passed, and
   *  only on a live-plane miss (a local stream always wins). The plane closes the source
   *  (`return()`) when the reader disconnects. An adapter that cannot serve `from` (pub/sub has no
   *  history) yields `stale` and stops — the reader converges on the durable plane (§5). */
  attach?(streamId: string, from: number): Promise<AsyncIterable<StreamFrame> | undefined>;
}

export interface StreamRelayErrorInfo {
  streamId: string;
  /** Where it failed: mirroring a frame out (`publish`), dialing the adapter (`attach`), or
   *  consuming/conforming its frames (`frames`). */
  phase: "publish" | "attach" | "frames";
}

export interface RindleStreamOptions<User> {
  /** Where checkpoints land: the app's tables (the default path) or a raw `commit` callback. */
  checkpoint: StreamCheckpointTarget;
  /** REQUIRED. Subscribing to a stream is reading someone's chat, so there is no default-allow.
   *  Runs BEFORE existence is checked, so a denial cannot be used to probe for stream ids. */
  authorize: Authorizer<AuthorizeStreamInput<User>>;
  policy?: StreamCheckpointPolicy;
  /** This process's identity — it must be UNIQUE per producer process, because the open CAS trusts
   *  it to distinguish rivals (§5.1). In `tables` mode, map {@link StreamColumns.host} and the open
   *  write persists it on the message row (so the app can route later subscribers to the hosting
   *  instance, §4) and uses it as the single-flight token; setting it WITHOUT a mapped `host` column
   *  is refused at construction. In `commit` mode it rides the `open` input. When a `host` column is
   *  mapped and no hostId is given, a random per-plane token is used — the CAS still holds, routing
   *  just has no stable name to read. */
  hostId?: string;
  /** Slack retained BELOW `durableSeq` so a client whose IVM view lags a checkpoint can still join
   *  without a `stale` round trip. Text at or above `durableSeq` is never trimmed. Default 64 KiB.
   *  `commit`-mode only — REFUSED (a construction-time `TypeError`) in `tables` mode, where
   *  compaction needs the whole produced text at close (§3.4), so the buffer is retained in full. */
  retainChars?: number;
  /** How long a sealed stream stays joinable before eviction. Default 30s. */
  lingerMs?: number;
  /** Per-subscriber frame queue cap; overflow drops that subscriber with `stale` (§4). Default 1024.
   *  Relayed readers reuse the same bound: a slow reader on a relayed stream costs itself the live
   *  leg exactly as a local one does. */
  maxQueuedFrames?: number;
  /** A checkpoint that exhausted its retries. The stream keeps streaming — this is a durability
   *  stall, not a stream stall — so the error must not vanish. Absent ⇒ `console.error`. */
  onCheckpointError?: (err: unknown, info: { streamId: string; from: number; seq: number }) => void;
  /** Cross-process transport for the live plane ({@link StreamRelay}). Without one, a subscriber
   *  that lands on a process not hosting its stream gets `absent` and reads the durable plane at
   *  checkpoint granularity — correct, just chunky. */
  relay?: StreamRelay;
  /** Bound on `relay.attach`: a hung adapter yields `absent`, not a hung HTTP request. An
   *  addressing adapter MAY deliberately spend this window waiting out a subscribe that races its
   *  own kick. Default 2000. */
  relayAttachTimeoutMs?: number;
  /** A diagnostic, never a control path: relay failures (a throwing or rejecting `publish`, a failed
   *  or timed-out `attach`, a conform violation in the frames) land here, wrapped so a throwing hook
   *  cannot reach the plane. The reader-facing outcome is always the same legal `absent`/`stale`.
   *  Absent ⇒ `console.error`. */
  onRelayError?: (err: unknown, info: StreamRelayErrorInfo) => void;
}

// ------------------------------------------------------------------------------- producer handle

export interface OpenStreamInput<User> {
  user: User;
  /** The app's message-row id: the durable pointer AND the live plane's key. The row must already
   *  exist (the app's own mutator wrote it, alongside the user's prompt) with `seq = 0`. */
  streamId: string;
  /** Opaque app payload, passed through to a `commit` callback. Unused in `tables` mode. */
  meta?: unknown;
  request?: unknown;
}

/** The producer's handle (§2). One writer per stream, by construction. */
export interface StreamHandle {
  readonly streamId: string;
  /** Total produced code units. */
  readonly seq: number;
  /** Total code units the store has committed. Never exceeds {@link seq} (contract P). */
  readonly durableSeq: number;
  /** True once a checkpoint round-trip has seen the reader's cancel flag (§6). `pump` stops on it;
   *  a hand-rolled generation loop should check it. */
  readonly cancelled: boolean;
  /** Append a delta: fanned to subscribers synchronously, checkpointed on policy. */
  push(text: string): void;
  /** Force a checkpoint and resolve once the store holds every character produced so far. This is
   *  the ORDERING primitive: `await flush()` before writing a discrete row (a tool call, a stop
   *  reason) so the text precedes it in the store (§1). Rejects if the checkpoint cannot commit. */
  flush(): Promise<number>;
  /** Drain a delta iterable into the stream (the shape every LLM SDK's text stream already has),
   *  stopping early — and closing the iterator, which aborts the underlying request — once the
   *  reader has cancelled. */
  pump(deltas: AsyncIterable<string>): Promise<void>;
  /** Seal the stream: `cancelled` if the reader asked it to stop, else `complete`. In `tables` mode
   *  this is the compaction (§3.4) — it writes the whole body and drops the chunk rows in one
   *  transaction, so it also REPAIRS any checkpoint that failed along the way. */
  close(): Promise<void>;
  /** Seal `error` at whatever was produced. Never throws for the reason it is sealing. */
  fail(error: unknown): Promise<void>;
}

export interface SubscribeStreamInput<User> {
  user: User;
  streamId: string;
  /** "I already have this many characters" — from the client's IVM view, or a `Last-Event-ID`.
   *  A non-negative integer; default 0. */
  from?: number;
  request?: unknown;
}

export interface StreamSubscription {
  readonly streamId: string;
  /** Terminates after exactly one of `end` / `stale` / `absent`. */
  readonly frames: AsyncIterable<StreamFrame>;
  /** Detach early (a disconnected client). Idempotent. */
  close(): void;
}

// ------------------------------------------------------------------------------- defaults

const DEFAULT_CHECKPOINT_CHARS = 512;
const DEFAULT_CHECKPOINT_INTERVAL_MS = 750;
const DEFAULT_CHECKPOINT_RETRIES = 3;
const DEFAULT_RETAIN_CHARS = 64 * 1024;
const DEFAULT_LINGER_MS = 30_000;
const DEFAULT_MAX_QUEUED_FRAMES = 1024;
const DEFAULT_RELAY_ATTACH_TIMEOUT_MS = 2000;
/** Retry backoff base — 50ms, 100ms, 200ms, … A checkpoint failure is usually a blip at the write
 *  authority; the text is safe in the buffer meanwhile, so there is nothing to rush. */
const RETRY_BACKOFF_MS = 50;

/** A SCHEDULING timer — the checkpoint cadence, the linger window, the SSE keep-alive. It never
 *  holds the process open: none of them is work in flight, and a stream plane must not keep a CLI
 *  alive. `unref` is Node-only — Workers' `setTimeout` has no such method, hence the guard. */
function timer(fn: () => void, ms: number): ReturnType<typeof setTimeout> {
  const t = setTimeout(fn, ms);
  (t as { unref?: () => void }).unref?.();
  return t;
}

/** The retry backoff, and deliberately NOT unref'd: a checkpoint mid-retry IS work in flight, and a
 *  process that exits during it drops text the caller is still awaiting. */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function errText(err: unknown): string {
  return String((err as Error)?.message ?? err);
}

/** A per-plane stand-in for {@link RindleStreamOptions.hostId} when only the open CAS — not
 *  subscribe routing — needs a token. */
function randomOpenToken(): string {
  const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
  return c?.randomUUID?.() ?? `open-${Math.random().toString(36).slice(2)}`;
}

// ------------------------------------------------------------------------------- mapped-table SQL

/** Every mapped column, defaults applied. `cancel`/`error` stay `undefined` unless named. */
export interface ResolvedStreamColumns {
  key: string;
  body: string;
  status: string;
  seq: string;
  cancel?: string;
  error?: string;
  host?: string;
  chunkKey: string;
  chunkStream: string;
  chunkSeq: string;
  chunkText: string;
}

export function resolveStreamColumns(columns: StreamColumns | undefined): ResolvedStreamColumns {
  return {
    key: columns?.key ?? "id",
    body: columns?.body ?? "body",
    status: columns?.status ?? "status",
    seq: columns?.seq ?? "seq",
    ...(columns?.cancel !== undefined ? { cancel: columns.cancel } : {}),
    ...(columns?.error !== undefined ? { error: columns.error } : {}),
    ...(columns?.host !== undefined ? { host: columns.host } : {}),
    chunkKey: columns?.chunkKey ?? "id",
    chunkStream: columns?.chunkStream ?? "streamId",
    chunkSeq: columns?.chunkSeq ?? "seq",
    chunkText: columns?.chunkText ?? "text",
  };
}

/** Quote an identifier for both dialects (`"x"` is standard in SQLite and Postgres alike). An
 *  embedded quote is refused rather than escaped: a mapping is app configuration, and a name that
 *  needs escaping is a mistake worth failing loudly on. */
function q(identifier: string): string {
  if (identifier.includes('"')) throw new TypeError(`invalid stream column/table name: ${JSON.stringify(identifier)}`);
  return `"${identifier}"`;
}

/** The chunk row's deterministic id: a replayed checkpoint collides with itself and is absorbed by
 *  `ON CONFLICT DO NOTHING` — idempotency without an envelope or a dedup ledger (§3.3). */
export function streamChunkId(streamId: string, seq: number): string {
  return `${streamId}:${seq}`;
}

/**
 * The chunk table's DDL, for the app's migration. The app owns the message table (this only states
 * the three columns the plane needs on it); the chunk table is entirely protocol-shaped, so it is
 * generated rather than hand-written.
 */
export function streamChunkTableDdl(tables: StreamTables, dialect: SqlDialect): string[] {
  const c = resolveStreamColumns(tables.columns);
  const text = dialect.name === "postgres" ? "text" : "TEXT";
  const int = dialect.name === "postgres" ? "bigint" : "INTEGER";
  return [
    `CREATE TABLE IF NOT EXISTS ${q(tables.chunks)} (` +
      `${q(c.chunkKey)} ${text} PRIMARY KEY, ` +
      `${q(c.chunkStream)} ${text} NOT NULL, ` +
      `${q(c.chunkSeq)} ${int} NOT NULL, ` +
      `${q(c.chunkText)} ${text} NOT NULL)`,
    // The read path is always "this message's chunks, in order" — and compaction deletes by stream.
    `CREATE INDEX IF NOT EXISTS ${q(`${tables.chunks}_stream_seq`)} ` +
      `ON ${q(tables.chunks)} (${q(c.chunkStream)}, ${q(c.chunkSeq)})`,
  ];
}

/** The plane's SQL, one place, both dialects. */
class MappedTableSql {
  private readonly tables: StreamTables;
  private readonly cols: ResolvedStreamColumns;
  private readonly dialect: SqlDialect;

  constructor(tables: StreamTables, dialect: SqlDialect) {
    this.tables = tables;
    this.cols = resolveStreamColumns(tables.columns);
    this.dialect = dialect;
  }

  private p(i: number): string {
    return this.dialect.placeholder(i);
  }

  /** The open probe: the row must exist and be empty. A generation is never a resume — regenerating
   *  is a NEW message id — so an already-advanced row is a bug worth refusing loudly. */
  probeOpen(): string {
    const { key, seq, status } = this.cols;
    return `SELECT ${q(seq)} AS seq, ${q(status)} AS status FROM ${q(this.tables.message)} WHERE ${q(key)} = ${this.p(1)}`;
  }

  /** The open write. With a `host` column mapped this is the CAS half of the single-flight guard:
   *  set the status AND this producer's token only if no producer holds the row, verified by the
   *  read-back in {@link probeHost}. Without one it is the plain flip the probe already vetted. */
  markStreaming(streamId: string, hostToken: string): SqlStatement {
    const { key, status, host } = this.cols;
    if (host !== undefined) {
      return {
        sql:
          `UPDATE ${q(this.tables.message)} SET ${q(status)} = ${this.p(1)}, ${q(host)} = ${this.p(2)} ` +
          `WHERE ${q(key)} = ${this.p(3)} AND ${q(status)} <> ${this.p(4)}`,
        params: [STREAM_STATUS_STREAMING, hostToken, streamId, STREAM_STATUS_STREAMING],
      };
    }
    return {
      sql: `UPDATE ${q(this.tables.message)} SET ${q(status)} = ${this.p(1)} WHERE ${q(key)} = ${this.p(2)}`,
      params: [STREAM_STATUS_STREAMING, streamId],
    };
  }

  /** The read-back half of the open CAS, or `undefined` when no `host` column is mapped. */
  probeHost(): string | undefined {
    const { host, key } = this.cols;
    if (host === undefined) return undefined;
    return `SELECT ${q(host)} AS host FROM ${q(this.tables.message)} WHERE ${q(key)} = ${this.p(1)}`;
  }

  /** ONE transaction: insert the slice, then CAS the message's durable length. BOTH statements are
   *  gated on the same `seq = :from` predicate: the CAS refuses to apply twice or out of order, and
   *  the guarded insert makes a STALE RE-COVER a no-op — after a committed-but-ack-lost append, the
   *  next slice's `from` lags the row, and an unguarded insert would land an OVERLAPPING chunk row
   *  (a different deterministic id, so `ON CONFLICT` alone cannot absorb it) and corrupt the
   *  assembled text until compaction. The `ON CONFLICT` clause stays as the belt under that
   *  guard's braces. Which case actually happened is decided by the read-back ({@link probeState}),
   *  never assumed (§3.2/§3.3). */
  append(streamId: string, from: number, seq: number, text: string): SqlStatement[] {
    const c = this.cols;
    return [
      {
        sql:
          `INSERT INTO ${q(this.tables.chunks)} ` +
          `(${q(c.chunkKey)}, ${q(c.chunkStream)}, ${q(c.chunkSeq)}, ${q(c.chunkText)}) ` +
          `SELECT ${this.p(1)}, ${this.p(2)}, ${this.p(3)}, ${this.p(4)} ` +
          `WHERE EXISTS (SELECT 1 FROM ${q(this.tables.message)} ` +
          `WHERE ${q(c.key)} = ${this.p(5)} AND ${q(c.seq)} = ${this.p(6)}) ` +
          `ON CONFLICT DO NOTHING`,
        params: [streamChunkId(streamId, seq), streamId, seq, text, streamId, from],
      },
      {
        sql:
          `UPDATE ${q(this.tables.message)} SET ${q(c.seq)} = ${this.p(1)} ` +
          `WHERE ${q(c.key)} = ${this.p(2)} AND ${q(c.seq)} = ${this.p(3)}`,
        params: [seq, streamId, from],
      },
    ];
  }

  /** Compaction (§3.4): write the whole body, seal the status, drop every chunk — atomically. Also
   *  the repair path: whatever the chunks did or did not capture, the body ends up correct. */
  compact(streamId: string, body: string, status: StreamStatus, error: string | undefined): SqlStatement[] {
    const c = this.cols;
    const sets = [`${q(c.body)} = ${this.p(1)}`, `${q(c.seq)} = ${this.p(2)}`, `${q(c.status)} = ${this.p(3)}`];
    const params: Array<string | number | null> = [body, body.length, status];
    if (c.error !== undefined) {
      sets.push(`${q(c.error)} = ${this.p(params.length + 1)}`);
      params.push(error ?? null);
    }
    params.push(streamId);
    return [
      {
        sql: `UPDATE ${q(this.tables.message)} SET ${sets.join(", ")} WHERE ${q(c.key)} = ${this.p(params.length)}`,
        params,
      },
      {
        sql: `DELETE FROM ${q(this.tables.chunks)} WHERE ${q(c.chunkStream)} = ${this.p(1)}`,
        params: [streamId],
      },
    ];
  }

  get hasCancel(): boolean {
    return this.cols.cancel !== undefined;
  }

  /** The post-append read-back: the row's authoritative length — what CONFIRMS an append applied
   *  (and absorbs a committed-but-ack-lost one) — plus the cancel flag when mapped (§6). */
  probeState(): string {
    const { cancel, key, seq } = this.cols;
    const picked = cancel === undefined ? `${q(seq)} AS seq` : `${q(seq)} AS seq, ${q(cancel)} AS cancel`;
    return `SELECT ${picked} FROM ${q(this.tables.message)} WHERE ${q(key)} = ${this.p(1)}`;
  }
}

// ------------------------------------------------------------------------------- subscribers

/** One attached reader. Frames queue; a reader that stops draining is bounded and then DROPPED with
 *  `stale` rather than allowed to pin the producer's memory — it resolves that by rejoining. */
class Subscriber {
  private queue: StreamFrame[] = [];
  private waiter?: (r: IteratorResult<StreamFrame>) => void;
  private done = false;
  private readonly cap: number;
  private readonly onDetach: (s: Subscriber) => void;

  constructor(cap: number, onDetach: (s: Subscriber) => void) {
    this.cap = cap;
    this.onDetach = onDetach;
  }

  /** @returns false when the queue overflowed (caller drops this subscriber). */
  offer(frame: StreamFrame): boolean {
    if (this.done) return true;
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = undefined;
      w({ value: frame, done: false });
      return true;
    }
    if (this.queue.length >= this.cap) return false;
    this.queue.push(frame);
    return true;
  }

  /** Deliver a terminal frame (bypassing the cap — it is the last one) and end the iterator. */
  finish(frame: StreamFrame): void {
    if (this.done) return;
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = undefined;
      this.done = true;
      w({ value: frame, done: false });
      return;
    }
    this.queue.push(frame);
    this.done = true;
  }

  close(): void {
    this.done = true;
    this.queue.length = 0;
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = undefined;
      w({ value: undefined as never, done: true });
    }
    this.onDetach(this);
  }

  frames(): AsyncIterable<StreamFrame> {
    const self = this;
    return {
      [Symbol.asyncIterator](): AsyncIterator<StreamFrame> {
        return {
          next(): Promise<IteratorResult<StreamFrame>> {
            const queued = self.queue.shift();
            if (queued !== undefined) return Promise.resolve({ value: queued, done: false });
            if (self.done) {
              self.onDetach(self);
              return Promise.resolve({ value: undefined as never, done: true });
            }
            return new Promise((resolve) => {
              self.waiter = resolve;
            });
          },
          return(): Promise<IteratorResult<StreamFrame>> {
            self.close();
            return Promise.resolve({ value: undefined as never, done: true });
          },
        };
      },
    };
  }
}

// ------------------------------------------------------------------------------- the relay conform pass

/**
 * One frame source arriving over a relay, conformed to the CP §4 contract
 * (designs-implemented/LM-STREAM-RELAY-DESIGN.md §4).
 *
 * An adapter is app code talking to Redis or a socket, and its frames feed `spliceStreamText` on a
 * browser — so the plane does not trust them. This pass enforces the frame invariants against the
 * prefix actually delivered and downgrades EVERY violation to a legal `stale` and nothing else:
 * `stale` already means "you are on the durable plane now, the store is the whole truth", so a
 * broken relay costs a reader smooth tokens, never corrupted text — and cannot wedge a producer.
 *
 * Replayed spans (a reconnecting adapter re-delivering what it already sent) are ABSORBED rather
 * than punished — deduping against the delivered prefix is what makes reconnect-replay safe without
 * every adapter hand-rolling it. Spans that overlap the prefix but extend past it pass through
 * whole: the client splices at the frame's own offset, so an exact overlap re-covers and appends.
 *
 * Pure state, no I/O, no plane: `feed` maps one incoming frame to 0-2 outgoing frames (a missing
 * `open` is synthesized at the join offset); `end`/`fail` close out a source that finished or threw
 * without a terminal. After a terminal, every method returns `[]`.
 */
export class StreamRelayConform {
  private readonly streamId: string;
  /** The requested join offset — the synthesized `open`'s position, and where the prefix starts. */
  private readonly from: number;
  private readonly onViolation: ((reason: string) => void) | undefined;
  /** End of the delivered prefix. */
  private pos: number;
  private lastDurable = 0;
  private opened = false;
  private done = false;

  constructor(streamId: string, from: number, onViolation?: (reason: string) => void) {
    this.streamId = streamId;
    this.from = from;
    this.pos = from;
    this.onViolation = onViolation;
  }

  feed(frame: StreamFrame): StreamFrame[] {
    if (this.done) return [];
    switch (frame.type) {
      case "open": {
        if (this.opened) return []; // a reconnecting adapter's second open: absorbed
        if (
          frame.streamId !== this.streamId ||
          !Number.isInteger(frame.from) ||
          !Number.isInteger(frame.seq) ||
          !Number.isInteger(frame.durableSeq) ||
          frame.from < 0 ||
          frame.seq < frame.from
        ) {
          return this.violate(`relay open for ${JSON.stringify(frame.streamId)} at ${frame.from} is malformed`);
        }
        // An open PAST the requested offset is the adapter saying it cannot serve `from` (pub/sub
        // has no history, §5): delivering it would leave a hole in the middle of the response, so
        // the honest answer is the durable plane.
        if (frame.from > this.from) {
          return this.violate(`relay open at ${frame.from} cannot serve the requested ${this.from}`);
        }
        this.opened = true;
        this.pos = frame.from;
        return [frame];
      }
      case "chunk": {
        if (
          !Number.isInteger(frame.from) ||
          !Number.isInteger(frame.seq) ||
          frame.from < 0 ||
          typeof frame.text !== "string" ||
          frame.text.length !== frame.seq - frame.from
        ) {
          return this.violate(`relay chunk ${frame.from}→${frame.seq} does not span exactly its offsets`);
        }
        if (frame.from > this.pos) {
          return this.violate(`relay chunk at ${frame.from} leaves a gap after ${this.pos}`);
        }
        if (frame.seq <= this.pos) return []; // entirely within the delivered prefix (a replay): absorbed
        const out = this.opened ? [] : [this.synthOpen()];
        this.pos = frame.seq;
        out.push(frame);
        return out;
      }
      case "durable": {
        // Purely informational to a reader (the hook ignores it; SSE uses it as a resume id), so a
        // claim that rewinds — or outruns what this subscription has SEEN produced, which P forbids
        // — is dropped rather than downgraded.
        if (!Number.isInteger(frame.seq) || frame.seq < this.lastDurable || frame.seq > this.pos) return [];
        const out = this.opened ? [] : [this.synthOpen()];
        this.lastDurable = frame.seq;
        out.push(frame);
        return out;
      }
      case "end": {
        if (!Number.isInteger(frame.seq) || frame.seq < 0) {
          return this.violate(`relay end at ${String(frame.seq)} is malformed`);
        }
        // An `end` whose durable length outruns the delivered prefix means the adapter LOST text
        // (durable never exceeds produced), so the reader is short and must not be told it saw
        // everything. Downgrading keeps `end` meaning the same thing relayed as local: the whole
        // produced text arrived.
        if (frame.seq > this.pos) {
          return this.violate(`relay end at ${frame.seq} outruns the ${this.pos} characters delivered`);
        }
        this.done = true;
        const out = this.opened ? [] : [this.synthOpen()];
        out.push(frame);
        return out;
      }
      case "stale":
      case "absent": {
        // Legal bare — the local plane's own floor/eviction answers carry no `open` either.
        this.done = true;
        return [frame];
      }
    }
  }

  /** The source completed without a terminal (a truncated relay): the reader falls back. */
  end(): StreamFrame[] {
    if (this.done) return [];
    this.onViolation?.("relay source ended without a terminal frame");
    return this.terminate();
  }

  /** The source threw mid-iteration, or the plane is dropping a reader that stopped draining:
   *  a bare `stale` at the delivered position. */
  fail(): StreamFrame[] {
    return this.done ? [] : this.terminate();
  }

  /** A synthesized join, for an adapter that (correctly, in broadcast mode) never mirrors the
   *  per-subscriber `open`: positioned at the requested offset, which the reader asked from because
   *  its durable view already holds it. */
  private synthOpen(): StreamFrame {
    this.opened = true;
    return { type: "open", streamId: this.streamId, from: this.from, seq: this.from, durableSeq: this.from, ended: false };
  }

  private terminate(): StreamFrame[] {
    this.done = true;
    return [{ type: "stale", floorSeq: this.pos, durableSeq: this.lastDurable }];
  }

  private violate(reason: string): StreamFrame[] {
    this.onViolation?.(reason);
    return this.terminate();
  }
}

// ------------------------------------------------------------------------------- the live stream

interface Waiter {
  at: number;
  resolve: (seq: number) => void;
  reject: (err: unknown) => void;
}

class LiveStream<User> {
  /** Retained text; `buf[0]` sits at offset {@link bufFrom}. */
  private buf = "";
  private bufFrom = 0;
  seq = 0;
  durableSeq = 0;
  ended = false;
  cancelled = false;

  readonly streamId: string;
  readonly user: User;
  readonly meta: unknown;
  private readonly plane: StreamPlane<User>;

  private readonly subs = new Set<Subscriber>();
  private sealing?: { status: StreamStatus; error?: string };
  private draining = false;
  private forced = false;
  /** The last drain round left a slice uncommitted: wait out the interval instead of re-attempting
   *  immediately, so a down store costs a retry cadence and not a hot loop. */
  private stalled = false;
  private tick: ReturnType<typeof setTimeout> | null = null;
  private waiters: Waiter[] = [];
  private sealed?: { resolve: () => void; reject: (e: unknown) => void; promise: Promise<void> };
  private sealError: unknown;

  constructor(streamId: string, user: User, meta: unknown, plane: StreamPlane<User>) {
    this.streamId = streamId;
    this.user = user;
    this.meta = meta;
    this.plane = plane;
  }

  // ---- producer side

  push(text: string): void {
    if (this.sealing || this.ended) throw new Error(`stream ${this.streamId} is closed`);
    if (text.length === 0) return;
    const from = this.seq;
    this.buf += text;
    this.seq += text.length;
    this.fanout({ type: "chunk", from, seq: this.seq, text });
    if (this.seq - this.durableSeq >= this.plane.chars) this.kick(true);
    else this.arm();
  }

  flush(): Promise<number> {
    const at = this.seq;
    if (this.durableSeq >= at) return Promise.resolve(this.durableSeq);
    // After a failed seal the shortfall is PERMANENT (the stream ended; nothing will retry it), so a
    // late flush rejects honestly instead of quietly re-running the seal.
    if (this.ended) {
      return Promise.reject(
        new Error(`stream ${this.streamId} ended with only ${this.durableSeq} of ${this.seq} characters durable`),
      );
    }
    const p = new Promise<number>((resolve, reject) => {
      this.waiters.push({ at, resolve, reject });
    });
    this.kick(true);
    return p;
  }

  /** Seal the stream. Resolves once the terminal checkpoint has committed; rejects if it could not
   *  (the stream still ENDS — subscribers always get their `end` frame — the caller just learns the
   *  store is short of what was produced). */
  seal(status: StreamStatus, error?: string): Promise<void> {
    // Already sealed: hand back the SAME settled promise (already marked handled below), so a second
    // `close()`/`fail()` can neither re-seal nor mint a stray rejection.
    if (this.ended) return this.sealed?.promise ?? Promise.resolve();
    if (!this.sealing) {
      this.sealing = { status, error };
      let resolve!: () => void;
      let reject!: (e: unknown) => void;
      const promise = new Promise<void>((res, rej) => {
        resolve = res;
        reject = rej;
      });
      // A `fail()` nobody awaited must not become an unhandled rejection at process level. This
      // marks the promise handled WITHOUT consuming it — a caller that does await still sees the
      // rejection, because `await` attaches its own handler to the same promise.
      promise.catch(() => {});
      this.sealed = { resolve, reject, promise };
      this.disarm();
      this.kick(true);
    }
    return this.sealed!.promise;
  }

  // ---- checkpoint driving

  private arm(): void {
    if (this.tick || this.seq === this.durableSeq) return;
    this.tick = timer(() => {
      this.tick = null;
      this.kick(true);
    }, this.plane.intervalMs);
  }

  private disarm(): void {
    if (this.tick) clearTimeout(this.tick);
    this.tick = null;
  }

  private kick(force: boolean): void {
    if (force) this.forced = true;
    if (this.draining) return;
    this.draining = true;
    void this.drain();
  }

  private async drain(): Promise<void> {
    this.stalled = false;
    try {
      for (;;) {
        // Sealing goes STRAIGHT to the seal: it writes the whole body (§3.4), so committing the
        // outstanding tail as one more chunk first would be pure waste — and the seal repairs any
        // earlier checkpoint that failed, which a tail commit could not.
        if (this.sealing) {
          await this.commitSeal();
          break;
        }
        const behind = this.seq - this.durableSeq;
        if (behind > 0 && (this.forced || behind >= this.plane.chars)) {
          // Consume the force BEFORE the round, not after: a `flush()` that lands while this round
          // is already in flight re-sets it and gets a round of its OWN — clearing it afterwards
          // would swallow that request and leave the flush to the interval cadence.
          this.forced = false;
          this.disarm();
          const failure = await this.commitTail();
          if (failure) {
            this.stalled = true;
            // A seal requested WHILE this checkpoint was in flight outranks the retry cadence: the
            // seal writes the whole body anyway, so it both supersedes and repairs the failed slice.
            // Backing off here instead would make `close()` wait out `intervalMs` for nothing. An
            // explicit re-force during the failed round likewise earns one immediate re-attempt —
            // someone is waiting on it — while an unforced failure stalls to the cadence.
            if (!this.sealing && !this.forced) break;
          }
          continue;
        }
        this.forced = false;
        break;
      }
    } finally {
      this.draining = false;
      if (!this.ended) {
        // A pending seal ALWAYS proceeds at once — including out of a stall (see the loop above);
        // otherwise a stall waits out the interval, and deltas that landed mid-commit may already
        // meet the threshold and deserve an immediate round.
        if (this.sealing) this.kick(false);
        else if (this.stalled) this.arm();
        else if (this.seq - this.durableSeq >= this.plane.chars) this.kick(false);
        else this.arm();
      }
    }
  }

  /** @returns the failure, or `undefined` when the slice committed. */
  private async commitTail(): Promise<{ err: unknown } | undefined> {
    const from = this.durableSeq;
    const to = this.seq;
    const text = this.buf.slice(from - this.bufFrom, to - this.bufFrom);
    let out: { cancelRequested?: boolean; seq?: number } | void;
    try {
      out = await this.plane.commit({ kind: "append", streamId: this.streamId, from, seq: to, text });
    } catch (err) {
      // The store may hold MORE than we believe — an append that committed while its ack was lost,
      // somewhere in the retry chain. The read-back is the truth, and adopting it is what keeps the
      // NEXT slice contiguous instead of overlapping (§3.3).
      const truth = await this.plane.probeDurableSeq(this.streamId);
      if (truth !== undefined && truth > this.durableSeq) this.advanceDurable(truth);
      this.plane.reportCheckpointError(err, { streamId: this.streamId, from, seq: to });
      // Waiters that asked for THIS prefix learn it did not land; later waiters keep waiting for a
      // later attempt. A durability failure is never silent.
      this.rejectWaiters(to, err);
      return { err };
    }
    if (out?.cancelRequested) this.cancelled = true;
    // `durableSeq` advances to what the store CONFIRMED — the mapped path reads it back, a `commit`
    // callback may report it — never to a plane-side assumption. A confirmation short of `to` means
    // this slice's `from` was stale (a prior ack loss the read-back could not see at the time): the
    // guarded statements made it a no-op, and the next round re-covers from the confirmed offset.
    const confirmed = out?.seq ?? to;
    if (confirmed > this.durableSeq) this.advanceDurable(confirmed);
    if (confirmed < to) {
      const err = new Error(
        `stream ${this.streamId}: the store confirmed ${confirmed} of ${to} — re-covering from there`,
      );
      this.plane.reportCheckpointError(err, { streamId: this.streamId, from, seq: to });
      this.rejectWaiters(to, err);
      return { err };
    }
    return undefined;
  }

  /** The one place durable progress is recorded: position, `durable` frame, buffer trim, waiters. */
  private advanceDurable(seq: number): void {
    this.durableSeq = seq;
    this.fanout({ type: "durable", seq });
    this.trim();
    this.resolveWaiters();
  }

  private async commitSeal(): Promise<void> {
    const seal = this.sealing!;
    const from = this.durableSeq;
    try {
      await this.plane.commit({
        kind: "close",
        streamId: this.streamId,
        from,
        seq: this.seq,
        // A trimmed buffer (only reachable in `commit`-callback mode, where the app opted into
        // trimming) cannot supply the whole body: `bodyFrom` says where the retained text starts,
        // so the app can still append exactly the outstanding tail (`body.slice(from - bodyFrom)`).
        body: this.buf,
        bodyFrom: this.bufFrom,
        status: seal.status,
        ...(seal.error !== undefined ? { error: seal.error } : {}),
      });
      this.durableSeq = this.seq;
    } catch (err) {
      this.sealError = err;
      this.plane.reportCheckpointError(err, { streamId: this.streamId, from, seq: this.seq });
    }
    this.ended = true;
    this.disarm();
    this.rejectWaiters(
      Number.POSITIVE_INFINITY,
      new Error(`stream ${this.streamId} ended with only ${this.durableSeq} of ${this.seq} characters durable`),
    );
    this.resolveWaiters();
    const frame: StreamFrame = {
      type: "end",
      seq: this.durableSeq,
      status: seal.status,
      ...(seal.error !== undefined ? { error: seal.error } : {}),
    };
    // The terminal reaches the relay too (it bypasses `fanout` locally only to bypass the cap).
    this.plane.publishRelay(this.streamId, frame);
    for (const sub of [...this.subs]) sub.finish(frame);
    this.plane.retire(this.streamId);
    if (this.sealError) this.sealed?.reject(this.sealError);
    else this.sealed?.resolve();
  }

  /** Keep everything at or above `durableSeq`, plus a slack window below it so a client whose IVM
   *  view lags one checkpoint can still join without a `stale` round trip (§4). A `retainChars` of
   *  `Infinity` (the mapped-table default — compaction needs the whole text) never trims. */
  private trim(): void {
    if (!Number.isFinite(this.plane.retainChars)) return;
    const floor = Math.max(0, this.durableSeq - this.plane.retainChars);
    if (floor <= this.bufFrom) return;
    this.buf = this.buf.slice(floor - this.bufFrom);
    this.bufFrom = floor;
  }

  private resolveWaiters(): void {
    if (this.waiters.length === 0) return;
    const still: Waiter[] = [];
    for (const w of this.waiters) {
      if (w.at <= this.durableSeq) w.resolve(this.durableSeq);
      else still.push(w);
    }
    this.waiters = still;
  }

  private rejectWaiters(upTo: number, err: unknown): void {
    if (this.waiters.length === 0) return;
    const still: Waiter[] = [];
    for (const w of this.waiters) {
      if (w.at <= upTo && w.at > this.durableSeq) w.reject(err);
      else still.push(w);
    }
    this.waiters = still;
  }

  // ---- subscriber side

  private fanout(frame: StreamFrame): void {
    // Every frame local subscribers get, the relay gets — including when nobody local is attached
    // (a broadcast relay's whole point). Wrapped so an outage costs the live leg, never this stream.
    this.plane.publishRelay(this.streamId, frame);
    for (const sub of [...this.subs]) {
      if (!sub.offer(frame)) {
        // Bounded, then dropped: a reader that stopped draining costs itself a rejoin, never the
        // producer's memory.
        sub.finish({ type: "stale", floorSeq: this.bufFrom, durableSeq: this.durableSeq });
        this.subs.delete(sub);
      }
    }
  }

  /** Attach a reader at `from`. Runs in ONE synchronous block — snapshot, replay, register — so no
   *  delta can slip between the replay and the live tail (the gap-free half of contract S). */
  subscribe(from: number): StreamSubscription {
    const sub = new Subscriber(this.plane.maxQueuedFrames, (s) => this.subs.delete(s));
    // Floored as well as clamped: a fractional `from` (a hand-built subscribe input) would otherwise
    // produce a replay chunk whose `text.length !== seq - from`, breaking the frame invariant.
    const at = Math.min(Math.max(Math.floor(from), 0), this.seq);
    if (at < this.bufFrom) {
      sub.finish({ type: "stale", floorSeq: this.bufFrom, durableSeq: this.durableSeq });
      return this.wrap(sub);
    }
    sub.offer({ type: "open", streamId: this.streamId, from: at, seq: this.seq, durableSeq: this.durableSeq, ended: this.ended });
    if (this.seq > at) {
      sub.offer({ type: "chunk", from: at, seq: this.seq, text: this.buf.slice(at - this.bufFrom) });
    }
    if (this.durableSeq > at) sub.offer({ type: "durable", seq: this.durableSeq });
    if (this.ended) {
      const seal = this.sealing ?? { status: "interrupted" as StreamStatus };
      sub.finish({
        type: "end",
        seq: this.durableSeq,
        status: seal.status,
        ...(seal.error !== undefined ? { error: seal.error } : {}),
      });
      return this.wrap(sub);
    }
    this.subs.add(sub);
    return this.wrap(sub);
  }

  private wrap(sub: Subscriber): StreamSubscription {
    return { streamId: this.streamId, frames: sub.frames(), close: () => sub.close() };
  }

  /** Drop every reader without sealing (process teardown — `drainStreams` seals first). */
  detachAll(): void {
    for (const sub of [...this.subs]) sub.close();
    this.subs.clear();
    this.disarm();
  }
}

// ------------------------------------------------------------------------------- the plane

/** What the plane needs from the api-server to write a checkpoint: the backend's OUTSIDE-transaction
 *  SQL surface (`batch` is one transaction on every backend) and its dialect. Deliberately narrow so
 *  `streams.ts` never imports the server (no cycle). */
export interface StreamSqlSink {
  readonly dialect: SqlDialect;
  readonly sql: ServerSql;
}

export class StreamPlane<User> {
  readonly chars: number;
  readonly intervalMs: number;
  readonly retries: number;
  readonly retainChars: number;
  readonly lingerMs: number;
  readonly maxQueuedFrames: number;
  readonly relayAttachTimeoutMs: number;

  private readonly live = new Map<string, LiveStream<User>>();
  /** Live relayed subscriptions, so teardown ({@link closeSync}) releases their drivers too. */
  private readonly relayed = new Set<Subscriber>();
  private readonly opts: RindleStreamOptions<User>;
  private readonly sink: StreamSqlSink | undefined;
  private readonly mapped: MappedTableSql | undefined;
  private readonly tables: StreamTables | undefined;
  /** The open CAS token (§5.1): `hostId`, or a random per-plane stand-in when only the CAS — not
   *  routing — needs it. */
  private readonly openToken: string;

  constructor(opts: RindleStreamOptions<User>, sink?: StreamSqlSink) {
    this.opts = opts;
    this.sink = sink;
    this.chars = opts.policy?.chars ?? DEFAULT_CHECKPOINT_CHARS;
    this.intervalMs = opts.policy?.intervalMs ?? DEFAULT_CHECKPOINT_INTERVAL_MS;
    this.retries = opts.policy?.retries ?? DEFAULT_CHECKPOINT_RETRIES;
    this.lingerMs = opts.lingerMs ?? DEFAULT_LINGER_MS;
    this.maxQueuedFrames = opts.maxQueuedFrames ?? DEFAULT_MAX_QUEUED_FRAMES;
    this.relayAttachTimeoutMs = opts.relayAttachTimeoutMs ?? DEFAULT_RELAY_ATTACH_TIMEOUT_MS;
    this.openToken = opts.hostId ?? randomOpenToken();
    // Relay misconfiguration is refused loudly (the room-profile rule), never ignored.
    if (opts.relay !== undefined && opts.relay.publish === undefined && opts.relay.attach === undefined) {
      throw new TypeError("streams.relay implements neither publish nor attach — which topology is this? (LM-STREAM-RELAY §3.1)");
    }
    if (opts.relay === undefined && (opts.relayAttachTimeoutMs !== undefined || opts.onRelayError !== undefined)) {
      throw new TypeError("streams.relayAttachTimeoutMs/onRelayError do nothing without streams.relay");
    }
    if ("tables" in opts.checkpoint) {
      if (!sink) throw new TypeError("streams.checkpoint.tables needs a SQL-capable mutation backend");
      this.tables = opts.checkpoint.tables;
      this.mapped = new MappedTableSql(opts.checkpoint.tables, sink.dialect);
      // Options this mode cannot honour are refused loudly (the room-profile rule), never ignored.
      if (opts.retainChars !== undefined) {
        throw new TypeError(
          "streams.retainChars is `commit`-mode only — mapped tables retain the whole text for compaction (§3.4)",
        );
      }
      if (opts.hostId !== undefined && resolveStreamColumns(opts.checkpoint.tables.columns).host === undefined) {
        throw new TypeError("streams.hostId does nothing in tables mode until columns.host names where to persist it");
      }
      // Compaction writes the WHOLE body at close, so the buffer is retained in full. `retainChars`
      // is honoured only where the app owns persistence and may not need the tail.
      this.retainChars = Number.POSITIVE_INFINITY;
    } else {
      this.retainChars = opts.retainChars ?? DEFAULT_RETAIN_CHARS;
    }
  }

  /** Open a stream on an EXISTING message row (§5): the app's own mutator wrote it, alongside the
   *  user's prompt, so the pointer is already durable and every client's query already shows the
   *  message. This verifies it and flips it to `streaming`. */
  async open(input: OpenStreamInput<User>): Promise<StreamHandle> {
    if (this.live.has(input.streamId)) {
      throw new Error(`stream ${input.streamId} is already open on this host`);
    }
    const stream = new LiveStream<User>(input.streamId, input.user, input.meta, this);
    this.live.set(input.streamId, stream);
    try {
      if (this.mapped) await this.assertOpenable(input.streamId);
      await this.commit({
        kind: "open",
        streamId: input.streamId,
        meta: input.meta ?? null,
        ...(this.opts.hostId !== undefined ? { hostId: this.opts.hostId } : {}),
        startedAt: Date.now(),
      });
    } catch (err) {
      this.live.delete(input.streamId);
      throw err;
    }
    return {
      streamId: stream.streamId,
      get seq() {
        return stream.seq;
      },
      get durableSeq() {
        return stream.durableSeq;
      },
      get cancelled() {
        return stream.cancelled;
      },
      push: (text) => stream.push(text),
      flush: () => stream.flush(),
      pump: async (deltas) => {
        // `for await` closes the iterator on `break`, which is what aborts the SDK's underlying
        // request — the whole point of honouring cancellation here rather than in the caller.
        for await (const d of deltas) {
          stream.push(d);
          if (stream.cancelled) break;
        }
      },
      close: () => stream.seal(stream.cancelled ? "cancelled" : "complete"),
      fail: (error) => stream.seal("error", errText(error)),
    };
  }

  /**
   * The read-only precondition on the app's message row, checked ONCE per `open` (§5.1). Read-only on
   * purpose: it is a decision about the app's data, so re-deciding it per write attempt would let a
   * lost ack turn a success into a refusal.
   *
   * The `streaming` check is the **single-flight guard**, and it lives at the ROW rather than in this
   * process's map because the thing it defends against is distributed: the kick that starts a
   * generation is an at-least-once effect (a retried mutation envelope re-runs its post-commit code,
   * §10.5), so a second kick can land on another instance, where the in-memory map is empty. Two
   * producers on one `streamId` would interleave: both CAS the same length, one stalls, and whichever
   * closes last overwrites the body with ITS buffer. Cheap to refuse; expensive to debug.
   *
   * This probe alone is check-then-act — it and the open write are separate round trips, so two
   * SIMULTANEOUS kicks can both pass it. Mapping a `host` column closes that window: the open write
   * turns conditional and {@link verifyOpenWinner}'s read-back names the winner.
   */
  private async assertOpenable(streamId: string): Promise<void> {
    const cols = resolveStreamColumns(this.tables!.columns);
    const rows = await this.sink!.sql.query<{ seq: number | null; status: string | null }>(
      this.mapped!.probeOpen(),
      [streamId],
    );
    const row = rows[0];
    if (!row) {
      throw new StreamOpenRefused(
        `stream ${streamId}: no row in "${this.tables!.message}" — the app's own mutator must write the message ` +
          `(with its chatId/role/…) BEFORE opening the stream on it`,
      );
    }
    // A NULL length is refused as loudly as an advanced one, and for a subtler reason: it is the CAS
    // column, and `WHERE seq = 0` can never match NULL. A nullable column would let every chunk
    // insert land while its length CAS silently matched nothing — the one failure mode this plane
    // cannot detect (`batch` reports no row count) and one compaction would paper over (§5.1).
    if (typeof row.seq !== "number") {
      throw new StreamOpenRefused(
        `stream ${streamId}: "${this.tables!.message}"."${cols.seq}" is ${row.seq === null ? "NULL" : typeof row.seq} — ` +
          `the length column must be NOT NULL DEFAULT 0 (it is compare-and-swapped on every checkpoint, and no ` +
          `comparison matches NULL)`,
      );
    }
    if (row.seq !== 0) {
      throw new StreamOpenRefused(
        `stream ${streamId}: the message row already holds ${row.seq} characters — a regeneration is a NEW message ` +
          `id, never a resume of an advanced row`,
      );
    }
    if (row.status === STREAM_STATUS_STREAMING) {
      throw new StreamOpenRefused(
        `stream ${streamId}: "${cols.status}" is already "${STREAM_STATUS_STREAMING}" — another producer holds this ` +
          `stream. Two producers on one stream interleave their checkpoints; if the first one's host died, the ` +
          `sweeper marking it "interrupted" is what releases the row (§7)`,
      );
    }
  }

  async subscribe(input: SubscribeStreamInput<User>): Promise<StreamSubscription> {
    const stream = this.live.get(input.streamId);
    const from = input.from ?? 0;
    // Authorize BEFORE existence is consulted: a denial must not double as an existence oracle.
    const verdict = await this.opts.authorize({
      user: input.user,
      streamId: input.streamId,
      from,
      meta: stream === undefined ? undefined : stream.meta,
      request: input.request,
    });
    if (verdict === false) throw new StreamForbidden(input.streamId);
    if (!stream) {
      // The relay is consulted only on a live-plane miss, and only after `authorize` passed — a
      // denial must not become an existence probe against the relay either. A local stream always
      // wins: the producer's own readers keep the lowest-latency path.
      const relayed = await this.attachRelay(input.streamId, from);
      return relayed ?? oneFrame(input.streamId, { type: "absent" });
    }
    return stream.subscribe(from);
  }

  /** The subscribe-miss leg (LM-STREAM-RELAY §3): ask the app's relay for the frames of a stream
   *  this process is not hosting. `undefined` — no relay, no `attach`, the adapter declined, timed
   *  out, or threw — is `absent`, exactly today's answer. */
  private async attachRelay(streamId: string, from: number): Promise<StreamSubscription | undefined> {
    const relay = this.opts.relay;
    if (relay?.attach === undefined) return undefined;
    // Floored like the local join, but clamped only below: the producer's length is not known here.
    const at = Number.isFinite(from) ? Math.max(Math.floor(from), 0) : 0;
    let source: AsyncIterable<StreamFrame> | undefined;
    try {
      source = await this.boundedAttach(relay, streamId, at);
    } catch (err) {
      this.reportRelayError(err, { streamId, phase: "attach" });
      return undefined;
    }
    if (source === undefined) return undefined;
    return this.relaySubscription(streamId, at, source);
  }

  /** `attach`, bounded by {@link RindleStreamOptions.relayAttachTimeoutMs}: a hung adapter yields
   *  `absent`, not a hung HTTP request. A source that resolves after the deadline is closed, not
   *  leaked. */
  private boundedAttach(
    relay: StreamRelay,
    streamId: string,
    from: number,
  ): Promise<AsyncIterable<StreamFrame> | undefined> {
    // `async` wrapping so a synchronously-throwing adapter is an attach failure, not a plane throw.
    const attempt = (async () => relay.attach!(streamId, from))();
    return new Promise((resolve, reject) => {
      let late = false;
      const t = timer(() => {
        late = true;
        reject(new Error(`stream ${streamId}: relay.attach timed out after ${this.relayAttachTimeoutMs}ms`));
      }, this.relayAttachTimeoutMs);
      attempt.then(
        (source) => {
          clearTimeout(t);
          if (!late) return resolve(source);
          closeFrameSource(source); // too late to serve the reader; don't leak the channel
        },
        (err) => {
          clearTimeout(t);
          if (!late) reject(err);
        },
      );
    });
  }

  /** Wrap an adapter's frame source as a plane subscription: conform every frame (LM-STREAM-RELAY
   *  §4), bound the reader with the same queue cap as a local one (§7), and tear the adapter down
   *  when either side lets go. The driver never throws into the plane: adapter failures become one
   *  `stale`. */
  private relaySubscription(streamId: string, from: number, source: AsyncIterable<StreamFrame>): StreamSubscription {
    const conform = new StreamRelayConform(streamId, from, (reason) =>
      this.reportRelayError(new Error(reason), { streamId, phase: "frames" }),
    );
    let closed = false;
    let signalClose!: () => void;
    const closedP = new Promise<void>((resolve) => (signalClose = resolve));
    const closedTag = closedP.then(() => "closed" as const);
    const release = (): void => {
      if (!closed) {
        closed = true;
        signalClose();
      }
    };
    const sub = new Subscriber(this.maxQueuedFrames, (s) => {
      this.relayed.delete(s);
      release();
    });
    this.relayed.add(sub);
    let it: AsyncIterator<StreamFrame> | undefined;
    /** @returns false once the subscription finished (terminal delivered, or the reader dropped). */
    const deliver = (frames: StreamFrame[]): boolean => {
      for (const frame of frames) {
        if (frame.type === "end" || frame.type === "stale" || frame.type === "absent") {
          sub.finish(frame);
          return false;
        }
        if (!sub.offer(frame)) {
          // The same bound as a local reader: a relayed subscriber that stops draining costs
          // itself the live leg, never unbounded memory.
          const [stale] = conform.fail();
          if (stale) sub.finish(stale);
          return false;
        }
      }
      return true;
    };
    void (async () => {
      try {
        // Iterator construction is adapter code too: keep a throwing factory inside the same
        // stale/report/cleanup boundary as a throwing `next()`.
        it = source[Symbol.asyncIterator]();
        for (;;) {
          // Raced rather than awaited bare: a reader disconnect must release this driver even when
          // the adapter never yields another frame (its own `return()` may be queued behind the
          // pending `next()` forever).
          const res = await Promise.race([it.next(), closedTag]);
          if (res === "closed") return;
          if (res.done) {
            deliver(conform.end());
            return;
          }
          if (!deliver(conform.feed(res.value))) return;
        }
      } catch (err) {
        this.reportRelayError(err, { streamId, phase: "frames" });
        deliver(conform.fail());
      } finally {
        release();
        // If construction itself threw there is no iterator to return, and asking the source to
        // construct a second one during cleanup could repeat side effects or throw again.
        closeFrameSource(undefined, it);
      }
    })();
    return { streamId, frames: sub.frames(), close: () => sub.close() };
  }

  /** Mirror one producer frame outward (LM-STREAM-RELAY §3). Never blocks or breaks the producer:
   *  a throw or rejected promise is reported and swallowed — a relay outage may cost relayed
   *  readers the live leg, never the generation or its checkpoints. */
  publishRelay(streamId: string, frame: StreamFrame): void {
    const relay = this.opts.relay;
    if (relay?.publish === undefined) return;
    try {
      const published = relay.publish(streamId, frame);
      if (published !== undefined) {
        void Promise.resolve(published).catch((err) =>
          this.reportRelayError(err, { streamId, phase: "publish" }),
        );
      }
    } catch (err) {
      this.reportRelayError(err, { streamId, phase: "publish" });
    }
  }

  reportRelayError(err: unknown, info: StreamRelayErrorInfo): void {
    // Same discipline as reportCheckpointError: a diagnostic must never take its caller down.
    try {
      if (this.opts.onRelayError) this.opts.onRelayError(err, info);
      else console.error(`[rindle api-server] stream ${info.streamId}: relay ${info.phase} failed:`, err);
    } catch (hookErr) {
      console.error(`[rindle api-server] stream ${info.streamId}: onRelayError itself threw:`, hookErr);
    }
  }

  /** Seal every live stream `interrupted`. In mapped-table mode the seal IS the compaction, so a
   *  graceful drain loses nothing PRODUCED; the status still says the response was cut short rather
   *  than claiming completion (§5). Wire it to SIGTERM. */
  async drainStreams(): Promise<void> {
    await Promise.allSettled([...this.live.values()].map((s) => s.seal("interrupted")));
  }

  /** Teardown: drop readers and timers WITHOUT a durable write (that is `drainStreams`). */
  closeSync(): void {
    for (const s of this.live.values()) s.detachAll();
    this.live.clear();
    for (const s of [...this.relayed]) s.close();
    this.relayed.clear();
  }

  /** A sealed stream stays joinable for the linger window, so a subscribe that races the last token
   *  still gets `end` (and the tail it missed) rather than a bare `absent`. */
  retire(streamId: string): void {
    timer(() => {
      const entry = this.live.get(streamId);
      if (entry?.ended) {
        this.live.delete(streamId);
        entry.detachAll();
      }
    }, this.lingerMs);
  }

  reportCheckpointError(err: unknown, info: { streamId: string; from: number; seq: number }): void {
    // Reporting runs INSIDE the drain loop, so a throwing hook would take the loop down with it and
    // wedge the stream — the one failure mode a diagnostic must never cause.
    try {
      if (this.opts.onCheckpointError) this.opts.onCheckpointError(err, info);
      else console.error(`[rindle api-server] stream ${info.streamId}: checkpoint ${info.from}→${info.seq} failed:`, err);
    } catch (hookErr) {
      console.error(`[rindle api-server] stream ${info.streamId}: onCheckpointError itself threw:`, hookErr);
    }
  }

  /** Drive ONE checkpoint, retrying on failure. Every statement it emits is idempotent under replay
   *  — the chunk insert dedups on its deterministic id, the length CAS refuses to apply twice, the
   *  compaction is a whole-row overwrite — so "retry until it sticks" needs no dedup ledger and no
   *  `lmid` (§3.3). */
  async commit(input: StreamCommitInput): Promise<{ cancelRequested?: boolean; seq?: number } | void> {
    let lastErr: unknown;
    for (let attempt = 0; attempt <= this.retries; attempt++) {
      if (attempt > 0) await delay(RETRY_BACKOFF_MS * 2 ** (attempt - 1));
      try {
        return await this.commitOnce(input);
      } catch (err) {
        if (err instanceof StreamOpenRefused) throw err; // a settled verdict, not a blip
        lastErr = err;
      }
    }
    throw lastErr;
  }

  private async commitOnce(input: StreamCommitInput): Promise<{ cancelRequested?: boolean; seq?: number } | void> {
    const target = this.opts.checkpoint;
    if ("commit" in target) return (await target.commit(input)) ?? undefined;
    const sql = this.sink!.sql;
    const mapped = this.mapped!;
    switch (input.kind) {
      case "open": {
        // The PRECONDITION is checked once, in `assertOpenable`, before this write — never here.
        // Re-reading it per attempt would make a lost ack on the write below refuse its own success
        // (the row would already say `streaming`).
        await sql.batch([mapped.markStreaming(input.streamId, this.openToken)]);
        await this.verifyOpenWinner(input.streamId);
        return undefined;
      }
      case "append": {
        try {
          await sql.batch(mapped.append(input.streamId, input.from, input.seq, input.text));
        } catch (err) {
          // The batch may have COMMITTED with its ack lost. The read-back is the truth: a row at
          // (or past) this slice's end means it landed, and the throw was only the reply.
          const truth = await this.readAppendState(input.streamId).catch(() => undefined);
          if (truth === undefined || truth.seq < input.seq) throw err;
          return truth;
        }
        // The read-back CONFIRMS the append (the guarded statements report no row count). If it
        // throws, the retry is safe: a replay of an applied slice no-ops on the guard and confirms
        // on its own read-back.
        return await this.readAppendState(input.streamId);
      }
      case "close": {
        await sql.batch(mapped.compact(input.streamId, input.body, input.status, input.error));
        return undefined;
      }
    }
  }

  /** The read-back half of the open CAS, run only when a `host` column is mapped. The probe in
   *  `assertOpenable` and the open write are separate round trips, so bare check-then-act leaves a
   *  window where two simultaneous kicks both pass the probe; the conditional `markStreaming`
   *  matches nothing when a rival got there first, and whichever token the row now holds names the
   *  winner — a true CAS with no row counts needed. A replayed open (lost ack) reads back its OWN
   *  token and proceeds. */
  private async verifyOpenWinner(streamId: string): Promise<void> {
    const probe = this.mapped!.probeHost();
    if (probe === undefined) return;
    const rows = await this.sink!.sql.query<{ host: unknown }>(probe, [streamId]);
    if (rows[0]?.host !== this.openToken) {
      throw new StreamOpenRefused(
        `stream ${streamId}: another producer won the open race (the row's host is ` +
          `${JSON.stringify(rows[0]?.host ?? null)}) — two producers on one stream interleave their checkpoints, ` +
          `so the loser stands down`,
      );
    }
  }

  /** The post-append read-back (§3.2): the row's authoritative length — which both CONFIRMS an
   *  append (the guarded batch reports no row count) and absorbs a committed-but-ack-lost one —
   *  plus the reader's cancel flag when mapped (§6), riding the same indexed point read. This read
   *  is LOAD-BEARING: a failure here fails the append attempt (retried, then stalled), because
   *  claiming durability the store did not confirm is the one dishonesty this plane refuses. */
  private async readAppendState(streamId: string): Promise<{ seq: number; cancelRequested?: boolean }> {
    const rows = await this.sink!.sql.query<{ seq: unknown; cancel?: unknown }>(this.mapped!.probeState(), [
      streamId,
    ]);
    const row = rows[0];
    if (row === undefined || typeof row.seq !== "number") {
      throw new Error(`stream ${streamId}: the read-back found no usable message row`);
    }
    return { seq: row.seq, ...(this.mapped!.hasCancel ? { cancelRequested: Boolean(row.cancel) } : {}) };
  }

  /** The store's word on how much is durable — for resynchronizing after a FAILED append, where a
   *  lost ack may have left the store ahead of the plane. `undefined` in `commit` mode (no readable
   *  authority) or when the read itself fails (the next attempt retries the resync too). */
  async probeDurableSeq(streamId: string): Promise<number | undefined> {
    if (!this.mapped) return undefined;
    try {
      return (await this.readAppendState(streamId)).seq;
    } catch {
      return undefined;
    }
  }
}

/** The open probe's verdict: the message row is missing or already advanced. Not retried — it is a
 *  settled statement about the app's data, and retrying re-asks a question already answered. */
export class StreamOpenRefused extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StreamOpenRefused";
  }
}

/** Refused by {@link RindleStreamOptions.authorize}. Distinct from the api-server's own
 *  `RindleApiError` so this module stays importable without the server (no cycle); the server
 *  translates it to a 403 at the handler seam. */
export class StreamForbidden extends Error {
  readonly streamId: string;

  constructor(streamId: string) {
    super(`stream ${streamId}: forbidden`);
    this.name = "StreamForbidden";
    this.streamId = streamId;
  }
}

/** Best-effort adapter teardown (the `for await` discipline, by hand): never awaited into the
 *  plane — a hung `return()` must not hold anything — and a synchronously-throwing one is the
 *  adapter's bug, not the plane's problem. */
function closeFrameSource(source: AsyncIterable<StreamFrame> | undefined, it?: AsyncIterator<StreamFrame>): void {
  try {
    const iter = it ?? source?.[Symbol.asyncIterator]();
    void Promise.resolve(iter?.return?.()).catch(() => {});
  } catch {
    // ignored — see above
  }
}

function oneFrame(streamId: string, frame: StreamFrame): StreamSubscription {
  let taken = false;
  return {
    streamId,
    frames: {
      [Symbol.asyncIterator](): AsyncIterator<StreamFrame> {
        return {
          next(): Promise<IteratorResult<StreamFrame>> {
            if (taken) return Promise.resolve({ value: undefined as never, done: true });
            taken = true;
            return Promise.resolve({ value: frame, done: false });
          },
        };
      },
    },
    close: () => {
      taken = true;
    },
  };
}

// ------------------------------------------------------------------------------- SSE transport

/** Headers for the SSE response. `x-accel-buffering` is the nginx-family opt-out — without it a
 *  buffering proxy holds the tokens and hands the user a paragraph at a time. */
export const STREAM_SSE_HEADERS: Record<string, string> = {
  "content-type": "text/event-stream; charset=utf-8",
  "cache-control": "no-cache, no-transform",
  connection: "keep-alive",
  "x-accel-buffering": "no",
};

/** Pull a subscribe request out of a fetch-style GET: `?streamId=…&from=…`, with `Last-Event-ID`
 *  winning over an explicit `from` (a reconnecting `EventSource` knows better than its own URL —
 *  the URL is the ORIGINAL join point, the header is where it actually got to). */
export function streamRequestFromHttp(req: {
  url: string;
  headers: { get(name: string): string | null };
}): { streamId: string; from: number } {
  const url = new URL(req.url);
  const streamId = url.searchParams.get("streamId") ?? "";
  const lastEventId = req.headers.get("last-event-id");
  const raw = lastEventId ?? url.searchParams.get("from") ?? "0";
  const from = Number.parseInt(raw, 10);
  return { streamId, from: Number.isFinite(from) && from > 0 ? from : 0 };
}

/**
 * Encode a subscription as an SSE body. Each positional frame carries `id: <seq>`, so a browser
 * `EventSource` that drops the connection resumes at exactly the right offset with no application
 * code — its own `Last-Event-ID` header is the `from` of the next subscribe ({@link
 * streamRequestFromHttp}).
 *
 * The reader must close the `EventSource` on the `end` frame: `EventSource` reconnects on ANY close,
 * including a clean one.
 */
export function streamFramesToSse(
  sub: StreamSubscription,
  opts?: { keepAliveMs?: number },
): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  const keepAliveMs = opts?.keepAliveMs ?? 15_000;
  let ping: ReturnType<typeof setTimeout> | null = null;
  let cancelled = false;
  let iter: AsyncIterator<StreamFrame>;
  // PULL-based on purpose: a frame is taken from the subscription only when the consumer has
  // demand, so a slow HTTP client backs frames up in the SUBSCRIBER's bounded queue — where
  // `maxQueuedFrames` drops it with `stale` (§4) — instead of unboundedly in this stream's own.
  return new ReadableStream<Uint8Array>({
    start(controller) {
      iter = sub.frames[Symbol.asyncIterator]();
      const beat = (): void => {
        if (cancelled) return;
        // A comment line: keeps intermediaries from reaping an idle connection, costs 8 bytes.
        controller.enqueue(encoder.encode(": ping\n\n"));
        ping = timer(beat, keepAliveMs);
      };
      ping = timer(beat, keepAliveMs);
    },
    async pull(controller) {
      const next = await iter.next();
      if (cancelled) return;
      if (next.done) {
        if (ping) clearTimeout(ping);
        sub.close();
        controller.close();
        return;
      }
      const id = frameResumePoint(next.value);
      const head = id === undefined ? "" : `id: ${id}\n`;
      controller.enqueue(encoder.encode(`${head}data: ${JSON.stringify(next.value)}\n\n`));
    },
    cancel() {
      cancelled = true;
      if (ping) clearTimeout(ping);
      sub.close();
    },
  });
}
