// Where the live change stream comes from. Two interchangeable sources behind one `Source` seam:
//
//   • wikimedia  — the REAL Wikimedia `recentchange` firehose (public SSE, no auth): every edit to
//                  the wikis, filtered here to human edits of English Wikipedia articles. A genuine
//                  CDC stream: each SSE `id` is a resumable Kafka offset, so a restart resumes from
//                  exactly where it left off — no gap, no re-seed. Needs egress to
//                  stream.wikimedia.org from the ingester.
//   • synthetic  — a deterministic, offline edit stream over a fixed set of fake pages. The
//                  bulletproof spine for tests / no-egress dev. (NOT themed as any real site.)
//
// A source never touches the engine. It just EMITS `WikiEvent`s (a page-upsert + the edit that
// caused it); the tier owns this source cursor and turns each into a `page` upsert + `edit` insert.

import { DaemonHttpError } from "@rindle/daemon-client";

import type { WikiEvent } from "./schema.ts";

export type Emit = (events: WikiEvent[]) => Promise<void>;

export interface StartOptions {
  /** Resume the stream from a previously-persisted SSE offset (restart-resume, gap-free). Takes
   *  precedence over `backfillSinceMs` — a known cursor always wins over a time-based cold start. */
  sinceId?: string;
  /** Cold-start backfill: with no `sinceId`, open the stream this many ms in the past (the
   *  EventStreams `?since=` cursor) so the retention window is seeded at once instead of filling
   *  over wall-clock time, then transitions to the live tail. Bounded by the stream's retention
   *  (~7d); ignored once an accepted event batch has been applied. */
  backfillSinceMs?: number;
  /** Reports the latest consumed-and-applied offset so the caller can persist it (debounced by
   *  the source's own flush cadence). */
  onOffset?: (id: string) => void;
  /** Called exactly once when a non-retryable error or the finite retry budget stops ingestion.
   *  Production uses this to begin a controlled nonzero shutdown rather than serving stale data. */
  onFatal?: (error: Error) => void;
}

export interface Source {
  readonly name: string;
  /** Begin the live stream of changes. The returned stop fn stops production, drains every event
   *  already accepted by the source, and rejects if that drain cannot be completed. */
  start(emit: Emit, opts?: StartOptions): () => Promise<void>;
}

/** One wiki event expands to a page upsert, an edit insert, and (when named) an editor upsert. Keep
 *  a transaction comfortably below HCTree's 8,192-change replication ceiling even in that
 *  three-statement worst case. */
export const MAX_SQL_STATEMENTS_PER_EVENT = 3;
export const MAX_EVENTS_PER_TRANSACTION = 1_000;
export const MAX_SQL_STATEMENTS_PER_TRANSACTION = MAX_EVENTS_PER_TRANSACTION * MAX_SQL_STATEMENTS_PER_EVENT;

// One transaction may be in flight while one more waits behind it. `enqueue` blocks at this bound,
// which propagates backpressure through the SSE reader instead of retaining an unbounded number of
// request bodies while the master is slower than a cold-start replay.
const MAX_BUFFERED_EVENTS = MAX_EVENTS_PER_TRANSACTION * 2;
const MAX_EMIT_ATTEMPTS = 8;
const INITIAL_EMIT_RETRY_MS = 250;
const MAX_EMIT_RETRY_MS = 10_000;

const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(done, ms);
    signal?.addEventListener("abort", done, { once: true });
    function done() {
      clearTimeout(timer);
      signal?.removeEventListener("abort", done);
      resolve();
    }
  });
const ri = (a: number, b: number) => Math.floor(a + Math.random() * (b - a + 1));
const pick = <T>(xs: readonly T[]): T => xs[ri(0, xs.length - 1)];

interface QueuedEvent {
  event: WikiEvent;
  /** The source cursor that becomes durable when this event's transaction commits. */
  offset?: string;
}

interface OrderedEmitterOptions {
  readonly sourceName: string;
  readonly emit: Emit;
  readonly flushMs: number;
  readonly maxEventsPerTransaction: number;
  readonly maxBufferedEvents: number;
  readonly initialOffset?: string;
  readonly onOffset?: (id: string) => void;
  readonly maxEmitAttempts: number;
  readonly initialRetryMs: number;
  readonly maxRetryMs: number;
  readonly onFatal: (error: Error) => void;
}

/** A small, bounded single-consumer queue. A batch stays at the head until `emit` succeeds, so an
 *  error cannot silently discard it. The tier makes event application idempotent, allowing an
 *  ambiguous request to retry serially. A finite exponential-backoff budget keeps transient master
 *  restarts survivable without letting a permanently broken source sit healthy-looking forever. */
class OrderedEmitter {
  private readonly options: OrderedEmitterOptions;
  private readonly queue: QueuedEvent[] = [];
  private readonly capacityWaiters: { resolve: () => void; reject: (error: Error) => void }[] = [];
  private flushTimer: ReturnType<typeof setTimeout> | undefined;
  private drainPromise: Promise<void> | undefined;
  private failure: Error | undefined;
  private accepting = true;
  private appliedOffset: string | undefined;

  constructor(options: OrderedEmitterOptions) {
    this.options = options;
    this.appliedOffset = options.initialOffset;
  }

  get lastAppliedOffset(): string | undefined {
    return this.appliedOffset;
  }

  get fatalError(): Error | undefined {
    return this.failure;
  }

  async enqueue(event: WikiEvent, offset?: string): Promise<void> {
    while (this.queue.length >= this.options.maxBufferedEvents) {
      this.throwIfFailed();
      if (!this.accepting) throw new Error(`[${this.options.sourceName}] source is stopping`);
      await new Promise<void>((resolve, reject) => this.capacityWaiters.push({ resolve, reject }));
    }
    this.throwIfFailed();
    if (!this.accepting) throw new Error(`[${this.options.sourceName}] source is stopping`);
    this.queue.push({ event, offset });
    if (this.queue.length >= this.options.maxEventsPerTransaction) this.startDrain();
    else this.scheduleFlush();
  }

  /** Flush all events accepted at the time this method is called (plus any appended while the one
   *  serial drain is in flight). */
  async flush(): Promise<void> {
    this.clearFlushTimer();
    if (this.queue.length && !this.failure) await this.startDrain();
    else if (this.drainPromise) await this.drainPromise;
    this.throwIfFailed();
  }

  async close(): Promise<void> {
    this.accepting = false;
    this.clearFlushTimer();
    await this.flush();
  }

  fail(error: unknown): void {
    this.recordFailure(error);
  }

  private scheduleFlush(): void {
    if (this.flushTimer || this.drainPromise || this.failure || !this.queue.length) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = undefined;
      this.startDrain();
    }, this.options.flushMs);
  }

  /** Starts at most one drain. The stored promise always settles normally; explicit `flush`/`close`
   *  calls surface the recorded terminal error after awaiting it, while timer-driven drains cannot
   *  become unhandled promise rejections. */
  private startDrain(): Promise<void> {
    if (this.drainPromise) return this.drainPromise;
    if (this.failure || !this.queue.length) return Promise.resolve();
    this.clearFlushTimer();
    const drain = this.drainLoop()
      .catch((error: unknown) => this.recordFailure(error))
      .finally(() => {
        if (this.drainPromise === drain) this.drainPromise = undefined;
        if (!this.failure && this.queue.length) this.scheduleFlush();
      });
    this.drainPromise = drain;
    return drain;
  }

  private async drainLoop(): Promise<void> {
    while (this.queue.length) {
      const count = Math.min(this.queue.length, this.options.maxEventsPerTransaction);
      const batch = this.queue.slice(0, count);
      // The head is intentionally removed only after the awaited transaction succeeds.
      await this.emitWithRetry(batch.map(({ event }) => event));
      this.queue.splice(0, count);
      this.releaseCapacityWaiters();

      // Offsets follow source order because no second emit can start until this one resolves.
      // Update our applied cursor before invoking user code: the DB transaction is committed at
      // this point and must never be re-emitted even if persistence of its cursor fails.
      const offset = [...batch].reverse().find(({ offset: id }) => id !== undefined)?.offset;
      if (offset !== undefined && offset !== this.appliedOffset) {
        this.appliedOffset = offset;
        this.options.onOffset?.(offset);
      }
    }
  }

  private async emitWithRetry(batch: WikiEvent[]): Promise<void> {
    let retryMs = this.options.initialRetryMs;
    for (let attempt = 1; ; attempt++) {
      try {
        await this.options.emit(batch);
        return;
      } catch (error) {
        const retryable = isRetryableEmitError(error);
        if (!retryable) {
          const detail = error instanceof Error ? error.message : String(error);
          throw new Error(`non-retryable emit failure: ${detail}`, { cause: error });
        }
        if (attempt >= this.options.maxEmitAttempts) {
          const detail = error instanceof Error ? error.message : String(error);
          throw new Error(`emit retry budget exhausted after ${attempt} attempts: ${detail}`, { cause: error });
        }
        const detail = error instanceof Error ? error.message : String(error);
        console.error(
          `[${this.options.sourceName}] emit attempt ${attempt}/${this.options.maxEmitAttempts} failed; ` +
            `retrying the same ${batch.length}-event batch in ${retryMs}ms: ${detail}`,
        );
        await sleep(retryMs);
        retryMs = Math.min(Math.max(1, retryMs * 2), this.options.maxRetryMs);
      }
    }
  }

  private recordFailure(cause: unknown): void {
    if (this.failure) return;
    const detail = cause instanceof Error ? cause.message : String(cause);
    this.failure = new Error(
      `[${this.options.sourceName}] emit failed; source stopped with ${this.queue.length} uncommitted event(s): ${detail}`,
      { cause },
    );
    this.accepting = false;
    this.clearFlushTimer();
    for (const waiter of this.capacityWaiters.splice(0)) waiter.reject(this.failure);
    console.error(this.failure.message);
    try {
      this.options.onFatal(this.failure);
    } catch (callbackError) {
      const detail = callbackError instanceof Error ? callbackError.message : String(callbackError);
      console.error(`[${this.options.sourceName}] onFatal callback failed: ${detail}`);
    }
  }

  private releaseCapacityWaiters(): void {
    for (const waiter of this.capacityWaiters.splice(0)) waiter.resolve();
  }

  private clearFlushTimer(): void {
    if (this.flushTimer) clearTimeout(this.flushTimer);
    this.flushTimer = undefined;
  }

  private throwIfFailed(): void {
    if (this.failure) throw this.failure;
  }
}

/** Generic/network errors are ambiguous and retryable. A daemon 4xx proves the request itself is
 *  invalid, except for the conventional transient/coordination statuses below. */
function isRetryableEmitError(error: unknown): boolean {
  if (!(error instanceof DaemonHttpError)) return true;
  return error.status === 408 || error.status === 409 || error.status === 425 || error.status === 429 || error.status >= 500;
}

// ── wikimedia: the real recentchange firehose ────────────────────────────────
const WIKIMEDIA_SSE = "https://stream.wikimedia.org/v2/stream/recentchange";
// Only keep human edits/creations to this wiki's main (article) namespace — a clean, legible
// board. Widen by relaxing these (drop the bot/namespace gates, or allow more domains).
const KEEP_DOMAIN = process.env.WIKI_DOMAIN ?? "en.wikipedia.org";
const FLUSH_MS = 250; // batch the trickle into one txn per quarter-second

interface RecentChange {
  meta?: { domain?: string; dt?: string };
  id?: number;
  type?: string; // edit | new | log | categorize | external
  namespace?: number;
  title?: string;
  title_url?: string;
  comment?: string;
  timestamp?: number;
  user?: string;
  bot?: boolean;
  server_name?: string;
  wiki?: string;
  length?: { old?: number; new?: number };
  revision?: { old?: number; new?: number };
}

/** Edit summaries are wikitext-ish (`/* section *​/` markers, links); flatten to a short snippet. */
function cleanComment(s: string | undefined): string {
  if (!s) return "";
  return s
    .replace(/\/\*.*?\*\//g, "") // section markers
    .replace(/\[\[([^\]|]*\|)?([^\]]*)\]\]/g, "$2") // [[Target|Label]] → Label
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 200);
}

function toEvent(rc: RecentChange): WikiEvent | null {
  if (rc.server_name !== KEEP_DOMAIN) return null;
  if (rc.namespace !== 0) return null; // articles only
  if (rc.type !== "edit" && rc.type !== "new") return null;
  if (rc.bot) return null; // human edits only
  if (!rc.title || typeof rc.timestamp !== "number") return null;
  const wiki = rc.wiki ?? "enwiki";
  const pageId = `${wiki}:${rc.title}`;
  const revId = String(rc.revision?.new ?? rc.id ?? `${pageId}:${rc.timestamp}`);
  return {
    page: { id: pageId, wiki, title: rc.title, url: rc.title_url ?? "", ts: rc.timestamp, user: rc.user ?? "" },
    edit: {
      id: revId,
      user: rc.user ?? "",
      comment: cleanComment(rc.comment),
      ts: rc.timestamp,
      delta: (rc.length?.new ?? 0) - (rc.length?.old ?? 0),
      bot: rc.bot ? 1 : 0,
    },
  };
}

/** Parse an SSE byte stream into (data, id) events. `id` persists across events per the SSE spec
 *  until a new `id:` line resets it. */
async function consumeSse(
  body: ReadableStream<Uint8Array>,
  onEvent: (data: string, id: string | undefined) => void | Promise<void>,
): Promise<void> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  let data = "";
  let id: string | undefined;
  let sawData = false;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) return;
    buf += decoder.decode(value, { stream: true });
    let nl: number;
    while ((nl = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, nl).replace(/\r$/, "");
      buf = buf.slice(nl + 1);
      if (line === "") {
        // Awaiting here is intentional: once the bounded ingest queue is full, backpressure reaches
        // the network reader instead of letting a backfill accumulate in process memory.
        if (sawData) await onEvent(data, id);
        data = "";
        sawData = false;
      } else if (line.startsWith("data:")) {
        data += line.slice(5).replace(/^ /, "");
        sawData = true;
      } else if (line.startsWith("id:")) {
        id = line.slice(3).replace(/^ /, "");
      }
      // ":" comments and "event:" lines are ignored
    }
  }
}

export interface WikimediaSourceConfig {
  /** Test seams; production callers use the defaults. Transaction size is always clamped to the
   *  exported HCTree-safe production ceiling. */
  fetch?: typeof fetch;
  endpoint?: string;
  flushMs?: number;
  maxEventsPerTransaction?: number;
  maxBufferedEvents?: number;
  initialReconnectBackoffMs?: number;
  maxReconnectBackoffMs?: number;
  maxEmitAttempts?: number;
  initialEmitRetryMs?: number;
  maxEmitRetryMs?: number;
}

export function createWikimediaSource(config: WikimediaSourceConfig = {}): Source {
  const fetchImpl = config.fetch ?? fetch;
  const endpoint = config.endpoint ?? WIKIMEDIA_SSE;
  const flushMs = config.flushMs ?? FLUSH_MS;
  const maxEventsPerTransaction = Math.max(
    1,
    Math.min(config.maxEventsPerTransaction ?? MAX_EVENTS_PER_TRANSACTION, MAX_EVENTS_PER_TRANSACTION),
  );
  const maxBufferedEvents = Math.max(
    maxEventsPerTransaction,
    config.maxBufferedEvents ?? MAX_BUFFERED_EVENTS,
  );
  const initialReconnectBackoffMs = Math.max(0, config.initialReconnectBackoffMs ?? 500);
  const maxReconnectBackoffMs = Math.max(initialReconnectBackoffMs, config.maxReconnectBackoffMs ?? 15_000);
  const maxEmitAttempts = Math.max(1, Math.floor(config.maxEmitAttempts ?? MAX_EMIT_ATTEMPTS));
  const initialEmitRetryMs = Math.max(0, config.initialEmitRetryMs ?? INITIAL_EMIT_RETRY_MS);
  const maxEmitRetryMs = Math.max(initialEmitRetryMs, config.maxEmitRetryMs ?? MAX_EMIT_RETRY_MS);

  return {
    name: "wikimedia",
    start(emit, opts) {
      const controller = new AbortController();
      let closed = false;
      // Cold-start seed, used only while there is no applied resumable cursor.
      const backfillSinceMs = opts?.backfillSinceMs;
      const batches = new OrderedEmitter({
        sourceName: "wikimedia",
        emit,
        flushMs,
        maxEventsPerTransaction,
        maxBufferedEvents,
        initialOffset: opts?.sinceId,
        onOffset: opts?.onOffset,
        maxEmitAttempts,
        initialRetryMs: initialEmitRetryMs,
        maxRetryMs: maxEmitRetryMs,
        onFatal: (error) => {
          controller.abort();
          opts?.onFatal?.(error);
        },
      });

      const run = async () => {
        let backoff = initialReconnectBackoffMs;
        while (!closed) {
          let streamError: unknown;
          try {
            const headers: Record<string, string> = { accept: "text/event-stream" };
            // Reconnect only from a COMMITTED cursor. The parser may already have observed much
            // newer ids while a transaction is still in flight; using one of those would create a
            // permanent gap if the transaction failed.
            const appliedOffset = batches.lastAppliedOffset;
            let target = endpoint;
            if (appliedOffset) {
              headers["Last-Event-ID"] = appliedOffset;
            } else if (backfillSinceMs !== undefined) {
              target += `?since=${encodeURIComponent(new Date(backfillSinceMs).toISOString())}`;
            }
            const res = await fetchImpl(target, { headers, signal: controller.signal });
            if (!res.ok || !res.body) throw new Error(`SSE responded ${res.status}`);
            backoff = initialReconnectBackoffMs; // connected — reset backoff
            await consumeSse(res.body, async (data, id) => {
              let rc: RecentChange;
              try {
                rc = JSON.parse(data) as RecentChange;
              } catch {
                return;
              }
              const ev = toEvent(rc);
              if (ev) await batches.enqueue(ev, id);
            });
          } catch (err) {
            streamError = err;
          }

          // Commit everything parsed from this connection before reconnecting. This both preserves
          // source order across connections and ensures the next Last-Event-ID never jumps past a
          // queued event.
          try {
            await batches.flush();
          } catch {
            break; // the batcher logged and retained its terminal batch; `stop` surfaces the error
          }
          if (closed) break;
          if (streamError !== undefined) {
            const detail = streamError instanceof Error ? streamError.message : String(streamError);
            console.error(`[wikimedia] stream dropped, reconnecting in ${backoff}ms:`, detail);
          }
          await sleep(backoff, controller.signal);
          backoff = Math.min(Math.max(1, backoff * 2), maxReconnectBackoffMs);
        }
      };
      const runPromise = run();
      let stopPromise: Promise<void> | undefined;

      return () => {
        if (stopPromise) return stopPromise;
        stopPromise = (async () => {
          closed = true;
          controller.abort();
          // `consumeSse` awaits each enqueue, so once the reader exits no producer can append behind
          // this close. Draining after that boundary gives shutdown its no-loss guarantee.
          await runPromise;
          await batches.close();
        })();
        return stopPromise;
      };
    },
  };
}

// ── synthetic: deterministic offline edit stream (NOT themed as any real site) ───────────────
const FAKE_TITLES = [
  "Incremental view maintenance",
  "Change data capture",
  "Differential dataflow",
  "Materialized view",
  "Write-ahead logging",
  "B-tree",
  "Conflict-free replicated data type",
  "Database index",
  "Query optimization",
  "Log-structured merge-tree",
  "Vector clock",
  "Two-phase commit protocol",
  "Bloom filter",
  "Consistent hashing",
  "Raft (algorithm)",
  "Event sourcing",
];
const FAKE_USERS = ["AvaT", "Bjorn", "CitationNeeded", "DeltaWatch", "EditH", "Fenwick", "Greta", "Halvar"];
const FAKE_SUMMARIES = [
  "fix typo",
  "added a citation",
  "rewrote the lead section",
  "→ History: expanded",
  "reverted unexplained removal",
  "updated reference",
  "copyedit",
  "added See also",
];

export interface SyntheticSourceConfig {
  intervalMs?: number;
  maxEventsPerTransaction?: number;
  maxBufferedEvents?: number;
  maxEmitAttempts?: number;
  initialEmitRetryMs?: number;
  maxEmitRetryMs?: number;
}

export function createSyntheticSource(config: SyntheticSourceConfig = {}): Source {
  const intervalMs = Math.max(0, config.intervalMs ?? 500);
  const maxEventsPerTransaction = Math.max(
    1,
    Math.min(config.maxEventsPerTransaction ?? MAX_EVENTS_PER_TRANSACTION, MAX_EVENTS_PER_TRANSACTION),
  );
  const maxBufferedEvents = Math.max(
    maxEventsPerTransaction,
    config.maxBufferedEvents ?? MAX_BUFFERED_EVENTS,
  );
  const maxEmitAttempts = Math.max(1, Math.floor(config.maxEmitAttempts ?? MAX_EMIT_ATTEMPTS));
  const initialEmitRetryMs = Math.max(0, config.initialEmitRetryMs ?? INITIAL_EMIT_RETRY_MS);
  const maxEmitRetryMs = Math.max(initialEmitRetryMs, config.maxEmitRetryMs ?? MAX_EMIT_RETRY_MS);

  return {
    name: "synthetic",
    start(emit, opts) {
      let rev = 1;
      let closed = false;
      const controller = new AbortController();
      const batches = new OrderedEmitter({
        sourceName: "synthetic",
        emit,
        flushMs: intervalMs,
        maxEventsPerTransaction,
        maxBufferedEvents,
        maxEmitAttempts,
        initialRetryMs: initialEmitRetryMs,
        maxRetryMs: maxEmitRetryMs,
        onFatal: (error) => {
          controller.abort();
          opts?.onFatal?.(error);
        },
      });

      const run = async () => {
        while (!closed && !batches.fatalError) {
          await sleep(intervalMs, controller.signal);
          if (closed || batches.fatalError) break;
          const events: WikiEvent[] = [];
          const n = ri(1, 4);
          const now = Math.floor(Date.now() / 1000);
          for (let i = 0; i < n; i++) {
            const title = pick(FAKE_TITLES);
            const user = pick(FAKE_USERS);
            events.push({
              page: {
                id: `synthwiki:${title}`,
                wiki: "synthwiki",
                title,
                url: `https://example.org/wiki/${encodeURIComponent(title)}`,
                ts: now,
                user,
              },
              edit: {
                id: `r${rev++}`,
                user,
                comment: pick(FAKE_SUMMARIES),
                ts: now,
                delta: ri(-400, 700),
                bot: 0,
              },
            });
          }
          for (const event of events) await batches.enqueue(event);
          // Keep the old cadence (one generated group per transaction) while serializing groups:
          // the next interval does not begin until this transaction has resolved.
          await batches.flush();
        }
      };
      const runPromise = run().catch((error: unknown) => {
        // OrderedEmitter owns/logs transaction failures. This guard is only for an unexpected
        // producer bug, which still needs to stop generation instead of becoming an unhandled task.
        if (!batches.fatalError) batches.fail(error);
      });
      let stopPromise: Promise<void> | undefined;

      return () => {
        if (stopPromise) return stopPromise;
        stopPromise = (async () => {
          closed = true;
          controller.abort();
          await runPromise;
          await batches.close();
        })();
        return stopPromise;
      };
    },
  };
}
