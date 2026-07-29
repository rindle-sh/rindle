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

import type { WikiEvent } from "./schema.ts";

export type Emit = (events: WikiEvent[]) => Promise<void>;

export interface StartOptions {
  /** Resume the stream from a previously-persisted SSE offset (restart-resume, gap-free). Takes
   *  precedence over `backfillSinceMs` — a known cursor always wins over a time-based cold start. */
  sinceId?: string;
  /** Cold-start backfill: with no `sinceId`, open the stream this many ms in the past (the
   *  EventStreams `?since=` cursor) so the retention window is seeded at once instead of filling
   *  over wall-clock time, then transitions to the live tail. Bounded by the stream's retention
   *  (~7d); ignored once any event has been consumed. */
  backfillSinceMs?: number;
  /** Reports the latest consumed-and-applied offset so the caller can persist it (debounced by
   *  the source's own flush cadence). */
  onOffset?: (id: string) => void;
}

export interface Source {
  readonly name: string;
  /** Begin the live stream of changes. Returns a stop fn. */
  start(emit: Emit, opts?: StartOptions): () => void;
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const ri = (a: number, b: number) => Math.floor(a + Math.random() * (b - a + 1));
const pick = <T>(xs: readonly T[]): T => xs[ri(0, xs.length - 1)];

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
  onEvent: (data: string, id: string | undefined) => void,
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
        if (sawData) onEvent(data, id);
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

export function createWikimediaSource(): Source {
  return {
    name: "wikimedia",
    start(emit, opts) {
      const controller = new AbortController();
      let closed = false;
      let lastId = opts?.sinceId; // resume cursor (Last-Event-ID)
      const backfillSinceMs = opts?.backfillSinceMs; // cold-start seed (only used while lastId unset)
      let pending: WikiEvent[] = [];
      let pendingId: string | undefined = lastId;

      // Flush the batched trickle as ONE txn, then persist the offset of the applied batch (so a
      // restart resumes just after the last row we actually wrote — gap-free).
      const flush = async () => {
        if (!pending.length) return;
        const batch = pending;
        const id = pendingId;
        pending = [];
        try {
          await emit(batch);
          if (id) opts?.onOffset?.(id);
        } catch (err) {
          console.error("[wikimedia] emit failed:", (err as Error).message);
        }
      };
      const flushTimer = setInterval(() => void flush(), FLUSH_MS);

      const run = async () => {
        let backoff = 500;
        while (!closed) {
          try {
            const headers: Record<string, string> = { accept: "text/event-stream" };
            // A known cursor always wins: once any event has been consumed (or we restarted from a
            // persisted offset), reconnect from exactly there. Only a cold first connection with no
            // cursor backfills by time, replaying the retention window faster-than-realtime before
            // catching up to the live tail.
            let target = WIKIMEDIA_SSE;
            if (lastId) {
              headers["Last-Event-ID"] = lastId;
            } else if (backfillSinceMs !== undefined) {
              target += `?since=${encodeURIComponent(new Date(backfillSinceMs).toISOString())}`;
            }
            const res = await fetch(target, { headers, signal: controller.signal });
            if (!res.ok || !res.body) throw new Error(`SSE responded ${res.status}`);
            backoff = 500; // connected — reset backoff
            await consumeSse(res.body, (data, id) => {
              if (id) lastId = id;
              let rc: RecentChange;
              try {
                rc = JSON.parse(data) as RecentChange;
              } catch {
                return;
              }
              const ev = toEvent(rc);
              if (ev) {
                pending.push(ev);
                pendingId = lastId;
              }
            });
          } catch (err) {
            if (closed) break;
            console.error(`[wikimedia] stream dropped, reconnecting in ${backoff}ms:`, (err as Error).message);
          }
          if (closed) break;
          await sleep(backoff);
          backoff = Math.min(backoff * 2, 15_000);
        }
      };
      void run();

      return () => {
        closed = true;
        clearInterval(flushTimer);
        controller.abort();
        void flush();
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

export function createSyntheticSource(): Source {
  return {
    name: "synthetic",
    start(emit) {
      let rev = 1;
      const timer = setInterval(() => {
        const events: WikiEvent[] = [];
        const n = ri(1, 4);
        const now = Math.floor(Date.now() / 1000);
        for (let i = 0; i < n; i++) {
          const title = pick(FAKE_TITLES);
          const user = pick(FAKE_USERS);
          events.push({
            page: { id: `synthwiki:${title}`, wiki: "synthwiki", title, url: `https://example.org/wiki/${encodeURIComponent(title)}`, ts: now, user },
            edit: { id: `r${rev++}`, user, comment: pick(FAKE_SUMMARIES), ts: now, delta: ri(-400, 700), bot: 0 },
          });
        }
        void emit(events).catch((err) => console.error("[synthetic] emit failed:", (err as Error).message));
      }, 500);
      return () => clearInterval(timer);
    },
  };
}
