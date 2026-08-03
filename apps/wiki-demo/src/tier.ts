// The "tiny machine" Node tier that fronts `rindled`: the app API server (resolves the named
// `latest`/`recentEditors` queries to leases, and PINS them so they stay warm with zero viewers), the
// ordered ingester (turns each Wikimedia change into a `page` upsert + an `edit` insert
// through the daemon's control plane), a bounded-window prune, and a combined HTTP endpoint
// serving the lease route + the metrics badge.
//
// The pins are re-asserted whenever the daemon restarts: `rindled` keeps no durable
// materialization state, so the daemon-client's `onBootId` (which rides the ingester's constant
// writes) calls `assertPins()` again — no polling. Browsers are READ-ONLY: there is no mutate
// route; the only writer is this ingester.

import { createHash } from "node:crypto";
import { createServer } from "node:http";
import type { Server } from "node:http";
import { connect as netConnect } from "node:net";

import { createRindleApiServer, defineApiQueries } from "@rindle/api-server";
import type { RindleApiServer } from "@rindle/api-server";
import { HttpRindleDaemonClient } from "@rindle/daemon-client";
import type { SqlStatement, WireValue } from "@rindle/daemon-client";

import { startMachineStats } from "./machine-stats.ts";
import { topEdited, topRecentEditors } from "./schema.ts";
import type { WikiEvent } from "./schema.ts";

/** HCTree admits at most 8,192 captured row changes in one transaction. These chunk sizes leave
 * ample headroom: ingest changes at most three rows per event (3,000), while prune can delete one
 * edit, update/delete one page, and update/delete one editor per edit (5,000 worst case). */
export const INGEST_EVENTS_PER_TXN = 1_000;
export const PRUNE_EDITS_PER_TXN = 1_000;
const HCTREE_CAPTURED_CHANGES_PER_TXN = 8_192;

export const WIKI_INDEX_NAMES = [
  "ix_page__last_ts_desc_id",
  "ix_editor__last_ts_desc_name",
  "ix_edit__page_id_ts_desc",
  "ix_edit__user_ts_desc",
  "ix_edit__ts",
] as const;

/** The query and prune indexes are DDL owned by the master. `/migrate` journals them so every
 * follower applies the same desired-index state; declaring them only on one SQLite file would drift. */
export const WIKI_INDEX_MIGRATION = {
  id: "wiki-demo-0001-query-and-prune-indexes",
  statements: [
    "CREATE INDEX IF NOT EXISTS ix_page__last_ts_desc_id ON page(last_ts DESC, id)",
    "CREATE INDEX IF NOT EXISTS ix_editor__last_ts_desc_name ON editor(last_ts DESC, name)",
    "CREATE INDEX IF NOT EXISTS ix_edit__page_id_ts_desc ON edit(page_id, ts DESC)",
    "CREATE INDEX IF NOT EXISTS ix_edit__user_ts_desc ON edit(user, ts DESC)",
    "CREATE INDEX IF NOT EXISTS ix_edit__ts ON edit(ts)",
  ],
} as const;

const WIKI_INDEX_MIGRATION_CHECKSUM = createHash("sha256")
  .update(JSON.stringify(WIKI_INDEX_MIGRATION.statements))
  .digest("hex");

/** Upsert the page only when the immediately preceding edit insert changed one row. SQLite's
 * `changes()` then carries the fresh-edit bit through this statement to the editor upsert. Page
 * recency is monotone: EventStreams can deliver a late older edit, which must increase the count
 * without replacing the newer edit that still defines `last_ts` / `last_user`. */
function pageUpsertAfterFreshEdit(p: WikiEvent["page"]): SqlStatement {
  return {
    sql:
      "INSERT INTO page (id, wiki, title, url, edits, last_ts, last_user) " +
      "SELECT ?, ?, ?, ?, 1, ?, ? WHERE changes() = 1 " +
      "ON CONFLICT(id) DO UPDATE SET edits = page.edits + 1, " +
      "last_user = CASE WHEN excluded.last_ts >= page.last_ts THEN excluded.last_user ELSE page.last_user END, " +
      "last_ts = max(page.last_ts, excluded.last_ts), title = excluded.title, url = excluded.url",
    params: [p.id, p.wiki, p.title, p.url, p.ts, p.user],
  };
}

/** The page upsert changes one row iff the edit was fresh, so this second `changes()` gate preserves
 * the same bit. Anonymous/empty usernames are skipped by the caller. */
function editorUpsertAfterFreshEdit(e: WikiEvent["edit"]): SqlStatement {
  return {
    sql:
      "INSERT INTO editor (name, edits, last_ts) SELECT ?, 1, ? WHERE changes() = 1 " +
      "ON CONFLICT(name) DO UPDATE SET edits = edits + 1, last_ts = max(last_ts, excluded.last_ts)",
    params: [e.user, e.ts],
  };
}

/** Insert the edit row. `DO NOTHING` so a resume that re-delivers an already-applied edit is a
 *  silent no-op (the offset is persisted post-apply, so overlap is near-impossible anyway). */
function editInsert(pageId: string, e: WikiEvent["edit"]): SqlStatement {
  return {
    sql:
      "INSERT INTO edit (id, page_id, user, comment, ts, delta, bot) VALUES (?, ?, ?, ?, ?, ?, ?) " +
      "ON CONFLICT(id) DO NOTHING",
    params: [e.id, pageId, e.user, e.comment, e.ts, e.delta, e.bot],
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Apply the index migration at the write owner, then wait until this follower physically has all
 * five indexes before hydrating the pinned views. An index-only DDL run applies live (no bounce). */
async function ensureWikiIndexes(
  writeDaemon: HttpRindleDaemonClient,
  daemon: HttpRindleDaemonClient,
): Promise<void> {
  await writeDaemon.migrate({
    id: WIKI_INDEX_MIGRATION.id,
    checksum: WIKI_INDEX_MIGRATION_CHECKSUM,
    statements: [...WIKI_INDEX_MIGRATION.statements],
  });

  const placeholders = WIKI_INDEX_NAMES.map(() => "?").join(", ");
  const deadline = Date.now() + 30_000;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const out = await daemon.executeSqlRead({
        sql: `SELECT name FROM sqlite_master WHERE type = 'index' AND name IN (${placeholders}) ORDER BY name`,
        params: [...WIKI_INDEX_NAMES],
      });
      const present = new Set(out.rows.map((row) => row[0]).filter((name): name is string => typeof name === "string"));
      if (WIKI_INDEX_NAMES.every((name) => present.has(name))) return;
    } catch (error) {
      // A follower still applying genesis/DDL can briefly refuse the read; retry to the deadline.
      lastError = error;
    }
    await sleep(50);
  }
  throw new Error(
    `timed out waiting for wiki indexes on the follower${lastError ? `: ${String((lastError as Error).message ?? lastError)}` : ""}`,
  );
}

interface ExpiredEdit {
  id: string;
  pageId: string;
  user: string;
}

function editIdentity(row: readonly WireValue[], context: string): ExpiredEdit {
  const [id, pageId, user] = row;
  if (
    typeof id !== "string" ||
    typeof pageId !== "string" ||
    (user !== null && typeof user !== "string")
  ) {
    throw new Error(`invalid edit row while ${context}: ${JSON.stringify(row)}`);
  }
  return { id, pageId, user: user ?? "" };
}

function placeholders(count: number): string {
  return Array.from({ length: count }, () => "?").join(", ");
}

function decrementStatement(
  table: "page" | "editor",
  keyColumn: "id" | "name",
  decrements: ReadonlyMap<string, number>,
): SqlStatement {
  const values = [...decrements];
  return {
    // Drive the update FROM the small selected-key set. Unlike one correlated COUNT(*) over the
    // remaining edit table per parent and per chunk, this stays O(selected keys) even when all
    // expired rows belong to one extremely hot page/editor.
    sql:
      `WITH decrements(target_id, amount) AS (VALUES ${values.map(() => "(?, ?)").join(", ")}) ` +
      `UPDATE ${table} SET edits = ${table}.edits - decrements.amount FROM decrements ` +
      `WHERE ${table}.${keyColumn} = decrements.target_id`,
    params: values.flatMap(([id, amount]) => [id, amount]),
  };
}

/** Delete one selected edit set, then decrement each affected parent by the exact number of selected
 * ids it owned. Explicit ids make every statement operate on the same set without a temp-table race;
 * deriving the small grouped counts in Node avoids repeatedly scanning a hot parent's remaining rows. */
function pruneStatements(expired: readonly ExpiredEdit[]): {
  statements: SqlStatement[];
  capturedChangeUpperBound: number;
} {
  const editIds = expired.map((edit) => edit.id);
  const pageDecrements = new Map<string, number>();
  const editorDecrements = new Map<string, number>();
  for (const edit of expired) {
    pageDecrements.set(edit.pageId, (pageDecrements.get(edit.pageId) ?? 0) + 1);
    if (edit.user) {
      editorDecrements.set(edit.user, (editorDecrements.get(edit.user) ?? 0) + 1);
    }
  }
  const pageIds = [...pageDecrements.keys()];
  const users = [...editorDecrements.keys()];
  const statements: SqlStatement[] = [
    {
      sql: `DELETE FROM edit WHERE id IN (${placeholders(editIds.length)})`,
      params: editIds,
    },
    decrementStatement("page", "id", pageDecrements),
  ];
  if (users.length) {
    statements.push(decrementStatement("editor", "name", editorDecrements));
  }
  statements.push({
    sql: `DELETE FROM page WHERE edits <= 0 AND id IN (${placeholders(pageIds.length)})`,
    params: pageIds,
  });
  if (users.length) {
    statements.push({
      sql: `DELETE FROM editor WHERE edits <= 0 AND name IN (${placeholders(users.length)})`,
      params: users,
    });
  }
  return {
    statements,
    capturedChangeUpperBound: editIds.length + 2 * pageIds.length + 2 * users.length,
  };
}

export interface TinyMachineOptions {
  /** The FOLLOWER's private control-plane base url — the READ leg (design 214): named-query leases,
   *  pins, the `/stats` badge. */
  daemonUrl: string;
  /** The FOLLOWER's public subscription ws url (handed to the metrics badge for the browser). */
  daemonWsUrl: string;
  daemonToken?: string;
  /** The MASTER's write-ingress base url — the WRITE leg (design 214): the ingester + prune land
   *  their `executeSqlTxn` here. The follower has no write plane. */
  masterUrl: string;
  /** The master's write-ingress bearer (when it is token-gated). */
  masterToken?: string;
  /** Port for the combined API + metrics HTTP server (0 = ephemeral). */
  apiPort?: number;
  /** A label for the metrics badge (`wikimedia` | `synthetic`). */
  sourceName: string;
  /** Spawned child pids for component-level CPU/RSS and an honest whole-app aggregate. */
  masterPid?: number;
  followerPid?: number;
}

export interface TinyMachine {
  readonly apiPort: number;
  /** Apply one ordered source batch (page upserts + edit inserts). */
  ingest(events: WikiEvent[]): Promise<void>;
  /** Prune edits older than `beforeTs` (unix seconds), decrement affected parent counts exactly,
   *  and drop parents that fall to zero — keeps the dataset and query window bounded. */
  prune(beforeTs: number): Promise<void>;
  /** Record the latest persisted source offset (shown on the badge as the CDC resume cursor). */
  noteOffset(id: string): void;
  close(): Promise<void>;
}

export async function startTinyMachine(opts: TinyMachineOptions): Promise<TinyMachine> {
  const authHeaders: Record<string, string> = opts.daemonToken
    ? { authorization: `Bearer ${opts.daemonToken}` }
    : {};

  // `api` is referenced by the daemon client's onBootId, but the api server needs the daemon —
  // a deliberate late binding (onBootId only fires once a response arrives, after construction).
  let api: RindleApiServer<string> | undefined;
  // The READ leg: the FOLLOWER's control plane. The api server resolves named queries to leases +
  // pins here, and the badge reads /stats here. onBootId fires on any follower response (the pins
  // themselves generate them), so a follower restart re-warms the headline pins.
  const daemon = new HttpRindleDaemonClient({
    baseUrl: opts.daemonUrl,
    headers: authHeaders,
    // The follower (re)started — re-assert the pins so the headline stays warm with zero viewers.
    onBootId: () => {
      void api?.assertPins().catch((err) => console.error("[wiki] re-pin after follower (re)start failed:", err));
    },
  });
  // The WRITE leg: the MASTER's write ingress. The ingester + the windowed prune land
  // their `executeSqlTxn` here (design 214 — the follower has no write plane); the effects replicate
  // to the follower and drive the live pipelines the browser sees.
  const writeDaemon = new HttpRindleDaemonClient({
    baseUrl: opts.masterUrl,
    headers: opts.masterToken ? { authorization: `Bearer ${opts.masterToken}` } : {},
  });

  // Index DDL belongs to the write owner and reaches this follower in log order. Wait for physical
  // coverage before pinning: otherwise a persisted, already-large database hydrates both boards via
  // the exact scans these indexes exist to prevent.
  await ensureWikiIndexes(writeDaemon, daemon);

  api = createRindleApiServer<string>({
    daemon,
    queries: defineApiQueries({
      latest: () => topEdited("latest"),
      recentEditors: () => topRecentEditors(),
    }),
    // Keep BOTH headline queries permanently materialized (warm with zero viewers): a new tab
    // attaches to already-maintained results, and many tabs SHARE the one pipeline each (dedup).
    // Two deeply-nested boards — newest pages and newest editors — kept exact off the single `edit`
    // write stream (one grouped by page, one by editor).
    pinnedQueries: [{ name: "latest" }, { name: "recentEditors" }],
  });

  // Warm the pins up front (don't wait for the first write to trigger onBootId).
  await api.assertPins();

  // Whole-machine plus per-component telemetry for the "tiny footprint" badge, and a live count of
  // concurrent browser viewers (one ws each through the proxy below).
  const machineStats = startMachineStats({
    masterPid: opts.masterPid,
    followerPid: opts.followerPid,
  });
  let viewers = 0;

  // --- the ordered source ingester ---------------------------------------------------------
  let edits = 0; // cumulative edits applied since boot (a throughput odometer, never decremented)
  let txns = 0;
  let lastOffset = "";
  // The LIVE working set: one entry per edit row currently in the retained window, carrying the
  // page + editor it touched. A committed prune chunk removes those exact edit ids and decrements
  // the per-key live counts below — an honest, self-bounded (O(window)) count of the rows the boards
  // run over right now even if the upstream delivers a slightly out-of-order timestamp.
  const editWindow = new Map<string, { pageId: string; user: string }>();
  // Distinct pages/editors with at least one edit STILL in the window — the bounded live set the
  // boards actually rank. Kept as ref-counts (live edits per key) so a key drops out exactly when
  // its last windowed edit is pruned, mirroring the SQL `DELETE FROM page/editor WHERE edits <= 0`.
  // (These replaced unbounded `Set`s that only ever grew — one entry per distinct key EVER seen,
  // never pruned — which leaked a few MB/hour off the continuous distinct-key churn even with zero
  // viewers, inflating this process's RSS on the footprint badge.)
  const pageEdits = new Map<string, number>();
  const editorEdits = new Map<string, number>();
  const bumpKey = (m: Map<string, number>, k: string) => m.set(k, (m.get(k) ?? 0) + 1);
  const dropKey = (m: Map<string, number>, k: string) => {
    const n = (m.get(k) ?? 0) - 1;
    if (n > 0) m.set(k, n);
    else m.delete(k);
  };

  // Ordinary restarts preserve the master database. Rebuild the bounded in-process telemetry from
  // its retained edit truth before accepting source work, paginating by the edit PK so a full window
  // cannot exceed the read endpoint's response-size limit. This also lets a reconnect's mixed
  // duplicate+fresh transaction identify the already-present ids exactly.
  let afterEditId: string | undefined;
  for (;;) {
    const out = await writeDaemon.executeSqlRead({
      sql:
        afterEditId === undefined
          ? "SELECT id, page_id, user FROM edit ORDER BY id LIMIT ?"
          : "SELECT id, page_id, user FROM edit WHERE id > ? ORDER BY id LIMIT ?",
      params:
        afterEditId === undefined
          ? [PRUNE_EDITS_PER_TXN]
          : [afterEditId, PRUNE_EDITS_PER_TXN],
      consistency: "strong",
    });
    for (const row of out.rows) {
      const edit = editIdentity(row as WireValue[], "hydrating wiki telemetry");
      editWindow.set(edit.id, { pageId: edit.pageId, user: edit.user });
      bumpKey(pageEdits, edit.pageId);
      if (edit.user) bumpKey(editorEdits, edit.user);
    }
    if (out.rows.length < PRUNE_EDITS_PER_TXN) break;
    const last = editIdentity(
      out.rows[out.rows.length - 1] as WireValue[],
      "paginating wiki telemetry",
    );
    afterEditId = last.id;
  }
  const startedAt = Date.now();
  // ONE ordered write lane for this app's sole source. Besides keeping overlapping flushes from
  // multiplying master memory, this makes each prune's select+delete atomic with respect to app
  // ingestion and preserves invocation order between ingest and prune calls.
  let writeTail: Promise<void> = Promise.resolve();
  const enqueueWrite = <T>(run: () => Promise<T>): Promise<T> => {
    const next = writeTail.then(run);
    writeTail = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  };

  const ingest = (batch: WikiEvent[]): Promise<void> =>
    enqueueWrite(async () => {
      for (let start = 0; start < batch.length; start += INGEST_EVENTS_PER_TXN) {
        const chunk = batch.slice(start, start + INGEST_EVENTS_PER_TXN);
        const statements: SqlStatement[] = [];
        for (const ev of chunk) {
          // Insert first. The two following statements use changes() as a fresh-edit bit, so a
          // replayed edit is a complete three-statement no-op instead of inflating parent counts.
          statements.push(editInsert(ev.page.id, ev.edit), pageUpsertAfterFreshEdit(ev.page));
          if (ev.edit.user) statements.push(editorUpsertAfterFreshEdit(ev.edit));
        }
        const capturedChangeUpperBound = chunk.length * 3;
        if (capturedChangeUpperBound >= HCTREE_CAPTURED_CHANGES_PER_TXN) {
          throw new Error(`wiki ingest chunk could capture ${capturedChangeUpperBound} changes`);
        }
        const outcome = await writeDaemon.executeSqlTxn({ statements });
        if (outcome.applied === false) continue; // An all-duplicate chunk changed no base row.
        txns++;
        for (const ev of chunk) {
          // A mixed chunk can contain both a fresh edit and a replay. The SQL changes() gates keep
          // its database counts exact; this membership gate does the same for in-process telemetry.
          if (editWindow.has(ev.edit.id)) continue;
          edits++;
          editWindow.set(ev.edit.id, { pageId: ev.page.id, user: ev.edit.user });
          bumpKey(pageEdits, ev.page.id);
          if (ev.edit.user) bumpKey(editorEdits, ev.edit.user);
        }
      }
    });

  const selectExpired = async (beforeTs: number): Promise<ExpiredEdit[]> => {
    const out = await writeDaemon.executeSqlRead({
      sql: "SELECT id, page_id, user FROM edit WHERE ts < ? ORDER BY ts LIMIT ?",
      params: [beforeTs, PRUNE_EDITS_PER_TXN],
      consistency: "strong",
    });
    return out.rows.map((row) => editIdentity(row as WireValue[], "selecting expired edits"));
  };

  const removeTrackedEdits = (expired: readonly ExpiredEdit[]): void => {
    for (const { id } of expired) {
      const tracked = editWindow.get(id);
      if (!tracked) continue; // The row can predate this process's in-memory telemetry window.
      editWindow.delete(id);
      dropKey(pageEdits, tracked.pageId);
      if (tracked.user) dropKey(editorEdits, tracked.user);
    }
  };

  const prune = (beforeTs: number): Promise<void> =>
    enqueueWrite(async () => {
      // Each committed chunk is independently exact and local telemetry advances with it. A later
      // chunk failure therefore leaves neither SQL nor the in-process window pretending it rolled
      // back earlier commits; the next scheduled prune simply resumes the remaining expired rows.
      for (;;) {
        const expired = await selectExpired(beforeTs);
        if (!expired.length) return;
        const batch = pruneStatements(expired);
        if (batch.capturedChangeUpperBound >= HCTREE_CAPTURED_CHANGES_PER_TXN) {
          throw new Error(`wiki prune chunk could capture ${batch.capturedChangeUpperBound} changes`);
        }
        const outcome = await writeDaemon.executeSqlTxn({ statements: batch.statements });
        if (outcome.applied === false) {
          throw new Error("wiki prune selected expired rows but its delete transaction changed none");
        }
        txns++;
        removeTrackedEdits(expired);
      }
    });

  const noteOffset = (id: string): void => {
    lastOffset = id;
  };

  // --- the daemon's live dedup signal (for the badge) --------------------------------------
  const daemonStats = async (): Promise<{ materializations: number; leases: number } | null> => {
    try {
      const res = await fetch(`${opts.daemonUrl}/stats`, {
        method: "POST",
        headers: { "content-type": "application/json", ...authHeaders },
        body: "{}",
      });
      if (!res.ok) return null;
      return (await res.json()) as { materializations: number; leases: number };
    } catch {
      return null;
    }
  };

  // --- the metrics badge payload, cached (TTL + single-flight) -----------------------------
  // EVERY viewer polls /metrics (~1 req / 2s each), so at 1k tabs this is ~500 req/s — and each one
  // otherwise does a fresh `fetch` to the daemon's /stats, hammering the single engine that's also
  // maintaining the pinned pipelines and fanning out to every ws. But the payload is slowly-changing
  // badge telemetry: `materializations` is the pinned set (≈constant), CPU% is already sampled on a
  // fixed 2s cadence, and edits/sec is derived client-side from the live stream — nothing here needs
  // per-request freshness. So build it at most once per TTL window behind a single-flight guard: many
  // concurrent viewers collapse to ONE daemon round-trip per window, and a window rollover can't
  // stampede the daemon (simultaneous misses all await the one in-flight build, not N fetches).
  const METRICS_TTL_MS = 1000;
  let metricsBody = "";
  let metricsAt = 0;
  let metricsInflight: Promise<string> | null = null;

  const buildMetricsBody = async (): Promise<string> => {
    const stats = await daemonStats();
    const machine = machineStats.read();
    return JSON.stringify({
      pages: pageEdits.size,
      editors: editorEdits.size,
      edits, // cumulative throughput odometer (since boot)
      // The live working set: edit rows currently in the retained window — what the nested boards run
      // over right now (the "thousands of rows, stays exact" headline).
      editsInWindow: editWindow.size,
      txns,
      uptimeMs: Date.now() - startedAt,
      source: opts.sourceName,
      daemonWsUrl: opts.daemonWsUrl,
      // The dedup story: many viewers, but `materializations` stays at the pinned set.
      materializations: stats?.materializations ?? null,
      leases: stats?.leases ?? null,
      // The footprint story: the whole VM's CPU + the app's RSS, against the machine's size — how
      // little it takes to keep both boards exact, plus how many tabs we're fanning to.
      vcpus: machine.vcpus,
      cpuPercent: machine.cpuPercent,
      nodeCpuPercent: machine.nodeCpuPercent,
      masterCpuPercent: machine.masterCpuPercent,
      followerCpuPercent: machine.followerCpuPercent,
      nodeRssBytes: machine.nodeRssBytes,
      masterRssBytes: machine.masterRssBytes,
      followerRssBytes: machine.followerRssBytes,
      memUsedBytes: machine.memUsedBytes,
      memLimitBytes: machine.memLimitBytes,
      viewers, // concurrent browser ws connections through the proxy (the deployed topology)
      // The CDC story: the resumable stream offset the daemon would resume from on restart.
      offset: lastOffset || null,
    });
  };

  const getMetricsBody = (): Promise<string> => {
    const now = Date.now();
    if (metricsBody && now - metricsAt < METRICS_TTL_MS) return Promise.resolve(metricsBody);
    if (metricsInflight) return metricsInflight; // a build is already in flight — share it (single-flight)
    metricsInflight = buildMetricsBody()
      .then((body) => {
        metricsBody = body;
        metricsAt = Date.now();
        return body;
      })
      .finally(() => {
        metricsInflight = null;
      });
    return metricsInflight;
  };

  // --- combined HTTP: the lease route + the metrics badge (CORS-open, read-only) -----------
  const cors = {
    "access-control-allow-origin": "*",
    "access-control-allow-headers": "content-type",
    "access-control-allow-methods": "POST, GET, OPTIONS",
  };
  const httpServer = createServer((req, res) => {
    void (async () => {
      if (req.method === "OPTIONS") {
        res.writeHead(204, cors);
        res.end();
        return;
      }
      const url = req.url ?? "";
      if (url.startsWith("/version")) {
        // The deployed commit, when a release pipeline stamps RINDLE_GIT_SHA; "unknown" locally.
        res.writeHead(200, { "content-type": "application/json", ...cors });
        res.end(JSON.stringify({ service: "wiki-demo", sha: process.env.RINDLE_GIT_SHA ?? "unknown" }));
        return;
      }
      if (url.startsWith("/api/rindle/query")) {
        try {
          const body = await readJson(req);
          // Public read-only demo: a fixed identity, no auth gate. The lease points at the
          // already-pinned materialization (the daemon dedupes by canonical query).
          const out = await api!.handleQueryJson(body, { user: "public" });
          res.writeHead(200, { "content-type": "application/json", ...cors });
          res.end(JSON.stringify(out));
        } catch (err) {
          res.writeHead(400, { "content-type": "application/json", ...cors });
          res.end(JSON.stringify({ error: String((err as Error)?.message ?? err) }));
        }
        return;
      }
      if (url.startsWith("/metrics")) {
        // Served from a 1s TTL + single-flight cache, so 1k polling viewers don't fan out into 1k
        // daemon /stats round-trips (see buildMetricsBody/getMetricsBody above).
        const body = await getMetricsBody();
        res.writeHead(200, { "content-type": "application/json", ...cors });
        res.end(body);
        return;
      }
      res.writeHead(404, cors);
      res.end();
    })();
  });

  // Reverse-proxy the public ws path to the LOCAL daemon ws (same host) so the whole tier presents
  // a SINGLE origin (api + metrics + ws) — a reverse proxy then needs just one
  // upstream. Locally the browser connects to the daemon ws directly, so this handler only fires in
  // the deployed topology. Raw TCP tunnel: replay the upgrade handshake to the daemon at its root
  // path, then pipe bytes both ways.
  const wsTarget = new URL(opts.daemonWsUrl);
  httpServer.on("upgrade", (req, socket, head) => {
    // Each browser tab opens exactly ONE multiplexed ws through this proxy (both boards ride it),
    // so live tunnels == concurrent viewers. Count up now, down once when either end closes.
    viewers++;
    let counted = true;
    const release = () => {
      if (counted) {
        counted = false;
        viewers = Math.max(0, viewers - 1);
      }
    };
    const up = netConnect({ host: wsTarget.hostname, port: Number(wsTarget.port) || 80 }, () => {
      const lines = ["GET / HTTP/1.1"];
      for (let i = 0; i < req.rawHeaders.length; i += 2) lines.push(`${req.rawHeaders[i]}: ${req.rawHeaders[i + 1]}`);
      up.write(`${lines.join("\r\n")}\r\n\r\n`);
      if (head && head.length) up.write(head);
      up.pipe(socket);
      socket.pipe(up);
    });
    const bail = () => {
      socket.destroy();
      up.destroy();
    };
    up.on("error", bail);
    socket.on("error", bail);
    up.on("close", release);
    socket.on("close", release);
  });

  const apiPort = await listen(httpServer, opts.apiPort ?? 0);

  return {
    apiPort,
    ingest,
    prune,
    noteOffset,
    close: async () => {
      // Source shutdown drains before calling us, but direct test/embedding callers may still have a
      // write queued. Do not tear the HTTP tier down until that ordered lane is settled.
      await writeTail;
      machineStats.stop();
      await new Promise<void>((resolve) => {
        httpServer.close(() => resolve());
      });
    },
  };
}

function readJson(req: import("node:http").IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (c) => chunks.push(typeof c === "string" ? Buffer.from(c) : c));
    req.on("end", () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"));
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

function listen(server: Server, port: number): Promise<number> {
  return new Promise((resolve) => {
    server.listen(port, () => {
      const addr = server.address();
      resolve(typeof addr === "object" && addr !== null ? addr.port : port);
    });
  });
}
