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

import { createServer } from "node:http";
import type { Server } from "node:http";
import { connect as netConnect } from "node:net";

import { createRindleApiServer, defineApiQueries } from "@rindle/api-server";
import type { RindleApiServer } from "@rindle/api-server";
import { HttpRindleDaemonClient } from "@rindle/daemon-client";

import { startMachineStats } from "./machine-stats.ts";
import { topEdited, topRecentEditors } from "./schema.ts";
import type { WikiEvent } from "./schema.ts";

/** Upsert the page (denormalized edit count + newest-edit fields) — `add` and `edit` both collapse
 *  to one idempotent statement keyed by `id`. */
function pageUpsert(p: WikiEvent["page"]) {
  return {
    sql:
      "INSERT INTO page (id, wiki, title, url, edits, last_ts, last_user) VALUES (?, ?, ?, ?, 1, ?, ?) " +
      "ON CONFLICT(id) DO UPDATE SET edits = edits + 1, last_ts = excluded.last_ts, " +
      "last_user = excluded.last_user, title = excluded.title, url = excluded.url",
    params: [p.id, p.wiki, p.title, p.url, p.ts, p.user],
  };
}

/** Upsert the editor (denormalized edit count + newest-edit time) — the parent of the second board.
 *  Anonymous/empty usernames are skipped by the caller (no nameless editor row). */
function editorUpsert(e: WikiEvent["edit"]) {
  return {
    sql:
      "INSERT INTO editor (name, edits, last_ts) VALUES (?, 1, ?) " +
      "ON CONFLICT(name) DO UPDATE SET edits = edits + 1, last_ts = max(last_ts, excluded.last_ts)",
    params: [e.user, e.ts],
  };
}

/** Insert the edit row. `DO NOTHING` so a resume that re-delivers an already-applied edit is a
 *  silent no-op (the offset is persisted post-apply, so overlap is near-impossible anyway). */
function editInsert(pageId: string, e: WikiEvent["edit"]) {
  return {
    sql:
      "INSERT INTO edit (id, page_id, user, comment, ts, delta, bot) VALUES (?, ?, ?, ?, ?, ?, ?) " +
      "ON CONFLICT(id) DO NOTHING",
    params: [e.id, pageId, e.user, e.comment, e.ts, e.delta, e.bot],
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
  /** The spawned follower rindled's pid — added to this tier's RSS so the memory badge is the whole app. */
  daemonPid?: number;
}

export interface TinyMachine {
  readonly apiPort: number;
  /** Apply one ordered source batch (page upserts + edit inserts). */
  ingest(events: WikiEvent[]): Promise<void>;
  /** Prune edits older than `beforeTs` (unix seconds), decrement their pages' counts, and drop
   *  pages that fall to zero — keeps the dataset (and the "most-edited" window) bounded. */
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

  // Whole-machine resource telemetry (CPU%/RAM of this tier + rindled) for the "tiny footprint"
  // badge, plus a live count of concurrent browser viewers (one ws each through the proxy below).
  const machineStats = startMachineStats(opts.daemonPid ? [opts.daemonPid] : []);
  let viewers = 0;

  // --- the ordered source ingester ---------------------------------------------------------
  let edits = 0; // cumulative edits applied since boot (a throughput odometer, never decremented)
  let txns = 0;
  let lastOffset = "";
  // The LIVE working set: one entry per edit row currently in the retained window, carrying the
  // page + editor it touched. The nested sub-queries scan this set; the prune shifts the aged-out
  // front off AND decrements the per-key live counts below. The stream is ~time-ordered, so a
  // front-shift while `ts < cutoff` matches the SQL `DELETE … WHERE ts < cutoff` — an honest,
  // self-bounded (O(window)) count of "rows the boards run over right now".
  const editWindow: { ts: number; pageId: string; user: string }[] = [];
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
  const startedAt = Date.now();
  const ingest = async (batch: WikiEvent[]): Promise<void> => {
    if (!batch.length) return;
    const statements = [];
    for (const ev of batch) {
      edits++;
      editWindow.push({ ts: ev.edit.ts, pageId: ev.page.id, user: ev.edit.user });
      bumpKey(pageEdits, ev.page.id);
      statements.push(pageUpsert(ev.page), editInsert(ev.page.id, ev.edit));
      // Skip nameless editors (rare missing username) so the board has no blank parent row.
      if (ev.edit.user) {
        bumpKey(editorEdits, ev.edit.user);
        statements.push(editorUpsert(ev.edit));
      }
    }
    txns++;
    await writeDaemon.executeSqlTxn({ statements });
  };

  const prune = async (beforeTs: number): Promise<void> => {
    await writeDaemon.executeSqlTxn({
      statements: [
        // Decrement the denormalized counts on the parents that had aged-out edits, then drop the
        // edits, then drop any parent that fell to zero. Both boards stay exact as rows leave.
        {
          sql:
            "UPDATE page SET edits = edits - (SELECT COUNT(*) FROM edit WHERE edit.page_id = page.id AND edit.ts < ?) " +
            "WHERE id IN (SELECT DISTINCT page_id FROM edit WHERE ts < ?)",
          params: [beforeTs, beforeTs],
        },
        {
          sql:
            "UPDATE editor SET edits = edits - (SELECT COUNT(*) FROM edit WHERE edit.user = editor.name AND edit.ts < ?) " +
            "WHERE name IN (SELECT DISTINCT user FROM edit WHERE ts < ?)",
          params: [beforeTs, beforeTs],
        },
        { sql: "DELETE FROM edit WHERE ts < ?", params: [beforeTs] },
        { sql: "DELETE FROM page WHERE edits <= 0", params: [] },
        { sql: "DELETE FROM editor WHERE edits <= 0", params: [] },
      ],
    });
    // Shift the aged-out edits off the front of the live window so `editsInWindow` tracks the actual
    // row count the boards now run over, and decrement their pages'/editors' live counts so a key
    // drops out of `pages`/`editors` exactly when its last windowed edit ages out (bounded, no leak).
    let i = 0;
    while (i < editWindow.length && editWindow[i].ts < beforeTs) i++;
    if (i > 0) {
      for (let j = 0; j < i; j++) {
        const e = editWindow[j];
        dropKey(pageEdits, e.pageId);
        if (e.user) dropKey(editorEdits, e.user);
      }
      editWindow.splice(0, i);
    }
  };

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
      editsInWindow: editWindow.length,
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
    close: () =>
      new Promise<void>((resolve) => {
        machineStats.stop();
        httpServer.close(() => resolve());
      }),
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
