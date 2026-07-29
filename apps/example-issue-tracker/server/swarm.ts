// The LOAD TEST: a swarm of bots that hammer the live issue tracker, plus the metrics badge that
// shows the footprint of the box absorbing them. This is the issue-tracker twin of the wiki demo's
// ingester tier — there, ONE writer replays the Wikimedia firehose; here, MANY virtual bots drive
// the SAME authoritative path a browser hits (`createIssueApi(...).pushMutation`), so every write
// runs the real mutators + policy (the "spam" rejection, the owner-gated delete) and the daemon's
// IVM does genuine work. A human who opens the tracker is subscribed to the live top-of-window and
// watches the bots' issues stream in, statuses flip, comments land, and rows backfill on delete — in
// realtime, no refetch.
//
// It is colocated with `rindled` (same box in prod, same host locally), so the /metrics endpoint can
// read the WHOLE-VM CPU from /proc and the resident memory of both this process and the daemon — the
// honest "tiny footprint under a write storm" the badge is for. The same 1s-TTL + single-flight
// cache as the wiki tier keeps many polling viewers from fanning out into many daemon /stats calls.
//
// Run: node --conditions=@rindle/source server/swarm.ts   (or `pnpm dev:swarm`)
//
// TWO targets:
//   • LOCAL (default): embed the authority (`createIssueApi`) and write to a local rindled's private
//     control plane (RINDLE_DAEMON_URL); viewers open ws to the topology-injected RINDLE_FLEET_WS
//     (or an explicit DAEMON_WS_URL override). This is the `pnpm dev:swarm` path, where the footprint
//     badge reads the colocated box's /proc.
//   • REMOTE (set SWARM_API_URL): drive the PUBLIC edge exactly like a browser — POST mutations/leases
//     to `${SWARM_API_URL}/api/rindle/{mutate,query}` (the Cloudflare Worker, `x-user`, NO daemon
//     token — it's an open demo) and open ws to DAEMON_WS_URL (the public edge). No control-plane
//     access, so the box-footprint fields (CPU/RAM · daemon materializations/viewers) are omitted;
//     the badge still reports this swarm's own load (mutations/sec, applied, rejected, viewers we hold).
//
// Env: SWARM_API_URL ("" = local; a URL = remote/public-edge) · SWARM_BOTS (2000, headline virtual
//      writers) · SWARM_RATE (150, aggregate mutations/sec) · SWARM_SUBSCRIBERS (500, lightweight ws
//      viewers that lease + watch the live window) · SWARM_USERS (64, the author pool — kept small so
//      bots don't bloat the user picker) · SWARM_MAX_ISSUES (4000, the live bot corpus cap; creates
//      yield to deletes past it) · SWARM_CONCURRENCY (64, max in-flight mutations) · SWARM_SPAM_RATE
//      (0.004, the share of creates that trip the policy, to exercise rejection under load) ·
//      SWARM_METRICS_PORT (7711) · RINDLE_DAEMON_URL · RINDLE_DAEMON_TOKEN (local only) ·
//      RINDLE_FLEET_WS (topology-injected local fleet ws) · DAEMON_WS_URL (explicit ws override) ·
//      SWARM_PID (local: the rindled pid, added to this tier's RSS so memory is the whole app).

import { createServer } from "node:http";
import type { Server } from "node:http";

import type { MutationEnvelope } from "@rindle/client";

import {
  ISSUE_PRIORITIES,
  ISSUE_STATUSES,
  PAGE_SIZE,
  type IssuePriority,
  type IssueStatus,
} from "../shared/app-def.ts";
import { DEFAULT_RINDLE_API_ROUTES } from "@rindle/api-server";
import type { RindleApiServer } from "@rindle/api-server";

import { createIssueApi } from "./app-api.ts";
import type { User } from "./app-api.ts";
import { startMachineStats } from "./machine-stats.ts";

// --------------------------------------------------------------------------- config

const numEnv = (name: string, fallback: number): number => {
  const v = Number(process.env[name]);
  return Number.isFinite(v) && v > 0 ? v : fallback;
};

/** Like numEnv but allows 0 (so a count can be explicitly disabled). */
const countEnv = (name: string, fallback: number): number => {
  const v = Number(process.env[name]);
  return Number.isFinite(v) && v >= 0 ? Math.floor(v) : fallback;
};

const BOTS = Math.floor(numEnv("SWARM_BOTS", 2000));
const RATE = numEnv("SWARM_RATE", 150); // aggregate mutations / second
const USERS = Math.max(1, Math.floor(numEnv("SWARM_USERS", 64)));
const MAX_ISSUES = Math.floor(numEnv("SWARM_MAX_ISSUES", 4000));
const CONCURRENCY = Math.floor(numEnv("SWARM_CONCURRENCY", 64));
// Lightweight ws SUBSCRIBERS (viewers): a configurable count of bots that also attach as live
// readers — lease a window in-process, open a raw ws, drain the fan-out. No wasm engine, so this
// scales to thousands and makes the daemon's "viewers watching now" number (and fan-out load) real.
const SUBSCRIBERS = countEnv("SWARM_SUBSCRIBERS", 500);
const SPAM_RATE = (() => {
  const v = Number(process.env.SWARM_SPAM_RATE);
  return Number.isFinite(v) && v >= 0 && v <= 1 ? v : 0.004;
})();
const METRICS_PORT = Math.floor(numEnv("SWARM_METRICS_PORT", 7711));

const DAEMON_URL = process.env.RINDLE_DAEMON_URL ?? "http://127.0.0.1:7600";
const DAEMON_TOKEN = process.env.RINDLE_DAEMON_TOKEN ?? "dev-daemon-token";
const DAEMON_WS_URL =
  process.env.DAEMON_WS_URL ?? process.env.RINDLE_FLEET_WS ?? "ws://127.0.0.1:7601";
const SWARM_PID = Number(process.env.SWARM_PID);
// REMOTE mode: when SWARM_API_URL is set, drive the PUBLIC edge (the Cloudflare Worker) instead of
// embedding the authority + talking to a local control plane. No daemon token — same path a browser hits.
const API_URL = (process.env.SWARM_API_URL ?? "").trim().replace(/\/$/, "");
const REMOTE = API_URL.length > 0;

// --------------------------------------------------------------------------- content

const TITLE_VERBS = ["fix", "investigate", "refactor", "ship", "polish", "debug", "tune", "harden"];
const TITLE_NOUNS = [
  "the reconnect path",
  "the pagination cursor",
  "the optimistic rebase",
  "the lease handshake",
  "the prune window",
  "the snapshot size",
  "the tag editor",
  "the comment composer",
  "the owner check",
  "the daemon restart heal",
];
const TAGS = ["daemon", "sync", "query", "ux", "docs", "api", "perf", "wasm", "ivm", "flake"];
const COMMENTS = [
  "repro is flaky — only on cold start",
  "ws drops the lease right before the snapshot lands",
  "confirmed on shared-cpu-2x, fine on a fat box",
  "the 50th row falls out a frame early",
  "rebased on confirmation, snap-back looks clean now",
  "this is the IVM doing the right thing actually",
  "needs a window prune so the corpus stays bounded",
];

const pick = <T>(xs: readonly T[]): T => xs[(Math.random() * xs.length) | 0];

// --------------------------------------------------------------------------- bots

interface Bot {
  /** Stable author/owner identity (from the bounded pool, so owner-gated deletes match). */
  author: string;
  /** The optimistic client id the daemon dedupes (clientID, mid) by. */
  clientID: string;
  /** Next mutation id for this bot (monotonic per client). */
  mid: number;
  /** This bot's own live issue ids — the only rows it may edit/delete (owner-gated). FIFO. */
  issues: string[];
}

/** A weighted action menu. createIssue dominates so the window churns; deletes balance it so the
 *  corpus stays bounded; the rest exercise every field/relationship edit a browser can make. */
const ACTIONS = [
  { name: "createIssue", weight: 50 },
  { name: "addComment", weight: 14 },
  { name: "setStatus", weight: 10 },
  { name: "deleteIssue", weight: 8 },
  { name: "setPriority", weight: 8 },
  { name: "addTag", weight: 6 },
  { name: "editTitle", weight: 4 },
] as const;
const WEIGHT_TOTAL = ACTIONS.reduce((a, x) => a + x.weight, 0);
type ActionName = (typeof ACTIONS)[number]["name"];

function pickAction(): ActionName {
  let r = Math.random() * WEIGHT_TOTAL;
  for (const a of ACTIONS) {
    r -= a.weight;
    if (r < 0) return a.name;
  }
  return "createIssue";
}

export interface SwarmHandle {
  readonly metricsPort: number;
  close(): Promise<void>;
}

// The two authority calls the swarm makes. LOCAL satisfies this with the embedded `createIssueApi`;
// REMOTE satisfies it by POSTing to the public API the same way the browser client does.
type SwarmApi = Pick<RindleApiServer<User>, "pushMutation" | "createQueryLease">;

/** A REMOTE swarm api: raw fetch against the public edge (Cloudflare Worker), user on `x-user`. */
function createRemoteApi(baseUrl: string): SwarmApi {
  const post = async (path: string, user: User, body: unknown): Promise<any> => {
    const res = await fetch(baseUrl + path, {
      method: "POST",
      headers: { "content-type": "application/json", "x-user": user ?? "" },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`${path} → ${res.status}`);
    return res.json();
  };
  return {
    // handleMutateJson accepts a bare envelope (`msg.envelope ?? msg`).
    pushMutation: ({ user, envelope }) => post(DEFAULT_RINDLE_API_ROUTES.mutate, user, envelope),
    createQueryLease: ({ user, name, args }) => post(DEFAULT_RINDLE_API_ROUTES.query, user, { name, args }),
  };
}

export async function startSwarm(): Promise<SwarmHandle> {
  const api: SwarmApi = REMOTE
    ? createRemoteApi(API_URL)
    : createIssueApi({
        daemonUrl: DAEMON_URL,
        daemonToken: DAEMON_TOKEN,
        // One topology (design 214): bot mutations go to the replicator's write ingress;
        // reads/leases keep coming off the follower (DAEMON_URL).
        replicatorUrl: process.env.RINDLE_REPLICATOR_URL,
        replicatorToken: process.env.RINDLE_REPLICATOR_TOKEN,
      });

  // The author pool is small so thousands of bots don't explode the (whole-table) user picker; the
  // headline "bots" number is the count of independent virtual actors, each with its own mutation
  // stream and its own live issues.
  const bots: Bot[] = Array.from({ length: BOTS }, (_, i) => ({
    author: `bot-${String(i % USERS).padStart(3, "0")}`,
    clientID: `swarm-${i}`,
    mid: 1,
    issues: [],
  }));

  let seq = 0; // global monotonic id source (issue/comment/tag ids stay unique across all bots)
  const nextId = (): string => `s${(seq++).toString(36)}`;

  // --- counters / telemetry ---------------------------------------------------------------
  let attempted = 0;
  let applied = 0;
  let rejected = 0;
  let errors = 0;
  let liveBotIssues = 0;
  const perAction: Record<string, number> = {};
  const startedAt = Date.now();
  // A trailing ring of applied-mutation timestamps → a live mutations/sec, decayed by a fixed-cadence
  // recompute rather than by the last push (so the rate falls during a lull, never freezes).
  const RATE_WINDOW_MS = 5000;
  const RATE_RING = 4096;
  const stamps = new Float64Array(RATE_RING);
  let stampHead = 0;
  let stampCount = 0;
  const stamp = () => {
    stamps[stampHead] = Date.now();
    stampHead = (stampHead + 1) % RATE_RING;
    if (stampCount < RATE_RING) stampCount++;
  };
  const mutationsPerSec = (): number => {
    const cutoff = Date.now() - RATE_WINDOW_MS;
    let n = 0;
    for (let i = 0; i < stampCount; i++) if (stamps[i] >= cutoff) n++;
    return n / (RATE_WINDOW_MS / 1000);
  };

  // --- a single mutation: build the envelope a browser would, push it the authoritative way ----
  const send = async (bot: Bot, name: ActionName, args: unknown): Promise<void> => {
    const envelope: MutationEnvelope = { clientID: bot.clientID, mid: bot.mid++, name, args };
    attempted++;
    perAction[name] = (perAction[name] ?? 0) + 1;
    try {
      const out = await api.pushMutation({ user: bot.author, envelope });
      if (out.accepted) {
        applied++;
        stamp();
      } else {
        rejected++;
      }
    } catch {
      errors++;
    }
  };

  const now = () => Date.now();
  const createIssue = (bot: Bot): Promise<void> => {
    const id = nextId();
    const spam = Math.random() < SPAM_RATE; // a sliver of creates trip the policy (rejection path)
    const title = spam
      ? `${pick(TITLE_VERBS)} spam ${pick(TITLE_NOUNS)}`
      : `${pick(TITLE_VERBS)} ${pick(TITLE_NOUNS)}`;
    const tags = [{ id: nextId(), name: pick(TAGS) }, { id: nextId(), name: pick(TAGS) }];
    const at = now();
    if (!spam) {
      bot.issues.push(id);
      liveBotIssues++;
    }
    return send(bot, "createIssue", {
      id,
      title,
      status: pick(ISSUE_STATUSES) as IssueStatus,
      priority: pick(ISSUE_PRIORITIES) as IssuePriority,
      owner: bot.author,
      tags,
      description: pick(COMMENTS),
      descriptionCommentId: `${id}-d`,
      createdAt: at,
    });
  };

  const deleteOldest = (bot: Bot): Promise<void> => {
    const id = bot.issues.shift();
    if (!id) return createIssue(bot); // nothing to delete yet → make work instead
    liveBotIssues--;
    return send(bot, "deleteIssue", { id });
  };

  /** A random own issue id, or undefined if this bot has none yet. */
  const ownIssue = (bot: Bot): string | undefined =>
    bot.issues.length ? bot.issues[(Math.random() * bot.issues.length) | 0] : undefined;

  const act = (bot: Bot): Promise<void> => {
    // Keep the corpus bounded: once the live bot issues pass the cap, bias hard toward deletes.
    if (liveBotIssues > MAX_ISSUES && bot.issues.length) return deleteOldest(bot);

    const action = pickAction();
    if (action === "createIssue") return createIssue(bot);
    if (action === "deleteIssue") return deleteOldest(bot);

    const id = ownIssue(bot);
    if (!id) return createIssue(bot); // can't edit what we haven't made — make work instead
    switch (action) {
      case "addComment":
        return send(bot, "addComment", {
          id: nextId(),
          issueId: id,
          body: pick(COMMENTS),
          createdAt: now(),
          updatedAt: now(),
        });
      case "setStatus":
        return send(bot, "setStatus", { id, status: pick(ISSUE_STATUSES) as IssueStatus, updatedAt: now() });
      case "setPriority":
        return send(bot, "setPriority", {
          id,
          priority: pick(ISSUE_PRIORITIES) as IssuePriority,
          updatedAt: now(),
        });
      case "addTag":
        return send(bot, "addTag", { id: nextId(), issueId: id, name: pick(TAGS), updatedAt: now() });
      case "editTitle":
        return send(bot, "editTitle", { id, title: `${pick(TITLE_VERBS)} ${pick(TITLE_NOUNS)}`, updatedAt: now() });
      default:
        return createIssue(bot);
    }
  };

  // --- the pacer: a token bucket fills at RATE/s; each token launches one bot action with bounded
  //     in-flight concurrency, so a slow daemon sheds load (tokens cap, then drop) instead of piling
  //     up unbounded promises. -------------------------------------------------------------------
  let inFlight = 0;
  let tokens = 0;
  let running = true;
  const PACE_MS = 50;
  const TOKEN_CAP = Math.max(1, RATE); // at most ~1s of backlog
  const pacer = setInterval(() => {
    if (!running) return;
    tokens = Math.min(TOKEN_CAP, tokens + (RATE * PACE_MS) / 1000);
    while (tokens >= 1 && inFlight < CONCURRENCY) {
      tokens -= 1;
      inFlight++;
      const bot = bots[(Math.random() * bots.length) | 0];
      void act(bot).finally(() => {
        inFlight--;
      });
    }
  }, PACE_MS);

  // --- the viewers: lightweight ws subscribers --------------------------------------------------
  // Each leases the SAME live window in-process (so all viewers SHARE one materialization — the
  // dedup story: viewers scale, pipelines don't), opens a raw ws to the daemon's public port, and
  // drains the fan-out. No wasm/optimistic engine, so thousands cost almost nothing here while the
  // daemon does the real per-subscriber fan-out work. A dropped socket (e.g. daemon restart) keeps
  // the viewer alive by reconnecting, so the count is stable.
  let subsConnected = 0;
  const subSockets = new Array<WebSocket | null>(SUBSCRIBERS).fill(null);
  let subClosing = false;

  const connectSubscriber = (i: number): void => {
    if (subClosing) return;
    void (async () => {
      let leaseToken: string;
      try {
        const lease = await api.createQueryLease({
          user: `viewer-${i % USERS}`,
          name: "issuesPage",
          // The same args shape the app's chrome subscribes with (issuesPageArgsSchema).
          args: { limit: PAGE_SIZE, filter: [] },
        });
        leaseToken = lease.leaseToken;
      } catch {
        if (!subClosing) setTimeout(() => connectSubscriber(i), 1000); // daemon not ready — retry
        return;
      }
      if (subClosing) return;
      let opened = false;
      const ws = new WebSocket(DAEMON_WS_URL);
      subSockets[i] = ws;
      ws.addEventListener("open", () => {
        opened = true;
        subsConnected++;
        ws.send(JSON.stringify({ t: "init", clientID: `viewer-${i}` }));
        ws.send(JSON.stringify({ t: "subscribe", queryId: 1, leaseToken }));
      });
      ws.addEventListener("message", () => {}); // drain the fan-out — receiving it IS the load
      ws.addEventListener("error", () => {}); // `close` follows; it owns the reconnect
      ws.addEventListener("close", () => {
        if (opened) {
          subsConnected = Math.max(0, subsConnected - 1);
          opened = false;
        }
        subSockets[i] = null;
        if (!subClosing) setTimeout(() => connectSubscriber(i), 500 + Math.random() * 1500); // keep the viewer alive
      });
    })();
  };

  // Ramp the viewers up gradually rather than opening every socket in one tick.
  if (SUBSCRIBERS > 0) {
    let nextSub = 0;
    const rampPerTick = Math.max(1, Math.ceil(SUBSCRIBERS / 100));
    const rampTimer = setInterval(() => {
      if (subClosing || nextSub >= SUBSCRIBERS) {
        clearInterval(rampTimer);
        return;
      }
      for (let k = 0; k < rampPerTick && nextSub < SUBSCRIBERS; k++) connectSubscriber(nextSub++);
    }, 50);
    rampTimer.unref?.();
  }

  // --- the daemon's live dedup signal (for the badge) -------------------------------------------
  const authHeaders = { authorization: `Bearer ${DAEMON_TOKEN}` };
  interface DaemonStats {
    materializations: number;
    leases: number;
    /** Live public ws connections — one per browser tab or subscriber bot ⇒ the viewers count. */
    connections?: number;
    /** Total active query subscriptions across those connections. */
    subscriptions?: number;
  }
  const daemonStats = async (): Promise<DaemonStats | null> => {
    try {
      const res = await fetch(`${DAEMON_URL}/stats`, {
        method: "POST",
        headers: { "content-type": "application/json", ...authHeaders },
        body: "{}",
      });
      if (!res.ok) return null;
      return (await res.json()) as DaemonStats;
    } catch {
      return null;
    }
  };

  // --- whole-machine footprint (this swarm + rindled) ------------------------------------------
  // LOCAL only: the badge reads the colocated box's /proc. REMOTE has no access to the host's
  // /proc or its bearer-gated control plane, so the footprint fields are omitted (null) there.
  const machineStats = REMOTE ? null : startMachineStats(Number.isFinite(SWARM_PID) ? [SWARM_PID] : []);

  // --- /metrics: 1s-TTL single-flight cache (see the wiki tier for why) ------------------------
  const METRICS_TTL_MS = 1000;
  let metricsBody = "";
  let metricsAt = 0;
  let metricsInflight: Promise<string> | null = null;

  const buildMetricsBody = async (): Promise<string> => {
    const stats = REMOTE ? null : await daemonStats();
    const machine = machineStats?.read() ?? null;
    return JSON.stringify({
      // The load story: how many virtual bots, how hard they're hitting, and what's alive.
      bots: BOTS,
      mutationsPerSec: Number(mutationsPerSec().toFixed(1)),
      mutations: attempted,
      applied,
      rejected,
      errors,
      inFlight,
      liveBotIssues,
      perAction,
      // The viewers side: how many subscriber bots we intend / have connected, and the daemon's
      // own truth — live ws connections (subscriber bots + real human tabs) = "watching now".
      subscribers: SUBSCRIBERS,
      subscribersConnected: subsConnected,
      viewers: stats?.connections ?? null,
      subscriptions: stats?.subscriptions ?? null,
      uptimeMs: Date.now() - startedAt,
      source: "swarm",
      daemonWsUrl: DAEMON_WS_URL,
      // The dedup story: the live materializations on the daemon — every viewer watches the SAME
      // window, so thousands of them SHARE one pipeline (materializations stays flat as viewers grow).
      materializations: stats?.materializations ?? null,
      leases: stats?.leases ?? null,
      // The footprint story: the whole VM's CPU + the app's RSS, against the machine's size — how
      // little it takes to keep every live window exact while the bots storm it. (null in REMOTE mode.)
      target: REMOTE ? API_URL : "local",
      remote: REMOTE,
      vcpus: machine?.vcpus ?? null,
      cpuPercent: machine?.cpuPercent ?? null,
      memUsedBytes: machine?.memUsedBytes ?? null,
      memLimitBytes: machine?.memLimitBytes ?? null,
    });
  };

  const getMetricsBody = (): Promise<string> => {
    const t = Date.now();
    if (metricsBody && t - metricsAt < METRICS_TTL_MS) return Promise.resolve(metricsBody);
    if (metricsInflight) return metricsInflight;
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

  // --- HTTP: the metrics badge (CORS-open, read-only) ------------------------------------------
  const cors = {
    "access-control-allow-origin": "*",
    "access-control-allow-headers": "content-type",
    "access-control-allow-methods": "GET, OPTIONS",
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
        res.writeHead(200, { "content-type": "application/json", ...cors });
        res.end(JSON.stringify({ service: "issue-swarm", sha: process.env.RINDLE_GIT_SHA ?? "unknown" }));
        return;
      }
      if (url.startsWith("/metrics")) {
        const body = await getMetricsBody();
        res.writeHead(200, { "content-type": "application/json", ...cors });
        res.end(body);
        return;
      }
      res.writeHead(404, cors);
      res.end();
    })();
  });

  const metricsPort = await listen(httpServer, METRICS_PORT);
  const writeTarget = REMOTE ? `${API_URL} (public edge)` : DAEMON_URL;
  if (REMOTE) {
    console.log(
      `[swarm] ⚠ REMOTE: hammering the LIVE demo at ${API_URL} — ${BOTS} bots @ ~${RATE} mutations/s + ` +
        `${SUBSCRIBERS} ws viewers (${DAEMON_WS_URL}). Writes are public + may be wiped by the reset cron.`,
    );
  }
  console.log(
    `[swarm] ${BOTS} bots @ ~${RATE} mutations/s + ${SUBSCRIBERS} ws viewers → ${writeTarget} · ` +
      `metrics http://127.0.0.1:${metricsPort}/metrics`,
  );

  return {
    metricsPort,
    close: () =>
      new Promise<void>((resolve) => {
        running = false;
        subClosing = true;
        clearInterval(pacer);
        for (const ws of subSockets) {
          try {
            ws?.close();
          } catch {
            /* already gone */
          }
        }
        machineStats?.stop();
        httpServer.close(() => resolve());
      }),
  };
}

function listen(server: Server, port: number): Promise<number> {
  return new Promise((resolve) => {
    server.listen(port, () => {
      const addr = server.address();
      resolve(typeof addr === "object" && addr !== null ? addr.port : port);
    });
  });
}

// Allow `node server/swarm.ts` standalone.
if (import.meta.url === `file://${process.argv[1]}`) {
  await startSwarm();
}
