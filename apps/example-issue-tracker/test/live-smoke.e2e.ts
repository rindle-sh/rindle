// The LIVE-pair smoke: the exact `smoke.e2e.ts` battery, but tiers 1 is a REAL deployed pair —
// a packed OVH box's rindle-replicator master + rindled follower behind cloudflared — instead of
// the local `startPair` fixture. Everything crosses real wires: migrations via the master's
// `/migrate` (the ddl entries reach the follower over the stream — no follower bounce), seeds and
// mutations over the tunnel'd write ingress, the wasm client's subscription over the public wss
// hostname. This is the milestone-7 "IVM client stack live on OVH" proof, executable.
//
// Endpoints/credentials ride env (the tokens live in the box's /etc/rindle/secrets.d/<app>.env):
//
//   WRITE_TOKEN=…  SQL_AUTH_TOKEN=…  DAEMON_TOKEN=…  pnpm smoke:live
//
// Optional overrides (defaults = the spike-a pair on gra-1):
//   LIVE_MASTER_URL   https://spike-a.rindle.cloud        (master write ingress)
//   LIVE_SYNC_URL     https://spike-a-sync.rindle.cloud   (follower control plane)
//   LIVE_WS_URL       wss://spike-a-ws.rindle.cloud       (follower public subscribe ws)
//
// Restartable: seeds are idempotency-keyed, and a pre-clean sweeps the mutable ids (`i1`/`i2`)
// a failed prior run may have stranded, so the window-top assertions hold on every run.

import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";

import { createRindleClient } from "@rindle/optimistic";
import type { RindleClient } from "@rindle/optimistic";
import { HttpRindleDaemonClient } from "@rindle/daemon-client";
import { initWasm } from "@rindle/wasm";
import type { ArrayView, ColsMap } from "@rindle/client";

import { FEED_LIMIT, mutators, schema, PAGE_SIZE } from "../shared/app-def.ts";
import { issuesPageQuery, myIssuesQuery } from "../src/components/IssueListItem.queries.ts";
import { issueDetailQuery } from "../src/components/IssueDetail.queries.ts";
import { recentCommentsQuery } from "../src/components/ActivityFeed.queries.ts";
import { usersQuery } from "../src/components/UserBadge.queries.ts";
import { seedStatements } from "../server/seed.ts";

const appRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const repoRoot = resolve(appRoot, "../..");
const SEEDED = 120; // 2 full windows + a partial third — matches the local smoke

const MASTER_URL = process.env.LIVE_MASTER_URL ?? "https://spike-a.rindle.cloud";
const SYNC_URL = process.env.LIVE_SYNC_URL ?? "https://spike-a-sync.rindle.cloud";
const WS_URL = process.env.LIVE_WS_URL ?? "wss://spike-a-ws.rindle.cloud";
const WRITE_TOKEN = process.env.WRITE_TOKEN ?? "";
// Same alias contract as the entrypoint: either spelling, but not two different values.
const SQL_TOKEN = process.env.SQL_AUTH_TOKEN ?? process.env.SQL_TOKEN ?? "";
const DAEMON_TOKEN = process.env.DAEMON_TOKEN ?? "";
for (const [name, value] of [
  ["WRITE_TOKEN", WRITE_TOKEN],
  ["SQL_AUTH_TOKEN (or SQL_TOKEN)", SQL_TOKEN],
  ["DAEMON_TOKEN", DAEMON_TOKEN],
] as const) {
  if (!value) {
    console.error(`live smoke: ${name} is required (see /etc/rindle/secrets.d/<app>.env on the box)`);
    process.exit(2);
  }
}

await initWasm();

// WAN + tunnel latency: the local smoke's 5s waits become 20s, and the whole run gets 5 minutes.
const WAIT_MS = 20_000;
setTimeout(() => {
  console.error("live smoke watchdog: exceeded 300s, forcing exit");
  process.exit(1);
}, 300_000).unref();

// --- tier 1 is already running on the box. Apply migrations/ against the LIVE master (minting
// `ddl` change-log entries that reach the follower over the stream — the production migrate flow,
// idempotent on re-runs). Auth rides RINDLE_DAEMON_TOKEN, exactly the DEPLOY.md recipe.
const cliBuild = spawnSync("cargo", ["build", "-p", "rindle-cli"], {
  cwd: join(repoRoot, "rust"),
  stdio: ["ignore", "inherit", "inherit"],
});
if (cliBuild.status !== 0) {
  console.error("cargo build -p rindle-cli failed");
  process.exit(1);
}
const migrated = spawnSync(
  join(repoRoot, "rust/target/debug/rindle"),
  ["migrate", "apply", "--dir", join(appRoot, "migrations"), "--url", MASTER_URL],
  { stdio: ["ignore", "inherit", "inherit"], env: { ...process.env, RINDLE_DAEMON_TOKEN: WRITE_TOKEN } },
);
if (migrated.status !== 0) {
  console.error("rindle migrate apply against the live master failed");
  process.exit(1);
}

const master = new HttpRindleDaemonClient({
  baseUrl: MASTER_URL,
  headers: { authorization: `Bearer ${WRITE_TOKEN}` },
});

// Pre-clean the mutable ids a failed prior run may have stranded (deleting absent rows is a
// no-op). Fresh key per run — this must EXECUTE, not replay-absorb.
await master.executeSqlTxn({
  idempotencyKey: `live-smoke-preclean-${Date.now()}`,
  statements: [
    { sql: "DELETE FROM comment WHERE issueId IN ('i1', 'i2')", params: [] },
    { sql: "DELETE FROM tag WHERE issueId IN ('i1', 'i2')", params: [] },
    { sql: "DELETE FROM issue WHERE id IN ('i1', 'i2')", params: [] },
  ],
});

// Seed. On the FIRST run against a box this applies; afterwards the durable idempotency key
// absorbs it — either way the corpus is present, and the in-run replay must always absorb.
const seeded = await master.executeSqlTxn({ idempotencyKey: "smoke-seed", statements: seedStatements(SEEDED) });
console.log(seeded.applied ? "seeded the live corpus" : "live corpus already seeded (idempotency key held)");
const replayed = await master.executeSqlTxn({ idempotencyKey: "smoke-seed", statements: seedStatements(SEEDED) });
assert.equal(replayed.applied, false, "the seed replay is absorbed by the idempotency key");

// --- tier 2: the demo's standalone API shell, running locally, pointed at the live pair.
const api = spawn(process.execPath, ["--conditions=@rindle/source", join(appRoot, "server/api.ts")], {
  stdio: ["ignore", "pipe", "inherit"],
  env: {
    ...process.env,
    RINDLE_DAEMON_URL: SYNC_URL,
    RINDLE_DAEMON_TOKEN: DAEMON_TOKEN,
    RINDLE_REPLICATOR_URL: MASTER_URL,
    RINDLE_REPLICATOR_TOKEN: WRITE_TOKEN,
    RINDLE_DATABASE_TOKEN: SQL_TOKEN,
    API_PORT: "0",
  },
});
const apiUrl = await new Promise<string>((resolveUrl, reject) => {
  const timer = setTimeout(() => reject(new Error("api server did not start")), 10_000);
  let buffer = "";
  api.stdout?.on("data", (chunk: Buffer) => {
    buffer += String(chunk);
    const m = buffer.match(/listening on (http:\/\/[^\s]+)/);
    if (m) {
      clearTimeout(timer);
      resolveUrl(m[1]);
    }
  });
});

// --- tier 3: two clients, two users — the browser stack, subscription over the public wss.
type IssueCardRow = ReturnType<ReturnType<typeof issuesPageQuery>["materialize"]>["data"][number];
type IssueCardView = ArrayView<IssueCardRow>;
type SmokeClient = RindleClient<ColsMap, typeof mutators> & {
  rejections: { name: string; reason: string }[];
  page1: IssueCardView;
};

const makeClient = async (user: string): Promise<SmokeClient> => {
  const rejections: { name: string; reason: string }[] = [];
  const client = await createRindleClient({
    schema,
    mutators,
    user: () => user,
    api: { url: apiUrl, headers: { "x-user": user } },
    daemon: { wsUrl: WS_URL },
    clientID: `live-smoke-${user}`,
    onRejected: (envelope, reason) => rejections.push({ name: envelope.name, reason }),
  });
  return { ...client, rejections, page1: client.store.materialize(issuesPageQuery({ limit: PAGE_SIZE })) };
};

const waitFor = (cond: () => boolean, label: string, ms = WAIT_MS): Promise<void> =>
  new Promise((resolveWait, reject) => {
    const start = Date.now();
    const tick = () => {
      if (cond()) return resolveWait();
      if (Date.now() - start > ms) return reject(new Error(`timeout: ${label}`));
      setTimeout(tick, 25);
    };
    tick();
  });

interface DaemonStats {
  subscriptions: number;
  materializations: number;
  leases: number;
  connections: number;
}
const daemonStats = async (): Promise<DaemonStats> => {
  const res = await fetch(`${SYNC_URL}/stats`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${DAEMON_TOKEN}` },
    body: "{}",
    signal: AbortSignal.timeout(8000),
  });
  if (!res.ok) throw new Error(`/stats failed: ${res.status}`);
  return (await res.json()) as DaemonStats;
};
const waitForStat = async (pred: (s: DaemonStats) => boolean, label: string, ms = WAIT_MS): Promise<DaemonStats> => {
  const start = Date.now();
  for (;;) {
    const stats = await daemonStats();
    if (pred(stats)) return stats;
    if (Date.now() - start > ms) throw new Error(`timeout: ${label}`);
    await new Promise((r) => setTimeout(r, 100));
  }
};

const has = (view: IssueCardView, id: string): boolean => view.data.some((row) => row.id === id);
const byId = (view: IssueCardView, id: string): IssueCardRow | undefined => view.data.find((row) => row.id === id);
const tagNames = (issue: IssueCardRow | undefined): string[] => (issue?.tags ?? []).map((t) => t.name);

const alice = await makeClient("alice");
const bob = await makeClient("bob");

// --- pagination over the seeded corpus ---
await waitFor(() => alice.page1.data.length === PAGE_SIZE, "first window hydrates full");
assert.equal(alice.page1.data[0].id, `seed-${String(SEEDED - 1).padStart(6, "0")}`, "newest seed tops the window");
assert.equal(alice.page1.resultType, "complete", "the hydrated window reports complete");

const wide = alice.store.materialize(issuesPageQuery({ limit: PAGE_SIZE * 2 }));
await waitFor(() => wide.data.length === PAGE_SIZE * 2, "the widened window grows to the larger limit");
assert.deepEqual(
  wide.data.slice(0, PAGE_SIZE).map((r) => r.id),
  alice.page1.data.map((r) => r.id),
  "the widened window keeps the first window's rows on top, in order",
);

// --- the user cast ---
const cast = alice.store.materialize(usersQuery());
await waitFor(() => cast.data.length >= 6, "the seeded user cast hydrates");

// --- a fresh write enters the live TOP window on every client, over real wires ---
alice.mutate.createIssue({
  id: "i1",
  title: "ship the demo",
  status: "todo",
  priority: "high",
  owner: "alice",
  tags: [
    { id: "t-i1-demo", name: "demo" },
    { id: "t-i1-release", name: "release" },
  ],
  description: "The issue tracker should be usable for real triage.",
  descriptionCommentId: "c-i1-desc",
  createdAt: Date.now(),
});
assert.equal(alice.page1.data[0].id, "i1", "optimistic create tops alice's window instantly");
await waitFor(() => bob.page1.data[0]?.id === "i1", "bob's window receives the issue live over the tunnel");
assert.deepEqual(tagNames(byId(bob.page1, "i1")), ["demo", "release"]);
assert.equal(byId(bob.page1, "i1")?.commentCount, 1, "the card's scalar comment count starts at 1");

// --- field + tag + comment edits converge across two live clients ---
alice.mutate.setPriority({ id: "i1", priority: "urgent", updatedAt: Date.now() });
bob.mutate.setStatus({ id: "i1", status: "in-progress", updatedAt: Date.now() });
alice.mutate.addTag({ id: "t-i1-localfirst", issueId: "i1", name: "local-first", updatedAt: Date.now() });
await waitFor(
  () =>
    byId(alice.page1, "i1")?.priority === "urgent" &&
    byId(bob.page1, "i1")?.status === "in-progress" &&
    tagNames(byId(bob.page1, "i1")).includes("local-first"),
  "field + tag edits converge",
);
alice.mutate.addComment({
  id: "c-i1-2",
  issueId: "i1",
  body: "Added comments as first-class issue context.",
  createdAt: Date.now(),
  updatedAt: Date.now(),
});
await waitFor(() => byId(bob.page1, "i1")?.commentCount === 2, "comment add bumps the scalar count on bob's window");

// --- server-pushed faceted search (a WHERE in the AST, materialized on the box) ---
const titled = alice.store.materialize(
  issuesPageQuery({ limit: PAGE_SIZE, filter: [{ axis: "title", value: "ship" }] }),
);
await waitFor(() => titled.data.some((r) => r.id === "i1"), "the title filter finds the matching issue");
const tagged = alice.store.materialize(
  issuesPageQuery({ limit: PAGE_SIZE, filter: [{ axis: "tag", value: "local-first" }] }),
);
await waitFor(() => tagged.data.some((r) => r.id === "i1"), "the tag filter (EXISTS over tag) finds the issue");

// --- the detail query folds the whole comment thread ---
const detail = alice.store.materialize(issueDetailQuery("i1"));
await waitFor(() => (detail.data?.comments.length ?? 0) >= 2, "the detail query hydrates the issue + full thread");
assert.equal(detail.data?.comments[0]?.author[0]?.id, "alice", "each comment folds its author in");

// --- projection over sync: an out-of-window issue arrives with only its SELECTED columns ---
const projected = alice.store.materialize(issueDetailQuery("seed-000000"));
await waitFor(() => projected.data !== null, "the projected detail query hydrates an out-of-window issue");
assert.ok(!("createdAt" in (projected.data ?? {})), "an UN-selected column was not synced — projected out");

// --- the activity feed reuses CommentCard against a different root ---
const feed = alice.store.materialize(recentCommentsQuery({ limit: FEED_LIMIT }));
await waitFor(() => feed.data.some((c) => c.id === "c-i1-2"), "the new comment appears in the activity feed");
assert.equal(feed.data.find((c) => c.id === "c-i1-2")?.issue[0]?.title, "ship the demo");

// --- context-scoped myIssues: the scope is the server's principal, not a wire arg ---
const aliceMine = alice.store.materialize(myIssuesQuery({ limit: PAGE_SIZE }, { user: "alice" }));
await waitFor(() => aliceMine.data.some((r) => r.id === "i1"), "alice's context-scoped window includes her issue");
const bobMine = bob.store.materialize(myIssuesQuery({ limit: PAGE_SIZE }, { user: "bob" }));
await waitFor(() => bobMine.resultType === "complete", "bob's context-scoped window hydrates");
assert.ok(!bobMine.data.some((r) => r.id === "i1"), "bob's identical call cannot reach alice's issue");

// --- teardown accounting against the LIVE daemon: one query = one subscription + one
// materialization; both reclaimed after the last reader leaves. The live follower runs the
// default 15s idle TTL, so the sweep wait is generous. ---
const sleepMs = (ms: number) => new Promise((r) => setTimeout(r, ms));
const isoClient = await createRindleClient({
  schema,
  mutators,
  user: () => "iso",
  api: { url: apiUrl, headers: { "x-user": "iso" } },
  daemon: { wsUrl: WS_URL },
  clientID: "live-smoke-iso",
});
await sleepMs(1500);
const teardownBase = await daemonStats();
const isoView = isoClient.store.materialize(issueDetailQuery("seed-000030"));
await waitFor(() => isoView.data !== null, "the isolated detail query hydrates");
const held = await daemonStats();
assert.equal(held.subscriptions, teardownBase.subscriptions + 1, "one query = one daemon subscription");
assert.equal(held.materializations, teardownBase.materializations + 1, "...and one engine materialization");
isoView.destroy();
await waitForStat((s) => s.subscriptions === teardownBase.subscriptions, "subscription torn down on unsubscribe");
await waitForStat(
  (s) => s.materializations <= teardownBase.materializations,
  "idle materialization reclaimed (not pinned)",
  60_000,
);
isoClient.close();

// --- the rejection story: policy says no spam → reason + snap-back ---
alice.mutate.createIssue({
  id: "i2",
  title: "totally spam offer",
  status: "todo",
  priority: "medium",
  owner: "alice",
  tags: [{ id: "t-i2-spam", name: "spam" }],
  description: "This should be rejected.",
  descriptionCommentId: "c-i2-desc",
  createdAt: Date.now(),
});
assert.equal(has(alice.page1, "i2"), true, "prediction visible locally");
await waitFor(() => alice.rejections.length === 1, "rejection reason surfaced");
assert.match(alice.rejections[0].reason, /spam/);
await waitFor(() => !has(alice.page1, "i2"), "rejected create snapped back");

// --- authority: bob deletes alice's issue → accepted-but-no-op → snap-back ---
bob.mutate.deleteIssue({ id: "i1" });
assert.equal(has(bob.page1, "i1"), false, "bob's optimistic delete is instant");
await waitFor(() => has(bob.page1, "i1"), "non-owner delete snapped back");
assert.equal(bob.rejections.length, 0, "no rejection — authority handled it in SQL");

// --- the owner CAN delete (also the run's own cleanup of i1) ---
alice.mutate.deleteIssue({ id: "i1" });
await waitFor(() => !has(bob.page1, "i1"), "owner delete propagated to bob");
assert.equal(bob.page1.data.length, PAGE_SIZE, "the window backfills to stay full");

alice.close();
bob.close();
api.kill("SIGKILL");
console.log(`live smoke passed against ${MASTER_URL} — the IVM client stack is live on the packed pair`);
// Unlike the local smoke, the daemons here (rightly) outlive the run — a lingering transport
// handle against the still-open tunnel keeps the loop alive after close(). Everything is
// asserted by this line; exit hard.
process.exit(0);
