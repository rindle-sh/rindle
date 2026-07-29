// Headless smoke for the demo app's API authority through the standalone Node shell
// (`server/api.ts`, child process)
// + the Rust `rindled` daemon + the same one-call client the browser uses — over a SEEDED
// corpus big enough that the client must paginate. Proves: idempotent bulk seeding, a live
// growing window (a wider limit is a superset of the narrower one, fresh writes entering the top
// of the window), tracker field/comment edits, the spam rejection (reason surfaces), and
// owner-enforced delete snap-back.

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";

import { createRindleClient } from "@rindle/optimistic";
import type { RindleClient } from "@rindle/optimistic";
import { initWasm } from "@rindle/wasm";
import type { ArrayView, ColsMap } from "@rindle/client";

import { FEED_LIMIT, mutators, schema, PAGE_SIZE } from "../shared/app-def.ts";
import { issuesPageQuery, myIssuesQuery } from "../src/components/IssueListItem.queries.ts";
import { issueDetailQuery } from "../src/components/IssueDetail.queries.ts";
import { recentCommentsQuery } from "../src/components/ActivityFeed.queries.ts";
import { usersQuery } from "../src/components/UserBadge.queries.ts";
import { seedStatements } from "../server/seed.ts";
import { buildPairBinaries, startPair } from "./pair-fixture.ts";

const appRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const SEEDED = 120; // 2 full windows + a partial third
const DAEMON_TOKEN = "smoke-token";

await initWasm();

// Safety net: never let a stalled assertion hang the run (open daemon/api child handles keep node
// alive). Unref'd so it doesn't keep the loop alive on a clean exit.
setTimeout(() => {
  console.error("smoke watchdog: exceeded 180s, forcing exit");
  process.exit(1);
}, 180_000).unref();

// tier 1: the ONE topology (design 214) — a `rindle-replicator` write-master + a follower
// `rindled`, booted by the shared `startPair` fixture (the same pair the ssr/swarm e2e use). Both
// boot with NO schema DDL: `migrations/` apply against the LIVE master (minting `ddl` change-log
// entries) and reach the follower over the stream, exactly the production migrate flow.
buildPairBinaries();
const dataDir = mkdtempSync(join(tmpdir(), "issue-smoke-"));
// Short idle TTL so the teardown assertion can observe a zero-subscriber materialization get
// reclaimed promptly (active views keep their own materializations alive regardless).
const pair = await startPair({ dataDir, token: DAEMON_TOKEN, idleTtlMs: 1000 });
const masterUrl = pair.masterUrl;
const daemonUrl = pair.daemonUrl;
const daemonWsUrl = pair.daemonWsUrl;

// Writes (the seed) go to the MASTER; the follower serves reads/stats/ws below.
const seeded = await pair.master.executeSqlTxn({
  idempotencyKey: "smoke-seed",
  statements: seedStatements(SEEDED),
});
assert.equal(seeded.applied, true);
const replayed = await pair.master.executeSqlTxn({
  idempotencyKey: "smoke-seed",
  statements: seedStatements(SEEDED),
});
assert.equal(replayed.applied, false, "the seed replay is absorbed by the idempotency key");

// tier 2: the demo's standalone API shell
const api = spawn(
  process.execPath,
  ["--conditions=@rindle/source", join(appRoot, "server/api.ts")],
  {
    stdio: ["ignore", "pipe", "inherit"],
    env: {
      ...process.env,
      RINDLE_DAEMON_URL: daemonUrl,
      RINDLE_DAEMON_TOKEN: DAEMON_TOKEN,
      RINDLE_REPLICATOR_URL: masterUrl,
      RINDLE_DATABASE_TOKEN: pair.sqlToken,
      API_PORT: "0",
    },
  },
);
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

// tier 3: two clients, two users — each holds its own live first window
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
    // The acting principal for a shared mutator's `ctx.user` (the author), matching the `x-user`
    // header the mutation authenticates as — the client predicts under the SAME identity the server
    // stamps authoritatively.
    user: () => user,
    api: { url: apiUrl, headers: { "x-user": user } },
    daemon: { wsUrl: daemonWsUrl },
    clientID: `smoke-${user}`,
    onRejected: (envelope, reason) => rejections.push({ name: envelope.name, reason }),
  });
  return { ...client, rejections, page1: client.store.materialize(issuesPageQuery({ limit: PAGE_SIZE })) };
};

const waitFor = (cond: () => boolean, label: string, ms = 5000): Promise<void> =>
  new Promise((resolveWait, reject) => {
    const start = Date.now();
    const tick = () => {
      if (cond()) return resolveWait();
      if (Date.now() - start > ms) return reject(new Error(`timeout: ${label}`));
      setTimeout(tick, 15);
    };
    tick();
  });

// The daemon's read-only control-plane stats. `subscriptions` is the live per-connection active
// query count — it drops the instant a subscription is torn down (the engine materialization it
// backed then goes idle for a later sweep; it is NOT pinned).
interface DaemonStats {
  subscriptions: number;
  materializations: number;
  leases: number;
  connections: number;
}
const daemonStats = async (): Promise<DaemonStats> => {
  // Mirror the daemon-client's proven control-plane POST: a JSON body + content-type (so the
  // single-threaded HTTP handler reads a bounded body, never blocking on EOF), bearer auth, and a
  // hard timeout so a stalled control plane fails fast instead of hanging the run.
  const res = await fetch(`${daemonUrl}/stats`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${DAEMON_TOKEN}` },
    body: "{}",
    signal: AbortSignal.timeout(4000),
  });
  if (!res.ok) throw new Error(`/stats failed: ${res.status}`);
  return (await res.json()) as DaemonStats;
};
const waitForStat = async (pred: (s: DaemonStats) => boolean, label: string, ms = 5000): Promise<DaemonStats> => {
  const start = Date.now();
  for (;;) {
    const stats = await daemonStats();
    if (pred(stats)) return stats;
    if (Date.now() - start > ms) throw new Error(`timeout: ${label}`);
    await new Promise((r) => setTimeout(r, 25));
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
// The window's lifecycle, surfaced on the view: once the server snapshot has landed it is
// server-authoritative (drives the UI's loading/end affordances).
assert.equal(alice.page1.resultType, "complete", "the hydrated window reports complete");

// growing limit: a wider window is a SUPERSET of the first — the same rows on top, in order, with
// the next page appended below.
const wide = alice.store.materialize(issuesPageQuery({ limit: PAGE_SIZE * 2 }));
await waitFor(() => wide.data.length === PAGE_SIZE * 2, "the widened window grows to the larger limit");
assert.deepEqual(
  wide.data.slice(0, PAGE_SIZE).map((r) => r.id),
  alice.page1.data.map((r) => r.id),
  "the widened window keeps the first window's rows on top, in order",
);
assert.equal(
  wide.data[PAGE_SIZE].id,
  `seed-${String(SEEDED - 1 - PAGE_SIZE).padStart(6, "0")}`,
  "the widened window continues exactly where the first ended",
);

// --- the user cast: a whole-table query (small, unlike issues) powers the pickers ---
const cast = alice.store.materialize(usersQuery());
await waitFor(() => cast.data.length >= 6, "the seeded user cast hydrates");
assert.ok(
  cast.data.some((u) => u.name === "Amara Okafor"),
  "seeded users carry real display names",
);
assert.deepEqual(
  cast.data.map((u) => u.name),
  [...cast.data].map((u) => u.name).sort((a, b) => a.localeCompare(b)),
  "the user cast is alphabetized by name",
);

// --- a fresh write enters the live TOP window (newest createdAt) on every client ---
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
assert.equal(alice.page1.data[0].priority, "high");
// The `IssueCard` fragment folds in the owner (a user row) + tags (alphabetized) and a SCALAR
// comment count (`countAs` over the whole thread — the card carries the count, not the rows; the
// detail query fetches the rows themselves). A fresh issue has exactly its one description comment.
assert.deepEqual(tagNames(alice.page1.data[0]), ["demo", "release"]);
assert.equal(alice.page1.data[0].owner[0]?.id, "alice", "owner user joined onto the issue (IssueCard)");
assert.equal(alice.page1.data[0].commentCount, 1, "the card's scalar comment count starts at 1 (the description)");
await waitFor(() => bob.page1.data[0]?.id === "i1", "bob's window receives the issue live");
assert.equal(bob.page1.data.length, PAGE_SIZE, "the window stays a window — the 50th row fell out");

// --- field + tag + comment edits converge ---
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
await waitFor(
  () => byId(bob.page1, "i1")?.commentCount === 2,
  "comment add bumps the card's scalar comment count on bob's window (1 description + 1 reply)",
);

// --- server-pushed faceted search: the daemon materializes an ALREADY-filtered window (a WHERE in
// the AST the authority resolves), not a client scan over a loaded window ---
const titled = alice.store.materialize(
  issuesPageQuery({ limit: PAGE_SIZE, filter: [{ axis: "title", value: "ship" }] }),
);
await waitFor(() => titled.data.some((r) => r.id === "i1"), "the title filter finds the matching issue");
assert.ok(
  titled.data.length >= 1 && titled.data.every((r) => r.title.toLowerCase().includes("ship")),
  "the title-filtered window excludes every non-matching title",
);

// a relationship facet (tag) becomes a correlated EXISTS over the whole tag table
const tagged = alice.store.materialize(
  issuesPageQuery({ limit: PAGE_SIZE, filter: [{ axis: "tag", value: "local-first" }] }),
);
await waitFor(() => tagged.data.some((r) => r.id === "i1"), "the tag filter (EXISTS over tag) finds the issue");
assert.ok(
  tagged.data.length >= 1 && tagged.data.every((r) => r.tags.some((t) => t.name === "local-first")),
  "every issue in the tag-filtered window actually carries the tag",
);

// --- the detail query (IssueDetailCard) folds the WHOLE comment thread the card reduces to a count;
// each comment carries its author (the shared CommentCard fragment) ---
const detail = alice.store.materialize(issueDetailQuery("i1"));
await waitFor(() => (detail.data?.comments.length ?? 0) >= 2, "the detail query hydrates the issue + full thread");
assert.equal(detail.data?.id, "i1", "detail returns the requested issue");
assert.ok((detail.data?.comments.length ?? 0) >= 2, "detail returns the full comment thread");
assert.equal(
  detail.data?.comments[0]?.body,
  "The issue tracker should be usable for real triage.",
  "the earliest comment is the description",
);
assert.equal(
  detail.data?.comments[0]?.author[0]?.id,
  "alice",
  "each comment folds its author in (CommentCard fragment, reused by the activity feed)",
);

// --- PROJECTION over sync: `IssueDetailCard` selects FEWER issue columns than the list card (no
// `createdAt`/`updatedAt`). An issue open ONLY in the detail — not in any list/board window — syncs
// a genuinely narrower row: the un-selected columns are never sent and read back Absent (omitted
// from the row object). seed-000000 is the oldest seed, well outside the newest-N windows alice
// holds, so its shared row is contributed by the projected detail query alone. ---
const projected = alice.store.materialize(issueDetailQuery("seed-000000"));
await waitFor(() => projected.data !== null, "the projected detail query hydrates an out-of-window issue");
assert.equal(projected.data?.id, "seed-000000", "the projected row hydrated");
assert.ok(typeof projected.data?.title === "string", "a SELECTED column (title) synced");
assert.ok(typeof projected.data?.ownerId === "string", "a SELECTED column (ownerId) synced");
assert.ok(!("createdAt" in (projected.data ?? {})), "an UN-selected column (createdAt) was not synced — projected out");
assert.ok(!("updatedAt" in (projected.data ?? {})), "an UN-selected column (updatedAt) was not synced — projected out");

// --- the BOARD VIEW: a status column is just `issuesPage` + a `status:` facet — the SAME named
// query and the SAME IssueCard fragment as the list. (bob set i1 to "in-progress" above.) ---
const inProgress = alice.store.materialize(
  issuesPageQuery({ limit: PAGE_SIZE, filter: [{ axis: "status", value: "in-progress" }] }),
);
await waitFor(() => inProgress.data.some((r) => r.id === "i1"), "the in-progress board column finds the issue");
assert.ok(
  inProgress.data.every((r) => r.status === "in-progress"),
  "every card in the in-progress column actually carries that status",
);
assert.equal(byId(inProgress, "i1")?.commentCount, 2, "a board card carries the same accurate comment count");

// --- the ACTIVITY FEED (recentComments): a DIFFERENT root (the comment table, newest first) reusing
// the SAME CommentCard fragment, with the issue folded in (FeedItem → IssueLink). The reply we just
// added is the newest comment, so it sits at the top. ---
const feed = alice.store.materialize(recentCommentsQuery({ limit: FEED_LIMIT }));
await waitFor(() => feed.data.some((c) => c.id === "c-i1-2"), "the new comment appears in the activity feed");
const feedItem = feed.data.find((c) => c.id === "c-i1-2");
assert.equal(feedItem?.body, "Added comments as first-class issue context.", "the feed shows the comment body");
assert.equal(feedItem?.author[0]?.id, "alice", "the feed reuses CommentCard — the author is folded in");
assert.equal(feedItem?.issue[0]?.id, "i1", "FeedItem folds the parent issue in (IssueLink)");
assert.equal(feedItem?.issue[0]?.title, "ship the demo", "so the feed can label which issue a comment is on");

// --- a CONTEXT-SCOPED query (myIssues): scoped to the AUTHENTICATED user via ctx, NOT a wire arg.
// The owner travels in the request (the `x-user` header the API tier authenticates), so the
// authority builds `ownerId = <its own principal>`; the wire carries only `{ limit }`. alice owns the
// issue she just created (i1), so it's in HER window — and bob's BYTE-IDENTICAL call (same name, same
// args) can't reach it, because there's no owner arg to tamper with. ---
const aliceMine = alice.store.materialize(myIssuesQuery({ limit: PAGE_SIZE }, { user: "alice" }));
await waitFor(() => aliceMine.data.some((r) => r.id === "i1"), "alice's context-scoped window includes her own issue");
assert.ok(
  aliceMine.data.every((r) => r.ownerId === "alice"),
  "every row in alice's myIssues is owned by alice (the server scoped it to her authenticated principal)",
);
const bobMine = bob.store.materialize(myIssuesQuery({ limit: PAGE_SIZE }, { user: "bob" }));
await waitFor(() => bobMine.resultType === "complete", "bob's context-scoped window hydrates");
assert.ok(
  !bobMine.data.some((r) => r.id === "i1"),
  "bob's identical myIssues call cannot reach alice's issue — the scope is the server's principal, not a wire arg",
);

// --- TEARDOWN: a query's whole daemon footprint is reclaimed once its last reader unsubscribes —
// nothing stays perma-pinned. Measured on a FRESH client that holds nothing but its lmid system
// query, so the /stats delta is attributable to exactly one query. A relationship-bearing query
// (issueDetail folds owner + tags + comments) is ONE pipeline — relationships are source connections
// inside it, not extra materializations — so it adds exactly +1 subscription and +1 materialization.
// On destroy the subscription drops promptly (on unsubscribe) and the now-idle materialization is
// reclaimed by the idle sweep (the one-shot lease just expires on its TTL). ---
const sleepMs = (ms: number) => new Promise((r) => setTimeout(r, ms));
const isoClient = await createRindleClient({
  schema,
  mutators,
  user: () => "iso",
  api: { url: apiUrl, headers: { "x-user": "iso" } },
  daemon: { wsUrl: daemonWsUrl },
  clientID: "smoke-iso",
});
await sleepMs(1000); // let init + the lmid system query settle into the baseline
const teardownBase = await daemonStats();
const isoView = isoClient.store.materialize(issueDetailQuery("seed-000030"));
await waitFor(() => isoView.data !== null, "the isolated detail query hydrates");
const held = await daemonStats();
assert.equal(
  held.subscriptions,
  teardownBase.subscriptions + 1,
  "a relationship-bearing query is ONE daemon subscription (relationships are in-pipeline source connections)",
);
assert.equal(held.materializations, teardownBase.materializations + 1, "...and ONE engine materialization (one pipeline)");

isoView.destroy();
const afterUnsub = await waitForStat((s) => s.subscriptions === teardownBase.subscriptions, "subscription torn down on unsubscribe", 10_000);
assert.equal(afterUnsub.subscriptions, teardownBase.subscriptions, "active subscriptions returned to baseline the moment the reader unsubscribed");
const afterSweep = await waitForStat((s) => s.materializations === teardownBase.materializations, "idle materialization reclaimed (not pinned)", 15_000);
assert.equal(afterSweep.materializations, teardownBase.materializations, "the engine materialization was reclaimed — nothing stayed pinned");
isoClient.close();

// --- the REJECTION story: policy says no spam → reason + snap-back ---
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
assert.equal(has(bob.page1, "i2"), false, "bob never saw the spam");

// --- the AUTHORITY story: bob deletes alice's issue → accepted-but-no-op → snap-back ---
bob.mutate.deleteIssue({ id: "i1" });
assert.equal(has(bob.page1, "i1"), false, "bob's optimistic delete is instant");
await waitFor(() => has(bob.page1, "i1"), "non-owner delete snapped back");
assert.equal(bob.rejections.length, 0, "no rejection — authority handled it in SQL");

// --- the owner CAN delete ---
alice.mutate.deleteIssue({ id: "i1" });
await waitFor(() => !has(bob.page1, "i1"), "owner delete propagated to bob");
assert.equal(bob.page1.data.length, PAGE_SIZE, "the window backfills to stay full");

alice.close();
bob.close();
api.kill("SIGKILL");
pair.cleanup();
rmSync(dataDir, { recursive: true, force: true });
console.log("issue-tracker smoke passed (seeded + paginated, replicator pair)");
