// Focused e2e for the SSR one-shot READ path (SSR-DESIGN.md §6) added by this PR:
//   rindled `/query`  ◄──  api-server readQuery/handleReadJson  ◄──  /api/rindle/read
// Boots the real Rust pair (design 214: write-master + follower) + the standalone demo API
// shell, seeds a corpus through the master, then POSTs named-query reads exactly as the SSR
// loader (src/ssr.ts) does and asserts the assembled first-paint rows. It also proves the read
// leaves NO subscriber (the leak-free contract) and that the seed key the loader uses matches
// what the chrome subscribes to.

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";

import { PAGE_SIZE, FEED_LIMIT } from "../shared/app-def.ts";
import { buildPairBinaries, startPair } from "./pair-fixture.ts";
import { seedStatements } from "../server/seed.ts";

const appRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const SEEDED = 120;
const DAEMON_TOKEN = "ssr-read-token";

setTimeout(() => {
  console.error("ssr-read watchdog: exceeded 120s, forcing exit");
  process.exit(1);
}, 120_000).unref();

// tier 1: the pair — master + bare follower, migrations applied at the master.
buildPairBinaries();
const dataDir = mkdtempSync(join(tmpdir(), "issue-ssr-read-"));
const pair = await startPair({ dataDir, token: DAEMON_TOKEN, idleTtlMs: 1000 });
const daemonUrl = pair.daemonUrl;
const seeded = await pair.master.executeSqlTxn({ idempotencyKey: "ssr-read-seed", statements: seedStatements(SEEDED) });
assert.equal(seeded.applied, true);
await pair.followerCatchUp("SELECT count(*) FROM issue", SEEDED, "seed");

// tier 2: the demo's REAL api server (the SSR loader dials THIS)
const api = spawn(
  process.execPath,
  ["--conditions=@rindle/source", join(appRoot, "server/api.ts")],
  {
    stdio: ["ignore", "pipe", "inherit"],
    env: {
      ...process.env,
      RINDLE_DAEMON_URL: daemonUrl,
      RINDLE_DAEMON_TOKEN: DAEMON_TOKEN,
      RINDLE_REPLICATOR_URL: pair.masterUrl,
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

// Exactly the loader's call shape (src/ssr.ts readViaApi): POST {name, args} with x-user.
interface ReadRow {
  cols: Record<string, unknown>;
  [rel: string]: unknown;
}
const read = async (name: string, args: unknown): Promise<{ rows: ReadRow[]; cvMin?: number; queryKey?: string }> => {
  const res = await fetch(`${apiUrl}/api/rindle/read`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-user": "ssr" },
    body: JSON.stringify({ name, args }),
  });
  if (!res.ok) throw new Error(`read ${name} failed (${res.status}): ${await res.text()}`);
  return (await res.json()) as { rows: ReadRow[]; cvMin?: number; queryKey?: string };
};

const daemonStats = async (): Promise<{ subscriptions: number; materializations: number }> => {
  const res = await fetch(`${daemonUrl}/stats`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${DAEMON_TOKEN}` },
    body: "{}",
  });
  return (await res.json()) as { subscriptions: number; materializations: number };
};

// --- the chrome's first-paint window: issuesPage({limit: PAGE_SIZE, filter: []}) — the EXACT key
//     the loader seeds (src/routes/__root.tsx) and AppChrome subscribes to. ---
const before = await daemonStats();
const page = await read("issuesPage", { limit: PAGE_SIZE, filter: [] });
assert.equal(page.rows.length, PAGE_SIZE, "the SSR window returns one full page of assembled rows");
assert.equal(
  page.rows[0].cols.id,
  `seed-${String(SEEDED - 1).padStart(6, "0")}`,
  "newest seed tops the SSR window (createdAt desc) — same order the live window hydrates",
);
// Assembled / nested: the IssueCard fragment folds the owner relationship in by alias.
assert.ok(Array.isArray(page.rows[0].owner), "the assembled row carries its `owner` relationship as a nested array");
assert.ok(typeof page.cvMin === "number", "the read returns a cvMin watermark baseline");

// --- the one-shot registered NO subscriber (leak-free, SSR-DESIGN.md §3.5). It may leave a warm
//     materialization (for the handoff) but it must NOT add a subscription. ---
const after = await daemonStats();
assert.equal(after.subscriptions, before.subscriptions, "a one-shot read leaves NO subscription (no leak)");

// --- users(): no-arg named query (args === null over the wire) ---
const users = await read("users", null);
assert.ok(users.rows.length >= 6, "the SSR user cast hydrates");
assert.ok(
  users.rows.some((r) => r.cols.name === "Amara Okafor"),
  "seeded users carry real display names in the assembled read",
);
// alphabetized by name (the query orders by name asc) — first-paint order matches the live list.
const names = users.rows.map((r) => String(r.cols.name));
assert.deepEqual(names, [...names].sort((a, b) => a.localeCompare(b)), "the SSR user cast is alphabetized");

// --- issueDetail(id): single-string arg + a .one() singular result (a single assembled row) ---
const detailId = `seed-${String(SEEDED - 1).padStart(6, "0")}`;
const detail = await read("issueDetail", detailId);
assert.equal(detail.rows.length, 1, "a .one() detail read returns exactly one assembled row");
assert.equal(detail.rows[0].cols.id, detailId, "the detail read returns the requested issue");
assert.ok(Array.isArray(detail.rows[0].comments), "the detail row folds its whole comment thread in (nested)");

// --- recentComments({limit}): the /activity feed's SSR seed ---
const feed = await read("recentComments", { limit: FEED_LIMIT });
assert.ok(feed.rows.length >= 1 && feed.rows.length <= FEED_LIMIT, "the activity feed read is a bounded window");
assert.ok(Array.isArray(feed.rows[0].author), "each feed row folds its author in (CommentCard fragment)");

// --- authority gate: an empty x-user is rejected by authorizeQuery (same gate as a lease) ---
const denied = await fetch(`${apiUrl}/api/rindle/read`, {
  method: "POST",
  headers: { "content-type": "application/json" }, // no x-user
  body: JSON.stringify({ name: "users", args: null }),
});
assert.equal(denied.status, 403, "a read with no authenticated user is rejected (403) before the daemon");

api.kill("SIGKILL");
pair.cleanup();
rmSync(dataDir, { recursive: true, force: true });
console.log("issue-tracker SSR read e2e passed (assembled first-paint rows, leak-free, gated)");
