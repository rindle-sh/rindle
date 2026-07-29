// Headless smoke for the LOAD TEST tier: boot the real Rust `rindled`, run the bot swarm in-process
// (the same authoritative path a browser hits), and prove (1) bot writes actually land on the daemon,
// (2) a real subscribed client sees a bot-created issue stream into its live window, and (3) the
// /metrics badge the footprint bar polls answers with live counters (bots, applied, cpu/mem shape).
//
// This is a MANUAL load harness, deliberately NOT in the `pnpm test` gate (that gate is the
// deterministic `smoke.e2e.ts` + the build). A load harness can't be a deterministic gate: it asserts
// against live, churning state under timing deadlines (the window's row count, viewer connects within
// N seconds), so its pass/fail tracks host speed, not correctness. The waits below are tuned to be
// tolerant, but tolerance ≠ determinism — run it to OBSERVE the tier under load: `pnpm smoke:swarm`.
//
// The swarm reads its config from env at import time, so we set a SMALL, fast config BEFORE importing
// it. Run: node --conditions=@rindle/source test/swarm.e2e.ts

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";

import { createRindleClient } from "@rindle/optimistic";
import { initWasm } from "@rindle/wasm";

import { mutators, schema, PAGE_SIZE } from "../shared/app-def.ts";
import { issuesPageQuery } from "../src/components/IssueListItem.queries.ts";
import { buildPairBinaries, startPair } from "./pair-fixture.ts";

const appRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const DAEMON_TOKEN = "swarm-smoke-token";
const METRICS_PORT = 7799;

await initWasm();

// tier 1: the pair (design 214) — write-master + bare follower, migrated at the master.
buildPairBinaries();
const dataDir = mkdtempSync(join(tmpdir(), "issue-swarm-smoke-"));
const pair = await startPair({ dataDir, token: DAEMON_TOKEN });
const daemonUrl = pair.daemonUrl;
const daemonWsUrl = pair.daemonWsUrl;

// tier 2: the swarm, configured small + fast, in THIS process (so we can read its handle + close it).
// Env must be set before the import — the swarm snapshots its config at module load. Bot
// mutations route to the master (RINDLE_REPLICATOR_URL); leases/reads come off the follower.
process.env.RINDLE_DAEMON_URL = daemonUrl;
process.env.RINDLE_DAEMON_TOKEN = DAEMON_TOKEN;
process.env.RINDLE_REPLICATOR_URL = pair.masterUrl;
delete process.env.DAEMON_WS_URL;
process.env.RINDLE_FLEET_WS = daemonWsUrl;
process.env.SWARM_BOTS = "20";
process.env.SWARM_RATE = "120";
process.env.SWARM_USERS = "5";
process.env.SWARM_MAX_ISSUES = "150";
process.env.SWARM_SUBSCRIBERS = "8";
process.env.SWARM_METRICS_PORT = String(METRICS_PORT);
const { startSwarm } = await import("../server/swarm.ts");
const swarm = await startSwarm();

// the demo's standalone API shell (for the watcher's query leases)
const apiProc = spawn(process.execPath, ["--conditions=@rindle/source", join(appRoot, "server/api.ts")], {
  stdio: ["ignore", "pipe", "inherit"],
  env: {
    ...process.env,
    RINDLE_DAEMON_URL: daemonUrl,
    RINDLE_DAEMON_TOKEN: DAEMON_TOKEN,
    RINDLE_REPLICATOR_URL: pair.masterUrl,
    API_PORT: "0",
  },
});
const apiUrl = await new Promise<string>((resolveUrl, reject) => {
  const timer = setTimeout(() => reject(new Error("api server did not start")), 10_000);
  let buffer = "";
  apiProc.stdout?.on("data", (chunk: Buffer) => {
    buffer += String(chunk);
    const m = buffer.match(/listening on (http:\/\/[^\s]+)/);
    if (m) {
      clearTimeout(timer);
      resolveUrl(m[1]);
    }
  });
});

// tier 3: a real client watching the live top window — it should see a bot issue arrive.
const client = await createRindleClient({
  schema,
  mutators,
  api: { url: apiUrl, headers: { "x-user": "watcher" } },
  daemon: { wsUrl: daemonWsUrl },
  clientID: "swarm-smoke-watcher",
});
const page = client.store.materialize(issuesPageQuery({ limit: PAGE_SIZE }));

const waitFor = (cond: () => boolean, label: string, ms = 12_000): Promise<void> =>
  new Promise((resolveWait, reject) => {
    const start = Date.now();
    const tick = () => {
      if (cond()) return resolveWait();
      if (Date.now() - start > ms) return reject(new Error(`timeout: ${label}`));
      setTimeout(tick, 25);
    };
    tick();
  });

// A bot-created issue (ids start with "s", owners "bot-NNN") streams into the live window.
await waitFor(
  () => page.data.some((row) => row.id.startsWith("s") && /^bot-\d+$/.test(row.ownerId)),
  "a bot issue streams into the live window",
);
const botIssue = page.data.find((row) => row.id.startsWith("s"))!;
assert.ok(botIssue.owner[0]?.id.startsWith("bot-"), "the bot issue carries its bot owner (sub join)");
assert.ok(botIssue.commentCount >= 1, "the bot issue's card counts its description comment (countAs)");

// The window fills to its limit and STAYS bounded under the write storm. createIssue dominates the
// bot action mix, so the corpus blows past PAGE_SIZE quickly — but how quickly depends on the host,
// and the first-bot-issue wait above is satisfied by the very first snapshot. So wait for the window
// to actually reach PAGE_SIZE rather than assuming it's full the instant one bot issue has streamed
// in; the limit then holds it there (it never grows past PAGE_SIZE) no matter how many bots write.
await waitFor(() => page.data.length === PAGE_SIZE, `the live window fills to ${PAGE_SIZE} and stays bounded`);
assert.equal(page.data.length, PAGE_SIZE, "the live window stays bounded under the swarm");

// /metrics answers with live load, viewer, and footprint counters
interface Metrics {
  bots: number;
  mutations: number;
  applied: number;
  subscribers: number;
  subscribersConnected: number;
  viewers: number | null;
  vcpus: number | null;
  memUsedBytes: number | null;
}
const readMetrics = async (): Promise<Metrics> =>
  (await (await fetch(`http://127.0.0.1:${swarm.metricsPort}/metrics`)).json()) as Metrics;

// the 8 subscriber bots lease + open ws; the daemon should report them (plus the watcher) as viewers
const waitForAsync = async (cond: () => Promise<boolean>, label: string, ms = 12_000): Promise<void> => {
  const start = Date.now();
  for (;;) {
    if (await cond()) return;
    if (Date.now() - start > ms) throw new Error(`timeout: ${label}`);
    await new Promise((r) => setTimeout(r, 50));
  }
};
await waitForAsync(async () => (await readMetrics()).subscribersConnected >= 8, "subscriber bots connect");

const metrics = await readMetrics();
assert.equal(metrics.bots, 20, "the badge reports the configured bot count");
assert.equal(metrics.subscribers, 8, "the badge reports the configured viewer count");
assert.ok(metrics.mutations > 0, "the swarm attempted mutations");
assert.ok(metrics.applied > 0, "the swarm applied mutations through the authoritative path");
assert.ok(metrics.subscribersConnected >= 8, "the subscriber bots are connected");
// daemon-truth viewers = 8 subscriber bots + the watcher client (+ its lmid connection)
assert.ok((metrics.viewers ?? 0) >= 9, "the daemon reports the ws viewers (subscriber bots + watcher)");
assert.ok((metrics.memUsedBytes ?? 0) > 0, "the footprint badge carries a resident-memory number");
assert.ok((metrics.vcpus ?? 0) >= 1, "the footprint badge carries a vCPU count");

client.close();
await swarm.close();
apiProc.kill("SIGKILL");
pair.cleanup();
rmSync(dataDir, { recursive: true, force: true });
console.log("issue-tracker swarm smoke passed (bots write + viewers watch, window stays live, /metrics reports)");
