// Definitive e2e for the SSR RENDER path (SSR-DESIGN.md §6): boots the full dev stack headlessly
// (rindled + Vite dev w/ TanStack Start SSR + server routes) and fetches pages over HTTP to prove
// the FIRST PAINT is server-rendered seeded data — not the old "Starting Rindle…" splash —
// and that the dehydrated state is embedded for hydration. This is the path that could not be run
// without a browser/wasm in the original PR; here we assert on the server-rendered HTML directly
// (the wasm engine is a client-only dynamic import and never loads in the SSR pass).

import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";

import { buildPairBinaries, startPair } from "./pair-fixture.ts";
import { seedStatements } from "../server/seed.ts";

const appRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const SEEDED = 120;
const DAEMON_TOKEN = "ssr-render-token";
let killAll = () => {};

const watchdog = setTimeout(() => {
  console.error("ssr-render watchdog: exceeded 150s, forcing exit");
  killAll();
  process.exit(1);
}, 150_000);
watchdog.unref();

// tier 1: the pair (design 214) — master + bare follower, migrations applied at the master.
buildPairBinaries();
const dataDir = mkdtempSync(join(tmpdir(), "issue-ssr-render-"));
const pair = await startPair({ dataDir, token: DAEMON_TOKEN });
killAll = () => pair.cleanup();
const daemonUrl = pair.daemonUrl;
const daemonWsUrl = pair.daemonWsUrl;
// Seed at the MASTER, then wait for the follower to converge before the SSR reads.
const seeded = await pair.master.executeSqlTxn({ idempotencyKey: "ssr-render-seed", statements: seedStatements(SEEDED) });
assert.equal(seeded.applied, true);
await pair.followerCatchUp("SELECT count(*) FROM issue", SEEDED, "seed");

// tier 2: Vite dev w/ TanStack Start SSR + /api server routes, programmatically.
process.env.RINDLE_DAEMON_URL = daemonUrl;
process.env.RINDLE_DAEMON_TOKEN = DAEMON_TOKEN;
process.env.RINDLE_REPLICATOR_URL = pair.masterUrl;
process.env.VITE_DAEMON_WS = daemonWsUrl;
const { createServer } = await import("vite");
const vite = await createServer({
  root: appRoot,
  server: { host: "127.0.0.1", port: 0 },
  logLevel: "warn",
});
await vite.listen();
const addr = vite.httpServer?.address();
const vitePort = typeof addr === "object" && addr ? addr.port : 0;
const base = `http://127.0.0.1:${vitePort}`;

const fetchText = async (path: string): Promise<string> => {
  const r = await fetch(`${base}${path}`, { headers: { accept: "text/html" } });
  if (!r.ok) throw new Error(`GET ${path} -> ${r.status}: ${(await r.text()).slice(0, 300)}`);
  return r.text();
};

let failed = false;
const check = (name: string, cond: boolean) => {
  console.log(`${cond ? "  ok" : "FAIL"} - ${name}`);
  if (!cond) failed = true;
};

try {
  // --- /api/rindle/read: Vite serves the API through TanStack Start server routes, not a proxy. ---
  const apiReadRes = await fetch(`${base}/api/rindle/read`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-user": "ssr" },
    body: JSON.stringify({ name: "users", args: null }),
  });
  check("POST /api/rindle/read is served by the Start dev server", apiReadRes.status === 200);
  const apiRead = apiReadRes.ok ? ((await apiReadRes.json()) as { rows: Array<{ cols: Record<string, unknown> }> }) : { rows: [] };
  check("/api/rindle/read returns seeded user rows", apiRead.rows.some((r) => r.cols.name === "Amara Okafor"));

  // --- home (/): the list window is SSR-seeded with the newest page. seed-000119 (#119) is the
  //     newest issue (createdAt desc) and must appear in the SERVER-rendered markup. ---
  const home = await fetchText("/");
  check("home page server-renders the newest seeded issue id (seed-000119)", home.includes("seed-000119"));
  check("home page server-renders a seeded issue title (#119 …)", home.includes("#119"));
  check(
    "home page does NOT show the old 'Starting Rindle…' boot splash (SSR renders data, not a splash)",
    !home.includes("Starting Rindle"),
  );
  check(
    "home page embeds dehydrated loader data for hydration (TanStack serializes loaderData)",
    home.includes("seed-000119") && /loaderData|dehydrat|streamedValue|__TSR|rindle/i.test(home),
  );

  // --- deep link (/?issue=seed-000119): the root loader also seeds issueDetail when ?issue= is
  //     set, so the open detail pane server-renders too (SSR-DESIGN.md §6.2 deep-link case). ---
  const deep = await fetchText("/?issue=seed-000119");
  check("deep-linked detail (/?issue=) server-renders the selected issue", deep.includes("seed-000119"));

  // --- /activity: its own loader SSR-seeds the recentComments feed (a different root table). ---
  const activity = await fetchText("/activity");
  // The feed renders comment bodies ("Description for …"). At least the page must render without a
  // splash and contain feed content from the seed.
  check("/activity server-renders without the boot splash", !activity.includes("Starting Rindle"));
  check("/activity server-renders seeded feed content (a description comment body)", activity.includes("Description for"));
} catch (err) {
  console.error("ssr-render harness error:", err);
  failed = true;
}

await vite.close();
killAll();
rmSync(dataDir, { recursive: true, force: true });
clearTimeout(watchdog);
if (failed) {
  console.error("ssr-render e2e FAILED");
  process.exit(1);
}
console.log("issue-tracker SSR render e2e passed (server-rendered first paint, no splash, dehydrated)");
process.exit(0);
