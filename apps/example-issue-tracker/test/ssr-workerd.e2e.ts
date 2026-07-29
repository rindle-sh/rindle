// Verifies the Cloudflare deploy artifact on the REAL runtime: `CF=1 vite build` emits a workerd
// Worker (the @cloudflare/vite-plugin output), and `CF=1 vite preview` runs it in miniflare/workerd.
// We boot the Rust daemon, point the Worker's env at it (.dev.vars), then node-fetch the worker to
// prove — ON WORKERD, not node — that: (1) pages server-render seeded data (the SSR first-paint read
// runs IN-PROCESS through the authority → daemon, NOT a fetch back to the Worker's own origin —
// that self-loopback fails on Cloudflare's edge), (2) /api/rindle/read returns assembled
// rows through the Start server route → daemon, (3) static assets serve. This is the path that was broken before the
// plugin (the build targeted node + couldn't supply the Start manifest). Client hydration is the same
// bundle already proven in ssr-hydrate-prod, so this harness checks the SERVER (workerd) side.

import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";

import { buildPairBinaries, startPair, type PairHandle } from "./pair-fixture.ts";
import { seedStatements } from "../server/seed.ts";

const appRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const SEEDED = 120;
const DAEMON_TOKEN = "ssr-workerd-token";
const DAEMON_HTTP = 7600;
const DAEMON_WS = 7601;
const PREVIEW_PORT = 7694;
const PREVIEW_ORIGIN = `http://127.0.0.1:${PREVIEW_PORT}`;
const procs: ChildProcess[] = [];
const cleanupFiles: string[] = [];
let pair: PairHandle | undefined;
const killAll = () => {
  pair?.cleanup();
  for (const p of procs) {
    try {
      // `wrangler dev` spawns a workerd child in its own process group; SIGKILL of just wrangler
      // leaks the workerd (it keeps the port). Kill the whole group when we have one (detached).
      if (p.pid && (p as ChildProcess & { __group?: boolean }).__group) process.kill(-p.pid, "SIGKILL");
      else p.kill("SIGKILL");
    } catch {
      /* already gone */
    }
  }
  for (const f of cleanupFiles) {
    try {
      rmSync(f, { recursive: true, force: true });
    } catch {
      /* best effort */
    }
  }
};

// CF=1 activates the cloudflare() plugin in vite.config.ts for BOTH the build spawn (inherits env)
// and this process's `vite preview` (workerd).
process.env.CF = "1";
process.env.VITE_DAEMON_WS = `ws://127.0.0.1:${DAEMON_WS}`;

const watchdog = setTimeout(() => {
  console.error("ssr-workerd watchdog: exceeded 180s, forcing exit");
  killAll();
  process.exit(1);
}, 180_000);
watchdog.unref();

// --- tier 1: the pair, follower on fixed ports ----------------------------------------------------
buildPairBinaries();
const dataDir = mkdtempSync(join(tmpdir(), "issue-ssr-workerd-"));
cleanupFiles.push(dataDir);
pair = await startPair({ dataDir, token: DAEMON_TOKEN, httpPort: DAEMON_HTTP, wsPort: DAEMON_WS });
assert.equal(
  (await pair.master.executeSqlTxn({ idempotencyKey: "ssr-workerd-seed", statements: seedStatements(SEEDED) })).applied,
  true,
);
await pair.followerCatchUp("SELECT count(*) FROM issue", SEEDED, "seed");

// --- build the Cloudflare Worker (CF=1) + the client bundle (ws baked) ----------------------------
console.log("[workerd] CF=1 vite build...");
if (spawnSync("pnpm", ["run", "build"], { cwd: appRoot, stdio: ["ignore", "inherit", "inherit"], env: process.env }).status !== 0) {
  console.error("CF build failed");
  killAll();
  process.exit(1);
}
assert.ok(existsSync(join(appRoot, "dist/server/index.js")), "the plugin emitted the workerd Worker (dist/server/index.js)");
assert.ok(existsSync(join(appRoot, "dist/server/wrangler.json")), "the plugin emitted the deploy config (dist/server/wrangler.json)");

// --- run the BUILT Worker in workerd via `wrangler dev` against the generated deploy config -------
// `wrangler dev` runs the real workerd runtime locally (miniflare) with the worker + the assets
// binding. The deploy `vars` hold the PROD daemon URL / origin; we override them with `--var` to
// point at the local daemon + this preview origin (in prod these come from wrangler.jsonc + a
// `wrangler secret put DAEMON_TOKEN`). No network/login: `wrangler dev` is local by default.
const wrangler = spawn(
  "npx",
  [
    "wrangler", "dev", "-c", "dist/server/wrangler.json",
    "--ip", "127.0.0.1", "--port", String(PREVIEW_PORT),
    "--var", `DAEMON_ORIGIN:http://127.0.0.1:${DAEMON_HTTP}`,
    "--var", `DAEMON_TOKEN:${DAEMON_TOKEN}`,
    "--var", `PUBLIC_ORIGIN:${PREVIEW_ORIGIN}`,
  ],
  { cwd: appRoot, stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, WRANGLER_SEND_METRICS: "false" }, detached: true },
);
(wrangler as ChildProcess & { __group?: boolean }).__group = true; // kill the whole group (reaps workerd)
procs.push(wrangler);
await new Promise<void>((res, rej) => {
  const t = setTimeout(() => rej(new Error("wrangler dev did not become ready in 45s")), 45_000);
  const onData = (c: Buffer) => {
    const s = String(c);
    if (/Ready on|Listening on|127\.0\.0\.1:7694|localhost:7694/.test(s)) (clearTimeout(t), res());
  };
  wrangler.stdout?.on("data", onData);
  wrangler.stderr?.on("data", onData);
});
// wrangler prints "Ready" slightly before the socket accepts; give it a beat.
await new Promise((r) => setTimeout(r, 1500));

let failed = false;
const check = (name: string, cond: boolean, extra = "") => {
  console.log(`${cond ? "  ok" : "FAIL"} - ${name}${extra ? ` ${extra}` : ""}`);
  if (!cond) failed = true;
};

try {
  // 1. a page server-renders seeded data ON WORKERD — proves the Worker's SSR runs AND its
  //    in-process first-paint read (authority → daemon, no self-origin fetch) resolves inside workerd.
  const homeRes = await fetch(`${PREVIEW_ORIGIN}/`, { headers: { accept: "text/html" } });
  const home = await homeRes.text();
  check("GET / on workerd returns 200", homeRes.status === 200, `(got ${homeRes.status})`);
  check("the workerd-rendered page contains the newest seeded issue title (#119)", home.includes("#119 owner check bypassed by stale cache"));
  check("the workerd-rendered page embeds the dehydrated seed (seed-000119)", home.includes("seed-000119"));
  check("the workerd page is NOT the SSR error boundary ('Something went wrong')", !home.includes("Something went wrong"));

  // 2. the Worker's /api/rindle/read path runs the Start server route → daemon on workerd.
  const readRes = await fetch(`${PREVIEW_ORIGIN}/api/rindle/read`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-user": "ssr" },
    body: JSON.stringify({ name: "users", args: null }),
  });
  check("POST /api/rindle/read on workerd returns 200", readRes.status === 200, `(got ${readRes.status})`);
  const read = readRes.ok ? ((await readRes.json()) as { rows: Array<{ cols: Record<string, unknown> }> }) : { rows: [] };
  check("the workerd read returns assembled user rows from the daemon", read.rows.length >= 6 && read.rows.some((r) => r.cols.name === "Amara Okafor"));

  // 3. a hashed static asset is served (from the assets binding / dist/client).
  const m = home.match(/\/assets\/[A-Za-z0-9_.-]+\.js/);
  if (m) {
    const assetRes = await fetch(`${PREVIEW_ORIGIN}${m[0]}`);
    check("a hashed client asset serves with 200 + a JS content-type", assetRes.status === 200 && /javascript|ecmascript/.test(assetRes.headers.get("content-type") ?? ""), `(${m[0]} → ${assetRes.status})`);
  } else {
    check("found a hashed client asset reference in the HTML", false);
  }
} catch (err) {
  console.error("ssr-workerd harness error:", err);
  failed = true;
}

killAll();
clearTimeout(watchdog);
console.log(failed ? "ssr-workerd e2e FAILED" : "ssr-workerd e2e passed (Cloudflare Worker runs on workerd: SSR + in-process first-paint read + assets)");
process.exit(failed ? 1 : 0);
