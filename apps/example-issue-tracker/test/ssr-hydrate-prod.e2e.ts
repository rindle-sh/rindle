// Real-browser e2e of the SSR→SPA handoff against the PRODUCTION build (the actual deploy
// artifact, production React — NO dev-only render logging). Decides whether the dev-mode
// "Loading issues…" stall is just dev-React tripping on the query proxy, or a real handoff bug.
// Builds the client with a FIXED daemon ws port, serves the prod build via `vite preview` (the
// Start plugin runs SSR there + same-origin /api/rindle/*), then drives headless Chrome.

import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync, accessSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";

import { buildPairBinaries, startPair, type PairHandle } from "./pair-fixture.ts";
import { seedStatements } from "../server/seed.ts";

const appRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const SEEDED = 120;
const DAEMON_TOKEN = "ssr-prod-token";
const DAEMON_HTTP = 7690;
const DAEMON_WS = 7691;
const PREVIEW_PORT = 7692;
const procs: ChildProcess[] = [];
let pair: PairHandle | undefined;
const killAll = () => {
  procs.forEach((p) => p.kill("SIGKILL"));
  pair?.cleanup();
};

function locateChrome(): string | null {
  for (const c of [process.env.CHROME_BIN, "/usr/bin/google-chrome", "/usr/bin/google-chrome-stable", "/usr/bin/chromium", "/usr/bin/chromium-browser"]) {
    if (!c) continue;
    try {
      accessSync(c);
      return c;
    } catch {
      /* keep looking */
    }
  }
  return null;
}
const chromeBin = locateChrome();
if (!chromeBin) {
  console.log("ssr-hydrate-prod: no Chrome found — skipping (exit 0)");
  process.exit(0);
}

const watchdog = setTimeout(() => {
  console.error("ssr-hydrate-prod watchdog: exceeded 180s, forcing exit");
  killAll();
  process.exit(1);
}, 180_000);
watchdog.unref();

// --- tier 1: the pair, follower on FIXED ports (the client bakes the ws url at build time) -----
buildPairBinaries();
const dataDir = mkdtempSync(join(tmpdir(), "issue-ssr-prod-"));
pair = await startPair({ dataDir, token: DAEMON_TOKEN, httpPort: DAEMON_HTTP, wsPort: DAEMON_WS });
const daemonUrl = pair.daemonUrl;
assert.equal(
  (await pair.master.executeSqlTxn({ idempotencyKey: "ssr-prod-seed", statements: seedStatements(SEEDED) })).applied,
  true,
);
await pair.followerCatchUp("SELECT count(*) FROM issue", SEEDED, "seed");

// --- build the PRODUCTION bundle with the fixed daemon ws baked in ----------------------------
console.log("[prod] vite build (VITE_DAEMON_WS baked)...");
const built = spawnSync("pnpm", ["run", "build"], {
  cwd: appRoot,
  stdio: ["ignore", "inherit", "inherit"],
  env: { ...process.env, VITE_DAEMON_WS: `ws://127.0.0.1:${DAEMON_WS}` },
});
if (built.status !== 0) {
  console.error("prod build failed");
  killAll();
  process.exit(1);
}

// --- serve the prod build with `vite preview` (Start plugin → SSR + same-origin /api) ---------
process.env.RINDLE_DAEMON_URL = daemonUrl;
process.env.RINDLE_DAEMON_TOKEN = DAEMON_TOKEN;
process.env.RINDLE_REPLICATOR_URL = pair.masterUrl;
const { preview } = await import("vite");
const previewServer = await preview({ root: appRoot, preview: { host: "127.0.0.1", port: PREVIEW_PORT, strictPort: true } });
const base = `http://127.0.0.1:${PREVIEW_PORT}`;

// sanity: the prod SSR server returns server-rendered rows over plain HTTP (no browser).
const ssrHtml = await (await fetch(`${base}/`)).text();
console.log("[prod] SSR HTML contains seed-000119:", ssrHtml.includes("seed-000119"));

// --- headless Chrome via CDP ------------------------------------------------------------------
const userDataDir = mkdtempSync(join(tmpdir(), "ssr-prod-chrome-"));
const chrome = spawn(chromeBin, ["--headless=new", "--disable-gpu", "--no-sandbox", "--remote-debugging-port=0", `--user-data-dir=${userDataDir}`, "about:blank"], { stdio: ["ignore", "pipe", "pipe"] });
procs.push(chrome);
const browserWsUrl = await new Promise<string>((res, rej) => {
  const t = setTimeout(() => rej(new Error("chrome devtools not ready")), 15_000);
  let buf = "";
  chrome.stderr?.on("data", (c: Buffer) => {
    buf += String(c);
    const m = buf.match(/DevTools listening on (ws:\/\/[^\s]+)/);
    if (m) (clearTimeout(t), res(m[1]));
  });
});

let nextId = 0;
const pending = new Map<number, { resolve: (v: any) => void; reject: (e: Error) => void }>();
const eventHandlers: Array<(method: string, params: any) => void> = [];
const sock = new WebSocket(browserWsUrl);
await new Promise<void>((res, rej) => {
  sock.addEventListener("open", () => res(), { once: true });
  sock.addEventListener("error", () => rej(new Error("cdp ws error")), { once: true });
});
sock.addEventListener("message", (e: MessageEvent) => {
  const msg = JSON.parse(String(e.data));
  if (typeof msg.id === "number" && pending.has(msg.id)) {
    const p = pending.get(msg.id)!;
    pending.delete(msg.id);
    msg.error ? p.reject(new Error(msg.error.message)) : p.resolve(msg.result);
  } else if (msg.method) for (const h of eventHandlers) h(msg.method, msg.params);
});
const send = (method: string, params: Record<string, unknown> = {}, sessionId?: string): Promise<any> => {
  const id = ++nextId;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    sock.send(JSON.stringify({ id, method, params, sessionId }));
  });
};
const { targetId } = await send("Target.createTarget", { url: "about:blank" });
const { sessionId } = await send("Target.attachToTarget", { targetId, flatten: true });
const wsCreated: string[] = [];
const consoleErrors: string[] = [];
eventHandlers.push((method, params) => {
  if (params?.sessionId && params.sessionId !== sessionId) return;
  if (method === "Network.webSocketCreated") wsCreated.push(params.url);
  if (method === "Runtime.consoleAPICalled" && params.type === "error") consoleErrors.push((params.args ?? []).map((a: any) => String(a.value ?? a.description ?? "")).join(" "));
  if (method === "Runtime.exceptionThrown") consoleErrors.push(params.exceptionDetails?.exception?.description ?? params.exceptionDetails?.text ?? "exception");
});
await send("Page.enable", {}, sessionId);
await send("Runtime.enable", {}, sessionId);
await send("Network.enable", {}, sessionId);
const evalText = async (expr: string): Promise<string> => {
  const r = await send("Runtime.evaluate", { expression: expr, returnByValue: true, awaitPromise: true }, sessionId);
  if (r.exceptionDetails) throw new Error(`eval threw: ${r.exceptionDetails.text}`);
  return String(r.result?.value ?? "");
};

let failed = false;
const check = (name: string, cond: boolean) => {
  console.log(`${cond ? "  ok" : "FAIL"} - ${name}`);
  if (!cond) failed = true;
};

try {
  await send("Page.navigate", { url: `${base}/` }, sessionId);
  console.log("--- timeline (prod): t ms: rows? | loading? | liveWs? ---");
  let everRows = false;
  let everLoading = false;
  let liveRows = false;
  // The newest seeded issue's rendered title (createdAt desc). Its id `seed-000119` lives only in
  // the SSR dehydration <script> (consumed at hydration), so we assert on the VISIBLE list markup.
  const NEWEST_TITLE = "#119 owner check bypassed by stale cache";
  for (let i = 0; i < 24; i++) {
    const rows = (await evalText(`String(!!document.body && document.body.innerText.includes(${JSON.stringify(NEWEST_TITLE)}))`).catch(() => "eval-err")) === "true";
    const loading = (await evalText(`String(!!document.body && document.body.innerText.includes("Loading issues"))`).catch(() => "eval-err")) === "true";
    const liveWs = wsCreated.some((u) => u.includes(String(DAEMON_WS)));
    if (rows) everRows = true;
    if (loading) everLoading = true;
    if (rows && !loading && liveWs) liveRows = true;
    if (i % 2 === 0) console.log(`  t=${i * 500}: rows=${rows} loading=${loading} liveWs=${liveWs}`);
    await new Promise((r) => setTimeout(r, 500));
  }
  const bodyText = await evalText(`document.body.innerText.slice(0,200)`).catch((e) => `eval-error: ${e}`);
  console.log("body.innerText[0:200]:", JSON.stringify(bodyText));
  console.log("wsCreated:", JSON.stringify(wsCreated));
  console.log("console errors:", consoleErrors.length, consoleErrors.map((e) => e.slice(0, 160)));

  check("the rendered issue list shows the newest seeded issue (server data, not a blank)", everRows);
  check("the live wasm engine opened a ws to the daemon", wsCreated.some((u) => u.includes(String(DAEMON_WS))));
  check("the list goes/stays live (rows present, not stuck on 'Loading issues…')", liveRows);
  check("the list never flashed the 'Loading issues…' empty state (the SSR seed bridged the swap)", !everLoading);
  check("no $$typeof / unknown-table proxy error in production console", !consoleErrors.some((m) => /unknown table|\$\$typeof/.test(m)));
} catch (err) {
  console.error("ssr-hydrate-prod harness error:", err);
  failed = true;
}

try {
  sock.close();
} catch {
  /* ignore */
}
previewServer.httpServer.close();
killAll();
await new Promise((r) => setTimeout(r, 300)); // let chrome release its user-data-dir
try {
  rmSync(dataDir, { recursive: true, force: true });
  rmSync(userDataDir, { recursive: true, force: true });
} catch {
  /* best-effort temp cleanup */
}
clearTimeout(watchdog);
console.log(failed ? "ssr-hydrate-prod e2e FAILED" : "ssr-hydrate-prod e2e passed (prod build: SSR paint → live wasm swap)");
process.exit(failed ? 1 : 0);
