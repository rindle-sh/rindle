// Real-browser e2e for the SSR→SPA handoff (SSR-DESIGN.md §6) — the path the PR could not run
// without a browser/wasm. Boots the full dev stack, then drives headless Chrome via the DevTools
// Protocol to assert the WHOLE handoff:
//   1. first paint is server-rendered seeded data (no boot splash),
//   2. React hydrates with NO hydration mismatch (the seed Store backs server + first client render),
//   3. the client-only wasm engine then boots and opens its live ws to the daemon (SPA is live),
//   4. the seeded rows survive the swap to the live store (no empty flash).
// Skips cleanly (exit 0) if no Chrome is present.

import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync, accessSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";

import { buildPairBinaries, startPair, type PairHandle } from "./pair-fixture.ts";
import { seedStatements } from "../server/seed.ts";

const appRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const SEEDED = 120;
const DAEMON_TOKEN = "ssr-hydrate-token";
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
  console.log("ssr-hydrate: no Chrome found — skipping (exit 0)");
  process.exit(0);
}

const watchdog = setTimeout(() => {
  console.error("ssr-hydrate watchdog: exceeded 150s, forcing exit");
  killAll();
  process.exit(1);
}, 150_000);
watchdog.unref();

// --- tier 1: the pair (design 214) — master + bare follower, migrated at the master ------------
buildPairBinaries();
const dataDir = mkdtempSync(join(tmpdir(), "issue-ssr-hydrate-"));
pair = await startPair({ dataDir, token: DAEMON_TOKEN });
const daemonUrl = pair.daemonUrl;
const daemonWsUrl = pair.daemonWsUrl;
const followerWsPort = Number(daemonWsUrl.split(":").pop());
assert.equal(
  (await pair.master.executeSqlTxn({ idempotencyKey: "ssr-hydrate-seed", statements: seedStatements(SEEDED) })).applied,
  true,
);
await pair.followerCatchUp("SELECT count(*) FROM issue", SEEDED, "seed");

// --- tier 2: Vite dev w/ SSR + /api server routes; hand daemon URLs to the server/client -------
process.env.RINDLE_DAEMON_URL = daemonUrl;
process.env.RINDLE_DAEMON_TOKEN = DAEMON_TOKEN;
process.env.RINDLE_REPLICATOR_URL = pair.masterUrl;
process.env.VITE_DAEMON_WS = daemonWsUrl;
const { createServer } = await import("vite");
const vite = await createServer({ root: appRoot, server: { host: "127.0.0.1", port: 0 }, logLevel: "error" });
await vite.listen();
const vaddr = vite.httpServer?.address();
const base = `http://127.0.0.1:${typeof vaddr === "object" && vaddr ? vaddr.port : 0}`;

// --- headless Chrome over the DevTools Protocol ----------------------------------------------
const userDataDir = mkdtempSync(join(tmpdir(), "ssr-hydrate-chrome-"));
const chrome = spawn(
  chromeBin,
  ["--headless=new", "--disable-gpu", "--no-sandbox", "--remote-debugging-port=0", `--user-data-dir=${userDataDir}`, "about:blank"],
  { stdio: ["ignore", "pipe", "pipe"] },
);
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

// A tiny CDP client over the browser-level ws (flatten mode: session events carry `sessionId`).
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
  } else if (msg.method) {
    for (const h of eventHandlers) h(msg.method, msg.params);
  }
});
const send = (method: string, params: Record<string, unknown> = {}, sessionId?: string): Promise<any> => {
  const id = ++nextId;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    sock.send(JSON.stringify({ id, method, params, sessionId }));
  });
};

// Attach to a fresh page target (flatten → one ws for everything).
const { targetId } = await send("Target.createTarget", { url: "about:blank" });
const { sessionId } = await send("Target.attachToTarget", { targetId, flatten: true });

const wsCreated: string[] = [];
const consoleErrors: string[] = [];
eventHandlers.push((method, params) => {
  if (params?.sessionId && params.sessionId !== sessionId) return;
  if (method === "Network.webSocketCreated") wsCreated.push(params.url);
  if (method === "Runtime.consoleAPICalled" && params.type === "error") {
    consoleErrors.push((params.args ?? []).map((a: any) => String(a.value ?? a.description ?? "")).join(" "));
  }
  if (method === "Runtime.exceptionThrown") {
    consoleErrors.push(params.exceptionDetails?.exception?.description ?? params.exceptionDetails?.text ?? "exception");
  }
});
await send("Page.enable", {}, sessionId);
await send("Runtime.enable", {}, sessionId);
await send("Network.enable", {}, sessionId);

const evalText = async (expr: string): Promise<string> => {
  const r = await send("Runtime.evaluate", { expression: expr, returnByValue: true, awaitPromise: true }, sessionId);
  if (r.exceptionDetails) throw new Error(`eval threw: ${r.exceptionDetails.text}`);
  return String(r.result?.value ?? "");
};
const poll = async (fn: () => Promise<boolean>, label: string, ms = 30_000): Promise<void> => {
  const start = Date.now();
  for (;;) {
    if (await fn().catch(() => false)) return;
    if (Date.now() - start > ms) throw new Error(`timeout: ${label}`);
    await new Promise((r) => setTimeout(r, 150));
  }
};

let failed = false;
const check = (name: string, cond: boolean) => {
  console.log(`${cond ? "  ok" : "FAIL"} - ${name}`);
  if (!cond) failed = true;
};

try {
  // The newest seeded issue's rendered title (createdAt desc). Its id `seed-000119` lives only in
  // the SSR dehydration <script> (consumed at hydration), so assert on the VISIBLE list markup.
  const NEWEST_TITLE = "#119 owner check bypassed by stale cache";
  const titleVisible = `String(!!document.body && document.body.innerText.includes(${JSON.stringify(NEWEST_TITLE)}))`;
  const isLoading = `String(!!document.body && document.body.innerText.includes("Loading issues"))`;
  await send("Page.navigate", { url: `${base}/` }, sessionId);

  // 1. First paint is the server-rendered issue list (not the old "Starting Rindle…" splash).
  await poll(async () => (await evalText(titleVisible)) === "true", "server-rendered issue list appears", 12_000);
  check("first paint shows the newest seeded issue (server-rendered list)", true);
  check("first paint is NOT the old boot splash", (await evalText(`document.body.innerText.includes("Starting Rindle")`)) === "false");

  // 2. The live wasm engine boots and opens its ws to the daemon — the SSR→SPA handoff completing.
  await poll(() => Promise.resolve(wsCreated.some((u) => u.includes(String(followerWsPort)))), "client opens live ws to the daemon", 15_000);
  check("the live wasm engine booted and opened a ws to the daemon (SPA is live)", true);

  // 3. After the live engine takes over, the list is still populated (the seed bridged the swap)
  //    and did NOT get stuck on the empty "Loading issues…" state.
  await new Promise((r) => setTimeout(r, 2000));
  check("the list stays populated after the swap to the live store (no empty flash / stall)", (await evalText(titleVisible)) === "true");
  check("the list is not stuck on the 'Loading issues…' empty state", (await evalText(isLoading)) === "false");

  // 4. No hydration mismatch AND no query-proxy ($$typeof / unknown table) error in the console.
  const badErr = consoleErrors.find((m) => /hydrat|did not match|unknown table|\$\$typeof|Minified React error #(418|423|425)/i.test(m));
  check(`no hydration / query-proxy error in the console${badErr ? ` (got: ${badErr.slice(0, 160)})` : ""}`, !badErr);
  if (consoleErrors.length) console.log("  (console errors seen: " + consoleErrors.map((e) => e.slice(0, 140)).join(" | ") + ")");
} catch (err) {
  console.error("ssr-hydrate harness error:", err);
  failed = true;
}

try {
  sock.close();
} catch {
  /* ignore */
}
await vite.close();
killAll();
rmSync(dataDir, { recursive: true, force: true });
rmSync(userDataDir, { recursive: true, force: true });
clearTimeout(watchdog);
if (failed) {
  console.error("ssr-hydrate e2e FAILED");
  process.exit(1);
}
console.log("issue-tracker SSR hydration e2e passed (real browser: SSR paint → clean hydrate → live wasm swap)");
process.exit(0);
