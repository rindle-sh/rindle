// Browser smoke test — proves the shipped artifacts load and run in a REAL browser, not just
// Node. The Node side here is only a harness: it serves the built `dist/` + wasm `pkg/` over
// http (with `application/wasm` so `instantiateStreaming` works), launches headless Chrome at
// a page that imports `@rindle/wasm` via an import map, takes the *browser* init path
// (`init()` → `fetch` + `WebAssembly.instantiateStreaming`), runs a query (incl. `.one()` and
// `.start()`), and POSTs the verdict back. Requires the dist build (`pnpm build`) + system
// Chrome. No browser ⇒ skipped (exit 0). Run via `pnpm --filter @rindle/wasm test:browser`.
import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { readFile, mkdtemp, access } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname, normalize } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const clientDist = join(here, "../../client/dist");
const wasmDist = join(here, "../dist");
const wasmPkg = join(here, "../pkg");

const MIME = {
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".wasm": "application/wasm",
  ".html": "text/html; charset=utf-8",
  ".map": "application/json; charset=utf-8",
};

// Map a URL prefix to a served directory (path-traversal-guarded).
const ROUTES = [
  ["/client/dist/", clientDist],
  ["/wasm/dist/", wasmDist],
  ["/wasm/pkg/", wasmPkg],
];

function locateChrome() {
  const fromEnv = process.env.CHROME_BIN;
  const candidates = [
    fromEnv,
    "/usr/bin/google-chrome-stable",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
  ].filter(Boolean);
  return candidates;
}

const PAGE = /* html */ `<!doctype html>
<meta charset="utf-8">
<title>@rindle/wasm browser smoke</title>
<script>
  // Catch module-load / async failures that never reach the module's try/catch.
  function fail(msg) {
    navigator.sendBeacon("/result", JSON.stringify({ ok: false, error: String(msg) }));
  }
  addEventListener("error", (e) => fail((e.error && e.error.stack) || e.message || "window error"));
  addEventListener("unhandledrejection", (e) => fail((e.reason && e.reason.stack) || e.reason || "unhandledrejection"));
</script>
<script type="importmap">
  { "imports": { "@rindle/client": "/client/dist/index.js", "@rindle/wasm": "/wasm/dist/index.js" } }
</script>
<script type="module">
  import { table, string, number, createSchema, createWasmStore } from "@rindle/wasm";

  const eq = (a, b) => JSON.stringify(a) === JSON.stringify(b);

  async function run() {
    const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
    const schema = createSchema({ tables: [issue] });
    // createWasmStore → initWasm() → browser branch → init() → fetch + instantiateStreaming.
    const store = await createWasmStore(schema);
    await store.write((tx) => {
      tx.add("issue", { id: 1, title: "hello" });
      tx.add("issue", { id: 2, title: "world" });
    });

    const all = store.query.issue.orderBy("id", "asc").materialize();
    const one = store.query.issue.orderBy("id", "asc").one().materialize();
    const paged = store.query.issue.orderBy("id", "asc").start({ id: 1 }, { exclusive: true }).materialize();

    const checks = [
      [eq(all.data, [{ id: 1, title: "hello" }, { id: 2, title: "world" }]), "plural .data"],
      [eq(one.data, { id: 1, title: "hello" }), ".one() unwraps to the row"],
      [eq(paged.data.map((r) => r.id), [2]), ".start() exclusive paging"],
    ];
    const failed = checks.filter(([ok]) => !ok).map(([, name]) => name);
    if (failed.length) throw new Error("failed: " + failed.join(", "));
    return { rows: all.data.length, one: one.data, paged: paged.data.map((r) => r.id) };
  }

  run().then(
    (detail) => navigator.sendBeacon("/result", JSON.stringify({ ok: true, detail })),
    (err) => navigator.sendBeacon("/result", JSON.stringify({ ok: false, error: String(err && err.stack || err) })),
  );
</script>
<body>running…</body>
`;

async function serveFile(res, filePath) {
  try {
    const body = await readFile(filePath);
    const ext = filePath.slice(filePath.lastIndexOf("."));
    res.writeHead(200, { "content-type": MIME[ext] ?? "application/octet-stream" });
    res.end(body);
  } catch {
    res.writeHead(404).end("not found");
  }
}

async function main() {
  // Fail fast with a helpful message if the dist build is missing.
  try {
    await access(join(wasmDist, "index.js"));
    await access(join(clientDist, "index.js"));
    await access(join(wasmPkg, "rindle_bg.wasm"));
  } catch {
    console.error("✗ browser smoke needs the dist build + wasm pkg. Run `pnpm build` first.");
    process.exit(1);
  }

  let resolveResult;
  const result = new Promise((r) => (resolveResult = r));

  const server = createServer((req, res) => {
    if (req.method === "POST" && req.url === "/result") {
      let buf = "";
      req.on("data", (c) => (buf += c));
      req.on("end", () => {
        res.writeHead(204).end();
        try {
          resolveResult(JSON.parse(buf || "{}"));
        } catch (e) {
          resolveResult({ ok: false, error: "bad result payload: " + String(e) });
        }
      });
      return;
    }
    if (req.method === "GET" && (req.url === "/" || req.url === "/index.html")) {
      res.writeHead(200, { "content-type": MIME[".html"] });
      res.end(PAGE);
      return;
    }
    const url = (req.url ?? "").split("?")[0];
    for (const [prefix, dir] of ROUTES) {
      if (url.startsWith(prefix)) {
        const rel = normalize(url.slice(prefix.length)).replace(/^(\.\.(\/|\\|$))+/, "");
        return void serveFile(res, join(dir, rel));
      }
    }
    res.writeHead(404).end("not found");
  });

  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  const { port } = server.address();
  const pageUrl = `http://127.0.0.1:${port}/`;

  // Find a browser; skip gracefully if none is installed (keeps CI without Chrome green).
  let chrome = null;
  for (const bin of locateChrome()) {
    try {
      await access(bin);
      chrome = bin;
      break;
    } catch {
      /* try next */
    }
  }
  if (!chrome) {
    console.log("⊘ browser smoke skipped (no Chrome/Chromium found; set CHROME_BIN to enable)");
    server.close();
    return;
  }

  const userDataDir = await mkdtemp(join(tmpdir(), "rindle-browser-smoke-"));
  const child = spawn(
    chrome,
    [
      "--headless=new",
      "--disable-gpu",
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--no-first-run",
      "--no-default-browser-check",
      `--user-data-dir=${userDataDir}`,
      pageUrl,
    ],
    { stdio: ["ignore", "ignore", "pipe"] },
  );
  let chromeErr = "";
  child.stderr.on("data", (c) => (chromeErr += c));

  const timeout = new Promise((_, reject) =>
    setTimeout(() => reject(new Error("timed out waiting for the browser result (30s)")), 30_000).unref(),
  );

  let outcome;
  try {
    outcome = await Promise.race([result, timeout]);
  } catch (e) {
    outcome = { ok: false, error: String(e?.message ?? e) };
  } finally {
    child.kill("SIGKILL");
    server.close();
  }

  if (outcome.ok) {
    console.log(`✅ @rindle/wasm runs in headless Chrome (${chrome})`);
    console.log("   detail:", JSON.stringify(outcome.detail));
  } else {
    console.error("✗ browser smoke FAILED:", outcome.error);
    if (chromeErr.trim()) console.error("  chrome stderr:\n" + chromeErr.trim().split("\n").slice(-12).join("\n"));
    process.exitCode = 1;
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
