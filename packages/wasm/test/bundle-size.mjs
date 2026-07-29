// Size budget for the shipped wasm engine — the headline cost a browser pays to load
// `@rindle/wasm`. The README advertises a "tiny" gzipped wasm; this test is the guard that
// keeps that claim honest, so we find out *in CI* (not from a user's network tab) when a
// change accidentally drags the bundle up — a heavyweight dependency, a lost `opt-level = "z"`
// / `wasm-opt -Oz` pass, a feature that pulls in formatting/panic machinery, etc.
//
// Run AFTER the wasm artifact exists (`pnpm --filter @rindle/wasm build:wasm`, or repo-root
// `pnpm run build:wasm`). CI's `js` job builds it before `pnpm test`, so this runs there.
//
// The numbers that matter:
//   • gzip(level 9) of pkg/rindle_bg.wasm — what a CDN serves; the metric users feel on load.
//   • raw pkg/rindle_bg.wasm — what the browser parses/instantiates; a backstop in case
//     something compresses well but still bloats parse time.
//
// Budgets are deliberately a little above the current measured size (headroom for honest,
// incremental growth) but tight enough that pulling in something substantial trips the gate.
// Every run prints the live size + headroom, so even sub-budget creep is visible in the log
// and in review. When growth is INTENTIONAL, bump the matching *_BUDGET_KIB below and say why
// in the commit — that diff is the deliberate, reviewable signal that the budget moved.
import { readFileSync, statSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { fileURLToPath } from "node:url";

// ── budgets ───────────────────────────────────────────────────────────────────
// As of this commit the artifact measures ≈211 KiB gz / ≈547 KiB raw (wasm-release + `wasm-opt
// -Oz`, wasm-bindgen 0.2.122). Measured attribution for the growth past the old 210-KiB line:
// the reduce operator's sum/avg aggregates +3.2 KiB gz, the realtime multi-source client engine
// (hold-back overlay, routing probes, writable-predicate compiler) +1.5 KiB gz. Budgets sit
// ~4–5% above the measurement. Raise deliberately, never to chase an accidental regression —
// first find what got pulled in (e.g. `twiggy top pkg/rindle_bg.wasm`).
const GZIP_BUDGET_KIB = 220; // headline: gzipped transfer size (current ≈211 KiB)
const RAW_BUDGET_KIB = 575; //  backstop: uncompressed parse size (current ≈547 KiB)

const KIB = 1024;
const wasmPath = fileURLToPath(new URL("../pkg/rindle_bg.wasm", import.meta.url));

// Fail loudly (not silently green) if the artifact isn't built — a size guard that measures
// nothing is worse than no guard. Matches the browser-smoke test's fail-fast.
let raw;
try {
  raw = statSync(wasmPath).size;
} catch {
  console.error("✗ bundle-size: pkg/rindle_bg.wasm not found — run `pnpm run build:wasm` first.");
  process.exit(1);
}

const bytes = readFileSync(wasmPath);
// Level 9 = best deflate; deterministic and closest to what a tuned CDN ships.
const gz = gzipSync(bytes, { level: 9 }).length;

const kib = (n) => (n / KIB).toFixed(1);
const fmt = (n) => n.toLocaleString("en-US");

let failed = false;
function check(label, size, budgetKiB) {
  const budget = budgetKiB * KIB;
  const delta = budget - size; // >0 ⇒ under budget
  const pct = ((Math.abs(delta) / budget) * 100).toFixed(1);
  if (delta >= 0) {
    console.log(
      `  ✓ ${label}: ${fmt(size)} B (${kib(size)} KiB) ` +
        `— budget ${budgetKiB} KiB, ${fmt(delta)} B (${pct}%) headroom`,
    );
  } else {
    failed = true;
    console.log(
      `  ✗ ${label}: ${fmt(size)} B (${kib(size)} KiB) ` +
        `— OVER budget ${budgetKiB} KiB by ${fmt(-delta)} B (${pct}%)`,
    );
  }
}

console.log("@rindle/wasm bundle size budget (pkg/rindle_bg.wasm):");
check("gzip (level 9)", gz, GZIP_BUDGET_KIB);
check("raw (-Oz)     ", raw, RAW_BUDGET_KIB);

if (failed) {
  console.error(
    "\n✗ wasm bundle EXCEEDS its size budget.\n" +
      "  The gzipped wasm is the first-load cost users pay (and what the README advertises).\n" +
      "  • If this growth is intentional: raise the matching *_BUDGET_KIB in this file and\n" +
      "    explain why in the commit message.\n" +
      "  • If not: find what got pulled in — check for a new/heavier dependency, a lost\n" +
      "    `opt-level = \"z\"` / `wasm-opt -Oz` pass, or a feature that drags in panic/format\n" +
      "    machinery. `twiggy top pkg/rindle_bg.wasm` shows the biggest contributors.",
  );
  process.exit(1);
}

console.log("\n✅ wasm bundle within size budget");
