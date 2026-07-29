// Timing harness + the engine-adapter contract both stores implement, plus markdown
// reporting. Timing mirrors the in-repo Rust/JS benches: warm up, then take the MIN over
// several rounds (min is the most noise-robust estimator for a microbenchmark — it strips
// GC pauses and scheduler jitter, leaving the floor cost).

export type QueryName =
  // full-materialization ladder — the whole result set crosses into JS
  | "scan"
  | "filter"
  | "filter_order_limit"
  | "one_to_many"
  | "many_to_one"
  | "nested"
  | "aggregate_count"
  // realistic "UI view" ladder — bounded (viewport-sized) result over the large store
  | "view_list"
  | "view_list_creator"
  | "view_list_count"
  | "view_list_comments"
  | "view_detail"
  | "view_page";

export interface LadderEntry {
  name: QueryName;
  label: string;
  desc: string;
}

/** The full-materialization ladder, simple → complex. These pull the WHOLE result into the
 *  host language — useful to characterize the engine, but not how a client app queries. */
export const LADDER_FULL: LadderEntry[] = [
  { name: "scan", label: "scan all issues", desc: "no filter; full table" },
  { name: "filter", label: "filter open", desc: "where open = true (~2/3)" },
  { name: "filter_order_limit", label: "filter+order+limit 50", desc: "open, newest 50 (top-K)" },
  { name: "one_to_many", label: "issue → comments[]", desc: "one-to-many nest" },
  { name: "many_to_one", label: "issue → creator", desc: "many-to-one join" },
  { name: "nested", label: "issue → comments → creator", desc: "two-level nest" },
  { name: "aggregate_count", label: "issue → commentCount", desc: "correlated count" },
];

/** The realistic ladder: every result is bounded to a viewport (≤ `VIEW_LIMIT` top-level
 *  rows) over the SAME large store — the shape a real client query has ("fetch enough to
 *  fill the view"). Only the viewport crosses the wasm boundary, so this is where the store
 *  is actually exercised the way an app uses it. */
export const LADDER_VIEWS: LadderEntry[] = [
  { name: "view_list", label: "list: newest 50 open", desc: "issue list page" },
  { name: "view_list_creator", label: "list + author", desc: "+ creator per row" },
  { name: "view_list_count", label: "list + comment count", desc: "+ count badge per row" },
  { name: "view_list_comments", label: "list + 3 recent comments", desc: "+ preview per row" },
  { name: "view_detail", label: "issue detail + comments", desc: "one issue, its comments" },
  { name: "view_page", label: "list: page 2", desc: "deep page (cursor/offset)" },
];

/** The secondary shape a query's rows carry — drives the full read and the cross-engine
 *  signature. `comments`/`creator` nest an array per parent; `count` is a scalar per row. */
export function resultShape(q: QueryName): "comments" | "creator" | "count" | "plain" {
  switch (q) {
    case "one_to_many":
    case "nested":
    case "view_list_comments":
    case "view_detail":
      return "comments";
    case "many_to_one":
    case "view_list_creator":
      return "creator";
    case "aggregate_count":
    case "view_list_count":
      return "count";
    default:
      return "plain";
  }
}

/** Result of hydrating one query from scratch: the disposable handle plus the signature
 *  used for cross-engine correctness checking. `count` = top-level rows; `nested` = a
 *  secondary invariant (total children, or summed aggregate) or 0 when not applicable. */
export interface HydrateResult {
  handle: unknown;
  count: number;
  nested: number;
}

/** A pre-materialized live view + a reversible incremental op (one add+remove pair) that
 *  leaves the dataset unchanged, so steady state holds across thousands of iterations. */
export interface Incremental {
  /** Live top-level row count of the maintained view (read after each op for a sanity check). */
  liveCount(): number;
  /** Apply one reversible add+remove pair (two engine writes). May be async (Rindle awaits). */
  pair(): void | Promise<void>;
  /** Release the persistent view. */
  dispose(): void | Promise<void>;
}

export interface Adapter {
  readonly name: string;
  /** Build + materialize + fully read one query from scratch (synchronous on both engines). */
  hydrate(q: QueryName): HydrateResult;
  /** Release a hydrate handle (async on TanStack — `cleanup()`). Kept OUT of the timed region. */
  disposeHydrate(handle: unknown): void | Promise<void>;
  /** A persistent materialized view + its reversible op for the incremental path. */
  incremental(q: QueryName): Incremental;
  /** Final teardown of the whole store. */
  teardown(): void | Promise<void>;
}

const nowMs = (): number => Number(process.hrtime.bigint()) / 1e6;
const gc: undefined | (() => void) = (globalThis as { gc?: () => void }).gc;

export interface HydrateTiming {
  bestMs: number;
  count: number;
  nested: number;
}

/** Time `hydrate(q)` from scratch: each round builds+reads+disposes a FRESH view, so we
 *  measure cold materialization every time (never a warm cache). MIN over `rounds`. */
export async function timeHydrate(adapter: Adapter, q: QueryName, rounds: number): Promise<HydrateTiming> {
  // warmup (1 build) — primes JITs/pipelines without counting
  await adapter.disposeHydrate(adapter.hydrate(q).handle);
  gc?.();
  let bestMs = Infinity;
  let count = 0;
  let nested = 0;
  for (let r = 0; r < rounds; r++) {
    const t0 = nowMs();
    const res = adapter.hydrate(q);
    const dt = nowMs() - t0; // build + materialize + full read only
    bestMs = Math.min(bestMs, dt);
    count = res.count;
    nested = res.nested;
    await adapter.disposeHydrate(res.handle); // outside the timed region
    gc?.();
  }
  return { bestMs, count, nested };
}

export interface IncrementalTiming {
  bestMsPerPair: number;
  liveCount: number;
}

/** Time the incremental path: with the view materialized ONCE, apply `pairs` reversible
 *  add+remove pairs and take the MIN per-pair over `rounds`. */
export async function timeIncremental(
  inc: Incremental,
  pairs: number,
  rounds: number,
): Promise<IncrementalTiming> {
  const warm = Math.min(20, pairs);
  for (let k = 0; k < warm; k++) await inc.pair();
  gc?.();
  let bestMsPerPair = Infinity;
  for (let r = 0; r < rounds; r++) {
    const t0 = nowMs();
    for (let k = 0; k < pairs; k++) await inc.pair();
    const perPair = (nowMs() - t0) / pairs;
    bestMsPerPair = Math.min(bestMsPerPair, perPair);
    gc?.();
  }
  return { bestMsPerPair, liveCount: inc.liveCount() };
}

// --------------------------------- reporting ---------------------------------

const fmtMs = (ms: number): string => (ms >= 1 ? ms.toFixed(2) : ms >= 0.001 ? ms.toFixed(3) : ms.toExponential(2));
const speed = (slow: number, fast: number): string => (fast > 0 ? `${(slow / fast).toFixed(2)}×` : "—");

export interface Row {
  name: QueryName;
  label: string;
  desc: string;
  rindleHydrateMs: number;
  tanstackHydrateMs: number;
  rindleIncrMs: number;
  tanstackIncrMs: number;
  rindleCount: number;
  tanstackCount: number;
  rindleNested: number;
  tanstackNested: number;
}

/** Render one ladder's results as a titled section: a hydrate table + an incremental table. */
export function section(title: string, intro: string, rows: Row[]): string {
  const lines: string[] = [];
  lines.push(`## ${title}`);
  lines.push("");
  lines.push(intro);
  lines.push("");
  lines.push("### Hydrate from scratch (ms; lower is better)");
  lines.push("");
  lines.push("| Query | Rindle | TanStack DB | Speedup | Rindle rows | match |");
  lines.push("|---|--:|--:|--:|--:|:--:|");
  for (const r of rows) {
    const ok = r.rindleCount === r.tanstackCount && r.rindleNested === r.tanstackNested;
    lines.push(
      `| ${r.label} | ${fmtMs(r.rindleHydrateMs)} | ${fmtMs(r.tanstackHydrateMs)} | ` +
        `**${speed(r.tanstackHydrateMs, r.rindleHydrateMs)}** | ${r.rindleCount.toLocaleString()}` +
        `${r.rindleNested ? ` (+${r.rindleNested.toLocaleString()})` : ""} | ${ok ? "✅" : "⚠️"} |`,
    );
  }
  lines.push("");
  lines.push("### Incremental update (ms per add+remove pair; lower is better)");
  lines.push("");
  lines.push("| Query | Rindle | TanStack DB | Speedup |");
  lines.push("|---|--:|--:|--:|");
  for (const r of rows) {
    lines.push(
      `| ${r.label} | ${fmtMs(r.rindleIncrMs)} | ${fmtMs(r.tanstackIncrMs)} | ` +
        `**${speed(r.tanstackIncrMs, r.rindleIncrMs)}** |`,
    );
  }
  return lines.join("\n");
}
