// Release-topology regression soak for the wiki demo's former runaway path:
//
//   master + follower + both pinned nested queries
//   → ingest more expired rows than HCTree's one-transaction change ceiling
//   → prune the whole set in bounded transactions
//   → prove the SQL and Node working sets return to zero
//
// The benchmark-shaped output (`label|value`, lower is better) also makes this runnable from
// `./orient.sh bench wiki_demo_soak` and recordable by the repository benchmark history tool.

import assert from "node:assert/strict";
import type { ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";

import { HttpRindleDaemonClient } from "@rindle/daemon-client";

import { startPair } from "../src/daemon.ts";
import { topEdited, topRecentEditors } from "../src/schema.ts";
import type { WikiEvent } from "../src/schema.ts";
import { WIKI_INDEX_NAMES, startTinyMachine } from "../src/tier.ts";

const EDITS = Number(process.env.WIKI_SOAK_EDITS ?? 9_000);
const SKEW_EDITS = Number(process.env.WIKI_SOAK_SKEW_EDITS ?? 8_193);
const INGEST_BATCH = Number(process.env.WIKI_SOAK_INGEST_BATCH ?? 200);
const N_WORKERS = Number(process.env.WIKI_SOAK_NWORKERS ?? 2);
const SKEW_EXPIRED_TS = 999;
const EXPIRED_TS = 1_000;
const PRUNE_BEFORE_TS = 2_000;
const EXPECTED_INDEXES = WIKI_INDEX_NAMES;
const EXPECTED_QUERY_INDEXES: Record<string, Record<string, string>> = {
  latest: {
    page: "ix_page__last_ts_desc_id",
    edit: "ix_edit__page_id_ts_desc",
  },
  recentEditors: {
    editor: "ix_editor__last_ts_desc_name",
    edit: "ix_edit__user_ts_desc",
  },
};

function event(i: number): WikiEvent {
  const page = i % 1_500;
  const editor = i % 800;
  return {
    page: {
      id: `enwiki:Soak page ${page}`,
      wiki: "enwiki",
      title: `Soak page ${page}`,
      url: `https://example.invalid/wiki/Soak_page_${page}`,
      ts: EXPIRED_TS,
      user: `Editor${editor}`,
    },
    edit: {
      id: `soak-r${i}`,
      user: `Editor${editor}`,
      comment: "bounded prune soak",
      ts: EXPIRED_TS,
      delta: 1,
      bot: 0,
    },
  };
}

function skewEvent(i: number, ts = SKEW_EXPIRED_TS): WikiEvent {
  return {
    page: {
      id: "enwiki:One extremely hot page",
      wiki: "enwiki",
      title: "One extremely hot page",
      url: "https://example.invalid/wiki/One_extremely_hot_page",
      ts,
      user: "OneExtremelyHotEditor",
    },
    edit: {
      id: i === SKEW_EDITS ? "skew-retained" : `skew-r${i}`,
      user: "OneExtremelyHotEditor",
      comment: "hot-key bounded prune soak",
      ts,
      delta: 1,
      bot: 0,
    },
  };
}

async function counts(client: HttpRindleDaemonClient): Promise<[number, number, number]> {
  const out = await client.executeSqlRead({
    sql: "SELECT (SELECT COUNT(*) FROM edit), (SELECT COUNT(*) FROM page), (SELECT COUNT(*) FROM editor)",
    params: [],
  });
  assert.equal(out.rows.length, 1, `one count row: ${JSON.stringify(out)}`);
  return out.rows[0].map(Number) as [number, number, number];
}

async function withTimeout<T>(label: string, promise: Promise<T>, ms = 120_000): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} exceeded ${ms}ms`)), ms);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function waitForChildExit(proc: ChildProcess, timeoutMs: number): Promise<boolean> {
  if (proc.exitCode !== null || proc.signalCode !== null) return Promise.resolve(true);
  return new Promise((resolveExit) => {
    const finish = (exited: boolean): void => {
      clearTimeout(timer);
      proc.off("exit", onExit);
      proc.off("error", onError);
      resolveExit(exited);
    };
    const onExit = (): void => finish(true);
    const onError = (): void => finish(false);
    const timer = setTimeout(() => finish(false), timeoutMs);
    proc.once("exit", onExit);
    proc.once("error", onError);
  });
}

async function closePairAndWait(): Promise<void> {
  // Register exit listeners before sending SIGTERM so a fast child cannot race the waiter.
  const graceful = pair.procs.map((proc) => waitForChildExit(proc, 5_000));
  pair.close();
  const exited = await Promise.all(graceful);
  const stubborn = pair.procs.filter((_, index) => !exited[index]);
  const forced = stubborn.map((proc) => waitForChildExit(proc, 1_000));
  for (const proc of stubborn) proc.kill("SIGKILL");
  const killed = await Promise.all(forced);
  const live = stubborn.filter((_, index) => !killed[index]);
  if (live.length > 0) {
    console.error(
      `[soak] warning: pair process(es) did not report exit after SIGKILL: ${live.map((proc) => proc.pid).join(", ")}`,
    );
    for (const proc of live) proc.unref();
  }
}

async function assertIndexedPlan(label: string, ast: unknown): Promise<void> {
  const response = await fetch(`${pair.httpUrl}/analyze`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ ast }),
  });
  if (!response.ok) throw new Error(`${label} analyze responded ${response.status}: ${await response.text()}`);
  const report = (await response.json()) as {
    sources?: Array<{ table?: string; advisory?: string | null; sqlitePlan?: string }>;
  };
  assert.ok(report.sources?.length, `${label} reported sources: ${JSON.stringify(report)}`);
  const expected = EXPECTED_QUERY_INDEXES[label];
  assert.ok(expected, `expected-index fixture exists for ${label}`);
  for (const source of report.sources) {
    assert.equal(source.advisory ?? null, null, `${label}/${source.table} has no missing-index advisory`);
    const table = source.table;
    assert.equal(typeof table, "string", `${label} source names its table: ${JSON.stringify(source)}`);
    if (typeof table !== "string") throw new Error(`${label} source omitted its table`);
    assert.ok(
      typeof source.sqlitePlan === "string" && source.sqlitePlan.length > 0,
      `${label}/${table} reports its SQLite access path: ${JSON.stringify(source)}`,
    );
    const expectedIndex = expected[table];
    assert.ok(expectedIndex, `${label}/${table} has an expected workload index`);
    assert.ok(
      source.sqlitePlan.includes(expectedIndex),
      `${label}/${table} uses ${expectedIndex}: ${source.sqlitePlan}`,
    );
    assert.ok(
      !source.sqlitePlan.includes("USE TEMP B-TREE FOR ORDER BY"),
      `${label}/${table} avoids an order-by temp tree: ${source.sqlitePlan}`,
    );
  }
  assert.deepEqual(
    new Set(report.sources.map((source) => source.table)),
    new Set(Object.keys(expected)),
    `${label} reports every expected source`,
  );
}

assert.ok(EDITS > 8_192, `soak must cross HCTree's 8,192-change ceiling (got ${EDITS})`);
assert.ok(
  SKEW_EDITS > 8_192,
  `hot-key soak must span multiple bounded prune transactions (got ${SKEW_EDITS})`,
);
assert.ok(INGEST_BATCH > 0, "ingest batch must be positive");
assert.ok(Number.isInteger(N_WORKERS) && N_WORKERS > 0, "worker count must be a positive integer");

const dataDir = mkdtempSync(join(tmpdir(), "wiki-soak-"));
const pair = await startPair({
  dataDir,
  nWorkers: N_WORKERS,
  httpPort: 0,
  wsPort: 0,
  masterHttpPort: 0,
  masterWsPort: 0,
});
const machine = await startTinyMachine({
  daemonUrl: pair.httpUrl,
  daemonWsUrl: pair.wsUrl,
  masterUrl: pair.masterUrl,
  sourceName: "soak",
  masterPid: pair.masterPid,
  followerPid: pair.followerPid,
});
const master = new HttpRindleDaemonClient({ baseUrl: pair.masterUrl });
const follower = new HttpRindleDaemonClient({ baseUrl: pair.httpUrl });

try {
  console.log(
    `[soak] context — workers=${N_WORKERS}, spread_edits=${EDITS}, hot_key_expired_edits=${SKEW_EDITS}`,
  );
  const indexes = await follower.executeSqlRead({
    sql: `SELECT name FROM sqlite_schema WHERE type = 'index' AND name IN (${EXPECTED_INDEXES.map(() => "?").join(", ")}) ORDER BY name`,
    params: [...EXPECTED_INDEXES],
  });
  assert.deepEqual(
    indexes.rows.map((row) => String(row[0])),
    [...EXPECTED_INDEXES].sort(),
    `wiki workload indexes installed: ${JSON.stringify(indexes)}`,
  );

  const ingestStarted = performance.now();
  // The older hot-key cohort is selected first by the prune index. More than 8,192 rows for one
  // page/editor proves chunking remains exact without rescanning that parent's shrinking remainder.
  for (let from = 0; from < SKEW_EDITS; from += INGEST_BATCH) {
    const to = Math.min(from + INGEST_BATCH, SKEW_EDITS);
    const batch = Array.from({ length: to - from }, (_, j) => skewEvent(from + j));
    await withTimeout(`hot-key ingest ${from}-${to}`, machine.ingest(batch));
  }
  for (let from = 0; from < EDITS; from += INGEST_BATCH) {
    const to = Math.min(from + INGEST_BATCH, EDITS);
    const batch = Array.from({ length: to - from }, (_, j) => event(from + j));
    await withTimeout(`ingest ${from}-${to}`, machine.ingest(batch));
  }
  // Keep one newer row for the hot parent. The first prune must decrement >8,192 old rows across
  // many commits and land on exactly one, rather than merely deleting a non-positive parent.
  await withTimeout(
    "retained hot-key ingest",
    machine.ingest([skewEvent(SKEW_EDITS, PRUNE_BEFORE_TS)]),
  );
  const ingestMs = performance.now() - ingestStarted;

  const before = await counts(master);
  const totalIngested = EDITS + SKEW_EDITS + 1;
  assert.deepEqual(
    before,
    [totalIngested, 1_501, 801],
    `seeded spread + hot-key working set: ${JSON.stringify(before)}`,
  );
  await assertIndexedPlan("latest", topEdited("latest").ast());
  await assertIndexedPlan("recentEditors", topRecentEditors().ast());

  const pruneStarted = performance.now();
  await withTimeout("bounded prune", machine.prune(PRUNE_BEFORE_TS));
  const pruneMs = performance.now() - pruneStarted;

  const retained = await counts(master);
  assert.deepEqual(retained, [1, 1, 1], `prune retained exactly the one fresh hot-key row: ${JSON.stringify(retained)}`);
  const retainedParents = await master.executeSqlRead({
    sql:
      "SELECT (SELECT edits FROM page WHERE id = ?), " +
      "(SELECT edits FROM editor WHERE name = ?), " +
      "(SELECT ts FROM edit WHERE id = ?)",
    params: ["enwiki:One extremely hot page", "OneExtremelyHotEditor", "skew-retained"],
    consistency: "strong",
  });
  assert.deepEqual(
    retainedParents.rows,
    [[1, 1, PRUNE_BEFORE_TS]],
    `per-chunk decrements leave exact parent counts: ${JSON.stringify(retainedParents)}`,
  );

  const retainedMetrics = await (await fetch(`http://127.0.0.1:${machine.apiPort}/metrics`)).json();
  assert.equal(retainedMetrics.editsInWindow, 1, `Node edit window retains one row: ${JSON.stringify(retainedMetrics)}`);
  assert.equal(retainedMetrics.pages, 1, `Node page map retains one key: ${JSON.stringify(retainedMetrics)}`);
  assert.equal(retainedMetrics.editors, 1, `Node editor map retains one key: ${JSON.stringify(retainedMetrics)}`);

  // Finish with the original zero-working-set invariant outside the measured expired-row prune.
  await withTimeout("final retained-row prune", machine.prune(PRUNE_BEFORE_TS + 1));
  const after = await counts(master);
  assert.deepEqual(after, [0, 0, 0], `final prune emptied SQL working set: ${JSON.stringify(after)}`);
  await new Promise((resolve) => setTimeout(resolve, 1_050)); // metrics has a one-second TTL
  const metrics = await (await fetch(`http://127.0.0.1:${machine.apiPort}/metrics`)).json();
  assert.equal(metrics.editsInWindow, 0, `Node edit window is bounded: ${JSON.stringify(metrics)}`);
  assert.equal(metrics.pages, 0, `Node page map is bounded: ${JSON.stringify(metrics)}`);
  assert.equal(metrics.editors, 0, `Node editor map is bounded: ${JSON.stringify(metrics)}`);

  const expiredEdits = EDITS + SKEW_EDITS;
  console.log(`wiki_demo|ingest_ns_per_edit|${Math.round((ingestMs * 1_000_000) / totalIngested)}`);
  console.log(`wiki_demo|prune_ns_per_edit|${Math.round((pruneMs * 1_000_000) / expiredEdits)}`);
  console.log(`wiki_demo|post_prune_edits|${after[0]}`);
  console.log(`wiki_demo|reported_rss_bytes|${Number(metrics.memUsedBytes)}`);
  console.log(
    `[soak] PASS — ingested ${totalIngested} edits and pruned ${expiredEdits} expired rows across the release topology`,
  );
} finally {
  try {
    await machine.close();
  } finally {
    try {
      await closePairAndWait();
    } finally {
      rmSync(dataDir, { recursive: true, force: true });
    }
  }
}

process.exit(0);
