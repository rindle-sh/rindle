// Head-to-head driver: @rindle/wasm vs TanStack DB over a shared dataset, on both the
// hydrate-from-scratch and incremental paths, for two query ladders:
//   1. realistic "UI views"  — bounded (viewport-sized) results, how a client app queries;
//   2. full materialization  — the whole result set into JS, to characterize the engine.
//
//   SCALE=small|medium|large|xl  pnpm --filter @rindle/bench-tanstack bench
//   SCALE="500,10000,50000" ...                       # explicit users,issues,comments
//   ROUNDS=4 PAIRS=25 IROUNDS=3 ...                    # timing knobs
//
// Run with `--expose-gc` (the package script does) so the harness can settle GC between
// rounds; it still works without it.

import { writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { generate, resolveScale, VIEW_LIMIT } from "./data.ts";
import {
  LADDER_FULL,
  LADDER_VIEWS,
  section,
  timeHydrate,
  timeIncremental,
  type Adapter,
  type LadderEntry,
  type Row,
} from "./harness.ts";
import { makeRindle } from "./rindle.ts";
import { makeTanstack } from "./tanstack.ts";

const ROUNDS = Number(process.env.ROUNDS ?? 4);
const PAIRS = Number(process.env.PAIRS ?? 25);
const IROUNDS = Number(process.env.IROUNDS ?? 3);

function tanstackVersion(): string {
  try {
    return createRequire(import.meta.url)("@tanstack/db/package.json").version as string;
  } catch {
    return "unknown";
  }
}

async function runLadder(
  banner: string,
  ladder: LadderEntry[],
  rindle: Adapter,
  tanstack: Adapter,
): Promise<{ rows: Row[]; mismatch: boolean }> {
  process.stdout.write(`\n${banner}\n`);
  const rows: Row[] = [];
  let mismatch = false;
  for (const { name, label, desc } of ladder) {
    process.stdout.write(`• ${label.padEnd(28)} `);

    const rH = await timeHydrate(rindle, name, ROUNDS);
    const tH = await timeHydrate(tanstack, name, ROUNDS);

    const rInc = rindle.incremental(name);
    const rI = await timeIncremental(rInc, PAIRS, IROUNDS);
    await rInc.dispose();

    const tInc = tanstack.incremental(name);
    const tI = await timeIncremental(tInc, PAIRS, IROUNDS);
    await tInc.dispose();

    const match = rH.count === tH.count && rH.nested === tH.nested;
    if (!match) mismatch = true;

    rows.push({
      name,
      label,
      desc,
      rindleHydrateMs: rH.bestMs,
      tanstackHydrateMs: tH.bestMs,
      rindleIncrMs: rI.bestMsPerPair,
      tanstackIncrMs: tI.bestMsPerPair,
      rindleCount: rH.count,
      tanstackCount: tH.count,
      rindleNested: rH.nested,
      tanstackNested: tH.nested,
    });

    const ratioH = (tH.bestMs / rH.bestMs).toFixed(1);
    const ratioI = (tI.bestMsPerPair / rI.bestMsPerPair).toFixed(1);
    process.stdout.write(
      `hydrate ${ratioH}× · incr ${ratioI}×  ` +
        `${match ? "✓" : `⚠ rows ${rH.count}/${rH.nested} vs ${tH.count}/${tH.nested}`}\n`,
    );
  }
  return { rows, mismatch };
}

const VIEWS_INTRO =
  `Every result is **bounded to a viewport** (≤ ${VIEW_LIMIT} top-level rows) over the same large store — ` +
  `the shape a real client query has ("fetch enough to fill the view"). Only the viewport crosses into JS.`;
const FULL_INTRO =
  `These pull the **entire result set** into the host language — useful to characterize the engine, but ` +
  `not how a client app queries (a UI fetches a viewport, not the whole table).`;

async function main(): Promise<void> {
  const scale = resolveScale();
  const data = generate(scale);
  const tsVersion = tanstackVersion();
  const total = (data.counts.users + data.counts.issues + data.counts.comments).toLocaleString();

  process.stdout.write(
    `\nRindle (@rindle/wasm) vs TanStack DB v${tsVersion}\n` +
      `scale "${scale.name}": ${data.counts.users.toLocaleString()} users · ` +
      `${data.counts.issues.toLocaleString()} issues · ${data.counts.comments.toLocaleString()} comments  (${total} rows)\n` +
      `node ${process.version} · hydrate=min/${ROUNDS} · incremental=min/${IROUNDS}×${PAIRS} pairs\n`,
  );

  const rindle: Adapter = await makeRindle(data, scale);
  const tanstack: Adapter = makeTanstack(data, scale);

  const views = await runLadder("── Realistic UI views (bounded result) ──", LADDER_VIEWS, rindle, tanstack);
  const full = await runLadder("── Full materialization (whole result into JS) ──", LADDER_FULL, rindle, tanstack);

  const md =
    `${section("Realistic UI views — bounded result", VIEWS_INTRO, views.rows)}\n\n` +
    `${section("Full materialization — whole result set into JS", FULL_INTRO, full.rows)}`;
  process.stdout.write(`\n${md}\n`);
  if (views.mismatch || full.mismatch) {
    process.stdout.write(
      "\n⚠ a result signature differs between engines — see the per-query line above; the " +
        "query shapes are logically equivalent but check before trusting that row.\n",
    );
  }

  const report =
    `# Rindle (@rindle/wasm) vs TanStack DB — results\n\n` +
    `- **Dataset:** scale "${scale.name}" — ${data.counts.users.toLocaleString()} users, ` +
    `${data.counts.issues.toLocaleString()} issues, ${data.counts.comments.toLocaleString()} comments (${total} rows).\n` +
    `- **TanStack DB:** v${tsVersion}. **Node:** ${process.version}.\n` +
    `- **Timing:** min of ${ROUNDS} hydrate rounds; min per-pair over ${IROUNDS}×${PAIRS} incremental add+remove pairs.\n` +
    `- Generated by \`pnpm --filter @rindle/bench-tanstack bench\` (\`SCALE=${scale.name}\`). Numbers are machine-specific; re-run locally.\n\n` +
    `> The **UI views** section is the representative client workload (bounded results). The **full** ` +
    `section materializes whole result sets — engine characterization, not real query shapes.\n\n` +
    `${md}\n`;
  writeFileSync(new URL("../RESULTS.md", import.meta.url), report);
  process.stdout.write(`\nwrote ${new URL("../RESULTS.md", import.meta.url).pathname}\n`);

  await rindle.teardown();
  await tanstack.teardown();
}

main().then(
  () => process.exit(0),
  (err) => {
    console.error(err);
    process.exit(1);
  },
);
