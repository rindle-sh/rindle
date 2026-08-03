// The tiny machine, booted as the ONE topology (design 214) behind a single Node front door:
//
//   rindle-replicator (write-master)  ◀──writes (ingest/prune)──┐
//        │ fan-out ws (loopback)                                │
//        ▼                                                      │
//   rindled (read-follower)  ◀──reads / pins / stats──  API + ingester (this Node process)  ◀── browsers
//                            ◀──────────── public ws (proxied by the Node tier) ──────────────────┘
//
// This process runs the API server (pins `latest`/`recentEditors`, all READS off the follower) and
// drives the ordered ingester, whose WRITES land on the master (the follower has no write
// plane). It boots the colocated PAIR with the fixed demo schema as the master's base `tables`
// (ready in one boot; the follower bootstraps its schema from the master's genesis `ddl` entry).
// Both engines stay on loopback — the Node tier is the single exposed front door and reverse-proxies
// the public ws to the follower. Browsers subscribe through the API server (lease) + that ws — many
// readers SHARE the one pinned materialization.
//
//   node --conditions=@rindle/source src/main.ts                      # the real Wikimedia firehose
//   node --conditions=@rindle/source src/main.ts --source synthetic   # deterministic offline stream
//
// Env: WIKI_API_PORT (7700) · RINDLED_BIN · WIKI_DAEMON_TOKEN · WIKI_SOURCE · WIKI_DATA_DIR
//      · WIKI_NWORKERS (2) · WIKI_WINDOW_MIN (180)
//      · WIKI_BACKFILL_MIN (=WINDOW_MIN; cold-start seed depth, 0=off) · WIKI_DOMAIN.

import { existsSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { resetWikiDataForImage, startPair } from "./daemon.ts";
import { createSyntheticSource, createWikimediaSource } from "./sources.ts";
import { startTinyMachine } from "./tier.ts";

function flag(name: string): string | undefined {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

const API_PORT = Number(process.env.WIKI_API_PORT ?? 7700);
const DAEMON_TOKEN = process.env.WIKI_DAEMON_TOKEN; // follower control-plane bearer; omit for open loopback dev
const WRITE_TOKEN = process.env.WIKI_WRITE_TOKEN; // master write-ingress bearer; omit for open loopback dev
const N_WORKERS = Number(process.env.WIKI_NWORKERS ?? 2);
const WINDOW_SEC = Number(process.env.WIKI_WINDOW_MIN ?? 180) * 60; // prune edits older than this
// Cold-start backfill depth: on a fresh volume (no persisted offset) we open the stream this far in
// the past so the window is seeded at once rather than filling over wall-clock time. Defaults to the
// retention window, so one knob bounds both how far back we seed and how far back we keep. 0 = off
// (start live from "now"); capped by the stream's retention (~7d).
const BACKFILL_SEC = Number(process.env.WIKI_BACKFILL_MIN ?? process.env.WIKI_WINDOW_MIN ?? 180) * 60;
const sourceName = flag("--source") ?? process.env.WIKI_SOURCE ?? "wikimedia";
// The persisted source offset. In production, point this at the same persistent volume as the
// daemon's SQLite file so the dataset and resume cursor survive restarts together.
const DATA_DIR = process.env.WIKI_DATA_DIR ?? join(process.cwd(), ".rindled-wiki");
const OFFSET_PATH = join(DATA_DIR, ".offset");
const OFFSET_TMP_PATH = join(DATA_DIR, ".offset.tmp");
const deploymentReset = resetWikiDataForImage(DATA_DIR, process.env.FLY_IMAGE_REF);
if (deploymentReset.reset) {
  console.log(
    `[wiki] new deployment image — reset ${deploymentReset.removed.length} disposable data file(s) before backfill`,
  );
}

const pair = await startPair({
  dataDir: DATA_DIR,
  authToken: DAEMON_TOKEN,
  writeToken: WRITE_TOKEN,
  httpPort: 7600,
  wsPort: 7601,
  masterWsPort: 7610,
  masterHttpPort: 7611,
  nWorkers: N_WORKERS,
});
console.log(
  `[wiki] pair up — follower control ${pair.httpUrl}, public ws ${pair.wsUrl}; ` +
    `master write ${pair.masterUrl} (boot ${pair.bootId})`,
);

let shuttingDown = false;
for (const proc of pair.procs) {
  proc.on("exit", (code, signal) => {
    if (shuttingDown) return;
    console.error(`[wiki] a pair process (pid ${proc.pid}) exited unexpectedly (code ${code}, signal ${signal})`);
    process.exit(1);
  });
}

const machine = await startTinyMachine({
  daemonUrl: pair.httpUrl,
  daemonWsUrl: pair.wsUrl,
  daemonToken: DAEMON_TOKEN,
  masterUrl: pair.masterUrl,
  masterToken: WRITE_TOKEN,
  apiPort: API_PORT,
  sourceName,
  masterPid: pair.masterPid,
  followerPid: pair.followerPid,
});
console.log(
  `[wiki] API + metrics on http://127.0.0.1:${machine.apiPort}  ` +
    `(query: /api/rindle/query, metrics: /metrics) — latest/recentEditors pinned`,
);

const source = sourceName === "synthetic" ? createSyntheticSource() : createWikimediaSource();

// Resume the real stream from where we left off (gap-free) — the persisted SSE offset on the
// volume. First boot has none; the board fills live within a minute or two.
const sinceId = existsSync(OFFSET_PATH) ? readFileSync(OFFSET_PATH, "utf8").trim() || undefined : undefined;
// Cold start (no persisted offset): backfill the window via `?since=` so the boards are full at
// once. A restart always resumes from the exact persisted offset instead (sinceId wins).
const backfillSinceMs = !sinceId && BACKFILL_SEC > 0 ? Date.now() - BACKFILL_SEC * 1000 : undefined;
if (sinceId) console.log(`[wiki] resuming "${source.name}" from persisted offset`);
else if (backfillSinceMs !== undefined)
  console.log(`[wiki] cold start — seeding "${source.name}" from ${Math.round(BACKFILL_SEC / 60)} min ago, then tailing live`);
else console.log(`[wiki] starting "${source.name}" live (no stored offset)`);

// Debounce offset persistence: the source reports committed offsets in strict source order. Write
// through a same-directory rename so a crash leaves either the previous complete cursor or the new
// one, never a torn file.
let pendingOffset: string | undefined;
const persistOffset = (id: string): void => {
  writeFileSync(OFFSET_TMP_PATH, id);
  renameSync(OFFSET_TMP_PATH, OFFSET_PATH);
};
const offsetTimer = setInterval(() => {
  if (pendingOffset === undefined) return;
  const id = pendingOffset;
  try {
    persistOffset(id);
    if (pendingOffset === id) pendingOffset = undefined;
  } catch (err) {
    console.error("[wiki] failed to persist offset:", (err as Error).message);
  }
}, 2_000);

const stopSource = source.start(machine.ingest, {
  sinceId,
  backfillSinceMs,
  onOffset: (id) => {
    pendingOffset = id;
    machine.noteOffset(id);
  },
  onFatal: (error) => {
    console.error("[wiki] source stopped after bounded retries:", error.message);
    void shutdown(1);
  },
});

// Keep the dataset (and the "most-edited" window) bounded: drop edits older than the window.
let pruneInFlight: Promise<void> | undefined;
const pruneTimer = setInterval(() => {
  if (pruneInFlight) return;
  pruneInFlight = machine
    .prune(Math.floor(Date.now() / 1000) - WINDOW_SEC)
    .catch((err) => console.error("[wiki] prune failed:", (err as Error).message))
    .finally(() => {
      pruneInFlight = undefined;
    });
}, 60_000);

let shutdownPromise: Promise<void> | undefined;
let shutdownExitCode = 0;
function shutdown(exitCode = 0): Promise<void> {
  shutdownExitCode = Math.max(shutdownExitCode, exitCode);
  if (shutdownPromise) return shutdownPromise;
  shuttingDown = true;
  clearInterval(offsetTimer);
  clearInterval(pruneTimer);
  shutdownPromise = (async () => {
    try {
      await stopSource();
    } catch (error) {
      shutdownExitCode = 1;
      console.error("[wiki] source drain failed during shutdown:", (error as Error).message);
    }
    if (pendingOffset !== undefined) {
      try {
        persistOffset(pendingOffset);
        pendingOffset = undefined;
      } catch (error) {
        shutdownExitCode = 1;
        console.error("[wiki] final offset persistence failed:", (error as Error).message);
      }
    }
    await machine.close();
    pair.close();
    process.exitCode = shutdownExitCode;
  })();
  return shutdownPromise;
}
process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());
