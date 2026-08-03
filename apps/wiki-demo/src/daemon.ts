// The ONE topology (design 214): every deployment is a `rindle-replicator` write-master + one or
// more `rindled` read-followers. `startPair` boots the COLOCATED pair on one box (mirroring the
// issue-tracker's test/pair-fixture.ts): the master owns the authoritative data + accepts writes,
// the bare follower tails it over loopback and serves the reads/subscriptions the browser sees.
//
// This demo's base schema is fixed and every new deployment image reseeds from the source, so it
// stays the textbook fit for the declarative `tables` bootstrap — declared here as the MASTER's
// base `tables`. The master creates + registers them at open AND logs them as a genesis `ddl`
// change-log entry, so the bare follower bootstraps its whole schema from the stream (no per-node
// table migration). The tier applies workload indexes through the master's migration plane, WRITES
// ingest/prune there, and READS from the follower — the follower has no write plane.

import { spawn, type ChildProcess } from "node:child_process";
import {
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

const here = fileURLToPath(new URL(".", import.meta.url));
const appRoot = resolve(here, "..");
const repoRoot = resolve(appRoot, "../..");

export interface Pair {
  /** The FOLLOWER's read control-plane base url (reads / pins / stats). */
  readonly httpUrl: string;
  /** The FOLLOWER's public subscription ws url (browser subscriptions). */
  readonly wsUrl: string;
  /** The MASTER's write-ingress base url (ingest / prune go here — the follower has no write plane). */
  readonly masterUrl: string;
  /** The follower's boot id (re-pin trigger). */
  readonly bootId: string;
  /** All spawned processes — the master + the follower. */
  readonly procs: ChildProcess[];
  /** Process ids for component-level CPU/RSS telemetry. */
  readonly masterPid?: number;
  readonly followerPid?: number;
  close(): void;
}

export interface PairOptions {
  dataDir: string;
  /** The follower's control-plane bearer (required on a non-loopback bind; omit for open local dev). */
  authToken?: string;
  /** The master's write-ingress bearer (required on a non-loopback bind; omit for open local dev). */
  writeToken?: string;
  /** Follower ports. */
  httpPort?: number;
  wsPort?: number;
  /** Master ports (fan-out ws + write ingress). */
  masterWsPort?: number;
  masterHttpPort?: number;
  nWorkers?: number;
  idleTtlMs?: number;
  /** The master only has one ordered writer in this demo, so one pooled connection is enough. */
  masterWriteConnections?: number;
  /** Optional, distinct Prometheus listeners for the two children. */
  masterMetricsAddr?: string;
  followerMetricsAddr?: string;
  /** Bind address for both engines. Default `127.0.0.1` (local); `0.0.0.0` in a container. */
  bindHost?: string;
}

const DEPLOYMENT_MARKER = ".wiki-image-ref";
const WIKI_DATA_FILE_PREFIXES = [
  "master.db",
  "follower.db",
  "wiki.db",
] as const;
const WIKI_DATA_FILES = [
  ".offset",
  ".offset.tmp",
  "master.json",
  "follower.json",
  "daemon.json",
  `${DEPLOYMENT_MARKER}.tmp`,
] as const;

export interface DeploymentReset {
  reset: boolean;
  removed: string[];
}

/**
 * Start each Fly image with a coherent, empty demo dataset while preserving data across ordinary
 * restarts of that same image. The wiki firehose is disposable and backfilled, so carrying a
 * database across engine-format/topology changes buys us nothing and has repeatedly hidden stale
 * state. `FLY_IMAGE_REF` is deployment-scoped; local runs (where it is absent) are untouched.
 *
 * This deliberately removes only files owned by this demo. A Fly volume can contain unrelated
 * operator data, so never recursively wipe the data directory itself.
 */
export function resetWikiDataForImage(
  dataDir: string,
  imageRef: string | undefined,
): DeploymentReset {
  mkdirSync(dataDir, { recursive: true });
  const currentImage = imageRef?.trim();
  if (!currentImage) return { reset: false, removed: [] };

  const markerPath = join(dataDir, DEPLOYMENT_MARKER);
  try {
    if (readFileSync(markerPath, "utf8").trim() === currentImage)
      return { reset: false, removed: [] };
  } catch {
    // A missing/unreadable marker is a first boot for this deployment. Reset the known demo files.
  }

  const removed: string[] = [];
  for (const name of readdirSync(dataDir)) {
    const owned =
      WIKI_DATA_FILES.some((file) => name === file) ||
      WIKI_DATA_FILE_PREFIXES.some(
        (prefix) => name === prefix || name.startsWith(`${prefix}-`),
      );
    if (!owned) continue;
    rmSync(join(dataDir, name), { recursive: true, force: true });
    removed.push(name);
  }

  // Publish the marker last. A crash before the rename simply repeats this idempotent reset.
  const markerTmp = join(dataDir, `${DEPLOYMENT_MARKER}.tmp`);
  writeFileSync(markerTmp, `${currentImage}\n`);
  renameSync(markerTmp, markerPath);
  return { reset: true, removed };
}

// The fixed demo schema, declared as the MASTER's base `tables` (positional column/pk/type form the
// `rindle-replicator` config parses — `schema.ts` is the TS-side mirror). One boot: the tables exist
// (and are logged as a genesis `ddl` entry that reaches the follower) the instant the pair is ready,
// with no `/migrate` round-trip. (A long-lived, evolving database would instead apply each schema
// change through the master's `/migrate` so it's journaled and versioned.)
const tables = [
  {
    name: "page",
    columns: ["id", "wiki", "title", "url", "edits", "last_ts", "last_user"],
    pk: [0],
    types: [
      "string",
      "string",
      "string",
      "string",
      "number",
      "number",
      "string",
    ],
  },
  {
    name: "editor",
    columns: ["name", "edits", "last_ts"],
    pk: [0],
    types: ["string", "number", "number"],
  },
  {
    name: "edit",
    columns: ["id", "page_id", "user", "comment", "ts", "delta", "bot"],
    pk: [0],
    types: [
      "string",
      "string",
      "string",
      "string",
      "number",
      "number",
      "number",
    ],
  },
] as const;

interface ReadyLine {
  ready?: boolean;
  httpPort?: number;
  wsPort?: number;
  bootId?: string;
}

/** Spawn a pair binary and resolve with its `{"ready":true,...}` line (ports + bootId). */
function spawnReady(
  bin: string,
  configPath: string,
  label: string,
  env: NodeJS.ProcessEnv,
): Promise<{ proc: ChildProcess; ready: ReadyLine }> {
  const proc = spawn(bin, ["--config", configPath], {
    env,
    stdio: ["ignore", "pipe", "inherit"],
  });
  return new Promise((resolveReady, reject) => {
    const rl = createInterface({ input: proc.stdout! });
    const timer = setTimeout(() => {
      proc.kill("SIGKILL");
      reject(
        new Error(
          `${label} did not become ready in time (${bin} --config ${configPath})`,
        ),
      );
    }, 15_000);
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      clearTimeout(timer);
      reject(
        new Error(
          `${label} exited before ready (code ${code}, signal ${signal})`,
        ),
      );
    };
    proc.once("exit", onExit);
    rl.on("line", (line) => {
      let msg: ReadyLine;
      try {
        msg = JSON.parse(line);
      } catch {
        console.log(line);
        return;
      }
      if (!msg.ready) return;
      clearTimeout(timer);
      proc.off("exit", onExit);
      resolveReady({ proc, ready: msg });
    });
  });
}

/** Give each child its own optional metrics address instead of letting both inherit one port. */
function childEnv(
  metricsAddr: string | undefined,
  overrides: NodeJS.ProcessEnv = {},
): NodeJS.ProcessEnv {
  const env = { ...process.env, ...overrides };
  const addr = metricsAddr?.trim();
  if (addr) env.RINDLE_METRICS_ADDR = addr;
  else delete env.RINDLE_METRICS_ADDR;
  return env;
}

export async function startPair(opts: PairOptions): Promise<Pair> {
  mkdirSync(opts.dataDir, { recursive: true });
  const bindHost = opts.bindHost ?? "127.0.0.1";
  const masterName = "rindle-master";
  const followerDb = join(opts.dataDir, "follower.db");
  const replicatorBin =
    process.env.RINDLE_REPLICATOR_BIN ??
    join(repoRoot, "rust/target/debug/rindle-replicator");
  const rindledBin =
    process.env.RINDLED_BIN ??
    process.env.RINDLED_BINARY_PATH ??
    join(repoRoot, "rust/target/debug/rindled");

  // Backward compatibility: a single legacy RINDLE_METRICS_ADDR belongs to the public follower.
  // The master is disabled unless it gets its own address, eliminating the old bind race on :9091.
  const masterMetricsAddr =
    opts.masterMetricsAddr ?? process.env.RINDLE_MASTER_METRICS_ADDR;
  const followerMetricsAddr =
    opts.followerMetricsAddr ??
    process.env.RINDLE_FOLLOWER_METRICS_ADDR ??
    process.env.RINDLE_METRICS_ADDR;

  // ── the write-master: holds the authoritative data, fans out on `masterWsPort`, accepts writes on
  // `masterHttpPort` (bearer-gated on a public bind). Base `tables` = the fixed demo schema. ──
  const masterConfig = join(opts.dataDir, "master.json");
  writeFileSync(
    masterConfig,
    JSON.stringify(
      {
        db: join(opts.dataDir, "master.db"),
        name: masterName,
        httpPort: opts.masterHttpPort ?? 7611,
        wsPort: opts.masterWsPort ?? 7610,
        bindHost,
        ...(opts.writeToken ? { authToken: opts.writeToken } : {}),
        tables,
        // Store-free colocated topology: reclaim the HCTree journal once this volume's follower
        // has durably applied it. Without this, the journal grows forever under the firehose.
        localRetention: {
          followerDb,
          source: masterName,
          intervalSecs: 60,
          graceSecs: 0,
        },
      },
      null,
      2,
    ),
  );
  const master = await spawnReady(
    replicatorBin,
    masterConfig,
    "rindle-replicator (master)",
    childEnv(masterMetricsAddr, {
      RINDLE_WRITE_CONNECTIONS: String(opts.masterWriteConnections ?? 1),
    }),
  );
  const masterWsPort = master.ready.wsPort ?? opts.masterWsPort ?? 7610;
  const masterUrl = `http://127.0.0.1:${master.ready.httpPort}`;

  // ── the read-follower: a BARE rindled tailing the master over loopback; schema arrives via the
  // master's genesis `ddl` entry. Public ws + read control plane. ──
  const followerConfig = join(opts.dataDir, "follower.json");
  writeFileSync(
    followerConfig,
    JSON.stringify(
      {
        db: followerDb,
        httpPort: opts.httpPort ?? 7600,
        wsPort: opts.wsPort ?? 7601,
        bindHost,
        ...(opts.authToken ? { authToken: opts.authToken } : {}),
        nWorkers: opts.nWorkers ?? 2,
        defaultIdleTtlMs: opts.idleTtlMs ?? 15_000,
        tables: [],
        sources: [
          {
            kind: "replicator",
            name: masterName,
            url: `ws://127.0.0.1:${masterWsPort}/subscribe`,
          },
        ],
      },
      null,
      2,
    ),
  );
  let follower: { proc: ChildProcess; ready: ReadyLine };
  try {
    follower = await spawnReady(
      rindledBin,
      followerConfig,
      "rindled (follower)",
      childEnv(followerMetricsAddr),
    );
  } catch (err) {
    master.proc.kill("SIGKILL");
    throw err;
  }

  const procs = [master.proc, follower.proc];
  return {
    httpUrl: `http://127.0.0.1:${follower.ready.httpPort}`,
    wsUrl: `ws://127.0.0.1:${follower.ready.wsPort}`,
    masterUrl,
    bootId: String(follower.ready.bootId ?? ""),
    procs,
    masterPid: master.proc.pid,
    followerPid: follower.proc.pid,
    close: () => procs.forEach((p) => p.kill("SIGTERM")),
  };
}
