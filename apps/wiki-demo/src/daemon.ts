// The ONE topology (design 214): every deployment is a `rindle-replicator` write-master + one or
// more `rindled` read-followers. `startPair` boots the COLOCATED pair on one box (mirroring the
// issue-tracker's test/pair-fixture.ts): the master owns the authoritative data + accepts writes,
// the bare follower tails it over loopback and serves the reads/subscriptions the browser sees.
//
// This demo's schema never evolves and is reseeded from the source on every boot, so it stays the
// textbook fit for the declarative `tables` bootstrap — declared here as the MASTER's base `tables`.
// The master creates + registers them at open AND logs them as a genesis `ddl` change-log entry, so
// the bare follower bootstraps its whole schema from the stream (no per-node migrate). The tier then
// WRITES to the master (ingest/prune) and READS from the follower — the follower has no write plane.

import { spawn, type ChildProcess } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
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
  /** The follower's pid — folded into the memory badge so the footprint is the whole app. */
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
  /** Bind address for both engines. Default `127.0.0.1` (local); `0.0.0.0` in a container. */
  bindHost?: string;
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
    types: ["string", "string", "string", "string", "number", "number", "string"],
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
    types: ["string", "string", "string", "string", "number", "number", "number"],
  },
] as const;

interface ReadyLine {
  ready?: boolean;
  httpPort?: number;
  wsPort?: number;
  bootId?: string;
}

/** Spawn a pair binary and resolve with its `{"ready":true,...}` line (ports + bootId). */
function spawnReady(bin: string, configPath: string, label: string): Promise<{ proc: ChildProcess; ready: ReadyLine }> {
  const proc = spawn(bin, ["--config", configPath], { stdio: ["ignore", "pipe", "inherit"] });
  return new Promise((resolveReady, reject) => {
    const rl = createInterface({ input: proc.stdout! });
    const timer = setTimeout(() => {
      proc.kill("SIGKILL");
      reject(new Error(`${label} did not become ready in time (${bin} --config ${configPath})`));
    }, 15_000);
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      clearTimeout(timer);
      reject(new Error(`${label} exited before ready (code ${code}, signal ${signal})`));
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

export async function startPair(opts: PairOptions): Promise<Pair> {
  mkdirSync(opts.dataDir, { recursive: true });
  const bindHost = opts.bindHost ?? "127.0.0.1";
  const replicatorBin =
    process.env.RINDLE_REPLICATOR_BIN ?? join(repoRoot, "rust/target/debug/rindle-replicator");
  const rindledBin = process.env.RINDLED_BIN ?? process.env.RINDLED_BINARY_PATH ?? join(repoRoot, "rust/target/debug/rindled");

  // ── the write-master: holds the authoritative data, fans out on `masterWsPort`, accepts writes on
  // `masterHttpPort` (bearer-gated on a public bind). Base `tables` = the fixed demo schema. ──
  const masterConfig = join(opts.dataDir, "master.json");
  writeFileSync(
    masterConfig,
    JSON.stringify(
      {
        db: join(opts.dataDir, "master.db"),
        name: "rindle-master",
        httpPort: opts.masterHttpPort ?? 7611,
        wsPort: opts.masterWsPort ?? 7610,
        bindHost,
        ...(opts.writeToken ? { authToken: opts.writeToken } : {}),
        tables,
      },
      null,
      2,
    ),
  );
  const master = await spawnReady(replicatorBin, masterConfig, "rindle-replicator (master)");
  const masterWsPort = master.ready.wsPort ?? opts.masterWsPort ?? 7610;
  const masterUrl = `http://127.0.0.1:${master.ready.httpPort}`;

  // ── the read-follower: a BARE rindled tailing the master over loopback; schema arrives via the
  // master's genesis `ddl` entry. Public ws + read control plane. ──
  const followerConfig = join(opts.dataDir, "follower.json");
  writeFileSync(
    followerConfig,
    JSON.stringify(
      {
        db: join(opts.dataDir, "follower.db"),
        httpPort: opts.httpPort ?? 7600,
        wsPort: opts.wsPort ?? 7601,
        bindHost,
        ...(opts.authToken ? { authToken: opts.authToken } : {}),
        nWorkers: opts.nWorkers ?? 3,
        defaultIdleTtlMs: opts.idleTtlMs ?? 15_000,
        tables: [],
        sources: [
          { kind: "replicator", name: "rindle-master", url: `ws://127.0.0.1:${masterWsPort}/subscribe` },
        ],
      },
      null,
      2,
    ),
  );
  let follower: { proc: ChildProcess; ready: ReadyLine };
  try {
    follower = await spawnReady(rindledBin, followerConfig, "rindled (follower)");
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
    followerPid: follower.proc.pid,
    close: () => procs.forEach((p) => p.kill("SIGTERM")),
  };
}
