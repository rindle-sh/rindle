// Test fixture: boot the app's ONE topology (design 214) — a `rindle-replicator` write-master
// plus a bare follower `rindled` — with `migrations/` applied against the LIVE master via the
// `rindle` CLI (exactly `pnpm migrate` / the deploy), the schema reaching the follower as `ddl`
// change-log entries. Replaces the retired standalone-daemon boot (`migrate-fixture.ts`): the
// daemon serves no write endpoints, so seeds go through the master and each write→read boundary
// polls the follower to its catch-up point (`followerCatchUp`) — the same eventual visibility a
// production deployment has.

import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { HttpRindleDaemonClient } from "@rindle/daemon-client";

const appRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const repoRoot = resolve(appRoot, "../..");
const migrationsDir = join(appRoot, "migrations");

export interface PairHandle {
  /** The master's write ingress — seeds/mutations/migrations go here (no bearer token). */
  masterUrl: string;
  /** Bearer for the master's public `/v1/sql/*` surface, where authoritative mutations execute. */
  sqlToken: string;
  /** The follower's private control plane — reads/materialize/stats (bearer-auth'd). */
  daemonUrl: string;
  /** The follower's public ws — client subscriptions. */
  daemonWsUrl: string;
  /** Typed client at the master (writes). */
  master: HttpRindleDaemonClient;
  /** Typed client at the follower (authenticated reads). */
  follower: HttpRindleDaemonClient;
  procs: ChildProcess[];
  /** Poll the follower until `sql` (a bare single-cell read) returns `expected`. */
  followerCatchUp(sql: string, expected: unknown, label: string): Promise<void>;
  cleanup(): void;
}

/** Cargo-build the pair binaries + the `rindle` CLI (cheap when warm). Exits on failure. */
export function buildPairBinaries(): void {
  const build = spawnSync(
    "cargo",
    ["build", "-p", "rindle-server", "--bin", "rindled", "-p", "rindle-replicator", "--bin", "rindle-replicator", "-p", "rindle-cli"],
    { cwd: join(repoRoot, "rust"), stdio: ["ignore", "inherit", "inherit"] },
  );
  if (build.status !== 0) {
    console.error("cargo build of the pair binaries failed");
    process.exit(1);
  }
}

const readReadyLine = <T>(proc: ChildProcess, label: string): Promise<T> =>
  new Promise<T>((resolveReady, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} did not become ready in 30s`)), 30_000);
    let buffer = "";
    proc.stdout?.on("data", (chunk: Buffer) => {
      buffer += String(chunk);
      const line = buffer.split("\n").find((l) => l.includes('"ready"'));
      if (line) {
        clearTimeout(timer);
        resolveReady(JSON.parse(line) as T);
      }
    });
    proc.on("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`${label} exited early with code ${code}`));
    });
  });

/** The master's public-SQL credential. Deliberately distinct from the pair's private `token`:
 *  `bind_with_sql_auth` rejects an equal pair, so reusing one secret fails at startup. */
const SQL_TOKEN = "smoke-sql-token";

/**
 * Boot master + follower into `dataDir` (caller owns the tempdir), apply `migrations/` against
 * the live master, and wait for both ready lines. `httpPort`/`wsPort` pin the FOLLOWER's ports
 * (for builds that bake the ws url); 0 (the default) picks ephemeral ports.
 */
export async function startPair(opts: {
  dataDir: string;
  token: string;
  httpPort?: number;
  wsPort?: number;
  idleTtlMs?: number;
}): Promise<PairHandle> {
  const { dataDir, token } = opts;
  const procs: ChildProcess[] = [];

  // The write master. No `tables`: the schema arrives via `rindle migrate apply` below.
  writeFileSync(
    join(dataDir, "master.json"),
    JSON.stringify({
      db: join(dataDir, "master.db"),
      name: "rindle-master",
      httpPort: 0,
      wsPort: 0,
      bindHost: "127.0.0.1",
      // The public SQL surface is where authoritative mutations execute. It must be a DIFFERENT
      // secret from the private plane's `authToken` — the replicator refuses an equal pair at start.
      sqlAuthToken: SQL_TOKEN,
      tables: [],
    }),
  );
  const masterProc = spawn(
    join(repoRoot, "rust/target/debug/rindle-replicator"),
    ["--config", join(dataDir, "master.json")],
    { stdio: ["ignore", "pipe", "inherit"] },
  );
  procs.push(masterProc);
  const masterReady = await readReadyLine<{ httpPort: number; wsPort: number }>(masterProc, "rindle-replicator");
  const masterUrl = `http://127.0.0.1:${masterReady.httpPort}`;

  // Apply migrations/ against the live master — the batch `/migrate` wire, exactly `pnpm migrate`.
  const migrated = spawnSync(
    join(repoRoot, "rust/target/debug/rindle"),
    ["migrate", "apply", "--dir", migrationsDir, "--url", masterUrl],
    { stdio: ["ignore", "inherit", "inherit"] },
  );
  if (migrated.status !== 0) {
    procs.forEach((p) => p.kill("SIGKILL"));
    throw new Error("rindle migrate apply against the master failed");
  }

  // The follower: a BARE rindled — its schema arrives from the master's ddl entries.
  writeFileSync(
    join(dataDir, "follower.json"),
    JSON.stringify({
      db: join(dataDir, "follower.db"),
      httpPort: opts.httpPort ?? 0,
      wsPort: opts.wsPort ?? 0,
      authToken: token,
      nWorkers: 2,
      ...(opts.idleTtlMs !== undefined ? { defaultIdleTtlMs: opts.idleTtlMs } : {}),
      tables: [],
      sources: [
        { kind: "replicator", name: "rindle-master", url: `ws://127.0.0.1:${masterReady.wsPort}/subscribe` },
      ],
    }),
  );
  const daemonProc = spawn(
    join(repoRoot, "rust/target/debug/rindled"),
    ["--config", join(dataDir, "follower.json")],
    { stdio: ["ignore", "pipe", "inherit"] },
  );
  procs.push(daemonProc);
  const ready = await readReadyLine<{ httpPort: number; wsPort: number }>(daemonProc, "rindled (follower)");

  const daemonUrl = `http://127.0.0.1:${ready.httpPort}`;
  const follower = new HttpRindleDaemonClient({
    baseUrl: daemonUrl,
    headers: { authorization: `Bearer ${token}` },
  });

  return {
    masterUrl,
    sqlToken: SQL_TOKEN,
    daemonUrl,
    daemonWsUrl: `ws://127.0.0.1:${ready.wsPort}`,
    master: new HttpRindleDaemonClient({ baseUrl: masterUrl }),
    follower,
    procs,
    async followerCatchUp(sql: string, expected: unknown, label: string): Promise<void> {
      const start = Date.now();
      for (;;) {
        const out = await follower.executeSqlRead({ sql }).catch(() => ({ rows: [] as unknown[][] }));
        if (out.rows.length && out.rows[0][0] === expected) return;
        if (Date.now() - start > 15_000) {
          throw new Error(`${label}: follower never reached ${String(expected)} (last: ${JSON.stringify(out.rows)})`);
        }
        await new Promise((r) => setTimeout(r, 25));
      }
    },
    cleanup() {
      procs.forEach((p) => p.kill("SIGKILL"));
    },
  };
}
