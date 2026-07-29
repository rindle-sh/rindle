// The canonical pair spawner (design 214: ONE topology): a `rindle-replicator` write-master
// plus a BARE follower `rindled` (its only path to the base schema is the master's genesis
// `ddl` entry), both real spawned binaries on ephemeral ports. Extracted from
// `mutator-read-parity.e2e.mjs`'s `startSplit` so every e2e that used to boot a standalone
// write-accepting daemon boots this instead.

import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const repoRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "../../../..");

/** Cargo-build both binaries of the pair (cheap when warm). Exits the process on failure. */
export function buildPairBinaries() {
  console.log("building rindled + rindle-replicator (cargo)...");
  const build = spawnSync(
    "cargo",
    ["build", "-p", "rindle-server", "--bin", "rindled", "-p", "rindle-replicator", "--bin", "rindle-replicator"],
    { cwd: join(repoRoot, "rust"), stdio: ["ignore", "inherit", "inherit"] },
  );
  if (build.status !== 0) {
    console.error("cargo build failed");
    process.exit(1);
  }
}

/** Spawn a binary and wait for its one-line `{"ready":true,...}` stdout handshake. */
export function spawnReady(bin, args, label) {
  const child = spawn(bin, args, { stdio: ["ignore", "pipe", "inherit"] });
  const ready = new Promise((resolveReady, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`${label} did not become ready in 30s`)),
      30_000,
    );
    let buffer = "";
    child.stdout.on("data", (chunk) => {
      buffer += String(chunk);
      const line = buffer.split("\n").find((l) => l.includes('"ready"'));
      if (line) {
        clearTimeout(timer);
        resolveReady(JSON.parse(line));
      }
    });
    child.on("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`${label} exited early with code ${code}`));
    });
  });
  return { child, ready };
}

/**
 * Boot the pair. `tables` is the replicator's TableSpec flavor
 * (`{name, columns, pk, types}` — ColType strings), minted to the follower via genesis ddl.
 *
 * Returns `{ writeUrl, readUrl, wsUrl, masterName, cleanup }` — writes + mutation sessions go
 * to `writeUrl` (the replicator), reads/materialize/subscribe to `readUrl`/`wsUrl` (the
 * follower, bearer-auth'd with `authToken`).
 */
export async function startPair({
  tables,
  authToken = "daemon-secret",
  // The master's PUBLIC SQL credential, where authoritative mutations execute. It must differ from
  // the private `authToken` — the replicator refuses an equal pair at startup.
  sqlAuthToken = "sql-secret",
  prefix = "rindle-pair-",
}) {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  const masterName = "rindle-master";
  writeFileSync(
    join(dir, "master.json"),
    JSON.stringify({
      db: join(dir, "master.db"),
      name: masterName,
      wsPort: 0,
      httpPort: 0,
      bindHost: "127.0.0.1",
      sqlAuthToken,
      tables,
    }),
  );
  const master = spawnReady(
    join(repoRoot, "rust/target/debug/rindle-replicator"),
    ["--config", join(dir, "master.json")],
    "rindle-replicator",
  );
  const masterInfo = await master.ready;

  writeFileSync(
    join(dir, "follower.json"),
    JSON.stringify({
      db: join(dir, "follower.db"),
      httpPort: 0,
      wsPort: 0,
      authToken,
      nWorkers: 2,
      // Bare follower: no `tables` — the schema arrives via the master's genesis `ddl`.
      sources: [
        {
          kind: "replicator",
          name: masterName,
          url: `ws://127.0.0.1:${masterInfo.wsPort}/subscribe`,
        },
      ],
    }),
  );
  const follower = spawnReady(
    join(repoRoot, "rust/target/debug/rindled"),
    ["--config", join(dir, "follower.json")],
    "rindled (follower)",
  );
  const followerInfo = await follower.ready;

  return {
    writeUrl: `http://127.0.0.1:${masterInfo.httpPort}`,
    sqlAuthToken,
    readUrl: `http://127.0.0.1:${followerInfo.httpPort}`,
    wsUrl: `ws://127.0.0.1:${followerInfo.wsPort}`,
    masterName,
    cleanup() {
      follower.child.kill("SIGKILL");
      master.child.kill("SIGKILL");
      rmSync(dir, { recursive: true, force: true });
    },
  };
}
