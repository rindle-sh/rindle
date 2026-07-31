// `pnpm migrate` — apply the forum's schema migrations via the `rindle` CLI, then seed.
//
// The wrangler model: migrations are applied by `rindle migrate apply` (the real operator CLI — the
// Rust binary shipped beside the engines), NOT a TS library. ONE topology (design 214): a migration
// is a WRITE, so it always targets the `rindle-replicator` WRITE-MASTER — the origin of the schema.
// The master applies the DDL as one co-transactional `ddl` change-log entry, fans it out to the
// follower(s) (relay_ddl → `apply_follower_ddl`), and re-derives its capture schema IN-process, so
// followers pick up the new tables over replication (bounce-and-resume on reshape) with no per-node
// migrate. `rindle migrate apply` waits for a self-restart only when the target reports one.
//
//   • dev   — the local pair (server/dev.ts / rindle.ncl). The master is at :7611.
//   • prod  — the Headwaters app hostname (path-routed to its write-master).
//
//   pnpm migrate                        # apply (against the master) → seed
//   node server/migrate.ts --deploy     # CI/prod: apply only (caller seeds separately)
//
// Idempotent throughout: the master dedups migrations by `id`, and the seed carries a durable
// idempotency key — re-running is a no-op.

import { spawnSync } from "node:child_process";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { seed } from "./seed.ts";

const appRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const repoRoot = resolve(appRoot, "../..");
const migrationsDir = join(appRoot, "migrations");

// Migrations + seed are WRITES → the write-master, named explicitly. There is no local-port
// fallback: the fleet's ports are allocated PER PROJECT, so a hardcoded default would either point
// at nothing or, worse, at another project's master. `pnpm dev` injects this from the rendered
// rindle.json bindings; `rindle render` prints the resolved URLs.
const DAEMON_URL = process.env.RINDLE_REPLICATOR_URL ?? process.env.REPLICATOR_ORIGIN;
if (!DAEMON_URL) {
  throw new Error("RINDLE_REPLICATOR_URL is required: migrations are writes and must target the write-master");
}
const DAEMON_TOKEN =
  process.env.RINDLE_REPLICATOR_TOKEN ??
  process.env.WRITE_TOKEN ??
  process.env.RINDLE_DAEMON_TOKEN ??
  "dev-daemon-token";
const deploy = process.argv.includes("--deploy");

/** Run a command inheriting stdio; throw with a legible message on failure. */
function run(cmd: string, args: string[], cwd?: string): void {
  const r = spawnSync(cmd, args, { stdio: "inherit", cwd });
  if (r.error) throw r.error;
  if (r.status !== 0) {
    throw new Error(`\`${cmd} ${args.join(" ")}\` exited with ${r.status ?? r.signal}`);
  }
}

async function main(): Promise<void> {
  // Build the `rindle` CLI (same cargo workspace as rindled; the binary lands at rust/target/debug/rindle).
  run("cargo", ["build", "-p", "rindle-cli"], join(repoRoot, "rust"));
  const rindle = join(repoRoot, "rust/target/debug/rindle");

  // Apply pending *.sql migrations through the write-master's /migrate. Idempotent (dedup by id),
  // additive-DDL-linted; the master re-derives its capture schema in-process (no restart wait) and
  // the DDL replicates to the follower(s).
  run(rindle, ["migrate", "apply", "--dir", migrationsDir, "--url", DAEMON_URL, "--token", DAEMON_TOKEN]);

  if (deploy) {
    console.log("[migrate] --deploy: applied; the caller seeds next.");
    return;
  }
  console.log("[migrate] seeding…");
  await seed({ url: DAEMON_URL, token: DAEMON_TOKEN });
}

main().catch((err) => {
  console.error(`[migrate] ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
