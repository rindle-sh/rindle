// One-command dev: boots the ONE topology (design 214) — a `rindle-replicator` write-master + one
// `rindled` read-follower, supervised by `rindle up` — plus the API server and the Vite dev server,
// and tears them all down together. There is one deployment shape: writes go to the master, while app
// reads/subscriptions enter through the stable fleet edge. `followers = 1` is a fleet of one, the smallest
// shape (rindle.ncl).
//
//   pnpm dev          # builds the pair + `rindle` + browser WASM, boots everything, applies
//                     # migrations + seeds
//   pnpm dev:oidc     # ALSO boots headwaters (wrangler dev :8787) + flips identity to real JWT/OIDC
//
// The DATABASE tier is one command: `rindle up --migrate --gen --watch` renders rindle.ncl, then
// supervises the fleet (replicator :7611/:7610 + follower :7600/:7601 + edge :7650, respawn on exit / migrate-at-
// bounce), applies the migrations/ DDL against the WRITE-MASTER once it's ready (the DDL replicates to
// the follower as `ddl` change-log entries), generates the `@rindle/client` schema TS from the
// FOLLOWER's introspected `/schema`, and re-runs both on every *.sql change — so this file no longer
// hand-rolls a respawn loop. The schema is SQL-first: migrations/*.sql is the source of truth and
// shared/schema.gen.ts is generated from it (committed, regenerated here on every boot). Seeding is a
// WRITE, so it targets the master: once the follower's schema is live we run the idempotent seed
// against RINDLE_REPLICATOR_URL. `pnpm migrate` drives the same apply+seed against the master for CI/
// prod. The rest is the APP tiers (api, vite), out of the CLI's scope.
//
// In --oidc mode the forum verifies a headwaters-minted JWT instead of the dev `x-forum-user` header:
// the "Sign in with Rindle Cloud" button (src/cloud-auth.ts) → headwaters /authorize → token back to
// /auth/callback → Bearer on every API call → server/auth-oidc.ts verifies it against the JWKS (§3.3).

import { spawn, spawnSync } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { copyFileSync, existsSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = fileURLToPath(new URL(".", import.meta.url));
const appRoot = resolve(here, "..");
const repoRoot = resolve(appRoot, "../..");

const OIDC = process.argv.includes("--oidc");
const HEADWATERS_ORIGIN = "http://localhost:8787"; // matches wrangler.jsonc BETTER_AUTH_URL

const DAEMON_TOKEN = "dev-daemon-token";
let DAEMON_URL: string;
let REPLICATOR_URL: string;
let wsUrl: string;
let schemaUrl: string;

const children: ChildProcess[] = []; // headwaters / api — share our process group
let supervisor: ChildProcess | undefined; // `rindle up` — its own group; it owns the rindled child
let shuttingDown = false;
const stop = () => {
  shuttingDown = true;
  // `rindle up` runs detached in its own process group and owns `rindled`, so kill the whole group to
  // take both down; the app tiers share our group and are killed individually.
  if (supervisor?.pid) {
    try {
      process.kill(-supervisor.pid, "SIGKILL");
    } catch {
      /* already gone */
    }
  }
  for (const child of children) child.kill("SIGKILL");
  process.exit(0);
};
process.on("SIGINT", stop);
process.on("SIGTERM", stop);

// Poll the FOLLOWER's control plane until the schema is live there — `rindle up --migrate` applied the
// DDL to the master and it replicated to the follower as `ddl` entries (tables appear). A real
// readiness gate that also rides through the migrate-at-bounce restart, and the same eventual
// visibility a production reader has.
async function waitForSchema(timeoutMs = 60_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${schemaUrl}/schema`, {
        headers: { authorization: `Bearer ${DAEMON_TOKEN}` },
      });
      if (res.ok) {
        const body = (await res.json()) as { tables?: unknown[] };
        if (Array.isArray(body.tables) && body.tables.length > 0) return true;
      }
    } catch {
      /* not up yet, or mid-bounce — retry */
    }
    await new Promise((r) => setTimeout(r, 300));
  }
  return false;
}

// In OIDC mode, stand up headwaters (the SaaS identity provider) alongside the forum: ensure local
// secrets + an applied local D1, then boot `wrangler dev`. The forum is then configured to verify the
// JWTs it mints. Best-effort dev tooling — the daemon/api/vite below still come up regardless.
function bootHeadwaters(): void {
  const dir = join(repoRoot, "headwaters");
  const wrangler = join(dir, "node_modules/.bin/wrangler");

  // 1. Local secrets for `wrangler dev` (gitignored) — BETTER_AUTH_SECRET is required to boot.
  const devVars = join(dir, ".dev.vars");
  if (!existsSync(devVars) && existsSync(join(dir, ".dev.vars.example"))) {
    copyFileSync(join(dir, ".dev.vars.example"), devVars);
    console.log("[dev] headwaters: created .dev.vars from the example (dev-only secret)");
  }

  // 2. Apply migrations to the LOCAL D1 (creates user/session/jwks/… in miniflare's sqlite).
  console.log("[dev] headwaters: applying local D1 migrations...");
  spawnSync(wrangler, ["d1", "migrations", "apply", "headwaters", "--local"], { cwd: dir, stdio: "inherit" });

  // 3. Boot the Worker (auth endpoints, /authorize, JWKS) on :8787.
  console.log(`[dev] headwaters: starting wrangler dev on ${HEADWATERS_ORIGIN} ...`);
  const hw = spawn(wrangler, ["dev", "--port", "8787"], { cwd: dir, stdio: "inherit" });
  children.push(hw);

  // 4. Point the forum's server tiers + browser bundle at it.
  process.env.FORUM_AUTH = "oidc";
  process.env.HEADWATERS_ISSUER = HEADWATERS_ORIGIN;
  process.env.VITE_FORUM_AUTH = "oidc";
  process.env.VITE_HEADWATERS_ORIGIN = HEADWATERS_ORIGIN;
}

if (OIDC) bootHeadwaters();

function node(script: string, env: NodeJS.ProcessEnv = {}): ChildProcess {
  const child = spawn(
    process.execPath,
    ["--conditions=@rindle/source", join(appRoot, script)],
    { stdio: "inherit", env: { ...process.env, RINDLE_DAEMON_TOKEN: DAEMON_TOKEN, ...env } },
  );
  children.push(child);
  return child;
}

// Build the four binaries from the workspace. They land side by side in rust/target/debug, so
// `rindle up` discovers every supervised component without an application-level runtime package.
console.log("[dev] building rindle + rindled + rindle-replicator + rindle-dev-edge (cargo)...");
for (const pkg of [
  ["build", "-p", "rindle-cli"], // the `rindle` CLI/supervisor
  ["build", "-p", "rindle-server", "--bin", "rindled"], // the read follower
  ["build", "-p", "rindle-replicator", "--bin", "rindle-replicator"], // the write-master
  ["build", "-p", "rindle-dev-edge"], // the stable local HTTP/ws affinity edge
]) {
  const build = spawnSync("cargo", pkg, { cwd: join(repoRoot, "rust"), stdio: "inherit" });
  if (build.status !== 0) process.exit(1);
}
console.log("[dev] building the browser WASM engine...");
const wasmBuild = spawnSync(join(repoRoot, "packages/wasm/build.sh"), [], {
  cwd: repoRoot,
  stdio: "inherit",
});
if (wasmBuild.status !== 0) process.exit(1);
const rindleBin = join(repoRoot, "rust/target/debug/rindle");

// Resolve every app-facing endpoint from rindle.ncl before starting any app process. `rindle up`
// renders the same values again for supervision; this early render lets the harness wait/migrate and
// configure Node/Vite without copying topology ports here.
const rendered = spawnSync(rindleBin, ["render"], { cwd: appRoot, stdio: "inherit" });
if (rendered.status !== 0) process.exit(1);
const manifest = JSON.parse(readFileSync(join(appRoot, "rindle.json"), "utf8")) as {
  bindings?: { readHttpUrl?: string; writeHttpUrl?: string; subscribeWsUrl?: string };
  components?: Record<string, { kind?: string; controlUrl?: string }>;
};
const bindings = manifest.bindings;
const follower = Object.values(manifest.components ?? {}).find(
  (component) => component.kind === "rindled" && component.controlUrl,
);
if (!bindings?.readHttpUrl || !bindings.writeHttpUrl || !bindings.subscribeWsUrl || !follower?.controlUrl) {
  console.error("[dev] rendered rindle.json has no app bindings — rebuild the rindle CLI");
  process.exit(1);
}
DAEMON_URL = bindings.readHttpUrl;
REPLICATOR_URL = bindings.writeHttpUrl;
wsUrl = bindings.subscribeWsUrl;
schemaUrl = follower.controlUrl;

// The database tier, one command: `rindle up` renders rindle.ncl (the ONE topology — replicator
// write-master + one follower; §214) into `.rindle/` + `rindle.json` at the app root, supervises the
// pair, regenerates the client schema off the FOLLOWER's `/schema` (`--gen` → shared/schema.gen.ts),
// and re-runs it on every *.sql change (`--watch`). No `--config`: the topology is rindle.ncl,
// discovered in the cwd. Migrations are applied explicitly below (against the master) rather than via
// `rindle up --migrate` so this dev boot owns the migrate→seed ordering and can BAIL on a migrate
// failure (a fire-and-forget `up --migrate` only logs it and keeps supervising). Both hit the same
// master `/migrate`.
// Detached into its own process group so `stop()` can group-kill it together with the pair it owns;
// stdin ignored (it never reads it — avoids TTY signals).
const migrationsDir = join(appRoot, "migrations");
const schemaGen = join(appRoot, "shared/schema.gen.ts"); // SQL-first: generated from migrations/*.sql
supervisor = spawn(
  rindleBin,
  ["up", "--gen", schemaGen, "--watch", "--dir", migrationsDir],
  {
    stdio: ["ignore", "inherit", "inherit"],
    detached: true,
    cwd: appRoot, // where rindle.ncl lives + where `.rindle/`/`rindle.json` render
    env: { ...process.env, RINDLE_DAEMON_TOKEN: DAEMON_TOKEN },
  },
);
supervisor.on("exit", (code, signal) => {
  if (shuttingDown) return;
  console.error(`[dev] rindle up exited (code ${code}, signal ${signal}) — shutting down`);
  stop();
});
/** Group-kill the supervised fleet and exit(1) — the dev-boot bail path. */
function bail(message: string): never {
  console.error(`[dev] ${message}`);
  if (supervisor?.pid) {
    try {
      process.kill(-supervisor.pid, "SIGKILL");
    } catch {
      /* already gone */
    }
  }
  process.exit(1);
}

// Apply migrations to the WRITE-MASTER once its ingress is reachable (a WRITE — it enters at the
// master's /migrate and replicates to the follower as `ddl` entries). Idempotent (the master dedups
// by id); exactly what `pnpm migrate` runs.
async function waitForMaster(timeoutMs = 60_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      if ((await fetch(`${REPLICATOR_URL}/version`)).ok) return true;
    } catch {
      /* master still booting — retry */
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  return false;
}
if (!(await waitForMaster())) bail("the write-master (rindle-replicator) did not come up in time");
const migrated = spawnSync(
  rindleBin,
  ["migrate", "apply", "--dir", migrationsDir, "--url", REPLICATOR_URL, "--token", DAEMON_TOKEN],
  { stdio: "inherit" },
);
if (migrated.status !== 0) bail("applying migrations to the write-master failed — check migrations/");

// Wait until the schema is live on the FOLLOWER (the master's DDL replicated + it re-introspected),
// then the API tier and the idempotent seed (a fresh DB shows an empty forum until the seed lands;
// re-running is a no-op).
if (!(await waitForSchema())) bail("the follower's schema did not replicate in time — check migrations/");
// The API tier reads off the FOLLOWER and writes to the MASTER (SplitDaemonClient — server/app-api.ts
// `resolveForumDaemon`). The seed is a WRITE, so it targets the master directly.
node("server/api.ts", { RINDLE_DAEMON_URL: DAEMON_URL, RINDLE_REPLICATOR_URL: REPLICATOR_URL });
node("server/seed.ts", { RINDLE_DAEMON_URL: DAEMON_URL, RINDLE_REPLICATOR_URL: REPLICATOR_URL });

// The web tier: Vite programmatically, with /api proxied to the API server and the daemon's PUBLIC ws
// url handed to the client bundle.
process.env.VITE_FLEET_WS = wsUrl;
const { createServer } = await import("vite");
const vite = await createServer({ root: appRoot });
await vite.listen();
vite.printUrls();
console.log("[dev] the one-topology pair (replicator :7611 + follower :7600) supervised by `rindle up` — open two browser windows to watch sync");
