// Run the serverless-local-first example against the RUST pair (design 214: ONE topology —
// a `rindle-replicator` write-master + a follower `rindled`): boots both binaries with the
// example's schema (minted to the follower via the genesis `ddl` entry), then executes
// app.mjs unchanged via env overrides — the end-goal topology with the production engine in
// both seats.
//
//   node --conditions=@rindle/source examples/serverless-local-first/rust-daemon.mjs

import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";

import { buildPairBinaries, startPair } from "../../test/spawn-pair.mjs";

const here = fileURLToPath(new URL(".", import.meta.url));

buildPairBinaries();
const pair = await startPair({
  tables: [{ name: "issue", columns: ["id", "title", "score"], pk: [0], types: ["number", "string", "number"] }],
  authToken: "daemon-secret",
  prefix: "rindled-example-",
});

const app = spawn(process.execPath, ["--conditions=@rindle/source", join(here, "app.mjs")], {
  stdio: "inherit",
  env: {
    ...process.env,
    RINDLE_DAEMON_URL: pair.readUrl,
    RINDLE_DAEMON_WS: pair.wsUrl,
    RINDLE_DAEMON_TOKEN: "daemon-secret",
    RINDLE_REPLICATOR_URL: pair.writeUrl,
    RINDLE_DATABASE_TOKEN: pair.sqlAuthToken,
  },
});
const code = await new Promise((resolveExit) => app.on("exit", resolveExit));
pair.cleanup();
if (code === 0) console.log("example passed against the RUST pair");
process.exit(code ?? 1);
