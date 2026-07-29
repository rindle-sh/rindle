// The shared-sandbox RESET: snap the issue table back to the pristine seed baseline in ONE atomic
// txn — a leading `DELETE FROM issue` then the same seed corpus `server/seed.ts` produces. This is
// what the Cloudflare cron (`worker.ts` `scheduled`) calls to keep a PUBLIC, anyone-can-write demo
// self-healing: everything visitors created OR edited since the last reset is dropped, and the 5k
// baseline returns.
//
// Unlike the boot seed (idempotency-keyed, so a restart never re-inserts), the reset carries NO
// key: the DELETE-all makes each run fresh by construction. Run on an empty table it is just the
// seed, so the FIRST cron fire also seeds a fresh deployment.
//
// ONE topology (design 214): the reset is a WRITE, so it targets the `rindle-replicator`
// write-master — NOT the read-only follower `rindled` (whose write plane 404s). Callers (the CF
// cron, deploy.sh first-seed) thread the master's control-plane URL; the local default is the
// rendered replicator http port (7611).

import { HttpRindleDaemonClient } from "@rindle/daemon-client";
import type { FetchLike, SqlStatement } from "@rindle/daemon-client";

import { SEED_COUNT, seedStatements } from "./seed.ts";

const WRITE_URL =
  process.env.RINDLE_REPLICATOR_URL ??
  process.env.REPLICATOR_ORIGIN ??
  "http://127.0.0.1:7611";
const WRITE_TOKEN =
  process.env.RINDLE_REPLICATOR_TOKEN ??
  process.env.WRITE_TOKEN ??
  process.env.RINDLE_DAEMON_TOKEN ??
  "dev-daemon-token";
const RESET_TABLES_CHILD_FIRST = ["tag", "comment", "issue", "user"] as const;

export interface ResetOptions {
  url?: string;
  token?: string;
  count?: number;
  fetch?: FetchLike;
}

export interface ResetResult {
  count: number;
  cv?: number;
}

export async function reset({
  url = WRITE_URL,
  token = WRITE_TOKEN,
  count = SEED_COUNT,
  fetch,
}: ResetOptions = {}): Promise<ResetResult> {
  const daemon = new HttpRindleDaemonClient({
    baseUrl: url,
    headers: { authorization: `Bearer ${token}` },
    fetch,
  });
  // DELETE-all (child tables first) then the full seed corpus, applied as one transaction (atomic —
  // viewers never see a half-empty table). seedStatements already chunks the inserts under SQLite's
  // bound-param cap.
  const wipe: SqlStatement[] = RESET_TABLES_CHILD_FIRST.map((name) => ({ sql: `DELETE FROM ${name}` }));
  const statements: SqlStatement[] = [...wipe, ...seedStatements(count)];
  const out = await daemon.executeSqlTxn({ statements });
  return { count, cv: out.cv };
}

// Allow `node server/reset.ts` standalone (deploy.sh uses it to seed the daemon right after the
// first deploy, so there's no empty-table window before the cron's first fire).
if (import.meta.url === `file://${process.argv[1]}`) {
  const r = await reset();
  console.log(`[reset] sandbox reset to ${r.count} issues (cv ${r.cv})`);
}
