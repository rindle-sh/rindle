// Seed the corpus (the reason the client paginates) across the four normalized tables — pushed as
// ONE foreign SqlTxn carrying an idempotency key: the file-backed engine won't re-seed across
// restarts (the key is durable). Retries until the write ingress answers, so the dev web script can
// run it while `rindle up` is still booting the fleet + finishing migrations/schema generation.
//
// ONE topology (design 214): the seed is a WRITE, so it targets the `rindle-replicator` write-master
// — NOT the read-only follower `rindled`. The write URL comes only from an explicitly named
// replicator origin, with the rendered local master port (7611) as the dev default.

import { HttpRindleDaemonClient } from "@rindle/daemon-client";
import type { SqlStatement, WireValue } from "@rindle/daemon-client";

import { ISSUE_PRIORITIES, ISSUE_STATUSES } from "../shared/app-def.ts";

const WRITE_URL =
  process.env.RINDLE_REPLICATOR_URL ??
  process.env.REPLICATOR_ORIGIN ??
  "http://127.0.0.1:7611";
const WRITE_TOKEN =
  process.env.RINDLE_REPLICATOR_TOKEN ??
  process.env.WRITE_TOKEN ??
  process.env.RINDLE_DAEMON_TOKEN ??
  "dev-daemon-token";

export const SEED_COUNT = Number(process.env.SEED_COUNT ?? 5000);
// SQLite's default bound-parameter cap is 999. The widest row (issue, 7 cols) means <=142 rows per
// statement; 100 keeps every table well under the cap.
const ROWS_PER_STATEMENT = 100;

const PHRASES = [
  "ws reconnect drops the lease",
  "pagination cursor skips a row after edit",
  "dark mode flickers on load",
  "comment composer double-fires on touch",
  "title edit loses focus mid-word",
  "daemon restart invalidates sessions",
  "sort order unstable for equal timestamps",
  "snapshot larger than expected",
  "rejected mutation toast lingers",
  "owner check bypassed by stale cache",
];
// The fixed cast: a short login handle paired with a real display name. Issue owners cycle through
// the people; the bot authors every seeded description.
const PEOPLE = [
  { id: "amara", name: "Amara Okafor" },
  { id: "li", name: "Li Wei" },
  { id: "rohan", name: "Rohan Kapoor" },
  { id: "elena", name: "Elena Soto" },
  { id: "taro", name: "Taro Nakamura" },
];
const TAGS = ["daemon", "sync", "query", "ux", "docs", "api", "perf", "wasm"];
const BOT = { id: "rindle-bot", name: "Rindle Bot" };
const OWNER_IDS = PEOPLE.map((p) => p.id);

/** `seed-000123` for issue/user-stable ids. */
const pad = (n: number): string => String(n).padStart(6, "0");
const issueId = (n: number): string => `seed-${pad(n)}`;
const at = (n: number): number => 1700000000000 + n * 60_000; // deterministic OLD timestamps, minutes apart

/** Chunk `count` rows into INSERT statements of `ROWS_PER_STATEMENT` each. `row(n)` returns the
 *  bound params for row `n`; `cols` is the column count (for the `(?, …)` tuple). */
function insertChunks(
  table: string,
  columns: string,
  cols: number,
  count: number,
  row: (n: number) => WireValue[],
): SqlStatement[] {
  const tuple = `(${Array(cols).fill("?").join(", ")})`;
  const statements: SqlStatement[] = [];
  for (let offset = 0; offset < count; offset += ROWS_PER_STATEMENT) {
    const rows = Math.min(ROWS_PER_STATEMENT, count - offset);
    const params: WireValue[] = [];
    for (let i = 0; i < rows; i++) params.push(...row(offset + i));
    statements.push({
      sql: `INSERT INTO ${table} (${columns}) VALUES ${Array(rows).fill(tuple).join(", ")}`,
      params,
    });
  }
  return statements;
}

export function seedStatements(count: number): SqlStatement[] {
  // The fixed cast: every person + the description bot, one user row each.
  const users = [...PEOPLE, BOT];
  const userStmt: SqlStatement = {
    sql: `INSERT INTO user (id, name) VALUES ${Array(users.length).fill("(?, ?)").join(", ")}`,
    params: users.flatMap((person) => [person.id, person.name]),
  };

  const issues = insertChunks(
    "issue",
    "id, title, status, priority, ownerId, createdAt, updatedAt",
    7,
    count,
    (n) => [
      issueId(n),
      `#${n} ${PHRASES[n % PHRASES.length]}`,
      ISSUE_STATUSES[n % ISSUE_STATUSES.length],
      ISSUE_PRIORITIES[n % ISSUE_PRIORITIES.length],
      OWNER_IDS[n % OWNER_IDS.length],
      at(n),
      at(n),
    ],
  );

  // One description comment per issue (authored by the bot, timestamped at the issue's creation so
  // it sorts first — the "description = earliest comment" convention).
  const descriptions = insertChunks(
    "comment",
    "id, issueId, authorId, body, createdAt",
    5,
    count,
    (n) => [`${issueId(n)}-desc`, issueId(n), BOT.id, `Description for ${PHRASES[n % PHRASES.length]}.`, at(n)],
  );

  // Two distinct tags per issue ((n) and (n+3) mod 8 never collide), one tag row each.
  const tags = insertChunks("tag", "id, issueId, name", 3, count * 2, (k) => {
    const n = k >> 1; // two tag rows per issue
    const which = k & 1;
    const name = which === 0 ? TAGS[n % TAGS.length] : TAGS[(n + 3) % TAGS.length];
    return [`${issueId(n)}-tag-${which}`, issueId(n), name];
  });

  return [userStmt, ...issues, ...descriptions, ...tags];
}

export interface SeedOptions {
  url?: string;
  token?: string;
  count?: number;
}

export async function seed({
  url = WRITE_URL,
  token = WRITE_TOKEN,
  count = SEED_COUNT,
}: SeedOptions = {}): Promise<void> {
  const daemon = new HttpRindleDaemonClient({
    baseUrl: url,
    headers: { authorization: `Bearer ${token}` },
  });
  for (let attempt = 1; ; attempt++) {
    try {
      const out = await daemon.executeSqlTxn({
        idempotencyKey: `seed-issues-v3-${count}`,
        statements: seedStatements(count),
      });
      console.log(
        out.applied
          ? `[seed] inserted ${count} issues across user/issue/tag/comment (cv ${out.cv})`
          : `[seed] already seeded (idempotency key absorbed the replay)`,
      );
      return;
    } catch (err) {
      if (attempt >= 20) throw err;
      await new Promise((r) => setTimeout(r, 500)); // daemon still booting — retry
    }
  }
}

// Allow `node server/seed.ts` standalone.
if (import.meta.url === `file://${process.argv[1]}`) await seed();
