// Seed a small starter corpus — a few categories, threads, posts, users, and votes — pushed as ONE
// foreign SqlTxn carrying an idempotency key: the file-backed engine won't re-seed across restarts
// (the key is durable). Retries until the write ingress answers, so dev.ts can fire-and-forget it
// during startup.
//
// ONE topology (design 214): the seed is a WRITE, so it targets the `rindle-replicator` write-master
// — NOT the read-only follower `rindled`. The write URL comes only from an explicitly named
// replicator origin, with the rendered local master port (7611) as the dev default.
//
// Unlike the issue tracker (5k rows to force pagination), a forum reads better with a curated handful;
// crank SEED_THREADS if you want to exercise the windowing.

import { HttpRindleDaemonClient } from "@rindle/daemon-client";
import type { SqlStatement, WireValue } from "@rindle/daemon-client";

import { voteId } from "../shared/app-def.ts";

// No local-port fallback: ports are allocated per project, so a hardcoded default would point at
// nothing — or at another project's write-master. `pnpm dev` injects this from rindle.json.
const DAEMON_URL = process.env.RINDLE_REPLICATOR_URL ?? process.env.REPLICATOR_ORIGIN;
if (!DAEMON_URL) {
  throw new Error("RINDLE_REPLICATOR_URL is required: the seed writes and must target the write-master");
}
const DAEMON_TOKEN =
  process.env.RINDLE_REPLICATOR_TOKEN ??
  process.env.WRITE_TOKEN ??
  process.env.RINDLE_DAEMON_TOKEN ??
  "dev-daemon-token";

const PEOPLE = [
  { id: "amara", displayName: "Amara Okafor" },
  { id: "li", displayName: "Li Wei" },
  { id: "rohan", displayName: "Rohan Kapoor" },
  { id: "elena", displayName: "Elena Soto" },
  { id: "taro", displayName: "Taro Nakamura" },
];

const CATEGORIES = [
  { id: "cat-announce", slug: "announcements", name: "Announcements", description: "Releases and news from the Rindle team.", position: 0 },
  { id: "cat-help", slug: "help", name: "Help & Support", description: "Stuck on a query, a sync, or the daemon? Ask here.", position: 1 },
  { id: "cat-show", slug: "show-and-tell", name: "Show & Tell", description: "Built something on Rindle? Show it off.", position: 2 },
  { id: "cat-meta", slug: "meta", name: "Meta", description: "About the forum itself.", position: 3 },
];

// A few seeded threads (categoryId, author, title, opening body). Replies are generated below.
const THREADS = [
  { id: "th-0001", categoryId: "cat-announce", author: "amara", title: "Rindle 0.1.0 is out", body: "Query once, maintain forever. The incremental engine now ships a typed query builder and live materialized views." },
  { id: "th-0002", categoryId: "cat-help", author: "rohan", title: "How do correlated subqueries stay fast both directions?", body: "Trying to understand how a changed child re-finds its parents. Is it the reverse index that makes it cheap?" },
  { id: "th-0003", categoryId: "cat-help", author: "elena", title: "Optimistic write snapped back — expected?", body: "I deleted a post I don't own and it reappeared. Working as intended?" },
  { id: "th-0004", categoryId: "cat-show", author: "li", title: "A live leaderboard in 40 lines", body: "countAs + orderBy and the standings just stay correct. No polling. Demo inside." },
  { id: "th-0005", categoryId: "cat-meta", author: "taro", title: "This forum is itself a Rindle app", body: "Threads, reply counts, and vote tallies are all incremental views. Meta enough?" },
];

const REPLY_BODIES = [
  "Great question — the reverse index is exactly it.",
  "This matches what I'm seeing too.",
  "Thanks, that clears it up.",
  "Love this. Mind sharing the schema?",
  "+1, ran into the same thing yesterday.",
];

const at = (n: number): number => 1717000000000 + n * 60_000; // deterministic timestamps, minutes apart

function valuesClause(rowCount: number, colCount: number): string {
  const tuple = `(${Array(colCount).fill("?").join(", ")})`;
  return Array(rowCount).fill(tuple).join(", ");
}

export function seedStatements(): SqlStatement[] {
  const statements: SqlStatement[] = [];

  const users = [...PEOPLE];
  statements.push({
    sql: `INSERT INTO user (id, displayName, avatarUrl) VALUES ${valuesClause(users.length, 3)}`,
    params: users.flatMap((u) => [u.id, u.displayName, ""]) as WireValue[],
  });

  statements.push({
    sql: `INSERT INTO category (id, slug, name, description, position) VALUES ${valuesClause(CATEGORIES.length, 5)}`,
    params: CATEGORIES.flatMap((c) => [c.id, c.slug, c.name, c.description, c.position]) as WireValue[],
  });

  // One thread row + its opening post each; then a couple of replies per thread bumping lastPostAt.
  const posts: WireValue[][] = [];
  const votes: WireValue[][] = [];
  let postSeq = 0;

  THREADS.forEach((t, ti) => {
    const created = at(ti);
    const openingId = `${t.id}-p0`;
    posts.push([openingId, t.id, t.author, t.body, created, 0, 0]);

    let lastPostAt = created;
    const replyCount = (ti % 3) + 1;
    for (let r = 0; r < replyCount; r++) {
      postSeq++;
      const replier = PEOPLE[(ti + r + 1) % PEOPLE.length].id;
      const ts = created + (r + 1) * 5 * 60_000;
      lastPostAt = ts;
      const pid = `${t.id}-p${r + 1}`;
      posts.push([pid, t.id, replier, REPLY_BODIES[postSeq % REPLY_BODIES.length], ts, 0, 0]);
      // The opening post collects an upvote from each replier.
      votes.push([voteId(openingId, replier), openingId, replier, ts]);
    }

    statements.push({
      sql: "UPDATE thread SET lastPostAt = ? WHERE id = ?",
      params: [lastPostAt, t.id] as WireValue[],
    });
  });

  // Thread rows (with lastPostAt initially = createdAt; the UPDATEs above bump them).
  statements.unshift({
    sql: `INSERT INTO thread (id, categoryId, authorId, title, createdAt, lastPostAt, locked, pinned) VALUES ${valuesClause(THREADS.length, 8)}`,
    params: THREADS.flatMap((t, ti) => [t.id, t.categoryId, t.author, t.title, at(ti), at(ti), 0, ti === 0 ? 1 : 0]) as WireValue[],
  });

  statements.push({
    sql: `INSERT INTO post (id, threadId, authorId, body, createdAt, editedAt, deleted) VALUES ${valuesClause(posts.length, 7)}`,
    params: posts.flat(),
  });
  if (votes.length > 0) {
    statements.push({
      sql: `INSERT INTO vote (id, postId, userId, createdAt) VALUES ${valuesClause(votes.length, 4)}`,
      params: votes.flat(),
    });
  }

  return statements;
}

export interface SeedOptions {
  url?: string;
  token?: string;
}

export async function seed({ url = DAEMON_URL, token = DAEMON_TOKEN }: SeedOptions = {}): Promise<void> {
  const daemon = new HttpRindleDaemonClient({ baseUrl: url, headers: { authorization: `Bearer ${token}` } });
  for (let attempt = 1; ; attempt++) {
    try {
      const out = await daemon.executeSqlTxn({ idempotencyKey: "seed-forum-v1", statements: seedStatements() });
      console.log(
        out.applied
          ? `[seed] seeded ${CATEGORIES.length} categories + ${THREADS.length} threads (cv ${out.cv})`
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
