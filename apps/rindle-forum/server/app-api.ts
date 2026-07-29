// The app authority, runtime-AGNOSTIC. The serverless-shaped tier's actual logic: it resolves named
// queries to ASTs (the daemon mints opaque leases), runs the AUTHORITATIVE mutators into approved SQL
// across the five normalized tables, enforces policy (identity required to write, ownership-gated
// edit/delete, lock enforcement, a "spam" rejection demo), and talks to the daemon over the private
// bearer-auth'd control plane.
//
// It is deliberately free of any host: `server/api.ts` wraps it in a Node HTTP server for local dev,
// and `worker.ts` wraps the SAME factory in a Cloudflare Worker `fetch` handler. The only per-host
// inputs are the daemon's control-plane URL + token and the AuthProvider that resolves the principal.
//
// ONE topology (design 214): every deployment is a `rindle-replicator` write-master + one or more
// `rindled` read-followers. That split is wired at the construction site below: `createForumApi` hands
// the api-server a `SplitDaemonClient` — writes (executeSqlTxn / rejectMutation / mutation sessions)
// to the replicator write-master, reads/control (materialize / query / …) to the follower. The
// follower has no write plane, so a write must never reach it. URLs are pure config (env), so no
// mutator/query/policy code above this seam changes.

import { createRindleApiServer, defineApiMutators, registerQueries, SplitDaemonClient } from "@rindle/api-server";
import type { ApiMutators, MutationContext, RindleApiServer, SqlMutationTx } from "@rindle/api-server";
import { HttpRindleDaemonClient } from "@rindle/daemon-client";
import type { RindleDaemonClient } from "@rindle/daemon-client";
import type { Fetch } from "@rindle/sql-client";

import {
  addReplyArgs,
  createThreadArgs,
  editPostArgs,
  normalizeBody,
  normalizeSubject,
  normalizeTitle,
  postIdArgs,
  removeVoteArgs,
  upvoteArgs,
  voteId,
} from "../shared/app-def.ts";
import type { ForumIdentity } from "../shared/auth.ts";
import { categoriesQuery, categoryBySlugQuery } from "../src/components/CategoryCard.queries.ts";
import { threadsPageQuery } from "../src/components/ThreadCard.queries.ts";
import { threadDetailQuery } from "../src/components/ThreadView.queries.ts";

/** The authority's principal is the verified identity (or undefined when anonymous). The host shells
 *  resolve it via the AuthProvider (server/auth-dev.ts by default) and put it on the ApiContext. */
export type User = ForumIdentity | undefined;

// The authority's query surface is just the list of co-located client queries. Each `defineQuery`
// re-runs its validator on the UNTRUSTED wire args before building the AST, so a malformed client
// can't smuggle a garbage limit or ill-typed arg in, and the authority builds the exact same nested
// query the client named.
const apiQueries = registerQueries<User>([
  categoriesQuery,
  categoryBySlugQuery,
  threadsPageQuery,
  threadDetailQuery,
]);

/** Upsert the author's user-projection row from the VERIFIED token (the SQL twin of the client's
 *  `ensureUser`) — refreshing the cached display fields on every authenticated write (§3.4). */
function ensureUser(tx: SqlMutationTx, who: ForumIdentity): void {
  tx.exec(
    `INSERT INTO user (id, displayName, avatarUrl) VALUES (?, ?, ?)
     ON CONFLICT(id) DO UPDATE SET displayName = excluded.displayName, avatarUrl = excluded.avatarUrl`,
    [normalizeSubject(who.subject), who.displayName, who.avatarUrl ?? ""],
  );
}

const apiMutators = defineApiMutators<User, ApiMutators<User>>({
  createThread: (tx: SqlMutationTx, raw: unknown, ctx: MutationContext<User>) => {
    const a = createThreadArgs.parse(raw);
    const who = requireUser(ctx.user);
    const title = cleanTitle(a.title);
    const body = normalizeBody(a.body);
    ensureUser(tx, who);
    tx.exec(
      `INSERT INTO thread (id, categoryId, authorId, title, createdAt, lastPostAt, locked, pinned)
       SELECT ?, ?, ?, ?, ?, ?, 0, 0 WHERE EXISTS (SELECT 1 FROM category WHERE id = ?)`,
      [a.id, a.categoryId, who.subject, title, a.createdAt, a.createdAt, a.categoryId],
    );
    tx.exec(
      `INSERT INTO post (id, threadId, authorId, body, createdAt, editedAt, deleted)
       SELECT ?, ?, ?, ?, ?, 0, 0 WHERE EXISTS (SELECT 1 FROM thread WHERE id = ?)`,
      [a.firstPostId, a.id, who.subject, body, a.createdAt, a.id],
    );
  },
  addReply: (tx: SqlMutationTx, raw: unknown, ctx: MutationContext<User>) => {
    const a = addReplyArgs.parse(raw);
    const who = requireUser(ctx.user);
    const body = normalizeBody(a.body);
    if (!body) return;
    ensureUser(tx, who);
    // Insert only into a thread that exists AND isn't locked — a racing client can't reply past a lock.
    tx.exec(
      `INSERT INTO post (id, threadId, authorId, body, createdAt, editedAt, deleted)
       SELECT ?, ?, ?, ?, ?, 0, 0 WHERE EXISTS (SELECT 1 FROM thread WHERE id = ? AND locked = 0)`,
      [a.id, a.threadId, who.subject, body, a.createdAt, a.threadId],
    );
    // Bump activity only if the reply actually landed.
    tx.exec(
      `UPDATE thread SET lastPostAt = ? WHERE id = ? AND EXISTS (SELECT 1 FROM post WHERE id = ?)`,
      [a.createdAt, a.threadId, a.id],
    );
  },
  // Ownership enforced IN the SQL: a non-author's edit is accepted-but-no-op (the WHERE matches no
  // row), and the client's optimistic edit snaps back on confirmation.
  editPost: (tx: SqlMutationTx, raw: unknown, ctx: MutationContext<User>) => {
    const a = editPostArgs.parse(raw);
    const who = requireUser(ctx.user);
    tx.exec("UPDATE post SET body = ?, editedAt = ? WHERE id = ? AND authorId = ? AND deleted = 0", [
      normalizeBody(a.body),
      a.editedAt,
      a.id,
      who.subject,
    ]);
  },
  deletePost: (tx: SqlMutationTx, raw: unknown, ctx: MutationContext<User>) => {
    const { id } = postIdArgs.parse(raw);
    const who = requireUser(ctx.user);
    // Soft delete (tombstone) — author-gated.
    tx.exec("UPDATE post SET body = '', deleted = 1 WHERE id = ? AND authorId = ?", [id, who.subject]);
  },
  upvote: (tx: SqlMutationTx, raw: unknown, ctx: MutationContext<User>) => {
    const a = upvoteArgs.parse(raw);
    const who = requireUser(ctx.user);
    ensureUser(tx, who);
    // PK is the deterministic ${postId}:${subject}; OR IGNORE makes a double-vote a no-op. Only votes
    // on a real, non-deleted post count.
    tx.exec(
      `INSERT OR IGNORE INTO vote (id, postId, userId, createdAt)
       SELECT ?, ?, ?, ? WHERE EXISTS (SELECT 1 FROM post WHERE id = ? AND deleted = 0)`,
      [voteId(a.postId, who.subject), a.postId, who.subject, a.createdAt, a.postId],
    );
  },
  removeVote: (tx: SqlMutationTx, raw: unknown, ctx: MutationContext<User>) => {
    const a = removeVoteArgs.parse(raw);
    const who = requireUser(ctx.user);
    tx.exec("DELETE FROM vote WHERE id = ? AND userId = ?", [voteId(a.postId, who.subject), who.subject]);
  },
});

/** A bearer-auth'd daemon control-plane target (base URL + shared token). */
export interface ForumDaemonTarget {
  /** The control-plane base URL. */
  url: string;
  /** The shared bearer token (this tier ↔ that daemon ONLY — never reaches the browser). */
  token: string;
}

export interface ForumApiOptions {
  /** The READ + control-plane daemon base URL — bearer-auth'd. In the one topology (design 214) this
   *  is the read FOLLOWER (`rindled`). */
  daemonUrl: string;
  /** The shared bearer token for {@link daemonUrl} (daemon ↔ this tier ONLY — never reaches the browser). */
  daemonToken: string;
  /** The required `rindle-replicator` WRITE-MASTER ingress. Writes route here while reads and the
   *  control plane stay on {@link daemonUrl} (the follower). */
  writeDaemon: ForumDaemonTarget;
  /** Bearer for the write-master's PUBLIC `/v1/sql/*` surface, where authoritative mutations now
   *  execute. It shares {@link writeDaemon}'s listener, so it needs no separate URL — but it MUST be
   *  a different secret from `writeDaemon.token`: the replicator refuses an equal pair at startup. */
  databaseToken: string;
  /** Override the daemon HTTP transport (defaults to global `fetch`). */
  fetch?: Fetch;
}

/** Build the configured API server. Stateless: safe to construct per-request (Worker) or once per
 *  process (Node). Reads are PUBLIC (anyone may browse); writes require a verified identity. */
export function createForumApi(opts: ForumApiOptions): RindleApiServer<User> {
  // Reads + control plane always target `daemonUrl` (the read follower).
  const reads = new HttpRindleDaemonClient({
    baseUrl: opts.daemonUrl,
    headers: { authorization: `Bearer ${opts.daemonToken}` },
    fetch: opts.fetch,
  });
  const daemon: RindleDaemonClient = new SplitDaemonClient(
    new HttpRindleDaemonClient({
      baseUrl: opts.writeDaemon.url,
      headers: { authorization: `Bearer ${opts.writeDaemon.token}` },
      fetch: opts.fetch,
    }),
    reads,
  );
  return createRindleApiServer<User>({
    daemon,
    // Authoritative mutations run over the versioned public SQL transport; `daemon` above keeps
    // serving query leases, SSR reads, materializations and room control.
    database: { url: opts.writeDaemon.url, authToken: opts.databaseToken, fetch: opts.fetch },
    queries: apiQueries,
    mutators: apiMutators,
    authorizeQuery: () => true, // public reads
    authorizeMutation: ({ user }) => !!user && user.subject.length > 0, // must be signed in
  });
}

/** Resolve the read/write daemon wiring from an environment bag — shared by every host shell (Node
 *  `api.ts`/`server.ts` read `process.env`; the Worker reads `env`). The reads + control plane come
 *  off `RINDLE_DAEMON_URL` (the stable fleet edge; legacy `RINDLE_FOLLOWER_URL` can bypass it);
 *  `RINDLE_REPLICATOR_URL` names
 *  the write-master, so writes route there via the {@link SplitDaemonClient} (its token defaults to the
 *  follower's). One topology (design 214): both targets are required because the follower has no
 *  write plane. */
export function resolveForumDaemon(
  env: Record<string, string | undefined>,
  defaults: { daemonUrl: string; daemonToken: string },
): Pick<ForumApiOptions, "daemonUrl" | "daemonToken" | "writeDaemon" | "databaseToken"> {
  const daemonUrl = env.RINDLE_DAEMON_URL ?? env.RINDLE_FOLLOWER_URL ?? defaults.daemonUrl;
  const daemonToken = env.RINDLE_DAEMON_TOKEN ?? defaults.daemonToken;
  const writeUrl = env.RINDLE_REPLICATOR_URL;
  if (!writeUrl) {
    throw new Error("RINDLE_REPLICATOR_URL is required: writes must target the replicator write-master");
  }
  const databaseToken = env.RINDLE_DATABASE_TOKEN ?? env.SQL_TOKEN;
  if (!databaseToken) {
    throw new Error(
      "RINDLE_DATABASE_TOKEN is required: authoritative mutations execute over the write-master's " +
        "public SQL surface, and its credential must differ from RINDLE_REPLICATOR_TOKEN",
    );
  }
  const writeDaemon = { url: writeUrl, token: env.RINDLE_REPLICATOR_TOKEN ?? daemonToken };
  return { daemonUrl, daemonToken, writeDaemon, databaseToken };
}

/** Map an error thrown out of the API server (or body parsing) to an HTTP status + message — shared
 *  by both host shells. `RindleApiError` carries its own status (400/403/404); anything else is 500. */
export function httpErrorOf(err: unknown): { status: number; message: string } {
  const status = typeof err === "object" && err !== null ? (err as { status?: unknown }).status : undefined;
  return {
    status: typeof status === "number" ? status : 500,
    message: String(err instanceof Error ? err.message : err),
  };
}

/** The demo policy that exercises the REJECTION path end to end (toast in the UI). */
function cleanTitle(title: string): string {
  const out = normalizeTitle(title);
  if (out.length === 0) throw new Error("a title is required");
  if (/\bspam\b/i.test(out)) throw new Error('the word "spam" is not allowed in titles');
  return out;
}

function requireUser(user: User): ForumIdentity {
  if (!user || user.subject.length === 0) throw new Error("you must be signed in");
  return { ...user, subject: normalizeSubject(user.subject) };
}
