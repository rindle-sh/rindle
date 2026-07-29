// The shared CONTRACT root: the NORMALIZED forum schema, the relationships the views join back over,
// normalization helpers, the mutator arg schemas, and the client's PREDICTED mutators. Both tiers
// import it — the browser for everything, the API server for the schema + normalization + the
// AUTHORITATIVE mutator twins (server/app-api.ts) with the same names.
//
// This module is the leaf of the contract DAG: it depends on nothing app-internal. The named root
// queries AND the per-component SELECTIONS they compose are co-located with their components in
// `src/components/*.queries.ts` (Relay-style co-location) — each a singular `defineQuery`/
// `defineFragment`. Keeping the schema here, free of those imports, is what keeps that graph acyclic.
//
// The data is normalized across five tables — `user`, `category`, `thread`, `post`, `vote` — joined
// back together at query time with correlated subqueries (`sub`) + scalar `countAs`. The SQLite DDL
// (with the indices that make those `sub` joins fast in both directions) lives in the schema
// migration `migrations/0001_init.sql`, applied on demand through `/migrate`; the `@rindle/client`
// table schema is GENERATED from it into `shared/schema.gen.ts` (re-exported below), so it can't
// drift from the DDL.

import { defineRelationships, newQueryBuilder, rel } from "@rindle/client";
import type { Row } from "@rindle/client";
import type { ClientRegistry, MutationTx } from "@rindle/optimistic";
import { z } from "zod";

// The normalized schema is GENERATED from the SQL migrations (migrations/*.sql) into ./schema.gen.ts
// by `rindle schema gen` — `pnpm dev` runs `rindle up --gen`, re-emitting it on every migration
// change, so the DDL is the single source of truth and the TS schema can't drift from it. We import
// the tables + `schema` here and re-export them below, keeping this contract root the one import for
// app code.
import { category, post, schema, thread, user, vote } from "./schema.gen.ts";

// --------------------------------------------------------------------------- tables (generated)
//
// Re-export the generated tables + `schema` so app code keeps importing the schema from this one
// contract root. The five normalized tables — `user`, `category`, `thread`, `post`, `vote` — and
// their column kinds come straight from migrations/0001_init.sql (TEXT → string(), REAL → number();
// the `locked`/`pinned`/`deleted` 0/1 flags are plain `number()`). The `id` columns include the
// deterministic vote PK `${postId}:${userId}` minted identically on both tiers (see `voteId` below).
export { category, post, schema, thread, user, vote };

/** One schema-bound query builder, shared by every co-located `*.queries.ts`. Each `q.<table>`
 *  access mints a fresh builder, so sharing the single instance is safe. */
export const q = newQueryBuilder(schema);

// --------------------------------------------------------------- row types (schema-derived)

export type User = Row<typeof user>;
export type Category = Row<typeof category>;
export type Thread = Row<typeof thread>;
export type Post = Row<typeof post>;
export type Vote = Row<typeof vote>;

// --------------------------------------------------------------- relationships (joins, declared once)
//
// Each join between two tables, declared ONCE as a typed value: the correlation (`parent.col →
// child.col`) lives here, not restated at every `sub`/`countAs`. The co-located fragments import
// these to spread an edge.
export const rels = defineRelationships({
  threadCategory: rel(thread, category, { categoryId: "id" }),
  threadAuthor: rel(thread, user, { authorId: "id" }),
  categoryThreads: rel(category, thread, { id: "categoryId" }),
  threadPosts: rel(thread, post, { id: "threadId" }),
  postAuthor: rel(post, user, { authorId: "id" }),
  postVotes: rel(post, vote, { id: "postId" }),
});

// --------------------------------------------------------------- query knobs

/** How many threads a category page grows by per "load more". */
export const PAGE_SIZE = 30;

/** The growing window is bounded — a client can't ask the authority to materialize an unbounded
 *  result. Large enough to scroll a seeded category. */
export const MAX_THREADS = 5000;

// --------------------------------------------------------------------------- normalization

export function normalizeTitle(title: string): string {
  return title.trim().replace(/\s+/g, " ").slice(0, 180);
}

export function normalizeBody(body: string): string {
  return body.trim().slice(0, 8000);
}

/** A login handle / OIDC subject reduced to a stable slug — the dev provider's subjects are handles,
 *  so the same cleaning makes the client's predicted author id match the server's. */
export function normalizeSubject(raw: string): string {
  return raw.trim().replace(/\s+/g, "-").toLowerCase().slice(0, 40) || "anon";
}

/** A human display name from a bare handle, for the client's predicted user projection (the server
 *  uses the real `displayName` off the verified token). */
export function handleToDisplayName(handle: string): string {
  const h = normalizeSubject(handle);
  return h.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

/** The deterministic vote PK — minted identically on both tiers so the optimistic insert dedups. */
export function voteId(postId: string, userId: string): string {
  return `${postId}:${normalizeSubject(userId)}`;
}

// --------------------------------------------------------------------------- mutator args
//
// One zod schema per mutator. The SERVER parses the UNTRUSTED wire args through it (server/app-api.ts);
// BOTH tiers derive the arg TYPE from it (`z.infer`). The client trusts its typed callsites and skips
// the parse on the optimistic hot path. `author` rides the args for the CLIENT's prediction only —
// the server ALWAYS overrides it with the authenticated identity (never the wire), so it is unspoofable
// for the authoritative write.

export const createThreadArgs = z.object({
  id: z.string(),
  categoryId: z.string(),
  title: z.string(),
  firstPostId: z.string(),
  body: z.string(),
  author: z.string(),
  createdAt: z.number(),
});
export type CreateThreadArgs = z.infer<typeof createThreadArgs>;

export const addReplyArgs = z.object({
  id: z.string(),
  threadId: z.string(),
  body: z.string(),
  author: z.string(),
  createdAt: z.number(),
});
export type AddReplyArgs = z.infer<typeof addReplyArgs>;

export const editPostArgs = z.object({ id: z.string(), body: z.string(), editedAt: z.number() });
export type EditPostArgs = z.infer<typeof editPostArgs>;

export const postIdArgs = z.object({ id: z.string() });
export type PostIdArgs = z.infer<typeof postIdArgs>;

export const upvoteArgs = z.object({ postId: z.string(), author: z.string(), createdAt: z.number() });
export type UpvoteArgs = z.infer<typeof upvoteArgs>;

export const removeVoteArgs = z.object({ postId: z.string(), author: z.string() });
export type RemoveVoteArgs = z.infer<typeof removeVoteArgs>;

// --------------------------------------------------------------------------- mutators (PREDICTED)
//
// Deterministic + replayable: every value that would otherwise come from the clock or a random source
// is passed in args (ids, timestamps). Each writes across as many tables as the change touches. The
// SQL twins in server/app-api.ts enforce the authoritative policy (identity, ownership, lock).

/** Upsert the author's user-projection row (the client only knows its handle; the server refreshes
 *  the projection from the verified token). */
function ensureUser(tx: MutationTx, id: string): void {
  const sub = normalizeSubject(id);
  if (!tx.row("user", { id: sub })) {
    tx.insert("user", { id: sub, displayName: handleToDisplayName(sub), avatarUrl: "" });
  }
}

export const mutators = {
  createThread: (tx: MutationTx, a: CreateThreadArgs) => {
    const author = normalizeSubject(a.author);
    ensureUser(tx, author);
    tx.insert("thread", {
      id: a.id,
      categoryId: a.categoryId,
      authorId: author,
      title: normalizeTitle(a.title),
      createdAt: a.createdAt,
      lastPostAt: a.createdAt,
      locked: 0,
      pinned: 0,
    });
    tx.insert("post", {
      id: a.firstPostId,
      threadId: a.id,
      authorId: author,
      body: normalizeBody(a.body),
      createdAt: a.createdAt,
      editedAt: 0,
      deleted: 0,
    });
  },
  addReply: (tx: MutationTx, a: AddReplyArgs) => {
    const body = normalizeBody(a.body);
    if (!body) return;
    const author = normalizeSubject(a.author);
    ensureUser(tx, author);
    tx.insert("post", {
      id: a.id,
      threadId: a.threadId,
      authorId: author,
      body,
      createdAt: a.createdAt,
      editedAt: 0,
      deleted: 0,
    });
    tx.update("thread", { id: a.threadId, lastPostAt: a.createdAt });
  },
  editPost: (tx: MutationTx, a: EditPostArgs) =>
    tx.update("post", { id: a.id, body: normalizeBody(a.body), editedAt: a.editedAt }),
  // Soft delete — keep the row (tombstoned) so thread numbering + counts stay stable. Ownership is
  // enforced authoritatively in SQL; the client predicts the tombstone and snaps back if the
  // authority kept the body.
  deletePost: (tx: MutationTx, a: PostIdArgs) => tx.update("post", { id: a.id, body: "", deleted: 1 }),
  upvote: (tx: MutationTx, a: UpvoteArgs) => {
    const userId = normalizeSubject(a.author);
    ensureUser(tx, userId);
    const id = voteId(a.postId, userId);
    if (!tx.row("vote", { id })) {
      tx.insert("vote", { id, postId: a.postId, userId, createdAt: a.createdAt });
    }
  },
  removeVote: (tx: MutationTx, a: RemoveVoteArgs) =>
    tx.delete("vote", { id: voteId(a.postId, a.author) }),
} satisfies ClientRegistry;
