// Co-located with the thread row on a category page. Holds the per-row SELECTION (`ThreadCardFragment`)
// AND the named root query that windows a category's threads (`threadsPageQuery`). React-free (see
// `UserBadge.queries.ts`).

import { defineFragment, defineQuery, eq, exists, fieldCondition } from "@rindle/client";
import type { FragmentRef } from "@rindle/client";
import { z } from "zod";

import { MAX_THREADS, q, rels, thread } from "../../shared/app-def.ts";
import { UserBadgeFragment } from "./UserBadge.queries.ts";

/** What a thread ROW renders: the core columns, the author badge folded in (`UserBadgeFragment`), and
 *  an ACCURATE post count over the whole thread (a scalar `countAs` — the post rows aren't folded into
 *  the card; the thread view fetches those on demand). Replies = postCount − 1 (the opening post). */
export const ThreadCardFragment = defineFragment(thread, (t) =>
  t
    .select("id", "categoryId", "authorId", "title", "createdAt", "lastPostAt", "locked", "pinned")
    .sub("author", rels.threadAuthor, UserBadgeFragment)
    .countAs("postCount", rels.threadPosts),
);
export type ThreadCardRef = FragmentRef<typeof ThreadCardFragment>;

/** A window limit: an integer in [1, MAX_THREADS], so a malformed client can't ask the authority to
 *  materialize an unbounded window. */
const limitSchema = z.number().int().min(1).max(MAX_THREADS);

/** `{ slug, limit }`, validated on BOTH tiers so the browser and the authority build a byte-identical
 *  AST. Keying on the category SLUG (not its id) lets the /c/:slug route + its SSR loader window the
 *  threads with no id-resolution waterfall — the category filter is a correlated `EXISTS` over the
 *  thread→category join. */
const threadsPageArgsSchema = z.object({ slug: z.string().max(80), limit: limitSchema });
export type ThreadsPageArgs = z.infer<typeof threadsPageArgsSchema>;

/** A growing window over a category's threads — pinned first, then most-recently-active. The per-row
 *  shape is `ThreadCardFragment`. Infinite scroll grows `limit` a page at a time — one named
 *  subscription whose limit the UI ratchets up. */
export const threadsPageQuery = defineQuery(
  "threadsPage",
  (raw) => threadsPageArgsSchema.parse(raw),
  ({ slug, limit }) =>
    q.thread
      .where(exists(rels.threadCategory, (c) => c.where(fieldCondition("slug", eq(slug)))))
      .orderBy("pinned", "desc")
      .orderBy("lastPostAt", "desc")
      .orderBy("id", "asc")
      .limit(limit)
      .include(ThreadCardFragment),
);
