// Co-located with the thread-detail view. Holds the detail SELECTION (`ThreadDetailFragment`) AND the
// named root query that fetches it on demand (`threadDetailQuery`). React-free (see
// `UserBadge.queries.ts`).

import { defineFragment, defineQuery } from "@rindle/client";
import type { FragmentRef } from "@rindle/client";
import { z } from "zod";

import { q, rels, thread } from "../../shared/app-def.ts";
import { PostCardFragment } from "./PostCard.queries.ts";
import { UserBadgeFragment } from "./UserBadge.queries.ts";

/** What the THREAD VIEW renders: the thread chrome (title, author, flags), the parent category (for the
 *  breadcrumb), plus the WHOLE post thread `sub`'d in oldest-first (the earliest is the opening body),
 *  each post via the shared `PostCardFragment`. To cap the thread you'd add `.limit(n)` in the posts
 *  sub-build. */
export const ThreadDetailFragment = defineFragment(thread, (t) =>
  t
    .select("id", "categoryId", "authorId", "title", "createdAt", "lastPostAt", "locked", "pinned")
    .sub("author", rels.threadAuthor, UserBadgeFragment)
    .sub("category", rels.threadCategory, (c) => c.select("id", "slug", "name"))
    .sub("posts", rels.threadPosts, PostCardFragment, (p) =>
      p.orderBy("createdAt", "asc").orderBy("id", "asc")
    ),
);
export type ThreadDetailRef = FragmentRef<typeof ThreadDetailFragment>;

/** The thread view's only arg: a thread id. */
const threadIdArgs = z.string().max(80);

/** A single thread by id with its WHOLE post thread (`ThreadDetailFragment`). Powers the thread page,
 *  which fetches the discussion the category-page card (a count only) deliberately omits. */
export const threadDetailQuery = defineQuery("threadDetail", (raw) => threadIdArgs.parse(raw), (id) =>
  q.thread.where.id(id).include(ThreadDetailFragment).one(),
);
