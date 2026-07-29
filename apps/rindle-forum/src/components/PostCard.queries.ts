// The `PostCardFragment` — co-located with the component that renders one post in a thread. React-free
// (see `UserBadge.queries.ts`). Folds in the author (`UserBadgeFragment`) and a LIVE vote score (a
// scalar `countAs` over the `vote` table, maintained incrementally).

import { defineFragment } from "@rindle/client";
import type { FragmentRef } from "@rindle/client";

import { post, rels } from "../../shared/app-def.ts";
import { UserBadgeFragment } from "./UserBadge.queries.ts";

/** A post with its author folded in + a live upvote `score`. Order is applied at the spread site
 *  (oldest-first in the thread), not baked in. */
export const PostCardFragment = defineFragment(post, (p) =>
  p
    .select("id", "threadId", "authorId", "body", "createdAt", "editedAt", "deleted")
    .sub("author", rels.postAuthor, UserBadgeFragment)
    .countAs("score", rels.postVotes),
);
export type PostCardRef = FragmentRef<typeof PostCardFragment>;
