// The `CommentCardFragment` fragment — co-located with the component that renders it. React-free (see
// `UserBadge.queries.ts`). The SAME selection feeds two roots: a row in the detail thread AND a row
// in the activity feed (`FeedItemFragment` includes it).

import { defineFragment } from "@rindle/client";
import type { FragmentRef } from "@rindle/client";

import { comment, rels } from "../../shared/app-def.ts";
import { UserBadgeFragment } from "./UserBadge.queries.ts";
import type { UserBadgeData, UserBadgeRef } from "./UserBadge.queries.ts";

/** A comment with its author folded in — the read-only display shared by the detail thread and the
 *  activity feed. Order is applied at each spread site (oldest-first in the thread), not baked in. */
export const CommentCardFragment = defineFragment(comment, (c) =>
  c
    .select("id", "authorId", "body", "createdAt")
    .sub("author", rels.commentAuthor, UserBadgeFragment),
);
export type CommentCardRef = FragmentRef<typeof CommentCardFragment>;

/** Inline payload shape used by parent-owned comment relationships. */
export interface CommentWithAuthor {
  id: string;
  authorId: string;
  body: string;
  createdAt: number;
  author?: Array<UserBadgeData | UserBadgeRef>;
}
