// Co-located with the activity feed that renders it. React-free (see `UserBadge.queries.ts`). Holds
// the feed-row SELECTION (`FeedItemFragment`) AND the named root query that windows it
// (`recentCommentsQuery`) — see `IssueListItem.queries.ts` for the `defineQuery` contract. Rooted on
// `comment` (a different root than the issue list), yet it reuses the `CommentCardFragment` the
// detail thread renders.

import { defineFragment, defineQuery } from "@rindle/client";
import type { FragmentRef } from "@rindle/client";
import { z } from "zod";

import { comment, FEED_LIMIT, issue, q, rels } from "../../shared/app-def.ts";
import { CommentCardFragment } from "./CommentCard.queries.ts";

/** Just enough of an issue to label + link it from elsewhere (the activity feed's "… on <issue>"). */
export const IssueLinkFragment = defineFragment(issue, (i) => i.select("id", "title", "status"));
export type IssueLinkRef = FragmentRef<typeof IssueLinkFragment>;

/** An ACTIVITY-FEED row: the SAME `CommentCardFragment` the detail thread uses, plus the issue it
 *  belongs to (`IssueLinkFragment`, `sub`'d in here) so the feed can read "<author> commented on
 *  <issue>". One selection, two roots. */
export const FeedItemFragment = defineFragment(comment, (c) =>
  c.include(CommentCardFragment).sub("issue", rels.commentIssue, IssueLinkFragment),
);
export type FeedItemRef = FragmentRef<typeof FeedItemFragment>;

/** The feed window is bounded at `FEED_LIMIT` so a client can't ask the authority to materialize the
 *  whole comment table. Validated on both tiers. */
const feedArgsSchema = z.object({ limit: z.number().int().min(1).max(FEED_LIMIT) });

/** The newest comments across the WHOLE `comment` table (the activity feed), each a `FeedItemFragment`
 *  — the shared `CommentCardFragment` (comment + author) plus the issue it lives on. A different root
 *  table than `issuesPage`, reusing the very same `CommentCardFragment` the detail thread renders. */
export const recentCommentsQuery = defineQuery("recentComments", (raw) => feedArgsSchema.parse(raw), ({ limit }) =>
  q.comment.orderBy("createdAt", "desc").orderBy("id", "asc").limit(limit).include(FeedItemFragment),
);
