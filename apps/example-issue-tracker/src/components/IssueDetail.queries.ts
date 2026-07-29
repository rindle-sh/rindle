// Co-located with the detail pane that renders it. React-free (see `UserBadge.queries.ts`). Holds the
// detail SELECTION (`IssueDetailCardFragment`) AND the named root query that fetches it on demand
// (`issueDetailQuery`) — see `IssueListItem.queries.ts` for the `defineQuery` contract.

import { defineFragment, defineQuery } from "@rindle/client";
import type { FragmentRef } from "@rindle/client";
import { z } from "zod";

import { issue, q, rels } from "../../shared/app-def.ts";
import { CommentCardFragment } from "./CommentCard.queries.ts";
import { IssueChromeFragment } from "./issue-chrome.queries.ts";

/** What the DETAIL PANE renders: the issue chrome plus the WHOLE comment thread `sub`'d in oldest-first
 *  (the earliest doubles as the description), each comment via the shared `CommentCardFragment`. It
 *  selects FEWER issue columns than `IssueCardFragment` (no `updatedAt`) — a genuinely narrower projected
 *  sync. To cap the thread you'd add `.limit(n)` in the comments sub-build below. */
export const IssueDetailCardFragment = defineFragment(issue, (i) =>
  i
    .include(IssueChromeFragment)
    .sub("comments", rels.issueComments, (c) => c.include(CommentCardFragment).orderBy("createdAt", "asc")),
);
export type IssueDetailRef = FragmentRef<typeof IssueDetailCardFragment>;

/** The detail pane's only arg: an issue id. */
const issueDetailArgs = z.string();

/** A single issue by id with the WHOLE comment thread (`IssueDetailCardFragment`). Powers the detail
 *  pane, which fetches the discussion the list/board card (a count only) deliberately omits. */
export const issueDetailQuery = defineQuery("issueDetail", (raw) => issueDetailArgs.parse(raw), (id) =>
  q.issue.where.id(id).include(IssueDetailCardFragment).one(),
);
