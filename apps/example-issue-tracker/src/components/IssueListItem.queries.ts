// Co-located with the list row / board card that renders it. React-free (see `UserBadge.queries.ts`).
// Holds BOTH the per-row selection (`IssueCardFragment`) AND the named root query that windows it
// (`issuesPageQuery`) — the query lives next to the fragment and the component it feeds.
//
// `issuesPageQuery` is a singular `defineQuery`: callable on the client (it stamps its result with
// the wire identity, so a subscription always SYNCS — there is no unstamped builder to import by
// accident) and registered on the server (`server/app-api.ts` → `registerQueries`). Its `validate`
// step turns untrusted wire args into the canonical `{ limit, filter }` and runs on BOTH tiers, so
// the browser and the authority build a byte-identical AST. Keeping it React-free is what lets the
// authority import it without dragging React into the server graph.

import { defineFragment, defineQuery } from "@rindle/client";
import type { FragmentRef } from "@rindle/client";
import { z } from "zod";

import { applyIssueFilter, FILTER_AXES, MAX_ISSUES, q } from "../../shared/app-def.ts";
import { issue, rels } from "../../shared/app-def.ts";
import { IssueChromeFragment } from "./issue-chrome.queries.ts";

/** What a LIST ROW / BOARD CARD renders: the issue chrome, plus `updatedAt` (board sort) and an
 *  ACCURATE `count` of the whole comment thread (a scalar `countAs` — the comment ROWS aren't folded
 *  into the card; the detail pane fetches those on demand). The merge canonicalizes order, so
 *  `IssueChromeFragment`'s owner/tags and these additions assemble deterministically into one query. */
export const IssueCardFragment = defineFragment(issue, (i) =>
  i
    .include(IssueChromeFragment)
    .select("updatedAt")
    .countAs("commentCount", rels.issueComments),
);
export type IssueCardRef = FragmentRef<typeof IssueCardFragment>;

/** A search box value is short; cap it so a filter can't smuggle a huge literal into the AST. */
const MAX_FILTER_VALUE = 200;

/** A window limit: an integer in [1, MAX_ISSUES], so a malformed client can't ask the authority to
 *  materialize an unbounded window. Shared by `issuesPage` and `myIssues`. */
const limitSchema = z.number().int().min(1).max(MAX_ISSUES);

/** One faceted-search term before it becomes part of the resolved AST: a KNOWN axis and a short
 *  string value. */
const filterTermSchema = z.object({
  axis: z.enum(FILTER_AXES),
  value: z.string().max(MAX_FILTER_VALUE),
});

/** `{ limit, filter? }`, so a malformed client can't smuggle a garbage limit or an unbounded/ill-typed
 *  filter into the AST. Validated on both tiers (client guard + server authority). A null/absent
 *  filter is treated as no filter. */
const issuesPageArgsSchema = z.object({
  limit: limitSchema,
  filter: z.array(filterTermSchema).readonly().nullish(),
});

/** The canonical, validated args of the issues window. The schema's inferred type IS this type —
 *  written once, it flows to both `issuesPageQuery`'s call signature and its builder. */
export type IssuesPageArgs = z.infer<typeof issuesPageArgsSchema>;

/** A growing window over the (big) issue table, newest first, optionally narrowed by a server-side
 *  faceted filter. The window's per-row shape is the `IssueCardFragment` (owner, tags, comment
 *  count) — the same card the list rows AND the board columns render. Infinite scroll grows `limit`
 *  a page at a time — one named subscription whose limit the UI ratchets up. A board column is just
 *  this query with a `status:<col>` facet added (no separate query). */
export const issuesPageQuery = defineQuery("issuesPage", (raw) => issuesPageArgsSchema.parse(raw), ({ limit, filter }) =>
  applyIssueFilter(q.issue, filter ?? [])
    .orderBy("createdAt", "desc")
    .orderBy("id", "asc")
    .limit(limit)
    .include(IssueCardFragment),
);

/** The validated args of the "my issues" window — just a limit. The OWNER is NOT here: it comes from
 *  context, never the wire. */
const myIssuesArgsSchema = z.object({ limit: limitSchema });
export type MyIssuesArgs = z.infer<typeof myIssuesArgsSchema>;

/** The current user's OWN issues, newest first — the same `IssueCardFragment` card as the list, but
 *  scoped to `ctx.user` (the AUTHENTICATED principal) instead of a wire arg. The client passes its
 *  session user at the callsite; the server injects the user it authenticated (the `x-user` header),
 *  so both tiers build a byte-identical AST and the wire carries only `{ limit }`. A client therefore
 *  CANNOT ask for someone else's issues by tampering with args — there is no owner arg to tamper with.
 *  (Contrast the `owner:` search facet on `issuesPage`, which is a client-supplied, spoofable filter;
 *  this is server-authoritative.) `ctx.user` is `string | undefined` so the one `build` satisfies both
 *  the client (`{ user: string }`) and the server's `ApiContext`; in practice the authority rejects an
 *  unauthenticated lease before this runs. */
export const myIssuesQuery = defineQuery(
  "myIssues",
  (raw) => myIssuesArgsSchema.parse(raw),
  ({ limit }, ctx: { user: string | undefined }) =>
    q.issue.where
      .ownerId(ctx.user ?? "")
      .orderBy("createdAt", "desc")
      .orderBy("id", "asc")
      .limit(limit)
      .include(IssueCardFragment),
);
