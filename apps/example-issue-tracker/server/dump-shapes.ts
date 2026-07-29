// Dump the app's query shapes for `rindle indices suggest` — feeder 1, exemplar enumeration.
//
// Each registered named query is invoked with exemplar args chosen to cover its BRANCHES (not
// its values — shapes dedupe with literals stripped): `issuesPage` runs once unfiltered and once
// per filter axis (each axis builds a structurally different WHERE — owner/tag/comment become
// EXISTS subqueries), `myIssues` runs under an exemplar principal (ctx-scoped), and the rest are
// single-shape. The output document goes to `.rindle/query-shapes.json`; derive + diff with:
//
//   pnpm shapes
//   pnpm rindle indices suggest .rindle/query-shapes.json
//
// Keep the exemplar table in sync when a query gains a new args-driven branch — a branch no
// exemplar takes is a shape this dump can't see (runtime shape recording is the safety net).

import { mkdirSync, writeFileSync } from "node:fs";

import { dumpQueryShapes, registerQueries } from "@rindle/api-server";

import { FEED_LIMIT, FILTER_AXES, PAGE_SIZE, schema } from "../shared/app-def.ts";
import type { User } from "./app-api.ts";
import { issuesPageQuery, myIssuesQuery } from "../src/components/IssueListItem.queries.ts";
import { issueDetailQuery } from "../src/components/IssueDetail.queries.ts";
import { recentCommentsQuery } from "../src/components/ActivityFeed.queries.ts";
import { usersQuery } from "../src/components/UserBadge.queries.ts";
import { tagOptionsQuery } from "../src/components/TagChip.queries.ts";

const doc = await dumpQueryShapes<User>({
  schema,
  queries: registerQueries<User>([
    issuesPageQuery,
    myIssuesQuery,
    issueDetailQuery,
    recentCommentsQuery,
    usersQuery,
    tagOptionsQuery,
  ]),
  exemplars: {
    issuesPage: [
      { args: { limit: PAGE_SIZE } },
      ...FILTER_AXES.map((axis) => ({
        args: { limit: PAGE_SIZE, filter: [{ axis, value: "x" }] },
      })),
    ],
    myIssues: [{ args: { limit: PAGE_SIZE }, user: "exemplar-user" }],
    issueDetail: [{ args: "exemplar-issue-id" }],
    recentComments: [{ args: { limit: FEED_LIMIT } }],
  },
});

mkdirSync(".rindle", { recursive: true });
const out = ".rindle/query-shapes.json";
writeFileSync(out, `${JSON.stringify(doc, null, 2)}\n`);
console.log(
  `wrote ${out}: ${doc.queries.length} shape(s) from ${new Set(doc.queries.map((q) => q.name.split("#")[0])).size} queries across ${doc.tables.length} tables`,
);
