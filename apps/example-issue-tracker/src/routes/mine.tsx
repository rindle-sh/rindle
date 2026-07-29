// The "Mine" view (`/mine`): the current user's OWN issues. Unlike the list/board (which window the
// whole `issue` table), this is its own root query — `myIssues` — scoped to the AUTHENTICATED user
// via CONTEXT, not a filter arg. The chrome owns the session user; this view passes it as the query's
// ctx at the callsite, the API tier injects its own authoritative user, and the wire carries only
// `{ limit }`. So switching the active user (TopBar) re-subscribes to a freshly-scoped window, and a
// client can't reach another user's issues — there's no owner arg to tamper with.

import { createFileRoute } from "@tanstack/react-router";
import { useRoot } from "@rindle/react";

import { useIssueView } from "../app-context.ts";
import { IssueCardFragment, myIssuesQuery } from "../components/IssueListItem.queries.ts";
import { IssueList } from "../components/IssueList.tsx";

export const Route = createFileRoute("/mine")({ component: MineView });

/** A bounded window over the user's own issues — typically far smaller than the whole table, so a
 *  single window (no infinite scroll) is plenty. */
const MINE_LIMIT = 200;

function MineView() {
  const { user, selectedId, onSelect } = useIssueView();
  const [issues, { status }] = useRoot(myIssuesQuery, { limit: MINE_LIMIT }, { user }, IssueCardFragment);

  return (
    <IssueList
      issues={issues}
      selectedId={selectedId}
      loading={status !== "complete" && issues.length === 0}
      canLoadMore={false}
      showEnd={issues.length > 0}
      onSelect={onSelect}
      onLoadMore={() => {}}
      eyebrow="Mine"
      title={`${user}'s issues`}
      empty={{
        title: "You don't own any issues yet.",
        hint: "Create one with “New issue,” or switch the active user above to see theirs.",
      }}
    />
  );
}
