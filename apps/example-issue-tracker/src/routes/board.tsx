// The board view (`/board`): one live window per status column, each the same `issuesPage` query as
// the list with a `status:` facet ANDed onto the active filter. It reads the shared filter +
// selection off the chrome's context.

import { createFileRoute } from "@tanstack/react-router";
import type { DehydratedState } from "@rindle/client";

import { ISSUE_STATUSES } from "../../shared/app-def.ts";
import { useIssueView } from "../app-context.ts";
import { COLUMN_LIMIT, KanbanBoard } from "../components/KanbanBoard.tsx";
import { issuesPageQuery } from "../components/IssueListItem.queries.ts";

export const Route = createFileRoute("/board")({
  loader: async (): Promise<{ rindle: DehydratedState }> => {
    if (!import.meta.env.SSR) return { rindle: {} };
    const queries = ISSUE_STATUSES.map((status) =>
      issuesPageQuery({ limit: COLUMN_LIMIT, filter: [{ axis: "status", value: status }] }),
    );
    const { preloadRindle } = await import("../ssr.ts");
    return { rindle: await preloadRindle(queries) };
  },
  component: BoardView,
});

function BoardView() {
  const { filter, selectedId, onSelect } = useIssueView();
  return <KanbanBoard filter={filter} selectedId={selectedId} onSelect={onSelect} />;
}
