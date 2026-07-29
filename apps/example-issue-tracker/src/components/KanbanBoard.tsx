// The board view: one column per status, side by side. Each column is its OWN live window — the
// SAME `issuesPage` named query the list uses, with a `status:<col>` facet ANDed onto the active
// search. So every card is the SAME `IssueCard` fragment as a list row (write the card once, render
// it in both views), and the five columns are five independent subscriptions the daemon maintains
// concurrently — a small demonstration of the engine fanning one dataset into many live views.

import { useMemo } from "react";
import { fragmentKey, useRoot } from "@rindle/react";

import { ISSUE_STATUSES } from "../../shared/app-def.ts";
import type { IssueFilter, IssueStatus } from "../../shared/app-def.ts";
import { IssueCardFragment, issuesPageQuery } from "./IssueListItem.queries.ts";
import { STATUS_LABELS } from "../lib/issue.ts";
import { IssueListItem } from "./IssueListItem.tsx";

/** A bounded window per column (like the list's page) — the board shows the most recent N per status. */
export const COLUMN_LIMIT = 25;

export function KanbanBoard({
  filter,
  selectedId,
  onSelect,
}: {
  filter: IssueFilter;
  selectedId: string | null;
  onSelect: (id: string) => void;
}) {
  return (
    <section className="it-kanban" aria-label="Issue board">
      {ISSUE_STATUSES.map((status) => (
        <KanbanColumn key={status} status={status} filter={filter} selectedId={selectedId} onSelect={onSelect} />
      ))}
    </section>
  );
}

function KanbanColumn({
  status,
  filter,
  selectedId,
  onSelect,
}: {
  status: IssueStatus;
  filter: IssueFilter;
  selectedId: string | null;
  onSelect: (id: string) => void;
}) {
  const columnFilter = useMemo<IssueFilter>(() => [...filter, { axis: "status", value: status }], [filter, status]);
  const [issues] = useRoot(issuesPageQuery, { limit: COLUMN_LIMIT, filter: columnFilter }, IssueCardFragment);
  const overflow = issues.length >= COLUMN_LIMIT;

  return (
    <div className={`it-kanban-col status-${status}`}>
      <header className="it-kanban-head">
        <span className="it-status-dot" aria-hidden="true" />
        <h2>{STATUS_LABELS[status]}</h2>
        <span className="it-kanban-count">{overflow ? `${COLUMN_LIMIT}+` : issues.length}</span>
      </header>
      <ul className="it-kanban-list">
        {issues.map((issue) => (
          <IssueListItem
            key={fragmentKey(issue)}
            issue={issue}
            selectedId={selectedId}
            onSelect={onSelect}
          />
        ))}
      </ul>
      {issues.length === 0 && <p className="it-kanban-empty">Empty</p>}
    </div>
  );
}
