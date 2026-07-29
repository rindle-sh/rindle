// A single issue row in the list (and, via the same fragment, a board card): title, owner / updated
// meta + an ACCURATE comment count, up to three tags, and the priority chip. Status + priority ride
// CSS classes (status-*/priority-*).
//
// The row reads its data through the `IssueCard` fragment — the same selection the board columns
// render. The comment count is a scalar `countAs` over the WHOLE thread (not the handful of folded
// rows), so unlike before it's exact; the rows themselves are fetched only when the detail opens.

import { useFragment } from "@rindle/react";

import { IssueCardFragment } from "./IssueListItem.queries.ts";
import type { IssueCardRef } from "./IssueListItem.queries.ts";
import { PRIORITY_LABELS } from "../lib/issue.ts";
import { formatDate } from "../lib/format.ts";
import { TagChip } from "./TagChip.tsx";
import { UserBadge } from "./UserBadge.tsx";

export function IssueListItem({
  issue,
  selectedId,
  onSelect,
}: {
  issue: IssueCardRef;
  selectedId: string | null;
  onSelect: (id: string) => void;
}) {
  const data = useFragment(IssueCardFragment, issue);
  if (!data) return null;

  const owner = data.owner?.[0];
  const tags = data.tags ?? [];
  const selected = data.id === selectedId;
  return (
    <li className="it-list-item">
      <button
        type="button"
        className={`it-issue-row status-${data.status} priority-${data.priority}${selected ? " is-selected" : ""}`}
        onClick={() => onSelect(data.id)}
      >
        <span className="it-status-dot" aria-hidden="true" />
        <span className="it-row-main">
          <span className="it-row-title">{data.title}</span>
          <span className="it-row-meta">
            {owner ? <UserBadge user={owner} className="it-row-owner" /> : data.ownerId}
            <span className="it-dot" aria-hidden="true">
              ·
            </span>
            {formatDate(data.updatedAt)}
            <span className="it-comment-count" title={`${data.commentCount} comments`}>
              {data.commentCount}
            </span>
          </span>
        </span>
        <span className="it-row-tags">
          {tags.slice(0, 3).map((tag) => (
            <TagChip key={tag.id} tag={tag} />
          ))}
        </span>
        <span className="it-priority">{PRIORITY_LABELS[data.priority]}</span>
      </button>
    </li>
  );
}
