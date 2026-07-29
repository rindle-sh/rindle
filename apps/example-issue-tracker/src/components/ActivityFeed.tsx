// The activity view: the newest comments across EVERY issue, newest first. Its root query
// (`recentComments`) is rooted on the `comment` table — a different root than the issue list — yet
// each row is the SAME `CommentCard` fragment the detail thread renders, now wrapped with the issue
// it belongs to (the `FeedItem` fragment folds `IssueLink` in). One selection, two very different
// roots: this is the reuse fragments buy you.

import { useMemo } from "react";
import { fragmentKey, useFragment, useRoot } from "@rindle/react";

import { FEED_LIMIT } from "../../shared/app-def.ts";
import { FeedItemFragment, IssueLinkFragment, recentCommentsQuery } from "./ActivityFeed.queries.ts";
import type { FeedItemRef, IssueLinkRef } from "./ActivityFeed.queries.ts";
import { CommentCard } from "./CommentCard.tsx";

export function ActivityFeed({ onSelect }: { onSelect: (issueId: string) => void }) {
  const feed = useMemo(() => recentCommentsQuery({ limit: FEED_LIMIT }), []);
  const [comments] = useRoot(feed, FeedItemFragment);

  return (
    <section className="it-list-pane" aria-label="Activity feed">
      <div className="it-pane-head">
        <div>
          <p className="it-eyebrow">Across all issues</p>
          <h1>Activity</h1>
        </div>
      </div>

      <ul className="it-feed">
        {comments.map((comment) => (
          <ActivityFeedItem key={fragmentKey(comment)} comment={comment} onSelect={onSelect} />
        ))}
      </ul>
      {comments.length === 0 && <p className="it-empty">No activity yet.</p>}
    </section>
  );
}

function ActivityFeedItem({ comment, onSelect }: { comment: FeedItemRef; onSelect: (issueId: string) => void }) {
  const data = useFragment(FeedItemFragment, comment);
  if (!data) return null;
  const issue = data.issue?.[0];

  return (
    <li className="it-feed-item">
      {issue && <ActivityIssueLink issue={issue} onSelect={onSelect} />}
      <CommentCard comment={data} />
    </li>
  );
}

function ActivityIssueLink({ issue, onSelect }: { issue: IssueLinkRef; onSelect: (issueId: string) => void }) {
  const data = useFragment(IssueLinkFragment, issue);
  if (!data) return null;

  return (
    <button
      type="button"
      className={`it-feed-issue status-${data.status}`}
      onClick={() => onSelect(data.id)}
      title={`Open ${data.title}`}
    >
      <span className="it-status-dot" aria-hidden="true" />
      <span className="it-feed-issue-title">{data.title}</span>
    </button>
  );
}
