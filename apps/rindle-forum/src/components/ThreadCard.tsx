// A single thread row on a category page: title, author, last-activity, and an ACCURATE reply count
// (a scalar `countAs` over the whole thread). Reads its data through the `ThreadCard` fragment.

import { Link } from "@tanstack/react-router";
import { useFragment } from "@rindle/react";

import { ThreadCardFragment } from "./ThreadCard.queries.ts";
import type { ThreadCardRef } from "./ThreadCard.queries.ts";
import { UserBadge } from "./UserBadge.tsx";
import { formatDate, replyLabel } from "../lib/format.ts";

export function ThreadCard({ thread }: { thread: ThreadCardRef }) {
  const data = useFragment(ThreadCardFragment, thread);
  if (!data) return null;

  const author = data.author?.[0];
  return (
    <li className="rf-thread-row">
      <Link to="/t/$id" params={{ id: data.id }} className="rf-thread-link">
        <span className="rf-thread-main">
          <span className="rf-thread-title">
            {data.pinned ? <span className="rf-pin" title="Pinned">📌</span> : null}
            {data.locked ? <span className="rf-lock" title="Locked">🔒</span> : null}
            {data.title}
          </span>
          <span className="rf-thread-meta">
            {author ? <UserBadge user={author} className="rf-thread-author" /> : data.authorId}
            <span className="rf-dot" aria-hidden="true">·</span>
            <span title="Last activity">{formatDate(data.lastPostAt)}</span>
          </span>
        </span>
        <span className="rf-replies" title={`${data.postCount} posts`}>
          {replyLabel(data.postCount)}
        </span>
      </Link>
    </li>
  );
}
