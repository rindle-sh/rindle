// A single comment — author + timestamp + body. Its data shape is declared by
// `CommentCardFragment`; both the issue detail and activity feed already own this inline payload.

import type { CommentWithAuthor } from "./CommentCard.queries.ts";
import type { UserBadgeData, UserBadgeRef } from "./UserBadge.queries.ts";
import { formatDateTime } from "../lib/format.ts";
import { UserBadge } from "./UserBadge.tsx";

export function CommentCard({ comment }: { comment: CommentWithAuthor }) {
  const author = comment.author?.[0];
  return (
    <article className="it-comment">
      <div className="it-comment-head">
        <b>{author ? <CommentAuthor author={author} /> : comment.authorId}</b>
        <span>{formatDateTime(comment.createdAt)}</span>
      </div>
      <p>{comment.body}</p>
    </article>
  );
}

function CommentAuthor({ author }: { author: UserBadgeData | UserBadgeRef }) {
  if ("name" in author) return <span className="it-comment-author">{author.name}</span>;
  return <UserBadge user={author} className="it-comment-author" />;
}
