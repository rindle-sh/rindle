// A single post in a thread: author + timestamp + body, a live upvote score, and — for the viewer's
// OWN posts — edit/delete. Reads its data through the `PostCard` fragment; fires `app.mutate.*` for
// votes/edits, which apply OPTIMISTICALLY in the local wasm engine and reconcile as the authority
// confirms (or snap back via onRejected).

import { useState } from "react";
import { useFragment } from "@rindle/react";

import { PostCardFragment } from "./PostCard.queries.ts";
import type { PostCardRef } from "./PostCard.queries.ts";
import { UserBadge } from "./UserBadge.tsx";
import { formatDateTime } from "../lib/format.ts";
import { useCurrentSubject } from "../lib/use-handle.ts";
import { app, currentHandle } from "../rindle-client.ts";

export function PostCard({ post, opening }: { post: PostCardRef; opening: boolean }) {
  const data = useFragment(PostCardFragment, post);
  const me = useCurrentSubject();

  // Phase-1 vote affordance: a local toggle. The server dedups (PK = postId:subject, OR IGNORE) so an
  // accidental double-upvote is a no-op; the score `countAs` updates live regardless. Deriving the
  // viewer's exact prior vote would add a per-user votes query — deferred.
  const [voted, setVoted] = useState(false);
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState("");

  if (!data) return null;

  const postData = data;
  const mine = postData.authorId === me;
  const author = postData.author?.[0];

  function toggleVote() {
    const handle = currentHandle();
    if (voted) {
      app.mutate.removeVote({ postId: postData.id, author: handle });
      setVoted(false);
    } else {
      app.mutate.upvote({ postId: postData.id, author: handle, createdAt: Date.now() });
      setVoted(true);
    }
  }

  function saveEdit() {
    const body = draft.trim();
    if (body) app.mutate.editPost({ id: postData.id, body, editedAt: Date.now() });
    setEditing(false);
  }

  if (postData.deleted) {
    return (
      <article className="rf-post rf-post-deleted">
        <p className="rf-post-body rf-muted">[deleted]</p>
      </article>
    );
  }

  return (
    <article className={`rf-post${opening ? " rf-post-opening" : ""}`}>
      <div className="rf-vote">
        <button type="button" className={`rf-vote-btn${voted ? " is-voted" : ""}`} onClick={toggleVote} aria-pressed={voted}>
          ▲
        </button>
        <span className="rf-score" title={`${postData.score} upvotes`}>{postData.score}</span>
      </div>
      <div className="rf-post-body-wrap">
        <div className="rf-post-head">
          {author ? <UserBadge user={author} className="rf-post-author" /> : <span>{postData.authorId}</span>}
          <span className="rf-muted">{formatDateTime(postData.createdAt)}{postData.editedAt ? " · edited" : ""}</span>
        </div>
        {editing ? (
          <div className="rf-editor">
            <textarea value={draft} onChange={(e) => setDraft(e.target.value)} rows={4} />
            <div className="rf-editor-actions">
              <button type="button" className="rf-btn" onClick={saveEdit}>Save</button>
              <button type="button" className="rf-btn rf-btn-ghost" onClick={() => { setEditing(false); setDraft(postData.body); }}>Cancel</button>
            </div>
          </div>
        ) : (
          <p className="rf-post-body">{postData.body}</p>
        )}
        {mine && !editing && (
          <div className="rf-post-actions">
            <button type="button" className="rf-link-btn" onClick={() => { setDraft(postData.body); setEditing(true); }}>Edit</button>
            <button type="button" className="rf-link-btn" onClick={() => app.mutate.deletePost({ id: postData.id })}>Delete</button>
          </div>
        )}
      </div>
    </article>
  );
}
