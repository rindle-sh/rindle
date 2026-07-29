// The detail pane: edit the issue's fields, its description (the earliest comment), and its tags,
// and add discussion comments. Each edit fires a focused mutation; owner is a typeahead dropdown
// over the user cast, and tags are removable pills plus an "Add tag…" combobox — each pill toggle
// emits one add/remove mutation (the normalized counterpart of a whole-set replace). Re-mounted per
// issue (`key`) so local edit state resets.
//
// Unlike the list (which folds only the first few comments per row), the pane fetches the issue's
// WHOLE thread on demand via its own named query (`issueDetailQuery`) — so opening an issue is
// what pulls the rest of the discussion the window deliberately omitted.

import { useEffect, useState } from "react";
import type { FormEvent, KeyboardEvent } from "react";
import { useRoot } from "@rindle/react";
import type { FragmentData } from "@rindle/react";

import {
  ISSUE_PRIORITIES,
  ISSUE_STATUSES,
  normalizeOwner,
  normalizeTagName,
  normalizeTitle,
} from "../../shared/app-def.ts";
import { IssueDetailCardFragment, issueDetailQuery } from "./IssueDetail.queries.ts";
import { app } from "../rindle-client.ts";
import {
  PRIORITY_LABELS,
  STATUS_LABELS,
  descriptionComment,
  discussionComments,
  issueTags,
  tagNames,
} from "../lib/issue.ts";
import { CommentCard } from "./CommentCard.tsx";
import { Combobox } from "./Combobox.tsx";
import type { ComboItem } from "./Combobox.tsx";
import { Segmented } from "./Segmented.tsx";
import { TagField } from "./TagField.tsx";

const MAX_TAGS = 8;

export function IssueDetail({
  issueId,
  user,
  users,
  tagOptions,
  onClose,
}: {
  issueId: string;
  user: string;
  users: ComboItem[];
  tagOptions: string[];
  onClose: () => void;
}) {
  const [issue, { status }] = useRoot(issueDetailQuery, issueId);

  // Once the query is server-authoritative and still has no row, the issue is gone (deleted by
  // another client while the pane was open) — close it.
  useEffect(() => {
    if (status === "complete" && issue === null) onClose();
  }, [status, issue, onClose]);

  if (!issue) {
    return (
      <aside className="it-detail-pane is-open" aria-label="Issue detail">
        <p className="it-empty">{status === "complete" ? "Issue not found." : "Loading issue…"}</p>
      </aside>
    );
  }

  return (
    <IssueDetailBody
      key={issue.id}
      issue={issue}
      user={user}
      users={users}
      tagOptions={tagOptions}
      onClose={onClose}
    />
  );
}

function IssueDetailBody({
  issue,
  user,
  users,
  tagOptions,
  onClose,
}: {
  issue: FragmentData<typeof IssueDetailCardFragment>;
  user: string;
  users: ComboItem[];
  tagOptions: string[];
  onClose: () => void;
}) {
  const normalizedUser = normalizeOwner(user);
  const description = descriptionComment(issue);
  const discussion = discussionComments(issue);
  const [title, setTitle] = useState(issue.title);
  const [descriptionText, setDescriptionText] = useState(description?.body ?? "");
  const [newComment, setNewComment] = useState("");

  useEffect(() => {
    setTitle(issue.title);
    setDescriptionText(descriptionComment(issue)?.body ?? "");
  }, [issue]);

  const commitTitle = () => {
    const next = normalizeTitle(title);
    if (!next) {
      setTitle(issue.title);
      return;
    }
    if (next !== issue.title) app.mutate.editTitle({ id: issue.id, title: next, updatedAt: Date.now() });
    setTitle(next);
  };

  const selectOwner = (next: string) => {
    const owner = normalizeOwner(next);
    if (owner !== issue.ownerId) app.mutate.setOwner({ id: issue.id, owner, updatedAt: Date.now() });
  };

  const addTag = (raw: string) => {
    const name = normalizeTagName(raw);
    const current = issueTags(issue);
    if (!name || current.length >= MAX_TAGS || current.some((row) => row.name === name)) return;
    app.mutate.addTag({ id: crypto.randomUUID(), issueId: issue.id, name, updatedAt: Date.now() });
  };

  const removeTag = (name: string) => {
    const row = issueTags(issue).find((r) => r.name === name);
    if (row) app.mutate.removeTag({ id: row.id, issueId: issue.id, updatedAt: Date.now() });
  };

  const commitDescription = () => {
    if (!description) return; // every issue is created with a description comment
    const next = descriptionText.trim();
    if (next !== description.body) {
      app.mutate.editComment({ id: description.id, issueId: issue.id, body: descriptionText, updatedAt: Date.now() });
    }
    setDescriptionText(next);
  };

  const submitComment = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const body = newComment.trim();
    if (!body) return;
    app.mutate.addComment({
      id: crypto.randomUUID(),
      issueId: issue.id,
      body,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });
    setNewComment("");
  };

  const canDelete = normalizedUser === issue.ownerId;

  return (
    <aside className="it-detail-pane is-open" aria-label="Issue detail">
      <div className="it-detail-head">
        <div>
          <p className="it-eyebrow">{issue.id}</p>
          <input
            className="it-detail-title"
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            onBlur={commitTitle}
            onKeyDown={(e: KeyboardEvent<HTMLInputElement>) => {
              if (e.key === "Enter") e.currentTarget.blur();
            }}
          />
        </div>
        <div className="it-detail-actions">
          <button type="button" className="it-quiet" onClick={onClose}>
            Close
          </button>
          <button
            type="button"
            className="it-danger"
            disabled={!canDelete}
            onClick={() => {
              app.mutate.deleteIssue({ id: issue.id });
              onClose();
            }}
          >
            Delete
          </button>
        </div>
      </div>

      <section className="it-detail-grid">
        <Segmented
          label="Status"
          options={ISSUE_STATUSES}
          labels={STATUS_LABELS}
          value={issue.status}
          onSelect={(status) => app.mutate.setStatus({ id: issue.id, status, updatedAt: Date.now() })}
        />
        <Segmented
          label="Priority"
          options={ISSUE_PRIORITIES}
          labels={PRIORITY_LABELS}
          value={issue.priority}
          onSelect={(priority) => app.mutate.setPriority({ id: issue.id, priority, updatedAt: Date.now() })}
        />
        <label className="it-field">
          <span>Owner</span>
          <Combobox value={issue.ownerId} items={users} allowCustom formatCustom={normalizeOwner} onSelect={selectOwner} />
        </label>
        <TagField tags={tagNames(issue)} suggestions={tagOptions} onAdd={addTag} onRemove={removeTag} />
      </section>

      <section className="it-description">
        <div className="it-section-head">
          <p className="it-eyebrow">Description</p>
          <button type="button" className="it-quiet" onClick={commitDescription}>
            Save
          </button>
        </div>
        <textarea
          rows={6}
          value={descriptionText}
          onChange={(e) => setDescriptionText(e.target.value)}
          onBlur={commitDescription}
        />
      </section>

      <section className="it-comments">
        <div className="it-section-head">
          <p className="it-eyebrow">Comments</p>
          <span>{discussion.length}</span>
        </div>
        <div className="it-comment-list">
          {discussion.map((comment) => (
            <CommentCard key={comment.id} comment={comment} />
          ))}
          {discussion.length === 0 && <p className="it-empty">No comments yet.</p>}
        </div>
        <form className="it-comment-form" onSubmit={submitComment}>
          <textarea rows={3} value={newComment} onChange={(e) => setNewComment(e.target.value)} />
          <button className="it-primary" type="submit">
            Comment
          </button>
        </form>
      </section>
    </aside>
  );
}
