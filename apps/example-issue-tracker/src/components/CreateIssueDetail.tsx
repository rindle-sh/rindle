// The "new issue" pane: title, status/priority, owner, tags, description. Owner is a typeahead
// dropdown over the user cast; tags are removable pills plus an "Add tag…" combobox (held in local
// state until submit). On submit it mints the ids + timestamp (so the predicted + authoritative
// mutators stay deterministic) and fires `createIssue`, which writes the issue + its description
// comment + its tag rows.

import { useState } from "react";
import type { FormEvent } from "react";

import {
  ISSUE_PRIORITIES,
  ISSUE_STATUSES,
  normalizeOwner,
  normalizeTagName,
  normalizeTitle,
} from "../../shared/app-def.ts";
import type { IssuePriority, IssueStatus } from "../../shared/app-def.ts";
import { app } from "../rindle-client.ts";
import { PRIORITY_LABELS, STATUS_LABELS } from "../lib/issue.ts";
import { Combobox } from "./Combobox.tsx";
import type { ComboItem } from "./Combobox.tsx";
import { Segmented } from "./Segmented.tsx";
import { TagField } from "./TagField.tsx";

const MAX_TAGS = 8;

export function CreateIssueDetail({
  user,
  users,
  tagOptions,
  onCancel,
  onCreated,
}: {
  user: string;
  users: ComboItem[];
  tagOptions: string[];
  onCancel: () => void;
  onCreated: (id: string) => void;
}) {
  const [title, setTitle] = useState("");
  const [owner, setOwner] = useState(() => normalizeOwner(user));
  const [tags, setTags] = useState<string[]>([]);
  const [status, setStatus] = useState<IssueStatus>("todo");
  const [priority, setPriority] = useState<IssuePriority>("medium");
  const [descriptionText, setDescriptionText] = useState("");

  const addTag = (raw: string) => {
    const name = normalizeTagName(raw);
    if (!name || tags.length >= MAX_TAGS || tags.includes(name)) return;
    setTags([...tags, name]);
  };
  const removeTag = (name: string) => setTags(tags.filter((t) => t !== name));

  const submit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const nextTitle = normalizeTitle(title);
    if (!nextTitle) return;
    const id = crypto.randomUUID();
    const createdAt = Date.now();
    app.mutate.createIssue({
      id,
      title: nextTitle,
      status,
      priority,
      owner: normalizeOwner(owner),
      tags: tags.map((name) => ({ id: crypto.randomUUID(), name })),
      description: descriptionText,
      descriptionCommentId: crypto.randomUUID(),
      createdAt,
    });
    onCreated(id);
  };

  return (
    <form className="it-detail-pane is-open" aria-label="Create issue" onSubmit={submit}>
      <div className="it-detail-head">
        <div>
          <p className="it-eyebrow">New issue</p>
          <input
            autoFocus
            className="it-detail-title"
            placeholder="Issue title"
            value={title}
            onChange={(e) => setTitle(e.target.value)}
          />
        </div>
        <div className="it-detail-actions">
          <button type="button" className="it-quiet" onClick={onCancel}>
            Cancel
          </button>
          <button className="it-primary compact" type="submit">
            Create issue
          </button>
        </div>
      </div>

      <section className="it-detail-grid">
        <Segmented label="Status" options={ISSUE_STATUSES} labels={STATUS_LABELS} value={status} onSelect={setStatus} />
        <Segmented
          label="Priority"
          options={ISSUE_PRIORITIES}
          labels={PRIORITY_LABELS}
          value={priority}
          onSelect={setPriority}
        />
        <label className="it-field">
          <span>Owner</span>
          <Combobox
            value={owner}
            items={users}
            allowCustom
            formatCustom={normalizeOwner}
            onSelect={(value) => setOwner(normalizeOwner(value))}
          />
        </label>
        <TagField tags={tags} suggestions={tagOptions} onAdd={addTag} onRemove={removeTag} />
      </section>

      <section className="it-description">
        <div className="it-section-head">
          <p className="it-eyebrow">Description</p>
        </div>
        <textarea rows={6} value={descriptionText} onChange={(e) => setDescriptionText(e.target.value)} />
      </section>
    </form>
  );
}
