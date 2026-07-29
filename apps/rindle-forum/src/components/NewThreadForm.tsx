// The "new thread" form on a category page. Submitting fires `app.mutate.createThread` (one optimistic
// mutation writing the thread + its opening post across two tables) and navigates to the fresh thread.
// A title containing the word "spam" exercises the authority's REJECTION path (snap-back + toast).

import { useState } from "react";
import { useNavigate } from "@tanstack/react-router";

import { app, currentHandle } from "../rindle-client.ts";

export function NewThreadForm({ categoryId }: { categoryId: string }) {
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const [title, setTitle] = useState("");
  const [body, setBody] = useState("");

  if (!open) {
    return (
      <button type="button" className="rf-btn" onClick={() => setOpen(true)}>
        New thread
      </button>
    );
  }

  function submit(e: React.FormEvent) {
    e.preventDefault();
    const t = title.trim();
    const b = body.trim();
    if (!t || !b) return;
    const now = Date.now();
    const id = `th-${now}-${Math.random().toString(36).slice(2, 7)}`;
    app.mutate.createThread({
      id,
      categoryId,
      title: t,
      firstPostId: `${id}-p0`,
      body: b,
      author: currentHandle(),
      createdAt: now,
    });
    setTitle("");
    setBody("");
    setOpen(false);
    void navigate({ to: "/t/$id", params: { id } });
  }

  return (
    <form className="rf-new-thread" onSubmit={submit}>
      <input
        className="rf-input"
        value={title}
        onChange={(e) => setTitle(e.target.value)}
        placeholder="Thread title"
        autoFocus
      />
      <textarea value={body} onChange={(e) => setBody(e.target.value)} placeholder="What's on your mind?" rows={4} />
      <div className="rf-composer-actions">
        <button type="submit" className="rf-btn" disabled={!title.trim() || !body.trim()}>
          Create thread
        </button>
        <button type="button" className="rf-btn rf-btn-ghost" onClick={() => setOpen(false)}>
          Cancel
        </button>
      </div>
    </form>
  );
}
