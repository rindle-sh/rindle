// The reply box at the foot of a thread. Submitting fires `app.mutate.addReply`, which applies
// OPTIMISTICALLY in the local wasm engine (the new post shows instantly) and reconciles when the
// authority confirms — or snaps back via onRejected (e.g. the thread was locked).

import { useState } from "react";

import { app, currentHandle } from "../rindle-client.ts";

export function ReplyComposer({ threadId, locked }: { threadId: string; locked: boolean }) {
  const [body, setBody] = useState("");

  if (locked) return <p className="rf-locked-note">🔒 This thread is locked.</p>;

  function submit(e: React.FormEvent) {
    e.preventDefault();
    const text = body.trim();
    if (!text) return;
    const now = Date.now();
    app.mutate.addReply({
      id: `post-${now}-${Math.random().toString(36).slice(2, 7)}`,
      threadId,
      body: text,
      author: currentHandle(),
      createdAt: now,
    });
    setBody("");
  }

  return (
    <form className="rf-composer" onSubmit={submit}>
      <textarea
        value={body}
        onChange={(e) => setBody(e.target.value)}
        placeholder="Write a reply…"
        rows={3}
      />
      <div className="rf-composer-actions">
        <button type="submit" className="rf-btn" disabled={!body.trim()}>
          Reply
        </button>
      </div>
    </form>
  );
}
