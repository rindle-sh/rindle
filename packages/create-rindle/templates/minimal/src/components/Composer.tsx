// The message box at the foot of a room. Submitting fires `app.mutate.postMessage`, which applies
// OPTIMISTICALLY in the local wasm engine (the message shows instantly) and reconciles when the
// authority confirms — or snaps back via onRejected (e.g. a body containing "spam").

import { useState } from "react";
import type { FormEvent } from "react";
import { ulid } from "ulid";

import { app } from "../rindle-client.ts";

export function Composer({ roomId }: { roomId: string }) {
  const [body, setBody] = useState("");

  function submit(e: FormEvent) {
    e.preventDefault();
    const text = body.trim();
    if (!text) return;
    // `ulid()` mints a k-sortable, time-ascending id at the callsite (stable across optimistic
    // replays). The author is NOT an arg — it's the acting principal (ctx.user), injected per tier.
    app.mutate.postMessage({
      id: ulid(),
      roomId,
      body: text,
      createdAt: Date.now(),
    });
    setBody("");
  }

  return (
    <form className="app-composer" onSubmit={submit}>
      <textarea value={body} onChange={(e) => setBody(e.target.value)} placeholder="Write a message…" rows={3} />
      <div className="app-composer-actions">
        <button type="submit" className="app-btn" disabled={!body.trim()}>
          Send
        </button>
      </div>
    </form>
  );
}
