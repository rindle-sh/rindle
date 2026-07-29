// Create a room from the home page. `app.mutate.createRoom` applies optimistically (the room appears
// instantly) and reconciles on confirmation — try the name "spam" to see the authority reject it and
// the optimistic row snap back with a toast.

import { useState } from "react";
import type { FormEvent } from "react";
import { ulid } from "ulid";

import { app } from "../rindle-client.ts";

export function NewRoomForm() {
  const [name, setName] = useState("");

  function submit(e: FormEvent) {
    e.preventDefault();
    const text = name.trim();
    if (!text) return;
    // `ulid()` mints a k-sortable, time-ascending id (48-bit ms prefix + 80 random bits) at the
    // callsite, so it's stable across the mutator's optimistic replays and orders chronologically.
    app.mutate.createRoom({ id: ulid(), name: text, createdAt: Date.now() });
    setName("");
  }

  return (
    <form className="app-new-room" onSubmit={submit}>
      <input value={name} onChange={(e) => setName(e.target.value)} placeholder="New room name…" maxLength={80} />
      <button type="submit" className="app-btn" disabled={!name.trim()}>
        Create room
      </button>
    </form>
  );
}
