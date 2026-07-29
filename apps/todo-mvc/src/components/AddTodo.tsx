import { useState } from "react";
import type { FormEvent } from "react";

import { app } from "../rindle-client.ts";

export function AddTodo() {
  const [title, setTitle] = useState("");

  function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const text = title.trim();
    if (!text) return;
    const createdAt = Date.now();
    app.mutate.createTodo({
      id: `todo-${createdAt}-${Math.random().toString(36).slice(2, 8)}`,
      title: text,
      createdAt,
    });
    setTitle("");
  }

  return (
    <form className="tm-add" onSubmit={onSubmit}>
      <input
        value={title}
        onChange={(event) => setTitle(event.currentTarget.value)}
        placeholder="Add a todo"
        aria-label="Add a todo"
      />
      <button type="submit">Add</button>
    </form>
  );
}
