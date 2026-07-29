import { memo, useRef } from "react";
import type { CSSProperties, KeyboardEvent, PointerEvent } from "react";
import { useFragment } from "@rindle/react";

import { app } from "../rindle-client.ts";
import { normalizeColor } from "../../shared/app-def.ts";
import { TodoItemFragment } from "./TodoItem.queries.ts";
import type { TodoItemRef } from "./TodoItem.queries.ts";

type TodoStyle = CSSProperties & {
  "--todo-hue": string;
  "--todo-x": string;
};

export const TodoItem = memo(function TodoItem({ todo }: { todo: TodoItemRef }) {
  const renders = useRenderCount();
  const lastColor = useRef<number | undefined>(undefined);
  const data = useFragment(TodoItemFragment, todo);
  if (!data) return null;

  const completed = data.completed === 1;
  const color = normalizeColor(data.color);
  const resonance = Math.round(color * 1000) / 10;
  const style: TodoStyle = {
    "--todo-hue": String(Math.round(color * 360)),
    "--todo-x": `${resonance}%`,
  };

  lastColor.current = color;

  const commitColor = (nextRaw: number, target: HTMLElement): void => {
    const next = normalizeColor(nextRaw);
    const nextPercent = Math.round(next * 1000) / 10;
    target.style.setProperty("--todo-x", `${nextPercent}%`);
    if (Math.abs(next - (lastColor.current ?? color)) < 0.002) return;
    lastColor.current = next;
    app.mutate.setColor.folded(
      { key: data.id, debounceMs: 80, maxWaitMs: 500 },
      { id: data.id, color: next, updatedAt: Date.now() },
    );
  };

  const handlePointerMove = (event: PointerEvent<HTMLLIElement>): void => {
    const rect = event.currentTarget.getBoundingClientRect();
    if (rect.width <= 0 || rect.height <= 0) return;

    const next = (event.clientX - rect.left) / rect.width;
    const y = normalizeColor((event.clientY - rect.top) / rect.height);
    event.currentTarget.style.setProperty("--todo-y", `${Math.round(y * 1000) / 10}%`);
    commitColor(next, event.currentTarget);
  };

  const handleKeyDown = (event: KeyboardEvent<HTMLLIElement>): void => {
    if (event.target !== event.currentTarget) return;

    let next: number | undefined;
    if (event.key === "ArrowLeft") next = color - 0.03;
    if (event.key === "ArrowRight") next = color + 0.03;
    if (event.key === "Home") next = 0;
    if (event.key === "End") next = 1;
    if (next === undefined) return;

    event.preventDefault();
    commitColor(next, event.currentTarget);
  };

  return (
    <li
      className={`tm-todo${completed ? " is-complete" : ""}`}
      data-testid={`todo-${data.id}`}
      style={style}
      tabIndex={0}
      onPointerDown={handlePointerMove}
      onPointerMove={handlePointerMove}
      onPointerLeave={() => app.flushFolds()}
      onPointerUp={() => app.flushFolds()}
      onBlur={() => app.flushFolds()}
      onKeyDown={handleKeyDown}
    >
      <label className="tm-check">
        <input
          type="checkbox"
          checked={completed}
          onChange={(event) =>
            app.mutate.toggleTodo({
              id: data.id,
              completed: event.currentTarget.checked,
              updatedAt: Date.now(),
            })
          }
        />
        <span>{data.title}</span>
      </label>

      <div className="tm-row-meta">
        <span className="tm-resonance" data-testid={`todo-resonance-${data.id}`}>
          resonance {resonance.toFixed(1)}
        </span>
        <span data-testid={`todo-render-count-${data.id}`} suppressHydrationWarning>
          renders {renders}
        </span>
        <button type="button" onClick={() => app.mutate.deleteTodo({ id: data.id })}>
          Delete
        </button>
      </div>
    </li>
  );
});

function useRenderCount(): number {
  const renders = useRef(0);
  renders.current += 1;
  return renders.current;
}
