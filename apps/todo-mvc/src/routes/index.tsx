import { useRef } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { useRoot } from "@rindle/react";
import { fragmentKey, type DehydratedState } from "@rindle/client";

import { AddTodo } from "../components/AddTodo.tsx";
import { TodoItem } from "../components/TodoItem.tsx";
import { todoListQuery } from "../components/TodoList.queries.ts";

export const Route = createFileRoute("/")({
  loader: async (): Promise<{ rindle: DehydratedState }> => {
    if (!import.meta.env.SSR) return { rindle: {} };
    const { preloadRindle } = await import("../ssr.ts");
    return { rindle: await preloadRindle([todoListQuery()]) };
  },
  component: Home,
});

function Home() {
  const renderCount = useRenderCount();
  const [list, { status }] = useRoot(todoListQuery);

  if (!list) {
    return (
      <main className="tm-shell">
        <section className="tm-panel">
          <p className="tm-empty">{status === "complete" ? "TODO list not found." : "Loading todos..."}</p>
        </section>
      </main>
    );
  }

  const todos = list.todos ?? [];
  return (
    <main className="tm-shell">
      <header className="tm-header">
        <div>
          <p className="tm-kicker">Rindle fragment demo</p>
          <h1>{list.name}</h1>
        </div>
        <div className="tm-render-meter" data-testid="list-render-count">
          list renders <strong suppressHydrationWarning>{renderCount}</strong>
        </div>
      </header>

      <section className="tm-toolbar" aria-label="Todo controls">
        <AddTodo />
        <div className="tm-stats">
          <span>{todos.length} total</span>
          <span>1 query</span>
        </div>
      </section>

      <ul className="tm-list" aria-label="Todos">
        {todos.length === 0 ? (
          <li className="tm-empty">No todos yet.</li>
        ) : (
          todos.map((todo) => <TodoItem key={fragmentKey(todo)} todo={todo} />)
        )}
      </ul>
    </main>
  );
}

function useRenderCount(): number {
  const renders = useRef(0);
  renders.current += 1;
  return renders.current;
}
