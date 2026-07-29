import { defineRelationships, newQueryBuilder, rel } from "@rindle/client";
import type { Row } from "@rindle/client";
import type { ClientRegistry, MutationTx } from "@rindle/optimistic";
import { z } from "zod";

import { schema, todo, todo_list } from "./schema.gen.ts";

export { schema, todo };
export const todoList = todo_list;

export const DEFAULT_LIST_ID = "main";
export const q = newQueryBuilder(schema);

export type TodoList = Row<typeof todoList>;
export type Todo = Row<typeof todo>;

export const rels = defineRelationships({
  todoListTodos: rel(todoList, todo, { id: "listId" }),
});

export function normalizeTitle(raw: string): string {
  return raw.trim().replace(/\s+/g, " ").slice(0, 140);
}

export function normalizeColor(raw: number): number {
  if (!Number.isFinite(raw)) return 0;
  return Math.max(0, Math.min(1, Math.round(raw * 1000) / 1000));
}

export const createTodoArgs = z.object({
  id: z.string(),
  title: z.string(),
  createdAt: z.number(),
});
export type CreateTodoArgs = z.infer<typeof createTodoArgs>;

export const toggleTodoArgs = z.object({
  id: z.string(),
  completed: z.boolean(),
  updatedAt: z.number(),
});
export type ToggleTodoArgs = z.infer<typeof toggleTodoArgs>;

export const setColorArgs = z.object({
  id: z.string(),
  color: z.number(),
  updatedAt: z.number(),
});
export type SetColorArgs = z.infer<typeof setColorArgs>;

export const deleteTodoArgs = z.object({ id: z.string() });
export type DeleteTodoArgs = z.infer<typeof deleteTodoArgs>;

export const mutators = {
  createTodo: (tx: MutationTx, a: CreateTodoArgs) => {
    const title = normalizeTitle(a.title);
    if (!title) return;
    tx.insert("todo", {
      id: a.id,
      listId: DEFAULT_LIST_ID,
      title,
      completed: 0,
      temperature: 0,
      color: 0.58,
      createdAt: a.createdAt,
      updatedAt: a.createdAt,
    });
  },
  toggleTodo: (tx: MutationTx, a: ToggleTodoArgs) => {
    tx.update("todo", {
      id: a.id,
      completed: a.completed ? 1 : 0,
      updatedAt: a.updatedAt,
    });
  },
  setColor: (tx: MutationTx, a: SetColorArgs) => {
    tx.update("todo", {
      id: a.id,
      color: normalizeColor(a.color),
      updatedAt: a.updatedAt,
    });
  },
  deleteTodo: (tx: MutationTx, a: DeleteTodoArgs) => {
    tx.delete("todo", { id: a.id });
  },
} satisfies ClientRegistry;
