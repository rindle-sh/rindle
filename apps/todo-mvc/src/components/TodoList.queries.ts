import { defineFragment, defineQuery } from "@rindle/client";
import type { FragmentRef } from "@rindle/client";

import { DEFAULT_LIST_ID, q, rels, todoList } from "../../shared/app-def.ts";
import { TodoItemFragment } from "./TodoItem.queries.ts";

export const TodoListFragment = defineFragment(todoList, (list) =>
  list
    .select("id", "name")
    .sub("todos", rels.todoListTodos, TodoItemFragment, (todo) =>
      todo.orderBy("createdAt", "asc").orderBy("id", "asc"),
    ),
);
export type TodoListRef = FragmentRef<typeof TodoListFragment>;

export const todoListQuery = defineQuery("todoList", () =>
  q.todo_list.where.id(DEFAULT_LIST_ID).include(TodoListFragment).one(),
);
