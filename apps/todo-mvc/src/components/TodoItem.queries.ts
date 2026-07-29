import { defineFragment } from "@rindle/client";
import type { FragmentRef } from "@rindle/client";

import { todo } from "../../shared/app-def.ts";

export const TodoItemFragment = defineFragment(todo, (t) =>
  t.select("id", "title", "completed", "color", "updatedAt"),
);
export type TodoItemRef = FragmentRef<typeof TodoItemFragment>;
