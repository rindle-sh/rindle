// Co-located with the message row in the room view. Holds just the per-row SELECTION
// (`MessageCardFragment`); the room view composes it into the room detail query. React-free.

import { defineFragment } from "@rindle/client";
import type { FragmentRef } from "@rindle/client";

import { message } from "../../shared/app-def.ts";

/** What a single message row renders. */
export const MessageCardFragment = defineFragment(message, (m) =>
  m.select("id", "author", "body", "createdAt"),
);
export type MessageCardRef = FragmentRef<typeof MessageCardFragment>;
