// Co-located with the component that renders it. Holds the user SELECTION (`UserBadgeFragment`) — the
// projection of an author reduced to what a name badge shows. Reused everywhere a person appears: a
// thread's author and a post's author.
//
// A `*.queries.ts` module is React-FREE: it imports only `@rindle/client` (the schema, `defineFragment`)
// and, where a fragment composes, its sibling `*.queries.ts`. That's what lets the authority pull these
// into the composed AST (server/app-api.ts → `registerQueries`) without the server graph reaching React.
// NEVER import a `.tsx` from here — the one rule that keeps the server graph clean.

import { defineFragment } from "@rindle/client";
import type { FragmentRef } from "@rindle/client";

import { user } from "../../shared/app-def.ts";

/** A person reduced to what a name badge shows. Spread as a thread's author and a post's author. */
export const UserBadgeFragment = defineFragment(user, (u) => u.select("id", "displayName", "avatarUrl"));
export type UserBadgeRef = FragmentRef<typeof UserBadgeFragment>;
