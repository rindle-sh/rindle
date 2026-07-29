// Co-located with the component that renders it. Holds the user SELECTION (`UserBadgeFragment`) AND
// the named root query over the (small) user table (`usersQuery`).
//
// A `*.queries.ts` module is React-FREE: it imports only `@rindle/client` (the schema, `defineFragment`,
// `defineQuery`) and, where a fragment composes, its sibling `*.queries.ts`. That's what lets the
// authority pull these queries into the composed AST (`server/app-api.ts` → `registerQueries`) without
// the server graph ever reaching React or `@rindle/react`. The rendering component (`UserBadge.tsx`)
// `useFragment`s the SAME exported value. NEVER import a `.tsx` from here — that is the one rule that
// keeps the server graph clean.

import { defineFragment, defineQuery } from "@rindle/client";
import type { FragmentData, FragmentRef } from "@rindle/client";

import { q, user } from "../../shared/app-def.ts";

/** A person reduced to what a name badge shows. Reused as an issue's owner and a comment's author. */
export const UserBadgeFragment = defineFragment(user, (u) => u.select("id", "name"));
export type UserBadgeRef = FragmentRef<typeof UserBadgeFragment>;
export type UserBadgeData = FragmentData<typeof UserBadgeFragment>;

/** The full cast of users, alphabetized. The `user` table is tiny (unlike `issue`), so — unlike the
 *  paginated issue windows — it's fine to query whole. Powers the user/owner pickers. No args, so no
 *  validator. */
export const usersQuery = defineQuery("users", () => q.user.orderBy("name", "asc"));
