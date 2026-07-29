// The `TagChipFragment` fragment — co-located with the component that renders it. React-free (see the note
// in `UserBadge.queries.ts`): both `TagChip.tsx` and the authority import this same value.
//
// Each fragment `select`s exactly the columns it displays — column-level PROJECTION that flows all
// the way through the optimistic client (PROJECTION-SUPPORT-DESIGN §4/§5). The TS row type is MASKED
// to that selection too: `TagChipRef` is `{ id, name }`, so the projected-away `issueId` (the join
// key back to the issue) isn't reachable — it's preserved internally for the join and re-supplied
// from the parent issue at mutate time.

import { defineFragment, defineQuery } from "@rindle/client";
import type { FragmentData, FragmentRef } from "@rindle/client";

import { q, tag } from "../../shared/app-def.ts";

/** One tag, as a chip. (`issueId` — the join key back to the issue — is preserved automatically.) */
export const TagChipFragment = defineFragment(tag, (t) => t.select("id", "name"));
export type TagChipRef = FragmentRef<typeof TagChipFragment>;
export type TagChipData = FragmentData<typeof TagChipFragment>;

/** Distinct tag names for the global search/create/detail suggestions. */
export const tagOptionsQuery = defineQuery("tagOptions", () => q.tag.select("name").orderBy("name", "asc"));
