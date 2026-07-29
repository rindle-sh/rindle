// The issue "chrome" — the selection shared by BOTH the list/board card (`IssueCardFragment`) and the
// detail pane (`IssueDetailCardFragment`). It has no component of its own; it's the common spread the
// two issue roots `include`, so the owner badge + tag chips are declared once here. React-free (see
// `UserBadge.queries.ts`). The owner/tags edges are `sub`'d inline — they're each spread from this one
// place, so wrapping them in their own per-edge fragments would be indirection without payoff.

import { defineFragment } from "@rindle/client";

import { issue, rels } from "../../shared/app-def.ts";
import { TagChipFragment } from "./TagChip.queries.ts";
import { UserBadgeFragment } from "./UserBadge.queries.ts";

/** The issue chrome shared by BOTH the card and the detail pane: the core columns every issue view
 *  shows, plus the owner badge (`UserBadgeFragment`) and the tag chips (`TagChipFragment`, ordered by
 *  name). Both roots `include` this, so the owner/tags spreads live in exactly one place. */
export const IssueChromeFragment = defineFragment(issue, (i) =>
  i
    .select("id", "title", "status", "priority", "ownerId")
    .sub("owner", rels.issueOwner, UserBadgeFragment)
    .sub("tags", rels.issueTags, (t) => t.include(TagChipFragment).orderBy("name", "asc")),
);
