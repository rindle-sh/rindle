// A person's display name, read off the shared root view via the `UserBadge` fragment. The SAME
// component renders an issue's owner and a comment's author — each parent spreads `UserBadge` into
// its own selection, and this leaf just `useFragment`s its slice (no query of its own).

import { useFragment } from "@rindle/react";

import { UserBadgeFragment } from "./UserBadge.queries.ts";
import type { UserBadgeRef } from "./UserBadge.queries.ts";

export function UserBadge({ user, className = "it-user" }: { user: UserBadgeRef; className?: string }) {
  const data = useFragment(UserBadgeFragment, user);
  if (!data) return null;

  return <span className={className}>{data.name}</span>;
}
