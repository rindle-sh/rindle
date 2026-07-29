// A person's display name (with an optional avatar), read off the shared root view via the `UserBadge`
// fragment. The SAME component renders a thread's author and a post's author — each parent spreads
// `UserBadge` into its own selection, and this leaf just `useFragment`s its slice (no query of its own).

import { useFragment } from "@rindle/react";

import { UserBadgeFragment } from "./UserBadge.queries.ts";
import type { UserBadgeRef } from "./UserBadge.queries.ts";

export function UserBadge({ user, className = "rf-user" }: { user: UserBadgeRef; className?: string }) {
  const data = useFragment(UserBadgeFragment, user);
  if (!data) return null;

  return (
    <span className={className}>
      <span className="rf-avatar" aria-hidden="true">
        {data.avatarUrl ? <img src={data.avatarUrl} alt="" /> : initials(data.displayName)}
      </span>
      {data.displayName}
    </span>
  );
}

function initials(name: string): string {
  const parts = name.trim().split(/\s+/);
  return ((parts[0]?.[0] ?? "?") + (parts[1]?.[0] ?? "")).toUpperCase();
}
