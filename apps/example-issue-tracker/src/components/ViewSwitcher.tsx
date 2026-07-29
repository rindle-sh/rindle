// The view switcher: list / board / activity. Each view is a DIFFERENT root query (or set of them)
// over the same data, but they reuse the same fragments — the list and the board columns both render
// `IssueCard`; the detail thread and the activity feed both render `CommentCard`. Each is its own
// route, so switching is a `<Link>` (the view is in the URL); the active link is highlighted by the
// router, and the current selection (`?issue=`/`?new=`) is carried across the switch.

import { Link } from "@tanstack/react-router";

const VIEWS = [
  { to: "/", label: "List", exact: true },
  { to: "/board", label: "Board", exact: false },
  { to: "/mine", label: "Mine", exact: false },
  { to: "/activity", label: "Activity", exact: false },
] as const;

export function ViewSwitcher() {
  return (
    <div className="it-segmented it-viewswitch" role="group" aria-label="View">
      {VIEWS.map(({ to, label, exact }) => (
        <Link
          key={to}
          to={to}
          search={(prev) => prev}
          activeOptions={{ exact }}
          activeProps={{ className: "is-active" }}
        >
          {label}
        </Link>
      ))}
    </div>
  );
}
