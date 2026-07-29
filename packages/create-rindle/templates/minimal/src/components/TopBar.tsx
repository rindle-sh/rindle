// The persistent top bar: the app wordmark + the dev identity control. "Who you are" is a handle
// persisted per browser; "Switch user" rewrites it. A real app would show the signed-in account from
// a verified token instead (see shared/auth.ts).

import { Link } from "@tanstack/react-router";

import { useCurrentHandle } from "../lib/use-handle.ts";
import { currentHandle, setCurrentHandle } from "../rindle-client.ts";

export function TopBar() {
  const me = useCurrentHandle();

  function switchUser() {
    const next = prompt("Switch dev user (handle):", currentHandle());
    if (next && next.trim()) {
      setCurrentHandle(next.trim());
      location.reload();
    }
  }

  return (
    <header className="app-topbar">
      <Link to="/" className="app-wordmark">
        rindle<span>starter</span>
      </Link>
      <div className="app-topbar-right">
        <span className="app-whoami" title="Your dev identity">
          signed in as <b>{me}</b>
        </span>
        <button type="button" className="app-btn app-btn-ghost" onClick={switchUser}>
          Switch user
        </button>
      </div>
    </header>
  );
}
