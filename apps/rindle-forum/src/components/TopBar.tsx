// The persistent top bar: the forum wordmark + the identity control. Two modes (RINDLE-FORUM-DESIGN §3):
//   - dev (OSS default): "who you are" is a handle persisted per browser; "Switch user" rewrites it.
//   - cloud (FORUM_AUTH=oidc): "Sign in with Rindle Cloud" runs the headwaters /authorize redirect;
//     once signed in we show the account name from the verified token and offer sign-out.
// The dev switcher stays available even in cloud mode (until you've signed in) so the example is still
// clickable without an account.

import { Link } from "@tanstack/react-router";

import { useIdentity } from "../lib/use-handle.ts";
import { currentHandle, setCurrentHandle } from "../rindle-client.ts";
import { cloudAuthEnabled, signInWithCloud, signOutCloud } from "../cloud-auth.ts";

export function TopBar() {
  const me = useIdentity();
  const cloudOn = cloudAuthEnabled();

  function switchUser() {
    const next = prompt("Switch dev user (handle):", currentHandle());
    if (next && next.trim()) {
      setCurrentHandle(next.trim());
      location.reload();
    }
  }

  return (
    <header className="rf-topbar">
      <Link to="/" className="rf-wordmark">
        rindle<span>forum</span>
      </Link>
      <div className="rf-topbar-right">
        {me.kind === "cloud" ? (
          <>
            <span className="rf-whoami" title="Signed in with Rindle Cloud">
              signed in as <b>{me.displayName}</b>
            </span>
            <button type="button" className="rf-btn rf-btn-ghost" onClick={signOutCloud}>
              Sign out
            </button>
          </>
        ) : (
          <>
            <span className="rf-whoami" title="Your dev identity">
              signed in as <b>{me.displayName}</b>
            </span>
            <button type="button" className="rf-btn rf-btn-ghost" onClick={switchUser}>
              Switch user
            </button>
            {cloudOn && (
              <button type="button" className="rf-btn" onClick={signInWithCloud}>
                Sign in with Rindle Cloud
              </button>
            )}
          </>
        )}
      </div>
    </header>
  );
}
