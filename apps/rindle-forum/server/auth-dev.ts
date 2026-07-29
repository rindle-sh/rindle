// The DEV identity provider — the open-source default (RINDLE-FORUM-DESIGN.md §3.2). It resolves the
// `x-forum-user` header to a ForumIdentity: no passwords, no SaaS, so the example runs standalone with
// zero dependency on Rindle Cloud. The browser persists a handle per-tab (src/rindle-client.ts) and
// sends it on every `/api/rindle/*` call; SSR uses a fixed viewer (reads are public).
//
// Production swaps this for server/auth-oidc.ts, which validates a headwaters-minted JWT against its
// JWKS and maps the claims to the SAME ForumIdentity shape — the api-server never changes.

import { handleToDisplayName, normalizeSubject } from "../shared/app-def.ts";
import type { AuthProvider, ForumIdentity } from "../shared/auth.ts";

/** Resolve a handle to a dev identity, or null when blank. The display name is derived from the
 *  handle (the OIDC provider supplies a real one off the token instead). */
export function identityFromHandle(handle: string | null | undefined): ForumIdentity | null {
  if (!handle) return null;
  const subject = normalizeSubject(handle);
  if (!subject || subject === "anon") return null;
  return { subject, displayName: handleToDisplayName(subject), avatarUrl: "" };
}

/** The dev AuthProvider: the principal rides the `x-forum-user` header. */
export const devAuth: AuthProvider = {
  verify(req: Request): Promise<ForumIdentity | null> {
    return Promise.resolve(identityFromHandle(req.headers.get("x-forum-user")));
  },
};
