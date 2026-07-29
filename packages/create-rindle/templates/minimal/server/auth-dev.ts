// The DEV identity provider — resolves the `x-rindle-user` header to an Identity: no passwords, no
// auth service, so the example runs standalone. The browser persists a handle per tab
// (src/rindle-client.ts) and sends it on every `/api/rindle/*` call; SSR uses a fixed viewer (reads
// are public). Swap this for a real provider (e.g. JWT-against-JWKS) without touching the API server.

import { normalizeSubject } from "../shared/app-def.ts";
import type { AuthProvider, Identity } from "../shared/auth.ts";

/** Resolve a handle to a dev identity, or null when blank. */
export function identityFromHandle(handle: string | null | undefined): Identity | null {
  if (!handle) return null;
  const subject = normalizeSubject(handle);
  if (!subject || subject === "anon") return null;
  return { subject };
}

/** The dev AuthProvider: the principal rides the `x-rindle-user` header. */
export const devAuth: AuthProvider = {
  verify(req: Request): Promise<Identity | null> {
    return Promise.resolve(identityFromHandle(req.headers.get("x-rindle-user")));
  },
};
