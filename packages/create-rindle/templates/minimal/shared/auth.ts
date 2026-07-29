// The identity SEAM — the whole contract the app depends on for "who is this?". The app owns no
// account lifecycle: it trusts a verified identity resolved from the inbound request and nothing more.
//
//   - This template wires the DEV provider (server/auth-dev.ts): a handle off an `x-rindle-user`
//     header, so the example runs standalone with no auth service.
//   - To go to production, write another AuthProvider that validates a real credential (e.g. a JWT
//     against a JWKS) and maps it to the SAME Identity shape — the API server never changes.

/** A verified identity. `subject` is the stable external id used as the author of writes. */
export interface Identity {
  subject: string;
}

/** Resolve the inbound request's credential to a verified identity, or null when anonymous. Reads are
 *  public; only mutations require a non-null identity (server/app-api.ts). */
export interface AuthProvider {
  verify(req: Request): Promise<Identity | null>;
}
