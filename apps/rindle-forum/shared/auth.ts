// The identity SEAM — the entire contract the forum depends on for "who is this?" (RINDLE-FORUM-DESIGN.md
// §3). The forum owns NO account lifecycle: no passwords, no signup, no billing. It trusts a verified
// identity resolved from the inbound request and nothing more.
//
//   - The OPEN-SOURCE build wires the dev provider (server/auth-dev.ts): a handle off a header, no SaaS.
//   - OUR deployment wires the OIDC provider (server/auth-oidc.ts): it validates a JWT minted
//     by headwaters (Rindle Cloud) against its JWKS and maps the claims to a ForumIdentity. That file
//     is NOT part of the open-source surface — it encodes SaaS specifics.
//
// Either way the api-server keys its principal off `identity.subject`, and the user-projection row is
// upserted from `displayName`/`avatarUrl` (§3.4).

/** A verified identity. `subject` is the OIDC `sub` — the stable external id, and the forum user's
 *  primary key. The display fields are cached into the `user` projection on each authenticated write. */
export interface ForumIdentity {
  subject: string;
  displayName: string;
  avatarUrl?: string;
}

/** Resolve the inbound request's credential to a verified identity, or null when anonymous. Reads are
 *  public (anyone may browse); only mutations require a non-null identity (server/app-api.ts). */
export interface AuthProvider {
  verify(req: Request): Promise<ForumIdentity | null>;
}
