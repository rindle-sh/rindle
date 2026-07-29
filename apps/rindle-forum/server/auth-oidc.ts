// The PRODUCTION identity provider (RINDLE-FORUM-DESIGN.md §3.3) — the counterpart to the dev provider
// (server/auth-dev.ts). It validates a JWT minted by headwaters (Rindle Cloud) against its published
// JWKS and maps the claims to the SAME ForumIdentity shape, so the api-server never changes.
//
// This file is NOT part of the open-source surface: it encodes our SaaS specifics (the headwaters
// issuer + JWKS URL). The OSS build keeps the dev provider as the default; a real deployment wires
// THIS one (see the toggle in server/api.ts / worker.ts).
//
// What it trusts, and what it does NOT: it trusts ONLY a signature from headwaters' JWKS plus the
// pinned `iss`/`aud`/`exp`. It never sees a password, never holds a shared secret, and never calls the
// SaaS database — the JWKS is fetched once and cached (jose's createRemoteJWKSet, with its own refresh
// + cooldown). headwaters mints the token (GET /api/auth/token); the browser forwards it to the forum
// as `Authorization: Bearer <jwt>`; everything past that is offline verification.

import { createRemoteJWKSet, jwtVerify } from "jose";
import type { JWTPayload, JWTVerifyGetKey } from "jose";

import type { AuthProvider, ForumIdentity } from "../shared/auth.ts";

export interface OidcAuthConfig {
  /** The headwaters origin — the JWT `iss` we pin (e.g. https://cloud.rindle.sh). */
  issuer: string;
  /** The `aud` we require. Mirrors headwaters' JWT_AUDIENCE (which defaults to its own origin). */
  audience: string;
  /** Where the public keys live. Defaults to `${issuer}/api/auth/jwks` (the `jwt` plugin's endpoint). */
  jwksUrl?: string;
  /**
   * The key resolver jose verifies against. Defaults to a cached remote JWKS over `jwksUrl`. Injectable
   * so a test (or an air-gapped deploy that ships keys out-of-band) can supply a local key set without
   * a network fetch — the verification logic is identical either way.
   */
  jwks?: JWTVerifyGetKey;
}

/** Map verified JWT claims → ForumIdentity. headwaters' definePayload emits sub/name/picture (§3.3). */
function identityFromClaims(payload: JWTPayload): ForumIdentity | null {
  const subject = typeof payload.sub === "string" ? payload.sub.trim() : "";
  if (!subject) return null;
  const name = typeof payload.name === "string" ? payload.name.trim() : "";
  const picture = typeof payload.picture === "string" ? payload.picture : "";
  // Fall back to the subject for the display name: a token must always render to *some* author label,
  // and the projection refreshes from a real name on the next write that carries one.
  return { subject, displayName: name || subject, avatarUrl: picture };
}

/** Pull the bearer token out of the Authorization header, or null when absent/malformed. */
function bearer(req: Request): string | null {
  const header = req.headers.get("authorization") ?? req.headers.get("Authorization");
  if (!header) return null;
  const match = /^Bearer\s+(.+)$/i.exec(header.trim());
  return match ? match[1]!.trim() : null;
}

/**
 * Build the OIDC AuthProvider for a given headwaters deployment. `verify` returns null (anonymous) for
 * a missing OR invalid token — it never throws — so reads stay public and only mutations, which
 * require a non-null identity (server/app-api.ts), get rejected (401) when the token is bad/expired.
 */
export function createOidcAuth(config: OidcAuthConfig): AuthProvider {
  const jwksUrl = config.jwksUrl ?? new URL("/api/auth/jwks", config.issuer).toString();
  const keys = config.jwks ?? createRemoteJWKSet(new URL(jwksUrl));

  return {
    async verify(req: Request): Promise<ForumIdentity | null> {
      const token = bearer(req);
      if (!token) return null;
      try {
        const { payload } = await jwtVerify(token, keys, {
          issuer: config.issuer,
          audience: config.audience,
        });
        return identityFromClaims(payload);
      } catch {
        // Bad signature / wrong iss|aud / expired / unknown kid → treat as anonymous. A mutation then
        // 401s; a read is unaffected.
        return null;
      }
    },
  };
}

/** Read the headwaters config from the environment. Throws if unset so a misconfigured prod deploy
 *  fails loud at boot rather than silently rejecting every user as anonymous. */
export function oidcAuthFromEnv(env: Record<string, string | undefined> = process.env): AuthProvider {
  const issuer = env.HEADWATERS_ISSUER;
  if (!issuer) {
    throw new Error("HEADWATERS_ISSUER is required to wire the OIDC auth provider (server/auth-oidc.ts)");
  }
  return createOidcAuth({
    issuer,
    // Defaults to the issuer, matching headwaters' own JWT_AUDIENCE default (§3.3).
    audience: env.HEADWATERS_AUDIENCE ?? issuer,
    jwksUrl: env.HEADWATERS_JWKS_URL,
  });
}
