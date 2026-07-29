// Pick the identity provider from configuration (RINDLE-FORUM-DESIGN.md §3). One switch, two wirings:
//
//   - default ("dev") → server/auth-dev.ts: the `x-forum-user` header. The open-source default; the
//     example runs standalone with no SaaS.
//   - "oidc"          → server/auth-oidc.ts: verify a headwaters-minted JWT against its JWKS. Our
//     production deployment. Needs HEADWATERS_ISSUER (+ optional HEADWATERS_AUDIENCE / _JWKS_URL).
//
// Both satisfy the SAME AuthProvider seam (shared/auth.ts), so the api-server is identical either way.
// `env` is injectable so the Cloudflare Worker can pass its bindings instead of process.env.

import type { AuthProvider } from "../shared/auth.ts";
import { devAuth } from "./auth-dev.ts";
import { oidcAuthFromEnv } from "./auth-oidc.ts";

export function selectAuth(env: Record<string, string | undefined> = process.env): AuthProvider {
  return (env.FORUM_AUTH ?? "dev").toLowerCase() === "oidc" ? oidcAuthFromEnv(env) : devAuth;
}
