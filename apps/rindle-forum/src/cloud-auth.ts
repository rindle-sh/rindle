// The browser half of the production identity flow (RINDLE-FORUM-DESIGN.md §3.3) — the counterpart to
// server/auth-oidc.ts. "Sign in with Rindle Cloud" is OIDC-implicit style: a top-level navigation to
// headwaters' /authorize, which (after a headwaters login) 303s back to /auth/callback with a JWT in
// the URL fragment. We stash that token and forward it as `Authorization: Bearer <jwt>` on every
// /api/rindle/* call; the server verifies it offline against headwaters' JWKS. No cross-origin cookie,
// no CORS — the token rides navigations + a fragment, never a credentialed fetch.
//
// The forum NEVER verifies the token itself (that's the server's job) — here we only decode the
// unverified claims to render the signed-in name/avatar. A forged token changes the label in YOUR
// browser and nothing else: the server rejects it, so no write lands.

const TOKEN_KEY = "rindle-forum-cloud-token";

/** Whether the cloud sign-in flow is wired (server set FORUM_AUTH=oidc, Vite mirrors it). When off,
 *  the OSS dev switcher (rindle-client.ts) is the only identity. */
export function cloudAuthEnabled(): boolean {
  return (import.meta.env.VITE_FORUM_AUTH ?? "dev").toLowerCase() === "oidc";
}

function headwatersOrigin(): string {
  return import.meta.env.VITE_HEADWATERS_ORIGIN ?? "http://localhost:8787";
}

interface CloudClaims {
  sub: string;
  name?: string;
  picture?: string;
  exp?: number;
}

/** Decode (do NOT verify) a JWT's payload. Returns null on anything malformed. */
function decodeClaims(token: string): CloudClaims | null {
  const part = token.split(".")[1];
  if (!part) return null;
  try {
    const json = atob(part.replace(/-/g, "+").replace(/_/g, "/"));
    const claims = JSON.parse(json) as CloudClaims;
    return typeof claims.sub === "string" ? claims : null;
  } catch {
    return null;
  }
}

/** The stored token if present and not expired; clears + returns null otherwise. Browser-only. */
export function getCloudToken(): string | null {
  if (typeof localStorage === "undefined") return null;
  const token = localStorage.getItem(TOKEN_KEY);
  if (!token) return null;
  const claims = decodeClaims(token);
  // exp is seconds; drop a token that's expired (or undecodable) so we fall back to anonymous/dev.
  if (!claims || (claims.exp && claims.exp * 1000 <= Date.now())) {
    localStorage.removeItem(TOKEN_KEY);
    return null;
  }
  return token;
}

export function setCloudToken(token: string): void {
  if (typeof localStorage !== "undefined") localStorage.setItem(TOKEN_KEY, token);
}

export function clearCloudToken(): void {
  if (typeof localStorage !== "undefined") localStorage.removeItem(TOKEN_KEY);
}

/** The signed-in cloud identity from the (unverified) token, for rendering only. Null when signed out. */
export function cloudIdentity(): { subject: string; displayName: string; avatarUrl: string } | null {
  const token = getCloudToken();
  if (!token) return null;
  const claims = decodeClaims(token);
  if (!claims) return null;
  return { subject: claims.sub, displayName: claims.name || claims.sub, avatarUrl: claims.picture ?? "" };
}

/** Start the flow: navigate to headwaters /authorize, asking it to bounce a token back to our callback. */
export function signInWithCloud(): void {
  const returnTo = `${location.origin}/auth/callback`;
  location.href = `${headwatersOrigin()}/authorize?returnTo=${encodeURIComponent(returnTo)}`;
}

export function signOutCloud(): void {
  clearCloudToken();
  location.assign("/");
}
