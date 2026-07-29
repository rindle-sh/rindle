// Follower-affinity tickets — the TypeScript twin of the `rindle-affinity` Rust crate
// (`rust/rindle-affinity/src/lib.rs`; `designs/FOLLOWER-AFFINITY-DESIGN.md` §3, §9). Used by the
// local dev-edge (S4) and any JS-side verify; the daemon mints/verifies with the Rust crate.
//
// # Byte-compatibility
//
// A ticket is `aff.<base64url(payload)>.<base64url(hmac_sha256(base64url(payload), key))>` where
// `payload` is COMPACT JSON with the field order below. `mint` here produces byte-for-byte the same
// token as the Rust crate for the same input — pinned by the shared `frozenTestVector` in the tests
// (mirrored from the crate's `frozen_test_vector`). Because `verify` authenticates the transmitted
// base64url payload segment BEFORE parsing it, a Rust-minted ticket verifies here and vice-versa
// regardless of either side's JSON handling.
//
// Placement, not authorization (design §3, §9): a forged ticket only *aims* a connection at a
// machine, granting no data access. The key is a symmetric per-fleet secret.

import { createHmac, timingSafeEqual } from "node:crypto";

/** The token namespace marker — the first dot-segment of every ticket. */
export const TICKET_PREFIX = "aff";

/** The base ws subprotocol offered alongside the `aff.*` ticket; the daemon echoes it on the 101. */
export const WS_SUBPROTOCOL = "rindle.v1";

/** The api-server → fleet control-plane header carrying the ticket on `/materialize` and `/query`. */
export const HEADER_NAME = "Rindle-Affinity";

/**
 * The signed ticket body. Field order here is the canonical mint order — it is exactly the compact
 * JSON that gets base64url-encoded and signed, so it must match the Rust `Payload` struct order.
 */
export interface Payload {
  /** App id the ticket is scoped to. */
  app: string;
  /** Placement target id. */
  mid: string;
  /** Region code of `mid`. */
  region: string;
  /** Subject: the connection identity (clientInstanceId). */
  sub: string;
  /** Issued-at, unix seconds (informational). */
  iat: number;
  /** Expiry, unix seconds. `verify` rejects once `now > exp`. */
  exp: number;
  /** Generation minted under; a fleet-wide bump drains older tickets. */
  gen: number;
}

/** What {@link verify} holds a ticket against, supplied by the verifier. */
export interface Expect {
  /** The verifier's app id; must equal `payload.app`. */
  app: string;
  /** Current wall-clock, unix seconds. `exp < now` ⇒ rejected. */
  now: number;
  /** Minimum accepted generation; `gen < minGen` ⇒ rejected. `0` accepts any. */
  minGen: number;
}

/** Why a ticket failed {@link verify}. Every variant is a terminal reject (fall back to re-pin). */
export type VerifyError =
  | "malformed"
  | "bad-signature"
  | "wrong-app"
  | "expired"
  | "stale-generation";

/** {@link verify}'s result: the authenticated payload, or a reason. */
export type VerifyResult =
  | { readonly ok: true; readonly payload: Payload }
  | { readonly ok: false; readonly error: VerifyError };

type Key = string | Uint8Array;

const B64URL = /^[A-Za-z0-9_-]+$/;

function keyBytes(key: Key): Uint8Array {
  return typeof key === "string" ? new TextEncoder().encode(key) : key;
}

function b64url(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("base64url");
}

/**
 * Mint a ticket over `payload`, signed with `key`. Deterministic (no randomness): the same
 * `(payload, key)` always yields the same token. The compact JSON is built in the fixed field order
 * so the bytes match the Rust crate's `serde_json` compact output.
 */
export function mint(payload: Payload, key: Key): string {
  const json = JSON.stringify({
    app: payload.app,
    mid: payload.mid,
    region: payload.region,
    sub: payload.sub,
    iat: payload.iat,
    exp: payload.exp,
    gen: payload.gen,
  });
  const payloadB64 = Buffer.from(json, "utf8").toString("base64url");
  const sig = createHmac("sha256", keyBytes(key)).update(payloadB64).digest();
  return `${TICKET_PREFIX}.${payloadB64}.${b64url(sig)}`;
}

/**
 * Verify `token` against `keys` (current first, then rotation-window predecessors) and `expect`.
 * The HMAC is checked (constant-time) over the transmitted base64url payload segment before the
 * payload is parsed, so a tampered payload fails as `bad-signature`, never as a parse of attacker
 * bytes.
 */
export function verify(token: string, keys: readonly Key[], expect: Expect): VerifyResult {
  const parts = splitToken(token);
  if (!parts) return { ok: false, error: "malformed" };
  const [payloadB64, sigB64] = parts;
  if (!B64URL.test(payloadB64) || !B64URL.test(sigB64)) return { ok: false, error: "malformed" };

  const sig = Buffer.from(sigB64, "base64url");
  const msg = Buffer.from(payloadB64, "utf8");
  const matches = keys.some((k) => {
    const mac = createHmac("sha256", keyBytes(k)).update(msg).digest();
    return mac.length === sig.length && timingSafeEqual(mac, sig);
  });
  if (!matches) return { ok: false, error: "bad-signature" };

  const payload = parsePayload(payloadB64);
  if (!payload) return { ok: false, error: "malformed" };
  if (payload.app !== expect.app) return { ok: false, error: "wrong-app" };
  if (expect.now > payload.exp) return { ok: false, error: "expired" };
  if (payload.gen < expect.minGen) return { ok: false, error: "stale-generation" };
  return { ok: true, payload };
}

/**
 * Extract the `aff.*` ticket from a `Sec-WebSocket-Protocol` header value (a comma list, e.g.
 * `rindle.v1, aff.<…>`). Returns the raw ticket segment for {@link verify}, or `null`. Does not verify.
 */
export function wsSubprotocolTicket(header: string): string | null {
  for (const raw of header.split(",")) {
    const p = raw.trim();
    if (isTicketShape(p)) return p;
  }
  return null;
}

/** Whether the header offers the base {@link WS_SUBPROTOCOL} — the daemon must echo it on the 101. */
export function wsOffersBase(header: string): boolean {
  return header.split(",").some((p) => p.trim() === WS_SUBPROTOCOL);
}

/** Extract the ticket from a {@link HEADER_NAME} value. Returns the raw segment, or `null`. */
export function headerTicket(value: string): string | null {
  const v = value.trim();
  return isTicketShape(v) ? v : null;
}

/** A syntactic ticket = `aff.<seg>.<seg>` with non-empty segments. Cheap prefilter, like the crate. */
function isTicketShape(s: string): boolean {
  return splitToken(s) !== null;
}

/** Split into (payloadB64, sigB64), validating the `aff.` prefix and exact 3-segment shape. */
function splitToken(token: string): [string, string] | null {
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  const [prefix, payloadB64, sigB64] = parts;
  if (prefix !== TICKET_PREFIX || payloadB64.length === 0 || sigB64.length === 0) return null;
  return [payloadB64, sigB64];
}

function parsePayload(payloadB64: string): Payload | null {
  let value: unknown;
  try {
    value = JSON.parse(Buffer.from(payloadB64, "base64url").toString("utf8"));
  } catch {
    return null;
  }
  if (typeof value !== "object" || value === null) return null;
  const p = value as Record<string, unknown>;
  if (
    typeof p.app !== "string" ||
    typeof p.mid !== "string" ||
    typeof p.region !== "string" ||
    typeof p.sub !== "string" ||
    typeof p.iat !== "number" ||
    typeof p.exp !== "number" ||
    typeof p.gen !== "number"
  ) {
    return null;
  }
  return { app: p.app, mid: p.mid, region: p.region, sub: p.sub, iat: p.iat, exp: p.exp, gen: p.gen };
}
