function randomBytes(): Uint8Array {
  const crypto = globalThis.crypto;
  if (typeof crypto?.getRandomValues !== "function") {
    throw new TypeError("Rindle SQL requires Web Crypto getRandomValues for request identities");
  }
  return crypto.getRandomValues(new Uint8Array(16));
}

function hexBytes(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

/** Runtime-portable one-shot idempotency key; its wire format is intentionally centralized here. */
export function newIdempotencyKey(): string {
  const now = Date.now();
  if (!Number.isSafeInteger(now) || now < 0 || now > 9_999_999_999_999) {
    throw new TypeError("system clock is outside the canonical SQL idempotency-key range");
  }
  return `sql1.${now.toString().padStart(13, "0")}.${hexBytes(randomBytes())}`;
}

/** One canonical design-225 logical request id backed by 128 cryptographically random bits. */
export function newRequestId(): string {
  return `rid1.${hexBytes(randomBytes())}`;
}
