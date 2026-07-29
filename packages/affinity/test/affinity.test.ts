import { test } from "node:test";
import assert from "node:assert/strict";

import {
  headerTicket,
  mint,
  type Payload,
  verify,
  wsOffersBase,
  wsSubprotocolTicket,
} from "../src/index.ts";

const KEY = "fleet-secret-key-v1";
const KEY2 = "fleet-secret-key-v0-previous";

function sample(): Payload {
  return {
    app: "rindle-demo",
    mid: "2861d42cd33578",
    region: "iad",
    sub: "client-abc123",
    iat: 1_752_600_000,
    exp: 1_752_603_600,
    gen: 1,
  };
}

const expectAt = (now: number) => ({ app: "rindle-demo", now, minGen: 0 });

test("mint/verify round-trips", () => {
  const p = sample();
  const tok = mint(p, KEY);
  assert.ok(tok.startsWith("aff."));
  const got = verify(tok, [KEY], expectAt(p.iat));
  assert.ok(got.ok);
  assert.deepEqual(got.payload, p);
});

test("frozen test vector is byte-identical to the rindle-affinity crate", () => {
  // The exact token the crate's `frozen_test_vector` asserts. If either side's encoding drifts,
  // one of the two suites goes red. Regenerate BOTH deliberately (design §1).
  assert.equal(
    mint(sample(), KEY),
    "aff.eyJhcHAiOiJyaW5kbGUtZGVtbyIsIm1pZCI6IjI4NjFkNDJjZDMzNTc4IiwicmVnaW9uIjoiaWFkIiwic3ViIjoiY2xpZW50LWFiYzEyMyIsImlhdCI6MTc1MjYwMDAwMCwiZXhwIjoxNzUyNjAzNjAwLCJnZW4iOjF9.V9db9kCjw3tt5NLNe-yxnqyNTR97CjSFJ1SBZ-trKbM",
  );
});

test("a tampered payload fails as bad-signature", () => {
  const tok = mint(sample(), KEY);
  const [prefix, payloadB64, sigB64] = tok.split(".");
  // Flip the last payload char, keep the now-stale signature.
  const flipped = payloadB64.slice(0, -1) + (payloadB64.at(-1) === "9" ? "8" : "9");
  const forged = `${prefix}.${flipped}.${sigB64}`;
  const got = verify(forged, [KEY], expectAt(sample().iat));
  assert.deepEqual(got, { ok: false, error: "bad-signature" });
});

test("the wrong key fails", () => {
  const got = verify(mint(sample(), KEY), [KEY2], expectAt(sample().iat));
  assert.deepEqual(got, { ok: false, error: "bad-signature" });
});

test("dual-key rotation window accepts either key", () => {
  const tok = mint(sample(), KEY2); // minted under the previous key
  assert.ok(verify(tok, [KEY, KEY2], expectAt(sample().iat)).ok);
  // Once the window closes (previous key dropped), the same ticket is rejected.
  assert.deepEqual(verify(tok, [KEY], expectAt(sample().iat)), { ok: false, error: "bad-signature" });
});

test("an expired ticket is rejected (exp is inclusive)", () => {
  const p = sample();
  const tok = mint(p, KEY);
  assert.deepEqual(verify(tok, [KEY], expectAt(p.exp + 1)), { ok: false, error: "expired" });
  assert.ok(verify(tok, [KEY], expectAt(p.exp)).ok);
});

test("a wrong-app ticket is rejected", () => {
  const got = verify(mint(sample(), KEY), [KEY], { app: "other-app", now: sample().iat, minGen: 0 });
  assert.deepEqual(got, { ok: false, error: "wrong-app" });
});

test("a stale-generation ticket is rejected", () => {
  const tok = mint(sample(), KEY); // gen = 1
  assert.deepEqual(verify(tok, [KEY], { app: "rindle-demo", now: sample().iat, minGen: 2 }), {
    ok: false,
    error: "stale-generation",
  });
  assert.ok(verify(tok, [KEY], { app: "rindle-demo", now: sample().iat, minGen: 1 }).ok);
});

test("malformed shapes are rejected", () => {
  const e = expectAt(sample().iat);
  for (const bad of [
    "",
    "aff",
    "aff.",
    "aff.onlytwo",
    "aff..sig",
    "aff.payload.",
    "nope.a.b",
    "aff.a.b.c",
    "aff.@@@.###",
  ]) {
    assert.deepEqual(verify(bad, [KEY], e), { ok: false, error: "malformed" }, `for ${JSON.stringify(bad)}`);
  }
});

test("ws subprotocol parsing", () => {
  const tok = mint(sample(), KEY);
  const hdr = `rindle.v1, ${tok}`;
  assert.equal(wsSubprotocolTicket(hdr), tok);
  assert.ok(wsOffersBase(hdr));
  assert.equal(wsSubprotocolTicket("rindle.v1"), null);
  assert.ok(wsOffersBase("rindle.v1"));
  assert.equal(wsSubprotocolTicket("something.else"), null);
  assert.ok(!wsOffersBase("something.else"));
});

test("header parsing", () => {
  const tok = mint(sample(), KEY);
  assert.equal(headerTicket(`  ${tok}  `), tok);
  assert.equal(headerTicket(""), null);
  assert.equal(headerTicket("garbage"), null);
});
