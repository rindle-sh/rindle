// REPRODUCTION — Adversarial-review CRITICAL #4
// "Normalized client never validates the server hello against its own schema — positional
//  rows silently corrupt on schema skew (fingerprint check is tautological)."
//  (packages/remote/src/normalized.ts:76)
//
// NORMALIZED-CHANGES-DESIGN.md §3 says the hello's `tables` exist so the client can validate
// column order / PK against its OWN typed schema, with `normalizedFp` the guard. The shipped
// NormalizedSubscriber instead recomputes the fingerprint FROM `hello.tables` and compares it
// to `hello.normalizedFp` — i.e. it checks the server's advertisement against itself. It is
// never handed the client schema, so it cannot detect column-order / PK skew.
//
// These tests assert the documented protection; they FAIL on HEAD (the subscriber accepts a
// schema-skewed hello without error).

import { test } from "node:test";
import assert from "node:assert/strict";

import { COMPARATOR_VERSION } from "@rindle/client";
import type { NormalizedEvent, NormalizedTableSchema } from "@rindle/client";
import { NormalizedSubscriber, normalizedFp, type NormalizedHello } from "../src/normalized.ts";

// The client's own typed schema: issue(id, title), pk = id.
const CLIENT_TABLES: NormalizedTableSchema[] = [
  { name: "issue", columns: ["id", "title"], primaryKey: [0] },
];
// The server's schema after a routine deployment reordered the columns: issue(title, id),
// pk = id. Same table + column NAMES + PK-by-name, different column ORDER.
const SERVER_TABLES: NormalizedTableSchema[] = [
  { name: "issue", columns: ["title", "id"], primaryKey: [1] },
];

test("CRIT#4: client and server schemas DO have distinct fingerprints (a real check could catch it)", () => {
  // Sanity: the fingerprint is sensitive to column order, so a client-side cross-check
  // WOULD detect this skew. The bug is that the comparison is against the wrong reference.
  assert.notStrictEqual(
    normalizedFp(CLIENT_TABLES),
    normalizedFp(SERVER_TABLES),
    "column-order skew must change the fingerprint",
  );
});

test("CRIT#4: subscriber must reject a hello that disagrees with the client schema", () => {
  const events: NormalizedEvent[] = [];
  // The server advertises ITS schema + a fingerprint computed over ITS schema — internally
  // consistent, so the tautological self-check passes.
  const serverHello: NormalizedHello = {
    epoch: 1,
    comparatorVersion: COMPARATOR_VERSION,
    tables: SERVER_TABLES,
    normalizedFp: normalizedFp(SERVER_TABLES),
  };

  // ORACLE: a subscriber HANDED THE CLIENT SCHEMA must reject this skewed hello (§3 "drift ⇒
  // re-subscribe", §8 invariant 5). On HEAD the subscriber had no client-schema arg, computed
  // fp(hello.tables) === hello.normalizedFp, and accepted it. The fix threads the client's own
  // typed per-table schemas (the backend supplies them via `expectClientSchema`).
  assert.throws(
    () => new NormalizedSubscriber(serverHello, (ev) => events.push(ev), CLIENT_TABLES),
    /schema|mismatch|drift/i,
    "a schema-skewed hello must be rejected, not silently accepted",
  );
  // And the rejection happens BEFORE any event is emitted, so the server-ordered row
  // ["hello world", 7] is never stored under the client's [id, title] order (the consequence
  // — cells transposed, refcount/GC keyed by the title — cannot occur).
  assert.deepStrictEqual(events, [], "a rejected hello must not emit any event");
});

test("CRIT#4: a hello that AGREES with the client schema is accepted (no false positive)", () => {
  const events: NormalizedEvent[] = [];
  // Server schema == client schema (issue = [id, title], pk = id).
  const serverHello: NormalizedHello = {
    epoch: 1,
    comparatorVersion: COMPARATOR_VERSION,
    tables: CLIENT_TABLES,
    normalizedFp: normalizedFp(CLIENT_TABLES),
  };
  const sub = new NormalizedSubscriber(serverHello, (ev) => events.push(ev), CLIENT_TABLES);
  // Accepted: the hello event is emitted, and a snapshot row is delivered positionally under
  // the AGREED column order — the client reads id 7 / title "hello world" correctly.
  assert.equal(events.length, 1);
  assert.equal(events[0]?.type, "hello");
  sub.apply({
    epoch: 1,
    seq: 0,
    normalizedFp: normalizedFp(CLIENT_TABLES),
    ops: [{ table: "issue", op: "add", row: [7, "hello world"] }],
  });
  const snap = events[1];
  assert.equal(snap?.type, "snapshot");
  const row = snap.type === "snapshot" ? snap.ops[0] : undefined;
  assert.ok(row && row.op === "add");
  assert.strictEqual(row.row[0], 7, "client reads col 0 as id");
  assert.strictEqual(row.row[1], "hello world", "client reads col 1 as title");
});
