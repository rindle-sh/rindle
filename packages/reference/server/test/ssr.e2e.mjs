// SSR one-shot, end-to-end against the REAL Rust pair (design 214: ONE topology — a
// `rindle-replicator` write-master + a follower `rindled`). Proves the whole client SSR data
// path (SSR-DESIGN.md §3 + §6) over the wire, not in unit isolation:
//
//   rindled  ──POST /query──►  HttpRindleDaemonClient.query()
//                              └─► createServerStore().preload()  (assembled → projected seed)
//                                  └─► store.materialize(q).data   (what renderToString reads)
//                                  └─► store.dehydrate()           (what ships in the HTML)
//
// The seed writes go to the MASTER (the daemon serves no write endpoints); the reads poll the
// follower to its catch-up point first — the same eventual visibility a production SSR loader
// lives with. It also confirms the no-leak / warm-handoff property end to end: after the
// reads, the follower reports ZERO subscriptions (the one-shot registered none). Built via
// cargo (no wasm toolchain), so it lives on the `test:rust-daemon` lane alongside
// daemon-rust.e2e.mjs.

import assert from "node:assert/strict";

import { HttpRindleDaemonClient } from "@rindle/daemon-client";
import { createSchema, createServerStore, newQueryBuilder, number, stableKey, string, table } from "@rindle/client";

import { buildPairBinaries, startPair } from "./spawn-pair.mjs";

const authToken = "secret";

// The client-side typed schema (mirrors the pair's tables below).
const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const comment = table("comment").columns({ id: number(), issueId: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });
const qb = newQueryBuilder(schema);

buildPairBinaries();
const pair = await startPair({
  tables: [
    { name: "issue", columns: ["id", "title"], pk: [0], types: ["number", "string"] },
    { name: "comment", columns: ["id", "issueId", "body"], pk: [0], types: ["number", "number", "string"] },
  ],
  authToken,
  prefix: "rindled-ssr-e2e-",
});

// Writes → the master; reads (/query, /stats) → the follower.
const writes = new HttpRindleDaemonClient({ baseUrl: pair.writeUrl });
const client = new HttpRindleDaemonClient({
  baseUrl: pair.readUrl,
  headers: { authorization: `Bearer ${authToken}` },
});

/** POST a raw control-plane op to the follower (for /stats, which the typed client doesn't model). */
async function control(path, body) {
  const res = await fetch(`${pair.readUrl}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${authToken}` },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${path} -> ${res.status} ${await res.text()}`);
  return res.json();
}

/** Poll the follower until `sql` returns `expected` (bare single-cell read), or time out. */
async function followerCatchUp(sql, expected, label) {
  const start = Date.now();
  for (;;) {
    const out = await client.executeSqlRead({ sql });
    if (out.rows.length && out.rows[0][0] === expected) return;
    if (Date.now() - start > 10_000) {
      throw new Error(`${label}: follower never reached ${expected} (last: ${JSON.stringify(out.rows)})`);
    }
    await new Promise((r) => setTimeout(r, 25));
  }
}

try {
  // Seed data through the real write plane (the same path an app's API server writes through),
  // then wait for the follower to converge before the SSR reads.
  await writes.executeSqlTxn({
    clientID: "seed",
    mid: 1,
    statements: [
      { sql: "INSERT INTO issue (id, title) VALUES (?, ?)", params: [1, "bug"] },
      { sql: "INSERT INTO issue (id, title) VALUES (?, ?)", params: [2, "lonely"] },
      { sql: "INSERT INTO comment (id, issueId, body) VALUES (?, ?, ?)", params: [10, 1, "me too"] },
      { sql: "INSERT INTO comment (id, issueId, body) VALUES (?, ?, ?)", params: [11, 1, "+1"] },
    ],
  });
  await followerCatchUp("SELECT count(*) FROM comment", 2, "seed");

  // The SSR server store: one-shot REST backend, fed by the real follower's /query.
  const server = createServerStore(schema, { query: (input) => client.query(input) });

  // --- flat read ---
  const flat = qb.issue.orderBy("id", "asc");
  await server.preload(flat);
  const flatView = server.store.materialize(flat);
  assert.deepStrictEqual(
    flatView.data,
    [
      { id: 1, title: "bug" },
      { id: 2, title: "lonely" },
    ],
    "flat: the server store seeds the assembled rows the render reads",
  );
  console.log("✓ flat one-shot: preload → seeded view matches the follower's assembled rows");

  // --- nested read (issue { comments }) ---
  const nested = qb.issue
    .sub("comments", comment, { parent: ["id"], child: ["issueId"] }, (c) => c.orderBy("id", "asc"))
    .orderBy("id", "asc");
  await server.preload(nested);
  const nestedView = server.store.materialize(nested);
  assert.deepStrictEqual(
    nestedView.data,
    [
      {
        id: 1,
        title: "bug",
        comments: [
          { id: 10, issueId: 1, body: "me too" },
          { id: 11, issueId: 1, body: "+1" },
        ],
      },
      { id: 2, title: "lonely", comments: [] },
    ],
    "nested: relationships inline by alias; a childless parent is an empty array",
  );
  console.log("✓ nested one-shot: comments inline by alias, empty array for the childless parent");

  // --- dehydrate carries projected rows + a real cvMin baseline, keyed by viewKey ---
  const dehydrated = server.dehydrate();
  const flatKey = stableKey(flat.ast());
  assert.ok(dehydrated[flatKey], "dehydrate is keyed by the shared viewKey");
  assert.equal(typeof dehydrated[flatKey].cvMin, "number", "dehydrate carries the cvMin baseline");
  assert.deepStrictEqual(dehydrated[flatKey].rows, [
    { id: 1, title: "bug" },
    { id: 2, title: "lonely" },
  ]);
  // It survives the HTML embed (JSON round-trip).
  assert.deepStrictEqual(JSON.parse(JSON.stringify(dehydrated))[flatKey].rows, dehydrated[flatKey].rows);
  console.log("✓ dehydrate: projected rows + cvMin per viewKey, JSON-serializable for the HTML");

  // --- the per-request visibilityKey/ttlMs are honored by the daemon (SSR-DESIGN.md §3.2/§3.4) ---
  // The visibilityKey scopes the dedup QueryKey, so the same AST under two keys never shares a
  // pipeline; the fingerprint the daemon returns proves it reached QueryKey::new.
  const ast = flat.ast();
  const bare = await client.query({ ast });
  const tenantA = await client.query({ ast, visibilityKey: "tenant-a", ttlMs: 1000 });
  const tenantA2 = await client.query({ ast, visibilityKey: "tenant-a" });
  const tenantB = await client.query({ ast, visibilityKey: "tenant-b" });
  assert.equal(tenantA.queryKey, tenantA2.queryKey, "same visibility dedups to one materialization");
  assert.notEqual(tenantA.queryKey, tenantB.queryKey, "different visibility forks the materialization");
  assert.notEqual(bare.queryKey, tenantA.queryKey, "the empty default key differs from a scoped one");
  console.log("✓ visibilityKey: the daemon scopes the dedup QueryKey per request (ttlMs accepted)");

  // --- no leak / warm handoff: the one-shots registered NO subscriber ---
  const stats = await control("/stats", {});
  assert.equal(stats.subscriptions, 0, `the one-shot reads leaked no subscription (got ${stats.subscriptions})`);
  console.log("✓ no leak: follower reports zero subscriptions after the one-shot reads");

  console.log("✅ SSR one-shot E2E (RUST pair): preload → seed → dehydrate, no subscription leaked");
} finally {
  pair.cleanup();
}
