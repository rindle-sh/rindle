// The topology conformance suite (design 214: ONE topology): the wire-contract assertions
// the pair must satisfy — every WRITE endpoint served by the `rindle-replicator` master
// (`writes`), every READ endpoint + the public ws served by a follower `rindled` (`reads`).
// The split below IS the contract: which node owns which route. Replication is asynchronous,
// so each write→read boundary polls the follower to its catch-up point first — the same
// eventual-visibility every production consumer of the pair lives with.

import assert from "node:assert/strict";

import { DaemonHttpError, HttpRindleDaemonClient } from "@rindle/daemon-client";
import { WebSocket } from "ws";

/** The schema the pair under test must serve, in the replicator's TableSpec flavor:
 * issue(id number pk, title string) — minted to the follower via the genesis ddl entry. */
export const CONFORMANCE_TABLES = [
  { name: "issue", columns: ["id", "title"], pk: [0], types: ["number", "string"] },
];

/** Poll the follower until `sql` returns `expected` (bare single-cell read), or time out. */
async function followerCatchUp(reads, sql, expected, label) {
  const start = Date.now();
  for (;;) {
    const out = await reads.executeSqlRead({ sql });
    if (out.rows.length && out.rows[0][0] === expected) return;
    if (Date.now() - start > 10_000) {
      throw new Error(`${label}: follower never reached ${expected} (last: ${JSON.stringify(out.rows)})`);
    }
    await new Promise((r) => setTimeout(r, 25));
  }
}

export async function runDaemonConformance({ writeUrl, readUrl, wsUrl, authToken }) {
  // Writes + mutation discipline → the master; reads/materialize/ws → the follower. (In an
  // app this split is `SplitDaemonClient`; here it is explicit — the split IS the contract.)
  const writes = new HttpRindleDaemonClient({ baseUrl: writeUrl });
  const reads = new HttpRindleDaemonClient({
    baseUrl: readUrl,
    headers: { authorization: `Bearer ${authToken}` },
  });

// --- auth: the follower's control plane requires the bearer token ---
const unauthed = new HttpRindleDaemonClient({ baseUrl: readUrl });
await assert.rejects(
  () => unauthed.materialize({ ast: { table: "issue" } }),
  (err) => err instanceof DaemonHttpError && err.status === 401,
);

// --- materialize → lease; identical canonical ASTs report reuse (follower) ---
const lease = await reads.materialize({ ast: { table: "issue", orderBy: [["id", "asc"]] } });
assert.ok(lease.leaseToken);
assert.equal(lease.reused, false);
const again = await reads.materialize({ ast: { orderBy: [["id", "asc"]], table: "issue" } });
assert.equal(again.reused, true, "field order must not change the canonical query key");

// --- the SQL-txn client-mutation discipline (master) ---
const first = await writes.executeSqlTxn({
  clientID: "c1",
  mid: 1,
  statements: [{ sql: "INSERT INTO issue (id, title) VALUES (?, ?)", params: [1, "first"] }],
});
assert.deepEqual(first.lmidAdvances, [{ clientID: "c1", lmid: 1 }]);

// read-your-writes: a strong read against the WRITE master sees the commit immediately —
// no replication wait (the route `SplitDaemonClient` picks for `consistency:"strong"`).
const strong = await writes.executeSqlRead({
  sql: "SELECT id, title FROM issue WHERE id = ?",
  params: [1],
  consistency: "strong",
});
assert.deepEqual(strong.rows, [[1, "first"]], "strong read must see the just-committed write");

// at-least-once replay: same mid absorbed (re-running the INSERT would violate the PK)
const replay = await writes.executeSqlTxn({
  clientID: "c1",
  mid: 1,
  statements: [{ sql: "INSERT INTO issue (id, title) VALUES (?, ?)", params: [1, "first"] }],
});
assert.equal(replay.applied, false);
assert.deepEqual(replay.lmidAdvances, [{ clientID: "c1", lmid: 1 }]);

// a mid gap is the client's protocol violation: 409, nothing applied
await assert.rejects(
  () =>
    writes.executeSqlTxn({
      clientID: "c1",
      mid: 5,
      statements: [{ sql: "INSERT INTO issue (id, title) VALUES (2, 'x')", params: [] }],
    }),
  (err) => err instanceof DaemonHttpError && err.status === 409,
);

// a master-side SQL failure is an ERROR (no lmid advance), not processed-as-no-op
await assert.rejects(
  () =>
    writes.executeSqlTxn({
      clientID: "c1",
      mid: 2,
      statements: [{ sql: "INSERT INTO issue (id, title) VALUES (1, 'pk conflict')", params: [] }],
    }),
  (err) => err instanceof DaemonHttpError && err.status === 500,
);
const afterFailure = await writes.executeSqlTxn({
  clientID: "c1",
  mid: 2,
  statements: [{ sql: "INSERT INTO issue (id, title) VALUES (?, ?)", params: [2, "second"] }],
});
assert.equal(afterFailure.applied, true, "the failed mid's slot stays open for the corrected write");

// --- rejection: lmid-only advance (master) ---
const rejected = await writes.rejectMutation({ clientID: "c1", mid: 3, reason: "policy" });
assert.equal(rejected.lmid, 3);
const postReject = await writes.executeSqlTxn({
  clientID: "c1",
  mid: 4,
  statements: [{ sql: "INSERT INTO issue (id, title) VALUES (?, ?)", params: [3, "third"] }],
});
assert.equal(postReject.applied, true);

// --- idempotency keys dedupe foreign (non-client) writes (master) ---
const foreign = await writes.executeSqlTxn({
  idempotencyKey: "import-1",
  statements: [{ sql: "INSERT INTO issue (id, title) VALUES (?, ?)", params: [10, "import"] }],
});
assert.equal(foreign.applied, true);
const foreignReplay = await writes.executeSqlTxn({
  idempotencyKey: "import-1",
  statements: [{ sql: "INSERT INTO issue (id, title) VALUES (?, ?)", params: [11, "dup"] }],
});
assert.equal(foreignReplay.applied, false);

// --- change-source ingest: replays of the same {source, offset} are absorbed (master) ---
const rc = await writes.applyRowChangeTxn({
  source: "upstream",
  offset: "42",
  changes: [{ table: "issue", op: "add", row: [20, "replicated"] }],
});
assert.equal(rc.applied, true);
const rcReplay = await writes.applyRowChangeTxn({
  source: "upstream",
  offset: "42",
  changes: [{ table: "issue", op: "add", row: [20, "replicated"] }],
});
assert.equal(rcReplay.applied, false);

// --- raw SQL reads (follower), after it has replicated every write above ---
// Accumulated rows: ids 1, 2, 3 (client mutations), 10 (import), 20 (row-change) = 5.
await followerCatchUp(reads, "SELECT count(*) FROM issue", 5, "pre-read catch-up");
const read = await reads.executeSqlRead({
  sql: "SELECT id, title FROM issue WHERE id = ?",
  params: [1],
});
assert.deepEqual(read.cols, ["id", "title"]);
assert.deepEqual(read.rows, [[1, "first"]]);
// multiple rows come back in result order, each row a bare cell array (no per-column keys)
const ordered = await reads.executeSqlRead({
  sql: "SELECT id FROM issue WHERE id IN (1, 2, 3) ORDER BY id",
});
assert.deepEqual(ordered.rows, [[1], [2], [3]]);
// a bad statement is the caller's error (400), not an engine fault (500)
await assert.rejects(
  () => reads.executeSqlRead({ sql: "SELECT * FROM does_not_exist" }),
  (err) => err instanceof DaemonHttpError && err.status === 400,
);
// the read path runs on a READ-ONLY connection: a write/DDL smuggled through it is refused (it
// must NOT silently mutate behind the IVM, bypassing the change-log). Each rejects, and `issue`
// is untouched afterward. The MASTER's read plane obeys the same discipline.
for (const plane of [reads, writes]) {
  const before = (await plane.executeSqlRead({ sql: "SELECT count(*) FROM issue" })).rows[0][0];
  for (const sql of [
    "DELETE FROM issue",
    "UPDATE issue SET title = 'x' RETURNING id",
    "DROP TABLE issue",
  ]) {
    await assert.rejects(
      () => plane.executeSqlRead({ sql }),
      (err) => err instanceof DaemonHttpError && err.status === 400,
      `read-only connection must refuse: ${sql}`,
    );
  }
  const after = (await plane.executeSqlRead({ sql: "SELECT count(*) FROM issue" })).rows[0][0];
  assert.equal(after, before, "no write smuggled through the read path");
  // ATTACH is forbidden too (no reading other database files through the read connection)
  await assert.rejects(
    () => plane.executeSqlRead({ sql: "ATTACH DATABASE 'evil.db' AS x" }),
    (err) => err instanceof DaemonHttpError && err.status === 400,
  );
}

// --- write endpoints on the FOLLOWER are fenced fail-closed ---
await assert.rejects(
  () =>
    reads.executeSqlTxn({
      statements: [{ sql: "INSERT INTO issue (id, title) VALUES (99, 'smuggled')", params: [] }],
    }),
  (err) => err instanceof DaemonHttpError && (err.status === 409 || err.status === 404),
  "a follower must never accept a direct write",
);

// --- the public ws (follower): subscribe by lease, receive the snapshot; bogus leases refused ---
const messages = [];
const ws = new WebSocket(wsUrl);
await new Promise((resolve) => ws.on("open", resolve));
ws.on("message", (data) => messages.push(JSON.parse(String(data))));
ws.send(JSON.stringify({ t: "init", clientID: "viewer" }));
ws.send(JSON.stringify({ t: "subscribe", queryId: 7, leaseToken: lease.leaseToken, mode: "normalized" }));
ws.send(JSON.stringify({ t: "subscribe", queryId: 8, leaseToken: "rindle-lease-bogus", mode: "normalized" }));

await new Promise((resolve, reject) => {
  const start = Date.now();
  const tick = () => {
    const gotHello = messages.some((m) => m.t === "nhello" && m.queryId === 7);
    const gotSnapshot = messages.some((m) => m.t === "nbatch" && m.queryId === 7 && m.batch.seq === 0);
    const gotRefusal = messages.some((m) => m.t === "queryError" && m.queryId === 8);
    if (gotHello && gotSnapshot && gotRefusal) return resolve();
    if (Date.now() - start > 5000) return reject(new Error(`timeout; saw: ${JSON.stringify(messages)}`));
    setTimeout(tick, 10);
  };
  tick();
});
const snapshot = messages.find((m) => m.t === "nbatch" && m.queryId === 7 && m.batch.seq === 0);
const issueOps = snapshot.batch.ops.filter((op) => op.table === "issue");
assert.ok(issueOps.length >= 4, `snapshot carries the pair's accumulated rows, got ${issueOps.length}`);

  ws.close();
  console.log("pair conformance passed");
}
