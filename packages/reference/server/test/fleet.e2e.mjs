// FLEET e2e — the full three-tier topology under load and shape variety:
//
//   many optimistic clients  ──►  a real in-process API server (@rindle/api-server)  ──►  rindled
//   (@rindle/optimistic)            • resolves NAMED queries to ASTs                     (the Rust daemon
//    each: wasm engine               • runs the AUTHORITATIVE mutators into SQL           binary, spawned)
//    + ws + lmid system query        • talks the private control plane
//
// A MULTI-TABLE fixture (issue / comment / label) drives SIX query shapes at once —
// flat windowed pagination, a flat filter, two nested-child shapes, an EXISTS gate, and a
// NOT-EXISTS gate — so the sync path is exercised across the operator surface, not one query.
//
// A relationship `count` aggregate is not duplicated as a fleet shape here — the optimistic
// backend's synthetic-`__agg_*`-table handling is covered directly by the optimistic package's
// own tests (packages/optimistic/test/e2e.mjs + aggregate-optimistic.test.ts).
//
// CORRECTNESS is checked two ways, every step:
//   • convergence — every client subscribed to a shape agrees byte-for-byte;
//   • an INDEPENDENT oracle — a local `createWasmStore` fed the same logical writes. The
//     fleet must equal the oracle for every shape after every mutation round.
//
// Plus the three structural invariants:
//   1. DEDUP — N clients subscribing the SAME canonical query share ONE engine pipeline
//      (observed over the wire via the daemon's `/stats.materializations`): each new shape
//      adds exactly +1 regardless of how many clients subscribe it; a distinct query is its
//      own pipeline. (Each client additionally has its own per-connection lmid system query,
//      so the baseline is exactly NUM_CLIENTS.)
//   2. LATE JOINER — a client that connects AFTER a burst of writes hydrates to the current
//      snapshot (== the oracle == its peers) and REUSES the existing shared pipelines.
//   3. TRANSACTIONAL store updates — a mutator that writes multiple rows in ONE transaction
//      lands on every client atomically: a nested view subscribed throughout NEVER observes a
//      torn intermediate (an issue created with two labels is seen with 0 or 2 — never 1).
//
// Run with `--conditions=@rindle/source`; needs the wasm pkg + the replica native addon built.

import assert from "node:assert/strict";
import { isDeepStrictEqual } from "node:util";
import { createServer } from "node:http";

import { createSchema, defineQuery, exists, ge, newQueryBuilder, notExists, number, string, table } from "@rindle/client";
import { createWasmStore, initWasm } from "@rindle/wasm";
import { HttpRindleDaemonClient } from "@rindle/daemon-client";
import { createRindleApiServer, defineApiMutators, defineApiQueries, SplitDaemonClient } from "@rindle/api-server";
import { createRindleClient } from "@rindle/optimistic";

import { buildPairBinaries, startPair } from "./spawn-pair.mjs";
const NUM_CLIENTS = 12;
const PAGE_SIZE = 10;
const DAEMON_TOKEN = "fleet-token";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Poll `cond` (sync or async) until truthy or timeout. */
async function waitFor(cond, label, ms = 20_000) {
  const start = Date.now();
  for (;;) {
    if (await cond()) return;
    if (Date.now() - start > ms) throw new Error(`timeout waiting for: ${label}`);
    await sleep(20);
  }
}

await initWasm();

// ---------------------------------------------------------------------------
// The shared fixture: schema, query shapes, and the mutator twins.
// ---------------------------------------------------------------------------

const issue = table("issue")
  .columns({ id: string(), title: string(), status: string(), priority: number(), owner: string(), createdAt: number() })
  .primaryKey("id");
const comment = table("comment")
  .columns({ id: string(), issueId: string(), body: string(), createdAt: number() })
  .primaryKey("id");
const label = table("label").columns({ id: string(), issueId: string(), name: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment, label] });

const COLS = {
  issue: ["id", "title", "status", "priority", "owner", "createdAt"],
  comment: ["id", "issueId", "body", "createdAt"],
  label: ["id", "issueId", "name"],
};

// Six shapes, each a pure function of a query root (works for the client builder, the API
// server builder, AND the oracle store's root). `page` takes a cursor; the rest ignore args.
const shapeBuilders = {
  // flat: a live ordered + limited WINDOW (pagination)
  page: (q, cursor) => {
    const base = q.issue.orderBy("createdAt", "desc").orderBy("id", "asc").limit(PAGE_SIZE);
    return cursor ? base.start({ createdAt: cursor.createdAt, id: cursor.id }, { exclusive: true }) : base;
  },
  // flat: a filter
  highPriority: (q) => q.issue.where.priority(ge(3)).orderBy("id", "asc"),
  // nested children
  withComments: (q) =>
    q.issue
      .sub("comments", comment, { parent: ["id"], child: ["issueId"] }, (c) => c.orderBy("createdAt", "asc").orderBy("id", "asc"))
      .orderBy("id", "asc"),
  // nested children (the transactional invariant's subject)
  withLabels: (q) =>
    q.issue.sub("labels", label, { parent: ["id"], child: ["issueId"] }, (l) => l.orderBy("id", "asc")).orderBy("id", "asc"),
  // EXISTS gate
  commented: (q) => q.issue.where(exists(comment, { parent: ["id"], child: ["issueId"] })).orderBy("id", "asc"),
  // NOT EXISTS gate
  unlabeled: (q) => q.issue.where(notExists(label, { parent: ["id"], child: ["issueId"] })).orderBy("id", "asc"),
};
const SHAPE_KEYS = Object.keys(shapeBuilders);
/** Materialize a named query on a client store (the shared canonical form: `page` at the first window). */
const materializeShape = (store, key) => store.materialize(key === "page" ? namedQueries.page(null) : namedQueries[key]());

const clientQ = newQueryBuilder(schema);
const namedQueries = Object.fromEntries(SHAPE_KEYS.map((k) => [k, defineQuery(k, (args) => shapeBuilders[k](clientQ, args))]));

const serverQ = newQueryBuilder(schema);
const apiQueries = defineApiQueries(Object.fromEntries(SHAPE_KEYS.map((k) => [k, (_ctx, args) => shapeBuilders[k](serverQ, args)])));

// PREDICTED mutators (client-side, instant local prediction).
const mutators = {
  createIssue: (tx, a) => tx.insert("issue", pick(a, COLS.issue)),
  setStatus: (tx, a) => tx.update("issue", { id: a.id, status: a.status }),
  setPriority: (tx, a) => tx.update("issue", { id: a.id, priority: a.priority }),
  setTitle: (tx, a) => tx.update("issue", { id: a.id, title: a.title }),
  addComment: (tx, a) => tx.insert("comment", pick(a, COLS.comment)),
  addLabel: (tx, a) => tx.insert("label", pick(a, COLS.label)),
  removeLabel: (tx, a) => tx.delete("label", { id: a.id }),
  deleteIssue: (tx, a) => tx.delete("issue", { id: a.id }),
  // MULTI-ROW, ONE TRANSACTION: an issue and its two labels, together.
  createTriad: (tx, a) => {
    tx.insert("issue", pick(a, COLS.issue));
    tx.insert("label", { id: `${a.id}-la`, issueId: a.id, name: "alpha" });
    tx.insert("label", { id: `${a.id}-lb`, issueId: a.id, name: "beta" });
  },
};

// AUTHORITATIVE mutators (API server → one SQL transaction on the daemon).
const apiMutators = defineApiMutators({
  createIssue: (tx, a) => {
    if (/\bspam\b/i.test(String(a.title))) throw new Error('the word "spam" is not allowed');
    tx.exec(insertSql("issue"), COLS.issue.map((c) => a[c]));
  },
  setStatus: (tx, a) => tx.exec("UPDATE issue SET status = ? WHERE id = ?", [a.status, a.id]),
  setPriority: (tx, a) => tx.exec("UPDATE issue SET priority = ? WHERE id = ?", [a.priority, a.id]),
  setTitle: (tx, a) => tx.exec("UPDATE issue SET title = ? WHERE id = ?", [a.title, a.id]),
  addComment: (tx, a) => tx.exec(insertSql("comment"), COLS.comment.map((c) => a[c])),
  addLabel: (tx, a) => tx.exec(insertSql("label"), COLS.label.map((c) => a[c])),
  removeLabel: (tx, a) => tx.exec("DELETE FROM label WHERE id = ?", [a.id]),
  deleteIssue: (tx, a) => tx.exec("DELETE FROM issue WHERE id = ?", [a.id]),
  createTriad: (tx, a) => {
    tx.exec(insertSql("issue"), COLS.issue.map((c) => a[c]));
    tx.exec(insertSql("label"), [`${a.id}-la`, a.id, "alpha"]);
    tx.exec(insertSql("label"), [`${a.id}-lb`, a.id, "beta"]);
  },
});

function pick(obj, cols) {
  const out = {};
  for (const c of cols) out[c] = obj[c];
  return out;
}
function insertSql(t) {
  return `INSERT INTO ${t} (${COLS[t].join(", ")}) VALUES (${COLS[t].map(() => "?").join(", ")})`;
}

// ---------------------------------------------------------------------------
// The independent oracle: a local wasm store + a JS model of the authoritative rows.
// Each high-level action drives the fleet (client.mutate.*) AND the oracle in lockstep.
// ---------------------------------------------------------------------------

const oracle = await createWasmStore(schema);
const model = { issue: new Map(), comment: new Map(), label: new Map() };

async function oAdd(t, row) {
  model[t].set(row.id, { ...row });
  await oracle.write((tx) => tx.add(t, row));
}
async function oEdit(t, id, patch) {
  const old = model[t].get(id);
  const next = { ...old, ...patch };
  model[t].set(id, next);
  await oracle.write((tx) => tx.edit(t, old, next));
}
async function oDel(t, id) {
  const old = model[t].get(id);
  if (!old) return;
  model[t].delete(id);
  await oracle.write((tx) => tx.remove(t, old));
}

// High-level actions: each fires the client prediction and mirrors the oracle.
const rotate = (i) => clients[i % clients.length];
async function createIssue(driver, row) {
  driver.mutate.createIssue(row);
  await oAdd("issue", pick(row, COLS.issue));
}
async function createTriad(driver, row) {
  driver.mutate.createTriad(row);
  await oAdd("issue", pick(row, COLS.issue));
  await oAdd("label", { id: `${row.id}-la`, issueId: row.id, name: "alpha" });
  await oAdd("label", { id: `${row.id}-lb`, issueId: row.id, name: "beta" });
}
async function addComment(driver, row) {
  driver.mutate.addComment(row);
  await oAdd("comment", pick(row, COLS.comment));
}
async function addLabel(driver, row) {
  driver.mutate.addLabel(row);
  await oAdd("label", pick(row, COLS.label));
}
async function removeLabel(driver, id) {
  driver.mutate.removeLabel({ id });
  await oDel("label", id);
}
async function setStatus(driver, id, status) {
  driver.mutate.setStatus({ id, status });
  await oEdit("issue", id, { status });
}
async function setPriority(driver, id, priority) {
  driver.mutate.setPriority({ id, priority });
  await oEdit("issue", id, { priority });
}
async function setTitle(driver, id, title) {
  driver.mutate.setTitle({ id, title });
  await oEdit("issue", id, { title });
}
async function deleteIssue(driver, id) {
  driver.mutate.deleteIssue({ id });
  await oDel("issue", id);
}

// ---------------------------------------------------------------------------
// Boot tier 1: the real `rindled` binary.
// ---------------------------------------------------------------------------

buildPairBinaries();

// The ONE topology (design 214): a replicator write-master + a follower rindled. Every seed
// row and every mutation crosses master → change-log fan-out → follower → ws before any
// client sees it.
const pair = await startPair({
  tables: [
    { name: "issue", columns: ["id", "title", "status", "priority", "owner", "createdAt"], pk: [0], types: ["string", "string", "string", "number", "string", "number"] },
    { name: "comment", columns: ["id", "issueId", "body", "createdAt"], pk: [0], types: ["string", "string", "string", "number"] },
    { name: "label", columns: ["id", "issueId", "name"], pk: [0], types: ["string", "string", "string"] },
  ],
  authToken: DAEMON_TOKEN,
  prefix: "fleet-e2e-",
});
const daemonUrl = pair.readUrl;
const daemonWsUrl = pair.wsUrl;
// Writes + mutation sessions → the master; reads/materialize → the follower.
const splitClient = () =>
  new SplitDaemonClient(
    new HttpRindleDaemonClient({ baseUrl: pair.writeUrl }),
    new HttpRindleDaemonClient({ baseUrl: pair.readUrl, headers: { authorization: `Bearer ${DAEMON_TOKEN}` } }),
  );
const control = splitClient();

/** The dedup signal over the wire: live materialization (pipeline) count. */
async function materializations() {
  const res = await fetch(`${daemonUrl}/stats`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${DAEMON_TOKEN}` },
    body: "{}",
  });
  if (!res.ok) throw new Error(`/stats failed: ${res.status}`);
  return (await res.json()).materializations;
}
const waitForMaterializations = (n, label) => waitFor(async () => (await materializations()) === n, label);

// ---------------------------------------------------------------------------
// Seed a baseline corpus (before any client connects) into BOTH the daemon and the oracle.
// ---------------------------------------------------------------------------

const SEED = [];
function seedRow(t, row) {
  model[t].set(row.id, { ...row });
  SEED.push({ table: t, row });
}
for (let i = 0; i < 24; i++) {
  const id = `iss-${String(i).padStart(3, "0")}`;
  seedRow("issue", { id, title: `issue ${i}`, status: i % 3 === 0 ? "done" : "todo", priority: i % 5, owner: `u${i % 4}`, createdAt: 1000 + i });
  if (i % 2 === 0) seedRow("comment", { id: `cm-${id}`, issueId: id, body: `seed comment ${i}`, createdAt: 2000 + i });
  if (i % 4 === 0) seedRow("label", { id: `lb-${id}`, issueId: id, name: "seed" });
}

const seeded = await control.executeSqlTxn({
  idempotencyKey: "fleet-seed",
  statements: SEED.map(({ table: t, row }) => ({ sql: insertSql(t), params: COLS[t].map((c) => row[c]) })),
});
assert.equal(seeded.applied, true, "baseline seed applied");
await oracle.write((tx) => {
  for (const { table: t, row } of SEED) tx.add(t, row);
});

// The oracle's own views, one per shape (recomputed locally on every oracle write).
const oracleViews = {};
for (const key of SHAPE_KEYS) {
  oracleViews[key] = oracle.materialize(key === "page" ? shapeBuilders.page(oracle.query, null) : shapeBuilders[key](oracle.query));
}

// ---------------------------------------------------------------------------
// Boot tier 2: the API server, in-process, pointed at the spawned daemon.
// ---------------------------------------------------------------------------

const api = createRindleApiServer({
  daemon: splitClient(),
  // Authoritative mutations execute over the master's public SQL surface; the split daemon client
  // above still serves leases, materializations and reads.
  database: { url: pair.writeUrl, authToken: pair.sqlAuthToken },
  queries: apiQueries,
  mutators: apiMutators,
  authorizeQuery: ({ user }) => typeof user === "string" && user.length > 0,
  authorizeMutation: ({ user }) => typeof user === "string" && user.length > 0,
});
const apiHttp = createServer((req, res) => {
  void (async () => {
    try {
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
      const userHeader = req.headers["x-user"];
      const context = { user: Array.isArray(userHeader) ? userHeader[0] : userHeader, request: req };
      let out;
      if (req.url === api.routes.query) out = await api.handleQueryJson(body, context);
      else if (req.url === api.routes.mutate) out = await api.handleMutateJson(body, context);
      else throw Object.assign(new Error("not found"), { status: 404 });
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(out));
    } catch (err) {
      res.writeHead(err?.status ?? 500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: String(err?.message ?? err) }));
    }
  })();
});
await new Promise((r) => apiHttp.listen(0, r));
const apiUrl = `http://127.0.0.1:${apiHttp.address().port}`;

// ---------------------------------------------------------------------------
// Boot tier 3: the fleet of optimistic clients.
// ---------------------------------------------------------------------------

const clients = [];
async function makeClient(clientID) {
  const rejections = [];
  const client = await createRindleClient({
    schema,
    mutators,
    api: { url: apiUrl, headers: { "x-user": clientID } },
    daemon: { wsUrl: daemonWsUrl },
    clientID,
    onRejected: (envelope, reason) => rejections.push({ name: envelope.name, reason }),
  });
  return { ...client, rejections, views: {} };
}

for (let i = 0; i < NUM_CLIENTS; i++) clients.push(await makeClient(`fleet-${i}`));

// --- convergence + oracle agreement, every shape ---------------------------
async function convergeAndAssert(label) {
  for (const key of SHAPE_KEYS) {
    await waitFor(
      () => clients.every((c) => isDeepStrictEqual(c.views[key].data, oracleViews[key].data)),
      `${label}: all ${clients.length} clients converge to the oracle on '${key}'`,
    );
  }
  // Cross-client convergence is implied by oracle-equality, but assert it explicitly.
  for (const key of SHAPE_KEYS) {
    const ref = clients[0].views[key].data;
    for (const c of clients) {
      assert(isDeepStrictEqual(c.views[key].data, ref), `${label}: client ${c.clientID} diverged from peers on '${key}'`);
    }
  }
  console.log(`  ✓ ${label}: ${clients.length} clients == oracle across ${SHAPE_KEYS.length} shapes`);
}

// === INVARIANT 1: DEDUP ====================================================
// Baseline: each client eagerly subscribes its OWN lmid system query (one per connection).
await waitForMaterializations(NUM_CLIENTS, `dedup baseline: ${NUM_CLIENTS} per-client lmid system queries`);
console.log(`✓ dedup: ${NUM_CLIENTS} clients ⇒ ${NUM_CLIENTS} lmid pipelines (one per connection)`);

let expectedPipelines = NUM_CLIENTS;
for (const key of SHAPE_KEYS) {
  for (const c of clients) c.views[key] = materializeShape(c.store, key);
  await waitFor(
    () => clients.every((c) => isDeepStrictEqual(c.views[key].data, oracleViews[key].data)),
    `hydrate '${key}' on all clients`,
  );
  expectedPipelines += 1;
  await waitForMaterializations(expectedPipelines, `dedup: ${NUM_CLIENTS} subscribers to '${key}' share ONE pipeline`);
  console.log(`✓ dedup: +${NUM_CLIENTS} subscribers to '${key}' ⇒ +1 pipeline (total ${expectedPipelines})`);
}

// A DISTINCT canonical query (same shape, different args) is its OWN pipeline.
clients[0].store.materialize(namedQueries.page({ createdAt: 1010, id: "iss-010" }));
await waitForMaterializations(expectedPipelines + 1, "a distinct query (different args) is its own pipeline");
expectedPipelines += 1;
console.log("✓ dedup: a distinct query (different args) gets its own pipeline");

await convergeAndAssert("hydrate");

// === INVARIANT 3 (instrumentation): TRANSACTIONAL store updates =============
// Subscribe a checker to every client's nested `withLabels` view. A `createTriad` writes an
// issue + exactly two labels in ONE transaction; if any client ever applies that commit
// non-atomically, a triad issue is momentarily seen with one label. Record every violation.
const txnViolations = [];
const unsubs = clients.map((c) =>
  c.views.withLabels.subscribe((rows) => {
    for (const row of rows) {
      if (typeof row.id === "string" && row.id.startsWith("triad-")) {
        const n = row.labels.length;
        if (n !== 0 && n !== 2) txnViolations.push({ client: c.clientID, id: row.id, labels: n });
      }
    }
  }),
);

// === drive a mutation workload from rotating clients =======================
await createIssue(rotate(0), { id: "new-001", title: "added live", status: "todo", priority: 4, owner: "u1", createdAt: 5001 });
await addComment(rotate(1), { id: "cm-new-001", issueId: "new-001", body: "first comment", createdAt: 6001 });
await addLabel(rotate(2), { id: "lb-new-001", issueId: "new-001", name: "fresh" });
await setStatus(rotate(3), "iss-001", "in-progress");
await setPriority(rotate(4), "iss-002", 5);
await setTitle(rotate(5), "iss-003", "renamed by the fleet");
await createTriad(rotate(6), { id: "triad-001", title: "triad one", status: "todo", priority: 3, owner: "u2", createdAt: 5100 });
await removeLabel(rotate(7), "lb-iss-000");
await deleteIssue(rotate(8), "iss-004");
await convergeAndAssert("round 1 (mixed writes from 9 clients)");

await createTriad(rotate(2), { id: "triad-002", title: "triad two", status: "review", priority: 4, owner: "u3", createdAt: 5200 });
await addComment(rotate(3), { id: "cm-extra", issueId: "iss-006", body: "late note", createdAt: 6100 });
await setStatus(rotate(4), "new-001", "done");
await addLabel(rotate(5), { id: "lb-extra", issueId: "iss-006", name: "triage" });
await convergeAndAssert("round 2");

// === INVARIANT 2: LATE JOINER ==============================================
const beforeLate = await materializations();
const late = await makeClient("fleet-late");
for (const key of SHAPE_KEYS) late.views[key] = materializeShape(late.store, key);
for (const key of SHAPE_KEYS) {
  await waitFor(
    () => isDeepStrictEqual(late.views[key].data, oracleViews[key].data),
    `late joiner hydrates '${key}' to the current snapshot`,
  );
}
const afterLate = await materializations();
assert.equal(
  afterLate,
  beforeLate + 1,
  `late joiner reuses every shared pipeline; only its own lmid query is new (${beforeLate} → ${afterLate})`,
);
clients.push(late);
console.log(`✓ late joiner: hydrated to the live snapshot, reused all shared pipelines (+1 lmid only)`);

// A write AFTER the late join must reach the late joiner too.
await setPriority(rotate(0), "iss-010", 4);
await createTriad(rotate(1), { id: "triad-003", title: "triad three", status: "todo", priority: 5, owner: "u0", createdAt: 5300 });
await convergeAndAssert("round 3 (after late join)");

// === REJECTION (behavioral): a policy-rejected mutation snaps back =========
const rc = clients[0];
const rejectsBefore = rc.rejections.length;
rc.mutate.createIssue({ id: "spam-1", title: "this is spam", status: "todo", priority: 1, owner: "u0", createdAt: 5999 });
await waitFor(() => rc.views.unlabeled.data.some((r) => r.id === "spam-1"), "spam create is predicted locally");
await waitFor(() => rc.rejections.length === rejectsBefore + 1, "rejection reason surfaces");
assert.match(rc.rejections.at(-1).reason, /spam/i);
await waitFor(() => !rc.views.unlabeled.data.some((r) => r.id === "spam-1"), "rejected create snaps back");
for (const c of clients) {
  if (c !== rc) assert(!c.views.unlabeled.data.some((r) => r.id === "spam-1"), `client ${c.clientID} never saw the phantom`);
}
console.log("✓ rejection: phantom predicted on the author, snapped back, never seen by peers");

// The oracle never held the rejected row, so the fleet must still match it.
await convergeAndAssert("after spam snap-back");

// === INVARIANT 3 (verdict): no torn intermediate ever observed =============
for (const u of unsubs) u();
assert.equal(
  txnViolations.length,
  0,
  `transactional store updates: a triad issue was observed with a partial label set: ${JSON.stringify(txnViolations.slice(0, 5))}`,
);
console.log(`✓ transactional: no torn multi-row commit observed across ${clients.length} clients`);

// ---------------------------------------------------------------------------
// Teardown.
// ---------------------------------------------------------------------------
for (const c of clients) c.close();
await new Promise((r) => apiHttp.close(r));
pair.cleanup();

console.log(`\n✅ fleet e2e: ${clients.length} optimistic clients × ${SHAPE_KEYS.length} query shapes through the API server + the replicator pair`);
console.log("✅ dedup, late-joiner snapshot, and transactional store updates all hold; fleet == independent oracle throughout");
