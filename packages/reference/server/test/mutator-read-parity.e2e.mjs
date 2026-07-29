// Client-vs-server READ PARITY, the real three-tier way (the (c) test) — run over BOTH masters.
//
// Seeds ONE authoritative database, lets a real optimistic client HYDRATE its wasm engine from
// the serving daemon over the wire (so the two DBs are identical BY CONSTRUCTION, not
// hand-matched), then runs the SAME isomorphic shared mutator body on BOTH tiers via
// `IsoTx.query`. The client executes the battery synchronously against its wasm IVM engine; the
// API server executes it against SQLite (compiled by `@rindle/query-compiler`, ridden through
// the interactive mutation session). Each tier stashes what every `tx.query` returned, and we
// assert the reads are BYTE-IDENTICAL — the exact "reads in the API server's mutators match
// reads on the client, on identical DBs" claim, focused on the 203 §9.2.1 divergence risks
// (orderBy tie-breaks / PK-append, NULL ordering, limit, keyset paging).
//
// Three phases per topology: COMMITTED (reads over the settled fixture), READ-YOUR-WRITES
// (begin-prefix writes observed by later reads), and INTERLEAVED (write→read→write→read — the
// only phase where a read must observe a write flushed over `/mutate-session/exec` into the
// already-open session; a wire spy proves the route was traversed).
//
// ONE topology (design 214): a `rindle-replicator` write-master + a follower `rindled` — the
// api-server runs a `SplitDaemonClient` (writes + mutation sessions → the replicator; reads →
// the follower), and the client hydrates from the FOLLOWER. Every fixture row therefore
// crosses master → change-log fan-out → follower → ws before the client sees it, and the
// read-your-writes phase holds a REAL interactive session on the replicator. (The solo
// standalone-rindled lane was retired with the shape — 214 W3.)
//
// All processes are real spawned binaries; the API server and the client run in THIS process
// but over the real protocol. Only the OS-process fan-out of the api tier is elided (it does
// not affect the reads, and in-process capture keeps the assertion a plain deepEqual).
//
//   node --conditions=@rindle/source test/mutator-read-parity.e2e.mjs
//   (or: pnpm --filter @rindle/server test:rust-daemon)

import assert from "node:assert/strict";
import { createServer } from "node:http";

import {
  createRindleApiServer,
  defineApiMutators,
  defineApiQueries,
  runSharedMutation,
} from "@rindle/api-server";
import { SplitDaemonClient } from "@rindle/api-server";
import { createSchema, defineQuery, newQueryBuilder, number, string, table } from "@rindle/client";
import { HttpRindleDaemonClient } from "@rindle/daemon-client";
import { createRindleClient } from "@rindle/optimistic";
import { WsTransport } from "@rindle/remote";

import { buildPairBinaries, startPair } from "./spawn-pair.mjs";

// ----------------------------------------------------------------- shared schema + fixture

const issue = table("issue")
  .columns({ id: number(), owner: string(), priority: number().nullable(), title: string() })
  .primaryKey("id");
// A tiny marker table so the read battery ends with a write (a clean lmid-advancing commit),
// keeping the reads themselves side-effect-free.
const probe = table("probe").columns({ id: number() }).primaryKey("id");
const schema = createSchema({ tables: [issue, probe] });

const q = newQueryBuilder(schema);

// Designed to STRESS the divergence risks: priority ties (ids 1/4/8 at 2, ids 2/7 at 1),
// NULL priorities (ids 3/5/9), and a title tie ("alpha" on ids 2/7) — every one of which
// forces an orderBy tie-break that only matches if the SQL compiler appends the PK exactly
// like the wasm engine does, and a NULL-ordering choice that only matches if the compiler
// emits the same NULLS FIRST/LAST the engine implies.
const FIXTURE = [
  { id: 1, owner: "alice", priority: 2, title: "gamma" },
  { id: 2, owner: "bob", priority: 1, title: "alpha" },
  { id: 3, owner: "alice", priority: null, title: "delta" },
  { id: 4, owner: "carol", priority: 2, title: "beta" },
  { id: 5, owner: "bob", priority: null, title: "epsilon" },
  { id: 6, owner: "alice", priority: 3, title: "zeta" },
  { id: 7, owner: "carol", priority: 1, title: "alpha" },
  { id: 8, owner: "bob", priority: 2, title: "theta" },
  { id: 9, owner: "alice", priority: null, title: "iota" },
  { id: 10, owner: "carol", priority: 4, title: "kappa" },
];

// The read battery: each entry is a full query the shared body runs via `tx.query`. Built once
// from the tier-agnostic `newQueryBuilder` and shared verbatim by both tiers.
const BATTERY = {
  allById: q.issue.orderBy("id", "asc"),
  byPriorityAsc: q.issue.orderBy("priority", "asc"), // NULLs + priority ties → tie-break + null order
  byPriorityDesc: q.issue.orderBy("priority", "desc"),
  byTitleAsc: q.issue.orderBy("title", "asc"), // "alpha" tie on ids 2/7 → PK tie-break
  ownerAliceById: q.issue.where.owner("alice").orderBy("id", "asc"), // string equality filter
  ownerAliceByPriority: q.issue.where.owner("alice").orderBy("priority", "asc"), // filter + null order
  priorityIsOne: q.issue.where.priority(1).orderBy("id", "asc"), // number equality filter
  top3ByPriority: q.issue.orderBy("priority", "desc").limit(3), // limit
  first4ById: q.issue.orderBy("id", "asc").limit(4),
  pageAfter4: q.issue.orderBy("id", "asc").start({ id: 4 }, { exclusive: true }).limit(3), // keyset paging
};
const BATTERY_NAMES = Object.keys(BATTERY);

// The ONE isomorphic body, parameterized only by WHERE it stashes each read (a test-harness
// concern — the mutator logic, the `tx.query(...)` calls, are byte-identical on both tiers).
// It ends with a fixed marker write so the mutation commits (advances lmid) cleanly.
const makeBattery = (sink) =>
  function* battery(tx) {
    for (const name of BATTERY_NAMES) {
      sink[name] = yield tx.query(BATTERY[name]);
    }
    yield tx.insert("probe", { id: 1 });
  };

// READ-YOUR-WRITES battery: WRITE (insert + update + delete) FIRST, then read queries that MUST
// observe those still-uncommitted writes — on the client via the staged wasm read-cache fork, on the
// server via the interactive mutation session (DAEMON-INTERACTIVE-TXN §5; on the split topology the
// session holds the REPLICATOR's write transaction). If either tier fell back to committed state,
// the reads would diverge and the comparison below would catch it.
const RYW_NAMES = ["afterInsert", "afterUpdate", "afterDelete", "reordered"];
const makeRywBattery = (sink) =>
  function* rywBattery(tx) {
    yield tx.insert("issue", { id: 100, owner: "zed", priority: 5, title: "inserted" });
    yield tx.update("issue", { id: 1, priority: 9 }); // was priority 2 → 9 (other cols stay)
    yield tx.delete("issue", { id: 2 }); // one of the two priority-1 rows (id 7 remains)
    sink.afterInsert = yield tx.query(q.issue.where.owner("zed").orderBy("id", "asc")); // sees id 100
    sink.afterUpdate = yield tx.query(q.issue.where.priority(9).orderBy("id", "asc")); // sees id 1 @ 9
    sink.afterDelete = yield tx.query(q.issue.where.priority(1).orderBy("id", "asc")); // id 2 gone → [7]
    sink.reordered = yield tx.query(q.issue.orderBy("priority", "desc").limit(4)); // reflects all three
  };

// INTERLEAVED battery: write → read → write → read → … . Every write BEFORE the first read
// rides the session `begin` as its prefix (that's all the RYW battery ever exercises); the
// writes AFTER a read land while the session is already open, so on the server they buffer in
// `DaemonLazyTx` and flush over `/mutate-session/exec` right before the NEXT read — the one
// interactive shape where a read must observe an EXEC-shipped write, not a begin-prefix write.
// On the split topology those execs mutate the REPLICATOR's held-open write transaction. The
// client drives the same generator against its staged wasm fork, and the sinks must match.
const INTERLEAVED_NAMES = ["afterFirstInsert", "afterMidUpdate", "afterMidDelete", "final"];
const makeInterleavedBattery = (sink) =>
  function* interleavedBattery(tx) {
    yield tx.insert("issue", { id: 200, owner: "yuri", priority: 0, title: "first" });
    sink.afterFirstInsert = yield tx.query(q.issue.where.owner("yuri").orderBy("id", "asc")); // begin-prefix write
    yield tx.update("issue", { id: 200, priority: 7 }); // exec-after-read round 1
    sink.afterMidUpdate = yield tx.query(q.issue.where.priority(7).orderBy("id", "asc")); // sees 200 @ 7
    yield tx.delete("issue", { id: 200 }); // exec-after-read round 2 (two ops in one flush)
    yield tx.insert("issue", { id: 201, owner: "yuri", priority: 8, title: "second" });
    sink.afterMidDelete = yield tx.query(q.issue.where.owner("yuri").orderBy("id", "asc")); // 200 gone → [201]
    sink.final = yield tx.query(q.issue.orderBy("priority", "desc").limit(4)); // reflects every round
  };

const allIssues = defineQuery("allIssues", () => q.issue.orderBy("id", "asc"));
const apiQueries = defineApiQueries({ allIssues: () => q.issue.orderBy("id", "asc") });

// ----------------------------------------------------------------- process harness

// The fixture schema in the replicator's TableSpec flavor (ColType `number` renders REAL),
// minted to the follower via the master's genesis `ddl` entry.
const REPLICATOR_TABLES = [
  { name: "issue", columns: ["id", "owner", "priority", "title"], pk: [0], types: ["number", "string", "number", "string"] },
  { name: "probe", columns: ["id"], pk: [0], types: ["number"] },
];

/// The one topology (spawn-pair.mjs): `rindle-replicator` write-master + a BARE follower
/// `rindled`. Writes AND mutation sessions go to the master; reads (and the client's whole
/// sync plane) come off the follower.
async function startSplit() {
  const pair = await startPair({
    tables: REPLICATOR_TABLES,
    authToken: "daemon-secret",
    prefix: "rindled-read-parity-split-",
  });
  return {
    label: "split replicator-master + follower",
    daemon: new SplitDaemonClient(
      // writes + interactive mutation sessions → the replicator write-master
      new HttpRindleDaemonClient({ baseUrl: pair.writeUrl }),
      // reads → the follower (the client also hydrates from it below)
      new HttpRindleDaemonClient({
        baseUrl: pair.readUrl,
        headers: { authorization: "Bearer daemon-secret" },
      }),
    ),
    clientWs: pair.wsUrl,
    // Authoritative mutations execute over the master's PUBLIC SQL surface; the split daemon
    // client above still serves reads, leases and materializations.
    database: { url: pair.writeUrl, authToken: pair.sqlAuthToken },
    cleanup: pair.cleanup,
  };
}

// ----------------------------------------------------------------- one parity run

async function runParity(topo) {
  console.log(`\n=== read parity over: ${topo.label} ===`);
  const clientReads = {};
  const serverReads = {};
  const clientRyw = {};
  const serverRyw = {};
  const clientInterleaved = {};
  const serverInterleaved = {};

  const clientMutators = {
    createIssue: (tx, args) =>
      tx.insert("issue", { id: args.id, owner: args.owner, priority: args.priority, title: args.title }),
    battery: makeBattery(clientReads),
    rywBattery: makeRywBattery(clientRyw),
    interleavedBattery: makeInterleavedBattery(clientInterleaved),
  };
  const apiMutators = defineApiMutators({
    createIssue: async (tx, args) => {
      await tx.insert("issue", { id: args.id, owner: args.owner, priority: args.priority, title: args.title });
    },
    battery: (tx, args, ctx) => runSharedMutation(makeBattery(serverReads), args, { user: String(ctx.user) }, tx),
    rywBattery: (tx, args, ctx) => runSharedMutation(makeRywBattery(serverRyw), args, { user: String(ctx.user) }, tx),
    interleavedBattery: (tx, args, ctx) =>
      runSharedMutation(makeInterleavedBattery(serverInterleaved), args, { user: String(ctx.user) }, tx),
  });

  // Wire spy: count mid-session write flushes — POSTs to
  // `/v1/sql/mutations/transactions/:id/execute`, the exec route of an ALREADY-OPEN mutation
  // transaction. The interleaved phase asserts the count ROSE BEFORE its reads returned, which is
  // wire-level proof the route was genuinely traversed, so the phase cannot silently degrade to
  // begin-prefix-only coverage if the api-server's buffering ever changes. (Begin and commit use
  // different paths, so neither inflates this counter.)
  let sessionExecs = 0;
  const countingFetch = (input, init) => {
    if (/\/v1\/sql\/mutations\/transactions\/[^/]+\/execute$/.test(new URL(input).pathname)) {
      sessionExecs += 1;
    }
    return fetch(input, init);
  };

  let apiHttp;
  let client;
  try {
    const api = createRindleApiServer({
      daemon: topo.daemon,
      database: { ...topo.database, fetch: countingFetch },
      schema, // needed so the shared body's logical tx.insert/tx.query render to dialect SQL
      queries: apiQueries,
      mutators: apiMutators,
      authorizeQuery: ({ user }) => Boolean(user),
      authorizeMutation: ({ user, envelope }) => user === envelope.clientID,
    });

    apiHttp = createServer((req, res) => {
      void (async () => {
        try {
          const chunks = [];
          for await (const chunk of req) chunks.push(chunk);
          const body = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
          const context = { user: req.headers["x-user"], request: req };
          let out;
          if (req.url === api.routes.query) out = await api.handleQueryJson(body, context);
          else if (req.url === api.routes.mutate) out = await api.handleMutateJson(body, context);
          else throw Object.assign(new Error("not found"), { status: 404 });
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify(out));
        } catch (err) {
          res.writeHead(err.status ?? 500, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: String(err?.message ?? err) }));
        }
      })();
    });
    await new Promise((r) => apiHttp.listen(0, r));
    const apiUrl = `http://127.0.0.1:${apiHttp.address().port}`;

    // ------------------------------------------------------------- tier 3: the real client

    const clientID = "client-1";
    client = await createRindleClient({
      schema,
      mutators: clientMutators,
      api: { url: apiUrl, headers: { "x-user": clientID } },
      daemon: { transport: new WsTransport(topo.clientWs) },
      clientID,
    });

    const waitFor = (cond, label, ms = 20_000) =>
      new Promise((res, rej) => {
        const start = Date.now();
        const tick = () => {
          if (cond()) return res();
          if (Date.now() - start > ms) return rej(new Error(`[${topo.label}] timeout waiting for: ${label}`));
          setTimeout(tick, 10);
        };
        tick();
      });

    // Hold a live subscription over ALL issues so the client's engine fully syncs the table
    // from its daemon — this is what makes "identical DBs": the client knows only what it
    // learned over the wire, and it learns the whole fixture (on the split topology each row
    // additionally crossed master → change log → follower first).
    const view = client.store.materialize(allIssues());

    // Seed authoritatively via the client's own mutators (the real write path): each create
    // predicts locally, ships to the API server, lands in the master, and confirms back.
    for (const row of FIXTURE) client.mutate.createIssue(row);
    await waitFor(() => client.backend.__inspect().pending.length === 0, "all seed writes confirmed");
    await waitFor(() => view.data.length === FIXTURE.length, "client hydrated the full fixture");

    // Run the SAME battery on both tiers. `client.mutate.battery` predicts synchronously (fills
    // clientReads against the wasm engine), then ships to the API server, which runs it
    // authoritatively against the master's SQLite (fills serverReads).
    client.mutate.battery({});
    await waitFor(() => BATTERY_NAMES.every((n) => serverReads[n] !== undefined), "API server ran the battery");
    await waitFor(() => client.backend.__inspect().pending.length === 0, "battery confirmed");

    // ------------------------------------------------------------- the assertion

    // deepStrictEqual on the arrays is ORDER-SENSITIVE — exactly what we want: row values AND row
    // order (the tie-break / null-ordering surface) must match byte-for-byte across the two tiers.
    const compareReads = (label, names, clientSink, serverSink) => {
      let mismatches = 0;
      for (const name of names) {
        try {
          assert.deepStrictEqual(clientSink[name], serverSink[name]);
          console.log(`  ✓ [${label}] ${name} — ${clientSink[name].length} rows, identical on both tiers`);
        } catch {
          mismatches++;
          console.error(`  ✗ [${label}] ${name} — MISMATCH between client (wasm IVM) and server SQLite:`);
          console.error(`      client: ${JSON.stringify(clientSink[name])}`);
          console.error(`      server: ${JSON.stringify(serverSink[name])}`);
        }
      }
      assert.equal(mismatches, 0, `[${topo.label}] [${label}] ${mismatches} of ${names.length} queries diverged`);
    };

    // Phase 1 — COMMITTED reads over the identical seeded fixture. Sanity-guard against a vacuous
    // match (both empty) by asserting the client genuinely read the whole fixture.
    assert.equal(clientReads.allById.length, FIXTURE.length, "client read the full fixture");
    assert.equal(serverReads.allById.length, FIXTURE.length, "server read the full fixture");
    compareReads("committed", BATTERY_NAMES, clientReads, serverReads);

    // Phase 2 — READ-YOUR-WRITES. A body that writes (insert+update+delete) then reads its own
    // uncommitted writes on both tiers — on the split topology this is the interactive session
    // holding the REPLICATOR's write transaction open across the round trips. Runs AFTER phase 1
    // settled, so it's the only pending mutation (one clean run, no rebase noise).
    client.mutate.rywBattery({});
    await waitFor(() => RYW_NAMES.every((n) => serverRyw[n] !== undefined), "API server ran the RYW battery");
    await waitFor(() => client.backend.__inspect().pending.length === 0, "RYW battery confirmed");
    // Sanity: the writes were actually observed (non-vacuous) — the insert and the delete are visible.
    assert.equal(clientRyw.afterInsert.length, 1, "client saw its own insert");
    assert.equal(serverRyw.afterInsert.length, 1, "server saw its own insert");
    assert.equal(clientRyw.afterDelete.length, 1, "client saw its own delete (one priority-1 row remains)");
    compareReads("read-your-writes", RYW_NAMES, clientRyw, serverRyw);

    // Phase 3 — INTERLEAVED write/read/write/read. The only phase where a read must observe a
    // write shipped over the mutation transaction's execute route into the ALREADY-OPEN session (phases 1–2 only
    // ever read begin-prefix writes; their exec traffic is the commit-time flush no read sees).
    const execsBeforeInterleave = sessionExecs;
    client.mutate.interleavedBattery({});
    await waitFor(
      () => INTERLEAVED_NAMES.every((n) => serverInterleaved[n] !== undefined),
      "API server ran the interleaved battery",
    );
    await waitFor(() => client.backend.__inspect().pending.length === 0, "interleaved battery confirmed");
    assert.ok(
      sessionExecs > execsBeforeInterleave,
      "the interleaved battery must flush mid-session writes over the mutation transaction's execute route",
    );
    // Non-vacuity: the mid-session update and the delete+insert round were genuinely observed.
    assert.equal(clientInterleaved.afterMidUpdate.length, 1, "client read observed its exec-shipped update");
    assert.equal(serverInterleaved.afterMidUpdate.length, 1, "server read observed its exec-shipped update");
    assert.equal(clientInterleaved.afterMidDelete.length, 1, "client read reflects the mid-session delete + insert");
    compareReads("interleaved", INTERLEAVED_NAMES, clientInterleaved, serverInterleaved);

    console.log(
      `[${topo.label}] read-parity passed: ${BATTERY_NAMES.length} committed + ${RYW_NAMES.length} read-your-writes + ${INTERLEAVED_NAMES.length} interleaved queries, client tx.query == server tx.query`,
    );
  } finally {
    try {
      client?.close();
    } catch {
      /* ignore */
    }
    apiHttp?.close();
  }
}

// ----------------------------------------------------------------- main

buildPairBinaries();

const topo = await startSplit();
try {
  await runParity(topo);
} finally {
  topo.cleanup();
}
console.log("\nmutator read-parity passed (replicator master + follower)");
