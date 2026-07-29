// The one-topology synced app from RINDLE-SERVER-DESIGN.md + design 214, running as REAL
// tiers over real wires:
//
//   pair        the Rust `rindle-replicator` write-master (all writes: mutations, rejections)
//               plus a follower `rindled` — public ws (lease subscriptions) + private HTTP
//               read plane (bearer-auth'd). Knows no query names, no app policy. Booted
//               externally by rust-daemon.mjs; this file talks to both over their real wires.
//   API server  createRindleApiServer behind a real node:http front — authenticates the
//               caller, resolves named queries to ASTs, runs named mutators to approved SQL,
//               and talks to the pair privately via SplitDaemonClient (writes → the master,
//               reads → the follower).
//   clients     RemoteOptimisticSource over WsTransport to the daemon; queries materialize
//               through the API server (opaque lease back), mutations flush through the
//               client-side queue (confirmed, in-order batches — the serverless hop is
//               unordered, the daemon's lmid discipline requires contiguous mids).
//
// Run from packages/server/ (boots `rindled` and runs this file against it):
//   node --conditions=@rindle/source examples/serverless-local-first/rust-daemon.mjs

import assert from "node:assert/strict";
import { createServer } from "node:http";
import { isDeepStrictEqual } from "node:util";

import { createRindleApiServer, defineApiMutators, defineApiQueries, SplitDaemonClient } from "@rindle/api-server";
import { createSchema, defineQuery, newQueryBuilder, number, string, table } from "@rindle/client";
import { HttpRindleDaemonClient } from "@rindle/daemon-client";
import { createRindleClient } from "@rindle/optimistic";
import { WsTransport } from "@rindle/remote";

// ----- shared schema (the one artifact both sides import in a real app) -----

const issue = table("issue").columns({ id: number(), title: string(), score: number() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });

// ----- client side: named queries + PREDICTED mutators (keyed rows: named columns,
// schema-checked at runtime — a typo'd column throws with the valid names listed) -----

const q = newQueryBuilder(schema);
const issueQueries = {
  allIssues: defineQuery("allIssues", () => q.issue.orderBy("id", "asc")),
};

const clientMutators = {
  createIssue: (tx, args) => tx.insert("issue", { id: args.id, title: args.title, score: args.score }),
  bumpScore: (tx, args) => {
    const cur = tx.row("issue", { id: args.id });
    if (!cur) return;
    tx.update("issue", { id: args.id, score: cur.score + args.delta });
  },
  forbidden: (tx, args) => tx.insert("issue", { id: args.id, title: "phantom", score: 0 }),
};

// ----- API server: the app authority (AUTHORITATIVE queries + mutators) -----

const serverQ = newQueryBuilder(schema);
const apiQueries = defineApiQueries({
  allIssues: () => serverQ.issue.orderBy("id", "asc"),
});

const apiMutators = defineApiMutators({
  createIssue: (tx, args) => {
    tx.exec("INSERT INTO issue (id, title, score) VALUES (?, ?, ?)", [args.id, args.title, args.score]);
  },
  bumpScore: (tx, args) => {
    tx.exec("UPDATE issue SET score = score + ? WHERE id = ?", [args.delta, args.id]);
  },
  forbidden: () => {
    throw new Error("rejected by policy");
  },
});

// ----- tier 1: the always-up pair (like Postgres) — the Rust write-master + follower.
// This example drives an EXTERNAL pair over its real wires: `rust-daemon.mjs` boots both and
// sets RINDLE_DAEMON_URL/_WS/_TOKEN (the follower) + RINDLE_REPLICATOR_URL (the master's
// write ingress), then runs this file unchanged. (Run it that way, not directly.) -----

const DAEMON_TOKEN = process.env.RINDLE_DAEMON_TOKEN ?? "daemon-secret";
if (!process.env.RINDLE_DAEMON_URL || !process.env.RINDLE_REPLICATOR_URL) {
  throw new Error(
    "set RINDLE_DAEMON_URL (and _WS/_TOKEN) to a running follower and RINDLE_REPLICATOR_URL " +
      "to its write-master, or launch this example via " +
      "`node --conditions=@rindle/source examples/serverless-local-first/rust-daemon.mjs`",
  );
}
const daemon = {
  url: process.env.RINDLE_DAEMON_URL,
  wsUrl: process.env.RINDLE_DAEMON_WS ?? process.env.RINDLE_DAEMON_URL.replace(/^http/, "ws"),
  close: async () => {},
};

// ----- tier 2: the (serverless-shaped) API server -----

const api = createRindleApiServer({
  // Writes (mutations, rejections) → the master; reads (named queries) → the follower.
  daemon: new SplitDaemonClient(
    new HttpRindleDaemonClient({ baseUrl: process.env.RINDLE_REPLICATOR_URL }),
    new HttpRindleDaemonClient({
      baseUrl: daemon.url,
      headers: { authorization: `Bearer ${DAEMON_TOKEN}` },
    }),
  ),
  // Authoritative mutations execute over the master's PUBLIC SQL surface — same listener as
  // RINDLE_REPLICATOR_URL, but its own credential (the replicator refuses an equal pair).
  database: {
    url: process.env.RINDLE_REPLICATOR_URL,
    authToken: process.env.RINDLE_DATABASE_TOKEN ?? "sql-secret",
  },
  queries: apiQueries,
  mutators: apiMutators,
  authorizeQuery: ({ user }) => Boolean(user),
  authorizeMutation: ({ user, envelope }) => user === envelope.clientID,
});

const apiHttp = createServer((req, res) => {
  void (async () => {
    try {
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
      const user = req.headers["x-user"]; // demo auth: a real app verifies a session/JWT here
      const context = { user, request: req };
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
await new Promise((resolve) => apiHttp.listen(0, resolve));
const apiUrl = `http://127.0.0.1:${apiHttp.address().port}`;

// ----- tier 3: web clients — `createRindleClient` is the whole wire-up: wasm init,
// ws transport, lease resolution through the API server, and the mutation queue
// (confirmed in-order batches over the unordered serverless hop) -----

/** Thin observation wrapper so the demo can assert what actually crossed the public wire. */
class RecordingTransport {
  constructor(url) {
    this.inner = new WsTransport(url);
    this.sent = [];
  }
  send(msg) {
    this.sent.push(msg);
    this.inner.send(msg);
  }
  onMessage(handler) {
    this.inner.onMessage(handler);
  }
  close() {
    this.inner.close();
  }
}

const makeClient = async (clientID) => {
  const transport = new RecordingTransport(daemon.wsUrl);
  const rejections = [];
  const app = await createRindleClient({
    schema,
    mutators: clientMutators,
    api: { url: apiUrl, headers: { "x-user": clientID } }, // demo auth; a real app sends a session/JWT
    daemon: { transport },
    clientID,
    onRejected: (envelope, reason) => rejections.push({ name: envelope.name, reason }),
  });
  return { transport, rejections, ...app };
};

function waitFor(cond, label, ms = 5000) {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = () => {
      if (cond()) return resolve();
      if (Date.now() - start > ms) return reject(new Error(`timeout waiting for: ${label}`));
      setTimeout(tick, 10);
    };
    tick();
  });
}

const rows = (view) => view.data.map((r) => [r.id, r.title, r.score]);

const c1 = await makeClient("client-1");
const c2 = await makeClient("client-2");
const v1 = c1.store.materialize(issueQueries.allIssues());
const v2 = c2.store.materialize(issueQueries.allIssues());

// The public wire carries lease tokens, never names or ASTs (the lmid system query is the
// one reserved name — it's wire contract, not an app query).
await waitFor(
  () => c1.transport.sent.some((m) => m.t === "subscribe" && "leaseToken" in m),
  "client 1 subscribed with a lease",
);
for (const msg of c1.transport.sent.filter((m) => m.t === "subscribe" && "leaseToken" in m)) {
  assert.equal("name" in msg, false, "lease subscribes carry no query name");
}

// Two back-to-back mutations WITHOUT awaiting: the queue serializes them into confirmed,
// in-order batches, so the daemon's contiguous-mid check holds over the HTTP hop.
c1.mutate.createIssue({ id: 1, title: "ship serverless", score: 10 });
c1.mutate.bumpScore({ id: 1, delta: 5 });
assert.deepEqual(rows(v1), [[1, "ship serverless", 15]], "both predictions applied immediately");

await waitFor(() => !c1.backend.pending(1), "both mutations confirmed");
await waitFor(() => isDeepStrictEqual(rows(v2), [[1, "ship serverless", 15]]), "client 2 hydrated the writes");

// A policy rejection: the daemon advances lmid with no effects (processed-as-no-op), the
// prediction snaps back on the lmid release, and the REASON arrives through the API
// server's response into the queue's onRejected callback.
c1.mutate.forbidden({ id: 999 });
assert.equal(v1.data.length, 2, "rejected row is visible only as a local prediction");
await waitFor(() => v1.data.length === 1, "rejection snapped back");
await waitFor(() => c1.rejections.length === 1, "rejection reason surfaced");
assert.equal(c1.rejections[0].name, "forbidden");
assert.match(c1.rejections[0].reason, /rejected by policy/);
assert.deepEqual(rows(v2), [[1, "ship serverless", 15]], "other clients never saw the rejected prediction");

// Continue mutating after a rejection: the lmid watermark stayed contiguous.
c1.mutate.bumpScore({ id: 1, delta: 1 });
await waitFor(() => isDeepStrictEqual(rows(v2), [[1, "ship serverless", 16]]), "post-rejection write flowed");

c1.transport.close();
c2.transport.close();
apiHttp.close();
await daemon.close();

console.log("serverless local-first example passed");
console.log(`public subscribes (all lease-based): ${c1.transport.sent.filter((m) => m.t === "subscribe" && "leaseToken" in m).length}`);
console.log(`rejections surfaced with reasons: ${c1.rejections.length}`);
