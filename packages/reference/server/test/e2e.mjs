// Stage B+C integration e2e: a real ws server (@rindle/server, replica-backed) + a RemoteBackend
// client (@rindle/remote) driving the SAME Store/ArrayView. Two parts:
//   1. happy path — the remote view must converge to a local wasm store's view (the oracle)
//      across hydrate + writes (proving the protocol + server + replica == the local engine);
//   2. gap → re-hydrate — a transport that DROPS one batch forces a seq gap; the RemoteBackend
//      must detect it, re-subscribe under a new epoch, and recover to the correct state.
// Run with `--conditions=@rindle/source`; needs the replica native addon + the wasm pkg built.
import assert from "node:assert/strict";
import { isDeepStrictEqual } from "node:util";

import { defineQuery, table, string, number, createSchema, newQueryBuilder } from "@rindle/client";
import { createReplicaServer } from "@rindle/server";
import { createRemoteStore, WsTransport } from "@rindle/remote";
import { createWasmStore } from "@rindle/wasm";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const comment = table("comment").columns({ id: number(), issueID: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });

const apply = (tx, ops) => {
  for (const o of ops) {
    if (o.k === "add") tx.add(o.t, o.r);
    else if (o.k === "remove") tx.remove(o.t, o.r);
    else tx.edit(o.t, o.old, o.r);
  }
};

/** Resolve once `view.data` deep-equals `expected()` (re-checked on every emit). */
function waitForEqual(view, expected, label, ms = 3000) {
  return new Promise((resolve, reject) => {
    let unsub = () => {};
    const timer = setTimeout(() => {
      unsub();
      reject(new Error(`timeout waiting for "${label}": got ${JSON.stringify(view.data)} want ${JSON.stringify(expected())}`));
    }, ms);
    const check = () => {
      if (isDeepStrictEqual(view.data, expected())) {
        clearTimeout(timer);
        unsub();
        resolve();
      }
    };
    unsub = view.subscribe(check); // fires immediately, then on each applied batch
  });
}

const build = (root) =>
  root.issue
    .sub("comments", comment, { parent: ["id"], child: ["issueID"] }, (c) => c.orderBy("id", "asc"))
    .orderBy("id", "asc");

const clientQ = newQueryBuilder(schema);
const clientQueries = {
  issuesWithComments: defineQuery("issuesWithComments", () => build(clientQ)),
  allIssues: defineQuery("allIssues", () => clientQ.issue.orderBy("id", "asc")),
};
const serverQ = newQueryBuilder(schema);
const serverQueries = {
  issuesWithComments: () => build(serverQ),
  allIssues: () => serverQ.issue.orderBy("id", "asc"),
};

// ----------------------------- Part 1: happy path -----------------------------
{
  const server = await createReplicaServer({ schema, queries: serverQueries });
  const transport = new WsTransport(`ws://127.0.0.1:${server.port}`);
  const remote = createRemoteStore(schema, transport);
  const wasm = await createWasmStore(schema); // the oracle

  const rv = remote.materialize(clientQueries.issuesWithComments());
  const wv = build(wasm.query).materialize();

  await waitForEqual(rv, () => wv.data, "empty hydrate");
  console.log("✓ empty hydrate (remote view == wasm oracle)");

  let n = 0;
  const step = async (label, ops) => {
    wasm.write((tx) => apply(tx, ops)); // sync oracle
    await remote.write((tx) => apply(tx, ops)); // async over ws
    await waitForEqual(rv, () => wv.data, label);
    console.log(`✓ ${label} (step ${++n})`);
  };

  await step("seed issues + comments", [
    { k: "add", t: "issue", r: { id: 1, title: "first" } },
    { k: "add", t: "issue", r: { id: 2, title: "second" } },
    { k: "add", t: "comment", r: { id: 10, issueID: 1, body: "a" } },
    { k: "add", t: "comment", r: { id: 11, issueID: 1, body: "b" } },
  ]);
  await step("child add", [{ k: "add", t: "comment", r: { id: 12, issueID: 1, body: "c" } }]);
  await step("child edit", [
    { k: "edit", t: "comment", old: { id: 10, issueID: 1, body: "a" }, r: { id: 10, issueID: 1, body: "A" } },
  ]);
  await step("child remove", [{ k: "remove", t: "comment", r: { id: 11, issueID: 1, body: "b" } }]);
  await step("root add + mixed", [
    { k: "add", t: "issue", r: { id: 3, title: "third" } },
    { k: "add", t: "comment", r: { id: 30, issueID: 3, body: "n" } },
  ]);
  await step("root remove", [{ k: "remove", t: "issue", r: { id: 2, title: "second" } }]);

  transport.close();
  await server.close();
  console.log("✅ Part 1: remote (replica server) == wasm oracle over hydrate + 6 steps\n");
}

// ----------------------------- Part 2: gap → re-hydrate -----------------------------
{
  // A transport that drops the FIRST incremental batch (seq 1) once, simulating packet loss.
  class DroppingTransport {
    constructor(url, predicate) {
      this.inner = new WsTransport(url);
      this.predicate = predicate;
      this.handler = () => {};
      this.dropped = 0;
      this.inner.onMessage((m) => {
        if (this.predicate(m)) {
          this.predicate = () => false; // drop once
          this.dropped++;
          return;
        }
        this.handler(m);
      });
    }
    send(m) {
      this.inner.send(m);
    }
    onMessage(h) {
      this.handler = h;
    }
    close() {
      this.inner.close();
    }
  }

  const server = await createReplicaServer({ schema, queries: serverQueries });
  const transport = new DroppingTransport(`ws://127.0.0.1:${server.port}`, (m) => m.t === "batch" && m.batch.seq === 1);
  const remote = createRemoteStore(schema, transport);
  const wasm = await createWasmStore(schema);

  const rv = remote.materialize(clientQueries.allIssues());
  const wv = wasm.query.issue.orderBy("id", "asc").materialize();

  await waitForEqual(rv, () => wv.data, "re-hydrate: empty hydrate");

  // Write A → its batch (seq 1) is DROPPED → the remote view stalls (does not see A).
  wasm.write((tx) => tx.add("issue", { id: 1, title: "A" }));
  await remote.write((tx) => tx.add("issue", { id: 1, title: "A" }));

  // Write B → its batch (seq 2) arrives → the RemoteBackend sees a gap (expected 1, got 2),
  // re-subscribes under a new epoch, and the fresh snapshot (A + B) recovers the view.
  wasm.write((tx) => tx.add("issue", { id: 2, title: "B" }));
  await remote.write((tx) => tx.add("issue", { id: 2, title: "B" }));

  await waitForEqual(rv, () => wv.data, "re-hydrate: recovered after a dropped batch");
  assert.deepStrictEqual(
    rv.data.map((r) => r.id),
    [1, 2],
    "view recovered to the correct state",
  );
  assert.strictEqual(transport.dropped, 1, "exactly one batch was dropped");

  transport.close();
  await server.close();
  console.log("✅ Part 2: gap → re-hydrate recovered the remote view (dropped 1 batch, re-subscribed)\n");
}

console.log("✅ @rindle/remote + @rindle/server: full remote round-trip verified (happy path + re-hydrate)");
