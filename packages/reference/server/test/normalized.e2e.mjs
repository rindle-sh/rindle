// Normalized ws round-trip e2e: a real ws server (@rindle/server `createNormalizedServer`,
// serving the native engine's NORMALIZED footprint stream) + a normalized local-first client
// (@rindle/normalized `createNormalizedStore` over `@rindle/remote`'s `RemoteNormalizedSource`).
// Two parts, mirroring the flat e2e:
//   1. happy path — the normalized client's view (server normalized stream → NormalizedSync →
//      local wasm engine → FlatArrayView) must converge to a local flat wasm store (the oracle)
//      across hydrate + writes;
//   2. gap → re-hydrate — a transport that DROPS one nbatch forces a seq gap; the
//      RemoteNormalizedSource re-subscribes under a new epoch, and NormalizedSync diffs the new
//      footprint against the old to recover (§5.3).
// Run with `--conditions=@rindle/source`; needs the replica native addon + the wasm pkg built.
import assert from "node:assert/strict";
import { isDeepStrictEqual } from "node:util";

import { defineQuery, table, string, number, createSchema, newQueryBuilder } from "@rindle/client";
import { createNormalizedServer } from "@rindle/server";
import { WsTransport, RemoteNormalizedSource } from "@rindle/remote";
import { createNormalizedStore } from "@rindle/normalized";
import { createWasmStore, initWasm } from "@rindle/wasm";

// `createNormalizedStore` builds a local wasm engine synchronously, so init the wasm first.
await initWasm();

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
    unsub = view.subscribe(check);
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
  const server = await createNormalizedServer({ schema, queries: serverQueries });
  const transport = new WsTransport(`ws://127.0.0.1:${server.port}`);
  const norm = createNormalizedStore(schema, new RemoteNormalizedSource(transport));
  const wasm = await createWasmStore(schema); // the oracle (flat local engine)

  const nv = norm.materialize(clientQueries.issuesWithComments());
  const wv = build(wasm.query).materialize();

  await waitForEqual(nv, () => wv.data, "empty hydrate");
  console.log("✓ empty hydrate (normalized client == wasm oracle)");

  let n = 0;
  const step = async (label, ops) => {
    wasm.write((tx) => apply(tx, ops)); // sync oracle
    await norm.write((tx) => apply(tx, ops)); // async over ws (→ server → normalized stream back)
    await waitForEqual(nv, () => wv.data, label);
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
  await step("root remove (with its comments)", [{ k: "remove", t: "issue", r: { id: 2, title: "second" } }]);

  transport.close();
  await server.close();
  console.log("✅ Part 1: normalized remote client == wasm oracle over hydrate + 6 steps\n");
}

// ----------------------------- Part 2: gap → re-hydrate -----------------------------
{
  // Drop the FIRST incremental nbatch (seq 1) once, simulating packet loss.
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

  const server = await createNormalizedServer({ schema, queries: serverQueries });
  const transport = new DroppingTransport(`ws://127.0.0.1:${server.port}`, (m) => m.t === "nbatch" && m.batch.seq === 1);
  const norm = createNormalizedStore(schema, new RemoteNormalizedSource(transport));
  const wasm = await createWasmStore(schema);

  const nv = norm.materialize(clientQueries.allIssues());
  const wv = wasm.query.issue.orderBy("id", "asc").materialize();

  await waitForEqual(nv, () => wv.data, "re-hydrate: empty hydrate");

  // Write A → its nbatch (seq 1) is DROPPED → the client stalls (does not see A).
  wasm.write((tx) => tx.add("issue", { id: 1, title: "A" }));
  await norm.write((tx) => tx.add("issue", { id: 1, title: "A" }));

  // Write B → its nbatch (seq 2) arrives → the source sees a gap (expected 1, got 2),
  // re-subscribes under a new epoch, and the fresh footprint (A + B) recovers the view.
  wasm.write((tx) => tx.add("issue", { id: 2, title: "B" }));
  await norm.write((tx) => tx.add("issue", { id: 2, title: "B" }));

  await waitForEqual(nv, () => wv.data, "re-hydrate: recovered after a dropped nbatch");
  assert.deepStrictEqual(
    nv.data.map((r) => r.id),
    [1, 2],
    "view recovered to the correct state",
  );
  assert.strictEqual(transport.dropped, 1, "exactly one nbatch was dropped");

  transport.close();
  await server.close();
  console.log("✅ Part 2: gap → re-hydrate recovered the normalized client (dropped 1 nbatch, re-subscribed)\n");
}

console.log("✅ @rindle/normalized over ws: full normalized round-trip verified (happy path + re-hydrate)");
