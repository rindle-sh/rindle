// REPRODUCTION — adversarial-review HIGH #12: any napi-thrown error in a ws message handler
// crashes the entire server process. createOptimisticServer / createReplicaServer /
// createNormalizedServer guard ONLY JSON.parse; every napi call inside the handler (an unknown
// table in a subscribe → queryNormalized/registerQuery throws) is a synchronous throw in a ws
// 'message' listener → uncaughtException → Node kills the process, taking down every connection.
//
// This sends a malformed subscribe (unknown table) over a real ws and asserts (a) the server
// process does NOT crash, and (b) the server still serves a healthy connection afterwards.
// On HEAD the bad message raises an uncaughtException; after the fix the handler isolates it.
//
// Run with `--conditions=@rindle/source`; needs the replica native addon.

import assert from "node:assert/strict";
import { WebSocket } from "ws";

import { table, string, number, createSchema } from "@rindle/client";
import { createOptimisticServer, createReplicaServer, createNormalizedServer } from "@rindle/server";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const serverRegistry = { noop: () => {} };
const serverQueries = {
  // Exercises the original crash path through native query registration.
  bad: () => ({ table: "no_such_table" }),
  good: () => ({ table: "issue" }),
};

// Trap uncaughtException so a HEAD crash doesn't kill THIS test process — record it instead.
const uncaught = [];
const onUncaught = (e) => uncaught.push(e);
process.on("uncaughtException", onUncaught);

// Never hang: a stuck server (or a HEAD crash leaving servers open) exits non-zero.
const hardTimeout = setTimeout(() => {
  console.error("server repro-high: timed out (server likely wedged/crashed)");
  process.exit(1);
}, 20000);

function connect(port) {
  return new Promise((resolve) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}`);
    ws.on("open", () => resolve(ws));
  });
}

function nextMessage(ws, pred = () => true, ms = 3000) {
  return new Promise((resolve, reject) => {
    const to = setTimeout(() => reject(new Error("timeout waiting for server message")), ms);
    const onMsg = (data) => {
      const m = JSON.parse(String(data));
      if (pred(m)) {
        clearTimeout(to);
        ws.off("message", onMsg);
        resolve(m);
      }
    };
    ws.on("message", onMsg);
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function checkSurvives(name, server) {
  uncaught.length = 0;

  // (1) A malformed subscribe (unknown table) → a napi/AST throw inside the handler.
  const bad = await connect(server.port);
  bad.send(JSON.stringify({ t: "init", clientID: "bad" }));
  bad.send(JSON.stringify({ t: "subscribe", queryId: 1, name: "bad", args: null }));
  await sleep(150); // let the server process it (and, on HEAD, crash)
  assert.equal(uncaught.length, 0, `${name}: a malformed message crashed the server process`);

  // (2) The server must still serve a healthy connection.
  const good = await connect(server.port);
  good.send(JSON.stringify({ t: "init", clientID: "good" }));
  good.send(JSON.stringify({ t: "subscribe", queryId: 1, name: "good", args: null }));
  const hello = await nextMessage(good, (m) => m.t === "hello" || m.t === "nhello");
  assert.ok(hello, `${name}: server still serves a valid subscribe after a bad one`);

  bad.close();
  good.close();
  console.log(`✓ ${name}: survives a malformed subscribe (no process crash)`);
}

const optimistic = await createOptimisticServer({ schema, registry: serverRegistry, queries: serverQueries });
await checkSurvives("optimistic", optimistic);
await optimistic.close();

const normalized = await createNormalizedServer({ schema, queries: serverQueries });
await checkSurvives("normalized", normalized);
await normalized.close();

const replica = await createReplicaServer({ schema, queries: serverQueries });
await checkSurvives("replica", replica);
await replica.close();

process.off("uncaughtException", onUncaught);
clearTimeout(hardTimeout);
console.log("✅ server repro-high: ws handlers survive malformed/throwing messages (#12)");
process.exit(0);
