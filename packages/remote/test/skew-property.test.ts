// Property test (deterministic, seeded) for the normalized hello's column-compatibility rule —
// the TS analog of fuzzing for `validateAgainstClientSchema`. The Rust differential fuzzer uses
// ONE shared schema for both sides, so it cannot model a client schema that is narrower/skewed vs.
// the server's; this sweeps that space directly.
//
// Invariant under test: the subscriber ACCEPTS a hello iff one table's column list is a subsequence
// of the other BY NAME — a projection (server ⊆ client) or an expansion (client ⊆ server) — and the
// PK names match. A genuine skew (a rename or a reordered shared column), where neither direction
// subsequences, is REJECTED. PK is held matched here (the dedicated PK-skew case is in
// projection-hello.test.ts) so the column rule is what's exercised.

import { test } from "node:test";
import assert from "node:assert/strict";

import { COMPARATOR_VERSION } from "@rindle/client";
import type { NormalizedTableSchema } from "@rindle/client";
import { NormalizedSubscriber, normalizedFp, type NormalizedHello } from "../src/normalized.ts";

function mulberry32(seed: number): () => number {
  return () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Oracle: is `sub` a subsequence of `sup` by name? (Independent reimplementation of the rule.) */
function subseq(sub: readonly string[], sup: readonly string[]): boolean {
  let i = 0;
  for (const name of sup) if (i < sub.length && sub[i] === name) i++;
  return i === sub.length;
}

function helloFor(tables: NormalizedTableSchema[]): NormalizedHello {
  return { epoch: 1, comparatorVersion: COMPARATOR_VERSION, tables, normalizedFp: normalizedFp(tables) };
}

function accepts(server: NormalizedTableSchema[], client: NormalizedTableSchema[]): boolean {
  try {
    new NormalizedSubscriber(helloFor(server), () => {}, client);
    return true;
  } catch {
    return false;
  }
}

test("property: a hello is accepted iff one column list subsequences the other (PK matched)", () => {
  const rand = mulberry32(0x5e3f00d);
  const pick = <T>(a: T[]): T => a[Math.floor(rand() * a.length)];
  const pool = ["id", "a", "b", "c", "d", "e"];
  const extras = ["x", "y", "z"];

  let sawAccept = false;
  let sawReject = false;
  for (let iter = 0; iter < 4000; iter++) {
    const client = pool.filter(() => rand() < 0.6);
    if (client.length < 1) continue;
    const pkIdx = Math.floor(rand() * client.length);
    const pkName = client[pkIdx];

    // Build the server column list via one of five transforms; the oracle is computed from the
    // RESULT (not the label), so a degenerate transform (e.g. an identity "reorder") is still scored
    // correctly. Every transform keeps the PK name so the PK-by-name check never decides the result.
    let server: string[];
    switch (pick(["project", "expand", "reorder", "rename", "mixed"] as const)) {
      case "project": // keep a subsequence (always the PK) ⇒ server ⊆ client ⇒ accept
        server = client.filter((_, j) => j === pkIdx || rand() < 0.7);
        break;
      case "expand": // keep ALL of client in order + splice extras ⇒ client ⊆ server ⇒ accept
        server = client.slice();
        for (const e of extras) if (rand() < 0.5) server.splice(Math.floor(rand() * (server.length + 1)), 0, e);
        break;
      case "reorder": { // shuffle ⇒ usually neither subsequence ⇒ reject
        server = client.slice();
        for (let i = server.length - 1; i > 0; i--) {
          const j = Math.floor(rand() * (i + 1));
          [server[i], server[j]] = [server[j], server[i]];
        }
        break;
      }
      case "rename": { // rename one NON-pk column ⇒ neither ⇒ reject
        server = client.slice();
        const cands = server.map((_, j) => j).filter((j) => j !== pkIdx);
        if (cands.length === 0) {
          server = client.filter((_, j) => j === pkIdx || rand() < 0.7); // fall back to a projection
        } else {
          server[pick(cands)] = pick(extras);
        }
        break;
      }
      default: { // mixed: drop a non-pk column AND add an extra ⇒ neither ⇒ reject
        server = client.filter((_, j) => j === pkIdx || rand() < 0.6);
        server.splice(Math.floor(rand() * (server.length + 1)), 0, pick(extras));
      }
    }

    const serverPk = server.indexOf(pkName);
    if (serverPk < 0) continue; // generator guarantees the PK survives; skip the rare degenerate case
    const clientTables: NormalizedTableSchema[] = [{ name: "t", columns: client, primaryKey: [pkIdx] }];
    const serverTables: NormalizedTableSchema[] = [{ name: "t", columns: server, primaryKey: [serverPk] }];

    const expected = subseq(server, client) || subseq(client, server);
    assert.equal(
      accepts(serverTables, clientTables),
      expected,
      `iter ${iter}: client=[${client}] pk=${pkName} server=[${server}] → expected accept=${expected}`,
    );
    expected ? (sawAccept = true) : (sawReject = true);
  }
  // The sweep must actually exercise BOTH outcomes (guards against a generator that only ever accepts).
  assert.ok(sawAccept && sawReject, "the random sweep covered both accepted and rejected helloes");
});
