// The shared (generator) mutator seam (MUTATORS-ISOMORPHIC): one body, driven synchronously on the
// client and asynchronously on the server. These tests pin the tier-agnostic pieces — the effect
// factory, the two drivers, and the read-your-writes / fan-out / detection semantics — independent of
// either tier's real transaction.

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  driveMutationAsync,
  driveMutationSync,
  isGeneratorMutator,
  isoTx,
} from "../src/index.ts";
import type { Ast, KeyedRow, MutationGen, MutationOp, QueryArg, QueryResultRow } from "../src/index.ts";

// Most bodies below drive no `tx.query`; a stub keeps the exec well-typed (the driver contract
// requires it) and loudly flags an unexpected query effect.
const noQuerySync = (): never => {
  throw new Error("unexpected tx.query in this test");
};
const noQueryAsync = (): Promise<never> => Promise.reject(new Error("unexpected tx.query in this test"));

test("isoTx builds effect objects and performs no I/O (stateless, shared)", () => {
  assert.deepEqual(isoTx.insert("issue", { id: "1" }), { kind: "insert", table: "issue", row: { id: "1" } });
  assert.deepEqual(isoTx.upsert("issue", { id: "1" }), { kind: "upsert", table: "issue", row: { id: "1" } });
  assert.deepEqual(isoTx.insertIgnore("user", { id: "u" }), { kind: "insertIgnore", table: "user", row: { id: "u" } });
  assert.deepEqual(isoTx.update("issue", { id: "1", title: "x" }), {
    kind: "update",
    table: "issue",
    row: { id: "1", title: "x" },
  });
  assert.deepEqual(isoTx.delete("issue", { id: "1" }), { kind: "delete", table: "issue", pk: { id: "1" } });
  assert.deepEqual(isoTx.row("issue", { id: "1" }), { kind: "row", table: "issue", pk: { id: "1" } });
  const q: QueryArg = { ast: () => ({}) as unknown as Ast };
  assert.deepEqual(isoTx.query(q), { kind: "query", query: q });
});

test("isGeneratorMutator distinguishes a generator body from a plain function", () => {
  function* gen(): MutationGen {
    yield isoTx.insert("issue", { id: "1" });
  }
  const plain = () => {};
  assert.equal(isGeneratorMutator(gen), true);
  assert.equal(isGeneratorMutator(plain), false);
  assert.equal(isGeneratorMutator(undefined), false);
  assert.equal(isGeneratorMutator(123), false);
});

// A shared body that reads-its-own-write: insert a user, then only insert the issue's comment if the
// user now exists. Exercises the write→read→conditional-write shape on ONE body, both drivers.
function* ensureThenComment(tx: typeof isoTx, id: string): MutationGen {
  yield tx.insertIgnore("user", { id, name: id });
  const u = yield tx.row("user", { id });
  if (u) yield tx.insert("comment", { id: `c-${id}`, authorId: id });
}

test("driveMutationSync: writes apply in order and a read is fed back (read-your-writes)", () => {
  const rows = new Map<string, KeyedRow>(); // table+pk → row
  const log: MutationOp[] = [];
  driveMutationSync(ensureThenComment(isoTx, "alice"), {
    apply: (op) => {
      log.push(op);
      if (op.kind === "insertIgnore" || op.kind === "insert") rows.set(`${op.table}:${op.row.id}`, op.row);
    },
    read: (table, pk) => rows.get(`${table}:${pk.id}`),
    query: noQuerySync,
  });
  assert.deepEqual(
    log.map((o) => o.kind),
    ["insertIgnore", "insert"], // the read (between them) is NOT an apply; the comment insert fired because the user was seen
  );
  assert.equal((log[1] as { row: KeyedRow }).row.id, "c-alice");
});

test("driveMutationAsync: same body, awaited — read resolves against prior writes", async () => {
  const rows = new Map<string, KeyedRow>();
  const log: MutationOp[] = [];
  await driveMutationAsync(ensureThenComment(isoTx, "bob"), {
    apply: async (op) => {
      log.push(op);
      if (op.kind === "insertIgnore" || op.kind === "insert") rows.set(`${op.table}:${op.row.id}`, op.row);
    },
    read: async (table, pk) => rows.get(`${table}:${pk.id}`),
    query: noQueryAsync,
  });
  assert.deepEqual(
    log.map((o) => o.kind),
    ["insertIgnore", "insert"],
  );
});

test("driveMutationAsync: a read that misses skips the dependent write (branch not taken)", async () => {
  const log: MutationOp[] = [];
  await driveMutationAsync(ensureThenComment(isoTx, "carol"), {
    apply: async (op) => {
      log.push(op);
    },
    read: async () => undefined, // the row is never visible → the comment insert must NOT fire
    query: noQueryAsync,
  });
  assert.deepEqual(
    log.map((o) => o.kind),
    ["insertIgnore"],
  );
});

test("tx.all fans out and returns results IN ORDER on both drivers (deterministic)", async () => {
  function* readTwo(tx: typeof isoTx): MutationGen {
    const pair = yield tx.all([tx.row("user", { id: "a" }), tx.row("user", { id: "b" })]);
    // both tiers hand back an array in the SAME order; the last write records what we saw
    const [a, b] = pair as unknown as (KeyedRow | undefined)[];
    yield tx.insert("audit", { id: "seen", a: a?.id ?? "none", b: b?.id ?? "none" });
  }
  const store: Record<string, KeyedRow> = { "user:a": { id: "a" }, "user:b": { id: "b" } };

  const syncLog: MutationOp[] = [];
  driveMutationSync(readTwo(isoTx), {
    apply: (op) => syncLog.push(op),
    read: (table, pk) => store[`${table}:${pk.id}`],
    query: noQuerySync,
  });
  const asyncLog: MutationOp[] = [];
  await driveMutationAsync(readTwo(isoTx), {
    apply: async (op) => {
      asyncLog.push(op);
    },
    read: async (table, pk) => store[`${table}:${pk.id}`],
    query: noQueryAsync,
  });

  const audited = (log: MutationOp[]) => (log[0] as { row: KeyedRow }).row;
  assert.deepEqual(audited(syncLog), { id: "seen", a: "a", b: "b" });
  assert.deepEqual(audited(asyncLog), { id: "seen", a: "a", b: "b" }); // identical result across tiers
});

test("tx.query resolves full-query rows and feeds them back — identical on both drivers", async () => {
  // A shared body that READS via a full query (not a pk read) and writes a summary of what it saw.
  const fakeQuery: QueryArg = { ast: () => ({ table: "issue" }) as unknown as Ast };
  function* summarize(tx: typeof isoTx): MutationGen {
    const rows = (yield tx.query(fakeQuery)) as unknown as QueryResultRow[];
    yield tx.insert("summary", { id: "s", count: rows.length, first: (rows[0]?.id as string) ?? "none" });
  }
  // The seam contract: identical DBs → identical rows, so both tiers resolve the query the SAME way.
  const queryRows: QueryResultRow[] = [{ id: "i1" }, { id: "i2" }];

  const syncLog: MutationOp[] = [];
  driveMutationSync(summarize(isoTx), {
    apply: (op) => syncLog.push(op),
    read: () => undefined,
    query: () => queryRows,
  });
  const asyncLog: MutationOp[] = [];
  await driveMutationAsync(summarize(isoTx), {
    apply: async (op) => {
      asyncLog.push(op);
    },
    read: async () => undefined,
    query: async () => queryRows,
  });

  const written = (log: MutationOp[]) => (log[0] as { row: KeyedRow }).row;
  assert.deepEqual(written(syncLog), { id: "s", count: 2, first: "i1" });
  assert.deepEqual(written(asyncLog), { id: "s", count: 2, first: "i1" }); // identical across tiers
});
