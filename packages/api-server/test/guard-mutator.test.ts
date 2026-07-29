// `guardMutator` — the multi-tenant authz twin of `sharedApiMutators`: parse args → derive the
// principal → evaluate a boolean access predicate against the OPEN txn → throw `forbidden` (403) on
// deny, else drive the SAME shared body the client predicts. These tests exercise the guard logic
// (deny/allow/read-in-txn/principal-throw/custom-message) directly against a recording tx.

import { test } from "node:test";
import assert from "node:assert/strict";

import { createSchema, defineMutators, string, table } from "@rindle/client";
import type { MutationOp, MutatorCtx } from "@rindle/client";

import { RindleApiError, guardMutator } from "../src/index.ts";
import type { MutationContext, ServerMutationTx } from "../src/index.ts";

const issue = table("issue").columns({ id: string(), title: string(), ownerId: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const { shared } = defineMutators(schema);

type CreateArgs = { id: string; title: string };
const createArgs = {
  parse: (r: unknown): CreateArgs => {
    const o = r as Record<string, unknown>;
    if (typeof o?.id !== "string" || typeof o?.title !== "string") throw new Error("bad args");
    return { id: o.id, title: o.title };
  },
};

// A shared body whose author is the PRINCIPAL (`ctx.user`), never an arg — the exact shape
// `sharedApiMutators` auto-drives and the client predicts.
const createIssue = shared(createArgs, function* (tx, a: CreateArgs, ctx: MutatorCtx) {
  yield tx.insert("issue", { id: a.id, title: a.title, ownerId: ctx.user });
});

/** A ServerMutationTx that records the logical ops `runSharedMutation` applies and returns a
 *  scriptable row from `query` (the read a guard predicate makes inside the open txn). */
class RecordingTx {
  ops: MutationOp[] = [];
  queried = 0;
  queryResult: unknown = null;
  insert(t: string, row: Record<string, unknown>): Promise<void> {
    this.ops.push({ kind: "insert", table: t, row } as MutationOp);
    return Promise.resolve();
  }
  upsert(t: string, row: Record<string, unknown>): Promise<void> {
    this.ops.push({ kind: "upsert", table: t, row } as MutationOp);
    return Promise.resolve();
  }
  insertIgnore(t: string, row: Record<string, unknown>): Promise<void> {
    this.ops.push({ kind: "insertIgnore", table: t, row } as MutationOp);
    return Promise.resolve();
  }
  update(t: string, row: Record<string, unknown>): Promise<void> {
    this.ops.push({ kind: "update", table: t, row } as MutationOp);
    return Promise.resolve();
  }
  delete(t: string, pk: Record<string, unknown>): Promise<void> {
    this.ops.push({ kind: "delete", table: t, pk } as MutationOp);
    return Promise.resolve();
  }
  row(): Promise<unknown> {
    return Promise.resolve(null);
  }
  query(_q: unknown): Promise<unknown> {
    this.queried++;
    return Promise.resolve(this.queryResult);
  }
  exec(): void {
    throw new Error("exec: not exercised here");
  }
  get statements(): [] {
    return [];
  }
}

const asTx = (t: RecordingTx): ServerMutationTx => t as unknown as ServerMutationTx;
const ctxOf = (user: string): MutationContext<string> => ({ user }) as unknown as MutationContext<string>;
const principal = (ctx: MutationContext<string>): MutatorCtx => {
  if (!ctx.user) throw new RindleApiError("forbidden", "a user is required", 403);
  return { user: ctx.user };
};

test("guardMutator: a predicate returning false throws forbidden and does NOT drive the body", async () => {
  const tx = new RecordingTx();
  const m = guardMutator(createIssue, principal, () => false, { message: "not permitted to edit this issue" });

  await assert.rejects(
    () => Promise.resolve(m(asTx(tx), { id: "i1", title: "hi" }, ctxOf("u1"))),
    (e: unknown) =>
      e instanceof RindleApiError &&
      e.code === "forbidden" &&
      e.status === 403 &&
      e.message === "not permitted to edit this issue",
  );
  assert.deepEqual(tx.ops, [], "the shared body never ran");
});

test("guardMutator: a predicate returning true drives the body with the principal as author", async () => {
  const tx = new RecordingTx();
  const m = guardMutator(createIssue, principal, () => true);

  await m(asTx(tx), { id: "i1", title: "hi" }, ctxOf("u1"));

  assert.deepEqual(tx.ops, [{ kind: "insert", table: "issue", row: { id: "i1", title: "hi", ownerId: "u1" } }]);
});

test("guardMutator: a void-returning predicate allows (only strict `false` denies)", async () => {
  const tx = new RecordingTx();
  const m = guardMutator(createIssue, principal, () => {
    /* an arg policy that throws-or-passes, never returns false */
  });

  await m(asTx(tx), { id: "i2", title: "ok" }, ctxOf("u2"));

  assert.equal(tx.ops.length, 1, "nothing (undefined) is treated as allow");
});

test("guardMutator: the predicate can READ the open txn and sees the derived principal", async () => {
  const tx = new RecordingTx();
  tx.queryResult = { id: "i1", ownerId: "u1" }; // the access read finds an editable row
  let sawUser: string | undefined;

  const m = guardMutator(createIssue, principal, async (t, a, ctx) => {
    sawUser = ctx.user;
    const found = await t.query({ id: a.id } as unknown as never);
    return found != null;
  });

  await m(asTx(tx), { id: "i1", title: "hi" }, ctxOf("u1"));

  assert.equal(sawUser, "u1", "predicate receives the MutatorCtx principal, not the raw wire ctx");
  assert.equal(tx.queried, 1, "predicate read the open txn");
  assert.equal(tx.ops.length, 1, "and then the write ran");
});

test("guardMutator: when the access read finds nothing, it denies and skips the write", async () => {
  const tx = new RecordingTx();
  tx.queryResult = null; // no editable row

  const m = guardMutator(createIssue, principal, async (t) => (await t.query({} as unknown as never)) != null);

  await assert.rejects(
    () => Promise.resolve(m(asTx(tx), { id: "i1", title: "hi" }, ctxOf("u1"))),
    (e: unknown) => e instanceof RindleApiError && e.status === 403,
  );
  assert.deepEqual(tx.ops, []);
});

test("guardMutator: principal derivation runs first — an anonymous caller is rejected before the predicate", async () => {
  const tx = new RecordingTx();
  let predicateRan = false;
  const m = guardMutator(createIssue, principal, () => {
    predicateRan = true;
    return true;
  });

  await assert.rejects(
    () => Promise.resolve(m(asTx(tx), { id: "i1", title: "hi" }, ctxOf(""))),
    (e: unknown) => e instanceof RindleApiError && e.code === "forbidden",
  );
  assert.equal(predicateRan, false, "the predicate never ran for an anonymous caller");
  assert.deepEqual(tx.ops, []);
});
