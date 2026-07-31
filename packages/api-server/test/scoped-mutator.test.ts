import { test } from "node:test";
import assert from "node:assert/strict";

import type { RindleDaemonClient, SqlTxn, SqlTxnOutput } from "@rindle/daemon-client";
import { createSchema, number, string, table } from "@rindle/client";

import {
  createRindleApiServer,
  daemonBackend,
  defineApiMutators,
  MutationRejected,
  postgresBackend,
  scoped,
  shared,
} from "../src/index.ts";
import type { MutationBackend, MutatorCtx } from "../src/index.ts";

import { FakePlugger, flat, isLmidUpsert, readOnlyDaemon } from "./helpers.ts";

// A schema exercising the LOGICAL (rendered) write path so a scoped mutator can drive a real shared
// (isomorphic) body via `scope.transact`.
const schema = createSchema({
  tables: [
    table("order")
      .columns({ id: string(), amount: number(), chargeId: string() })
      .primaryKey("id"),
  ],
});

/** The shared (isomorphic) body — identical on both tiers. `ctx.chargeId` is a SERVER-ONLY value the
 *  outside-tx phase computes; the client predicts/omits it (here it's just part of the server ctx). */
type OrderCtx = MutatorCtx & { chargeId: string };
const createOrder = shared<{ id: string; amount: number }, OrderCtx>(
  { parse: (r) => r as { id: string; amount: number } },
  function* (tx, args, ctx) {
    yield tx.insert("order", { id: args.id, amount: args.amount, chargeId: ctx.chargeId });
  },
);

test("scoped: outside-tx work runs BEFORE the tx, the shared body commits inside it (lmid last), after-commit runs AFTER", async () => {
  const events: string[] = [];
  const plugger = new FakePlugger(events);
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema,
    mutators: defineApiMutators({
      createOrder: scoped(async (scope, raw: { id: string; amount: number }, ctx) => {
        events.push("charge"); // an external side effect — must be OUTSIDE the tx
        const chargeId = `ch_${ctx.envelope.mid}`;
        await scope.transact(createOrder, raw, { user: "u1", chargeId });
        events.push("receipt"); // post-commit
      }),
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 7, name: "createOrder", args: { id: "o1", amount: 42 } },
  });

  assert.equal(out.accepted, true);
  // The charge landed before BEGIN; the receipt after COMMIT — the whole point of the split.
  assert.deepEqual(events, ["charge", "tx-open", "tx-commit", "receipt"]);
  // ONE committed txn: the rendered insert + the lmid upsert LAST (§2.2), carrying the server chargeId.
  assert.equal(plugger.txns.length, 1);
  const txn = plugger.txns[0];
  assert.ok(txn[0].sql.startsWith('INSERT INTO "order"'));
  assert.deepEqual(txn[0].params, ["o1", 42, "ch_7"]);
  assert.ok(isLmidUpsert(txn[txn.length - 1].sql));
  assert.deepEqual(txn[txn.length - 1].params, ["c1", 7]);
});

test("scoped: a business rejection inside transact throws MutationRejected (compensation runs), still advances lmid alone", async () => {
  const events: string[] = []; // mutator-step log only (no plugger markers — the PG reject path opens two txns)
  const plugger = new FakePlugger();
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema,
    mutators: defineApiMutators({
      createOrder: scoped(async (scope, raw: { id: string; amount: number }) => {
        events.push("charge");
        try {
          await scope.transact((tx) => {
            throw new Error("out of stock"); // a business rejection in the transacted body
          });
        } catch (e) {
          assert.ok(e instanceof MutationRejected);
          assert.equal((e as MutationRejected).reason, "out of stock");
          events.push("refund"); // COMPENSATE the outside-tx charge
          throw e;
        }
        events.push("receipt"); // must NOT run
      }),
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 3, name: "createOrder", args: { id: "o1", amount: 1 } },
  });

  assert.deepEqual([out.accepted, out.rejected && out.reason], [false, "out of stock"]);
  assert.deepEqual(events, ["charge", "refund"]); // no receipt; refund ran
  // Data rolled back, but lmid STILL advanced (§2.4) — an lmid-only txn.
  assert.equal(plugger.txns.length, 1);
  const only = plugger.txns[0];
  assert.equal(only.length, 1);
  assert.ok(isLmidUpsert(only[0].sql));
  assert.deepEqual(only[0].params, ["c1", 3]);
});

test("scoped: swallowing the MutationRejected does NOT flip the outcome to accepted", async () => {
  const plugger = new FakePlugger();
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema,
    mutators: defineApiMutators({
      createOrder: scoped(async (scope, raw: { id: string; amount: number }) => {
        try {
          await scope.transact((tx) => {
            throw new Error("denied");
          });
        } catch {
          // swallow — the harness must still report the sealed rejection, not the clean return
        }
      }),
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 5, name: "createOrder", args: { id: "o1", amount: 1 } },
  });
  assert.deepEqual([out.accepted, out.rejected && out.reason], [false, "denied"]);
  assert.equal(plugger.txns.length, 1); // lmid-only still committed
});

test("scoped: never calling transact is an accepted no-op that STILL advances lmid", async () => {
  const plugger = new FakePlugger();
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema,
    mutators: defineApiMutators({
      createOrder: scoped(async (_scope, _raw) => {
        // decided (after outside-tx work) there is nothing to write — but the client predicted a
        // write, so its pending entry must resolve.
      }),
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 9, name: "createOrder", args: { id: "o1", amount: 1 } },
  });
  assert.equal(out.accepted, true);
  assert.equal(plugger.txns.length, 1);
  assert.deepEqual(flat(plugger.txns).map(isLmidUpsert), [true]);
});

test("scoped: a throw BEFORE transact is a business rejection — lmid advances, no data txn", async () => {
  const plugger = new FakePlugger();
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema,
    mutators: defineApiMutators({
      createOrder: scoped(async (_scope, _raw) => {
        throw new Error("card declined"); // outside-tx work failed; nothing was written
      }),
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 2, name: "createOrder", args: { id: "o1", amount: 1 } },
  });
  assert.deepEqual([out.accepted, out.rejected && out.reason], [false, "card declined"]);
  assert.equal(plugger.txns.length, 1);
  assert.ok(isLmidUpsert(plugger.txns[0][0].sql)); // lmid-only advance
});

test("scoped: an INFRA failure in transact propagates and does NOT advance lmid (client retries)", async () => {
  const plugger = new FakePlugger();
  plugger.failWith = new Error("db down");
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema,
    mutators: defineApiMutators({
      createOrder: scoped(async (scope, raw: { id: string; amount: number }, ctx) => {
        await scope.transact(createOrder, raw, { user: "u1", chargeId: `ch_${ctx.envelope.mid}` });
      }),
    }),
  });

  await assert.rejects(
    () =>
      api.pushMutation({
        user: "u1",
        envelope: { clientID: "c1", mid: 4, name: "createOrder", args: { id: "o1", amount: 1 } },
      }),
    /db down/,
  );
  assert.equal(plugger.txns.length, 0); // nothing committed — lmid untouched, safe to retry
});

test("scoped: calling transact twice throws (one atomic write per mutation)", async () => {
  const plugger = new FakePlugger();
  let secondError: unknown;
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema,
    mutators: defineApiMutators({
      createOrder: scoped(async (scope, raw: { id: string; amount: number }, ctx) => {
        await scope.transact(createOrder, raw, { user: "u1", chargeId: "ch_a" });
        try {
          await scope.transact(createOrder, raw, { user: "u1", chargeId: "ch_b" });
        } catch (e) {
          secondError = e;
        }
      }),
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 1, name: "createOrder", args: { id: "o1", amount: 1 } },
  });
  // The first transact committed (accepted); the second threw and was caught here.
  assert.equal(out.accepted, true);
  assert.ok(secondError instanceof Error && /at most once/.test((secondError as Error).message));
  assert.equal(plugger.txns.length, 1); // only the first write committed
});

// --------------------------------------------------------------------------- harness-owns-the-boundary edge cases

test("scoped: a transact the author FORGOT to await is still sealed from its REAL outcome (no phantom no-op)", async () => {
  const plugger = new FakePlugger();
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema,
    mutators: defineApiMutators({
      // NB: NO `await` — a floating promise (nothing lints against it). The harness must still seal
      // from the real write, not reply with a phantom lmid-only no-op while the insert commits after.
      createOrder: scoped((scope, raw: { id: string; amount: number }) => {
        void scope.transact(createOrder, raw, { user: "u1", chargeId: "ch_x" });
      }),
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 11, name: "createOrder", args: { id: "o1", amount: 3 } },
  });

  assert.equal(out.accepted, true);
  // EXACTLY one txn, and it is the REAL insert (+ lmid) — not a phantom lmid-only commit plus an
  // out-of-band write. Before the fix, txns[0] would be the lmid-only no-op (its first stmt an upsert).
  assert.equal(plugger.txns.length, 1);
  const txn = plugger.txns[0];
  assert.ok(txn[0].sql.startsWith('INSERT INTO "order"'));
  assert.deepEqual(txn[0].params, ["o1", 3, "ch_x"]);
  assert.ok(isLmidUpsert(txn[txn.length - 1].sql));
});

test("scoped: an infra failure that rejects with a FALSY value is still infra (never a business rejection)", async () => {
  // A backend whose transaction rejects with a falsy value (a real driver can). `reject` throwing
  // proves the business-rejection path never runs — a lost commit must NOT advance lmid.
  const base = daemonBackend(readOnlyDaemon());
  const infraBackend: MutationBackend = {
    dialect: base.dialect,
    runMutation: () => Promise.reject(undefined),
    reject: () => Promise.reject(new Error("the business-reject path must NOT run for an infra failure")),
  };
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: infraBackend,
    schema,
    mutators: defineApiMutators({
      createOrder: scoped(async (scope, raw: { id: string; amount: number }) => {
        await scope.transact(createOrder, raw, { user: "u1", chargeId: "ch" });
      }),
    }),
  });

  let caught: unknown = "no-throw";
  try {
    await api.pushMutation({
      user: "u1",
      envelope: { clientID: "c1", mid: 6, name: "createOrder", args: { id: "o1", amount: 1 } },
    });
  } catch (e) {
    caught = e;
  }
  // The falsy value propagates VERBATIM (client retries). Before the fix, `scope.infra !== undefined`
  // was false, so it took the business-reject branch and `caught` would be the reject-path Error.
  assert.equal(caught, undefined);
});

test("scoped: a FAILED post-reject compensation is surfaced via onScopeError; the outcome stays rejected", async () => {
  const plugger = new FakePlugger();
  const scopeErrors: Array<{ err: unknown; phase: string }> = [];
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema,
    onScopeError: (err, info) => scopeErrors.push({ err, phase: info.phase }),
    mutators: defineApiMutators({
      createOrder: scoped(async (scope, _raw: { id: string; amount: number }) => {
        try {
          await scope.transact(() => {
            throw new Error("denied");
          });
        } catch (e) {
          assert.ok(e instanceof MutationRejected);
          throw new Error("refund failed"); // the compensation ITSELF fails — must not vanish
        }
      }),
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 12, name: "createOrder", args: { id: "o1", amount: 1 } },
  });

  // The compensation throw can't flip the sealed outcome...
  assert.deepEqual([out.accepted, out.rejected && out.reason], [false, "denied"]);
  // ...but it is surfaced, not swallowed.
  assert.equal(scopeErrors.length, 1);
  assert.equal(scopeErrors[0].phase, "rejected");
  assert.ok(scopeErrors[0].err instanceof Error && (scopeErrors[0].err as Error).message === "refund failed");
  assert.equal(plugger.txns.length, 1); // lmid-only still committed
});

test("scoped: rethrowing the MutationRejected itself is the sanctioned signal — NOT surfaced as an error", async () => {
  const plugger = new FakePlugger();
  const scopeErrors: unknown[] = [];
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema,
    onScopeError: (err) => scopeErrors.push(err),
    mutators: defineApiMutators({
      createOrder: scoped(async (scope, _raw: { id: string; amount: number }) => {
        try {
          await scope.transact(() => {
            throw new Error("denied");
          });
        } catch (e) {
          throw e; // compensate + rethrow the SAME MutationRejected — the documented pattern
        }
      }),
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 14, name: "createOrder", args: { id: "o1", amount: 1 } },
  });

  assert.deepEqual([out.accepted, out.rejected && out.reason], [false, "denied"]);
  assert.deepEqual(scopeErrors, []); // the rethrown MutationRejected carries no NEW failure to report
});

test("scoped: a FAILED post-commit effect is surfaced via onScopeError; the outcome stays accepted", async () => {
  const plugger = new FakePlugger();
  const phases: string[] = [];
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema,
    onScopeError: (_err, info) => phases.push(info.phase),
    mutators: defineApiMutators({
      createOrder: scoped(async (scope, raw: { id: string; amount: number }) => {
        await scope.transact(createOrder, raw, { user: "u1", chargeId: "ch_z" });
        throw new Error("receipt send failed"); // a post-commit effect fails
      }),
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 13, name: "createOrder", args: { id: "o1", amount: 2 } },
  });

  assert.equal(out.accepted, true); // committed — a post-commit throw can't un-accept it
  assert.deepEqual(phases, ["committed"]); // but it is surfaced
  assert.equal(plugger.txns.length, 1);
});

// --------------------------------------------------------------------------- daemon backend

/** Minimal daemon fake: records executeSqlTxn / rejectMutation in order. */
class BatchDaemon {
  log: string[] = [];
  txns: SqlTxn[] = [];
  client(): RindleDaemonClient {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    return {
      materialize: () => Promise.reject(new Error("unused")),
      dematerialize: () => Promise.reject(new Error("unused")),
      query: () => Promise.reject(new Error("unused")),
      migrate: () => Promise.reject(new Error("unused")),
      applyRowChangeTxn: () => Promise.reject(new Error("unused")),
      executeSqlTxn: async (input: SqlTxn): Promise<SqlTxnOutput> => {
        self.log.push("executeSqlTxn");
        self.txns.push(input);
        return { applied: true };
      },
      rejectMutation: async () => {
        self.log.push("rejectMutation");
        return {};
      },
    } as unknown as RindleDaemonClient;
  }
}

test("scoped: on the daemon backend, outside-tx work precedes the accumulated batch; the shared body renders to SQLite", async () => {
  const events: string[] = [];
  const daemon = new BatchDaemon();
  const client = daemon.client();
  const api = createRindleApiServer({
    daemon: client,
    backend: daemonBackend(client),
    schema,
    mutators: defineApiMutators({
      createOrder: scoped(async (scope, raw: { id: string; amount: number }, ctx) => {
        events.push("charge");
        await scope.transact(createOrder, raw, { user: "u1", chargeId: `ch_${ctx.envelope.mid}` });
        events.push("batch-shipped");
      }),
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 8, name: "createOrder", args: { id: "o1", amount: 5 } },
  });

  assert.equal(out.accepted, true);
  assert.deepEqual(events, ["charge", "batch-shipped"]);
  assert.deepEqual(daemon.log, ["executeSqlTxn"]);
  assert.equal(daemon.txns.length, 1);
  assert.equal(daemon.txns[0].mid, 8);
  const insert = daemon.txns[0].statements[0];
  assert.ok(insert.sql.startsWith('INSERT INTO "order"'));
  assert.deepEqual(insert.params, ["o1", 5, "ch_8"]); // SQLite `?` placeholders, chargeId from outside-tx
});

// --------------------------------------------------------------------------- transact's return value

test("scope.transact returns its body's value — a post-commit effect decided by a TRANSACTIONAL read", async () => {
  const plugger = new FakePlugger();
  const fired: unknown[] = [];
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema,
    mutators: defineApiMutators({
      place: scoped(async (scope, raw) => {
        const a = raw as { id: string; amount: number };
        // The decision is computed INSIDE the transaction, from a read that sees exactly the state
        // the write commits against — not from a second, racy read afterwards.
        const kick = await scope.transact(async (tx) => {
          await tx.sql.execute("insert into order (id, amount) values ($1, $2)", [a.id, a.amount]);
          const rows = await tx.sql.query<{ total: number }>("select 1 as total");
          return rows.length >= 0 ? { shipTo: a.id, at: "in-tx" } : undefined;
        });
        // Only reachable when the transaction COMMITTED.
        if (kick) fired.push(kick);
      }),
    }),
  });

  const res = await api.pushMutation({
    user: undefined,
    envelope: { clientID: "c1", mid: 1, name: "place", args: { id: "o1", amount: 5 } },
  });

  assert.equal(res.accepted, true);
  assert.deepEqual(fired, [{ shipTo: "o1", at: "in-tx" }]);
});

test("a rolled-back transaction yields NO value — the effect cannot fire on work that vanished", async () => {
  const plugger = new FakePlugger();
  const fired: unknown[] = [];
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema,
    mutators: defineApiMutators({
      place: scoped(async (scope, _raw) => {
        try {
          const kick = await scope.transact(async (tx) => {
            await tx.sql.execute("insert into order (id, amount) values ($1, $2)", ["o1", 1]);
            throw new Error("over quota"); // a BUSINESS rejection: the data rolls back
          });
          fired.push(kick); // unreachable
        } catch (err) {
          assert.ok(err instanceof MutationRejected);
        }
      }),
    }),
  });

  const res = await api.pushMutation({
    user: undefined,
    envelope: { clientID: "c1", mid: 1, name: "place", args: {} },
  });

  assert.equal(res.rejected, true);
  assert.deepEqual(fired, [], "no value escaped the rejected transaction");
  // `lmid` still advanced alone, so the client's queue drains.
  assert.ok(flat(plugger.txns).some(isLmidUpsert));
});
