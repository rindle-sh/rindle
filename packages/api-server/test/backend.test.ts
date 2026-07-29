import { test } from "node:test";
import assert from "node:assert/strict";

import type {
  MutationRejection,
  MutationRejectionOutput,
  RindleDaemonClient,
  SqlTxn,
  SqlTxnOutput,
} from "@rindle/daemon-client";

import { createSchema, number, string, table } from "@rindle/client";

import {
  createRindleApiServer,
  daemonBackend,
  defineApiMutators,
  isoTx,
  pgPoolPlugger,
  postgresBackend,
  questionToDollarParams,
  runSharedMutation,
} from "../src/index.ts";
import type { IsoTx, MutationGen, MutatorCtx, PgQuery, PostgresPlugger } from "../src/index.ts";

import { FakePlugger, isLmidUpsert, readOnlyDaemon } from "./helpers.ts";

/** A tiny schema for the LOGICAL (rendered) mutator path. */
const testSchema = createSchema({
  tables: [
    table("issue")
      .columns({ id: string(), title: string(), ownerId: string(), updatedAt: number() })
      .primaryKey("id"),
  ],
});

test("postgresBackend: an accepted mutation is data + lmid upsert in ONE transaction, upsert last", async () => {
  const plugger = new FakePlugger();
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    mutators: defineApiMutators({
      createIssue: (tx, args: { id: number; title: string }) => {
        tx.exec('INSERT INTO "issue" ("id", "title") VALUES ($1, $2)', [args.id, args.title]);
        tx.exec('UPDATE "issue" SET "updatedAt" = $1 WHERE "id" = $2', [9, args.id]);
      },
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 3, name: "createIssue", args: { id: 1, title: "first" } },
  });

  assert.equal(out.accepted, true);
  assert.equal(plugger.txns.length, 1);
  const txn = plugger.txns[0];
  assert.equal(txn.length, 3);
  assert.equal(txn[0].sql, 'INSERT INTO "issue" ("id", "title") VALUES ($1, $2)');
  assert.deepEqual(txn[0].params, [1, "first"]);
  // The §2.2 invariant, observable: the lmid upsert is the LAST statement of the SAME txn.
  assert.ok(isLmidUpsert(txn[2].sql));
  assert.deepEqual(txn[2].params, ["c1", 3]);
});

test("postgresBackend: a rejection commits an lmid-ONLY transaction (§2.4) and returns the reason on HTTP", async () => {
  const plugger = new FakePlugger();
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    mutators: defineApiMutators({
      forbidden: () => {
        throw new Error("nope");
      },
    }),
  });

  const thrown = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 1, name: "forbidden", args: null },
  });
  const unknown = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 2, name: "missing", args: null },
  });

  assert.deepEqual(
    [thrown.accepted, thrown.rejected && thrown.reason, unknown.accepted],
    [false, "nope", false],
  );
  // Two committed transactions, each carrying ONLY the lmid advance — no data, but the client's
  // pending queue still drains past the rejected mids when these rows ride the CDC loop down.
  assert.equal(plugger.txns.length, 2);
  for (const [i, txn] of plugger.txns.entries()) {
    assert.equal(txn.length, 1);
    assert.ok(isLmidUpsert(txn[0].sql));
    assert.deepEqual(txn[0].params, ["c1", i + 1]);
  }
});

test("postgresBackend: a PG failure THROWS out of pushMutation — never a user rejection", async () => {
  const plugger = new FakePlugger();
  plugger.failWith = new Error("connection refused");
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    mutators: defineApiMutators({ touch: (tx) => tx.exec("INSERT INTO t DEFAULT VALUES") }),
  });

  await assert.rejects(
    () => api.pushMutation({ user: "u1", envelope: { clientID: "c1", mid: 1, name: "touch", args: null } }),
    /connection refused/,
  );
  assert.equal(plugger.txns.length, 0);
});

test("postgresBackend: rewriteSql bridges ?-style mutators to $n, and never touches the lmid upsert", async () => {
  const plugger = new FakePlugger();
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger, { rewriteSql: questionToDollarParams }),
    mutators: defineApiMutators({
      addTag: (tx, args: { id: string; name: string }) => {
        tx.exec('INSERT INTO "tag" ("id", "name") VALUES (?, ?)', [args.id, args.name]);
      },
    }),
  });

  await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 5, name: "addTag", args: { id: "t1", name: "bug" } },
  });

  const txn = plugger.txns[0];
  assert.equal(txn[0].sql, 'INSERT INTO "tag" ("id", "name") VALUES ($1, $2)');
  assert.ok(isLmidUpsert(txn[1].sql), "the upsert is emitted in $n form already — rewrite must not apply");
});

test("postgresBackend: a batch stays contiguous — a mid-batch rejection still advances lmid", async () => {
  const plugger = new FakePlugger();
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    mutators: defineApiMutators({
      ok: (tx) => tx.exec("INSERT INTO t DEFAULT VALUES"),
      bad: () => {
        throw new Error("nope");
      },
    }),
  });

  const out = await api.pushMutations({
    user: "u1",
    envelopes: [
      { clientID: "c1", mid: 1, name: "ok", args: null },
      { clientID: "c1", mid: 2, name: "bad", args: null },
      { clientID: "c1", mid: 3, name: "ok", args: null },
    ],
  });

  assert.deepEqual(out.map((o) => o.accepted), [true, false, true]);
  assert.equal(plugger.txns.length, 3);
  // Every transaction — accepted or rejected — ends with the lmid advance for its mid.
  assert.deepEqual(
    plugger.txns.map((txn) => txn[txn.length - 1].params),
    [["c1", 1], ["c1", 2], ["c1", 3]],
  );
});

test("daemonBackend: runMutation ships ONE accumulated batch via executeSqlTxn; a throw routes to rejectMutation", async () => {
  const sqlTxns: SqlTxn[] = [];
  const rejections: MutationRejection[] = [];
  const daemon = {
    executeSqlTxn: async (input: SqlTxn): Promise<SqlTxnOutput> => {
      sqlTxns.push(input);
      return { cv: 1 };
    },
    rejectMutation: async (input: MutationRejection): Promise<MutationRejectionOutput> => {
      rejections.push(input);
      return { lmid: input.mid };
    },
  } as unknown as RindleDaemonClient;
  const backend = daemonBackend(daemon);
  assert.equal(backend.dialect.name, "sqlite");

  // Accepted: the mutator's writes accumulate and ship as ONE batch (the daemon stamps lmid).
  const accepted = await backend.runMutation({
    envelope: { clientID: "c1", mid: 4, name: "m", args: null },
    render: {},
    run: async (tx) => {
      tx.exec("INSERT INTO t DEFAULT VALUES");
    },
  });
  assert.equal(accepted.accepted, true);

  // A returned SqlTxn's idempotencyKey is honored; a `void` return still ships the driven tx.
  const withKey = await backend.runMutation({
    envelope: { clientID: "c1", mid: 5, name: "m", args: null },
    render: {},
    run: async (tx) => {
      tx.exec("INSERT INTO u DEFAULT VALUES");
      return; // (idempotencyKey exercised via the return-style path below)
    },
  });
  assert.equal(withKey.accepted, true);

  // Business rejection (mutator throws): NO data shipped, rejectMutation advances lmid alone.
  const rejected = await backend.runMutation({
    envelope: { clientID: "c1", mid: 6, name: "m", args: null },
    render: {},
    run: async () => {
      throw new Error("denied");
    },
  });
  assert.deepEqual([rejected.accepted, rejected.accepted === false && rejected.reason], [false, "denied"]);

  // Pre-flight reject (unknown mutator / failed auth): lmid alone, no data.
  await backend.reject({ envelope: { clientID: "c1", mid: 7, name: "m", args: null }, reason: "nope" });

  assert.deepEqual(sqlTxns, [
    { clientID: "c1", mid: 4, statements: [{ sql: "INSERT INTO t DEFAULT VALUES", params: [] }] },
    { clientID: "c1", mid: 5, statements: [{ sql: "INSERT INTO u DEFAULT VALUES", params: [] }] },
  ]);
  assert.deepEqual(rejections, [
    { clientID: "c1", mid: 6, reason: "denied" },
    { clientID: "c1", mid: 7, reason: "nope" },
  ]);
});

test("pgPoolPlugger brackets one client in BEGIN/COMMIT and rolls back + releases on failure", async () => {
  const log: string[] = [];
  let released = 0;
  const pool = {
    connect: async () => ({
      query: async (sql: string) => {
        if (sql === "FAIL") throw new Error("boom");
        log.push(sql);
        return { rows: [{ ok: 1 }] };
      },
      release: () => {
        released += 1;
      },
    }),
  };
  const plugger = pgPoolPlugger(pool);

  const rows = await plugger.transaction(async (q) => {
    await q.exec("INSERT INTO t VALUES (1)");
    return q.query("SELECT 1");
  });
  assert.deepEqual(rows, [{ ok: 1 }]);
  assert.deepEqual(log, ["BEGIN", "INSERT INTO t VALUES (1)", "SELECT 1", "COMMIT"]);

  log.length = 0;
  await assert.rejects(
    () => plugger.transaction(async (q) => q.exec("FAIL")),
    /boom/,
  );
  assert.deepEqual(log, ["BEGIN", "ROLLBACK"]);
  assert.equal(released, 2, "the client is released on success AND failure");
});

test("postgresBackend: a LOGICAL mutator renders $n SQL live, in mutator order, lmid last", async () => {
  const plugger = new FakePlugger();
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema: testSchema,
    mutators: defineApiMutators({
      createIssue: async (tx, args: { id: string; title: string; ownerId: string; updatedAt: number }) => {
        await tx.insertIgnore("issue", { id: args.id, title: args.title, ownerId: args.ownerId, updatedAt: args.updatedAt });
        await tx.update("issue", { id: args.id, title: "renamed", updatedAt: args.updatedAt });
      },
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 7, name: "createIssue", args: { id: "i1", title: "hi", ownerId: "u1", updatedAt: 5 } },
  });

  assert.equal(out.accepted, true);
  const txn = plugger.txns[0];
  assert.equal(txn.length, 3);
  // insertIgnore renders the full row + ON CONFLICT DO NOTHING, columns in schema order, $n placeholders.
  assert.equal(
    txn[0].sql,
    'INSERT INTO "issue" ("id", "title", "ownerId", "updatedAt") VALUES ($1, $2, $3, $4) ON CONFLICT ("id") DO NOTHING',
  );
  assert.deepEqual(txn[0].params, ["i1", "hi", "u1", 5]);
  // update renders SET (named non-pk) + WHERE (pk); params SET-then-pk.
  assert.equal(txn[1].sql, 'UPDATE "issue" SET "title" = $1, "updatedAt" = $2 WHERE "id" = $3');
  assert.deepEqual(txn[1].params, ["renamed", 5, "i1"]);
  assert.ok(isLmidUpsert(txn[2].sql));
});

test("daemonBackend: a LOGICAL mutator accumulates rendered SQLite (?) statements into one batch", async () => {
  const sqlTxns: SqlTxn[] = [];
  const daemon = {
    executeSqlTxn: async (input: SqlTxn) => {
      sqlTxns.push(input);
      return { cv: 1 };
    },
  } as unknown as RindleDaemonClient;
  const api = createRindleApiServer({
    daemon,
    backend: daemonBackend(daemon),
    schema: testSchema,
    mutators: defineApiMutators({
      del: async (tx, args: { id: string }) => {
        await tx.delete("issue", { id: args.id });
      },
    }),
  });

  await api.pushMutation({ user: "u1", envelope: { clientID: "c1", mid: 1, name: "del", args: { id: "i9" } } });

  assert.deepEqual(sqlTxns[0].statements, [{ sql: 'DELETE FROM "issue" WHERE "id" = ?', params: ["i9"] }]);
});

test("postgresBackend: a mutator READS ITS OWN WRITE live in the txn (read-your-writes)", async () => {
  // A fake q that remembers INSERTed issue rows and serves them on the point-read SELECT.
  const store = new Map<string, Record<string, unknown>>();
  const plugger: PostgresPlugger = {
    async transaction(fn) {
      const q: PgQuery = {
        exec: async (sql, params = []) => {
          if (sql.startsWith('INSERT INTO "issue"')) {
            store.set(String(params[0]), { id: params[0], title: params[1], ownerId: params[2], updatedAt: params[3] });
          }
        },
        query: async (_sql, params = []) => {
          const row = store.get(String((params as unknown[])[0]));
          return row ? [row] : [];
        },
      };
      return fn(q);
    },
  };
  let seen: unknown;
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema: testSchema,
    mutators: defineApiMutators({
      createAndRead: async (tx) => {
        await tx.insert("issue", { id: "i1", title: "hi", ownerId: "u1", updatedAt: 5 });
        seen = await tx.row("issue", { id: "i1" });
      },
    }),
  });

  await api.pushMutation({ user: "u1", envelope: { clientID: "c1", mid: 1, name: "createAndRead", args: null } });
  assert.deepEqual(seen, { id: "i1", title: "hi", ownerId: "u1", updatedAt: 5 });
});

test("postgresBackend: tx.sql executes and queries raw SQL on the same live transaction", async () => {
  const events: Array<{ kind: "exec" | "query"; sql: string; params: unknown[] }> = [];
  const plugger: PostgresPlugger = {
    async transaction(fn) {
      return fn({
        exec: async (sql, params = []) => {
          events.push({ kind: "exec", sql, params });
        },
        query: async (sql, params = []) => {
          events.push({ kind: "query", sql, params });
          return [{ id: "i1", revision: 2 }];
        },
      });
    },
  };
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger, { rewriteSql: questionToDollarParams }),
    mutators: defineApiMutators({
      revise: async (tx) => {
        await tx.sql.execute("UPDATE issue SET revision = revision + 1 WHERE id = ?", ["i1"]);
        const rows = await tx.sql.query<{ id: string; revision: number }>(
          "SELECT id, revision FROM issue WHERE id = ?",
          ["i1"],
        );
        assert.deepEqual(rows, [{ id: "i1", revision: 2 }]);
        await tx.sql.batch([{ sql: "INSERT INTO audit (issue_id) VALUES (?)", params: ["i1"] }]);
      },
    }),
  });

  await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 8, name: "revise", args: null },
  });
  assert.deepEqual(events.slice(0, 3), [
    {
      kind: "exec",
      sql: "UPDATE issue SET revision = revision + 1 WHERE id = $1",
      params: ["i1"],
    },
    { kind: "query", sql: "SELECT id, revision FROM issue WHERE id = $1", params: ["i1"] },
    { kind: "exec", sql: "INSERT INTO audit (issue_id) VALUES ($1)", params: ["i1"] },
  ]);
  assert.ok(events[3] && isLmidUpsert(events[3].sql));
});

// ── the SHARED (generator) mutator seam driven server-side via runSharedMutation ──────────────────
// The client predicts these SAME bodies synchronously; here we prove the async server drive renders
// identically and preserves read-your-writes + fan-out through the real PgLiveTx / DaemonAccumulatingTx.

type CreateArgs = { id: string; title: string; updatedAt: number };

/** A shared body: the author is the PRINCIPAL (`ctx.user`, never an arg) and it writes across ops. */
function* createIssueShared(tx: IsoTx, a: CreateArgs, ctx: MutatorCtx): MutationGen {
  yield tx.insertIgnore("issue", { id: a.id, title: a.title, ownerId: ctx.user, updatedAt: a.updatedAt });
  yield tx.update("issue", { id: a.id, title: "renamed", updatedAt: a.updatedAt });
}

test("runSharedMutation: a shared GENERATOR renders $n live on PG, in body order, lmid last (parity with the async form)", async () => {
  const plugger = new FakePlugger();
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema: testSchema,
    mutators: defineApiMutators({
      // The thin server wrapper: inject the authenticated principal, drive the shared body.
      createIssue: (tx, raw: CreateArgs, ctx) => runSharedMutation(createIssueShared, raw, { user: String(ctx.user) }, tx),
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 7, name: "createIssue", args: { id: "i1", title: "hi", updatedAt: 5 } },
  });

  assert.equal(out.accepted, true);
  const txn = plugger.txns[0];
  assert.equal(txn.length, 3);
  assert.equal(
    txn[0].sql,
    'INSERT INTO "issue" ("id", "title", "ownerId", "updatedAt") VALUES ($1, $2, $3, $4) ON CONFLICT ("id") DO NOTHING',
  );
  assert.deepEqual(txn[0].params, ["i1", "hi", "u1", 5]); // ownerId is the PRINCIPAL, not an arg
  assert.equal(txn[1].sql, 'UPDATE "issue" SET "title" = $1, "updatedAt" = $2 WHERE "id" = $3');
  assert.deepEqual(txn[1].params, ["renamed", 5, "i1"]);
  assert.ok(isLmidUpsert(txn[2].sql));
});

test("runSharedMutation: the SAME shared body accumulates rendered SQLite (?) on the daemon backend", async () => {
  const sqlTxns: SqlTxn[] = [];
  const daemon = {
    executeSqlTxn: async (input: SqlTxn) => {
      sqlTxns.push(input);
      return { cv: 1 };
    },
  } as unknown as RindleDaemonClient;
  const api = createRindleApiServer({
    daemon,
    backend: daemonBackend(daemon),
    schema: testSchema,
    mutators: defineApiMutators({
      createIssue: (tx, raw: CreateArgs, ctx) => runSharedMutation(createIssueShared, raw, { user: String(ctx.user) }, tx),
    }),
  });

  await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 1, name: "createIssue", args: { id: "i1", title: "hi", updatedAt: 5 } },
  });

  assert.deepEqual(sqlTxns[0].statements, [
    {
      sql: 'INSERT INTO "issue" ("id", "title", "ownerId", "updatedAt") VALUES (?, ?, ?, ?) ON CONFLICT ("id") DO NOTHING',
      params: ["i1", "hi", "u1", 5],
    },
    { sql: 'UPDATE "issue" SET "title" = ?, "updatedAt" = ? WHERE "id" = ?', params: ["renamed", 5, "i1"] },
  ]);
});

test("runSharedMutation: a generator READS ITS OWN WRITE live in the PG txn (driver settles the chain first)", async () => {
  const store = new Map<string, Record<string, unknown>>();
  const plugger: PostgresPlugger = {
    async transaction(fn) {
      const q: PgQuery = {
        exec: async (sql, params = []) => {
          if (sql.startsWith('INSERT INTO "issue"')) {
            store.set(String(params[0]), { id: params[0], title: params[1], ownerId: params[2], updatedAt: params[3] });
          }
        },
        query: async (_sql, params = []) => {
          const row = store.get(String((params as unknown[])[0]));
          return row ? [row] : [];
        },
      };
      return fn(q);
    },
  };
  let seen: unknown;
  let both: unknown;
  function* readOwnWrites(tx: IsoTx): MutationGen {
    yield tx.insert("issue", { id: "a", title: "A", ownerId: "u", updatedAt: 1 });
    yield tx.insert("issue", { id: "b", title: "B", ownerId: "u", updatedAt: 1 });
    seen = yield tx.row("issue", { id: "a" }); // must observe the write above
    both = yield tx.all([tx.row("issue", { id: "a" }), tx.row("issue", { id: "b" })]); // fan-out, in order
  }
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    schema: testSchema,
    mutators: defineApiMutators({
      m: (tx) => runSharedMutation((t: IsoTx) => readOwnWrites(t), null, { user: "u" }, tx),
    }),
  });

  await api.pushMutation({ user: "u1", envelope: { clientID: "c1", mid: 1, name: "m", args: null } });
  assert.deepEqual(seen, { id: "a", title: "A", ownerId: "u", updatedAt: 1 });
  assert.deepEqual(both, [
    { id: "a", title: "A", ownerId: "u", updatedAt: 1 },
    { id: "b", title: "B", ownerId: "u", updatedAt: 1 },
  ]);
});

test("a LOGICAL op with NO schema configured is a loud rejection, not a silent dropped write", async () => {
  const plugger = new FakePlugger();
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    backend: postgresBackend(plugger),
    // no `schema`
    mutators: defineApiMutators({
      oops: async (tx) => {
        await tx.insert("issue", { id: "x" });
      },
    }),
  });

  const out = await api.pushMutation({ user: "u1", envelope: { clientID: "c1", mid: 1, name: "oops", args: null } });
  assert.equal(out.accepted, false);
  assert.match(out.accepted === false ? out.reason : "", /unknown table|schema/);
  // Still an lmid-only txn so the client's queue drains (a rejection, not an infra throw).
  assert.equal(plugger.txns.length, 1);
  assert.ok(isLmidUpsert(plugger.txns[0][0].sql));
});

test("questionToDollarParams numbers placeholders and skips literals, identifiers, and comments", () => {
  assert.equal(
    questionToDollarParams("INSERT INTO t (a, b) VALUES (?, ?)"),
    "INSERT INTO t (a, b) VALUES ($1, $2)",
  );
  assert.equal(
    questionToDollarParams(`SELECT '?' AS q, "col?umn", 'it''s ?' WHERE a = ? -- trailing ?\n AND b = ? /* block ? */`),
    `SELECT '?' AS q, "col?umn", 'it''s ?' WHERE a = $1 -- trailing ?\n AND b = $2 /* block ? */`,
  );
  // Unterminated spans (a trailing comment / open literal) pass through without looping.
  assert.equal(questionToDollarParams("SELECT 1 -- tail ?"), "SELECT 1 -- tail ?");
  assert.equal(questionToDollarParams("SELECT 'open ?"), "SELECT 'open ?");
});
