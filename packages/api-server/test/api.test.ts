import { test } from "node:test";
import assert from "node:assert/strict";

import type {
  DematerializeInput,
  DematerializeOutput,
  MaterializeInput,
  MaterializeOutput,
  MigrateInput,
  MigrateOutput,
  MutationRejection,
  MutationRejectionOutput,
  QueryOnceInput,
  QueryOnceOutput,
  RindleDaemonClient,
  RowChangeTxn,
  RowChangeTxnOutput,
  SqlRead,
  SqlReadOutput,
  SqlTxn,
  SqlTxnOutput,
} from "@rindle/daemon-client";

import { createSchema, defineQuery, newQueryBuilder, string, table } from "@rindle/client";

import {
  DEFAULT_RINDLE_API_ROUTES,
  RindleApiError,
  createRindleApiServer,
  defineApiMutators,
  defineApiQueries,
  registerQueries,
} from "../src/index.ts";

class MemoryDaemon implements RindleDaemonClient {
  materialized: MaterializeInput[] = [];
  sqlTxns: SqlTxn[] = [];
  sqlReads: SqlRead[] = [];
  rejections: MutationRejection[] = [];
  reads: QueryOnceInput[] = [];

  async materialize(input: MaterializeInput): Promise<MaterializeOutput> {
    this.materialized.push(input);
    return { materializationId: `mat-${this.materialized.length}`, leaseToken: `lease-${this.materialized.length}` };
  }

  async dematerialize(_input: DematerializeInput): Promise<DematerializeOutput> {
    return { removed: true };
  }

  async executeSqlTxn(input: SqlTxn): Promise<SqlTxnOutput> {
    this.sqlTxns.push(input);
    return { cv: this.sqlTxns.length };
  }

  async executeSqlRead(input: SqlRead): Promise<SqlReadOutput> {
    this.sqlReads.push(input);
    return { cols: [], rows: [] };
  }

  async applyRowChangeTxn(_input: RowChangeTxn): Promise<RowChangeTxnOutput> {
    return { cv: 1 };
  }

  async rejectMutation(input: MutationRejection): Promise<MutationRejectionOutput> {
    this.rejections.push(input);
    return { lmid: input.mid };
  }

  async query(input: QueryOnceInput): Promise<QueryOnceOutput> {
    this.reads.push(input);
    return {
      cvMin: 100 + this.reads.length,
      queryKey: `qk-${this.reads.length}`,
      rows: [{ cols: { id: 1, title: "first" } }],
    };
  }

  async migrate(_input: MigrateInput): Promise<MigrateOutput> {
    return { applied: true };
  }
}

test("createQueryLease resolves a named query to an AST and asks the daemon for a lease", async () => {
  const daemon = new MemoryDaemon();
  const api = createRindleApiServer({
    daemon,
    queries: defineApiQueries({
      issuesForTenant: (_ctx, args: { tenantID: number }) => ({
        table: "issue",
        where: {
          type: "simple",
          op: "=",
          left: { type: "column", name: "tenantID" },
          right: { type: "literal", value: args.tenantID },
        },
      }),
    }),
    authorizeQuery: ({ user }) => user === "u1",
    subject: ({ user }) => `user:${user}`,
  });

  const lease = await api.createQueryLease({
    user: "u1",
    name: "issuesForTenant",
    args: { tenantID: 7 },
  });

  assert.deepEqual(api.routes, DEFAULT_RINDLE_API_ROUTES);
  assert.deepEqual(lease, { materializationId: "mat-1", leaseToken: "lease-1", queryKey: undefined, reused: undefined });
  assert.equal(daemon.materialized.length, 1);
  assert.equal(daemon.materialized[0].mode, "normalized");
  assert.deepEqual(daemon.materialized[0].policy, { kind: "whileSubscribed" });
  assert.equal(daemon.materialized[0].subject, "user:u1");
  assert.deepEqual(daemon.materialized[0].ast, {
    table: "issue",
    where: {
      type: "simple",
      op: "=",
      left: { type: "column", name: "tenantID" },
      right: { type: "literal", value: 7 },
    },
  });
});

test("registerQueries forwards the AUTHORITATIVE ctx — a per-user query is scoped to the server's principal", async () => {
  const issue = table("issue").columns({ id: string(), ownerId: string() }).primaryKey("id");
  const schema = createSchema({ tables: [issue] });
  const qb = newQueryBuilder(schema);

  // The query is scoped by ctx.user — but the wire carries only (name, args), never the user.
  const myIssues = defineQuery(
    "myIssues",
    (raw): { limit: number } => ({ limit: (raw as { limit: number }).limit }),
    ({ limit }, ctx: { user: string | undefined }) => qb.issue.where.ownerId(ctx.user ?? "").limit(limit),
  );

  const daemon = new MemoryDaemon();
  const api = createRindleApiServer<string | undefined>({ daemon, queries: registerQueries<string | undefined>([myIssues]) });

  // Two different authenticated users name the SAME query with the SAME args; each is scoped to its
  // own server-side principal, not to anything the client supplied.
  await api.createQueryLease({ user: "alice", name: "myIssues", args: { limit: 5 } });
  await api.createQueryLease({ user: "bob", name: "myIssues", args: { limit: 5 } });

  const ownerOf = (ast: any): unknown => ast.where.right.value;
  assert.equal(ownerOf(daemon.materialized[0].ast), "alice");
  assert.equal(ownerOf(daemon.materialized[1].ast), "bob");
});

test("readQuery resolves a named query and asks the daemon for a one-shot assembled read", async () => {
  const daemon = new MemoryDaemon();
  const api = createRindleApiServer({
    daemon,
    queries: defineApiQueries({
      issuesForTenant: (_ctx, args: { tenantID: number }) => ({
        table: "issue",
        where: {
          type: "simple",
          op: "=",
          left: { type: "column", name: "tenantID" },
          right: { type: "literal", value: args.tenantID },
        },
      }),
    }),
    authorizeQuery: ({ user }) => user === "u1",
    // The one-shot must scope its dedup key by the SAME subject as the lease path so the warm
    // pipeline it leaves behind is the one the browser's follow-up subscribe reuses.
    subject: ({ user }) => `user:${user}`,
    readIdleTtlMs: 20000,
  });

  const out = await api.readQuery({ user: "u1", name: "issuesForTenant", args: { tenantID: 7 } });

  assert.equal(api.routes.read, "/api/rindle/read");
  // The assembled rows + watermark + queryKey flow straight back to the loader to seed + dehydrate.
  assert.deepEqual(out, { rows: [{ cols: { id: 1, title: "first" } }], cvMin: 101, queryKey: "qk-1" });
  // The daemon saw the resolved AST, the subject as the visibility key, and the read idle TTL — and
  // registered NO subscriber (the one-shot path, not materialize).
  assert.equal(daemon.reads.length, 1);
  assert.equal(daemon.materialized.length, 0);
  assert.equal(daemon.reads[0].visibilityKey, "user:u1");
  assert.equal(daemon.reads[0].ttlMs, 20000);
  assert.deepEqual(daemon.reads[0].ast, {
    table: "issue",
    where: {
      type: "simple",
      op: "=",
      left: { type: "column", name: "tenantID" },
      right: { type: "literal", value: 7 },
    },
  });
});

test("readQuery passes the same authorizeQuery gate as a lease and rejects before the daemon read", async () => {
  const daemon = new MemoryDaemon();
  const api = createRindleApiServer({
    daemon,
    queries: defineApiQueries({ allIssues: () => ({ table: "issue" }) }),
    authorizeQuery: () => false,
  });

  await assert.rejects(
    () => api.readQuery({ user: null, name: "allIssues", args: null }),
    (err: unknown) => err instanceof RindleApiError && err.status === 403,
  );
  assert.equal(daemon.reads.length, 0);
});

test("handleReadJson parses a default {name, args} read body", async () => {
  const daemon = new MemoryDaemon();
  const api = createRindleApiServer({
    daemon,
    queries: defineApiQueries({ hot: (_ctx, args: { limit: number }) => ({ table: "item", limit: args.limit }) }),
  });

  const out = await api.handleReadJson({ name: "hot", args: { limit: 5 } }, { user: "u1" });

  assert.deepEqual(out.rows, [{ cols: { id: 1, title: "first" } }]);
  assert.equal(daemon.reads.length, 1);
  assert.deepEqual(daemon.reads[0].ast, { table: "item", limit: 5 });
});

test("authorizeQuery is optional sugar and can reject before materialization", async () => {
  const daemon = new MemoryDaemon();
  const api = createRindleApiServer({
    daemon,
    queries: defineApiQueries({ allIssues: () => ({ table: "issue" }) }),
    authorizeQuery: () => false,
  });

  await assert.rejects(
    () => api.createQueryLease({ user: null, name: "allIssues", args: null }),
    (err: unknown) => err instanceof RindleApiError && err.status === 403,
  );
  assert.equal(daemon.materialized.length, 0);
});

test("pushMutation runs a SQL-producing mutator and confirms the client mid on the daemon txn", async () => {
  const daemon = new MemoryDaemon();
  const api = createRindleApiServer({
    daemon,
    mutators: defineApiMutators({
      createIssue: (tx, args: { id: number; title: string }) => {
        tx.exec("INSERT INTO issue (id, title) VALUES (?, ?)", [args.id, args.title]);
      },
    }),
    authorizeMutation: ({ user }) => user === "u1",
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 3, name: "createIssue", args: { id: 1, title: "first" } },
  });

  assert.deepEqual(out, { accepted: true, rejected: false, output: { cv: 1 } });
  assert.deepEqual(daemon.sqlTxns, [
    {
      clientID: "c1",
      mid: 3,
      statements: [{ sql: "INSERT INTO issue (id, title) VALUES (?, ?)", params: [1, "first"] }],
    },
  ]);
  assert.deepEqual(daemon.rejections, []);
});

test("pushMutation records daemon rejections for denied or thrown mutators", async () => {
  const daemon = new MemoryDaemon();
  const api = createRindleApiServer({
    daemon,
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

  assert.equal(thrown.accepted, false);
  assert.equal(unknown.accepted, false);
  assert.deepEqual(daemon.rejections, [
    { clientID: "c1", mid: 1, reason: "nope" },
    { clientID: "c1", mid: 2, reason: "unknown mutator: missing" },
  ]);
});

test("pushMutation does not turn daemon write failures into user rejections", async () => {
  const daemon = new MemoryDaemon();
  daemon.executeSqlTxn = async (input: SqlTxn): Promise<SqlTxnOutput> => {
    daemon.sqlTxns.push(input);
    throw new Error("daemon unavailable");
  };
  const api = createRindleApiServer({
    daemon,
    mutators: defineApiMutators({
      createIssue: (tx) => {
        tx.exec("INSERT INTO issue DEFAULT VALUES");
      },
    }),
  });

  await assert.rejects(
    () =>
      api.pushMutation({
        user: "u1",
        envelope: { clientID: "c1", mid: 1, name: "createIssue", args: null },
      }),
    /daemon unavailable/,
  );
  assert.equal(daemon.rejections.length, 0);
});

test("pushMutations applies an in-order batch; a rejection doesn't stop later envelopes", async () => {
  const daemon = new MemoryDaemon();
  const api = createRindleApiServer({
    daemon,
    mutators: defineApiMutators({
      ok: (tx) => tx.exec("INSERT INTO issue DEFAULT VALUES"),
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
  // The rejection advanced the daemon's lmid (the MemoryDaemon recorded it), so mid 3
  // stayed contiguous and applied.
  assert.deepEqual(daemon.rejections, [{ clientID: "c1", mid: 2, reason: "nope" }]);
  assert.deepEqual(daemon.sqlTxns.map((t) => t.mid), [1, 3]);
});

test("handleMutateJson accepts an envelope batch and replies with an array", async () => {
  const daemon = new MemoryDaemon();
  const api = createRindleApiServer({
    daemon,
    mutators: defineApiMutators({ touch: () => [] }),
  });

  const out = await api.handleMutateJson(
    {
      envelopes: [
        { clientID: "c1", mid: 1, name: "touch", args: null },
        { clientID: "c1", mid: 2, name: "touch", args: null },
      ],
    },
    { user: "u1" },
  );

  assert.equal(Array.isArray(out), true);
  assert.equal((out as unknown[]).length, 2);
  assert.equal(daemon.sqlTxns.length, 2);
});

test("JSON handlers parse default wire bodies", async () => {
  const daemon = new MemoryDaemon();
  const api = createRindleApiServer({
    daemon,
    queries: defineApiQueries({ allIssues: () => ({ table: "issue" }) }),
    mutators: defineApiMutators({ touch: () => [] }),
  });

  await api.handleQueryJson({ name: "allIssues" }, { user: "u1" });
  await api.handleMutateJson(
    { envelope: { clientID: "c1", mid: 1, name: "touch", args: { ok: true } } },
    { user: "u1" },
  );

  assert.equal(daemon.materialized.length, 1);
  assert.equal(daemon.sqlTxns.length, 1);
});

test("assertPins materializes every pinned query with a pinned policy and resolved args", async () => {
  const daemon = new MemoryDaemon();
  const api = createRindleApiServer({
    daemon,
    queries: defineApiQueries({
      hot: (_ctx, args: { limit: number }) => ({ table: "item", limit: args.limit }),
      latest: () => ({ table: "item", orderBy: [["time", "desc"]] }),
    }),
    // A per-viewer auth gate must NOT block a system pin.
    authorizeQuery: ({ user }) => user === "viewer",
    pinnedQueries: [
      { name: "hot", args: { limit: 30 } },
      { name: "latest" },
    ],
  });

  await api.assertPins();

  assert.equal(daemon.materialized.length, 2);
  const hot = daemon.materialized.find((m) => (m.ast as { limit?: number }).limit === 30);
  assert.ok(hot, "hot pin materialized");
  assert.deepEqual(hot!.policy, { kind: "pinned", name: "hot" });
  assert.equal(hot!.mode, "normalized");
  const latest = daemon.materialized.find((m) => m.policy && "name" in m.policy && m.policy.name === "latest");
  assert.deepEqual(latest!.policy, { kind: "pinned", name: "latest" });

  // Idempotent re-assert (a restart trigger) issues the pins again — the daemon dedupes them.
  await api.assertPins();
  assert.equal(daemon.materialized.length, 4);
  assert.ok(daemon.materialized.every((m) => m.policy?.kind === "pinned"));
});

test("assertPins is a no-op without pinnedQueries", async () => {
  const daemon = new MemoryDaemon();
  const api = createRindleApiServer({ daemon, queries: defineApiQueries({ hot: () => ({ table: "item" }) }) });
  await api.assertPins();
  assert.equal(daemon.materialized.length, 0);
});
