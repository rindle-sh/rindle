// The daemon backend's lazy upgrade to interactive mutation sessions
// (DAEMON-INTERACTIVE-TXN-DESIGN.md §5): pure-write mutators keep the one-batch path
// byte-identical; the FIRST read opens a session carrying the accumulated prefix + the read;
// later writes buffer and flush before the next read/commit; begin-time dedup absorbs replays
// without re-running the body; rejection rolls back BEFORE the lmid-only reject commit.

import { test } from "node:test";
import assert from "node:assert/strict";

import type {
  MutationSessionBegin,
  MutationSessionBeginOutput,
  MutationSessionExec,
  MutationSessionQuery,
  MutationSessionRef,
  RindleDaemonClient,
  SqlReadOutput,
  SqlTxn,
  SqlTxnOutput,
} from "@rindle/daemon-client";
import { DaemonHttpError } from "@rindle/daemon-client";

import { createSchema, newQueryBuilder, string, table } from "@rindle/client";

import { createRindleApiServer, daemonBackend, defineApiMutators, runSharedMutation } from "../src/index.ts";
import type { IsoTx, MutationGen } from "../src/index.ts";

const testSchema = createSchema({
  tables: [table("issue").columns({ id: string(), title: string() }).primaryKey("id")],
});

/** A session-capable fake daemon: records every call in order, answers canned reads. */
class SessionDaemon {
  log: string[] = [];
  txns: SqlTxn[] = [];
  begins: MutationSessionBegin[] = [];
  execs: MutationSessionExec[] = [];
  queries: MutationSessionQuery[] = [];
  /** Rows the fake "database" answers point reads with (cols fixed to the issue table). */
  readReply: SqlReadOutput = { cols: ["id", "title"], rows: [] };
  beginReply?: MutationSessionBeginOutput;
  failQueryWith?: Error;
  commitFailures: Error[] = [];

  client(): RindleDaemonClient {
    /* eslint-disable @typescript-eslint/no-this-alias */
    const self = this;
    return {
      materialize: () => Promise.reject(new Error("unused")),
      dematerialize: () => Promise.reject(new Error("unused")),
      query: () => Promise.reject(new Error("unused")),
      migrate: () => Promise.reject(new Error("unused")),
      applyRowChangeTxn: () => Promise.reject(new Error("unused")),
      async executeSqlTxn(input: SqlTxn): Promise<SqlTxnOutput> {
        self.log.push("executeSqlTxn");
        self.txns.push(input);
        return { applied: true };
      },
      async executeSqlRead(): Promise<SqlReadOutput> {
        self.log.push("executeSqlRead");
        return self.readReply;
      },
      async rejectMutation() {
        self.log.push("rejectMutation");
        return {};
      },
      async beginMutationSession(input: MutationSessionBegin): Promise<MutationSessionBeginOutput> {
        self.log.push("begin");
        self.begins.push(input);
        return self.beginReply ?? { sessionId: "ms1", read: self.readReply };
      },
      async execInMutationSession(input: MutationSessionExec): Promise<unknown> {
        self.log.push("exec");
        self.execs.push(input);
        return {};
      },
      async queryInMutationSession(input: MutationSessionQuery): Promise<SqlReadOutput> {
        self.log.push("query");
        self.queries.push(input);
        if (self.failQueryWith) throw self.failQueryWith;
        return self.readReply;
      },
      async commitMutationSession(): Promise<SqlTxnOutput> {
        self.log.push("commit");
        const failure = self.commitFailures.shift();
        if (failure) throw failure;
        return { applied: true, lmidAdvances: [{ clientID: "c1", lmid: 1 }] };
      },
      async rollbackMutationSession(): Promise<unknown> {
        self.log.push("rollback");
        return {};
      },
    } as unknown as RindleDaemonClient;
  }
}

function api(daemon: SessionDaemon, mutators: Parameters<typeof defineApiMutators>[0]) {
  const client = daemon.client();
  return createRindleApiServer({
    daemon: client,
    backend: daemonBackend(client),
    schema: testSchema,
    mutators: defineApiMutators(mutators),
  });
}

const envelope = (name: string, args: unknown) => ({ clientID: "c1", mid: 1, name, args });

test("a pure-write mutator never opens a session: one batch, byte-identical", async () => {
  const daemon = new SessionDaemon();
  const server = api(daemon, {
    create: async (tx, args: { id: string; title: string }) => {
      await tx.insert("issue", { id: args.id, title: args.title });
      await tx.update("issue", { id: args.id, title: "renamed" });
    },
  });

  const out = await server.pushMutation({ user: "u", envelope: envelope("create", { id: "i1", title: "t" }) });
  assert.equal(out.accepted, true);
  assert.deepEqual(daemon.log, ["executeSqlTxn"]);
  assert.equal(daemon.txns[0].statements.length, 2);
  assert.equal(daemon.txns[0].clientID, "c1");
  assert.equal(daemon.txns[0].mid, 1);
});

test("the first read upgrades: begin carries the prefix AND the read; later writes flush before the next read; commit stamps", async () => {
  const daemon = new SessionDaemon();
  daemon.readReply = { cols: ["id", "title"], rows: [["i1", "current"]] };
  const seen: Array<Record<string, unknown> | undefined> = [];
  const server = api(daemon, {
    edit: async (tx, args: { id: string }) => {
      await tx.insert("issue", { id: "pre", title: "prefix" }); // pre-read: accumulates
      seen.push(await tx.row("issue", { id: args.id })); // read 1 → upgrade
      await tx.update("issue", { id: args.id, title: "v2" }); // buffers
      seen.push(await tx.row("issue", { id: args.id })); // read 2 → flush + query
      await tx.insert("issue", { id: "post", title: "tail" }); // buffers; flushes at commit
    },
  });

  const out = await server.pushMutation({ user: "u", envelope: envelope("edit", { id: "i1" }) });
  assert.equal(out.accepted, true);

  // begin(prefix + first read) → [flush]exec + query → [flush]exec + commit; never executeSqlTxn.
  assert.deepEqual(daemon.log, ["begin", "exec", "query", "exec", "commit"]);
  const begin = daemon.begins[0];
  assert.equal(begin.clientID, "c1");
  assert.equal(begin.mid, 1);
  assert.equal(begin.statements?.length, 1); // the pre-read insert rode the begin
  assert.match(begin.statements![0].sql, /INSERT INTO "issue"/);
  assert.match(begin.query!.sql, /SELECT .* FROM "issue" WHERE/);
  assert.match(daemon.execs[0].statements[0].sql, /UPDATE "issue"/); // flushed before read 2
  assert.match(daemon.execs[1].statements[0].sql, /INSERT INTO "issue"/); // flushed at commit
  // Both reads decoded to keyed rows.
  assert.deepEqual(seen, [
    { id: "i1", title: "current" },
    { id: "i1", title: "current" },
  ]);
});

test("a begin-absorbed replay short-circuits: the body stops at the read, nothing ships", async () => {
  const daemon = new SessionDaemon();
  daemon.beginReply = { absorbed: true, applied: false, lmid: 5 };
  let ranPastRead = false;
  const server = api(daemon, {
    edit: async (tx) => {
      await tx.insert("issue", { id: "x", title: "y" });
      await tx.row("issue", { id: "x" });
      ranPastRead = true;
    },
  });

  const out = await server.pushMutation({ user: "u", envelope: envelope("edit", null) });
  assert.equal(out.accepted, true);
  assert.equal((out as { output: SqlTxnOutput }).output.applied, false);
  assert.equal(ranPastRead, false);
  assert.deepEqual(daemon.log, ["begin"]); // no exec/query/commit/rollback/executeSqlTxn
});

test("the absorbed LATCH is authoritative even when the body swallows the unwind", async () => {
  const daemon = new SessionDaemon();
  daemon.beginReply = { absorbed: true, applied: false, lmid: 5 };
  const server = api(daemon, {
    edit: async (tx) => {
      await tx.insert("issue", { id: "x", title: "y" });
      try {
        await tx.row("issue", { id: "x" });
      } catch {
        // A defensive mutator swallowing the control-flow unwind…
      }
      // …and blithely writing on: nothing may ship.
      await tx.insert("issue", { id: "z", title: "w" });
    },
  });

  const out = await server.pushMutation({ user: "u", envelope: envelope("edit", null) });
  assert.equal(out.accepted, true);
  assert.deepEqual(daemon.log, ["begin"]);
});

test("a business rejection rolls the session back BEFORE the lmid-only reject", async () => {
  const daemon = new SessionDaemon();
  const server = api(daemon, {
    guarded: async (tx) => {
      await tx.row("issue", { id: "i1" }); // upgrade
      throw new Error("not yours");
    },
  });

  const out = await server.pushMutation({ user: "u", envelope: envelope("guarded", null) });
  assert.equal(out.accepted, false);
  assert.equal((out as { reason: string }).reason, "not yours");
  // Order matters: the rollback releases the single writer the reject's commit needs.
  assert.deepEqual(daemon.log, ["begin", "rollback", "rejectMutation"]);
});

test("an infra failure mid-session rolls back and propagates (client retries; dedup absorbs)", async () => {
  const daemon = new SessionDaemon();
  daemon.failQueryWith = new Error("daemon restarted: 410");
  const server = api(daemon, {
    twoReads: async (tx) => {
      await tx.row("issue", { id: "a" }); // upgrade (begin answers)
      await tx.row("issue", { id: "b" }); // second read fails
    },
  });

  await assert.rejects(
    server.pushMutation({ user: "u", envelope: envelope("twoReads", null) }),
    /410/,
  );
  assert.deepEqual(daemon.log, ["begin", "query", "rollback"]);
});

test("a structured OCC commit conflict re-drives the whole read-bearing mutator", async () => {
  const daemon = new SessionDaemon();
  daemon.readReply = { cols: ["id", "title"], rows: [["i1", "current"]] };
  daemon.commitFailures.push(
    new DaemonHttpError(
      409,
      "Conflict",
      JSON.stringify({ error: "snapshot lost validation", code: "retryable-conflict", retryable: true }),
    ),
  );
  let runs = 0;
  const server = api(daemon, {
    edit: async (tx) => {
      runs++;
      const row = await tx.row("issue", { id: "i1" });
      await tx.update("issue", { id: "i1", title: `${row?.title}-next` });
    },
  });

  const out = await server.pushMutation({ user: "u", envelope: envelope("edit", null) });
  assert.equal(out.accepted, true);
  assert.equal(runs, 2, "the mutator body must establish its reads again");
  assert.deepEqual(daemon.log, ["begin", "exec", "commit", "begin", "exec", "commit"]);
});

test("an unclassified 409 is not treated as an OCC retry", async () => {
  const daemon = new SessionDaemon();
  daemon.commitFailures.push(new DaemonHttpError(409, "Conflict", JSON.stringify({ error: "mutation gap" })));
  let runs = 0;
  const server = api(daemon, {
    edit: async (tx) => {
      runs++;
      await tx.row("issue", { id: "i1" });
    },
  });

  await assert.rejects(server.pushMutation({ user: "u", envelope: envelope("edit", null) }), /mutation gap/);
  assert.equal(runs, 1);
});

test("tx.query compiles to ONE sqlite SELECT with bind params (no casts) and rides the session", async () => {
  const daemon = new SessionDaemon();
  daemon.readReply = {
    cols: ["rindle_result"],
    rows: [['[{"id":"i1","title":"t"}]']],
  };
  let result: unknown;
  const server = api(daemon, {
    report: async (tx) => {
      await tx.insert("issue", { id: "pre", title: "prefix" });
      result = await tx.query({
        table: "issue",
        where: { type: "simple", op: "=", left: { type: "column", name: "id" }, right: { type: "literal", value: "i1" } },
      } as never);
    },
  });

  const out = await server.pushMutation({ user: "u", envelope: envelope("report", null) });
  assert.equal(out.accepted, true);

  // The full-shape read was the FIRST read → it rode the begin, alongside the prefix.
  assert.deepEqual(daemon.log, ["begin", "commit"]);
  const compiled = daemon.begins[0].query!;
  assert.match(compiled.sql, /json_group_array|json_object/);
  assert.match(compiled.sql, /"rindle_result"/);
  // Bind params, never inlined literals (§5.4): the filter value travels as a parameter.
  assert.ok(!compiled.sql.includes("'i1'"), `literal leaked into SQL: ${compiled.sql}`);
  assert.deepEqual(compiled.params, ["i1"]);
  // No casting: raw storage-class cells, parsed from the single JSON column.
  assert.deepEqual(result, [{ id: "i1", title: "t" }]);
});

test("runSharedMutation: a generator yields a full tx.query — compiles to a SELECT, rides the session, and feeds rows back as an array", async () => {
  const daemon = new SessionDaemon();
  daemon.readReply = { cols: ["rindle_result"], rows: [['[{"id":"i1","title":"t"}]']] };
  const qb = newQueryBuilder(testSchema);
  let seen: unknown;
  function* reportShared(tx: IsoTx): MutationGen {
    yield tx.insert("issue", { id: "pre", title: "prefix" });
    seen = yield tx.query(qb.issue.where.id("i1")); // full-shape read inside the SHARED body
  }
  const server = api(daemon, {
    report: (tx) => runSharedMutation((t: IsoTx) => reportShared(t), null, { user: "u" }, tx),
  });

  const out = await server.pushMutation({ user: "u", envelope: envelope("report", null) });
  assert.equal(out.accepted, true);
  // The full-shape read was the FIRST read → it rode the begin (a real query-compiler SELECT).
  assert.deepEqual(daemon.log, ["begin", "commit"]);
  const compiled = daemon.begins[0].query!;
  assert.match(compiled.sql, /json_group_array|json_object/);
  assert.deepEqual(compiled.params, ["i1"]); // bind param, not an inlined literal
  // read-your-writes THROUGH THE GENERATOR DRIVE (runSharedMutation → driveMutationAsync → tx.query):
  // the query rows parsed and fed back to the body, as an ARRAY (the shared-seam contract).
  assert.deepEqual(seen, [{ id: "i1", title: "t" }]);
});

test("runSharedMutation: a `.one()` root query is normalized to a one-element ARRAY (byte-shape parity with the client)", async () => {
  const daemon = new SessionDaemon();
  // A `.one()` root: the server's tx.query returns a SCALAR object (not an array) for it.
  daemon.readReply = { cols: ["rindle_result"], rows: [['{"id":"i1","title":"t"}']] };
  const qb = newQueryBuilder(testSchema);
  let seen: unknown;
  function* reportOne(tx: IsoTx): MutationGen {
    seen = yield tx.query(qb.issue.where.id("i1").one());
  }
  const server = api(daemon, {
    report: (tx) => runSharedMutation((t: IsoTx) => reportOne(t), null, { user: "u" }, tx),
  });

  const out = await server.pushMutation({ user: "u", envelope: envelope("report", null) });
  assert.equal(out.accepted, true);
  // The shared seam normalizes the server's `.one()` scalar to `[obj]` so ONE body sees the same array
  // shape on both tiers — the client's WriteTxn.query never unwraps a root `.one()` (rust/src/wasm/db.rs).
  assert.deepEqual(seen, [{ id: "i1", title: "t" }]);
});

test("a daemon client without session support keeps the legacy committed-state read", async () => {
  const daemon = new SessionDaemon();
  const bare = daemon.client() as unknown as Record<string, unknown>;
  delete bare.beginMutationSession; // a client predating sessions
  const server = createRindleApiServer({
    daemon: bare as unknown as RindleDaemonClient,
    backend: daemonBackend(bare as unknown as RindleDaemonClient),
    schema: testSchema,
    mutators: defineApiMutators({
      edit: async (tx) => {
        await tx.row("issue", { id: "i1" });
        await tx.insert("issue", { id: "i2", title: "t" });
      },
    }),
  });

  const out = await server.pushMutation({ user: "u", envelope: envelope("edit", null) });
  assert.equal(out.accepted, true);
  assert.deepEqual(daemon.log, ["executeSqlRead", "executeSqlTxn"]);
});
