import assert from "node:assert/strict";
import { test } from "node:test";

import { createSchema, number, string, table } from "@rindle/client";
import { createSqlClient, type Fetch } from "@rindle/sql-client";

import { createRindleApiServer, defineApiMutators, scoped } from "../src/index.ts";
import { readOnlyDaemon } from "./helpers.ts";

interface Call {
  path: string;
  body: Record<string, unknown>;
}

function mutationSql(
  handler: (call: Call) => { status?: number; body: unknown },
): { database: { url: string; authToken: string; fetch: Fetch }; calls: Call[] } {
  const calls: Call[] = [];
  const fetch: Fetch = async (input, init = {}) => {
    const call: Call = {
      path: new URL(String(input)).pathname,
      body: JSON.parse(String(init.body ?? "{}")) as Record<string, unknown>,
    };
    calls.push(call);
    const result = handler(call);
    return new Response(JSON.stringify(result.body), {
      status: result.status ?? 200,
      headers: { "content-type": "application/json" },
    });
  };
  return {
    database: { url: "https://db.example", authToken: "secret", fetch },
    calls,
  };
}

function executeResponse(columns: string[] = [], rows: unknown[][] = []): unknown {
  return {
    result: {
      columns: columns.map((name) => ({ name, decltype: null })),
      rows,
      rows_affected: 0,
      last_insert_rowid: null,
      rows_read: null,
      rows_written: null,
    },
    commit_cursor: null,
    routing: { served_by: "master", applied_lag_ms: null, fence_fallback: false },
  };
}

test("api-server prefers its sql client for one-shot mutations and lmid-only rejections", async () => {
  const remote = mutationSql(({ path, body }) => {
    const mid = body.mid as number;
    if (path === "/v1/sql/mutations/execute") {
      return { body: { applied: true, cursor: `w:000000000000000${mid}`, lmid: mid } };
    }
    if (path === "/v1/sql/mutations/reject") {
      return { body: { applied: true, cursor: `w:000000000000000${mid}`, lmid: mid } };
    }
    throw new Error(`unexpected path ${path}`);
  });
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    database: remote.database,
    mutators: defineApiMutators({
      create: (tx, args: { id: string }) => tx.exec("INSERT INTO issue (id) VALUES (?)", [args.id]),
      denied: () => {
        throw new Error("nope");
      },
    }),
  });

  const accepted = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 1, name: "create", args: { id: "i1" } },
  });
  const denied = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 2, name: "denied", args: null },
  });
  const missing = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 3, name: "missing", args: null },
  });

  assert.equal(accepted.accepted, true);
  assert.deepEqual(accepted.accepted && accepted.output, {
    applied: true,
    cursor: "w:0000000000000001",
    lmid: 1,
    lmidAdvances: [{ clientID: "c1", lmid: 1 }],
  });
  assert.deepEqual([denied.accepted, denied.rejected && denied.reason], [false, "nope"]);
  assert.deepEqual([missing.accepted, missing.rejected && missing.reason], [false, "unknown mutator: missing"]);
  assert.deepEqual(
    remote.calls.map((call) => call.path),
    [
      "/v1/sql/mutations/execute",
      "/v1/sql/mutations/reject",
      "/v1/sql/mutations/reject",
    ],
  );
  assert.deepEqual(remote.calls[0]?.body, {
    client_id: "c1",
    mid: 1,
    statements: [{ sql: "INSERT INTO issue (id) VALUES (?)", args: ["i1"] }],
  });
});

test("sql-backed read-bearing mutators lazy-begin with their prefix and commit through one handle", async () => {
  const remote = mutationSql(({ path }) => {
    if (path === "/v1/sql/mutations/transactions") {
      return {
        body: {
          sessionId: "ms1",
          read: {
            cols: ["id", "title", "ownerId", "updatedAt"],
            rows: [["i1", "before", "u1", 1]],
          },
        },
      };
    }
    if (path === "/v1/sql/mutations/transactions/ms1/execute") return { body: {} };
    if (path === "/v1/sql/mutations/transactions/ms1/commit") {
      return { body: { applied: true, cursor: "w:0000000000000004", lmid: 4 } };
    }
    throw new Error(`unexpected path ${path}`);
  });
  const schema = createSchema({
    tables: [
      table("issue")
        .columns({ id: string(), title: string(), ownerId: string(), updatedAt: number() })
        .primaryKey("id"),
    ],
  });
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    database: remote.database,
    schema,
    mutators: defineApiMutators({
      update: async (tx) => {
        tx.exec("UPDATE issue SET updatedAt = ? WHERE id = ?", [2, "i1"]);
        const row = await tx.row("issue", { id: "i1" });
        assert.equal(row?.title, "before");
        tx.exec("UPDATE issue SET title = ? WHERE id = ?", ["after", "i1"]);
      },
    }),
  });

  const output = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 4, name: "update", args: null },
  });
  assert.equal(output.accepted, true);
  assert.deepEqual(
    remote.calls.map((call) => call.path),
    [
      "/v1/sql/mutations/transactions",
      "/v1/sql/mutations/transactions/ms1/execute",
      "/v1/sql/mutations/transactions/ms1/commit",
    ],
  );
  const begin = remote.calls[0]?.body;
  assert.equal(begin?.client_id, "c1");
  assert.equal(begin?.mid, 4);
  assert.equal((begin?.statements as unknown[]).length, 1, "the pure-write prefix rides begin");
  assert.match(String((begin?.query as { sql?: unknown }).sql), /^SELECT /);
  assert.equal((remote.calls[1]?.body.statements as unknown[]).length, 1, "post-read writes flush before commit");
});

test("tx.sql exposes transaction-bound raw reads and writes without a second client", async () => {
  const remote = mutationSql(({ path }) => {
    if (path === "/v1/sql/mutations/transactions") {
      return {
        body: {
          sessionId: "raw1",
          read: { cols: ["id", "revision"], rows: [["i1", 2]] },
        },
      };
    }
    if (path === "/v1/sql/mutations/transactions/raw1/execute") return { body: {} };
    if (path === "/v1/sql/mutations/transactions/raw1/commit") {
      return { body: { applied: true, cursor: "w:0000000000000005", lmid: 5 } };
    }
    throw new Error(`unexpected path ${path}`);
  });
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    database: remote.database,
    mutators: defineApiMutators({
      revise: async (tx) => {
        await tx.sql.execute("UPDATE issue SET revision = revision + 1 WHERE id = ?", ["i1"]);
        const rows = await tx.sql.query<{ id: string; revision: number }>(
          "SELECT id, revision FROM issue WHERE id = ?",
          ["i1"],
        );
        assert.deepEqual(rows, [{ id: "i1", revision: 2 }]);
        await tx.sql.batch([
          { sql: "INSERT INTO audit (issue_id, action) VALUES (?, ?)", params: ["i1", "revised"] },
          { sql: "UPDATE issue SET touched = 1 WHERE id = ?", params: ["i1"] },
        ]);
      },
    }),
  });

  const output = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 5, name: "revise", args: null },
  });
  assert.equal(output.accepted, true);
  assert.deepEqual(
    remote.calls.map((call) => call.path),
    [
      "/v1/sql/mutations/transactions",
      "/v1/sql/mutations/transactions/raw1/execute",
      "/v1/sql/mutations/transactions/raw1/commit",
    ],
  );
  assert.deepEqual(remote.calls[0]?.body.statements, [
    { sql: "UPDATE issue SET revision = revision + 1 WHERE id = ?", args: ["i1"] },
  ]);
  assert.deepEqual(remote.calls[0]?.body.query, {
    sql: "SELECT id, revision FROM issue WHERE id = ?",
    args: ["i1"],
  });
  assert.equal((remote.calls[1]?.body.statements as unknown[]).length, 2);
});

test("scope.sql runs separately from the mutator transaction", async () => {
  const remote = mutationSql(({ path, body }) => {
    if (path === "/v1/sql/execute") {
      const statement = body.statement as { sql: string };
      if (statement.sql.startsWith("SELECT")) return { body: executeResponse(["next"], [[7]]) };
      return { body: executeResponse() };
    }
    if (path === "/v1/sql/mutations/execute") {
      return { body: { applied: true, cursor: "w:0000000000000006", lmid: 6 } };
    }
    throw new Error(`unexpected path ${path}`);
  });
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    database: remote.database,
    mutators: defineApiMutators({
      create: scoped(async (scope) => {
        const rows = await scope.sql.query<{ next: number }>("SELECT ? AS next", [7]);
        assert.deepEqual(rows, [{ next: 7 }]);
        await scope.sql.execute("INSERT INTO audit (id) VALUES (?)", ["a1"]);
        await scope.transact((tx) => tx.sql.execute("INSERT INTO issue (id) VALUES (?)", ["i1"]));
      }),
    }),
  });

  const output = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 6, name: "create", args: null },
  });
  assert.equal(output.accepted, true);
  assert.deepEqual(
    remote.calls.map((call) => call.path),
    ["/v1/sql/execute", "/v1/sql/execute", "/v1/sql/mutations/execute"],
  );
  assert.deepEqual((remote.calls[0]?.body.statement as Record<string, unknown>).want_rows, true);
  assert.deepEqual(remote.calls[2]?.body.statements, [
    { sql: "INSERT INTO issue (id) VALUES (?)", args: ["i1"] },
  ]);
});

test("scope.sql failures before transact remain infrastructure and do not burn lmid", async () => {
  const remote = mutationSql(({ path }) => {
    assert.equal(path, "/v1/sql/execute");
    return {
      status: 500,
      body: { code: "STATEMENT_FAILED", message: "outside writer unavailable", retry_scope: "never" },
    };
  });
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    database: remote.database,
    mutators: defineApiMutators({
      create: scoped(async (scope) => {
        await scope.sql.execute("INSERT INTO audit (id) VALUES (?)", ["a1"]);
      }),
    }),
  });

  await assert.rejects(
    api.pushMutation({
      user: "u1",
      envelope: { clientID: "c1", mid: 1, name: "create", args: null },
    }),
    /outside writer unavailable/,
  );
  assert.deepEqual(remote.calls.map((call) => call.path), ["/v1/sql/execute"]);
});

test("api.close closes the SQL client constructed from database config", async () => {
  const remote = mutationSql(({ path }) => {
    throw new Error(`closed client unexpectedly fetched ${path}`);
  });
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    database: remote.database,
    mutators: defineApiMutators({ touch: (tx) => tx.sql.execute("UPDATE issue SET n = n + 1") }),
  });

  api.close();
  api.close();
  await assert.rejects(
    api.pushMutation({
      user: "u1",
      envelope: { clientID: "c1", mid: 1, name: "touch", args: null },
    }),
    /SQL client is closed/,
  );
  assert.deepEqual(remote.calls, []);
});

test("api.close leaves an advanced injected SQL session caller-owned", async () => {
  const remote = mutationSql(({ path }) => {
    assert.equal(path, "/v1/sql/mutations/execute");
    return { body: { applied: true, cursor: "w:0000000000000001", lmid: 1 } };
  });
  const sql = createSqlClient(remote.database);
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    sql,
    mutators: defineApiMutators({ touch: (tx) => tx.sql.execute("UPDATE issue SET n = n + 1") }),
  });

  api.close();
  const output = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 1, name: "touch", args: null },
  });
  assert.equal(output.accepted, true);
  sql.close();
});

test("sql-backed mutation transport failures remain infrastructure failures and never burn lmid", async () => {
  const remote = mutationSql(({ path }) => {
    assert.equal(path, "/v1/sql/mutations/execute");
    return {
      status: 500,
      body: { code: "STATEMENT_FAILED", message: "writer unavailable", retry_scope: "never" },
    };
  });
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    database: remote.database,
    mutators: defineApiMutators({ touch: (tx) => tx.exec("UPDATE issue SET n = n + 1") }),
  });

  await assert.rejects(
    api.pushMutation({
      user: "u1",
      envelope: { clientID: "c1", mid: 1, name: "touch", args: null },
    }),
    /writer unavailable/,
  );
  assert.deepEqual(remote.calls.map((call) => call.path), ["/v1/sql/mutations/execute"]);
});

test("sql-backed mutation OCC conflicts re-drive the whole read-bearing mutator", async () => {
  let begins = 0;
  let runs = 0;
  const remote = mutationSql(({ path }) => {
    if (path === "/v1/sql/mutations/transactions") {
      begins += 1;
      return {
        body: {
          sessionId: `ms${begins}`,
          read: { cols: ["id", "title"], rows: [["i1", "current"]] },
        },
      };
    }
    if (path === "/v1/sql/mutations/transactions/ms1/commit") {
      return {
        status: 409,
        body: { error: "snapshot conflict", code: "retryable-conflict", retryable: true },
      };
    }
    if (path === "/v1/sql/mutations/transactions/ms2/commit") {
      return { body: { applied: true, cursor: "w:0000000000000001", lmid: 1 } };
    }
    throw new Error(`unexpected path ${path}`);
  });
  const schema = createSchema({
    tables: [table("issue").columns({ id: string(), title: string() }).primaryKey("id")],
  });
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    database: remote.database,
    schema,
    mutators: defineApiMutators({
      inspect: async (tx) => {
        runs += 1;
        assert.equal((await tx.row("issue", { id: "i1" }))?.title, "current");
      },
    }),
  });

  const output = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 1, name: "inspect", args: null },
  });
  assert.equal(output.accepted, true);
  assert.equal(runs, 2);
  assert.equal(begins, 2);
});

test("an unencodable bind is a business rejection, not an infra failure that wedges the queue", async () => {
  const remote = mutationSql(({ path, body }) => {
    const mid = body.mid as number;
    if (path === "/v1/sql/mutations/reject") {
      return { body: { applied: true, cursor: null, lmid: mid } };
    }
    throw new Error(`unexpected path ${path}`);
  });
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    database: remote.database,
    mutators: defineApiMutators({
      // `undefined` is the day-one case: an optional mutator arg left unset. The daemon's JSON
      // encoder coerced it to NULL; the SQL codec refuses it outright.
      undef: (tx, args: { id: string }) =>
        tx.exec("INSERT INTO issue (id, assignee) VALUES (?, ?)", [args.id, undefined as never]),
      when: (tx) => tx.exec("INSERT INTO issue (at) VALUES (?)", [new Date(0) as never]),
    }),
  });

  const undef = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 1, name: "undef", args: { id: "i1" } },
  });
  const when = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 2, name: "when", args: null },
  });

  // Rejected (not thrown) is what advances lmid and retires the browser's optimistic prediction.
  assert.equal(undef.accepted, false);
  assert.match(String(undef.rejected && undef.reason), /bind 1 .* cannot be stored/);
  assert.equal(when.accepted, false);
  assert.match(String(when.rejected && when.reason), /bind 0 .* cannot be stored/);

  // The refusal happens before transport: no statement was ever shipped, only the lmid-only commits.
  assert.deepEqual(
    remote.calls.map((call) => call.path),
    ["/v1/sql/mutations/reject", "/v1/sql/mutations/reject"],
  );
});

test("collapse-sql: a refused statement CLASS is a business rejection, never a shipped poison", async () => {
  const remote = mutationSql(({ path, body }) => {
    const mid = body.mid as number;
    if (path === "/v1/sql/mutations/reject") {
      return { body: { applied: true, cursor: `w:000000000000000${mid}`, lmid: mid } };
    }
    // The refused statement must never reach the transport (that is the poison the guard prevents).
    throw new Error(`unexpected path ${path}`);
  });
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    database: remote.database,
    mutators: defineApiMutators({
      pragma: (tx) => tx.exec("PRAGMA defer_foreign_keys=ON"),
      read: (tx) => tx.exec("SELECT 1"),
    }),
  });

  const pragma = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 1, name: "pragma", args: null },
  });
  const read = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 2, name: "read", args: null },
  });

  assert.equal(pragma.accepted, false);
  assert.match(String(pragma.rejected && pragma.reason), /cannot begin with PRAGMA/);
  assert.equal(read.accepted, false);
  assert.match(String(read.rejected && read.reason), /cannot begin with SELECT/);
  assert.deepEqual(
    remote.calls.map((call) => call.path),
    ["/v1/sql/mutations/reject", "/v1/sql/mutations/reject"],
  );
});

test("collapse-sql: an unencodable READ bind is a business rejection, not an infra poison", async () => {
  const remote = mutationSql(({ path, body }) => {
    const mid = body.mid as number;
    if (path === "/v1/sql/mutations/reject") {
      return { body: { applied: true, cursor: `w:000000000000000${mid}`, lmid: mid } };
    }
    // begin/query must never be reached: the bad bind is refused before the read ships.
    throw new Error(`unexpected path ${path}`);
  });
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    database: remote.database,
    mutators: defineApiMutators({
      readDate: async (tx) => {
        await tx.sql.query("SELECT id FROM issue WHERE created > ?", [new Date() as never]);
      },
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 1, name: "readDate", args: null },
  });

  assert.equal(out.accepted, false);
  assert.match(String(out.rejected && out.reason), /bind 0 .* cannot be stored/);
  assert.deepEqual(
    remote.calls.map((call) => call.path),
    ["/v1/sql/mutations/reject"],
  );
});

test("collapse-sql: a duplicate projected column is refused loudly instead of silently collapsing", async () => {
  const remote = mutationSql(({ path, body }) => {
    const mid = body.mid as number;
    if (path === "/v1/sql/mutations/transactions") {
      // Two columns named `status`: keyedSqlRows would otherwise keep only the last.
      return { body: { sessionId: "ms1", read: { cols: ["status", "status"], rows: [["open", "closed"]] } } };
    }
    if (path === "/v1/sql/mutations/transactions/ms1/rollback") return { body: {} };
    if (path === "/v1/sql/mutations/reject") {
      return { body: { applied: true, cursor: `w:000000000000000${mid}`, lmid: mid } };
    }
    throw new Error(`unexpected path ${path}`);
  });
  const api = createRindleApiServer({
    daemon: readOnlyDaemon(),
    database: remote.database,
    mutators: defineApiMutators({
      dupRead: async (tx) => {
        await tx.sql.query("SELECT parent.status, child.status FROM issue parent JOIN issue child");
      },
    }),
  });

  const out = await api.pushMutation({
    user: "u1",
    envelope: { clientID: "c1", mid: 1, name: "dupRead", args: null },
  });

  assert.equal(out.accepted, false);
  assert.match(String(out.rejected && out.reason), /more than once/);
});
