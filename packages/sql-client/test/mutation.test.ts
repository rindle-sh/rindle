import assert from "node:assert/strict";
import { test } from "node:test";

import { RindleSqlError, createSqlClient } from "../src/index.ts";
import { response, scriptedFetch } from "./helpers.ts";

test("mutation one-shot and rejection carry clientId+mid and advance the session fence", async () => {
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/mutations/execute") {
      return response({ applied: true, cursor: "w:0000000000000004", lmid: 4 });
    }
    if (path === "/v1/sql/mutations/reject") {
      return response({ applied: true, cursor: "w:0000000000000005", lmid: 5 });
    }
    throw new Error(`unexpected path ${path}`);
  });
  const sql = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });

  assert.deepEqual(
    await sql.executeMutation({
      clientId: "c1",
      mid: 4,
      statements: [{ sql: "insert into issue(id, done) values (?, ?)", args: [9n, true] }],
    }),
    { applied: true, lmid: 4, commitCursor: "w:0000000000000004" },
  );
  assert.deepEqual(mock.calls[0]?.body, {
    client_id: "c1",
    mid: 4,
    statements: [
      {
        sql: "insert into issue(id, done) values (?, ?)",
        args: [
          { $rindle: "i64", value: "9" },
          { $rindle: "i64", value: "1" },
        ],
      },
    ],
  });

  assert.deepEqual(await sql.rejectMutation({ clientId: "c1", mid: 5, reason: "denied" }), {
    applied: true,
    lmid: 5,
    commitCursor: "w:0000000000000005",
  });
  assert.deepEqual(mock.calls[1]?.body, { client_id: "c1", mid: 5, reason: "denied" });
  assert.equal(sql.getSessionCursor(), "w:0000000000000005");
});

test("mutation begin exposes absorbed replay without opening a transaction", async () => {
  const mock = scriptedFetch(() => response({ absorbed: true, applied: false, lmid: 7 }));
  const sql = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });

  const result = await sql.beginMutation({ clientId: "c1", mid: 7 });
  assert.deepEqual(result, {
    absorbed: true,
    receipt: { applied: false, lmid: 7, commitCursor: null },
  });
  assert.equal(mock.calls[0]?.url, "https://db.example/v1/sql/mutations/transactions");
});

test("interactive mutation transactions preserve the lazy prefix/read and commit lmid atomically", async () => {
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/mutations/transactions") {
      return response({
        sessionId: "ms/opaque",
        read: { cols: ["id", "enabled"], rows: [[1, true]] },
      });
    }
    if (path.endsWith("/execute")) return response({});
    if (path.endsWith("/query")) return response({ cols: ["n"], rows: [[3]] });
    if (path.endsWith("/commit")) {
      return response({ applied: true, cursor: "w:0000000000000008", lmid: 8 });
    }
    throw new Error(`unexpected path ${path}`);
  });
  const sql = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });

  const begun = await sql.beginMutation({
    clientId: "c1",
    mid: 8,
    statements: [{ sql: "update issue set n = ? where id = ?", args: [2, 1] }],
    query: { sql: "select id, enabled from issue where id = ?", args: [1] },
  });
  assert.equal(begun.absorbed, false);
  if (begun.absorbed) throw new Error("expected an open transaction");
  assert.deepEqual(begun.read, { columns: ["id", "enabled"], rows: [[1, true]] });

  await begun.transaction.batch([
    { sql: "update issue set n = n + 1 where id = ?", args: [1] },
    { sql: "update issue set enabled = ? where id = ?", args: [false, 1] },
  ]);
  assert.deepEqual(await begun.transaction.query("select n from issue where id = 1"), {
    columns: ["n"],
    rows: [[3]],
  });
  assert.deepEqual(await begun.transaction.commit(), {
    applied: true,
    lmid: 8,
    commitCursor: "w:0000000000000008",
  });
  assert.equal(sql.getSessionCursor(), "w:0000000000000008");

  assert.deepEqual(mock.calls[0]?.body, {
    client_id: "c1",
    mid: 8,
    statements: [
      {
        sql: "update issue set n = ? where id = ?",
        args: [
          { $rindle: "i64", value: "2" },
          { $rindle: "i64", value: "1" },
        ],
      },
    ],
    query: {
      sql: "select id, enabled from issue where id = ?",
      args: [{ $rindle: "i64", value: "1" }],
    },
  });
  assert.equal(
    new URL(mock.calls[1]!.url).pathname,
    "/v1/sql/mutations/transactions/ms%2Fopaque/execute",
  );
  await assert.rejects(
    begun.transaction.execute("select 1"),
    (error: unknown) => error instanceof RindleSqlError && error.code === "TRANSACTION_CLOSED",
  );
});

test("a 5xx leaves the mutation transaction owing a rollback that actually goes out", async () => {
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/mutations/transactions") return response({ sessionId: "ms0-0-4" });
    // The writer is still held server-side: an unanswered request is NOT a declared closure.
    if (path.endsWith("/execute")) return response({ error: "backend unavailable" }, 503);
    if (path.endsWith("/rollback")) return response(undefined, 200);
    throw new Error(`unexpected path ${path}`);
  });
  const sql = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });

  const begun = await sql.beginMutation({ clientId: "c1", mid: 4 });
  if (begun.absorbed) throw new Error("expected an open transaction");
  await assert.rejects(begun.transaction.execute("update issue set n = 1"));

  // No further work may be queued onto a transaction whose state is unknown...
  await assert.rejects(
    begun.transaction.execute("update issue set n = 2"),
    (error: unknown) => error instanceof RindleSqlError && error.code === "TRANSACTION_CLOSED",
  );
  // ...but the rollback MUST still reach the server, or the writer leaks until its deadline.
  await begun.transaction.rollback();
  assert.equal(
    mock.calls.filter((call) => new URL(call.url).pathname.endsWith("/rollback")).length,
    1,
    "an unknown-state mutation transaction must still issue its rollback",
  );
});

test("a 410 retires the rollback obligation instead of reporting failure", async () => {
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/mutations/transactions") return response({ sessionId: "ms0-0-5" });
    if (path.endsWith("/execute")) return response({ error: "gone" }, 410);
    throw new Error(`unexpected path ${path}`);
  });
  const sql = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });

  const begun = await sql.beginMutation({ clientId: "c1", mid: 5 });
  if (begun.absorbed) throw new Error("expected an open transaction");
  await assert.rejects(begun.transaction.execute("update issue set n = 1"));

  await begun.transaction.rollback();
  assert.equal(
    mock.calls.filter((call) => new URL(call.url).pathname.endsWith("/rollback")).length,
    0,
    "a server-declared closure owes no rollback",
  );
});

test("mutation identity validation fails before transport", async () => {
  const mock = scriptedFetch(() => response({}));
  const sql = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  await assert.rejects(
    sql.executeMutation({ clientId: "", mid: 0, statements: [] }),
    /clientId must be a non-empty string/,
  );
  assert.equal(mock.calls.length, 0);
});
