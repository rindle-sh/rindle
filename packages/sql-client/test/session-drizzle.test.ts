import assert from "node:assert/strict";
import { test } from "node:test";

import { RindleSqlError, createSqlClient, type SqlClient, type SqlTransaction, type StatementResult } from "../src/index.ts";
import { createDrizzleClient, toDrizzleResultSet } from "../src/drizzle.ts";
import { executeResponse, response, scriptedFetch } from "./helpers.ts";

test("session contexts keep per-user cursors independent and support a stateless override", async () => {
  let cursor = 0;
  const mock = scriptedFetch(() => response(executeResponse(`cursor-${++cursor}`)));
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", sessionCursor: "client-seed", fetch: mock.fetch });
  const alice = client.session("alice-seed");
  const bob = client.session("bob-seed");

  await alice.execute("update t set v = 1");
  await bob.execute("update t set v = 2");
  await alice.execute("select * from t", { sessionCursor: undefined });
  await alice.execute("select * from t", { sessionCursor: null });
  await alice.execute("select * from t", { sessionCursor: "stateless-fence" });

  assert.equal(mock.calls[0]?.body?.session_cursor, "alice-seed");
  assert.equal(mock.calls[1]?.body?.session_cursor, "bob-seed");
  assert.equal(mock.calls[2]?.body?.session_cursor, "cursor-1", "explicit undefined inherits the live session fence");
  assert.equal(mock.calls[3]?.body?.session_cursor, null, "explicit null remains a stateless override");
  assert.equal(mock.calls[4]?.body?.session_cursor, "stateless-fence");
  assert.equal(alice.getSessionCursor(), "cursor-5");
  assert.equal(bob.getSessionCursor(), "cursor-2");
  assert.equal(client.getSessionCursor(), "client-seed");
  alice.resetSessionCursor();
  assert.equal(alice.getSessionCursor(), null);
});

test("Drizzle result conversion builds positional + named hybrid rows", () => {
  const native: StatementResult = {
    columns: [
      { name: "id", decltype: "INTEGER" },
      { name: "name", decltype: "TEXT" },
      { name: "name", decltype: "TEXT" },
    ],
    rows: [[7n, "Ada", "Lovelace"]],
    rowsAffected: 1,
    lastInsertRowid: "7",
    rowsRead: 1,
    rowsWritten: 0,
  };
  const result = toDrizzleResultSet(native);
  assert.deepEqual([...result.rows[0]!], [7, "Ada", "Lovelace"]);
  assert.equal(result.rows[0]!.id, 7);
  assert.equal(result.rows[0]!.name, "Lovelace", "duplicate aliases use the last column like libSQL hybrid rows");
  assert.deepEqual(result.columnTypes, ["INTEGER", "TEXT", "TEXT"]);
  assert.equal(result.lastInsertRowid, 7n);
});

test("Drizzle result conversion rejects integers its number-based mappers cannot represent", () => {
  const native: StatementResult = {
    columns: [{ name: "id", decltype: "INTEGER" }],
    rows: [[9_007_199_254_740_992n]],
    rowsAffected: 0,
    lastInsertRowid: null,
    rowsRead: 1,
    rowsWritten: 0,
  };
  assert.throws(
    () => toDrizzleResultSet(native),
    (error: unknown) => error instanceof RindleSqlError && error.code === "VALUE_UNSUPPORTED" && /Drizzle/.test(error.message),
  );
});

test("Drizzle-created native clients stay lossless; the facade converts per column", async () => {
  // Design 226 §7.2: the internal client uses intMode "bigint" so precision is
  // not discarded before result metadata is considered — the `.rindle` escape
  // hatch sees the exact integer, and toDrizzleResultSet's per-column decltype
  // dispatch converts an ordinary INTEGER column to Drizzle's safe number.
  const wireResult = {
    columns: [{ name: "id", decltype: "INTEGER" }],
    rows: [[{ $rindle: "i64", value: "7" }]],
    rows_affected: 0,
    last_insert_rowid: null,
    rows_read: 1,
    rows_written: 0,
  };
  const mock = scriptedFetch(() => response({ ...executeResponse(null), result: wireResult }));
  const drizzle = createDrizzleClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  const native = await drizzle.rindle.execute("select 7 as id");
  assert.deepEqual(native.result.rows, [[7n]], "the native lane is lossless");

  const mock2 = scriptedFetch(() => response({ ...executeResponse(null), result: wireResult }));
  const drizzle2 = createDrizzleClient({ url: "https://db.example", authToken: "secret", fetch: mock2.fetch });
  const converted = await drizzle2.execute("select 7 as id");
  assert.deepEqual([...converted.rows[0]!], [7], "the facade's INTEGER column arm is a safe number");
});

test("Drizzle hybrid rows keep positional cells when aliases look like array indexes", () => {
  const native: StatementResult = {
    columns: [
      { name: "1", decltype: "TEXT" },
      { name: "value", decltype: "TEXT" },
    ],
    rows: [["first", "second"]],
    rowsAffected: 0,
    lastInsertRowid: null,
    rowsRead: 1,
    rowsWritten: 0,
  };
  const result = toDrizzleResultSet(native);
  assert.deepEqual([...result.rows[0]!], ["first", "second"]);
  assert.equal(result.rows[0]!.value, "second");
});

test("Drizzle facade delegates execute and maps read transactions to typed readOnly begin", async () => {
  const emptyResult: StatementResult = {
    columns: [],
    rows: [],
    rowsAffected: 0,
    lastInsertRowid: null,
    rowsRead: null,
    rowsWritten: null,
  };
  const begins: unknown[] = [];
  const statements: unknown[] = [];
  const tx: SqlTransaction = {
    execute: async (statement) => {
      statements.push(statement);
      return emptyResult;
    },
    batch: async () => [emptyResult],
    commit: async () => ({ commitCursor: null }),
    rollback: async () => {},
  };
  const native = {
    execute: async (statement: unknown) => {
      statements.push(statement);
      return {
        result: emptyResult,
        commitCursor: null,
        routing: { servedBy: "master", appliedLagMs: null, fenceFallback: false },
      };
    },
    batch: async () => ({
      results: [emptyResult],
      commitCursor: null,
      routing: { servedBy: "master", appliedLagMs: null, fenceFallback: false },
    }),
    begin: async (options: unknown) => {
      begins.push(options);
      return tx;
    },
    withTransaction: async () => undefined,
    withTransactionRetry: async () => undefined,
    executeDdl: async () => ({
      result: emptyResult,
      commitCursor: null,
      routing: { servedBy: "master", appliedLagMs: null, fenceFallback: false },
    }),
    migrate: async () => ({ applied: true, commitCursor: "c" }),
    executeMultiple: async () => {},
    session: () => native,
    getSessionCursor: () => null,
    resetSessionCursor: () => {},
    ping: async () => {},
    close: () => {},
  } as unknown as SqlClient;

  const drizzle = createDrizzleClient(native);
  await drizzle.execute({ sql: "select ?", args: [1] });
  const drizzleTx = await drizzle.transaction("read");
  await drizzleTx.execute("select 1");
  await drizzleTx.commit();

  assert.deepEqual(begins, [{ readOnly: true }]);
  assert.deepEqual(statements, [{ sql: "select ?", args: [1] }, "select 1"]);
});
