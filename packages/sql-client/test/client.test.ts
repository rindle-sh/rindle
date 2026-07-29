import assert from "node:assert/strict";
import { test } from "node:test";

import { RindleSqlError, createSqlClient } from "../src/index.ts";
import { executeResponse, response, scriptedFetch, wireStatementResult } from "./helpers.ts";

const CANONICAL_REQUEST_ID = /^rid1\.[0-9a-f]{32}$/;
const CANONICAL_IDEMPOTENCY_KEY = /^sql1\.[0-9]{13}\.[0-9a-f]{32}$/;

test("execute emits the frozen v1 DTO and decodes snake_case results", async () => {
  const mock = scriptedFetch(() =>
    response(
      executeResponse(
        "cursor-1",
        wireStatementResult(
          [
            { name: "id", decltype: "INTEGER" },
            { name: "ratio", decltype: "REAL" },
          ],
          [[{ $rindle: "i64", value: "7" }, { $rindle: "float", value: "Infinity" }]],
          { rows_affected: 1, last_insert_rowid: "7", rows_read: 1, rows_written: 1 },
        ),
      ),
    ),
  );
  const client = createSqlClient({ url: "https://db.example/", authToken: "secret", fetch: mock.fetch });
  const result = await client.execute({ sql: "insert into t(v) values (?) returning id, ratio", args: [7n], wantRows: true });

  assert.equal(mock.calls[0]?.url, "https://db.example/v1/sql/execute");
  const headers = new Headers(mock.calls[0]?.init.headers);
  assert.equal(headers.get("authorization"), "Bearer secret");
  const requestId = headers.get("rindle-request-id") ?? "";
  assert.match(requestId, CANONICAL_REQUEST_ID);
  assert.equal(requestId.length, 37);
  assert.deepEqual(mock.calls[0]?.body?.statement, {
    sql: "insert into t(v) values (?) returning id, ratio",
    args: [{ $rindle: "i64", value: "7" }],
    want_rows: true,
  });
  assert.equal(mock.calls[0]?.body?.consistency, undefined, "the client-wide read default is not an explicit write option");
  assert.equal(mock.calls[0]?.body?.default_consistency, "session");
  const idempotencyKey = String(mock.calls[0]?.body?.idempotency_key);
  assert.match(idempotencyKey, CANONICAL_IDEMPOTENCY_KEY);
  assert.equal(idempotencyKey.length, 51);
  assert.equal(mock.calls[0]?.body?.operation_id, undefined, "one-shot identity has no ignored operation_id alias");
  assert.deepEqual(result.result.rows, [[7n, Infinity]]);
  assert.equal(result.result.rowsAffected, 1);
  assert.equal(result.result.lastInsertRowid, "7");
  assert.equal(result.commitCursor, "cursor-1");
  assert.equal(client.getSessionCursor(), "cursor-1");
});

test("batch, DDL, migration, scripts, and ping use their frozen routes and identities", async () => {
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/batch") {
      return response({
        results: [wireStatementResult([{ name: "v", decltype: null }], [[1]])],
        commit_cursor: "prior-cursor",
        routing: { served_by: "follower", applied_lag_ms: 12, fence_fallback: false },
      });
    }
    if (path === "/v1/sql/execute") return response(executeResponse("ddl-cursor"));
    if (path === "/v1/sql/migrate") return response({ applied: true, commit_cursor: "migration-cursor" });
    if (path === "/v1/sql/execute-multiple") return response({ commit_cursor: "script-cursor" });
    if (path === "/version") return response({ version: "test" });
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", consistency: "eventual", fetch: mock.fetch });

  const batch = await client.batch([{ sql: "select 1" }]);
  assert.equal(batch.routing.servedBy, "follower");
  assert.equal(batch.routing.appliedLagMs, 12);
  assert.equal(mock.calls[0]?.body?.consistency, undefined);
  assert.equal(mock.calls[0]?.body?.default_consistency, "eventual");
  assert.match(String(mock.calls[0]?.body?.idempotency_key), CANONICAL_IDEMPOTENCY_KEY);
  assert.equal(mock.calls[0]?.body?.operation_id, undefined);

  await client.executeDdl("create table t(id integer primary key)");
  assert.equal(mock.calls[1]?.body?.consistency, undefined, "explicit DDL never inherits a read-routing default");
  assert.equal(mock.calls[1]?.body?.session_cursor, undefined);
  assert.match(String(mock.calls[1]?.body?.idempotency_key), CANONICAL_IDEMPOTENCY_KEY);
  assert.equal(mock.calls[1]?.body?.operation_id, undefined);

  assert.deepEqual(
    await client.migrate({ id: "001", checksum: "sha256:abc", statements: ["create table u(id integer primary key)"] }),
    { applied: true, commitCursor: "migration-cursor" },
  );
  assert.deepEqual(mock.calls[2]?.body, {
    id: "001",
    checksum: "sha256:abc",
    statements: ["create table u(id integer primary key)"],
  });

  await client.executeMultiple("insert into t values (1); insert into t values (2)");
  assert.equal(client.getSessionCursor(), "script-cursor");
  assert.match(String(mock.calls[3]?.body?.idempotency_key), CANONICAL_IDEMPOTENCY_KEY);
  assert.equal(mock.calls[3]?.body?.operation_id, undefined);

  await client.ping();
  assert.equal(new Headers(mock.calls[4]?.init.headers).get("authorization"), null, "ping is unauthenticated");
  assert.equal(mock.calls[4]?.init.method, "GET");
  const requestIds = mock.calls.map((call) => new Headers(call.init.headers).get("rindle-request-id") ?? "");
  assert.ok(requestIds.every((requestId) => CANONICAL_REQUEST_ID.test(requestId)));
  assert.equal(new Set(requestIds).size, requestIds.length, "each logical request gets a fresh request id");
});

test("executeMultiple requires the frozen commit cursor response", async () => {
  const missing = scriptedFetch(() => response({}));
  const missingClient = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: missing.fetch });
  await assert.rejects(
    missingClient.executeMultiple("select 1"),
    (error: unknown) => error instanceof RindleSqlError && error.code === "PROTOCOL_ERROR",
  );

  const empty = scriptedFetch(() => response(undefined, 204));
  const emptyClient = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: empty.fetch });
  await assert.rejects(
    emptyClient.executeMultiple("select 1"),
    (error: unknown) => error instanceof RindleSqlError && error.code === "PROTOCOL_ERROR",
  );
});

test("batch rejects a success response with the wrong result cardinality", async () => {
  const mock = scriptedFetch(() =>
    response({
      results: [],
      commit_cursor: null,
      routing: { served_by: "master", applied_lag_ms: null, fence_fallback: false },
    }),
  );
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  await assert.rejects(
    client.batch([{ sql: "select 1" }]),
    (error: unknown) => error instanceof RindleSqlError && error.code === "PROTOCOL_ERROR",
  );
});

test("server error envelopes retain stable fields and request ids", async () => {
  const mock = scriptedFetch(() =>
    response(
      {
        error: {
          code: "READ_ONLY_TRANSACTION",
          message: "writes are not allowed",
          sqlite_code: 8,
          retry_scope: "never",
          transaction_state: "open",
        },
      },
      409,
      { "x-request-id": "proxy-hop-42" },
    ),
  );
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  await assert.rejects(
    client.execute("update t set v = 1"),
    (error: unknown) => {
      assert.ok(error instanceof RindleSqlError);
      assert.equal(error.code, "READ_ONLY_TRANSACTION");
      assert.equal(error.sqliteCode, 8);
      assert.equal(error.retryScope, "never");
      assert.equal(error.transactionState, "open");
      assert.equal(error.status, 409);
      assert.match(error.requestId ?? "", CANONICAL_REQUEST_ID);
      assert.equal(error.requestId, new Headers(mock.calls[0]?.init.headers).get("rindle-request-id"));
      assert.notEqual(error.requestId, "proxy-hop-42");
      return true;
    },
  );
  assert.equal(mock.calls.length, 1);
});

test("minimal intake errors drive bounded 5xx retries without leaking an unusable retry scope", async () => {
  const mock = scriptedFetch(() => response({ error: "write pool is full" }, 503));
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  await assert.rejects(
    client.execute("select 1"),
    (error: unknown) => {
      assert.ok(error instanceof RindleSqlError);
      assert.equal(error.code, "HTTP_503");
      assert.equal(error.message, "write pool is full");
      assert.equal(error.retryScope, "never");
      return true;
    },
  );
  assert.equal(mock.calls.length, 3, "the inferred request scope drives the bounded retry loop");
});

test("request-scope retry preserves idempotency and logical request identities", async () => {
  const mock = scriptedFetch((_call, index) =>
    index === 0
      ? response({ code: "BACKPRESSURE", message: "busy", retry_scope: "request" }, 503, { "retry-after": "0" })
      : response(executeResponse(null)),
  );
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  await client.execute("update t set v = 1");
  assert.equal(mock.calls.length, 2);
  assert.equal(mock.calls[0]?.body?.idempotency_key, mock.calls[1]?.body?.idempotency_key);
  assert.match(String(mock.calls[0]?.body?.idempotency_key), CANONICAL_IDEMPOTENCY_KEY);
  assert.equal(mock.calls[0]?.body?.operation_id, undefined);
  const firstRequestId = new Headers(mock.calls[0]?.init.headers).get("rindle-request-id");
  const retriedRequestId = new Headers(mock.calls[1]?.init.headers).get("rindle-request-id");
  assert.match(firstRequestId ?? "", CANONICAL_REQUEST_ID);
  assert.equal(retriedRequestId, firstRequestId);
});

test("an exhausted one-shot transport does not invite an unsafe new logical invocation", async () => {
  const mock = scriptedFetch(() => {
    throw new Error("network unavailable");
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  await assert.rejects(
    client.execute("update t set v = 1"),
    (error: unknown) =>
      error instanceof RindleSqlError && error.code === "TRANSPORT_ERROR" && error.retryScope === "never",
  );
  assert.equal(mock.calls.length, 3);
  assert.equal(
    new Set(mock.calls.map((call) => call.body?.idempotency_key)).size,
    1,
    "the bounded internal attempts remain one logical operation",
  );
});

test("per-call consistency stays explicit while concurrent current-format cursors merge monotonically", async () => {
  const responders: Array<(response: Response) => void> = [];
  const mock = scriptedFetch(
    () =>
      new Promise<Response>((resolve) => {
        responders.push(resolve);
      }),
  );
  const client = createSqlClient({
    url: "https://db.example",
    authToken: "secret",
    consistency: "eventual",
    fetch: mock.fetch,
  });

  const older = client.execute("select 1", { consistency: "strong" });
  const newer = client.execute("select 2");
  assert.equal(mock.calls[0]?.body?.consistency, "strong");
  assert.equal(mock.calls[0]?.body?.default_consistency, "eventual");
  assert.equal(mock.calls[1]?.body?.consistency, undefined);
  assert.equal(mock.calls[1]?.body?.default_consistency, "eventual");

  responders[1]!(response(executeResponse("w:000000000000000a")));
  await newer;
  responders[0]!(response(executeResponse("w:0000000000000009")));
  await older;
  assert.equal(client.getSessionCursor(), "w:000000000000000a");
});

test("close is idempotent and rejects subsequent operations", async () => {
  const mock = scriptedFetch(() => response(executeResponse(null)));
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  client.close();
  client.close();
  await assert.rejects(
    client.execute("select 1"),
    (error: unknown) => error instanceof RindleSqlError && error.code === "CLIENT_CLOSED",
  );
  assert.equal(mock.calls.length, 0);
});
