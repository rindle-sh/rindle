import assert from "node:assert/strict";
import { test } from "node:test";

import { RindleSqlError, createSqlClient } from "../src/index.ts";
import { response, scriptedFetch, wireStatementResult } from "./helpers.ts";

test("typed transaction DTOs execute ordered statement arrays and commit a cursor", async () => {
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/transactions") return response({ transaction_id: "tx/opaque" });
    if (path.endsWith("/execute")) {
      const statements = call.body?.statements as unknown[];
      return response({ results: statements.map(() => wireStatementResult([{ name: "n", decltype: "INTEGER" }], [[{ $rindle: "i64", value: "3" }]])) });
    }
    if (path.endsWith("/commit")) return response({ commit_cursor: "tx-cursor" });
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  const tx = await client.begin({ isolation: "snapshot" });
  assert.deepEqual(mock.calls[0]?.body, {
    read_only: false,
    isolation: "snapshot",
  });

  assert.deepEqual((await tx.execute({ sql: "select ?", args: [3n] })).rows, [[3n]]);
  assert.equal(mock.calls[1]?.url, "https://db.example/v1/sql/transactions/tx%2Fopaque/execute");
  assert.deepEqual(mock.calls[1]?.body?.statements, [{ sql: "select ?", args: [{ $rindle: "i64", value: "3" }] }]);
  assert.equal(mock.calls[1]?.body?.operation_id, "1");

  assert.equal((await tx.batch([{ sql: "select 1" }, { sql: "select 2" }])).length, 2);
  assert.equal(mock.calls[2]?.body?.operation_id, "2");
  assert.deepEqual(await tx.commit(), { commitCursor: "tx-cursor" });
  assert.equal(mock.calls[3]?.body?.operation_id, "3");
  assert.equal(client.getSessionCursor(), "tx-cursor");
  await assert.rejects(
    tx.execute("select 4"),
    (error: unknown) => error instanceof RindleSqlError && error.code === "TRANSACTION_CLOSED",
  );
});

test("transaction execute rejects a success response with the wrong result cardinality", async () => {
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/transactions") return response({ transaction_id: "tx1" });
    if (path.endsWith("/execute")) return response({ results: [] });
    if (path.endsWith("/rollback")) return response(undefined, 204);
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  const tx = await client.begin();
  await assert.rejects(
    tx.execute("select 1"),
    (error: unknown) => error instanceof RindleSqlError && error.code === "PROTOCOL_ERROR",
  );
  await tx.rollback();
});

test("transaction error prefixes decode exactly and malformed partial-result DTOs fail closed", async () => {
  let executeAttempts = 0;
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/transactions") return response({ transaction_id: "tx1" });
    if (path.endsWith("/execute") && executeAttempts++ === 0) {
      return response(
        {
          code: "STATEMENT_FAILED",
          message: "second statement failed",
          retry_scope: "never",
          transaction_state: "open",
          statement_index: 1,
          partial_results: [
            wireStatementResult(
              [{ name: "n", decltype: "INTEGER" }],
              [[{ $rindle: "i64", value: "9223372036854775807" }]],
            ),
          ],
        },
        400,
      );
    }
    if (path.endsWith("/execute")) {
      return response(
        {
          code: "STATEMENT_FAILED",
          message: "malformed prefix",
          retry_scope: "never",
          transaction_state: "open",
          statement_index: 1,
          partial_results: [],
        },
        400,
      );
    }
    if (path.endsWith("/rollback")) return response(undefined, 204);
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  const tx = await client.begin();

  await assert.rejects(tx.batch([{ sql: "select max" }, { sql: "bad" }]), (error: unknown) => {
    assert.ok(error instanceof RindleSqlError);
    assert.equal(error.statementIndex, 1);
    assert.deepEqual(error.partialResults?.[0]?.rows, [[9223372036854775807n]]);
    return true;
  });
  await assert.rejects(
    tx.execute("bad again"),
    (error: unknown) => error instanceof RindleSqlError && error.code === "PROTOCOL_ERROR",
  );
  await tx.rollback();
});

test("number result mode does not silently discard an unrepresentable error prefix", async () => {
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/transactions") return response({ transaction_id: "tx1" });
    if (path.endsWith("/execute")) {
      return response(
        {
          code: "STATEMENT_FAILED",
          message: "second statement failed",
          retry_scope: "never",
          transaction_state: "open",
          statement_index: 1,
          partial_results: [
            wireStatementResult(
              [{ name: "n", decltype: "INTEGER" }],
              [[{ $rindle: "i64", value: "9223372036854775807" }]],
            ),
          ],
        },
        400,
      );
    }
    if (path.endsWith("/rollback")) return response(undefined, 204);
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({
    url: "https://db.example",
    authToken: "secret",
    intMode: "number",
    fetch: mock.fetch,
  });
  const tx = await client.begin();
  await assert.rejects(
    tx.batch([{ sql: "select max" }, { sql: "bad" }]),
    (error: unknown) => error instanceof RindleSqlError && error.code === "VALUE_UNSUPPORTED",
  );
  await tx.rollback();
});

test("read-only begin carries consistency and inherits its session cursor through explicit undefined", async () => {
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/transactions") return response({ transaction_id: "tx1" });
    if (path.endsWith("/rollback")) return response(undefined, 204);
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", sessionCursor: "seed", fetch: mock.fetch });
  const tx = await client.begin({ readOnly: true, consistency: "strong", sessionCursor: undefined });
  assert.deepEqual(mock.calls[0]?.body, {
    read_only: true,
    isolation: "serializable",
    default_consistency: "session",
    consistency: "strong",
    session_cursor: "seed",
  });
  await tx.rollback();
  await tx.rollback();
  assert.equal(mock.calls.length, 2);
  assert.equal(mock.calls[1]?.body?.operation_id, "1");
});

test("an explicit commit retry reuses the terminal operation id after transport exhaustion", async () => {
  let failedCommitAttempts = 0;
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/transactions") return response({ transaction_id: "tx1" });
    if (path.endsWith("/execute")) return response({ results: [wireStatementResult()] });
    if (path.endsWith("/commit") && failedCommitAttempts++ < 3) throw new Error("response lost");
    if (path.endsWith("/commit")) return response({ commit_cursor: "committed" });
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  const tx = await client.begin();
  await tx.execute("select 1");
  await tx.execute("select 2");

  await assert.rejects(
    tx.commit(),
    (error: unknown) =>
      error instanceof RindleSqlError && error.code === "TRANSPORT_ERROR" && error.retryScope === "request",
  );
  assert.deepEqual(await tx.commit(), { commitCursor: "committed" });

  const commitCalls = mock.calls.filter((call) => new URL(call.url).pathname.endsWith("/commit"));
  assert.equal(commitCalls.length, 4);
  assert.deepEqual(commitCalls.map((call) => call.body?.operation_id), ["3", "3", "3", "3"]);
});

test("withTransaction re-drives an outcome-unknown commit instead of rolling back over it", async () => {
  // The first commit commits durably but its response is lost. `withTransaction` must repeat the
  // same terminal operation id to read the server's record back — not report the durable write as
  // failed and roll back, which would leave the fence unadvanced and make a caller retry
  // double-apply.
  let commitAttempts = 0;
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/transactions") return response({ transaction_id: "tx1" });
    if (path.endsWith("/execute")) return response({ results: [wireStatementResult()] });
    if (path.endsWith("/commit") && commitAttempts++ < 3) throw new Error("response lost");
    if (path.endsWith("/commit")) return response({ commit_cursor: "w:0000000000000009" });
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });

  assert.equal(
    await client.withTransaction(async (tx) => {
      await tx.execute("select 1");
      return "callback-value";
    }),
    "callback-value",
  );

  const paths = mock.calls.map((call) => new URL(call.url).pathname);
  assert.equal(
    paths.some((path) => path.endsWith("/rollback")),
    false,
    "a possibly-durable commit must never be rolled back over",
  );
  const commitCalls = mock.calls.filter((call) => new URL(call.url).pathname.endsWith("/commit"));
  assert.deepEqual(
    commitCalls.map((call) => call.body?.operation_id),
    ["2", "2", "2", "2"],
    "every commit attempt reuses the one terminal identity",
  );
  assert.equal(client.getSessionCursor(), "w:0000000000000009", "the resolved commit advances the fence");
});

test("withTransaction rolls back a definite commit rejection but not an unknown one", async () => {
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/transactions") return response({ transaction_id: "tx1" });
    if (path.endsWith("/execute")) return response({ results: [wireStatementResult()] });
    if (path.endsWith("/commit")) {
      return response({ error: { code: "TRANSACTION_CONFLICT", message: "occ" }, transaction_state: "closed" }, 409);
    }
    if (path.endsWith("/rollback")) return response(undefined, 204);
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });

  await assert.rejects(
    client.withTransaction(async (tx) => {
      await tx.execute("select 1");
    }),
    (error: unknown) => error instanceof RindleSqlError && error.code === "TRANSACTION_CONFLICT",
  );
  const commitCalls = mock.calls.filter((call) => new URL(call.url).pathname.endsWith("/commit"));
  assert.equal(commitCalls.length, 1, "a definite negative is not re-driven");
});

test("an explicit statement retry reuses its operation id after request-scope exhaustion", async () => {
  let executeAttempts = 0;
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/transactions") return response({ transaction_id: "tx1" });
    if (path.endsWith("/execute") && executeAttempts++ < 3) {
      return response({ error: "writer session capacity exhausted" }, 503);
    }
    if (path.endsWith("/execute")) return response({ results: [wireStatementResult()] });
    if (path.endsWith("/rollback")) return response(undefined, 204);
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  const tx = await client.begin();

  await assert.rejects(
    tx.execute("select 1"),
    (error: unknown) => error instanceof RindleSqlError && error.retryScope === "request",
  );
  await assert.rejects(
    tx.execute("select 2"),
    (error: unknown) => error instanceof RindleSqlError && error.code === "TRANSACTION_OPERATION_PENDING",
  );
  await tx.execute("select 1");

  const executeCalls = mock.calls.filter((call) => new URL(call.url).pathname.endsWith("/execute"));
  assert.equal(executeCalls.length, 4, "the different pending request is rejected before transport");
  assert.deepEqual(executeCalls.map((call) => call.body?.operation_id), ["1", "1", "1", "1"]);
  await tx.rollback();
});

test("a minimal pre-admission body rejection reuses its unconsumed operation sequence", async () => {
  let executeAttempts = 0;
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/transactions") return response({ transaction_id: "tx1" });
    if (path.endsWith("/execute") && executeAttempts++ === 0) {
      return response({ error: "write body exceeds the configured byte limit" }, 413);
    }
    if (path.endsWith("/execute")) return response({ results: [wireStatementResult()] });
    if (path.endsWith("/rollback")) return response(undefined, 204);
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  const tx = await client.begin();

  await assert.rejects(
    tx.batch([{ sql: "select 1" }, { sql: "select 2" }]),
    (error: unknown) =>
      error instanceof RindleSqlError &&
      error.code === "HTTP_413" &&
      error.status === 413 &&
      error.transactionState === undefined,
  );
  await tx.execute("select corrected");

  const executeCalls = mock.calls.filter((call) => new URL(call.url).pathname.endsWith("/execute"));
  assert.deepEqual(executeCalls.map((call) => call.body?.operation_id), ["1", "1"]);
  await tx.rollback();
});

test("a pre-admission statement refusal reuses its unconsumed operation sequence", async () => {
  let executeAttempts = 0;
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/transactions") return response({ transaction_id: "tx1" });
    if (path.endsWith("/execute") && executeAttempts++ === 0) {
      return response(
        {
          code: "STATEMENT_INVALID",
          message: "one Statement may contain only one SQL statement",
          retry_scope: "never",
          transaction_state: "unknown",
        },
        400,
      );
    }
    if (path.endsWith("/execute")) return response({ results: [wireStatementResult()] });
    if (path.endsWith("/rollback")) return response(undefined, 204);
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  const tx = await client.begin();

  await assert.rejects(
    tx.execute("select 1; select 2"),
    (error: unknown) => error instanceof RindleSqlError && error.transactionState === "unknown",
  );
  await tx.execute("select 3");

  const executeCalls = mock.calls.filter((call) => new URL(call.url).pathname.endsWith("/execute"));
  assert.deepEqual(executeCalls.map((call) => call.body?.operation_id), ["1", "1"]);
  await tx.rollback();
});

test("rollback after an outcome-unknown commit reuses the terminal sequence", async () => {
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/transactions") return response({ transaction_id: "tx1" });
    if (path.endsWith("/execute")) return response({ results: [wireStatementResult()] });
    if (path.endsWith("/commit")) throw new Error("commit transport lost");
    if (path.endsWith("/rollback")) return response(undefined, 204);
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  const tx = await client.begin();
  await tx.execute("select 1");
  await assert.rejects(tx.commit(), (error: unknown) => error instanceof RindleSqlError);
  await tx.rollback();

  const terminalCalls = mock.calls.filter((call) => {
    const path = new URL(call.url).pathname;
    return path.endsWith("/commit") || path.endsWith("/rollback");
  });
  assert.deepEqual(terminalCalls.map((call) => call.body?.operation_id), ["2", "2", "2", "2"]);
});

test("withTransaction rolls back when the application callback throws", async () => {
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/transactions") return response({ transaction_id: "tx1" });
    if (path.endsWith("/rollback")) return response(undefined, 204);
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  const boom = new Error("application failed");
  await assert.rejects(client.withTransaction(async () => Promise.reject(boom)), boom);
  assert.equal(new URL(mock.calls[1]!.url).pathname, "/v1/sql/transactions/tx1/rollback");
});

test("withTransactionRetry re-runs only a closure-level transaction conflict", async () => {
  let beginCount = 0;
  let commitCount = 0;
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    if (path === "/v1/sql/transactions") return response({ transaction_id: `tx${++beginCount}` });
    if (path.endsWith("/commit")) {
      commitCount += 1;
      if (commitCount === 1) {
        return response(
          { code: "TRANSACTION_CONFLICT", message: "conflict", retry_scope: "closure", transaction_state: "closed" },
          409,
        );
      }
      return response({ commit_cursor: "won" });
    }
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  let closures = 0;
  const result = await client.withTransactionRetry(
    async () => {
      closures += 1;
      return closures;
    },
    { maxAttempts: 3, baseDelayMs: 0, maxDelayMs: 0 },
  );
  assert.equal(result, 2);
  assert.equal(closures, 2);
  assert.equal(beginCount, 2);
  assert.equal(commitCount, 2);
});

test("aborting an in-flight transaction statement sends explicit cancel then rollback", async () => {
  const controller = new AbortController();
  const paths: string[] = [];
  const mock = scriptedFetch((call) => {
    const path = new URL(call.url).pathname;
    paths.push(path);
    if (path === "/v1/sql/transactions") return response({ transaction_id: "tx1" });
    if (path.endsWith("/execute")) {
      return new Promise<Response>((_resolve, reject) => {
        call.init.signal?.addEventListener("abort", () => reject(call.init.signal?.reason), { once: true });
      });
    }
    if (path.endsWith("/cancel")) return response({ cancelled: true });
    if (path.endsWith("/rollback")) return response(undefined, 204);
    throw new Error(`unexpected path ${path}`);
  });
  const client = createSqlClient({ url: "https://db.example", authToken: "secret", fetch: mock.fetch });
  const tx = await client.begin();
  const executing = tx.execute("select expensive()", { signal: controller.signal });
  controller.abort(new DOMException("stop", "AbortError"));
  await assert.rejects(executing, (error: unknown) => error instanceof DOMException && error.name === "AbortError");
  assert.deepEqual(paths, [
    "/v1/sql/transactions",
    "/v1/sql/transactions/tx1/execute",
    "/v1/sql/transactions/tx1/cancel",
    "/v1/sql/transactions/tx1/rollback",
  ]);
  assert.equal(mock.calls[1]?.body?.operation_id, "1");
  assert.equal(mock.calls[2]?.body?.operation_id, "1", "cancel names the in-flight operation");
  assert.equal(mock.calls[3]?.body?.operation_id, "2");
});
