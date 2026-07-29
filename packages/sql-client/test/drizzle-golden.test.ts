import assert from "node:assert/strict";
import { test } from "node:test";

import type { drizzle as drizzleFactory } from "drizzle-orm/libsql";
import { getTableConfig, integer, primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core";

import { createDrizzleClient, type DrizzleClient } from "../src/drizzle.ts";
import type { SqlClient, SqlTransaction, StatementResult } from "../src/index.ts";

const users = sqliteTable("users", {
  id: integer("id").primaryKey(),
  name: text("name").notNull(),
  createdAt: integer("created_at", { mode: "timestamp" }).notNull().default(new Date(0)),
});

// This is Drizzle's idiomatic composite-primary-key spelling. In 0.44.7 the table constraint does
// not mark either column NOT NULL, so the golden deliberately pins the schema shape that exposed
// Rindle's compatibility gap.
const memberships = sqliteTable(
  "memberships",
  {
    userId: text("user_id"),
    groupId: text("group_id"),
    role: text("role"),
  },
  (table) => [primaryKey({ columns: [table.userId, table.groupId] })],
);

// The public factory import eagerly loads @libsql/client even when passed a structural client. Keep
// the compile-time call against its exact overload, then exercise Drizzle's exported constructor
// directly so this package's golden does not acquire an otherwise unused libSQL runtime.
function assertPinnedFactoryContract(client: DrizzleClient): void {
  if (false) {
    const factory = null as unknown as typeof drizzleFactory;
    factory(client);
  }
}

function result(rows: StatementResult["rows"] = [], rowsAffected = 0): StatementResult {
  return {
    columns: [],
    rows,
    rowsAffected,
    lastInsertRowid: null,
    rowsRead: null,
    rowsWritten: null,
  };
}

test("Drizzle golden pins nullable columns under an idiomatic table-level composite primary key", () => {
  const config = getTableConfig(memberships);
  assert.deepEqual(
    config.columns.map((column) => ({ name: column.name, notNull: column.notNull })),
    [
      { name: "user_id", notNull: false },
      { name: "group_id", notNull: false },
      { name: "role", notNull: false },
    ],
  );
  assert.deepEqual(config.primaryKeys.map((key) => key.columns.map((column) => column.name)), [["user_id", "group_id"]]);
});

test("Drizzle 0.44.7 compiles against and executes through the Rindle facade", async () => {
  const calls: Array<{ scope: "client" | "transaction"; statement: unknown }> = [];
  const createdAtSeconds = 1_700_000_000n;
  const executeResults = [result([[7n, "Ada", createdAtSeconds]], 1), result([["Ada"]])];
  const batchResults = [result([], 1), result([["Ada"]])];
  const tx: SqlTransaction = {
    execute: async (statement) => {
      calls.push({ scope: "transaction", statement });
      return result([], 1);
    },
    batch: async () => [],
    commit: async () => ({ commitCursor: null }),
    rollback: async () => {},
  };
  const begins: unknown[] = [];
  const native = {
    execute: async (statement: unknown) => {
      calls.push({ scope: "client", statement });
      return {
        result: executeResults.shift()!,
        commitCursor: null,
        routing: { servedBy: "master", appliedLagMs: null, fenceFallback: false },
      };
    },
    batch: async (statements: unknown) => {
      calls.push({ scope: "client", statement: statements });
      return {
        results: batchResults,
        commitCursor: null,
        routing: { servedBy: "master", appliedLagMs: null, fenceFallback: false },
      };
    },
    begin: async (options: unknown) => {
      begins.push(options);
      return tx;
    },
    withTransaction: async () => undefined,
    withTransactionRetry: async () => undefined,
    executeDdl: async () => ({
      result: result(),
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

  const facade = createDrizzleClient(native);
  assertPinnedFactoryContract(facade);
  const driverCore = await import("drizzle-orm/libsql/driver-core");
  const construct = Reflect.get(driverCore, "construct") as (client: DrizzleClient) => ReturnType<typeof drizzleFactory>;
  const db = construct(facade);

  assert.deepEqual(
    await db
      .insert(users)
      .values({ name: "Ada" })
      .returning({ id: users.id, name: users.name, createdAt: users.createdAt }),
    [{ id: 7, name: "Ada", createdAt: new Date(Number(createdAtSeconds) * 1_000) }],
  );
  assert.deepEqual(await db.select({ name: users.name }).from(users), [{ name: "Ada" }]);

  await db.transaction(async (transaction) => {
    await transaction.update(users).set({ name: "Grace" }).where(undefined);
  });
  assert.deepEqual(begins, [{ readOnly: false }]);

  const [inserted, selected] = await db.batch([
    db.insert(users).values({ name: "Lin" }),
    db.select({ name: users.name }).from(users),
  ]);
  assert.equal(inserted.rowsAffected, 1);
  assert.deepEqual(selected, [{ name: "Ada" }]);

  assert.match(String((calls[0]!.statement as { sql: string }).sql), /^insert into "users"/);
  assert.match(String((calls[1]!.statement as { sql: string }).sql), /^select "name" from "users"/);
  assert.equal(calls[2]!.scope, "transaction");
});
