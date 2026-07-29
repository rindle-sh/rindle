// The exact-i64 Drizzle lane (design 226 §7.2, Stage C6): `rindleBigint` emits
// BIGINT DDL and infers `bigint`; the facade's per-column decltype dispatch keeps
// a BIGINT/INT8 result cell an exact `bigint` while every other integer column
// stays Drizzle's safe-number plane — an unsafe integer there is a typed
// VALUE_UNSUPPORTED, never a rounded number. That last arm is also the §7.2
// caveat pinned: an EXPRESSION result (`max(big_id)`, `big_id + 1`) carries no
// decltype, so past the bound it errors rather than rounding; the native client
// with `intMode: "bigint"` is the exact escape hatch.
import assert from "node:assert/strict";
import { test } from "node:test";

import { getTableConfig, integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

import { rindleBigint, toDrizzleResultSet } from "../src/drizzle.ts";
import { RindleSqlError } from "../src/errors.ts";
import type { StatementResult } from "../src/index.ts";

const UNSAFE = 9_007_199_254_740_993n; // 2^53 + 1
const I64_MAX = 9_223_372_036_854_775_807n;
const I64_MIN = -9_223_372_036_854_775_808n;

function result(
  columns: { name: string; decltype: string | null }[],
  rows: StatementResult["rows"],
): StatementResult {
  return {
    columns,
    rows,
    rowsAffected: 0,
    lastInsertRowid: null,
    rowsRead: null,
    rowsWritten: null,
  };
}

test("rindleBigint emits BIGINT DDL and infers bigint; integer() beside it stays INTEGER/number", () => {
  const ledger = sqliteTable("ledger", {
    id: rindleBigint("id").primaryKey(),
    ref: text("ref").notNull(),
    count: integer("count").notNull(),
  });
  const config = getTableConfig(ledger);
  const byName = new Map(config.columns.map((c) => [c.name, c]));
  assert.equal(byName.get("id")!.getSQLType(), "BIGINT");
  assert.equal(byName.get("count")!.getSQLType(), "integer");

  // Compile-time: the selected value infers as bigint (a number would not assign).
  type IdData = typeof ledger.id._.data;
  const exact: IdData = UNSAFE;
  assert.equal(typeof exact, "bigint");
});

test("a BIGINT/INT8-declared result column keeps the exact bigint, positional and named", () => {
  for (const decltype of ["BIGINT", "INT8", " bigint ", "int8"]) {
    const set = toDrizzleResultSet(
      result(
        [
          { name: "id", decltype },
          { name: "ref", decltype: "TEXT" },
        ],
        [
          [UNSAFE, "a"],
          [I64_MAX, "b"],
          [I64_MIN, "c"],
        ],
      ),
    );
    assert.equal(set.rows[0]![0], UNSAFE, `decltype ${JSON.stringify(decltype)}`);
    assert.equal(set.rows[0]!.id, UNSAFE, "named access sees the same exact bigint");
    assert.equal(set.rows[1]![0], I64_MAX);
    assert.equal(set.rows[2]![0], I64_MIN);
    assert.equal(set.rows[0]![1], "a", "sibling columns unaffected");
  }
});

test("a non-INTEGER physical cell in a BIGINT/INT8 column refuses typed, never mistypes", () => {
  // SQLite affinity happily stores REAL or TEXT in a BIGINT-declared column, and
  // schema admission never scans rows — so a direct select can meet one. The cell
  // must raise VALUE_UNSUPPORTED rather than hand a `rindleBigint` field
  // (statically `bigint`) a number/string at runtime.
  for (const cell of [1.5, "seven"]) {
    assert.throws(
      () => toDrizzleResultSet(result([{ name: "n", decltype: "BIGINT" }], [[cell]])),
      (e: unknown) => e instanceof RindleSqlError && e.code === "VALUE_UNSUPPORTED",
      `physical ${typeof cell} cell must refuse`,
    );
  }
  // NULL stays the nullable-BIGINT value, and sibling columns are untouched.
  const set = toDrizzleResultSet(
    result(
      [
        { name: "n", decltype: "INT8" },
        { name: "ref", decltype: "TEXT" },
      ],
      [[null, "a"]],
    ),
  );
  assert.strictEqual(set.rows[0]![0], null);
  assert.strictEqual(set.rows[0]![1], "a");
});

test("ordinary and decorated integer decltypes stay the safe-number plane", () => {
  // Safe values convert to numbers (Drizzle's integer/timestamp mappers consume them)…
  for (const decltype of ["INTEGER", "UNSIGNED BIGINT", null]) {
    const set = toDrizzleResultSet(result([{ name: "n", decltype }], [[42n]]));
    assert.strictEqual(set.rows[0]![0], 42, `decltype ${JSON.stringify(decltype)}`);
  }
  // …and an unsafe value is a typed VALUE_UNSUPPORTED, never a rounded number.
  for (const decltype of ["INTEGER", "UNSIGNED BIGINT", null]) {
    assert.throws(
      () => toDrizzleResultSet(result([{ name: "n", decltype }], [[UNSAFE]])),
      (e: unknown) => e instanceof RindleSqlError && e.code === "VALUE_UNSUPPORTED",
      `decltype ${JSON.stringify(decltype)} must refuse, not round`,
    );
  }
});

test("the §7.2 caveat: an expression result has no decltype and refuses past the bound", () => {
  // SQLite reports decltype NULL for `max(big_id)` / `big_id + 1` — the exact
  // shape a caller might expect to stay bigint. It errors typed instead of
  // silently rounding; the native client (`intMode: "bigint"`) is the exact path.
  assert.throws(
    () => toDrizzleResultSet(result([{ name: "max(big_id)", decltype: null }], [[I64_MAX]])),
    (e: unknown) => e instanceof RindleSqlError && e.code === "VALUE_UNSUPPORTED",
  );
  // Within the safe range the same shape stays a plain number.
  const set = toDrizzleResultSet(result([{ name: "max(small)", decltype: null }], [[7n]]));
  assert.strictEqual(set.rows[0]![0], 7);
});

test("lastInsertRowid remains bigint regardless of column dispatch", () => {
  const set = toDrizzleResultSet({
    columns: [],
    rows: [],
    rowsAffected: 1,
    lastInsertRowid: "9223372036854775807",
    rowsRead: null,
    rowsWritten: null,
  });
  assert.equal(set.lastInsertRowid, I64_MAX);
});
