import assert from "node:assert/strict";
import { test } from "node:test";

import { RindleSqlError, decodeSqlValue, encodeArgs, encodeSqlValue, encodeStatement } from "../src/index.ts";

test("v1 value codec tags integers, booleans, and non-finite floats without losing JSON fidelity", () => {
  assert.equal(encodeSqlValue(null), null);
  assert.equal(encodeSqlValue("hello"), "hello");
  assert.deepEqual(encodeSqlValue(42), { $rindle: "i64", value: "42" });
  assert.deepEqual(encodeSqlValue(-0), { $rindle: "i64", value: "0" });
  assert.equal(encodeSqlValue(1.25), 1.25);
  assert.equal(encodeSqlValue(Number.MAX_SAFE_INTEGER + 1), 9_007_199_254_740_992);
  assert.deepEqual(encodeSqlValue(42n), { $rindle: "i64", value: "42" });
  assert.deepEqual(encodeSqlValue(true), { $rindle: "i64", value: "1" });
  assert.deepEqual(encodeSqlValue(false), { $rindle: "i64", value: "0" });
  assert.deepEqual(encodeSqlValue(Infinity), { $rindle: "float", value: "Infinity" });
  assert.deepEqual(encodeSqlValue(-Infinity), { $rindle: "float", value: "-Infinity" });

  assert.equal(decodeSqlValue({ $rindle: "i64", value: "42" }), 42n);
  assert.equal(decodeSqlValue({ $rindle: "i64", value: "42" }, "number"), 42);
  assert.equal(decodeSqlValue({ $rindle: "i64", value: "42" }, "string"), "42");
  assert.equal(decodeSqlValue({ $rindle: "float", value: "Infinity" }), Infinity);
  assert.equal(decodeSqlValue({ $rindle: "float", value: "-Infinity" }), -Infinity);
});

test("bigint binds preserve the full signed-i64 wire range and reject binary values", () => {
  assert.deepEqual(encodeSqlValue(1n << 53n), { $rindle: "i64", value: "9007199254740992" });
  assert.deepEqual(encodeSqlValue((1n << 53n) + 1n), { $rindle: "i64", value: "9007199254740993" });
  assert.deepEqual(encodeSqlValue(-(1n << 63n)), { $rindle: "i64", value: "-9223372036854775808" });
  assert.deepEqual(encodeSqlValue((1n << 63n) - 1n), { $rindle: "i64", value: "9223372036854775807" });
  assert.throws(
    () => encodeSqlValue(1n << 63n),
    (error: unknown) => error instanceof RindleSqlError && error.code === "VALUE_UNSUPPORTED" && /signed 64-bit/.test(error.message),
  );
  assert.throws(
    () => encodeSqlValue(new Uint8Array([1, 2]) as never),
    (error: unknown) => error instanceof RindleSqlError && error.code === "VALUE_UNSUPPORTED" && /Uint8Array/.test(error.message),
  );
  assert.throws(
    () => encodeSqlValue(NaN),
    (error: unknown) => error instanceof RindleSqlError && error.code === "VALUE_UNSUPPORTED",
  );
});

test("every decoded i64 can be encoded back without losing its wire value", () => {
  for (const value of ["9007199254740993", "-9223372036854775808", "9223372036854775807"]) {
    const decoded = decodeSqlValue({ $rindle: "i64", value });
    assert.equal(typeof decoded, "bigint");
    assert.deepEqual(encodeSqlValue(decoded), { $rindle: "i64", value });
  }
});

test("number result mode refuses tagged integers outside Number's safe range", () => {
  assert.throws(
    () => decodeSqlValue({ $rindle: "i64", value: "9007199254740992" }, "number"),
    (error: unknown) => error instanceof RindleSqlError && error.code === "VALUE_UNSUPPORTED" && /safe integer/.test(error.message),
  );
  assert.equal(decodeSqlValue({ $rindle: "i64", value: "9007199254740992" }, "bigint"), 1n << 53n);
  assert.throws(
    () => decodeSqlValue({ $rindle: "i64", value: "01" }),
    (error: unknown) => error instanceof RindleSqlError && error.code === "PROTOCOL_ERROR",
  );
  assert.throws(
    () => decodeSqlValue({ $rindle: "float", value: "NaN" }),
    (error: unknown) => error instanceof RindleSqlError && error.code === "PROTOCOL_ERROR",
  );
});

test("statement and args codecs preserve positional/named binds and frozen snake_case", () => {
  assert.deepEqual(encodeStatement({ sql: "select ?", args: [7n], wantRows: false }), {
    sql: "select ?",
    args: [{ $rindle: "i64", value: "7" }],
    want_rows: false,
  });
  assert.deepEqual(encodeArgs({ name: "Ada", active: true }), {
    name: "Ada",
    active: { $rindle: "i64", value: "1" },
  });
});
