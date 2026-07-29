import { protocolError, valueUnsupported } from "./errors.ts";
import type { IntMode, SqlArgs, SqlValue, Statement, WireArgs, WireSqlValue, WireStatement } from "./types.ts";

const I64_MIN = -(1n << 63n);
const I64_MAX = (1n << 63n) - 1n;
const DECIMAL_I64 = /^(?:0|-[1-9][0-9]*|[1-9][0-9]*)$/;

function binaryKind(value: unknown): string | null {
  if (typeof ArrayBuffer !== "undefined") {
    if (value instanceof ArrayBuffer) return "ArrayBuffer";
    if (ArrayBuffer.isView(value)) return value?.constructor?.name ?? "binary view";
  }
  if (typeof Blob !== "undefined" && value instanceof Blob) return "Blob";
  return null;
}

function assertI64(value: bigint, source: string): void {
  if (value < I64_MIN || value > I64_MAX) {
    throw valueUnsupported(`${source} is outside SQLite's signed 64-bit integer range`);
  }
}

/** Encode one public SDK value into the frozen v1 tagged JSON representation. */
export function encodeSqlValue(value: SqlValue): WireSqlValue {
  if (value === null || typeof value === "string") return value;
  if (typeof value === "boolean") return { $rindle: "i64", value: value ? "1" : "0" };
  if (typeof value === "bigint") {
    assertI64(value, `integer ${value.toString()}`);
    return { $rindle: "i64", value: value.toString() };
  }
  if (typeof value === "number") {
    if (Number.isNaN(value)) throw valueUnsupported("NaN is not a supported SQLite value");
    if (value === Infinity) return { $rindle: "float", value: "Infinity" };
    if (value === -Infinity) return { $rindle: "float", value: "-Infinity" };
    // JavaScript has one number type, but SQLite distinguishes INTEGER and REAL storage classes.
    // Safe integral numbers carry the exact i64 tag; non-integral and larger numeric values remain
    // REALs. Callers that need a larger integer must use bigint so intent and precision are explicit.
    if (Number.isSafeInteger(value)) {
      return { $rindle: "i64", value: BigInt(value).toString() };
    }
    return value;
  }

  const kind = binaryKind(value);
  if (kind !== null) throw valueUnsupported(`${kind} bind values are not supported in Rindle SQL v1`);
  throw valueUnsupported(`unsupported bind value of type ${typeof value}`);
}

function parseWireI64(decimal: unknown): bigint {
  if (typeof decimal !== "string" || !DECIMAL_I64.test(decimal)) {
    throw protocolError("server returned a malformed tagged i64 value");
  }
  let value: bigint;
  try {
    value = BigInt(decimal);
  } catch (cause) {
    throw protocolError("server returned a malformed tagged i64 value", cause);
  }
  if (value < I64_MIN || value > I64_MAX) {
    throw protocolError("server returned an out-of-range tagged i64 value");
  }
  return value;
}

/** Decode one frozen v1 tagged JSON value using the client's integer policy. */
export function decodeSqlValue(value: unknown, intMode: IntMode = "bigint"): SqlValue {
  if (value === null || typeof value === "string") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw protocolError("server returned a non-finite untagged JSON number");
    return value;
  }
  if (typeof value !== "object" || Array.isArray(value)) {
    throw protocolError("server returned a malformed SQL value");
  }

  const tagged = value as { $rindle?: unknown; value?: unknown };
  if (tagged.$rindle === "float") {
    if (tagged.value === "Infinity") return Infinity;
    if (tagged.value === "-Infinity") return -Infinity;
    throw protocolError("server returned a malformed tagged float value");
  }
  if (tagged.$rindle !== "i64") throw protocolError("server returned an unknown tagged SQL value");

  const integer = parseWireI64(tagged.value);
  if (intMode === "bigint") return integer;
  if (intMode === "string") return integer.toString();
  const number = Number(integer);
  if (!Number.isSafeInteger(number)) {
    throw valueUnsupported(`integer ${integer.toString()} is outside Number's safe integer range`);
  }
  return number;
}

export function encodeArgs(args: SqlArgs): WireArgs {
  if (Array.isArray(args)) return args.map((value) => encodeSqlValue(value));
  if (args === null || typeof args !== "object") {
    throw valueUnsupported("statement args must be a positional array or named object");
  }
  return Object.fromEntries(Object.entries(args).map(([name, value]) => [name, encodeSqlValue(value)]));
}

export function encodeStatement(statement: Statement | string): WireStatement {
  const normalized: Statement = typeof statement === "string" ? { sql: statement } : statement;
  if (normalized === null || typeof normalized !== "object" || typeof normalized.sql !== "string") {
    throw new TypeError("statement must be a SQL string or an object with a string sql field");
  }
  if (normalized.sql.length === 0) throw new TypeError("statement sql must not be empty");
  const wire: WireStatement = { sql: normalized.sql };
  if (normalized.args !== undefined) wire.args = encodeArgs(normalized.args);
  if (normalized.wantRows !== undefined) wire.want_rows = normalized.wantRows;
  return wire;
}
