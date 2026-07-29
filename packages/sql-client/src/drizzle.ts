import { customType } from "drizzle-orm/sqlite-core";

import { createSqlClient } from "./client.ts";
import { RindleSqlError, valueUnsupported } from "./errors.ts";
import type {
  ClientOptions,
  SqlClient,
  SqlTransaction,
  SqlValue,
  Statement,
  StatementResult,
} from "./types.ts";

/**
 * The exact-i64 column for Drizzle schemas (design 226 §7.2). Three promises,
 * lined up with the column contract: generated DDL says `BIGINT` (the opt-in
 * exact int64 declaration), bind/predicate values are JS `bigint`, and selected
 * values infer as JS `bigint`. Drizzle's own `integer()` remains the
 * number/date/boolean surface and still emits `INTEGER` (the safe-range f64
 * plane).
 *
 * Caveat (§7.2): decltype dispatch fires only for **direct column references**.
 * SQLite reports no decltype for expression results, so `max(big_id)` or
 * `big_id + 1` falls into the safe-number arm and is a typed
 * `VALUE_UNSUPPORTED` past the round-trip bound — never a rounded number. For
 * exact expression results, use the native client with `intMode: "bigint"`.
 */
export const rindleBigint = customType<{
  data: bigint;
  driverData: bigint;
}>({
  dataType: () => "BIGINT",
});

/** The small, structural statement shape consumed by drizzle-orm/libsql. */
export interface DrizzleStatement {
  sql: string;
  args?: DrizzleArgs;
}

export type DrizzleArgs = readonly unknown[] | Readonly<Record<string, unknown>>;
export type DrizzleInputStatement = string | DrizzleStatement | readonly [sql: string, args?: DrizzleArgs];
export type DrizzleTransactionMode = "write" | "read" | "deferred";

// Booleans are a bind convenience only; SQLite result storage classes never decode to boolean.
// `bigint` appears ONLY in cells of a column whose decltype is exactly BIGINT/INT8 (the
// `rindleBigint` opt-in, design 226 §7.2); every other integer cell stays a safe number.
export type DrizzleValue = Exclude<SqlValue, boolean>;
export type DrizzleRow = DrizzleValue[] & Record<string, DrizzleValue>;

export interface DrizzleResultSet {
  columns: string[];
  columnTypes: string[];
  rows: DrizzleRow[];
  rowsAffected: number;
  lastInsertRowid: bigint | undefined;
  toJSON(): unknown;
}

export interface DrizzleTransaction {
  readonly closed: boolean;
  execute(statement: DrizzleInputStatement): Promise<DrizzleResultSet>;
  batch(statements: DrizzleInputStatement[]): Promise<DrizzleResultSet[]>;
  executeMultiple(sql: string): Promise<void>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
  close(): void;
}

export interface DrizzleClient {
  readonly protocol: string;
  readonly closed: boolean;
  execute(statement: DrizzleInputStatement, args?: DrizzleArgs): Promise<DrizzleResultSet>;
  batch(statements: DrizzleInputStatement[], mode?: DrizzleTransactionMode): Promise<DrizzleResultSet[]>;
  /** Present for structural Client typing; Drizzle's libSQL migrator is deliberately unsupported. */
  migrate(statements: DrizzleInputStatement[]): Promise<DrizzleResultSet[]>;
  transaction(mode?: DrizzleTransactionMode): Promise<DrizzleTransaction>;
  executeMultiple(sql: string): Promise<void>;
  /** Embedded-replica sync is deliberately unsupported. */
  sync(): Promise<never>;
  reconnect(): void;
  close(): void;
  /** Escape hatch for Rindle-specific operations and session-cursor persistence. */
  readonly rindle: SqlClient;
}

function toNativeStatement(input: DrizzleInputStatement, args?: DrizzleArgs): Statement | string {
  if (typeof input === "string") return args === undefined ? input : { sql: input, args: args as Statement["args"] };
  if (Array.isArray(input)) return { sql: input[0], args: input[1] as Statement["args"] };
  return {
    sql: (input as DrizzleStatement).sql,
    // The native codec performs the deliberate runtime refusal for binary/unsupported values.
    args: (input as DrizzleStatement).args as Statement["args"],
  };
}

function defineNamedCell(row: DrizzleRow, name: string, value: DrizzleValue): void {
  // Array length and numeric aliases overlap the positional row representation; positions win.
  const numericIndex = Number(name);
  if (
    name === "length" ||
    (Number.isInteger(numericIndex) && numericIndex >= 0 && numericIndex < 0xffff_ffff && String(numericIndex) === name)
  ) {
    return;
  }
  try {
    Object.defineProperty(row, name, { value, writable: true, enumerable: true, configurable: true });
  } catch {
    // An exotic/non-configurable property name cannot be represented in the hybrid row.
  }
}

/** The exact whole-declaration match (design 226 §4.1): `BIGINT`/`INT8` and nothing
 *  else — `UNSIGNED BIGINT` and every decorated spelling keeps its safe-number
 *  INTEGER-affinity meaning, mirroring the daemon's `value_type_of`. */
function isInt64Decltype(decltype: string | null | undefined): boolean {
  if (!decltype) return false;
  const t = decltype.trim().toUpperCase();
  return t === "BIGINT" || t === "INT8";
}

function toDrizzleValue(value: SqlValue, int64Column: boolean): DrizzleValue {
  if (typeof value === "bigint") {
    // A BIGINT/INT8-declared result column preserves the exact value as JS bigint
    // (the `rindleBigint` lane, design 226 §7.2). This dispatch must be per-column:
    // globally returning bigint would break Drizzle's integer/timestamp mappers,
    // globally returning number would defeat `rindleBigint`.
    if (int64Column) return value;
    const number = Number(value);
    if (!Number.isSafeInteger(number)) {
      throw valueUnsupported(`integer ${value.toString()} is outside Number's safe integer range required by Drizzle`);
    }
    return number;
  }
  if (int64Column && value !== null) {
    // Under the facade's lossless-bigint contract every non-NULL INTEGER cell arrives
    // as bigint, so a number or string here is a REAL/TEXT physical cell — SQLite
    // affinity permits them and schema admission never scans rows. Passing it through
    // would hand a `rindleBigint` field (statically `bigint`) the wrong runtime type;
    // §7.2's contract is a typed error, never a mistyped value.
    throw valueUnsupported(
      `BIGINT/INT8-declared column holds a ${typeof value} cell; exact int64 columns require INTEGER storage`,
    );
  }
  // The native decoder never produces booleans, but normalize a structurally supplied result too.
  if (typeof value === "boolean") return value ? 1 : 0;
  return value;
}

/** Convert Rindle's lossless positional result into libSQL's positional + named hybrid rows. */
export function toDrizzleResultSet(result: StatementResult): DrizzleResultSet {
  const columns = result.columns.map((column) => column.name);
  const columnTypes = result.columns.map((column) => column.decltype ?? "");
  const int64Columns = result.columns.map((column) => isInt64Decltype(column.decltype));
  const rows = result.rows.map((cells) => {
    const row = cells.map((cell, index) => toDrizzleValue(cell, int64Columns[index] === true)) as DrizzleRow;
    for (let index = 0; index < columns.length; index += 1) {
      defineNamedCell(row, columns[index]!, row[index]!);
    }
    return row;
  });
  const converted: DrizzleResultSet = {
    columns,
    columnTypes,
    rows,
    rowsAffected: result.rowsAffected,
    lastInsertRowid: result.lastInsertRowid === null ? undefined : BigInt(result.lastInsertRowid),
    toJSON() {
      return {
        columns: this.columns,
        columnTypes: this.columnTypes,
        rows: this.rows,
        rowsAffected: this.rowsAffected,
        lastInsertRowid: this.lastInsertRowid?.toString(),
      };
    },
  };
  return converted;
}

class TransactionFacade implements DrizzleTransaction {
  private isClosed = false;
  private readonly tx: SqlTransaction;

  constructor(tx: SqlTransaction) {
    this.tx = tx;
  }

  get closed(): boolean {
    return this.isClosed;
  }

  async execute(statement: DrizzleInputStatement): Promise<DrizzleResultSet> {
    return toDrizzleResultSet(await this.tx.execute(toNativeStatement(statement)));
  }

  async batch(statements: DrizzleInputStatement[]): Promise<DrizzleResultSet[]> {
    const native = statements.map((statement) => {
      const converted = toNativeStatement(statement);
      return typeof converted === "string" ? { sql: converted } : converted;
    });
    return (await this.tx.batch(native)).map(toDrizzleResultSet);
  }

  async executeMultiple(_sql: string): Promise<void> {
    throw new RindleSqlError({
      code: "STATEMENT_UNSUPPORTED",
      message: "executeMultiple is not supported inside a Rindle interactive transaction; use batch()",
    });
  }

  async commit(): Promise<void> {
    if (this.isClosed) return;
    await this.tx.commit();
    this.isClosed = true;
  }

  async rollback(): Promise<void> {
    if (this.isClosed) return;
    await this.tx.rollback();
    this.isClosed = true;
  }

  close(): void {
    if (this.isClosed) return;
    this.isClosed = true;
    void this.tx.rollback().catch(() => {});
  }
}

class ClientFacade implements DrizzleClient {
  readonly rindle: SqlClient;
  readonly protocol = "http";
  private isClosed = false;

  constructor(rindle: SqlClient) {
    this.rindle = rindle;
  }

  get closed(): boolean {
    return this.isClosed;
  }

  async execute(statement: DrizzleInputStatement, args?: DrizzleArgs): Promise<DrizzleResultSet> {
    return toDrizzleResultSet((await this.rindle.execute(toNativeStatement(statement, args))).result);
  }

  async batch(statements: DrizzleInputStatement[], _mode?: DrizzleTransactionMode): Promise<DrizzleResultSet[]> {
    const native = statements.map((statement) => {
      const converted = toNativeStatement(statement);
      return typeof converted === "string" ? { sql: converted } : converted;
    });
    return (await this.rindle.batch(native)).results.map(toDrizzleResultSet);
  }

  async transaction(mode: DrizzleTransactionMode = "write"): Promise<DrizzleTransaction> {
    if (mode !== "write" && mode !== "read" && mode !== "deferred") {
      throw new TypeError(`unsupported transaction mode: ${String(mode)}`);
    }
    return new TransactionFacade(await this.rindle.begin({ readOnly: mode === "read" }));
  }

  executeMultiple(sql: string): Promise<void> {
    return this.rindle.executeMultiple(sql);
  }

  async migrate(_statements: DrizzleInputStatement[]): Promise<DrizzleResultSet[]> {
    throw new RindleSqlError({
      code: "MIGRATOR_UNSUPPORTED",
      message: "drizzle-orm/libsql/migrator is not supported; apply declared migrations with SqlClient.migrate()",
    });
  }

  async sync(): Promise<never> {
    throw new RindleSqlError({
      code: "SYNC_UNSUPPORTED",
      message: "embedded-replica sync is not supported by Rindle SQL",
    });
  }

  reconnect(): void {
    throw new RindleSqlError({
      code: "RECONNECT_UNSUPPORTED",
      message: "a closed Rindle SQL client cannot be reopened; create a new client",
    });
  }

  close(): void {
    if (this.isClosed) return;
    this.isClosed = true;
    this.rindle.close();
  }
}

/** Create the structural libSQL-client facade used by drizzle-orm/libsql. */
export function createDrizzleClient(options: ClientOptions | SqlClient): DrizzleClient {
  // The internal client is LOSSLESS (`intMode: "bigint"`) so precision is not
  // discarded before result metadata is considered (design 226 §7.2):
  // toDrizzleResultSet then dispatches per column decltype — BIGINT/INT8 keeps
  // the exact bigint (the `rindleBigint` lane); every other column converts to
  // the safe number Drizzle's integer/timestamp mappers consume, with an unsafe
  // integer a typed VALUE_UNSUPPORTED, never a rounded number. An INJECTED
  // structural client used with `rindleBigint` must likewise expose lossless
  // bigint results, or exact values are lost before this facade sees them.
  const client = isSqlClient(options) ? options : createSqlClient({ ...options, intMode: "bigint" });
  return new ClientFacade(client);
}

function isSqlClient(value: ClientOptions | SqlClient): value is SqlClient {
  const candidate = value as Partial<SqlClient>;
  return typeof candidate.execute === "function" && typeof candidate.close === "function";
}
