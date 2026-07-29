// @rindle/query-compiler — compile a Rindle query `Ast` into one SQL `SELECT` whose single
// column `"rindle_result"` is the entire nested result tree as JSON, run on a live SQL
// backend inside the caller's open transaction (POSTGRES-READ-COMPILER-DESIGN.md §4).
//
// Dialect-parametric, with BOTH dialects product targets:
// - `postgres` — the BYO-Postgres server mutator read, with the §6.2 `::text::<type>` cast
//   discipline (PG is a second type system whose driver parsing must be pinned + reconciled).
// - `sqlite` — the daemon-backend mutator read, ridden through an interactive mutation
//   session (DAEMON-INTERACTIVE-TXN-DESIGN.md §5.4). NO casts, by design: SQLite IS the
//   canonical store and results return as raw storage classes over Rindle's own value
//   encoding, so there is no second representation to reconcile — every value binds as a
//   native parameter (`rindle-d2s`, the Rust twin, states the same principle: "Values
//   (minimal — no casting)"). It doubles as the offline differential oracle (§9, §11),
//   which is what proves the shared walker end-to-end against `node:sqlite`.

export type {
  Aggregate,
  Ast,
  Bound,
  Condition,
  CorrelatedSubquery,
  Correlation,
  Dir,
  ExistsOp,
  LitValue,
  OrderPart,
  SimpleOp,
  ValuePosition,
} from "./ast.ts";
export type { Cardinality, Catalog, ColumnType, TableSchema } from "./catalog.ts";
export type { Dialect } from "./dialect.ts";
export { sqliteDialect } from "./dialect.ts";
export { postgresDialect } from "./postgres.ts";

import type { Ast } from "./ast.ts";
import type { Catalog } from "./catalog.ts";
import { sqliteDialect } from "./dialect.ts";
import { postgresDialect } from "./postgres.ts";
import { compileWith } from "./walker.ts";

/** Which SQL dialect to emit. Both are product targets: `postgres` for the BYO-PG backend,
 *  `sqlite` for the daemon backend's session reads (and the offline oracle, §9). */
export type DialectName = "postgres" | "sqlite";

export interface CompileOptions {
  dialect: DialectName;
}

/**
 * A compiled query: one SQL `SELECT` plus its positional bound parameters. `sql` returns a
 * single row with a single column `"rindle_result"` — the whole nested result as JSON
 * (`jsonb` on Postgres, `json` text on SQLite). No runtime value is ever interpolated into
 * `sql`; every filter/paging value is a bound parameter (Invariant 4).
 */
export interface CompiledQuery {
  sql: string;
  /** In order: `$1..$n` for Postgres, `?` for SQLite. */
  params: unknown[];
}

/**
 * Compile `ast` against `catalog` into `{ sql, params }` (§4). A pure function of its
 * inputs — no database access (Invariant 2). The root is **singular** when `ast.one` is set
 * (a single JSON object or `null`), else **plural** (a JSON array).
 */
export function compile(ast: Ast, catalog: Catalog, opts: CompileOptions): CompiledQuery {
  const singular = ast.one === true;
  switch (opts.dialect) {
    case "sqlite":
      return compileWith(ast, catalog, sqliteDialect, singular);
    case "postgres":
      return compileWith(ast, catalog, postgresDialect, singular);
  }
}
