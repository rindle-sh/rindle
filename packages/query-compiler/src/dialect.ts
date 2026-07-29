// The dialect seam (§6.1). The walker is dialect-independent; only these leaves differ
// between SQLite and Postgres: the Postgres leaf carries the §6.2 `::text::<type>` casts and
// explicit NULLS ordering; the SQLite leaf binds natives with NO casts (the canonical store
// needs no reconciliation — DAEMON-INTERACTIVE-TXN-DESIGN.md §5.4).

import type { Dir } from "./ast.ts";
import type { ColumnType } from "./catalog.ts";

/** A scalar value bindable as a parameter (an array filter value is split into scalars). */
export type LitScalar = null | boolean | number | string;

export interface Dialect {
  readonly name: "postgres" | "sqlite";

  /** `json_object('k', v, …)` | `jsonb_build_object('k', v, …)`. `parts` are the flat
   *  `'key', valueExpr` fragments, in order. */
  objectBuild(parts: string[]): string;

  /** The array aggregate over `elem` with `orderSql` (e.g. ` ORDER BY __o0 ASC`) inside:
   *  `json_group_array(elem ORDER BY …)` | `jsonb_agg(elem ORDER BY …)`. */
  groupArray(elem: string, orderSql: string): string;

  /** The empty-array default for the `COALESCE` wrapper: `'[]'` | `'[]'::jsonb`. */
  readonly emptyArray: string;

  /** Re-assert the JSON subtype on a value round-tripped through a subquery's `AS` column
   *  (`json(x)` in SQLite; identity in Postgres, where `jsonb` is a real type). */
  reassertJson(x: string): string;

  /** The truthy/falsy constants for empty `AND`/`OR` folding and boolean keyword positions:
   *  `1`/`0` | `TRUE`/`FALSE`. */
  readonly trueLit: string;
  readonly falseLit: string;

  /** An `ORDER BY` direction with the engine's null-low ordering made explicit where the
   *  dialect default differs (§8): SQLite is null-low by default (`ASC`/`DESC`); Postgres's
   *  default is the opposite, so it must emit `ASC NULLS FIRST` / `DESC NULLS LAST`. */
  orderDir(dir: Dir): string;

  /** Project a stored column into the value model's representation for the result JSON.
   *  Identity in SQLite; the outbound half of §6.2 in Postgres (e.g. timestamptz → epoch-ms). */
  projectColumn(colRef: string, col: ColumnType | null): string;

  /** Bind a scalar filter/paging value as a parameter: push onto `params`, return the
   *  placeholder SQL. `col` is the compared column's type when known (drives the Postgres
   *  `::text::<type>` casts, §6.2); `null` ⇒ infer from the JS type. SQLite ignores `col`
   *  and `isComparison` — it binds natives. */
  bindValue(params: unknown[], value: LitScalar, col: ColumnType | null, isComparison: boolean): string;

  /** Double-quote an identifier, doubling embedded `"`. Identical across dialects (kept on
   *  the seam for uniformity). */
  quoteIdent(name: string): string;
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/**
 * The SQLite dialect — the daemon backend's session-read target (DAEMON-INTERACTIVE-TXN §5.4)
 * and the offline differential oracle (§9). It binds native values as parameters (no
 * `::text::<type>` casts — SQLite is the canonical store, so there is no driver seam to pin
 * and no second representation to reconcile), relies on SQLite's null-low default ordering
 * (which matches the engine's `null < everything`), and re-asserts the `json()` subtype where
 * objects round-trip through a subquery column. Matches `rindle-d2s`'s raw SQL shapes so the
 * two agree modulo parameterization; the executing connection must run
 * `PRAGMA case_sensitive_like = ON` (every daemon cluster connection does — `open_wal2`).
 */
export const sqliteDialect: Dialect = {
  name: "sqlite",
  objectBuild: (parts) => `json_object(${parts.join(", ")})`,
  groupArray: (elem, orderSql) => `json_group_array(${elem}${orderSql})`,
  emptyArray: "'[]'",
  reassertJson: (x) => `json(${x})`,
  trueLit: "1",
  falseLit: "0",
  orderDir: (dir) => (dir === "asc" ? "ASC" : "DESC"),
  projectColumn: (colRef) => colRef,
  bindValue: (params, value) => {
    // SQLite has no boolean type; its driver binds 1/0. Everything else binds natively.
    params.push(typeof value === "boolean" ? (value ? 1 : 0) : value);
    return "?";
  },
  quoteIdent,
};
