// The static, declared type catalog (POSTGRES-READ-COMPILER-DESIGN.md §7) — the second
// input to the compiler, beside the `Ast`. Compilation is a pure function of
// `(Ast, Catalog)`; the catalog carries the facts the AST does not: column order, primary
// key, relationship cardinality, and — for the Postgres dialect — the per-column native
// type detail the `::text::<type>` cast strategy (§6.2) branches on.

/** Whether a relationship yields a single child object (`one`) or an array (`many`). */
export type Cardinality = "one" | "many";

/**
 * Per-column type detail the Postgres compiler needs for value-model↔native-type
 * reconciliation (§6.2). Mirrors z2s's `ServerColumnSchema`: the raw Postgres type name
 * plus the two flags the cast switch branches on. The extension over what
 * `rindle-pg-source` derives today (§7, review decision 2): enums and arrays are carried
 * distinctly instead of collapsing into a text fallback.
 *
 * - `type` — the native Postgres type name: `"int4"`, `"text"`, `"bool"`, `"float8"`,
 *   `"numeric"`, `"timestamptz"`, `"timestamp"`, `"date"`, `"timetz"`, `"time"`, `"uuid"`,
 *   `"json"`/`"jsonb"`, or an **enum type name** (paired with `isEnum: true`).
 * - `isEnum` — `type` names a Postgres enum ⇒ cast via `$N::text::"<type>"`.
 * - `isArray` — the column is an array ⇒ unnest via `jsonb_array_elements_text`.
 *
 * The SQLite oracle dialect ignores this entirely: it binds native values (no casts).
 */
export interface ColumnType {
  type: string;
  isEnum: boolean;
  isArray: boolean;
}

/**
 * Per-table metadata the compiler needs beyond the `Ast`:
 * - `columns` — the projected column list, **in projection order** (the order
 *   `json_object`/`jsonb_build_object` enumerates), matching the View's `Schema`.
 * - `primaryKey` — the PK columns, for ordering-completion (§8): the compiler appends the
 *   full PK to every `orderBy` at every level for a total order, exactly as the engine's
 *   builder does (`rindle::complete_ordering`).
 * - `columnTypes` — per-column native type detail, keyed by column name (§6.2; Postgres
 *   dialect only).
 * - `relationships` — declared relationships → cardinality. This is the one structural
 *   fact not carried by the `Ast` (the relationship *shape* — correlation keys, nesting,
 *   where/order/limit — lives in the `Ast`'s `related` subqueries).
 */
export interface TableSchema {
  columns: string[];
  primaryKey: string[];
  columnTypes: Record<string, ColumnType>;
  relationships: Record<string, Cardinality>;
}

/**
 * The catalog: every table the compiler may reference, keyed by table name. Produced
 * statically by `rindle pg prepare` from an ephemeral migration-built Postgres (§7,
 * Phase B); hand-authored for tests. A pure input — the compiler never touches a database
 * to obtain it.
 */
export interface Catalog {
  tables: Record<string, TableSchema>;
}
