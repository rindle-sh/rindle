// The Zero-wire query AST — the exact JSON shape the Rust `Ast` deserializes
// (src/ast.rs). The builder emits these; the wasm `Db.query` (and a remote server) parse
// them. camelCase keys, `type`-tagged unions, ops as SQL-ish strings, untagged literals.

export type Dir = "asc" | "desc";

/** One `(field, direction)` ordering — serializes as the 2-tuple `["id", "asc"]`. */
export type OrderPart = [field: string, dir: Dir];

/** An untagged literal (`null | bool | number | string | array`). Numbers are one f64. */
export type LitValue = null | boolean | number | string | LitValue[];

/** A column reference or a literal — `type`-tagged. */
export type ValuePosition =
  | { type: "column"; name: string }
  | { type: "literal"; value: LitValue };

/** The wire comparison operators (exact strings, `src/ast.rs` `Op`). */
export type SimpleOp =
  | "="
  | "!="
  | "<"
  | "<="
  | ">"
  | ">="
  | "IS"
  | "IS NOT"
  | "LIKE"
  | "NOT LIKE"
  | "ILIKE"
  | "NOT ILIKE"
  | "IN"
  | "NOT IN";

export type ExistsOp = "EXISTS" | "NOT EXISTS";

/** The filter tree — `type`-tagged; recursive. There is no generic NOT node (negation is
 *  via the negated operators and `NOT EXISTS`, matching `src/ast.rs` `Condition`). */
export type Condition =
  | { type: "simple"; op: SimpleOp; left: ValuePosition; right: ValuePosition }
  | { type: "and"; conditions: Condition[] }
  | { type: "or"; conditions: Condition[] }
  | { type: "correlatedSubquery"; related: CorrelatedSubquery; op: ExistsOp; flip?: boolean; scalar?: boolean };

export interface Correlation {
  parentField: string[];
  childField: string[];
}

/** A paging lower bound (`Bound`, `src/ast.rs`). `row` is a *partial* wire row — the bound
 *  columns by name (only the sort columns are read by the engine's `Skip` comparator).
 *  `exclusive` ⇒ start *after* the bound row (`Basis::After`); else *at* it (`Basis::At`). */
export interface Bound {
  row: Record<string, LitValue>;
  exclusive: boolean;
}

export interface CorrelatedSubquery {
  correlation: Correlation;
  subquery: Ast;
  system?: "client" | "permissions" | "test";
}

/** An aggregate over a (correlated) subquery's rows (`REDUCE-DESIGN.md`). v1: `count(*)`.
 *  Set on a `related` subquery (`Ast.aggregate`), it marks that relationship a count
 *  aggregate the builder lowers to a scalar-projected singular relationship (§9). */
export type Aggregate = "count";

/** The query AST. `table` is the only required field; the builder omits empty/false fields
 *  (matching the Rust `skip_serializing_if`), which the deserializer treats as absent. */
export interface Ast {
  table: string;
  alias?: string;
  /** Projection (PROJECTION-SUPPORT-DESIGN.md §6). Absent ⇒ select all columns; present ⇒
   *  project to just these (drives what syncs + what the view reports). Serializes only when
   *  set, matching the Rust `Ast.select`'s `skip_serializing_if`. */
  select?: string[];
  where?: Condition;
  related?: CorrelatedSubquery[];
  start?: Bound;
  limit?: number;
  one?: boolean;
  /** Aggregate this (sub)query's rows instead of materializing them (`REDUCE-DESIGN.md`
   *  §9). `count` on a `related` subquery surfaces a scalar `commentCount` field; the
   *  builder lowers it to a scalar-projected singular relationship. Absent ⇒ ordinary rows. */
  aggregate?: Aggregate;
  /** The {@link Ast.aggregate} value is **precomputed** — supplied as rows of a (synthetic)
   *  source table rather than reduced from child rows. Set by the normalized client's AST
   *  rewrite (`AGGREGATE-SYNC-DESIGN.md` §3.3) so the local engine reads the server's count
   *  with a plain singular join + the same projection instead of a `reduce`. Only meaningful
   *  with `aggregate`; absent ⇒ `false`. */
  aggregatePrecomputed?: boolean;
  /** Top-level `GROUP BY` columns (names), meaningful only alongside a root {@link Ast.aggregate}
   *  (`REDUCE-DESIGN.md` §8). Empty + `aggregate` set ⇒ a **global** aggregate (one `[count]` row);
   *  non-empty ⇒ one `[group…, count]` row per distinct value-tuple. Distinct from a relationship
   *  aggregate's implicit grouping (the correlation child key). Absent on the wire ⇒ empty. */
  groupBy?: string[];
  /** `HAVING` — a filter over the **post-aggregation** rows of a root {@link Ast.aggregate}
   *  (`REDUCE-DESIGN.md` §4: a filter directly above the `reduce`). Its condition addresses the
   *  aggregate's *output* columns — the {@link Ast.groupBy} columns and the synthetic `count`
   *  column — not base-table columns (those go in {@link Ast.where}, which filters rows *below* the
   *  reduce). Absent ⇒ no post-aggregation filter. */
  having?: Condition;
  orderBy?: OrderPart[];
}
