// The compiler core — a syntax-directed `Ast → SELECT` lowering, ported one-to-one from
// `rindle-d2s` (rust/rindle-d2s/src/lib.rs) and bound to our exact wire `Ast`. The one
// deliberate change from `rindle-d2s` is that filter/paging **values are parameterized**
// (pushed onto `params`, emitted as placeholders by the dialect) instead of inlined — so the
// output is injection-safe and plan-cacheable (Invariant 4). Everything else — the walker
// structure, ordering completion, keyset-paging predicate, empty-fold — matches the oracle.

import type {
  Ast,
  Bound,
  Condition,
  CorrelatedSubquery,
  Correlation,
  LitValue,
  OrderPart,
  ValuePosition,
} from "./ast.ts";
import type { Catalog, ColumnType } from "./catalog.ts";
import type { Dialect, LitScalar } from "./dialect.ts";
import { completeOrdering } from "./complete-ordering.ts";
import { missingAlias, unknownRelationship, unknownTable } from "./errors.ts";
import { rejectUnsupported } from "./reject.ts";

/** A `simple` comparison condition. */
type SimpleCond = Extract<Condition, { type: "simple" }>;
/** An EXISTS/NOT-EXISTS condition. */
type ExistsCond = Extract<Condition, { type: "correlatedSubquery" }>;

/** The table + alias a condition/projection is evaluated against (for column-type lookup). */
interface Scope {
  table: string;
  alias: string;
}

export interface Compiled {
  sql: string;
  params: unknown[];
}

/** Compile `ast` against `catalog` for `dialect`. `singular` ⇒ a `.one()` root (single
 *  object or `null`); else a plural root (a JSON array). Pure: mutates only a clone. */
export function compileWith(
  ast: Ast,
  catalog: Catalog,
  dialect: Dialect,
  singular: boolean,
): Compiled {
  rejectUnsupported(ast);
  const cloned = structuredClone(ast);
  completeOrdering(cloned, (t) => pkOf(catalog, t));
  const cx = new Cx(catalog, dialect);
  const expr = cx.collection(cloned, singular, null);
  return { sql: `SELECT ${expr} AS "rindle_result"`, params: cx.params };
}

/** Compile context: the catalog, dialect, the accumulating params, and an alias counter. */
class Cx {
  readonly params: unknown[] = [];
  #n = 0;
  // Explicit fields (not constructor parameter properties): Node's strip-only type stripping
  // does not support parameter properties, and this source runs directly from `.ts`.
  readonly #catalog: Catalog;
  readonly #d: Dialect;

  constructor(catalog: Catalog, d: Dialect) {
    this.#catalog = catalog;
    this.#d = d;
  }

  #alias(table: string): string {
    return `${table}_${this.#n++}`;
  }

  /** A scalar-subquery expression producing the JSON for `ast` as a single object
   *  (`singular`) or a `json_group_array`/`jsonb_agg` of objects, correlated to a parent
   *  when `correlate` is set (`[parentAlias, Correlation]`). */
  collection(ast: Ast, singular: boolean, correlate: [string, Correlation] | null): string {
    const table = ast.table;
    const cols = columnsOf(this.#catalog, table);
    const alias = this.#alias(table);
    const scope: Scope = { table, alias };
    const row = this.#rowObject(ast, scope, cols);

    const conds: string[] = [];
    if (correlate) conds.push(correlateSql(this.#d, correlate[0], alias, correlate[1]));
    if (ast.where) conds.push(this.#whereSql(ast.where, scope));
    // Keyset paging: keep rows at/after `start` under the already-PK-completed sort — the
    // exact comparator the engine's `Skip` uses.
    if (ast.start) conds.push(this.#startSql(ast.start, ast.orderBy ?? [], scope));

    const from = `${this.#d.quoteIdent(table)} AS ${this.#d.quoteIdent(alias)}`;
    return this.#pluralOrSingular(singular, row, from, whereClause(conds), ast.orderBy ?? [], alias, ast.limit);
  }

  /** `json_object('c', a."c", …, 'rel', <child expr>, …)`. A column shadowed by a
   *  same-named relationship is dropped (the relationship wins, as in the View). */
  #rowObject(ast: Ast, scope: Scope, cols: string[]): string {
    const relNames = new Set(
      (ast.related ?? []).map((r) => r.subquery.alias).filter((a): a is string => a != null),
    );
    const selected = ast.select ? new Set(ast.select) : null;

    const parts: string[] = [];
    for (const col of cols) {
      if (relNames.has(col)) continue;
      if (selected && !selected.has(col)) continue;
      const colRef = `${this.#d.quoteIdent(scope.alias)}.${this.#d.quoteIdent(col)}`;
      const projected = this.#d.projectColumn(colRef, columnTypeOf(this.#catalog, scope.table, col));
      parts.push(`${jsonKey(col)}, ${projected}`);
    }
    for (const rel of ast.related ?? []) {
      const name = rel.subquery.alias;
      if (name == null) throw missingAlias(rel.subquery.table);
      // A relationship aggregate (`count(child)`) is a scalar column, not a nested collection.
      const expr =
        rel.subquery.aggregate === "count"
          ? this.#countExpr(rel, scope.alias)
          : this.#relationExpr(scope.table, rel, scope.alias);
      parts.push(`${jsonKey(name)}, ${expr}`);
    }
    return this.#d.objectBuild(parts);
  }

  /** One materialized relationship as a nested-JSON column. */
  #relationExpr(parentTable: string, rel: CorrelatedSubquery, parentAlias: string): string {
    const name = rel.subquery.alias;
    if (name == null) throw missingAlias(rel.subquery.table);
    const singular = cardinalityOf(this.#catalog, parentTable, name) === "one";
    return this.collection(rel.subquery, singular, [parentAlias, rel.correlation]);
  }

  /** A scalar `count(*)` for a relationship aggregate: `(SELECT count(*) FROM child AS a
   *  WHERE <correlation> [AND <child.where>])`. `0` when no child matches. */
  #countExpr(rel: CorrelatedSubquery, parentAlias: string): string {
    const sub = rel.subquery;
    const alias = this.#alias(sub.table);
    const conds = [correlateSql(this.#d, parentAlias, alias, rel.correlation)];
    if (sub.where) conds.push(this.#whereSql(sub.where, { table: sub.table, alias }));
    return `(SELECT count(*) FROM ${this.#d.quoteIdent(sub.table)} AS ${this.#d.quoteIdent(alias)} WHERE ${conds.join(" AND ")})`;
  }

  /** A boolean SQL expression for a `where` condition tree. */
  #whereSql(cond: Condition, scope: Scope): string {
    switch (cond.type) {
      case "and":
        return cond.conditions.length === 0
          ? this.#d.trueLit
          : `(${cond.conditions.map((c) => this.#whereSql(c, scope)).join(" AND ")})`;
      case "or":
        return cond.conditions.length === 0
          ? this.#d.falseLit
          : `(${cond.conditions.map((c) => this.#whereSql(c, scope)).join(" OR ")})`;
      case "correlatedSubquery":
        return this.#existsSql(cond, scope.alias);
      case "simple":
        return this.#simpleSql(cond, scope);
    }
  }

  /** `[NOT] EXISTS (SELECT 1 FROM child AS a WHERE <correlation> [AND <child.where>])`.
   *  `flip`/`scalar` are IVM routing flags with no effect on the SQL. */
  #existsSql(c: ExistsCond, parentAlias: string): string {
    const sub = c.related.subquery;
    const alias = this.#alias(sub.table);
    const conds = [correlateSql(this.#d, parentAlias, alias, c.related.correlation)];
    if (sub.where) conds.push(this.#whereSql(sub.where, { table: sub.table, alias }));
    const inner = `SELECT 1 FROM ${this.#d.quoteIdent(sub.table)} AS ${this.#d.quoteIdent(alias)} WHERE ${conds.join(" AND ")}`;
    return c.op === "EXISTS" ? `EXISTS (${inner})` : `NOT EXISTS (${inner})`;
  }

  /** A single comparison. Operators render verbatim except: `IS`/`IS NOT` render the RHS as
   *  a SQL keyword (`NULL`/`TRUE`/`FALSE`) not a param; `ILIKE` folds via `lower()`; `IN`/
   *  `NOT IN` bind the list (empty ⇒ the constant truth value). */
  #simpleSql(sc: SimpleCond, scope: Scope): string {
    // A literal is bound with the *other* side's column type when that side is a column
    // (drives Postgres casts); `null` ⇒ infer from the JS type.
    const leftColType = this.#colTypeOf(scope, sc.left);
    const rightColType = this.#colTypeOf(scope, sc.right);
    const l = this.#valuePos(scope, sc.left, rightColType);
    const r = () => this.#valuePos(scope, sc.right, leftColType);
    switch (sc.op) {
      case "=":
        return `${l} = ${r()}`;
      case "!=":
        return `${l} != ${r()}`;
      case "<":
        return `${l} < ${r()}`;
      case "<=":
        return `${l} <= ${r()}`;
      case ">":
        return `${l} > ${r()}`;
      case ">=":
        return `${l} >= ${r()}`;
      case "IS":
        return `${l} IS ${this.#isRhs(scope, sc.right)}`;
      case "IS NOT":
        return `${l} IS NOT ${this.#isRhs(scope, sc.right)}`;
      case "LIKE":
        return `${l} LIKE ${r()} ESCAPE '\\'`;
      case "NOT LIKE":
        return `${l} NOT LIKE ${r()} ESCAPE '\\'`;
      case "ILIKE":
        return `lower(${l}) LIKE lower(${r()}) ESCAPE '\\'`;
      case "NOT ILIKE":
        return `lower(${l}) NOT LIKE lower(${r()}) ESCAPE '\\'`;
      case "IN":
        return this.#inSql(scope, l, sc.right, false, leftColType);
      case "NOT IN":
        return this.#inSql(scope, l, sc.right, true, leftColType);
    }
  }

  /** The RHS of `IS`/`IS NOT` — a SQL keyword position (Postgres forbids a param here), so
   *  `NULL`/`TRUE`/`FALSE` render literally. Any other RHS (unusual) falls back to a value. */
  #isRhs(scope: Scope, vp: ValuePosition): string {
    if (vp.type === "literal") {
      if (vp.value === null) return "NULL";
      if (vp.value === true) return this.#d.trueLit;
      if (vp.value === false) return this.#d.falseLit;
    }
    return this.#valuePos(scope, vp, null);
  }

  /** The column type of `vp` when it is a column reference; `null` for a literal. */
  #colTypeOf(scope: Scope, vp: ValuePosition): ColumnType | null {
    return vp.type === "column" ? columnTypeOf(this.#catalog, scope.table, vp.name) : null;
  }

  /** A column reference (`a."c"`) or a bound literal (`?` / `$N::text::<type>`). `siblingCol`
   *  is the compared column's type, used when `vp` is a literal. */
  #valuePos(scope: Scope, vp: ValuePosition, siblingCol: ColumnType | null): string {
    if (vp.type === "column") {
      return `${this.#d.quoteIdent(scope.alias)}.${this.#d.quoteIdent(vp.name)}`;
    }
    return this.#d.bindValue(this.params, vp.value as LitScalar, siblingCol, true);
  }

  /** `lhs [NOT] IN (v0, v1, …)`. An empty list folds to the constant truth value; a non-array
   *  RHS falls back to `=`/`!=` (as `rindle-d2s`'s `in_sql`). */
  #inSql(
    scope: Scope,
    lhs: string,
    right: ValuePosition,
    negate: boolean,
    lhsCol: ColumnType | null,
  ): string {
    if (right.type === "literal" && Array.isArray(right.value)) {
      const items = right.value;
      if (items.length === 0) return negate ? this.#d.trueLit : this.#d.falseLit;
      const list = items
        .map((it) => this.#d.bindValue(this.params, it as LitScalar, lhsCol, true))
        .join(", ");
      return `${lhs} ${negate ? "NOT IN" : "IN"} (${list})`;
    }
    const rr = this.#valuePos(scope, right, lhsCol);
    return negate ? `${lhs} != ${rr}` : `${lhs} = ${rr}`;
  }

  /** Emit the plural (`json_group_array`/`jsonb_agg`) or singular (`… LIMIT 1`) form. When a
   *  `LIMIT` is present the rows are selected (ordered + limited) in an inner subquery and
   *  then array-aggregated in declared order, so both the top-N selection and the array order
   *  are correct. */
  #pluralOrSingular(
    singular: boolean,
    row: string,
    from: string,
    where: string,
    order: OrderPart[],
    alias: string,
    limit: number | undefined,
  ): string {
    if (singular) {
      return `(SELECT ${row} FROM ${from}${where}${this.#orderClause(order, alias)} LIMIT 1)`;
    }
    if (limit !== undefined) {
      const innerOrder = this.#orderClause(order, alias);
      const proj = this.#orderProj(order, alias);
      const agg = this.#aggOrderProjected(order);
      // Re-assert the JSON subtype: the object round-trips through the inner subquery's
      // `AS __r` column, which SQLite would otherwise re-encode as a string.
      const elem = this.#d.reassertJson("__r");
      const lim = Math.max(0, Math.trunc(Number(limit)));
      return `COALESCE((SELECT ${this.#d.groupArray(elem, agg)} FROM (SELECT ${row} AS __r${proj} FROM ${from}${where}${innerOrder} LIMIT ${lim})), ${this.#d.emptyArray})`;
    }
    const agg = this.#aggOrderInline(order, alias);
    return `COALESCE((SELECT ${this.#d.groupArray(row, agg)} FROM ${from}${where}), ${this.#d.emptyArray})`;
  }

  /** ` ORDER BY a."c0" ASC, a."c1" DESC` (empty when `order` is empty). */
  #orderClause(order: OrderPart[], alias: string): string {
    return order.length === 0 ? "" : ` ORDER BY ${this.#orderTerms(order, alias)}`;
  }

  /** In-aggregate ordering referencing the FROM columns. */
  #aggOrderInline(order: OrderPart[], alias: string): string {
    return order.length === 0 ? "" : ` ORDER BY ${this.#orderTerms(order, alias)}`;
  }

  /** In-aggregate ordering over the inner subquery's projected order columns (`__o0`, …). */
  #aggOrderProjected(order: OrderPart[]): string {
    if (order.length === 0) return "";
    return ` ORDER BY ${order.map(([, dir], i) => `__o${i} ${this.#d.orderDir(dir)}`).join(", ")}`;
  }

  /** Project the order columns into the inner subquery: `, a."c0" AS __o0, …`. */
  #orderProj(order: OrderPart[], alias: string): string {
    return order
      .map(([field], i) => `, ${this.#d.quoteIdent(alias)}.${this.#d.quoteIdent(field)} AS __o${i}`)
      .join("");
  }

  #orderTerms(order: OrderPart[], alias: string): string {
    return order
      .map(([field, dir]) => `${this.#d.quoteIdent(alias)}.${this.#d.quoteIdent(field)} ${this.#d.orderDir(dir)}`)
      .join(", ");
  }

  /** The keyset paging predicate for a `start` bound: keep a row iff the bound sorts before
   *  it (`B < R`), or sorts equal with an inclusive bound (`B <= R`), under the already-PK-
   *  completed sort. Lexicographic with **null-low** ordering (NULL before any value on ASC,
   *  after every value on DESC) — faithful to the engine's `Skip`. */
  #startSql(start: Bound, order: OrderPart[], scope: Scope): string {
    // Per sort column: the column ref, direction, bound value (absent/NULL ⇒ null-low), type.
    const cols = order.map(([field, dir]) => ({
      x: `${this.#d.quoteIdent(scope.alias)}.${this.#d.quoteIdent(field)}`,
      asc: dir === "asc",
      b: nullLowBound(start.row[field]),
      col: columnTypeOf(this.#catalog, scope.table, field),
    }));
    if (cols.length === 0) return this.#d.trueLit; // no sort columns (unreachable post-completion)

    // `B < R` = OR_i ( AND_{j<i} eq_j AND before_i ). Each `eq`/`before` fragment is
    // (re)emitted exactly where it appears — regenerated, not string-reused — so every
    // placeholder gets its own bound param, in left-to-right order. (`rindle-d2s` can reuse a
    // fragment string because its literals are inlined and side-effect-free; our params are
    // positional, so a reused or discarded fragment would desync placeholders from params.)
    const orTerms: string[] = [];
    for (let i = 0; i < cols.length; i++) {
      const conj: string[] = [];
      for (let j = 0; j < i; j++) conj.push(this.#eqSql(cols[j].x, cols[j].b, cols[j].col));
      conj.push(this.#beforeSql(cols[i].x, cols[i].asc, cols[i].b, cols[i].col));
      orTerms.push(`(${conj.join(" AND ")})`);
    }
    const strictLt = `(${orTerms.join(" OR ")})`;
    if (start.exclusive) return strictLt;
    // `B <= R` adds the all-columns-equal term.
    const allEq = cols.map((c) => this.#eqSql(c.x, c.b, c.col)).join(" AND ");
    return `(${strictLt} OR (${allEq}))`;
  }

  /** `x` strictly after the bound value `b` in this column's direction, null-low. `b` is
   *  `undefined` for an omitted/NULL bound. */
  #beforeSql(x: string, asc: boolean, b: LitScalar | undefined, col: ColumnType | null): string {
    if (b === undefined) {
      // NULL bound: before every non-null `x` on ASC; never before on DESC.
      return asc ? `${x} IS NOT NULL` : this.#d.falseLit;
    }
    const v = this.#d.bindValue(this.params, b, col, true);
    // ASC: `v < x` (NULL `x` ⇒ NULL ⇒ false, i.e. a null row is not after a non-null bound).
    // DESC: a null `x` sorts last, so it is after the bound; else `v > x`.
    return asc ? `${v} < ${x}` : `(${x} IS NULL OR ${v} > ${x})`;
  }

  /** `x` equal to the bound value `b` under the sort's null-identical equality. */
  #eqSql(x: string, b: LitScalar | undefined, col: ColumnType | null): string {
    if (b === undefined) return `${x} IS NULL`;
    return `${x} = ${this.#d.bindValue(this.params, b, col, true)}`;
  }
}

// ── free helpers (dialect-independent) ────────────────────────────────────────────────────

/** `parent."p0" = child."c0" AND parent."p1" = child."c1" …`. */
function correlateSql(d: Dialect, parentAlias: string, childAlias: string, corr: Correlation): string {
  return corr.parentField
    .map((p, i) => {
      const c = corr.childField[i];
      return `${d.quoteIdent(parentAlias)}.${d.quoteIdent(p)} = ${d.quoteIdent(childAlias)}.${d.quoteIdent(c)}`;
    })
    .join(" AND ");
}

function whereClause(conds: string[]): string {
  return conds.length === 0 ? "" : ` WHERE ${conds.join(" AND ")}`;
}

/** A `json_object`/`jsonb_build_object` key: a single-quoted string literal. */
function jsonKey(name: string): string {
  return `'${name.replace(/'/g, "''")}'`;
}

/** A paging bound value, with an absent or explicit-NULL value normalized to `undefined`
 *  (the null-low sentinel `beforeSql`/`eqSql` treat as "sorts below everything"). */
function nullLowBound(raw: LitValue | undefined): LitScalar | undefined {
  return raw === undefined || raw === null ? undefined : (raw as LitScalar);
}

// ── catalog accessors ─────────────────────────────────────────────────────────────────────

function columnsOf(catalog: Catalog, table: string): string[] {
  const t = catalog.tables[table];
  if (!t) throw unknownTable(table);
  return t.columns;
}

function pkOf(catalog: Catalog, table: string): string[] {
  return catalog.tables[table]?.primaryKey ?? [];
}

function cardinalityOf(catalog: Catalog, table: string, rel: string): "one" | "many" {
  const card = catalog.tables[table]?.relationships[rel];
  if (!card) throw unknownRelationship(table, rel);
  return card;
}

function columnTypeOf(catalog: Catalog, table: string, col: string): ColumnType | null {
  return catalog.tables[table]?.columnTypes[col] ?? null;
}
