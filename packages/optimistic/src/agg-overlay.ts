// The optimistic aggregate overlay (AGGREGATE-SYNC-DESIGN.md §4–§6) — the pure half of
// "displayed = server_base ⊕ watermark-gated local_pending_delta".
//
// The DISPLAY path (§3) makes a relationship `count(comments)` read a server-authoritative
// synthetic base table `__agg_*` (the reduce's `(group_key, count)` output) via a plain
// projected join. This module computes the CLIENT's own pending delta on top: a mini
// invertible counter over only the pending child mutations, correlated by the same group key
// (`+1` per pending child add, `−1` per remove, net per edit). The backend applies the delta
// as an optimistic add/edit/remove to the `__agg` head row, so the displayed scalar updates
// through the same join + projection with no special path.
//
// Two things are pure and live here: the per-aggregate DEFINITIONS derived from the AST
// (`AggDef`, incl. the §6.2 static `evaluable` gate), and the accumulator (`AggOverlay`) that
// folds observed child ops into a per-group delta. The engine-touching reconcile (reading
// `server_base`, writing the `__agg` head row) stays in `backend.ts`.

import type { Ast, Condition, ValuePosition, WireValue } from "@rindle/client";
import { aggTableName } from "@rindle/normalized";

/** The scalar comparison ops the v1 predicate evaluator (and the `evaluable` gate) cover.
 *  `LIKE`/`ILIKE`/`IN` are deliberately EXCLUDED — a filter using them is `evaluable: false`
 *  and falls back to the bare server value (avoids the LIKE-escape parity rabbit hole; always
 *  correct, just not optimistic for that aggregate). */
const SCALAR_OPS = new Set(["=", "!=", "<", "<=", ">", ">=", "IS", "IS NOT"]);

/** One aggregate's definition, derived from the original AST — everything needed to compute a
 *  per-group delta from the child table's pending mutations. */
export interface AggDef {
  /** The synthetic table name (`__agg_*`) the delta is applied to. */
  aggTable: string;
  /** The child table whose mutations drive the count (e.g. `"comment"`). */
  childTable: string;
  /** The child column names (schema order) — resolves the group key + filter columns. */
  childColumns: string[];
  /** Indices into a positional child row of the correlation child fields = the group key. */
  groupKeyCols: number[];
  /** The child `where`, if any (already baked into the server count; re-evaluated locally
   *  to decide a pending child's membership). */
  where?: Condition;
  /** §6.2: whether the optimistic delta is computable — the `where` references only the
   *  child's own columns + literals over scalar ops. Unevaluable ⇒ no delta (server value). */
  evaluable: boolean;
}

/** Collect every aggregate's `AggDef` from `ast` (recursively — a nested aggregate under a
 *  materialized relationship included), resolving child columns via `getColumns(table)`. The
 *  twin of `aggTableSchemas` (`@rindle/normalized`) but carrying the delta-computation fields. */
export function collectAggDefs(
  ast: Ast,
  getColumns: (table: string) => string[] | undefined,
  isLocal?: (table: string) => boolean,
): AggDef[] {
  const out: AggDef[] = [];
  for (const r of ast.related ?? []) {
    if (r.subquery.aggregate) {
      const childTable = r.subquery.table;
      // L1 (`201-LOCAL-ONLY-TABLES-DESIGN.md` §5.2): a count over a LOCAL child is a native reduce
      // with no server base and no pending stack to fold — the overlay manages nothing for it.
      if (isLocal?.(childTable)) continue;
      const childColumns = getColumns(childTable);
      if (!childColumns) continue; // child table not in the client schema — cannot key locally
      const groupKeyCols = r.correlation.childField.map((n) => childColumns.indexOf(n));
      if (groupKeyCols.some((i) => i < 0)) continue; // a correlation field not in the schema
      const where = r.subquery.where;
      out.push({
        aggTable: aggTableName(r),
        childTable,
        childColumns,
        groupKeyCols,
        where,
        evaluable: where === undefined || isEvaluable(where, childColumns),
      });
    } else {
      out.push(...collectAggDefs(r.subquery, getColumns, isLocal));
    }
  }
  return out;
}

/** §6.2 static gate: a condition is locally evaluable iff every leaf compares the child's own
 *  columns / literals with a scalar op. A `correlatedSubquery` (a join the client may not hold)
 *  or a column outside the child schema makes the whole aggregate unevaluable. */
export function isEvaluable(cond: Condition, childColumns: string[]): boolean {
  switch (cond.type) {
    case "simple":
      return SCALAR_OPS.has(cond.op) && pos(cond.left, childColumns) && pos(cond.right, childColumns);
    case "and":
    case "or":
      return cond.conditions.every((c) => isEvaluable(c, childColumns));
    case "correlatedSubquery":
      return false; // reaches into a join the client may not have
  }
}

function pos(vp: ValuePosition, childColumns: string[]): boolean {
  return vp.type === "literal" || childColumns.includes(vp.name);
}

/** Evaluate an EVALUABLE condition against a positional child row (SQL-ish three-valued logic
 *  collapsed to a boolean — an unknown comparison is `false`, i.e. the row is not a member).
 *  Only the grammar `isEvaluable` admits reaches here. */
export function evalCondition(cond: Condition, row: WireValue[], childColumns: string[]): boolean {
  switch (cond.type) {
    case "simple":
      return evalSimple(cond.op, value(cond.left, row, childColumns), value(cond.right, row, childColumns));
    case "and":
      return cond.conditions.every((c) => evalCondition(c, row, childColumns));
    case "or":
      return cond.conditions.some((c) => evalCondition(c, row, childColumns));
    case "correlatedSubquery":
      return false; // never evaluable — defensive
  }
}

function value(vp: ValuePosition, row: WireValue[], childColumns: string[]): WireValue {
  return vp.type === "literal" ? (vp.value as WireValue) : row[childColumns.indexOf(vp.name)];
}

function evalSimple(op: string, l: WireValue, r: WireValue): boolean {
  switch (op) {
    case "IS":
      return nullSafeEq(l, r); // null-safe: NULL IS NULL → true
    case "IS NOT":
      return !nullSafeEq(l, r);
    case "=":
      return l == null || r == null ? false : nullSafeEq(l, r);
    case "!=":
      return l == null || r == null ? false : !nullSafeEq(l, r);
    case "<":
      return less(l, r);
    case "<=":
      return less(l, r) || nullSafeEq(l, r);
    case ">":
      return less(r, l);
    case ">=":
      return less(r, l) || nullSafeEq(l, r);
    default:
      return false;
  }
}

function nullSafeEq(l: WireValue, r: WireValue): boolean {
  return l === r; // scalar cells: number/string/boolean/null compare by identity
}

function less(l: WireValue, r: WireValue): boolean {
  if (l == null || r == null) return false; // ordered comparison with NULL → unknown → false
  if (typeof l === "number" && typeof r === "number") return l < r;
  if (typeof l === "string" && typeof r === "string") return l < r;
  if (typeof l === "boolean" && typeof r === "boolean") return Number(l) < Number(r);
  return false; // mixed types: not a member (matches no row optimistically; self-corrects)
}

/** One observed child-table op (the wire shape the collector feeds the overlay). */
export interface ChildOp {
  table: string;
  kind: "add" | "remove" | "edit";
  row: WireValue[];
  /** The pre-image (edit only). */
  old?: WireValue[];
}

/** One group's accumulated optimistic delta. */
export interface DeltaEntry {
  aggTable: string;
  def: AggDef;
  /** The group-key cells (PK of the synthetic row). */
  cells: WireValue[];
  /** The net optimistic count delta for this group over the pending child mutations. */
  n: number;
}

/** The stateful accumulator: holds the aggregate definitions and the per-group pending delta.
 *  Engine-agnostic — the backend feeds it observed child ops and reads back the entries to
 *  reconcile against the `__agg` head rows. */
export class AggOverlay {
  /** By synthetic table name. Sharing a definition across queries is automatic (idempotent
   *  {@link register}); the backend refcounts readers and calls {@link unregister} when the
   *  last query over an aggregate is gone, so a definition is reclaimed with its table — not
   *  kept for the backend's life. */
  private readonly defs = new Map<string, AggDef>();
  /** Child table → the defs it feeds, for O(1) dispatch in `observe`. */
  private readonly byChild = new Map<string, AggDef[]>();
  /** `aggTable\0JSON(groupCells)` → the group's accumulated delta. */
  private readonly delta = new Map<string, DeltaEntry>();

  /** Register aggregate definitions (idempotent per synthetic table name). */
  register(defs: AggDef[]): void {
    for (const d of defs) {
      if (this.defs.has(d.aggTable)) continue;
      this.defs.set(d.aggTable, d);
      const list = this.byChild.get(d.childTable);
      if (list) list.push(d);
      else this.byChild.set(d.childTable, [d]);
    }
  }

  /** Forget an aggregate definition (its synthetic table is being removed because the last
   *  query reading it was unregistered — `AGGREGATE-SYNC-DESIGN.md` §4). Drops the def, its
   *  child-dispatch entry, and any accumulated delta for it. The inverse of the per-table half
   *  of {@link register}; a no-op for an unknown table. */
  unregister(aggTable: string): void {
    const d = this.defs.get(aggTable);
    if (!d) return;
    this.defs.delete(aggTable);
    const rest = (this.byChild.get(d.childTable) ?? []).filter((x) => x.aggTable !== aggTable);
    if (rest.length) this.byChild.set(d.childTable, rest);
    else this.byChild.delete(d.childTable);
    // Drop this table's accumulated groups (match on the stored aggTable — separator-agnostic).
    for (const [k, e] of this.delta) if (e.aggTable === aggTable) this.delta.delete(k);
  }

  /** Does any aggregate count rows of `table`? (cheap collector guard) */
  hasChild(table: string): boolean {
    return this.byChild.has(table);
  }

  /** Any aggregate definitions at all? */
  get hasDefs(): boolean {
    return this.defs.size > 0;
  }

  /** Drop all accumulated deltas — called at the start of a reconcile cycle rebuild, where the
   *  delta is recomputed from scratch off the (confirm-filtered) pending set. */
  reset(): void {
    this.delta.clear();
  }

  /** Fold one observed child op into the per-group delta, for every EVALUABLE aggregate over
   *  that child table. (Unevaluable defs contribute nothing — §6.2 server-value fallback.) */
  observe(op: ChildOp): void {
    const defs = this.byChild.get(op.table);
    if (!defs) return;
    for (const d of defs) {
      if (!d.evaluable) continue;
      if (op.kind === "add") this.bump(d, op.row, 1);
      else if (op.kind === "remove") this.bump(d, op.row, -1);
      else {
        if (op.old) this.bump(d, op.old, -1);
        this.bump(d, op.row, 1);
      }
    }
  }

  private bump(d: AggDef, childRow: WireValue[], sign: number): void {
    if (d.where && !evalCondition(d.where, childRow, d.childColumns)) return; // not a member
    const cells = d.groupKeyCols.map((i) => childRow[i]);
    const key = `${d.aggTable}|${JSON.stringify(cells)}`;
    const e = this.delta.get(key);
    if (e) e.n += sign;
    else this.delta.set(key, { aggTable: d.aggTable, def: d, cells, n: sign });
  }

  /** The current non-trivial deltas to reconcile against the `__agg` head rows. */
  entries(): DeltaEntry[] {
    return [...this.delta.values()];
  }

  /** Forget groups whose delta has returned to 0 (their head row was reverted to the server
   *  value by the reconcile that observed the 0) — keeps the map bounded between releases. */
  pruneZeros(): void {
    for (const [k, e] of this.delta) if (e.n === 0) this.delta.delete(k);
  }
}
