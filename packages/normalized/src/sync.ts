// NormalizedSync — the client-side cross-query refcount + GC layer of the normalized
// local-first path (NORMALIZED-CHANGES-DESIGN.md §5), extended with the client column
// UNION for projection support (PROJECTION-SUPPORT-DESIGN.md §4). It sits between the
// per-query normalized streams (each an independent, validated `NormalizedOp` batch from
// the server, via `@rindle/remote`) and the client's one shared normalized store (the wasm
// `Db`'s base tables). The server already de-duplicates *intra-query* multi-path (§4.2);
// this layer does the *cross-query* refcount the server deliberately does NOT — so a base
// row shared by N queries lands in the local `Db` exactly once and is GC'd only when the
// last query stops referencing it.
//
// It is the half the design calls out as "NO CVR" ([[no-cvr-client-refcounts]]): the
// refcount is per-row on the *client*, keyed by `(table, pk)`, with each query's footprint
// tracked as a membership set. The query-id rides at the subscription envelope, never on a
// row (§5.1) — so this layer is fed per-query (`applyBatch(queryId, ops)`).
//
// **Projection (the client column union, PROJECTION-SUPPORT-DESIGN.md §4).** A *projected*
// query streams rows positional over its OWN (narrower) schema; the client scatters those
// cells into the shared base-table positions via the query's registered column map
// (`registerProjection`) and leaves the rest ABSENT (the `undefined` marshal token, §5.3).
// The same map also absorbs the reverse skew — a server WIDER than the client (an expanded
// table mid `expand-then-contract` migration): a wire column the client doesn't have maps to a
// DROP sentinel (`-1`) and its cell is discarded, so an old client keeps working against a
// newer server. (Mapping is BY NAME, so neither a narrower nor a wider server mis-aligns.)
// The base row a query group holds is the **union over all referencing queries'
// projections** (§2): widening (`add`/`edit` brings new columns) column-merges them in;
// narrowing (`dropQuery`, or a query's `remove` of a still-referenced row) recomputes the
// union from the *remaining* queries and resets dropped columns to ABSENT — the client-side
// equivalent of Zero's `remove-keys` (§4.2). A `'*'` query (no registered projection)
// contributes all columns, so any row it references is full and present everywhere: the
// union never introduces ABSENT and behavior is byte-identical to before projection (§7).
//
// Output: a NET `Mutation[]` to apply to the wasm `Db` in ONE `write()` transaction (the
// design's "stage into one txn, commit once"). This module is pure logic — it never
// touches the `Db`; the `NormalizedStore` (Slice 5) feeds the mutations to it.

import type { Mutation, NormalizedOp, QueryId, WireValue } from "@rindle/client";

// `NormalizedOp` (the table-tagged, path-free wire op, §3) lives in `@rindle/client` so both the
// protocol (`@rindle/remote`) and this sync layer share one type. Re-exported here for callers.
export type { NormalizedOp } from "@rindle/client";

/** Per-table primary-key column indices (into a positional row) — how `(table, pk)` is keyed. */
export type PkCols = Record<string, number[]>;

/** Per-table FULL column count — the width of a base/union row. Needed to scatter a projected
 *  (narrower) wire row into the shared positional layout (PROJECTION-SUPPORT-DESIGN.md §5.3). */
export type ColCounts = Record<string, number>;

/**
 * The Absent token on the JS side (PROJECTION-SUPPORT-DESIGN.md §5.3): an absent cell in a
 * positional UNION row. The wasm marshal maps `undefined` → `OwnedValue::Absent` (and `null`
 * → present-and-null), so forwarding a union row with `undefined` cells is how the local
 * engine learns a column is not present on a shared row.
 */
const ABSENT = undefined;

/** A union-row cell: a present wire value, or `ABSENT` (`undefined`) when the column is not
 *  present on the shared row. */
type UnionCell = WireValue | undefined;

interface BaseEntry {
  table: string;
  /** The shared UNION row (present cells filled; absent cells carry {@link ABSENT}). */
  row: UnionCell[];
  /** The queries currently referencing this row. Its size is the cross-query refcount; its
   *  members drive the per-row column union (§4.1). */
  queries: Set<QueryId>;
}

/** Elementwise equality over positional rows (json columns compare as strings; `ABSENT`
 *  compares equal only to `ABSENT`). */
function rowEq(a: UnionCell[], b: UnionCell[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

export class NormalizedSync {
  private readonly pkCols: PkCols;
  /** Per-table full width, for scattering projected rows. Empty ⇒ projection unsupported
   *  (every query must be `'*'`), which is the pre-projection behavior. */
  private readonly colCounts: ColCounts;
  /** `(table, pk)` key → the one shared base entry. */
  private readonly base = new Map<string, BaseEntry>();
  /** queryId → the set of `(table, pk)` keys that query currently footprints. */
  private readonly qfoot = new Map<QueryId, Set<string>>();
  /** queryId → (table → the base ColIds it contributes for that table, in the order its wire
   *  rows are positional against — its `required_cols` for the table, §4.1). A query spans
   *  multiple base tables (root + related/EXISTS children), each projected independently; a
   *  table absent from the inner map ⇒ that query syncs it `'*'` (all columns, full width). */
  private readonly qcols = new Map<QueryId, Map<string, number[]>>();

  constructor(pkCols: PkCols, colCounts: ColCounts = {}) {
    this.pkCols = pkCols;
    this.colCounts = colCounts;
  }

  /**
   * Register a query's projection for one base table (PROJECTION-SUPPORT-DESIGN.md §4.1): the
   * base ColIds it contributes for `table`, in the order that table's wire rows are positional
   * against. Omit a table (or never call) for a `'*'` table — it then contributes every column
   * and its rows are full width. Must be called before the query's first batch.
   *
   * A `cols[i] < 0` entry is a DROP sentinel: wire column `i` is an EXPANDED server column the
   * client doesn't have (the server side of an `expand-then-contract` migration), so its cell is
   * discarded rather than scattered. The query still contributes only its real (`>= 0`) ColIds.
   */
  registerProjection(queryId: QueryId, table: string, cols: number[]): void {
    let byTable = this.qcols.get(queryId);
    if (!byTable) {
      byTable = new Map<string, number[]>();
      this.qcols.set(queryId, byTable);
    }
    byTable.set(table, cols.slice());
  }

  /**
   * Drop a query's projection for one table, reverting it to `'*'` (full presence, rows scattered
   * verbatim). The inverse of {@link registerProjection}; a no-op if none was registered. Needed
   * because `qcols` persists across re-hydrate epochs: if a live subscription's hello narrows from
   * an EXPANDED layout (a `-1`-bearing map) back to an exact full-width one, the stale map must be
   * cleared or it would mis-scatter the now-exact rows. A no-op for an unprojected (`'*'`) table.
   */
  unregisterProjection(queryId: QueryId, table: string): void {
    this.qcols.get(queryId)?.delete(table);
  }

  /**
   * Register a table's primary-key columns after construction — for a **synthetic aggregate
   * table** (`AGGREGATE-SYNC-DESIGN.md` §3.3): a relationship `count` is synced as a
   * server-authoritative `__agg_*` base table that is not in the client's typed schema, so
   * the backend registers it here (and on the local engine) as queries that use it arrive.
   * Idempotent — re-registering the same table (a second query over the same aggregate)
   * is a no-op. Once registered, its rows refcount/GC exactly like any base table.
   */
  registerTable(table: string, primaryKey: number[]): void {
    this.pkCols[table] = primaryKey;
  }

  /**
   * The inverse of {@link registerTable} for a synthetic aggregate table whose last
   * referencing query is gone (`AGGREGATE-SYNC-DESIGN.md` §4): drop its primary-key
   * registration so the table is unknown again. A balanced stream has already GC'd its rows
   * at the `1→0` transition (via {@link dropQuery}); defensively this also sweeps any residual
   * base rows + per-query footprint entries for the table, so a later re-registration of the
   * same name starts clean. A no-op for an unregistered table.
   */
  unregisterTable(table: string): void {
    delete this.pkCols[table];
    // A balanced stream GC's the rows at 1→0 (dropQuery); defensively sweep any residual base
    // rows + footprint references for this table (matched on the entry's stored table —
    // separator-agnostic) so a re-register of the same name starts clean.
    const stale = new Set<string>();
    for (const [k, e] of this.base) if (e.table === table) stale.add(k);
    for (const k of stale) this.base.delete(k);
    if (stale.size) for (const foot of this.qfoot.values()) for (const k of stale) foot.delete(k);
  }

  /**
   * Apply one query's normalized batch (its hydrate snapshot or one transaction's ops) and
   * return the NET base-table mutations to commit to the wasm `Db` in a single transaction.
   * Cross-query refcount + per-query dedup + column union (§4, §5):
   * - `add`: counted into this query's footprint once; on the base `0→1` transition the row
   *   enters at this query's projection; on a `1→N` transition the query's columns are merged
   *   into the shared union (widen) and an `edit` is forwarded if the union changed.
   * - `remove`: on the base `N→0` transition the row leaves; on `N→M>0` the union is recomputed
   *   from the remaining queries and an `edit` narrows the shared row.
   * - `edit`: column-merge this query's cells into the shared union; forward once (idempotent
   *   across queries — same source row, same values).
   */
  applyBatch(queryId: QueryId, ops: NormalizedOp[]): Mutation[] {
    const muts: Mutation[] = [];
    const foot = this.footOf(queryId);
    for (const op of ops) {
      if (op.op === "add")
        this.add(queryId, foot, op.table, this.scatter(queryId, op.table, op.row), muts);
      else if (op.op === "remove")
        this.remove(queryId, foot, op.table, this.scatter(queryId, op.table, op.row), muts);
      else this.edit(op.table, this.scatter(queryId, op.table, op.new), muts);
    }
    return muts;
  }

  /**
   * Re-hydrate one query under a new epoch (§5.3): the server re-sent `queryId`'s whole
   * footprint as seq-0 `add`s. Diff the new footprint against the query's current one —
   * rows only in the old set leave (refcount out, GC/narrow), rows only in the new set enter
   * (refcount in / widen), and an intersecting row whose value changed during the gap is an
   * edit. Only `queryId`'s references move; other queries' counts are untouched. Returns the
   * net mutations to commit.
   */
  rehydrate(queryId: QueryId, snapshot: NormalizedOp[]): Mutation[] {
    const muts: Mutation[] = [];
    const foot = this.footOf(queryId);
    // The new footprint, keyed; a re-hydrate snapshot is all `add`s.
    const next = new Map<string, { table: string; row: UnionCell[] }>();
    for (const op of snapshot) {
      if (op.op !== "add") continue;
      const row = this.scatter(queryId, op.table, op.row);
      next.set(this.key(op.table, row), { table: op.table, row });
    }
    // Rows that left: in the old footprint but not the new one.
    for (const key of [...foot]) {
      if (!next.has(key)) this.removeKey(queryId, foot, key, muts);
    }
    // Rows that entered or changed.
    for (const [key, { table, row }] of next) {
      if (foot.has(key)) {
        // Still referenced — only forward an edit if the union changed during the gap.
        const e = this.base.get(key);
        if (e) this.mergeInto(e, row, table, muts);
      } else {
        this.add(queryId, foot, table, row, muts);
      }
    }
    return muts;
  }

  /**
   * Drop a query (§5.1): decrement its footprint's refcounts, GC each row at the last
   * reference (or narrow the union if others remain), and forget the query. `O(footprint)`,
   * no per-row scan. Returns the net mutations. (The caller also tells the server to
   * deregister the stream.)
   */
  dropQuery(queryId: QueryId): Mutation[] {
    const muts: Mutation[] = [];
    const foot = this.qfoot.get(queryId);
    if (!foot) return muts;
    for (const key of [...foot]) this.removeKey(queryId, foot, key, muts);
    this.qfoot.delete(queryId);
    this.qcols.delete(queryId);
    return muts;
  }

  // --- introspection (for tests / the Store) ---------------------------------

  /** The number of distinct base rows currently synced into the local store. */
  baseSize(): number {
    return this.base.size;
  }

  /** How many queries currently reference `(table, row)` (0 if absent). The lookup keys by PK,
   *  so a projected `row` need only carry its PK columns at the base positions. */
  refCount(table: string, row: UnionCell[]): number {
    return this.base.get(this.key(table, row))?.queries.size ?? 0;
  }

  /**
   * The synced (server-authoritative) row currently held for `(table, pkCells)`, or
   * `undefined` if no query references it. `pkCells` are the primary-key cells in
   * `primaryKey` order (NOT a full row) — the same key the cross-query refcount uses.
   *
   * The optimistic aggregate overlay (`AGGREGATE-SYNC-DESIGN.md` §4) reads the server's
   * `__agg` count cell through this so it can compute `displayed = server_base ⊕ delta`
   * from the authoritative base rather than re-deriving it from the local engine's head
   * (which already carries the optimistic layer — a torn read).
   */
  baseRow(table: string, pkCells: WireValue[]): (WireValue | undefined)[] | undefined {
    const cols = this.pkCols[table];
    if (!cols) throw new Error(`NormalizedSync: no primary key registered for table ${table}`);
    // Place the pk cells at their schema positions so `key()` reduces this probe to the
    // exact same string it builds for any full row with these key cells.
    const probe: WireValue[] = [];
    cols.forEach((c, j) => (probe[c] = pkCells[j]));
    return this.base.get(this.key(table, probe))?.row;
  }

  // --- internals -------------------------------------------------------------

  private footOf(queryId: QueryId): Set<string> {
    let foot = this.qfoot.get(queryId);
    if (!foot) {
      foot = new Set<string>();
      this.qfoot.set(queryId, foot);
    }
    return foot;
  }

  /** Scatter a query's (possibly narrower) wire row for `table` into the shared full-width
   *  positional layout. No registered projection for `(queryId, table)` ⇒ a `'*'` table whose
   *  row is already full width (returned as-is). Otherwise allocate a full-width row of
   *  `ABSENT` and place `row[i]` at `cols[i]`. */
  private scatter(queryId: QueryId, table: string, row: WireValue[]): UnionCell[] {
    const cols = this.qcols.get(queryId)?.get(table);
    if (!cols) return row; // '*' — already full width, present everywhere
    const width = this.colCounts[table];
    if (width === undefined) {
      throw new Error(`NormalizedSync: no column count registered for projected table ${table}`);
    }
    const out: UnionCell[] = new Array<UnionCell>(width).fill(ABSENT);
    // `cols[i] < 0` ⇒ wire column `i` is an expanded server column the client lacks: drop its
    // cell, leaving the client's narrower row intact. Known columns map by name to their base
    // position, so a server wider than (or reordered vs.) the client still scatters correctly.
    for (let i = 0; i < cols.length; i++) if (cols[i] >= 0) out[cols[i]] = row[i];
    return out;
  }

  /** Column-merge `incoming`'s present cells over `e.row` (a widen / pure-value change) and
   *  forward a single `edit` if the union changed. Both rows are full width. */
  private mergeInto(e: BaseEntry, incoming: UnionCell[], table: string, muts: Mutation[]): void {
    const merged = e.row.map((cur, i) => (incoming[i] !== ABSENT ? incoming[i] : cur));
    if (!rowEq(merged, e.row)) {
      muts.push(mut("edit", table, e.row, merged));
      e.row = merged;
    }
  }

  private add(queryId: QueryId, foot: Set<string>, table: string, row: UnionCell[], muts: Mutation[]): void {
    const key = this.key(table, row);
    if (foot.has(key)) return; // this query already references it
    foot.add(key);
    const e = this.base.get(key);
    if (e) {
      e.queries.add(queryId); // another query already synced the row → no Db add (would dup-key)
      // …but this query may carry NEW columns (widen) or a NEWER value for a shared column
      // (the gainer half of a server commit that edits the same row on a holder query's
      // stream, CRIT#3). Column-merge and forward an edit at the new union if it changed.
      this.mergeInto(e, row, table, muts);
    } else {
      this.base.set(key, { table, row, queries: new Set([queryId]) });
      muts.push(mut("add", table, undefined, row)); // 0→1
    }
  }

  private remove(
    queryId: QueryId,
    foot: Set<string>,
    table: string,
    row: UnionCell[],
    muts: Mutation[],
  ): void {
    this.removeKey(queryId, foot, this.key(table, row), muts);
  }

  private removeKey(queryId: QueryId, foot: Set<string>, key: string, muts: Mutation[]): void {
    if (!foot.has(key)) return; // not referenced by this query
    foot.delete(key);
    const e = this.base.get(key);
    if (!e) return; // defensive: inconsistent stream
    e.queries.delete(queryId);
    if (e.queries.size === 0) {
      this.base.delete(key);
      muts.push(mut("remove", e.table, undefined, e.row)); // N→0
    } else {
      // Others still reference it → recompute the union from the remaining queries and narrow
      // the shared row (reset any column no longer contributed to ABSENT). §4.2.
      const present = this.presentCols(e.queries, e.table);
      if (present === null) return; // a '*' query still holds it → full presence, no narrowing
      const narrowed = e.row.map((cell, i) => (present.has(i) ? cell : ABSENT));
      if (!rowEq(narrowed, e.row)) {
        muts.push(mut("edit", e.table, e.row, narrowed));
        e.row = narrowed;
      }
    }
  }

  private edit(table: string, next: UnionCell[], muts: Mutation[]): void {
    // PK is stable across an edit, so `next` keys the existing entry.
    const key = this.key(table, next);
    const e = this.base.get(key);
    if (!e) return; // defensive: edit of an untracked row
    this.mergeInto(e, next, table, muts); // column-merge; dedups (no-op) if already applied
  }

  /** The columns present on a `table` row footprinted by `queries`: the union of their
   *  projections for that table. `null` ⇒ all columns present (some referencing query syncs
   *  the table `'*'`), so the row is full. */
  private presentCols(queries: Set<QueryId>, table: string): Set<number> | null {
    const out = new Set<number>();
    for (const q of queries) {
      const cols = this.qcols.get(q)?.get(table);
      if (!cols) return null; // a '*' query contributes every column for this table
      for (const c of cols) if (c >= 0) out.add(c); // skip DROP sentinels (expanded server cols)
    }
    return out;
  }

  private key(table: string, row: UnionCell[]): string {
    const cols = this.pkCols[table];
    if (!cols) throw new Error(`NormalizedSync: no primary key registered for table ${table}`);
    return `${table} ${JSON.stringify(cols.map((i) => row[i]))}`;
  }
}

/** Build a `Mutation`. A union row may carry `ABSENT` (`undefined`) cells — the wasm `mutate`
 *  ABI accepts them as the Absent token (§5.3) — so the row crosses as `WireValue[]` with that
 *  understood sentinel. */
function mut(
  op: "add" | "remove" | "edit",
  table: string,
  old: UnionCell[] | undefined,
  row: UnionCell[],
): Mutation {
  if (op === "edit") return { op, table, old: (old as WireValue[]) ?? [], new: row as WireValue[] };
  return { op, table, row: row as WireValue[] } as Mutation;
}
