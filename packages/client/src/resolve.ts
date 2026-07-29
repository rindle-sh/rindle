// Positional → named change resolution: the inverse of the wire encoder.
//
// A backend ships per-query `FlatChange`s (types.ts): `{ path: PathSeg[], op: add|remove|edit }`,
// where `path` indexes into the view's relationship tree and rows are POSITIONAL cells. The view
// (view.ts) folds those into the materialized tree positionally; this module instead LIFTS one
// change out of positional/indexed wire form into NAMED rows, resolving it against the query's OWN
// `WireSchema` (the per-subscription view schema the engine ships once on its `hello` frame): per
// level the column NAMES in wire order, and the relationships in SLOT order (a gating exists/notExists
// slot is `child: null`; a `countAs` slot carries a `project` annotation). So `path[i].rel` indexes
// `schema.relationships[i]` directly, and a positional `row` names against `schema.columns` — both
// authoritative, straight from the engine.
//
// Resolving against the hello schema (rather than re-deriving positions from the query `Ast`) means
// we assume NOTHING about slot ordering and NOTHING about column order: it stays correct under
// `.select()` projections and any future slot layout. The result — named rows + an aggregate's exact
// new value — is what a higher layer (e.g. @rindle/narrator) renders into prose, but it is broadly
// useful to anything consuming a `FlatChange` (logging, devtools, change-driven overlays).
//
// NOTE on aggregates (`countAs`): the slot's `project.col` names the EXACT count cell in the child
// row, so a count change reports the exact new value (and previous, on an edit) — not a best-effort
// guess. A remove of the aggregate row means the count fell to its identity (`project.identity`).

import type { FlatChange, PathSeg, WireNode, WireRel, WireSchema, WireValue } from "./types.ts";

/** A row named against its level's wire columns. */
export type NamedRow = Record<string, WireValue>;

/** One resolved change: a `FlatChange` lifted out of positional/indexed wire form into names,
 *  using the query's `WireSchema` (from `hello`) as the sole position→name source. */
export interface ResolvedChange {
  /** Relationship-alias chain from the query root to the changed level (`[]` ⇒ the root rows). */
  aliasChain: string[];
  /** The alias of the changed level (`""` ⇒ root), i.e. the last of `aliasChain`. */
  alias: string;
  op: "add" | "remove" | "edit";
  /** The affected row, named. For `edit` this is the NEW row; see `old` for the prior one. */
  row: NamedRow;
  /** The prior row, named (present only for `edit`). */
  old?: NamedRow;
  /** The PARENT row (named), for a nested/aggregate change — e.g. the `ticket_type` whose `sold`
   *  count moved. Taken from the path's last `parentRow`; absent for a root-level change. */
  parent?: NamedRow;
  /** Set when the changed level is a `countAs`/aggregate slot. The value is EXACT — read from the
   *  slot's projected count column (`WireRel.project.col`). */
  aggregate?: { alias: string; value: WireValue; previous?: WireValue };
  /** The raw node whose children a consumer can dig a named sub-row out of (via {@link subRow}). On
   *  an `add` the engine always ships it; on a `remove` it is present only when the consumer opted
   *  into the removed subtree (see the `op` mapping below). */
  node?: WireNode;
  /** The changed level's `WireSchema` — used by {@link subRow} to resolve a named sub of `node`. */
  levelSchema: WireSchema;
}

/** Name positional `cells` against a level's wire `columns` (insertion = wire order). */
function nameRow(cells: WireValue[] | undefined, cols: string[]): NamedRow {
  const out: NamedRow = {};
  if (!cells) return out;
  for (let i = 0; i < cols.length; i++) out[cols[i]] = cells[i] ?? null;
  return out;
}

/** Walk a `path` from the root `WireSchema` down the relationship tree. Returns the reached level,
 *  the alias chain, and the LAST relationship traversed (its `project` marks an aggregate slot).
 *  `null` if a hop addresses an unknown or gating (`child: null`) slot. */
function descend(
  root: WireSchema,
  path: PathSeg[],
): { level: WireSchema; lastRel: WireRel | null; aliasChain: string[] } | null {
  let level = root;
  let lastRel: WireRel | null = null;
  const aliasChain: string[] = [];
  for (const seg of path) {
    const rel = level.relationships[seg.rel];
    if (!rel || !rel.child) return null; // unknown / gating slot — not a materialized level
    lastRel = rel;
    level = rel.child;
    aliasChain.push(rel.name);
  }
  return { level, lastRel, aliasChain };
}

/** Lift one `FlatChange` into a {@link ResolvedChange} against the query's `WireSchema`, or `null`
 *  if its path doesn't resolve to a materialized level. */
export function resolveChange(schema: WireSchema, change: FlatChange): ResolvedChange | null {
  const here = descend(schema, change.path);
  if (!here) return null;
  const { level, lastRel, aliasChain } = here;
  const cols = level.columns;
  const alias = aliasChain.length ? aliasChain[aliasChain.length - 1] : "";
  const base: Omit<ResolvedChange, "op" | "row"> = { aliasChain, alias, levelSchema: level };
  // The parent row (for a nested/aggregate change) sits in the last path seg, named against the
  // level one hop up.
  if (change.path.length) {
    const up = descend(schema, change.path.slice(0, -1));
    const parentCells = change.path[change.path.length - 1].parentRow;
    if (up) base.parent = nameRow(parentCells, up.level.columns);
  }

  // A `countAs` slot carries a scalar `project` annotation on the relationship we descended through:
  // `project.col` is the exact count cell, `project.identity` the empty value (0 for count).
  const proj = lastRel?.project ?? null;
  const agg = (cells: WireValue[] | undefined): WireValue => (proj && cells ? (cells[proj.col] ?? null) : null);

  const op = change.op;
  if (op.tag === "add") {
    const r: ResolvedChange = { ...base, op: "add", row: nameRow(op.node.row, cols), node: op.node };
    if (proj) r.aggregate = { alias, value: agg(op.node.row) };
    return r;
  }
  if (op.tag === "remove") {
    const r: ResolvedChange = { ...base, op: "remove", row: nameRow(op.row, cols) };
    // The removed subtree rides along only when the consumer opted into it (the ArrayView attaches
    // it client-side — `Store.subscribeChanges(_, { removedSubtree: true })`). When present, `subRow`
    // resolves a removed row's nested subs exactly as on an `add`; absent, a remove is row-only.
    if (op.node) r.node = op.node;
    if (proj) r.aggregate = { alias, value: proj.identity };
    return r;
  }
  // edit
  const r: ResolvedChange = { ...base, op: "edit", row: nameRow(op.new, cols), old: nameRow(op.old, cols) };
  if (proj) r.aggregate = { alias, value: agg(op.new), previous: agg(op.old) };
  return r;
}

/** Read a named sub-row off a change's `node` by relationship alias (e.g. the `guest` under an
 *  `rsvp`) — the `add` node, or a `remove`'s subtree when the consumer opted into it. The alias →
 *  wire slot mapping comes from the changed level's `WireSchema`. `null` when no node rode along. */
export function subRow(rc: ResolvedChange, alias: string): NamedRow | null {
  if (!rc.node) return null;
  const rel = rc.levelSchema.relationships.find((r) => r.name === alias);
  if (!rel || !rel.child) return null;
  const slot = rc.node.rels.find((s) => s.rel === rel.slot);
  const child = slot?.children[0];
  if (!child) return null;
  return nameRow(child.row, rel.child.columns);
}
