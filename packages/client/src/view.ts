// The JS ArrayView — the materialized, typed result tree a backend's ChangeEvent stream
// folds into (WASM-CLIENT-DESIGN.md §7, §8). A faithful port of the View's `apply_change`
// (src/view.rs / src/flat_receiver.rs), the §6 receiver contract of FLAT-CHANGES-DESIGN.md:
// descend the path with the in-view gate, then Add / Remove / Edit (rc + the edit-move
// ghost/adjusted-position/merge protocol) located by value-driven binary search.
//
// Reads are cheap and reference-stable: each node memoizes its projected output (`out`),
// invalidated only along the root→mutation spine, so `.data` re-projects only what changed
// and untouched subtrees keep their object identity (React/Solid memoize on it).

import { compareRows } from "./compare.ts";
import type { ColType, FlatChange, FlatOp, QueryId, ResultType, WireNode, WireSchema, WireValue } from "./types.ts";

/** Per-level column types (parallel to the WireSchema), used to JSON.parse json columns on
 *  projection. Built by the Store from the typed schema; absent ⇒ no parsing (bare values). */
export interface ViewTypes {
  columnTypes: ColType[];
  rels: Record<number, ViewTypes>;
}

/** The phase of an {@link ArrayView.onChanges} delivery: the initial hydrate `snapshot` vs a later
 *  incremental `batch` — the same distinction a narrator draws (`ChangeEvent` `snapshot`/`batch`). */
export type ChangePhase = "snapshot" | "batch";

/** A per-view change listener ({@link ArrayView.onChanges}): the net `FlatChange[]` this view folded,
 *  the {@link ChangePhase} it arrived on, and the view's `WireSchema` (the position→name source for
 *  `resolveChange`). This is the DIFF the data channel ({@link ArrayView.subscribe}) discards — the
 *  seam a narrator drives off. The `schema` is passed (not closed over) because the first `snapshot`
 *  fires synchronously inside `materialize`, before the caller holds the view handle. */
export type ViewChangeListener = (changes: FlatChange[], phase: ChangePhase, schema: WireSchema) => void;

/** The public ArrayView contract `materialize()` returns. */
export interface ArrayView<R> {
  /** The current materialized result (reference-stable where data is unchanged). */
  readonly data: readonly R[];
  /** The engine query id the Store assigned this view (1:1 with the view). The same `qid` the raw
   *  change stream ({@link Store.subscribeChanges}) tags each frame with, so a consumer can
   *  correlate this query with its `ChangeEvent`s (e.g. to bind a narrator) straight off
   *  `materialize(query).qid` — no separate handle needed. */
  readonly qid: QueryId;
  /** The query's view `WireSchema` (the position→name source for {@link resolveChange}), captured
   *  from its `hello` frame. `null` while PENDING — a remote backend's `hello` arrives async; an
   *  in-process backend (wasm/replica) populates it synchronously during `materialize`. */
  readonly schema: WireSchema | null;
  /** The query's SERVER-CHANNEL state (`unknown` while loading, `complete` once server-authoritative;
   *  the `error` variant is reserved and currently unproduced). A pending optimistic mutation no
   *  longer moves this — it is a separate axis (FOLDED-MUTATIONS-DESIGN §7). `complete` for backends
   *  with no server lifecycle. Changes notify subscribers. */
  readonly resultType: ResultType;
  /** Subscribe; fires immediately with the current data, then after each applied batch (and after
   *  a {@link resultType} change — re-read `resultType` in the listener). */
  subscribe(listener: (data: readonly R[]) => void): () => void;
  /** Subscribe to this view's folded CHANGE stream — the `FlatChange[]` it applies, NET of no-op
   *  cycles (a rebase's balanced `remove`+`add` / edit round-trip cancels, so a correctly predicted
   *  optimistic write delivers nothing here). Carries the diff {@link subscribe} throws away; the
   *  per-view seam a narrator rides. Does NOT replay on subscribe — attach via
   *  `store.materialize(query, { onChanges })` to catch a synchronous backend's first snapshot.
   *  A view with any change listener also enriches its own `remove` ops with the evicted subtree
   *  (per-view, no global opt-in). Returns a detach function. */
  onChanges(listener: ViewChangeListener): () => void;
  /** Tear down + stop receiving updates. */
  destroy(): void;
}

/** What a top-level `.one()` query materializes to: the single row (or `null`), not an array.
 *  A thin adapter over a {@link FlatArrayView} — all the folding is shared; only the result
 *  boundary unwraps (`data[0] ?? null`). Reference identity of the row is preserved. */
export interface SingularArrayView<R> {
  /** The single current row, or `null` when the query matches nothing. */
  readonly data: R | null;
  /** The engine query id — see {@link ArrayView.qid}. */
  readonly qid: QueryId;
  /** The query's view `WireSchema` — see {@link ArrayView.schema}. */
  readonly schema: WireSchema | null;
  /** The query's lifecycle state — see {@link ArrayView.resultType}. */
  readonly resultType: ResultType;
  /** Subscribe; fires immediately with the current row, then after each applied batch (and after
   *  a {@link resultType} change). */
  subscribe(listener: (data: R | null) => void): () => void;
  /** Subscribe to the view's folded CHANGE stream — see {@link ArrayView.onChanges}. The changes
   *  are the same positional `FlatChange`s as the plural view (a `.one()` is just the list capped to
   *  one), so a narrator resolves them identically. */
  onChanges(listener: ViewChangeListener): () => void;
  /** Tear down + stop receiving updates. */
  destroy(): void;
}

/** Wrap a plural {@link FlatArrayView} as a {@link SingularArrayView} for a `.one()` query
 *  (the engine caps it to `limit = 1`, so the top list holds at most one node). */
export class SingularView<R> implements SingularArrayView<R> {
  private readonly inner: ArrayView<R>;

  constructor(inner: ArrayView<R>) {
    this.inner = inner;
  }

  get data(): R | null {
    return this.inner.data[0] ?? null;
  }

  get qid(): QueryId {
    return this.inner.qid;
  }

  get schema(): WireSchema | null {
    return this.inner.schema;
  }

  get resultType(): ResultType {
    return this.inner.resultType;
  }

  subscribe(listener: (data: R | null) => void): () => void {
    return this.inner.subscribe((d) => listener(d[0] ?? null));
  }

  onChanges(listener: ViewChangeListener): () => void {
    return this.inner.onChanges(listener);
  }

  destroy(): void {
    this.inner.destroy();
  }
}

/** An internal reconstruction node: the row, multi-path refcount, per-slot child lists,
 *  and a memoized projected output (`null` ⇒ stale, rebuilt on next read). */
interface Node {
  row: WireValue[];
  rc: number;
  rels: Node[][];
  out: unknown;
}

function binarySearch(
  list: Node[],
  row: WireValue[],
  sort: [number, boolean][],
): { found: boolean; index: number } {
  let lo = 0;
  let hi = list.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    const c = compareRows(list[mid].row, row, sort);
    if (c < 0) lo = mid + 1;
    else if (c > 0) hi = mid;
    else return { found: true, index: mid };
  }
  return { found: false, index: lo };
}

/** Build a fresh node (rc = 1) with its inline subtree, mirroring `init_rels_for_new_entry`:
 *  one list per declared slot; fill in-view slots from the payload (children inserted in the
 *  order shipped — NEVER re-sorted — folding same-sort-key dups to rc += 1). */
function buildNode(wnode: WireNode, schema: WireSchema): Node {
  const rels: Node[][] = schema.relationships.map(() => []);
  for (const { rel, children } of wnode.rels) {
    const child = schema.relationships[rel]?.child;
    if (!child) continue; // join-only / gating slot — not materialized
    const built: Node[] = [];
    for (const c of children) {
      const at = binarySearch(built, c.row, child.sort);
      if (at.found) built[at.index].rc += 1;
      else built.splice(at.index, 0, buildNode(c, child));
    }
    rels[rel] = built;
  }
  return { row: wnode.row, rc: 1, rels, out: null };
}

function cloneNode(n: Node): Node {
  return { row: n.row.slice(), rc: n.rc, rels: n.rels.map((r) => r.map(cloneNode)), out: null };
}

/** Reconstruct a positional {@link WireNode} (row + its populated child slots) from a maintained
 *  {@link Node} — the inverse of {@link buildNode}. Used to attach a removed subtree to a `remove`
 *  op so a change consumer (a narrator) can resolve its nested subs, which a bare positional remove
 *  (just the leaving row) cannot carry. Only in-view (`child !== null`), non-empty slots are emitted,
 *  matching how an `add` node ships only populated relationships. */
function toWireNode(node: Node, schema: WireSchema): WireNode {
  const rels: { rel: number; children: WireNode[] }[] = [];
  for (const rel of schema.relationships) {
    if (rel.child === null) continue;
    const childList = node.rels[rel.slot] ?? [];
    if (childList.length === 0) continue;
    rels.push({ rel: rel.slot, children: childList.map((c) => toWireNode(c, rel.child as WireSchema)) });
  }
  return { row: node.row, rels };
}

/** Elementwise equality over positional bare-cell rows — THE row comparator for every JS-side
 *  diff (views, aggregate heads, the persistence mirror), exported so no caller grows a drifted
 *  private copy. Per cell: `===` keeps `-0 === 0` (the engine's key semantics treat them equal);
 *  the `Object.is` arm makes a NaN cell equal itself (under `!==` alone, a NaN-bearing row never
 *  matches any copy of itself, so every diff re-emits it as a spurious edit forever). */
export function rowsEqual(a: WireValue[], b: WireValue[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i] && !Object.is(a[i], b[i])) return false;
  }
  return true;
}

/** JSON.stringify for keying a row, except the numbers JSON folds together — NaN/±Infinity (→ null)
 *  and -0 (→ 0) — get a sentinel encoding, so two rows that `rowsEqual` (Object.is) distinguishes
 *  never share a net key (a `remove [1, Infinity]` must not cancel an `add [1, null]`). */
function keyRow(row: WireValue[]): string {
  return JSON.stringify(row, (_k, v: unknown) =>
    typeof v === "number" && (!Number.isFinite(v) || Object.is(v, -0)) ? [" num", Object.is(v, -0) ? "-0" : String(v)] : v,
  );
}

/** The key a subtractive net matches an op on: the path (parent locators, one hop per level) plus the
 *  row bytes. Two ops with the same key touch the same row at the same tree position. */
function opRowKey(path: FlatChange["path"], row: WireValue[]): string {
  let k = "";
  for (const seg of path) k += seg.rel + ":" + keyRow(seg.parentRow) + "/";
  return k + "|" + keyRow(row);
}

/** Walk `path` down `schema` to the changed level's `WireSchema` — `null` when a hop crosses a
 *  gating (join-only) slot; such ops never fold, so they never reach the net. */
function levelSchemaAt(schema: WireSchema, path: FlatChange["path"]): WireSchema | null {
  let level: WireSchema = schema;
  for (const seg of path) {
    const child = level.relationships[seg.rel]?.child;
    if (!child) return null;
    level = child;
  }
  return level;
}

/** Deep structural equality of two canonical (maintained-order) subtrees: rows via Object.is,
 *  per-slot children pairwise, rc included — a dup-path child only equals an equally-duplicated one. */
function nodesEqual(a: Node, b: Node): boolean {
  if (a.rc !== b.rc || !rowsEqual(a.row, b.row) || a.rels.length !== b.rels.length) return false;
  for (let s = 0; s < a.rels.length; s++) {
    const ra = a.rels[s];
    const rb = b.rels[s];
    if (ra.length !== rb.length) return false;
    for (let i = 0; i < ra.length; i++) if (!nodesEqual(ra[i], rb[i])) return false;
  }
  return true;
}

/** Cancel no-op cycles in a folded batch SUBTRACTIVELY — the safe half of the removed engine
 *  coalescer (OPTIMISTIC-WRITES-DESIGN §3, and `wasm/db.rs` "Step 2 of the removal"). A rebase
 *  re-invokes a still-pending prediction, emitting a balanced `remove R`+`add R` (or `edit b→a`+
 *  `edit a→b`) that nets to nothing; so a correctly predicted optimistic write delivers ZERO changes
 *  to a narrator, and only genuine (mispredicted / other-client) changes survive. We ONLY drop exact
 *  inverse pairs — byte-identical row at the same path AND a structurally identical subtree: a row
 *  that leaves and re-enters within one batch carrying a CHANGED child set is NOT an inverse (the
 *  child never got its own event while the row was out — the re-add is its only carrier), so that
 *  pair survives. Survivors pass through UNMODIFIED: we never rewrite a locator, reorder, or
 *  reconstruct a gated subtree (the reconstructive netting that shipped four correctness bugs and
 *  was pulled from the engine). Because this feeds prose, not a locate-by-value receiver, even an
 *  imperfect net is at worst a spurious line, never a corrupt view — and every conservative bail
 *  here (unknown subtree, gated level) errs on the spurious-line side, never the missed-line side.
 *  Order-preserving for survivors; returns the input array unchanged when nothing cancels. */
function netChanges(changes: FlatChange[], schema: WireSchema): FlatChange[] {
  if (changes.length < 2) return changes;
  const alive = new Array<boolean>(changes.length).fill(true);
  const removes = new Map<string, number[]>(); // key → indices of not-yet-cancelled removes
  const adds = new Map<string, number[]>(); // key → indices of not-yet-cancelled adds
  const edits = new Map<string, number[]>(); // pathKey+old+new → indices of not-yet-cancelled edits
  // The subtree an add/remove carries, canonicalized to maintained (sorted, dup-folded) form so a
  // shipped-order `add` node and a maintained-order enriched `remove` node compare structurally.
  // Built ONLY when two ops collide on the row key — the common no-collision delivery never builds
  // one — and memoized per op index. `null` ⇒ subtree unknown (a gated level, or a remove that was
  // not enriched); conservative: an unknown subtree never cancels, so we err toward a spurious line.
  const subCache = new Map<number, Node | null>();
  const subtreeOf = (i: number): Node | null => {
    let s = subCache.get(i);
    if (s !== undefined) return s;
    const { path, op } = changes[i];
    const level = levelSchemaAt(schema, path);
    // A remove reaching the net actually evicted its node (only applied ops are delivered) and so was
    // enriched with its subtree on `op.node`; an add always carries `op.node`. Either is canonicalized.
    const wnode = op.tag === "add" ? op.node : op.tag === "remove" ? op.node : undefined;
    s = level && wnode ? buildNode(wnode, level) : null;
    subCache.set(i, s);
    return s;
  };
  // Take the LATEST pending opposite at the same key whose subtree also matches (LIFO, like the
  // plain `take` below, so nested cycles unwind innermost-first).
  const takeInverse = (m: Map<string, number[]>, k: string, i: number): number | undefined => {
    const arr = m.get(k);
    if (!arr || arr.length === 0) return undefined;
    const mine = subtreeOf(i);
    if (mine === null) return undefined;
    for (let x = arr.length - 1; x >= 0; x--) {
      const theirs = subtreeOf(arr[x]);
      if (theirs !== null && nodesEqual(mine, theirs)) {
        const j = arr[x];
        arr.splice(x, 1);
        return j;
      }
    }
    return undefined;
  };
  const take = (m: Map<string, number[]>, k: string): number | undefined => {
    const arr = m.get(k);
    return arr && arr.length ? arr.pop() : undefined;
  };
  const put = (m: Map<string, number[]>, k: string, i: number): void => {
    const arr = m.get(k);
    if (arr) arr.push(i);
    else m.set(k, [i]);
  };
  let cancelled = false;
  for (let i = 0; i < changes.length; i++) {
    const { path, op } = changes[i];
    if (op.tag === "remove") {
      const k = opRowKey(path, op.row);
      const j = takeInverse(adds, k, i);
      if (j !== undefined) {
        alive[i] = alive[j] = false;
        cancelled = true;
      } else put(removes, k, i);
    } else if (op.tag === "add") {
      const k = opRowKey(path, op.node.row);
      const j = takeInverse(removes, k, i);
      if (j !== undefined) {
        alive[i] = alive[j] = false;
        cancelled = true;
      } else put(adds, k, i);
    } else {
      // edit: cancel against a prior INVERSE edit (a→b then b→a) at the same path. The old/new
      // bytes are part of the map key, so the inverse lookup is O(1) instead of a rescan.
      const pk = opRowKey(path, []);
      const oldS = keyRow(op.old);
      const newS = keyRow(op.new);
      const j = take(edits, pk + " " + newS + " " + oldS);
      if (j !== undefined) {
        alive[i] = alive[j] = false;
        cancelled = true;
      } else put(edits, pk + " " + oldS + " " + newS, i);
    }
  }
  if (!cancelled) return changes;
  const out: FlatChange[] = [];
  for (let i = 0; i < changes.length; i++) if (alive[i]) out.push(changes[i]);
  return out;
}

const EMPTY: readonly never[] = Object.freeze([]);

export class FlatArrayView<R = unknown> implements ArrayView<R> {
  // The engine query id (Store-assigned, 1:1 with this view), exposed read-only via {@link qid}.
  // `0` ⇒ unbound (a bare view never registered with a Store); the Store passes it at construction.
  private readonly _qid: QueryId;
  // `null` ⇒ PENDING (no schema yet): a remote backend's `hello` arrives async, so the view
  // exists (and reads as `[]`) before its schema lands. Set by `reset` (first hello / re-hydrate).
  // Exposed read-only via the {@link schema} getter (the position→name source for `resolveChange`).
  private _schema: WireSchema | null;
  private types?: ViewTypes;
  // SSR first-paint seed (SSR-DESIGN.md §6): pre-projected rows installed by `seed`, returned by
  // `data` until the maintained tree's first live SNAPSHOT lands (the Store calls `retireSeed` then).
  // It deliberately SURVIVES `reset` (the `hello`), so a seeded view bridges the `hello`→snapshot gap
  // instead of flashing empty in between (a `hello` sets the schema but its data arrives one round-trip
  // later, on the snapshot). `null` ⇒ no seed.
  private seeded: readonly R[] | null = null;
  private top: Node[] = [];
  private dirty = true;
  private cached: readonly R[] | null = null;
  // `complete` by default: a backend with no server lifecycle (the in-process engine) never pushes
  // a resultType, so its views read as authoritative. The Store overrides this for backends that do
  // (the optimistic backend: `unknown` until hydrated, etc.).
  private rt: ResultType = "complete";
  private readonly listeners = new Set<(data: readonly R[]) => void>();
  // The per-view CHANGE stream ({@link onChanges}) — the diff a narrator rides, distinct from the
  // data channel above. Empty until a narrator attaches: an idle view pays one `.size` check per fold.
  private readonly changeListeners = new Set<ViewChangeListener>();
  // Changes folded but not yet delivered to `changeListeners` — buffered while the Store defers a
  // commit (mirrors the deferred `notify`), then netted + flushed at the commit boundary. Segmented
  // BY PHASE (not one last-writer-wins phase scalar): a bracket that folds both a `snapshot` and a
  // `batch` into this view keeps them apart, each netted and delivered under its OWN phase, so a
  // snapshot is never mislabeled `batch` (nor a batch dropped as `snapshot`). Consecutive same-phase
  // folds coalesce into one segment so a whole commit's batch nets together.
  private pendingSegments: { phase: ChangePhase; changes: FlatChange[] }[] = [];

  constructor(schema?: WireSchema, types?: ViewTypes, qid: QueryId = 0) {
    this._schema = schema ?? null;
    this.types = types;
    this._qid = qid;
  }

  get qid(): QueryId {
    return this._qid;
  }

  get schema(): WireSchema | null {
    return this._schema;
  }

  get resultType(): ResultType {
    return this.rt;
  }

  /** Set the query's lifecycle state (the Store routes the backend's per-query signal here).
   *  Notifies subscribers on a change so a status-bound listener (React `useQueryStatus`) re-reads,
   *  WITHOUT re-projecting data (it is unchanged). */
  setResultType(rt: ResultType): void {
    if (this.rt === rt) return;
    this.rt = rt;
    for (const l of this.listeners) l(this.data);
  }

  /** (Re)bind to a schema and clear the tree IN PLACE. The first `hello` (pending → ready)
   *  and a re-hydrate (gap → new epoch — FLAT-CHANGES-DESIGN.md §2.3) both go through here, so
   *  the caller's view reference and its subscribers survive a re-subscribe. Does NOT notify —
   *  the snapshot that follows (`applyChanges`) does, avoiding an empty-then-filled flicker.
   *  KEEPS any SSR `seeded` rows: they are retired only when the first live snapshot lands
   *  ({@link retireSeed}, driven by the Store), so `data` shows the seed — not an empty tree —
   *  across the whole `hello`→first-`snapshot` gap. */
  reset(schema: WireSchema, types?: ViewTypes): void {
    this._schema = schema;
    this.types = types;
    this.top = [];
    this.cached = null;
    this.dirty = true;
    // Drop any changes buffered under the OLD epoch: a re-hydrate cuts a new schema, and
    // `deliverChanges` resolves against the current `_schema`, so pre-epoch changes must never
    // survive to be delivered against the new one (mirrors `destroy`). The snapshot that follows
    // this reset re-establishes the buffer for the new epoch.
    this.pendingSegments = [];
  }

  /** Install a pre-projected SSR first-paint snapshot (SSR-DESIGN.md §6). The rows are already
   *  in result shape (json columns parsed, relationships nested), so a view with no live backend
   *  (the server one-shot Store) reads them directly, and a browser view shows them until its
   *  first live snapshot lands ({@link retireSeed}). Does NOT notify — it is set at materialize
   *  time, before any subscriber, and the live snapshot that follows notifies. */
  seed(rows: readonly R[]): void {
    this.seeded = rows;
  }

  /** Retire the SSR first-paint seed — the Store calls this as it folds the maintained tree's first
   *  live snapshot, so `data` switches from the seed to the live tree with no empty gap between them
   *  (the seed deliberately survived the earlier `reset`/`hello`). Idempotent. Does NOT notify — the
   *  snapshot fold it accompanies does; BUT when that fold is empty (a 0-row result, or rows already
   *  in `top`) it notifies nothing, so the Store forces a {@link notify} on the strength of the `true`
   *  return here — else the view reads the live tree yet never re-renders (a frozen seed). Returns
   *  whether a live seed was actually cleared (so the Store knows a forced notify is owed). */
  retireSeed(): boolean {
    if (this.seeded === null) return false;
    this.seeded = null;
    return true;
  }

  /** Apply a batch (the hydrate snapshot or one transaction's events) in order, then
   *  notify subscribers once. Order is significant (FLAT-CHANGES-DESIGN.md §5.4). A no-op
   *  while pending (changes never precede the `hello` that resets the schema).
   *
   *  `enrichRemoves` ⇒ before a removed node is dropped, reconstruct its full subtree and attach it
   *  to the `remove` op's `node` (in place, so the same event object the Store fans out to its
   *  change subscribers carries it). Off by default — paid only when a consumer asked for it, and
   *  only on a real eviction (an rc-decrement that keeps the row leaves `node` absent). A view with
   *  an attached {@link onChanges} listener ALSO enriches (per-view, no global opt-in), so a
   *  narrator can resolve a removed row's subs whether or not the store-global counter is set.
   *
   *  `deferNotify` ⇒ fold but do NOT notify; the caller is responsible for calling {@link flush}
   *  later. The Store uses this to fold every view in one commit before notifying any subscriber
   *  (cross-view-atomic notification — `Store.onCommitBoundary`); standalone use leaves it off, so
   *  a bare view still fires its subscribers after each applied batch.
   *
   *  `phase` tags the {@link onChanges} delivery (`snapshot` for the hydrate, `batch` otherwise); it
   *  does not affect the fold. Returns whether the batch changed the view (so a deferring caller
   *  knows it must be flushed). */
  applyChanges(events: FlatChange[], enrichRemoves = false, deferNotify = false, phase: ChangePhase = "batch"): boolean {
    if (this._schema === null) return false;
    // Enrich this view's removes when a narrator is attached (see the `enrichRemoves` doc above) —
    // the per-view twin of the store's global `removedSubtree` counter.
    const enrich = enrichRemoves || this.changeListeners.size > 0;
    // Deliver ONLY the ops that actually changed the tree: a duplicate-path add (rc bump), an
    // rc-decrement remove that left the row, a no-op edit, and a gated op all return false — none is
    // a real change, so none should reach a narrator. (Enrichment already attached the subtree to a
    // real remove during the fold, so a surviving remove carries its `node`.)
    const applied: FlatChange[] = [];
    for (const e of events) {
      if (this.applyAt(this.top, this._schema, e.path, e.op, 0, enrich)) applied.push(e);
    }
    if (applied.length === 0) return false;
    this.dirty = true;
    if (deferNotify) {
      if (this.changeListeners.size > 0) {
        const segs = this.pendingSegments;
        const last = segs.length > 0 ? segs[segs.length - 1] : undefined;
        if (last && last.phase === phase) for (const e of applied) last.changes.push(e);
        else segs.push({ phase, changes: applied.slice() });
      }
      return true;
    }
    // Inline (standalone / no commit bracket): notify data subscribers, then deliver the change
    // stream — each isolated, first error re-raised once both have run (mirroring the Store's
    // per-view flush isolation) so a throwing narration listener never starves the data channel,
    // aborts the fold's return, or propagates raw into the engine's push path.
    let firstError: unknown;
    let hasError = false;
    const note = (e: unknown): void => {
      if (!hasError) {
        hasError = true;
        firstError = e;
      }
    };
    try {
      this.notify();
    } catch (e) {
      note(e);
    }
    this.deliverChanges(applied, phase, note);
    if (hasError) throw firstError;
    return true;
  }

  /** Notify subscribers with the current data. The deferred half of {@link applyChanges} (when
   *  `deferNotify` was set): the Store calls this at the commit-notify barrier — after every view
   *  touched by the same commit has folded — so a subscriber that re-reads a sibling view inside
   *  its callback observes post-commit data, never a torn mid-commit state. */
  flush(): void {
    // Take the buffer BEFORE delivery so a throwing listener can never leave changes buffered to be
    // re-delivered — merged and cross-phase-netted — into a LATER commit's flush. Notify and every
    // segment's delivery are isolated; the first error is re-raised after all have run, so one bad
    // subscriber starves neither the data channel nor a sibling segment (matching `flushCommit`).
    const segs = this.pendingSegments;
    this.pendingSegments = [];
    let firstError: unknown;
    let hasError = false;
    const note = (e: unknown): void => {
      if (!hasError) {
        hasError = true;
        firstError = e;
      }
    };
    try {
      this.notify();
    } catch (e) {
      note(e);
    }
    for (const seg of segs) this.deliverChanges(seg.changes, seg.phase, note);
    if (hasError) throw firstError;
  }

  get data(): readonly R[] {
    // A live SSR seed owns `data` until it is retired on the first live snapshot ({@link retireSeed}) —
    // this spans pending (no schema) AND the post-`hello`, pre-snapshot window (schema set, tree still
    // empty), so a seeded query never flashes empty during the handoff. `null` seed ⇒ normal behavior.
    if (this.seeded !== null) return this.seeded;
    if (this._schema === null) return EMPTY as readonly R[];
    if (!this.dirty && this.cached !== null) return this.cached;
    this.cached = this.top.map((n) => this.project(n, this._schema as WireSchema, this.types)) as R[];
    this.dirty = false;
    return this.cached;
  }

  subscribe(listener: (data: readonly R[]) => void): () => void {
    this.listeners.add(listener);
    listener(this.data);
    return () => {
      this.listeners.delete(listener);
    };
  }

  onChanges(listener: ViewChangeListener): () => void {
    this.changeListeners.add(listener);
    return () => {
      this.changeListeners.delete(listener);
    };
  }

  /** Net the folded batch and hand the survivors to the change listeners, AFTER the data `notify`
   *  (the order `Store.subscribeChanges` consumers already observe). A batch that nets to nothing —
   *  a correctly predicted rebase — makes no call, so a narrator sees only real change. Each listener
   *  is isolated: a throwing one reports to `note` (the caller re-raises the first) and the rest still
   *  run, so one bad narration template never starves a sibling listener nor corrupts the view. */
  private deliverChanges(events: FlatChange[], phase: ChangePhase, note: (e: unknown) => void): void {
    if (this.changeListeners.size === 0) return;
    // `_schema` is non-null here: `applyChanges` early-returns while pending, and `flush` runs only
    // after a fold — so a listener always has the position→name source in hand.
    const schema = this._schema as WireSchema;
    const net = netChanges(events, schema);
    if (net.length === 0) return;
    for (const l of this.changeListeners) {
      try {
        l(net, phase, schema);
      } catch (e) {
        note(e);
      }
    }
  }

  destroy(): void {
    this.listeners.clear();
    this.changeListeners.clear();
    this.pendingSegments = [];
    this.top = [];
  }

  // --- apply -------------------------------------------------------------------

  private applyAt(list: Node[], schema: WireSchema, path: FlatChange["path"], op: FlatOp, depth: number, enrichRemoves: boolean): boolean {
    if (depth === path.length) {
      return this.applyOp(list, schema, op, enrichRemoves);
    }
    const seg = path[depth];
    const child = schema.relationships[seg.rel]?.child;
    if (!child) return false; // in-view gate: a gating slot drops the whole change
    const { found, index } = binarySearch(list, seg.parentRow, schema.sort);
    if (!found) throw new Error("flat ArrayView: parent not found at path hop (inconsistent stream)");
    const node = list[index];
    const changed = this.applyAt(node.rels[seg.rel], child, path, op, depth + 1, enrichRemoves);
    if (changed) node.out = null; // a descendant changed → this node must re-project its rel array
    return changed;
  }

  private applyOp(list: Node[], schema: WireSchema, op: FlatOp, enrichRemoves: boolean): boolean {
    if (op.tag === "add") return this.applyAdd(list, schema, op.node);
    if (op.tag === "remove") {
      const removed = this.applyRemove(list, schema, op.row);
      // Attach the full removed subtree (in place) for an opted-in consumer; absent when the row
      // only lost a reference (rc > 1) and so did not actually leave the result.
      if (removed && enrichRemoves) op.node = toWireNode(removed, schema);
      return removed !== null;
    }
    return this.applyEdit(list, schema, op.old, op.new);
  }

  private applyAdd(list: Node[], schema: WireSchema, wnode: WireNode): boolean {
    const at = binarySearch(list, wnode.row, schema.sort);
    if (at.found) {
      list[at.index].rc += 1; // duplicate path to an existing row; ignore subtree
      return false;
    }
    list.splice(at.index, 0, buildNode(wnode, schema));
    return true;
  }

  /** Drop one path to `row`. Returns the evicted {@link Node} (its full subtree intact) when the
   *  last path went away — `null` when another path still holds it (rc decremented, nothing left
   *  the result). */
  private applyRemove(list: Node[], schema: WireSchema, row: WireValue[]): Node | null {
    const at = binarySearch(list, row, schema.sort);
    if (!at.found) throw new Error("flat ArrayView: remove of a non-existent node");
    const node = list[at.index];
    if (node.rc === 1) {
      list.splice(at.index, 1);
      return node;
    }
    node.rc -= 1;
    return null;
  }

  private applyEdit(list: Node[], schema: WireSchema, oldRow: WireValue[], newRow: WireValue[]): boolean {
    if (rowsEqual(oldRow, newRow)) return false;
    const sort = schema.sort;
    if (compareRows(oldRow, newRow, sort) === 0) {
      // Sort key unchanged → edit in place (keeps position + children + rc).
      const at = binarySearch(list, oldRow, sort);
      if (!at.found) throw new Error("flat ArrayView: edit of a non-existent node");
      list[at.index].row = newRow;
      list[at.index].out = null;
      return true;
    }

    // Sort key changed → the row may move; rc may be > 1.
    const oldAt = binarySearch(list, oldRow, sort);
    if (!oldAt.found) throw new Error("flat ArrayView: edit old node does not exist");
    const oldPos = oldAt.index;
    const oldRc = list[oldPos].rc;
    const raw = binarySearch(list, newRow, sort);
    const found = raw.found;
    const pos = raw.index;
    const oldEntry = cloneNode(list[oldPos]); // capture (with children) BEFORE mutating

    // Fast path: rc==1 and the row lands in the same slot after removal → edit in place.
    if (oldRc === 1 && (pos === oldPos || pos - 1 === oldPos)) {
      list[oldPos].row = newRow;
      list[oldPos].out = null;
      return true;
    }

    // General move.
    const newRc = oldRc - 1;
    let adjusted: number;
    if (newRc === 0) {
      list.splice(oldPos, 1);
      adjusted = oldPos < pos ? pos - 1 : pos;
    } else {
      list[oldPos].rc = newRc; // ghost survives for the other path(s); its row is unchanged
      adjusted = pos;
    }
    if (found) {
      // Merge into the existing destination entry (keep its children); bump its rc.
      const existingRc = list[adjusted].rc;
      list[adjusted].row = newRow;
      list[adjusted].rc = existingRc + 1;
      list[adjusted].out = null;
    } else {
      // Move the (edited) old entry — keeping its children — to the new position.
      oldEntry.row = newRow;
      oldEntry.rc = 1;
      list.splice(adjusted, 0, oldEntry);
    }
    return true;
  }

  // --- projection (memoized, structurally shared) -----------------------------

  private project(node: Node, schema: WireSchema, types?: ViewTypes): unknown {
    if (node.out !== null) return node.out;
    const obj: Record<string, unknown> = {};
    for (let i = 0; i < schema.columns.length; i++) {
      const v = node.row[i];
      // A projected query carries an ABSENT cell (`undefined`, PROJECTION-SUPPORT-DESIGN.md
      // §5.3) for a column it did not sync; omit it from the result object so a query never
      // resolves a row from columns it did not select (§6). A present-and-null cell is `null`
      // and is kept. For a `'*'` query no cell is ever `undefined`, so this is inert (§7).
      if (v === undefined) continue;
      // convert-once: a json column's raw string is parsed to an object on store (cached in `out`).
      obj[schema.columns[i]] = types?.columnTypes[i] === "json" && typeof v === "string" ? JSON.parse(v) : v;
    }
    for (const rel of schema.relationships) {
      if (rel.child === null) continue; // gating slot — not part of .data
      const childList = node.rels[rel.slot] ?? [];
      const ct = types?.rels[rel.slot];
      if (rel.project) {
        // A scalar-projected relationship aggregate (REDUCE-DESIGN.md §9): unwrap the one-row
        // child to a bare scalar (its project.col cell), substituting the aggregate identity
        // when empty (a childless parent). A json-typed projected column is parsed like a column.
        const cell = childList.length > 0 ? childList[0].row[rel.project.col] : rel.project.identity;
        const projType = ct?.columnTypes[rel.project.col];
        obj[rel.name] = projType === "json" && typeof cell === "string" ? JSON.parse(cell) : cell;
        continue;
      }
      obj[rel.name] = rel.child.singular
        ? childList.length > 0
          ? this.project(childList[0], rel.child, ct)
          : null
        : childList.map((c) => this.project(c, rel.child as WireSchema, ct));
    }
    node.out = obj;
    return obj;
  }

  /** Notify data subscribers with the current {@link data}. Normally driven by a fold ({@link
   *  applyChanges}/{@link flush}); the Store also calls it directly to land a seed retirement whose
   *  accompanying fold was empty (see {@link retireSeed}). */
  notify(): void {
    const d = this.data;
    for (const l of this.listeners) l(d);
  }
}
