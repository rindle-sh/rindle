// DevtoolsCore — the framework-agnostic engine (DEBUG-TOOLS-BROWSER-DESIGN.md §6.1). It attaches to
// a running client through the read-only seams (`Store.subscribeChanges` / `subscribeResultType` /
// `__inspect`, and the optimistic backend's `__inspect`), then maintains the §4 read-model: the
// mutation TIMELINE
// (reconstructed by diffing successive pending-stack snapshots), the QUERIES inspector, and the
// DELTA stream (preferentially off authoritative backend/server frames, with the Store's raw
// per-query `ChangeEvent` tap as the local fallback). No DOM, no app-code changes, no hot path.

import { collectTables, summarizeAst } from "./ast.ts";
import type {
  BackendServerDelta,
  ChangeEvent,
  DeltaEntry,
  DeltaKind,
  DevtoolsBackend,
  DevtoolsCoreOptions,
  DevtoolsState,
  DevtoolsStore,
  DevtoolsTarget,
  FlatOp,
  NormalizedOp,
  OptimisticInspect,
  QueryEntry,
  QueryId,
  ResultType,
  TimelineEntry,
} from "./types.ts";

/** Narrow a candidate backend to the optional devtools capability surface (§6.2). */
function asDevtoolsBackend(backend: unknown): DevtoolsBackend | undefined {
  if (!backend || typeof backend !== "object") return undefined;
  const b = backend as DevtoolsBackend;
  return typeof b.__inspect === "function" || typeof b.__attachDevtoolsServerDeltas === "function" ? b : undefined;
}

const EMPTY_STATE: DevtoolsState = {
  timeline: [],
  queries: [],
  deltas: [],
  optimistic: undefined,
  capabilities: { optimistic: false },
};

export class DevtoolsCore {
  private readonly store: DevtoolsStore;
  private readonly backend?: DevtoolsBackend;
  private readonly detachStore: () => void;
  private readonly detachServerDeltas?: () => void;
  private readonly hasServerDeltaTap: boolean;

  private readonly timelineCap: number;
  private readonly deltaCap: number;
  private readonly sampleRows: number;
  private readonly autoFlush: boolean;
  private readonly now: () => number;
  private readonly pollHandle?: ReturnType<typeof setInterval>;

  // Timeline state: entries by stable id + an insertion-ordered id list (newest last).
  private readonly timeline = new Map<string, TimelineEntry>();
  private readonly order: string[] = [];
  /** The pending keys seen at the previous recompute — the diff basis for lifecycle transitions. */
  private prevPendingKeys = new Set<string>();

  // Delta ring.
  private deltas: DeltaEntry[] = [];
  private deltaSeq = 0;
  /** Set by a `batch` delta, consumed by the next recompute: did view churn coincide with a
   *  confirmation this turn? (the §4.1 snap-back heuristic). */
  private churnSeen = false;

  private queries: QueryEntry[] = [];
  private optimistic?: OptimisticInspect;

  private readonly listeners = new Set<() => void>();
  private dirty = false;
  private flushScheduled = false;
  private snapshot: DevtoolsState = EMPTY_STATE;
  /** Optional deregistration from the global hub (set by {@link attachDevtools}). */
  onDetach?: () => void;

  constructor(target: DevtoolsTarget, opts: DevtoolsCoreOptions = {}) {
    this.store = target.store;
    this.backend = asDevtoolsBackend(target.backend);
    this.timelineCap = opts.timelineCap ?? 200;
    this.deltaCap = opts.deltaCap ?? 500;
    this.sampleRows = opts.sampleRows ?? 25;
    this.autoFlush = opts.autoFlush ?? true;
    this.now = opts.now ?? (() => Date.now());

    this.detachServerDeltas = this.attachServerDeltas();
    this.hasServerDeltaTap = !!this.detachServerDeltas;

    // The post-fold delta + resultType taps off the Store's supported subscription seams (the same
    // `subscribeChanges` an app narrates off — no private back door). Both fire AFTER the view folds,
    // which is the timing the snap-back/churn heuristics need.
    const detachDeltas = this.store.subscribeChanges((qid, ev) => this.onStoreDelta(qid, ev));
    const detachResultType = this.store.subscribeResultType((qid, rt) => this.onResultType(qid, rt));
    this.detachStore = () => {
      detachDeltas();
      detachResultType();
    };

    const pollMs = opts.pollMs ?? 0;
    if (pollMs > 0) {
      this.pollHandle = setInterval(() => this.refresh(), pollMs);
      // Don't keep a Node process alive on the poll timer (no-op in the browser).
      (this.pollHandle as { unref?: () => void }).unref?.();
    }

    // Seed the initial snapshot from whatever the app already holds (e.g. queries mounted before
    // the pane attached).
    this.refresh();
  }

  // --- public surface ----------------------------------------------------------

  /** The current read-model. Reference-stable between updates (rebuilt only on recompute), so it is
   *  safe to feed a `useSyncExternalStore`-style binding. */
  getState(): DevtoolsState {
    return this.snapshot;
  }

  /** Subscribe to updates; fires immediately with the current state, then after each recompute. */
  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    listener();
    return () => {
      this.listeners.delete(listener);
    };
  }

  /** Force a synchronous recompute (also used by the safety-net poll and by tests). */
  refresh(): void {
    this.dirty = true;
    this.flush();
  }

  /** Clear the delta stream ring (a panel "clear" affordance). */
  clearDeltas(): void {
    this.deltas = [];
    this.refresh();
  }

  /** Drop every SETTLED timeline row (confirmed/dropped), keeping live pending ones + the deltas. */
  clearHistory(): void {
    for (let i = this.order.length - 1; i >= 0; i--) {
      const id = this.order[i];
      const e = this.timeline.get(id);
      if (e && e.state !== "pending") {
        this.timeline.delete(id);
        this.order.splice(i, 1);
      }
    }
    this.refresh();
  }

  /** Detach from the client: stop the poll, drop the store tap, deregister from the global hub. */
  detach(): void {
    if (this.pollHandle) clearInterval(this.pollHandle);
    this.detachStore();
    this.detachServerDeltas?.();
    this.onDetach?.();
    this.listeners.clear();
  }

  // --- event taps --------------------------------------------------------------

  private attachServerDeltas(): (() => void) | undefined {
    try {
      return this.backend?.__attachDevtoolsServerDeltas?.({
        onServerDelta: (qid, ev) => this.onServerDelta(qid, ev),
      });
    } catch {
      return undefined;
    }
  }

  private onStoreDelta(qid: QueryId, ev: ChangeEvent): void {
    // Prefer the authoritative backend/server stream for the visible Deltas pane. The Store tap is
    // still observed for timeline churn: it fires after the view folded the event, which is the
    // signal the snap-back heuristic needs.
    if (!this.hasServerDeltaTap) this.pushChangeDelta(qid, ev);
    // A `batch` carrying real changes is view churn — the signal the snap-back heuristic keys on.
    if (ev.type === "batch" && ev.events.length > 0) this.churnSeen = true;
    this.markDirty();
  }

  private onServerDelta(qid: QueryId, ev: BackendServerDelta): void {
    this.pushServerDelta(qid, ev);
    this.markDirty();
  }

  private onResultType(_qid: QueryId, _rt: ResultType): void {
    this.markDirty();
  }

  private markDirty(): void {
    this.dirty = true;
    if (this.autoFlush && !this.flushScheduled) {
      this.flushScheduled = true;
      queueMicrotask(() => {
        this.flushScheduled = false;
        this.flush();
      });
    }
  }

  private flush(): void {
    if (!this.dirty) return;
    this.dirty = false;
    this.recompute();
    for (const l of this.listeners) l();
  }

  // --- recompute ---------------------------------------------------------------

  private recompute(): void {
    let opt: OptimisticInspect | undefined;
    if (this.backend?.__inspect) {
      try {
        opt = this.backend.__inspect();
      } catch {
        opt = undefined; // a malformed/throwing dev hook never breaks the pane
      }
    }
    if (opt) this.reconcileTimeline(opt);
    this.rebuildQueries(opt);
    this.optimistic = opt;
    this.churnSeen = false;
    this.snapshot = {
      timeline: this.order.map((id) => this.timeline.get(id)).filter((e): e is TimelineEntry => !!e),
      queries: this.queries,
      deltas: this.deltas.slice(),
      optimistic: opt,
      capabilities: { optimistic: typeof this.backend?.__inspect === "function" },
    };
  }

  /** Reconstruct the fork/rebase lifecycle by diffing this pending snapshot against the previous
   *  one (DEBUG-TOOLS-BROWSER-DESIGN §4.1): new keys → invoked; vanished keys → confirmed (mid ≤
   *  confirmedLmid) or dropped; a fold's `f:<foldKey>` → `m:<mid>` transition is linked, not double
   *  counted. */
  private reconcileTimeline(opt: OptimisticInspect): void {
    const now = this.now();
    const cur = opt.pending;
    const curKeys = new Set(cur.map((p) => p.key));

    const removed: string[] = [];
    for (const k of this.prevPendingKeys) if (!curKeys.has(k)) removed.push(k);
    const added = cur.filter((p) => !this.prevPendingKeys.has(p.key));

    // Fold-flush linking: an `f:<foldKey>` that vanished as an `m:<mid>` of the same (name, args)
    // appeared is one logical mutation crossing the wire — relabel in place (FOLDED-MUTATIONS §4.1).
    const linkedF = new Set<string>();
    const linkedM = new Set<string>();
    for (const fkey of removed) {
      if (!fkey.startsWith("f:")) continue;
      const fEntry = this.timeline.get(fkey);
      if (!fEntry) continue;
      const match = added.find(
        (p) => p.key.startsWith("m:") && !linkedM.has(p.key) && p.name === fEntry.name && argsEqual(p.args, fEntry.args),
      );
      if (!match) continue;
      this.renameEntry(fkey, match.key);
      const e = this.timeline.get(match.key);
      if (e) {
        e.mid = match.mid;
        e.tables = match.tables;
        e.folded = true;
        // A flushed fold has already left `folds`, so the m:<mid> snapshot carries no `fold` window —
        // keep the one captured while it was debouncing and just mark it flushed.
        const window = match.fold ?? e.fold;
        e.fold = window ? { ...window, flushed: true } : undefined;
      }
      linkedF.add(fkey);
      linkedM.add(match.key);
    }

    // Freshly invoked entries (not the m: side of a link).
    for (const p of added) {
      if (linkedM.has(p.key) || this.timeline.has(p.key)) continue;
      this.addEntry({
        id: p.key,
        mid: p.mid,
        name: p.name,
        args: p.args,
        tables: p.tables,
        state: "pending",
        folded: !!p.fold,
        fold: p.fold,
        invokedAt: now,
        reconciledWithChurn: false,
        affectedQueries: [],
      });
    }

    // Refresh still-pending entries (a rebase re-invoke can change touched tables / args / mid).
    for (const p of cur) {
      const e = this.timeline.get(p.key);
      if (!e || e.state !== "pending") continue;
      e.mid = p.mid;
      e.args = p.args;
      e.tables = p.tables;
      if (p.fold) {
        e.folded = true;
        e.fold = p.fold;
      }
    }

    // Settle vanished entries (those not consumed by a fold link).
    for (const k of removed) {
      if (linkedF.has(k)) continue;
      const e = this.timeline.get(k);
      if (!e || e.state !== "pending") continue;
      const confirmed = e.mid != null && e.mid <= opt.confirmedLmid;
      e.state = confirmed ? "confirmed" : "dropped";
      e.settledAt = now;
      e.reconciledWithChurn = confirmed && this.churnSeen;
    }

    this.prevPendingKeys = curKeys;
  }

  private rebuildQueries(opt: OptimisticInspect | undefined): void {
    const pendingTables = new Set(opt?.pendingTables ?? []);
    const queries: QueryEntry[] = this.store.__inspect(this.sampleRows).queries.map((q) => {
      const tables = [...collectTables(q.ast)];
      return {
        qid: q.qid,
        ast: q.ast,
        table: q.ast.table,
        summary: summarizeAst(q.ast),
        tables,
        resultType: q.resultType,
        rowCount: q.rowCount,
        sample: q.sample,
        pending: tables.some((t) => pendingTables.has(t)),
      };
    });
    this.queries = queries;

    // Recompute each timeline entry's affected queries against the CURRENT live views.
    if (this.timeline.size > 0) {
      for (const e of this.timeline.values()) {
        const touched = new Set(e.tables);
        e.affectedQueries = queries.filter((q) => q.tables.some((t) => touched.has(t))).map((q) => q.qid);
      }
    }
  }

  // --- delta ring --------------------------------------------------------------

  private pushServerDelta(qid: QueryId, ev: BackendServerDelta): void {
    if (ev.format === "flat") {
      this.pushChangeDelta(qid, ev.event);
      return;
    }
    this.pushNormalizedDelta(qid, ev.event);
  }

  private pushChangeDelta(qid: QueryId, ev: ChangeEvent): void {
    if (ev.type === "hello") {
      this.appendDelta(qid, "hello", 0, `hello · ${ev.schema.columns.length} col${ev.schema.columns.length === 1 ? "" : "s"}`);
    } else if (ev.type === "snapshot") {
      const n = ev.adds.length;
      this.appendDelta(qid, "snapshot", 0, `snapshot · +${n} row${n === 1 ? "" : "s"}${ev.last ? "" : " (chunk)"}`);
    } else {
      for (const ch of ev.events) {
        const depth = ch.path.length;
        this.appendDelta(qid, ch.op.tag, depth, describeOp(ch.op) + (depth > 0 ? `  (child @${depth})` : ""));
      }
    }
  }

  private pushNormalizedDelta(qid: QueryId, ev: Extract<BackendServerDelta, { format: "normalized" }>["event"]): void {
    if (ev.type === "hello") {
      this.appendDelta(qid, "hello", 0, `server hello · ${ev.tables.length} table${ev.tables.length === 1 ? "" : "s"}`);
    } else if (ev.type === "snapshot") {
      const n = ev.ops.length;
      this.appendDelta(qid, "snapshot", 0, `server snapshot · ${n} op${n === 1 ? "" : "s"}${cvLabel(ev.cv)}`);
    } else {
      for (const op of ev.ops) this.appendDelta(qid, op.op, 0, describeNormalizedOp(op) + cvLabel(ev.cv));
    }
  }

  private appendDelta(qid: QueryId, kind: DeltaKind, depth: number, label: string): void {
    this.deltas.push({ seq: this.deltaSeq++, at: this.now(), qid, kind, depth, label });
    if (this.deltas.length > this.deltaCap) this.deltas.splice(0, this.deltas.length - this.deltaCap);
  }

  // --- timeline map bookkeeping ------------------------------------------------

  private addEntry(entry: TimelineEntry): void {
    this.timeline.set(entry.id, entry);
    this.order.push(entry.id);
    // Cap: evict the oldest SETTLED rows first; never drop a live pending row.
    while (this.order.length > this.timelineCap) {
      const idx = this.order.findIndex((id) => this.timeline.get(id)?.state !== "pending");
      if (idx < 0) break;
      const [evicted] = this.order.splice(idx, 1);
      this.timeline.delete(evicted);
    }
  }

  private renameEntry(oldId: string, newId: string): void {
    const e = this.timeline.get(oldId);
    if (!e) return;
    this.timeline.delete(oldId);
    e.id = newId;
    this.timeline.set(newId, e);
    const i = this.order.indexOf(oldId);
    if (i >= 0) this.order[i] = newId;
  }
}

/** Structural args equality for fold-flush linking (args are JSON-serializable mutator inputs). */
function argsEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  try {
    return JSON.stringify(a) === JSON.stringify(b);
  } catch {
    return false;
  }
}

function describeOp(op: FlatOp): string {
  if (op.tag === "add") return `add ${rowPreview(op.node.row)}`;
  if (op.tag === "remove") return `remove ${rowPreview(op.row)}`;
  return `edit ${rowPreview(op.old)} → ${rowPreview(op.new)}`;
}

function describeNormalizedOp(op: NormalizedOp): string {
  if (op.op === "add") return `add ${op.table} ${rowPreview(op.row)}`;
  if (op.op === "remove") return `remove ${op.table} ${rowPreview(op.row)}`;
  return `edit ${op.table} ${rowPreview(op.old)} → ${rowPreview(op.new)}`;
}

function cvLabel(cv: number | undefined): string {
  return cv === undefined ? "" : ` · cv ${cv}`;
}

function rowPreview(row: readonly unknown[]): string {
  const head = row.slice(0, 3).map(cellStr).join(", ");
  return `[${head}${row.length > 3 ? ", …" : ""}]`;
}

function cellStr(v: unknown): string {
  if (typeof v === "string") return v.length > 16 ? `"${v.slice(0, 15)}…"` : `"${v}"`;
  return String(v);
}
