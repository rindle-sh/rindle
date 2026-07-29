// OptimisticBackend — the composition that adds OPTIMISTIC WRITES to the normalized
// local-first path (OPTIMISTIC-WRITES-DESIGN.md §8.5, §9). The richer cousin of
// `NormalizedBackend`: the same substrate (a local `@rindle/wasm` engine + a server
// stream + `NormalizedSync`), plus
//
//   - a **named client-mutator registry**: `invoke("createIssue", args)` runs the
//     mutator against the live engine NOW (the prediction shows instantly), pushes
//     `(mid, name, args)` — never effects — onto the pending stack, and ships the
//     envelope upstream (§4);
//   - **lmid-as-data**: at construction the backend subscribes its own one-row
//     SYSTEM QUERY (`LMID_QUERY_NAME` → `_rindle_client_mutations WHERE client_id =
//     me`). The server writes `lmid` in the SAME transaction as a mutation's
//     effects, so the confirmation arrives as an ordinary cv-tagged data frame and
//     is released by the same `cvMin` — confirmation can never skew from the data
//     of its own commit (the drain race the old frame-carried lmid suffered);
//   - **cv-buffering**: every `cv`-tagged data frame buffers; a progress frame
//     releases all `cv ≤ cvMin` as ONE coherent step — data ops into the engine's
//     `sync` side, lmid-table ops into `confirmedLmid` (§1.3.1/§8.5 — no torn reads
//     across queries, no apply ahead of the release point);
//   - the **§1.3 reconcile cycle** per release: the wasm engine rewinds
//     (`serverBatchBegin`), every still-pending mutator is **re-invoked** against the
//     rebased base (read-dependent mutators read current values — §4.1), and the
//     whole cycle coalesces to one minimal delivery (`serverBatchEnd`) — a
//     confirmed-correct prediction produces zero churn;
//   - per-query **ResultType** is the SERVER CHANNEL's state only (FOLDED-MUTATIONS-DESIGN
//     §7): `unknown` while not hydrated, else `complete`. A pending local mutation no longer
//     moves it — "is a prediction pending here?" is its own reactive axis (`pending(qid)` /
//     `onPending`), so a fold held through its debounce window doesn't pin a query loading.
//     There is NO server rejection signal: a failed mutation is processed-as-no-op (lmid
//     advances, effects rolled back) and the prediction snaps back on the ordinary release.
//   - **folded mutations** (FOLDED-MUTATIONS-DESIGN): `invokeFolded` collapses a run of
//     same-key absorbing writes to ONE pending entry whose `args` are overwritten in place,
//     debounces the server write (a virtual `clock` seam keeps it test-deterministic), and
//     ships only the last value. The `mid` is assigned at FLUSH (not invoke), so the gapless
//     wire sequence is preserved under debounce (§4.1); an overlapping non-fold write drains
//     the interacting fold first (`drainOverlapping`, §4.2 flush-on-enqueue).
//
// `Store`/`ArrayView` are untouched: this implements `Backend`, so
// `new Store(schema, new OptimisticBackend(...))` reuses the whole machinery.

import {
  CLIENT_MUTATIONS_SCHEMA,
  driveMutationSync,
  insertCell,
  insertPlan,
  isGeneratorMutator,
  isoTx,
  LMID_QUERY_NAME,
  localTableNames,
  normalizedTableSchemas,
  rowsEqual,
  tableSpec,
  toCell,
} from "@rindle/client";
import type {
  Ast,
  Backend,
  BackendDevObserver,
  ChangeEvent,
  ColsMap,
  ColType,
  Condition,
  CorrelatedSubquery,
  IsoTx,
  KeyedRow,
  Mutation,
  MutationEnvelope,
  MutationGen,
  MutationOp,
  MutationOutcomeFrame,
  MutatorCtx,
  NormalizedEvent,
  NormalizedOp,
  NormalizedTableSchema,
  OptimisticSource,
  ProgressFrame,
  QueryArg,
  QueryId,
  QueryResultRow,
  RemoteQuery,
  ResultType,
  Schema,
  WireValue,
} from "@rindle/client";
import { aggTableSchemas, NormalizedSync, rewriteAggregates, type ColCounts, type PkCols } from "@rindle/normalized";
import { WasmBackend, type ServerDeltaOp, type WasmWriteTxn } from "@rindle/wasm";

import { AggOverlay, type ChildOp, collectAggDefs } from "./agg-overlay.ts";
import {
  decodeOutcomeRow,
  LIFECYCLE_TABLE_SCHEMAS,
  ROOM_CLIENT_MUTATIONS_TABLE,
  ROOM_MUTATION_OUTCOMES_TABLE,
  ROOM_WATERMARK_TABLE,
  roomDomainKey,
  SCOPE_SESSIONS_TABLE,
  type SystemStreamSpec,
} from "./system-streams.ts";

export type { SystemStreamSpec, SystemStreamTable } from "./system-streams.ts";

/** A keyed row: column name → cell. The ergonomic shape — column names are validated against the
 *  schema at runtime, so a typo throws immediately with the valid names. Re-exported from
 *  `@rindle/client` (the leaf both tiers share). */
export type { KeyedRow };

/** {@link MutationTx.query}'s query handle ({@link QueryArg}, `{ ast(): Ast }`) and result row
 *  ({@link QueryResultRow}) — the SAME types the isomorphic seam uses (203-MUTATOR-READS-DESIGN.md
 *  §5.2/§9.1), re-exported from `@rindle/client` so the client and shared-generator surfaces share
 *  one definition. */
export type { QueryArg, QueryResultRow };

/** The write handle a client mutator runs against (the client `MutationTx`, §4.2):
 *  reads see the current base + this transaction's own staged writes (§4.1).
 *
 *  Prefer the KEYED methods (`insert`/`update`/`upsert`/`delete`/`row`) — named columns,
 *  schema-checked. The positional methods (`get`/`add`/`remove`/`edit`) are the raw wire
 *  shape: bare cells in schema column order, `pk` cells in `primaryKey` order. */
export interface MutationTx {
  // --- keyed (schema-aware) ---
  /** Read one row by primary key (e.g. `tx.row("issue", { id: 1 })`). */
  row(table: string, pk: KeyedRow): KeyedRow | undefined;
  /** Insert a FULL row (every column named; missing or unknown columns throw). */
  insert(table: string, row: KeyedRow): void;
  /** Update the row identified by the pk columns; only the named non-pk columns change.
   *  A missing row is a NO-OP (rebase-friendly: the row may have vanished upstream). */
  update(table: string, row: KeyedRow): void;
  /** Insert, or fully replace when the pk already exists (a FULL row, like `insert`). */
  upsert(table: string, row: KeyedRow): void;
  /** Insert a FULL row, or do nothing if the pk already exists (the isomorphic form of the classic
   *  `if (!tx.row(pk)) tx.insert(row)` upsert-if-absent; renders `ON CONFLICT DO NOTHING` server-side). */
  insertIgnore(table: string, row: KeyedRow): void;
  /** Delete the row identified by the pk columns. A missing row is a NO-OP. */
  delete(table: string, pk: KeyedRow): void;
  /** Run a one-shot read query (`where`/`orderBy`/`limit`/join) over the state this
   *  mutator is mutating — it sees this transaction's own writes-so-far, the same
   *  read-your-writes contract as `get`/`row` (§4.1; 203-MUTATOR-READS-DESIGN.md §5.2).
   *  Synchronous; returns the matching rows in the query's order, each with its materialized
   *  relationship children nested by name (presented identically to a `view.data` row). Pass
   *  a query from the typed builder, e.g. `tx.query(q.issue.where("owner", "=", me))`.
   *  Refused inside a FOLDED mutator (a reading mutator is non-absorbing, §9.1). */
  query(query: QueryArg): QueryResultRow[];
  // --- positional (the wire shape) ---
  get(table: string, pk: WireValue[]): WireValue[] | undefined;
  add(table: string, row: WireValue[]): void;
  remove(table: string, row: WireValue[]): void;
  edit(table: string, oldRow: WireValue[], newRow: WireValue[]): void;
}

/** A client mutator: optimistic, deterministic, replayable — a pure function of `(base, args)` (§5:
 *  no clock, no randomness; it is RE-INVOKED on every rebase). Either shape is accepted:
 *  - a plain synchronous function `(tx, args) => void` (client-only), OR
 *  - a shared GENERATOR `(tx, args, ctx) => MutationGen` (the isomorphic form: the SAME body the API
 *    server runs against a live async transaction — MUTATORS-ISOMORPHIC). The driver detects which. */
export type ClientMutator =
  | ((tx: MutationTx, args: never) => void)
  | ((tx: IsoTx, args: never, ctx: MutatorCtx) => MutationGen);

/** The client registry (§4.2) — one of the two registries; the server's authoritative
 *  twin shares names (and possibly code), never the wire. */
export type ClientRegistry = Record<string, ClientMutator>;

export type { ResultType };

/** The reserved source-qid of the backend's own lmid system query. User/local query ids
 *  are assigned by the `Store` starting at 1, so 0 never collides. */
const LMID_QID: QueryId = 0;

/** One captured write, pk-granular (RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md §3.2 #1: "the
 *  writers already receive `(table, row)`... this is pure capture, no semantic change"). `row` is
 *  the post-write image (positional wire cells, schema column order); `undefined` for a `remove` —
 *  no row survives it, and `row === undefined` stays the remove marker.
 *
 *  `oldRow` is the PRE-IMAGE, captured for a `remove` AND an `edit` (H-ii): the full-width row
 *  read via `tx.get` immediately BEFORE the write staged — read-your-writes, so a write to the
 *  same pk earlier in the SAME invocation shows through — falling back to the caller's asserted
 *  old row when the pk is not txn-visible (a raw remove/edit of an absent row; incl. the
 *  pk-MOVING raw edit, whose record is keyed by the NEW pk yet whose pre-image is the caller's
 *  OLD row). A captured remove/edit thus always carries a full-width pre-image — with ONE
 *  exception: a record that collapses to a (re-)insert has none, see the matrix.
 *
 *  Coalescing within one invocation is last-write-wins per pk on `row` (the final image matches
 *  the engine head's own semantics for that pk) with `oldRow` pinned to the TXN-ENTRY BASE — the
 *  H-ii matrix:
 *    - edit-after-add / edit-after-remove: the record collapses to a (re-)insert — post-image
 *      only, NO `oldRow` (the pk did not pre-exist this invocation's base; presence hold-back
 *      uses `row`).
 *    - edit-after-edit: keeps the FIRST pre-image (the txn-entry base — the chain nets to ONE
 *      edit from the base to the final image).
 *    - remove-after-edit / remove-after-remove: keeps the ORIGINAL pre-image (the first write's
 *      captured base), NOT the edited transient — the net effect is a remove of the row the
 *      external world last knew.
 *    - remove-after-add: keeps the txn-visible pre-image (the transient added row — the pk had
 *      no base, and this is the only truthful full-width row there is; G-iii pinned it).
 *    - add-after-remove (a re-insert): drops `oldRow` (presence hold-back uses `row`).
 *  Across rebase re-invocations the write-set is union-never-shrink ({@link mergeWriteSet}): a
 *  re-run that no-ops keeps the prior record — and its pre-image — intact. */
export interface WriteRecord {
  table: string;
  pk: WireValue[];
  row: WireValue[] | undefined;
  oldRow?: WireValue[];
}

/** The pk-granular write-set captured over ONE mutator invocation: table → pk-key (a stable-JSON
 *  encoding of the pk cells, {@link stableJson}) → that pk's write, LAST-WRITE-WINS within the
 *  invocation — an add-then-edit or edit-then-edit of the SAME pk collapses to its final image,
 *  matching the engine head's own semantics for that pk. Chosen (over a flat array) because the
 *  later routing proof needs "is pk P in the writable scope" / "did we already see this pk in this
 *  invocation" as cheap lookups, and rebase re-invocation needs to MERGE a fresh write-set into an
 *  accumulated one ({@link mergeWriteSet}) — both are Map operations, not scans.
 *
 *  `touched` (the pre-existing table-granular `Set<string>` the pending axis reads, §7.2) is
 *  exactly `new Set(writeSet.keys())` — derived from this, never separately populated, so the two
 *  can never drift. */
export type WriteSet = Map<string, Map<string, WriteRecord>>;

/** Whether a recorded point read (`tx.get`/`tx.row`) found a row. */
export type ReadOutcome = "present" | "absent";

/** One recorded point read, pk-granular (§3.2 #2). Recording-mode only — see {@link ReadLog}.
 *  Since H-ii this covers BOTH the public reads (`tx.get`/`tx.row`) and the keyed writers'
 *  internal pre-existence probes (§3.2 #3 — see the `rawGet` note in {@link trackingTx}). */
export interface ReadRecord {
  table: string;
  pk: WireValue[];
  outcome: ReadOutcome;
}

/** The read-log captured over ONE mutator invocation when recording is armed (RINDLE-REALTIME-
 *  QUERY-ENABLEMENT-DESIGN.md §3.2 #2): every point read (`reads` — the public `tx.get`/`tx.row`
 *  and, since H-ii, the keyed writers' internal pre-existence probes) plus every resolved query AST
 *  (`queries`, from `tx.query`). A SIBLING of the folded read TRAP (`FoldReadError` below) — the
 *  trap arms on the folded path and throws before any read completes (recording never runs there);
 *  recording arms on the ordinary (non-folded) prediction run and never throws. Pure capture for
 *  devtools/inspection (the §3 routing derivation it once fed was removed by
 *  302-ROOM-STORE-SEPARATION-DESIGN.md §5 — mutators DECLARE their domain now). */
export interface ReadLog {
  reads: ReadRecord[];
  queries: Ast[];
}

interface PendingMutation {
  /** The wire mutation id. A FOLDED entry carries `null` until its window flushes — the `mid`
   *  is dealt from `nextMid` in SEND order, never reserved at invoke, so the wire sequence stays
   *  gapless under debounce (FOLDED-MUTATIONS-DESIGN §4.1). A `null` entry is never confirmable
   *  (the confirm-drop retains it) and re-invokes AFTER every assigned mid. */
  mid: number | null;
  /** The client-global deal sequence, stamped in the same breath as {@link mid} (`null` while the
   *  mid is). Mids are PER-DOMAIN (each authority numbers its own confirms, §7.1), so mids from
   *  different domains are incomparable — a daemon mid 5 and a room mid 1 say nothing about which
   *  was sent first. `seq` is the ONE total order across domains: **confirmation order is
   *  per-domain; replay order is client-global** — the reconcile's re-invocation sort keys on
   *  `seq`, never on `mid`. Within one domain `seq` order equals `mid` order (both dealt at the
   *  same send-time choke point) EXCEPT across an H-v deopt re-enqueue: a flipped entry keeps its
   *  ORIGINAL seq while its fresh daemon mid is dealt later, so its seq may undercut daemon
   *  entries with smaller mids. That is the point — seq is the REPLAY order and the flip must not
   *  move the entry's overlay position (a read-dependent sibling invoked after it replays on its
   *  value); the only consumer of the ordering is the seq-keyed reconcile sort, which wants
   *  exactly this. Single-domain behavior without deopts is unchanged. */
  seq: number | null;
  name: string;
  args: unknown;
  /** The confirming stream (RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md §7.1): which domain's ledger
   *  dealt this entry's `mid` and whose confirm watermark can retire it. Resolved from the injectable
   *  `domainPolicy` when the mid is dealt (for a folded entry, re-resolved at flush). `"daemon"` in
   *  the single-domain configuration. An un-flushed fold (`mid == null`) carries its provisional
   *  domain but is never confirmable until the flush stamps a real mid. */
  domain: string;
  /** Tables this mutator touched at its LAST invocation (drives the pending axis, §7.2). Derived
   *  from `writes.keys()` — see {@link WriteSet}. */
  touched: Set<string>;
  /** The pk-granular write-set captured at this entry's LAST invocation (§3.2 #1). A rebase
   *  re-invocation MERGES its fresh write-set into this one ({@link mergeWriteSet}), mirroring the
   *  `touched` union: the key set only grows across re-invocations (a re-run that no-ops must not
   *  shrink it, §7.2), each key's value is always the newest. Tables are ENGINE names: a
   *  room-domain entry's writes on the room's own tables record the namespaced name (302 §2). */
  writes: WriteSet;
  /** The read-log captured at this entry's LAST *recorded* invocation (§3.2 #2). Empty for a
   *  FOLDED entry (the trap, not recording, arms on that path — nothing is ever recorded there)
   *  and left as the ORIGINAL invoke's log across a rebase re-invocation: recording is armed only
   *  on the initial `invoke`, not the reconcile replay (a re-invocation runs against a rebased
   *  base, so its read outcomes would need re-proving against the NEW base anyway — re-arming
   *  recording there is future work, not required for pure capture). */
  reads: ReadLog;
}

/** A virtual-clock seam for the fold debounce/maxWait timers (FOLDED-MUTATIONS-DESIGN §9): the
 *  oracle injects a deterministic scheduler; production defaults to real timers + `Date.now`. */
export interface FoldClock {
  setTimeout(cb: () => void, ms: number): unknown;
  clearTimeout(handle: unknown): void;
  now(): number;
}

/** Options for a folded call site (FOLDED-MUTATIONS-DESIGN §3). `key` is the identity half of the
 *  fold key (combined with the mutator name); the rest tune the debounce policy. */
export interface FoldOptions {
  /** The identity half of the fold key (typically the targeted primary key). Required. */
  key: unknown;
  /** Trailing debounce: the flush fires this long after the LAST invoke for `key`. Default 120ms. */
  debounceMs?: number;
  /** Hard cap so a never-idle drag still persists periodically (trailing throttle). Unbounded if
   *  omitted — an idle gap of `debounceMs` is then the only thing that flushes. */
  maxWaitMs?: number;
  /** §9.3 room-aware cadence. When this fold's write ROUTES INTO A ROOM (a collaborator is live on
   *  the shared head), flush at this (short) interval instead of `debounceMs`/`maxWaitMs`, so the
   *  intermediate frames STREAM to the room rather than collapsing to last-value-wins — the pen is
   *  watched, so its growth matters. OFF the room (solo / daemon-served) this is ignored and the
   *  caller's `debounceMs` collapse governs. `0` ⇒ per-frame (never coalesce while in a room); a
   *  small value (e.g. 40ms) animates while still capping the write rate. Absent ⇒ same cadence
   *  room or not (today's behavior). The room decision is probed from the write-set at the fold
   *  window's first invoke; it stays fixed for that window. */
  roomDebounceMs?: number;
  /** Keep deferring across overlapping non-fold writes for maximum economy, accepting the §4.2
   *  read-dependent reorder snap. Default `false` (flush-on-enqueue — correct-and-boring). */
  deferAcrossWrites?: boolean;
}

/** The handle a folded call site gets back (FOLDED-MUTATIONS-DESIGN §3): no `mid` is assigned yet
 *  (§4.1), so this exposes `flush()` to force the window now and a `mid` promise that resolves with
 *  the wire id once the window flushes — a caller that needs the server ack can await it. */
export interface FoldHandle {
  flush(): void;
  readonly mid: Promise<number>;
}

/** The single live fold entry for one fold key, plus its debounce bookkeeping (§8). */
interface FoldRecord {
  /** The single pending entry; `entry.mid` stays `null` until `flushFold`. */
  entry: PendingMutation;
  /** Latest args observed for this key — the value the flush envelope ships (mirror of entry.args). */
  args: unknown;
  /** The live debounce timer handle (cleared on re-arm / flush). */
  timer: unknown;
  /** `clock.now()` at the first invoke of this window — for the `maxWaitMs` cap. */
  firstAt: number;
  debounceMs: number;
  maxWaitMs?: number;
  deferAcrossWrites: boolean;
  /** Resolves with the assigned mid at flush (the handle's `mid` promise). */
  midPromise: Promise<number>;
  resolveMid: (mid: number) => void;
}

/** Thrown by the read-trap tx when a FOLDED mutator reads state (`tx.get`/`tx.row`) — the classic
 *  non-absorbing shape, refused at the folded path (FOLDED-MUTATIONS-DESIGN §5). */
class FoldReadError extends Error {}

interface BufferedFrame {
  cv: number;
  qid: QueryId;
  kind: "snapshot" | "batch";
  ops: NormalizedOp[];
  /** Arrival order — the tiebreak for equal-`cv` frames (release applies in order). */
  seq: number;
}

/** ONE authority channel's coherence gate (RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md §5.1): the
 *  per-source generalization of what used to be the backend's single `(buffer, appliedCv)` pair.
 *  Each connected source — the daemon always; a `room:doc:X` once Slice G wires the live feed —
 *  buffers its own cv-tagged frames and releases on its OWN `cvMin`, an independent cycle feeding
 *  the one `applyRelease`: **coherent within a source; eventual across sources** (no joint
 *  barrier, by design). `key` doubles as the lmid fold domain (§7.1) and the physical source the
 *  release rebases (§5.3). `sync` is this source's OWN refcount/baseline space — folding two
 *  sources through one `NormalizedSync` would refcount an overlapping row 1→2 and emit NOTHING
 *  for the second source, silently starving its per-source baseline (§5.2 "a real store: its own
 *  baseline"). */
interface SourceGate {
  key: string;
  source: OptimisticSource;
  sync: NormalizedSync;
  buffer: BufferedFrame[];
  /** Arrival counter behind {@link BufferedFrame.seq}, scoped to THIS gate's buffer. */
  nextSeq: number;
  appliedCv: number;
  /** A ROOM gate's wire-table → engine-table map (302 §2 — one source per table): the channel's
   *  released deltas rename into the room's own namespaced tables, and a wire table NOT in the
   *  map (context the room still publishes, or an unknown table) is DROPPED — the daemon is the
   *  sole authority for context, so its copy must never enter the store from a room channel
   *  (302 §6). Absent on the daemon gate (its deltas apply verbatim). */
  tableMap?: ReadonlyMap<string, string>;
}

/** One I-iv doorbell event ({@link OptimisticBackend.onScopeSessions}, §4.1): a release folded
 *  scope-session rows for `scope`, and `others` is the count of OTHER clients' unexpired sessions
 *  there — {@link OptimisticBackend.otherScopeSessions} evaluated at fold time (the same one rule,
 *  on the injectable {@link FoldClock}, so a virtual-clock harness gets deterministic verdicts).
 *  The consumer (client.ts) triggers its one debounced re-lease on the 0→≥1 transition; expired
 *  and own-clientID rows never count, so a solo client's own row can never ring its own bell. */
export interface ScopeSessionsEvent {
  scope: string;
  others: number;
}

/** One demoted room source's §4.2 SWAP-BACK gate record (Slice I-v, re-expressed by 302 §4.2):
 *  after a downgrade the room's namespaced tables keep backing their views — frozen at the room's
 *  last state (the channel is disconnected) — until the daemon plane has provably absorbed the
 *  room's final flush. Swapping the views back earlier would show the falling-back follower's
 *  PRE-flush images (a visibly rolled-back document). The drop condition is evaluated after every
 *  release ({@link OptimisticBackend.evaluateGhosts}):
 *
 *    `roomWatermarks[doc] ≥ finalFlushSeq`  (0 ⇒ trivially true — a never-flushed room)
 *    AND no pending mutation with `domain === sourceKey` remains (sent-pins-domain, §7.5 —
 *    room-domain entries retire ONLY through the outcome-resolved daemon-carried folds, I-iii).
 *
 *  Both satisfied ⇒ {@link OptimisticBackend.dropGhost}: every room-swapped view re-registers on
 *  its ORIGINAL (daemon-table) AST — value-equal under the fence, so visually a no-op — and the
 *  room's namespaced tables unregister. */
interface RoomGhost {
  doc: string;
  finalFlushSeq: number;
  /** Whether the ONE stuck-downgrade event already fired for this ghost. */
  stuckReported: boolean;
}

/** The I-v stuck-downgrade event ({@link OptimisticBackend.onDowngradeStuck}): the ghost's fence
 *  is satisfied but these SENT room-domain mids never resolved (an entry that never reached the
 *  room — sent-but-undelivered when the socket died — is undecidable in general, §7.5). The ghost
 *  HOLDS (fail LOUD, never silent; no timeout-retire is invented) and the mids are surfaced once,
 *  actionably. */
export interface DowngradeStuckEvent {
  sourceKey: string;
  doc: string;
  mids: number[];
}

export interface OptimisticBackendOptions {
  /** Stable per-client identity for the upstream envelopes (§8.1). */
  clientID: string;
  /** The acting principal, for a shared (generator) mutator's `ctx.user` — re-read per invoke so a
   *  mid-session login is picked up, and stable across a rebase re-invoke (replayable). Plain
   *  client-only mutators ignore it. Defaults to the empty string (an app that registers generator
   *  mutators must supply this). */
  user?: () => string;
  /** Buffered-frame ceiling before the §8.5 escape (drop + re-hydrate). */
  bufferCap?: number;
  /** Virtual-clock seam for the fold debounce timers (FOLDED-MUTATIONS-DESIGN §9). Defaults to
   *  real `setTimeout`/`clearTimeout`/`Date.now`; the fold oracle injects a deterministic clock. */
  clock?: FoldClock;
  /** The DECLARED confirming stream per mutation (302 §5: declared, not derived — there is no
   *  routing proof). A policy returning a string pins that domain verbatim: the mutation stages
   *  onto that room's namespaced tables and ships on its channel. Returning `undefined` (or
   *  configuring no policy) means `"daemon"`. The client layer builds this from the app's declared
   *  realtime mutators + the currently attached rooms; a misdeclaration fails SOFT (302 §5.1) —
   *  the write lands on the other authority's tables and the view simply stops feeling instant
   *  until the echo relays it. */
  domainPolicy?: (name: string, args: unknown) => string | undefined;
  /** A FINAL (authz/validation) mutation rejection's reason surface — the room plane's twin of the
   *  HTTP mutate route's `onRejected` (H-v; the H-iv-b `mutationOutcome {kind:"rejected"}` frame).
   *  The prediction's snap-back is NOT this callback's job: the room burns the mid and its lmid
   *  release drops the entry exactly as a daemon-path rejection does (processed-as-no-op) — this
   *  is where the REASON reaches the app, same contract as the queue's callback. Also invoked when
   *  a DEOPT's fresh re-invocation (the already-retired arm) throws — that mutation is dead on the
   *  current base with no stream left to confirm it, the closest thing to a rejection there is. */
  onRejected?: (envelope: MutationEnvelope, reason: string) => void;
}

// --- dev-only introspection (DEBUG-TOOLS-BROWSER-DESIGN §2/§4.1) -----------------
// A single read-only snapshot of the optimistic loop's state for a devtools pane — the source the
// "mutation timeline" reconstructs the fork/rebase lifecycle from. Every field below is already
// held by the backend; `__inspect()` just copies it out (no new hot-path state). The shapes are
// mirrored by `@rindle/devtools`' own `OptimisticInspect` (kept structurally identical there so the
// core need not import this package and drag in the wasm artifact at typecheck time).

/** One folded entry's debounce window, for the timeline's fold drill-down (§4.1). */
export interface FoldInspect {
  /** The fold key (`${name}\0${identityJSON}`) collapsing same-key invokes into one entry. */
  foldKey: string;
  debounceMs: number;
  maxWaitMs?: number;
  deferAcrossWrites: boolean;
  /** Whether the window has flushed (a real `mid` was dealt); an un-flushed fold has `mid == null`. */
  flushed: boolean;
}

/** One pending mutation, as the timeline sees it (DEBUG-TOOLS-BROWSER-DESIGN §4.1). */
export interface PendingInspect {
  /** Stable identity across snapshots: `m:<mid>` once a mid is assigned, else `f:<foldKey>` for an
   *  un-flushed folded entry (the mid is dealt at flush, FOLDED-MUTATIONS-DESIGN §4.1). */
  key: string;
  /** The wire mutation id, or `null` for an un-flushed fold. */
  mid: number | null;
  name: string;
  args: unknown;
  /** Tables this mutator touched at its last (re)invocation — the pending-axis basis (§7.2). */
  tables: string[];
  /** The pk-granular write-set captured at this entry's LAST invocation (RINDLE-REALTIME-QUERY-
   *  ENABLEMENT-DESIGN.md §3.2 #1), flattened from the {@link WriteSet} map for inspection — one
   *  entry per `(table, pk)` currently held. Pure capture; no routing consumer yet. */
  writes: WriteRecord[];
  /** The read-log captured at this entry's LAST *recorded* invocation (§3.2 #2). Empty for a
   *  folded entry — the read TRAP arms there, not recording (see {@link PendingMutation.reads}). */
  reads: ReadLog;
  /** Present iff this entry is a folded (debounced) write. */
  fold?: FoldInspect;
}

/** A read-only snapshot of the optimistic loop ({@link OptimisticBackend.__inspect}). */
export interface OptimisticInspect {
  /** The pending stack in array order (assigned mids ascending, un-flushed folds interleaved by
   *  creation — the re-invoke order is derived from this in `runReconcileCycle`). */
  pending: PendingInspect[];
  /** High-water confirmed mid: an entry with `mid <= confirmedLmid` has been confirmed/dropped. */
  confirmedLmid: number;
  /** The next mid to be dealt (so `nextMid - 1` is the highest issued). */
  nextMid: number;
  /** The applied coherent-release watermark (`cvMin`, §8.6). */
  appliedCv: number;
  /** Frames still buffered awaiting their release point (§8.5) — a backpressure gauge. */
  bufferedFrames: number;
  /** Every table some pending mutation currently touches (the coarse pending indicator set, §7.2). */
  pendingTables: string[];
}

/** Default trailing-debounce window for a folded call site (FOLDED-MUTATIONS-DESIGN §3 example). */
const DEFAULT_FOLD_DEBOUNCE_MS = 120;

/** The real-timer clock used when none is injected. */
const REAL_CLOCK: FoldClock = {
  setTimeout: (cb, ms) => setTimeout(cb, ms),
  clearTimeout: (h) => clearTimeout(h as ReturnType<typeof setTimeout>),
  now: () => Date.now(),
};

/** The (shared, frozen-by-convention) empty map {@link OptimisticBackend.roomTablesFor} answers
 *  for a room with no registered tables. */
const EMPTY_ROOM_TABLES: ReadonlyMap<string, string> = new Map();

/** Per-domain retention cap for the processed-outcome set (H-v) — mirrors the shell's
 *  `MAX_RECORDED_OUTCOMES_PER_CLIENT`: the sender caps what it can re-answer at 512 per client,
 *  so remembering more than 512 processed mids per domain buys nothing. */
const MAX_PROCESSED_OUTCOMES_PER_DOMAIN = 512;

export class OptimisticBackend<S extends ColsMap> implements Backend {
  private readonly local: WasmBackend<S>;
  private readonly sync: NormalizedSync;
  private readonly source: OptimisticSource;
  private readonly registry: ClientRegistry;
  private readonly clientID: string;
  /** The acting principal provider for a shared mutator's `ctx.user` (§ shared mutators). */
  private readonly user: () => string;
  private readonly bufferCap: number;
  /** Column order + pk indices per table, for the keyed `MutationTx` methods. */
  private readonly specs: TableSpecs;
  /** Local-only table names (`201-LOCAL-ONLY-TABLES-DESIGN.md` §4). Drives: the agg-rewrite gate
   *  (L1 — a local-child count stays a native reduce), the mutator guard (M1 — a replayable
   *  mutator may not read/write one), and `writeLocal` (M2 — it accepts ONLY these). */
  private readonly localTables: Set<string>;
  /** Each table's full column count (union-row width) + column-name → base ColId — to learn a
   *  projected query's per-table projection off its `hello` and register it with the sync layer,
   *  so it scatters that query's narrower rows into the shared union (PROJECTION-SUPPORT-DESIGN
   *  §5.2). Without this a projected query's short rows reach the wasm `Db` un-scattered and fail
   *  its width check. */
  private readonly colCounts: ColCounts;
  private readonly colIndex: Record<string, Map<string, number>>;
  /** Per-table pk column indices — held so `connectSource` can build a fresh per-source
   *  `NormalizedSync` with the same layout the daemon's uses. */
  private readonly pkCols: PkCols;
  /** The client's OWN typed per-table schemas + the reserved lmid table — the fixed base
   *  of the expected-schema set (CRIT#4 validation). Synthetic agg tables are appended as
   *  queries arrive (`ensureSyntheticTables`). */
  private readonly clientTablesBase: NormalizedTableSchema[];
  /** Synthetic aggregate tables (`__agg_*`) registered so far, by name (AGGREGATE-SYNC-DESIGN
   *  §3.3). Per aggregate DEFINITION (not per query), so two queries over the same count
   *  share one table. */
  private readonly synthetic = new Map<string, NormalizedTableSchema>();
  /** Synthetic table name → how many registered LOCAL queries reference it. A table is
   *  materialized on the `0→1` transition and reclaimed (engine source + baseline + refcount
   *  layer + overlay def) on `1→0` — so aggregate state is not permanent (§4). */
  private readonly syntheticRefs = new Map<string, number>();
  /** Local qid → the synthetic tables it referenced at registration, to decrement on teardown. */
  private readonly queryAggTables = new Map<QueryId, string[]>();
  /** The optimistic aggregate overlay (§4–§6): the per-aggregate definitions + the per-group
   *  pending delta `displayed = server_base ⊕ local_pending_delta` is applied from. */
  private readonly overlay = new AggOverlay();
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  // The local qids whose reconcile-cycle batch delivery this release is their FIRST hydration (empty
  // → full): their batch is the initial result set arriving as a delta, so it is stamped `catchUp`
  // and the Store maps it to the `snapshot` change-phase (a narrator ignores it by default). Non-null
  // only for the duration of the reconcile cycle in `onProgress`; a re-hydrate after a drop is a real
  // footprint diff (genuine change) and is NOT remapped. See {@link ChangeEvent} `catchUp`.
  private catchUpQids: Set<QueryId> | null = null;
  /** Newly-hydrated qids whose reconcile ACTUALLY emitted a (catch-up-stamped) batch — recorded by the
   *  local-event forwarder alongside {@link catchUpQids}. After the reconcile, any newly-hydrated qid
   *  NOT in here folded nothing (0 rows, or its result already present via a sibling → 0 net muts, or
   *  the reconcile was skipped), so `onProgress` sends it an explicit empty catch-up — else its SSR
   *  seed would never retire (the view freezes). Non-null only for the reconcile's duration. */
  private catchUpEmitted: Set<QueryId> | null = null;
  /** The Store's commit-boundary handler ({@link Backend.onCommitBoundary}), forwarded from the
   *  local engine's `dispatch` brackets so the Store folds every affected view before notifying any
   *  subscriber (cross-view-atomic notification). All this backend's data deltas originate from the
   *  local engine, so its commit brackets are this backend's commit brackets. */
  private boundaryHandler: (phase: "begin" | "end") => void = () => {};
  private readonly devObservers = new Set<BackendDevObserver>();

  private pendingMutations: PendingMutation[] = [];
  /** The next mid to deal, PER DOMAIN (RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md §7.1): a client
   *  writing through room + daemon concurrently must not alias one lmid counter. Seeded with the
   *  `"daemon"` stream at 1; a domain absent from the map starts at 1. In the single-domain
   *  configuration only `"daemon"` is ever touched, so the sequence is byte-for-byte as before. */
  private nextMid = new Map<string, number>([["daemon", 1]]);
  /** The client-global deal counter behind {@link PendingMutation.seq}: one sequence across ALL
   *  domains, bumped whenever any domain's mid is dealt. The replay order (mids are per-domain and
   *  incomparable across domains — see the `seq` field doc). */
  private dealSeq = 0;
  /** The explicit confirming-stream override (§7.1/§3) — see
   *  {@link OptimisticBackendOptions.domainPolicy}. `undefined` from it ⇒ H-iii derivation. */
  private readonly domainPolicy: (name: string, args: unknown) => string | undefined;
  /** The final-rejection reason surface ({@link OptimisticBackendOptions.onRejected}). */
  private readonly rejectedHandler: (envelope: MutationEnvelope, reason: string) => void;
  /** Processed `(domain, mid)` outcome frames (H-v) — the deopt handshake's idempotence guard: a
   *  duplicate frame (the original plus a reconnect re-send's re-answer, or two re-answers across
   *  two reconnects) must not double-invoke. Needed precisely because a deopt frame can arrive for
   *  an ALREADY-RETIRED mid (the replay gotcha) — "no matching entry" alone cannot distinguish
   *  "handle it fresh" from "already handled". Per-domain FIFO, capped like the shell's
   *  recorded-outcome map ({@link MAX_PROCESSED_OUTCOMES_PER_DOMAIN}); past the cap a duplicate of
   *  an evicted mid would be re-processed — the same bounded-window trade the shell makes, and it
   *  takes 512 interleaving non-applied outcomes on one domain to open it. */
  private readonly outcomesProcessed = new Map<string, Set<number>>();
  /** THE room-table registry (302 §2 — one source per table): per connected room `sourceKey`, the
   *  wire-table → engine-table map for the tables that room OWNS (its writable scope). Written by
   *  {@link registerRoomTables} (same breath as the engine registration); read by the gate's
   *  release rename/filter, the mutator staging map, the view swap ({@link processSwapIns}), and
   *  the client's `__realtimeInspect` bookkeeping. The record outlives a downgrade's disconnect —
   *  the ghost's views still read the engine tables — and drops at {@link dropGhost} (or the last
   *  clean release via {@link unregisterRoomTables}). */
  private readonly roomTables = new Map<string, Map<string, string>>();
  /** Local view qids currently REGISTERED on a room's namespaced tables (302 §4 swap-in), →
   *  their sourceKey. Set by {@link processSwapIns}; cleared by the swap-back ({@link dropGhost})
   *  and view teardown. The original AST stays in {@link asts} throughout — the swap re-registers
   *  only the ENGINE query. */
  private readonly roomSwappedViews = new Map<QueryId, string>();
  /** Room subs whose FIRST snapshot released in the current release — their views swap onto the
   *  room tables at the release tail ({@link processSwapIns}), strictly AFTER the reconcile folded
   *  the snapshot into those tables (swapping earlier would hydrate the view EMPTY, a flash). */
  private readonly pendingSwapIns = new Set<RemoteSub>();
  /** The live fold entries, by fold key `${name}\0${identityJSON}` — at most one per key
   *  (FOLDED-MUTATIONS-DESIGN §8). Insertion order is creation order (the drain/flush tiebreak). */
  private readonly folds = new Map<string, FoldRecord>();
  /** The fold debounce clock (real timers by default; the oracle injects a virtual one). */
  private readonly clock: FoldClock;
  /** The high-water confirmed mutation id, PER DOMAIN (RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md
   *  §7.2 per-domain confirm-drop): an entry with `mid <= watermark[entry.domain]` has been
   *  confirmed. The `"daemon"` domain is folded from the lmid system query's RELEASED ops
   *  (lmid-as-data) — never from a frame; a room domain will fold from its own lmid stream (later
   *  slice). Seeded with `"daemon"` at 0; the daemon scalar `confirmedLmid` (devtools) is
   *  `watermark.get("daemon")`. */
  private watermark = new Map<string, number>([["daemon", 0]]);
  /** The per-source coherence gates (§5.1), by source key. Seeded with the daemon gate at
   *  construction; a room channel attaches later (`connectSource`). Single-domain: one entry,
   *  and every gate-generalized path degenerates to the old single-buffer code. NOT the same
   *  space as {@link watermark}/{@link nextMid}: a DOMAIN can confirm with no gate connected
   *  (the `__testRelease` seam); a gate's `key` names the domain its lmid stream folds into. */
  private readonly gates = new Map<string, SourceGate>();
  /** The daemon's gate — the always-present channel (constructor-attached). The devtools
   *  scalars (`__inspect`) read it directly; its `sync` IS {@link sync} (the agg overlay and
   *  synthetic tables are daemon-tracked by design). */
  private readonly daemonGate: SourceGate;
  // --- the §4 lifecycle SYSTEM-STREAM plane (Slice I-iii) --------------------------------
  /** System retains by source qid ({@link retainSystemQuery}): a subscription with NO store view
   *  and NO user-visible table — its frames buffer on its gate exactly like {@link LMID_QID}'s and
   *  fold at RELEASE time ({@link foldSystemFrames}), never entering the sync layer or the local
   *  engine. The spec names which system table the qid serves and the scope/doc it was minted for
   *  (the fold's row filter). Empty on every non-lifecycle client — every partition below is then
   *  a structural no-op and the release path is byte-identical to before. */
  private readonly systemQids = new Map<QueryId, SystemStreamSpec>();
  /** The §4.2 fence state: room doc → highest `flush_seq` delivered through the daemon plane
   *  (monotone max-fold; a remove never regresses it). Slice I-v's ghost-drop consumer — I-iii
   *  only maintains + exposes it (`__inspectDomains().lifecycle`). */
  private readonly roomWatermarks = new Map<string, number>();
  /** The §4.1 occupancy state: scope → (client_id → expires_at) from the doorbell stream. Slice
   *  I-iv's doorbell consumer (the 1→2 re-lease reaction) — I-iii only maintains + exposes it.
   *  A snapshot REPLACES the scope's map (authoritative re-hydrate); a batch folds add/edit/remove
   *  incrementally (the age-out sweep's deletes arrive as removes). */
  private readonly scopeSessions = new Map<string, Map<string, number>>();
  /** The I-iv doorbell event sink ({@link onScopeSessions}) — fired once per scope a release's
   *  scope-session fold touched, AFTER the whole release applied. Default no-op: a client that
   *  never registers (no lifecycle plane) pays nothing. */
  private scopeSessionsHandler: (event: ScopeSessionsEvent) => void = () => {};
  /** Deferred old-channel row GC for in-flight upgrade retargets ({@link retargetRemoteQuery}):
   *  sub sourceQid → the channel it left. The rows the OLD gate's sync holds for the qid stay
   *  visible (merge: daemon tier) until the sub's first snapshot RELEASES on its new room channel
   *  ({@link flushRetargetGc}) — dropping them at retarget time would emit net removes ahead of
   *  the room's re-adds, the flicker the two-phase cutover exists to avoid. Doubles as the
   *  wrong-channel GRACE window in {@link onFrame}: a frame already in flight from the old
   *  channel when the sub moved is stale, not a wiring bug. Empty on every non-upgrade client —
   *  every consultation below is then a structural no-op. */
  private readonly pendingRetargetGc = new Map<QueryId, string>();
  /** The §4.2 GHOSTS (Slice I-v): demoted room sources awaiting their watermark fence, by
   *  sourceKey. Written only by {@link demoteRoomSource}; evaluated after every release
   *  ({@link evaluateGhosts}) and dropped by {@link dropGhost} once the fence clears with no
   *  sent room-domain pending left. Empty on every non-downgrade client — the per-release
   *  evaluation is then a structural no-op. */
  private readonly ghosts = new Map<string, RoomGhost>();
  /** The I-v stuck-downgrade surface ({@link onDowngradeStuck}) — fired AT MOST ONCE per ghost
   *  when its fence is satisfied but sent room-domain mids remain unresolved (§7.5: they retire
   *  only through outcome resolution; the ghost holds rather than inventing a timeout-retire).
   *  Default no-op. */
  private downgradeStuckHandler: (event: DowngradeStuckEvent) => void = () => {};

  private readonly asts = new Map<QueryId, Ast>();
  /** Per query: the base tables its result can draw from (from the AST tree). */
  private readonly queryTables = new Map<QueryId, Set<string>>();
  private readonly remoteSubs = new Map<string, RemoteSub>();
  private readonly sourceToRemote = new Map<QueryId, string>();
  private readonly localToRemote = new Map<QueryId, string>();
  private readonly remoteRetainToLocal = new Map<QueryId, QueryId | undefined>();
  private readonly resultTypes = new Map<QueryId, ResultType>();
  /** Local view qids that are server-authoritative: a query with no remote sub (purely local) is
   *  hydrated on registration; a remote query is hydrated when its sub's first snapshot releases.
   *  An un-hydrated query reports `unknown` (still loading) — the basis of `resultType`. */
  private readonly hydrated = new Set<QueryId>();
  private resultTypeHandler: (qid: QueryId, rt: ResultType) => void = () => {};
  /** The pending AXIS (§7.2), split off `ResultType`: per query, whether any pending mutation
   *  touches its tables. Cached so `onPending` fires only on transitions (invoke ↔ confirm). */
  private readonly pendingState = new Map<QueryId, boolean>();
  private pendingHandler: (qid: QueryId, pending: boolean) => void = () => {};
  /** The local-persistence write-through tap (`207-LOCAL-TABLE-PERSISTENCE-DESIGN.md` §5.1):
   *  {@link writeLocal} invokes it post-commit; {@link applyLocalReplica} deliberately does not. */
  private localWriteObserver: ((mutations: Mutation[]) => void) | null = null;

  constructor(
    schema: Schema<S>,
    source: OptimisticSource,
    registry: ClientRegistry,
    opts: OptimisticBackendOptions,
  ) {
    this.local = new WasmBackend(schema);
    this.local.onEvent((qid, ev) => {
      // Stamp a newly-hydrating query's reconcile batch as a catch-up (initial-hydration) delivery,
      // so the Store phases it as a `snapshot` rather than narrating the whole first result set. Record
      // that we emitted a hydration batch for this qid, so `onProgress` knows which newly-hydrated qids
      // still need an explicit empty catch-up (they folded nothing — see {@link catchUpEmitted}).
      const stamp = ev.type === "batch" && this.catchUpQids?.has(qid) === true;
      if (stamp) this.catchUpEmitted?.add(qid);
      this.handler(qid, stamp ? { ...ev, catchUp: true } : ev);
    });
    // Forward the local engine's commit brackets up to the Store (cross-view-atomic notification):
    // every data delta this backend emits comes from `this.local`, so its commit boundaries are ours.
    this.local.onCommitBoundary((phase) => this.boundaryHandler(phase));
    this.colCounts = colCountsFromSchema(schema);
    this.colIndex = colIndexFromSchema(schema);
    this.pkCols = pkColsFromSchema(schema);
    this.sync = new NormalizedSync(this.pkCols, this.colCounts);
    this.specs = tableSpecsFromSchema(schema);
    this.localTables = localTableNames(schema);
    this.source = source;
    this.registry = registry;
    this.clientID = opts.clientID;
    this.user = opts.user ?? (() => "");
    this.bufferCap = opts.bufferCap ?? 1024;
    this.clock = opts.clock ?? REAL_CLOCK;
    // No policy configured ⇒ every route DERIVES (H-iii §3). With no room gate connected the
    // derivation short-circuits to "daemon", so a single-domain app is byte-for-byte as before.
    this.domainPolicy = opts.domainPolicy ?? (() => undefined);
    this.rejectedHandler = opts.onRejected ?? (() => {});
    // The reserved lmid table + (I-iii) the four lifecycle system tables join the expected set so
    // a system subscription's hello passes CRIT#4 validation. Extra CLIENT-side entries are inert
    // for every other server hello (validation only checks tables a server advertises), so a
    // client that never receives a lifecycle block is byte-identical.
    this.clientTablesBase = [...normalizedTableSchemas(schema), CLIENT_MUTATIONS_SCHEMA, ...LIFECYCLE_TABLE_SCHEMAS];
    // The daemon is the always-present channel: its gate is attached at construction, and its
    // per-source refcount space IS `this.sync` (the agg overlay reads it directly). A room
    // channel attaches through the same seam later (§5.1; Slice G).
    this.daemonGate = this.attachGate("daemon", source, this.sync);
  }

  /** Wire one authority channel into its own coherence gate (§5.1): every frame the channel
   *  delivers buffers on THIS gate's cv timeline, its progress frames release THIS buffer, its
   *  restart resets THIS gate alone, and its reserved lmid stream folds into `watermark[key]`.
   *  Validates each server hello against our OWN typed schema → reject a schema skew (CRIT#4);
   *  the reserved lmid table is part of the expected set so the system query's hello passes, and
   *  synthetic agg tables join the set as queries register them. */
  private attachGate(key: string, source: OptimisticSource, sync: NormalizedSync): SourceGate {
    const gate: SourceGate = { key, source, sync, buffer: [], nextSeq: 0, appliedCv: 0 };
    this.gates.set(key, gate);
    source.expectClientSchema?.([...this.clientTablesBase, ...this.synthetic.values()]);
    source.onNormalized((qid, ev) => this.onFrame(gate, qid, ev));
    source.onProgress((frame) => this.onGateProgress(gate, frame));
    source.onRestart?.(() => this.resetGate(gate));
    // The deopt handshake's client half (H-v §3.3): the channel's `mutationOutcome` frames arrive
    // as `(domain = gate.key, frame)`. OUT-OF-BAND — the source dispatches on arrival and this
    // handler runs immediately, NEVER behind the gate's cv buffer: a deopt must migrate its entry
    // BEFORE the buffered lmid release that would otherwise retire it as a success (and the §7.3
    // hold-back trigger, keyed on `p.domain`, would park its staged writes the wrong way).
    source.onMutationOutcome?.((frame) => this.handleMutationOutcome(gate.key, frame));
    // §7.5 rule 3 (H-v): a re-established session re-sends this DOMAIN's unconfirmed pending
    // envelopes with their ORIGINAL mids — the authority's own ledger dedups (an applied mid is
    // silent; a non-applied one is re-answered from the recorded-outcome map into the handler
    // above). This is the deopt crash-window closer: a frame lost with its socket is re-earned.
    source.onResync?.(() => this.resendPending(gate.key));
    // The lmid system query (lmid-as-data): confirmations arrive on this channel's stream,
    // cv-tagged, released by the same cvMin as the data they belong to. The server derives
    // the identity from the connection; args are advisory. Qid 0 is reserved PER CHANNEL —
    // it never collides with Store-dealt qids and never enters the sync layer.
    source.registerQuery(LMID_QID, { name: LMID_QUERY_NAME, args: {} });
    return gate;
  }

  /** Attach a SECOND authority channel (§5.1) — the seam Slice G's room upgrade calls with the
   *  ws-backed room feed. Rooms speak the daemon protocol verbatim (§2.4: the client cannot tell
   *  a room from the daemon), so the argument is a full {@link OptimisticSource} — exactly what
   *  `@rindle/remote` builds from `{roomUrl, leaseToken}`. The channel buffers/releases on its
   *  own cv timeline (an independent §5.1 gate: coherent within, eventual across) and its
   *  reserved lmid stream folds into `watermark[sourceKey]` — so `sourceKey` must equal the
   *  `domainPolicy` name for the mutations this authority confirms. The converse is NOT required:
   *  a domain may exist with no connected gate (`__testRelease` drives confirms gate-less); the
   *  live production path stays daemon-only until G calls this. */
  connectSource(sourceKey: string, source: OptimisticSource): void {
    if (this.gates.has(sourceKey)) {
      throw new Error(`optimistic backend: source ${sourceKey} is already connected`);
    }
    const gate = this.attachGate(sourceKey, source, new NormalizedSync(this.pkCols, this.colCounts));
    // A re-upgrade of a doc whose tables are still registered (a ghost that never dropped, or a
    // quick down/up bounce) adopts the surviving record as this incarnation's rename map.
    const tables = this.roomTables.get(sourceKey);
    if (tables !== undefined) gate.tableMap = tables;
    // …and CANCELS the pending swap-back: the room is the authority again, its views stay swapped,
    // and a ghost left armed would fire against this LIVE gate when the old fence clears —
    // un-swapping the views and unregistering the namespaced tables the gate's tableMap still
    // renames deltas into (the next release would then throw from serverBatchBegin and poison the
    // rebase state). A future downgrade arms a fresh ghost with its own fence.
    this.ghosts.delete(sourceKey);
  }

  /** Register the tables room `sourceKey` OWNS (its writable scope — 302 §2): each wire table
   *  gets its own namespaced ENGINE table (`{@link roomEngineTable}`), an ordinary tracked table
   *  whose sole authority is the room channel. From here on the channel's released deltas rename
   *  into these tables (wire tables outside the map are DROPPED — context stays daemon-owned,
   *  302 §6), room-domain mutators stage onto them, and a room-homed view swaps onto them once
   *  the room sub hydrates ({@link processSwapIns}). Idempotent per (sourceKey, table); a wire
   *  table unknown to the schema is skipped (nothing to hold rows for). */
  registerRoomTables(sourceKey: string, tables: readonly string[]): void {
    if (sourceKey === "daemon") {
      throw new Error("optimistic backend: the daemon is not a room — no namespaced tables");
    }
    let map = this.roomTables.get(sourceKey);
    if (!map) this.roomTables.set(sourceKey, (map = new Map()));
    for (const table of tables) {
      if (map.has(table)) continue;
      const spec = this.specs[table];
      if (spec === undefined || this.localTables.has(table)) continue;
      const engineTable = roomEngineTable(table, sourceKey);
      this.local.registerTable(engineTable, { columns: spec.columns, primaryKey: spec.primaryKey });
      map.set(table, engineTable);
    }
    const gate = this.gates.get(sourceKey);
    if (gate !== undefined) gate.tableMap = map;
  }

  /** The wire-table → engine-table map for room `sourceKey`'s owned tables (empty when none) —
   *  the client's idempotence check and `__realtimeInspect` read THIS record (one source of
   *  truth; the client keeps no shadow copy). */
  roomTablesFor(sourceKey: string): ReadonlyMap<string, string> {
    return this.roomTables.get(sourceKey) ?? EMPTY_ROOM_TABLES;
  }

  // --- the Backend seam ---------------------------------------------------------

  /** `channel` (G-iii registration-time routing) names the authority channel the remote sub
   *  registers on — a `connectSource`d gate key; default `"daemon"` (every existing caller is
   *  byte-identical). Slice G-v threads the lease's `realtime.sourceKey` here. Validated FIRST
   *  (like the E3 check below): a bad channel must throw before any per-query state is recorded. */
  registerQuery(qid: QueryId, ast: Ast, remote?: RemoteQuery, channel?: string): void {
    if (remote) this.requireGate(channel ?? "daemon");
    // queryTables is derived from the ORIGINAL ast — its `count(comments)` subquery names
    // `comment`, so an optimistic comment mutation flips this query to `unknown` (§6). The
    // local engine, by contrast, runs the REWRITTEN ast (reads the synthetic `__agg_*`).
    const tables = collectTables(ast);
    // E3/Q1 (`201-LOCAL-ONLY-TABLES-DESIGN.md`, backend chokepoint): a REMOTE (named) query may
    // never reference a local-only table — the server has no such table, and a smuggled ref would
    // either leak its existence or hit an unknown-table error upstream. A local query is nameless
    // (no `remote`) and runs entirely on the local engine, so it is exempt. This runs BEFORE we
    // record any per-query state (asts/queryTables): the throw path never reaches unregisterQuery
    // (the only cleanup), and refreshPending() iterates queryTables.keys(), so a rejected qid left
    // in those maps would be an orphaned leak processed by the pending axis.
    if (remote) {
      for (const t of tables) {
        if (this.localTables.has(t)) {
          throw new Error(
            `remote query "${remote.name}" references local-only table "${t}" — local tables never cross the wire (201-LOCAL-ONLY-TABLES-DESIGN.md E3).`,
          );
        }
      }
    }
    this.asts.set(qid, ast);
    this.queryTables.set(qid, tables);
    // A relationship `count` is DISPLAYED from a server-authoritative synthetic base table,
    // not recomputed locally (AGGREGATE-SYNC-DESIGN.md §3.3): register that table (engine +
    // refcount layer + hello validation), then drive the local engine off a rewritten AST
    // whose `count` relationships read it with a plain projected join. The remote query stays
    // un-rewritten — the server always emits the synthetic `__agg_*` rows. A count over a LOCAL
    // child is left a native reduce (L1) — `rewriteAggregates`/`ensureSyntheticTables` skip it.
    this.ensureSyntheticTables(qid, ast);
    // Local first (synchronous empty view), then the server stream hydrates it.
    this.local.registerQuery(qid, this.plainEngineAst(ast));
    if (remote) {
      // A remote query is `unknown` until its first server snapshot lands (hydration); retainRemote
      // attaches it to the sub and sets the lifecycle against the sub's hydration state.
      this.retainRemote(qid, remote, qid, channel);
    } else {
      // No server stream (a purely local AST view — or the local half of a split retain whose
      // remote attaches separately via `retainRemoteQuery`): local data is synchronous, so it is
      // `complete` with nothing to await. A later remote retain flips it back to `unknown` if it
      // attaches an un-hydrated sub.
      this.hydrated.add(qid);
      this.setResultType(qid, "complete");
    }
  }

  /** Register every synthetic aggregate table `ast` needs that we haven't seen yet: on the
   *  local engine (which auto-tracks it for the optimistic rebase loop), on `NormalizedSync`
   *  (so its rows refcount/GC by group key), and into the source's expected-schema set (so
   *  the server's `hello` — which advertises the same table — passes CRIT#4 validation).
   *  Idempotent across queries that share an aggregate definition. */
  private ensureSyntheticTables(qid: QueryId, ast: Ast): void {
    // Idempotent per qid: a re-register of the same query keeps the refcounts balanced.
    if (this.queryAggTables.has(qid)) return;
    let added = false;
    const names: string[] = [];
    // L1: a count over a LOCAL child is a native reduce with no synthetic `__agg_*` base — skip it.
    for (const t of aggTableSchemas(ast, (table) => this.localTables.has(table))) {
      names.push(t.name);
      const prev = this.syntheticRefs.get(t.name) ?? 0;
      this.syntheticRefs.set(t.name, prev + 1);
      if (prev > 0) continue; // another query already materialized this table — just refcount
      this.synthetic.set(t.name, t);
      this.local.registerTable(t.name, { columns: t.columns, primaryKey: t.primaryKey });
      this.sync.registerTable(t.name, t.primaryKey);
      added = true;
    }
    if (names.length) this.queryAggTables.set(qid, names);
    // The optimistic delta (§4) needs each aggregate's child table + group key + filter, which
    // the synthetic schema alone doesn't carry — derive the definitions from the original AST.
    this.overlay.register(collectAggDefs(ast, (t) => this.specs[t]?.columns, (t) => this.localTables.has(t)));
    if (added) this.source.expectClientSchema?.([...this.clientTablesBase, ...this.synthetic.values()]);
  }

  /** Decrement the refcount of every synthetic table query `qid` referenced; for each one that
   *  reaches 0 (no live reader left), remove it from the engine, the refcount layer, and the
   *  overlay — so aggregate state is reclaimed, not permanent (§4). Must run AFTER
   *  `local.unregisterQuery(qid)` so the engine source has no live connection when
   *  `unregisterTable` frees it (the engine refuses otherwise). */
  private releaseSyntheticTables(qid: QueryId): void {
    const names = this.queryAggTables.get(qid);
    if (!names) return;
    this.queryAggTables.delete(qid);
    let removed = false;
    for (const name of names) {
      const next = (this.syntheticRefs.get(name) ?? 1) - 1;
      if (next > 0) {
        this.syntheticRefs.set(name, next);
        continue;
      }
      this.syntheticRefs.delete(name);
      this.local.unregisterTable(name);
      this.sync.unregisterTable(name);
      this.overlay.unregister(name);
      this.synthetic.delete(name);
      removed = true;
    }
    if (removed) this.source.expectClientSchema?.([...this.clientTablesBase, ...this.synthetic.values()]);
  }

  unregisterQuery(qid: QueryId): void {
    this.roomSwappedViews.delete(qid); // a swapped view's teardown forgets its room backing
    const remoteQid = this.releaseRemote(qid);
    // GC: rows this remote footprint SOLELY referenced fall to refcount 0 → net removes. A qid
    // lives on ONE channel, so at most one gate's dropQuery is non-empty (dropQuery of an
    // unknown qid returns []) — but sweep every gate so this needs no ownership lookup.
    const gcs: [string, Mutation[]][] = [];
    if (remoteQid !== undefined) {
      this.pendingRetargetGc.delete(remoteQid); // the sweep below covers a mid-retarget teardown
      for (const gate of this.gates.values()) {
        gate.buffer = gate.buffer.filter((f) => f.qid !== remoteQid);
        const gc = mapGateDeltas(gate, gate.sync.dropQuery(remoteQid));
        if (gc.length) gcs.push([gate.key, gc]);
      }
    }
    // Tear down the local pipeline+view first so the reconcile cycle below skips it.
    this.local.unregisterQuery(qid);
    // The GC removals must leave BOTH head AND the engine's `sync` baseline. A plain
    // `local.mutate` (a HEAD-only write) leaves them in `sync`, so they look like a pending
    // optimistic REMOVE: the next release's rewind diffs head against sync+D and RESURRECTS
    // them, GC never frees anything, and a later query is served the stale/deleted row
    // forever (CRIT#2). Deliver them as a coherent SERVER delta instead — the same
    // sync-moving boundary the release gate uses — so head and sync both drop the rows,
    // against the SOURCE whose baseline held them.
    for (const [key, gc] of gcs) this.runReconcileCycle(key, gc);
    // The local pipeline is gone (no live conn) and the remote footprint's `__agg` rows were
    // GC'd above, so any synthetic table this was the last reader of can now be freed (§4).
    this.releaseSyntheticTables(qid);
    this.asts.delete(qid);
    this.queryTables.delete(qid);
    this.resultTypes.delete(qid);
    this.hydrated.delete(qid);
    this.pendingState.delete(qid); // §7.2 cache, keyed by the local materialized qid (a monotonic
    // Store counter — re-materialize gets a fresh id, never this one again), so drop it on teardown.
  }

  /** `channel` as in {@link registerQuery} (G-iii): the gate the remote sub registers on; default
   *  `"daemon"`. This is the split-retain seam G-v's resolve-then-register drives — resolve the
   *  lease, learn `realtime.sourceKey`, `connectSource` it, then retain the query on that channel.
   *  Validated FIRST so a bad channel throws before any synthetic-table refcount moves. */
  retainRemoteQuery(qid: QueryId, remote: RemoteQuery, localQueryId?: QueryId, ast?: Ast, channel?: string): void {
    this.requireGate(channel ?? "daemon");
    if (ast) this.ensureSyntheticTables(qid, ast);
    this.retainRemote(qid, remote, localQueryId, channel);
  }

  releaseRemoteQuery(qid: QueryId): void {
    const remoteQid = this.releaseRemote(qid);
    this.releaseSyntheticTables(qid);
    if (!this.queryTables.has(qid)) {
      this.resultTypes.delete(qid);
      this.hydrated.delete(qid);
    }
    if (remoteQid === undefined) return;
    // A mid-retarget release: the every-gate sweep below IS the deferred old-channel GC
    // (dropQuery hits the old gate's sync too), so retire the pending record — and its
    // wrong-channel grace — with it.
    this.pendingRetargetGc.delete(remoteQid);
    // Per-gate sweep, like `unregisterQuery`: at most one gate owned this qid's frames/rows.
    for (const gate of this.gates.values()) {
      gate.buffer = gate.buffer.filter((f) => f.qid !== remoteQid);
      const gc = mapGateDeltas(gate, gate.sync.dropQuery(remoteQid));
      if (gc.length) this.runReconcileCycle(gate.key, gc);
    }
  }

  /** The Slice I-iv upgrade retarget (§4.1 "Retarget" / the doorbell reaction): move a LIVE
   *  (name, args) sub — every retain of it and every local view it feeds, wholesale — from the
   *  channel it lives on onto `sourceKey`'s (already-`connectSource`d, already-promoted) room
   *  channel, WITHOUT the view ever dropping its rows. Returns the sub's wire `sourceQid` (the
   *  identity the client's renewal loop re-subscribes with).
   *
   *  Why a dedicated primitive: the one-channel-per-(name,args) invariant ({@link retainRemote}'s
   *  loud throw) is correct — a sub's frames must never split across two cv timelines — so the
   *  upgrade cannot simply retain a second sub on the room and release the daemon one; and the
   *  naive release-then-retain order GCs the daemon sync's rows synchronously (net removes emit,
   *  the view flashes empty) a full ws round trip before the room's seq-0 snapshot refills it.
   *  The cutover is therefore TWO-PHASE around the room's first release:
   *
   *   1. NOW (here): unsubscribe the old channel's wire sub, sweep its still-buffered frames for
   *      this qid (their cv timeline continues without the sub — the hello-supersession
   *      precedent), flip `sub.channel`, re-arm `sub.hydrated` (the room's own snapshot is the
   *      cutover point), and register on the room source (its resolver presents the handed
   *      roomToken). The old gate's SYNC rows are deliberately NOT dropped: they keep the view's
   *      plain tables populated through the window — the view still reads them until the swap.
   *   2. AT THE ROOM'S FIRST RELEASED SNAPSHOT: the reconcile folds the snapshot into the room's
   *      namespaced tables, the release tail SWAPS every local view onto them (302 §4.1,
   *      {@link processSwapIns} — the accepted-flash boundary), and {@link flushRetargetGc}'s
   *      deferred `dropQuery`+reconcile on the OLD gate then GCs the plain-table rows the sub
   *      alone referenced — invisible to the swapped views.
   *
   *  Idempotent per target channel: a sub already on `sourceKey` returns immediately (the
   *  double-doorbell / re-entrancy guard — one retarget per (query, sourceKey)). Validates before
   *  mutating: a throw here leaves the sub fully daemon-attached (the client's fail-open). */
  retargetRemoteQuery(remote: RemoteQuery, sourceKey: string): QueryId {
    const newGate = this.requireGate(sourceKey); // throw loudly BEFORE any sub state moves
    const key = remoteKey(remote);
    const sub = this.remoteSubs.get(key);
    if (!sub) {
      throw new Error(
        `optimistic backend: no live sub for query "${remote.name}" — nothing to retarget`,
      );
    }
    if (sub.channel === sourceKey) return sub.sourceQid; // already there — idempotent
    const oldGate = this.gates.get(sub.channel) ?? this.daemonGate;
    oldGate.source.unregisterQuery(sub.sourceQid);
    oldGate.buffer = oldGate.buffer.filter((f) => f.qid !== sub.sourceQid);
    this.pendingRetargetGc.set(sub.sourceQid, sub.channel);
    sub.channel = sourceKey;
    sub.hydrated = false;
    newGate.source.registerQuery(sub.sourceQid, remote);
    return sub.sourceQid;
  }

  /** Phase 2 of {@link retargetRemoteQuery}, run at the end of every gate release: once a
   *  retargeted sub's first snapshot has RELEASED on its new channel (`sub.hydrated` re-armed at
   *  retarget, re-set by {@link markSubHydrated} inside this very release), drop the qid's rows
   *  from the OLD gate's sync and reconcile them out — after the room's rows are already applied,
   *  so the winner flip is value-equal (net-zero; see the phase table above). A sub torn down
   *  mid-window was already swept by `releaseRemoteQuery`/`unregisterQuery` (which delete the
   *  record); a vanished record here is pruned defensively. */
  private flushRetargetGc(gate: SourceGate): void {
    if (this.pendingRetargetGc.size === 0) return; // every non-upgrade release: structural no-op
    for (const [sourceQid, oldGateKey] of this.pendingRetargetGc) {
      const key = this.sourceToRemote.get(sourceQid);
      const sub = key !== undefined ? this.remoteSubs.get(key) : undefined;
      if (!sub) {
        this.pendingRetargetGc.delete(sourceQid);
        continue;
      }
      if (sub.channel !== gate.key || !sub.hydrated) continue; // not this gate / not yet cut over
      this.pendingRetargetGc.delete(sourceQid);
      const oldGate = this.gates.get(oldGateKey);
      if (!oldGate) continue;
      const gc = mapGateDeltas(oldGate, oldGate.sync.dropQuery(sourceQid));
      if (gc.length) this.runReconcileCycle(oldGateKey, gc);
    }
  }

  // --- the §4.2 downgrade: demote → ghost → fence → drop (Slice I-v) ----------------------

  /** The I-v downgrade orchestration primitive (§4.2/§7.4, re-expressed by 302 §4.2 as the
   *  SWAP-BACK GATE): retire room `sourceKey` behind the watermark fence. The caller has ALREADY
   *  retargeted every live sub off the channel ({@link retargetRemoteQuery} room→daemon —
   *  validated loudly below) and holds the fence from the api-server's downgrade response
   *  (`finalFlushSeq` = the room's last COMMITTED flush seq; `doc` keys the §4.2 watermark fold,
   *  {@link roomWatermarks}). Steps, in order:
   *
   *   1. **Disconnect** the channel ({@link disconnectSource}): handlers detached, gate + buffer
   *      dropped. `nextMid`/`watermark`/processed-outcomes for the domain are KEPT FOREVER (§7.1:
   *      an assigned mid pins its domain; a later re-upgrade of the same doc continues the
   *      sequence — {@link connectSource} attaches a fresh gate and the lmid snapshot max-folds
   *      into the surviving watermark). Disconnecting BEFORE the daemon sub's first release is
   *      load-bearing: it makes {@link flushRetargetGc}'s deferred old-channel GC a no-op (gate
   *      gone ⇒ record deleted, nothing dropped). The room's namespaced tables — and the views
   *      swapped onto them — deliberately stay: frozen at the room's last state, they keep the
   *      document visible while the falling-back follower may still lack the final flush.
   *      Swapping back earlier would show its pre-flush images — the regression §4.2 prevents.
   *   2. **Ghost + first evaluation**: the record joins {@link ghosts} and is evaluated once
   *      immediately — `finalFlushSeq === 0` (a never-flushed room) with no room-domain pending
   *      drops on the spot, the single-daemon first-frame case.
   *
   *  In-flight discipline (§7.5): entries with `mid !== null` on `sourceKey` stay PINNED (rule
   *  2 — never re-route a sent mutation); their resolution arrives via the daemon-carried
   *  ledger+outcome folds (I-iii) and blocks the drop until then. Idempotent per sourceKey (a
   *  second labeled query sharing the room demotes into the existing ghost). */
  demoteRoomSource(sourceKey: string, doc: string, finalFlushSeq: number): void {
    if (sourceKey === "daemon") {
      throw new Error("optimistic backend: the daemon source cannot be demoted");
    }
    // Validate FIRST (nothing mutated yet): a live sub still on the channel would silently
    // starve once the gate detaches — the caller must retarget every sub off the room first.
    for (const sub of this.remoteSubs.values()) {
      if (sub.channel === sourceKey) {
        throw new Error(
          `optimistic backend: cannot demote ${JSON.stringify(sourceKey)} — query "${sub.remote.name}" is still retained on it (retarget it to the daemon first)`,
        );
      }
    }
    // Idempotent per sourceKey (co-tenant queries sharing the room demote into the existing
    // ghost) — but NEVER a bare early-return: each demote carries its own fence, so keep the
    // NEWEST flush (monotone max — swapping back on an older fence would show pre-flush images),
    // and disconnect defensively in case a gate re-attached since the ghost was armed (a
    // down→up→down bounce; {@link connectSource} cancels the ghost on re-upgrade, so this arm
    // normally finds no gate — but a stale gate left connected would let the next daemon release
    // GC the room slice out from under the still-swapped views, the §4.2 regression).
    const existing = this.ghosts.get(sourceKey);
    if (existing) {
      this.disconnectSource(sourceKey);
      existing.finalFlushSeq = Math.max(existing.finalFlushSeq, finalFlushSeq);
      this.evaluateGhosts();
      return;
    }
    this.disconnectSource(sourceKey); // (1) the channel
    this.ghosts.set(sourceKey, { doc, finalFlushSeq, stuckReported: false }); // (2)
    this.evaluateGhosts();
  }

  /** Detach one connected room channel (Slice I-v step 3): the source's handlers are replaced
   *  with no-ops (the {@link OptimisticSource} handler seam is single-registration, so this IS
   *  the detach — a late frame from a dying socket can no longer touch any bookkeeping), its
   *  reserved lmid sub is unregistered, and the gate — buffer, per-source sync, cv watermark —
   *  is dropped from {@link gates}. The DOMAIN state deliberately survives forever:
   *  `nextMid[sourceKey]`, `watermark[sourceKey]`, and the processed-outcome set are untouched
   *  (§7.1 — an assigned mid pins its domain; a re-upgrade must continue, never restart, the mid
   *  sequence; {@link connectSource} then attaches a fresh gate whose lmid snapshot max-folds
   *  into the surviving watermark via {@link foldConfirm}). Closing the underlying transport is
   *  the caller's job. Idempotent (a missing gate is a no-op). */
  disconnectSource(sourceKey: string): void {
    if (sourceKey === "daemon") {
      throw new Error("optimistic backend: the daemon source cannot be disconnected");
    }
    const gate = this.gates.get(sourceKey);
    if (!gate) return;
    this.gates.delete(sourceKey);
    gate.source.onNormalized(() => {});
    gate.source.onProgress(() => {});
    gate.source.onRestart?.(() => {});
    gate.source.onMutationOutcome?.(() => {});
    gate.source.onResync?.(() => {});
    gate.source.unregisterQuery(LMID_QID);
  }

  /** Register the I-v stuck-downgrade sink — see {@link DowngradeStuckEvent}. One handler (a
   *  later registration replaces it, the {@link onScopeSessions} convention); client.ts maps it
   *  onto the loud anomaly surface. */
  onDowngradeStuck(handler: (event: DowngradeStuckEvent) => void): void {
    this.downgradeStuckHandler = handler;
  }

  /** The I-v ghost-drop watcher (§4.2), run after every applied release ({@link applyRelease} —
   *  the seam where {@link roomWatermarks} has just folded and the confirm-drop has just run) and
   *  once at demote time. For each ghost: the fence must be satisfied
   *  (`roomWatermarks[doc] ≥ finalFlushSeq`; 0 is trivially satisfied) AND no SENT room-domain
   *  pending may remain (§7.5 — such entries resolve only through the daemon-carried
   *  outcome/ledger folds; an entry that never reached the room is undecidable, so the ghost
   *  HOLDS and the stuck event fires exactly once, naming the mids). Both satisfied ⇒
   *  {@link dropGhost}. */
  private evaluateGhosts(): void {
    if (this.ghosts.size === 0) return; // every non-downgrade release: structural no-op
    for (const [sourceKey, ghost] of [...this.ghosts]) {
      // A LIVE gate means the doc re-upgraded — dropping now would dismantle the live room
      // (un-swap its views, unregister the tables its tableMap renames into). connectSource
      // cancels the ghost on re-upgrade, so this guard is purely defensive; hold, never drop.
      if (this.gates.has(sourceKey)) continue;
      if ((this.roomWatermarks.get(ghost.doc) ?? 0) < ghost.finalFlushSeq) continue; // fence holds
      const stuck = this.pendingMutations.filter((p) => p.domain === sourceKey && p.mid !== null);
      if (stuck.length > 0) {
        if (!ghost.stuckReported) {
          ghost.stuckReported = true;
          this.downgradeStuckHandler({ sourceKey, doc: ghost.doc, mids: stuck.map((p) => p.mid as number) });
        }
        continue; // hold — never a timeout-retire (§7.5 rule 2)
      }
      this.dropGhost(sourceKey);
    }
  }

  /** Drop one cleared ghost — the 302 §4.2 SWAP-BACK: under the fence the daemon tables are
   *  value-equal-or-ahead of the room's final state, so (1) every view swapped onto the room's
   *  namespaced tables re-registers on its ORIGINAL (daemon-table) AST — visually a no-op, the
   *  Store folds the re-hello as an in-place reset; (2) the namespaced tables unregister (no
   *  reader is left after the swap); (3) ONE daemon reconcile re-invokes the pending set so any
   *  entry whose writes had staged onto the now-gone room tables re-stages onto the daemon tables
   *  (its domain policy stopped naming the dead room when the client dropped it). The whole drop
   *  runs under one commit boundary so the swap and the re-staged predictions notify as ONE step.
   *  After this, a FUTURE upgrade of the same doc registers again from scratch. */
  private dropGhost(sourceKey: string): void {
    this.ghosts.delete(sourceKey);
    this.inOneCommit(() => {
      for (const [qid, key] of [...this.roomSwappedViews]) {
        if (key !== sourceKey) continue;
        this.roomSwappedViews.delete(qid);
        const ast = this.asts.get(qid);
        if (ast === undefined) continue;
        this.local.unregisterQuery(qid);
        this.local.registerQuery(qid, this.plainEngineAst(ast));
      }
      this.unregisterRoomTables(sourceKey);
      // One daemon reconcile re-stages the pending set onto the surviving tables. Run whenever
      // any pending exists: unregistering the room tables took their staged copies with the tree.
      if (this.pendingMutations.length > 0) this.runReconcileCycle("daemon", []);
    });
    this.refreshPending(); // the reconcile may have dropped a throwing re-invocation
  }

  /** Unregister room `sourceKey`'s namespaced engine tables and drop the {@link roomTables}
   *  record. Callers must have no view registered on them (the engine refuses otherwise —
   *  loud by design). No-op for an unknown sourceKey. */
  unregisterRoomTables(sourceKey: string): void {
    const map = this.roomTables.get(sourceKey);
    if (!map) return;
    this.roomTables.delete(sourceKey);
    for (const engineTable of map.values()) this.local.unregisterTable(engineTable);
  }

  // --- the §4 lifecycle SYSTEM-STREAM retains (Slice I-iii) ------------------------------

  /** Retain one minted SYSTEM subscription (RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md §4, Slice
   *  I-iii): a wire sub with NO store view and NO user-visible table. Registered through the same
   *  {@link RemoteSub} bookkeeping as any remote retain — so qid→channel ownership, the overflow
   *  re-subscribe, and refcounted release all work unchanged — but with an EMPTY `localQids` set
   *  (no hydration/resultType coupling) and a {@link systemQids} record telling the release path
   *  which system table this qid's frames carry (`spec.table`) and which scope/doc it was minted
   *  for (the fold's row filter). Its frames then buffer on the channel's gate exactly like
   *  {@link LMID_QID}'s and fold at RELEASE time in {@link foldSystemFrames} — riding the SAME
   *  buffered cv path as the data they co-committed with (fence coherence: an out-of-band
   *  shortcut would break I-ii's co-commit ordering guarantee).
   *
   *  `channel` defaults to `"daemon"` — the system tables live in the DAEMON store (that is the
   *  point: outcome/ledger/watermark rows must be readable with no room socket alive, §7.1
   *  "load-bearing for §7.5"). Idempotence per (table, scope/doc) is the CALLER's job (client.ts
   *  keys its retains on exactly that); a duplicate retain of the SAME remote identity refcounts
   *  like any sub. */
  retainSystemQuery(retainQid: QueryId, remote: RemoteQuery, spec: SystemStreamSpec, channel = "daemon"): void {
    const gate = this.requireGate(channel); // throw loudly BEFORE any sub state moves
    const key = remoteKey(remote);
    let sub = this.remoteSubs.get(key);
    if (sub) {
      if (sub.channel !== channel) {
        throw new Error(
          `optimistic backend: system query "${remote.name}" is already retained on channel ${JSON.stringify(sub.channel)} — cannot retain it on ${JSON.stringify(channel)}`,
        );
      }
      sub.refCount++;
      this.localToRemote.set(retainQid, key);
      this.remoteRetainToLocal.set(retainQid, undefined);
      return;
    }
    // A fresh sub: deliberately NOT `retainRemote` — its `localQueryId` default would couple this
    // retain's qid to the view-hydration machinery (`hydrated`/`resultType`), and a system stream
    // has no view to hydrate.
    sub = { sourceQid: retainQid, remote, refCount: 1, localQids: new Map(), hydrated: false, channel };
    this.remoteSubs.set(key, sub);
    this.sourceToRemote.set(retainQid, key);
    this.localToRemote.set(retainQid, key);
    this.remoteRetainToLocal.set(retainQid, undefined);
    this.systemQids.set(retainQid, { ...spec });
    gate.source.registerQuery(retainQid, remote);
  }

  /** Release a {@link retainSystemQuery} retain. Refcounted like any sub; the LAST release
   *  unregisters from the owning channel, sweeps its buffered frames, and drops the
   *  {@link systemQids} record. The folded lifecycle STATE (`roomWatermarks`/`scopeSessions`/
   *  processed outcomes) deliberately survives — the fence is monotone truth about the store, not
   *  about the subscription (a re-retained fence must not forget a cleared watermark). */
  releaseSystemQuery(retainQid: QueryId): void {
    const remoteQid = this.releaseRemote(retainQid);
    if (remoteQid === undefined) return; // still refcounted (or unknown)
    for (const gate of this.gates.values()) {
      gate.buffer = gate.buffer.filter((f) => f.qid !== remoteQid);
    }
    this.systemQids.delete(remoteQid);
  }

  /** Raw CRUD has no optimistic story (§9 replaces it with named mutators). Register a
   *  mutator — even a trivial one — and `invoke` it. */
  mutate(_mutations: Mutation[]): Promise<void> {
    return Promise.reject(
      new Error("optimistic backend: writes go through named mutators — use invoke(name, args)"),
    );
  }

  /** Direct-commit a batch of LOCAL-only writes (`201-LOCAL-ONLY-TABLES-DESIGN.md` §6 / M2):
   *  straight to the local engine, OUTSIDE the optimistic pending stack — a local table is
   *  untracked, so it never rebases, reverts, or waits on a confirmation. Rejects a synced/tracked
   *  table (the local engine's `writeLocal` is the chokepoint). Reconcile is synchronous and
   *  non-reentrant (A5), so a local write can never interleave with an open server cycle.
   *
   *  Fires the {@link onLocalWrite} observer AFTER the engine commit but BEFORE subscriber
   *  delivery — i.e. for exactly the batches that passed the M2 guard and committed (the
   *  persistence layer's write-through tap, `207-LOCAL-TABLE-PERSISTENCE-DESIGN.md` §5.1). A
   *  subscriber throwing during delivery re-raises out of this call, but only after the tap has
   *  seen the batch: a committed write can never be invisible to the persistence plane. */
  writeLocal(mutations: Mutation[]): void {
    this.local.writeLocal(mutations, () => this.localWriteObserver?.(mutations));
  }

  /** The write-through tap for the local-persistence layer (207 §5.1): `observer` sees every
   *  {@link writeLocal} batch post-commit. One observer (the layer); a later registration
   *  replaces it. The observer must not throw — a persistence failure degrades durability, never
   *  the write path (P9); the layer catches internally. */
  onLocalWrite(observer: (mutations: Mutation[]) => void): void {
    this.localWriteObserver = observer;
  }

  /** Apply a REPLICATED local batch (a restore snapshot / a leader `commit` — 207 §5.1): delegates
   *  to the engine's `writeLocal`, so the M2 locality guard still fires (P8 — a corrupt record
   *  naming a synced table dies loudly here), but does NOT invoke the {@link onLocalWrite}
   *  observer — the echo guard is structural, so the persistence layer can never re-enter itself.
   *  `onCommitted` fires post-commit pre-delivery (same anchor as {@link writeLocal}'s tap): the
   *  layer updates its mirror there, so a subscriber throw can never desync mirror from engine. */
  applyLocalReplica(mutations: Mutation[], onCommitted?: () => void): void {
    this.local.writeLocal(mutations, onCommitted);
  }

  onEvent(handler: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = handler;
  }

  onCommitBoundary(handler: (phase: "begin" | "end") => void): void {
    this.boundaryHandler = handler;
  }

  // --- the named-mutator entry (§9) ----------------------------------------------

  /** Bracket a multi-step optimistic apply as ONE notification commit (cross-view-atomic
   *  notification — {@link Backend.onCommitBoundary}). `invoke`/`invokeFolded` apply the prediction
   *  and then reconcile the `__agg` head in TWO `this.local` commits, but they are two halves of one
   *  logical mutation: a relationship-`count` view and the data view it counts must update together.
   *  The Store's `commitDepth` is a counter, so each inner commit's own `begin`/`end` nests under this
   *  outer pair and the Store flushes every affected view (data AND count) once, together, at the
   *  outer `end` — a subscriber re-reading a sibling view then sees post-commit data, never a torn
   *  half. Balanced on throw (the prediction mutator may reject) via the `finally`, so a thrown
   *  prediction never wedges the Store in deferred mode. */
  private inOneCommit<T>(apply: () => T): T {
    this.boundaryHandler("begin");
    try {
      return apply();
    } finally {
      this.boundaryHandler("end");
    }
  }

  /** Run one client mutator against the staged `tx`, accepting BOTH forms (§ shared mutators):
   *  a plain sync function runs as-is; a shared GENERATOR is driven synchronously — every yielded
   *  write applies to the wasm txn now, every `tx.row` read is resolved against the same staged
   *  state (read-your-writes), the SAME body the API server drives asynchronously. `ctx.user` is
   *  the acting principal (re-read per invoke, stable across a rebase re-invoke). */
  private runMutator(mutator: ClientMutator, tx: MutationTx, args: unknown): void {
    if (isGeneratorMutator(mutator)) {
      const gen = (mutator as (t: IsoTx, a: never, c: MutatorCtx) => MutationGen)(
        isoTx,
        args as never,
        { user: this.user() },
      );
      driveMutationSync(gen, {
        apply: (op) => applyOpToTx(tx, op),
        read: (table, pk) => tx.row(table, pk),
        query: (q) => tx.query(q),
      });
    } else {
      (mutator as (t: MutationTx, a: never) => void)(tx, args as never);
    }
  }

  /** Deal the next wire mid from `domain`'s ledger (RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md
   *  §7.1) and advance that counter. A domain absent from the map starts at 1. Per-domain, so a
   *  client writing through room + daemon concurrently keeps two gapless, non-aliasing sequences.
   *  The client-global `seq` is stamped in the same breath — the ONE cross-domain total order
   *  (confirmation order is per-domain; replay order is client-global). Bundled here so no call
   *  site can deal a mid without its seq. ONE caller discards the seq deliberately: the H-v deopt
   *  flip ({@link handleMutationOutcome}) keeps the entry's ORIGINAL seq — its replay position —
   *  and takes only the fresh mid (the dealSeq bump is harmless: seq consumers order, never
   *  count). */
  private dealMid(domain: string): { mid: number; seq: number } {
    const mid = this.nextMid.get(domain) ?? 1;
    this.nextMid.set(domain, mid + 1);
    return { mid, seq: ++this.dealSeq };
  }

  // --- the DECLARED router (302 §5: declared, not derived) --------------------------------
  //
  // The user declares which mutators are room mutators; the client neither proves, derives,
  // widens, nor falls back. The declaration reaches this backend as `domainPolicy` — the client
  // layer resolves (mutator name, args) against its declared realtime mutators and the currently
  // attached rooms. A misdeclaration fails SOFT (302 §5.1): a daemon-declared mutator touching
  // room-visible data stages onto the daemon tables while the room-homed view reads the room
  // tables — no optimistic feedback until the echo relays it a hop later, never a divergence.
  // The room GATE stays the authoritative backstop: a room-routed mutation the room refuses comes
  // back as a `mutationOutcome` deopt/reject frame and the H-v machinery below re-enqueues or
  // surfaces it.

  /** The declared confirming stream for one invocation: the `domainPolicy`'s verdict, `"daemon"`
   *  when it abstains. Resolved BEFORE the prediction runs — the domain picks the staging map
   *  (a room domain stages its owned tables onto the room's namespaced twins). */
  private resolveDomain(name: string, args: unknown): string {
    return this.domainPolicy(name, args) ?? "daemon";
  }

  /** The staging table map for a `domain`-routed prediction ({@link trackingTx}'s `stage`):
   *  wire table → the room's namespaced engine table for the tables the room owns; identity for
   *  everything else (including the whole map for the daemon domain). */
  private stagingMap(domain: string): ReadonlyMap<string, string> | undefined {
    return domain === "daemon" ? undefined : this.roomTables.get(domain);
  }

  /** The PLAIN (daemon-homed) engine AST for `ast` — aggregate relationships rewritten to their
   *  synthetic `__agg_*` reads, no room renames. The ONE form every non-swapped engine
   *  registration uses ({@link registerQuery}, {@link dropGhost}'s swap-back) and the base the
   *  swap-in renames ({@link processSwapIns}). */
  private plainEngineAst(ast: Ast): Ast {
    return rewriteAggregates(ast, (t) => this.localTables.has(t));
  }

  /** Mutator names the cross-authority warn below already fired for (once per name). */
  private readonly warnedCrossAuthority = new Set<string>();

  /** 302 §5.1 dev-time guard: a room-DECLARED mutator wrote tables the room does not own. Those
   *  writes staged onto the PLAIN daemon tables (the staging map covers only owned tables), but
   *  the entry confirms on the ROOM stream — and only the room's OWNED tables flush back to the
   *  daemon, so nothing upstream ever echoes them: once the room confirm retires the entry, the
   *  next release's whole-store rewind reverts them for good. The first-party room shell refuses
   *  such a mutation (the §3.3 deopt/reject backstop re-routes it to the daemon), so this warns
   *  for the shapes where that backstop may be absent (a BYO relay) — loud, once, soft (§5.1:
   *  misdeclarations never throw). */
  private warnCrossAuthorityWrites(name: string, domain: string, touched: ReadonlySet<string>): void {
    if (domain === "daemon" || this.warnedCrossAuthority.has(name)) return;
    const map = this.roomTables.get(domain);
    const staged = new Set(map?.values() ?? []);
    const outside = [...touched].filter((t) => !staged.has(t));
    if (outside.length === 0) return;
    this.warnedCrossAuthority.add(name);
    console.warn(
      `[rindle] room mutator "${name}" wrote table(s) ${outside.join(", ")} that room ${JSON.stringify(domain)} does not own` +
        ` (owned: ${map !== undefined && map.size > 0 ? [...map.keys()].join(", ") : "none"}) — these writes rely on the room` +
        ` shell's deopt backstop and revert after the room confirm if the shell applies the mutation anyway (302 §5.1).`,
    );
  }

  /** Run the named client mutator optimistically: the prediction applies to the live
   *  engine now (affected views update synchronously), `(mid, name, args)` joins the
   *  pending stack, and the envelope ships upstream. Returns the assigned `mid`. */
  invoke(name: string, args: unknown): number {
    return this.invokeWith(name, args);
  }

  /** {@link invoke} with an optional PINNED confirming domain (H-v): the deopt handshake's
   *  already-retired arm re-invokes the frame's echoed `(name, args)` as a FRESH invocation pinned
   *  to `"daemon"` — an honest re-prediction on the current base, never derived (`pin` bypasses
   *  {@link resolveDomain} entirely, so the router never runs and no Q6 counter moves). Every
   *  other step is `invoke` verbatim: prediction now, capture, drainOverlapping, mid dealt from
   *  the pinned domain's ledger, envelope on its channel. */
  private invokeWith(name: string, args: unknown, pin?: string): number {
    const mutator = this.registry[name];
    if (!mutator) throw new Error(`unknown client mutator: ${name}`);
    // One commit boundary spans the prediction AND the `__agg`-head reconcile below, so their views
    // (data + count) flush together rather than tearing across two engine commits.
    return this.inOneCommit(() => {
      // The confirming stream is DECLARED (302 §5), so it resolves BEFORE the prediction: the
      // domain picks the staging map — a room-domain mutator's writes to the room's owned tables
      // land on the namespaced engine twins the room-homed views read. An H-v deopt re-invocation
      // pins via `pin` and the policy never runs.
      const domain = pin ?? this.resolveDomain(name, args);
      // Apply the prediction. If the mutator throws (client-side validation, a bad read),
      // the staged write is discarded (the wasm txn is a clean no-op until commit) and the throw
      // propagates with NO mid consumed — a burnt mid is a permanent server-side gap that
      // silently refuses every later mutation from this client (#10).
      const writes: WriteSet = new Map();
      const reads: ReadLog = { reads: [], queries: [] };
      const ops: ChildOp[] = [];
      this.local.writeWith((tx) => {
        this.runMutator(
          mutator,
          trackingTx(tx, writes, this.specs, this.localTables, this.opCollector(ops), false, reads, this.stagingMap(domain)),
          args,
        );
      });
      // `touched` is DERIVED, never separately populated (§3.2 #1) — see {@link WriteSet}.
      const touched = new Set(writes.keys());
      this.warnCrossAuthorityWrites(name, domain, touched);
      // Flush-on-enqueue (§4.2): a fold whose tables overlap this write must take its mid NOW, BEFORE
      // this write does, so wire order == local-apply order for any pair that can observe each other
      // (a read-dependent write reading a folded cell sees the same value optimistically and on the
      // wire — no snap). Drained folds ship with smaller mids; this write's mid is dealt after.
      this.drainOverlapping(touched);
      // The confirming stream's ledger deals the mid and its watermark alone retires the entry
      // (§7.1). An assigned mid pins its domain forever — a re-invocation never re-routes.
      const { mid, seq } = this.dealMid(domain);
      this.pendingMutations.push({ mid, seq, name, args, domain, touched, writes, reads });
      // The prediction stuck — fold its child ops into the optimistic agg delta and push it onto
      // the `__agg` head rows (§4). No reset here (this is the §1.3 trivial case, no rewind): the
      // delta accumulates on top of the prior pending set, and `reconcileAggHead` recomputes each
      // touched group's head as the absolute `server_base ⊕ delta`.
      for (const op of ops) this.overlay.observe(op);
      this.reconcileAggHead();
      this.refreshPending(); // §7.2: this write now touches its queries' pending axis (NOT ResultType).
      void this.channelFor(domain).pushMutation({ clientID: this.clientID, mid, name, args });
      return mid;
    });
  }

  /** Run a FOLDED invoke (FOLDED-MUTATIONS-DESIGN §8): apply the prediction to the live engine now
   *  (like `invoke`), but collapse a run of same-key invokes into ONE pending entry whose `args`
   *  are overwritten in place, debounce the server write, and ship only the last value. The `mid`
   *  is assigned at flush, not here (§4.1) — so the return is a {@link FoldHandle}, not a mid. */
  invokeFolded(name: string, opts: FoldOptions, args: unknown): FoldHandle {
    const mutator = this.registry[name];
    if (!mutator) throw new Error(`unknown client mutator: ${name}`);
    const foldKey = `${name}\0${stableJson(opts.key)}`;
    // One commit boundary spans the prediction AND the `__agg`-head reconcile (see {@link inOneCommit}),
    // so a folded mutation's list view and count view flush together, never torn across two commits.
    // The declared domain (302 §5) — resolved up front, like `invoke`'s: it picks the staging
    // map, the §9.3 cadence, and the provisional confirming stream (the flush re-resolves).
    const domain = this.resolveDomain(name, args);
    return this.inOneCommit(() => {
      // Apply the prediction with the read trap armed (§5): a folded mutator that reads state to
      // compute its write is non-absorbing and refused. A throw discards the staged write (clean
      // no-op) and consumes no mid — exactly `invoke`'s guarantee. NO `readLog` here — the trap
      // path stays byte-for-byte as it was; recording (§3.2 #2) never arms alongside the trap.
      const writes: WriteSet = new Map();
      const ops: ChildOp[] = [];
      try {
        this.local.writeWith((tx) => {
          this.runMutator(mutator, trackingTx(tx, writes, this.specs, this.localTables, this.opCollector(ops), true, undefined, this.stagingMap(domain)), args);
        });
      } catch (e) {
        if (e instanceof FoldReadError) {
          throw new Error(
            `cannot fold "${name}": it reads state via tx.get/tx.row, so it is not absorbing — folded mutators must be last-writer-wins (FOLDED-MUTATIONS-DESIGN §5)`,
          );
        }
        throw e;
      }
      for (const op of ops) this.overlay.observe(op);
      this.reconcileAggHead();

      // `touched` is DERIVED, never separately populated (§3.2 #1) — see {@link WriteSet}.
      const touched = new Set(writes.keys());
      this.warnCrossAuthorityWrites(name, domain, touched);
      const now = this.clock.now();
      let f = this.folds.get(foldKey);
      if (f) {
        // Overwrite the single entry in place — the pending stack does NOT grow (§1 #2). The head
        // already carries this new prediction (absorbing, last-wins on the cell); the entry holds
        // only the LATEST args, which is what a rebase re-derives from and what the flush ships.
        // `domain` too: THIS invocation staged through the freshly-resolved domain's map above, so
        // a mid-window rebase must re-stage through the same one (the flush re-resolves anyway;
        // no mid is pinned yet — `entry.mid` is null until flush).
        f.entry.args = args;
        f.entry.touched = touched;
        f.entry.writes = writes;
        f.entry.domain = domain;
        f.args = args;
        this.clock.clearTimeout(f.timer);
      } else {
        // §9.3: pick the window's cadence. Routing into a room ⇒ flush at roomDebounceMs so
        // intermediates stream to the shared head; off the room, the caller's collapse debounce
        // governs.
        const inRoom = opts.roomDebounceMs !== undefined && domain !== "daemon";
        const debounceMs = inRoom ? opts.roomDebounceMs! : opts.debounceMs ?? DEFAULT_FOLD_DEBOUNCE_MS;
        const maxWaitMs = inRoom ? opts.roomDebounceMs! : opts.maxWaitMs;
        const entry: PendingMutation = { mid: null, seq: null, name, args, domain, touched, writes, reads: { reads: [], queries: [] } };
        this.pendingMutations.push(entry);
        let resolveMid!: (mid: number) => void;
        const midPromise = new Promise<number>((res) => (resolveMid = res));
        f = {
          entry,
          args,
          timer: undefined,
          firstAt: now,
          debounceMs,
          maxWaitMs,
          deferAcrossWrites: opts.deferAcrossWrites ?? false,
          midPromise,
          resolveMid,
        };
        this.folds.set(foldKey, f);
      }
      // (Re)arm the trailing debounce — unless the maxWaitMs cap is already due (a never-idle drag
      // still persists periodically, §3/#4), in which case flush now instead of re-arming.
      if (f.maxWaitMs !== undefined && now - f.firstAt >= f.maxWaitMs) {
        this.flushFold(foldKey);
      } else {
        f.timer = this.clock.setTimeout(() => this.flushFold(foldKey), f.debounceMs);
      }
      this.refreshPending();
      const handle: FoldHandle = { flush: () => this.flushFold(foldKey), mid: f.midPromise };
      return handle;
    });
  }

  /** Flush-on-enqueue (§4.2): for each outstanding fold whose touched tables overlap `tables`,
   *  assign its mid NOW and ship it — in creation (insertion) order, so the wire stays gapless. A
   *  `deferAcrossWrites` fold opts out (it keeps deferring, accepting the read-dependent snap). The
   *  incoming write's own fold key (if any) is skipped — it is being folded into, not flushed. */
  private drainOverlapping(tables: Set<string>, exceptKey?: string): void {
    // Snapshot the entries first: `flushFold` mutates `this.folds` mid-iteration.
    for (const [key, f] of [...this.folds]) {
      if (key === exceptKey || f.deferAcrossWrites) continue;
      if (intersects(f.entry.touched, tables)) this.flushFold(key);
    }
  }

  /** Flush one fold (§8): deal its `mid` from `nextMid` (SEND order — never reserved, so gapless
   *  by construction), stamp the entry, ship the envelope with the LATEST args, resolve the handle.
   *  The entry stays on `pendingMutations` (now with a real mid) until the lmid release confirms it. */
  private flushFold(foldKey: string): void {
    const f = this.folds.get(foldKey);
    if (!f) return;
    this.clock.clearTimeout(f.timer);
    this.folds.delete(foldKey);
    // Re-resolve the DECLARED confirming stream from the FINAL args (§7.1) and deal the mid from
    // that domain's ledger — SEND order, never reserved, so gapless within the domain. The mid
    // dealt below then pins this domain. (A domain that changed since the window opened — a room
    // attached or dropped mid-window — re-stages on the next reconcile's re-invocation.)
    const domain = this.resolveDomain(f.entry.name, f.args);
    f.entry.domain = domain;
    const { mid, seq } = this.dealMid(domain);
    f.entry.mid = mid;
    f.entry.seq = seq;
    void this.channelFor(domain).pushMutation({ clientID: this.clientID, mid, name: f.entry.name, args: f.args });
    f.resolveMid(mid);
  }

  /** The transport a `domain`-confirmed mutation ships on (§7.5 sent-pins-domain: only the
   *  domain's own authority can confirm it, so its channel is the only correct transport). A
   *  domain with NO connected gate ships on the daemon channel — the gate-less configurations
   *  (`__testRelease`-driven tests) and today's entire live path resolve `"daemon"` anyway. */
  private channelFor(domain: string): OptimisticSource {
    return (this.gates.get(domain) ?? this.daemonGate).source;
  }

  /** The gate a channel-keyed retain registers through (G-iii registration-time routing). The
   *  channel MUST already be connected (`connectSource`; the daemon is constructor-attached) —
   *  loud by design: a typo'd or not-yet-connected sourceKey must throw at retain time, never
   *  silently register on the daemon and split the query's frames across channels. */
  private requireGate(channel: string): SourceGate {
    const gate = this.gates.get(channel);
    if (!gate) {
      throw new Error(
        `optimistic backend: no source connected for channel ${JSON.stringify(channel)} — call connectSource(${JSON.stringify(channel)}, source) before retaining a query on it`,
      );
    }
    return gate;
  }

  /** The channel that owns `sourceQid` — {@link RemoteSub.channel}, the ONE source of truth for
   *  qid routing (G-iii). `undefined` when no sub owns the qid (a harness-delivered raw feed, or
   *  a just-released sub): such frames buffer on whatever gate they arrive at. */
  private channelOf(sourceQid: QueryId): string | undefined {
    const key = this.sourceToRemote.get(sourceQid);
    return key ? this.remoteSubs.get(key)?.channel : undefined;
  }

  // --- the §3.3 deopt handshake, client half (H-v) ---------------------------------
  //
  // THE NAMED INVARIANT (Slice I inherits it): **never retire a room-domain entry off a
  // daemon-carried lmid without outcome resolution.** On the room socket it holds by
  // construction: every room lmid folds through the room's OWN gate, whose socket also carries
  // the outcome frames — same-socket ordering puts the frame before the ack, and the reconnect
  // re-send re-earns a lost frame, so a room-domain entry is only ever retired as a success when
  // the room really applied it. Slice I's downgrade path breaks that coupling: the doc-scoped
  // ledger row becomes readable THROUGH THE DAEMON with no room socket alive (§7.1 "load-bearing
  // for §7.5"), and an lmid adopted that way covers burnt non-applied mids with no frame to say
  // so — retiring a deopted entry there as a silent success is exactly the lost-write this
  // handshake exists to prevent. ENFORCED since I-iii by {@link foldSystemFrames}: the I-ii
  // outcome ROWS (co-committed, in ONE daemon transaction, with the ledger row that covers them)
  // are synthesized into frames and routed through THIS machine BEFORE the ledger fold advances
  // the domain watermark — one verdict path for frames and rows, with the processed set as the
  // cross-release resolved-verdict memory, and absence-under-a-covering-lmid = applied (I-ii's
  // atomicity makes that the sound default).

  /** Record `(domain, mid)` as processed; `false` if it already was (a duplicate frame —
   *  ignore it). FIFO-capped per domain ({@link MAX_PROCESSED_OUTCOMES_PER_DOMAIN}). */
  private markOutcomeProcessed(domain: string, mid: number): boolean {
    let mids = this.outcomesProcessed.get(domain);
    if (!mids) this.outcomesProcessed.set(domain, (mids = new Set()));
    if (mids.has(mid)) return false;
    mids.add(mid);
    while (mids.size > MAX_PROCESSED_OUTCOMES_PER_DOMAIN) {
      mids.delete(mids.values().next().value as number);
    }
    return true;
  }

  /** One `mutationOutcome` frame from `domain`'s channel (H-v — the §3.3 handshake's client
   *  half). The frame arrives OUT-OF-BAND (see {@link attachGate}); the state machine:
   *
   *  1. `mid` never issued on `domain` ⇒ ignore (a confused/foreign frame must not invent work).
   *  2. `(domain, mid)` already processed ⇒ ignore — idempotence under duplicate frames (the
   *     original + a re-send's re-answer; a deopt for a mid whose entry ALREADY FLIPPED also
   *     lands here harmlessly on its second frame).
   *  3. `kind:"rejected"` ⇒ FINAL. Surface the reason through {@link rejectedHandler} (room-plane
   *     parity with the HTTP queue's callback) and STOP — the drop + snap-back is the EXISTING
   *     failed-mutation machinery: the room burnt the mid, its lmid release retires the entry
   *     per-domain and the reconcile rewinds the prediction, exactly the daemon path's
   *     processed-as-no-op rejection. No new drop path.
   *  4. `kind:"deopt"`, entry found (pending `(domain, mid)`) ⇒ FLIP IN PLACE: `domain` becomes
   *     `"daemon"`, a fresh daemon mid is dealt and the envelope ships NOW on the daemon channel
   *     ("deal-and-send-now" — the conforming §3.3 re-enqueue: there is no flush machinery for
   *     non-fold entries, so the design's "mid: null until the daemon flush" is satisfied
   *     momentarily inside this call). THE ENTRY'S `seq` IS KEPT — settled (§5.3, commit
   *     68141096): `seq` is the client-global REPLAY order; re-sequencing would move the entry's
   *     overlay position and change read-dependent SIBLINGS' replay base. Everything else stays
   *     (writes/reads/touched/touchedSources/writeSources — union-never-shrink), the prediction
   *     stays applied (the entry never leaves `pendingMutations`, so no rewind fires), and the
   *     router does NOT re-run nor does `drainOverlapping` (§3.3 re-enqueues, never re-derives;
   *     any open overlapping fold was invoked later and flushes later with a larger mid).
   *  5. `kind:"deopt"`, entry NOT found ⇒ the burnt-mid confirm won the race, or the frame is a
   *     replay re-answer for an entry a previous session retired (the replay gotcha): re-invoke
   *     the frame's echoed `name`/`args` as a FRESH invocation PINNED to `"daemon"` — an honest
   *     re-prediction on the current base, never a derived route ({@link invokeWith}). A frame
   *     without `name` (not self-contained) has nothing to re-invoke and is dropped; a re-invoke
   *     that THROWS (the base moved from under it) is surfaced through {@link rejectedHandler} —
   *     the mutation is dead with no stream left to confirm it.
   *
   *  A `"deopt"` bump joins the Q6 routing counters either way (`routing.reasons.deopt`) —
   *  derived-and-deopted routes are visible beside derived successes. */
  private handleMutationOutcome(domain: string, frame: MutationOutcomeFrame): void {
    if (frame.mid >= (this.nextMid.get(domain) ?? 1)) return; // never issued here — not ours
    if (!this.markOutcomeProcessed(domain, frame.mid)) return; // duplicate frame
    if (frame.kind === "rejected") {
      const entry = this.pendingMutations.find((p) => p.domain === domain && p.mid === frame.mid);
      this.rejectedHandler(
        {
          clientID: this.clientID,
          mid: frame.mid,
          name: entry?.name ?? frame.name ?? "",
          args: entry !== undefined ? entry.args : frame.args,
        },
        frame.reason ?? "mutation rejected",
      );
      return;
    }
    // kind === "deopt": the room gate refused a declared-room mutation — re-enqueue onto the daemon.
    const entry = this.pendingMutations.find((p) => p.domain === domain && p.mid === frame.mid);
    if (entry) {
      entry.domain = "daemon";
      // Deal the fresh daemon mid but DISCARD its seq — the entry keeps its own (state-machine
      // step 4 above; the harmless dealSeq bump is accepted). This is the ONE place a dealt seq
      // is dropped, so "within one domain seq order == mid order" weakens to "except deopt
      // re-enqueues" — see the {@link PendingMutation.seq} doc.
      const { mid } = this.dealMid("daemon");
      entry.mid = mid;
      void this.channelFor("daemon").pushMutation({ clientID: this.clientID, mid, name: entry.name, args: entry.args });
      return;
    }
    if (frame.name === undefined) return; // not self-contained — nothing to re-invoke
    try {
      this.invokeWith(frame.name, frame.args, "daemon");
    } catch (err) {
      this.rejectedHandler(
        { clientID: this.clientID, mid: frame.mid, name: frame.name, args: frame.args },
        `deopt re-invocation failed: ${String((err as Error)?.message ?? err)}`,
      );
    }
  }

  /** §7.5 rule 3 (H-v): re-send `domain`'s unconfirmed pending envelopes with their ORIGINAL
   *  mids, in mid order, on the domain's own channel. Folds with `mid === null` are excluded —
   *  nothing was ever sent for them (the flush deals their mid). Envelopes are reconstructed from
   *  the pending entries exactly as `invoke` shipped them (`clientID`/`mid`/`name`/`args` —
   *  entries carry everything the wire needs). Idempotent under the domain's ledger: an APPLIED
   *  mid dedups silently and its lmid coverage retires the entry; a NON-APPLIED mid is re-answered
   *  from the shell's recorded-outcome map into {@link handleMutationOutcome}. Confirmed entries
   *  are already gone from `pendingMutations`, so no filter against the watermark is needed. */
  private resendPending(domain: string): void {
    const unconfirmed = this.pendingMutations
      .filter((p) => p.domain === domain && p.mid !== null)
      .sort((a, b) => a.mid! - b.mid!);
    if (unconfirmed.length === 0) return;
    const channel = this.channelFor(domain);
    for (const p of unconfirmed) {
      void channel.pushMutation({ clientID: this.clientID, mid: p.mid!, name: p.name, args: p.args });
    }
  }

  /** Drain every outstanding fold immediately (FOLDED-MUTATIONS-DESIGN §3): the explicit
   *  `app.flushFolds()` and the `beforeunload`/`close` hook. Creation (insertion) order. */
  flushFolds(): void {
    for (const key of [...this.folds.keys()]) this.flushFold(key);
  }

  /** A `trackingTx` op collector that records only the ops over a tracked aggregate's child
   *  table (the others can't move any count). Applied to the overlay by the caller AFTER the
   *  mutator succeeds, so a throwing mutator (whose staged write is discarded) leaves no delta. */
  private opCollector(ops: ChildOp[]): (op: ChildOp) => void {
    return (op) => {
      if (this.overlay.hasChild(op.table)) ops.push(op);
    };
  }

  /** Push the optimistic per-group delta onto the `__agg` head rows (§4):
   *  `target = server_base ⊕ delta`. A head-only write to the (tracked) synthetic table, so it
   *  joins the optimistic layer and is rewound/rebuilt by the reconcile cycle like any
   *  prediction. `server_base` is read from `NormalizedSync` (the authoritative base) — NOT
   *  from head, which already carries the optimistic layer (a torn read). Works standalone (an
   *  ordinary delivery) and inside an open cycle (the write buffers into it). */
  private reconcileAggHead(): void {
    const entries = this.overlay.entries();
    if (entries.length === 0) return;
    this.local.writeWith((tx) => {
      for (const e of entries) {
        const countCol = e.def.groupKeyCols.length; // row is [group…, count]; count is at index k
        const serverRow = this.sync.baseRow(e.aggTable, e.cells);
        const serverBase = serverRow ? Number(serverRow[countCol]) : 0;
        const target = Math.max(0, serverBase + e.n); // a displayed count never goes below 0
        const headRow = tx.get(e.aggTable, e.cells) as WireValue[] | undefined;
        const wantRow = serverRow !== undefined || target > 0;
        if (wantRow) {
          const desired = [...e.cells, target];
          if (!headRow) tx.add(e.aggTable, desired); // §6.1 optimistic birth (no server group yet)
          else if (!rowsEqual(headRow, desired)) tx.edit(e.aggTable, headRow, desired);
        } else if (headRow) {
          tx.remove(e.aggTable, headRow); // delta took a not-yet-on-server group back to identity
        }
      }
    });
    this.overlay.pruneZeros();
  }

  /** The SERVER CHANNEL's state for a query (§7): `unknown` while not hydrated, else `complete`.
   *  A pending local mutation no longer moves it — see {@link pending}. */
  resultType(qid: QueryId): ResultType {
    return this.resultTypes.get(qid) ?? "complete";
  }

  onResultType(handler: (qid: QueryId, rt: ResultType) => void): void {
    this.resultTypeHandler = handler;
  }

  __attachDevtoolsServerDeltas(observer: BackendDevObserver): () => void {
    this.devObservers.add(observer);
    return () => {
      this.devObservers.delete(observer);
    };
  }

  // --- the pending AXIS (§7.2): orthogonal to ResultType -------------------------------

  /** Whether any pending mutation (folded or not) touches this query's tables — "is a prediction
   *  pending here?" (FOLDED-MUTATIONS-DESIGN §7.2). Orthogonal to {@link resultType}; this is the
   *  same `queryTables ∩ pending.touched` computation that used to be smuggled into `unknown`. */
  pending(qid: QueryId): boolean {
    const tables = this.queryTables.get(qid);
    if (!tables) return false;
    return this.pendingMutations.some((p) => intersects(tables, p.touched));
  }

  /** Reactive pending axis (§7.2): fires when a query's pending-ness flips (invoke ↔ confirm), so a
   *  "saving…" affordance clears on its own when `lmid` catches up. */
  onPending(handler: (qid: QueryId, pending: boolean) => void): void {
    this.pendingHandler = handler;
  }

  /** The coarse, table-level pending indicator set (§7.2): every table some pending mutation touched. */
  pendingTables(): Set<string> {
    const out = new Set<string>();
    for (const p of this.pendingMutations) for (const t of p.touched) out.add(t);
    return out;
  }

  /** A read-only snapshot of the optimistic loop for a devtools pane (DEBUG-TOOLS-BROWSER-DESIGN
   *  §4.1). Built fresh per call from state the backend already holds — no new instrumentation, no
   *  mutation. Only ever called by `@rindle/devtools` (imported in dev). */
  __inspect(): OptimisticInspect {
    // Reverse-map each FoldRecord's entry → its fold key, so an un-flushed folded pending entry can
    // be tagged with its debounce window. A flushed fold has already been removed from `folds`
    // (`flushFold`), so it reads here as an ordinary `mid`-bearing entry — the pane links the
    // `f:<foldKey>` → `m:<mid>` transition itself.
    const foldByEntry = new Map<PendingMutation, [string, FoldRecord]>();
    for (const [foldKey, f] of this.folds) foldByEntry.set(f.entry, [foldKey, f]);
    const pending: PendingInspect[] = this.pendingMutations.map((p) => {
      const folded = foldByEntry.get(p);
      const key = p.mid != null ? `m:${p.mid}` : folded ? `f:${folded[0]}` : `?:${p.name}`;
      const writes: WriteRecord[] = [];
      for (const byPk of p.writes.values()) for (const rec of byPk.values()) writes.push(rec);
      const out: PendingInspect = {
        key,
        mid: p.mid,
        name: p.name,
        args: p.args,
        tables: [...p.touched],
        writes,
        reads: { reads: [...p.reads.reads], queries: [...p.reads.queries] },
      };
      if (folded) {
        const f = folded[1];
        out.fold = {
          foldKey: folded[0],
          debounceMs: f.debounceMs,
          maxWaitMs: f.maxWaitMs,
          deferAcrossWrites: f.deferAcrossWrites,
          flushed: p.mid != null,
        };
      }
      return out;
    });
    return {
      pending,
      // Back-compat scalars for the devtools `OptimisticInspect` mirror (unchanged shape): the DAEMON
      // domain's ledger — the only one in single-domain. Per-domain state is `__inspectDomains()`.
      confirmedLmid: this.watermark.get("daemon") ?? 0,
      nextMid: this.nextMid.get("daemon") ?? 1,
      appliedCv: this.daemonGate.appliedCv,
      bufferedFrames: this.daemonGate.buffer.length,
      pendingTables: [...this.pendingTables()],
    };
  }

  /** Test-only per-domain ledger snapshot (RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md §7.1/§8.5).
   *  Kept separate from {@link __inspect} so the devtools `OptimisticInspect` mirror stays byte-for-
   *  byte identical (daemon-scalar-only). Exposes the per-domain `nextMid`/`watermark` maps plus each
   *  pending entry's confirming domain — the axes the §8.5 ledger-isolation assertion checks. */
  __inspectDomains(): {
    nextMid: Record<string, number>;
    watermark: Record<string, number>;
    /** Per connected CHANNEL (§5.1): its release watermark + buffered-frame depth — the axis the
     *  gate-isolation assertions read (one source's laggy cvMin must never move the other's). */
    gates: Record<string, { appliedCv: number; bufferedFrames: number }>;
    /** Per connected/registered room: its wire-table → engine-table map (302 §2) and which local
     *  view qids are currently swapped onto it (302 §4). */
    roomTables: Record<string, Record<string, string>>;
    swappedViews: Record<number, string>;
    /** The §4 lifecycle plane's folded state (Slice I-iii introspection): the per-doc §4.2 fence
     *  value (`roomWatermarks`, I-v's ghost-drop input), the per-scope §4.1 occupancy map
     *  (`scopeSessions`: scope → client_id → expires_at, I-iv's doorbell input), and the live
     *  I-v ghosts (demoted room sources still awaiting their swap-back fence). */
    lifecycle: {
      roomWatermarks: Record<string, number>;
      scopeSessions: Record<string, Record<string, number>>;
      ghosts: Record<string, { doc: string; finalFlushSeq: number }>;
    };
    pending: { mid: number | null; seq: number | null; name: string; domain: string }[];
  } {
    return {
      nextMid: Object.fromEntries(this.nextMid),
      watermark: Object.fromEntries(this.watermark),
      gates: Object.fromEntries(
        [...this.gates].map(([k, g]) => [k, { appliedCv: g.appliedCv, bufferedFrames: g.buffer.length }]),
      ),
      roomTables: Object.fromEntries(
        [...this.roomTables].map(([k, m]) => [k, Object.fromEntries(m)]),
      ),
      swappedViews: Object.fromEntries(this.roomSwappedViews),
      lifecycle: {
        roomWatermarks: Object.fromEntries(this.roomWatermarks),
        scopeSessions: Object.fromEntries(
          [...this.scopeSessions].map(([scope, sessions]) => [scope, Object.fromEntries(sessions)]),
        ),
        ghosts: Object.fromEntries(
          [...this.ghosts].map(([k, g]) => [k, { doc: g.doc, finalFlushSeq: g.finalFlushSeq }]),
        ),
      },
      pending: this.pendingMutations.map((p) => ({
        mid: p.mid,
        // The client-global deal sequence — the REPLAY order (mids are per-domain, incomparable
        // across domains; see PendingMutation.seq). The harness asserts send order with this.
        seq: p.seq,
        name: p.name,
        domain: p.domain,
      })),
    };
  }

  /** Recompute the pending axis for every query and fire `onPending` on transitions only. Called
   *  from the two points that move the pending set: invoke/invokeFolded (add) and the confirm-drop
   *  (remove) — exactly where `:359`/`:468` used to flip ResultType (§7.3). */
  private refreshPending(): void {
    for (const qid of this.queryTables.keys()) {
      const now = this.pending(qid);
      if (this.pendingState.get(qid) !== now) {
        this.pendingState.set(qid, now);
        this.pendingHandler(qid, now);
      }
    }
  }

  // --- the downstream stream (§8.5: buffer, then release coherently — PER GATE, §5.1) ------

  private onFrame(gate: SourceGate, qid: QueryId, ev: NormalizedEvent): void {
    // Ownership is fixed at RETAIN time (G-iii registration-time routing: {@link RemoteSub.channel},
    // the one source of truth) — a qid lives on the ONE channel its sub registered on. So a frame
    // arriving on any OTHER gate means the server routed a qid to the wrong channel: a wiring bug —
    // fail loudly rather than silently splitting one query's frames across two cv timelines. (This
    // used to be a lazy first-arrival CLAIM; it is now a pure assertion.) A qid with NO sub (a
    // harness-delivered raw feed) has no owner and buffers on the arriving gate; the per-channel
    // reserved LMID_QID is exempt — each gate owns its own.
    if (qid !== LMID_QID) {
      const owner = this.channelOf(qid);
      if (owner !== undefined && owner !== gate.key) {
        // I-iv retarget grace: a frame already in flight from the sub's PREVIOUS channel when
        // {@link retargetRemoteQuery} moved it (the unsubscribe races the server's last frames)
        // is stale, not a wiring bug — drop it. The grace window is exactly the deferred-GC
        // window: {@link flushRetargetGc} deletes the record, and the loud throw is restored.
        if (this.pendingRetargetGc.get(qid) === gate.key) return;
        throw new Error(`optimistic backend: qid ${qid} arrived on ${gate.key} but is owned by ${owner}`);
      }
    }
    // System-plane frames (I-iii) are bookkeeping, not view data: like the lmid stream they skip
    // the devtools server-delta tap (there is no local view to attribute them to).
    if (qid !== LMID_QID && !this.systemQids.has(qid)) this.emitServerDelta(qid, ev);
    if (ev.type === "hello") {
      // A hello is a (re)subscribe = a NEW epoch. The column map below is mutated eagerly, but
      // data frames are cv-buffered and drained later (the gate's progress). So any frame still
      // buffered for this qid is from a SUPERSEDED epoch and must NOT be scattered through this
      // epoch's (possibly changed) map — drop it. This epoch's snapshot, which always follows the
      // hello, re-hydrates the qid from scratch, so the dropped frames are redundant. Scoped to
      // this qid: other queries' frames (and the lmid system query's) keep their coherent release.
      gate.buffer = gate.buffer.filter((f) => f.qid !== qid);
      this.addServerDependencyTables(qid, ev.tables.map((t) => t.name));
      // Learn this query's per-table column map (PROJECTION-SUPPORT-DESIGN.md §5.2): map each
      // advertised column to its base ColId BY NAME. The hello may carry FEWER columns than the
      // client's schema (a projection) or MORE (an EXPANDED server table mid an
      // `expand-then-contract` migration) — a column the client lacks maps to `-1`, a DROP
      // sentinel the sync layer discards while keeping the rest Absent. Register the map so the
      // sync layer scatters the rows into the shared union; a table whose map is an in-order,
      // drop-free full width IS '*' (a verbatim full row) and stays unregistered (a synthetic agg
      // table is unknown here, also '*'). Idempotent across re-hydrate epochs.
      for (const t of ev.tables) {
        const full = this.colCounts[t.name];
        if (full === undefined) continue; // unknown/synthetic table → '*' (full presence)
        const index = this.colIndex[t.name];
        const cols = t.columns.map((name) => index?.get(name) ?? -1);
        // Register a non-trivial map; otherwise revert to '*' — and CLEAR any stale map a prior
        // epoch left (a server that expanded then contracted back), so the now-exact rows don't
        // scatter through a `-1`-bearing layout (silent cell corruption).
        if (cols.length !== full || cols.some((c, i) => c !== i)) gate.sync.registerProjection(qid, t.name, cols);
        else gate.sync.unregisterProjection(qid, t.name);
      }
      return; // envelope validation is the source's job
    }
    const cv = ev.cv ?? 0;
    if (cv <= gate.appliedCv && ev.type === "batch") return; // stale redelivery ON THIS TIMELINE
    gate.buffer.push({ cv, qid, kind: ev.type, ops: ev.ops, seq: gate.nextSeq++ });
    if (gate.buffer.length > this.bufferCap) this.overflow(gate);
  }

  private emitServerDelta(sourceQid: QueryId, ev: NormalizedEvent): void {
    if (!this.devObservers.size) return;
    for (const qid of this.localQidsForSource(sourceQid)) {
      for (const o of this.devObservers) o.onServerDelta?.(qid, { format: "normalized", event: ev });
    }
  }

  private localQidsForSource(sourceQid: QueryId): QueryId[] {
    const key = this.sourceToRemote.get(sourceQid);
    const sub = key ? this.remoteSubs.get(key) : undefined;
    if (!sub) return [sourceQid];
    const localQids = [...sub.localQids.keys()];
    return localQids.length ? localQids : [sourceQid];
  }

  /** One gate's release (§5.1 release gate): compute the coherent delta from THIS gate's cv-buffer,
   *  then apply it against the gate's source/domain. Split into {@link computeRelease} (buffer →
   *  delta, lmid → watermark) and {@link applyRelease} (per-source confirm-drop + reconcile) —
   *  N independent gates all feed the ONE apply half; {@link __testRelease} drives it directly. */
  private onGateProgress(gate: SourceGate, frame: ProgressFrame): void {
    const { deltas, newlyHydrated, touchedScopes } = this.computeRelease(gate, frame);
    this.applyRelease(gate.key, deltas, undefined, newlyHydrated);
    // I-iv phase 2: a retargeted sub whose first ROOM snapshot released just now gets its old
    // channel's rows GC'd — AFTER the release fully applied, so the winner flip is value-equal
    // against the freshly-folded room rows (never a remove-before-the-refill).
    this.flushRetargetGc(gate);
    // I-iv doorbell events LAST — everything this release carried (data, confirms, the occupancy
    // fold itself, the retarget cutover) is already applied when the consumer's reaction (an
    // async re-lease) is kicked off. One event per touched scope, count evaluated at the fold
    // clock's now (deterministic under an injected clock).
    if (touchedScopes !== null) {
      for (const scope of touchedScopes) {
        this.scopeSessionsHandler({ scope, others: this.otherScopeSessions(scope) });
      }
    }
  }

  /** Compute one coherent release from ONE gate's cv-buffer (§5.1) — gate-scoped: its buffer, its
   *  cvMin timeline. Take every buffered frame at `cv ≤ cvMin`, in (cv, arrival) order, and fold
   *  it: the lmid system-query frame advances `watermark[gate.key]` (via {@link foldLmidOps} — the
   *  daemon stream folds "daemon", a room stream folds its own domain); data frames fold through
   *  this SOURCE's cross-query refcount into ONE net base delta — the §1.3 `D`. Returns that delta
   *  plus the set of local views this release JUST hydrated (so their reconcile batch phases as a
   *  `snapshot`). Mutates the gate's buffer/`appliedCv`, hydration, and the gate's domain
   *  watermark; the pending set and the reconcile are {@link applyRelease}'s job. */
  private computeRelease(
    gate: SourceGate,
    frame: ProgressFrame,
  ): { deltas: Mutation[]; newlyHydrated: Set<QueryId> | null; touchedScopes: Set<string> | null } {
    // Snapshot which local views are already hydrated BEFORE this release folds: any that cross into
    // hydrated below get their first result set as this cycle's batch, which must phase as a snapshot.
    const wasHydrated = new Set(this.hydrated);
    const ready = gate.buffer
      .filter((f) => f.cv <= frame.cvMin)
      .sort((a, b) => a.cv - b.cv || a.seq - b.seq);
    gate.buffer = gate.buffer.filter((f) => f.cv > frame.cvMin);
    // The §4 lifecycle SYSTEM frames fold FIRST, in a FIXED structural category order (Slice
    // I-iii; see {@link foldSystemFrames} for why the order is load-bearing), then the ordinary
    // lmid + data frames fold exactly as before. With no system retain the partition is empty and
    // this release is byte-identical to pre-I-iii. The returned scope set feeds the I-iv doorbell
    // events `onGateProgress` fires once the WHOLE release has applied.
    const touchedScopes = this.foldSystemFrames(ready.filter((f) => this.systemQids.has(f.qid)));
    const muts: Mutation[] = [];
    for (const f of ready) {
      if (this.systemQids.has(f.qid)) {
        // Folded above; a system stream has no store view and MUST NOT enter the sync layer (its
        // tables are not in the schema) — but its first snapshot still marks the sub hydrated so
        // the overflow/introspection bookkeeping stays uniform.
        if (f.kind === "snapshot") this.markSubHydrated(f.qid);
        continue;
      }
      if (f.qid === LMID_QID) {
        // Confirmation and data of the same commit share a cv, so they release together — each
        // channel's lmid stream folds into ITS OWN domain's watermark (§7.1).
        this.foldLmidOps(f.ops, gate.key);
        continue;
      }
      // A ROOM gate's deltas rename into the room's namespaced tables — and a wire table outside
      // the registered map is DROPPED (302 §6: context comes from the daemon, one authority per
      // table; a room's relayed context copy must never enter the store).
      muts.push(
        ...mapGateDeltas(gate, f.kind === "snapshot" ? gate.sync.rehydrate(f.qid, f.ops) : gate.sync.applyBatch(f.qid, f.ops)),
      );
      // A query's first released snapshot is its hydration point — even an empty one (0 rows is an
      // authoritative answer): lift every local view this sub feeds out of `unknown` (loading).
      if (f.kind === "snapshot") this.markSubHydrated(f.qid);
    }
    gate.appliedCv = Math.max(gate.appliedCv, frame.cvMin);
    let newlyHydrated: Set<QueryId> | null = null;
    for (const qid of this.hydrated) {
      if (!wasHydrated.has(qid)) (newlyHydrated ??= new Set()).add(qid);
    }
    return { deltas: muts, newlyHydrated, touchedScopes };
  }

  /** Apply one released delta against `sourceKey`'s domain (§7.2 per-domain confirm-drop + the §1.3
   *  reconcile cycle). `watermarkUpdate`, when given, advances `watermark[sourceKey]` first — the
   *  hook a per-source lmid confirm rides on (the daemon path folds its watermark in
   *  {@link computeRelease} and passes `undefined`). Then: drop every pending entry its OWN domain's
   *  watermark now covers (a room confirm can never retire a daemon entry, and vice-versa — the §7.1
   *  ledger-collision fix), and run the reconcile cycle against `sourceKey` when the base delta or the
   *  pending set changed. `newlyHydrated` stamps the initial-hydration batch as a catch-up. */
  private applyRelease(
    sourceKey: string,
    deltas: Mutation[],
    watermarkUpdate?: number,
    newlyHydrated: Set<QueryId> | null = null,
  ): void {
    if (watermarkUpdate !== undefined) {
      this.watermark.set(sourceKey, Math.max(this.watermark.get(sourceKey) ?? 0, watermarkUpdate));
    }
    // Drop confirmed pending (§1.3 step 5's bookkeeping half), PER DOMAIN: an entry is retired only
    // when ITS domain's watermark reaches its mid — so two concurrent streams never alias one counter
    // (§7.1). A failed mutation drops the same way (the release carries no effects, so the rewind snaps
    // the prediction back). An UNFLUSHED fold (`mid == null`) is never confirmable — retained until its
    // flush stamps a real mid (FOLDED-MUTATIONS-DESIGN §4.1), regardless of any domain's watermark.
    // H-v NOTE — retiring here treats coverage as SUCCESS, which for a room domain is sound only
    // because outcome resolution ALWAYS precedes the coverage that retires: on the room socket
    // the outcome frames outrun the lmid acks (same-socket ordering + the resync re-send), and on
    // the daemon-carried path (I-iii) `foldSystemFrames` routes the co-committed outcome ROWS
    // through handleMutationOutcome BEFORE the ledger fold advances the watermark this filter
    // reads — a deopted entry has already flipped off the domain by the time its burnt mid is
    // covered, either way (the named invariant above handleMutationOutcome).
    const before = this.pendingMutations.length;
    this.pendingMutations = this.pendingMutations.filter(
      (p) => p.mid === null || p.mid > (this.watermark.get(p.domain) ?? 0),
    );
    const pendingChanged = this.pendingMutations.length !== before;

    // The reconcile cycle — only when something can have changed: a base delta to fold in, or a
    // pending set that shrank (its optimistic layer must rewind out). The batch it emits for any view
    // that JUST became hydrated is that view's initial result set, so mark those qids so the
    // local-event forwarder stamps their batch `catchUp` (→ Store phases it `snapshot`).
    if (deltas.length || pendingChanged) {
      const emitted = (this.catchUpEmitted = new Set<QueryId>());
      this.catchUpQids = newlyHydrated;
      try {
        this.runReconcileCycle(sourceKey, deltas);
      } finally {
        this.catchUpQids = null;
        this.catchUpEmitted = null;
      }
      // Drop the qids the reconcile actually delivered a batch for; the rest folded nothing.
      if (newlyHydrated) for (const qid of emitted) newlyHydrated.delete(qid);
    }

    // ResultType is the SERVER CHANNEL's state only now (§7): `unknown` while not hydrated, else
    // `complete` — a pending mutation no longer moves it. The pending axis moves separately.
    for (const qid of this.queryTables.keys()) {
      this.setResultType(qid, this.hydrated.has(qid) ? "complete" : "unknown");
    }
    // A newly-hydrated query whose reconcile emitted NO batch (0 rows, its whole result already present
    // via a sibling → 0 net muts, or the reconcile was skipped) still needs a hydration signal, or its
    // SSR seed never retires and the view freezes. Send an explicit empty catch-up (now that it reads
    // `complete`, the Store retires the seed and reveals whatever is already in its tree).
    if (newlyHydrated) {
      for (const qid of newlyHydrated) this.handler(qid, { type: "batch", events: [], catchUp: true });
    }
    this.refreshPending();
    // The 302 §4.1 swap-in — strictly AFTER the reconcile above folded this release's data, so a
    // room sub whose first snapshot just released swaps its views onto room tables that already
    // hold the snapshot (swapping earlier would hydrate them empty). Structural no-op with no
    // pending swap (every single-domain client).
    this.processSwapIns();
    // The I-v ghost-drop watcher (§4.2), LAST: this release's watermark rows have folded
    // (computeRelease) and its confirm-drop has retired what it covers — exactly the two inputs
    // the drop condition reads. Structural no-op with no ghost.
    this.evaluateGhosts();
  }

  /** Test-only per-source release seam (RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md §7.2/§8.5): drive
   *  {@link applyRelease} for `sourceKey` directly — an explicit `watermarkUpdate` (a simulated lmid
   *  confirm for that domain) and `deltas` (a coherent base delta), with no real gate. Lets a harness
   *  exercise a room-domain confirm before the real second lmid stream / per-source gate is wired
   *  (E-iii-b/c). The `__`-prefix marks it a test hook, alongside {@link __inspect}. */
  __testRelease(sourceKey: string, deltas: Mutation[], watermarkUpdate?: number): void {
    this.applyRelease(sourceKey, deltas, watermarkUpdate);
  }

  // --- the 302 §4 swap-in ------------------------------------------------------------------

  /** Swap every view of each just-hydrated ROOM sub onto the room's namespaced tables (302 §4.1):
   *  re-register the local engine query with the AST's room-owned table references renamed
   *  ({@link remapAstTables}); the Store folds the re-hello as an in-place reset, so the caller's
   *  view reference survives and subscribers see ONE transition. Runs at the applyRelease tail —
   *  the reconcile has already folded the sub's snapshot into the room tables, so the swapped
   *  view hydrates straight to the room state (swapping earlier would flash it empty). The
   *  ORIGINAL ast stays in {@link asts}; the swap-back ({@link dropGhost}) re-registers it.
   *
   *  This is the accepted-flash boundary (302 §4.1/§7.1): the room's copy may be behind the
   *  daemon rows the view showed a moment ago — accepted by decision, revisit on a real
   *  two-region deploy. */
  private processSwapIns(): void {
    if (this.pendingSwapIns.size === 0) return; // every single-domain release: structural no-op
    const subs = [...this.pendingSwapIns];
    this.pendingSwapIns.clear();
    this.inOneCommit(() => {
      for (const sub of subs) {
        const map = this.roomTables.get(sub.channel);
        for (const qid of sub.localQids.keys()) {
          const ast = this.asts.get(qid);
          if (ast === undefined) continue;
          if (map === undefined || map.size === 0) continue; // no owned tables — nothing to swap
          if (this.roomSwappedViews.get(qid) === sub.channel) continue; // already swapped
          const rewritten = remapAstTables(this.plainEngineAst(ast), map);
          this.local.unregisterQuery(qid);
          this.local.registerQuery(qid, rewritten);
          this.roomSwappedViews.set(qid, sub.channel);
          // The pending axis follows the engine tables the view now reads (union — the wire
          // names stay too, conservatively: a daemon-declared write to a room-visible table is
          // still an honest "pending elsewhere" signal).
          const tables = this.queryTables.get(qid);
          if (tables) for (const t of map.values()) tables.add(t);
        }
      }
    });
  }

  /** Fold `domain`'s lmid system query's released ops (lmid-as-data): the one row's
   *  `last_mutation_id` cell is this client's confirmed high-water mid in that domain — it advances
   *  `watermark[domain]` and, on a fresh session ahead of our issued mids, `nextMid[domain]`. The
   *  daemon stream folds `"daemon"`; a room stream folds its own `"room:doc:X"`; the daemon-carried
   *  §7.1 ledger rows fold through the same {@link foldConfirm} core (Slice I-iii). */
  private foldLmidOps(ops: NormalizedOp[], domain: string): void {
    for (const op of ops) {
      const row = op.op === "add" ? op.row : op.op === "edit" ? op.new : undefined;
      if (!row) continue; // a remove (client GC) confirms nothing
      this.foldConfirm(domain, Number(row[1]));
    }
  }

  /** THE one confirm fold (§7.1/§7.2): advance `watermark[domain]` to `lmid` (monotone max) and,
   *  on a fresh session ahead of our issued mids, adopt `nextMid[domain]`. Shared verbatim by the
   *  per-channel lmid system query ({@link foldLmidOps}) and the daemon-carried room-ledger rows
   *  ({@link foldSystemFrames} — one core so the two paths cannot drift). */
  private foldConfirm(domain: string, lmid: number): void {
    if (!Number.isFinite(lmid)) return;
    const highestIssued = (this.nextMid.get(domain) ?? 1) - 1;
    if (lmid > highestIssued) {
      // Only in-flight mutations of THIS domain can contradict its watermark (§7.1: the counters
      // are independent — a room-domain mutation pending while the daemon's historical lmid
      // snapshot arrives is a normal fresh-session interleaving, not a second writer). An
      // unflushed fold (`mid == null`) has issued nothing yet either way: it cannot explain a
      // confirmed-ahead lmid, and its eventual flush deals from the adopted counter below.
      if (this.pendingMutations.some((p) => p.domain === domain && p.mid !== null)) {
        // The server confirmed a mid we never issued while we have mutations in
        // flight on this domain — two writers on one clientID or corrupted state. Unrecoverable.
        throw new Error(
          `optimistic backend: confirmed lmid ${lmid} is ahead of issued mids (${highestIssued})`,
        );
      }
      // A fresh session over a clientID with history: adopt the server's high-water
      // mark so our next mid continues the sequence instead of colliding below it.
      this.nextMid.set(domain, lmid + 1);
    }
    this.watermark.set(domain, Math.max(this.watermark.get(domain) ?? 0, lmid));
  }

  // --- the §4 lifecycle system-stream folds (Slice I-iii) --------------------------------

  /** Fold one release's SYSTEM frames in a FIXED category order — the order is STRUCTURAL (one
   *  function, categories in sequence), because it is the client half of THE NAMED INVARIANT
   *  (§3.3's shipped note; documented above {@link handleMutationOutcome}): **never retire a
   *  room-domain entry off a daemon-carried lmid without outcome resolution.**
   *
   *    1. **outcome rows** (`_rindle_room_mutation_outcomes`) — each row for OUR clientID is
   *       synthesized into a {@link MutationOutcomeFrame} and routed through
   *       {@link handleMutationOutcome}, the SAME H-v state machine the room socket's frames use
   *       (one verdict path: frames and rows cannot drift). A deopt flips its pending entry to
   *       the daemon IN PLACE (keep-seq, deal-and-send-now); a rejection surfaces + stays for the
   *       ordinary burnt-mid retire; a duplicate (frame already seen, or the row re-delivered) is
   *       absorbed by the processed set — which doubles as the resolved-verdict memory across
   *       releases (per-domain FIFO, {@link MAX_PROCESSED_OUTCOMES_PER_DOMAIN}, mirroring the
   *       shell's recorded-outcome cap).
   *    2. **room-ledger rows** (`_rindle_room_client_mutations`) — the FIRST daemon-carried
   *       room-lmid path: OUR row's `last_mutation_id` folds into `watermark[room:<doc>]` via
   *       {@link foldConfirm}. Because step 1 ALREADY resolved every non-applied verdict this
   *       release carries (and earlier releases' verdicts were resolved at their own release),
   *       the confirm-drop that follows in {@link applyRelease} retires only entries whose
   *       outcome is resolution-by-absence — which I-ii's co-commit atomicity defines as APPLIED
   *       (a room flush co-commits the ledger row and every non-applied mid's outcome row in ONE
   *       daemon transaction, so a covering lmid without a row IS the applied verdict).
   *       Processing this category before step 1 is the violation, in two proven directions
   *       (each run break→fail→revert against `test/system_streams.test.ts`): (a) the ledger's
   *       fresh-session `nextMid` ADOPTION must not run before historical outcome rows are
   *       judged — adopted-first, a previous session's retained deopt row passes the
   *       "never-issued" guard and spuriously re-invokes a mutation that session already handled
   *       (a double-apply); (b) the RETIRE must not precede resolution — it does not BECAUSE the
   *       confirm-drop runs in {@link applyRelease}, strictly after this whole function. That
   *       deferral is load-bearing: an "optimization" retiring inline with the watermark fold
   *       retires a deopted entry as a silent success (the exact lost-write H-v exists to
   *       prevent) and mis-attributes a rejected row's reason.
   *    3. **watermark rows** (`_rindle_room_watermark`) — the §4.2 fence value, max-folded per
   *       doc ({@link roomWatermarks}); I-v's ghost-drop consumer, no reaction here.
   *    4. **scope-session rows** (`_rindle_scope_sessions`) — the §4.1 occupancy map
   *       ({@link scopeSessions}); I-iv's doorbell consumer, no reaction here.
   *
   *  Ordinary data ops fold AFTER all of these (the caller's main loop) — outcome/ledger state
   *  must be in place before {@link applyRelease}'s confirm-drop + reconcile consume the release.
   *  Every row is filtered against the retain's {@link SystemStreamSpec} scope/doc AND (for the
   *  client-keyed tables) our own `clientID` — defense in depth: the server predicate may have
   *  been minted doc-only (no `clientId` at lease time), so other clients' rows are expected and
   *  must be ignored, and a row for a doc this retain was not minted for is never folded.
   *
   *  Returns the scopes category 4 touched (snapshot or ops) — the I-iv doorbell events' input;
   *  `null` when none (every non-lifecycle release). The events themselves fire from
   *  `onGateProgress` AFTER the release applies, never from inside the fold. */
  private foldSystemFrames(frames: BufferedFrame[]): Set<string> | null {
    if (frames.length === 0) return null;
    const byTable = (table: string): { spec: SystemStreamSpec; frame: BufferedFrame }[] =>
      frames.flatMap((frame) => {
        const spec = this.systemQids.get(frame.qid);
        return spec !== undefined && spec.table === table ? [{ spec, frame }] : [];
      });
    // (1) outcome rows → the H-v machine, BEFORE any ledger fold (the named invariant).
    for (const { spec, frame } of byTable(ROOM_MUTATION_OUTCOMES_TABLE)) {
      for (const op of frame.ops) {
        const cells = op.op === "add" ? op.row : op.op === "edit" ? op.new : undefined;
        if (!cells) continue; // a remove is retention pruning (mid ≤ lmid − 512), never a verdict
        const row = decodeOutcomeRow(cells);
        if (!row || row.clientId !== this.clientID) continue;
        if (spec.doc !== undefined && row.doc !== spec.doc) continue;
        const frameShape: MutationOutcomeFrame = {
          mid: row.mid,
          kind: row.kind,
          ...(row.reason !== undefined ? { reason: row.reason } : {}),
          ...(row.name !== undefined ? { name: row.name } : {}),
          ...(row.args !== undefined ? { args: row.args } : {}),
        };
        // Release-time invocation is sound here where out-of-band was REQUIRED for the socket
        // frames (`attachGate`): the socket frame races a buffered lmid ack it must beat, so it
        // may not wait behind the gate — a ROW cannot race its own release (it and the covering
        // ledger row co-committed at one cv and fold in THIS function's fixed order). The
        // machine's steps need nothing from an open release: the flip/reject only move pending
        // bookkeeping + ship an envelope, and the not-found re-invoke arm runs a fresh prediction
        // — legal before `applyRelease` opens the reconcile cycle, identical to an app invoke
        // racing the release.
        this.handleMutationOutcome(roomDomainKey(row.doc), frameShape);
      }
    }
    // (2) room-ledger rows → the daemon-carried per-domain confirm (outcomes above resolved first).
    for (const { spec, frame } of byTable(ROOM_CLIENT_MUTATIONS_TABLE)) {
      for (const op of frame.ops) {
        const cells = op.op === "add" ? op.row : op.op === "edit" ? op.new : undefined;
        if (!cells) continue; // a ledger remove confirms nothing (mirrors foldLmidOps)
        const [doc, clientId, lmid] = cells;
        if (typeof doc !== "string" || clientId !== this.clientID) continue;
        if (spec.doc !== undefined && doc !== spec.doc) continue;
        this.foldConfirm(roomDomainKey(doc), Number(lmid));
      }
    }
    // (3) watermark rows → the monotone §4.2 fence value per doc.
    for (const { spec, frame } of byTable(ROOM_WATERMARK_TABLE)) {
      for (const op of frame.ops) {
        const cells = op.op === "add" ? op.row : op.op === "edit" ? op.new : undefined;
        if (!cells) continue; // the fence is monotone — a remove never regresses it
        const [doc, flushSeq] = cells;
        const seq = Number(flushSeq);
        if (typeof doc !== "string" || !Number.isFinite(seq)) continue;
        if (spec.doc !== undefined && doc !== spec.doc) continue;
        this.roomWatermarks.set(doc, Math.max(this.roomWatermarks.get(doc) ?? 0, seq));
      }
    }
    // (4) scope-session rows → the §4.1 occupancy map (a snapshot REPLACES the scope's map — an
    // authoritative re-hydrate must drop sessions that aged out while the stream was down; a
    // batch folds add/edit/remove incrementally). Touched scopes are collected for the I-iv
    // doorbell events (a snapshot touches its minted scope even with zero ops — an emptied-out
    // scope is a legitimate 1→0 observation for the transition tracker).
    let touchedScopes: Set<string> | null = null;
    const touch = (scope: string): void => {
      (touchedScopes ??= new Set()).add(scope);
    };
    for (const { spec, frame } of byTable(SCOPE_SESSIONS_TABLE)) {
      if (frame.kind === "snapshot" && spec.scope !== undefined) {
        this.scopeSessions.set(spec.scope, new Map());
        touch(spec.scope);
      }
      for (const op of frame.ops) {
        // A remove's identity rides its (full) removed row; add/edit carry the post-image.
        const cells = op.op === "edit" ? op.new : op.row;
        const [scope, clientId, expiresAt] = cells;
        if (typeof scope !== "string" || typeof clientId !== "string") continue;
        if (spec.scope !== undefined && scope !== spec.scope) continue;
        let sessions = this.scopeSessions.get(scope);
        if (!sessions) this.scopeSessions.set(scope, (sessions = new Map()));
        if (op.op === "remove") {
          sessions.delete(clientId);
        } else {
          const exp = Number(expiresAt);
          if (Number.isFinite(exp)) sessions.set(clientId, exp);
        }
        touch(scope);
      }
    }
    return touchedScopes;
  }

  /** The I-iv occupancy count — THE one rule (§4.1/D7): unexpired (`expires_at >` the fold
   *  clock's now) sessions under `scope` from OTHER clientIDs. Shared by the doorbell events
   *  ({@link onGateProgress}) and the client's registration-time check (a doorbell that folded
   *  BEFORE a candidate registered must still be able to trigger it) so the two can never
   *  disagree. Own-clientID rows never count — a solo client cannot ring its own bell — and
   *  expiry is judged on the injectable {@link FoldClock} (deterministic in a virtual-clock
   *  harness, the folded-oracle discipline). */
  otherScopeSessions(scope: string): number {
    const sessions = this.scopeSessions.get(scope);
    if (!sessions) return 0;
    const now = this.clock.now();
    let n = 0;
    for (const [clientId, expiresAt] of sessions) {
      if (clientId !== this.clientID && expiresAt > now) n++;
    }
    return n;
  }

  /** Register the I-iv doorbell event sink — see {@link ScopeSessionsEvent}. One handler (a later
   *  registration replaces it, the {@link onLocalWrite} convention); client.ts is the consumer. */
  onScopeSessions(handler: (event: ScopeSessionsEvent) => void): void {
    this.scopeSessionsHandler = handler;
  }

  /** One §1.3 reconcile cycle: rewind the optimistic layer and fold the coherent SERVER
   *  delta into BOTH head AND the `sync` baseline (`serverBatchBegin`), re-invoke every
   *  still-pending mutator to re-stage the optimistic layer (the rewind un-applied it), then
   *  deliver the coalesced result (`serverBatchEnd`). This is the engine's only sync-moving
   *  boundary — `onProgress` releases and `unregisterQuery`'s GC both go through here so head
   *  and sync never diverge (the §1.2 invariant; CRIT#2). */
  private runReconcileCycle(_sourceKey: string, serverDeltas: Mutation[]): void {
    // `_sourceKey` names the authority these `deltas` confirm — `"daemon"` on the live daemon
    // path (and the GC path), a `room:doc:X` string on a room release, whose deltas already carry
    // the room's ENGINE table names (the gate's rename/filter). Kept for call-site readability
    // and tracing only: the engine itself is source-agnostic (302: one authority per table) — its
    // rewind covers EVERY tracked table and every pending mutation re-invokes below regardless of
    // which channel released, so NOTHING in this cycle may branch on it.
    this.local.serverBatchBegin(serverDeltas.map(toServerOp));
    // The rewind covers EVERY tracked table (302: the engine is source-agnostic — there is no
    // per-source rewind) — including the `__agg_*` head rows — whichever channel released. So the
    // optimistic agg delta rebuilds on EVERY cycle, room or daemon: reset here, re-observe from
    // the re-invoked pending set below, re-apply onto the rewound heads at the end. Gating any of
    // the three on a daemon-only cycle (the pre-302 per-source-rewind contract) would let a room
    // release wipe the optimistic `__agg` edits and skip the rebuild — every count() view snaps
    // back to the server base until the next daemon release. The delta stays sound across
    // domains: `reconcileAggHead` recomputes each head as the absolute `server_base ⊕ delta`,
    // and the server base (`this.sync`) only moves on daemon releases.
    this.overlay.reset();
    // Sort ALL pending into SEND order (the client-global `seq` ascending, then unflushed folds
    // last by creation order — the deterministic §4.1 slot; the comparator is explicit, NOT
    // `(seq ?? ∞) - (seq ?? ∞)` which is `∞ - ∞ = NaN` and corrupts V8's sort). The key MUST be
    // `seq`, never `mid`: mids are per-domain (§7.1) so mids from different domains are
    // incomparable — a mid-sort would replay a room mid 1 before a daemon mid 5 that was sent
    // FIRST, letting a read-dependent mutator re-predict from a base it never saw (confirmation
    // order is per-domain; replay order is client-global). EVERY entry re-invokes — the engine's
    // rewind covers every tracked table (302: there is no per-source rewind), so every entry's
    // staged writes were just un-applied, whichever channel released. Single-domain: seq order ==
    // mid order (except H-v deopt re-enqueues, which keep their ORIGINAL seq under a later daemon
    // mid — deliberately, so this very sort replays them at their original overlay position).
    const order = [...this.pendingMutations].sort((a, b) => {
      if (a.seq === null && b.seq === null) return 0; // both unflushed → stable creation order
      if (a.seq === null) return 1; // an unflushed fold sorts after every dealt seq
      if (b.seq === null) return -1;
      return a.seq - b.seq;
    });
    const dropped = new Set<PendingMutation>();
    try {
      for (const p of order) {
        // NO `readLog` here — recording is armed only on the initial `invoke` (§3.2 #2 note on
        // `PendingMutation.reads`); a re-invocation's write-set still needs fresh capture (below).
        // The staging map follows the entry's CURRENT domain — a deopt-flipped or re-routed entry
        // re-stages onto its new domain's tables here.
        const writes: WriteSet = new Map();
        const ops: ChildOp[] = [];
        try {
          this.local.writeWith((tx) => {
            this.runMutator(this.registry[p.name], trackingTx(tx, writes, this.specs, this.localTables, this.opCollector(ops), false, undefined, this.stagingMap(p.domain)), p.args);
          });
        } catch {
          // A re-invocation threw — e.g. a read-dependent mutator whose base row the server
          // deleted, or one reading a row a now-rejected sibling created. Drop it from pending
          // (its staged write was discarded); its prediction simply snaps back on this release —
          // a throwing mutation surfaces on the mutation axis (`onRejected`), not as a query-level
          // `error` (§7.4). The `finally` below ALWAYS closes the cycle, so one bad mutator can't
          // wedge the engine forever (#7).
          dropped.add(p);
          continue;
        }
        // The re-invocation stuck — fold its child ops into the rebuilt optimistic agg delta.
        for (const op of ops) this.overlay.observe(op);
        // The pending footprint is the UNION across invocations: a re-run that no-ops (touched =
        // {}) must NOT shrink it, else a still-pending mutation reports not-pending and its
        // pending-axis clear fires early (§7.2). `writes` mirrors this: merge, never replace.
        for (const t of writes.keys()) p.touched.add(t);
        mergeWriteSet(p.writes, writes);
      }
      // Preserve creation order in the live array (the unflushed-fold sort tiebreak depends on it).
      if (dropped.size) this.pendingMutations = this.pendingMutations.filter((p) => !dropped.has(p));
      // Re-apply the optimistic agg delta onto the (rewound) `__agg` head rows — INSIDE the open
      // cycle, so the writes buffer and coalesce into the one per-query delivery `serverBatchEnd`
      // makes (and never escape as a separate batch). Every cycle (see the reset above).
      this.reconcileAggHead();
    } finally {
      this.local.serverBatchEnd(); // ALWAYS close the cycle — ONE delivery per affected query.
    }
  }

  /** ONE channel's authority restarted (a new boot id): it lost all materialization + `cv` state
   *  and its `cv` sequence reset, so previously-released `cv`s no longer bound the new stream. The
   *  source has already re-subscribed every query (reconnect → resync); drop THIS gate's buffer
   *  and `cv` watermark so the fresh, low-`cv` snapshots are RELEASED instead of dropped as stale
   *  (`onFrame`/`computeRelease` gate on `appliedCv`). The OTHER gates are untouched — an
   *  authority restart is per-channel (§5.1). Pending optimistic mutations stay put — they
   *  re-apply on the next reconcile, and the channel's lmid system query's fresh snapshot restores
   *  its domain's confirmation watermark. */
  private resetGate(gate: SourceGate): void {
    gate.buffer = [];
    gate.appliedCv = 0;
  }

  /** The §8.5 escape: ONE gate's buffer outgrew its cap (a pinned `cvMin` under churn on that
   *  channel). Drop everything it buffered and re-register every query on that source — the fresh
   *  snapshots arrive as ordinary frames and the next release re-hydrates via the footprint diff
   *  (the §5.3 path); still-pending optimism re-applies in that cycle. The other gates' buffers
   *  and subscriptions are untouched. */
  private overflow(gate: SourceGate): void {
    gate.buffer = [];
    for (const sub of this.remoteSubs.values()) {
      // Re-register only the subs THIS channel owns ({@link RemoteSub.channel} — the one source
      // of truth, G-iii): resubscribing another channel's sub here would fork its stream.
      if (sub.channel !== gate.key) continue;
      gate.source.unregisterQuery(sub.sourceQid);
      gate.source.registerQuery(sub.sourceQid, sub.remote);
    }
    // The lmid system query's buffered frames were dropped too — re-subscribe it so a
    // fresh snapshot restores the confirmation watermark.
    gate.source.unregisterQuery(LMID_QID);
    gate.source.registerQuery(LMID_QID, { name: LMID_QUERY_NAME, args: {} });
  }

  private setResultType(qid: QueryId, rt: ResultType): void {
    if (this.resultTypes.get(qid) === rt) return;
    this.resultTypes.set(qid, rt);
    this.resultTypeHandler(qid, rt);
  }

  /** Recompute a query's server-channel state from hydration alone (§7): a pending mutation no
   *  longer affects it. Used when a remote sub attaches to or hydrates a local view. */
  private recomputeResultType(qid: QueryId): void {
    this.setResultType(qid, this.hydrated.has(qid) ? "complete" : "unknown");
  }

  /** A remote sub's first snapshot landed: mark it (and every local view it feeds) hydrated, then
   *  lift those views out of `unknown` (loading). A ROOM sub's hydration additionally queues the
   *  302 §4.1 swap-in — performed at the applyRelease TAIL ({@link processSwapIns}), once the
   *  reconcile has folded this snapshot into the room tables. Idempotent — a re-hydrate snapshot
   *  re-marks harmlessly; a source qid with no sub (the lmid system query) is a no-op. */
  private markSubHydrated(sourceQid: QueryId): void {
    const key = this.sourceToRemote.get(sourceQid);
    if (!key) return;
    const sub = this.remoteSubs.get(key);
    if (!sub || sub.hydrated) return;
    sub.hydrated = true;
    if (sub.channel !== "daemon" && !this.systemQids.has(sub.sourceQid)) this.pendingSwapIns.add(sub);
    for (const localQid of sub.localQids.keys()) {
      this.hydrated.add(localQid);
      this.recomputeResultType(localQid);
    }
  }

  /** `channel` (G-iii registration-time routing): the gate the sub registers on — the qid's
   *  ownership is fixed HERE, at retain time (no lazy claim; `onFrame` only asserts it). Default
   *  `"daemon"`, so every channel-less caller is byte-identical to before. */
  private retainRemote(
    retainQid: QueryId,
    remote: RemoteQuery,
    localQueryId: QueryId | undefined = retainQid,
    channel = "daemon",
  ): void {
    const gate = this.requireGate(channel); // throw loudly BEFORE any sub state moves
    const key = remoteKey(remote);
    let sub = this.remoteSubs.get(key);
    let isNew = false;
    if (!sub) {
      sub = { sourceQid: retainQid, remote, refCount: 0, localQids: new Map(), hydrated: false, channel };
      this.remoteSubs.set(key, sub);
      this.sourceToRemote.set(sub.sourceQid, key);
      isNew = true;
    } else if (sub.channel !== channel) {
      // A (name,args) sub lives on ONE channel — a second retain naming another is a wiring bug
      // (it would split the query's frames across two cv timelines). Fail loudly.
      throw new Error(
        `optimistic backend: query "${remote.name}" is already retained on channel ${JSON.stringify(sub.channel)} — cannot retain it on ${JSON.stringify(channel)}`,
      );
    }
    sub.refCount++;
    if (localQueryId !== undefined) {
      sub.localQids.set(localQueryId, (sub.localQids.get(localQueryId) ?? 0) + 1);
      // A late-joiner to an already-hydrated sub is immediately hydrated; otherwise this view now
      // awaits the sub's first snapshot (so a split-path local view registered `complete` flips to
      // `unknown` here). Then recompute its lifecycle.
      if (sub.hydrated) {
        this.hydrated.add(localQueryId);
        // 302 §4.1 LATE JOIN: a ROOM sub's one-shot swap queue ({@link markSubHydrated}) fired at
        // its first released snapshot — long gone by now — so a view attaching afterwards must
        // swap onto the room's namespaced tables HERE, or its engine query stays registered on
        // the plain daemon tables the room channel never feeds (empty/stale, reported complete,
        // diverging from its already-swapped siblings forever). The room tables already hold the
        // released state (hydrated ⇒ folded), so swapping immediately is the ordinary
        // after-the-data order; processSwapIns skips already-swapped siblings, and
        // pendingSwapIns is empty outside a release, so exactly this sub's un-swapped views move.
        if (sub.channel !== "daemon" && !this.systemQids.has(sub.sourceQid)) {
          this.pendingSwapIns.add(sub);
          this.processSwapIns();
        }
        // FORCE the notify past setResultType's dedup: the labeled split registers the local
        // half `complete`, then flips the STORE view to `unknown` for the lease window WITHOUT
        // touching our record — so a complete→complete recompute here would swallow the event
        // and strand the late-joining view `unknown` forever. Redundant notifies are idempotent
        // Store-side; a swallowed transition is not recoverable.
        this.resultTypes.set(localQueryId, "complete");
        this.resultTypeHandler(localQueryId, "complete");
      } else {
        this.hydrated.delete(localQueryId);
        this.recomputeResultType(localQueryId);
      }
    }
    this.localToRemote.set(retainQid, key);
    this.remoteRetainToLocal.set(retainQid, localQueryId);
    // Register on the CHANNEL's source (G-iii): the qid's frames will arrive — and buffer, release,
    // and overflow — on that channel's own §5.1 gate.
    if (isNew) gate.source.registerQuery(sub.sourceQid, remote);
  }

  private releaseRemote(retainQid: QueryId): QueryId | undefined {
    const key = this.localToRemote.get(retainQid);
    if (!key) return undefined;
    this.localToRemote.delete(retainQid);
    const localQueryId = this.remoteRetainToLocal.get(retainQid);
    this.remoteRetainToLocal.delete(retainQid);
    const sub = this.remoteSubs.get(key);
    if (!sub) return undefined;
    sub.refCount--;
    if (localQueryId !== undefined) {
      const refs = (sub.localQids.get(localQueryId) ?? 0) - 1;
      if (refs > 0) sub.localQids.set(localQueryId, refs);
      else sub.localQids.delete(localQueryId);
    }
    if (sub.refCount > 0) return undefined;
    // Unregister from the SAME gate's source the retain registered on. Since I-v a gate CAN be
    // removed ({@link disconnectSource}) — but never with a live sub on it ({@link
    // demoteRoomSource} validates loudly), so the daemon fallback is purely defensive.
    (this.gates.get(sub.channel) ?? this.daemonGate).source.unregisterQuery(sub.sourceQid);
    this.sourceToRemote.delete(sub.sourceQid);
    this.remoteSubs.delete(key);
    return sub.sourceQid;
  }

  private addServerDependencyTables(sourceQid: QueryId, names: string[]): void {
    const key = this.sourceToRemote.get(sourceQid);
    if (!key) return;
    const sub = this.remoteSubs.get(key);
    if (!sub) return;
    let grew = false;
    for (const localQid of sub.localQids.keys()) {
      const tables = this.queryTables.get(localQid);
      if (!tables) continue;
      for (const name of names) {
        if (!tables.has(name)) grew = true;
        tables.add(name);
      }
    }
    // A newly-learned server dependency may bring a query into a pending mutation's footprint (§7.2);
    // ResultType is unaffected (server-channel-only, §7).
    if (grew) this.refreshPending();
  }
}

// --- helpers -----------------------------------------------------------------------

interface RemoteSub {
  sourceQid: QueryId;
  remote: RemoteQuery;
  refCount: number;
  localQids: Map<QueryId, number>;
  /** Whether this sub's first server snapshot has been released (drives hydration of its views). */
  hydrated: boolean;
  /** The authority channel (gate/source key) this sub registered on — fixed at retain time
   *  (G-iii registration-time routing), `"daemon"` unless a channel-keyed retain named another.
   *  The ONE source of truth for qid→channel ownership: `onFrame`'s wrong-channel assertion and
   *  the per-gate `overflow` both read it (via {@link OptimisticBackend.channelOf} / directly). */
  channel: string;
}

function remoteKey(remote: RemoteQuery): string {
  return stableJson([remote.name, remote.args]);
}

function stableJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  const obj = value as Record<string, unknown>;
  return `{${Object.keys(obj)
    .sort()
    .map((k) => `${JSON.stringify(k)}:${stableJson(obj[k])}`)
    .join(",")}}`;
}

function pkColsFromSchema<S extends ColsMap>(schema: Schema<S>): PkCols {
  const out: PkCols = {};
  for (const name of Object.keys(schema.tables)) out[name] = tableSpec(schema.tables[name]).primaryKey;
  return out;
}

/** Per-table column order + pk indices + the insert plan (which columns a full insert requires vs
 *  may omit-to-null) + column kinds (for the `json` stringify), for the keyed `MutationTx` methods. */
type TableSpecs = Record<
  string,
  { columns: string[]; primaryKey: number[]; required: string[]; nullable: ReadonlySet<string>; types: Record<string, ColType> }
>;

function tableSpecsFromSchema<S extends ColsMap>(schema: Schema<S>): TableSpecs {
  const out: TableSpecs = {};
  for (const name of Object.keys(schema.tables)) {
    const { columns, primaryKey } = tableSpec(schema.tables[name]);
    const { required, nullable } = insertPlan(schema.tables[name]);
    const types: Record<string, ColType> = {};
    for (const c of columns) types[c] = schema.tables[name].columns[c].type;
    out[name] = { columns, primaryKey, required, nullable, types };
  }
  return out;
}

/** Each table's FULL column count (the union-row width), from the typed schema. */
function colCountsFromSchema<S extends ColsMap>(schema: Schema<S>): ColCounts {
  const out: ColCounts = {};
  for (const name of Object.keys(schema.tables)) out[name] = tableSpec(schema.tables[name]).columns.length;
  return out;
}

/** Each table's column name → base ColId, from the typed schema (for mapping a projected hello's
 *  columns back to base positions, PROJECTION-SUPPORT-DESIGN.md §5.2). */
function colIndexFromSchema<S extends ColsMap>(schema: Schema<S>): Record<string, Map<string, number>> {
  const out: Record<string, Map<string, number>> = {};
  for (const name of Object.keys(schema.tables)) {
    const cols = tableSpec(schema.tables[name]).columns;
    out[name] = new Map(cols.map((c, i) => [c, i]));
  }
  return out;
}

/** Merge a fresh invocation's write-set into a `PendingMutation`'s accumulated one (§3.2 #1, rebase
 *  re-invocation): each pk's value is OVERWRITTEN with the newest image (a later invocation ran
 *  against the base the rebase just replaced, so its view supersedes the earlier one), but a key
 *  present only in `dest` is left alone — the same union-never-shrink rule `touched` already
 *  follows (§7.2: "a re-run that no-ops must NOT shrink it"). */
function mergeWriteSet(dest: WriteSet, src: WriteSet): void {
  for (const [table, byPk] of src) {
    let d = dest.get(table);
    if (!d) dest.set(table, (d = new Map()));
    for (const [pkKey, rec] of byPk) d.set(pkKey, rec);
  }
}

// --- the 302 room-table helpers -------------------------------------------------------

/** The namespaced ENGINE table backing wire `table` for room `sourceKey` (302 §2: `room_deck` ≠
 *  `deck` — one authority per table). `@` appears in no schema table name — ENFORCED by
 *  `createSchema`/`extendSchema`'s addTableMeta ban (packages/client/src/schema.ts), so the name
 *  cannot collide with a real table. */
export function roomEngineTable(table: string, sourceKey: string): string {
  return `${table}@${sourceKey}`;
}

/** Rename a room gate's released deltas into the room's namespaced tables, DROPPING deltas for
 *  wire tables outside the map (context / unknown — the daemon is their sole authority, 302 §6).
 *  Identity (no copy) for a map-less gate — the daemon path is untouched. */
function mapGateDeltas(gate: SourceGate, muts: Mutation[]): Mutation[] {
  const map = gate.tableMap;
  if (map === undefined) return muts;
  const out: Mutation[] = [];
  for (const m of muts) {
    const engineTable = map.get(m.table);
    if (engineTable === undefined) continue;
    out.push({ ...m, table: engineTable });
  }
  return out;
}

/** Rename every TABLE reference in a query AST through `map` (302 §2 point 3 — the room-homed
 *  view's rewrite): the root `table`, every `related` subquery, every `correlatedSubquery`
 *  (EXISTS) condition — walking the KNOWN wire-AST shape, never a blind key scan: `start.row` is
 *  keyed by COLUMN name (a schema column literally named `table` must keep its bound value), and
 *  the same goes for any future column-keyed record. Tables absent from the map keep their name —
 *  that is the client-side join across kinds (a room table joined to daemon-owned context,
 *  201-style). Structural clone; the input AST is never mutated. */
export function remapAstTables(ast: Ast, map: ReadonlyMap<string, string>): Ast {
  const walkCond = (c: Condition): Condition => {
    if (c.type === "and" || c.type === "or") return { ...c, conditions: c.conditions.map(walkCond) };
    if (c.type === "correlatedSubquery") return { ...c, related: walkSub(c.related) };
    return c; // "simple" — column refs and literals carry no table reference
  };
  const walkSub = (s: CorrelatedSubquery): CorrelatedSubquery => ({ ...s, subquery: walk(s.subquery) });
  const walk = (a: Ast): Ast => ({
    ...a,
    table: map.get(a.table) ?? a.table,
    ...(a.where !== undefined ? { where: walkCond(a.where) } : {}),
    ...(a.having !== undefined ? { having: walkCond(a.having) } : {}),
    ...(a.related !== undefined ? { related: a.related.map(walkSub) } : {}),
  });
  return walk(ast);
}

/** Wrap the raw wasm txn as the client `MutationTx`, capturing a pk-granular write-set as it
 *  applies (`writes`, a {@link WriteSet} — table → pk-key → last-write-wins image, §3.2 #1);
 *  `touched` (the pending axis's table-granular Set, §7.2) is derived by the CALLER as
 *  `new Set(writes.keys())`, never populated here. The keyed methods validate column names eagerly:
 *  a typo'd table or column throws with the valid names listed, at the moment the mutator runs.
 *
 *  With `trapReads` (the FOLDED path, §5), the PUBLIC reads `tx.get`/`tx.row`/`tx.query` throw
 *  `FoldReadError` — a folded mutator that reads state to compute its write is non-absorbing and
 *  refused. The keyed writers (`update`/`upsert`/`insertIgnore`/`delete`) still read internally to
 *  preserve unspecified columns / check pre-existence; that is fold-legal (the trap wraps only the
 *  returned object's `get`/`row`/`query` surface, never the writers' internal probe) — unchanged
 *  by H-ii, which records those probes but arms recording only where the trap never is.
 *
 *  With `readLog` (recording mode, RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md §3.2 #2) — a SIBLING
 *  of `trapReads`, the two never armed together by any call site — the PUBLIC `tx.get`/`tx.row`
 *  push a `(table, pk, outcome, source?)` {@link ReadRecord} (per-read provenance via the
 *  `provenance` probe, H-ii §3.2 #3), `tx.query` pushes its resolved AST, and (H-ii) the keyed
 *  writers' pre-existence probes record through the same path. Pure capture: it changes no return
 *  value, throws nothing, and is a no-op when `readLog` is omitted. */
/** Apply one logical {@link MutationOp} (yielded by a shared generator mutator) onto the client's
 *  keyed {@link MutationTx} — the same methods a plain client mutator calls directly. Column
 *  validation, write-set capture, and op collection all happen inside those methods. */
function applyOpToTx(tx: MutationTx, op: MutationOp): void {
  switch (op.kind) {
    case "insert":
      return tx.insert(op.table, op.row);
    case "upsert":
      return tx.upsert(op.table, op.row);
    case "insertIgnore":
      return tx.insertIgnore(op.table, op.row);
    case "update":
      return tx.update(op.table, op.row);
    case "delete":
      return tx.delete(op.table, op.pk);
  }
}

function trackingTx(
  tx: WasmWriteTxn,
  writes: WriteSet,
  specs: TableSpecs,
  localTables: Set<string>,
  onOp?: (op: ChildOp) => void,
  trapReads = false,
  readLog?: ReadLog,
  /** The 302 staging map for a room-DECLARED mutation: wire table → the room's namespaced engine
   *  table for the tables the room owns; identity for everything else. Every raw engine access —
   *  reads and writes — goes through it, so a room mutator reads/writes the room's own state
   *  (its optimistic effects land where the room-homed views look) while its envelope still
   *  ships the wire names. Absent (or a non-owned table) ⇒ the plain table, verbatim. */
  stage?: ReadonlyMap<string, string>,
): MutationTx {
  const spec = (table: string) => {
    const s = specs[table];
    if (!s) throw new Error(`unknown table ${JSON.stringify(table)} — tables: ${Object.keys(specs).join(", ")}`);
    return s;
  };
  /** The ENGINE table a wire-named access lands on (302 §2). Schema/column validation always
   *  runs on the WIRE name (the namespaced twin shares the spec). */
  const staged = (table: string): string => stage?.get(table) ?? table;

  // M1 (`201-LOCAL-ONLY-TABLES-DESIGN.md` §6): a replayable mutator is a pure function of
  // (synced base + args) — it neither READS nor WRITES a local-only table. The server runs the
  // same mutator from `args` alone and cannot see local tables, so any dependence diverges the
  // prediction from authority by construction, with no confirmation that can reconcile it. Both
  // directions are refused at stage time, the same loud shape as the unknown-column throw; local
  // writes go through `store.writeLocal` instead.
  const assertNotLocal = (table: string, verb: string): void => {
    if (localTables.has(table)) {
      throw new Error(
        `cannot ${verb} local-only table ${JSON.stringify(table)} inside a mutator — a replayable mutator may not touch local tables (use store.writeLocal; 201-LOCAL-ONLY-TABLES-DESIGN.md §6 / M1).`,
      );
    }
  };

  /** Validate `obj`'s keys against the table's columns; require the pk columns; with
   *  `full`, require every NON-nullable column (a nullable column may be omitted and is filled with
   *  `null` in {@link toCells}, design 206 §6.2). */
  const checkColumns = (table: string, obj: KeyedRow, full: boolean): void => {
    const s = spec(table);
    const unknown = Object.keys(obj).filter((k) => !s.columns.includes(k));
    if (unknown.length) {
      throw new Error(
        `unknown column${unknown.length > 1 ? "s" : ""} ${unknown.join(", ")} on ${table} — columns: ${s.columns.join(", ")}`,
      );
    }
    const required = full ? s.required : s.primaryKey.map((i) => s.columns[i]);
    const missing = required.filter((c) => !(c in obj));
    if (missing.length) {
      throw new Error(`missing ${full ? "column" : "primary-key column"}${missing.length > 1 ? "s" : ""} ${missing.join(", ")} on ${table}`);
    }
  };

  const pkCells = (table: string, obj: KeyedRow): WireValue[] =>
    spec(table).primaryKey.map((i) => obj[spec(table).columns[i]]);

  // pk from the POSITIONAL wire shape (raw cells in schema column order) — the counterpart of
  // `pkCells` (which reads a KeyedRow) for the raw `add`/`remove`/`edit` writers below (§3.2 #1).
  const pkFromCells = (table: string, cells: WireValue[]): WireValue[] =>
    spec(table).primaryKey.map((i) => cells[i]);

  // Record (or overwrite) this pk's write for the invocation (§3.2 #1): last-write-wins WITHIN
  // this invocation — an add-then-edit (or edit-then-edit) of the same pk collapses to its final
  // image, matching the engine head's own semantics for that pk. The record is replaced with
  // exactly the arguments given: the CALLERS (`edit`/`remove` below, consulting `prior`) decide
  // the pre-image per the H-ii coalescing matrix on {@link WriteRecord}. Keyed by the STAGED
  // (engine) table name, so the pending axis and the write-set match what the engine holds.
  const recordWrite = (engineTable: string, pk: WireValue[], row: WireValue[] | undefined, oldRow?: WireValue[]): void => {
    let byPk = writes.get(engineTable);
    if (!byPk) writes.set(engineTable, (byPk = new Map()));
    const pkKey = stableJson(pk);
    // Defensive copies: the wasm binding's returned arrays are not contractually immutable/unique,
    // so a captured record must not alias a cell array the engine could later reuse or mutate.
    byPk.set(pkKey, { table: engineTable, pk: [...pk], row: row ? [...row] : undefined, ...(oldRow ? { oldRow: [...oldRow] } : {}) });
  };

  // A full insert row: each cell is `obj[c]`, or `null` for an omitted nullable column (design 206
  // §6.2); a `json` object is stringified for the engine (`toCell`). Non-nullable columns are
  // guaranteed present by `checkColumns(full)`.
  const toCells = (table: string, obj: KeyedRow): WireValue[] => {
    const s = spec(table);
    return s.columns.map((c) => toCell(insertCell(obj, c), s.types[c]));
  };

  const toKeyed = (table: string, cells: WireValue[]): KeyedRow => {
    const out: KeyedRow = {};
    spec(table).columns.forEach((c, i) => (out[c] = cells[i]));
    return out;
  };

  // The raw, UN-recorded read primitive behind `getImpl`/`rowImpl` — the M1 local guard + the txn
  // read, nothing else. Slice B deliberately kept the keyed writers (`update`/`upsert`/
  // `insertIgnore`/`delete`) on this, un-recorded ("recording is about the PUBLIC read entry
  // points"). H-ii deliberately REVERSES that: the pre-existence probe each keyed writer BRANCHES
  // on is genuine value-dependence the §3 routing proof must see — the concrete silent-drop shape
  // is `update("cards", {id:5,…})` where the client's daemon slice has the row but the room's
  // footprint lacks it: the room-side update no-ops, the commit "succeeds" with zero effects, the
  // confirm retires the entry, and the user's edit silently vanishes. The proof can only refuse
  // that route if the probe is on the record. So the keyed writers now probe through `getImpl`
  // (recorded like any public read, §3.2 #3); the FOLD trap is unaffected — it wraps only the
  // returned object's `get`/`row`/`query` surface, so keyed writers stay fold-legal and the
  // trapped path (where `readLog` is never armed) records nothing, exactly as before.
  const rawGet = (table: string, pk: WireValue[]) => {
    assertNotLocal(table, "read");
    return tx.get(staged(table), pk) as WireValue[] | undefined;
  };

  // Push one {@link ReadRecord} when recording is armed (§3.2 #2/#3): outcome from `row`'s
  // presence. Pure capture for inspection.
  const recordRead = (table: string, pk: WireValue[], row: WireValue[] | undefined): void => {
    if (!readLog) return;
    readLog.reads.push({
      table,
      pk: [...pk],
      outcome: row === undefined ? "absent" : "present",
    });
  };

  // The PUBLIC positional read (§3.2 #2) — and, since H-ii, the keyed writers' pre-existence
  // probe (§3.2 #3, see the `rawGet` note): `rawGet` plus a `readLog` record when recording is
  // armed. A no-op record when `readLog` is omitted — exactly `rawGet`'s behavior then.
  const getImpl = (table: string, pk: WireValue[]): WireValue[] | undefined => {
    const result = rawGet(table, pk);
    recordRead(table, pk, result);
    return result;
  };

  // The PUBLIC keyed read (§3.2 #2), the `row` counterpart of `getImpl`.
  const rowImpl = (table: string, pk: KeyedRow): KeyedRow | undefined => {
    checkColumns(table, pk, false);
    const pkc = pkCells(table, pk);
    const cells = rawGet(table, pkc);
    recordRead(table, pkc, cells);
    return cells ? toKeyed(table, cells) : undefined;
  };

  // The pk's existing record from THIS invocation, if any — the coalescing-matrix input for
  // `edit`/`remove` below (see {@link WriteRecord}). Keyed by the STAGED name like the records.
  const prior = (table: string, pk: WireValue[]): WriteRecord | undefined =>
    writes.get(staged(table))?.get(stableJson(pk));
  const add = (table: string, row: WireValue[]) => {
    assertNotLocal(table, "write");
    const t = staged(table);
    recordWrite(t, pkFromCells(table, row), row);
    // ChildOps carry the WIRE name (unlike the write-set): the agg overlay's defs are keyed by
    // the ORIGINAL AST's child tables (`collectAggDefs`), and the `__agg_*` heads it feeds are
    // shared by plain and swapped views alike — a staged name would silently miss the dispatch
    // and the optimistic count would lag every room-declared write until its echo.
    onOp?.({ table, kind: "add", row });
    tx.add(t, row);
  };
  const remove = (table: string, row: WireValue[]) => {
    assertNotLocal(table, "write");
    const t = staged(table);
    const pk = pkFromCells(table, row);
    // The remove PRE-IMAGE (the H-ii matrix on {@link WriteRecord}): remove-after-edit/-remove
    // keeps the ORIGINAL captured pre-image (the txn-entry base — the net effect is a remove of
    // the row the external world last knew, never the edited transient). Otherwise (first touch,
    // or remove-after-add) the truthful full-width row is the txn-visible one — `tx.get` read
    // BEFORE the remove stages (read-your-writes: an add of this pk earlier in the SAME
    // invocation shows through). Falls back to the caller's asserted `row` when the pk is not
    // resident (a raw remove of an absent row) — a captured remove thus always carries a
    // full-width pre-image.
    const oldRow = prior(table, pk)?.oldRow ?? (tx.get(t, pk) as WireValue[] | undefined) ?? row;
    recordWrite(t, pk, undefined, oldRow);
    onOp?.({ table, kind: "remove", row }); // wire name — see `add`
    tx.remove(t, row);
  };
  const edit = (table: string, oldRow: WireValue[], newRow: WireValue[]) => {
    assertNotLocal(table, "write");
    const t = staged(table);
    const pk = pkFromCells(table, newRow);
    // The edit PRE-IMAGE (the H-ii matrix on {@link WriteRecord}). First touch: the txn-visible
    // row read BEFORE staging, falling back to the caller's asserted `oldRow` when the pk is not
    // resident (covers the pk-MOVING raw edit — the record is keyed by the NEW pk; the pre-image
    // carries the OLD row). Edit-after-edit: keep the FIRST pre-image (the txn-entry base).
    // Edit-after-add / edit-after-remove: the record collapses to a (re-)insert — NO pre-image
    // (the pk did not pre-exist this invocation's base).
    const p = prior(table, pk);
    const pre =
      p === undefined
        ? ((tx.get(t, pk) as WireValue[] | undefined) ?? oldRow)
        : p.row !== undefined && p.oldRow !== undefined
          ? p.oldRow
          : undefined;
    recordWrite(t, pk, newRow, pre);
    onOp?.({ table, kind: "edit", row: newRow, old: oldRow }); // wire name — see `add`
    tx.edit(t, oldRow, newRow);
  };

  // The folded read trap (§5): a mutator that reads to compute its write is refused. `() => never`
  // is assignable to the wider read signatures (extra args ignored, `never` widens to the result).
  const trapped = (): never => {
    throw new FoldReadError();
  };

  // One-shot query (203 §5.2): lower the builder to an AST, refuse any local-only table it
  // reads (M1 — same guard as `rawGet`), and run it over the engine's read-cache fork. The
  // wasm engine returns the rows already keyed by column name with their materialized
  // relationship children nested by name (`marshal::caught_node_to_js`), in the query's order —
  // identical in shape to `view.data` — so this is a pass-through.
  const runQuery = (q: QueryArg): QueryResultRow[] => {
    const ast = q.ast();
    for (const t of collectTables(ast)) assertNotLocal(t, "read");
    readLog?.queries.push(ast);
    // A room-declared mutator's one-shot query reads the room's own staged state for the tables
    // the room owns (the same staging rule as the point reads above).
    return tx.query(stage !== undefined && stage.size > 0 ? remapAstTables(ast, stage) : ast) as QueryResultRow[];
  };

  return {
    get: trapReads ? trapped : getImpl,
    query: trapReads ? trapped : runQuery,
    add,
    remove,
    edit,
    row: trapReads ? trapped : rowImpl,
    insert: (table, row) => {
      checkColumns(table, row, true);
      add(table, toCells(table, row));
    },
    // The keyed writers' pre-existence probes go through `getImpl` — RECORDED reads since H-ii
    // (§3.2 #3): each writer BRANCHES on the probe, a value-dependence the routing proof must see
    // (the silent-drop rationale on `rawGet` above). Fold-legal exactly as before (the trap wraps
    // the public surface above, never these), and byte-identical when recording is off.
    update: (table, row) => {
      checkColumns(table, row, false);
      const current = getImpl(table, pkCells(table, row));
      if (!current) return; // rebase-friendly: the row may have vanished upstream
      const s = spec(table);
      // Named columns overwrite (a `json` object stringified via `toCell`); unnamed keep `current`,
      // which the engine already holds as a wire value (a json string), so no re-encode.
      const next = s.columns.map((c, i) => (c in row ? toCell(row[c], s.types[c]) : current[i]));
      edit(table, current, next);
    },
    upsert: (table, row) => {
      checkColumns(table, row, true);
      const current = getImpl(table, pkCells(table, row));
      if (current) edit(table, current, toCells(table, row));
      else add(table, toCells(table, row));
    },
    insertIgnore: (table, row) => {
      checkColumns(table, row, true);
      if (!getImpl(table, pkCells(table, row))) add(table, toCells(table, row));
    },
    delete: (table, pk) => {
      checkColumns(table, pk, false);
      const current = getImpl(table, pkCells(table, pk));
      if (!current) return; // rebase-friendly no-op
      remove(table, current);
    },
  };
}

function toServerOp(m: Mutation): ServerDeltaOp {
  if (m.op === "add") return { table: m.table, type: "add", row: m.row };
  if (m.op === "remove") return { table: m.table, type: "remove", row: m.row };
  return { table: m.table, type: "edit", row: m.new, old: m.old };
}

/** Every base table a query's AST can draw rows from: the root + every related
 *  subquery + EXISTS subqueries (a conservative deep scan for `table` fields). */
function collectTables(ast: Ast): Set<string> {
  const out = new Set<string>();
  const walk = (v: unknown): void => {
    if (Array.isArray(v)) {
      for (const x of v) walk(x);
    } else if (v && typeof v === "object") {
      const o = v as Record<string, unknown>;
      if (typeof o.table === "string") out.add(o.table);
      for (const k of Object.keys(o)) walk(o[k]);
    }
  };
  walk(ast);
  return out;
}

function intersects(a: Set<string>, b: Set<string>): boolean {
  for (const x of b) if (a.has(x)) return true;
  return false;
}
