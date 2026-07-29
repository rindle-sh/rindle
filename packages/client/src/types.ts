// Wire + protocol types for the flat-change client core (WASM-CLIENT-DESIGN.md §2, §6).
//
// Cells cross the boundary BARE (the host has a typed schema, so per-column types are
// known here, not per-cell). JSON columns cross as their raw JSON string.

import type { Ast } from "./ast.ts";

/** A bare wire cell. (JSON columns arrive as their raw JSON string.) */
export type WireValue = number | string | boolean | null;

/** Declared column type (from the typed schema) — drives the comparator + JSON parsing.
 *  `"int64"` is the exact-i64 column plane (design 226): the vocabulary exists from
 *  Stage C4 (codegen can name it), but no exact integer cell crosses this wire — and
 *  `WireValue` gains no `bigint` — until Stage E lands the browser bigint plane; until
 *  then the daemon refuses IVM queries whose footprint touches such a column. */
export type ColType = "string" | "number" | "boolean" | "json" | "int64";

/** A scalar-projection annotation on a relationship slot (REDUCE-DESIGN.md §9): a
 *  relationship aggregate (`issue { commentCount: count(comments) }`) lives as a singular
 *  one-row relationship whose child is the aggregate row; this tells the receiver to unwrap
 *  it into a scalar field — `commentCount: 5` instead of `commentCount: [{ count: 5 }]`. */
export interface WireProjection {
  /** Which CHILD column to surface as the scalar value (the aggregate column). */
  col: number;
  /** The value to emit when the relationship is empty (a childless parent) — the
   *  aggregate identity (`0` for count, `null` for sum/avg). */
  identity: WireValue;
}

export interface WireRel {
  name: string;
  slot: number;
  /** `null` ⇒ an out-of-view / gating slot (the in-view gate drops `Child`s addressed here). */
  child: WireSchema | null;
  /** Non-null ⇒ a scalar-projected relationship aggregate the receiver unwraps to a scalar
   *  (REDUCE-DESIGN.md §9). Absent/`null` for an ordinary (plural or `.one()`) relationship. */
  project?: WireProjection | null;
}

export interface WireSchema {
  columns: string[];
  primaryKey: number[];
  /** Resolved, PK-completed sort: `[columnIndex, ascending]` pairs (the comparator input). */
  sort: [number, boolean][];
  singular: boolean;
  relationships: WireRel[];
}

export interface WireNode {
  row: WireValue[];
  rels: { rel: number; children: WireNode[] }[];
}

export interface PathSeg {
  rel: number;
  parentRow: WireValue[];
}

/** A positional change at one level of the view tree. A `remove` ships only the leaving `row`
 *  (the receiver already holds the subtree and locates it by key) — `node` is NEVER on the wire;
 *  it is an OPTIONAL client-side enrichment carrying the full removed subtree, attached by the
 *  ArrayView when a change consumer opts in (`Store.subscribeChanges(_, { removedSubtree: true })`),
 *  so a narrator can resolve a removed row's nested subs just as it can on an `add`. */
export type FlatOp =
  | { tag: "add"; node: WireNode }
  | { tag: "remove"; row: WireValue[]; node?: WireNode }
  | { tag: "edit"; old: WireValue[]; new: WireValue[] };

export interface FlatChange {
  path: PathSeg[];
  op: FlatOp;
}

/**
 * What a {@link Backend} pushes per query: the handshake (`hello`), the (possibly
 * chunked) hydrate `snapshot`, then incremental `batch`es. The core builds an ArrayView
 * on `hello`, hydrates on `snapshot`, folds on `batch` — identically for any backend.
 */
export type ChangeEvent =
  | { type: "hello"; schema: WireSchema; comparatorVersion: number }
  | { type: "snapshot"; adds: FlatChange[]; last: boolean }
  // `catchUp` marks a batch that is really a query's INITIAL hydration delivered as a delta — the
  // optimistic backend hydrates a fresh query through its reconcile cycle (a `serverBatchEnd`), so
  // the whole first result set arrives as a `batch`, not a `snapshot`. The Store maps a catch-up
  // batch to the `snapshot` change-phase so a narrator's "what CHANGED" default ignores the initial
  // rows instead of narrating every one as a fresh add. A normal incremental batch omits it.
  | { type: "batch"; events: FlatChange[]; catchUp?: boolean };

export type Mutation =
  | { op: "add"; table: string; row: WireValue[] }
  | { op: "remove"; table: string; row: WireValue[] }
  | { op: "edit"; table: string; old: WireValue[]; new: WireValue[] };

/**
 * A table-tagged, path-free row delta — the **normalized** wire payload (the path-free twin
 * of {@link FlatChange}; NORMALIZED-CHANGES-DESIGN.md §3). Rows are positional (bare cells; a
 * json column is its raw JSON string). `op` is the discriminant. Lives here (not in
 * `@rindle/normalized`) so both the protocol (`@rindle/remote`) and the sync layer share one type.
 */
export type NormalizedOp =
  | { table: string; op: "add"; row: WireValue[] }
  | { table: string; op: "remove"; row: WireValue[] }
  | { table: string; op: "edit"; old: WireValue[]; new: WireValue[] };

/** One base table's flat schema on the normalized `hello` (§3): column names (in order) +
 *  primary-key column indices. Wire rows are positional against `columns`. */
export interface NormalizedTableSchema {
  name: string;
  columns: string[];
  primaryKey: number[];
}

/**
 * A per-query NORMALIZED stream event — the path-free twin of {@link ChangeEvent}. The
 * `hello` carries the flat per-table schemas (no nested view schema, §3); `snapshot`/`batch`
 * carry table-tagged {@link NormalizedOp}s. The `NormalizedSync` layer folds these into the
 * local engine's base tables.
 *
 * `cv` (the global commit version the frame's data reflects — OPTIMISTIC-WRITES-DESIGN.md
 * §8.3/§8.6) is present on sources that speak the optimistic protocol; the plain normalized
 * path may omit it.
 */
export type NormalizedEvent =
  | { type: "hello"; tables: NormalizedTableSchema[]; comparatorVersion: number; normalizedFp: string }
  | { type: "snapshot"; ops: NormalizedOp[]; cv?: number }
  | { type: "batch"; ops: NormalizedOp[]; cv?: number };

export type QueryId = number;

/** A dev-only authoritative server delta exposed by backends that can distinguish the upstream
 *  server stream from their local view/IVM stream. Flat remote backends surface clean
 *  {@link ChangeEvent}s; local-first normalized/optimistic backends surface the path-free
 *  {@link NormalizedEvent}s before they are folded into the local engine. */
export type BackendServerDelta =
  | { format: "flat"; event: ChangeEvent }
  | { format: "normalized"; event: NormalizedEvent };

/** Dev-only passive tap over a backend's authoritative server stream. Optional because in-process
 *  backends have no server stream, and older/custom backends may only expose the Store tap. */
export interface BackendDevObserver {
  onServerDelta?(qid: QueryId, ev: BackendServerDelta): void;
}

/** A query's SERVER-CHANNEL state, surfaced on its {@link ArrayView} (FOLDED-MUTATIONS-DESIGN §7 —
 *  formerly conflated with pending-ness, OPTIMISTIC-WRITES-DESIGN.md §6):
 *   - `unknown`  — not hydrated: the server has not produced a first result for this query yet;
 *   - `complete` — the server has answered. STAYS `complete` while a local mutation is pending (the
 *                  prediction is the client's best current answer); reversion on rejection is an
 *                  event (`onRejected`), not a downgrade of completeness;
 *   - `error`    — RESERVED for a future server-side, query-level error signal
 *                  (see `designs/QUERY-ERRORS-DESIGN.md`); no longer produced by a pending mutation.
 *  "Is a prediction pending here?" is now a separate reactive axis (the backend's `pending(qid)` /
 *  `onPending`), not folded into this type. A backend with no server lifecycle (the in-process
 *  engine) leaves every view `complete`. */
export type ResultType = "unknown" | "complete" | "error";

/** The network identity for a named query subscription. The local AST remains local; remote
 *  normalized/optimistic sources use only this `(name,args)` pair upstream. */
export interface RemoteQuery {
  name: string;
  args: unknown;
}

/**
 * The seam the core talks to. Backend-agnostic: the same interface for the in-process
 * WASM backend and a remote (network) backend. The core never knows which. A backend
 * pushes a per-query {@link ChangeEvent} stream via {@link Backend.onEvent}; the remote
 * backend additionally owns the epoch/seq/gap protocol and emits only clean, in-order
 * events (so the ArrayView never sees the wire protocol).
 */
export interface Backend {
  registerQuery(queryId: QueryId, ast: unknown, remote?: RemoteQuery): void;
  unregisterQuery(queryId: QueryId): void;
  /** Optional split path for local-first backends: retain/release a named remote footprint
   *  without creating another local materialized view. `localQueryId`, when provided, names
   *  the local AST view this remote footprint feeds. `ast`, when provided, gives the backend
   *  the local schema context for sync-only retains that still need aggregate/table setup. */
  retainRemoteQuery?(queryId: QueryId, remote: RemoteQuery, localQueryId?: QueryId, ast?: Ast): void;
  releaseRemoteQuery?(queryId: QueryId): void;
  /** Local: applies now (changes flow back on the stream). Remote: sends to the server. */
  mutate(mutations: Mutation[]): Promise<void>;
  /** Optional DIRECT-COMMIT path for LOCAL-only tables (`201-LOCAL-ONLY-TABLES-DESIGN.md` §6):
   *  applies the writes straight to the engine, OUTSIDE the optimistic pending stack (a local
   *  table is untracked, so it never rebases). The backend rejects any synced/tracked table (M2).
   *  Backends with no local-table support (a plain remote sync backend) omit it.
   *
   *  `onCommitted` (207 §5.1) runs after the engine commit is applied but BEFORE subscriber
   *  delivery: a subscriber throwing during delivery re-raises out of this call, and a caller
   *  that must stay coherent with the engine (the persistence tap) cannot tell that throw from
   *  a pre-commit rejection — the callback can, because it fires exactly when the commit is in. */
  writeLocal?(mutations: Mutation[], onCommitted?: () => void): void;
  onEvent(handler: (queryId: QueryId, event: ChangeEvent) => void): void;
  /** Optional: the backend pushes per-query {@link ResultType} changes here and the core routes
   *  each to the matching view (so `view.resultType` tracks it). Backends with no lifecycle (the
   *  in-process engine) omit it and every view stays `complete`. */
  onResultType?(handler: (queryId: QueryId, resultType: ResultType) => void): void;
  /** Optional: brackets one commit's coherent multi-query delivery so the Store can fold every
   *  affected view BEFORE notifying any subscriber — cross-view-atomic notification. The
   *  in-process engine derives the whole commit's per-query deltas against one consistent post-
   *  commit snapshot, then dispatches them one query at a time; without a barrier a subscriber on
   *  the first-dispatched view, re-reading a sibling view in its callback, would observe that
   *  sibling's PRE-commit state (it has not folded yet). The backend calls the handler with
   *  `"begin"` before delivering a commit's per-query `batch` events (via {@link onEvent}) and
   *  `"end"` after the last one; between them the Store folds each view but DEFERS its
   *  notification, firing every affected view's subscribers together at `"end"`. Backends with no
   *  synchronous multi-query commit boundary (a plain remote backend, where each query's frame
   *  arrives in its own message) omit it — the Store then notifies per event, exactly as before. */
  onCommitBoundary?(handler: (phase: "begin" | "end") => void): void;
  /** Optional dev-only authoritative server stream tap. It is additive, like
   *  `Store.subscribeChanges`, and must not displace the backend's normal event handler. */
  __attachDevtoolsServerDeltas?(observer: BackendDevObserver): () => void;
}

/**
 * The server side of the **normalized** local-first path (NORMALIZED-CHANGES-DESIGN.md §5/§7):
 * registers queries and pushes each one's normalized footprint stream ({@link NormalizedEvent}s).
 * Both the in-process native (`@rindle/replica`) source and the ws (`@rindle/remote`
 * `RemoteNormalizedSource`) implement it; `@rindle/normalized`'s `NormalizedBackend` consumes one,
 * never knowing which — a sibling seam of {@link Backend} for the normalized composition.
 */
export interface NormalizedSource {
  registerQuery(queryId: QueryId, remote: RemoteQuery): void;
  unregisterQuery(queryId: QueryId): void;
  /** Send base-table writes to the server; the authoritative stream flows back. */
  mutate(mutations: Mutation[]): Promise<void>;
  onNormalized(handler: (queryId: QueryId, event: NormalizedEvent) => void): void;
  /** Optional: the backend hands its OWN typed per-table schemas so the source can validate
   *  each server `hello` against them (column order / PK by name) and reject a schema skew
   *  rather than silently transposing positional cells (CRIT#4 / §3 "drift ⇒ re-subscribe").
   *  Sources that can't skew (the in-process native source) may omit it. */
  expectClientSchema?(tables: NormalizedTableSchema[]): void;
}

/** The upstream named-mutator envelope (OPTIMISTIC-WRITES-DESIGN.md §8.1): the wire
 *  carries the name + JSON args, never code; `mid` totally orders a client's mutations. */
export interface MutationEnvelope {
  clientID: string;
  mid: number;
  name: string;
  args: unknown;
}

/** A room authority's verdict for a NON-applied mutation (RINDLE-REALTIME-QUERY-ENABLEMENT
 *  §3.3, the deopt handshake; Slice H-iv-b server half / H-v client half). Sent on the author's
 *  own socket for every mutation the room did NOT apply, always BEFORE the lmid ack that burns
 *  the `mid` (same-socket ordering only — the ack may reach the client through another path
 *  first, e.g. a replayed lmid snapshot). Applied mutations send NOTHING: silence + lmid
 *  coverage ⇒ applied.
 *
 *  - `kind: "deopt"` — the room's §3.3 commit gate refused the routed mutation (or its
 *    environment fell short, e.g. `tx.query`); the mid is burnt in the ROOM ledger with zero
 *    effects and the client re-enqueues the same logical mutation onto the daemon stream.
 *    `name`/`args` are echoed so the frame is SELF-CONTAINED: a client that already retired the
 *    entry (the burnt-mid confirm won the race, or the frame is a replay re-answer) re-invokes
 *    from the frame alone.
 *  - `kind: "rejected"` — FINAL (authz/validation): the mid is burnt the same way and the
 *    prediction snaps back on the ordinary lmid release; no re-route.
 *
 *  `reason` may be absent on a re-answered frame whose record was seeded from journal replay
 *  (the verdict is journaled; the reason is not). */
export interface MutationOutcomeFrame {
  mid: number;
  kind: "deopt" | "rejected";
  reason?: string;
  /** Echoed on DEOPT frames only (self-contained re-invoke — see above). */
  name?: string;
  args?: unknown;
}

/** The connection-level progress frame (§8.6): advances the coherent-apply release point
 *  (`cvMin`). A pure release signal — mutation confirmation does NOT ride it: `lmid` is a
 *  row in {@link CLIENT_MUTATIONS_TABLE}, delivered through the client's own per-client
 *  system query ({@link LMID_QUERY_NAME}) like any data, so it is released by the same
 *  `cvMin` as the commit's effects (transactionally coherent by construction). */
export interface ProgressFrame {
  cvMin: number;
}

/** The replicated bookkeeping table carrying each client's high-water mutation id
 *  (`lmid`). Engine-hosted and served like any base table; reserved (never part of a
 *  user schema). Columns: `[client_id, last_mutation_id]`, PK `client_id`. */
export const CLIENT_MUTATIONS_TABLE = "_rindle_client_mutations";

/** The reserved server-query name every optimistic client subscribes at connect: the
 *  one-row system query `CLIENT_MUTATIONS_TABLE WHERE client_id = <me>`. The server
 *  derives the identity from the connection (args are ignored). */
export const LMID_QUERY_NAME = "_rindle/clientLmid";

/** The system table's wire schema — appended to the client's expected schemas so the
 *  lmid query's `hello` passes CRIT#4 validation. */
export const CLIENT_MUTATIONS_SCHEMA: NormalizedTableSchema = {
  name: CLIENT_MUTATIONS_TABLE,
  columns: ["client_id", "last_mutation_id"],
  primaryKey: [0],
};

/**
 * The server side of the OPTIMISTIC path (OPTIMISTIC-WRITES-DESIGN.md §8): the
 * {@link NormalizedSource} stream with `cv`-tagged data frames, plus the connection-level
 * {@link ProgressFrame} channel and the named-mutator upstream. The client buffers data
 * frames by `cv` and applies all `cv ≤ cvMin` as one coherent release (§8.5).
 */
export interface OptimisticSource {
  registerQuery(queryId: QueryId, remote: RemoteQuery): void;
  unregisterQuery(queryId: QueryId): void;
  /** Ship one named-mutator invocation upstream (§8.1). Confirmation rides the progress frames. */
  pushMutation(envelope: MutationEnvelope): Promise<void>;
  onNormalized(handler: (queryId: QueryId, event: NormalizedEvent) => void): void;
  onProgress(handler: (frame: ProgressFrame) => void): void;
  /** Optional: the backend hands its OWN typed per-table schemas so the source validates each
   *  server `hello` against them and rejects a schema skew (CRIT#4); see {@link NormalizedSource}. */
  expectClientSchema?(tables: NormalizedTableSchema[]): void;
  /** Optional: fired when the server restarts (a transport that can detect it, e.g. via a daemon
   *  boot id). The backend resets its `cv` watermark so the server's reset `cv` sequence is
   *  accepted rather than dropped as stale. In-process sources never restart and omit it. */
  onRestart?(handler: () => void): void;
  /** Optional (Slice H-v): the channel's {@link MutationOutcomeFrame} stream — the room deopt
   *  handshake's client half. **OUT-OF-BAND BY DESIGN**: the frame carries no `cv` and the source
   *  MUST dispatch it immediately on arrival, never behind the cv buffer — a deopt has to migrate
   *  its pending entry BEFORE the buffered lmid release that would otherwise retire it as a
   *  success (and the §7.3 hold-back trigger, keyed on the entry's confirming domain, would then
   *  park its staged writes the wrong way). Sources whose authority never deopts (the in-process
   *  native source, a plain daemon) omit it. */
  onMutationOutcome?(handler: (frame: MutationOutcomeFrame) => void): void;
  /** Optional (Slice H-v, the §7.5 rule-3 crash-window closer): fired when the transport
   *  RE-establishes its session (reconnect → re-`init`), BEFORE any post-reconnect frame is
   *  processed — the ordering is load-bearing: the re-subscribed lmid stream's fresh snapshot may
   *  cover a mid whose `mutationOutcome` frame died with the old socket, and once that release
   *  retires the entry as an apparent success there is nothing left to re-send. The backend
   *  re-sends this domain's unconfirmed pending envelopes with their ORIGINAL mids, in mid order;
   *  the source may DEFER their delivery until the session is re-authorized (a room's
   *  `pushMutation` requires the lease-token subscribe's subject). Idempotent under the domain's
   *  own ledger — a processed mid dedups silently (silence + lmid coverage ⇒ applied), a
   *  non-applied mid is re-answered from the authority's recorded-outcome map (resolving even an
   *  already-retired entry through the handshake's not-found arm). Distinct from
   *  {@link onRestart} (a NEW server incarnation): a same-incarnation socket drop re-syncs
   *  without restarting, and envelopes sent into the dead socket are exactly what this recovers.
   *  In-process sources never drop a session and omit it. */
  onResync?(handler: () => void): void;
}
