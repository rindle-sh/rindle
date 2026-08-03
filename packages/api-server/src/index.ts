import { driveMutationAsync, insertCell, insertPlan, isGeneratorMutator, isoTx, toCell } from "@rindle/client";
import type {
  Ast,
  ColType,
  Condition,
  KeyedRow,
  MutationEnvelope,
  MutationOp,
  MutatorCtx,
  NamedQuery,
  Query,
  QueryResultRow,
  Schema,
  SharedMutator,
  SharedMutatorWithArgs,
  ServerWriteTx,
} from "@rindle/client";
import { DaemonHttpError, HttpRindleDaemonClient } from "@rindle/daemon-client";
import { createSqlClient, encodeSqlValue, RindleSqlError } from "@rindle/sql-client";
import type {
  ClientOptions as SqlClientOptions,
  MutationReceipt as SqlMutationReceipt,
  MutationRows as SqlMutationRows,
  SqlClient,
  SqlMutationTransaction,
  SqlSession,
  Statement as PublicSqlStatement,
} from "@rindle/sql-client";
import { compile as compileQueryAst } from "@rindle/query-compiler";
import type { Catalog, ColumnType as QueryColumnType, TableSchema } from "@rindle/query-compiler";
import type {
  ClaimRoomEpochInput,
  ClaimRoomEpochOutput,
  DematerializeInput,
  DematerializeOutput,
  MaterializationPolicy,
  MaterializeInput,
  MaterializeOutput,
  MigrateInput,
  MigrateOutput,
  MutationRejection,
  MutationRejectionOutput,
  MutationSessionBegin,
  MutationSessionBeginOutput,
  MutationSessionExec,
  MutationSessionQuery,
  MutationSessionRef,
  QueryOnceInput,
  QueryOnceOutput,
  RindleDaemonClient,
  RoomLmidsInput,
  RoomLmidsOutput,
  RowChangeTxn,
  RowChangeTxnOutput,
  SqlRead,
  SqlReadOutput,
  SqlStatement,
  SqlTxn,
  SqlTxnOutput,
  StreamMode,
  WireValue,
} from "@rindle/daemon-client";

import {
  assertLabeledProfilesExist,
  assertUnwindowedFootprint,
  attachRealtimeLabel,
  compileRoomProfiles,
  compileRoomScopeSpecs,
  compileRoomTableSpecs,
  mintRoomDoc,
  queryRealtimeLabel,
  queryResultToAst,
  splitRoomDoc,
} from "./rooms.ts";
import type { RoomProfile, RoomScopeSpec, RoomTableSpec } from "./rooms.ts";
import {
  STREAM_SSE_HEADERS,
  StreamForbidden,
  StreamPlane,
  resolveStreamColumns,
  streamFramesToSse,
  streamRequestFromHttp,
} from "./streams.ts";
import type {
  OpenStreamInput,
  RindleStreamOptions,
  StreamHandle,
  StreamSubscription,
  StreamTables,
  SubscribeStreamInput,
} from "./streams.ts";
// The room lease token (RINDLE-REALTIME §10.1): minted here, verified by the room SHELL against
// its `downstream.tokenKeys` ring — the `/token` subpath is pure WebCrypto (no wasm, no shell).
// Loaded LAZILY at the first mint: `@rindle/room` is an OPTIONAL dependency (see package.json), so
// it is installed transitively — a consumer bundling api-server (Vite/Rollup/esbuild) can resolve
// this dynamic import even when it never uses rooms — yet the mint only runs when
// `realtime.roomTokenKey` is configured. Should the module be genuinely absent (an install that
// skipped the optional dep), the serve decision fail-opens: daemon-served leases plus a one-time
// warning naming the missing module. It is NOT a hard `dependency` because the entire code path is
// optional; optionalDependencies keeps a failed install of it non-fatal.
import type { mintRoomToken as MintRoomToken, scopeSpecsHash as ScopeSpecsHash } from "@rindle/room/token";
let roomTokenModule: { mintRoomToken: typeof MintRoomToken; scopeSpecsHash: typeof ScopeSpecsHash } | undefined;
async function loadRoomTokenModule(): Promise<{ mintRoomToken: typeof MintRoomToken; scopeSpecsHash: typeof ScopeSpecsHash }> {
  if (roomTokenModule === undefined) {
    const m = await import("@rindle/room/token");
    roomTokenModule = { mintRoomToken: m.mintRoomToken, scopeSpecsHash: m.scopeSpecsHash };
  }
  return roomTokenModule;
}

// Re-export the shared (generator) mutator seam so an app builds its server mutators from ONE import:
// co-locate each body with its arg schema (`shared`), bulk-drive the registry ({@link sharedApiMutators}),
// keeping only server-only authority as explicit overrides (see MUTATORS-ISOMORPHIC).
export { isoTx, shared } from "@rindle/client";
export type { ArgSchema, IsoTx, MutationGen, MutatorCtx, SharedMutator, SharedMutatorWithArgs } from "@rindle/client";

// The room-profile declaration layer (RINDLE-REALTIME-QUERY-ENABLEMENT §2, slice G-iv-a). The
// compiled-profile shapes stay internal to `./rooms.ts` — G-iv-b consumes them in-package.
// `RoomTableSpec` (G-iv-b) is public: it rides the lease wire (`QueryLeaseResponse.realtime`).
// `RoomScopeSpec` (H-iv-b) is public: it rides the boot wire (`RoomBootResponse.scopes`).
export { queryRealtimeLabel, queryResultToAst } from "./rooms.ts";
export type { RoomProfile, RoomScopeSpec, RoomTableSpec } from "./rooms.ts";
export type { RealtimeQueryLabel } from "@rindle/client";

// The LM stream plane (designs-implemented/LM-STREAM-CHECKPOINT-DESIGN.md): a model response runs on two planes
// — every delta straight to subscribers, one chunk row per coarse checkpoint to the app's tables —
// joined by one monotone `seq`. The plane itself (`StreamPlane`) stays internal; the api-server owns
// it and feeds it the backend's outside-transaction SQL surface (checkpoints are SYSTEM writes: no
// clientID, no mid, no lmid).
export {
  STREAM_SSE_HEADERS,
  STREAM_STATUS_STREAMING,
  StreamOpenRefused,
  StreamRelayConform,
  assembleDurableText,
  frameResumePoint,
  spliceStreamText,
  streamChunkId,
  streamChunkTableDdl,
  streamFramesToSse,
  streamRequestFromHttp,
} from "./streams.ts";
export type {
  AuthorizeStreamInput,
  OpenStreamInput,
  RindleStreamOptions,
  StreamCheckpointPolicy,
  StreamCheckpointTarget,
  StreamColumns,
  StreamCommit,
  StreamCommitInput,
  StreamFrame,
  StreamHandle,
  StreamRelay,
  StreamRelayErrorInfo,
  StreamStatus,
  StreamSubscription,
  StreamTables,
  SubscribeStreamInput,
} from "./streams.ts";

export const DEFAULT_RINDLE_API_ROUTES = {
  query: "/api/rindle/query",
  read: "/api/rindle/read",
  mutate: "/api/rindle/mutate",
  // The room write-authority host (RINDLE-REALTIME-DESIGN.md §5.3.1): the room's
  // SOLE flush counterpart — the store's own handler is private ingress behind these.
  applyRowChangeTxn: "/api/rindle/apply-row-change-txn",
  claimRoomEpoch: "/api/rindle/claim-room-epoch",
  roomLmids: "/api/rindle/room-lmids",
  // The DO shell's cold-boot callback (§10.1) — active only when `realtime` is configured.
  roomBoot: "/api/rindle/room-boot",
  // The LM stream subscribe leg (LM-STREAM-CHECKPOINT §4) — active only when `streams` is
  // configured. GET + `EventSource` is the intended shape (`Last-Event-ID` IS the resume offset).
  stream: "/api/rindle/stream",
} as const;

export type MaybePromise<T> = T | PromiseLike<T>;

export interface ApiContext<User> {
  user: User;
  request?: unknown;
}

export type ApiQueryResult = Ast | Query<any, any, any>;
export type ApiQuery<User, Args> = (ctx: ApiContext<User>, args: Args) => MaybePromise<ApiQueryResult>;
export type ApiQueries<User> = Record<string, ApiQuery<User, any>>;

export interface RunQueryInput<User> {
  user: User;
  name: string;
  args: unknown;
  query: ApiQuery<User, any>;
  context: ApiContext<User>;
}

export type RunQuery<User> = (input: RunQueryInput<User>) => MaybePromise<ApiQueryResult>;

export interface AuthorizeQueryInput<User> {
  user: User;
  name: string;
  args: unknown;
  context: ApiContext<User>;
}

export interface AuthorizeMutationInput<User> {
  user: User;
  envelope: MutationEnvelope;
  context: ApiContext<User>;
}

export type Authorizer<T> = (input: T) => MaybePromise<boolean | void>;

export interface MutationContext<User> {
  user: User;
  envelope: MutationEnvelope;
  daemon: RindleDaemonClient;
  request?: unknown;
}

/** A deliberately narrow raw-SQL facade exposed by the API server. On {@link ServerMutationTx}
 *  it is bound to the open mutation transaction; on {@link MutationScope} each call runs in its
 *  own transaction outside the mutation boundary. Column aliases should be unique: positional
 *  driver rows are keyed by column name, so a duplicate alias is represented by its last value. */
export interface ServerSql {
  /** Queue/execute one statement. A transaction-bound call commits with the surrounding mutation. */
  execute(sql: string, params?: readonly WireValue[]): Promise<void>;
  /** Queue/execute an ordered statement batch. An empty batch is a no-op. On an
   *  outside-transaction surface ({@link MutationScope.sql}, `backend.outsideSql`) the batch MUST
   *  execute as ONE atomic transaction — all statements or none. Every built-in backend does (the
   *  daemon's `execute-sql-txn`, the sql-client's `/v1/sql/batch`, the Postgres plugger's
   *  BEGIN/COMMIT); a custom backend that loops statements without a transaction silently breaks
   *  the stream plane's chunk+CAS and compaction invariants, which ride single `batch` calls. */
  batch(statements: readonly SqlStatement[]): Promise<void>;
  /** Run a read and return rows keyed by their column names. */
  query<Row = Record<string, unknown>>(sql: string, params?: readonly WireValue[]): Promise<Row[]>;
}

/** The raw-SQL escape hatch for relational/authority statements a keyed op can't express — an
 *  owner-gated cascade, a `NOT EXISTS` dedup. Prefer `tx.sql`; `exec` remains the synchronous
 *  compatibility shorthand for a queued `tx.sql.execute`, and `statements` is the raw write list. */
export interface SqlMutationTx {
  readonly sql: ServerSql;
  exec(sql: string, params?: WireValue[]): void;
  readonly statements: readonly SqlStatement[];
}

/** The write handle a server mutator runs against — the ASYNC twin of the client's `MutationTx`. It
 *  is both the isomorphic {@link ServerWriteTx} logical surface (insert/update/upsert/insertIgnore/
 *  delete/row, rendered to dialect SQL) AND the legacy {@link SqlMutationTx} raw escape hatch. Both
 *  implementations run reads through the OPEN transaction (read-your-writes): Postgres executes
 *  everything live; the SQL-client and daemon adapters accumulate writes and lazily upgrade to an
 *  interactive mutation session at the first read (DAEMON-INTERACTIVE-TXN-DESIGN.md §5). */
export interface ServerMutationTx extends ServerWriteTx, SqlMutationTx {
  /** Run a full query (a fluent `Query` or its wire `Ast`) INSIDE the open transaction —
   *  read-your-writes, like {@link ServerWriteTx.row} but for arbitrary shapes. Returns the
   *  parsed nested result tree: an array for a plural root, an object or `null` for a `.one()`
   *  root, with cells in their raw SQLite storage-class representations (the same vocabulary
   *  `row` speaks). Remote SQLite backends: compiled by `@rindle/query-compiler`'s sqlite dialect
   *  (bind params, NO casts — §5.4) and executed through the mutation session. Postgres: lands with
   *  the read-compiler catalog integration (POSTGRES-READ-COMPILER-DESIGN.md Phase B). */
  query(q: Ast | Query<any, any, any>): Promise<unknown>;
}

export type ApiMutatorResult = void | SqlStatement[] | SqlTxn;
/** A server mutator: a plain async function against the live {@link ServerMutationTx}. Two ways to
 *  write one (MUTATORS-ISOMORPHIC): drive it directly (the raw escape hatch — an owner-gated cascade,
 *  a `NOT EXISTS` dedup — plus a returned `SqlStatement[]`/`SqlTxn`), OR delegate to a SHARED generator
 *  (the SAME body the client predicts) via {@link runSharedMutation}, keeping only the server-only
 *  authority (arg parse, principal, policy) in the wrapper. */
export type ApiMutator<User, Args> = (
  tx: ServerMutationTx,
  args: Args,
  ctx: MutationContext<User>,
) => MaybePromise<ApiMutatorResult>;

// --------------------------------------------------------------------------- scoped (outside-tx) mutators
//
// The tx-form {@link ApiMutator} above runs ENTIRELY inside the transaction. A SCOPED mutator
// (WORK-OUTSIDE-TX) instead controls the boundary itself: it receives a {@link MutationScope}, runs
// server-only code BEFORE opening the one atomic transaction (`scope.transact`), and MAY run code
// AFTER it commits. The outside-tx code is server-only by nature (the client's optimistic prediction
// can't call Stripe), so it lives HERE, never in the isomorphic body — the shared generator stays
// pure and identical on both tiers; server-computed values flow into it through `ctx`, exactly like
// `ctx.user` (undefined/predicted on the client, authoritative here).

/** Thrown by {@link MutationScope.transact} when the transacted body BUSINESS-rejects: the data
 *  rolled back and `lmid` advanced alone (§2.4). Catch it to COMPENSATE an outside-tx side effect
 *  (refund the charge), then rethrow or return — the mutation's protocol outcome is already sealed
 *  as rejected, so a post-reject throw can't change it. A DB/infra failure is NOT this — it
 *  propagates as the raw driver error (the client retries; `lmid` did not advance). */
export class MutationRejected extends Error {
  readonly reason: string;
  constructor(reason: string) {
    super(reason);
    this.name = "MutationRejected";
    this.reason = reason;
  }
}

/** The per-mutation server handle a {@link ScopedMutator} runs against. Code before {@link transact}
 *  runs OUTSIDE the transaction; code after a clean `transact` runs AFTER the commit. The
 *  `lmid`-always-advances invariant is the HARNESS's, not the author's: {@link RindleApiServer.pushMutation}
 *  seals the response from this handle's recorded outcome, so an early return, a never-called
 *  `transact`, or a swallowed {@link MutationRejected} still advances `lmid` and never wedges the
 *  client's pending queue. */
export interface MutationScope {
  /** Raw SQL OUTSIDE the mutation transaction. Every call commits independently and therefore may
   *  be observed even if {@link transact} later rejects or fails. Calls may also repeat when an
   *  envelope is retried, so outside writes need their own idempotency key/unique constraint. */
  readonly sql: ServerSql;
  /** Open the ONE atomic write transaction and drive `body` inside it, committing (stamping `lmid`
   *  co-transactionally) on a clean return. MAY be called at most once — a second call throws.
   *
   *  Two forms:
   *   - `transact(sharedMutator, args, ctx)` — drive a SHARED (generator) mutator (the same body the
   *     client predicts); pass the already-parsed `args` and the server `ctx` (fold server-only
   *     values like a charge id into `ctx` here).
   *   - `transact(run)` — a raw callback receiving the live {@link ServerMutationTx} (the escape
   *     hatch: `tx.exec`, logical writes, read-your-writes reads).
   *
   *  A THROW from the body that is not a {@link BackendError} is a BUSINESS rejection: the data rolls
   *  back, `lmid` advances alone, and this method throws {@link MutationRejected} (so surrounding
   *  code can compensate). A {@link BackendError} is INFRA: it propagates (the client retries).
   *
   *  **The callback form RETURNS ITS BODY'S VALUE**, and only on the committed path — a rejection
   *  throws, so there is never a value to act on for a transaction that rolled back. That is what
   *  lets a post-commit effect be decided by a TRANSACTIONAL read instead of a second, racy one
   *  afterwards:
   *
   *  ```ts
   *  const kick = await scope.transact(async (tx) => {
   *    const prior = await tx.row("message", { id: a.assistantMessageId });
   *    if (prior) return undefined;                 // a replayed envelope — do NOT re-fire the effect
   *    tx.insert("message", …);
   *    return { streamId: a.assistantMessageId, history: await readHistory(tx, a.chatId) };
   *  });
   *  if (kick) void startGeneration(kick);          // committed, and decided against committed state
   *  ```
   *
   *  A tx-form mutator cannot do this: its return value is already the logical-write channel
   *  ({@link ApiMutatorResult}). A post-commit effect chosen by a transactional read is exactly what
   *  the scoped form is for. */
  transact<T>(run: (tx: ServerMutationTx) => T | Promise<T>): Promise<T>;
  transact<A, C extends MutatorCtx>(mutator: SharedMutator<A, C>, args: A, ctx: C): Promise<void>;
}

/** A SCOPED server mutator (WORK-OUTSIDE-TX): server-only code, ONE `scope.transact`, optional
 *  post-commit code. Register it by wrapping in {@link scoped} — the tag the api-server routes on to
 *  hand it a {@link MutationScope} instead of running its whole body inside the transaction. */
export type ScopedMutator<User, Args> = (
  scope: MutationScope,
  args: Args,
  ctx: MutationContext<User>,
) => void | Promise<void>;

/** A {@link ScopedMutator} tagged by {@link scoped} so the harness invokes it with a
 *  {@link MutationScope}. Typed as a BRANDED tx-form {@link ApiMutator} purely so it registers in the
 *  `mutators` record without widening it to a union (which would break contextual inference for every
 *  plain tx-form entry). Its true runtime shape is `(scope, args, ctx)`; the tag — not the type —
 *  routes it, and it is never actually called as a tx-form mutator. */
export type ScopedApiMutator<User, Args> = ApiMutator<User, Args> & { readonly __rindleScoped: true };

/** Mark a mutator as SCOPED so the api-server gives it a {@link MutationScope} (author-controlled tx
 *  boundary via `scope.transact`) rather than running its whole body inside the transaction. Register
 *  it alongside the tx-form mutators — it wins by key like any override:
 *
 *  ```ts
 *  mutators: defineApiMutators({
 *    ...sharedApiMutators(sharedMutators, sharedCtx),        // tx-form (common case)
 *    createOrder: scoped(async (scope, raw, ctx) => {        // needs outside-tx work
 *      const args = createOrder.args.parse(raw);
 *      const chargeId = await stripe.charge(args.amount, { idempotencyKey: ctx.envelope.mid }); // outside tx
 *      try {
 *        await scope.transact(createOrder, args, { ...sharedCtx(ctx), chargeId });              // inside tx
 *      } catch (e) {
 *        await stripe.refund(chargeId);                       // compensate — the write rejected
 *        throw e;
 *      }
 *      await sendReceipt(ctx.user);                           // after commit
 *    }),
 *  }),
 *  ```
 */
export function scoped<User, Args>(fn: ScopedMutator<User, Args>): ScopedApiMutator<User, Args> {
  // The brand carries the scoped runtime shape; typing the RETURN as the (branded) tx-form keeps it
  // assignable into `mutators` WITHOUT unioning that record — the harness routes on the brand and
  // never calls it as a tx-form mutator, so the cast is sound.
  return Object.assign(fn, { __rindleScoped: true as const }) as unknown as ScopedApiMutator<User, Args>;
}

function isScoped<User>(m: ApiMutator<User, any>): m is ScopedApiMutator<User, any> {
  return (m as { __rindleScoped?: boolean }).__rindleScoped === true;
}

export type ApiMutators<User> = Record<string, ApiMutator<User, any>>;

export interface QueryLeaseRequest<User> {
  user: User;
  name: string;
  args: unknown;
  request?: unknown;
  /** The browser's stable `clientId` (sent on the query POST) — the anonymous routing-key fallback
   *  when there is no authenticated subject and no session cookie (READ-ROUTER-DESIGN.md §1.5/§2.2).
   *  A routing HINT only, never authorization. */
  clientId?: string;
  /** The browser's opaque follower-affinity ticket (FOLLOWER-AFFINITY-DESIGN.md §3), read off the
   *  query POST and forwarded opaquely on `materialize` so the fleet edge selects the follower
   *  the browser's ws is pinned to (§2, §4) — both legs co-locate. The api-server does NOT verify
   *  it (the fleet does); it holds no signing key. Absent ⇒ single daemon / affinity off. */
  affinity?: string;
}

/**
 * The room-serve block on a query lease (RINDLE-REALTIME-QUERY-ENABLEMENT §2.1 step 5 / §2.4,
 * slice G-iv-b): present when the named query carries a realtime label naming a configured room
 * profile (302 §5 — declared, not derived; no coverage proof). The G-v client uses it to open the
 * room transport for THIS query beside — never instead of — its daemon session.
 *
 * It is a dedicated block on purpose: the ROOM ws is a SEPARATE connection this query opens beside
 * its daemon session (never a migration of the daemon session — the daemon ws host is fixed and
 * placed by the affinity ticket). A room-served lease's top-level fields are byte-identical to the
 * daemon-served ones.
 */
export interface QueryLeaseRealtime {
  /** The client store's gate/domain key for this room source (`connectSource`) AND the string the
   *  wasm engine's `parse_source_key` accepts: any string other than the reserved `"daemon"`
   *  parses as a room source, and the established convention is `"room:" + doc`
   *  (e.g. `room:document/doc:d1`). */
  sourceKey: string;
  /** Where the client opens the ROOM ws for this query (from `realtime.locateRoom`) — its OWN
   *  connection, distinct from the daemon session's fixed ws host. */
  wsEndpoint: string;
  /** The room's self-authorizing signed lease (`@rindle/room/token`): the APPROVED query AST +
   *  doc + subject, HMAC-signed with `realtime.roomTokenKey` so the room shell's
   *  `downstream.tokenKeys` ring verifies it. The room materializes on first presentation. */
  roomToken: string;
  /** Token expiry (ms epoch) — the client's renewal clock (renewal = a fresh lease). */
  exp: number;
  /** The wire room doc (`"<profile>/<key>"`, minted server-side — never client-derived). */
  doc: string;
  /** Per-footprint-table specs: the §2.2 owned/followed split + §3.2 routing metadata. Since
   *  H-iii each spec also carries `footprintWhere` — the same exact membership predicate the boot
   *  wire ships the room gate (one compiler, `compileRoomScopeSpecs`) — feeding the client's §3
   *  prove-or-slow-path router. Advisory routing metadata, never a credential (§3.2). */
  tables: RoomTableSpec[];
}

/**
 * One minted SYSTEM-STREAM lease on a query lease's `lifecycle` block (RINDLE-REALTIME-QUERY-
 * ENABLEMENT §4, Slice I-iii): an ordinary daemon materialization over one of the four
 * `_rindle_*` lifecycle system tables (registered by the daemon's `enable_realtime_lifecycle` —
 * `rust/rindle-replica/src/mutations.rs`), attachable by the EXISTING client subscribe path
 * (present `leaseToken` on a `subscribe` frame, exactly like the primary lease). The identity
 * fields (`scope`/`doc`/`clientId`) document the minted AST's predicate — the client keys its
 * retains and its release-time row filters on them.
 */
export interface QueryLeaseLifecycleLease {
  /** Which system table this lease's subscription serves. */
  table: string;
  leaseToken: string;
  /** DOORBELL only: the §4.1 occupancy scope — the wire room doc (`"<profile>/<key>"`). */
  scope?: string;
  /** FENCE entries only: the room doc the predicate is scoped to. */
  doc?: string;
  /** FENCE ledger/outcome entries: present iff the predicate was ALSO client-scoped (the lease
   *  request carried `clientId`). Absent ⇒ doc-only predicate — the client filters to its own
   *  rows regardless (defense in depth). */
  clientId?: string;
}

/** The §4 lifecycle block on a query lease (Slice I-iii): present only when BOTH the realtime
 *  `lifecycle` config is on AND the query is realtime-labeled. `doorbell` rides EVERY labeled
 *  lease (occupancy is counted whether or not the query is room-served — the 1→2 upgrade trigger
 *  needs solo watchers subscribed BEFORE any room exists, §4.1); `fence` rides only a ROOM-SERVED
 *  lease (the §4.2/§7.1/§3.3 downgrade surfaces are meaningful only where a room domain exists).
 *  The §4.2 fence VALUE (`finalFlushSeq`) is deliberately NOT here — it arrives with the I-v
 *  downgrade response; I-iii only stands up the streams. */
export interface QueryLeaseLifecycle {
  doorbell: QueryLeaseLifecycleLease;
  fence?: QueryLeaseLifecycleLease[];
}

/** The §4.2 downgrade fence on a query lease (Slice I-v): rides a labeled reply whose §4.1
 *  occupancy gate CLOSED (so there is NO `realtime` block) when the server could drain the room.
 *  A SIBLING of `realtime`, never nested inside it — block-ABSENCE is the downgrade signal, and
 *  the fence rides alongside that absence. Its `finalFlushSeq` is the room's last COMMITTED flush
 *  seq; the client's frozen room ghost holds visible until the daemon plane has provably absorbed
 *  it (`_rindle_room_watermark(doc) ≥ finalFlushSeq`). Absent from every non-downgrade reply. */
export interface QueryLeaseRealtimeFence {
  /** The retiring room source's gate/domain key — `"room:" + doc`, matching what the room-served
   *  lease's {@link QueryLeaseRealtime.sourceKey} carried. */
  sourceKey: string;
  doc: string;
  finalFlushSeq: number;
}

export interface QueryLeaseResponse {
  leaseToken: string;
  materializationId: string;
  queryKey?: string;
  reused?: boolean;
  /** The public daemon/fleet WebSocket endpoint. With the unified `rindle` connection this is
   *  derived from the same ingress URL (or `rindle.wsUrl`) so a browser can open its transport from
   *  this lease and needs no application-authored runtime-config route. */
  wsEndpoint?: string;
  /** Fresh opaque follower-placement ticket minted with this follower-local lease. The optimistic
   *  client offers it on the WebSocket opened at {@link wsEndpoint}, pinning both legs to the same
   *  follower even though the first connection is created only after this response. */
  affinity?: string;
  /** The room-serve block (G-iv-b) — see {@link QueryLeaseRealtime}. Absent ⇒ the lease is
   *  byte-identical to the legacy daemon-served shape. */
  realtime?: QueryLeaseRealtime;
  /** The §4.2 downgrade fence (Slice I-v) — see {@link QueryLeaseRealtimeFence}. Present only on a
   *  labeled reply whose occupancy gate closed AND `realtime.lifecycle.drainRoom` could drain the
   *  room; absent otherwise (including on every room-served reply). */
  realtimeFence?: QueryLeaseRealtimeFence;
  /** The §4 lifecycle system-stream block (Slice I-iii) — see {@link QueryLeaseLifecycle}.
   *  Minted ONLY under the opt-in `realtime.lifecycle` config; absent ⇒ byte-identical to the
   *  pre-lifecycle response. */
  lifecycle?: QueryLeaseLifecycle;
}

/** A one-shot SSR read of a named query (SSR-DESIGN.md §6): same `(name, args)` surface as a lease,
 *  but the daemon serializes the current view ONCE and registers no subscriber. */
export interface QueryReadRequest<User> {
  user: User;
  name: string;
  args: unknown;
  request?: unknown;
  /** The browser's stable `clientId` — the anonymous routing-key fallback (see
   *  {@link QueryLeaseRequest.clientId}). Lets the SSR read co-locate on the follower the booting
   *  client's first subscribe will hit (READ-ROUTER-DESIGN.md §2.4). */
  clientId?: string;
  /** The browser's opaque follower-affinity ticket — see {@link QueryLeaseRequest.affinity}.
   *  Forwarded on the one-shot `query` so an SSR read lands on the same pinned follower. */
  affinity?: string;
}

/** The assembled (nested-by-name) first-paint snapshot the server-side Store seeds + dehydrates
 *  (SSR-DESIGN.md §3.3). `rows` hydrate without an engine; `cvMin` is their watermark baseline. */
export interface QueryReadResponse {
  rows: Array<{ cols: Record<string, unknown>; [rel: string]: unknown }>;
  cvMin?: number;
  queryKey?: string;
}

/** The context a {@link MutationBackend} needs to run one mutation inside its transaction. */
export interface MutationRunInput {
  envelope: MutationEnvelope;
  /** Schema-derived render metadata (from {@link RindleApiServerOptions.schema}). A logical op on a
   *  table absent here throws loudly — never a silent dropped write. `{}` when no schema is set. */
  render: RenderIndex;
  /** Invoke the (authorized) mutator against the backend-provided tx. A THROW that is NOT a
   *  {@link BackendError} is a BUSINESS rejection (roll the data back, then advance `lmid` alone,
   *  §2.4); a {@link BackendError} (a DB-layer failure) is INFRA and rejects the returned promise. */
  run(tx: ServerMutationTx): Promise<void>;
}

/** The result of {@link MutationBackend.runMutation}: either the data+lmid committed together, or a
 *  business rejection whose data was rolled back but whose `lmid` still advanced (§2.4). */
export type MutationOutcome =
  | { accepted: true; output: SqlTxnOutput }
  | { accepted: false; reason: string; output?: unknown };

/**
 * Where a mutation runs and who stamps `lmid` — the seam that makes the mutator authoring surface
 * backend-agnostic (`BYO-POSTGRES-LMID-CONTRACT-DESIGN.md` §6; MUTATORS-ISOMORPHIC plan). Three
 * implementations ship: {@link sqlBackend} is the preferred managed-SQL path; {@link daemonBackend}
 * keeps the private control-plane compatibility path; and {@link postgresBackend} runs a real
 * interactive PG transaction with confirmation riding the CDC loop down.
 *
 * The load-bearing invariant is that a mutation ALWAYS advances the client's `last_mutation_id`:
 * - `runMutation` runs the mutator inside the backend's transaction; on success it advances `lmid`
 *   to `envelope.mid` in the SAME atomic unit (OPTIMISTIC-WRITES-DESIGN.md §8.2); on a BUSINESS
 *   rejection it rolls the data back but STILL advances `lmid` (§2.4 — else the client's pending
 *   queue never drains and the optimistic stack wedges).
 * - `reject` is the pre-flight path (unknown mutator, failed authorization): NO data, `lmid` alone.
 * - An infrastructure failure THROWS from `runMutation` — never a user rejection (the client
 *   retries; mid dedup absorbs any applied prefix).
 */
export interface MutationBackend {
  /** The SQL dialect this backend renders logical ops to (drives placeholder style). */
  readonly dialect: SqlDialect;
  /** Optional raw-SQL surface outside the mutation transaction. Built-in backends provide it;
   *  custom backends may omit it, in which case `scope.sql` fails as an infrastructure error. */
  readonly outsideSql?: ServerSql;
  runMutation(input: MutationRunInput): Promise<MutationOutcome>;
  reject(input: { envelope: MutationEnvelope; reason: string }): Promise<unknown>;
}

export interface PushMutationRequest<User> {
  user: User;
  envelope: MutationEnvelope;
  request?: unknown;
}

export interface PushMutationsRequest<User> {
  user: User;
  envelopes: MutationEnvelope[];
  request?: unknown;
}

export type PushMutationResponse =
  | { accepted: true; rejected: false; output: SqlTxnOutput }
  | { accepted: false; rejected: true; reason: string; output?: unknown };

export interface RindleApiRoutes {
  query: string;
  read: string;
  mutate: string;
  applyRowChangeTxn: string;
  claimRoomEpoch: string;
  roomLmids: string;
  roomBoot: string;
  stream: string;
}

/** A room-host reply the transport writes VERBATIM (`status` + JSON `body`): the
 *  daemon's fence/conflict/identity semantics ride specific statuses and body shapes
 *  the room's `httpAuthority` decodes, so this endpoint trio can't run through the
 *  throw-on-error result shapes the viewer endpoints use. */
export interface RoomHostResponse {
  status: number;
  body: unknown;
}

/** A named query to keep permanently materialized (warm with zero subscribers). */
export interface PinnedQuery {
  name: string;
  args?: unknown;
}

/** The explicit fleet pin fan-out seam (READ-ROUTER-DESIGN.md §4.2). The api-server resolves each
 *  pin's authoritative AST under `pinUser` and hands the ready {@link MaterializeInput}s here; the
 *  implementation (the read router) fans EACH across all live followers. Distinct, on purpose, from
 *  a per-viewer `materialize` (which routes ONE) — a pin-assert always sprays ALL. */
export interface PinFanout {
  assertPins(pins: readonly MaterializeInput[]): Promise<void>;
}

// --------------------------------------------------------------------------- realtime (room) host
//
// Enabling Rindle Realtime for an app (RINDLE-REALTIME-ENABLEMENT-DESIGN.md §3.1) is ONE named
// opt-in: the `realtime` options block. Its presence activates the flush trio AND `/room-boot`;
// its absence keeps every room endpoint 403 and adds no route. The deprecated bare `authorizeRoom`
// still gates the trio alone (it never activates `/room-boot`).

/** The room-boot flush leg (enablement §5): where the placed room's write-behind lands and what
 *  credential it presents. `urls` are the trio's ROUTE PATHS (root-relative — the shell resolves
 *  them against the boot call's origin), so an app that overrides `routes` needs no out-of-band
 *  sync; `headers` ride every flush call verbatim (`httpAuthority`'s `headers`). */
export interface RoomBootFlush {
  urls: { apply: string; claim: string; lmids: string };
  headers: Record<string, string>;
}

/** The `/room-boot` response (RINDLE-REALTIME-DESIGN.md §10.1 — the DO shell's cold-boot
 *  callback): the claimed placement epoch, the room's upstream footprint lease, and the flush
 *  leg. A cold room boots inert and serves nothing until this returns. */
export interface RoomBootResponse {
  epoch: number;
  upstreamLeaseToken: string;
  /** Where the room opens its upstream subscription (a routed deploy's follower). Absent ⇒ the
   *  shell's statically configured rindled ws endpoint. */
  upstreamWsEndpoint?: string;
  /** Fresh opaque follower-placement ticket minted alongside `upstreamLeaseToken`. The DO offers
   *  it with `rindle.v1` on the separate upstream ws so a static fleet endpoint replays to the
   *  exact follower holding that local lease. Absent when daemon affinity is off. */
  upstreamAffinity?: string;
  /** Per-footprint-table scope specs (H-iv-b), compiled from the resolved footprint AST + the
   *  profile's context set (the legacy anonymous profile compiles with an empty context set):
   *  what the shell hands the wasm room's `enableWritesV2` — the §3.3 commit gate. Optional
   *  only for wire compatibility with pre-H-iv-b servers; a shell that doesn't receive them
   *  enables the v1 table-granular write plane exactly as before. */
  scopes?: RoomScopeSpec[];
  flush: RoomBootFlush;
}

export interface RindleRealtimeOptions<User> {
  /** The boot shell secret (§10.1 — a host binding, never client-derived). Authenticates the
   *  room's `Authorization: Bearer` on `/room-boot` (the default {@link authorizeBoot}) and keys
   *  the DEFAULT epoch-bound flush credential. */
  shellSecret: string;
  /** NAMED room profiles (RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md §2.1): profile name → key
   *  derivation + canonical unwindowed footprint + read-only context tables. The wire room key
   *  for a named profile is `"<profile>/<key>"`; `/room-boot` splits it and resolves the
   *  profile's footprint with the bare key. Compiled + validated LOUDLY at construction (§2.3):
   *  a windowed footprint, a context table missing from the schema/footprint, or a registered
   *  query whose realtime label names a missing profile all throw from `createRindleApiServer`. */
  rooms?: Record<string, RoomProfile<User>>;
  /** doc → the room's approved upstream footprint (§3.1) — an `Ast` or fluent `Query`. MAY
   *  delegate to the named-query registry internally; throw `RindleApiError("not-found", …, 404)`
   *  for a doc that shouldn't exist. The §9 footprint budget belongs here — it runs once per
   *  placement, at lease mint.
   *  @deprecated Prefer named {@link rooms} profiles (RINDLE-REALTIME-QUERY-ENABLEMENT §2.1).
   *  This bare form remains as the single-profile LEGACY alias — the anonymous/default profile:
   *  a doc with no known `"<profile>/"` prefix resolves here, byte-identically to before named
   *  profiles existed (and with none of their construction/boot-time validation). */
  resolveFootprint?: (doc: string, ctx: ApiContext<User>) => MaybePromise<ApiQueryResult>;
  /** Gates the flush trio — `authorizeRoom`, relocated. Default: verify the default flush
   *  credential from the {@link ROOM_FLUSH_CREDENTIAL_HEADER} request header — which requires the
   *  transport to pass its incoming request as `context.request` (Fetch `Request` and node
   *  `IncomingMessage` shapes are both understood). */
  authorize?: Authorizer<ApiContext<User>>;
  /** Gates `/room-boot`. Default: constant-time `Authorization: Bearer` check against
   *  {@link shellSecret} (same `context.request` requirement as {@link authorize}). */
  authorizeBoot?: Authorizer<ApiContext<User>>;
  /** Mint the flush headers a placed room presents on every flush call. Default:
   *  {@link mintRoomFlushCredential} under {@link ROOM_FLUSH_CREDENTIAL_HEADER}. Override this
   *  and {@link authorize} TOGETHER — they are the two ends of one credential. */
  mintFlushHeaders?: (input: { doc: string; epoch: number }) => MaybePromise<Record<string, string>>;
  /** Lease TTL for the room's upstream footprint materialization (defaults to the server-wide
   *  `leaseTtlMs`, else the daemon's default). */
  upstreamLeaseTtlMs?: number;
  /** Static endpoint where rooms open their upstream subscription. In a follower fleet this is the
   *  fleet ws URL; `/room-boot` pairs it with the materialization's fresh placement ticket so the
   *  room lands on the exact follower holding its lease. Absent ⇒ no explicit upstream (the Node
   *  room shell may use its own default; the shipped DO shell requires this endpoint). */
  upstreamWsEndpoint?: string;
  /** Locate (or place) the room serving `doc` and return the ROOM ws endpoint a room-served
   *  lease's client should open (G-iv-b; on the DO shell this is the Worker's room URL). The
   *  endpoint rides the lease's dedicated `realtime.wsEndpoint` — its OWN connection, distinct from
   *  the daemon session's fixed ws host. Absent ⇒ room-serving is OFF: labeled queries serve from
   *  the daemon exactly as today (fail-open). */
  locateRoom?: (doc: string) => MaybePromise<{ wsEndpoint: string }>;
  /** The room lease token signing key (`@rindle/room/token`): `kid` + secret, matching an entry
   *  in the room shell's `downstream.tokenKeys` ring. Required for room-serving (without it a
   *  labeled query fail-opens to the daemon with a one-time warning). A separate secret from
   *  `shellSecret` on purpose — the shell's ring is the client-token trust domain, the shell
   *  secret is the boot/flush trust domain. */
  roomTokenKey?: { kid: string; secret: string };
  /** Room lease token TTL, ms (default 5 minutes — the §4.1 short-TTL backstop; renewal is a
   *  fresh lease through this server, never an extension). */
  roomTokenTtlMs?: number;
  /** Loud-diagnostics sink for the realtime layer (profile compilation warnings + the one-time
   *  per-(query, profile) "not room-served" serve-decision warnings). Defaults to
   *  `console.warn`; injectable for tests. */
  warn?: (message: string) => void;
  /** The §4 upgrade/downgrade lifecycle plane (RINDLE-REALTIME-QUERY-ENABLEMENT §4, Slice
   *  I-iii): PRESENCE of this block is the opt-in — every realtime-labeled lease then
   *  additionally mints the doorbell system lease (occupancy, §4.1) and every ROOM-SERVED lease
   *  the fence bundle (watermark + ledger + outcomes, §4.2/§7.1/§3.3) — see
   *  {@link QueryLeaseLifecycle}. Requires the daemon to have run `enable_realtime_lifecycle`
   *  (the four `_rindle_*` system tables must be registered or the minted materializations fail
   *  — which fail-opens with a one-time warning, never blocking the lease). Absent ⇒ the lease
   *  response is byte-identical to pre-lifecycle. */
  lifecycle?: RindleRealtimeLifecycleOptions;
}

/** {@link RindleRealtimeOptions.lifecycle}. PRESENCE of the block is the opt-in switch (I-iii);
 *  the fields below are the Slice I-iv occupancy knobs (§4.1, decisions D4/D6/D7). All optional —
 *  `lifecycle: {}` gets the designed defaults. */
export interface RindleRealtimeLifecycleOptions {
  /** D6 (§4.1): the occupancy threshold for room-serving. A labeled lease whose scope counts
   *  FEWER than this many distinct unexpired sessions (the caller's own included) ships WITHOUT
   *  the realtime block — served from the daemon, indistinguishable from an uncovered query —
   *  but WITH the doorbell, so the 1→2 transition wakes it (that is the point: solo docs never
   *  cost room infrastructure). Default **2** (the design's 1→2 trigger). Set `1` to room-serve
   *  solo viewers (the pre-I-iv behavior under lifecycle config). */
  minSessions?: number;
  /** The §9.1 hysteresis window, ms (default **120_000**). Two consumers: (a) the lazy sweep
   *  (D4) keeps expired session rows lingering at least this long past expiry — Slice I-v's
   *  downgrade decision ("no other unexpired row AND the newest other row expired > graceMs
   *  ago") is read FROM those rows, so they must survive to be read; (b) this slice's gate
   *  applies the same hysteresis upward: a scope with an other-session row expired ≤ graceMs
   *  ago keeps room-serving through the window (see `lifecycleOccupancy` — no flap on one
   *  client's brief lapse). §9.1-tunable: raise it for docs where collaborators churn slowly. */
  graceMs?: number;
  /** TTL of an occupancy session row, ms — `expires_at = now + sessionTtlMs` on every labeled
   *  lease mint/renewal (D7: session identity = the request's `clientId`; two tabs are two
   *  sessions iff their clientIds differ). Default = the server's `leaseTtlMs`, else 5 minutes —
   *  matching the room-token renewal cadence (`roomTokenTtlMs`, renewed 30s early), so a
   *  room-attached client's renewals keep its row unexpired; a daemon-attached solo client's row
   *  MAY lapse (it has no renewal timer) and is refreshed by its next doorbell-triggered
   *  re-lease — occupancy converges through the doorbell itself. */
  sessionTtlMs?: number;
  /** The §4.2 downgrade drain hook (Slice I-v). When the occupancy gate CLOSES for a labeled
   *  lease whose scope PLAUSIBLY hosted a room (an other-session row still lingers — never a
   *  never-shared solo doc), the api-server calls this to drain the room's pending write-behind
   *  and learn its last COMMITTED `flush_seq`, then rides the value back on the lease as the
   *  {@link QueryLeaseRealtimeFence}. The deployment wires it to the room shell's / DO's `/drain`
   *  control. Absent ⇒ no fence is attached (the client hits its loud legacy downgrade path);
   *  a throw fails OPEN to the same (a downgrade never blocks the lease). Concurrent drains across
   *  api-server instances are fine — `/drain` is idempotent. */
  drainRoom?: (doc: string) => Promise<{ finalFlushSeq: number }>;
}

// The DEFAULT flush credential: `rfc1.<b64url payload>.<b64url hmac-sha256>`, payload
// `{v:1, doc, epoch, iat}`. Deliberately EPOCH-bound, not time-bound: the credential's lifecycle
// IS the placement fence (§8.3 — a superseded epoch's flushes 409 at the store no matter what
// credential they carry), and platform revocation is registry suspension, so an `exp` would only
// force spurious re-boots of long-lived rooms. HMAC via WebCrypto (`crypto.subtle`) so the exact
// same code runs in Node and a Cloudflare Worker — no `node:crypto` import (matching
// `@rindle/room`'s token module).

export const ROOM_FLUSH_CREDENTIAL_HEADER = "x-rindle-room-credential";

const FLUSH_CREDENTIAL_PREFIX = "rfc1";

export interface RoomFlushCredentialPayload {
  v: 1;
  doc: string;
  epoch: number;
  iat: number;
}

function b64url(bytes: Uint8Array): string {
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

// Return type inferred (`Uint8Array<ArrayBuffer>` under TS ≥5.7 libs) — an explicit
// `Uint8Array` annotation widens to `ArrayBufferLike` and fails `crypto.subtle`'s
// `BufferSource` under consumers compiling this source with newer lib types.
function unb64url(s: string) {
  const bin = atob(s.replace(/-/g, "+").replace(/_/g, "/"));
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

async function flushHmacKey(secret: string, usage: "sign" | "verify") {
  return crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    [usage],
  );
}

/** Sign the default epoch-bound flush credential (`/room-boot` mints one per placement). */
export async function mintRoomFlushCredential(opts: {
  shellSecret: string;
  doc: string;
  epoch: number;
  /** Mint time; defaults to `Date.now()`. Injectable for tests. */
  now?: number;
}): Promise<string> {
  const payload: RoomFlushCredentialPayload = {
    v: 1,
    doc: opts.doc,
    epoch: opts.epoch,
    iat: opts.now ?? Date.now(),
  };
  const body = b64url(new TextEncoder().encode(JSON.stringify(payload)));
  const signed = `${FLUSH_CREDENTIAL_PREFIX}.${body}`;
  const key = await flushHmacKey(opts.shellSecret, "sign");
  const sig = new Uint8Array(
    await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(signed)),
  );
  return `${signed}.${b64url(sig)}`;
}

/** Verify a flush credential's MAC, then its claims; returns the payload or throws. The MAC is
 *  checked FIRST — no claim is trusted before it passes. */
export async function verifyRoomFlushCredential(
  credential: string,
  shellSecret: string,
): Promise<RoomFlushCredentialPayload> {
  const parts = credential.split(".");
  if (parts.length !== 3 || parts[0] !== FLUSH_CREDENTIAL_PREFIX) {
    throw new Error("not a room flush credential");
  }
  const [, body, sig] = parts;
  const key = await flushHmacKey(shellSecret, "verify");
  const ok = await crypto.subtle.verify(
    "HMAC",
    key,
    unb64url(sig),
    new TextEncoder().encode(`${FLUSH_CREDENTIAL_PREFIX}.${body}`),
  );
  if (!ok) throw new Error("bad signature");
  let payload: RoomFlushCredentialPayload;
  try {
    payload = JSON.parse(new TextDecoder().decode(unb64url(body))) as RoomFlushCredentialPayload;
  } catch {
    throw new Error("malformed payload");
  }
  if (payload.v !== 1) throw new Error("unknown version");
  if (typeof payload.doc !== "string" || payload.doc.length === 0) {
    throw new Error("missing doc");
  }
  if (typeof payload.epoch !== "number") throw new Error("missing epoch");
  return payload;
}

/** Database connection used by the API server's managed SQL path.
 *
 *  `intMode` defaults to `"number"` because logical Rindle rows use the JSON-safe {@link WireValue}
 *  vocabulary — `"bigint"` does not survive `JSON.stringify`, and `"string"` silently retypes every
 *  integer, breaking arithmetic in mutator bodies. The cost is a HARD BOUND: a mutator read of an
 *  integer outside ±(2^53 − 1) rejects that mutation rather than silently rounding it. Tables with
 *  keys beyond that range (snowflake ids, and so on) must override `intMode` and have their mutators
 *  handle the resulting type. Commit receipts are unaffected — they never decode row values. */
export type RindleDatabaseOptions = Pick<SqlClientOptions, "url" | "authToken" | "fetch" | "intMode">;

/** The unified fleet connection — one URL, one key. A Rindle is BOTH layers at once: the SQL
 *  database below and the sync/IVM control plane above, served by a single ingress that routes
 *  each request to the right tier. This option derives both from that one origin: the SQL layer
 *  ({@link RindleApiServerOptions.database}) at `url` with `token`, and the control-plane client
 *  ({@link RindleApiServerOptions.daemon}) at the same `url` — so an application configures "a
 *  rindle", not two subsystems.
 *
 *  The derived control-plane client sends the same bearer. A unified ingress is an explicitly
 *  merged customer API-server trust tier: it routes SQL to the master and control requests to a
 *  follower, while the browser still receives neither credential. Deployments that keep those
 *  trust tiers separate pass an explicit {@link RindleApiServerOptions.daemon}; explicit fields
 *  always win over this derivation. */
export interface RindleConnectionOptions {
  /** The single ingress origin. Defaults to `$RINDLE_URL` — exported by `rindle dev` whenever
   *  the rendered topology collapsed read+write onto one ingress. */
  url?: string;
  /** Public subscription WebSocket endpoint returned on query leases. Defaults to {@link url} with
   *  `http:` → `ws:` / `https:` → `wss:`. Override when HTTP and WebSocket ingress differ. */
  wsUrl?: string;
  /** The server-side bearer for both legs of the unified ingress. Defaults to
   *  `$RINDLE_DATABASE_TOKEN` (also a `rindle dev` export); required through one of those channels
   *  unless both legs are configured explicitly. It never reaches the browser. */
  token?: string;
}

export interface RindleApiServerOptions<User> {
  /** The sync/IVM control-plane client (leases, materialization, rooms). Optional once
   *  {@link rindle} is configured — the api-server then derives an HTTP client against the single
   *  ingress. Pass one explicitly for a tokened production fleet, a split/routed deployment, or a
   *  custom transport; an explicit client wins over the derivation. */
  daemon?: RindleDaemonClient;
  /** The unified connection (one URL, one key) that derives {@link daemon} and {@link database}
   *  from the fleet's single ingress. `rindle: {}` resolves both halves from the `rindle dev`
   *  environment. Any explicitly configured `daemon` / `database` / `sql` / `backend` field takes
   *  precedence over its derived counterpart. */
  rindle?: RindleConnectionOptions;
  /** Preferred managed setup. The API server constructs and owns its SQL client; authoritative
   *  mutators, `tx.sql`, and `scope.sql` use it, while `daemon` remains only the lease/query/
   *  materialization/room control plane. Mutually exclusive with {@link sql} unless `backend`
   *  explicitly replaces both. */
  database?: RindleDatabaseOptions;
  /** Advanced injection/test seam for an already-created SQL session. Most applications should
   *  configure {@link database} and never import `createSqlClient`. When present (and `backend` is
   *  absent), authoritative mutators run through {@link sqlBackend}. */
  sql?: SqlSession;
  /** Where mutations are applied and `lmid` is stamped ({@link MutationBackend}). Default:
   *  managed `sqlBackend` when `database` or `sql` is configured, otherwise the compatibility
   *  `daemonBackend(daemon)`. Pass `postgresBackend(...)` when Postgres is the source of truth. */
  backend?: MutationBackend;
  /** The typed schema (`createSchema`/`refineSchema`). Required only when a mutator uses the LOGICAL
   *  write vocabulary (`tx.insert`/`update`/`upsert`/`insertIgnore`/`delete`/`row`) — it drives the
   *  dialect SQL renderer (column order, pk, quoting). A logical op with no schema configured throws
   *  loudly. Pure raw-`tx.exec` mutators do not need it. */
  schema?: Schema;
  queries?: ApiQueries<User>;
  runQuery?: RunQuery<User>;
  mutators?: ApiMutators<User>;
  authorizeQuery?: Authorizer<AuthorizeQueryInput<User>>;
  authorizeMutation?: Authorizer<AuthorizeMutationInput<User>>;
  /** Rindle Realtime (RINDLE-REALTIME-ENABLEMENT-DESIGN.md §3.1): the ONE named opt-in.
   *  Presence activates the room flush trio AND `/room-boot`; absence keeps every room
   *  endpoint 403. Takes precedence over the deprecated {@link authorizeRoom}. */
  realtime?: RindleRealtimeOptions<User>;
  /** The LM stream plane (LM-STREAM-CHECKPOINT-DESIGN.md): the ONE named opt-in for streaming a
   *  model response live while the durable store only ever sees coarse checkpoints. Presence
   *  activates {@link RindleApiServer.openStream}/{@link RindleApiServer.subscribeStream} and
   *  `/stream`; absence keeps them 403. */
  streams?: RindleStreamOptions<User>;
  /** The room write-authority gate (§5.3.1): validates the caller is a placed room —
   *  the epoch-bound flush credential rides `context.request`, and what it means is
   *  the app's to define. The room endpoints are DISABLED (403) until this is set:
   *  hosting a write authority is an explicit opt-in, never a default.
   *  @deprecated Use {@link realtime} (its `authorize`) — this bare form gates the
   *  flush trio but never activates `/room-boot`. When both are set, `realtime` wins. */
  authorizeRoom?: Authorizer<ApiContext<User>>;
  routes?: Partial<RindleApiRoutes>;
  mode?: StreamMode;
  materializationPolicy?:
    | MaterializationPolicy
    | ((input: QueryLeaseRequest<User>) => MaybePromise<MaterializationPolicy>);
  leaseTtlMs?: number;
  /** Idle TTL (ms) the warm pipeline a one-shot SSR {@link RindleApiServer.readQuery read} leaves
   *  behind is held at (SSR-DESIGN.md §3.4) — it must comfortably cover page-load + client-boot +
   *  the follow-up live `subscribe` so the browser lands on a still-warm pipeline (the warm
   *  handoff). The TTL is NOT part of the dedup key (max-wins), so it only ever extends a shared
   *  query's window. Absent ⇒ the daemon's default idle TTL. */
  readIdleTtlMs?: number;
  subject?: string | ((input: QueryLeaseRequest<User>) => MaybePromise<string | undefined>);
  /** The ANONYMOUS routing key forwarded to the read router (READ-ROUTER-DESIGN.md §2.2) — used by
   *  HRW placement when there is no authenticated `subject`. The router keys on `subject ?? this`;
   *  the resolved value rides `metadata.routingKey` to the daemon. Default: the browser-supplied
   *  `clientId` (from the query POST body). NOTE: the default cannot co-locate an ANONYMOUS SSR read
   *  with the booting client — an SSR server can't see the browser's localStorage `clientId`, so the
   *  two legs compute different keys and §2.4's warm handoff misses (still correct — just an extra
   *  first-touch materialize). For anonymous SSR co-location, set this to read a server-set session
   *  cookie from `input.request` (it rides the SSR request AND every browser request). A routing
   *  HINT only — never authorization. Ignored by a single (unrouted) daemon, which has nothing to
   *  route. */
  routingKey?: string | ((input: QueryLeaseRequest<User>) => MaybePromise<string | undefined>);
  /** The EXPLICIT fleet pin fan-out — when set, {@link RindleApiServer.assertPins} fans each
   *  resolved pin across ALL live followers through it (a fleet control action over the machine
   *  list — FOLLOWER-AFFINITY-DESIGN.md §11) instead of materializing each pin once on the (single)
   *  daemon. A per-viewer `materialize` always routes ONE; a pin-assert always fans ALL — never
   *  inferred from `policy.kind`. Absent ⇒ single-daemon behavior (one materialize per pin). */
  pinFanout?: PinFanout;
  /** Named queries to keep permanently materialized via {@link RindleApiServer.assertPins}.
   *  Each is materialized with a `pinned` policy (survives zero subscribers) so late joiners
   *  attach to an already-warm result. Pins are viewer-independent — resolved with `pinUser`. */
  pinnedQueries?: PinnedQuery[];
  /** The user context pins resolve under (pins are shared, so they should not depend on a
   *  per-viewer identity). Defaults to `undefined`. */
  pinUser?: User;
  /** Surfaced when a SCOPED mutator ({@link scoped}) throws from code that runs AFTER `scope.transact`
   *  has already sealed the protocol outcome — a post-commit effect, or a compensation handler running
   *  after a business rejection. The outcome is fixed (this callback CANNOT change the client's
   *  response or the `lmid` advance), but the throw must not vanish: a failed refund is real money.
   *  Absent ⇒ the error is logged to `console.error`. */
  onScopeError?: (err: unknown, info: { phase: "committed" | "rejected"; envelope: MutationEnvelope }) => void;
}

export interface RindleApiServer<User> {
  readonly routes: RindleApiRoutes;
  /** Close the SQL client created from {@link RindleApiServerOptions.database}, and drop every live
   *  stream's readers and timers WITHOUT a durable write (that is {@link drainStreams}). Injected
   *  SQL sessions and custom backends remain caller-owned. Idempotent. */
  close(): void;
  /** Open an LM stream (LM-STREAM-CHECKPOINT §2): commits the durable POINTER row, then hands back
   *  the producer handle. A resolved handle means the message already exists for every client's
   *  query — so a subscriber that arrives before the first token has something to attach to.
   *  Throws 403 unless {@link RindleApiServerOptions.streams} is configured. */
  openStream(input: OpenStreamInput<User>): Promise<StreamHandle>;
  /** Attach a reader at `from` — the same call serves a first-touch subscriber (`from: 0`), a late
   *  joiner (`from` = the seq its IVM view shows), and a reconnect (`from` = `Last-Event-ID`).
   *  Terminates with `end`, or with `stale`/`absent` when the client should fall back to the
   *  durable plane (both are ordinary answers, never errors). */
  subscribeStream(input: SubscribeStreamInput<User>): Promise<StreamSubscription>;
  /** Parse a default `{streamId, from?}` subscribe body and run {@link subscribeStream}. For the
   *  GET + `EventSource` shape, use {@link streamResponse} (or build the body with
   *  `streamRequestFromHttp(request)` yourself). */
  handleStreamJson(body: unknown, context: ApiContext<User>): Promise<StreamSubscription>;
  /** The subscribe route in ONE call: parse a GET (`?streamId=…&from=…`, with `Last-Event-ID`
   *  winning), authorize + subscribe, and encode the SSE response. A refusal comes back as a JSON
   *  error `Response` (403 for denied or unconfigured) rather than a throw, so the route body is a
   *  single expression after authentication. For custom transports, compose
   *  `streamRequestFromHttp` + {@link subscribeStream} + `streamFramesToSse` instead. */
  streamResponse(
    request: { url: string; headers: { get(name: string): string | null } },
    context: ApiContext<User> & { keepAliveMs?: number },
  ): Promise<Response>;
  /** Checkpoint every live stream's outstanding tail, then seal it `interrupted`
   *  (LM-STREAM-CHECKPOINT §5). Wire it to SIGTERM: without it a rolling deploy drops each
   *  response's un-checkpointed tail and strands rows saying `streaming` forever. */
  drainStreams(): Promise<void>;
  createQueryLease(input: QueryLeaseRequest<User>): Promise<QueryLeaseResponse>;
  /** (Re-)materialize every `pinnedQueries` entry with a pinned policy. Idempotent — the daemon
   *  dedupes by canonical query, so a re-assert reuses the existing materialization. Call it at
   *  startup and whenever the daemon restarts (e.g. from the daemon-client `onBootId` hook), since
   *  the daemon holds no durable materialization state. No-op when `pinnedQueries` is empty. */
  assertPins(): Promise<void>;
  pushMutation(input: PushMutationRequest<User>): Promise<PushMutationResponse>;
  /** Apply an in-order batch (the client mutation queue's flush). Envelopes run strictly
   *  sequentially; a rejection still advances the daemon's lmid, so later envelopes in the
   *  batch stay contiguous and keep applying. A daemon ERROR throws for the whole batch —
   *  the client retries it and the daemon's mid dedup absorbs the already-applied prefix. */
  pushMutations(input: PushMutationsRequest<User>): Promise<PushMutationResponse[]>;
  /** One-shot SSR read (SSR-DESIGN.md §6): resolve `(name, args)` → AST (same authority path as a
   *  lease, `authorizeQuery` enforced), have the daemon serialize the current view once, and return
   *  the assembled rows for the loader to seed + dehydrate. Registers NO subscriber — a dropped
   *  render leaks nothing; the pipeline self-reclaims after the idle TTL ({@link
   *  RindleApiServerOptions.readIdleTtlMs}) unless the browser's follow-up `subscribe` lands first. */
  readQuery(input: QueryReadRequest<User>): Promise<QueryReadResponse>;
  handleQueryJson(body: unknown, context: ApiContext<User>): Promise<QueryLeaseResponse>;
  /** Parse a default `{name, args}` read body and run {@link readQuery}. */
  handleReadJson(body: unknown, context: ApiContext<User>): Promise<QueryReadResponse>;
  /** Accepts `{envelope}` (one) or `{envelopes: [...]}` (an in-order batch → array reply). */
  handleMutateJson(
    body: unknown,
    context: ApiContext<User>,
  ): Promise<PushMutationResponse | PushMutationResponse[]>;
  /** The room's flush (§5.3.1): gate on `authorizeRoom`, forward the txn to the write
   *  authority, and pass the store's verdict through VERBATIM — `200 {applied, cv}`,
   *  `409 {error:"fenced"|"conflict", …}`, or the loud identity `500`. Write
   *  `status` + `body` as-is; the room's `httpAuthority` decodes them. */
  handleApplyRowChangeTxnJson(
    body: unknown,
    context: ApiContext<User>,
  ): Promise<RoomHostResponse>;
  /** Claim the next placement epoch for a doc (§2.5), same gate + envelope. */
  handleClaimRoomEpochJson(body: unknown, context: ApiContext<User>): Promise<RoomHostResponse>;
  /** The room's boot probe (§3.3), same gate + envelope. */
  handleRoomLmidsJson(body: unknown, context: ApiContext<User>): Promise<RoomHostResponse>;
  /** The DO shell's cold-boot callback (§10.1; enablement §3.1): authenticate the shell
   *  secret, resolve the doc's footprint, claim the placement epoch, mint the upstream
   *  lease and the flush leg. 403 until {@link RindleApiServerOptions.realtime} is set. */
  handleRoomBootJson(body: unknown, context: ApiContext<User>): Promise<RoomHostResponse>;
}

export type RindleApiErrorCode = "bad-request" | "forbidden" | "not-found" | "rejected";

export class RindleApiError extends Error {
  readonly code: RindleApiErrorCode;
  readonly status: number;

  constructor(code: RindleApiErrorCode, message: string, status: number) {
    super(message);
    this.name = "RindleApiError";
    this.code = code;
    this.status = status;
  }
}

/**
 * A read/write-split `RindleDaemonClient` (READ-ROUTER-DESIGN.md §2.1). Writes
 * (`executeSqlTxn` / `rejectMutation` / `applyRowChangeTxn` / `migrate`) go to the single
 * write-master, UNCHANGED and never through the router; reads (`materialize` / `query` /
 * `dematerialize`, and raw `executeSqlRead`) go to the read router. Raw reads default to a replica
 * (`consistency:"eventual"`) so they scale off the write-master; pass `consistency:"strong"` on a
 * read to route it to the master for read-your-writes after a write to the same data. Hand one of
 * these to {@link createRindleApiServer} as `daemon` to point the reads leg at the router while
 * writes stay on the master — no placement logic enters the api-server.
 *
 * ```ts
 * const daemon = new SplitDaemonClient(
 *   new HttpRindleDaemonClient({ baseUrl: MASTER_URL, headers: writeAuth }),  // writes → master
 *   new HttpRindleDaemonClient({ baseUrl: ROUTER_URL, headers: routerAuth }), // reads  → router
 * );
 * ```
 */
export class SplitDaemonClient implements RindleDaemonClient {
  private readonly writes: RindleDaemonClient;
  private readonly reads: RindleDaemonClient;

  constructor(writes: RindleDaemonClient, reads: RindleDaemonClient) {
    this.writes = writes;
    this.reads = reads;
  }

  // writes → the single master, never through the router
  executeSqlTxn(input: SqlTxn): Promise<SqlTxnOutput> {
    return this.writes.executeSqlTxn(input);
  }
  // raw reads → a replica by default (scaled off the master); `consistency:"strong"` opts that
  // read into the master for read-your-writes after a write to the same data.
  executeSqlRead(input: SqlRead): Promise<SqlReadOutput> {
    return input.consistency === "strong"
      ? this.writes.executeSqlRead(input)
      : this.reads.executeSqlRead(input);
  }
  rejectMutation(input: MutationRejection): Promise<MutationRejectionOutput> {
    return this.writes.rejectMutation(input);
  }
  // Interactive mutation sessions hold the MASTER's write transaction — never a replica's
  // (DAEMON-INTERACTIVE-TXN-DESIGN.md §4.1; the follower write-fence enforces the same).
  beginMutationSession(input: MutationSessionBegin): Promise<MutationSessionBeginOutput> {
    const begin = this.writes.beginMutationSession?.bind(this.writes);
    if (!begin) return Promise.reject(new Error("the write master lacks mutation sessions"));
    return begin(input);
  }
  execInMutationSession(input: MutationSessionExec): Promise<unknown> {
    const exec = this.writes.execInMutationSession?.bind(this.writes);
    if (!exec) return Promise.reject(new Error("the write master lacks mutation sessions"));
    return exec(input);
  }
  queryInMutationSession(input: MutationSessionQuery): Promise<SqlReadOutput> {
    const query = this.writes.queryInMutationSession?.bind(this.writes);
    if (!query) return Promise.reject(new Error("the write master lacks mutation sessions"));
    return query(input);
  }
  commitMutationSession(input: MutationSessionRef): Promise<SqlTxnOutput> {
    const commit = this.writes.commitMutationSession?.bind(this.writes);
    if (!commit) return Promise.reject(new Error("the write master lacks mutation sessions"));
    return commit(input);
  }
  rollbackMutationSession(input: MutationSessionRef): Promise<unknown> {
    const rollback = this.writes.rollbackMutationSession?.bind(this.writes);
    if (!rollback) return Promise.reject(new Error("the write master lacks mutation sessions"));
    return rollback(input);
  }
  applyRowChangeTxn(input: RowChangeTxn): Promise<RowChangeTxnOutput> {
    return this.writes.applyRowChangeTxn(input);
  }
  claimRoomEpoch(input: ClaimRoomEpochInput): Promise<ClaimRoomEpochOutput> {
    const claim = this.writes.claimRoomEpoch?.bind(this.writes);
    if (!claim) return Promise.reject(new Error("the write master lacks claimRoomEpoch"));
    return claim(input);
  }
  roomLmids(input: RoomLmidsInput): Promise<RoomLmidsOutput> {
    const lmids = this.writes.roomLmids?.bind(this.writes);
    if (!lmids) return Promise.reject(new Error("the write master lacks roomLmids"));
    return lmids(input);
  }
  migrate(input: MigrateInput): Promise<MigrateOutput> {
    return this.writes.migrate(input);
  }

  // reads → the fleet (one FLEET_URL; the affinity ticket + edge place the follower)
  materialize(input: MaterializeInput): Promise<MaterializeOutput> {
    return this.reads.materialize(input);
  }
  query(input: QueryOnceInput): Promise<QueryOnceOutput> {
    return this.reads.query(input);
  }
  dematerialize(input: DematerializeInput): Promise<DematerializeOutput> {
    return this.reads.dematerialize(input);
  }
}

// --------------------------------------------------------------------------- dialect SQL renderer
//
// A logical {@link MutationOp} → dialect `SqlStatement`. The whole per-dialect delta is the
// PLACEHOLDER STYLE (`?` vs `$n`): identifiers are always double-quoted (SQLite tolerates it, PG
// requires it — `user` is reserved, camelCase folds), and upserts use portable `ON CONFLICT` (both
// engines). So `sqliteDialect`/`postgresDialect` differ only in `placeholder`.

/** A SQL dialect for the logical mutation renderer. */
export interface SqlDialect {
  readonly name: "sqlite" | "postgres";
  /** Render the i-th (1-based) bind placeholder. sqlite: `?`; postgres: `$i`. */
  placeholder(oneBased: number): string;
  /** Optional value coercion hook (e.g. a future SQLite `0/1` boolean). Default: identity. */
  encodeValue?(v: WireValue, type: ColType): WireValue;
}

export const sqliteDialect: SqlDialect = { name: "sqlite", placeholder: () => "?" };
export const postgresDialect: SqlDialect = { name: "postgres", placeholder: (i) => `$${i}` };

/** Per-table metadata the renderer needs (all reachable from a `TableMeta`). */
export interface TableRenderMeta {
  /** Columns in schema (wire) order — the stable INSERT column list + completeness check. */
  columns: string[];
  /** Primary-key column NAMES — the WHERE / ON CONFLICT target / SET partition. */
  pkNames: string[];
  /** Column name → declared type (only consulted by {@link SqlDialect.encodeValue}). */
  types: Record<string, ColType>;
  /** Columns a full insert must name — the non-nullable ones (design 206 §6.2). */
  required: string[];
  /** The nullable (omittable-to-null) columns — an omitted one binds `NULL` (design 206 §6.2). */
  nullable: ReadonlySet<string>;
}

export type RenderIndex = Record<string, TableRenderMeta>;

/** Build the {@link RenderIndex} from a typed schema (`schema.tables[name]` is a `TableMeta`). */
export function buildRenderIndex(schema: Schema): RenderIndex {
  const out: RenderIndex = {};
  const tables = (schema as unknown as { tables: Record<string, { columns: Record<string, { type: ColType }>; primaryKey: readonly string[] }> }).tables;
  for (const name of Object.keys(tables)) {
    const meta = tables[name];
    const columns = Object.keys(meta.columns);
    const types: Record<string, ColType> = {};
    for (const c of columns) types[c] = meta.columns[c].type;
    // Same schema-derived plan the client funnel uses (design 206 §6.1), so their required-sets
    // and null-fills can't drift.
    const { required, nullable } = insertPlan(schema.tables[name]);
    out[name] = { columns, pkNames: [...meta.primaryKey], types, required, nullable };
  }
  return out;
}

const quoteIdent = (name: string): string => `"${name.replace(/"/g, '""')}"`;

function tableMeta(render: RenderIndex, table: string): TableRenderMeta {
  const meta = render[table];
  if (!meta) {
    const known = Object.keys(render);
    throw new Error(
      `logical mutator write to unknown table ${JSON.stringify(table)} — did you pass \`schema\` to createRindleApiServer? known tables: ${known.length ? known.join(", ") : "(none — no schema configured)"}`,
    );
  }
  return meta;
}

/** Validate a keyed row against a table: reject unknown columns; require the pk columns; with
 *  `full`, require every NON-nullable column (a nullable column may be omitted and is filled with
 *  `NULL`, design 206 §6.2). Mirrors the client `trackingTx.checkColumns` messages so an author sees
 *  the same error on both tiers. */
function checkColumns(table: string, obj: KeyedRow, meta: TableRenderMeta, full: boolean): void {
  const unknown = Object.keys(obj).filter((k) => !meta.columns.includes(k));
  if (unknown.length) {
    throw new Error(`unknown column${unknown.length > 1 ? "s" : ""} ${unknown.join(", ")} on ${table} — columns: ${meta.columns.join(", ")}`);
  }
  const required = full ? meta.required : meta.pkNames;
  const missing = required.filter((c) => !(c in obj));
  if (missing.length) {
    throw new Error(`missing ${full ? "column" : "primary-key column"}${missing.length > 1 ? "s" : ""} ${missing.join(", ")} on ${table}`);
  }
}

const encode = (dialect: SqlDialect, v: WireValue, type: ColType): WireValue =>
  dialect.encodeValue ? dialect.encodeValue(v, type) : v;

/** Render one {@link MutationOp} to a `{sql, params}` for the dialect, or `null` for a no-op (an
 *  `update` whose row names only pk columns — nothing to SET, matching the client's no-op edit). */
export function renderOp(op: MutationOp, meta: TableRenderMeta, dialect: SqlDialect): SqlStatement | null {
  const t = quoteIdent(op.table);
  const params: WireValue[] = [];
  // Bind a value and return its placeholder at the correct 1-based index (post-push length).
  // `insertCell` fills NULL for an omitted nullable column on the insert arm (design 206 §6.2);
  // on update/delete every bound column is guaranteed present, so it is a pass-through there.
  // `toCell` stringifies a `json` object (a typed mutator passes the parsed object) — a string
  // passes through, so an author may still pass pre-stringified json.
  const bind = (c: string, row: KeyedRow): string => {
    params.push(encode(dialect, toCell(insertCell(row, c), meta.types[c]), meta.types[c]));
    return dialect.placeholder(params.length);
  };

  if (op.kind === "insert" || op.kind === "insertIgnore" || op.kind === "upsert") {
    checkColumns(op.table, op.row, meta, true);
    const cols = meta.columns;
    const values = cols.map((c) => bind(c, op.row));
    let sql = `INSERT INTO ${t} (${cols.map(quoteIdent).join(", ")}) VALUES (${values.join(", ")})`;
    if (op.kind === "insertIgnore") {
      sql += ` ON CONFLICT (${meta.pkNames.map(quoteIdent).join(", ")}) DO NOTHING`;
    } else if (op.kind === "upsert") {
      const nonPk = cols.filter((c) => !meta.pkNames.includes(c));
      sql += ` ON CONFLICT (${meta.pkNames.map(quoteIdent).join(", ")}) `;
      sql += nonPk.length
        ? `DO UPDATE SET ${nonPk.map((c) => `${quoteIdent(c)} = excluded.${quoteIdent(c)}`).join(", ")}`
        : `DO NOTHING`;
    }
    return { sql, params };
  }

  if (op.kind === "update") {
    checkColumns(op.table, op.row, meta, false);
    const setCols = meta.columns.filter((c) => !meta.pkNames.includes(c) && c in op.row);
    if (!setCols.length) return null; // pk-only row → nothing to change (client no-op edit)
    const setSql = setCols.map((c) => `${quoteIdent(c)} = ${bind(c, op.row)}`);
    const whereSql = meta.pkNames.map((c) => `${quoteIdent(c)} = ${bind(c, op.row)}`);
    return { sql: `UPDATE ${t} SET ${setSql.join(", ")} WHERE ${whereSql.join(" AND ")}`, params };
  }

  // delete
  checkColumns(op.table, op.pk, meta, false);
  const whereSql = meta.pkNames.map((c) => `${quoteIdent(c)} = ${bind(c, op.pk)}`);
  return { sql: `DELETE FROM ${t} WHERE ${whereSql.join(" AND ")}`, params };
}

/** Render a point read (`tx.row`) — `SELECT <cols> FROM "T" WHERE <pk>` — for read-your-writes. */
export function renderPointRead(table: string, pk: KeyedRow, meta: TableRenderMeta, dialect: SqlDialect): SqlStatement {
  checkColumns(table, pk, meta, false);
  const params: WireValue[] = [];
  const whereSql = meta.pkNames.map((c) => {
    params.push(encode(dialect, pk[c], meta.types[c]));
    return `${quoteIdent(c)} = ${dialect.placeholder(params.length)}`;
  });
  const cols = meta.columns.map(quoteIdent).join(", ");
  return { sql: `SELECT ${cols} FROM ${quoteIdent(table)} WHERE ${whereSql.join(" AND ")}`, params };
}

/** Map a driver row (column-name keyed) to a {@link KeyedRow} over the table's known columns. */
function rowToKeyed(row: Record<string, unknown> | undefined, meta: TableRenderMeta): KeyedRow | undefined {
  if (!row) return undefined;
  const out: KeyedRow = {};
  for (const c of meta.columns) out[c] = row[c] as WireValue;
  return out;
}

// --------------------------------------------------------------------------- backends + server tx

/** Thrown (wrapping the driver error) by a server tx's DB calls, so the seam can tell an INFRA
 *  failure (retry) from a mutator-body throw (business rejection). */
export class BackendError extends Error {
  readonly driverError: unknown;
  constructor(driverError: unknown) {
    super(driverError instanceof Error ? driverError.message : String(driverError));
    this.name = "BackendError";
    this.driverError = driverError;
  }
}

/** Build the compiler {@link Catalog} for ONE ast from the render index: columns/pk from the
 *  schema; relationship cardinality from the AST ITSELF — a Rindle relationship is declared at
 *  the query site (`sub(alias, rel)` / `.one()`), never on the schema, so the alias→cardinality
 *  map is inherently per-query. `columnTypes` are stubs: the sqlite dialect binds natives and
 *  never consults them (DAEMON-INTERACTIVE-TXN §5.4 — no casts). */
function catalogFor(render: RenderIndex, root: Ast): Catalog {
  const tables: Record<string, TableSchema> = {};
  const ensure = (table: string): TableSchema => {
    const existing = tables[table];
    if (existing) return existing;
    const meta = tableMeta(render, table);
    const columnTypes: Record<string, QueryColumnType> = {};
    for (const c of meta.columns) columnTypes[c] = { type: "text", isEnum: false, isArray: false };
    return (tables[table] = {
      columns: [...meta.columns],
      primaryKey: [...meta.pkNames],
      columnTypes,
      relationships: {},
    });
  };
  const walkCondition = (cond: Condition | undefined): void => {
    if (!cond) return;
    if (cond.type === "and" || cond.type === "or") {
      for (const c of cond.conditions) walkCondition(c);
    } else if (cond.type === "correlatedSubquery") {
      walkAst(cond.related.subquery);
    }
  };
  const walkAst = (ast: Ast): void => {
    const t = ensure(ast.table);
    for (const rel of ast.related ?? []) {
      const alias = rel.subquery.alias;
      if (alias != null) t.relationships[alias] = rel.subquery.one === true ? "one" : "many";
      walkAst(rel.subquery);
    }
    walkCondition(ast.where);
  };
  walkAst(root);
  return { tables };
}

/** The control-flow unwind for a begin-absorbed replay (DAEMON-INTERACTIVE-TXN §4.1): thrown
 *  from the first read so the mutator body stops re-running an already-committed envelope; the
 *  backend answers with the tx's latched authoritative output. Never surfaces to users. */
class AbsorbedReplay extends Error {
  constructor() {
    super("mutation absorbed by mid dedup at session begin");
  }
}

const MUTATOR_CONFLICT_MAX_ATTEMPTS = 5;

function isRetryableCommitConflict(error: unknown): boolean {
  if (error instanceof RindleSqlError) {
    return error.status === 409 && (error.code === "retryable-conflict" || error.code === "TRANSACTION_CONFLICT");
  }
  if (!(error instanceof DaemonHttpError) || error.status !== 409) return false;
  try {
    const body = JSON.parse(error.body) as { code?: unknown; retryable?: unknown };
    return body.code === "retryable-conflict" && body.retryable === true;
  } catch {
    return false;
  }
}

async function mutatorConflictBackoff(attempt: number): Promise<void> {
  const ceiling = Math.min(32, 2 ** attempt);
  const millis = ceiling + Math.floor(Math.random() * 4);
  await new Promise<void>((resolve) => setTimeout(resolve, millis));
}

/** True when this error is the SQL codec refusing a bind value outright (`undefined`, `Date`, `NaN`,
 *  a binary view, an out-of-i64 bigint) rather than a transport or database failure. */
function isUnencodableBind(error: unknown): boolean {
  return error instanceof RindleSqlError && error.code === "VALUE_UNSUPPORTED";
}

/** Refuse an unencodable bind at the point the MUTATOR supplies it, so it surfaces as a BUSINESS
 *  rejection (lmid advances, the browser retires its prediction) instead of an infrastructure
 *  failure. Left as infra it is retried forever against a deterministic mutator, which wedges the
 *  client's mutation queue behind a poison message.
 *
 *  Only the SQL transport needs this: the legacy daemon encoder is JSON, which silently coerces the
 *  same values (`undefined`/`NaN` -> null, `Date` -> an ISO string). Asserting there would invent a
 *  failure that the wire does not actually have. */
function assertEncodableParams(sql: string, params: readonly WireValue[] | undefined): void {
  if (params === undefined) return;
  for (let index = 0; index < params.length; index++) {
    try {
      encodeSqlValue(params[index] as Parameters<typeof encodeSqlValue>[0]);
    } catch (error) {
      if (!isUnencodableBind(error)) throw error;
      throw new Error(`bind ${index} of \`${sql}\` cannot be stored: ${errMessage(error)}`);
    }
  }
}

/** Leading keywords the SQL mutation surface structurally REFUSES inside a mutator's write batch: a
 *  read (`SELECT`/`EXPLAIN`), transaction control, a connection `PRAGMA`, or DDL. None can begin a
 *  valid mutation write, so refusing them has no false positives — a `WITH`-prefixed statement is
 *  deliberately absent because it may resolve to either a read or a write, and the server stays the
 *  authority for that case. */
const MUTATION_REFUSED_LEADING_KEYWORDS = new Set([
  "SELECT", "EXPLAIN", "VALUES",
  "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "END",
  "PRAGMA", "VACUUM", "ATTACH", "DETACH",
  "CREATE", "ALTER", "DROP", "REINDEX", "ANALYZE",
]);

/** Refuse a statement whose CLASS the mutation surface rejects, at the point the MUTATOR supplies it,
 *  so it surfaces as a business rejection instead of a poison. When the batch reaches the transport
 *  the body has already returned, so a server 400 there is (mis)read as infrastructure and retried
 *  forever — the same wedge {@link assertEncodableParams} prevents for bind values. Conservative by
 *  design: it fires only for a leading keyword that can never start a valid write, and leaves every
 *  ambiguous case (including CTE-prefixed writes) to the server's authoritative classifier. */
function assertMutationWriteStatement(sql: string): void {
  const match = /^[\s;]*([a-zA-Z]+)/.exec(sql);
  if (match === null) return;
  const keyword = match[1]!.toUpperCase();
  if (MUTATION_REFUSED_LEADING_KEYWORDS.has(keyword)) {
    throw new Error(
      `a mutator write statement cannot begin with ${keyword} (\`${sql}\`); ` +
        `mutations write rows only — use tx.sql.query(...) for reads and migrations for DDL`,
    );
  }
}

interface MutationTransportBegin {
  handle?: unknown;
  absorbed?: SqlTxnOutput;
  read?: SqlReadOutput;
}

/** The mutation-only transport consumed by the API-server transaction harness. Both the legacy
 *  daemon client and `@rindle/sql-client` adapt to this one shape, so lmid/rejection/lazy-session
 *  policy is implemented once. */
interface MutationTransport {
  readonly interactive: boolean;
  /** Whether this transport's wire REFUSES values the daemon's JSON encoder coerces. Drives
   *  {@link assertEncodableParams} — see its docs for why the daemon adapter opts out. */
  readonly strictValues: boolean;
  execute(input: {
    envelope: MutationEnvelope;
    statements: SqlStatement[];
    idempotencyKey?: string;
  }): Promise<SqlTxnOutput>;
  reject(input: { envelope: MutationEnvelope; reason: string }): Promise<unknown>;
  begin(input: {
    envelope: MutationEnvelope;
    statements: SqlStatement[];
    query: SqlStatement;
    idempotencyKey?: string;
  }): Promise<MutationTransportBegin>;
  exec(handle: unknown, statements: SqlStatement[]): Promise<void>;
  query(handle: unknown, statement: SqlStatement): Promise<SqlReadOutput>;
  commit(handle: unknown): Promise<SqlTxnOutput>;
  rollback(handle: unknown): Promise<void>;
  readCommitted(statement: SqlStatement): Promise<SqlReadOutput>;
}

function daemonMutationTransport(daemon: RindleDaemonClient): MutationTransport {
  return {
    interactive: daemon.beginMutationSession !== undefined,
    strictValues: false,
    execute({ envelope, statements, idempotencyKey }) {
      const txn: SqlTxn = { statements, clientID: envelope.clientID, mid: envelope.mid };
      if (idempotencyKey !== undefined) txn.idempotencyKey = idempotencyKey;
      return daemon.executeSqlTxn(txn);
    },
    reject({ envelope, reason }) {
      return daemon.rejectMutation({ clientID: envelope.clientID, mid: envelope.mid, reason });
    },
    async begin({ envelope, statements, query, idempotencyKey }) {
      if (!daemon.beginMutationSession) throw new Error("the daemon client does not support mutation sessions");
      const input: MutationSessionBegin = {
        clientID: envelope.clientID,
        mid: envelope.mid,
        statements,
        query,
      };
      if (idempotencyKey !== undefined) input.idempotencyKey = idempotencyKey;
      const opened = await daemon.beginMutationSession(input);
      if (opened.absorbed) {
        const { absorbed: _absorbed, sessionId: _sessionId, read: _read, ...output } = opened;
        return { absorbed: output as SqlTxnOutput };
      }
      return { handle: opened.sessionId, read: opened.read };
    },
    async exec(handle, statements) {
      await daemon.execInMutationSession!({ sessionId: handle as string, statements });
    },
    query(handle, statement) {
      return daemon.queryInMutationSession!({
        sessionId: handle as string,
        sql: statement.sql,
        params: statement.params,
      });
    },
    commit(handle) {
      return daemon.commitMutationSession!({ sessionId: handle as string });
    },
    async rollback(handle) {
      await daemon.rollbackMutationSession!({ sessionId: handle as string });
    },
    readCommitted(statement) {
      return daemon.executeSqlRead({ sql: statement.sql, params: statement.params });
    },
  };
}

function mutationReceiptOutput(receipt: SqlMutationReceipt, clientID: string): SqlTxnOutput {
  const output: SqlTxnOutput = {
    applied: receipt.applied,
    lmid: receipt.lmid,
    lmidAdvances: [{ clientID, lmid: receipt.lmid }],
  };
  if (receipt.commitCursor !== null) output.cursor = receipt.commitCursor;
  return output;
}

function publicMutationStatement(statement: SqlStatement): PublicSqlStatement {
  return statement.params === undefined ? { sql: statement.sql } : { sql: statement.sql, args: statement.params };
}

function mutationRowsOutput(rows: SqlMutationRows): SqlReadOutput {
  return { cols: rows.columns, rows: rows.rows as WireValue[][] };
}

/** Convert the transports' compact positional rows into the ergonomic server-only raw-SQL shape. */
function keyedSqlRows<Row = Record<string, unknown>>(
  columns: readonly string[],
  rows: readonly (readonly unknown[])[],
): Row[] {
  // Row objects are keyed by column NAME, so a read that projects the same name twice
  // (`SELECT parent.status, child.status ...`) would silently keep only the last value — and a
  // mutator branching on `row.status` would then authorize against the wrong cell. Refuse it loudly
  // so the collision surfaces as a rejection reason instead of silent, wrong data.
  const seen = new Set<string>();
  for (const column of columns) {
    if (seen.has(column)) {
      throw new Error(
        `raw SQL read projects the column name ${JSON.stringify(column)} more than once; ` +
          `alias them to distinct names (e.g. SELECT a.id AS a_id, b.id AS b_id)`,
      );
    }
    seen.add(column);
  }
  return rows.map((cells) => Object.fromEntries(columns.map((column, index) => [column, cells[index]])) as Row);
}

function daemonOutsideSql(daemon: RindleDaemonClient): ServerSql {
  return {
    async execute(sql, params = []) {
      await daemon.executeSqlTxn({ statements: [{ sql, params: [...params] }] });
    },
    async batch(statements) {
      if (statements.length === 0) return;
      await daemon.executeSqlTxn({
        statements: statements.map((statement) => ({
          sql: statement.sql,
          ...(statement.params !== undefined ? { params: [...statement.params] } : {}),
        })),
      });
    },
    async query<Row = Record<string, unknown>>(sql: string, params: readonly WireValue[] = []): Promise<Row[]> {
      const out = await daemon.executeSqlRead({ sql, params: [...params], consistency: "strong" });
      return keyedSqlRows<Row>(out.cols, out.rows);
    },
  };
}

function sqlSessionOutsideSql(sql: SqlSession): ServerSql {
  return {
    async execute(text, params = []) {
      await sql.execute({ sql: text, args: [...params] });
    },
    async batch(statements) {
      if (statements.length === 0) return;
      await sql.batch(statements.map(publicMutationStatement));
    },
    async query<Row = Record<string, unknown>>(text: string, params: readonly WireValue[] = []): Promise<Row[]> {
      const out = await sql.execute({ sql: text, args: [...params], wantRows: true }, { consistency: "strong" });
      return keyedSqlRows<Row>(
        out.result.columns.map((column) => column.name),
        out.result.rows,
      );
    },
  };
}

function sqlClientMutationTransport(sql: SqlSession): MutationTransport {
  return {
    interactive: true,
    strictValues: true,
    // NOTE: `execute`/`begin` deliberately ignore the interface's optional `idempotencyKey`. On the
    // SQL mutation wire the (clientID, mid) pair IS the durable retry identity — a redelivery is
    // absorbed by mid, so there is no idempotency key to carry. The field stays on the shared
    // MutationTransport only because the legacy daemon foreign-write path still threads it.
    async execute({ envelope, statements }) {
      const receipt = await sql.executeMutation({
        clientId: envelope.clientID,
        mid: envelope.mid,
        statements: statements.map(publicMutationStatement),
      });
      return mutationReceiptOutput(receipt, envelope.clientID);
    },
    async reject({ envelope, reason }) {
      return mutationReceiptOutput(
        await sql.rejectMutation({ clientId: envelope.clientID, mid: envelope.mid, reason }),
        envelope.clientID,
      );
    },
    async begin({ envelope, statements, query }) {
      const opened = await sql.beginMutation({
        clientId: envelope.clientID,
        mid: envelope.mid,
        statements: statements.map(publicMutationStatement),
        query: publicMutationStatement(query),
      });
      if (opened.absorbed) {
        return { absorbed: mutationReceiptOutput(opened.receipt, envelope.clientID) };
      }
      return {
        handle: opened.transaction,
        ...(opened.read !== undefined ? { read: mutationRowsOutput(opened.read) } : {}),
      };
    },
    async exec(handle, statements) {
      await (handle as SqlMutationTransaction).batch(statements.map(publicMutationStatement));
    },
    async query(handle, statement) {
      return mutationRowsOutput(await (handle as SqlMutationTransaction).query(publicMutationStatement(statement)));
    },
    async commit(handle) {
      const receipt = await (handle as SqlMutationTransaction).commit();
      const advance = receipt.lmid;
      // The handle is opened for exactly one client; RemoteLazyTx patches the client id from its
      // envelope after this call so the legacy MutationBackend receipt remains byte-compatible.
      return {
        applied: receipt.applied,
        cursor: receipt.commitCursor ?? undefined,
        lmid: advance,
      };
    },
    async rollback(handle) {
      await (handle as SqlMutationTransaction).rollback();
    },
    async readCommitted(statement) {
      const result = await sql.execute(publicMutationStatement(statement), { consistency: "strong" });
      return {
        cols: result.result.columns.map((column) => column.name),
        rows: result.result.rows as WireValue[][],
      };
    },
  };
}

/**
 * The remote SQLite server tx (DAEMON-INTERACTIVE-TXN-DESIGN.md §5): ONE authoring surface, two
 * execution strategies. It starts ACCUMULATING — a pure-write mutator ships one batch to
 * the selected mutation transport — and LAZILY UPGRADES to an interactive
 * mutation session at the mutator's first read: `begin` carries the envelope identity, the
 * accumulated statement prefix (sound to replay — nothing before the first read observed DB
 * state, §5.2), and the read itself, so a one-read mutator pays exactly one extra round trip.
 * From then on reads run THROUGH the open transaction (read-your-writes — PG parity, and no
 * read-then-write race) and writes buffer locally, flushing before the next read/commit: k
 * reads cost k+2 round trips regardless of write count.
 *
 * Begin-time mid dedup can ABSORB the envelope (a redelivery whose commit response was lost):
 * the replay output is latched on {@link RemoteLazyTx.absorbed} and {@link AbsorbedReplay}
 * unwinds the body — the latch (not the throw) is authoritative, so a mutator that swallows
 * the unwind still cannot re-apply (no session opened; buffered writes are never shipped).
 * A daemon client without session support keeps the LEGACY committed-state point read.
 */
class RemoteLazyTx implements ServerMutationTx {
  /** Pre-upgrade: the accumulated batch/prefix. Post-upgrade: writes buffered for the next flush. */
  private readonly stmts: SqlStatement[] = [];
  private readonly render: RenderIndex;
  private readonly transport: MutationTransport;
  private readonly envelope: MutationEnvelope;
  private sessionHandle?: unknown;
  readonly sql: ServerSql;
  /** The begin-absorbed replay output (§4.1), latched for the backend. */
  absorbed?: SqlTxnOutput;
  idempotencyKey?: string;

  constructor(render: RenderIndex, transport: MutationTransport, envelope: MutationEnvelope) {
    this.render = render;
    this.transport = transport;
    this.envelope = envelope;
    this.sql = {
      execute: async (sql, params = []) => {
        this.exec(sql, [...params]);
      },
      batch: async (statements) => {
        for (const statement of statements) {
          this.exec(statement.sql, statement.params === undefined ? [] : [...statement.params]);
        }
      },
      query: <Row = Record<string, unknown>>(sql: string, params: readonly WireValue[] = []) =>
        this.querySql<Row>(sql, params),
    };
  }

  /** True once the tx upgraded to an interactive session (the backend then commits it). */
  get session(): boolean {
    return this.sessionHandle !== undefined;
  }

  get statements(): readonly SqlStatement[] {
    return this.stmts;
  }

  exec(sql: string, params: WireValue[] = []): void {
    // Refuse here, INSIDE the mutator body, so the harness reads it as a business rejection. By the
    // time the statement reaches the transport the body has returned and the throw is infra.
    if (this.transport.strictValues) {
      assertMutationWriteStatement(sql);
      assertEncodableParams(sql, params);
    }
    this.stmts.push({ sql, params });
  }

  private push(op: MutationOp): Promise<void> {
    const rendered = renderOp(op, tableMeta(this.render, op.table), sqliteDialect);
    if (rendered) {
      if (this.transport.strictValues) assertEncodableParams(rendered.sql, rendered.params);
      this.stmts.push(rendered);
    }
    return Promise.resolve();
  }

  insert(table: string, row: KeyedRow): Promise<void> {
    return this.push({ kind: "insert", table, row });
  }
  update(table: string, row: KeyedRow): Promise<void> {
    return this.push({ kind: "update", table, row });
  }
  upsert(table: string, row: KeyedRow): Promise<void> {
    return this.push({ kind: "upsert", table, row });
  }
  insertIgnore(table: string, row: KeyedRow): Promise<void> {
    return this.push({ kind: "insertIgnore", table, row });
  }
  delete(table: string, pk: KeyedRow): Promise<void> {
    return this.push({ kind: "delete", table, pk });
  }

  async row(table: string, pk: KeyedRow): Promise<KeyedRow | undefined> {
    const read = renderPointRead(table, pk, tableMeta(this.render, table), sqliteDialect);
    const out = await this.readThroughTxn(read);
    const cells = out.rows[0];
    if (!cells) return undefined;
    const keyed: KeyedRow = {}; // the daemon returns positional cells — zip with `cols`
    out.cols.forEach((c, i) => (keyed[c] = cells[i]));
    return keyed;
  }

  /** A full-shape read inside the open transaction (§5.4): compile to ONE SQLite `SELECT`
   *  (native bind params, no casts — SQLite is the canonical store), ride the session like
   *  `row` (including the lazy upgrade / begin ride-along), parse the single JSON cell. */
  async query(q: Ast | Query<any, any, any>): Promise<unknown> {
    const ast = typeof (q as Query<any, any, any>).ast === "function" ? (q as Query<any, any, any>).ast() : (q as Ast);
    const compiled = compileQueryAst(ast, catalogFor(this.render, ast), { dialect: "sqlite" });
    const out = await this.readThroughTxn({ sql: compiled.sql, params: compiled.params as WireValue[] });
    const cell = out.rows[0]?.[0];
    if (typeof cell !== "string") return ast.one === true ? null : [];
    return JSON.parse(cell) as unknown;
  }

  private async querySql<Row>(sql: string, params: readonly WireValue[]): Promise<Row[]> {
    const out = await this.readThroughTxn({ sql, params: [...params] });
    return keyedSqlRows<Row>(out.cols, out.rows);
  }

  /** Run one read: upgrade to a session at the first (§5.1), ride the open one after, or fall
   *  back to the legacy committed-state read when the daemon client lacks sessions. */
  private async readThroughTxn(read: SqlStatement): Promise<SqlReadOutput> {
    if (this.absorbed) throw new AbsorbedReplay();
    // Refuse an unencodable read bind at the mutator boundary, exactly as `exec` does for writes.
    // A read's parameters are encoded inside the transport, where the throw becomes a BackendError
    // (infra) that retries the deterministic mutator forever and wedges the client's queue; asserting
    // here makes it a business rejection instead.
    if (this.transport.strictValues) assertEncodableParams(read.sql, read.params);
    if (!this.transport.interactive) {
      try {
        return await this.transport.readCommitted(read);
      } catch (err) {
        throw new BackendError(err);
      }
    }
    try {
      if (this.sessionHandle === undefined) {
        const opened = await this.transport.begin({
          envelope: this.envelope,
          statements: this.stmts.splice(0),
          query: read,
          ...(this.idempotencyKey !== undefined ? { idempotencyKey: this.idempotencyKey } : {}),
        });
        if (opened.absorbed) {
          this.absorbed = opened.absorbed;
          throw new AbsorbedReplay();
        }
        if (opened.handle === undefined || !opened.read) {
          throw new Error(`malformed mutate-session begin reply: ${JSON.stringify(opened)}`);
        }
        this.sessionHandle = opened.handle;
        return opened.read;
      }
      await this.flush();
      return await this.transport.query(this.sessionHandle, read);
    } catch (err) {
      if (err instanceof AbsorbedReplay || err instanceof BackendError) throw err;
      throw new BackendError(err);
    }
  }

  /** Ship buffered writes into the open session, order-preserving; a no-op when none pend. */
  private async flush(): Promise<void> {
    if (this.stmts.length === 0) return;
    await this.transport.exec(this.sessionHandle!, this.stmts.splice(0));
  }

  /** Flush + commit the open session — the daemon stamps lmid co-transactionally (§4.4) and
   *  answers the same shape `/execute-sql-txn` does. */
  async commitSession(): Promise<SqlTxnOutput> {
    try {
      await this.flush();
      const output = await this.transport.commit(this.sessionHandle!);
      if (output.lmid !== undefined && output.lmidAdvances === undefined) {
        output.lmidAdvances = [{ clientID: this.envelope.clientID, lmid: output.lmid }];
      }
      return output;
    } catch (err) {
      throw err instanceof BackendError ? err : new BackendError(err);
    }
  }

  /** Best-effort rollback (the daemon's deadline is the backstop). MUST be awaited before a
   *  follow-up `/reject-mutation`: that lmid-only commit needs the writer this session holds. */
  async rollbackSessionQuietly(): Promise<void> {
    if (this.sessionHandle === undefined) return;
    const sessionHandle = this.sessionHandle;
    this.sessionHandle = undefined;
    try {
      await this.transport.rollback(sessionHandle);
    } catch {
      // Unreachable daemon / already-expired session: the deadline rollback covers it.
    }
  }
}

/** The Postgres server tx: a REAL interactive transaction. Logical writes render to `$n` and run
 *  LIVE against the open txn; raw `exec` runs live too (after `rewrite`); `row` reads the open txn
 *  (read-your-writes). Ops append to an internally-serialized chain so order holds even when a legacy
 *  sync mutator does not `await`; the backend drains the chain (`settle`) before the lmid upsert. */
class PgLiveTx implements ServerMutationTx {
  private chain: Promise<void> = Promise.resolve();
  private readonly stmts: SqlStatement[] = [];
  private readonly q: PgQuery;
  private readonly render: RenderIndex;
  private readonly rewrite: (sql: string) => string;
  readonly sql: ServerSql;
  idempotencyKey?: string;

  constructor(q: PgQuery, render: RenderIndex, rewrite: (sql: string) => string) {
    this.q = q;
    this.render = render;
    this.rewrite = rewrite;
    this.sql = {
      execute: async (sql, params = []) => {
        this.exec(sql, [...params]);
        await this.settle();
      },
      batch: async (statements) => {
        for (const statement of statements) {
          this.exec(statement.sql, statement.params === undefined ? [] : [...statement.params]);
        }
        await this.settle();
      },
      query: <Row = Record<string, unknown>>(sql: string, params: readonly WireValue[] = []) =>
        this.querySql<Row>(sql, params),
    };
  }

  get statements(): readonly SqlStatement[] {
    return this.stmts;
  }

  settle(): Promise<void> {
    return this.chain;
  }

  private execLive(sql: string, params: WireValue[]): void {
    this.chain = this.chain.then(async () => {
      try {
        await this.q.exec(sql, params as unknown[]);
      } catch (err) {
        throw new BackendError(err);
      }
    });
  }

  exec(sql: string, params: WireValue[] = []): void {
    const stmt = { sql: this.rewrite(sql), params };
    this.stmts.push(stmt);
    this.execLive(stmt.sql, params);
  }

  private write(op: MutationOp): Promise<void> {
    let rendered: SqlStatement | null;
    try {
      rendered = renderOp(op, tableMeta(this.render, op.table), postgresDialect);
    } catch (err) {
      return Promise.reject(err); // a validation error (business rejection), synchronous shape
    }
    if (rendered) this.execLive(rendered.sql, rendered.params ?? []);
    return this.chain;
  }

  insert(table: string, row: KeyedRow): Promise<void> {
    return this.write({ kind: "insert", table, row });
  }
  update(table: string, row: KeyedRow): Promise<void> {
    return this.write({ kind: "update", table, row });
  }
  upsert(table: string, row: KeyedRow): Promise<void> {
    return this.write({ kind: "upsert", table, row });
  }
  insertIgnore(table: string, row: KeyedRow): Promise<void> {
    return this.write({ kind: "insertIgnore", table, row });
  }
  delete(table: string, pk: KeyedRow): Promise<void> {
    return this.write({ kind: "delete", table, pk });
  }
  async row(table: string, pk: KeyedRow): Promise<KeyedRow | undefined> {
    const meta = tableMeta(this.render, table);
    const read = renderPointRead(table, pk, meta, postgresDialect); // validates before draining
    await this.settle(); // read-your-writes: drain queued writes first
    let rows: Array<Record<string, unknown>>;
    try {
      rows = await this.q.query(read.sql, read.params as unknown[]);
    } catch (err) {
      throw new BackendError(err);
    }
    return rowToKeyed(rows[0], meta);
  }

  query(): Promise<unknown> {
    // The compiler's postgres dialect ships (@rindle/query-compiler); what remains is the §7
    // static-catalog + driver-pin wiring (POSTGRES-READ-COMPILER-DESIGN.md Phase B).
    return Promise.reject(
      new Error(
        "tx.query is not wired on the Postgres backend yet (POSTGRES-READ-COMPILER-DESIGN.md Phase B) — use tx.row for point reads meanwhile",
      ),
    );
  }

  private async querySql<Row>(sql: string, params: readonly WireValue[]): Promise<Row[]> {
    await this.settle();
    try {
      return (await this.q.query(this.rewrite(sql), [...params])) as Row[];
    } catch (err) {
      throw new BackendError(err);
    }
  }
}

/** Shared remote-SQL mutation backend. A pure-write mutator remains one request; a read-bearing
 * mutator lazily upgrades at its first read; accepted effects commit with lmid; business rejection
 * rolls effects back before an lmid-only commit. Both daemonBackend and sqlBackend use this exact
 * policy implementation. */
function remoteMutationBackend(transport: MutationTransport, outsideSql: ServerSql): MutationBackend {
  return {
    dialect: sqliteDialect,
    outsideSql,
    async runMutation(input) {
      for (let attempt = 0; attempt < MUTATOR_CONFLICT_MAX_ATTEMPTS; attempt++) {
        try {
          const { envelope, render, run } = input;
          const tx = new RemoteLazyTx(render, transport, envelope);
          try {
            await run(tx);
          } catch (err) {
            // A begin-absorbed replay: the authoritative outcome already committed — answer it,
            // whatever the body did with the unwind (§4.1; the latch, not the throw, decides).
            if (tx.absorbed) return { accepted: true, output: tx.absorbed };
            if (err instanceof BackendError) {
              await tx.rollbackSessionQuietly();
              throw err.driverError; // infra — never a user rejection
            }
            const reason = errMessage(err);
            // Data first, watermark second: rollback releases this session's connection before
            // the lmid-only rejection commit.
            await tx.rollbackSessionQuietly();
            const output = await transport.reject({ envelope, reason });
            return { accepted: false, reason, output };
          }
          if (tx.absorbed) return { accepted: true, output: tx.absorbed };
          if (tx.session) {
            try {
              return { accepted: true, output: await tx.commitSession() };
            } catch (err) {
              if (err instanceof BackendError) throw err.driverError;
              throw err;
            }
          }
          return {
            accepted: true,
            output: await transport.execute({
              envelope,
              statements: [...tx.statements],
              ...(tx.idempotencyKey !== undefined ? { idempotencyKey: tx.idempotencyKey } : {}),
            }),
          };
        } catch (error) {
          if (!isRetryableCommitConflict(error) || attempt + 1 === MUTATOR_CONFLICT_MAX_ATTEMPTS) {
            throw error;
          }
          await mutatorConflictBackoff(attempt);
        }
      }
      throw new Error("unreachable mutator conflict retry loop");
    },
    reject({ envelope, reason }) {
      return transport.reject({ envelope, reason });
    },
  };
}

/** Legacy/private-plane adapter. Kept for existing deployments; its mutation policy is shared with
 * {@link sqlBackend}, so the two transports cannot drift. */
export function daemonBackend(daemon: RindleDaemonClient): MutationBackend {
  return remoteMutationBackend(daemonMutationTransport(daemon), daemonOutsideSql(daemon));
}

/** Run API-server mutators through `@rindle/sql-client`'s explicit mutation facade. Query leases,
 * SSR reads and room control continue to use `daemon`; only authoritative mutation execution moves
 * to the versioned SQL transport. */
export function sqlBackend(sql: SqlSession): MutationBackend {
  return remoteMutationBackend(sqlClientMutationTransport(sql), sqlSessionOutsideSql(sql));
}

/** The query surface a {@link PostgresPlugger} transaction exposes. `exec` runs one statement;
 *  `query` returns rows keyed by column name (read-your-own-writes inside the txn). */
export interface PgQuery {
  exec(sql: string, params?: unknown[]): Promise<void>;
  query(sql: string, params?: unknown[]): Promise<Array<Record<string, unknown>>>;
}

/** The thin driver adapter that keeps `pg` / `postgres.js` out of this package's dependencies
 *  (`BYO-POSTGRES-LMID-CONTRACT-DESIGN.md` §6.2): run `fn` inside ONE transaction — commit on
 *  resolve, roll back on throw. {@link pgPoolPlugger} adapts a node-postgres `Pool`. */
export interface PostgresPlugger {
  transaction<T>(fn: (q: PgQuery) => Promise<T>): Promise<T>;
}

export interface PostgresBackendOptions {
  /** Rewrite each MUTATOR statement's SQL before it runs (never the lmid upsert). The intended
   *  use is dialect bridging for a dual-topology app whose mutators are written SQLite-style:
   *  pass {@link questionToDollarParams} to convert `?` placeholders to `$1..$n`. */
  rewriteSql?: (sql: string) => string;
}

/** The §2.3 upsert, verbatim from the contract: monotonic via GREATEST, keyed by client. The
 *  identifiers are lowercase so quoting is cosmetic, but quote-everything is the repo's PG rule. */
const LMID_UPSERT = `INSERT INTO "_rindle_client_mutations" ("client_id", "last_mutation_id")
VALUES ($1, $2)
ON CONFLICT ("client_id") DO UPDATE
  SET "last_mutation_id" = GREATEST("_rindle_client_mutations"."last_mutation_id", EXCLUDED."last_mutation_id")`;

/**
 * The BYO-Postgres {@link MutationBackend} (`BYO-POSTGRES-LMID-CONTRACT-DESIGN.md` §6.3): one PG
 * transaction runs the mutator's statements and ALWAYS upserts `_rindle_client_mutations` —
 * the upsert sits outside any acceptance guard by construction, so the §2.4 footgun (a rejection
 * that forgets to advance `lmid` and wedges the client's pending queue) cannot be written.
 *
 * Confirmation does NOT come from this call's response: the lmid row rides the same PG commit
 * through CDC → relay → follower and reaches the client in the same coherent release as the
 * data (§8.2 relocated upstream). A rejection's `reason` still returns on the HTTP reply, but
 * nothing rejection-shaped travels the replication path — the optimistic prediction snaps back
 * when the advanced `lmid` arrives.
 */
export function postgresBackend(plugger: PostgresPlugger, opts: PostgresBackendOptions = {}): MutationBackend {
  const rewrite = opts.rewriteSql ?? ((sql: string) => sql);
  // A tagged business rejection escaping `plugger.transaction` — the plugger rolls the data back on
  // any throw; this marker distinguishes "mutator said no" (reject) from an infra failure (rethrow).
  class RejectSignal extends Error {}
  const lmidOnly = (envelope: MutationEnvelope): Promise<SqlTxnOutput> =>
    plugger.transaction(async (q) => {
      await q.exec(LMID_UPSERT, [envelope.clientID, envelope.mid]);
      return { applied: true, lmidAdvances: [{ clientID: envelope.clientID, lmid: envelope.mid }] };
    });
  const outsideSql: ServerSql = {
    async execute(sql, params = []) {
      await plugger.transaction(async (q) => {
        await q.exec(rewrite(sql), [...params]);
      });
    },
    async batch(statements) {
      if (statements.length === 0) return;
      await plugger.transaction(async (q) => {
        for (const statement of statements) {
          await q.exec(rewrite(statement.sql), statement.params === undefined ? [] : [...statement.params]);
        }
      });
    },
    query<Row = Record<string, unknown>>(sql: string, params: readonly WireValue[] = []): Promise<Row[]> {
      return plugger.transaction(async (q) => (await q.query(rewrite(sql), [...params])) as Row[]);
    },
  };
  return {
    dialect: postgresDialect,
    outsideSql,
    async runMutation({ envelope, render, run }) {
      try {
        const output = await plugger.transaction(async (q) => {
          const tx = new PgLiveTx(q, render, rewrite);
          try {
            await run(tx);
            await tx.settle(); // drain any un-awaited queued writes before the lmid stamp
          } catch (err) {
            if (err instanceof BackendError) throw err; // infra → rollback + propagate
            throw new RejectSignal(errMessage(err)); // business → rollback data, tag for §2.4
          }
          // ALWAYS on the accepted path, SAME transaction (§2.2): the lmid upsert commits with data.
          await q.exec(LMID_UPSERT, [envelope.clientID, envelope.mid]);
          return { applied: true, lmidAdvances: [{ clientID: envelope.clientID, lmid: envelope.mid }] };
        });
        return { accepted: true, output };
      } catch (err) {
        if (err instanceof RejectSignal) {
          // Data rolled back; STILL advance lmid alone (§2.4 — the client's queue must drain).
          return { accepted: false, reason: err.message, output: await lmidOnly(envelope) };
        }
        if (err instanceof BackendError) throw err.driverError; // infra
        throw err;
      }
    },
    reject: ({ envelope }) => lmidOnly(envelope).then(() => undefined),
  };
}

/** The slice of a node-postgres `Pool` the plugger needs — structural, so `pg` stays a
 *  dependency of the APP, never of this package. */
export interface PgPoolLike {
  connect(): Promise<{
    query(sql: string, params?: unknown[]): Promise<{ rows: Array<Record<string, unknown>> }>;
    release(err?: unknown): void;
  }>;
}

/** Adapt a node-postgres `Pool` (or anything pool-shaped) to a {@link PostgresPlugger}:
 *  one client per transaction, `BEGIN`/`COMMIT` bracketing, `ROLLBACK` + rethrow on failure. */
export function pgPoolPlugger(pool: PgPoolLike): PostgresPlugger {
  return {
    async transaction<T>(fn: (q: PgQuery) => Promise<T>): Promise<T> {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        const q: PgQuery = {
          exec: async (sql, params) => {
            await client.query(sql, params);
          },
          query: async (sql, params) => (await client.query(sql, params)).rows,
        };
        const out = await fn(q);
        await client.query("COMMIT");
        return out;
      } catch (err) {
        try {
          await client.query("ROLLBACK");
        } catch {
          // the connection may already be unusable; the original error is the one that matters
        }
        throw err;
      } finally {
        client.release();
      }
    },
  };
}

/**
 * Rewrite SQLite-style `?` positional placeholders to Postgres `$1..$n`, for mutators written
 * once and run against either backend (pass as {@link PostgresBackendOptions.rewriteSql}).
 * Skips `'…'` string literals (with `''` escapes), `"…"` quoted identifiers, `--` line comments,
 * and non-nested C-style block comments. Do not mix `?` and `$n` styles in one statement.
 */
export function questionToDollarParams(sql: string): string {
  let out = "";
  let n = 0;
  let i = 0;
  while (i < sql.length) {
    const c = sql[i];
    if (c === "?") {
      out += `$${++n}`;
      i += 1;
    } else if (c === "'" || c === '"') {
      // consume the quoted span; a doubled quote is an escape inside it
      const quote = c;
      let j = i + 1;
      while (j < sql.length) {
        if (sql[j] === quote) {
          if (sql[j + 1] === quote) j += 2;
          else break;
        } else {
          j += 1;
        }
      }
      out += sql.slice(i, j + 1);
      i = j + 1;
    } else if (c === "-" && sql[i + 1] === "-") {
      const end = sql.indexOf("\n", i);
      const j = end === -1 ? sql.length : end;
      out += sql.slice(i, j);
      i = j;
    } else if (c === "/" && sql[i + 1] === "*") {
      const end = sql.indexOf("*/", i + 2);
      const j = end === -1 ? sql.length : end + 2;
      out += sql.slice(i, j);
      i = j;
    } else {
      out += c;
      i += 1;
    }
  }
  return out;
}

export function defineApiQueries<User, Q extends ApiQueries<User>>(queries: Q): Q {
  return queries;
}

/**
 * Register a list of co-located client {@link NamedQuery `defineQuery`} values as the server's query
 * surface — the bulk, no-boilerplate counterpart to {@link defineApiQueries}. Each query already
 * carries its wire `name` and a `resolve` that re-runs its validator on the UNTRUSTED wire args and
 * builds the authoritative `Query`, so the server just imports every co-located query and hands the
 * list here. The same validated args build a byte-identical AST on both tiers.
 *
 * The query's AUTHORITATIVE {@link ApiContext} is forwarded as `resolve`'s ctx — so a context-scoped
 * `defineQuery` (e.g. "my issues") is built from the server's trusted principal, never the client's.
 * A context-free query simply ignores the extra argument.
 *
 * Use {@link defineApiQueries} instead (or in addition) only when the server must DIVERGE from the
 * client — define a server-specific `defineQuery` with the same name and register that.
 *
 * ```ts
 * queries: registerQueries<User>([issuesPageQuery, issueDetailQuery, recentCommentsQuery, usersQuery]),
 * ```
 */
export function registerQueries<User>(queries: readonly NamedQuery<any, any, any>[]): ApiQueries<User> {
  const out: Record<string, ApiQuery<User, any>> = {};
  for (const query of queries) {
    if (Object.prototype.hasOwnProperty.call(out, query.queryName)) {
      throw new Error(`registerQueries: duplicate query name "${query.queryName}"`);
    }
    const wrapped: ApiQuery<User, any> = (ctx, args) => query.resolve(args, ctx);
    // The §2.1 realtime label survives this seam (read it back with {@link queryRealtimeLabel}) —
    // the lease path looks up (room profile, args mapping) by query name. Unlabeled queries get
    // the exact bare wrapper they always did.
    out[query.queryName] = query.realtime === undefined ? wrapped : attachRealtimeLabel(wrapped, query.realtime);
  }
  return out;
}

export function defineApiMutators<User, M extends ApiMutators<User>>(mutators: M): M {
  return mutators;
}

/**
 * Bulk-register a SHARED (generator) mutator registry as server mutators — the mutator twin of
 * {@link registerQueries} (which does the same for co-located `defineQuery` values). Each shared
 * mutator carries its own arg validator (`shared(schema, gen)`), so this wraps every one with the
 * UNIVERSAL server triad and nothing else: parse the UNTRUSTED wire args (its `.args`), map the server
 * {@link MutationContext} to the shared {@link MutatorCtx} principal, and drive the SAME body the
 * client predicts ({@link runSharedMutation}). The point is that a shared mutator whose server run
 * adds NO authority beyond that triad needs no hand-written wrapper.
 *
 * Server-only AUTHORITY the client cannot predict (a title guard, an owner-gated cascade, a
 * `NOT EXISTS` dedup) stays an explicit {@link ApiMutator} that OVERRIDES the auto-wrapped default —
 * spread this first, then the overrides win by key:
 *
 * ```ts
 * mutators: defineApiMutators({
 *   ...sharedApiMutators(sharedMutators, (ctx) => ({ user: requireUser(ctx.user) })),
 *   createIssue: withTitleGuard(sharedMutators.createIssue), // + server-only policy
 *   deleteIssue: async (tx, raw, ctx) => { ... },            // raw owner-gated cascade
 * }),
 * ```
 */
export function sharedApiMutators<User>(
  registry: Record<string, SharedMutatorWithArgs<any>>,
  principal: (ctx: MutationContext<User>) => MutatorCtx,
): ApiMutators<User> {
  const out: ApiMutators<User> = {};
  for (const [name, mutator] of Object.entries(registry)) {
    out[name] = (tx, raw, ctx) => runSharedMutation(mutator, mutator.args.parse(raw), principal(ctx), tx);
  }
  return out;
}

/**
 * Wrap a SHARED (generator) mutator with a row-level ACCESS GUARD — the multi-tenant authz twin of
 * {@link sharedApiMutators}. It parses the untrusted wire args, derives the {@link MutatorCtx}
 * principal (the SAME mapping you pass to `sharedApiMutators`), evaluates `predicate` against the OPEN
 * mutation txn (so it can READ the rows the write depends on), and throws `forbidden` (403 — the
 * client's optimistic write snaps back) when access is denied; otherwise it drives the SAME body the
 * client predicts ({@link runSharedMutation}). Use it for the entries that need server-only authority
 * the client cannot predict, OVERRIDING the auto-wrapped default (spread `sharedApiMutators(...)`
 * first, then the guarded overrides win by key):
 *
 * ```ts
 * const principal = (ctx) => ({ user: requireUser(ctx.user) });
 * mutators: defineApiMutators({
 *   ...sharedApiMutators(sharedMutators, principal),
 *   updateSlide: guardMutator(sharedMutators.updateSlide, principal,
 *     async (tx, a, { user }) =>
 *       (await tx.query(q.slide.where.id(a.slideId).where(editableBy(user)).one())) != null,
 *     { message: "not permitted to edit this slide" }),
 * }),
 * ```
 *
 * The predicate keeps the shared body READ-FREE, so the client's `.folded` hot paths (drag/keystroke)
 * still fold — the read is server-side only. Return `false` to deny (→ the default or `opts.message`
 * forbidden); return `true`/nothing to allow. To reject with a different status/message (a business
 * rejection, a not-found), throw a {@link RindleApiError} from inside the predicate instead. `principal`
 * runs before the predicate, so it too may throw `forbidden` for an anonymous caller.
 */
export function guardMutator<User, Args>(
  gen: SharedMutatorWithArgs<Args>,
  principal: (ctx: MutationContext<User>) => MutatorCtx,
  predicate: (tx: ServerMutationTx, args: Args, ctx: MutatorCtx) => boolean | void | Promise<boolean | void>,
  opts?: { message?: string },
): ApiMutator<User, unknown> {
  return async (tx, raw, ctx) => {
    const args = gen.args.parse(raw);
    const pctx = principal(ctx);
    if ((await predicate(tx, args, pctx)) === false) {
      throw new RindleApiError("forbidden", opts?.message ?? "not permitted", 403);
    }
    return runSharedMutation(gen, args, pctx, tx);
  };
}

/** One exemplar invocation for {@link dumpQueryShapes} — the `args`/`user` a query is built with.
 *  Literal values never matter to the dump (shapes are deduped with literals stripped); what an
 *  exemplar buys is BRANCH coverage, so supply one per code path a query function can take
 *  (an optional filter present/absent, each enum axis, …). */
export interface ShapeExemplar<User = unknown> {
  args?: unknown;
  user?: User;
}

/** The query-shapes document `rindle indices suggest` consumes: the app's synced tables (name +
 *  primary key) and one wire AST per structurally distinct shape a registered query can build. */
export interface QueryShapesDoc {
  tables: Array<{ name: string; primaryKey: string[] }>;
  queries: Array<{ name: string; ast: Ast }>;
}

/**
 * Dump every registered named query's wire AST — feeder 1 ("exemplar enumeration") of
 * `rindle indices suggest` (docs/INDEXING.md applied mechanically to the query set).
 *
 * Because named queries are FUNCTIONS of `(args, ctx)`, one query can build structurally
 * different ASTs on different args; each exemplar invocation contributes its shape, and shapes
 * that differ only in literal values (a limit, a filter string) dedupe to one entry. A query
 * with no configured exemplars is invoked once with no args. The registry is the app's whole
 * server-side query surface, so the resulting document is the complete static shape set —
 * modulo arg-value-dependent branches, which need an exemplar (or runtime shape recording) to
 * surface.
 */
export async function dumpQueryShapes<User>(opts: {
  schema: Schema;
  queries: ApiQueries<User>;
  exemplars?: Partial<Record<string, ReadonlyArray<ShapeExemplar<User>>>>;
}): Promise<QueryShapesDoc> {
  const tables = Object.values(opts.schema.tables)
    // Local-only tables (both `true` and `"session"`) live in the browser's memory source,
    // never a TableSource — no indexes.
    .filter((t) => !t.local)
    .map((t) => ({ name: t.name, primaryKey: [...t.primaryKey] }))
    .sort((a, b) => a.name.localeCompare(b.name));
  const queries: QueryShapesDoc["queries"] = [];
  for (const [name, query] of Object.entries(opts.queries).sort(([a], [b]) => a.localeCompare(b))) {
    const seen = new Set<string>();
    for (const ex of opts.exemplars?.[name] ?? [{}]) {
      const ast = queryResultToAst(await query({ user: ex.user as User }, ex.args));
      const key = JSON.stringify(normalizeShape(ast));
      if (seen.has(key)) continue;
      seen.add(key);
      queries.push({ name: seen.size > 1 ? `${name}#${seen.size}` : name, ast });
    }
  }
  return { tables, queries };
}

/** The literal-stripped structure of an AST — the dedupe key for {@link dumpQueryShapes}. */
function normalizeShape(ast: Ast): Record<string, unknown> {
  return {
    table: ast.table,
    where: ast.where && normalizeCondition(ast.where),
    related: ast.related?.map((r) => ({
      correlation: r.correlation,
      subquery: normalizeShape(r.subquery),
    })),
    orderBy: ast.orderBy,
    limit: ast.limit !== undefined,
    start: ast.start
      ? { keys: Object.keys(ast.start.row).sort(), exclusive: ast.start.exclusive }
      : undefined,
    aggregate: ast.aggregate,
    groupBy: ast.groupBy,
    having: ast.having && normalizeCondition(ast.having),
    one: ast.one,
  };
}

function normalizeCondition(c: Condition): unknown {
  switch (c.type) {
    case "simple":
      return {
        type: c.type,
        op: c.op,
        left: c.left,
        right: c.right.type === "literal" ? { type: "literal" } : c.right,
      };
    case "and":
    case "or":
      return { type: c.type, conditions: c.conditions.map(normalizeCondition) };
    case "correlatedSubquery":
      return {
        type: c.type,
        op: c.op,
        related: {
          correlation: c.related.correlation,
          subquery: normalizeShape(c.related.subquery),
        },
      };
  }
}

/** Keep outside-SQL driver failures on the infrastructure path even when they happen before the
 * scoped mutator has opened its mutation transaction. */
function scopedOutsideSql(sql: ServerSql | undefined): ServerSql {
  const unavailable = (): BackendError =>
    new BackendError(new Error("scope.sql is unavailable on this custom MutationBackend"));
  // An unencodable bind is a deterministic authoring error, not a database failure. Wrapping it in
  // BackendError would latch `scope.infra` and retry the envelope forever; leaving it a plain throw
  // lets the scoped harness treat it as a business rejection and advance lmid.
  const infra = (error: unknown): unknown =>
    isUnencodableBind(error) ? new Error(errMessage(error)) : error instanceof BackendError ? error : new BackendError(error);
  return {
    async execute(text, params = []) {
      if (!sql) throw unavailable();
      try {
        await sql.execute(text, params);
      } catch (error) {
        throw infra(error);
      }
    },
    async batch(statements) {
      if (!sql) throw unavailable();
      try {
        await sql.batch(statements);
      } catch (error) {
        throw infra(error);
      }
    },
    async query<Row = Record<string, unknown>>(text: string, params: readonly WireValue[] = []): Promise<Row[]> {
      if (!sql) throw unavailable();
      try {
        return await sql.query<Row>(text, params);
      } catch (error) {
        throw infra(error);
      }
    },
  };
}

/**
 * The runtime {@link MutationScope} handed to a {@link ScopedMutator}. It owns the single atomic
 * transaction (delegating to {@link MutationBackend.runMutation} — the exact machinery a tx-form
 * mutator uses), but lets the AUTHOR decide when it opens, so server-only work can run outside it.
 *
 * It records its outcome so the harness — not the author — enforces the `lmid`-always-advances
 * invariant: `phase` reports whether the tx committed, business-rejected, or never ran, and `infra`
 * latches a backend (DB) failure. Because the backend's `runMutation` RETURNS `{accepted:false}` for
 * a business rejection (having already advanced `lmid` alone) and THROWS only for infra, `transact`
 * can cleanly re-throw {@link MutationRejected} on the former (for author compensation) and propagate
 * the raw driver error on the latter.
 */
class MutationScopeImpl implements MutationScope {
  private attempted = false;
  private readonly backend: MutationBackend;
  private readonly envelope: MutationEnvelope;
  private readonly render: RenderIndex;
  readonly sql: ServerSql;
  /** Set once `transact` resolved through the backend (accepted OR business-rejected). */
  outcome?: MutationOutcome;
  /** The value the backend threw on INFRA (the DB failed) — always propagated, never an `lmid`
   *  advance. Its presence is tracked by {@link infraLatched}, NOT by testing this for `undefined`:
   *  a driver may legitimately reject with a falsy value, and misreading that as "no infra" would
   *  reclassify a lost-connection failure as a business rejection and wrongly advance `lmid`. */
  infra?: unknown;
  /** True once an INFRA failure latched, regardless of its (possibly falsy) value. */
  infraLatched = false;
  /** The in-flight `transact` promise. `settle` awaits it before sealing, so a transact the author
   *  FORGOT to await (a floating promise — nothing here lints against it) is still resolved to its
   *  real outcome first; otherwise the seal would read `untouched`, reply with a phantom no-op, and
   *  let the real write commit out-of-band after the response was already sent. */
  pending?: Promise<void>;

  constructor(backend: MutationBackend, envelope: MutationEnvelope, render: RenderIndex) {
    this.backend = backend;
    this.envelope = envelope;
    this.render = render;
    this.sql = scopedOutsideSql(backend.outsideSql);
  }

  transact(
    first: SharedMutator<any, any> | ((tx: ServerMutationTx) => unknown),
    args?: unknown,
    ctx?: MutatorCtx,
  ): Promise<any> {
    if (this.attempted) throw new Error("scope.transact may be called at most once per mutation");
    this.attempted = true;
    const promise = this.drive(first, args, ctx);
    // Record the in-flight promise so `settle` can await it even when the author didn't. Errors are
    // latched onto `this` (outcome / infra), so this tracking copy swallows them — the author's
    // returned `promise` still rejects for them to await/catch.
    this.pending = promise.then(
      () => undefined,
      () => undefined,
    );
    return promise;
  }

  private async drive(
    first: SharedMutator<any, any> | ((tx: ServerMutationTx) => unknown),
    args?: unknown,
    ctx?: MutatorCtx,
  ): Promise<unknown> {
    // The callback form's return value, captured INSIDE the transaction and handed back only below,
    // after the commit is confirmed accepted — so a value can never describe work that rolled back.
    let value: unknown;
    // A shared (generator) mutator is driven via the isomorphic seam; a plain callback gets the raw tx.
    const run = isGeneratorMutator(first)
      ? async (tx: ServerMutationTx) => {
          await runSharedMutation(first as SharedMutator<unknown, MutatorCtx>, args, ctx as MutatorCtx, tx);
        }
      : async (tx: ServerMutationTx) => {
          value = await (first as (tx: ServerMutationTx) => unknown)(tx);
        };
    let outcome: MutationOutcome;
    try {
      outcome = await this.backend.runMutation({ envelope: this.envelope, render: this.render, run });
    } catch (err) {
      this.infra = err; // the backend throws ONLY for infra; a business rejection returns {accepted:false}
      this.infraLatched = true;
      throw err;
    }
    this.outcome = outcome;
    if (!outcome.accepted) throw new MutationRejected(outcome.reason);
    return value;
  }
}

/** Resolve {@link RindleApiServerOptions.rindle} into concrete `daemon` + `database` fields (env
 *  fallback, explicit fields win), and assert the one hard requirement: SOME control-plane client
 *  must exist. Returned options carry a non-optional `daemon`, which the rest of construction
 *  relies on. */
function withResolvedConnection<User>(
  opts: RindleApiServerOptions<User>,
): RindleApiServerOptions<User> & { daemon: RindleDaemonClient } {
  const connection = opts.rindle;
  if (connection === undefined) {
    if (opts.daemon === undefined) {
      throw new TypeError(
        "createRindleApiServer needs a connection: pass `rindle: { url, token }` (or `rindle: {}` " +
          "under `rindle dev`), or an explicit `daemon` client",
      );
    }
    return opts as RindleApiServerOptions<User> & { daemon: RindleDaemonClient };
  }
  // Determine which legs actually need deriving BEFORE requiring an endpoint: when every leg is
  // explicitly configured, `rindle` is inert and must not demand a unified URL — explicit fields
  // always win. The database leg counts as covered by any explicit SQL machinery (`database`,
  // `sql`, or `backend`) — an explicit session or backend must not gain a second, owned SQL
  // client beside it.
  const needsDaemon = opts.daemon === undefined;
  const needsDatabase =
    opts.database === undefined && opts.sql === undefined && opts.backend === undefined;
  if (!needsDaemon && !needsDatabase) {
    return opts as RindleApiServerOptions<User> & { daemon: RindleDaemonClient };
  }
  // `process` is absent on some serverless runtimes; the env fallback simply does not apply there.
  const env = typeof process !== "undefined" ? process.env : undefined;
  const url = connection.url ?? env?.RINDLE_URL;
  if (url === undefined || url === "") {
    throw new TypeError(
      "rindle.url is required: pass it explicitly, or run under `rindle dev`, which exports " +
        "RINDLE_URL once the rendered topology serves one ingress",
    );
  }
  const token = connection.token ?? env?.RINDLE_DATABASE_TOKEN;
  if (token === undefined || token === "") {
    throw new TypeError(
      "rindle.token is required to derive the unified connection: pass it explicitly, run under " +
        "`rindle dev` (which exports RINDLE_DATABASE_TOKEN), or configure `database`/`sql`/" +
        "`backend` and `daemon` yourself",
    );
  }
  const daemon =
    opts.daemon ??
    new HttpRindleDaemonClient({
      baseUrl: url,
      headers: { authorization: `Bearer ${token}` },
    });
  if (!needsDatabase) {
    return { ...opts, daemon };
  }
  return { ...opts, daemon, database: { url, authToken: token } };
}

/** Resolve the public subscription endpoint carried by query leases. This is intentionally derived
 *  only when the unified `rindle` model is configured; a caller supplying only a custom/split
 *  daemon retains the legacy wire shape. */
function resolveRindleWsEndpoint(
  connection: RindleConnectionOptions | undefined,
  deriveFromOrigin: boolean,
): string | undefined {
  if (connection === undefined) return undefined;
  if (connection.wsUrl !== undefined && connection.wsUrl !== "") return connection.wsUrl;
  if (!deriveFromOrigin) return undefined;
  const env = typeof process !== "undefined" ? process.env : undefined;
  const origin = connection.url ?? env?.RINDLE_URL;
  if (origin === undefined || origin === "") return undefined;
  const url = new URL(origin);
  if (url.protocol === "http:") url.protocol = "ws:";
  else if (url.protocol === "https:") url.protocol = "wss:";
  else {
    throw new TypeError(`rindle.url must use http: or https: to derive a WebSocket endpoint (got ${url.protocol})`);
  }
  const rendered = url.toString();
  return url.pathname === "/" && url.search === "" && url.hash === "" ? rendered.replace(/\/$/, "") : rendered;
}

export function createRindleApiServer<User = unknown>(options: RindleApiServerOptions<User>): RindleApiServer<User> {
  const opts = withResolvedConnection(options);
  // An origin-derived socket belongs only to an origin-derived daemon. If the caller supplied a
  // split/custom daemon, its leases must not advertise the unrelated SQL origin; only an explicit
  // wsUrl is sufficiently intentional to accompany that override.
  const wsEndpoint = resolveRindleWsEndpoint(opts.rindle, options.daemon === undefined);
  const routes = { ...DEFAULT_RINDLE_API_ROUTES, ...opts.routes };
  const mode = opts.mode ?? "normalized";
  // Explicit backend wins; otherwise prefer the versioned Rindle-SQL mutation transport and retain
  // daemonBackend as the compatibility path for deployments that have not exposed it yet.
  let ownedSql: SqlClient | undefined;
  let backend: MutationBackend;
  if (opts.backend !== undefined) {
    backend = opts.backend;
  } else {
    if (opts.database !== undefined && opts.sql !== undefined) {
      throw new TypeError("configure either database or sql, not both");
    }
    const sql =
      opts.sql ??
      (opts.database !== undefined
        ? // Default FIRST so `database.intMode` can override it; see RindleDatabaseOptions.
          (ownedSql = createSqlClient({ intMode: "number", ...opts.database }))
        : undefined);
    backend = sql === undefined ? daemonBackend(opts.daemon) : sqlBackend(sql);
  }
  // Schema-derived render metadata for logical mutator writes; `{}` when no schema is configured (a
  // logical op then throws loudly — the tx never silently drops a write). Each backend renders in its
  // own dialect (`backend.dialect`: daemon→sqlite, postgres→postgres).
  const renderIndex: RenderIndex = opts.schema ? buildRenderIndex(opts.schema) : {};
  // Names that are ALSO configured pins — a lease for one is forced to a `pinned` policy (the lazy
  // floor, §4.1) so the first viewer to route to a follower warms it for late joiners.
  const pinnedNames = new Set((opts.pinnedQueries ?? []).map((p) => p.name));

  // Rindle Realtime declaration layer (RINDLE-REALTIME-QUERY-ENABLEMENT §2, slice G-iv-a):
  // compile the named room profiles and run every "loud at registration" (§2.3) check NOW —
  // construction is the moment a misconfigured profile or label can still fail the deploy,
  // not a 3am room boot. The legacy flat `resolveFootprint` stays the anonymous profile and
  // is deliberately NOT probed or validated (byte-identical legacy behavior).
  const realtime = opts.realtime;
  const roomProfiles = compileRoomProfiles<User>({
    rooms: realtime?.rooms,
    schema: opts.schema,
    warn: realtime?.warn,
  });
  assertLabeledProfilesExist(opts.queries, roomProfiles);
  if (realtime !== undefined && roomProfiles.size === 0 && realtime.resolveFootprint === undefined) {
    throw new Error(
      "realtime: configure at least one room profile (realtime.rooms) or the legacy resolveFootprint — " +
        "a realtime host with neither can never boot a room.",
    );
  }

  // Resolve a named query (+ args) to its AST under a given context — the shared path for both
  // a per-viewer lease and a system-level pin (which skips per-user authorization).
  const resolveAst = async (name: string, args: unknown, context: ApiContext<User>): Promise<Ast> => {
    const query = opts.queries?.[name];
    if (!query) throw new RindleApiError("not-found", `unknown query: ${name}`, 404);
    const result = opts.runQuery
      ? await opts.runQuery({ user: context.user, name, args, query, context })
      : await query(context, args as never);
    return queryResultToAst(result);
  };

  // ---------------------------------------------------------------- the room-serve decision
  //
  // RINDLE-REALTIME-QUERY-ENABLEMENT §2.1 lease-flow steps 2–5, slice G-iv-b. Everything here is
  // FAIL-OPEN: any missing wiring, refused proof, or thrown error means the lease is served from
  // the daemon EXACTLY as today (no `realtime` block, top-level fields untouched) plus a one-time
  // diagnostic — a coverage/config problem must never block a lease.

  const realtimeWarn = realtime?.warn ?? ((message: string) => console.warn(message));
  // One-time per (queryName, profile): the serve decision runs on EVERY lease, so an uncovered
  // labeled query would otherwise warn once per viewer per mount.
  const warnedRoomServe = new Set<string>();
  const warnRoomServeOnce = (queryName: string, profile: string, reasons: readonly string[]): void => {
    const key = `${queryName}\u0000${profile}`;
    if (warnedRoomServe.has(key)) return;
    warnedRoomServe.add(key);
    realtimeWarn(
      `query "${queryName}" is labeled realtime (room profile "${profile}") but is NOT room-served — ` +
        `${reasons.join("; ")}. It serves from the daemon (correct, just not room-accelerated). ` +
        `This warning fires once per (query, profile).`,
    );
  };

  // Room-served aggregates are refused: a room-retargeted query carrying a count()/reduce reads an
  // `__agg` head only the daemon feed maintains, and the client's room gate DROPS the `__agg` rows
  // the room publishes — a known-unsupported shape (302 post-impl review). Room serving otherwise
  // trusts the declaration (302 §5: declared, not derived); this one shape stays a policy refusal
  // until room-served aggregates are designed.
  const AGGREGATE_REFUSAL =
    "the query contains an aggregate/reduce shape — room-served aggregates are not yet supported (the room gate drops `__agg` rows)";

  const maybeRoomServe = async (
    input: QueryLeaseRequest<User>,
    queryAst: Ast,
    context: ApiContext<User>,
    subject: string | undefined,
  ): Promise<QueryLeaseRealtime | undefined> => {
    // (a) the label + (b) its profile — the fast bail keeps unlabeled leases byte-identical.
    const label = queryRealtimeLabel(opts.queries?.[input.name]);
    if (label === undefined) return undefined;
    const profile = roomProfiles.get(label.room);
    if (profile === undefined) return undefined; // unreachable: construction asserted it exists
    try {
      // (c) the wiring gates — each absence fail-opens with a one-time, named reason.
      if (realtime?.locateRoom === undefined) {
        warnRoomServeOnce(input.name, profile.name, ["realtime.locateRoom is not configured"]);
        return undefined;
      }
      const tokenKey = realtime.roomTokenKey;
      if (tokenKey === undefined) {
        warnRoomServeOnce(input.name, profile.name, [
          "realtime.roomTokenKey is not configured — the room lease token cannot be signed",
        ]);
        return undefined;
      }
      // The token's subject: the same resolved subject the daemon lease carries, else the
      // browser's clientId. The shell refuses a subject-less token, so with neither we fail open.
      const sub = subject ?? input.clientId;
      if (sub === undefined) {
        warnRoomServeOnce(input.name, profile.name, [
          "no token subject — configure `subject` (or have the client send clientId)",
        ]);
        return undefined;
      }

      // §2.1: (roomProfile, roomArgs) via the label's args mapping; key + doc minted SERVER-side
      // (input.args just passed the query's own validation inside resolveAst).
      const roomArgs = label.args !== undefined ? label.args(input.args) : input.args;
      const key = profile.key(roomArgs);
      const doc = mintRoomDoc(profile.name, key);

      // The profile footprint for THIS key under the request ctx (works for non-static
      // profiles), with the §2.3 unwindowed backstop `/room-boot` also applies.
      const footprintAst = queryResultToAst(await profile.footprint(key, context));
      assertUnwindowedFootprint(footprintAst, profile.name);

      // Trust the declaration (302 §5): a labeled + wired query is room-served, no coverage proof.
      // The one shape still refused is the aggregate (a policy gate, not a coverage verdict).
      if (astHasAggregate(queryAst)) {
        warnRoomServeOnce(input.name, profile.name, [AGGREGATE_REFUSAL]);
        return undefined;
      }

      // Assemble the realtime block. The room endpoint rides ITS OWN field
      // (`realtime.wsEndpoint`) — a separate connection from the daemon session's fixed ws host.
      const { wsEndpoint } = await realtime.locateRoom(doc);
      const now = Date.now();
      const ttlMs = realtime.roomTokenTtlMs ?? DEFAULT_ROOM_TOKEN_TTL_MS;
      const { mintRoomToken, scopeSpecsHash } = await loadRoomTokenModule();
      // The lease-wire specs, hashed ONCE: the same value is stamped on the token (so the
      // shell can flag scope skew — a profile edited under a live room, whose gate armed
      // with the OLD specs at boot) and returned as the client's `tables`.
      const tables = compileRoomTableSpecs(footprintAst, profile.context);
      const roomToken = await mintRoomToken({
        doc,
        ast: queryAst, // the APPROVED resolved AST — the client carries it, it can't mint/alter it
        sub,
        kid: tokenKey.kid,
        key: tokenKey.secret,
        ttlMs,
        now,
        scopesHash: scopeSpecsHash(tables),
      });
      return {
        // `parse_source_key` (rust/src/wasm/db.rs): anything but the reserved "daemon" is a room
        // source; the client-store convention is `room:` + the wire doc.
        sourceKey: `room:${doc}`,
        wsEndpoint,
        roomToken,
        exp: now + ttlMs,
        doc,
        tables,
      };
    } catch (e) {
      // Fail open — a lease is never blocked on room-serve wiring (footprint resolution,
      // locateRoom, token minting). A failure here just serves the query from the daemon.
      warnRoomServeOnce(input.name, profile.name, [`room-serve failed: ${errMessage(e)}`]);
      return undefined;
    }
  };

  // ---------------------------------------------------------------- the §4 lifecycle mint (I-iii)
  //
  // Gated on the OPT-IN `realtime.lifecycle` block: absent, this whole section is dead code and
  // every lease response is byte-identical to pre-lifecycle. Present, a labeled lease gains the
  // doorbell system lease and a ROOM-SERVED one the fence bundle (see {@link QueryLeaseLifecycle}).
  // FAIL-OPEN like the room-serve decision: a mint failure (e.g. a daemon that never ran
  // `enable_realtime_lifecycle`) warns once per query and the lease ships without the block.

  const warnedLifecycle = new Set<string>();
  const warnLifecycleOnce = (queryName: string, reason: string): void => {
    if (warnedLifecycle.has(queryName)) return;
    warnedLifecycle.add(queryName);
    realtimeWarn(
      `query "${queryName}" is realtime-labeled with lifecycle configured, but its lifecycle ` +
        `system leases were not minted — ${reason}. The lease serves without the lifecycle block ` +
        `(correct, just no §4 upgrade/downgrade plane). This warning fires once per query.`,
    );
  };

  /** THE SCOPE-KEY DECISION (§4.1): the doorbell scope IS the wire room doc — `"<profile>/<key>"`
   *  via {@link mintRoomDoc}, the same computation `maybeRoomServe` runs (label args mapping →
   *  `profile.key`) and the same key `locateRoom`/`/room-boot` address the room by. Occupancy
   *  (I-iv writes the `_rindle_scope_sessions` rows) must be counted on EXACTLY the key the 1→2
   *  transition provisions, and this is that key. Computed independently of the room-serve
   *  decision on purpose: the doorbell rides every LABELED lease — an uncovered/unwired labeled
   *  query still counts toward occupancy (its collaborators still want the upgrade). */
  const lifecycleScopeDoc = (input: QueryLeaseRequest<User>): string | undefined => {
    const label = queryRealtimeLabel(opts.queries?.[input.name]);
    if (label === undefined) return undefined; // unlabeled — no scope to count on
    const profile = roomProfiles.get(label.room);
    if (profile === undefined) return undefined; // unreachable: construction asserted it exists
    const roomArgs = label.args !== undefined ? label.args(input.args) : input.args;
    return mintRoomDoc(profile.name, profile.key(roomArgs));
  };

  const maybeLifecycle = async (
    input: QueryLeaseRequest<User>,
    roomServed: boolean,
    subject: string | undefined,
    routingKey: string | undefined,
  ): Promise<QueryLeaseLifecycle | undefined> => {
    if (realtime?.lifecycle === undefined) return undefined; // the opt-in gate — mint NOTHING
    try {
      const doc = lifecycleScopeDoc(input);
      if (doc === undefined) return undefined;
      // Each system lease is an ordinary daemon materialization (the room-boot direct pattern),
      // carrying the SAME subject/routingKey as the primary lease so a routed deploy co-locates
      // the system streams on the follower the client's daemon session already lives on. The
      // daemon dedups by canonical query, so N clients' doorbells over one scope share ONE
      // materialization (each still minting its own leaseToken); the client-scoped fence ASTs
      // are per-client by construction.
      const mint = (ast: Ast) =>
        opts.daemon.materialize({
          ast,
          mode,
          subject,
          leaseTtlMs: opts.leaseTtlMs,
          metadata: routingKey !== undefined ? { routingKey } : undefined,
          // Lifecycle leases are follower-local exactly like the primary lease. Forward the SAME
          // opaque placement ticket so every doorbell/fence materialization is minted on the
          // browser socket's follower instead of independently anycasting across the fleet.
          ...(input.affinity !== undefined ? { affinity: input.affinity } : {}),
        });
      const lease = (table: string, out: MaterializeOutput, id: { scope?: string; doc?: string; clientId?: string }): QueryLeaseLifecycleLease => ({
        table,
        leaseToken: out.leaseToken,
        ...(id.scope !== undefined ? { scope: id.scope } : {}),
        ...(id.doc !== undefined ? { doc: id.doc } : {}),
        ...(id.clientId !== undefined ? { clientId: id.clientId } : {}),
      });
      const lifecycle: QueryLeaseLifecycle = {
        doorbell: lease(SCOPE_SESSIONS_TABLE, await mint(scopeSessionsAst(doc)), { scope: doc }),
      };
      // The fence bundle only where a room domain exists to fence (room-served leases): the
      // §4.2 watermark, the §7.1 daemon-carried ledger, and the §3.3 outcome rows.
      if (roomServed) {
        const clientId = input.clientId;
        lifecycle.fence = [
          lease(ROOM_WATERMARK_TABLE, await mint(roomWatermarkAst(doc)), { doc }),
          lease(ROOM_CLIENT_MUTATIONS_TABLE, await mint(docClientAst(ROOM_CLIENT_MUTATIONS_TABLE, doc, clientId)), { doc, clientId }),
          lease(ROOM_MUTATION_OUTCOMES_TABLE, await mint(docClientAst(ROOM_MUTATION_OUTCOMES_TABLE, doc, clientId)), { doc, clientId }),
        ];
      }
      return lifecycle;
    } catch (e) {
      warnLifecycleOnce(input.name, errMessage(e)); // fail open — a lease is never blocked
      return undefined;
    }
  };

  // ------------------------------------------------------------ the §4.1 occupancy gate (I-iv)
  //
  // Runs on EVERY labeled lease under the opt-in `realtime.lifecycle` config (mint AND renewal —
  // both land on this same route), BEFORE the room-serve decision: (1) sweep + upsert the
  // caller's session row through the normal write path (the write is the doorbell — I-i's CDC
  // capture fans the row delta to every solo watcher's doorbell subscription), then (2) read the
  // occupancy count and return the D6 gate verdict `maybeRoomServe` is conditioned on. The upsert
  // deliberately precedes the count so the caller's own row is on disk when the verdict is
  // computed (its own presence rides the `+ 1`, and — more importantly — a concurrent second
  // client's read sees it). Ordering within the pair is otherwise value-neutral: the count
  // EXCLUDES the caller's clientId and adds the `+ 1` analytically.
  //
  // THE RENEWAL-vs-FRESH DECISION (grounded here because the task forces it): this server is
  // stateless and the lease request carries no "I am currently room-attached" field, so the gate
  // CANNOT distinguish a fresh mint from a live room's renewal. Instead of gating on the raw
  // count (which would suppress a momentarily-solo room's renewal and force the loud client-side
  // downgrade anomaly), the gate applies the §9.1 hysteresis DIRECTLY FROM THE LINGERING ROWS the
  // D4 sweep preserves: room-serve iff `liveOthers + self ≥ minSessions` OR some other session
  // expired within `graceMs`. A renewal is therefore never suppressed until the scope has been
  // solo SUSTAINED past the grace window — which is exactly Slice I-v's downgrade condition, read
  // from the same rows; I-v replaces that post-grace loud suppression with the fenced downgrade
  // dance, refining (not re-deciding) this verdict. A truly fresh solo scope (no other row, live
  // or lingering) is suppressed immediately — the D6 point.
  //
  // Timestamps are `Date.now()` server-side throughout (mint, sweep, count): occupancy tolerates
  // clock skew between api-server instances up to ~grace — a skewed `now` moves a session between
  // "live" and "in-grace", both of which hold the gate open; only skew past the grace+slack band
  // could mis-sweep, and the slack exists to keep that band clear.
  //
  // A request with NO `clientId` (a non-shipped client — the shipped one always sends it, see
  // `postLease`) upserts NO row and contributes NOTHING to occupancy, including to its own gate:
  // it room-serves only if the OTHER sessions alone reach `minSessions` (there is no session
  // identity to count it under, D7). It still gets its doorbell (`maybeLifecycle` is independent).
  //
  // FAIL-OPEN, like every lifecycle surface: an occupancy failure (e.g. a daemon that never ran
  // `enable_realtime_lifecycle`) warns once per query and returns `true` — the gate falls away
  // and the lease serves exactly as pre-I-iv. Suppressing on infrastructure failure would turn
  // realtime off fleet-wide from one missing table; never block, never suppress, on an error.

  const warnedOccupancy = new Set<string>();
  interface LifecycleOccupancy {
    /** The D6 room-serve gate verdict — `false` ⇒ suppress the room block (solo/uncovered). */
    gateOpen: boolean;
    /** The I-v downgrade guard: a room plausibly hosts this scope (some other-session row lives
     *  or lingers). Only a `!gateOpen && roomPlausible` reply drains — never a never-shared doc. */
    roomPlausible: boolean;
    /** The wire doc the scope maps to (`"<profile>/<key>"`); `undefined` when unlabeled / off. */
    doc: string | undefined;
  }
  const lifecycleOccupancy = async (input: QueryLeaseRequest<User>): Promise<LifecycleOccupancy> => {
    const lc = realtime?.lifecycle;
    if (lc === undefined) return { gateOpen: true, roomPlausible: false, doc: undefined }; // lifecycle off — the gate does not exist (inert-until-fed)
    const doc = lifecycleScopeDoc(input);
    if (doc === undefined) return { gateOpen: true, roomPlausible: false, doc: undefined }; // unlabeled — no scope to count on, nothing to gate
    try {
      const now = Date.now();
      const minSessions = lc.minSessions ?? DEFAULT_LIFECYCLE_MIN_SESSIONS;
      const graceMs = lc.graceMs ?? DEFAULT_LIFECYCLE_GRACE_MS;
      const sessionTtlMs = lc.sessionTtlMs ?? opts.leaseTtlMs ?? DEFAULT_SESSION_TTL_MS;
      const clientId = input.clientId;
      // (1) sweep + upsert, ONE write txn (D4: the sweep shares the upsert's transaction — no
      // separate maintenance pass, and the linger bound holds atomically with the refresh).
      const statements: SqlStatement[] = [
        { sql: SESSION_SWEEP_SQL, params: [doc, now - (graceMs + SESSION_SWEEP_SLACK_MS)] },
      ];
      if (clientId !== undefined) {
        statements.push({ sql: SESSION_UPSERT_SQL, params: [doc, clientId, now + sessionTtlMs] });
      }
      await opts.daemon.executeSqlTxn({ statements });
      // (2) the count — read-your-writes via `consistency: "strong"` (see the section note above).
      const read = await opts.daemon.executeSqlRead({
        sql: clientId !== undefined ? SESSION_COUNT_OTHERS_SQL : SESSION_COUNT_SQL,
        params:
          clientId !== undefined
            ? [now, now, now - graceMs, doc, clientId]
            : [now, now, now - graceMs, doc],
        consistency: "strong",
      });
      const cells = read.rows[0] ?? [];
      const liveOthers = Number(cells[0] ?? 0); // SUM over zero rows is NULL — coerce
      const graceOthers = Number(cells[1] ?? 0);
      const totalOthers = Number(cells[2] ?? 0); // ALL other rows (any expiry, pre-sweep)
      const self = clientId !== undefined ? 1 : 0;
      return {
        gateOpen: liveOthers + self >= minSessions || graceOthers > 0,
        // Room plausibly exists ⇒ this scope was shared (a room was provisioned on the 1→2). A
        // never-shared solo doc has NO other row and must never drain (no wasted room boot).
        roomPlausible: totalOthers > 0,
        doc,
      };
    } catch (e) {
      if (!warnedOccupancy.has(input.name)) {
        warnedOccupancy.add(input.name);
        realtimeWarn(
          `query "${input.name}" is realtime-labeled with lifecycle configured, but the §4.1 ` +
            `occupancy step failed — ${errMessage(e)}. The occupancy gate fail-opens (the lease ` +
            `serves exactly as pre-I-iv; no session row was counted). This warning fires once per query.`,
        );
      }
      return { gateOpen: true, roomPlausible: false, doc };
    }
  };

  // ------------------------------------------------------------ the §4.2 downgrade drain (I-v)
  //
  // When the occupancy gate closes for a scope a room plausibly hosted, drain that room to a
  // COMMITTED flush seq and hand it back as the fence. `drainRoom` (deployment-wired to the room
  // shell / DO `/drain`) is idempotent (concurrent api-server instances may both call it) and
  // fails OPEN — a downgrade must never block a lease, so an unconfigured or throwing hook simply
  // omits the fence (warn-once) and the client falls to its loud legacy downgrade path.
  const warnedDrain = new Set<string>();
  const warnDrainOnce = (queryName: string, reason: string): void => {
    if (warnedDrain.has(queryName)) return;
    warnedDrain.add(queryName);
    realtimeWarn(
      `query "${queryName}" downgraded (occupancy gate closed) but no §4.2 fence was attached — ` +
        `${reason}. The lease ships without the fence; a room-attached client falls back to its ` +
        `loud legacy downgrade (correct, just not graceful). This warning fires once per query.`,
    );
  };
  const maybeDrainRoom = async (queryName: string, doc: string): Promise<QueryLeaseRealtimeFence | undefined> => {
    const drainRoom = realtime?.lifecycle?.drainRoom;
    if (drainRoom === undefined) {
      warnDrainOnce(queryName, "realtime.lifecycle.drainRoom is not configured");
      return undefined;
    }
    try {
      const { finalFlushSeq } = await drainRoom(doc);
      return { sourceKey: `room:${doc}`, doc, finalFlushSeq };
    } catch (e) {
      warnDrainOnce(queryName, `drainRoom threw — ${errMessage(e)}`);
      return undefined;
    }
  };

  const createQueryLease = async (input: QueryLeaseRequest<User>): Promise<QueryLeaseResponse> => {
    const context: ApiContext<User> = { user: input.user, request: input.request };
    await assertAuthorized(opts.authorizeQuery, {
      user: input.user,
      name: input.name,
      args: input.args,
      context,
    });
    const ast = await resolveAst(input.name, input.args, context);
    // Lazy pin tier (§4.1): a leased query that is ALSO a configured pin is materialized with a
    // `pinned` policy regardless of the configured default, so it survives its first viewer's
    // departure. Otherwise use the configured policy.
    const policy = pinnedNames.has(input.name)
      ? ({ kind: "pinned", name: input.name } as MaterializationPolicy)
      : await resolvePolicy(opts.materializationPolicy, input);
    const subject = await resolveSubject(opts.subject, input);
    const routingKey = await resolveRoutingKey(opts.routingKey, input);
    const out = await opts.daemon.materialize({
      ast,
      mode,
      policy,
      subject,
      leaseTtlMs: opts.leaseTtlMs,
      // The anonymous routing key rides `metadata.routingKey`; the router keys on
      // `subject ?? metadata.routingKey` (§2.2). Omitted when there is none.
      metadata: routingKey !== undefined ? { routingKey } : undefined,
      // Forward the browser's opaque affinity ticket (if any) so the fleet edge places this
      // materialize to the follower the ws is pinned to (FOLLOWER-AFFINITY-DESIGN.md §4). Opaque —
      // never verified here. Inert when the reads client is a single daemon (no fleet edge).
      ...(input.affinity !== undefined ? { affinity: input.affinity } : {}),
    });
    const res = queryLeaseResponse(out, wsEndpoint);
    // I-iv (§4.1): the occupancy step FIRST — session sweep+upsert, then the D6 gate verdict. A
    // closed gate suppresses the room-serve ONLY (the lease ships without the realtime block,
    // indistinguishable from a non-room-served query — the daemon path) while the doorbell
    // below still rides; lifecycle-off ⇒ `gateOpen: true` unconditionally and this line is inert.
    const occ = await lifecycleOccupancy(input);
    // G-iv-b: a labeled + wired query ADDITIONALLY gains the realtime block. The daemon lease
    // above is unconditional (and its fields untouched) — room-serving only ever adds a field,
    // so a non-room-served/legacy lease stays byte-identical and nothing here can block one.
    const rt = occ.gateOpen ? await maybeRoomServe(input, ast, context, subject) : undefined;
    if (rt !== undefined) res.realtime = rt;
    // I-v (§4.2): the gate CLOSED and a room plausibly hosted this scope — drain it and ride the
    // fence back so a room-attached client runs the GRACEFUL downgrade instead of the loud legacy
    // anomaly. A never-shared solo doc (`!roomPlausible`) never drains (no wasted room boot); a
    // daemon-attached client that receives a stray fence ignores it (its resolver reads only the
    // daemon fields). `drainRoom` absent/throwing ⇒ no fence (fail-open, warn-once).
    if (!occ.gateOpen && occ.roomPlausible && occ.doc !== undefined) {
      const fence = await maybeDrainRoom(input.name, occ.doc);
      if (fence !== undefined) res.realtimeFence = fence;
    }
    // I-iii: under the opt-in `realtime.lifecycle` config a LABELED lease additionally gains the
    // §4 system-stream block (doorbell always; the fence bundle iff room-served OR downgrade-fenced
    // — a downgrading client needs the watermark/ledger/outcome streams to run the ghost drop).
    // Same additive discipline as the realtime block: absent config ⇒ byte-identical response.
    const lc = await maybeLifecycle(input, rt !== undefined || res.realtimeFence !== undefined, subject, routingKey);
    if (lc !== undefined) res.lifecycle = lc;
    return res;
  };

  const readQuery = async (input: QueryReadRequest<User>): Promise<QueryReadResponse> => {
    const context: ApiContext<User> = { user: input.user, request: input.request };
    // Same per-viewer gate a live lease passes — a one-shot read returns the same rows a subscribe
    // would, so it must clear the same authorization.
    await assertAuthorized(opts.authorizeQuery, {
      user: input.user,
      name: input.name,
      args: input.args,
      context,
    });
    const ast = await resolveAst(input.name, input.args, context);
    // Scope the one-shot's dedup `QueryKey` by the SAME key the lease path routes on — the routing
    // key (`subject ?? cookie ?? clientId`) — so the warm pipeline this read leaves behind is the
    // very one the browser's follow-up `subscribe` reuses, ON THE SAME FOLLOWER HRW will place it
    // (the warm handoff, SSR-DESIGN.md §3.4 / READ-ROUTER-DESIGN.md §2.4) rather than a second,
    // viewer-mismatched materialization.
    const subject = await resolveSubject(opts.subject, input);
    const routingKey = await resolveRoutingKey(opts.routingKey, input);
    const visibilityKey = subject ?? routingKey;
    const out = await opts.daemon.query({
      ast,
      visibilityKey,
      ttlMs: opts.readIdleTtlMs,
      ...(input.affinity !== undefined ? { affinity: input.affinity } : {}),
    });
    return { rows: out.rows, cvMin: out.cvMin, queryKey: out.queryKey };
  };

  const pushMutation = async (input: PushMutationRequest<User>): Promise<PushMutationResponse> => {
    const context: ApiContext<User> = { user: input.user, request: input.request };
    const mutator = opts.mutators?.[input.envelope.name];
    // PRE-FLIGHT rejections — no txn, no data, `lmid` alone (the queue must still drain).
    if (!mutator) return reject(backend, input.envelope, `unknown mutator: ${input.envelope.name}`);
    try {
      await assertAuthorized(opts.authorizeMutation, {
        user: input.user,
        envelope: input.envelope,
        context,
      });
    } catch (err) {
      return reject(backend, input.envelope, errMessage(err));
    }
    const mctx: MutationContext<User> = {
      user: input.user,
      envelope: input.envelope,
      daemon: opts.daemon,
      request: input.request,
    };

    // SCOPED mutator (WORK-OUTSIDE-TX): the author controls the tx boundary via `scope.transact`,
    // running server-only code before/after it. The `lmid`-always-advances invariant is OURS, not
    // the author's — we seal the response from the scope's recorded state, so an early return, a
    // never-called transact, or a swallowed rejection can't wedge the client's pending queue.
    if (isScoped<User>(mutator)) {
      const scope = new MutationScopeImpl(backend, input.envelope, renderIndex);
      // A throw that reaches `settle` AFTER the outcome is already sealed (post-commit effect, or a
      // compensation handler after a business rejection) can't change the response — but it must not
      // vanish. Route it to the app's hook, else log so a failed refund is never fully silent.
      const reportSealed = (err: unknown, phase: "committed" | "rejected"): void => {
        if (opts.onScopeError) opts.onScopeError(err, { phase, envelope: input.envelope });
        else console.error(`[rindle api-server] scoped mutator ${input.envelope.name}: post-${phase} code threw (outcome already sealed):`, err);
      };
      // Derive the response from the scope's OUTCOME (not the body's return), so control flow in the
      // author's function can't skip the lmid advance. `caught` distinguishes "the body threw"
      // (present, even if the thrown value was `undefined`) from "it returned cleanly".
      const settle = async (caught?: { err: unknown }): Promise<PushMutationResponse> => {
        // Seal from the REAL outcome even if the author forgot to `await` transact: draining its
        // in-flight promise here records the outcome/infra before we read it (else a phantom no-op
        // ships while the real write commits out-of-band). Already-resolved when it WAS awaited.
        if (scope.pending) await scope.pending;
        // Infra always wins: the backend threw, the commit state is unknown — never advance lmid.
        // Keyed on the latched BOOLEAN, so a driver that rejects with a falsy value is still infra.
        if (scope.infraLatched) throw scope.infra;
        // transact resolved (committed OR business-rejected): seal from its recorded outcome. A
        // post-commit / post-reject-compensation throw can't change the sealed outcome (its effects
        // can't roll the tx back, and lmid already advanced §2.4). Rethrowing the MutationRejected is
        // the sanctioned "compensated, stay rejected" signal — expected, not surfaced. Any OTHER throw
        // (a FAILED refund, a post-commit effect) must not vanish — surface it.
        if (scope.outcome) {
          if (caught && !(caught.err instanceof MutationRejected)) {
            reportSealed(caught.err, scope.outcome.accepted ? "committed" : "rejected");
          }
          return outcomeToResponse(scope.outcome);
        }
        // Never transacted:
        if (caught) {
          // A throw before/around transact. A BackendError is the author signaling INFRA (retry);
          // any other throw is a BUSINESS rejection — advance lmid alone so the prediction snaps back.
          if (caught.err instanceof BackendError) throw caught.err.driverError;
          return reject(backend, input.envelope, errMessage(caught.err));
        }
        // Clean return with no transact — an accepted no-op that STILL advances lmid (the client
        // predicted a write; its pending entry must resolve).
        return outcomeToResponse(
          await backend.runMutation({ envelope: input.envelope, render: renderIndex, run: async () => {} }),
        );
      };
      try {
        await (mutator as unknown as ScopedMutator<User, unknown>)(scope, input.envelope.args as never, mctx);
      } catch (err) {
        return settle({ err });
      }
      return settle();
    }

    // Run the (tx-form) mutator INSIDE the backend's transaction. A throw from the mutator body is a
    // business rejection (roll data back, advance `lmid`); a BackendError (DB failure) rejects this promise.
    const outcome = await backend.runMutation({
      envelope: input.envelope,
      render: renderIndex,
      run: async (tx) => {
        const result = await mutator(tx, input.envelope.args as never, mctx);
        applyResultToTx(result, tx);
      },
    });
    return outcomeToResponse(outcome);
  };

  const pushMutations = async (input: PushMutationsRequest<User>): Promise<PushMutationResponse[]> => {
    const out: PushMutationResponse[] = [];
    for (const envelope of input.envelopes) {
      out.push(await pushMutation({ user: input.user, envelope, request: input.request }));
    }
    return out;
  };

  const assertPins = async (): Promise<void> => {
    const pins = opts.pinnedQueries;
    if (!pins?.length) return;
    const context: ApiContext<User> = { user: opts.pinUser as User, request: undefined };
    // Resolve every pin's authoritative AST under the system pin user (a transient failure on one
    // shouldn't strand the rest), then drive the warm-up — fleet fan-out via the router if
    // configured, else one materialize per pin on the single daemon.
    const resolved = await Promise.allSettled(
      pins.map(async (pin) => ({
        pin,
        req: {
          ast: await resolveAst(pin.name, pin.args ?? null, context),
          mode,
          policy: { kind: "pinned", name: pin.name } as MaterializationPolicy,
          leaseTtlMs: opts.leaseTtlMs,
        } satisfies MaterializeInput,
      })),
    );
    const failures: string[] = [];
    const pairs: Array<{ pin: PinnedQuery; req: MaterializeInput }> = [];
    resolved.forEach((r, i) => {
      if (r.status === "fulfilled") pairs.push(r.value);
      else failures.push(`${pins[i].name}: ${errMessage(r.reason)}`);
    });
    const reqs = pairs.map((p) => p.req);
    if (reqs.length) {
      if (opts.pinFanout) {
        // Push tier (§4.2): fan EACH pin across ALL live followers via the router (fire-and-forget,
        // router-stateless). A whole-fan-out failure is surfaced so the caller retries.
        try {
          await opts.pinFanout.assertPins(reqs);
        } catch (e) {
          failures.push(errMessage(e));
        }
      } else {
        // Single-daemon: materialize each pin once (the daemon dedups by canonical query).
        const results = await Promise.allSettled(reqs.map((req) => opts.daemon.materialize(req)));
        results.forEach((r, i) => {
          if (r.status === "rejected") failures.push(`${pairs[i].pin.name}: ${errMessage(r.reason)}`);
        });
      }
    }
    if (failures.length) {
      throw new Error(`assertPins: ${failures.length} failed — ${failures.join("; ")}`);
    }
  };

  // The room write-authority gate (§5.3.1): endpoints are disabled until the app opts in —
  // the `realtime` block (which also activates `/room-boot`) or the deprecated bare
  // `authorizeRoom` (trio only). Hosting an authority is never a default.
  const roomAuthorizer: Authorizer<ApiContext<User>> | undefined = realtime
    ? (realtime.authorize ?? defaultFlushGate(realtime.shellSecret))
    : opts.authorizeRoom;
  const roomGate = async (context: ApiContext<User>): Promise<void> => {
    if (!roomAuthorizer) {
      throw new RindleApiError("forbidden", "room authority not configured", 403);
    }
    await assertAuthorized(roomAuthorizer, context);
  };

  // §2.1 room-key routing: a "<profile>/<key>" doc resolves through its NAMED profile — with the
  // boot-time unwindowed backstop (§2.3), which covers footprints that weren't statically
  // resolvable at construction AND key-dependent branches that window only some docs. Anything
  // else falls through to the legacy single-profile alias BYTE-IDENTICALLY (the anonymous
  // profile, bare-key form). A named profile wins over a legacy doc that merely contains "/".
  // Returns the profile's context set beside the AST (H-iv-b: the scope-spec compilation needs
  // the §2.2 owned/followed split; the legacy anonymous profile has no declaration — empty set).
  const resolveRoomFootprint = async (
    rt: RindleRealtimeOptions<User>,
    doc: string,
    context: ApiContext<User>,
  ): Promise<{ ast: Ast; contextTables: ReadonlySet<string> }> => {
    const split = splitRoomDoc(doc);
    if (split !== undefined) {
      const profile = roomProfiles.get(split.profile);
      if (profile !== undefined) {
        const ast = queryResultToAst(await profile.footprint(split.key, context));
        assertUnwindowedFootprint(ast, profile.name);
        return { ast, contextTables: profile.context };
      }
    }
    if (rt.resolveFootprint) {
      return {
        ast: queryResultToAst(await rt.resolveFootprint(doc, context)),
        contextTables: new Set<string>(),
      };
    }
    throw new RindleApiError(
      "not-found",
      `no room profile matches doc "${doc}" — named profiles are addressed as "<profile>/<key>"`,
      404,
    );
  };

  // The store's verdict rides specific statuses + body shapes (fence / conflict /
  // identity) the room decodes — pass a daemon HTTP error through VERBATIM.
  const daemonVerdict = (e: unknown): RoomHostResponse => {
    if (e instanceof DaemonHttpError) {
      let body: unknown;
      try {
        body = JSON.parse(e.body);
      } catch {
        body = { error: e.body };
      }
      return { status: e.status, body };
    }
    throw e;
  };

  // The LM stream plane (LM-STREAM-CHECKPOINT §2). Checkpoints are SYSTEM writes — no clientID, no
  // mid, no `lmid` advance (that exists to release a CLIENT's optimistic rebase point, and a
  // server-authored checkpoint has no prediction to release) — so they ride the backend's
  // outside-transaction SQL surface, whose `batch` is one transaction on every backend. CDC → IVM
  // fanout happens on any write; it never needed an envelope.
  if (opts.streams && "tables" in opts.streams.checkpoint) {
    assertStreamTables(opts.streams.checkpoint.tables, opts.schema);
  }
  const streamPlane = opts.streams
    ? new StreamPlane<User>(
        opts.streams,
        backend.outsideSql ? { dialect: backend.dialect, sql: backend.outsideSql } : undefined,
      )
    : undefined;
  const streams = (): StreamPlane<User> => {
    if (!streamPlane) throw new RindleApiError("forbidden", "streams not configured", 403);
    return streamPlane;
  };
  // The plane's own refusal, translated at the seam (it stays free of the server's error type so
  // `streams.ts` can be imported without the server — no cycle).
  const streamForbidden = (e: unknown): never => {
    if (e instanceof StreamForbidden) throw new RindleApiError("forbidden", "rindle request forbidden", 403);
    throw e;
  };

  return {
    routes,
    close: () => {
      streamPlane?.closeSync();
      ownedSql?.close();
    },
    createQueryLease,
    readQuery,
    assertPins,
    pushMutation,
    pushMutations,
    // `async` throughout so a misconfiguration surfaces as a REJECTION like every other refusal on
    // this interface, never as a synchronous throw the transport forgot to catch.
    openStream: async (input) => streams().open(input),
    subscribeStream: async (input) => streams().subscribe(input).catch(streamForbidden),
    drainStreams: async () => streamPlane?.drainStreams(),
    handleStreamJson: async (body, context) => {
      const msg = parseObject(body, "stream request");
      const from = msg.from === undefined ? undefined : parseNumber(msg.from, "from");
      // A non-negative INTEGER, like the GET leg's clamp: a fractional `from` would yield a replay
      // chunk whose `text.length !== seq - from`, breaking the frame invariant on the client.
      if (from !== undefined && (!Number.isInteger(from) || from < 0)) {
        throw new RindleApiError("bad-request", "invalid from", 400);
      }
      return streams()
        .subscribe({
          user: context.user,
          streamId: parseString(msg.streamId, "streamId"),
          ...(from !== undefined ? { from } : {}),
          request: context.request,
        })
        .catch(streamForbidden);
    },
    streamResponse: async (request, context) => {
      try {
        const { streamId, from } = streamRequestFromHttp(request);
        const sub = await streams()
          .subscribe({ user: context.user, streamId, from, request: context.request ?? request })
          .catch(streamForbidden);
        const sse = streamFramesToSse(
          sub,
          context.keepAliveMs !== undefined ? { keepAliveMs: context.keepAliveMs } : undefined,
        );
        return new Response(sse, { headers: STREAM_SSE_HEADERS });
      } catch (e) {
        // The route helper OWNS the transport, so refusals become responses here rather than
        // throws the route forgot to catch. The generic body never reveals stream existence.
        if (e instanceof RindleApiError) {
          return new Response(JSON.stringify({ error: e.message }), {
            status: e.status,
            headers: { "content-type": "application/json" },
          });
        }
        throw e;
      }
    },
    handleApplyRowChangeTxnJson: async (body, context) => {
      await roomGate(context);
      const msg = parseObject(body, "row-change txn");
      try {
        const out = await opts.daemon.applyRowChangeTxn(msg as unknown as RowChangeTxn);
        return { status: 200, body: out };
      } catch (e) {
        return daemonVerdict(e);
      }
    },
    handleClaimRoomEpochJson: async (body, context) => {
      await roomGate(context);
      const msg = parseObject(body, "claim-room-epoch request");
      const doc = parseString(msg.doc, "doc");
      const claim = opts.daemon.claimRoomEpoch?.bind(opts.daemon);
      if (!claim) {
        throw new Error("the configured daemon client does not implement claimRoomEpoch");
      }
      try {
        return { status: 200, body: await claim({ doc }) };
      } catch (e) {
        return daemonVerdict(e);
      }
    },
    handleRoomLmidsJson: async (body, context) => {
      await roomGate(context);
      const msg = parseObject(body, "room-lmids request");
      const doc = parseString(msg.doc, "doc");
      if (!Array.isArray(msg.clients) || msg.clients.some((c) => typeof c !== "string")) {
        throw new RindleApiError("bad-request", "clients must be an array of strings", 400);
      }
      const lmids = opts.daemon.roomLmids?.bind(opts.daemon);
      if (!lmids) {
        throw new Error("the configured daemon client does not implement roomLmids");
      }
      try {
        return { status: 200, body: await lmids({ doc, clients: msg.clients as string[] }) };
      } catch (e) {
        return daemonVerdict(e);
      }
    },
    handleRoomBootJson: async (body, context) => {
      if (!realtime) {
        throw new RindleApiError("forbidden", "realtime not configured", 403);
      }
      await assertAuthorized(realtime.authorizeBoot ?? defaultBootGate(realtime.shellSecret), context);
      const msg = parseObject(body, "room-boot request");
      const doc = parseString(msg.doc, "doc");
      if (msg.instance !== undefined) parseString(msg.instance, "instance"); // diagnostic identity only
      const { ast, contextTables } = await resolveRoomFootprint(realtime, doc, context);
      const claim = opts.daemon.claimRoomEpoch?.bind(opts.daemon);
      if (!claim) {
        throw new Error("the configured daemon client does not implement claimRoomEpoch");
      }
      try {
        // Claim FIRST (§2.5): the lease below is minted for THIS placement, so a boot
        // that loses the epoch race learns it here, before any materialization exists.
        const { epoch } = await claim({ doc });
        const lease = await opts.daemon.materialize({
          ast,
          // The room's upstream leg IS the normalized protocol (§3) — never the app's
          // viewer `mode`.
          mode: "normalized",
          leaseTtlMs: realtime.upstreamLeaseTtlMs ?? opts.leaseTtlMs,
        });
        const headers = realtime.mintFlushHeaders
          ? await realtime.mintFlushHeaders({ doc, epoch })
          : {
              [ROOM_FLUSH_CREDENTIAL_HEADER]: await mintRoomFlushCredential({
                shellSecret: realtime.shellSecret,
                doc,
                epoch,
              }),
            };
        const res: RoomBootResponse = {
          epoch,
          upstreamLeaseToken: lease.leaseToken,
          // H-iv-b: the §3.3 commit-gate scope specs, for named-profile AND legacy docs alike
          // (the footprint AST is resolved either way; legacy has an empty context set).
          scopes: compileRoomScopeSpecs(ast, contextTables),
          flush: {
            urls: {
              apply: routes.applyRowChangeTxn,
              claim: routes.claimRoomEpoch,
              lmids: routes.roomLmids,
            },
            headers,
          },
        };
        if (lease.affinity !== undefined) res.upstreamAffinity = lease.affinity;
        const upstreamWsEndpoint = realtime.upstreamWsEndpoint;
        if (upstreamWsEndpoint !== undefined) res.upstreamWsEndpoint = upstreamWsEndpoint;
        return { status: 200, body: res };
      } catch (e) {
        return daemonVerdict(e);
      }
    },
    handleQueryJson: (body, context) => {
      const msg = parseObject(body, "query request");
      return createQueryLease({
        user: context.user,
        name: parseString(msg.name, "name"),
        args: msg.args ?? null,
        request: context.request,
        clientId: typeof msg.clientId === "string" ? msg.clientId : undefined,
        affinity: typeof msg.affinity === "string" ? msg.affinity : undefined,
      });
    },
    handleReadJson: (body, context) => {
      const msg = parseObject(body, "read request");
      return readQuery({
        user: context.user,
        name: parseString(msg.name, "name"),
        args: msg.args ?? null,
        request: context.request,
        clientId: typeof msg.clientId === "string" ? msg.clientId : undefined,
        affinity: typeof msg.affinity === "string" ? msg.affinity : undefined,
      });
    },
    handleMutateJson: (body, context) => {
      const msg = parseObject(body, "mutation request");
      if (Array.isArray(msg.envelopes)) {
        return pushMutations({
          user: context.user,
          envelopes: msg.envelopes.map(parseEnvelope),
          request: context.request,
        });
      }
      const envelope = parseEnvelope(msg.envelope ?? msg);
      return pushMutation({ user: context.user, envelope, request: context.request });
    },
  };
}

/** Run a SHARED generator mutator (the SAME body the client predicts) against a live server
 *  transaction (MUTATORS-ISOMORPHIC): bind the tier-agnostic {@link isoTx} factory and drive it —
 *  each yielded logical op renders + runs against `tx` (dialect SQL, per backend), each `tx.row`
 *  suspends for read-your-writes, and `tx.all` fans out. A server mutator uses this to delegate its
 *  write body after parsing untrusted args and applying its server-only authority (principal, policy).
 *  A mutator-body throw remains a business rejection; a DB failure propagates as infra. */
export function runSharedMutation<Args, Ctx extends MutatorCtx>(
  mutator: SharedMutator<Args, Ctx>,
  args: Args,
  ctx: Ctx,
  tx: ServerMutationTx,
): Promise<void> {
  return driveMutationAsync(mutator(isoTx, args, ctx), {
    apply: (op) => applyOpToServerTx(tx, op),
    read: (table, pk) => tx.row(table, pk),
    query: async (q) => {
      // The shared-seam contract is ALWAYS an array of rows: the client's `WriteTxn.query` returns
      // an array even for a root `.one()` (the root unwrap is the Store's `materialize()`, not the
      // in-mutator read — rust/src/wasm/db.rs). The server's `tx.query` returns a scalar (object|null)
      // for a `.one()` root, so normalize it to `[]`/`[row]` here — one body sees the same shape on
      // both tiers. (Postgres backend: `tx.query` rejects until POSTGRES-READ-COMPILER-DESIGN.md
      // Phase B; the daemon/SQLite backend compiles + rides the open session — DAEMON §5.4.)
      const ast = q.ast();
      const res = await tx.query(ast);
      if (ast.one === true) return res == null ? [] : [res as QueryResultRow];
      return (res ?? []) as QueryResultRow[];
    },
  });
}

/** Run one logical {@link MutationOp} (yielded by a shared generator mutator) against the live server
 *  write surface — the SAME async methods a plain async mutator calls (they render dialect SQL and
 *  execute/accumulate per backend). */
function applyOpToServerTx(tx: ServerWriteTx, op: MutationOp): Promise<void> {
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

/** Feed a mutator's RETURNED result (the alternative to calling `tx.exec`/logical ops directly) into
 *  the backend tx: a returned `SqlStatement[]` / `SqlTxn` is exec'd onto `tx`, and a carried
 *  `idempotencyKey` is stashed for the legacy daemon adapter; the SQL mutation facade uses `mid`
 *  as its durable retry identity and PG ignores it. A `void` return is a no-op — the mutator already
 *  drove the tx. Preserves the return-style contract. */
function applyResultToTx(result: ApiMutatorResult, tx: ServerMutationTx): void {
  if (!result) return;
  const statements = Array.isArray(result) ? result : result.statements;
  for (const s of statements) tx.exec(s.sql, s.params);
  if (!Array.isArray(result) && result.idempotencyKey !== undefined) {
    (tx as { idempotencyKey?: string }).idempotencyKey = result.idempotencyKey;
  }
}

/**
 * The stream table mapping, checked LOUDLY at construction (the room-profile rule — a misconfigured
 * mapping should fail the deploy, not a 3am generation). Only checkable when a `schema` is
 * configured; without one the plane's SQL fails at the first checkpoint like any other unschema'd
 * write. The message table is the APP's, so only the three-to-five columns the plane touches are
 * asserted — never its shape.
 */
function assertStreamTables(tables: StreamTables, schema: Schema | undefined): void {
  if (!schema) return;
  const c = resolveStreamColumns(tables.columns);
  const columnsOf = (table: string): Set<string> | undefined => {
    const meta = (schema.tables as Record<string, { columns?: Record<string, unknown> }> | undefined)?.[table];
    return meta?.columns ? new Set(Object.keys(meta.columns)) : undefined;
  };
  const check = (table: string, needed: Array<[string, string]>): void => {
    const cols = columnsOf(table);
    if (!cols) {
      throw new TypeError(
        `streams.checkpoint.tables names "${table}", which is not in the configured schema — add it (and run its ` +
          `migration; \`streamChunkTableDdl\` generates the chunk table's DDL)`,
      );
    }
    for (const [role, name] of needed) {
      if (!cols.has(name)) {
        throw new TypeError(
          `streams.checkpoint.tables: "${table}" has no column "${name}" (the ${role} column) — ` +
            `add it, or point \`columns.${role}\` at the one you have. Known columns: ${[...cols].join(", ")}`,
        );
      }
    }
  };
  check(tables.message, [
    ["key", c.key],
    ["body", c.body],
    ["status", c.status],
    ["seq", c.seq],
    // `cancel`/`error`/`host` are opt-in BY NAMING — present here only when the app asked for them.
    ...(c.cancel !== undefined ? ([["cancel", c.cancel]] as Array<[string, string]>) : []),
    ...(c.error !== undefined ? ([["error", c.error]] as Array<[string, string]>) : []),
    ...(c.host !== undefined ? ([["host", c.host]] as Array<[string, string]>) : []),
  ]);
  check(tables.chunks, [
    ["chunkKey", c.chunkKey],
    ["chunkStream", c.chunkStream],
    ["chunkSeq", c.chunkSeq],
    ["chunkText", c.chunkText],
  ]);
}

async function assertAuthorized<T>(authorizer: Authorizer<T> | undefined, input: T): Promise<void> {
  if (!authorizer) return;
  const result = await authorizer(input);
  if (result === false) throw new RindleApiError("forbidden", "rindle request forbidden", 403);
}

/** The default flush-trio gate: verify the default epoch-bound credential from the request
 *  header. The two refusal messages are deliberately distinct — "missing" is a transport wiring
 *  bug (no `context.request`), "refused" is an invalid credential. */
function defaultFlushGate<User>(shellSecret: string): Authorizer<ApiContext<User>> {
  return async (context) => {
    const credential = requestHeader(context.request, ROOM_FLUSH_CREDENTIAL_HEADER);
    if (!credential) {
      throw new RindleApiError(
        "forbidden",
        `missing ${ROOM_FLUSH_CREDENTIAL_HEADER} header — is the transport passing its incoming request as context.request?`,
        403,
      );
    }
    try {
      await verifyRoomFlushCredential(credential, shellSecret);
    } catch (e) {
      throw new RindleApiError("forbidden", `flush credential refused: ${errMessage(e)}`, 403);
    }
  };
}

/** The default `/room-boot` gate: `Authorization: Bearer <shell secret>` (README contract),
 *  compared constant-time. */
function defaultBootGate<User>(shellSecret: string): Authorizer<ApiContext<User>> {
  return (context) => {
    const auth = requestHeader(context.request, "authorization");
    const bearer = auth && /^bearer\s/i.test(auth) ? auth.replace(/^bearer\s+/i, "") : undefined;
    if (!bearer || !timingSafeEqualStr(bearer, shellSecret)) {
      throw new RindleApiError("forbidden", "room-boot: shell secret refused", 403);
    }
  };
}

/** Best-effort header extraction from whatever the transport put in `context.request`: a Fetch
 *  `Request` (`headers.get`), a node `IncomingMessage` (lowercased header map), or a plain
 *  `{headers: {...}}`. `undefined` when there is no request or no such header. */
function requestHeader(request: unknown, name: string): string | undefined {
  if (!request || typeof request !== "object") return undefined;
  const headers = (request as { headers?: unknown }).headers;
  if (!headers || typeof headers !== "object") return undefined;
  if (typeof (headers as { get?: unknown }).get === "function") {
    return (headers as { get(n: string): string | null }).get(name) ?? undefined;
  }
  const map = headers as Record<string, unknown>;
  const v = map[name.toLowerCase()] ?? map[name];
  if (typeof v === "string") return v;
  if (Array.isArray(v) && typeof v[0] === "string") return v[0];
  return undefined;
}

/** Constant-time string equality (a length mismatch fails fast — length is not the secret). */
function timingSafeEqualStr(a: string, b: string): boolean {
  const ab = new TextEncoder().encode(a);
  const bb = new TextEncoder().encode(b);
  if (ab.length !== bb.length) return false;
  let diff = 0;
  for (let i = 0; i < ab.length; i++) diff |= ab[i] ^ bb[i];
  return diff === 0;
}

async function resolvePolicy<User>(
  policy: RindleApiServerOptions<User>["materializationPolicy"],
  input: QueryLeaseRequest<User>,
): Promise<MaterializationPolicy> {
  if (!policy) return { kind: "whileSubscribed" };
  return typeof policy === "function" ? await policy(input) : policy;
}

async function resolveSubject<User>(
  subject: RindleApiServerOptions<User>["subject"],
  input: QueryLeaseRequest<User>,
): Promise<string | undefined> {
  if (typeof subject === "function") return subject(input);
  return subject;
}

/** Resolve the anonymous routing key (READ-ROUTER-DESIGN.md §2.2): an explicit value/resolver if
 *  configured, otherwise the browser-supplied `clientId`. The router keys on `subject ?? this`. */
async function resolveRoutingKey<User>(
  routingKey: RindleApiServerOptions<User>["routingKey"],
  input: QueryLeaseRequest<User>,
): Promise<string | undefined> {
  if (routingKey === undefined) return input.clientId;
  return typeof routingKey === "function" ? routingKey(input) : routingKey;
}

function queryLeaseResponse(out: MaterializeOutput, wsEndpoint?: string): QueryLeaseResponse {
  return {
    leaseToken: out.leaseToken,
    materializationId: out.materializationId,
    queryKey: out.queryKey,
    reused: out.reused,
    ...(wsEndpoint !== undefined ? { wsEndpoint } : {}),
    ...(out.affinity !== undefined ? { affinity: out.affinity } : {}),
  };
}

function errMessage(reason: unknown): string {
  return String((reason as Error)?.message ?? reason);
}

// --------------------------------------------------------- lifecycle system leases (Slice I-iii)

// The four §4 lifecycle system tables, mirrored VERBATIM from the daemon DDL — the source of
// truth is `rust/rindle-replica/src/mutations.rs` (`realtime_lifecycle_ddl()` + the room-ledger
// DDL in `enable_client_mutations`); duplicated here like `DEFAULT_ROUTES` is client-side so this
// package needs no engine import. `Db::enable_realtime_lifecycle` REGISTERS all four, so a
// hand-built AST over them materializes and resolves `hello` like any base table (the room-boot
// direct-materialize pattern).
const SCOPE_SESSIONS_TABLE = "_rindle_scope_sessions";
const ROOM_WATERMARK_TABLE = "_rindle_room_watermark";
const ROOM_CLIENT_MUTATIONS_TABLE = "_rindle_room_client_mutations";
const ROOM_MUTATION_OUTCOMES_TABLE = "_rindle_room_mutation_outcomes";

// --------------------------------------------------------- occupancy counting (Slice I-iv, §4.1)
//
// The occupancy step rides the NORMAL surfaces end to end: the session upsert + lazy sweep are one
// `executeSqlTxn` (a plain write txn — CDC-captured since I-i, so the row landing IS the doorbell
// delta fanning to every subscribed solo client; no clientID/mid — a system write must never
// advance an lmid — and no idempotencyKey — a renewal's re-upsert must re-run, that is the
// refresh), and the count is one `executeSqlRead` with `consistency: "strong"` — the read surface
// the api-server already has against the daemon (the `RemoteLazyTx` fallback precedent above).
// "strong" routes the read to the WRITE MASTER in a split deploy, which just serialized our
// upsert: read-your-writes without a mutation session (the interactive-txn machinery is optional
// on the daemon interface and far heavier than this two-round-trip pair needs).

/** Default {@link RindleRealtimeLifecycleOptions.minSessions} — the §4.1 1→2 trigger. */
const DEFAULT_LIFECYCLE_MIN_SESSIONS = 2;
/** Default {@link RindleRealtimeLifecycleOptions.graceMs} — the §9.1 hysteresis window. */
const DEFAULT_LIFECYCLE_GRACE_MS = 120_000;
/** Default {@link RindleRealtimeLifecycleOptions.sessionTtlMs} fallback when no `leaseTtlMs` is
 *  configured either — 5 minutes, the {@link DEFAULT_ROOM_TOKEN_TTL_MS} cadence (see the field doc). */
const DEFAULT_SESSION_TTL_MS = 5 * 60_000;
/** Sweep slack past the grace window (D4): rows are deleted only once expired for MORE than
 *  `graceMs + this` — the linger I-v's downgrade decision reads must comfortably outlive the
 *  grace comparison itself under clock skew between api-server instances (occupancy tolerates
 *  skew ≤ grace; the slack keeps the boundary case out of the deletable band). */
const SESSION_SWEEP_SLACK_MS = 60_000;

/** D7 upsert: one row per (scope, clientId) — `(scope, client_id)` is the table's PRIMARY KEY
 *  (`realtime_lifecycle_ddl()`), so a renewal refreshes `expires_at` in place. */
const SESSION_UPSERT_SQL =
  `INSERT INTO ${SCOPE_SESSIONS_TABLE} (scope, client_id, expires_at) VALUES (?, ?, ?) ` +
  `ON CONFLICT(scope, client_id) DO UPDATE SET expires_at = excluded.expires_at`;
/** The D4 lazy sweep, in the SAME txn as the upsert: age out THIS scope's long-expired rows.
 *  Param 2 is `now − (graceMs + SESSION_SWEEP_SLACK_MS)` — never tighter (the linger contract). */
const SESSION_SWEEP_SQL = `DELETE FROM ${SCOPE_SESSIONS_TABLE} WHERE scope = ? AND expires_at < ?`;
/** The occupancy read, one SELECT: cell 0 = DISTINCT unexpired sessions (`expires_at > now`;
 *  distinct by construction — `(scope, client_id)` is the PK), cell 1 = sessions expired WITHIN
 *  the grace window (`now − graceMs < expires_at ≤ now`) — the upward hysteresis input, cell 2 =
 *  ALL matching rows regardless of expiry (the I-v "room plausibly exists" signal: a scope with
 *  ANY other-session row — live OR still lingering pre-sweep — was shared, so a room was
 *  provisioned; a never-shared solo doc has none and must never drain). Params:
 *  `[now, now, now − graceMs, scope]`. */
const SESSION_COUNT_SQL =
  `SELECT SUM(CASE WHEN expires_at > ? THEN 1 ELSE 0 END), ` +
  `SUM(CASE WHEN expires_at <= ? AND expires_at > ? THEN 1 ELSE 0 END), ` +
  `COUNT(*) ` +
  `FROM ${SCOPE_SESSIONS_TABLE} WHERE scope = ?`;
/** {@link SESSION_COUNT_SQL} excluding the CALLER's own row (D6 counts *other* sessions; the
 *  caller contributes itself as the `+ 1`). One extra trailing param: the caller's clientId. */
const SESSION_COUNT_OTHERS_SQL = `${SESSION_COUNT_SQL} AND client_id <> ?`;

/** `col = <string literal>` — the only predicate shape the lifecycle ASTs need. */
function colEq(name: string, value: string): Condition {
  return { type: "simple", op: "=", left: { type: "column", name }, right: { type: "literal", value } };
}

/** The doorbell AST (§4.1): every unexpired row under the scope is one live session; the row
 *  delta arriving through a solo client's daemon subscription IS the 1→2 upgrade signal. The
 *  expiry filter is deliberately NOT in the predicate — `expires_at > now()` would freeze `now`
 *  at mint time; liveness is the READER's judgment (I-iv), the stream just carries the rows. */
function scopeSessionsAst(scope: string): Ast {
  return { table: SCOPE_SESSIONS_TABLE, where: colEq("scope", scope) };
}

/** The §4.2 fence AST: the doc's monotone `flush_seq` row. */
function roomWatermarkAst(doc: string): Ast {
  return { table: ROOM_WATERMARK_TABLE, where: colEq("doc", doc) };
}

/** The §7.1 ledger / §3.3 outcome ASTs share one shape: doc-scoped, and ADDITIONALLY
 *  client-scoped when the lease request carried the browser's stable `clientId` (the same id the
 *  mutation envelopes stamp, so it is exactly the ledger/outcome `client_id`). Without it the
 *  predicate stays doc-only and the client filters to its own rows (defense in depth either
 *  way — the client always filters). */
function docClientAst(table: string, doc: string, clientId: string | undefined): Ast {
  const docCond = colEq("doc", doc);
  return {
    table,
    where: clientId === undefined ? docCond : { type: "and", conditions: [docCond, colEq("client_id", clientId)] },
  };
}

// --------------------------------------------------------- room-serve helpers (G-iv-b)

/** Default room lease token TTL: short (minutes) per RINDLE-REALTIME §4.1 — renewal is a fresh
 *  lease through the api-server, never an extension of this token. */
const DEFAULT_ROOM_TOKEN_TTL_MS = 5 * 60_000;


/** Does the AST contain an aggregate/reduce shape ANYWHERE (root, a `related` subquery, or an
 *  `EXISTS` child)? Room-serving refuses these regardless of coverage: the client's aggregate
 *  overlay (AGGREGATE-SYNC) is computed against the DAEMON's normalized stream and stays
 *  daemon-gated until post-G. (`groupBy`/`having` only occur alongside `aggregate`, so testing
 *  `aggregate` covers them; `having` is still walked for nested EXISTS aggregates.) */
function astHasAggregate(ast: Ast): boolean {
  if (ast.aggregate !== undefined) return true;
  for (const rel of ast.related ?? []) {
    if (astHasAggregate(rel.subquery)) return true;
  }
  return conditionHasAggregate(ast.where) || conditionHasAggregate(ast.having);
}

function conditionHasAggregate(cond: Condition | undefined): boolean {
  if (cond === undefined) return false;
  switch (cond.type) {
    case "simple":
      return false;
    case "and":
    case "or":
      return cond.conditions.some(conditionHasAggregate);
    case "correlatedSubquery":
      return astHasAggregate(cond.related.subquery);
  }
}

async function reject(
  backend: MutationBackend,
  envelope: MutationEnvelope,
  reason: string,
): Promise<PushMutationResponse> {
  const output = await backend.reject({ envelope, reason });
  return { accepted: false, rejected: true, reason, output };
}

/** The single place a {@link MutationOutcome} becomes the wire {@link PushMutationResponse} — shared
 *  by the tx-form path and every scoped-mutator seal branch so the accepted/rejected shape can never
 *  drift between them. */
function outcomeToResponse(outcome: MutationOutcome): PushMutationResponse {
  return outcome.accepted
    ? { accepted: true, rejected: false, output: outcome.output }
    : { accepted: false, rejected: true, reason: outcome.reason, output: outcome.output };
}

function parseObject(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new RindleApiError("bad-request", `invalid ${label}`, 400);
  }
  return value as Record<string, unknown>;
}

function parseString(value: unknown, label: string): string {
  if (typeof value !== "string") throw new RindleApiError("bad-request", `invalid ${label}`, 400);
  return value;
}

function parseEnvelope(value: unknown): MutationEnvelope {
  const obj = parseObject(value, "mutation envelope");
  return {
    clientID: parseString(obj.clientID, "clientID"),
    mid: parseNumber(obj.mid, "mid"),
    name: parseString(obj.name, "name"),
    args: obj.args ?? null,
  };
}

function parseNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new RindleApiError("bad-request", `invalid ${label}`, 400);
  }
  return value;
}
