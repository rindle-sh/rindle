export type WireValue = number | string | boolean | null;

export type StreamMode = "flat" | "normalized";

export type MaterializationPolicy =
  | { kind: "pinned"; name?: string }
  | { kind: "whileSubscribed"; idleTtlMs?: number };

export interface MaterializeInput {
  ast: unknown;
  mode?: StreamMode;
  policy?: MaterializationPolicy;
  subject?: string;
  leaseTtlMs?: number;
  maxSubscribers?: number;
  metadata?: Record<string, unknown>;
  /** The browser's opaque follower-affinity ticket (FOLLOWER-AFFINITY-DESIGN.md §3), forwarded by
   *  the api-server. {@link HttpRindleDaemonClient} lifts it into the `Rindle-Affinity` request
   *  HEADER (never the body) so the fleet edge routes this `/materialize` to the follower the
   *  browser's ws is pinned to (§4) — both legs co-locate on one follower. Placement, not
   *  authorization: the follower verifies it, the api-server forwards it opaquely. Absent ⇒ no
   *  affinity (single daemon / affinity off), byte-identical to today. */
  affinity?: string;
}

export interface MaterializeOutput {
  materializationId: string;
  queryKey?: string;
  leaseToken: string;
  reused?: boolean;
  /** Fresh follower-placement ticket returned by an affinity-enabled daemon. A machine client
   *  opening a separate ws for this follower-local lease (for example a room upstream) offers it
   *  as a subprotocol so the fleet edge routes that socket back to the minting follower. */
  affinity?: string;
}

export interface DematerializeInput {
  materializationId?: string;
  queryKey?: string;
  name?: string;
}

export interface DematerializeOutput {
  removed: boolean;
}

export interface SqlStatement {
  sql: string;
  params?: WireValue[];
}

export interface SqlTxn {
  idempotencyKey?: string;
  clientID?: string;
  mid?: number;
  statements: SqlStatement[];
}

export interface SqlTxnOutput {
  applied?: boolean;
  cv?: number;
  txId?: number | string;
  /** Replicator-master commit identity (the public mutation SQL facade calls this `cursor`). */
  cursor?: string;
  /** Flat convenience mirror of this client's entry in `lmidAdvances`. */
  lmid?: number;
  lmidAdvances?: Array<{ clientID: string; lmid: number }>;
}

/** A raw read: one `SELECT` and its positional parameters. The read counterpart to {@link SqlTxn}
 *  — no idempotency/mutation fields (reads are idempotent).
 *
 *  `consistency` / `routingKey` are **client-tier routing directives**: `consistency` is consumed
 *  by {@link SplitDaemonClient}, `routingKey` by the read router. They ride along the wire (the
 *  router is reached through an `HttpRindleDaemonClient`, so `routingKey` must) but the daemon
 *  itself ignores them and reads only `sql` + `params`. `consistency` defaults to `"eventual"`
 *  (route to a replica, scaling reads off the write-master); set `"strong"` to route to the master
 *  for read-your-writes after a write to the same data. `routingKey` co-locates a replica read on a
 *  chosen follower (e.g. by tenant); absent, the router places it by the SQL text. */
export interface SqlRead {
  sql: string;
  params?: WireValue[];
  consistency?: "eventual" | "strong";
  routingKey?: string;
}

/** The `/execute-sql-read` response: result columns in order and each row as a bare cell array
 *  (NOT keyed objects — the daemon keeps per-cell object construction off the write-master; zip
 *  `cols` with each row client-side if you want objects). Lossless for duplicate/aliased columns. */
export interface SqlReadOutput {
  cols: string[];
  rows: WireValue[][];
}

export type RowChangeTxn = {
  source: string;
  offset: string;
  changes: Array<
    | { table: string; op: "add"; row: WireValue[] }
    | { table: string; op: "remove"; old: WireValue[] }
    | { table: string; op: "edit"; old: WireValue[]; row: WireValue[] }
  >;
  /** The room write-behind extensions (RINDLE-REALTIME §5.3, each opt-in by
   *  presence): the placement fence (`doc` + `epoch` — a stale epoch gets a
   *  `409 {error:"fenced"}`), the §8.3 batch identity (`batchHash` — same id +
   *  different body is a loud 500), and the CAS preconditions (`cas: true` — a miss
   *  gets a `409 {error:"conflict", conflicts}` with authoritative images, nothing
   *  applied). Plain change sources send none and are byte-compatible unchanged. */
  doc?: string;
  epoch?: number;
  batchHash?: string;
  cas?: boolean;
};

export interface RowChangeTxnOutput {
  /** `false` = the `(source, offset)` keyset absorbed a replay. */
  applied?: boolean;
  cv?: number;
}

/** `/claim-room-epoch` (RINDLE-REALTIME §2.5): bump + return the placement epoch. */
export interface ClaimRoomEpochInput {
  doc: string;
}
export interface ClaimRoomEpochOutput {
  epoch: number;
}

/** `/room-lmids` (§3.3): the domain-scoped ledger's lmid per client under `doc` — the
 *  room's boot probe. `doc` scopes the read to `_rindle_room_client_mutations` (§7.1), so a
 *  client's room stream never aliases its slow-path daemon stream. */
export interface RoomLmidsInput {
  doc: string;
  clients: string[];
}
export interface RoomLmidsOutput {
  lmids: Record<string, number>;
}

/** `/mutate-session/begin` (DAEMON-INTERACTIVE-TXN-DESIGN.md §4.1): open an interactive write
 *  transaction on the master, held open across round trips under the daemon-owned deadline.
 *  Client-attributed sessions (`clientID` + `mid`) get mid dedup/gap resolution UP FRONT — an
 *  absorbed replay opens no session and the caller must skip the mutator. `statements` is the
 *  accumulated write prefix to replay into the fresh transaction (sound: nothing before the
 *  first read observed DB state); `query` rides the begin so a one-read mutator pays exactly
 *  one extra round trip over the batch path. */
export interface MutationSessionBegin {
  clientID?: string;
  mid?: number;
  idempotencyKey?: string;
  statements?: SqlStatement[];
  query?: SqlStatement;
}

export interface MutationSessionBeginOutput {
  /** Present iff a session opened. */
  sessionId?: string;
  /** The `query` read's result, when one rode the begin (`{cols, rows}`, bare cell arrays). */
  read?: SqlReadOutput;
  /** True when mid/idempotency dedup absorbed the envelope at begin: NO session opened, the
   *  mutator must not run — the remaining fields are the authoritative replay output, the same
   *  shape {@link RindleDaemonClient.executeSqlTxn} answers for a deduped mid. */
  absorbed?: boolean;
  applied?: boolean;
  lmid?: number;
  lmidAdvances?: Array<{ clientID: string; lmid: number }>;
}

export interface MutationSessionExec {
  sessionId: string;
  statements: SqlStatement[];
}

/** A read through the open transaction (read-your-writes) — flat `{sessionId, sql, params}`,
 *  answered in the `/execute-sql-read` shape. */
export interface MutationSessionQuery {
  sessionId: string;
  sql: string;
  params?: WireValue[];
}

export interface MutationSessionRef {
  sessionId: string;
}

export interface MutationRejection {
  clientID: string;
  mid: number;
  reason?: string;
}

export interface MutationRejectionOutput {
  cv?: number;
  lmid?: number;
}

/** One pure migration file: either ordered DDL statements or ordered DML statements. The opaque
 *  `id` is the journaled idempotency key. `checksum` is the canonical SHA-256 content identity
 *  sent by current deploy tooling; it is required for data migrations and optional only for
 *  compatibility with legacy private DDL callers. `overrideHash` is an operator-reviewed checksum
 *  for the content this migration originally applied with. DDL may be additive or destructive,
 *  while `RENAME` and raw `blob` remain unsupported. */
export interface MigrateInput {
  id: string;
  checksum?: string;
  overrideHash?: string;
  statements: string[];
}

/** The `/migrate` response: `applied` is false when `id` was already journaled (an idempotent
 *  replay ran nothing); `hashOverridden` reports that the supplied override matched the journaled
 *  content identity; `schemaVersion` is the version the daemon will report AFTER it serves the new
 *  shape (the latest applied migration id, `DRIZZLE-MIGRATIONS-DESIGN.md` §5.3). `restarting` is
 *  true when the daemon is self-restarting to re-introspect (a supervised daemon with
 *  `RINDLE_RESTART_ON_MIGRATE`) — the caller should wait for it to come back before its next call. */
export interface MigrateOutput {
  applied: boolean;
  hashOverridden?: boolean;
  schemaVersion?: string;
  restarting?: boolean;
}

/** One entry in a {@link MigrateBatchOutput}: the migration `id`, whether this apply ran it
 *  (false = the `id` was already journaled, an idempotent no-op), and whether its override matched
 *  the journaled content identity. */
export interface MigrateResultItem {
  id: string;
  applied: boolean;
  hashOverridden?: boolean;
  schemaVersion?: string;
}

/** The response to the ARRAY form of `/migrate` ({@link HttpRindleDaemonClient.migrateBatch}): a
 *  whole ordered set applied in one round-trip so a supervised daemon self-restarts ONCE for the
 *  set instead of once per migration. `results` are the migrations processed before any failure, in
 *  order; `applied` counts the newly-applied ones; `schemaVersion` is the latest applied id;
 *  `restarting` is true when the daemon is self-restarting to re-introspect (wait for it before the
 *  next call). On a failed migration `error`/`failedId` name it — the batch stops there, earlier
 *  migrations are committed (forward-only), and the daemon does NOT restart: fix forward and
 *  re-send. Unlike the single-migration form, this always arrives as HTTP 200; inspect `error`. */
export interface MigrateBatchOutput {
  results: MigrateResultItem[];
  applied: number;
  schemaVersion?: string;
  restarting?: boolean;
  error?: string;
  errorCode?: string;
  failedId?: string;
}

/** The SSR one-shot read (`SSR-DESIGN.md` §3). Materialize-or-reuse the query, read its current
 *  view ONCE, return it assembled; registers no subscriber. `visibilityKey`/`ttlMs` are optional. */
export interface QueryOnceInput {
  ast: unknown;
  visibilityKey?: string;
  ttlMs?: number;
  /** The browser's opaque follower-affinity ticket — see {@link MaterializeInput.affinity}. Lifted
   *  into the `Rindle-Affinity` header so an SSR / one-shot read co-locates on the same pinned
   *  follower the browser's ws is (or will be) on. */
  affinity?: string;
}

/** The `POST /query` response: assembled `rows` (nested by name, ready to hydrate without an
 *  engine), the `cvMin` baseline they reflect, the per-query `schema` hello, and `queryKey`. */
export interface QueryOnceOutput {
  queryKey?: string;
  cvMin?: number;
  bootId?: string;
  schema?: unknown;
  rows: Array<{ cols: Record<string, WireValue>; [rel: string]: unknown }>;
}

/** One column in a {@link SchemaTable}: its name and the daemon's `ColType` wire name. */
export interface SchemaColumn {
  name: string;
  /** The daemon's `ColType` wire name. Because the daemon only ever *introspects* the SQLite file
   *  and affinity is lossy, this is `"string"` or `"number"` in practice — a column's app-level
   *  `"boolean"`/`"json"` intent is not recoverable from the file (`DRIZZLE-MIGRATIONS-DESIGN.md`
   *  §6.2). The full vocabulary is `"string" | "number" | "boolean" | "json"`. */
  type: string;
  /** `true` when the column is nullable (introspected `pragma_table_info.notnull == 0`); the
   *  generated column becomes `.nullable()`, typing it `T | null`. PK columns are always `false`
   *  (row identity). Absent on older daemons ⇒ treat as non-nullable. See design 206. */
  nullable?: boolean;
}

/** One base table in the daemon's introspected schema: its name, ordered columns, and PK columns. */
export interface SchemaTable {
  name: string;
  columns: SchemaColumn[];
  primaryKey: string[];
}

/** The daemon's introspected base-table schema (`GET /schema`) — the input to client-schema
 *  codegen (`DRIZZLE-MIGRATIONS-DESIGN.md` §6.2). Tables are sorted by name and the daemon's own
 *  bookkeeping tables (`_rindle_*`) are excluded. */
export interface SchemaOutput {
  tables: SchemaTable[];
}

export interface RindleDaemonClient {
  materialize(input: MaterializeInput): Promise<MaterializeOutput>;
  dematerialize(input: DematerializeInput): Promise<DematerializeOutput>;
  executeSqlTxn(input: SqlTxn): Promise<SqlTxnOutput>;
  /** Raw SQL read (a single `SELECT` + params) against the latest committed snapshot, returning
   *  `{ cols, rows }` (bare row arrays). The read counterpart to {@link executeSqlTxn}. Under
   *  {@link SplitDaemonClient} this defaults to a replica (`consistency:"eventual"`); pass
   *  `consistency:"strong"` for read-your-writes on the master. */
  executeSqlRead(input: SqlRead): Promise<SqlReadOutput>;
  applyRowChangeTxn(input: RowChangeTxn): Promise<RowChangeTxnOutput>;
  rejectMutation(input: MutationRejection): Promise<MutationRejectionOutput>;
  /** SSR one-shot read (`SSR-DESIGN.md` §3): the current assembled view, no subscription. */
  query(input: QueryOnceInput): Promise<QueryOnceOutput>;
  /** Apply one schema migration through the daemon's controlled DDL channel + journal it
   *  (`DRIZZLE-MIGRATIONS-DESIGN.md` §5.1). Idempotent by `id`. The new shape is served only after
   *  the daemon is bounced (migrate-at-bounce, §5.2) — this applies DDL to the file but does not
   *  re-introspect the running engine. Drive it from a migration runner, not the request path. */
  migrate(input: MigrateInput): Promise<MigrateOutput>;
  /** Open an interactive mutation session (DAEMON-INTERACTIVE-TXN-DESIGN.md §4) — the write
   *  transaction a read-bearing mutator lazily upgrades onto. Optional as a group with the four
   *  ops below: only the daemon's control plane serves sessions (master-only — a follower's
   *  write-fence rejects begin); a client that lacks them forces the legacy committed-state
   *  read path. Session ops against a finished/expired session reject with a
   *  {@link DaemonHttpError} whose `status` is 410 — treat it as infra (retry the envelope;
   *  begin-time dedup absorbs a committed outcome). */
  beginMutationSession?(input: MutationSessionBegin): Promise<MutationSessionBeginOutput>;
  execInMutationSession?(input: MutationSessionExec): Promise<unknown>;
  queryInMutationSession?(input: MutationSessionQuery): Promise<SqlReadOutput>;
  commitMutationSession?(input: MutationSessionRef): Promise<SqlTxnOutput>;
  rollbackMutationSession?(input: MutationSessionRef): Promise<unknown>;
  /** Claim the next placement epoch for a room's doc (RINDLE-REALTIME §2.5). Optional:
   *  only daemons serving as a room write authority implement it; the API server's
   *  room host refuses loudly when its configured client lacks it. */
  claimRoomEpoch?(input: ClaimRoomEpochInput): Promise<ClaimRoomEpochOutput>;
  /** The room's boot probe (§3.3). Optional, same contract as {@link claimRoomEpoch}. */
  roomLmids?(input: RoomLmidsInput): Promise<RoomLmidsOutput>;
}

export interface HttpRindleDaemonClientPaths {
  materialize: string;
  dematerialize: string;
  executeSqlTxn: string;
  executeSqlRead: string;
  applyRowChangeTxn: string;
  rejectMutation: string;
  query: string;
  migrate: string;
  schema: string;
  claimRoomEpoch: string;
  roomLmids: string;
  mutateSessionBegin: string;
  mutateSessionExec: string;
  mutateSessionQuery: string;
  mutateSessionCommit: string;
  mutateSessionRollback: string;
}

export type HeaderValue = string | undefined;
export type HeadersInput = Record<string, HeaderValue>;
export type HeadersFactory = () => HeadersInput | PromiseLike<HeadersInput>;

export interface FetchResponseLike {
  ok: boolean;
  status: number;
  statusText: string;
  text(): Promise<string>;
  /** Optional response headers — the real `fetch` Response exposes these. Used to read the
   *  daemon's `Rindle-Boot-Id` (see {@link HttpRindleDaemonClientOptions.onBootId}). */
  headers?: { get(name: string): string | null };
}

export type FetchLike = (
  input: string,
  init: { method: string; headers: Record<string, string>; body: string },
) => Promise<FetchResponseLike>;

export interface HttpRindleDaemonClientOptions {
  baseUrl: string;
  fetch?: FetchLike;
  headers?: HeadersInput | HeadersFactory;
  paths?: Partial<HttpRindleDaemonClientPaths>;
  /** Fired with the daemon's boot id on the first control-plane response and again whenever it
   *  CHANGES — i.e. the daemon restarted (it keeps no durable lease/materialization state). Use
   *  it to re-assert pins / re-materialize. Treat every call as "(re)assert now"; it rides
   *  responses you already make (e.g. an ingester's writes), so no polling is needed. Must not
   *  throw or reject — handle your own errors (e.g. `onBootId: () => server.assertPins().catch(log)`). */
  onBootId?: (bootId: string) => void;
}

/** The api-server → fleet control-plane header carrying the browser's opaque affinity ticket on
 *  `/materialize` and `/query` (FOLLOWER-AFFINITY-DESIGN.md §5). Inlined (not imported from
 *  `@rindle/affinity`'s `HEADER_NAME`) to keep this client free of that crate's `node:crypto`
 *  dependency — an api-server may run on a Worker. Keep the two spellings in lock-step. */
const AFFINITY_HEADER = "Rindle-Affinity";

const defaultPaths: HttpRindleDaemonClientPaths = {
  materialize: "/materialize",
  dematerialize: "/dematerialize",
  executeSqlTxn: "/execute-sql-txn",
  executeSqlRead: "/execute-sql-read",
  applyRowChangeTxn: "/apply-row-change-txn",
  rejectMutation: "/reject-mutation",
  query: "/query",
  migrate: "/migrate",
  schema: "/schema",
  claimRoomEpoch: "/claim-room-epoch",
  roomLmids: "/room-lmids",
  mutateSessionBegin: "/mutate-session/begin",
  mutateSessionExec: "/mutate-session/exec",
  mutateSessionQuery: "/mutate-session/query",
  mutateSessionCommit: "/mutate-session/commit",
  mutateSessionRollback: "/mutate-session/rollback",
};

export class DaemonHttpError extends Error {
  readonly status: number;
  readonly statusText: string;
  readonly body: string;

  constructor(status: number, statusText: string, body: string) {
    super(`rindle daemon request failed: ${status} ${statusText}${body ? `: ${body}` : ""}`);
    this.name = "DaemonHttpError";
    this.status = status;
    this.statusText = statusText;
    this.body = body;
  }
}

export class HttpRindleDaemonClient implements RindleDaemonClient {
  private readonly baseUrl: string;
  private readonly fetchImpl: FetchLike;
  private readonly headers?: HeadersInput | HeadersFactory;
  private readonly paths: HttpRindleDaemonClientPaths;
  private readonly onBootId?: (bootId: string) => void;
  private lastBootId?: string;

  constructor(opts: HttpRindleDaemonClientOptions) {
    this.baseUrl = opts.baseUrl;
    this.fetchImpl = opts.fetch ?? defaultFetch;
    this.headers = opts.headers;
    this.paths = { ...defaultPaths, ...opts.paths };
    this.onBootId = opts.onBootId;
  }

  materialize(input: MaterializeInput): Promise<MaterializeOutput> {
    // The affinity ticket rides the `Rindle-Affinity` HEADER, not the body (the fleet edge reads it
    // pre-dispatch this POST to the pinned follower). Strip it from the body so the
    // daemon's `MaterializeInput` deserialization never sees an unknown field.
    const { affinity, ...body } = input;
    return this.post(this.paths.materialize, body, affinity);
  }

  dematerialize(input: DematerializeInput): Promise<DematerializeOutput> {
    return this.post(this.paths.dematerialize, input);
  }

  executeSqlTxn(input: SqlTxn): Promise<SqlTxnOutput> {
    return this.post(this.paths.executeSqlTxn, input);
  }

  executeSqlRead(input: SqlRead): Promise<SqlReadOutput> {
    // Sends the full input (like `query` carries `visibilityKey`): `routingKey` must survive this
    // hop so the read router — reached THROUGH this client — can place by it. The daemon itself
    // reads only `sql` + `params` and ignores the routing directives.
    return this.post(this.paths.executeSqlRead, input);
  }

  applyRowChangeTxn(input: RowChangeTxn): Promise<RowChangeTxnOutput> {
    return this.post(this.paths.applyRowChangeTxn, input);
  }

  claimRoomEpoch(input: ClaimRoomEpochInput): Promise<ClaimRoomEpochOutput> {
    return this.post(this.paths.claimRoomEpoch, input);
  }

  roomLmids(input: RoomLmidsInput): Promise<RoomLmidsOutput> {
    return this.post(this.paths.roomLmids, input);
  }

  rejectMutation(input: MutationRejection): Promise<MutationRejectionOutput> {
    return this.post(this.paths.rejectMutation, input);
  }

  beginMutationSession(input: MutationSessionBegin): Promise<MutationSessionBeginOutput> {
    return this.post(this.paths.mutateSessionBegin, input);
  }

  execInMutationSession(input: MutationSessionExec): Promise<unknown> {
    return this.post(this.paths.mutateSessionExec, input);
  }

  queryInMutationSession(input: MutationSessionQuery): Promise<SqlReadOutput> {
    return this.post(this.paths.mutateSessionQuery, input);
  }

  commitMutationSession(input: MutationSessionRef): Promise<SqlTxnOutput> {
    return this.post(this.paths.mutateSessionCommit, input);
  }

  rollbackMutationSession(input: MutationSessionRef): Promise<unknown> {
    return this.post(this.paths.mutateSessionRollback, input);
  }

  query(input: QueryOnceInput): Promise<QueryOnceOutput> {
    const { affinity, ...body } = input;
    return this.post(this.paths.query, body, affinity);
  }

  migrate(input: MigrateInput): Promise<MigrateOutput> {
    return this.post(this.paths.migrate, input);
  }

  /** Apply an ordered SET of migrations in one round-trip — the array form of {@link migrate}. The
   *  daemon applies them in order (stopping at the first failure) and, when supervised, self-restarts
   *  ONCE for the whole set. Drive it from a migration runner (`rindle migrate apply` does). See
   *  {@link MigrateBatchOutput} — inspect its `error` field rather than relying on the HTTP status. */
  migrateBatch(inputs: MigrateInput[]): Promise<MigrateBatchOutput> {
    return this.post(this.paths.migrate, inputs);
  }

  /** The daemon's introspected base-table schema for client-schema codegen
   *  (`DRIZZLE-MIGRATIONS-DESIGN.md` §6.2). Read-only `GET /schema`, bearer-auth'd when the daemon
   *  has a token configured. Deliberately NOT on {@link RindleDaemonClient}: schema codegen is a
   *  dev/CI concern, not part of the runtime control-plane contract a router/splitter satisfies. */
  schema(): Promise<SchemaOutput> {
    return this.get(this.paths.schema);
  }

  private async post<T>(path: string, input: unknown, affinity?: string): Promise<T> {
    const headers = await this.resolveHeaders();
    // The api-server → fleet control-plane affinity ticket rides as a header on top of the usual
    // bearer/content headers (FOLLOWER-AFFINITY-DESIGN.md §5); the fleet edge reads it to route
    // to the pinned follower. Only `/materialize` + `/query` carry one.
    if (affinity !== undefined) headers[AFFINITY_HEADER] = affinity;
    const res = await this.fetchImpl(urlJoin(this.baseUrl, path), {
      method: "POST",
      headers: { "content-type": "application/json", ...headers },
      body: JSON.stringify(input),
    });
    this.observeBootId(res);
    const body = await res.text();
    if (!res.ok) throw new DaemonHttpError(res.status, res.statusText, body);
    return (body ? JSON.parse(body) : undefined) as T;
  }

  /** A bearer-auth'd GET (the read-only `/schema`, `/version`-style routes). Carries no request
   *  body — `defaultFetch` strips the placeholder before the real fetch (WHATWG fetch rejects a
   *  GET body). */
  private async get<T>(path: string): Promise<T> {
    const headers = await this.resolveHeaders();
    const res = await this.fetchImpl(urlJoin(this.baseUrl, path), {
      method: "GET",
      headers,
      body: "",
    });
    this.observeBootId(res);
    const body = await res.text();
    if (!res.ok) throw new DaemonHttpError(res.status, res.statusText, body);
    return (body ? JSON.parse(body) : undefined) as T;
  }

  /** Notice the daemon's boot id on any response; fire `onBootId` on the first one and on every
   *  change (a restart). Updates `lastBootId` BEFORE the hook so a re-assert it triggers (which
   *  POSTs again, same boot id) does not re-fire — no recursion. */
  private observeBootId(res: FetchResponseLike): void {
    const bootId = res.headers?.get("rindle-boot-id") ?? undefined;
    if (!bootId || bootId === this.lastBootId) return;
    this.lastBootId = bootId;
    try {
      this.onBootId?.(bootId);
    } catch {
      // A misbehaving hook must never break the originating request.
    }
  }

  private async resolveHeaders(): Promise<Record<string, string>> {
    const raw = typeof this.headers === "function" ? await this.headers() : (this.headers ?? {});
    const out: Record<string, string> = {};
    for (const [key, value] of Object.entries(raw)) {
      if (value !== undefined) out[key] = value;
    }
    return out;
  }
}

function urlJoin(baseUrl: string, path: string): string {
  const base = baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
  const rel = path.startsWith("/") ? path.slice(1) : path;
  return new URL(rel, base).toString();
}

type RealFetch = (
  input: string,
  init: { method: string; headers: Record<string, string>; body?: string },
) => Promise<FetchResponseLike>;

const defaultFetch: FetchLike = async (input, init) => {
  const fetchImpl = (globalThis as { fetch?: RealFetch }).fetch;
  if (!fetchImpl) throw new Error("global fetch is unavailable; pass opts.fetch to HttpRindleDaemonClient");
  // A GET/HEAD must not carry a request body (WHATWG fetch throws); the client passes "" as a
  // placeholder for those, so drop it before reaching the real fetch.
  if (init.method === "GET" || init.method === "HEAD") {
    return fetchImpl(input, { method: init.method, headers: init.headers });
  }
  return fetchImpl(input, init);
};
