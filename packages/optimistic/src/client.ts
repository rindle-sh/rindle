// createRindleClient — the one-call client for the daemon/serverless topology:
//
//   const app = await createRindleClient({ schema, mutators, api: { url } });
//   const view = app.store.materialize(queries.allIssues());
//   app.mutate.createIssue({ id: 1, title: "hi", score: 0 });
//
// It wires what the pieces otherwise wire by hand: the wasm engine init, a stable per-tab clientID,
// the ws transport to the daemon, lease resolution through the API server's query route,
// and the mutation queue flushing confirmed in-order batches through the mutate route
// (rejection reasons surface via `onRejected`).

import { localTableNames, QueryEnsureCache } from "@rindle/client";
import type {
  AnyQuery,
  Ast,
  ColsMap,
  Condition,
  EnsureQueryOptions,
  MutationEnvelope,
  Query,
  QueryId,
  RealtimeQueryLabel,
  RemoteQuery,
  Schema,
} from "@rindle/client";
import {
  RemoteOptimisticSource,
  WsTransport,
  createAffinityTicketStore,
  createQueuedMutationSender,
  offerSubprotocols,
} from "@rindle/remote";
import type { AffinityTicketStore, PushOutcome, RemoteOptimisticConnection, Transport } from "@rindle/remote";
import { initWasm } from "@rindle/wasm";

import type { OptimisticBackend } from "./backend.ts";
import type { ClientRegistry, MutationTx } from "./backend.ts";
import { resetStableClientID, sessionTicketPersistence, stableClientID } from "./client-id.ts";
import {
  LIFECYCLE_QUERY_NAME,
  ROOM_CLIENT_MUTATIONS_TABLE,
  ROOM_MUTATION_OUTCOMES_TABLE,
  ROOM_WATERMARK_TABLE,
  SCOPE_SESSIONS_TABLE,
  type SystemStreamTable,
} from "./system-streams.ts";
import { createOptimisticStore, type MutateFn } from "./index.ts";
import { attachLocalPersistence, type LocalPersistence, type PersistLocalOptions } from "./local-persist.ts";
import type { Store } from "@rindle/client";

/** Must mirror `DEFAULT_RINDLE_API_ROUTES` in `@rindle/api-server` (the app-wire contract;
 *  duplicated so the browser bundle doesn't import the server package). */
const DEFAULT_ROUTES = { query: "/api/rindle/query", mutate: "/api/rindle/mutate" } as const;

export type HeadersInit = Record<string, string>;

// --------------------------------------------------------------------------- Rindle Realtime (G-v)
//
// A named query stamped with a `realtime` label (RINDLE-REALTIME-QUERY-ENABLEMENT §2.1) resolves
// its lease FIRST (resolve-then-register): the local AST view still materializes synchronously,
// but the remote retain attaches only when the lease answers — on the ROOM channel when the lease
// carries a `realtime` block, byte-identically on the daemon when it doesn't (fail-open). The
// types below mirror the api-server's `QueryLeaseRealtime`/`RoomTableSpec` wire shapes (duplicated
// like DEFAULT_ROUTES so the browser bundle never imports the server package).

/** One footprint table's spec on the lease wire (mirror of the api-server's `RoomTableSpec`).
 *  `footprintWhere` (H-iii lease-wire flip) is the EXACT footprint-membership predicate from the
 *  ONE unified compiler (`compileRoomScopeSpecs` — the same output the boot wire ships the room
 *  gate): present only for an exact footprint ROOT (lossless row-local extraction; the vacuous-true
 *  empty AND for an unconstrained one), ABSENT for child/correlated tables. It feeds the §3
 *  router's pk-membership read proof (`OptimisticBackend`'s routing table) — never authorization. */
export interface RealtimeLeaseTableSpec {
  table: string;
  footprintWhere?: Condition;
  writable:
    | { kind: "none" }
    | { kind: "predicate"; where?: Condition; joinKeyCols: string[] };
}

/** The room-serve block on a query lease (mirror of the api-server's `QueryLeaseRealtime`). */
export interface RealtimeLeaseBlock {
  /** The store's gate/domain key for this room source (`connectSource`) — `"room:<profile>/<key>"`. */
  sourceKey: string;
  /** Where the ROOM ws opens — the lease's DEDICATED field. Never confuse it with the TOP-LEVEL
   *  `wsEndpoint` (the read-router's whole-DAEMON-session migration signal). */
  wsEndpoint: string;
  /** The room shell's self-authorizing signed lease (seals the APPROVED query AST) — presented as
   *  the room subscribe's `leaseToken`. */
  roomToken: string;
  /** Token expiry (ms epoch) — the renewal clock (renewal = a fresh lease through the app route). */
  exp: number;
  doc: string;
  tables: RealtimeLeaseTableSpec[];
}

/** One minted SYSTEM-STREAM lease on the `lifecycle` block (mirror of the api-server's
 *  `QueryLeaseLifecycleLease`; Slice I-iii): an ordinary daemon materialization over one of the
 *  four `_rindle_*` lifecycle tables, presented on the wire exactly like the primary lease
 *  (subscribe-with-`leaseToken`). The identity fields document the minted predicate — this client
 *  keys its retains (idempotence per (table, scope/doc/clientId)) and the backend keys its
 *  release-time row filters on them. */
export interface LifecycleLeaseEntry {
  table: string;
  leaseToken: string;
  wsEndpoint?: string;
  /** DOORBELL only: the §4.1 occupancy scope (= the wire room doc, `"<profile>/<key>"`). */
  scope?: string;
  /** FENCE entries only: the room doc. */
  doc?: string;
  /** FENCE ledger/outcomes when the server could client-scope the predicate. */
  clientId?: string;
}

/** The §4 lifecycle block on a query lease (mirror of the api-server's `QueryLeaseLifecycle`):
 *  `doorbell` on every labeled lease under the opt-in server config, `fence` (watermark + ledger
 *  + outcomes) only when the lease is ALSO room-served. Absent ⇒ this client behaves exactly as
 *  today — the whole plane is inert-until-fed. */
export interface LifecycleLeaseBlock {
  doorbell: LifecycleLeaseEntry;
  fence?: LifecycleLeaseEntry[];
}

/** The §4.2 downgrade fence block on a query lease (mirror of the api-server's
 *  `QueryLeaseRealtimeFence`, Slice I-v): rides a labeled reply whose occupancy gate CLOSED
 *  (no `realtime` block) when the server could drain the room — `finalFlushSeq` is the room's
 *  last COMMITTED flush seq, the value the client's ghost holds against
 *  (`_rindle_room_watermark(doc) ≥ finalFlushSeq` through the daemon plane). A room-attached
 *  query receiving it runs the GRACEFUL downgrade dance instead of the loud legacy anomaly. */
export interface RealtimeFenceBlock {
  /** The retiring room source's gate/domain key (`"room:" + doc`). */
  sourceKey: string;
  doc: string;
  finalFlushSeq: number;
}

/** The query-lease reply as this client reads it (top-level daemon lease + optional room block
 *  + optional §4.2 downgrade fence + optional §4 lifecycle system-stream block). */
interface QueryLeaseWire {
  leaseToken: string;
  wsEndpoint?: string;
  /** Fresh placement ticket for the follower that minted this lease. When present on the first
   *  pure-lazy lease, it enables affinity before the returned endpoint opens. */
  affinity?: string;
  realtime?: RealtimeLeaseBlock;
  realtimeFence?: RealtimeFenceBlock;
  lifecycle?: LifecycleLeaseBlock;
}

export type RealtimeAnomalyKind =
  /** A re-lease (renewal / reconnect re-resolution) came back WITHOUT a realtime block AND
   *  without a §4.2 fence — the query is no longer room-served but the server gave nothing to
   *  downgrade behind (a legacy/pre-I-v server, or `lifecycle.drainRoom` unconfigured). Surfaced
   *  loudly; a reply WITH a `realtimeFence` takes the graceful I-v dance instead. */
  | "downgrade"
  /** The I-v ghost is STUCK (§7.5): its watermark fence cleared but sent room-domain mids never
   *  resolved (sent-but-undelivered when the socket died — undecidable in general). The ghost
   *  holds — no timeout-retire is invented — and the mids are named once, actionably. */
  | "downgrade-stuck"
  /** A lease named a DIFFERENT `sourceKey` than the query's live room sub — surfaced loudly, no
   *  re-attach. Deliberately NOT composed from demote+upgrade (deferred to §7.6's rare-case
   *  follow-up): a sourceKey-change reply carries a realtime block for the NEW room but NO
   *  fence for the OLD one, and without `finalFlushSeq` the old slice cannot be ghosted soundly. */
  | "source-key-changed"
  /** The lease POST failed or the room attach threw. The initial-materialize case fails OPEN to
   *  the daemon path (indistinguishable from an unlabeled query's recovery). */
  | "lease-failed";

/** A loud realtime lease anomaly (always ALSO `console.error`'d). */
export interface RealtimeAnomaly {
  kind: RealtimeAnomalyKind;
  name: string;
  args: unknown;
  message: string;
}

/** Rindle Realtime client knobs (Slice G-v). All optional — an app with no labeled queries never
 *  touches any of this. */
export interface RealtimeClientOptions {
  /** The DECLARED room mutators (302 §5: declared, not derived). A mutator named here routes to
   *  the attached room — it stages onto the room's own tables and ships on the room socket —
   *  whenever exactly ONE room is attached; solo (no room) it takes the ordinary daemon path,
   *  and with several rooms attached it routes daemon too (explicit multi-room binding is a
   *  later slice). Every mutator NOT named here is a daemon mutator. A misdeclaration fails
   *  SOFT (302 §5.1): the write lands on the other authority's tables, so the view just stops
   *  feeling instant until the echo relays it — never a divergence. An explicit top-level
   *  `domainPolicy` overrides this entirely. */
  mutators?: readonly string[];
  /** Build the ROOM ws transport for a lease's `realtime.wsEndpoint`. Default
   *  `(endpoint) => new WsTransport(endpoint)`. Injectable for tests / custom ws impls. */
  transport?: (endpoint: string) => Transport;
  /** Loud anomaly surface — see {@link RealtimeAnomaly}. Every anomaly is also `console.error`'d. */
  onAnomaly?: (anomaly: RealtimeAnomaly) => void;
  /** How long before a room lease's `exp` the proactive token renewal fires (default 30s). The
   *  renewal is a FRESH lease through the app query route (renewal-as-reauthorization), and the
   *  live room sub proactively re-subscribes with the fresh token so the shell's TTL backstop
   *  never fires on a healthy session. */
  renewMarginMs?: number;
}

/** Read-only realtime bookkeeping snapshot ({@link RindleClient.__realtimeInspect}) — test/devtools
 *  introspection, mirroring the backend's `__inspect` convention. */
export interface RealtimeInspect {
  rooms: Record<
    string,
    {
      wsEndpoint: string;
      /** The room's OWNED tables (302 §2): wire table → its namespaced engine table — read back
       *  from the BACKEND's registry (`backend.roomTablesFor`, the one source of truth; the
       *  client keeps no shadow copy). */
      promoted: Record<string, string>;
      /** Live room-retained queries on this room, by remote key. */
      queries: Record<string, { name: string; sourceQid: QueryId; exp: number; refCount: number }>;
    }
  >;
}

/** Default {@link RealtimeClientOptions.renewMarginMs}. */
const DEFAULT_RENEW_MARGIN_MS = 30_000;
/** Renewal-delay floor: a nearly-expired lease still renews soon, but never in a hot loop. */
const MIN_RENEW_DELAY_MS = 1_000;
/** Retry delay after a failed renewal POST (only while the current token is still live). */
const RENEW_RETRY_MS = 5_000;
/** A one-shot token handoff not consumed within this window is stale — the resolver falls through
 *  to a fresh lease POST instead of presenting a token the server may already refuse. */
const HANDOFF_MAX_AGE_MS = 15_000;
/** How long a fresh (ticketless) connect waits for its affinity mint frame before leasing
 *  TICKETLESS + warning (FOLLOWER-AFFINITY-DESIGN.md §4.1). Generous — the frame normally arrives in
 *  well under one RTT; this bound only trips on an affinity-off daemon (misconfig / rolling upgrade),
 *  so it degrades loudly instead of hanging. */
const AFFINITY_TICKET_TIMEOUT_MS = 4_000;
/** Client-minted remote-retain qids live in their own high band so they can never collide with the
 *  Store's own 1, 2, 3, … view qids or the reserved per-channel lmid qid 0. Exact in f64 (the wire
 *  number type), far below 2^53. */
const REALTIME_RETAIN_QID_BASE = 2 ** 30;

export interface RindleClientOptions<S extends ColsMap, R extends ClientRegistry> {
  schema: Schema<S>;
  /** The PREDICTED mutators (the API server holds the authoritative twins by name). */
  mutators: R;
  /** The acting principal for a shared (generator) mutator's `ctx.user` — the local identity the
   *  optimistic prediction writes under (the server injects its OWN authenticated user for the
   *  authoritative run). Re-read per invoke. Only needed when `mutators` are shared generators. */
  user?: () => string;
  /** The app API server: named queries resolve to leases here, mutations push here. */
  api: {
    url: string;
    routes?: { query?: string; mutate?: string };
    /** Extra headers per request (auth). A function is re-evaluated per call. */
    headers?: HeadersInit | (() => HeadersInit | Promise<HeadersInit>);
    fetch?: typeof fetch;
  };
  /** Optional subscription transport override. Omit it in the normal unified setup: the first
   *  query lease carries `wsEndpoint` + a fresh affinity ticket and opens the transport lazily.
   *  - `{ wsUrl }` — a static endpoint (single daemon), opened eagerly; in a routed deploy this is
   *    the SSR-injected bootstrap endpoint (READ-ROUTER-DESIGN.md §2.4). A routed lease naming a
   *    different follower migrates the connection there.
   *  - `{ wsUrl }` omitted (or this whole option omitted) — pure-lazy: the first lease's
   *    `wsEndpoint` opens the connection (a
   *    routed SPA with no SSR bootstrap).
   *  - `{ transport }` — a pre-built transport (tests / in-process); fixed, no migration.
   *
   *  With a fixed `wsUrl`, set `affinity: true` to opt into FOLLOWER-AFFINITY mode (design §2).
   *  With no daemon option, a lease that returns an affinity ticket enables the same mode
   *  automatically before opening its socket. The ticket is persisted per-tab and forwarded on
   *  later leases so both legs pin the same nearby follower. Ignored for `{ transport }`. */
  daemon?: { wsUrl?: string; affinity?: boolean } | { transport: Transport };
  /** Stable client identity. Default: a per-origin base (localStorage) plus per-tab and per-instance
   *  suffixes, so each tab — and each client instance within a tab — gets its own mid sequence yet a
   *  reload keeps it; falls back to a fresh random id when web storage is unavailable. Pass a value
   *  to override. */
  clientID?: string;
  /** A policy rejection's reason (the prediction's snap-back rides the lmid release). Fires for
   *  BOTH planes since H-v: the HTTP mutate route's per-envelope rejections AND a room's
   *  `mutationOutcome {kind:"rejected"}` frames — one surface, whichever authority said no. */
  onRejected?: (envelope: MutationEnvelope, reason: string) => void;
  /** Persist `local: true` tables across reloads and keep them live-coherent across tabs
   *  (`207-LOCAL-TABLE-PERSISTENCE-DESIGN.md`). `user` is the storage identity (§3.2) — one IDB
   *  database per (origin, user); pass a sentinel like `"anon"` for a signed-out mode. When set,
   *  `createRindleClient` awaits the initial restore before returning, so the first render never
   *  flashes empty local state (§5.2). Logout is `deleteLocalPersistence(user)` — never implicit.
   *  Per-table opt-out: declare `table(name, { local: "session" })` for local state that must stay
   *  ephemeral and per-tab (e.g. selection) even with persistence on (§5.4). */
  persistLocal?: PersistLocalOptions;
  queue?: { maxBatch?: number; retryDelayMs?: (attempt: number) => number };
  /** The explicit confirming-stream OVERRIDE (RINDLE-REALTIME-QUERY-ENABLEMENT §7.1/§3): which
   *  domain's ledger a mutation is dealt from — its mid comes from that domain's counter, it ships
   *  on that domain's channel (§7.5), and only that domain's confirm watermark retires it. Since
   *  Slice H-iii a returned string PINS that domain verbatim (no proof runs); `undefined` — or no
   *  policy at all, the default — DERIVES the route per §3 from the prediction run's write/read
   *  capture (prove-or-slow-path; any unproven condition routes to the daemon). With no room gate
   *  connected the derivation is `"daemon"`, byte-for-byte as before. A room route the gate
   *  DEOPTS at commit (H-iv-b) is recovered automatically since H-v: the client re-enqueues the
   *  same logical mutation onto the daemon stream (prediction applied throughout, one burnt room
   *  mid), so deriving is safe for realtime apps. */
  domainPolicy?: (name: string, args: unknown) => string | undefined;
  /** Rindle Realtime client knobs (G-v resolve-then-register) — see {@link RealtimeClientOptions}. */
  realtime?: RealtimeClientOptions;
  /** Development-only recovery knobs. Keep off in production: a mutation gap means state loss
   *  or two writers sharing a clientID, and should be investigated. */
  dev?: {
    /** On a mutation-gap response, clear the persisted clientID and hard reload the page. This
     *  recovers from dev DB wipes while making the reset visible to the developer. */
    resetOnMutationGap?: boolean;
  };
}

export interface RindleClient<S extends ColsMap, R extends ClientRegistry> {
  store: Store<S>;
  backend: OptimisticBackend<S>;
  /** Retain a named query for navigation/prefetch. By default waits for server authority; pass
   *  `{ until: "present" }` to continue as soon as the local view has a result. */
  ensure<Q extends AnyQuery>(query: Q, options?: EnsureQueryOptions): Promise<void>;
  /** Call `mutate.foo(args)` for a normal optimistic write, or `mutate.foo.folded(opts, args)` for a
   *  debounced, last-value-wins folded write (FOLDED-MUTATIONS-DESIGN §3). */
  mutate: { [K in keyof R]: MutateFn<Parameters<R[K]>[1]> };
  /** Drain every outstanding fold immediately (FOLDED-MUTATIONS-DESIGN §3) — also fired on
   *  `beforeunload`/`pagehide` so a fold in flight isn't lost on navigation. */
  flushFolds(): void;
  clientID: string;
  close(): void;
  /** Read-only realtime bookkeeping snapshot (rooms, promoted tables + their client-held
   *  `joinKeyCols`, live room queries) — the `__inspect`-convention test/devtools hook. */
  __realtimeInspect(): RealtimeInspect;
}

export async function createRindleClient<S extends ColsMap, R extends ClientRegistry>(
  opts: RindleClientOptions<S, R>,
): Promise<RindleClient<S, R>> {
  await initWasm();

  const clientID = opts.clientID ?? stableClientID();
  const routes = { ...DEFAULT_ROUTES, ...opts.api.routes };
  const fetchImpl = opts.api.fetch ?? ((...args: Parameters<typeof fetch>) => fetch(...args));
  const base = opts.api.url.replace(/\/$/, "");

  const post = async (path: string, payload: unknown): Promise<unknown> => {
    const extra = typeof opts.api.headers === "function" ? await opts.api.headers() : (opts.api.headers ?? {});
    const res = await fetchImpl(`${base}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json", ...extra },
      body: JSON.stringify(payload),
    });
    const text = await res.text();
    if (!res.ok) throw new RindleApiHttpError(path, res.status, text);
    return text ? JSON.parse(text) : undefined;
  };

  // FOLLOWER-AFFINITY mode (design §2): explicit for an eager fleet endpoint, or activated by the
  // first pure-lazy lease's placement ticket. A pre-built transport has no fleet edge to route
  // through. The store is shared by the ws transport, source, and lease POST, and persisted per-tab.
  const daemon = opts.daemon ?? {};
  const leaseDiscoversAffinity = !("transport" in daemon) && daemon.wsUrl === undefined;
  let affinityOn = !("transport" in daemon) && daemon.affinity === true;
  // A replaceable connection always has a dormant store. The first lease can activate it before
  // that same lease's `wsEndpoint` constructs the pure-lazy socket.
  const affinityStore: AffinityTicketStore | undefined =
    "transport" in daemon ? undefined : createAffinityTicketStore(sessionTicketPersistence());

  // The ONE app-lease POST both legs share. Sends the stable `clientId` so the api-server/router
  // can use it as the anonymous routing key (READ-ROUTER-DESIGN.md §2.2); the reply's top-level
  // fields are the daemon lease, and a room-served labeled query ADDITIONALLY carries `realtime`.
  // In affinity mode it ALSO forwards the placement `affinity` ticket — awaited first so a fresh
  // (ticketless) connect leases only AFTER its mint frame arrives, co-locating both legs (§4.1). On
  // reconnect, the WebSocket may offer the persisted ticket on its handshake, but the lease waits
  // for that connection's fresh mint frame. The wait is BOUNDED: if no mint frame arrives (the daemon is
  // affinity-off — a misconfig, or mid rolling-upgrade), we lease
  // TICKETLESS and warn rather than hang. That first timeout LATCHES ticketless mode in the store:
  // later leases return immediately (and do not accumulate abandoned waiters) until an affinity
  // frame actually arrives. A persisted ticket remains useful for the ws handshake but is never
  // forwarded on a lease until the CURRENT connection refreshes it, preventing a restored stale
  // ticket from independently re-pinning the HTTP and ws legs.
  let affinityFallbackWarned = false;
  const performPostLease = async (remote: RemoteQuery): Promise<QueryLeaseWire> => {
    let affinity: string | undefined;
    if (affinityOn && affinityStore) {
      const ticket = await affinityStore.leaseTicket(AFFINITY_TICKET_TIMEOUT_MS);
      affinity = ticket.ticket;
      if (affinity !== undefined) affinityFallbackWarned = false;
      if (ticket.timedOut && !affinityFallbackWarned) {
        affinityFallbackWarned = true;
        console.warn(
          `[rindle] no affinity ticket after ${AFFINITY_TICKET_TIMEOUT_MS}ms — leasing ticketless ` +
            "(is the daemon affinity-enabled / RINDLE_AFFINITY_KEY set?)",
        );
      }
    }
    const out = (await post(routes.query, {
      name: remote.name,
      args: remote.args,
      clientId: clientID,
      ...(affinity !== undefined ? { affinity } : {}),
    })) as QueryLeaseWire;
    // A lease ticket ACTIVATES affinity (recorded before the pure-lazy socket opens), but never
    // refreshes an already-active store: once a connection exists, only ITS `{t:"affinity"}` frames
    // may mark the ticket connection-fresh (`connectionPending` discipline) — an HTTP response
    // landing mid-reconnect must not re-pin the legs onto a follower the socket already left.
    if (affinityStore && out.affinity !== undefined && !affinityOn && leaseDiscoversAffinity) {
      affinityStore.set(out.affinity);
      affinityOn = true;
    }
    // A successful ticketless reply settles discovery either way: the backend does not do
    // placement, so stop serializing leases. (A thrown POST leaves discovery pending — the next
    // queued lease becomes the discoverer.)
    discoveryPending = false;
    return out;
  };

  // Pure-lazy clients do not know they are affinity-enabled until the first lease replies. Queue
  // that discovery window so only one ticketless POST can be in flight: once it establishes a
  // placement, every queued lease re-checks `affinityOn` in `performPostLease` and forwards the
  // same ticket. Without this gate, concurrent first materializations could land on different
  // followers and mint leases that the one stable WebSocket can never subscribe to correctly.
  // The window closes on the first completed lease — with a ticket (affinity on) or without (the
  // backend mints none; leases run unserialized from then on).
  let discoveryPending = leaseDiscoversAffinity;
  let affinityDiscoveryTail = Promise.resolve();
  const postLease = async (remote: RemoteQuery): Promise<QueryLeaseWire> => {
    if (!discoveryPending || affinityOn) return performPostLease(remote);

    const previous = affinityDiscoveryTail;
    let release!: () => void;
    affinityDiscoveryTail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await performPostLease(remote);
    } finally {
      release();
    }
  };

  // One-shot fresh-token handoffs, by remote key: G-v's resolve-then-register (and the proactive
  // renewal) has ALREADY leased when the subscribe fires, so the resolver consumes the handed
  // token instead of POSTing a second time — one lease per subscribe, exactly the unlabeled
  // cadence. Age-capped: an entry no subscribe consumed (e.g. a refcount-only retain) must not
  // serve a stale token to a much-later re-subscribe (which re-leases fresh instead).
  const tokenHandoffs = new Map<string, { target: { leaseToken: string; wsEndpoint?: string }; at: number }>();
  const takeHandoff = (key: string): { leaseToken: string; wsEndpoint?: string } | undefined => {
    const handed = tokenHandoffs.get(key);
    if (!handed) return undefined;
    tokenHandoffs.delete(key);
    return Date.now() - handed.at <= HANDOFF_MAX_AGE_MS ? handed.target : undefined;
  };

  /** RE-resolve a SYSTEM (lifecycle) subscription (Slice I-iii — a reconnect / gap repair /
   *  overflow re-subscribe whose mint-time handoff is long consumed). `_rindle/lifecycle` is a
   *  reserved CLIENT-side name (like the lmid query's): the api-server cannot lease it by name,
   *  so the re-resolution re-leases the PARENT labeled query — renewal-as-reauthorization, the
   *  room-token precedent — and picks the matching entry out of the fresh `lifecycle` block. A
   *  reply without the entry throws: the server no longer minting this stream (config off, label
   *  dropped) must not silently re-attach — the source logs the failed subscribe, and I-iv/I-v
   *  own any reaction. */
  const resolveLifecycleTarget = async (args: LifecycleRemoteArgs): Promise<{ leaseToken: string; wsEndpoint?: string }> => {
    const out = (await postLease({ name: args.parent.name, args: args.parent.args })) as QueryLeaseWire;
    const want = systemEntryKey(args);
    const entry = out.lifecycle === undefined
      ? undefined
      : [out.lifecycle.doorbell, ...(out.lifecycle.fence ?? [])].find((e) => systemEntryKey(e) === want);
    if (entry === undefined) {
      throw new Error(
        `lifecycle re-lease of "${args.parent.name}" no longer carries the ${args.table} system lease`,
      );
    }
    return { leaseToken: entry.leaseToken, ...(entry.wsEndpoint !== undefined ? { wsEndpoint: entry.wsEndpoint } : {}) };
  };

  // Reads-leg connection: a fixed transport (tests/in-process) or a replaceable connection built
  // from `wsUrl` (eager when present, lazy when omitted). A routed lease's `wsEndpoint` migrates it.
  // In affinity mode each transport offers the current ticket as a subprotocol (evaluated per
  // connect, so a reconnect presents the freshest — or freshly cleared — ticket).
  const connection: RemoteOptimisticConnection =
    "transport" in daemon
      ? { transport: daemon.transport }
      : {
          factory: (endpoint) =>
            new WsTransport(
              endpoint,
              affinityOn && affinityStore ? { subprotocols: () => offerSubprotocols(affinityStore) } : {},
            ),
          endpoint: daemon.wsUrl,
        };

  const source = new RemoteOptimisticSource(connection, clientID, {
    ...(affinityStore ? { affinity: () => (affinityOn ? affinityStore : undefined) } : {}),
    resolveSubscribe: async ({ remote }) => {
      // A fail-open labeled register already leased — present exactly that token (see
      // `tokenHandoffs`); otherwise lease now. Read back the follower's `wsEndpoint` for placement.
      // A `realtime` block on a RE-resolution is deliberately IGNORED here even now that the
      // upgrade dance exists (I-iv): retargeting from inside a reconnect's resolve would race the
      // very re-subscribe it resolves. The §4.1 DOORBELL path owns upgrades (`runUpgrade` below)
      // — the occupancy row that made this lease carry a block will (re)ring it.
      const handed = takeHandoff(remoteKey(remote));
      if (handed) return handed;
      // A SYSTEM (lifecycle) sub re-resolves through its PARENT labeled query (I-iii) — the
      // reserved name is never leaseable by itself (see resolveLifecycleTarget).
      if (remote.name === LIFECYCLE_QUERY_NAME) return resolveLifecycleTarget(remote.args as LifecycleRemoteArgs);
      const out = await postLease(remote);
      return { leaseToken: out.leaseToken, wsEndpoint: out.wsEndpoint };
    },
    pushMutation: createQueuedMutationSender({
      maxBatch: opts.queue?.maxBatch,
      retryDelayMs: opts.queue?.retryDelayMs,
      onRejected: opts.onRejected,
      send: async (envelopes) => {
        try {
          const outcomes = (await post(routes.mutate, { envelopes })) as Array<{ accepted: boolean; reason?: string }>;
          return outcomes.map((o): PushOutcome => ({ accepted: o.accepted, reason: o.reason }));
        } catch (err) {
          if (opts.dev?.resetOnMutationGap && reloadAfterMutationGap(err)) {
            return new Promise<never>(() => {});
          }
          throw err;
        }
      },
    }),
  });

  const { store, backend, mutate } = createOptimisticStore(opts.schema, source, opts.mutators, {
    clientID,
    user: opts.user,
    // The DECLARED router (302 §5): an explicit `domainPolicy` wins; otherwise the app's declared
    // realtime mutators route to the one attached room (`rooms` is read lazily at invoke time —
    // it is declared below, after this construction).
    ...(opts.domainPolicy
      ? { domainPolicy: opts.domainPolicy }
      : opts.realtime?.mutators !== undefined
        ? { domainPolicy: declaredMutatorPolicy(new Set(opts.realtime.mutators), () => rooms) }
        : {}),
    // Room-plane rejection parity (H-v): a room's `mutationOutcome {kind:"rejected"}` frame
    // surfaces through the SAME callback the HTTP mutate path uses below — one app-level
    // rejection surface, whichever authority said no.
    ...(opts.onRejected ? { onRejected: opts.onRejected } : {}),
  });

  // ---- Rindle Realtime (G-v): resolve-then-register for LABELED queries -------------------------
  //
  // `store.materialize` is wrapped: an UNLABELED query takes the original path byte-identically; a
  // query stamped with a `realtime` label materializes its local view synchronously (the Store's
  // ordinary seed/`unknown` pre-marking runs untouched) while the remote register is SPLIT — the
  // shadowed `backend.registerQuery` below registers the LOCAL half only, and the remote retain
  // attaches when the lease answers: on `realtime.sourceKey`'s room channel when the lease carries
  // a realtime block, on the daemon (fail-open, indistinguishable from unlabeled) when it doesn't.

  const realtimeOpts = opts.realtime ?? {};
  const roomTransportFactory = realtimeOpts.transport ?? ((endpoint: string) => new WsTransport(endpoint));
  const renewMarginMs = realtimeOpts.renewMarginMs ?? DEFAULT_RENEW_MARGIN_MS;
  const localTables = localTableNames(opts.schema);
  let realtimeClosed = false;

  const anomaly = (kind: RealtimeAnomalyKind, remote: RemoteQuery, message: string): void => {
    // LOUD by contract: every anomaly hits the console even with a handler installed.
    console.error(`[rindle] realtime ${kind} for query "${remote.name}": ${message}`);
    try {
      realtimeOpts.onAnomaly?.({ kind, name: remote.name, args: remote.args, message });
    } catch (err) {
      console.error("[rindle] realtime onAnomaly handler threw:", err);
    }
  };

  /** One connected room: its gate key + its `RemoteOptimisticSource`. The promoted-table
   *  bookkeeping lives in the backend's 302 roomTables record (`backend.roomTablesFor(sourceKey)`
   *  — the wire→namespaced-twin rename map; one source of truth for the gate's rename/DROP, the
   *  idempotence check, and `__realtimeInspect`). */
  interface RoomConnection {
    sourceKey: string;
    wsEndpoint: string;
    source: RemoteOptimisticSource;
  }
  /** One room-retained (name, args): its ONE wire sub (`sourceQid` = the creating retain's qid),
   *  how many live views hold it, and the renewal clock. `lifecycleClaims` (I-iii) holds the
   *  system-stream claims RENEWAL-path re-leases made on this query's behalf (a renewal may mint
   *  entries the original attach never saw, e.g. the query became room-served); released when the
   *  last view drops the query. */
  interface RoomQueryState {
    remote: RemoteQuery;
    sourceKey: string;
    sourceQid: QueryId;
    refCount: number;
    exp: number;
    renewTimer?: ReturnType<typeof setTimeout>;
    lifecycleClaims: Set<string>;
    /** The §4.1 doorbell scope this room-served query counts on (`lease.lifecycle.doorbell.scope`
     *  = the wire doc). Captured on attach/upgrade/renewal so the I-v downgrade dance — which runs
     *  from the renewal loop with no lease-block in scope for co-tenant queries — can re-register
     *  each surviving view as an upgrade candidate under the scope its next doorbell will ring. */
    doorbellScope?: string;
  }
  const rooms = new Map<string, RoomConnection>();
  const roomQueries = new Map<string, RoomQueryState>();
  let nextRetainQid: QueryId = REALTIME_RETAIN_QID_BASE;

  // ---- the §4 lifecycle SYSTEM-STREAM plane, client wiring (Slice I-iii) -----------------------
  //
  // A lease's `lifecycle` block names minted daemon subscriptions over the four `_rindle_*`
  // system tables. They are retained on the DAEMON channel (that is the point of the plane: the
  // outcome/ledger/watermark rows must reach the client with no room socket alive) through
  // `backend.retainSystemQuery` — no store view, no user-visible table; the backend folds their
  // rows at release time. Retains are IDEMPOTENT per (table, scope/doc/clientId): every live
  // holder (a labeled view; a room query's renewal loop) claims a key at most once, one wire sub
  // exists per key, and the LAST holder's release drops it. NO reactions are wired here — the
  // doorbell-triggered re-lease is I-iv, the ghost-drop fence consumer is I-v. Absent block ⇒
  // this whole section never runs.

  /** One live system sub: the backend retain + how many holders claim it. */
  interface SystemSubState {
    retainQid: QueryId;
    refCount: number;
  }
  const systemSubs = new Map<string, SystemSubState>();

  /** Claim every entry of `block` for one holder (`claims` — the holder's own claim set; a key
   *  already claimed by THIS holder is skipped, so renewal re-presentations are idempotent).
   *  Unknown tables are skipped (forward-compat: a newer server minting a fifth stream must not
   *  break this client). */
  const claimLifecycle = (claims: Set<string>, block: LifecycleLeaseBlock | undefined, parent: RemoteQuery): void => {
    if (block === undefined || realtimeClosed) return;
    for (const entry of [block.doorbell, ...(block.fence ?? [])]) {
      if (!isSystemTable(entry.table)) continue;
      const key = systemEntryKey(entry);
      if (claims.has(key)) continue;
      let live = systemSubs.get(key);
      if (!live) {
        // The sub's wire identity embeds the PARENT labeled query so a RE-resolution can
        // re-lease it (resolveLifecycleTarget); the minted token is handed to the resolver so
        // the first subscribe presents exactly it — one lease per subscribe, the G-v cadence.
        const remote: RemoteQuery = {
          name: LIFECYCLE_QUERY_NAME,
          args: {
            table: entry.table,
            ...(entry.scope !== undefined ? { scope: entry.scope } : {}),
            ...(entry.doc !== undefined ? { doc: entry.doc } : {}),
            ...(entry.clientId !== undefined ? { clientId: entry.clientId } : {}),
            parent: { name: parent.name, args: parent.args },
          } satisfies LifecycleRemoteArgs,
        };
        tokenHandoffs.set(remoteKey(remote), {
          target: { leaseToken: entry.leaseToken, ...(entry.wsEndpoint !== undefined ? { wsEndpoint: entry.wsEndpoint } : {}) },
          at: Date.now(),
        });
        const retainQid = nextRetainQid++;
        backend.retainSystemQuery(retainQid, remote, {
          table: entry.table,
          ...(entry.scope !== undefined ? { scope: entry.scope } : {}),
          ...(entry.doc !== undefined ? { doc: entry.doc } : {}),
        });
        live = { retainQid, refCount: 0 };
        systemSubs.set(key, live);
      }
      live.refCount++;
      claims.add(key);
    }
  };

  /** Release one holder's claims; the LAST holder of a key releases the backend retain (the wire
   *  sub unsubscribes; the backend's folded fence/occupancy STATE deliberately survives). */
  const releaseLifecycle = (claims: Set<string>): void => {
    for (const key of claims) {
      const live = systemSubs.get(key);
      if (!live) continue;
      if (--live.refCount <= 0) {
        systemSubs.delete(key);
        backend.releaseSystemQuery(live.retainQid);
      }
    }
    claims.clear();
  };

  /** Register the room's OWNED tables (302 §2 — one source per table): every lease table spec
   *  whose `writable` kind is not `"none"` names a table the room owns; the backend registers a
   *  namespaced engine twin the room channel feeds and the room-homed views swap onto. Context
   *  tables (`kind: "none"`) are deliberately NOT registered — the daemon is their sole
   *  authority, and the gate DROPS the room's relayed copies (302 §6). Idempotent per
   *  (sourceKey, table) — the backend's record is the one source of truth; a footprint table
   *  absent from the client schema has nothing to hold rows for and is skipped backend-side. */
  const promoteRoomTables = (room: RoomConnection, specs: RealtimeLeaseTableSpec[]): void => {
    const owned = specs.filter((s) => s.writable.kind !== "none").map((s) => s.table);
    // ALWAYS register — even an all-context lease's `owned = []`: the installed (empty) map is
    // what makes the room gate DROP every relayed delta (302 §6). Skipping the call would leave
    // `gate.tableMap` undefined — the DAEMON identity path — and fold the room's relayed copies
    // of daemon-authoritative rows verbatim into the plain tables, two syncs fighting over one
    // baseline (stale overwrites + dueling GC removes).
    backend.registerRoomTables(room.sourceKey, owned);
  };

  /** (Re)arm a room query's proactive renewal from its current `exp`. Timers are unref'd (Node)
   *  so an idle renewal never holds the process open; cleared on release/close. */
  const scheduleRenewal = (key: string, state: RoomQueryState, delayMs?: number): void => {
    if (state.renewTimer !== undefined) clearTimeout(state.renewTimer);
    if (realtimeClosed) return;
    const delay = delayMs ?? Math.max(state.exp - Date.now() - renewMarginMs, MIN_RENEW_DELAY_MS);
    state.renewTimer = setTimeout(() => {
      state.renewTimer = undefined;
      void renewRoomQuery(key, state);
    }, delay);
    (state.renewTimer as unknown as { unref?: () => void }).unref?.();
  };

  /** Proactive token renewal (renewal-as-reauthorization): re-lease through the SAME app query
   *  route; only a reply WITH a realtime block (and the SAME sourceKey) re-authorizes — the fresh
   *  token is handed to the resolver and the live sub re-subscribes with it BEFORE the room
   *  shell's TTL backstop can drop it. A reply without the block is the DOWNGRADE signal: loud;
   *  the sub is left to die at `exp` (the graceful downgrade dance is Slice I). */
  const renewRoomQuery = async (key: string, state: RoomQueryState): Promise<void> => {
    if (realtimeClosed || roomQueries.get(key) !== state) return;
    let lease: QueryLeaseWire;
    try {
      lease = await postLease(state.remote);
    } catch (err) {
      anomaly("lease-failed", state.remote, `token renewal failed: ${String((err as Error)?.message ?? err)}`);
      // Retry while the current token is still live; past `exp` the shell has dropped the sub
      // anyway and the next reconnect re-resolution owns recovery.
      if (Date.now() < state.exp && roomQueries.get(key) === state) scheduleRenewal(key, state, RENEW_RETRY_MS);
      return;
    }
    if (realtimeClosed || roomQueries.get(key) !== state) return;
    // I-iii: a renewal re-presents the lifecycle block — re-claim idempotently (a key this query
    // already holds is skipped; a NEW entry, e.g. the fence appearing when the query became
    // room-served mid-life, is retained now). Claimed BEFORE the realtime check on purpose: a
    // downgraded renewal (no realtime block) still carries the doorbell, and the occupancy
    // stream must survive the downgrade (it is what re-upgrades, §4.1).
    claimLifecycle(state.lifecycleClaims, lease.lifecycle, state.remote);
    const rt = lease.realtime;
    if (rt === undefined) {
      // No realtime block: the occupancy gate closed server-side. WITH a §4.2 fence, run the
      // graceful I-v downgrade dance (retarget → demote behind the watermark → re-arm the
      // doorbell); WITHOUT one, stay loud (a pre-I-v server, or `lifecycle.drainRoom`
      // unconfigured — nothing to ghost behind soundly).
      if (lease.realtimeFence !== undefined) {
        // Hand the fresh daemon token so the driving query's daemon re-subscribe presents it (no
        // extra POST); co-tenant queries sharing the room re-lease on their own daemon re-subscribe.
        tokenHandoffs.set(key, {
          target: { leaseToken: lease.leaseToken, ...(lease.wsEndpoint !== undefined ? { wsEndpoint: lease.wsEndpoint } : {}) },
          at: Date.now(),
        });
        downgradeRoom(lease.realtimeFence);
      } else {
        anomaly(
          "downgrade",
          state.remote,
          "the renewal lease carries no realtime block AND no §4.2 downgrade fence — the query is no longer room-served and the server offered nothing to fall back behind (a pre-I-v server, or lifecycle.drainRoom unconfigured); its room sub will lapse at exp",
        );
      }
      return;
    }
    if (rt.sourceKey !== state.sourceKey) {
      anomaly(
        "source-key-changed",
        state.remote,
        `the renewal lease names sourceKey ${JSON.stringify(rt.sourceKey)} but the live sub is on ${JSON.stringify(state.sourceKey)} (teardown + re-register is the §7.6 rare-case follow-up — deferred, no fence for the old room)`,
      );
      return;
    }
    const room = rooms.get(state.sourceKey);
    if (!room) return;
    try {
      // Promotion is per-(sourceKey, table) idempotent, so a renewal compiles only tables that
      // are NEW to the lease (a profile edit mid-life). A compile throw (schema skew) must not
      // kill the renewal — this is a timer-driven void promise, so an escape would be an
      // unhandled rejection — and the live sub keeps its already-promoted tables + the fresh
      // token below; the new table's routing simply never arms (its writes route slow).
      promoteRoomTables(room, rt.tables);
    } catch (err) {
      anomaly("lease-failed", state.remote, `renewal promotion failed: ${String((err as Error)?.message ?? err)} (the room keeps its already-promoted tables)`);
    }
    state.exp = rt.exp;
    if (lease.lifecycle?.doorbell.scope !== undefined) state.doorbellScope = lease.lifecycle.doorbell.scope;
    // Re-present NOW with the fresh token: hand it to the resolver and re-subscribe the live sub
    // (an ordinary epoch bump server-side; the fresh snapshot re-hydrates through the room gate as
    // a net-zero footprint diff).
    tokenHandoffs.set(key, { target: { leaseToken: rt.roomToken, wsEndpoint: rt.wsEndpoint }, at: Date.now() });
    room.source.registerQuery(state.sourceQid, state.remote);
    scheduleRenewal(key, state);
  };

  /** The room channel's subscribe resolver: first subscribe consumes the handed fresh token; every
   *  RE-resolution (reconnect, gap repair, endpoint recovery) is a full re-lease through the app
   *  route — renewal-as-reauthorization, so a revoked/downgraded query cannot silently re-attach. */
  const roomResolver =
    (room: RoomConnection) =>
    async ({ remote }: { queryId: QueryId; remote: RemoteQuery }) => {
      const key = remoteKey(remote);
      const handed = takeHandoff(key);
      if (handed) return handed;
      const lease = await postLease(remote);
      const rt = lease.realtime;
      if (rt === undefined) {
        anomaly(
          "downgrade",
          remote,
          "the re-lease carries no realtime block — the query is no longer room-served (room subscribe aborted; graceful downgrade is Slice I)",
        );
        throw new Error(`realtime downgrade: query "${remote.name}" is no longer room-served`);
      }
      if (rt.sourceKey !== room.sourceKey) {
        anomaly(
          "source-key-changed",
          remote,
          `the re-lease names sourceKey ${JSON.stringify(rt.sourceKey)} but the live sub is on ${JSON.stringify(room.sourceKey)} (teardown + re-register is the §7.6 rare-case follow-up — deferred, no fence for the old room)`,
        );
        throw new Error(`realtime sourceKey changed for query "${remote.name}"`);
      }
      // A re-lease may widen the footprint (new tables) and always refreshes the renewal clock.
      promoteRoomTables(room, rt.tables);
      const state = roomQueries.get(key);
      if (state) {
        state.exp = rt.exp;
        scheduleRenewal(key, state);
        // I-iii: a re-resolution's lifecycle block re-claims like a renewal's (idempotent).
        claimLifecycle(state.lifecycleClaims, lease.lifecycle, state.remote);
      }
      return { leaseToken: rt.roomToken, wsEndpoint: rt.wsEndpoint };
    };

  /** Connect (once) the room source for a lease's `sourceKey` — multiple labeled queries on the
   *  same room share the one source/gate. The endpoint is the lease's DEDICATED
   *  `realtime.wsEndpoint`; the TOP-LEVEL `wsEndpoint` (whole-daemon-session migration) never
   *  reaches a room transport. NO `pushMutation` override: a room-domain mutation ships over the
   *  ROOM socket itself (§7.5 sent-pins-domain — the backend's `channelFor` picks this source). */
  const ensureRoom = (rt: RealtimeLeaseBlock): RoomConnection => {
    const existing = rooms.get(rt.sourceKey);
    if (existing) return existing;
    const room: RoomConnection = {
      sourceKey: rt.sourceKey,
      wsEndpoint: rt.wsEndpoint,
      source: undefined as unknown as RemoteOptimisticSource,
    };
    room.source = new RemoteOptimisticSource(
      { factory: roomTransportFactory, endpoint: rt.wsEndpoint },
      clientID,
      { resolveSubscribe: roomResolver(room) },
    );
    rooms.set(rt.sourceKey, room);
    // connectSource BEFORE any retain on this channel (the backend throws otherwise); it also
    // auto-registers the reserved lmid system query, so the room's confirms fold into
    // `watermark[sourceKey]` from the first frame.
    backend.connectSource(rt.sourceKey, room.source);
    return room;
  };

  // ---- the §4.1 doorbell reaction + upgrade retarget (Slice I-iv) ------------------------------
  //
  // A labeled query the lease left DAEMON-attached (the api-server's occupancy gate suppressed
  // its room-serve — or the server simply couldn't serve it yet) registers as an UPGRADE
  // CANDIDATE under its doorbell scope. The backend's scope-session fold then reports occupancy
  // per release (`onScopeSessions`); on the 0→≥1 transition of ANOTHER clientID's unexpired
  // session the candidate re-leases ONCE — debounced per (name, args): one in-flight re-lease,
  // repeat doorbells coalesce into it — and a reply that NOW carries a realtime block runs the
  // retarget: ensureRoom → promoteRoomTables → hand the roomToken → `backend.retargetRemoteQuery`
  // (the two-phase no-flicker cutover; see its doc) — mirroring `attachRoom`'s exact order
  // (connect + promote BEFORE any wire sub moves), with the retarget primitive replacing the
  // fresh retain. Failures fail OPEN and LOUD: the daemon retain is untouched (the primitive
  // validates before mutating), the anomaly surfaces, and the NEXT doorbell/renewal is the retry
  // — no retry loop of our own. A reply still without a block is SILENT: suppression is the
  // occupancy gate's designed state, not an anomaly.

  interface UpgradeViewHook {
    /** Flip this view's client-side bookkeeping onto the room query state (sets `roomKey`,
     *  joins the refcount) — a released view declines. */
    adoptRoom(key: string, state: RoomQueryState): void;
    /** The I-v inverse: detach this view's bookkeeping from a dismantled room query state (the
     *  downgrade deleted it wholesale — the view must not decrement a dead record on destroy). */
    clearRoom(): void;
  }
  interface UpgradeCandidate {
    remote: RemoteQuery;
    scope: string;
    views: Set<UpgradeViewHook>;
    inFlight: boolean;
    /** Lifecycle system-stream claims this candidate carries between a DOWNGRADE and the next
     *  upgrade (I-v): the dismantled room query's renewal-loop claims move here so the fence
     *  streams (the ghost drop's watermark input) outlive the room state. Adopted by the next
     *  upgrade's fresh {@link RoomQueryState}; released when the candidate dies with its last
     *  view. Empty for a fresh (never-downgraded) candidate. */
    claims: Set<string>;
  }
  /** Candidates by remote key — ONE re-lease upgrades every view of the (name, args) at once
   *  (the backend moves the sub wholesale). */
  const upgradeCandidates = new Map<string, UpgradeCandidate>();
  /** EVERY live labeled view's hook, by remote key (I-v): the downgrade dance runs from the
   *  renewal loop — no view reference in scope — yet must re-register each surviving view as an
   *  upgrade candidate (the doorbell re-arms the next upgrade) and clear its room bookkeeping.
   *  Registered at materialize, dropped at destroy. */
  const labeledViewHooks = new Map<string, Set<UpgradeViewHook>>();
  /** Last observed other-session count per scope — the 0→≥1 transition tracker. A first
   *  observation at ≥1 counts as a transition (there was none before we could see). */
  const lastOthers = new Map<string, number>();

  const runUpgrade = async (cand: UpgradeCandidate): Promise<void> => {
    let lease: QueryLeaseWire;
    try {
      lease = await postLease(cand.remote);
    } catch (err) {
      anomaly(
        "lease-failed",
        cand.remote,
        `doorbell re-lease failed: ${String((err as Error)?.message ?? err)} (staying daemon-attached; the next doorbell/renewal is the retry)`,
      );
      return;
    }
    if (realtimeClosed || cand.views.size === 0) return; // torn down while the lease was in flight
    const rt = lease.realtime;
    if (rt === undefined) return; // still gated server-side (e.g. its minSessions is higher) — stay daemon-attached, silently
    const key = remoteKey(cand.remote);
    if (roomQueries.has(key)) return; // already room-attached (a racing fresh view won) — nothing to move
    try {
      // The G-v attach order, verbatim, up to the sub move: room source/gate first, engine
      // promotion second (both idempotent — `ensureRoom` per sourceKey, `promoteRoomTables` per
      // (sourceKey, table) via the backend's routing record), THEN the wire cutover with the
      // fresh roomToken handed to the room resolver. `retargetRemoteQuery` is itself idempotent
      // per (query, sourceKey), so a duplicate doorbell that slipped the `inFlight` guard cannot
      // double-attach.
      const room = ensureRoom(rt);
      promoteRoomTables(room, rt.tables);
      tokenHandoffs.set(key, { target: { leaseToken: rt.roomToken, wsEndpoint: rt.wsEndpoint }, at: Date.now() });
      const sourceQid = backend.retargetRemoteQuery(cand.remote, rt.sourceKey);
      const state: RoomQueryState = {
        remote: cand.remote,
        sourceKey: rt.sourceKey,
        sourceQid,
        refCount: 0,
        exp: rt.exp,
        // ADOPT the candidate's claims (I-v): a re-upgrade after a downgrade inherits the fence
        // streams the ghost still needs, already subscribed — so they are NOT re-subscribed; a
        // fresh candidate's set is empty. `upgradeCandidates.delete(key)` below leaves the set
        // owned by this state.
        lifecycleClaims: cand.claims,
        doorbellScope: cand.scope,
      };
      // The re-lease's lifecycle block now carries the fence bundle (the query is room-served):
      // claim it on the query's renewal-loop set (idempotent — an adopted key is skipped), exactly
      // as a renewal that turned room-served mid-life would (I-iii) — released when the last view
      // drops the query.
      claimLifecycle(state.lifecycleClaims, lease.lifecycle, cand.remote);
      // No awaits since the `views.size` check above — destroys cannot have interleaved, so at
      // least one view adopts (a released one declines via its own flag, defensively).
      for (const view of [...cand.views]) view.adoptRoom(key, state);
      upgradeCandidates.delete(key);
      roomQueries.set(key, state);
      scheduleRenewal(key, state);
    } catch (err) {
      // Fail open: the retarget primitive validates before mutating, so the daemon retain is
      // intact — the query keeps serving from the daemon exactly as before the doorbell.
      tokenHandoffs.delete(key); // never leave a room token where the DAEMON resolver could eat it
      anomaly(
        "lease-failed",
        cand.remote,
        `upgrade retarget failed: ${String((err as Error)?.message ?? err)} (staying daemon-attached; the next doorbell/renewal is the retry)`,
      );
    }
  };

  /** Kick every idle candidate on `scope` — the doorbell reaction proper. */
  const maybeUpgrade = (scope: string): void => {
    if (realtimeClosed) return;
    for (const cand of upgradeCandidates.values()) {
      if (cand.scope !== scope || cand.inFlight || cand.views.size === 0) continue;
      cand.inFlight = true;
      void runUpgrade(cand).finally(() => {
        cand.inFlight = false;
      });
    }
  };

  const registerUpgradeCandidate = (remote: RemoteQuery, scope: string, hook: UpgradeViewHook): void => {
    const key = remoteKey(remote);
    let cand = upgradeCandidates.get(key);
    if (!cand) upgradeCandidates.set(key, (cand = { remote, scope, views: new Set(), inFlight: false, claims: new Set() }));
    cand.views.add(hook);
    // Registration-time check: a doorbell that FOLDED before this candidate existed (the lease
    // resolve raced the occupancy delta) must still trigger — same count rule as the events.
    if (backend.otherScopeSessions(scope) >= 1) maybeUpgrade(scope);
  };

  const dropUpgradeCandidate = (remote: RemoteQuery, hook: UpgradeViewHook): void => {
    const cand = upgradeCandidates.get(remoteKey(remote));
    if (!cand) return;
    cand.views.delete(hook);
    if (cand.views.size === 0) {
      upgradeCandidates.delete(remoteKey(remote));
      // A candidate carrying a downgrade's fence-stream claims (I-v) releases them with its last
      // view — the LAST holder unsubscribes the wire sub (empty set ⇒ no-op for a fresh candidate).
      releaseLifecycle(cand.claims);
    }
  };

  /** The §4.2 graceful downgrade dance (Slice I-v): a renewal came back with NO realtime block
   *  but WITH a fence. Handle the WHOLE room at once — retarget every live sub sharing the source
   *  onto the daemon (the I-iv retarget in REVERSE), demote the room source behind the watermark
   *  fence (its rows persist as a FROZEN ghost until the daemon plane absorbs the final flush),
   *  close the room transport, and re-register each surviving view as an upgrade candidate so the
   *  next doorbell re-upgrades the same doc. `demoteRoomSource` refuses to demote while any sub is
   *  still on the channel, so all subs must retarget first; a co-tenant query's own later renewal
   *  then finds the room gone and no-ops (retarget-to-daemon + demote are both idempotent).
   *
   *  Ordering with disconnect: `demoteRoomSource` → `disconnectSource` drops the room gate, which
   *  makes the retarget's deferred phase-2 GC (`flushRetargetGc`, run at the daemon's first
   *  release) a no-op — it deletes the pending-GC record then finds no old gate to rewind, so the
   *  room slice's rows leave ONLY through the ghost's `removeRoomSource` under the fence (never via
   *  a GC rewind that would surface a lagging follower's pre-flush images). */
  const downgradeRoom = (fence: RealtimeFenceBlock): void => {
    if (realtimeClosed) return;
    const sourceKey = fence.sourceKey;
    const onRoom = [...roomQueries].filter(([, s]) => s.sourceKey === sourceKey);
    for (const [key, state] of onRoom) {
      backend.retargetRemoteQuery(state.remote, "daemon"); // room → daemon; the no-block reply IS a daemon lease
      if (state.renewTimer !== undefined) clearTimeout(state.renewTimer);
      roomQueries.delete(key);
      const hooks = labeledViewHooks.get(key);
      if (state.doorbellScope !== undefined && hooks !== undefined && hooks.size > 0) {
        let cand = upgradeCandidates.get(key);
        if (!cand) {
          upgradeCandidates.set(key, (cand = { remote: state.remote, scope: state.doorbellScope, views: new Set(), inFlight: false, claims: new Set() }));
        }
        // Carry the renewal-loop's lifecycle claims (the fence streams — the ghost's watermark
        // input, delivered on the DAEMON channel) onto the candidate so they outlive the room
        // state and are adopted by the next upgrade (`runUpgrade`).
        for (const c of state.lifecycleClaims) cand.claims.add(c);
        state.lifecycleClaims.clear();
        for (const h of hooks) {
          h.clearRoom(); // forget the dead room bookkeeping (destroy must not decrement a gone state)
          cand.views.add(h);
        }
        // Self-heal: a collaborator still present at downgrade re-rings immediately. Normally the
        // scope is solo here (that IS why the server downgraded), so this is inert.
        if (backend.otherScopeSessions(state.doorbellScope) >= 1) maybeUpgrade(state.doorbellScope);
      } else {
        // No re-upgrade possible (no doorbell scope, or no surviving view): release the claims.
        if (hooks !== undefined) for (const h of hooks) h.clearRoom();
        releaseLifecycle(state.lifecycleClaims);
      }
    }
    backend.demoteRoomSource(sourceKey, fence.doc, fence.finalFlushSeq); // frozen ghost behind the fence
    const room = rooms.get(sourceKey);
    if (room !== undefined) {
      rooms.delete(sourceKey);
      room.source.close(); // every sub retargeted off it
    }
  };

  // The trigger: the backend reports (scope, other-session count) after each release that folded
  // occupancy rows; the 0→≥1 transition rings. `others` never counts our own clientID or expired
  // rows (the backend's one rule), so a solo tab's own row cannot ring its own bell, and a stale
  // collaborator aging out then re-appearing rings again (0→1 anew) — which is idempotent here
  // (an already-room-attached query has no candidate left to kick).
  backend.onScopeSessions(({ scope, others }) => {
    const prev = lastOthers.get(scope) ?? 0;
    lastOthers.set(scope, others);
    if (prev === 0 && others >= 1) maybeUpgrade(scope);
  });

  // The I-v stuck-downgrade surface (§7.5): a ghost whose watermark fence cleared but whose sent
  // room-domain mids never resolved through the daemon-carried folds. The ghost HOLDS (no
  // timeout-retire is invented) — surface it loudly, naming the mids.
  backend.onDowngradeStuck(({ sourceKey, doc, mids }) => {
    anomaly(
      "downgrade-stuck",
      { name: sourceKey, args: { doc, mids } },
      `the downgrade ghost for doc ${JSON.stringify(doc)} is stuck: sent room mids [${mids.join(", ")}] never resolved through the daemon-carried outcome/ledger folds (§7.5 sent-pins-domain — undecidable in general; the ghost holds, investigate the lost outcome frames)`,
    );
  });

  // The split-register ticket: set (synchronously) by the wrapped `materialize` just before it
  // delegates, consumed by the shadowed `backend.registerQuery` below — which registers the LOCAL
  // half only (the Store's SSR-seed + `unknown` pre-marking has already run) and defers the remote
  // retain to the lease resolution. Everything else (unlabeled queries, React retains, re-registers)
  // flows through untouched.
  let labeledTicket: { consumed: boolean } | null = null;
  const origRegisterQuery = backend.registerQuery.bind(backend);
  backend.registerQuery = (qid: QueryId, ast: Ast, remote?: RemoteQuery, channel?: string): void => {
    if (labeledTicket === null || remote === undefined) {
      origRegisterQuery(qid, ast, remote, channel);
      return;
    }
    const ticket = labeledTicket;
    labeledTicket = null;
    ticket.consumed = true;
    // The LOCAL half of the split retain (the backend's documented split-retain shape): the remote
    // attaches via `retainRemoteQuery` on the channel the lease names, once it answers.
    origRegisterQuery(qid, ast, undefined);
  };

  const origMaterialize = store.materialize.bind(store) as (
    query: Query<any, any, any>,
    mOpts?: unknown,
  ) => MaterializedViewLike;

  /** The G-v labeled-materialize: synchronous local view now, remote retain when the lease answers. */
  const materializeLabeled = (query: Query<any, any, any> & { name: string }, mOpts?: unknown): MaterializedViewLike => {
    const remote: RemoteQuery = { name: query.name, args: query.args };
    const ast = query.ast() as Ast;
    // E3 parity (201-LOCAL-ONLY-TABLES-DESIGN.md): the unlabeled remote path rejects a remote query
    // naming a local-only table synchronously inside materialize; the labeled path defers the
    // remote register past the lease, so run the SAME guard here — identical throw, identical
    // timing, no view leaked.
    for (const t of collectAstTables(ast)) {
      if (localTables.has(t)) {
        throw new Error(
          `remote query "${remote.name}" references local-only table "${t}" — local tables never cross the wire (201-LOCAL-ONLY-TABLES-DESIGN.md E3).`,
        );
      }
    }
    const ticket = { consumed: false };
    labeledTicket = ticket;
    let view: MaterializedViewLike;
    try {
      view = origMaterialize(query, mOpts);
    } finally {
      labeledTicket = null;
    }
    const localQid = view.qid;
    // The Store pre-marked the view `unknown` (a remote-identity register under a lifecycle
    // backend), but the local-half register flipped it back to `complete` (a local-only
    // registration is synchronously authoritative). Re-flip for the lease window so the view never
    // reads server-authoritative before ANY authority answered — the retain below recomputes it
    // against real hydration. (`readOnce` on a labeled query correctly waits because of this.)
    if (ticket.consumed) flipResultTypeUnknown(view);

    let released = false;
    let retainQid: QueryId | undefined;
    let roomKey: string | undefined;
    // I-iii: the lifecycle system-stream claims THIS VIEW holds (claimed once per key when its
    // lease resolves; released with the view — the LAST holder of a scope/doc drops the sub).
    const viewLifecycleClaims = new Set<string>();
    // I-iv/I-v: this view's hook — `adoptRoom` (an upgrade joins the view to the new room state)
    // and `clearRoom` (the downgrade dismantled the room state wholesale — forget it so destroy
    // never decrements a dead record). Registered in `labeledViewHooks` for EVERY labeled view so
    // the downgrade dance (which runs from the renewal loop, no view in scope) can find and
    // re-candidate each surviving view; used as the candidate hook for the daemon-attached shape.
    const viewHook: UpgradeViewHook = {
      adoptRoom: (key: string, state: RoomQueryState): void => {
        if (released) return; // a released view never joins (its retain is already gone)
        roomKey = key;
        state.refCount++;
      },
      clearRoom: (): void => {
        roomKey = undefined;
      },
    };
    const hookKey = remoteKey(remote);
    let viewHooks = labeledViewHooks.get(hookKey);
    if (viewHooks === undefined) labeledViewHooks.set(hookKey, (viewHooks = new Set()));
    viewHooks.add(viewHook);

    /** Fail-open: retain on the daemon, indistinguishable from an unlabeled query. The ONE lease
     *  already resolved (when it succeeded) is handed to the daemon resolver so the subscribe
     *  presents exactly that token — one POST per subscribe, the unlabeled cadence. */
    const attachDaemon = (target?: { leaseToken: string; wsEndpoint?: string }): void => {
      if (target) tokenHandoffs.set(remoteKey(remote), { target, at: Date.now() });
      retainQid = nextRetainQid++;
      backend.retainRemoteQuery(retainQid, remote, localQid, ast);
    };

    /** Room-served: ensure the shared room source/gate, promote the engine per the lease's table
     *  specs BEFORE retaining, then retain the sub on the room channel with the roomToken handed
     *  to the resolver. `doorbellScope` (from the lease's lifecycle block) is pinned on the room
     *  state so a later I-v downgrade can re-candidate this query. */
    const attachRoom = (rt: RealtimeLeaseBlock, doorbellScope?: string): void => {
      const key = remoteKey(remote);
      const existing = roomQueries.get(key);
      if (existing && existing.sourceKey !== rt.sourceKey) {
        anomaly(
          "source-key-changed",
          remote,
          `this lease names sourceKey ${JSON.stringify(rt.sourceKey)} but the live sub is on ${JSON.stringify(existing.sourceKey)} (teardown + re-register is the §7.6 rare-case follow-up, deferred; this view stays local-only)`,
        );
        return;
      }
      const room = ensureRoom(rt);
      promoteRoomTables(room, rt.tables);
      // Only the retain that CREATES the wire sub consumes a token at subscribe time — hand one
      // exactly then (a refcount-only retain issues no wire subscribe; the age cap covers races).
      if (!existing) {
        tokenHandoffs.set(key, { target: { leaseToken: rt.roomToken, wsEndpoint: rt.wsEndpoint }, at: Date.now() });
      }
      retainQid = nextRetainQid++;
      backend.retainRemoteQuery(retainQid, remote, localQid, ast, rt.sourceKey);
      let state = existing;
      if (!state) {
        state = { remote, sourceKey: rt.sourceKey, sourceQid: retainQid, refCount: 0, exp: rt.exp, lifecycleClaims: new Set() };
        roomQueries.set(key, state);
      } else {
        state.exp = Math.max(state.exp, rt.exp);
      }
      if (doorbellScope !== undefined) state.doorbellScope = doorbellScope;
      state.refCount++;
      roomKey = key;
      scheduleRenewal(key, state);
    };

    // Resolve-then-register: the lease FIRST; the register follows its verdict.
    void (async () => {
      let lease: QueryLeaseWire;
      try {
        lease = await postLease(remote);
      } catch (err) {
        // The lease POST itself failed: fail OPEN to the daemon with no handoff — the daemon
        // retain's own resolver re-leases (and the transport's resync retries), exactly an
        // unlabeled query's recovery story.
        anomaly("lease-failed", remote, `query lease failed: ${String((err as Error)?.message ?? err)}`);
        if (!released && !realtimeClosed) attachDaemon();
        return;
      }
      if (released || realtimeClosed) return;
      try {
        if (lease.realtime === undefined) {
          attachDaemon({ leaseToken: lease.leaseToken, wsEndpoint: lease.wsEndpoint });
          // I-iv: a daemon-attached labeled view under a doorbell scope is an UPGRADE CANDIDATE —
          // the occupancy stream's 0→≥1 transition re-leases it and (block permitting) retargets
          // the whole (name, args) sub onto the room. A blockless lease (pre-lifecycle server)
          // registers nothing: the plane stays inert-until-fed.
          const doorbellScope = lease.lifecycle?.doorbell.scope;
          if (doorbellScope !== undefined) registerUpgradeCandidate(remote, doorbellScope, viewHook);
        } else {
          attachRoom(lease.realtime, lease.lifecycle?.doorbell.scope);
        }
        // I-iii: retain the lease's lifecycle system streams on the DAEMON channel — for the
        // room-served AND the daemon-served (labeled, not covered) shapes alike (the doorbell
        // rides both; the fence only where a room block exists). Absent block ⇒ no-op — a
        // pre-lifecycle server leaves this client byte-identical.
        claimLifecycle(viewLifecycleClaims, lease.lifecycle, remote);
      } catch (err) {
        // Fail OPEN, exactly like a lease without a block: a room-attach throw (most plausibly a
        // lease `where` this bundle's schema cannot compile — version skew) must not strand the
        // view local-only. `retainQid === undefined` ⇒ no retain was established (room OR daemon),
        // so the daemon fallback cannot double-attach; a throw AFTER a successful retain (a
        // lifecycle claim, say) leaves the live sub alone. Partial promotion is harmless (it is
        // idempotent, and the room gate re-proves any routed write) — but never leave the room
        // token where the daemon resolver could eat it.
        anomaly("lease-failed", remote, `realtime attach failed: ${String((err as Error)?.message ?? err)} (falling back to the daemon lease)`);
        if (!released && !realtimeClosed && retainQid === undefined) {
          tokenHandoffs.delete(remoteKey(remote));
          try {
            attachDaemon({ leaseToken: lease.leaseToken, wsEndpoint: lease.wsEndpoint });
          } catch (fallbackErr) {
            anomaly("lease-failed", remote, `daemon fallback failed: ${String((fallbackErr as Error)?.message ?? fallbackErr)}`);
          }
        }
      }
    })();

    // Teardown rides the view: release the remote retain (room or daemon) with the local view, and
    // drop the room query's refcount/renewal when the last view goes.
    const origDestroy = view.destroy.bind(view);
    view.destroy = () => {
      if (!released) {
        released = true;
        if (retainQid !== undefined) backend.releaseRemoteQuery(retainQid);
        // I-iv/I-v: drop this view's hook — from the per-key hook registry and the candidate set
        // (both no-ops when it was never a candidate; the candidate's own last-view release frees
        // any fence-stream claims a downgrade parked on it).
        viewHooks.delete(viewHook);
        if (viewHooks.size === 0) labeledViewHooks.delete(hookKey);
        dropUpgradeCandidate(remote, viewHook);
        // I-iii: this view's lifecycle claims drop with it; the LAST holder of a key releases
        // the system sub (the backend's folded fence/occupancy state deliberately survives).
        releaseLifecycle(viewLifecycleClaims);
        if (roomKey !== undefined) {
          const state = roomQueries.get(roomKey);
          if (state && --state.refCount <= 0) {
            if (state.renewTimer !== undefined) clearTimeout(state.renewTimer);
            roomQueries.delete(roomKey);
            // …including any claims the renewal loop made on this query's behalf.
            releaseLifecycle(state.lifecycleClaims);
          }
        }
      }
      origDestroy();
    };
    return view;
  };

  store.materialize = ((query: Query<any, any, any>, mOpts?: unknown) => {
    const label = (query as { realtime?: RealtimeQueryLabel }).realtime;
    if (label === undefined || typeof query.name !== "string") return origMaterialize(query, mOpts);
    return materializeLabeled(query as Query<any, any, any> & { name: string }, mOpts);
  }) as Store<S>["materialize"];

  // Local-table persistence (207 §5.2): attach immediately after the store exists — before any
  // app write can reach `writeLocal` — and AWAIT the initial restore (one `getAll` over small
  // tables) so the first render never flashes empty local state. Restored rows arrive as ordinary
  // deltas, so correctness never needed the gate — only UX does.
  let persistence: LocalPersistence | undefined;
  if (opts.persistLocal) {
    persistence = attachLocalPersistence(backend, opts.schema, opts.persistLocal);
    await persistence.ready;
  }

  const queryEnsures = new QueryEnsureCache(store);

  // Drain folds before the tab goes away (FOLDED-MUTATIONS-DESIGN §0.1/§3): a fold deliberately
  // holds its server write through the debounce window, so an unclean navigation would otherwise
  // lose the applied-locally tail. `pagehide`/`beforeunload` is the last-chance flush; `close`
  // also drains. The same hook is the persistence layer's best-effort final forward/persist
  // (207 §9 — bounds the unacked-tail loss). (Best-effort: an unclean crash still loses the
  // tail — an accepted cost, §0.1.)
  const flushFolds = () => backend.flushFolds();
  const onPageHide = () => {
    flushFolds();
    void persistence?.flush();
  };
  // Structural typing — this package has no DOM lib, but the browser global carries these.
  const target = globalThis as unknown as {
    addEventListener?: (type: string, listener: () => void) => void;
    removeEventListener?: (type: string, listener: () => void) => void;
  };
  target.addEventListener?.("pagehide", onPageHide);
  target.addEventListener?.("beforeunload", onPageHide);

  return {
    store,
    backend,
    mutate,
    ensure: (query, options) => queryEnsures.ensure(query, options),
    flushFolds,
    clientID,
    close: () => {
      flushFolds();
      target.removeEventListener?.("pagehide", onPageHide);
      target.removeEventListener?.("beforeunload", onPageHide);
      persistence?.close(); // releases leadership + the channel + the IDB handle (207 P10)
      queryEnsures.close();
      // Realtime teardown: renewal timers first (no renewal may fire into a closing client), then
      // every room socket; in-flight lease resolutions are made inert via the flag.
      realtimeClosed = true;
      upgradeCandidates.clear(); // no doorbell may retarget into a closing client
      for (const state of roomQueries.values()) {
        if (state.renewTimer !== undefined) clearTimeout(state.renewTimer);
      }
      roomQueries.clear();
      for (const room of rooms.values()) room.source.close();
      rooms.clear();
      source.close();
    },
    __realtimeInspect: (): RealtimeInspect => ({
      rooms: Object.fromEntries(
        [...rooms].map(([sourceKey, room]) => [
          sourceKey,
          {
            wsEndpoint: room.wsEndpoint,
            // Read back from the backend's room-table registry (302 §2): wire → engine table.
            promoted: Object.fromEntries(backend.roomTablesFor(sourceKey)),
            queries: Object.fromEntries(
              [...roomQueries]
                .filter(([, s]) => s.sourceKey === sourceKey)
                .map(([key, s]) => [
                  key,
                  { name: s.remote.name, sourceQid: s.sourceQid, exp: s.exp, refCount: s.refCount },
                ]),
            ),
          },
        ]),
      ),
    }),
  };
}

// --------------------------------------------------------------------------- realtime helpers

/** The minimal view surface the labeled-materialize wrapper needs (structural — the real return
 *  type flows through unchanged). */
interface MaterializedViewLike {
  readonly qid: QueryId;
  destroy(): void;
}

/** The default DECLARED router (302 §5) when the app names `realtime.mutators` and passes no
 *  explicit `domainPolicy`: a declared mutator routes to the ONE attached room; solo or
 *  multi-room it abstains (⇒ daemon). `getRooms` is read lazily per invoke so the policy tracks
 *  attach/downgrade live. */
function declaredMutatorPolicy(
  declared: ReadonlySet<string>,
  getRooms: () => ReadonlyMap<string, unknown>,
): (name: string, args: unknown) => string | undefined {
  return (name) => {
    if (!declared.has(name)) return undefined;
    const rooms = getRooms();
    if (rooms.size !== 1) return undefined; // solo or ambiguous — the daemon path (302 §5)
    return rooms.keys().next().value as string;
  };
}

/** Flip a just-materialized labeled view back to `unknown` for the lease-resolve window. The
 *  plural `FlatArrayView` exposes `setResultType`; a `.one()` query's `SingularView` wrapper hides
 *  it behind its (runtime-visible) `inner` — reach through. Best-effort by design: the deferred
 *  retain recomputes the lifecycle authoritatively the moment it attaches, and the Store keeps
 *  routing backend transitions to the SAME underlying view either way. */
function flipResultTypeUnknown(view: unknown): void {
  const v = view as {
    setResultType?: (rt: "unknown") => void;
    inner?: { setResultType?: (rt: "unknown") => void };
  };
  if (typeof v.setResultType === "function") v.setResultType("unknown");
  else if (typeof v.inner?.setResultType === "function") v.inner.setResultType("unknown");
}

/** Every base table an AST tree can draw from (root, related subtrees, EXISTS children) — the E3
 *  guard's input. Mirrors the backend's own `collectTables`. */
function collectAstTables(ast: Ast, out = new Set<string>()): Set<string> {
  out.add(ast.table);
  for (const rel of ast.related ?? []) collectAstTables(rel.subquery, out);
  collectConditionTables(ast.where, out);
  collectConditionTables(ast.having, out);
  return out;
}

function collectConditionTables(cond: Condition | undefined, out: Set<string>): void {
  if (cond === undefined) return;
  if (cond.type === "and" || cond.type === "or") {
    for (const c of cond.conditions) collectConditionTables(c, out);
  } else if (cond.type === "correlatedSubquery") {
    collectAstTables(cond.related.subquery, out);
  }
}

/** The (name, args) sub identity — key-order-stable, mirroring the backend's own `remoteKey` so
 *  the client-side room bookkeeping groups retains exactly as the backend dedups subs. */
function remoteKey(remote: RemoteQuery): string {
  return stableJson([remote.name, remote.args]);
}

// ---- lifecycle system-sub helpers (Slice I-iii) ----

/** A system sub's wire `args` (under the reserved `_rindle/lifecycle` name): the lease entry's
 *  identity fields plus the PARENT labeled query, so a re-resolution can re-lease the parent and
 *  re-find the entry (`resolveLifecycleTarget`). */
interface LifecycleRemoteArgs {
  table: SystemStreamTable;
  scope?: string;
  doc?: string;
  clientId?: string;
  parent: { name: string; args: unknown };
}

const SYSTEM_TABLES: ReadonlySet<string> = new Set([
  SCOPE_SESSIONS_TABLE,
  ROOM_WATERMARK_TABLE,
  ROOM_CLIENT_MUTATIONS_TABLE,
  ROOM_MUTATION_OUTCOMES_TABLE,
]);

function isSystemTable(table: string): table is SystemStreamTable {
  return SYSTEM_TABLES.has(table);
}

/** The idempotence key a lifecycle retain is claimed under: the minted predicate's full identity
 *  (table + scope/doc/clientId) — two labeled queries on one scope share ONE doorbell sub; two
 *  docs' fences never alias. */
function systemEntryKey(entry: { table: string; scope?: string; doc?: string; clientId?: string }): string {
  return stableJson([entry.table, entry.scope ?? null, entry.doc ?? null, entry.clientId ?? null]);
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

export type { ClientRegistry, MutationTx };
export type { MutateFn } from "./index.ts";

class RindleApiHttpError extends Error {
  readonly path: string;
  readonly status: number;
  readonly body: string;

  constructor(path: string, status: number, body: string) {
    super(`rindle api ${path} failed: ${status}${body ? ` ${body}` : ""}`);
    this.name = "RindleApiHttpError";
    this.path = path;
    this.status = status;
    this.body = body;
  }
}

const MUTATION_GAP_RE = /\bmutation(?: id)? gap\b/i;

function reloadAfterMutationGap(err: unknown): boolean {
  const text =
    err instanceof RindleApiHttpError
      ? `${err.status} ${err.body} ${err.message}`
      : String((err as Error)?.message ?? err);
  if (!MUTATION_GAP_RE.test(text)) return false;

  resetStableClientID();
  console.error(
    "[rindle] mutation gap detected; cleared the dev client id and reloading so this tab starts a fresh mutation stream.",
    err,
  );
  const loc = (globalThis as unknown as { location?: { reload?: () => void } }).location;
  if (typeof loc?.reload !== "function") return false;
  try {
    loc.reload();
    return true;
  } catch {
    return false;
  }
}
