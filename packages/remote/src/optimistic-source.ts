// RemoteOptimisticSource: an `OptimisticSource` over a network transport — the ws sibling of
// the in-process native source (OPTIMISTIC-WRITES-DESIGN.md §8). The optimistic protocol is
// the normalized subscription stream with `cv`-stamped frames, plus two extras:
//
//   - upstream: `init` (the connection's stable clientID, sent once) and `pushMutation`
//     (one named-mutator envelope, §8.1) — confirmation rides the progress frames, so
//     `pushMutation` resolves on send;
//   - downstream: connection-level `progress` frames `{cvMin}` (§8.6; mutation confirmation is DATA — the lmid system query),
//     relayed verbatim to the `OptimisticBackend` (which buffers data frames by `cv` and
//     releases all `cv ≤ cvMin` as one coherent step, §8.5).
//
// Per-query validation is the ordinary `NormalizedSubscriber` (epoch/fp/seq); on a gap it
// re-subscribes, and the server re-registers under a NEW epoch and replies with a fresh
// `cv`-stamped snapshot + a progress frame that releases it.

import { LMID_QUERY_NAME } from "@rindle/client";
import type {
  MutationEnvelope,
  MutationOutcomeFrame,
  NormalizedEvent,
  NormalizedTableSchema,
  OptimisticSource,
  ProgressFrame,
  QueryId,
  RemoteQuery,
} from "@rindle/client";

import type { AffinityTicketStore } from "./affinity.ts";
import { NormalizedSubscriber } from "./normalized.ts";
import { retryDelayMs } from "./query-error.ts";
import type { NormalizedBatch, NormalizedHello } from "./normalized.ts";
import { ProtocolError } from "./protocol.ts";
import type { ServerMsg } from "./protocol.ts";
import {
  defaultSubscribeTarget,
  isThenable,
  subscribeMessage,
  type MutationEnvelopeSender,
  type SubscribeResolver,
  type SubscribeTarget,
} from "./subscribe.ts";
import { WsTransport } from "./transport.ts";
import type { Transport } from "./transport.ts";

interface QState {
  remote: RemoteQuery;
  subscriber: NormalizedSubscriber | null;
  /** The epoch of the current subscription (0 before the first hello). */
  epoch: number;
  /** True between sending a re-subscribe and receiving its hello (so a second gap is ignored). */
  resubscribing: boolean;
  /** Monotonic token that cancels stale async lease resolutions. */
  subscribeTicket: number;
  /** Pending retryable-error re-subscribe timer (FOLLOWER-LAG-SHED §6.3), if any. */
  retryTimer: ReturnType<typeof setTimeout> | undefined;
  /** Consecutive retryable errors without a successful hello — the backoff exponent. */
  retryAttempt: number;
  /** Whether the LAST subscribe sent for this query presented a `leaseToken` — i.e. its hello
   *  proves the connection is (re-)AUTHENTICATED (a room shell sets the socket's subject from the
   *  first verified token; `pushMutation` requires it). After a reconnect on a lease-auth session,
   *  queued pushes flush at the first such hello, never earlier — an envelope racing ahead of the
   *  token subscribe would be refused by the shell's subject gate (H-v §7.5 rule 3). */
  authed: boolean;
}

/** Build a transport to a follower's public ws endpoint (READ-ROUTER-DESIGN.md §2.3). Default
 *  `(endpoint) => new WsTransport(endpoint)`. */
export type TransportFactory = (endpoint: string) => Transport;

/** How the source obtains its transport:
 *  - a pre-built {@link Transport} (or `{ transport }`) — FIXED: no endpoint migration, `wsEndpoint`
 *    on leases is ignored (in-process / tests / a single static daemon);
 *  - `{ factory, endpoint? }` — REPLACEABLE: transports are built on demand. An initial `endpoint`
 *    (a static `wsUrl` or an SSR-injected bootstrap) opens eagerly; otherwise the first lease's
 *    `wsEndpoint` opens it lazily, and a later lease naming a DIFFERENT endpoint migrates the whole
 *    session there. */
export type RemoteOptimisticConnection =
  | Transport
  | { transport: Transport }
  | { factory: TransportFactory; endpoint?: string };

export interface RemoteOptimisticSourceOptions {
  /** Resolve the upstream subscribe target. Defaults to embedded-server `{name,args}`. */
  resolveSubscribe?: SubscribeResolver;
  /** Override named-mutator delivery, e.g. POST envelopes to the app API server. */
  pushMutation?: MutationEnvelopeSender;
  /** Follower-affinity mode (FOLLOWER-AFFINITY-DESIGN.md §3): the shared ticket store. When set, the
   *  source records the follower's minted ticket from the `{t:"affinity"}` frame and CLEARS it on a
   *  sustained outage so the next (ticketless) reconnect anycasts to a live follower and re-pins
   *  (§8). Absent ⇒ affinity off (today's behavior). */
  affinity?: AffinityTicketStore | (() => AffinityTicketStore | undefined);
}

export class RemoteOptimisticSource implements OptimisticSource {
  /** The CURRENT transport (undefined in pure-lazy mode until the first lease opens one). */
  private transport: Transport | undefined;
  /** The ws endpoint the current transport points at (undefined for a fixed transport). */
  private currentEndpoint: string | undefined;
  /** Builds a transport for an endpoint; undefined ⇒ fixed transport (no migration). */
  private readonly transportFactory: TransportFactory | undefined;
  private readonly clientID: string;
  private readonly resolveSubscribe: SubscribeResolver;
  private readonly pushMutationSender?: MutationEnvelopeSender;
  /** Resolve the current affinity ticket store. A thunk lets the one-call client turn affinity on
   *  after its first pure-lazy lease returns a placement ticket, before it opens the socket. */
  private readonly affinityStore: () => AffinityTicketStore | undefined;
  private handler: (qid: QueryId, ev: NormalizedEvent) => void = () => {};
  private progressHandler: (frame: ProgressFrame) => void = () => {};
  private restartHandler: () => void = () => {};
  private outcomeHandler: (frame: MutationOutcomeFrame) => void = () => {};
  private resyncHandler: () => void = () => {};
  /** Set by {@link resync} when this is a LEASE-AUTH session (some sub presented a `leaseToken`):
   *  transport pushes queue in {@link pendingPushes} until the first authenticated re-subscribe's
   *  hello re-establishes the socket's subject, then flush (see QState.authed). Never set on a
   *  token-less (embedded/rindled) session — its pushes need no subject and go straight out. */
  private awaitingAuthedHello = false;
  /** Envelopes held while {@link awaitingAuthedHello} (H-v §7.5 rule 3). Every entry corresponds
   *  to a still-pending backend mutation (the re-send reconstructs from pending entries; app
   *  invokes in the window are pending by definition), so a superseding resync may CLEAR this —
   *  its own re-send regenerates whatever still matters. */
  private pendingPushes: MutationEnvelope[] = [];
  private readonly subs = new Map<QueryId, QState>();
  /** Queries whose subscribe is waiting for a transport to exist — endpoint-less subscribes issued
   *  in pure-lazy mode before any lease opens a transport (the lmid system query is registered by
   *  the backend at construction). Flushed when a transport comes up. */
  private readonly deferred = new Set<QueryId>();
  /** One warning per source when an APP lease resolves without a `wsEndpoint` in pure-lazy mode —
   *  nothing will ever open the transport, which is otherwise silent (views just stay empty). */
  private warnedEndpointlessLease = false;
  /** The daemon's boot id (from each `nhello`); a change means it restarted. */
  private lastBootId: string | undefined;
  /** The client's own typed per-table schemas, for hello validation (CRIT#4); set by the backend. */
  private clientTables: NormalizedTableSchema[] | undefined;
  /** Once true (set by {@link close}), in-flight lease resolutions are inert — they must not open a
   *  new transport or send after teardown. */
  private closed = false;
  /** True once any lease has carried a routed `wsEndpoint`. Gates onDown re-leasing so a single
   *  UNROUTED daemon keeps its pre-router behavior (recover via reconnect→resync only), not an extra
   *  lease POST during an outage. */
  private sawRoutedEndpoint = false;
  /** Bumped at the start of every re-subscribe-all pass. A migrate triggered mid-pass starts a new
   *  pass (higher generation); the outer pass then aborts instead of re-subscribing queries twice. */
  private resubscribeGen = 0;

  constructor(connection: RemoteOptimisticConnection, clientID: string, opts: RemoteOptimisticSourceOptions = {}) {
    this.clientID = clientID;
    this.resolveSubscribe = opts.resolveSubscribe ?? defaultSubscribeTarget;
    this.pushMutationSender = opts.pushMutation;
    const affinity = opts.affinity;
    this.affinityStore = typeof affinity === "function" ? affinity : () => affinity;
    if (isTransport(connection)) {
      this.transportFactory = undefined;
      this.bringUp(connection, undefined);
    } else if ("transport" in connection) {
      this.transportFactory = undefined;
      this.bringUp(connection.transport, undefined);
    } else {
      this.transportFactory = connection.factory;
      if (connection.endpoint !== undefined) this.openEndpoint(connection.endpoint);
    }
  }

  /** Wire a transport's handlers (no `init`). */
  private attach(transport: Transport): void {
    transport.onMessage((msg) => this.onServerMsg(msg));
    // Heal a dropped/restarted connection (same endpoint): on reconnect, replay init + re-subscribe.
    transport.onReconnect?.(() => {
      // The held ticket was useful for routing this ws handshake, but HTTP re-leases must wait for
      // THIS connection's first affinity frame. Otherwise an expired/rotated persisted ticket can
      // independently re-pin the control leg before the fresh frame arrives.
      this.affinityStore()?.connectionPending();
      this.resync();
    });
    // Sustained outage on this endpoint: re-lease (the router may move us off a dead follower, §3).
    transport.onDown?.(() => this.onDown());
  }

  /** Make `transport` the current one, announce identity, and (re)subscribe anything deferred. */
  private bringUp(transport: Transport, endpoint: string | undefined): void {
    this.transport = transport;
    this.currentEndpoint = endpoint;
    // `WsTransport` has already evaluated the ticket thunk while constructing this connection.
    // From this point the old/persisted ticket is handshake-only until the follower confirms the
    // selected machine with its first affinity frame.
    this.affinityStore()?.connectionPending();
    this.attach(transport);
    transport.send({ t: "init", clientID: this.clientID });
    this.flushDeferred();
  }

  /** Build + bring up a fresh transport to `endpoint` (replaceable mode only). */
  private openEndpoint(endpoint: string): void {
    if (!this.transportFactory) return;
    this.bringUp(this.transportFactory(endpoint), endpoint);
  }

  /** Migrate the whole session to a new follower (§2.3): build the new transport, tear the old one
   *  down, and re-subscribe EVERY active query there (re-leasing — the old tokens are
   *  follower-local and invalid on the new node). */
  private migrate(endpoint: string): void {
    const old = this.transport;
    this.openEndpoint(endpoint);
    old?.close();
    this.resubscribeAll();
  }

  /** Re-subscribe every live query on the current transport (each re-resolves its lease). A
   *  re-subscribe can synchronously trigger a `migrate` (lease names a new endpoint), whose own
   *  re-subscribe pass supersedes this one — the generation check then aborts this pass so a query
   *  is never re-subscribed (and re-leased) twice. */
  private resubscribeAll(): void {
    const gen = ++this.resubscribeGen;
    for (const [qid, s] of this.subs) {
      if (this.resubscribeGen !== gen) return; // a nested migrate/resubscribe took over — stop
      s.subscriber = null;
      s.resubscribing = true;
      this.subscribe(qid, s.remote);
    }
  }

  /** Flush subscribes deferred until a transport existed (e.g. the lmid query in pure-lazy mode). */
  private flushDeferred(): void {
    if (this.deferred.size === 0) return;
    const qids = [...this.deferred];
    this.deferred.clear();
    for (const qid of qids) {
      const s = this.subs.get(qid);
      if (s) this.subscribe(qid, s.remote);
    }
  }

  /** The current follower's ws is sustainedly down — re-lease every query. The router returns a
   *  (possibly new) `wsEndpoint`: a changed one migrates the session; an unchanged one re-subscribes
   *  over the reconnecting transport (READ-ROUTER-DESIGN.md §3). No-op for an UNROUTED daemon (no
   *  lease ever carried a `wsEndpoint`) — there is nowhere to move, so we keep the pre-router
   *  behavior and let the transport's own reconnect→resync recover. */
  private onDown(): void {
    if (this.closed) return;
    const affinityStore = this.affinityStore();
    if (affinityStore) {
      // Affinity: the pinned follower is gone (sustained outage). Drop the ticket so the transport's
      // ongoing reconnects go TICKETLESS — the fleet edge then selects a live follower, which mints
      // a fresh ticket, and that reconnect's `onReconnect` → resync re-leases there (FOLLOWER-AFFINITY
      // §8, one bounded reassignment). Nothing to migrate: the ws host is fixed; the edge routes by ticket.
      affinityStore.clear();
      return;
    }
    if (!this.sawRoutedEndpoint) return;
    this.resubscribeAll();
  }

  /** Tear down the current transport and make any in-flight lease resolution inert (a late lease
   *  must NOT open a new transport after the consumer closed the client). */
  close(): void {
    this.closed = true;
    this.transport?.close();
    for (const s of this.subs.values()) {
      if (s.retryTimer !== undefined) clearTimeout(s.retryTimer);
    }
    this.subs.clear();
    this.deferred.clear();
    this.pendingPushes.length = 0;
  }

  /** Register a handler fired when the DAEMON restarts (a new boot id) — the backend resets its
   *  `cv` watermark so the new daemon's reset `cv` sequence is accepted instead of dropped. */
  onRestart(handler: () => void): void {
    this.restartHandler = handler;
  }

  expectClientSchema(tables: NormalizedTableSchema[]): void {
    this.clientTables = tables;
  }

  registerQuery(qid: QueryId, remote: RemoteQuery): void {
    this.subs.set(qid, {
      remote,
      subscriber: null,
      epoch: 0,
      resubscribing: false,
      subscribeTicket: 0,
      retryTimer: undefined,
      retryAttempt: 0,
      authed: false,
    });
    this.subscribe(qid, remote);
  }

  unregisterQuery(qid: QueryId): void {
    const s = this.subs.get(qid);
    if (s?.retryTimer !== undefined) clearTimeout(s.retryTimer);
    this.subs.delete(qid);
    this.deferred.delete(qid);
    this.transport?.send({ t: "unsubscribe", queryId: qid });
  }

  pushMutation(envelope: MutationEnvelope): Promise<void> {
    if (this.pushMutationSender) return Promise.resolve(this.pushMutationSender(envelope));
    // A lease-auth session that just reconnected is not yet re-authenticated (the shell's
    // `pushMutation` subject gate would refuse) — hold the envelope until the first token
    // re-subscribe's hello, then flush in order (H-v §7.5 rule 3).
    if (this.awaitingAuthedHello) {
      this.pendingPushes.push(envelope);
      return Promise.resolve();
    }
    this.transport?.send({ t: "pushMutation", envelope });
    return Promise.resolve();
  }

  onNormalized(handler: (qid: QueryId, ev: NormalizedEvent) => void): void {
    this.handler = handler;
  }

  onProgress(handler: (frame: ProgressFrame) => void): void {
    this.progressHandler = handler;
  }

  /** The room deopt handshake's verdict stream (H-v). Dispatched OUT-OF-BAND on arrival — see
   *  {@link onServerMsg}'s `mutationOutcome` arm for why it must never wait behind the cv buffer. */
  onMutationOutcome(handler: (frame: MutationOutcomeFrame) => void): void {
    this.outcomeHandler = handler;
  }

  /** Fired once per re-established session, SYNCHRONOUSLY inside {@link resync} — before any
   *  post-reconnect frame can release (the §7.5 rule-3 window: a replayed lmid snapshot must not
   *  retire an entry whose outcome frame died with the old socket before the re-send captured
   *  it). The backend re-sends the domain's unconfirmed pending envelopes with their original
   *  mids; on a lease-auth session their DELIVERY is deferred until the first token hello
   *  re-authenticates the socket ({@link pendingPushes}). */
  onResync(handler: () => void): void {
    this.resyncHandler = handler;
  }

  // --- internals ---------------------------------------------------------------

  private subscribe(qid: QueryId, remote: RemoteQuery): void {
    const s = this.subs.get(qid);
    if (!s) return;
    // A fresh subscribe (reconnect resync, gap recovery, the retry timer itself) supersedes any
    // scheduled retryable-error retry — never leave two subscribe paths racing for one query.
    if (s.retryTimer !== undefined) {
      clearTimeout(s.retryTimer);
      s.retryTimer = undefined;
    }
    const request = { queryId: qid, remote, mode: "normalized" as const };
    const ticket = ++s.subscribeTicket;
    const send = (target: SubscribeTarget) => {
      if (this.closed) return; // a lease that resolved after close() must not (re)open anything
      const cur = this.subs.get(qid);
      if (cur !== s || cur.subscribeTicket !== ticket) return;
      const endpoint = "leaseToken" in target ? target.wsEndpoint : undefined;
      s.authed = "leaseToken" in target; // a token subscribe (re-)authenticates the socket (H-v)
      if (endpoint !== undefined) this.sawRoutedEndpoint = true;
      if (this.transport) {
        if (endpoint !== undefined && endpoint !== this.currentEndpoint && this.transportFactory) {
          // The router placed this key on a DIFFERENT follower — migrate the whole session there.
          // `migrate` re-subscribes every query (incl. this one) over the new transport, so return.
          this.migrate(endpoint);
          return;
        }
        this.transport.send(subscribeMessage(request, target));
        return;
      }
      // No transport yet (pure-lazy): the first lease naming an endpoint opens it.
      if (endpoint !== undefined && this.transportFactory) {
        this.openEndpoint(endpoint);
        this.transport!.send(subscribeMessage(request, target));
        return;
      }
      // Endpoint-less with no transport (the lmid system query before the first lease): defer until
      // a transport comes up, then re-run this subscribe. An APP lease landing here means the
      // server never names an endpoint (e.g. an api-server with an explicit `daemon` and no
      // `rindle.wsUrl`) while the client has no `wsUrl` of its own — the deferral would be
      // permanent and silent, so say it once.
      if ("leaseToken" in target && !this.warnedEndpointlessLease) {
        this.warnedEndpointlessLease = true;
        console.warn(
          "[rindle-remote] a query lease returned no wsEndpoint and no daemon.wsUrl/transport is " +
            "configured — the live subscription cannot open. Configure the API server's " +
            "rindle.wsUrl (or pass daemon.wsUrl to createRindleClient).",
        );
      }
      this.deferred.add(qid);
    };
    const fail = (err: unknown) => {
      const cur = this.subs.get(qid);
      if (cur !== s || cur.subscribeTicket !== ticket) return;
      s.resubscribing = false;
      console.error(
        `[rindle-remote] optimistic query ${qid} subscribe resolution failed: ${String((err as Error)?.message ?? err)}`,
      );
    };
    try {
      // The reserved lmid system query is part of the optimistic WIRE contract, not an app
      // query: the server resolves it from the connection's own `init` identity. It must
      // never route through the app's subscribe resolver (the API server has no such named
      // query, and a lease for it would be meaningless).
      const target =
        remote.name === LMID_QUERY_NAME ? defaultSubscribeTarget(request) : this.resolveSubscribe(request);
      if (isThenable(target)) void target.then(send, fail);
      else send(target);
    } catch (err) {
      fail(err);
    }
  }

  private onServerMsg(msg: ServerMsg): void {
    if (msg.t === "affinity") {
      // Connection-level: the follower minted/refreshed this connection's placement ticket. Persist
      // it (via the store) so the next connect offers it as a subprotocol and the lease POST forwards
      // it — both legs then pin THIS follower (§4). Off ⇒ no store ⇒ dropped.
      this.affinityStore()?.set(msg.ticket);
      return;
    }
    if (msg.t === "progress") {
      this.progressHandler(msg.frame);
      return;
    }
    if (msg.t === "mutationOutcome") {
      // OUT-OF-BAND BY DESIGN (H-v): the frame has no `cv`, so it must NEVER be routed through the
      // backend's cv buffer — dispatch immediately. A deopt has to migrate its pending entry to
      // the daemon stream BEFORE the buffered lmid release that would otherwise retire it as a
      // success (silence + lmid coverage ⇒ applied), and the §7.3 hold-back trigger — keyed on the
      // entry's confirming domain — would then park its staged writes the wrong way.
      this.outcomeHandler({
        mid: msg.mid,
        kind: msg.kind,
        ...(msg.reason !== undefined ? { reason: msg.reason } : {}),
        ...(msg.name !== undefined ? { name: msg.name } : {}),
        ...("args" in msg ? { args: msg.args } : {}),
      });
      return;
    }
    if (msg.t === "queryError") {
      this.onQueryError(msg.queryId, msg);
      return;
    }
    if (msg.t !== "nhello" && msg.t !== "nbatch") return;
    // Restart detection rides every nhello (connection-level) and runs BEFORE this query's
    // snapshot buffers, so the backend's reset clears stale state ahead of the fresh hydrate.
    if (msg.t === "nhello") this.observeBootId(msg.bootId);
    const s = this.subs.get(msg.queryId);
    if (!s) return; // unsubscribed / unknown query
    if (msg.t === "nhello") this.openSubscriber(msg.queryId, s, msg.hello);
    else this.applyBatch(msg.queryId, s, msg.batch);
  }

  /** On reconnect: re-announce identity, fire the `onResync` re-send, and re-subscribe every live
   *  query (each re-resolves its lease, so a restarted daemon re-materializes + re-leases on the
   *  transiently). The re-send fires HERE — synchronously, before any post-reconnect frame can be
   *  processed — because the §7.5 rule-3 window closes fast: the re-subscribed lmid stream's
   *  fresh snapshot may cover a mid whose outcome frame died with the OLD socket, and once the
   *  release retires that entry as an apparent success there is nothing left to re-send (the
   *  lost-deopt write would silently vanish). Firing now captures the in-flight set intact; on a
   *  lease-auth session the envelopes themselves are HELD ({@link pendingPushes}) until the first
   *  token re-subscribe's hello re-authenticates the socket, then flush in order — so the shell's
   *  subject gate never refuses them, and its re-answer (a recorded outcome for any non-applied
   *  mid) resolves even an already-retired entry via the handshake's not-found arm. */
  private resync(): void {
    if (this.closed) return;
    this.transport?.send({ t: "init", clientID: this.clientID });
    // Lease-auth session ⇒ hold pushes until re-authed. A stale queue from a superseded resync is
    // cleared first: every held envelope maps to a still-pending mutation, and THIS resync's
    // re-send below regenerates whatever still matters (no loss, no stale duplicates).
    this.pendingPushes.length = 0;
    this.awaitingAuthedHello = [...this.subs.values()].some((s) => s.authed);
    this.resyncHandler();
    this.resubscribeAll();
  }

  /** Track the daemon's boot id; a change (after the first) means it restarted — fire onRestart. */
  private observeBootId(bootId: string | undefined): void {
    if (!bootId) return;
    if (this.lastBootId !== undefined && bootId !== this.lastBootId) this.restartHandler();
    this.lastBootId = bootId;
  }

  /** Route a `queryError` by its 101 §5 classification: retryable ⇒ keep the QState (and the
   *  rows already folded downstream — 101 §6) and re-subscribe after a jittered backoff
   *  honoring `retryAfterMs`; terminal (or pre-classification servers) ⇒ drop the
   *  subscription, exactly as before. Fixes the stranded-client gap: a worker fault's or a
   *  shedding follower's error now heals end-to-end (FOLLOWER-LAG-SHED §6.3). */
  private onQueryError(qid: QueryId, err: { message: string; code?: string; retryable?: boolean; retryAfterMs?: number }): void {
    const s = this.subs.get(qid);
    if (!s) return;
    if (err.retryable !== true) {
      if (s.retryTimer !== undefined) clearTimeout(s.retryTimer);
      this.subs.delete(qid);
      console.error(`[rindle-remote] optimistic query ${qid} subscription rejected: ${err.message}`);
      return;
    }
    if (s.retryTimer !== undefined) return; // a retry is already scheduled — don't stack them
    s.subscriber = null; // stop validating the dead epoch; recovery is a fresh seq-0 hydrate
    s.resubscribing = true;
    const delay = retryDelayMs(s.retryAttempt++, err.retryAfterMs);
    console.warn(
      `[rindle-remote] optimistic query ${qid} ${err.code ?? "error"} (retryable): re-subscribing in ${delay}ms: ${err.message}`,
    );
    const timer = setTimeout(() => {
      const cur = this.subs.get(qid);
      if (cur !== s) return;
      s.retryTimer = undefined;
      this.subscribe(qid, s.remote);
    }, delay);
    // Node returns a Timeout (unref keeps a retiring process from being pinned by a retry);
    // browsers return a number, where the optional call is a no-op.
    (timer as { unref?: () => void }).unref?.();
    s.retryTimer = timer;
  }

  private openSubscriber(qid: QueryId, s: QState, hello: NormalizedHello): void {
    try {
      s.subscriber = new NormalizedSubscriber(hello, (ev) => this.handler(qid, ev), this.clientTables);
      s.epoch = hello.epoch;
      s.resubscribing = false;
      s.retryAttempt = 0; // a successful hello resets the retryable-error backoff
      // H-v §7.5 rule 3: the first AUTHENTICATED hello after a reconnect proves the socket is
      // re-authorized (the shell set its subject from the verified token — pushMutation-ready):
      // flush the held envelopes, in order. A token-less hello (the lmid system query) does not
      // qualify — an envelope racing ahead of the lease-token subscribe would be refused.
      if (this.awaitingAuthedHello && s.authed) {
        this.awaitingAuthedHello = false;
        for (const envelope of this.pendingPushes.splice(0)) {
          this.transport?.send({ t: "pushMutation", envelope });
        }
      }
    } catch (e) {
      // A comparator/fp mismatch at hello is unrecoverable (a code-contract divergence).
      s.subscriber = null;
      console.error(`[rindle-remote] optimistic query ${qid} subscription rejected: ${(e as Error).message}`);
    }
  }

  private applyBatch(qid: QueryId, s: QState, batch: NormalizedBatch): void {
    if (!s.subscriber) return; // no hello yet (or mid re-hydrate)
    if (batch.epoch < s.epoch) return; // a stale batch from a superseded epoch — drop
    try {
      s.subscriber.apply(batch);
    } catch (e) {
      if (!(e instanceof ProtocolError)) throw e;
      if (s.resubscribing) return; // already recovering
      // Gap / drift → re-hydrate under a new epoch; the fresh snapshot arrives `cv`-stamped
      // and releases (re-hydrating the footprint) at the server's accompanying progress frame.
      s.resubscribing = true;
      s.subscriber = null;
      this.subscribe(qid, s.remote);
    }
  }
}

/** Convenience: a `RemoteOptimisticSource` over a ws URL or a custom transport. A URL becomes a
 *  replaceable connection seeded at that endpoint (so a routed lease can still migrate it); a
 *  pre-built transport stays fixed. */
export function createRemoteOptimisticSource(
  urlOrTransport: string | Transport,
  clientID: string,
  opts: RemoteOptimisticSourceOptions = {},
): RemoteOptimisticSource {
  const connection: RemoteOptimisticConnection =
    typeof urlOrTransport === "string"
      ? { factory: (endpoint) => new WsTransport(endpoint), endpoint: urlOrTransport }
      : urlOrTransport;
  return new RemoteOptimisticSource(connection, clientID, opts);
}

function isTransport(value: RemoteOptimisticConnection): value is Transport {
  return typeof (value as Transport).send === "function" && typeof (value as Transport).onMessage === "function";
}
