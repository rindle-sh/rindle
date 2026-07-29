// The RemoteBackend: a `@rindle/client` `Backend` over a network transport. It owns the
// epoch/seq/gap protocol (via {@link Subscriber}) and emits only CLEAN, in-order
// `hello`/`snapshot`/`batch` `ChangeEvent`s upward — so the same `Store`/`ArrayView` that
// drives the local backends drives this one, never seeing seq/epoch (WASM-CLIENT-DESIGN.md §2.2).
//
// On a gap (or epoch/schema drift) the backend re-hydrates INTERNALLY: it re-subscribes, the
// server re-registers the query under a NEW epoch and replies with a fresh hello + snapshot,
// and the `ArrayView` resets in place — the caller's materialized view reference survives.

import { Store } from "@rindle/client";
import type { Backend, BackendDevObserver, ChangeEvent, ColsMap, Mutation, QueryId, RemoteQuery, Schema } from "@rindle/client";

import { ProtocolError, Subscriber } from "./protocol.ts";
import type { Batch, Hello, ServerMsg } from "./protocol.ts";
import {
  defaultSubscribeTarget,
  isThenable,
  subscribeMessage,
  type RawMutationSender,
  type SubscribeResolver,
} from "./subscribe.ts";
import { WsTransport } from "./transport.ts";
import { retryDelayMs } from "./query-error.ts";
import type { Transport } from "./transport.ts";

interface QState {
  remote: RemoteQuery;
  subscriber: Subscriber | null;
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
}

export interface RemoteBackendOptions {
  /** Resolve the upstream subscribe target. Defaults to embedded-server `{name,args}`. */
  resolveSubscribe?: SubscribeResolver;
  /** Override raw authoritative writes, e.g. POST them to an app API server. */
  sendMutation?: RawMutationSender;
}

export class RemoteBackend implements Backend {
  private readonly transport: Transport;
  private readonly resolveSubscribe: SubscribeResolver;
  private readonly sendMutation?: RawMutationSender;
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  private readonly devObservers = new Set<BackendDevObserver>();
  private readonly subs = new Map<QueryId, QState>();

  constructor(transport: Transport, opts: RemoteBackendOptions = {}) {
    this.transport = transport;
    this.resolveSubscribe = opts.resolveSubscribe ?? defaultSubscribeTarget;
    this.sendMutation = opts.sendMutation;
    this.transport.onMessage((msg) => this.onServerMsg(msg));
  }

  registerQuery(qid: QueryId, _ast: unknown, remote?: RemoteQuery): void {
    if (!remote) {
      console.error(`[rindle-remote] flat query ${qid} has no named remote identity`);
      return;
    }
    this.subs.set(qid, { remote, subscriber: null, epoch: 0, resubscribing: false, subscribeTicket: 0, retryTimer: undefined, retryAttempt: 0 });
    this.subscribe(qid, remote);
  }

  unregisterQuery(qid: QueryId): void {
    const s = this.subs.get(qid);
    if (s?.retryTimer !== undefined) clearTimeout(s.retryTimer);
    this.subs.delete(qid);
    this.transport.send({ t: "unsubscribe", queryId: qid });
  }

  /** Eventually-consistent: the mutation is sent; the resulting batches arrive async on the
   *  stream. The promise resolves once sent (the §2.3 write asymmetry, named not leaked). */
  mutate(mutations: Mutation[]): Promise<void> {
    if (this.sendMutation) return Promise.resolve(this.sendMutation(mutations));
    this.transport.send({ t: "mutate", mutations });
    return Promise.resolve();
  }

  onEvent(handler: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = handler;
  }

  __attachDevtoolsServerDeltas(observer: BackendDevObserver): () => void {
    this.devObservers.add(observer);
    return () => {
      this.devObservers.delete(observer);
    };
  }

  // --- internals ---------------------------------------------------------------

  private subscribe(qid: QueryId, remote: RemoteQuery): void {
    const s = this.subs.get(qid);
    if (!s) return;
    // A fresh subscribe (gap recovery, the retry timer itself) supersedes any scheduled
    // retryable-error retry — never leave two subscribe paths racing for one query.
    if (s.retryTimer !== undefined) {
      clearTimeout(s.retryTimer);
      s.retryTimer = undefined;
    }
    const request = { queryId: qid, remote, mode: "flat" as const };
    const ticket = ++s.subscribeTicket;
    const send = (target: ReturnType<typeof defaultSubscribeTarget>) => {
      const cur = this.subs.get(qid);
      if (cur !== s || cur.subscribeTicket !== ticket) return;
      this.transport.send(subscribeMessage(request, target));
    };
    const fail = (err: unknown) => {
      const cur = this.subs.get(qid);
      if (cur !== s || cur.subscribeTicket !== ticket) return;
      s.resubscribing = false;
      console.error(`[rindle-remote] query ${qid} subscribe resolution failed: ${String((err as Error)?.message ?? err)}`);
    };
    try {
      const target = this.resolveSubscribe(request);
      if (isThenable(target)) void target.then(send, fail);
      else send(target);
    } catch (err) {
      fail(err);
    }
  }

  private onServerMsg(msg: ServerMsg): void {
    if (msg.t === "queryError") {
      this.onQueryError(msg.queryId, msg);
      return;
    }
    // This backend is flat-only; it ignores normalized frames (`nhello`/`nbatch`).
    if (msg.t !== "hello" && msg.t !== "batch") return;
    const s = this.subs.get(msg.queryId);
    if (!s) return; // unsubscribed / unknown query
    if (msg.t === "hello") this.openSubscriber(msg.queryId, s, msg.hello);
    else this.applyBatch(msg.queryId, s, msg.batch);
  }

  /** Route a `queryError` by its 101 §5 classification: retryable ⇒ keep the QState (and the
   *  rows already folded downstream — 101 §6) and re-subscribe after a jittered backoff
   *  honoring `retryAfterMs`; terminal (or pre-classification servers) ⇒ drop the
   *  subscription, exactly as before (FOLLOWER-LAG-SHED §6.3). */
  private onQueryError(qid: QueryId, err: { message: string; code?: string; retryable?: boolean; retryAfterMs?: number }): void {
    const s = this.subs.get(qid);
    if (!s) return;
    if (err.retryable !== true) {
      if (s.retryTimer !== undefined) clearTimeout(s.retryTimer);
      this.subs.delete(qid);
      console.error(`[rindle-remote] query ${qid} subscription rejected: ${err.message}`);
      return;
    }
    if (s.retryTimer !== undefined) return; // a retry is already scheduled — don't stack them
    s.subscriber = null; // stop validating the dead epoch; recovery is a fresh seq-0 hydrate
    s.resubscribing = true;
    const delay = retryDelayMs(s.retryAttempt++, err.retryAfterMs);
    console.warn(
      `[rindle-remote] query ${qid} ${err.code ?? "error"} (retryable): re-subscribing in ${delay}ms: ${err.message}`,
    );
    const timer = setTimeout(() => {
      const cur = this.subs.get(qid);
      if (cur !== s) return;
      s.retryTimer = undefined;
      this.subscribe(qid, s.remote);
    }, delay);
    (timer as { unref?: () => void }).unref?.();
    s.retryTimer = timer;
  }

  private openSubscriber(qid: QueryId, s: QState, hello: Hello): void {
    try {
      // The Subscriber ctor validates the comparator + fingerprint and emits the `hello`
      // ChangeEvent (→ the Store resets the view to this schema).
      s.subscriber = new Subscriber(hello, (ev) => this.emitServerEvent(qid, ev));
      s.epoch = hello.epoch;
      s.resubscribing = false;
      s.retryAttempt = 0; // a successful hello resets the retryable-error backoff
    } catch (e) {
      // A comparator/schema mismatch at hello is unrecoverable (a code-contract divergence) —
      // leave the view pending and report it; do not loop.
      s.subscriber = null;
      console.error(`[rindle-remote] query ${qid} subscription rejected: ${(e as Error).message}`);
    }
  }

  private applyBatch(qid: QueryId, s: QState, batch: Batch): void {
    if (!s.subscriber) return; // no hello yet (or mid re-hydrate)
    if (batch.epoch < s.epoch) return; // a stale batch from a superseded epoch — drop
    try {
      s.subscriber.apply(batch);
    } catch (e) {
      if (!(e instanceof ProtocolError)) throw e;
      if (s.resubscribing) return; // already recovering
      // Gap / drift → re-hydrate under a new epoch (the server bumps it on re-subscribe).
      s.resubscribing = true;
      s.subscriber = null;
      this.subscribe(qid, s.remote);
    }
  }

  private emitServerEvent(qid: QueryId, ev: ChangeEvent): void {
    this.handler(qid, ev);
    if (this.devObservers.size) {
      for (const o of this.devObservers) o.onServerDelta?.(qid, { format: "flat", event: ev });
    }
  }
}

/** Convenience: a `Store` backed by a remote server, over a ws URL or a custom transport. */
export function createRemoteStore<S extends ColsMap>(
  schema: Schema<S>,
  urlOrTransport: string | Transport,
  opts: RemoteBackendOptions = {},
): Store<S> {
  const transport = typeof urlOrTransport === "string" ? new WsTransport(urlOrTransport) : urlOrTransport;
  return new Store(schema, new RemoteBackend(transport, opts));
}
