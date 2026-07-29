// RemoteNormalizedSource: a `NormalizedSource` over a network transport — the ws sibling of
// the in-process native source (NORMALIZED-CHANGES-DESIGN.md §7). It subscribes in normalized
// mode, owns the epoch/seq/gap protocol (via {@link NormalizedSubscriber}), and emits clean
// `NormalizedEvent`s upward — so `@rindle/normalized`'s `NormalizedBackend` drives the local
// engine identically whether the footprint stream comes from in-process or over the wire.
//
// On a gap (or epoch/fp drift) it re-subscribes; the server re-registers the query under a NEW
// epoch and replies with a fresh hello + snapshot, and `NormalizedSync` diffs the new footprint
// against the old (the set-analogue re-hydrate, §5.3).

import type {
  Mutation,
  NormalizedEvent,
  NormalizedSource,
  NormalizedTableSchema,
  QueryId,
  RemoteQuery,
} from "@rindle/client";

import { NormalizedSubscriber } from "./normalized.ts";
import { retryDelayMs } from "./query-error.ts";
import type { NormalizedBatch, NormalizedHello } from "./normalized.ts";
import { ProtocolError } from "./protocol.ts";
import type { ServerMsg } from "./protocol.ts";
import {
  defaultSubscribeTarget,
  isThenable,
  subscribeMessage,
  type RawMutationSender,
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
}

export interface RemoteNormalizedSourceOptions {
  /** Resolve the upstream subscribe target. Defaults to embedded-server `{name,args}`. */
  resolveSubscribe?: SubscribeResolver;
  /** Override raw authoritative writes, e.g. POST them to an app API server. */
  sendMutation?: RawMutationSender;
}

export class RemoteNormalizedSource implements NormalizedSource {
  private readonly transport: Transport;
  private readonly resolveSubscribe: SubscribeResolver;
  private readonly sendMutation?: RawMutationSender;
  private handler: (qid: QueryId, ev: NormalizedEvent) => void = () => {};
  private readonly subs = new Map<QueryId, QState>();
  /** The client's own typed per-table schemas, for hello validation (CRIT#4); set by the backend. */
  private clientTables: NormalizedTableSchema[] | undefined;

  constructor(transport: Transport, opts: RemoteNormalizedSourceOptions = {}) {
    this.transport = transport;
    this.resolveSubscribe = opts.resolveSubscribe ?? defaultSubscribeTarget;
    this.sendMutation = opts.sendMutation;
    this.transport.onMessage((msg) => this.onServerMsg(msg));
  }

  expectClientSchema(tables: NormalizedTableSchema[]): void {
    this.clientTables = tables;
  }

  registerQuery(qid: QueryId, remote: RemoteQuery): void {
    this.subs.set(qid, { remote, subscriber: null, epoch: 0, resubscribing: false, subscribeTicket: 0, retryTimer: undefined, retryAttempt: 0 });
    this.subscribe(qid, remote);
  }

  unregisterQuery(qid: QueryId): void {
    const s = this.subs.get(qid);
    if (s?.retryTimer !== undefined) clearTimeout(s.retryTimer);
    this.subs.delete(qid);
    this.transport.send({ t: "unsubscribe", queryId: qid });
  }

  mutate(mutations: Mutation[]): Promise<void> {
    if (this.sendMutation) return Promise.resolve(this.sendMutation(mutations));
    this.transport.send({ t: "mutate", mutations });
    return Promise.resolve();
  }

  onNormalized(handler: (qid: QueryId, ev: NormalizedEvent) => void): void {
    this.handler = handler;
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
    const request = { queryId: qid, remote, mode: "normalized" as const };
    const ticket = ++s.subscribeTicket;
    const send = (target: SubscribeTarget) => {
      const cur = this.subs.get(qid);
      if (cur !== s || cur.subscribeTicket !== ticket) return;
      this.transport.send(subscribeMessage(request, target));
    };
    const fail = (err: unknown) => {
      const cur = this.subs.get(qid);
      if (cur !== s || cur.subscribeTicket !== ticket) return;
      s.resubscribing = false;
      console.error(
        `[rindle-remote] normalized query ${qid} subscribe resolution failed: ${String((err as Error)?.message ?? err)}`,
      );
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
    // This source is normalized-only; it sees `nhello`/`nbatch` (flat frames are ignored).
    if (msg.t !== "nhello" && msg.t !== "nbatch") return;
    const s = this.subs.get(msg.queryId);
    if (!s) return; // unsubscribed / unknown query
    if (msg.t === "nhello") this.openSubscriber(msg.queryId, s, msg.hello);
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
      console.error(`[rindle-remote] normalized query ${qid} subscription rejected: ${err.message}`);
      return;
    }
    if (s.retryTimer !== undefined) return; // a retry is already scheduled — don't stack them
    s.subscriber = null; // stop validating the dead epoch; recovery is a fresh seq-0 hydrate
    s.resubscribing = true;
    const delay = retryDelayMs(s.retryAttempt++, err.retryAfterMs);
    console.warn(
      `[rindle-remote] normalized query ${qid} ${err.code ?? "error"} (retryable): re-subscribing in ${delay}ms: ${err.message}`,
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

  private openSubscriber(qid: QueryId, s: QState, hello: NormalizedHello): void {
    try {
      s.subscriber = new NormalizedSubscriber(hello, (ev) => this.handler(qid, ev), this.clientTables);
      s.epoch = hello.epoch;
      s.resubscribing = false;
      s.retryAttempt = 0; // a successful hello resets the retryable-error backoff
    } catch (e) {
      // A comparator/fp mismatch at hello is unrecoverable (a code-contract divergence).
      s.subscriber = null;
      console.error(`[rindle-remote] normalized query ${qid} subscription rejected: ${(e as Error).message}`);
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
      // Gap / drift → re-hydrate under a new epoch (the server bumps it on re-subscribe).
      s.resubscribing = true;
      s.subscriber = null;
      this.subscribe(qid, s.remote);
    }
  }
}

/** Convenience: a `RemoteNormalizedSource` over a ws URL or a custom transport. */
export function createRemoteNormalizedSource(
  urlOrTransport: string | Transport,
  opts: RemoteNormalizedSourceOptions = {},
): RemoteNormalizedSource {
  const transport = typeof urlOrTransport === "string" ? new WsTransport(urlOrTransport) : urlOrTransport;
  return new RemoteNormalizedSource(transport, opts);
}
