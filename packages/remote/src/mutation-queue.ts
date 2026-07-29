// The client-side mutation queue for daemon/serverless deployments. The optimistic engine
// requires mids to arrive at the daemon CONTIGUOUSLY (`mid == lmid + 1`); a websocket gives
// that ordering for free, but the serverless hop (client → API server over HTTP → daemon)
// does not — two parallel fetches can reorder. The queue restores the invariant the cheap
// way: envelopes are sent as in-order batches, and the next batch does not leave until the
// prior one is confirmed (the API server's response means the daemon durably processed it).
//
// Delivery is at-least-once: a failed flush retries the SAME batch with backoff, which is
// safe because the daemon absorbs `mid ≤ lmid` as an idempotent replay. A per-envelope
// REJECTION (the API server's policy saying no) is not retried — the daemon has already
// advanced lmid past it, the authoritative snap-back rides the lmid release, and the reason
// surfaces here through `onRejected` (the wire itself carries no rejection signal).

import type { MutationEnvelope } from "@rindle/client";

import type { MutationEnvelopeSender } from "./subscribe.ts";

/** One envelope's outcome from the API server's mutate endpoint. */
export interface PushOutcome {
  accepted: boolean;
  reason?: string;
}

export interface QueuedMutationSenderOptions {
  /** Deliver one in-order batch; resolve once the daemon durably processed it (i.e. the
   *  API server awaited its daemon call before responding). Throw to have the SAME batch
   *  retried. Return per-envelope outcomes, or void when everything was accepted. */
  send: (envelopes: MutationEnvelope[]) => Promise<PushOutcome[] | void>;
  /** A policy rejection for one envelope (never retried; the prediction snaps back via the
   *  lmid release — this callback is where the reason reaches the app). */
  onRejected?: (envelope: MutationEnvelope, reason: string) => void;
  /** A failed flush attempt, before its retry (observability). */
  onError?: (err: unknown, attempt: number) => void;
  /** Backoff before retry `attempt` (1-based). Default: 200ms · 2^(attempt-1), capped at 5s. */
  retryDelayMs?: (attempt: number) => number;
  /** Max envelopes per flush. Default 32. */
  maxBatch?: number;
}

interface Pending {
  envelope: MutationEnvelope;
  /** Resolves when the envelope's batch is confirmed (accepted OR rejected — both are
   *  durably processed states). */
  resolve: () => void;
}

const defaultDelay = (attempt: number): number => Math.min(200 * 2 ** (attempt - 1), 5_000);

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** A `MutationEnvelopeSender` (the `pushMutation` option of `RemoteOptimisticSource`) that
 *  queues envelopes and delivers them as confirmed, in-order batches. */
export function createQueuedMutationSender(opts: QueuedMutationSenderOptions): MutationEnvelopeSender {
  const queue: Pending[] = [];
  const maxBatch = Math.max(1, opts.maxBatch ?? 32);
  const delay = opts.retryDelayMs ?? defaultDelay;
  let flushing = false;

  const flush = async (): Promise<void> => {
    if (flushing) return;
    flushing = true;
    try {
      while (queue.length > 0) {
        const batch = queue.slice(0, maxBatch);
        let outcomes: PushOutcome[] | void;
        for (let attempt = 1; ; attempt++) {
          try {
            outcomes = await opts.send(batch.map((p) => p.envelope));
            break;
          } catch (err) {
            opts.onError?.(err, attempt);
            await sleep(delay(attempt));
          }
        }
        queue.splice(0, batch.length);
        batch.forEach((pending, i) => {
          const outcome = Array.isArray(outcomes) ? outcomes[i] : undefined;
          if (outcome && !outcome.accepted) {
            opts.onRejected?.(pending.envelope, outcome.reason ?? "mutation rejected");
          }
          pending.resolve();
        });
      }
    } finally {
      flushing = false;
    }
    // Envelopes enqueued during the final await: a new flush owns them.
    if (queue.length > 0) void flush();
  };

  return (envelope) =>
    new Promise<void>((resolve) => {
      queue.push({ envelope, resolve });
      void flush();
    });
}
