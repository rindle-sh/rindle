// Retryable-vs-terminal `queryError` handling (101-QUERY-ERRORS-DESIGN.md §5, the client half
// of FOLLOWER-LAG-SHED-DESIGN.md §6.3 — the load-bearing contract fix).
//
// A `queryError` used to be terminal on every source: `subs.delete(queryId)`, no re-subscribe,
// so a server-side worker fault or a follower shedding load STRANDED the subscription forever
// (only a transport reconnect or a seq gap ever re-subscribed). Per 101 §5 the server now
// classifies: a frame carrying `retryable: true` (a shed follower's `code:"shed"`, a worker
// fault's `code:"faulted"`, a lease expiry) means the subscription is still WANTED and the
// server expects the client to come back — with jittered backoff honoring `retryAfterMs`.
//
// Rows already folded downstream are deliberately NOT cleared on a retryable error (101 §6 —
// row lifecycle stays server-driven): the degraded UX is a frozen but fully-rendered consistent
// view, the optimistic layer still absorbing local writes, then a fresh seq-0 hydrate on
// recovery. An absent/false `retryable` keeps today's terminal behavior.

/** The wire shape of a `queryError` frame (the optional fields ride 101 §4's reason shape). */
export interface QueryErrorFields {
  message: string;
  /** A machine code (`"shed"`, `"faulted"`, …); absent from older servers. */
  code?: string;
  /** True ⇒ schedule a re-subscribe; absent/false ⇒ terminal (today's behavior). */
  retryable?: boolean;
  /** The server's suggested floor for the first retry delay. */
  retryAfterMs?: number;
}

/** Cap on the exponential backoff between re-subscribe attempts. */
const MAX_RETRY_DELAY_MS = 30_000;

/** Default first-attempt delay when the server suggests none. */
const DEFAULT_RETRY_DELAY_MS = 500;

/** The `attempt`-th (0-based) re-subscribe delay: exponential from the server's suggested
 *  `retryAfterMs` (or 500 ms when it suggests none), capped at 30 s, with up-to-25 % upward
 *  jitter so a shed follower's re-admission gate is not hit by a thundering herd of
 *  synchronized retries. Never returns less than the server's suggestion — the floor is
 *  honored exactly, and the server picked it knowing its own re-admission rate. */
export function retryDelayMs(attempt: number, retryAfterMs: number | undefined): number {
  const base = Math.max(retryAfterMs ?? DEFAULT_RETRY_DELAY_MS, 1);
  const backoff = Math.min(base * 2 ** attempt, MAX_RETRY_DELAY_MS);
  return Math.round(backoff * (1 + Math.random() * 0.25));
}
