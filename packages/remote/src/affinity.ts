// Browser-side follower-affinity ticket transport (FOLLOWER-AFFINITY-DESIGN.md §3, §5). The ticket
// is OPAQUE here — the browser never mints or verifies it (placement, not authorization; §3, §9),
// so this module carries no crypto. It is the thin seam three pieces share:
//
//   - {@link WsTransport} offers the held ticket as a ws subprotocol at each connect so the fleet
//     edge selects the pinned follower;
//   - {@link RemoteOptimisticSource} writes the follower's minted ticket from the `{t:"affinity"}`
//     frame, and CLEARS it on a sustained outage (the pinned follower is gone → the next reconnect
//     anycasts to a live one and re-pins, §8);
//   - the lease POST forwards the held ticket as `affinity`, sequenced AFTER a fresh connect's mint
//     frame ({@link AffinityTicketStore.waitForTicket}) so both legs land on the same follower (§4.1).

/** The base ws subprotocol the browser offers alongside its `aff.*` ticket; the fleet follower
 *  echoes it on the 101 (a strict browser closes a socket whose selected subprotocol it never
 *  offered, so the base is always offered in affinity mode). Inlined — NOT imported from
 *  `@rindle/affinity`'s `WS_SUBPROTOCOL` — to keep that crate's `node:crypto` out of the browser
 *  bundle (the same duplicate-to-stay-bundle-clean discipline the lease wire types use). Keep the
 *  two spellings in lock-step. */
export const WS_SUBPROTOCOL = "rindle.v1";

/** A mutable holder for the current opaque affinity ticket, shared by the transport, the source,
 *  and the lease POST (see the module header). */
export interface AffinityTicketStore {
  /** The ticket to offer on the next ws handshake, or undefined — ticketless, so the edge
   *  anycasts + mints a fresh one. A persisted/previous-connection ticket may be returned here
   *  even while it is NOT yet safe for a lease; see {@link leaseTicket}. */
  get(): string | undefined;
  /** Record a freshly minted/refreshed ticket (from a connection's `{t:"affinity"}` frame). */
  set(ticket: string): void;
  /** Drop the ticket so the next connect goes ticketless (the pinned follower is gone, §8). */
  clear(): void;
  /** Mark the held ticket handshake-only until this connection emits a fresh mint frame. Called
   *  before the first connection and every reconnect, after the held ticket was already selected
   *  for the ws subprotocol offer. This prevents a restored/rotated/expired persisted ticket from
   *  racing an HTTP lease onto an independently-anycast follower. */
  connectionPending(): void;
  /** Resolve with a ticket minted/refreshed on the CURRENT connection, or the next one
   *  {@link set}. A persisted ticket deliberately does not resolve this wait. */
  waitForTicket(): Promise<string>;
  /** Obtain the current connection's ticket for an HTTP lease. The first missing-ticket wait is
   *  bounded; its timeout removes the waiter and latches ticketless fallback, so later leases
   *  return immediately until a mint frame arrives. Concurrent callers share the one timer.
   *  `timedOut` is true only for that transition, allowing one warning per fallback episode. */
  leaseTicket(timeoutMs: number): Promise<{ ticket?: string; timedOut: boolean }>;
}

/** Where a store persists the ticket across reloads. The browser backs this with sessionStorage
 *  (per-TAB, so two tabs can pin two regions — design §13); tests pass none (pure in-memory). */
export interface TicketPersistence {
  load(): string | undefined;
  save(ticket: string): void;
  clear(): void;
}

/** Build an {@link AffinityTicketStore}, optionally persisted. Pure/in-memory when `persist` is
 *  omitted. */
export function createAffinityTicketStore(persist?: TicketPersistence): AffinityTicketStore {
  let current = persist?.load();
  // A loaded ticket is useful for the ws handshake, but is not lease-safe until the follower on
  // THIS connection confirms it by minting a fresh frame. The same distinction is re-armed on
  // every reconnect via `connectionPending`.
  let currentConnectionFresh = false;
  let ticketlessFallback = false;
  let waiters: Array<(ticket: string) => void> = [];
  let leaseWait:
    | {
        promise: Promise<{ ticket?: string; timedOut: boolean }>;
        resolve: (result: { ticket?: string; timedOut: boolean }) => void;
        timer: ReturnType<typeof setTimeout>;
      }
    | undefined;

  const finishLeaseWait = (result: { ticket?: string; timedOut: boolean }): void => {
    const pending = leaseWait;
    if (!pending) return;
    clearTimeout(pending.timer);
    leaseWait = undefined;
    pending.resolve(result);
  };

  return {
    get: () => current,
    set: (ticket) => {
      current = ticket;
      currentConnectionFresh = true;
      ticketlessFallback = false;
      persist?.save(ticket);
      // Resolve everyone waiting for the (now-arrived) ticket, in FIFO order.
      const pending = waiters;
      waiters = [];
      for (const resolve of pending) resolve(ticket);
      finishLeaseWait({ ticket, timedOut: false });
    },
    clear: () => {
      // Drop the ticket but leave any waiters pending — the next connect's mint frame resolves them
      // (a dead-follower reassignment must still be able to lease once the new ticket arrives).
      current = undefined;
      currentConnectionFresh = false;
      persist?.clear();
    },
    connectionPending: () => {
      currentConnectionFresh = false;
    },
    waitForTicket: () =>
      currentConnectionFresh && current !== undefined
        ? Promise.resolve(current)
        : new Promise<string>((resolve) => waiters.push(resolve)),
    leaseTicket: (timeoutMs) => {
      if (currentConnectionFresh && current !== undefined) {
        return Promise.resolve({ ticket: current, timedOut: false });
      }
      if (ticketlessFallback) return Promise.resolve({ timedOut: false });
      if (leaseWait) return leaseWait.promise;

      let resolve!: (result: { ticket?: string; timedOut: boolean }) => void;
      const promise = new Promise<{ ticket?: string; timedOut: boolean }>((r) => {
        resolve = r;
      });
      const timer = setTimeout(() => {
        // Clear the shared waiter before resolving callers. A later mint starts a new, fresh
        // episode; no abandoned resolver accumulates per lease while affinity is off.
        leaseWait = undefined;
        ticketlessFallback = true;
        resolve({ timedOut: true });
      }, Math.max(0, timeoutMs));
      leaseWait = { promise, resolve, timer };
      return promise;
    },
  };
}

/** The subprotocol list to offer for one connection: the base protocol always, plus the held ticket
 *  (already an `aff.<…>` token) when one exists. */
export function offerSubprotocols(store: AffinityTicketStore): string[] {
  const ticket = store.get();
  return ticket ? [WS_SUBPROTOCOL, ticket] : [WS_SUBPROTOCOL];
}
