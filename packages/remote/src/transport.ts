// The transport seam: the RemoteBackend talks JSON messages through a `Transport`, so the
// wire (ws / sse / http) is swappable + mockable. The default {@link WsTransport} uses the
// global `WebSocket` (Node 22+ and browsers both provide it — zero dependency).

import type { ClientMsg, ServerMsg } from "./protocol.ts";

export interface Transport {
  /** Send a message up to the server. */
  send(msg: ClientMsg): void;
  /** Register the single handler for incoming server messages. */
  onMessage(handler: (msg: ServerMsg) => void): void;
  /** Register a handler fired after the connection is RE-established (not the first open) — the
   *  source uses it to re-`init` + re-subscribe so a dropped/restarted daemon heals. Optional:
   *  transports without reconnect (mocks, in-process) may omit it. */
  onReconnect?(handler: () => void): void;
  /** Register a handler fired when the connection is SUSTAINEDLY down — repeated reconnects to the
   *  same endpoint have failed (a dead/removed follower, READ-ROUTER-DESIGN.md §3). The source uses
   *  it to re-lease: the router returns a (possibly new) `wsEndpoint`, and a changed one migrates the
   *  whole session off the dead node. Optional: transports without failover (mocks, in-process,
   *  fixed endpoints) may omit it. */
  onDown?(handler: () => void): void;
  /** Tear down the connection. */
  close(): void;
}

/** A `Transport` over a `WebSocket` (text JSON frames). Messages sent before the socket
 *  opens are buffered and flushed on open (so `registerQuery`/`mutate` can be called eagerly).
 *  Reconnects with capped exponential backoff: if the socket drops (e.g. the daemon restarted)
 *  it reopens and fires `onReconnect` so the source rebuilds its subscriptions. */
export class WsTransport implements Transport {
  private readonly url: string;
  /** Reads the subprotocols to offer at each (re)connect — in affinity mode, `["rindle.v1", "aff.…"]`
   *  with the CURRENT ticket (FOLLOWER-AFFINITY-DESIGN.md §5). Undefined ⇒ offer none (today's
   *  single-daemon behavior, byte-identical). Evaluated per connect so a reconnect presents the
   *  freshest (or freshly cleared) ticket. */
  private readonly subprotocols?: () => string[];
  private ws: WebSocket;
  private handler: (msg: ServerMsg) => void = () => {};
  private reconnectHandler: () => void = () => {};
  private downHandler: () => void = () => {};
  /** Buffered as PRE-SERIALIZED frames: serialization happens at `send` time so an
   *  unserializable message (a `bigint` query arg — `JSON.stringify` throws on bigint)
   *  throws typed INTO ITS CALLER instead of detonating later inside the socket's
   *  `open` listener, where it would strand every frame queued behind it. */
  private readonly pending: string[] = [];
  private open = false;
  private everOpened = false;
  private closedByUser = false;
  private attempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  /** Failed reconnect attempts after which the connection is declared "down" (fires `onDown`). */
  private readonly downThreshold: number;
  /** True once `onDown` has fired for the CURRENT down episode; reset on the next successful open
   *  so a later outage fires again (but a single episode fires `onDown` exactly once — no re-lease
   *  storm while a follower is gone). */
  private downFired = false;

  constructor(url: string, opts: { downThreshold?: number; subprotocols?: () => string[] } = {}) {
    this.url = url;
    this.downThreshold = opts.downThreshold ?? 4;
    this.subprotocols = opts.subprotocols;
    this.ws = this.connect();
  }

  private connect(): WebSocket {
    // Offer the affinity subprotocols (base + current ticket) when configured; otherwise open bare,
    // exactly as before. An empty list is treated as bare (never send `Sec-WebSocket-Protocol: `).
    const protocols = this.subprotocols?.();
    const ws = protocols && protocols.length > 0 ? new WebSocket(this.url, protocols) : new WebSocket(this.url);
    ws.addEventListener("open", () => {
      this.open = true;
      this.attempt = 0;
      this.downFired = false; // a fresh connection clears the down episode
      if (!this.everOpened) {
        // First connection: flush whatever was buffered eagerly (init + subscribes).
        // Frames were serialized at `send` time, so this loop cannot throw.
        this.everOpened = true;
        for (const m of this.pending) ws.send(m);
        this.pending.length = 0;
      } else {
        // A reconnect: drop any stale buffered frames — the source rebuilds the full desired
        // state (re-init + re-subscribe, re-leasing as it goes) in onReconnect.
        this.pending.length = 0;
        this.reconnectHandler();
      }
    });
    ws.addEventListener("message", (ev: MessageEvent) => {
      this.handler(JSON.parse(typeof ev.data === "string" ? ev.data : String(ev.data)) as ServerMsg);
    });
    ws.addEventListener("close", () => {
      this.open = false;
      if (!this.closedByUser) this.scheduleReconnect();
    });
    // `error` is followed by `close`; let the close handler own reconnection.
    ws.addEventListener("error", () => {});
    return ws;
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer !== undefined) return;
    const delay = Math.min(250 * 2 ** this.attempt, 5000);
    this.attempt++;
    // Sustained failure (we had a connection, and several reconnects to this endpoint have failed):
    // declare the connection down ONCE so the source can re-lease and migrate (§3). We keep
    // reconnecting underneath in case the same follower returns (a reboot, same endpoint).
    if (this.everOpened && !this.downFired && this.attempt >= this.downThreshold) {
      this.downFired = true;
      this.downHandler();
    }
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      if (!this.closedByUser) this.ws = this.connect();
    }, delay);
  }

  send(msg: ClientMsg): void {
    // Serialize HERE, open or not: a frame the wire cannot carry (a `bigint` query
    // arg on the live-query plane — the browser bigint lane ships with design 226
    // Stage E) must throw typed into its caller, never poison the pending queue or
    // the socket's `open` flush.
    let text: string;
    try {
      text = JSON.stringify(msg);
    } catch (e) {
      throw new Error(
        "query args must be JSON-serializable: bigint values are not supported on the " +
          "live-query wire until the browser bigint lane ships (design 226) — " +
          `${e instanceof Error ? e.message : String(e)}`,
      );
    }
    if (this.open) this.ws.send(text);
    else this.pending.push(text);
  }

  onMessage(handler: (msg: ServerMsg) => void): void {
    this.handler = handler;
  }

  onReconnect(handler: () => void): void {
    this.reconnectHandler = handler;
  }

  onDown(handler: () => void): void {
    this.downHandler = handler;
  }

  close(): void {
    this.closedByUser = true;
    if (this.reconnectTimer !== undefined) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.ws.close();
  }
}
