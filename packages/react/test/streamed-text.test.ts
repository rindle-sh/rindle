// `useStreamedText` (designs/LM-STREAM-CHECKPOINT-DESIGN.md §10.7) — one test per TRAP, because the
// traps are the entire reason this hook exists rather than 20 lines in each app.
//
// Real React in jsdom, driven through `act`. The transport is injected: jsdom has no `EventSource`,
// and a fake one lets a test deliver frames at exact moments (including synchronously, which a real
// transport can do and which used to be a TDZ crash).

import { test } from "node:test";
import assert from "node:assert/strict";

import { JSDOM } from "jsdom";

const dom = new JSDOM('<!DOCTYPE html><html><body><div id="root"></div></body></html>');
const g = globalThis as Record<string, unknown>;
const win = dom.window as unknown as Record<string, unknown>;
const setGlobal = (k: string, v: unknown) => Object.defineProperty(g, k, { value: v, configurable: true, writable: true });
setGlobal("window", dom.window);
setGlobal("document", dom.window.document);
setGlobal("navigator", win.navigator);
for (const k of ["HTMLElement", "Element", "Node", "Text", "DocumentFragment", "Event", "customElements"]) {
  setGlobal(k, win[k]);
}
g.IS_REACT_ACT_ENVIRONMENT = true;

const { act, createElement: h } = await import("react");
const { createRoot } = await import("react-dom/client");
const { DEFAULT_STREAM_ENDPOINT, useStreamedText, streamSubscribeUrl, eventSourceTransport } = await import(
  "../src/index.ts"
);
import type { StreamFrame, StreamTransport } from "../src/index.ts";

/** A transport a test drives by hand. Records every subscribe URL, so "did it reconnect?" is an
 *  assertion rather than a guess. */
class FakeTransport implements StreamTransport {
  urls: string[] = [];
  detaches = 0;
  private emit?: (frame: StreamFrame) => void;
  /** Frames delivered synchronously from inside `subscribe`. */
  syncFrames: StreamFrame[] = [];

  subscribe(url: string, onFrame: (frame: StreamFrame) => void): () => void {
    this.urls.push(url);
    this.emit = onFrame;
    for (const f of this.syncFrames) onFrame(f);
    return () => {
      this.detaches++;
      this.emit = undefined;
    };
  }

  get attached(): boolean {
    return this.emit !== undefined;
  }

  send(frame: StreamFrame): void {
    this.emit?.(frame);
  }
}

interface Harness {
  render(props: { streamId: string; durable: string; live: boolean }): Promise<void>;
  unmount(): Promise<void>;
  readonly text: string;
}

async function mount(transport: StreamTransport, initial: { streamId: string; durable: string; live: boolean }): Promise<Harness> {
  const host = dom.window.document.getElementById("root")!;
  const root = createRoot(host);
  let text = "";

  function Probe(props: { streamId: string; durable: string; live: boolean }) {
    text = useStreamedText(props, { transport });
    return h("p", null, text);
  }

  const harness: Harness = {
    async render(props) {
      await act(async () => {
        root.render(h(Probe, props));
      });
    },
    async unmount() {
      await act(async () => {
        root.unmount();
      });
    },
    get text() {
      return text;
    },
  };
  await harness.render(initial);
  return harness;
}

// --------------------------------------------------------------------------- the contract

test("DEFAULT_STREAM_ENDPOINT pins the hand-mirrored server route", () => {
  // Mirrors DEFAULT_RINDLE_API_ROUTES.stream (`@rindle/api-server`), which the browser must never
  // import; each package pins the literal so either side drifting fails its own suite.
  assert.equal(DEFAULT_STREAM_ENDPOINT, "/api/rindle/stream");
});

test("splices the durable prefix with the live tail", async () => {
  const t = new FakeTransport();
  const h1 = await mount(t, { streamId: "m1", durable: "Hello", live: true });
  assert.equal(h1.text, "Hello", "before any frame, the durable text is the whole truth");
  assert.deepEqual(t.urls, [streamSubscribeUrl("/api/rindle/stream", "m1", 5)], "joins at the durable length");

  await act(async () => {
    t.send({ type: "open", streamId: "m1", from: 5, seq: 5, durableSeq: 5, ended: false });
    t.send({ type: "chunk", from: 5, seq: 11, text: ", world" });
  });
  assert.equal(h1.text, "Hello, world");
  await h1.unmount();
});

// --------------------------------------------------------------------------- TRAP 1

test("TRAP 1: an advancing durable text does NOT reconnect", async () => {
  const t = new FakeTransport();
  const h1 = await mount(t, { streamId: "m1", durable: "", live: true });
  assert.equal(t.urls.length, 1);

  await act(async () => {
    t.send({ type: "chunk", from: 0, seq: 6, text: "abcdef" });
  });
  // A checkpoint lands: the query re-renders with a longer durable text. That must not be a new
  // request — at 512 chars a checkpoint, it would be one reconnect per half-kilobyte.
  await h1.render({ streamId: "m1", durable: "abcdef", live: true });
  await h1.render({ streamId: "m1", durable: "abcdef", live: true });

  assert.equal(t.urls.length, 1, "still the original connection");
  assert.equal(t.detaches, 0);
  assert.equal(h1.text, "abcdef");
  await h1.unmount();
});

// --------------------------------------------------------------------------- TRAP 2

test("TRAP 2: the tail is seeded with the join prefix, so a late join is not discarded", async () => {
  const t = new FakeTransport();
  // Join at 9 characters — as a late joiner or a reload does.
  const h1 = await mount(t, { streamId: "m1", durable: "The quick", live: true });
  await act(async () => {
    t.send({ type: "chunk", from: 9, seq: 15, text: " brown" });
  });
  // Unseeded, the tail would be " brown" (6 chars) — SHORTER than the 9-char durable text, so the
  // length-based splice would throw it away and the UI would freeze at "The quick".
  assert.equal(h1.text, "The quick brown");
  await h1.unmount();
});

// --------------------------------------------------------------------------- TRAP 3

test("TRAP 3: a terminal frame detaches — no reconnect after end/stale/absent", async () => {
  for (const terminal of [
    { type: "end", seq: 4, status: "complete" } as StreamFrame,
    { type: "stale", floorSeq: 99, durableSeq: 99 } as StreamFrame,
    { type: "absent" } as StreamFrame,
  ]) {
    const t = new FakeTransport();
    const h1 = await mount(t, { streamId: "m1", durable: "", live: true });
    await act(async () => {
      t.send({ type: "chunk", from: 0, seq: 4, text: "done" });
      t.send(terminal);
    });
    assert.equal(t.detaches, 1, `${terminal.type} detached`);
    assert.equal(t.attached, false);
    // Frames after a terminal one are ignored rather than appended.
    await act(async () => {
      t.send({ type: "chunk", from: 4, seq: 8, text: "MORE" });
    });
    assert.equal(h1.text, "done", `${terminal.type}: nothing accumulates after the terminal frame`);
    await h1.unmount();
  }
});

test("a stale/absent reader keeps rendering the durable plane as it advances", async () => {
  const t = new FakeTransport();
  const h1 = await mount(t, { streamId: "m1", durable: "so far", live: true });
  await act(async () => {
    t.send({ type: "absent" });
  });
  // The live plane is gone; checkpoints still arrive through the query, and the text still grows.
  await h1.render({ streamId: "m1", durable: "so far and further", live: true });
  assert.equal(h1.text, "so far and further");
  await h1.unmount();
});

// --------------------------------------------------------------------------- TRAP 4

test("TRAP 4: switching stream mid-render never shows the previous message's text", async () => {
  const t = new FakeTransport();
  const h1 = await mount(t, { streamId: "m1", durable: "", live: true });
  await act(async () => {
    t.send({ type: "chunk", from: 0, seq: 46, text: "a very long first answer that goes on for a while" .slice(0, 46) });
  });
  assert.equal(h1.text.length, 46);

  // Now the component is pointed at a DIFFERENT, shorter message. If the tail were reset in an
  // effect instead of keyed during render, this render would splice the old 46-char tail over the
  // new 2-char durable text and show the previous answer.
  await h1.render({ streamId: "m2", durable: "hi", live: true });
  assert.equal(h1.text, "hi");
  assert.equal(t.urls.length, 2, "and it rejoined for the new stream");
  assert.match(t.urls[1], /streamId=m2&from=2/);
  await h1.unmount();
});

// --------------------------------------------------------------------------- TRAP 5

test("TRAP 5: a resumed replay that overlaps the accumulator is spliced, not appended", async () => {
  const t = new FakeTransport();
  const h1 = await mount(t, { streamId: "m1", durable: "", live: true });
  await act(async () => {
    t.send({ type: "chunk", from: 0, seq: 6, text: "abcdef" });
    t.send({ type: "durable", seq: 3 }); // the store trails the chunk progress — always
  });
  assert.equal(h1.text, "abcdef");

  // The connection drops. EventSource reconnects with Last-Event-ID = 3 (the durable frame's id —
  // the LAST id it saw), and the server replays everything after 3 as one coalesced chunk. Blindly
  // appending would render "abcdefdefghi".
  await act(async () => {
    t.send({ type: "chunk", from: 3, seq: 9, text: "defghi" });
  });
  assert.equal(h1.text, "abcdefghi", "the overlap is spliced at the frame's own offset");

  // A GAP (a chunk starting AHEAD of the accumulator) can only mean lost frames: detach and let the
  // durable plane carry the reader rather than rendering text with a hole in it.
  await act(async () => {
    t.send({ type: "chunk", from: 99, seq: 105, text: "zzzzzz" });
  });
  assert.equal(h1.text, "abcdefghi", "a gapped chunk is never rendered");
  assert.equal(t.attached, false, "the gap detached the reader");
  await h1.unmount();
});

// --------------------------------------------------------------------------- lifecycle

test("attaches only while live, and detaches on unmount", async () => {
  const t = new FakeTransport();
  const h1 = await mount(t, { streamId: "m1", durable: "", live: false });
  assert.deepEqual(t.urls, [], "a settled message opens no connection");

  await h1.render({ streamId: "m1", durable: "", live: true });
  assert.equal(t.urls.length, 1);
  await act(async () => {
    t.send({ type: "chunk", from: 0, seq: 2, text: "hi" });
  });

  // The seal arrives through the query (status → complete): the hook lets go, and the compacted body
  // renders identically — the handoff is invisible.
  await h1.render({ streamId: "m1", durable: "hi", live: false });
  assert.equal(t.detaches, 1);
  assert.equal(h1.text, "hi");

  await h1.unmount();
  assert.equal(t.detaches, 1, "already detached; unmount does not double-count");
});

test("a transport that completes SYNCHRONOUSLY inside subscribe is handled", async () => {
  // `detach` is assigned by `subscribe` itself, so a synchronous terminal frame used to reach the
  // detach reference before it existed.
  const t = new FakeTransport();
  t.syncFrames = [{ type: "chunk", from: 0, seq: 3, text: "syn" }, { type: "absent" }];
  const h1 = await mount(t, { streamId: "m1", durable: "", live: true });
  assert.equal(h1.text, "syn");
  assert.equal(t.detaches, 1, "the synchronous terminal frame detached exactly once");
  await h1.unmount();
});

// --------------------------------------------------------------------------- the default transport

test("the default transport is inert without EventSource, and decodes frames with it", async () => {
  // Absent `EventSource` (SSR, an old runtime): attach nothing, stay on the durable plane.
  assert.equal(g.EventSource, undefined);
  const inert = eventSourceTransport();
  const detachInert = inert.subscribe("/x", () => assert.fail("no frames without EventSource"));
  detachInert();

  // With one, frames are JSON-decoded and a bad payload reaches `onError` rather than throwing.
  const seen: StreamFrame[] = [];
  const errors: unknown[] = [];
  let closed = 0;
  let live: FakeES | undefined;
  class FakeES {
    onmessage: ((e: { data: string }) => void) | null = null;
    onerror: ((e: unknown) => void) | null = null;
    readonly url: string;
    constructor(url: string) {
      this.url = url;
      live = this;
    }
    close() {
      closed++;
    }
  }
  setGlobal("EventSource", FakeES);
  try {
    const transport = eventSourceTransport((e) => errors.push(e));
    const detach = transport.subscribe("/api/rindle/stream?streamId=m1&from=0", (f) => seen.push(f));
    assert.equal(live?.url, "/api/rindle/stream?streamId=m1&from=0");
    live!.onmessage?.({ data: JSON.stringify({ type: "chunk", from: 0, seq: 2, text: "ok" }) });
    live!.onmessage?.({ data: "not json" });
    live!.onerror?.({ kind: "network" });
    detach();
    assert.deepEqual(seen, [{ type: "chunk", from: 0, seq: 2, text: "ok" }]);
    assert.equal(errors.length, 2, "the bad frame and the transport error both surfaced");
    assert.equal(closed, 1);
  } finally {
    Object.defineProperty(g, "EventSource", { value: undefined, configurable: true, writable: true });
  }
});
