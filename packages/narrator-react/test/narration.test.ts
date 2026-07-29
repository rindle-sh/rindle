// useNarration end-to-end with REAL React (mount + effects in jsdom): a live query's change stream
// is narrated into a buffer drained by `take()`. Proves the wiring — materialize(onChanges) → narrate
// → buffer — plus the batch-only default (the snapshot is ignored) and the netted rebase no-op.
//
// Node strips TS types but not JSX, so this uses `createElement` (`h`) directly.

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

const { act, Component, createElement: h, useState } = await import("react");
const { createRoot } = await import("react-dom/client");

const { createSchema, newQueryBuilder, number, Store, string, table } = await import("@rindle/client");
import type { Backend, ChangeEvent, FlatChange, QueryId, WireSchema } from "@rindle/client";

const { Rindle } = await import("@rindle/react");
const { useNarration } = await import("../src/index.ts");
import type { Narration } from "../src/index.ts";
import type { NarratorRegistry } from "@rindle/narrator";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const qb = newQueryBuilder(schema);
const issuesQuery = qb.issue.orderBy("id", "asc");

const issueWire: WireSchema = { columns: ["id", "title"], primaryKey: [0], sort: [[0, true]], singular: false, relationships: [] };
const add = (id: number, title: string): FlatChange => ({ path: [], op: { tag: "add", node: { row: [id, title], rels: [] } } });
const remove = (id: number, title: string): FlatChange => ({ path: [], op: { tag: "remove", row: [id, title] } });

class FakeBackend implements Backend {
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  readonly registered: QueryId[] = [];
  snapshot: FlatChange[] = [];
  registerQuery(qid: QueryId): void {
    this.registered.push(qid);
    this.handler(qid, { type: "hello", schema: issueWire, comparatorVersion: 1 });
    this.handler(qid, { type: "snapshot", adds: this.snapshot, last: true });
  }
  unregisterQuery(): void {}
  mutate(): Promise<void> {
    return Promise.resolve();
  }
  onEvent(handler: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = handler;
  }
  emit(qid: QueryId, ev: ChangeEvent): void {
    this.handler(qid, ev);
  }
}

const REGISTRY: NarratorRegistry = {
  issues: {
    salience: "info",
    root: {
      add: ({ row }) => `Added issue "${row.title as string}".`,
      remove: ({ row }) => `Removed issue "${row.title as string}".`,
    },
  },
};

let handle: Narration | null = null;
function Probe() {
  handle = useNarration(issuesQuery, REGISTRY, { as: "issues" });
  return h("div", null, "probe");
}

test("useNarration buffers narrated batches (snapshot ignored), drained by take(), netted across a rebase", async () => {
  const be = new FakeBackend();
  be.snapshot = [add(1, "one")]; // initial state — must NOT be narrated (default phases = batch)
  const store = new Store(schema, be);
  const root = createRoot(document.getElementById("root")!);

  await act(async () => {
    root.render(h(Rindle, { store }, h(Probe)));
  });

  // The snapshot is the current state, not a change — nothing buffered.
  assert.deepEqual(handle!.take(), []);

  const qid = be.registered[0];

  // A real write narrates.
  await act(async () => {
    be.emit(qid, { type: "batch", events: [add(2, "two")] });
  });
  const events = handle!.take();
  assert.equal(events.length, 1);
  assert.equal(events[0].text, 'Added issue "two".');
  assert.equal(events[0].salience, "info");
  assert.deepEqual(handle!.take(), [], "take() drained the buffer");

  // A rebase that reproduces a still-pending prediction nets to nothing → no narration.
  await act(async () => {
    be.emit(qid, { type: "batch", events: [remove(2, "two"), add(2, "two")] });
  });
  assert.deepEqual(handle!.take(), [], "a no-op rebase cycle narrates nothing");

  await act(async () => {
    root.unmount();
  });
});

test("useNarration throws on an ad-hoc (unnamed) query with no `as` — never silently narrates nothing", async () => {
  const be = new FakeBackend();
  const store = new Store(schema, be);
  const root = createRoot(document.getElementById("root")!);
  let caught: unknown;
  class Boundary extends Component<{ children: unknown }, { failed: boolean }> {
    state = { failed: false };
    static getDerivedStateFromError() {
      return { failed: true };
    }
    componentDidCatch(e: unknown) {
      caught = e;
    }
    render() {
      return this.state.failed ? h("div", null, "err") : (this.props.children as never);
    }
  }
  // issuesQuery is `qb.issue.orderBy(...)` — an ad-hoc builder query with no name. Without `{ as }`
  // the registry key would be "" (silent no-op); the hook must throw instead.
  function BadProbe() {
    useNarration(issuesQuery, REGISTRY);
    return h("div", null, "bad");
  }
  await act(async () => {
    root.render(h(Rindle, { store }, h(Boundary, null, h(BadProbe))));
  });
  assert.ok(caught instanceof Error && /as/.test((caught as Error).message), "threw a helpful error naming `as`");
  await act(async () => {
    root.unmount();
  });
});

test("useNarration clears its buffer on re-subscribe (a dep-identity change does not replay stale events)", async () => {
  const be = new FakeBackend();
  be.snapshot = [add(1, "one")];
  const store = new Store(schema, be);
  const root = createRoot(document.getElementById("root")!);

  // Two same-content registries with DISTINCT identities; swapping to the second changes the hook's
  // `narrator` dep and forces the effect to re-subscribe (the clean twin of a StrictMode double-mount).
  const registries: NarratorRegistry[] = [REGISTRY, { ...REGISTRY }];
  let swap: (gen: number) => void = () => {};
  function Probe2() {
    const [gen, setGen] = useState(0);
    swap = setGen;
    handle = useNarration(issuesQuery, registries[gen], { as: "issues" });
    return h("div", null, "probe2");
  }

  await act(async () => {
    root.render(h(Rindle, { store }, h(Probe2)));
  });
  const qid = be.registered[0];

  // Buffer a real change but do NOT drain it.
  await act(async () => {
    be.emit(qid, { type: "batch", events: [add(2, "two")] });
  });

  // Force a re-subscribe: the effect cleanup must clear the buffer, and the re-subscribe's snapshot is
  // not narrated (default phases = batch), so the buffer is empty afterward.
  await act(async () => {
    swap(1);
  });
  assert.deepEqual(handle!.take(), [], "the pre-resubscribe event was cleared, not replayed");

  await act(async () => {
    root.unmount();
  });
});

test("useNarration ignores a catchUp (hydration) batch by default, just like a snapshot", async () => {
  const be = new FakeBackend();
  const store = new Store(schema, be);
  const root = createRoot(document.getElementById("root")!);

  await act(async () => {
    root.render(h(Rindle, { store }, h(Probe)));
  });
  const qid = be.registered[0];

  // A catch-up batch is the initial hydration delivered as a delta (optimistic backend) → phased as a
  // snapshot → the default "what CHANGED" narrator ignores it, rather than narrating every row.
  await act(async () => {
    be.emit(qid, { type: "batch", events: [add(1, "one"), add(2, "two")], catchUp: true });
  });
  assert.deepEqual(handle!.take(), [], "the hydration set is not narrated as changes");

  // A subsequent normal batch still narrates.
  await act(async () => {
    be.emit(qid, { type: "batch", events: [add(3, "three")] });
  });
  assert.equal(handle!.take().length, 1, "a real later change still narrates");

  await act(async () => {
    root.unmount();
  });
});
