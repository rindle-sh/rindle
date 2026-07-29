// SSR render → hydrate, with REAL React (renderToString + hydrateRoot in jsdom). Proves the
// client SSR contract (SSR-DESIGN.md §6) at the React layer:
//   1. renderToString reads `getServerSnapshot` (the seed) — never opens a subscription;
//   2. the client's hydration pass renders byte-identical markup (no hydration mismatch);
//   3. after hydration the live backend's first `hello` reconciles the view in the DOM.
//
// Node strips TS types but not JSX, so this uses `createElement` (`h`) directly.

import { test } from "node:test";
import assert from "node:assert/strict";

import { JSDOM } from "jsdom";

// jsdom globals must exist before react-dom/client touches the document.
const dom = new JSDOM('<!DOCTYPE html><html><body><div id="root"></div></body></html>');
const g = globalThis as Record<string, unknown>;
const win = dom.window as unknown as Record<string, unknown>;
// Some globals (e.g. `navigator`) are getter-only on globalThis — define rather than assign.
const setGlobal = (k: string, v: unknown) => Object.defineProperty(g, k, { value: v, configurable: true, writable: true });
setGlobal("window", dom.window);
setGlobal("document", dom.window.document);
setGlobal("navigator", win.navigator);
for (const k of ["HTMLElement", "Element", "Node", "Text", "DocumentFragment", "Event", "customElements"]) {
  setGlobal(k, win[k]);
}
g.IS_REACT_ACT_ENVIRONMENT = true;

const { act, createElement: h } = await import("react");
const { renderToString } = await import("react-dom/server");
const { hydrateRoot } = await import("react-dom/client");

const { createSchema, defineFragment, defineQuery, newQueryBuilder, number, OneShotBackend, stableKey, Store, string, table } = await import(
  "@rindle/client"
);
import type { Backend, ChangeEvent, FlatChange, FragmentRef, Mutation, QueryId, RemoteQuery, WireSchema } from "@rindle/client";

const { Rindle, RindleSSR, useFragment, useQuery, useRoot } = await import("../src/index.ts");

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const qb = newQueryBuilder(schema);
const issuesQuery = qb.issue.orderBy("id", "asc");
const IssueFragment = defineFragment(issue, (f) => f.select("id", "title"));
const issueFragments = defineQuery("issueFragments", () => qb.issue.orderBy("id", "asc").include(IssueFragment));
const issueFragmentsQuery = issueFragments();

// A list component reading the query — the same `useQuery` on the server and the client.
function IssueList() {
  const issues = useQuery(issuesQuery) as Array<{ id: number; title: string }>;
  return h(
    "ul",
    null,
    issues.map((it) => h("li", { key: it.id }, it.title)),
  );
}

function FragmentIssue({ issue: issueRef }: { issue: FragmentRef<typeof IssueFragment> }) {
  const data = useFragment(IssueFragment, issueRef);
  return h("li", null, data?.title ?? "loading");
}

function FragmentIssueList() {
  const [issues] = useRoot(issueFragmentsQuery, IssueFragment);
  return h(
    "ul",
    null,
    issues.map((it) => h(FragmentIssue, { key: stableKey(it.pk), issue: it })),
  );
}

const app = (store: InstanceType<typeof Store>) => h(Rindle, { store }, h(IssueList));

// A backend whose hello/snapshot is deferred until `goLive()` — so a hydrated view shows its
// seed until live data lands (the browser story).
class DeferredBackend implements Backend {
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  private pending: QueryId[] = [];
  snapshot: FlatChange[] = [];
  helloSchema: WireSchema = {
    columns: ["id", "title"],
    primaryKey: [0],
    sort: [[0, true]],
    singular: false,
    relationships: [],
  };
  registerQuery(qid: QueryId, _ast: unknown, _remote?: RemoteQuery): void {
    this.pending.push(qid);
  }
  unregisterQuery(_qid: QueryId): void {}
  mutate(_m: Mutation[]): Promise<void> {
    return Promise.resolve();
  }
  onEvent(handler: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = handler;
  }
  goLive(): void {
    for (const qid of this.pending) {
      this.handler(qid, { type: "hello", schema: this.helloSchema, comparatorVersion: 1 });
      this.handler(qid, { type: "snapshot", adds: this.snapshot, last: true });
    }
    this.pending = [];
  }
}

const SEED = { [stableKey(issuesQuery.ast())]: { rows: [{ id: 1, title: "bug" }, { id: 2, title: "lonely" }], cvMin: 7 } };
const FRAGMENT_SEED = {
  [stableKey(issueFragmentsQuery.ast())]: { rows: [{ id: 1, title: "bug" }, { id: 2, title: "lonely" }], cvMin: 7 },
};

test("renderToString reads the seed (getServerSnapshot) and emits the SSR markup", () => {
  const server = new Store(schema, new OneShotBackend());
  server.hydrate(SEED);

  const html = renderToString(app(server));
  assert.match(html, /<li>bug<\/li>/);
  assert.match(html, /<li>lonely<\/li>/);
});

test("renderToString reads root refs and fragment row seeds", () => {
  const server = new Store(schema, new OneShotBackend());
  server.hydrate(FRAGMENT_SEED);

  const html = renderToString(h(Rindle, { store: server }, h(FragmentIssueList)));
  assert.match(html, /<li>bug<\/li>/);
  assert.match(html, /<li>lonely<\/li>/);
  assert.doesNotMatch(html, /loading/);
});

test("fragment hydration keeps seeded markup until live data arrives", async () => {
  const server = new Store(schema, new OneShotBackend());
  server.hydrate(FRAGMENT_SEED);
  const serverHtml = renderToString(h(Rindle, { store: server }, h(FragmentIssueList)));

  const backend = new DeferredBackend();
  const browser = new Store(schema, backend);
  browser.hydrate(JSON.parse(JSON.stringify(server.dehydrate())));

  const container = dom.window.document.getElementById("root")!;
  container.innerHTML = serverHtml;

  let root: ReturnType<typeof hydrateRoot>;
  await act(async () => {
    root = hydrateRoot(container, h(Rindle, { store: browser }, h(FragmentIssueList)));
  });

  assert.deepStrictEqual(
    [...container.querySelectorAll("li")].map((li) => li.textContent),
    ["bug", "lonely"],
    "hydration keeps the fragment seed instead of flashing loading/empty rows",
  );

  await act(async () => {
    root!.unmount();
  });
});

test("RindleSSR drives the whole handoff: seed on the server, boot the live store, swap without a flash", async () => {
  // Server render THROUGH RindleSSR: it builds the transport-less seed store from `ssrState`, reads
  // the seed via getServerSnapshot, and never runs `boot` (the effect doesn't fire on the server).
  const serverBoot = async (): Promise<{ store: InstanceType<typeof Store> }> => {
    throw new Error("boot must not run during the server render");
  };
  const serverHtml = renderToString(h(RindleSSR, { schema, ssrState: SEED, boot: serverBoot }, h(IssueList)));
  assert.match(serverHtml, /<li>bug<\/li>/);
  assert.match(serverHtml, /<li>lonely<\/li>/);

  // Browser: `boot` hands back a live (deferred) store. RindleSSR must seed THAT store from the same
  // snapshot so the swap shows the SSR rows rather than flashing empty while the backend goes live.
  const backend = new DeferredBackend();
  const browser = new Store(schema, backend);
  let booted = 0;
  const boot = async () => {
    booted += 1;
    return { store: browser };
  };

  const container = dom.window.document.getElementById("root")!;
  container.innerHTML = serverHtml;

  let root: ReturnType<typeof hydrateRoot>;
  await act(async () => {
    root = hydrateRoot(container, h(RindleSSR, { schema, ssrState: SEED, boot }, h(IssueList)));
  });

  assert.equal(booted, 1, "RindleSSR booted the live client exactly once, after hydration");
  assert.deepStrictEqual(
    [...container.querySelectorAll("li")].map((li) => li.textContent),
    ["bug", "lonely"],
    "the live store was seeded from ssrState — the swap keeps the SSR rows, no empty flash",
  );

  // The booted backend goes live with fresher data → the swapped-in live store reconciles the DOM.
  backend.snapshot = [
    { path: [], op: { tag: "add", node: { row: [1, "bug (live)"], rels: [] } } },
    { path: [], op: { tag: "add", node: { row: [3, "brand new"], rels: [] } } },
  ];
  await act(async () => {
    backend.goLive();
  });
  assert.deepStrictEqual(
    [...container.querySelectorAll("li")].map((li) => li.textContent),
    ["bug (live)", "brand new"],
    "after the handoff the live backend drives the view — RindleSSR swapped seed → live",
  );

  await act(async () => {
    root!.unmount();
  });
});

test("the client hydration pass matches the SSR markup, then a live hello reconciles", async () => {
  // Server render (what ships in the HTML).
  const server = new Store(schema, new OneShotBackend());
  server.hydrate(SEED);
  const serverHtml = renderToString(app(server));

  // Browser store: a live (deferred) backend, seeded from the dehydrated state over the "wire".
  const dehydrated = JSON.parse(JSON.stringify(server.dehydrate()));
  const backend = new DeferredBackend();
  const browser = new Store(schema, backend);
  browser.hydrate(dehydrated);

  // Put the SSR markup in the container, then hydrate over it — capturing any mismatch error.
  const container = dom.window.document.getElementById("root")!;
  container.innerHTML = serverHtml;

  const errors: string[] = [];
  const origError = console.error;
  console.error = (...args: unknown[]) => {
    errors.push(args.map(String).join(" "));
  };

  let root: ReturnType<typeof hydrateRoot>;
  try {
    await act(async () => {
      root = hydrateRoot(container, app(browser));
    });
  } finally {
    console.error = origError;
  }

  // No hydration mismatch — the client's getServerSnapshot produced identical markup.
  assert.equal(
    errors.filter((e) => /hydrat|did not match|mismatch/i.test(e)).length,
    0,
    `unexpected hydration errors: ${errors.join("\n")}`,
  );
  assert.deepStrictEqual(
    [...container.querySelectorAll("li")].map((li) => li.textContent),
    ["bug", "lonely"],
    "first paint shows the seeded rows",
  );

  // The live snapshot arrives (fresher data) → the view reconciles in the DOM.
  backend.snapshot = [
    { path: [], op: { tag: "add", node: { row: [1, "bug (live)"], rels: [] } } },
    { path: [], op: { tag: "add", node: { row: [3, "brand new"], rels: [] } } },
  ];
  await act(async () => {
    backend.goLive();
  });
  assert.deepStrictEqual(
    [...container.querySelectorAll("li")].map((li) => li.textContent),
    ["bug (live)", "brand new"],
    "the live hello+snapshot reconciled the hydrated view",
  );

  await act(async () => {
    root!.unmount();
  });
});
