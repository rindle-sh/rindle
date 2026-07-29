import { test } from "node:test";
import assert from "node:assert/strict";

import { createSchema, defineQuery, newQueryBuilder, number, string, table } from "@rindle/client";
import type {
  AnyQuery,
  EnsureQueryOptions,
  Store,
} from "@rindle/client";

import {
  createRindleTanStack,
  mergeRindleLoaderData,
  type RindleLoaderContext,
} from "../src/index.ts";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const qb = newQueryBuilder(schema);
const byID = defineQuery("issueByID", (id: number) => qb.issue.where.id(id).one());
const comments = defineQuery("comments", (issueID: number) => qb.issue.where.id(issueID));

function context(id = "7"): RindleLoaderContext {
  return {
    abortController: new AbortController(),
    preload: false,
    params: { id },
    deps: {},
    context: {},
    location: {},
    cause: "enter",
  };
}

test("server loader preloads the primary query plus deduplicated SSR extras", async () => {
  const preloaded: AnyQuery[][] = [];
  const integration = createRindleTanStack({
    schema,
    boot: async () => assert.fail("server loader must not boot the browser client"),
    preload: async (queries) => {
      preloaded.push([...queries]);
      return { seeded: { rows: [{ id: 7, title: "server" }], cvMin: 1 } };
    },
  });
  const loader = integration.loader({
    query: ({ params }) => byID(Number(params.id)),
    ssr: ({ params }) => [byID(Number(params.id)), comments(Number(params.id))],
    until: "present",
  });

  const data = await loader.handler(context());

  assert.equal(loader.staleReloadMode, "blocking");
  assert.deepStrictEqual(preloaded[0].map((query) => query.name), ["issueByID", "comments"]);
  assert.deepStrictEqual(data, { rindle: { seeded: { rows: [{ id: 7, title: "server" }], cvMin: 1 } } });
});

test("browser loader defaults to present and forwards explicit readiness + cancellation", async () => {
  const calls: Array<{ query: AnyQuery; options?: EnsureQueryOptions }> = [];
  const client = {
    store: null as unknown as Store<any>,
    ensure: async (query: AnyQuery, options?: EnsureQueryOptions) => {
      calls.push({ query, options });
    },
  };
  const integration = createRindleTanStack({
    schema,
    boot: async () => client,
    preload: async () => assert.fail("browser loader must not call the SSR preloader"),
  });
  const loader = integration.loader({
    query: ({ params }) => byID(Number(params.id)),
    ssr: ({ params }) => comments(Number(params.id)),
  });
  const loaderContext = context();
  const previousWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
  Object.defineProperty(globalThis, "window", { configurable: true, value: {} });

  try {
    const data = await loader.handler(loaderContext);
    assert.deepStrictEqual(data, { rindle: {} });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].query.name, "issueByID");
    assert.equal(calls[0].options?.until, "present");
    assert.equal(calls[0].options?.signal, loaderContext.abortController.signal);

    const strictContext = context("8");
    const strictLoader = integration.loader({
      query: ({ params }) => byID(Number(params.id)),
      until: "complete",
    });
    await strictLoader.handler(strictContext);
    assert.equal(calls.length, 2);
    assert.equal(calls[1].query.name, "issueByID");
    assert.equal(calls[1].options?.until, "complete");
    assert.equal(calls[1].options?.signal, strictContext.abortController.signal);
  } finally {
    if (previousWindow) Object.defineProperty(globalThis, "window", previousWindow);
    else Reflect.deleteProperty(globalThis, "window");
  }
});

test("browser loaders share one in-flight boot and one client", async () => {
  let boots = 0;
  const ensured: string[] = [];
  const client = {
    store: null as unknown as Store<any>,
    ensure: async (query: AnyQuery) => {
      ensured.push(String(query.args));
    },
  };
  const integration = createRindleTanStack({
    schema,
    boot: async () => {
      boots++;
      return client;
    },
    preload: async () => assert.fail("browser loader must not call the SSR preloader"),
  });
  const loader = integration.loader({
    query: ({ params }) => byID(Number(params.id)),
  });
  const previousWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
  Object.defineProperty(globalThis, "window", { configurable: true, value: {} });

  try {
    await Promise.all([loader.handler(context("7")), loader.handler(context("8"))]);
    assert.equal(boots, 1);
    assert.deepStrictEqual(ensured.sort(), ["7", "8"]);
  } finally {
    if (previousWindow) Object.defineProperty(globalThis, "window", previousWindow);
    else Reflect.deleteProperty(globalThis, "window");
  }
});

test("an SSR-only browser loader returns immediately without booting the client", async () => {
  let boots = 0;
  const integration = createRindleTanStack({
    schema,
    boot: async () => {
      boots++;
      return null as never;
    },
    preload: async () => ({}),
  });
  const loader = integration.loader({ ssr: ({ params }) => byID(Number(params.id)) });
  const previousWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
  Object.defineProperty(globalThis, "window", { configurable: true, value: {} });

  try {
    assert.deepStrictEqual(await loader.handler(context()), { rindle: {} });
    assert.equal(boots, 0);
  } finally {
    if (previousWindow) Object.defineProperty(globalThis, "window", previousWindow);
    else Reflect.deleteProperty(globalThis, "window");
  }
});

test("mergeRindleLoaderData combines matched route slices and ignores unrelated loader data", () => {
  assert.deepStrictEqual(
    mergeRindleLoaderData([
      undefined,
      { other: true },
      { rindle: { a: { rows: [1], cvMin: 1 } } },
      { rindle: { b: { rows: [2], cvMin: 2 } } },
    ]),
    {
      a: { rows: [1], cvMin: 1 },
      b: { rows: [2], cvMin: 2 },
    },
  );
});
