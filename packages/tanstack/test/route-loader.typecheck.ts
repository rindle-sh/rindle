// Compile-only contract fixture. TanStack's generated route tree normally supplies this module
// augmentation; keeping a minimal one here makes `tsc --noEmit` verify that the object returned by
// rindle.loader(...) remains a valid createFileRoute loader.

import { createFileRoute, createRootRoute } from "@tanstack/react-router";
import { createSchema, defineQuery, newQueryBuilder, number, string, table } from "@rindle/client";

import { createRindleTanStack } from "../src/index.ts";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const qb = newQueryBuilder(schema);
const byID = defineQuery("issueByID", (id: number) => qb.issue.where.id(id).one());
const rootRoute = createRootRoute();

declare module "@tanstack/react-router" {
  interface FileRoutesByPath {
    "/issues/$id": {
      id: "/issues/$id";
      path: "/issues/$id";
      fullPath: "/issues/$id";
      preLoaderRoute: typeof rootRoute;
      parentRoute: typeof rootRoute;
    };
  }
}

const rindle = createRindleTanStack({
  schema,
  boot: async () => null as never,
  preload: async () => ({}),
});

export const Route = createFileRoute("/issues/$id")({
  loader: rindle.loader({
    query: ({ params }) => byID(Number(params.id)),
  }),
});
