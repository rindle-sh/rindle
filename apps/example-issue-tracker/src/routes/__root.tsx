// The root route: the HTML document + the app frame. On the first (server-rendered) visit the
// `loader` preloads the chrome's first-paint queries through the API tier (one /api/rindle/read
// each, SSR-DESIGN.md §6) and dehydrates them into the HTML; inside the document, <RindleApp>
// renders that seed on the server AND through hydration, then boots the in-browser wasm engine
// (client-only) and swaps to the live store — the SSR→SPA handoff. <AppChrome> draws the persistent
// frame around the matched view (`children`). The selection + create pane are validated search
// params here so every nested view inherits them.

import { useMemo } from "react";
import { HeadContent, Outlet, Scripts, createRootRoute, useMatches } from "@tanstack/react-router";
import type { DehydratedState, Query } from "@rindle/client";

import { PAGE_SIZE } from "../../shared/app-def.ts";
import { issuesPageQuery } from "../components/IssueListItem.queries.ts";
import { issueDetailQuery } from "../components/IssueDetail.queries.ts";
import { tagOptionsQuery } from "../components/TagChip.queries.ts";
import { usersQuery } from "../components/UserBadge.queries.ts";
import { AppChrome } from "../AppChrome.tsx";
import { RindleApp } from "../RindleApp.tsx";
import type { AppSearch } from "../app-context.ts";
import appCss from "../styles.css?url";

export const Route = createRootRoute({
  validateSearch: (search: Record<string, unknown>): AppSearch => ({
    issue: typeof search.issue === "string" ? search.issue : undefined,
    new: search.new === true || search.new === "true" ? true : undefined,
  }),
  // The detail pane is URL state (`?issue=`) rendered by the chrome, not a child route — so its
  // first-paint query belongs to the ROOT loader. Declaring it a loader dep means a deep link like
  // `/?issue=42` server-renders the open pane too (and the loader re-runs only when the id changes).
  loaderDeps: ({ search }: { search: AppSearch }) => ({ issue: search.issue }),
  // SERVER-ONLY first-paint preload (SSR-DESIGN.md §6.2). The chrome (src/AppChrome.tsx) opens the
  // live window + user/tag casts with `useRoot` on first render, so we seed those exact queries by their
  // canonical viewKey — the chrome's default page (one PAGE_SIZE window, no filter) and the whole
  // user table — plus the open issue's thread when `?issue=` is set. After hydration the live wasm
  // engine owns every read, so the loader no-ops on client nav; each seed is consumed on its first
  // live `hello` (no stale SSR flash).
  loader: async ({ deps }): Promise<{ rindle: DehydratedState }> => {
    if (!import.meta.env.SSR) return { rindle: {} };
    // Typed from preloadRindle so the plural window/user queries and the singular `.one()` issue
    // thread share one array (their result shapes differ).
    const toPreload: Array<Query<any, any, any>> = [
      issuesPageQuery({ limit: PAGE_SIZE, filter: [] }),
      usersQuery(),
      tagOptionsQuery(),
    ];
    if (deps.issue) toPreload.push(issueDetailQuery(deps.issue));
    const { preloadRindle } = await import("../ssr.ts");
    return { rindle: await preloadRindle(toPreload) };
  },
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1.0" },
      { title: "Rindle Issue Tracker" },
      { name: "theme-color", content: "#f2ede3" },
    ],
    links: [
      { rel: "stylesheet", href: appCss },
      { rel: "preconnect", href: "https://fonts.googleapis.com" },
      { rel: "preconnect", href: "https://fonts.gstatic.com", crossOrigin: "anonymous" },
      {
        rel: "stylesheet",
        href: "https://fonts.googleapis.com/css2?family=Hanken+Grotesk:wght@300;400;500;600;700;800&family=DM+Mono:ital,wght@0,300;0,400;0,500;1,400&display=swap",
      },
    ],
  }),
  shellComponent: RootDocument,
});

function RootDocument() {
  // Merge the dehydrated first-paint cache from EVERY matched route (the root's chrome + detail
  // seeds, plus a child route's own — e.g. /activity's feed), so a first visit to any route seeds
  // exactly the queries it renders. TanStack serializes loader data into the HTML, so the server
  // render and the client's matching hydration pass build <RindleApp> from the same snapshot.
  const matches = useMatches();
  const ssrState = useMemo<DehydratedState>(() => {
    const merged: DehydratedState = {};
    for (const match of matches) {
      const slice = (match.loaderData as { rindle?: DehydratedState } | undefined)?.rindle;
      if (slice) Object.assign(merged, slice);
    }
    return merged;
  }, [matches]);
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        <RindleApp ssrState={ssrState}>
          <AppChrome>
            <Outlet />
          </AppChrome>
        </RindleApp>
        <Scripts />
      </body>
    </html>
  );
}
