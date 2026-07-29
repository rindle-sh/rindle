// The root route: the HTML document + the app frame. Inside the document, <RindleApp> renders the
// merged SSR seed on the server AND through hydration, then boots the in-browser wasm engine
// (client-only) and swaps to the live store — the SSR→SPA handoff. <TopBar> + <Toaster> are the
// persistent chrome around the matched view (`children`).
//
// Each LEAF route owns its own first-paint preload (its loader returns `{ rindle }`); RootDocument
// merges every matched route's slice, so a first visit to any route seeds exactly the queries it
// renders. No preload lives here — the root has no view of its own.

import { useMemo } from "react";
import { HeadContent, Outlet, Scripts, createRootRoute, useMatches } from "@tanstack/react-router";
import type { DehydratedState } from "@rindle/client";

import { RindleApp } from "../RindleApp.tsx";
import { TopBar } from "../components/TopBar.tsx";
import { Toaster } from "../components/Toaster.tsx";
import appCss from "../styles.css?url";

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1.0" },
      { title: "Rindle Forum" },
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
  // Merge the dehydrated first-paint cache from EVERY matched route, so a first visit to any route
  // seeds exactly the queries it renders.
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
          <TopBar />
          <main className="rf-main">
            <Outlet />
          </main>
          <Toaster />
        </RindleApp>
        <Scripts />
      </body>
    </html>
  );
}
