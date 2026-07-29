// The root route: the HTML document + the app frame. Inside the document, rindle.Provider merges
// every matched route's SSR seed, renders it on the server AND through hydration, then boots the
// in-browser wasm engine and swaps to the live store. <TopBar> + <Toaster> are the persistent chrome.
//
// Each leaf route declares its first-paint/navigation query through rindle.loader(...); the adapter
// owns server preloading, client readiness, cancellation, and the SSR→SPA handoff.

import { HeadContent, Outlet, Scripts, createRootRoute } from "@tanstack/react-router";

import { TopBar } from "../components/TopBar.tsx";
import { Toaster } from "../components/Toaster.tsx";
import { DevTools } from "../devtools.tsx";
import { rindle } from "../rindle-tanstack.ts";
import appCss from "../styles.css?url";

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1.0" },
      { title: "__PROJECT_NAME__" },
      { name: "theme-color", content: "#f2eee7" },
    ],
    links: [
      // Hanken Grotesk (UI) + DM Mono (counts, timestamps, labels). Swap for a self-hosted font to drop the CDN.
      { rel: "preconnect", href: "https://fonts.googleapis.com" },
      { rel: "preconnect", href: "https://fonts.gstatic.com", crossOrigin: "anonymous" },
      {
        rel: "stylesheet",
        href: "https://fonts.googleapis.com/css2?family=Hanken+Grotesk:wght@300;400;500;600;700;800&family=DM+Mono:ital,wght@0,300;0,400;0,500;1,400&display=swap",
      },
      { rel: "stylesheet", href: appCss },
    ],
  }),
  shellComponent: RootDocument,
});

function RootDocument() {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        <rindle.Provider>
          <TopBar />
          <main className="app-main">
            <Outlet />
          </main>
          <Toaster />
        </rindle.Provider>
        {/* Dev-only floating devtools pane (tree-shaken out of production builds). */}
        <DevTools />
        <Scripts />
      </body>
    </html>
  );
}
