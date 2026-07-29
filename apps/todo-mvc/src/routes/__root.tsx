import { useMemo } from "react";
import { HeadContent, Outlet, Scripts, createRootRoute, useMatches } from "@tanstack/react-router";
import type { DehydratedState } from "@rindle/client";

import { DevTools } from "../devtools.tsx";
import { RindleApp } from "../RindleApp.tsx";
import appCss from "../styles.css?url";

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1.0" },
      { title: "Rindle TODO MVC" },
      { name: "theme-color", content: "#eef2f0" },
    ],
    links: [{ rel: "stylesheet", href: appCss }],
  }),
  shellComponent: RootDocument,
});

function RootDocument() {
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
          <Outlet />
        </RindleApp>
        <DevTools />
        <Scripts />
      </body>
    </html>
  );
}
