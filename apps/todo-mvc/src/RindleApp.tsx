// The SSR→SPA store handoff (SSR-DESIGN.md §6.1). The handoff itself — seed the render from the SSR
// snapshot, boot the wasm engine after hydration, swap the store under `useQuery` with no flash —
// lives in `<RindleSSR>` (@rindle/react). This file just binds THIS app's `schema` + `bootClient`
// into it, so the route tree keeps rendering `<RindleApp ssrState={…}>` unchanged.

import type { ReactNode } from "react";
import { RindleSSR } from "@rindle/react";
import type { DehydratedState } from "@rindle/client";

import { schema } from "../shared/app-def.ts";
import { bootClient } from "./rindle-client.ts";

export function RindleApp({ ssrState, children }: { ssrState: DehydratedState; children: ReactNode }) {
  return (
    <RindleSSR schema={schema} ssrState={ssrState} boot={bootClient}>
      {children}
    </RindleSSR>
  );
}
