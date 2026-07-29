// Pull @tanstack/react-start's augmentation of TanStack Router's route options into the type program,
// so the `server` key our API routes (src/routes/api.rindle.*.tsx) use is typed under `tsc`. The vite
// Start plugin injects this import into routeTree.gen.ts during dev/build, but `pnpm typecheck`
// regenerates that file with the bare `tsr` CLI (which omits it) — so we load it here once, for the
// whole project, where the generator can't strip it.
import type {} from "@tanstack/react-start";
