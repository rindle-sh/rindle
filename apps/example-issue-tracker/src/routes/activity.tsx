// The activity view (`/activity`): the newest comments across every issue (its own `recentComments`
// root query). It only needs to route a clicked issue into the detail pane. Its loader SSR-seeds the
// feed (SSR-DESIGN.md §6) so a first visit to /activity server-renders the comments, not an empty
// pane; the seed is merged with the chrome's in src/routes/__root.tsx.

import { createFileRoute } from "@tanstack/react-router";
import type { DehydratedState } from "@rindle/client";

import { FEED_LIMIT } from "../../shared/app-def.ts";
import { recentCommentsQuery } from "../components/ActivityFeed.queries.ts";
import { useIssueView } from "../app-context.ts";
import { ActivityFeed } from "../components/ActivityFeed.tsx";

export const Route = createFileRoute("/activity")({
  loader: async (): Promise<{ rindle: DehydratedState }> => {
    if (!import.meta.env.SSR) return { rindle: {} };
    const { preloadRindle } = await import("../ssr.ts");
    return { rindle: await preloadRindle([recentCommentsQuery({ limit: FEED_LIMIT })]) };
  },
  component: ActivityView,
});

function ActivityView() {
  const { onSelect } = useIssueView();
  return <ActivityFeed onSelect={onSelect} />;
}
