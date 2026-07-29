// The list view (`/`): the growing live window of issues, infinite-scrolled. It reads the shared
// window the chrome keeps subscribed (src/AppChrome.tsx) off context and renders it as rows.

import { createFileRoute } from "@tanstack/react-router";

import { useIssueView } from "../app-context.ts";
import { IssueList } from "../components/IssueList.tsx";

export const Route = createFileRoute("/")({ component: ListView });

function ListView() {
  const { issues, selectedId, loadingFirst, canLoadMore, reachedEnd, onSelect, loadMore } = useIssueView();
  return (
    <IssueList
      issues={issues}
      selectedId={selectedId}
      loading={loadingFirst}
      canLoadMore={canLoadMore}
      showEnd={reachedEnd && issues.length > 0}
      onSelect={onSelect}
      onLoadMore={loadMore}
    />
  );
}
