// A category view (`/c/:slug`): the category header + its live, paginated thread window, plus the
// "new thread" composer. Both first-paint queries (the category header by slug, the thread window) are
// seeded by the loader; after hydration the wasm engine owns the live reads.

import { useState } from "react";
import { Link, createFileRoute } from "@tanstack/react-router";
import { fragmentKey, useRoot } from "@rindle/react";
import type { DehydratedState } from "@rindle/client";

import { PAGE_SIZE } from "../../shared/app-def.ts";
import { categoryBySlugQuery } from "../components/CategoryCard.queries.ts";
import { ThreadCardFragment, threadsPageQuery } from "../components/ThreadCard.queries.ts";
import { ThreadCard } from "../components/ThreadCard.tsx";
import { NewThreadForm } from "../components/NewThreadForm.tsx";
import { preloadRindle } from "../ssr.ts";

export const Route = createFileRoute("/c/$slug")({
  loader: async ({ params }): Promise<{ rindle: DehydratedState }> => {
    if (!import.meta.env.SSR) return { rindle: {} };
    return {
      rindle: await preloadRindle([
        categoryBySlugQuery(params.slug),
        threadsPageQuery({ slug: params.slug, limit: PAGE_SIZE }),
      ]),
    };
  },
  component: CategoryView,
});

function CategoryView() {
  const { slug } = Route.useParams();
  const [limit, setLimit] = useState(PAGE_SIZE);

  const [category] = useRoot(categoryBySlugQuery, slug);
  const [threads] = useRoot(threadsPageQuery, { slug, limit }, ThreadCardFragment);
  const loading = !category && threads.length === 0;
  const canLoadMore = threads.length >= limit;

  return (
    <section className="rf-page">
      <div className="rf-breadcrumb">
        <Link to="/">Categories</Link> <span aria-hidden="true">/</span> <span>{category?.name ?? slug}</span>
      </div>
      <div className="rf-page-head rf-page-head-row">
        <div>
          <h1>{category?.name ?? slug}</h1>
          {category?.description ? <p className="rf-muted">{category.description}</p> : null}
        </div>
        {category ? <NewThreadForm categoryId={category.id} /> : null}
      </div>

      {loading ? (
        <p className="rf-empty">Loading threads…</p>
      ) : threads.length === 0 ? (
        <div className="rf-empty-state" role="status">
          <p className="rf-empty-state-title">No threads yet.</p>
          <p className="rf-empty-state-hint">Be the first to start one.</p>
        </div>
      ) : (
        <ul className="rf-threads">
          {threads.map((thread) => (
            <ThreadCard key={fragmentKey(thread)} thread={thread} />
          ))}
        </ul>
      )}
      {canLoadMore && (
        <button type="button" className="rf-btn rf-btn-ghost rf-more" onClick={() => setLimit((l) => l + PAGE_SIZE)}>
          Load {PAGE_SIZE} more
        </button>
      )}
    </section>
  );
}
