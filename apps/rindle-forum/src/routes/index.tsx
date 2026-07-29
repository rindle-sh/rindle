// The home view (`/`): the list of forum categories, each with a LIVE thread count. Its loader seeds
// the categories query for first paint (SSR); after hydration the wasm engine owns the live read.

import { createFileRoute } from "@tanstack/react-router";
import { fragmentKey, useRoot } from "@rindle/react";
import type { DehydratedState } from "@rindle/client";

import { CategoryCardFragment, categoriesQuery } from "../components/CategoryCard.queries.ts";
import { CategoryCard } from "../components/CategoryCard.tsx";
import { preloadRindle } from "../ssr.ts";

export const Route = createFileRoute("/")({
  loader: async (): Promise<{ rindle: DehydratedState }> => {
    if (!import.meta.env.SSR) return { rindle: {} };
    return { rindle: await preloadRindle([categoriesQuery()]) };
  },
  component: Home,
});

function Home() {
  const [categories] = useRoot(categoriesQuery, CategoryCardFragment);
  const loading = categories.length === 0;

  return (
    <section className="rf-page">
      <div className="rf-page-head">
        <p className="rf-eyebrow">Rindle community</p>
        <h1>Categories</h1>
      </div>
      {loading ? (
        <p className="rf-empty">Loading categories…</p>
      ) : (
        <ul className="rf-categories">
          {categories.map((category) => (
            <CategoryCard key={fragmentKey(category)} category={category} />
          ))}
        </ul>
      )}
    </section>
  );
}
