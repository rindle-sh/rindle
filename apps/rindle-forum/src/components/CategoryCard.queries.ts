// Co-located with the category card on the home page. Holds the per-card SELECTION
// (`CategoryCardFragment`) AND the named root query over the (small) category table (`categoriesQuery`).
// React-free (see `UserBadge.queries.ts`).

import { defineFragment, defineQuery } from "@rindle/client";
import type { FragmentRef } from "@rindle/client";
import { z } from "zod";

import { category, q, rels } from "../../shared/app-def.ts";

/** A category row + a LIVE count of its threads (a scalar `countAs` over the whole `thread` table —
 *  maintained incrementally, never polled). */
export const CategoryCardFragment = defineFragment(category, (c) =>
  c.select("id", "slug", "name", "description", "position").countAs("threadCount", rels.categoryThreads),
);
export type CategoryCardRef = FragmentRef<typeof CategoryCardFragment>;

/** Every category, ordered by position. The table is tiny (curated sections), so — unlike the
 *  paginated thread windows — it's fine to query whole. No args, so no validator. */
export const categoriesQuery = defineQuery("categories", () =>
  q.category.orderBy("position", "asc").orderBy("id", "asc").include(CategoryCardFragment),
);

/** The category-page header: one category resolved by its URL slug. The threads themselves come from
 *  `threadsPageQuery` keyed by the resolved id (ThreadCard.queries.ts). */
const slugArgs = z.string().max(80);
export const categoryBySlugQuery = defineQuery("categoryBySlug", (raw) => slugArgs.parse(raw), (slug) =>
  q.category.where.slug(slug).include(CategoryCardFragment).one(),
);
