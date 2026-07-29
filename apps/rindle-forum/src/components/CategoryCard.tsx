// A single category on the home page: name, description, and a LIVE thread count (the `countAs` stays
// exact as threads are created — no polling). Reads its data through the `CategoryCard` fragment.

import { Link } from "@tanstack/react-router";
import { useFragment } from "@rindle/react";

import { CategoryCardFragment } from "./CategoryCard.queries.ts";
import type { CategoryCardRef } from "./CategoryCard.queries.ts";

export function CategoryCard({ category }: { category: CategoryCardRef }) {
  const data = useFragment(CategoryCardFragment, category);
  if (!data) return null;

  return (
    <li className="rf-category">
      <Link to="/c/$slug" params={{ slug: data.slug }} className="rf-category-link">
        <div className="rf-category-main">
          <h2>{data.name}</h2>
          <p>{data.description}</p>
        </div>
        <span className="rf-count" title={`${data.threadCount} threads`}>
          {data.threadCount}
          <small>threads</small>
        </span>
      </Link>
    </li>
  );
}
