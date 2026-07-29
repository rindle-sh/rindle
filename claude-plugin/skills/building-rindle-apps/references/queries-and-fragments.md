<!-- GENERATED FILE — do not edit.
     Source: product-page/src/content/docs/fragments.md (https://rindle.sh/docs/fragments)
     Regenerate: node product-page/scripts/gen-skill.mjs -->

# Compose the UI with fragments

> Let each component declare the columns it renders as a fragment, compose them into one named coverage query, and root the whole screen with useRoot — no request waterfall.

A **fragment** is a reusable projection over a table — the columns (and nested relationships) a
component renders. Each component declares its own fragment; a named query composes them with
`.include(...)` and `.sub(...)`, and the screen roots the lot in **one** live coverage query. There
is no per-component fetch and no waterfall — the whole tree is one subscription.

## In the wild — tantaman.github.io

A card declares exactly what it draws:

```ts
// src/components/PostCard.queries.ts — the card's own projection
export const PostCardFragment = defineFragment(post, (p) =>
  p.select(
    "id", "title", "date", "publishedAt", "description", "thesis", "tags",
    "concern", "author", "form", "kind", "cardImage", "color", "pinned", "readingMinutes",
  ),
);
export type PostCardRef = FragmentRef<typeof PostCardFragment>;
```

*[`rindle-site/src/components/PostCard.queries.ts` L17–35](https://github.com/tantaman/tantaman.github.io/blob/5889c6d72add4bd2825230223130fe896ceac4e3/rindle-site/src/components/PostCard.queries.ts#L17-L35) · tantaman.github.io*

The list query pulls that fragment in with `.include(...)`, so the page's coverage is defined by the
components it renders:

```ts
export const postsQuery = defineQuery("posts", (raw) => postsArgs.parse(raw), ({ limit }) =>
  q.post
    .orderBy("pinned", "desc")
    .orderBy("publishedAt", "desc")
    .orderBy("id", "asc")
    .limit(limit + 1)
    .include(PostCardFragment),
);
```

*[`rindle-site/src/components/PostCard.queries.ts` L49–56](https://github.com/tantaman/tantaman.github.io/blob/5889c6d72add4bd2825230223130fe896ceac4e3/rindle-site/src/components/PostCard.queries.ts#L49-L56) · tantaman.github.io*

Relationships nest with `.sub(name, rel, builder)` — here a paste with its parent and its forks in one
query:

```ts
export const pasteQuery = defineQuery("paste", (raw) => pasteIdArgs.parse(raw), (id) =>
  q.paste
    .where.id(id)
    .select("id", "body", "language", "title", "excerpt", "createdAt", "parentId", "shared", "sharedAt")
    .sub("parent", relationships.pasteParent, (parent) =>
      parent.limit(1).select("id", "title", "createdAt", "parentId"),
    )
    .sub("children", relationships.pasteChildren, (child) =>
      child.orderBy("createdAt", "asc").orderBy("id", "asc").limit(PASTE_FORKS_LIMIT)
        .select("id", "title", "createdAt", "parentId"),
    )
    .one(),
);
```

*[`rindle-site/src/components/Paste.queries.ts` L55–70](https://github.com/tantaman/tantaman.github.io/blob/5889c6d72add4bd2825230223130fe896ceac4e3/rindle-site/src/components/Paste.queries.ts#L55-L70) · tantaman.github.io*

The component reads its slice — and only its slice — through `useFragment`. A parent maps a list of
refs, keying each with `fragmentKey(post)`:

```tsx
// src/components/PostCard.tsx
export function PostCard({ post }: { post: PostCardRef }) {
  const data = useFragment(PostCardFragment, post);
  if (!data) return null;
  const authors = parseList(data.author);
  // …render data.title, authors, data.readingMinutes …
}
```

*[`rindle-site/src/components/PostCard.tsx` L13–19](https://github.com/tantaman/tantaman.github.io/blob/5889c6d72add4bd2825230223130fe896ceac4e3/rindle-site/src/components/PostCard.tsx#L13-L19) · tantaman.github.io*

Root the screen once with `useRoot(postsQuery, …)`; every `useFragment` under it reads from that same
live subscription.

## See also

- [Fine-grained reactivity](https://rindle.sh/docs/fine-grained-reactivity) — why per-row `useFragment` keeps updates
  local to one component.
- [Preload & navigate](https://rindle.sh/docs/preloads) — seeding the root coverage query from a route loader.
