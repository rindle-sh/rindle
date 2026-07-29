// A thread view (`/t/:id`): the whole post thread (opening post + replies, oldest-first), each with a
// live vote score, plus the reply composer. The single detail query is seeded by the loader for first
// paint; after hydration the wasm engine owns the live read and every new reply / vote streams in.

import { Link, createFileRoute } from "@tanstack/react-router";
import { fragmentKey, useRoot } from "@rindle/react";
import type { DehydratedState } from "@rindle/client";

import { threadDetailQuery } from "../components/ThreadView.queries.ts";
import { PostCard } from "../components/PostCard.tsx";
import { ReplyComposer } from "../components/ReplyComposer.tsx";
import { UserBadge } from "../components/UserBadge.tsx";
import { formatDateTime } from "../lib/format.ts";

export const Route = createFileRoute("/t/$id")({
  loader: async ({ params }): Promise<{ rindle: DehydratedState }> => {
    if (!import.meta.env.SSR) return { rindle: {} };
    const { preloadRindle } = await import("../ssr.ts");
    return { rindle: await preloadRindle([threadDetailQuery(params.id)]) };
  },
  component: ThreadView,
});

function ThreadView() {
  const { id } = Route.useParams();
  const [thread, { status }] = useRoot(threadDetailQuery, id);

  if (!thread) {
    return (
      <section className="rf-page">
        <p className="rf-empty">{status === "complete" ? "Thread not found." : "Loading thread…"}</p>
        <Link to="/" className="rf-link-btn">← Back to categories</Link>
      </section>
    );
  }

  const author = thread.author?.[0];
  const category = thread.category?.[0];
  const posts = thread.posts ?? [];

  return (
    <section className="rf-page rf-thread-view">
      <div className="rf-breadcrumb">
        <Link to="/">Categories</Link> <span aria-hidden="true">/</span>{" "}
        {category ? (
          <Link to="/c/$slug" params={{ slug: category.slug }}>{category.name}</Link>
        ) : (
          <span>category</span>
        )}
      </div>
      <div className="rf-page-head">
        <h1>
          {thread.pinned ? <span className="rf-pin" title="Pinned">📌 </span> : null}
          {thread.locked ? <span className="rf-lock" title="Locked">🔒 </span> : null}
          {thread.title}
        </h1>
        <p className="rf-muted">
          {author ? <UserBadge user={author} /> : thread.authorId} · {formatDateTime(thread.createdAt)}
        </p>
      </div>

      <div className="rf-posts">
        {posts.map((post, i) => (
          <PostCard key={fragmentKey(post)} post={post} opening={i === 0} />
        ))}
      </div>

      <ReplyComposer threadId={thread.id} locked={!!thread.locked} />
    </section>
  );
}
