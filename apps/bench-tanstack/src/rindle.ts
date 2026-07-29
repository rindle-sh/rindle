// Rindle adapter: the @rindle/wasm in-process store. Seeds the whole dataset in one write
// transaction (a snapshot load), then materializes each query in the ladder. Materialize is
// synchronous: the wasm engine runs the query and the JS ArrayView folds its snapshot before
// `materialize()` returns, so `view.data` is readable immediately.

import {
  table,
  number,
  string,
  boolean,
  createSchema,
  createWasmStore,
  type Store,
} from "@rindle/wasm";
import type { Dataset, Scale } from "./data.ts";
import { probeIds, viewSetup, VIEW_LIMIT, PREVIEW_LIMIT } from "./data.ts";
import { resultShape, type Adapter, type HydrateResult, type Incremental, type QueryName } from "./harness.ts";

const issueTable = table("issue")
  .columns({ id: number(), title: string(), open: boolean(), created: number(), creatorID: number() })
  .primaryKey("id");
const commentTable = table("comment")
  .columns({ id: number(), issueID: number(), body: string(), creatorID: number() })
  .primaryKey("id");
const userTable = table("user").columns({ id: number(), name: string(), role: string() }).primaryKey("id");

const schema = createSchema({ tables: [issueTable, commentTable, userTable] });
type S = typeof schema extends { __cols: infer C } ? C : never;

let sink = 0;

const ISSUE_COMMENTS = { parent: ["id"], child: ["issueID"] } as const;
const ISSUE_CREATOR = { parent: ["creatorID"], child: ["id"] } as const;

/** Build the query for `q` against a query root (the store's typed `store.query`). `ctx`
 *  carries data-dependent inputs (the issue a detail view targets, the page-1 cursor). */
function build(root: Store<S>["query"], q: QueryName, ctx: { detailId: number; page1Cursor: number }): unknown {
  // newest-open issue list, bounded to a viewport — the spine of every "view_list*" query.
  const list = () => root.issue.where.open(true).orderBy("created", "desc").limit(VIEW_LIMIT);
  switch (q) {
    case "scan":
      return root.issue;
    case "filter":
      return root.issue.where.open(true);
    case "filter_order_limit":
      return list();
    case "one_to_many":
      return root.issue.sub("comments", commentTable, ISSUE_COMMENTS);
    case "many_to_one":
      return root.issue.sub("creator", userTable, ISSUE_CREATOR);
    case "nested":
      return root.issue.sub("comments", commentTable, ISSUE_COMMENTS, (c) =>
        c.sub("creator", userTable, ISSUE_CREATOR),
      );
    case "aggregate_count":
      return root.issue.countAs("commentCount", commentTable, ISSUE_COMMENTS);

    // ---- realistic bounded views ----
    case "view_list":
      return list();
    case "view_list_creator":
      return list().sub("creator", userTable, ISSUE_CREATOR);
    case "view_list_count":
      return list().countAs("commentCount", commentTable, ISSUE_COMMENTS);
    case "view_list_comments":
      return list().sub("comments", commentTable, ISSUE_COMMENTS, (c) => c.orderBy("id", "desc").limit(PREVIEW_LIMIT));
    case "view_detail":
      return root.issue.where.id(ctx.detailId).limit(1).sub("comments", commentTable, ISSUE_COMMENTS);
    case "view_page":
      return root.issue.where
        .open(true)
        .orderBy("created", "desc")
        .start({ created: ctx.page1Cursor }, { exclusive: true })
        .limit(VIEW_LIMIT);
  }
}

/** Fully read a materialized view's `data`, touching every projected cell/child so the read
 *  cost is inside the timed region (and DCE can't drop it). Returns the cross-engine signature. */
function read(q: QueryName, data: readonly Record<string, unknown>[]): { count: number; nested: number } {
  const shape = resultShape(q);
  let nested = 0;
  let acc = 0;
  for (const row of data) {
    acc += (row.id as number) | 0;
    if (shape === "comments") {
      const cs = row.comments as Array<Record<string, unknown>>;
      nested += cs.length;
      for (const c of cs) {
        acc += (c.id as number) | 0;
        if (q === "nested") {
          const cr = c.creator as Array<Record<string, unknown>>;
          if (cr.length > 0) acc += (cr[0].id as number) | 0;
        }
      }
    } else if (shape === "creator") {
      const cr = row.creator as Array<Record<string, unknown>>;
      nested += cr.length;
      if (cr.length > 0) acc += (cr[0].id as number) | 0;
    } else if (shape === "count") {
      nested += (row.commentCount as number) | 0;
    }
  }
  sink = (sink + acc) | 0;
  return { count: data.length, nested };
}

interface View {
  data: readonly Record<string, unknown>[];
  destroy(): void;
}

export async function makeRindle(dataset: Dataset, scale: Scale): Promise<Adapter> {
  const store = await createWasmStore(schema);
  // snapshot load — one transaction, all rows. Not part of any timed measurement.
  await store.write((tx) => {
    for (const r of dataset.users) tx.add("user", r);
    for (const r of dataset.issues) tx.add("issue", r);
    for (const r of dataset.comments) tx.add("comment", r);
  });

  const ids = probeIds(scale);
  const vs = viewSetup(dataset, scale);
  const ctx = { detailId: vs.visibleIssueId, page1Cursor: vs.page1Cursor };
  const newIssue = {
    id: ids.newIssueId,
    title: "incremental probe issue",
    open: true,
    created: ids.newIssueCreated,
    creatorID: ids.existingCreatorId,
  };
  const newComment = {
    id: ids.newCommentId,
    issueID: ids.existingIssueId,
    body: "incremental probe comment",
    creatorID: ids.existingCreatorId,
  };

  // The reversible add+remove that actually flows through query `q`: a comment for the
  // relationship/aggregate/detail views, an issue for the scans and list windows.
  function probeOp(q: QueryName): { table: "issue" | "comment"; row: Record<string, unknown> } {
    if (q === "one_to_many" || q === "nested" || q === "aggregate_count") return { table: "comment", row: newComment };
    if (q === "view_list_count" || q === "view_list_comments" || q === "view_detail")
      return { table: "comment", row: vs.newVisibleComment };
    if (q === "view_list" || q === "view_list_creator") return { table: "issue", row: vs.newTopIssue };
    if (q === "view_page") return { table: "issue", row: vs.newPageIssue };
    return { table: "issue", row: newIssue }; // scan, filter, filter_order_limit, many_to_one
  }

  return {
    name: "Rindle",

    hydrate(q: QueryName): HydrateResult {
      const view = (build(store.query, q, ctx) as { materialize(): View }).materialize();
      const sig = read(q, view.data);
      return { handle: view, count: sig.count, nested: sig.nested };
    },

    disposeHydrate(handle: unknown): void {
      (handle as View).destroy();
    },

    incremental(q: QueryName): Incremental {
      const view = (build(store.query, q, ctx) as { materialize(): View }).materialize();
      const { table: t, row } = probeOp(q);
      const pair = async () => {
        await store.write((tx) => tx.add(t, row));
        await store.write((tx) => tx.remove(t, row));
      };
      return {
        liveCount: () => view.data.length,
        pair,
        dispose: () => view.destroy(),
      };
    },

    teardown(): void {
      if (sink === 0x7fffffff) process.stderr.write(""); // keep `sink` live
    },
  };
}
