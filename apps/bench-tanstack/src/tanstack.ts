// TanStack DB adapter. Base data lives in local-only collections, seeded through
// `initialData` (the authoritative sync path — one begin/write*/commit, O(n)); the per-row
// `.insert()` path is ~45× slower and only used for the incremental probe, where a single
// mutation is exactly what we want to measure. Live queries are `createLiveQueryCollection`
// with `startSync: true`, so the initial result is computed synchronously on creation.
//
// Indexes mirror what Rindle's engine maintains for its correlations/sorts: without them
// TanStack logs "Join requires an index … Falling back to loading all data", so giving it
// the indexes is the fair comparison (each engine indexed for the workload).

import {
  createCollection,
  createLiveQueryCollection,
  localOnlyCollectionOptions,
  eq,
  count,
  toArray,
  BasicIndex,
  BTreeIndex,
} from "@tanstack/db";
import type { Dataset, Scale, IssueRow, CommentRow, UserRow } from "./data.ts";
import { probeIds, viewSetup, VIEW_LIMIT, PREVIEW_LIMIT } from "./data.ts";
import { resultShape, type Adapter, type HydrateResult, type Incremental, type QueryName } from "./harness.ts";

let sink = 0;

type Coll<T> = {
  insert(rows: T | T[]): unknown;
  delete(key: number): unknown;
  createIndex(fn: (r: T) => unknown, opts: { indexType: unknown }): unknown;
  cleanup(): Promise<unknown>;
};
type LiveQ = { toArray: Record<string, unknown>[]; size: number; cleanup(): Promise<unknown> };

/** Build the live-query definition for `q`. Includes (one-to-many / many-to-one) use a
 *  `toArray(subquery)` in `select`, the TanStack idiom for nested results — the shape that
 *  matches Rindle's `.sub()` (an array of children per parent). `ctx` carries the detail
 *  view's target issue. */
function build(
  q: QueryName,
  c: { issues: unknown; comments: unknown; users: unknown },
  ctx: { detailId: number },
): (qb: any) => any {
  // newest-open viewport — the spine of the list views.
  const list = (qb: any) =>
    qb
      .from({ issue: c.issues })
      .where(({ issue }: any) => eq(issue.open, true))
      .orderBy(({ issue }: any) => issue.created, "desc")
      .limit(VIEW_LIMIT);
  switch (q) {
    case "scan":
      return (qb) => qb.from({ issue: c.issues });
    case "filter":
      return (qb) => qb.from({ issue: c.issues }).where(({ issue }: any) => eq(issue.open, true));
    case "filter_order_limit":
      return (qb) =>
        qb
          .from({ issue: c.issues })
          .where(({ issue }: any) => eq(issue.open, true))
          .orderBy(({ issue }: any) => issue.created, "desc")
          .limit(50);
    case "one_to_many":
      return (qb) =>
        qb.from({ issue: c.issues }).select(({ issue }: any) => ({
          id: issue.id,
          comments: toArray(
            qb.from({ cm: c.comments }).where(({ cm }: any) => eq(cm.issueID, issue.id)).select(({ cm }: any) => ({
              id: cm.id,
              body: cm.body,
            })),
          ),
        }));
    case "many_to_one":
      return (qb) =>
        qb.from({ issue: c.issues }).select(({ issue }: any) => ({
          id: issue.id,
          creator: toArray(
            qb.from({ u: c.users }).where(({ u }: any) => eq(u.id, issue.creatorID)).select(({ u }: any) => ({
              id: u.id,
              name: u.name,
            })),
          ),
        }));
    case "nested":
      return (qb) =>
        qb.from({ issue: c.issues }).select(({ issue }: any) => ({
          id: issue.id,
          comments: toArray(
            qb.from({ cm: c.comments }).where(({ cm }: any) => eq(cm.issueID, issue.id)).select(({ cm }: any) => ({
              id: cm.id,
              creator: toArray(
                qb.from({ u: c.users }).where(({ u }: any) => eq(u.id, cm.creatorID)).select(({ u }: any) => ({
                  id: u.id,
                })),
              ),
            })),
          ),
        }));
    case "aggregate_count":
      return (qb) =>
        qb
          .from({ issue: c.issues })
          .join({ cm: c.comments }, ({ issue, cm }: any) => eq(issue.id, cm.issueID), "left")
          .groupBy(({ issue }: any) => issue.id)
          .select(({ issue, cm }: any) => ({ id: issue.id, commentCount: count(cm.id) }));

    // ---- realistic bounded views ----
    case "view_list":
      return list;
    case "view_list_creator":
      return (qb) =>
        list(qb).select(({ issue }: any) => ({
          id: issue.id,
          creator: toArray(
            qb.from({ u: c.users }).where(({ u }: any) => eq(u.id, issue.creatorID)).select(({ u }: any) => ({
              id: u.id,
              name: u.name,
            })),
          ),
        }));
    case "view_list_count":
      // limit to the viewport FIRST (a subquery), THEN count its children — mirrors Rindle's
      // `limit(…).countAs(…)` (count the 50 shown, not every issue).
      return (qb) => {
        const top = list(qb);
        return qb
          .from({ issue: top })
          .join({ cm: c.comments }, ({ issue, cm }: any) => eq(issue.id, cm.issueID), "left")
          .groupBy(({ issue }: any) => issue.id)
          .select(({ issue, cm }: any) => ({ id: issue.id, commentCount: count(cm.id) }));
      };
    case "view_list_comments":
      return (qb) =>
        list(qb).select(({ issue }: any) => ({
          id: issue.id,
          comments: toArray(
            qb
              .from({ cm: c.comments })
              .where(({ cm }: any) => eq(cm.issueID, issue.id))
              .orderBy(({ cm }: any) => cm.id, "desc")
              .limit(PREVIEW_LIMIT)
              .select(({ cm }: any) => ({ id: cm.id, body: cm.body })),
          ),
        }));
    case "view_detail":
      return (qb) =>
        qb.from({ issue: c.issues }).where(({ issue }: any) => eq(issue.id, ctx.detailId)).select(({ issue }: any) => ({
          id: issue.id,
          comments: toArray(
            qb.from({ cm: c.comments }).where(({ cm }: any) => eq(cm.issueID, issue.id)).select(({ cm }: any) => ({
              id: cm.id,
              body: cm.body,
            })),
          ),
        }));
    case "view_page":
      return (qb) => list(qb).offset(VIEW_LIMIT);
  }
}

function read(q: QueryName, rows: Record<string, unknown>[]): { count: number; nested: number } {
  const shape = resultShape(q);
  let nested = 0;
  let acc = 0;
  for (const row of rows) {
    acc += (row.id as number) | 0;
    if (shape === "comments") {
      const comments = row.comments as Array<Record<string, unknown>>;
      nested += comments.length;
      for (const cm of comments) {
        acc += (cm.id as number) | 0;
        if (q === "nested") {
          const creator = cm.creator as Array<Record<string, unknown>>;
          if (creator.length > 0) acc += (creator[0].id as number) | 0;
        }
      }
    } else if (shape === "creator") {
      const creator = row.creator as Array<Record<string, unknown>>;
      nested += creator.length;
      if (creator.length > 0) acc += (creator[0].id as number) | 0;
    } else if (shape === "count") {
      nested += (row.commentCount as number) | 0;
    }
  }
  sink = (sink + acc) | 0;
  return { count: rows.length, nested };
}

export function makeTanstack(dataset: Dataset, scale: Scale): Adapter {
  const issues = createCollection(
    localOnlyCollectionOptions({ getKey: (r: IssueRow) => r.id, initialData: dataset.issues }),
  ) as unknown as Coll<IssueRow>;
  const comments = createCollection(
    localOnlyCollectionOptions({ getKey: (r: CommentRow) => r.id, initialData: dataset.comments }),
  ) as unknown as Coll<CommentRow>;
  const users = createCollection(
    localOnlyCollectionOptions({ getKey: (r: UserRow) => r.id, initialData: dataset.users }),
  ) as unknown as Coll<UserRow>;

  // Indexes for the workload (equality probes → BasicIndex; the ordered top-K → BTreeIndex).
  comments.createIndex((r) => r.issueID, { indexType: BasicIndex });
  comments.createIndex((r) => r.creatorID, { indexType: BasicIndex });
  issues.createIndex((r) => r.creatorID, { indexType: BasicIndex });
  issues.createIndex((r) => r.created, { indexType: BTreeIndex });

  const colls = { issues, comments, users };
  const ids = probeIds(scale);
  const vs = viewSetup(dataset, scale);
  const ctx = { detailId: vs.visibleIssueId };
  const newIssue: IssueRow = {
    id: ids.newIssueId,
    title: "incremental probe issue",
    open: true,
    created: ids.newIssueCreated,
    creatorID: ids.existingCreatorId,
  };
  const newComment: CommentRow = {
    id: ids.newCommentId,
    issueID: ids.existingIssueId,
    body: "incremental probe comment",
    creatorID: ids.existingCreatorId,
  };

  // The reversible add+remove that flows through query `q` (mirrors the Rindle adapter).
  function probeOp(q: QueryName): { coll: Coll<{ id: number }>; row: { id: number } } {
    if (q === "one_to_many" || q === "nested" || q === "aggregate_count")
      return { coll: comments as Coll<{ id: number }>, row: newComment };
    if (q === "view_list_count" || q === "view_list_comments" || q === "view_detail")
      return { coll: comments as Coll<{ id: number }>, row: vs.newVisibleComment };
    if (q === "view_list" || q === "view_list_creator")
      return { coll: issues as Coll<{ id: number }>, row: vs.newTopIssue };
    if (q === "view_page") return { coll: issues as Coll<{ id: number }>, row: vs.newPageIssue };
    return { coll: issues as Coll<{ id: number }>, row: newIssue };
  }

  return {
    name: "TanStack DB",

    hydrate(q: QueryName): HydrateResult {
      const lq = createLiveQueryCollection({ startSync: true, query: build(q, colls, ctx) }) as unknown as LiveQ;
      const sig = read(q, lq.toArray);
      return { handle: lq, count: sig.count, nested: sig.nested };
    },

    async disposeHydrate(handle: unknown): Promise<void> {
      await (handle as LiveQ).cleanup();
    },

    incremental(q: QueryName): Incremental {
      const lq = createLiveQueryCollection({ startSync: true, query: build(q, colls, ctx) }) as unknown as LiveQ;
      const { coll, row } = probeOp(q);
      const pair = () => {
        coll.insert(row);
        coll.delete(row.id);
      };
      return {
        liveCount: () => lq.size,
        pair,
        dispose: () => lq.cleanup(),
      };
    },

    async teardown(): Promise<void> {
      if (sink === 0x7fffffff) process.stderr.write("");
      await Promise.all([issues.cleanup(), comments.cleanup(), users.cleanup()]);
    },
  };
}
