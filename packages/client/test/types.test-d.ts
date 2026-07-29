// Type-level conformance: positive cases must compile; every `@ts-expect-error` must error
// (tsc fails on an unused expect-error). Checked by `tsc --noEmit`; NOT run by `node --test`
// (the filename doesn't match `*.test.ts`).

import { boolean, createSchema, extendSchema, number, string, table } from "../src/schema.ts";
import { and, eq, gt, ilike, like, or } from "../src/operators.ts";
import { defineQuery, exists, newQueryBuilder, queries } from "../src/query.ts";
import type { SingularArrayView } from "../src/view.ts";

const issue = table("issue")
  .columns({ id: string(), title: string(), closed: boolean(), priority: number() })
  .primaryKey("id");
const comment = table("comment")
  .columns({ id: string(), issueID: string(), body: string() })
  .primaryKey("id");
const root = queries(createSchema({ tables: [issue, comment] }));
const selection = table("selection", { local: true })
  .columns({ id: string(), issueID: string() })
  .primaryKey("id");
const extended = queries(extendSchema(createSchema({ tables: [issue, comment] }), { tables: [selection] }), undefined, {
  includeLocal: true,
});
extended.selection.where.issueID("i1").materialize();

// ---- positive: full inference ----
root.issue.where.closed(false).where.priority(gt(3)).whereTitle(like("%x%"));
root.issue.where(
  or(
    issue.priority(gt(8)),
    exists(comment, { parent: ["id"], child: ["issueID"] }, (c) => c.where.body(ilike("%x%"))),
  ),
);
const view = root.issue
  .sub("comments", comment, { parent: ["id"], child: ["issueID"] }, (c) => c.orderBy("id", "asc"))
  .materialize();
// nested result type flows into `.data`
const row: {
  id: string;
  title: string;
  closed: boolean;
  priority: number;
  comments: ReadonlyArray<{ id: string; issueID: string; body: string }>;
} = view.data[0];
void row;

// ---- positive: top-level .one() flips materialize() to a SingularArrayView (R | null) ----
const single: SingularArrayView<{ id: string; title: string; closed: boolean; priority: number }> = root.issue
  .where.closed(false)
  .one()
  .materialize();
const oneRow: { id: string; title: string } | null = single.data;
void oneRow;
// .one() survives further chaining and keeps the singular result type
const stillSingle: SingularArrayView<unknown> = root.issue.one().where.closed(false).orderBy("id", "asc").materialize();
void stillSingle;

// ---- positive: named query helpers preserve the query/view type and expose metadata ----
const qb = newQueryBuilder(createSchema({ tables: [issue, comment] }));
const named = {
  openByPriority: defineQuery("openByPriority", (args: { minPriority: number }) => qb.issue.where.closed(false).where.priority(gt(args.minPriority))),
};
const namedQuery = named.openByPriority({ minPriority: 4 });
const namedName: string | undefined = namedQuery.name;
const namedArgs: unknown = namedQuery.args;
const namedViewData: ReadonlyArray<{ id: string; title: string; closed: boolean; priority: number }> =
  namedQuery.materialize().data;
void namedName;
void namedArgs;
void namedViewData;

// ---- positive: .start() accepts a partial row + optional exclusive flag, stays chainable ----
root.issue.orderBy("priority", "desc").start({ priority: 3, id: "x" }).limit(10);
root.issue.start({ id: "x" }, { exclusive: true });

// ---- negative: must be type errors ----
// @ts-expect-error a plural query's .data is an array, not a single row
const notSingle: { id: string } = root.issue.materialize().data;
void notSingle;
// @ts-expect-error .start() cursor values must match the column types
root.issue.start({ priority: "not-a-number" });
// @ts-expect-error .start() rejects unknown columns
root.issue.start({ nope: 1 });
// @ts-expect-error wrong value type for a boolean field
root.issue.where.closed(gt(3));
// @ts-expect-error unknown field on the proxy
root.issue.where.nope(eq(1));
// @ts-expect-error camelCase sugar enforces the field's type
root.issue.whereClosed(123);
// @ts-expect-error `like` only applies to string fields
root.issue.where.priority(like("x"));
// @ts-expect-error unknown column in orderBy
root.issue.orderBy("nope", "asc");
// @ts-expect-error unknown table on the query root
root.nosuch.where.closed(false);

// ---- positive: a root count() reshapes the result row to the aggregate OUTPUT ----
// global count: one row, only the synthetic `count`
const globalCount: ReadonlyArray<{ count: number }> = root.issue.count().materialize().data;
void globalCount;
// grouped: the group-by column joins the result row beside `count`
const grouped: ReadonlyArray<{ priority: number; count: number }> = root.issue
  .groupBy("priority")
  .count()
  .materialize().data;
void grouped;
// compound group-by accumulates BOTH group columns into the result row
const grouped2: ReadonlyArray<{ priority: number; closed: boolean; count: number }> = root.issue
  .groupBy("priority")
  .groupBy("closed")
  .count()
  .materialize().data;
void grouped2;
// having's binder names the group-by columns AND the synthetic numeric `count`
root.issue.groupBy("priority").count().having((h) => h.count(gt(3)));
root.issue.groupBy("priority").count().having((h) => and(h.priority(gt(1)), h.count(gt(3))));

// ---- negative: aggregate result + having typing is honest ----
// @ts-expect-error a global count row has no base-table columns, only `count`
const badAgg: ReadonlyArray<{ title: string; count: number }> = root.issue.count().materialize().data;
void badAgg;
// @ts-expect-error having cannot name a column that is not in the aggregate output (not grouped)
root.issue.groupBy("priority").count().having((h) => h.title(eq("x")));
// @ts-expect-error the synthetic `count` is numeric — a string predicate is rejected
root.issue.groupBy("priority").count().having((h) => h.count(like("x")));
// @ts-expect-error `count` is not nameable in having until count() reshapes the query
root.issue.groupBy("priority").having((h) => h.count(gt(1)));

// ---- positive: .having(alias, op, n) filters a PARENT by a child count aggregate ----
// Binding an existing countAs alias is allowed; the result row stays the parent row (the display
// countAs still surfaces its scalar), so `.data` keeps the parent shape plus `commentCount`.
const gated: ReadonlyArray<{ id: string; title: string; commentCount: number }> = root.issue
  .countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] })
  .having("commentCount", ">", 10)
  .materialize().data;
void gated;

// ---- negative: .having(alias, …) only accepts a count-aggregate alias ----
// @ts-expect-error no countAs on this query → no aggregate alias to bind
root.issue.having("commentCount", ">", 10);
// @ts-expect-error "nope" is not an attached aggregate alias
root.issue.countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] }).having("nope", ">", 10);
// @ts-expect-error a plain `sub` relationship (a row object, not a numeric count) is not a valid alias
root.issue.sub("comments", comment, { parent: ["id"], child: ["issueID"] }).having("comments", ">", 10);
// @ts-expect-error the threshold must be a number
root.issue.countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] }).having("commentCount", ">", "x");

export {};
