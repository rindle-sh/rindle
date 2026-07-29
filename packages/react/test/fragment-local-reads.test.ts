import { test } from "node:test";
import assert from "node:assert/strict";

import { JSDOM } from "jsdom";

const dom = new JSDOM('<!DOCTYPE html><html><body><div id="root"></div></body></html>');
const g = globalThis as Record<string, unknown>;
const win = dom.window as unknown as Record<string, unknown>;
const setGlobal = (k: string, v: unknown) => Object.defineProperty(g, k, { value: v, configurable: true, writable: true });
setGlobal("window", dom.window);
setGlobal("document", dom.window.document);
setGlobal("navigator", win.navigator);
for (const k of ["HTMLElement", "Element", "Node", "Text", "DocumentFragment", "Event", "customElements"]) {
  setGlobal(k, win[k]);
}
g.IS_REACT_ACT_ENVIRONMENT = true;

const { act, createElement: h, memo, useState } = await import("react");
const { createRoot } = await import("react-dom/client");

const {
  createSchema,
  defineFragment,
  defineQuery,
  newQueryBuilder,
  OneShotBackend,
  stableKey,
  Store,
  string,
  table,
} = await import("@rindle/client");
import type {
  Ast,
  Backend,
  ChangeEvent,
  FlatChange,
  FragmentRef,
  Mutation,
  QueryId,
  RemoteQuery,
  ResultType,
  WireSchema,
} from "@rindle/client";

const { Rindle, fragmentKey, useFragment, useRoot } = await import("../src/index.ts");

const comment = table("comment")
  .columns({ id: string(), issueId: string(), body: string() })
  .primaryKey("id");
const issue = table("issue").columns({ id: string(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [comment, issue] });
const qb = newQueryBuilder(schema);

const CommentRow = defineFragment(comment, (f) => f.select("id", "body"));
const IssueCard = defineFragment(issue, (f) =>
  f.select("id", "title").sub("comments", comment, { parent: ["id"], child: ["issueId"] }, CommentRow),
);
const IssueInlineComments = defineFragment(issue, (f) =>
  f.select("id", "title").sub("comments", comment, { parent: ["id"], child: ["issueId"] }, (c) => c.select("id", "body")),
);
const IssueCoverage = defineQuery("issueCoverage", (args: { id: string }) =>
  qb.issue.where.id(args.id).include(IssueCard).one(),
);
const SingleIssueCoverage = defineQuery("singleIssueCoverage", () =>
  qb.issue.where.id("i1").include(IssueCard).one(),
);
const IssueInlineCoverage = defineQuery("issueInlineCoverage", (args: { id: string }) =>
  qb.issue.where.id(args.id).include(IssueInlineComments).one(),
);
const coverageQuery = IssueCoverage({ id: "i1" });

const rows = {
  issue: { id: "i1", title: "Fix the bug" },
  comments: new Map([
    ["c1", "first comment"],
    ["c2", "second comment"],
  ]),
};

function literalID(ast: Ast): string {
  const scan = (where: Ast["where"]): string | undefined => {
    if (!where) return undefined;
    if (where.type === "and") {
      for (const c of where.conditions) {
        const found = scan(c);
        if (found !== undefined) return found;
      }
      return undefined;
    }
    if (where.type !== "simple" || where.op !== "=") return undefined;
    if (where.left.type === "column" && where.left.name === "id" && where.right.type === "literal") {
      return String(where.right.value);
    }
    if (where.right.type === "column" && where.right.name === "id" && where.left.type === "literal") {
      return String(where.left.value);
    }
    return undefined;
  };
  const id = scan(ast.where);
  if (id === undefined) throw new Error(`test backend expected an id equality in ${JSON.stringify(ast)}`);
  return id;
}

function add(row: unknown[], rels: Array<{ rel: number; children: Array<{ row: unknown[]; rels: [] }> }> = []): FlatChange {
  return { path: [], op: { tag: "add", node: { row, rels } } } as FlatChange;
}

class LocalFirstBackend implements Backend {
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  private resultTypeHandler: (qid: QueryId, rt: ResultType) => void = () => {};
  readonly registered: Array<{ qid: QueryId; ast: Ast; remote?: RemoteQuery }> = [];
  readonly unregistered: QueryId[] = [];
  readonly remoteRetained: Array<{ qid: QueryId; remote: RemoteQuery; localQueryId?: QueryId }> = [];
  readonly remoteReleased: QueryId[] = [];
  readonly commentQids = new Map<string, QueryId>();
  readonly commentColumns = new Map<QueryId, string[]>();
  private issueQid: QueryId | undefined;

  registerQuery(qid: QueryId, ast: Ast, remote?: RemoteQuery): void {
    assert.equal(remote, undefined, "fragment local reads must not retain remote subscriptions");
    this.registered.push({ qid, ast, remote });
    if (ast.table === "issue") this.registerIssue(qid, ast);
    else if (ast.table === "comment") this.registerComment(qid, ast);
    else throw new Error(`unexpected table ${ast.table}`);
  }

  unregisterQuery(qid: QueryId): void {
    this.unregistered.push(qid);
  }

  retainRemoteQuery(qid: QueryId, remote: RemoteQuery, localQueryId?: QueryId): void {
    this.remoteRetained.push({ qid, remote, localQueryId });
    this.resultTypeHandler(qid, "complete");
  }

  releaseRemoteQuery(qid: QueryId): void {
    this.remoteReleased.push(qid);
  }

  mutate(_mutations: Mutation[]): Promise<void> {
    return Promise.resolve();
  }

  onEvent(handler: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = handler;
  }

  onResultType(handler: (qid: QueryId, rt: ResultType) => void): void {
    this.resultTypeHandler = handler;
  }

  editComment(id: string, body: string): void {
    const qid = this.commentQids.get(id);
    if (qid === undefined) throw new Error(`comment ${id} was not registered`);
    const old = rows.comments.get(id)!;
    rows.comments.set(id, body);
    const cols = this.commentColumns.get(qid) ?? ["id", "body"];
    this.handler(qid, {
      type: "batch",
      events: [{
        path: [],
        op: {
          tag: "edit",
          old: cols.map((col) => (col === "id" ? id : col === "body" ? old : "i1")),
          new: cols.map((col) => (col === "id" ? id : col === "body" ? body : "i1")),
        },
      } as FlatChange],
    });
  }

  addComment(id: string, body: string): void {
    if (this.issueQid === undefined) throw new Error("issue local query was not registered");
    rows.comments.set(id, body);
    this.handler(this.issueQid, {
      type: "batch",
      events: [{
        path: [{ rel: 0, parentRow: [rows.issue.id, rows.issue.title] }],
        op: { tag: "add", node: { row: [id], rels: [] } },
      } as FlatChange],
    });
  }

  private registerIssue(qid: QueryId, ast: Ast): void {
    this.issueQid = qid;
    const child = ast.related?.[0]?.subquery;
    if (child !== undefined) {
      assert.deepEqual(
        child.select,
        child.select?.includes("body") ? ["body", "id"] : ["id"],
        "fragment children keep only identity columns while inline children keep their payload",
      );
    }
    const childColumns = child?.select ?? ["id"];
    const schema: WireSchema = {
      columns: ast.select ?? ["id", "title"],
      primaryKey: [0],
      sort: [[0, true]],
      singular: ast.one === true,
      relationships: child === undefined
        ? []
        : [
            {
              name: "comments",
              slot: 0,
              child: {
                columns: childColumns,
                primaryKey: [childColumns.indexOf("id")],
                sort: [[childColumns.indexOf("id"), true]],
                singular: false,
                relationships: [],
              },
            },
          ],
    };
    this.handler(qid, { type: "hello", schema, comparatorVersion: 1 });
    const rels: Array<{ rel: number; children: Array<{ row: unknown[]; rels: [] }> }> = child === undefined
      ? []
      : [{
          rel: 0,
          children: [...rows.comments.keys()].map((id) => ({
            row: childColumns.map((col) => (col === "id" ? id : col === "body" ? rows.comments.get(id)! : "i1")),
            rels: [],
          })),
        }];
    this.handler(qid, {
      type: "snapshot",
      adds: [
        add(
          schema.columns.map((col) => rows.issue[col as "id" | "title"]),
          rels,
        ),
      ],
      last: true,
    });
  }

  private registerComment(qid: QueryId, ast: Ast): void {
    const id = literalID(ast);
    this.commentQids.set(id, qid);
    const schema: WireSchema = {
      columns: ast.select ?? ["id", "issueId", "body"],
      primaryKey: [(ast.select ?? ["id", "issueId", "body"]).indexOf("id")],
      sort: [[(ast.select ?? ["id", "issueId", "body"]).indexOf("id"), true]],
      singular: ast.one === true,
      relationships: [],
    };
    this.commentColumns.set(qid, schema.columns);
    this.handler(qid, { type: "hello", schema, comparatorVersion: 1 });
    this.handler(qid, {
      type: "snapshot",
      adds: [add(schema.columns.map((col) => (col === "id" ? id : col === "body" ? rows.comments.get(id)! : "i1")))],
      last: true,
    });
  }
}

// Same as LocalFirstBackend, but the coverage lease NEVER reports "complete" — it models a server
// round-trip still in flight while the local IVM view already holds rows (synced earlier, optimistic,
// or hydrated). Used to prove the stale-while-revalidate reads: data renders off the local view
// rather than blanking to a loading state until the server confirms coverage.
class StalledCoverageBackend extends LocalFirstBackend {
  override retainRemoteQuery(qid: QueryId, remote: RemoteQuery, localQueryId?: QueryId): void {
    this.remoteRetained.push({ qid, remote, localQueryId });
    // intentionally do NOT call onResultType("complete"): coverage stays `unknown`.
  }
}

let issueRenders = 0;
const commentRenders = new Map<string, number>();

const CommentView = memo(function CommentView({ comment }: { comment: FragmentRef<typeof CommentRow> }) {
  const data = useFragment(CommentRow, comment);
  if (!data) return null;
  commentRenders.set(data.id, (commentRenders.get(data.id) ?? 0) + 1);
  return h("li", null, data.body);
});

function IssueView() {
  const [data] = useRoot(SingleIssueCoverage);
  if (!data) return null;
  issueRenders++;
  return h(
    "section",
    null,
    h("h1", null, data.title),
    h(
      "ul",
      null,
      data.comments.map((ref) => h(CommentView, { key: fragmentKey(ref), comment: ref })),
    ),
  );
}

function Screen() {
  return h(IssueView);
}

function InlineIssueView({ issue: issueRef }: { issue: FragmentRef<typeof IssueInlineComments> }) {
  const data = useFragment(IssueInlineComments, issueRef);
  if (!data) return null;
  return h(
    "section",
    null,
    h("h1", null, data.title),
    h("p", { id: "inline-comments" }, data.comments.map((comment) => comment.body).join(", ")),
  );
}

function InlineScreen() {
  const [root] = useRoot(IssueInlineCoverage, { id: "i1" }, IssueInlineComments);
  return root ? h(InlineIssueView, { issue: root }) : h("div", null, "loading");
}

test("fragment local reads isolate child-only updates from the parent reader", async () => {
  issueRenders = 0;
  commentRenders.clear();
  rows.comments.set("c1", "first comment");
  rows.comments.set("c2", "second comment");

  const backend = new LocalFirstBackend();
  const store = new Store(schema, backend);
  const container = dom.window.document.getElementById("root")!;

  let root: ReturnType<typeof createRoot> | undefined;
  await act(async () => {
    root = createRoot(container);
    root.render(h(Rindle, { store }, h(Screen)));
  });

  assert.equal(container.querySelector("h1")?.textContent, "Fix the bug");
  assert.deepEqual(
    [...container.querySelectorAll("li")].map((li) => li.textContent),
    ["first comment", "second comment"],
  );
  assert.equal(issueRenders, 1);
  assert.deepEqual(Object.fromEntries(commentRenders), { c1: 1, c2: 1 });
  assert.equal(backend.remoteRetained.length, 1, "the shared coverage query is retained without materializing a view");
  assert.equal(new Set(backend.remoteRetained.map((r) => stableKey(r.remote))).size, 1);

  const beforeParentLocalAst = backend.registered.find((r) => r.ast.table === "issue")?.ast;
  assert.ok(beforeParentLocalAst, "parent local query was registered");
  assert.equal(stableKey(beforeParentLocalAst).includes("body"), false, "parent local AST does not contain child body");

  await act(async () => {
    backend.editComment("c1", "updated comment");
  });

  assert.deepEqual(
    [...container.querySelectorAll("li")].map((li) => li.textContent),
    ["updated comment", "second comment"],
  );
  assert.equal(issueRenders, 1, "the parent fragment reader did not rerender for a child-only field edit");
  assert.deepEqual(Object.fromEntries(commentRenders), { c1: 2, c2: 1 });

  await act(async () => {
    backend.addComment("c3", "third comment");
  });

  assert.deepEqual(
    [...container.querySelectorAll("li")].map((li) => li.textContent),
    ["updated comment", "second comment", "third comment"],
  );
  assert.equal(issueRenders, 2, "the parent fragment reader rerenders for a child identity change");
  assert.deepEqual(
    Object.fromEntries(commentRenders),
    { c1: 2, c2: 1, c3: 1 },
    "stable child fragment refs let memoized existing child components bail out",
  );

  await act(async () => {
    root!.unmount();
  });
});

test("inline relationships preserve their selected payload in local fragment reads", async () => {
  rows.comments.clear();
  rows.comments.set("c1", "first comment");
  rows.comments.set("c2", "second comment");

  const backend = new LocalFirstBackend();
  const store = new Store(schema, backend);
  const container = dom.window.document.createElement("div");
  dom.window.document.body.appendChild(container);

  let root: ReturnType<typeof createRoot> | undefined;
  await act(async () => {
    root = createRoot(container);
    root.render(h(Rindle, { store }, h(InlineScreen)));
  });

  assert.equal(container.querySelector("h1")?.textContent, "Fix the bug");
  assert.equal(container.querySelector("#inline-comments")?.textContent, "first comment, second comment");

  const parentLocalAst = backend.registered.find((r) => r.ast.table === "issue" && r.ast.related !== undefined)?.ast;
  assert.ok(parentLocalAst, "inline parent local query was registered");
  assert.deepEqual(parentLocalAst.related?.[0]?.subquery.select, ["body", "id"]);

  await act(async () => {
    root!.unmount();
  });
  container.remove();
});

function OneShotScreen() {
  const [issue, { status }] = useRoot(IssueCoverage, { id: "i1" });
  return h("div", null, `${status}:${issue ? `${issue.title}:${issue.comments.length}` : "none"}`);
}

test("root can mount on a one-shot SSR seed store without a sync-only remote lease", async () => {
  const store = new Store(schema, new OneShotBackend());
  store.hydrate({
    [stableKey(coverageQuery.ast())]: {
      cvMin: 1,
      rows: [{ id: "i1", title: "Fix the bug", comments: [{ id: "c1" }, { id: "c2" }] }],
    },
  });
  const container = dom.window.document.getElementById("root")!;

  let root: ReturnType<typeof createRoot> | undefined;
  await act(async () => {
    root = createRoot(container);
    root.render(h(Rindle, { store }, h(OneShotScreen)));
  });

  assert.equal(container.textContent, "complete:Fix the bug:2");

  await act(async () => {
    root!.unmount();
  });
});

test("stale-while-revalidate: a boundary + ref fragments render locally-known rows before coverage is server-complete", async () => {
  issueRenders = 0;
  commentRenders.clear();
  rows.comments.clear();
  rows.comments.set("c1", "first comment");
  rows.comments.set("c2", "second comment");

  // Coverage round-trip is in flight (never reports "complete"), but the local views already hold
  // the issue + its comments. Pre-fix, useRoot/useFragment blanked to empty until status flipped —
  // so this rendered nothing and flashed a loading state on every navigation. Now it renders the
  // local rows immediately; `status` is the separate "server-authoritative yet" axis.
  const backend = new StalledCoverageBackend();
  const store = new Store(schema, backend);
  const container = dom.window.document.createElement("div");
  dom.window.document.body.appendChild(container);

  let root: ReturnType<typeof createRoot> | undefined;
  await act(async () => {
    root = createRoot(container);
    root.render(h(Rindle, { store }, h(Screen)));
  });

  assert.equal(container.querySelector("h1")?.textContent, "Fix the bug");
  assert.deepEqual(
    [...container.querySelectorAll("li")].map((li) => li.textContent),
    ["first comment", "second comment"],
  );
  assert.equal(backend.remoteRetained.length, 1, "coverage was retained — the round-trip is still in flight");

  await act(async () => {
    root!.unmount();
  });
  container.remove();
});

test("stale-while-revalidate: a root-ref boundary with inline children renders before coverage completes", async () => {
  rows.comments.clear();
  rows.comments.set("c1", "first comment");
  rows.comments.set("c2", "second comment");

  const backend = new StalledCoverageBackend();
  const store = new Store(schema, backend);
  const container = dom.window.document.createElement("div");
  dom.window.document.body.appendChild(container);

  let root: ReturnType<typeof createRoot> | undefined;
  await act(async () => {
    root = createRoot(container);
    root.render(h(Rindle, { store }, h(InlineScreen)));
  });

  // The with-fragment boundary (useRootRefData) and the nested inline useFragment both project off
  // the local view despite coverage being `unknown` — so this shows the rows, not "loading".
  assert.equal(container.querySelector("h1")?.textContent, "Fix the bug");
  assert.equal(container.querySelector("#inline-comments")?.textContent, "first comment, second comment");
  assert.notEqual(container.textContent, "loading");

  await act(async () => {
    root!.unmount();
  });
  container.remove();
});

test("filter changes read synchronously from locally retained coverage while the new coverage loads", async () => {
  const filterIssue = table("filter_issue")
    .columns({ id: string(), title: string(), status: string() })
    .primaryKey("id");
  const filterSchema = createSchema({ tables: [filterIssue] });
  const filterQb = newQueryBuilder(filterSchema);
  const issuesByStatus = defineQuery("issuesByStatus", (args: { status: "all" | "todo" }) => {
    const q = args.status === "all" ? filterQb.filter_issue : filterQb.filter_issue.where.status(args.status);
    return q.orderBy("id", "asc");
  });

  const rowsById = new Map([
    ["i1", { id: "i1", title: "todo issue", status: "todo" }],
    ["i2", { id: "i2", title: "done issue", status: "done" }],
  ]);

  class FilterWarmBackend implements Backend {
    private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
    private resultTypeHandler: (qid: QueryId, rt: ResultType) => void = () => {};
    readonly remoteReleased: QueryId[] = [];
    private readonly activeCoverage = new Map<QueryId, Set<string>>();

    registerQuery(qid: QueryId, ast: Ast, remote?: RemoteQuery): void {
      assert.equal(remote, undefined);
      const columns = ast.select ?? ["id", "title", "status"];
      this.handler(qid, {
        type: "hello",
        comparatorVersion: 1,
        schema: {
          columns,
          primaryKey: [columns.indexOf("id")],
          sort: [[columns.indexOf("id"), true]],
          singular: false,
          relationships: [],
        },
      });
      this.handler(qid, {
        type: "snapshot",
        adds: this.evaluate(ast).map((row) => add(columns.map((col) => row[col as "id" | "title" | "status"]))),
        last: true,
      });
    }

    unregisterQuery(_qid: QueryId): void {}

    retainRemoteQuery(qid: QueryId, remote: RemoteQuery): void {
      if ((remote.args as { status?: string }).status === "all") {
        this.activeCoverage.set(qid, new Set(rowsById.keys()));
        this.resultTypeHandler(qid, "complete");
      } else {
        this.resultTypeHandler(qid, "unknown");
      }
    }

    releaseRemoteQuery(qid: QueryId): void {
      this.remoteReleased.push(qid);
      this.activeCoverage.delete(qid);
    }

    mutate(_mutations: Mutation[]): Promise<void> {
      return Promise.resolve();
    }

    onEvent(handler: (qid: QueryId, ev: ChangeEvent) => void): void {
      this.handler = handler;
    }

    onResultType(handler: (qid: QueryId, rt: ResultType) => void): void {
      this.resultTypeHandler = handler;
    }

    private evaluate(ast: Ast): Array<{ id: string; title: string; status: string }> {
      const covered = new Set<string>();
      for (const ids of this.activeCoverage.values()) for (const id of ids) covered.add(id);
      const status = statusEq(ast.where);
      return [...rowsById.values()].filter((row) => covered.has(row.id) && (status === undefined || row.status === status));
    }
  }

  function statusEq(where: Ast["where"]): string | undefined {
    if (!where) return undefined;
    if (where.type === "and") {
      for (const c of where.conditions) {
        const found = statusEq(c);
        if (found !== undefined) return found;
      }
      return undefined;
    }
    if (where.type !== "simple" || where.op !== "=") return undefined;
    if (where.left.type === "column" && where.left.name === "status" && where.right.type === "literal") {
      return String(where.right.value);
    }
    if (where.right.type === "column" && where.right.name === "status" && where.left.type === "literal") {
      return String(where.left.value);
    }
    return undefined;
  }

  function FilterScreen() {
    const [status, setStatus] = useState<"all" | "todo">("all");
    const [issues, { status: resultStatus }] = useRoot(issuesByStatus, { status });
    return h(
      "section",
      null,
      h("button", { type: "button", onClick: () => setStatus("todo") }, "todo"),
      h("p", { id: "result-status" }, resultStatus),
      h("ul", null, issues.map((issue) => h("li", { key: issue.id }, issue.title))),
    );
  }

  const backend = new FilterWarmBackend();
  const store = new Store(filterSchema, backend);
  const container = dom.window.document.createElement("div");
  dom.window.document.body.appendChild(container);

  let root: ReturnType<typeof createRoot> | undefined;
  await act(async () => {
    root = createRoot(container);
    root.render(h(Rindle, { store }, h(FilterScreen)));
  });

  assert.deepEqual(
    [...container.querySelectorAll("li")].map((li) => li.textContent),
    ["todo issue", "done issue"],
  );

  await act(async () => {
    container.querySelector("button")!.dispatchEvent(new dom.window.MouseEvent("click", { bubbles: true }));
  });

  assert.equal(container.querySelector("#result-status")?.textContent, "unknown");
  assert.deepEqual(
    [...container.querySelectorAll("li")].map((li) => li.textContent),
    ["todo issue"],
    "the new filtered AST reads the row already held by the previous coverage",
  );
  assert.deepEqual(backend.remoteReleased, [], "old coverage stays warm during the stale-read window");

  await act(async () => {
    root!.unmount();
  });
  container.remove();
});
