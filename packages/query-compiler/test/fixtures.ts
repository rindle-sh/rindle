// Test catalogs. `issueTracker` is a synthetic catalog that deliberately exercises **every**
// branch of the Postgres `::text::<type>` cast switch (§6.2) — scalar (int4/text/bool/float8),
// numeric, temporal (timestamptz/timestamp/date/timetz/time), uuid, enum, array, jsonb — plus
// both `one` and `many` relationships. It proves the `ColumnType` descriptor can express the
// whole reconciliation surface, and becomes the golden-SQL fixture in M3.
//
// The `node:sqlite` execution oracle (M2) uses a separate chinook-shaped catalog paired with
// real seed rows; that lands with the oracle harness.

import type { Catalog } from "../src/catalog.ts";

export const issueTracker: Catalog = {
  tables: {
    user: {
      columns: ["id", "name", "createdAt"],
      primaryKey: ["id"],
      columnTypes: {
        id: { type: "uuid", isEnum: false, isArray: false },
        name: { type: "text", isEnum: false, isArray: false },
        createdAt: { type: "timestamptz", isEnum: false, isArray: false },
      },
      relationships: {
        authoredIssues: "many",
      },
    },
    issue: {
      columns: [
        "id",
        "title",
        "status",
        "priority",
        "open",
        "score",
        "estimate",
        "dueDate",
        "labels",
        "metadata",
        "createdAt",
        "authorId",
      ],
      primaryKey: ["id"],
      columnTypes: {
        id: { type: "uuid", isEnum: false, isArray: false },
        title: { type: "text", isEnum: false, isArray: false },
        status: { type: "issue_status", isEnum: true, isArray: false },
        priority: { type: "int4", isEnum: false, isArray: false },
        open: { type: "bool", isEnum: false, isArray: false },
        score: { type: "float8", isEnum: false, isArray: false },
        estimate: { type: "numeric", isEnum: false, isArray: false },
        dueDate: { type: "date", isEnum: false, isArray: false },
        labels: { type: "text", isEnum: false, isArray: true },
        metadata: { type: "jsonb", isEnum: false, isArray: false },
        createdAt: { type: "timestamptz", isEnum: false, isArray: false },
        authorId: { type: "uuid", isEnum: false, isArray: false },
      },
      relationships: {
        comments: "many",
        author: "one",
      },
    },
    comment: {
      columns: ["id", "issueId", "body", "editWindow", "createdAt"],
      primaryKey: ["id"],
      columnTypes: {
        id: { type: "uuid", isEnum: false, isArray: false },
        issueId: { type: "uuid", isEnum: false, isArray: false },
        body: { type: "text", isEnum: false, isArray: false },
        editWindow: { type: "timetz", isEnum: false, isArray: false },
        createdAt: { type: "timestamptz", isEnum: false, isArray: false },
      },
      relationships: {},
    },
  },
};
