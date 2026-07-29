# @rindle/normalized

The **normalized local-first** client glue (`NORMALIZED-CHANGES-DESIGN.md` §5/§7) — the
sibling of the flat remote client that runs its **own local DB** and queries over it (so it
can do optimistic writes + local query resolution), with its base tables fed by a server
stream instead of by direct user writes.

This package's Slice 4 surface is **`NormalizedSync`**: the cross-query refcount + GC layer
that sits between the per-query normalized streams (`NormalizedOp` batches, validated by
`@rindle/remote`) and the one shared local store (the `@rindle/wasm` `Db`'s base tables).

- The **server** already de-duplicates *intra-query* multi-path (NormalizeFold, §4.2).
- `NormalizedSync` does the *cross-query* refcount the server deliberately does **not** —
  keyed by `(table, pk)`, each query's footprint a membership set. A row shared by N queries
  lands in the local `Db` **once** and is GC'd only when the last query stops referencing it.
  No CVR — the refcount lives on the client, per row.

It is pure logic: `applyBatch(queryId, ops)` / `rehydrate(queryId, snapshot)` /
`dropQuery(queryId)` each return the NET `Mutation[]` to commit to the wasm `Db` in one
transaction. The `Db` re-materializes and each query's `FlatArrayView` folds the result.

The `NormalizedStore` wiring (remote normalized stream → `NormalizedSync` → wasm `Db`) lands
in Slice 5.

Site docs — the change vocabulary these streams carry:
[rindle.sh/docs/change-model](https://rindle.sh/docs/change-model) · for agents:
[llms.txt](https://rindle.sh/llms.txt)
