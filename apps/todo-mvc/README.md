# Rindle TODO MVC

A small TanStack Start app for exercising fragment-local reads.

- `TodoListFragment` composes the single root query with `TodoItemFragment`.
- `TodoItem` reads each row with `useFragment(TodoItemFragment, todo)`.
- Moving across a todo uses `app.mutate.setColor.folded(...)`.
- Row and list render counters are visible in the UI so pointer movement can verify that only the changed row re-renders.

Run with:

```sh
pnpm --filter @rindle/todo-mvc dev
```

`pnpm dev` uses `rindle dev` as one lifecycle boundary: it evaluates `rindle.ncl`, supervises and
waits for the write-master, follower, and fleet edge, applies migrations, generates the schema, then
starts Vite with the unified server bindings. The browser discovers its WebSocket endpoint and
affinity ticket from the first query lease. In this monorepo, the local `rindle` script points
`@rindle/cli` at `../../target/debug`; if those binaries are missing, build them once from the repo
root:

```sh
cargo build -p rindle-cli -p rindle-server -p rindle-replicator -p rindle-dev-edge --bins
```

Useful scripts:

```sh
pnpm --filter @rindle/todo-mvc rindle status
```
