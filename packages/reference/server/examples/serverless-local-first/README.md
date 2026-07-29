# Serverless Local-First Example

The daemon/serverless topology from `RINDLE-SERVER-DESIGN.md`, running as three REAL tiers
over real wires (HTTP + WebSocket):

```text
client materialize(name,args) --HTTP--> API server --HTTP--> daemon mints an opaque lease
client subscribe(lease)       --ws----> daemon streams cv-stamped normalized rows
client mutate(name,args)      --HTTP--> API server authorizes + runs the named mutator
API server approved SQL       --HTTP--> daemon applies it with the lmid discipline
```

Run it from `packages/server/`:

```bash
node --conditions=@rindle/source examples/serverless-local-first/rust-daemon.mjs  # Rust daemon (rindled)
```

`rust-daemon.mjs` boots `rindled`, points `RINDLE_DAEMON_URL`/`_WS`/`_TOKEN` at it, and runs
`app.mjs` against that real wire (`test/daemon-conformance.mjs` pins the contract). To drive
your own already-running daemon, set those env vars and run `app.mjs` directly.

The pieces:

- **daemon** — the Rust `rindled` (`rindle-server`'s network front): one port serving the
  public ws (lease subscriptions only — no names, no ASTs) and the private bearer-auth'd HTTP
  control plane (`/materialize`, `/execute-sql-txn`, `/reject-mutation`, …). The single
  canonical daemon implementation (the in-process JS daemon has been retired).
- **API server** — `createRindleApiServer` (`@rindle/api-server`) behind a plain `node:http`
  front. The app authority: authenticates callers, resolves named queries to ASTs, runs
  named mutators into approved SQL, and forwards `{clientID, mid}` so the daemon advances
  the client's lmid co-transactionally (the confirmation is data).
- **clients** — `RemoteOptimisticSource` with `resolveSubscribe` posting to the API server
  (lease back) and `pushMutation` wrapped in `createQueuedMutationSender`: mutations flush
  as confirmed, in-order batches, restoring the contiguous-mid invariant the unordered
  serverless hop would otherwise break. Policy rejections snap back via the lmid release
  and the *reason* surfaces through the queue's `onRejected`.
