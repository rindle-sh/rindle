// The app authority, runtime-AGNOSTIC. This is the serverless-shaped tier's actual logic:
// it resolves named queries to ASTs (the daemon mints opaque leases), runs the AUTHORITATIVE
// mutators into approved SQL across the four normalized tables, enforces policy (the "spam"
// rejection, the ownership-gated delete, cursor validation), and talks to the daemon over the
// private bearer-auth'd control plane.
//
// It is deliberately free of any host (`node:http`, Workers `fetch`, …): TanStack Start server
// routes wrap it for normal dev, `server/api.ts` can wrap it in a standalone Node HTTP server for
// focused harnesses, and `worker.ts` installs Cloudflare bindings for the Start server routes in
// the deployed demo. The only per-host input is the daemon's control-plane URL + bearer token (and,
// optionally, the `fetch` implementation).

import {
  createRindleApiServer,
  defineApiMutators,
  registerQueries,
  runSharedMutation,
  sharedApiMutators,
  SplitDaemonClient,
} from "@rindle/api-server";
import type {
  ApiMutator,
  ApiMutators,
  MutationContext,
  MutatorCtx,
  RindleApiServer,
  ServerMutationTx,
  SharedMutatorWithArgs,
} from "@rindle/api-server";
import { HttpRindleDaemonClient } from "@rindle/daemon-client";
import type { Fetch } from "@rindle/sql-client";

import {
  addTagArgs,
  issueIdArgs,
  mutators as sharedMutators,
  normalizeOwner,
  normalizeTagName,
  normalizeTitle,
  schema,
} from "../shared/app-def.ts";
import { issuesPageQuery, myIssuesQuery } from "../src/components/IssueListItem.queries.ts";
import { issueDetailQuery } from "../src/components/IssueDetail.queries.ts";
import { recentCommentsQuery } from "../src/components/ActivityFeed.queries.ts";
import { usersQuery } from "../src/components/UserBadge.queries.ts";
import { tagOptionsQuery } from "../src/components/TagChip.queries.ts";

/** The demo's "user" rides a header (`x-user`); a real deployment verifies a session/JWT. */
export type User = string | undefined;

// The authority's query surface is just the list of co-located client queries. Each `defineQuery`
// already carries its wire name and re-runs its validator on the UNTRUSTED wire args before building
// the AST — so a malformed client can't smuggle a garbage limit or an unbounded/ill-typed filter in,
// and the authority builds the exact same nested query the client named. `registerQueries` wires
// them up by name (and forwards the request's authoritative `ApiContext` to context-scoped queries
// like `myIssues`, which is built from the server's own principal — never a client-supplied owner);
// the per-query validation lives next to each query (its `*.queries.ts`). Were the server to need to
// DIVERGE from the client, it would `defineApiQueries` a server-specific resolver with the same name
// and register that instead.
const apiQueries = registerQueries<User>([
  issuesPageQuery,
  myIssuesQuery,
  issueDetailQuery,
  recentCommentsQuery,
  usersQuery,
  tagOptionsQuery,
]);

// MUTATORS ARE ISOMORPHIC — defined ONCE (shared/app-def.ts `mutators`) as generators that carry their
// own arg schema (`shared(schema, gen)`) and run on BOTH tiers. `sharedApiMutators` auto-drives the
// WHOLE registry below: for each it parses the untrusted wire args, injects this tier's authority
// (`sharedCtx`), and hands the body to `runSharedMutation`, which renders dialect-correct SQL against
// the authoritative HCTree transaction. No hand-quoting is needed on the logical path; the renderer
// double-quotes identifiers and the write master commits the resulting statements in one order.
//
// AUTHORITY that the client must NOT predict is the ONLY thing that stays an explicit entry, each
// OVERRIDING its auto-wrapped default: the author is always the AUTHENTICATED `ctx.user` (`sharedCtx`,
// not a client arg, injected for EVERY shared body); `cleanTitle` throws → rejection (`withTitleGuard`,
// so the demo's spam path stays exercised). Two mutators can't be expressed as keyed ops at all, so
// they keep the raw `tx.exec` escape hatch: `addTag`'s dedup-by-NAME (the tag pk is `id`) and
// `deleteIssue`'s owner-gated cascade. Those raw `?` statements run through the master's SQL
// transaction alongside the rendered logical operations.

type ServerCtx = MutationContext<User>;

/** The {@link MutatorCtx} a shared body sees on the server: the AUTHENTICATED principal (throws if
 *  absent — a business rejection). Never a client-supplied author. */
function sharedCtx(ctx: ServerCtx): MutatorCtx {
  return { user: requireUser(ctx.user) };
}

/** Wrap a shared mutator with the server-only TITLE policy: `cleanTitle` throws on an empty or spam
 *  title BEFORE any write — the authority the client deliberately does NOT predict, so the demo's
 *  rejection path stays exercised end to end. Parses the untrusted args through the mutator's
 *  co-located `.args`, then drives the SAME body the client predicts. The body re-normalizes
 *  (idempotent). */
function withTitleGuard<A extends { title: string }>(gen: SharedMutatorWithArgs<A>): ApiMutator<User, unknown> {
  return (tx, raw, ctx) => {
    const a = gen.args.parse(raw);
    cleanTitle(a.title);
    return runSharedMutation(gen, a, sharedCtx(ctx), tx);
  };
}

const apiMutators = defineApiMutators<User, ApiMutators<User>>({
  // Every shared mutator, auto-driven by the UNIVERSAL server triad (parse the untrusted args via each
  // mutator's co-located schema, inject the AUTHENTICATED principal via `sharedCtx`, drive the SAME
  // body the client predicts). The mutator twin of `registerQueries` above: a shared mutator whose
  // server run adds NO authority beyond that triad needs no hand-written wrapper.
  ...sharedApiMutators(sharedMutators, sharedCtx),

  // The ONLY entries that stay explicit — server-only AUTHORITY the client must NOT predict, each
  // OVERRIDING its auto-wrapped default above.
  createIssue: withTitleGuard(sharedMutators.createIssue),
  editTitle: withTitleGuard(sharedMutators.editTitle),

  // No duplicate tag name per issue — guard in SQL so a racing client can't double it (the tag pk is
// `id`, so no keyed insertIgnore/upsert dedups by NAME). Raw `?` exec. The
  // client predicts a plain insert (shared `addTag`) and snaps back if this dedup rejects the row.
  addTag: async (tx: ServerMutationTx, raw: unknown) => {
    const a = addTagArgs.parse(raw);
    const name = normalizeTagName(a.name);
    if (!name) return;
    tx.exec(
      `INSERT INTO "tag" ("id", "issueId", "name")
       SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM "tag" WHERE "issueId" = ? AND "name" = ?)`,
      [a.id, a.issueId, name, a.issueId, name],
    );
    await tx.update("issue", { id: a.issueId, updatedAt: a.updatedAt });
  },
  // Ownership enforced IN the SQL: a non-owner's delete is accepted-but-no-op (the gating subquery
  // yields no id), and the client's optimistic delete (shared `deleteIssue`) snaps back on the
  // confirmation release. The children cascade — but only when the owner check passes. Raw `?` exec:
  // an owner-gated cascade is relational authority a keyed `tx.delete` can't express. Order matters —
  // the child deletes read the issue's `ownerId`, so the issue row must be deleted LAST.
  deleteIssue: async (tx: ServerMutationTx, raw: unknown, ctx: MutationContext<User>) => {
    const { id } = issueIdArgs.parse(raw);
    const user = requireUser(ctx.user);
    tx.exec(
      'DELETE FROM "comment" WHERE "issueId" = (SELECT "id" FROM "issue" WHERE "id" = ? AND "ownerId" = ?)',
      [id, user],
    );
    tx.exec('DELETE FROM "tag" WHERE "issueId" = (SELECT "id" FROM "issue" WHERE "id" = ? AND "ownerId" = ?)', [
      id,
      user,
    ]);
    tx.exec('DELETE FROM "issue" WHERE "id" = ? AND "ownerId" = ?', [id, user]);
  },
});

export interface IssueApiOptions {
  /** The daemon's private control-plane base URL (bearer-auth'd). In the one-topology shape
   *  (design 214) this is the READ leg: the follower rindled. */
  daemonUrl: string;
  /** The shared bearer token (daemon ↔ this tier ONLY — never reaches the browser). */
  daemonToken: string;
  /** The replicator write-master's ingress URL (design 214). Required for the daemon backend;
   *  writes + mutation sessions route there while reads keep going to `daemonUrl`. */
  replicatorUrl?: string;
  /** Bearer token for the replicator's write ingress, when it is token-gated. */
  replicatorToken?: string;
  /** Bearer token for the replicator's PUBLIC `/v1/sql/*` surface, which is where authoritative
   *  mutations now execute. It shares the write ingress listener, so it needs no separate URL — but
   *  it MUST be a different secret from `replicatorToken`: the replicator hard-errors at startup
   *  when the private and SQL credentials are equal. */
  databaseToken?: string;
  /** Override the daemon HTTP transport (defaults to global `fetch`). */
  fetch?: Fetch;
}

/** Build the configured API server. Stateless: safe to construct per-request (Worker) or once
 *  per process (Node). */
export function createIssueApi(opts: IssueApiOptions): RindleApiServer<User> {
  if (!opts.replicatorUrl) {
    throw new Error(
      "RINDLE_REPLICATOR_URL is required for daemon-backed writes; followers are read-only",
    );
  }
  if (!opts.databaseToken) {
    throw new Error(
      "RINDLE_DATABASE_TOKEN is required: authoritative mutations execute over the replicator's " +
        "public SQL surface, and its credential must differ from RINDLE_REPLICATOR_TOKEN",
    );
  }
  const reads = new HttpRindleDaemonClient({
    baseUrl: opts.daemonUrl,
    headers: { authorization: `Bearer ${opts.daemonToken}` },
    fetch: opts.fetch,
  });
  return createRindleApiServer<User>({
    // One topology: writes always land on the HCTree master; reads stay on the follower.
    daemon: new SplitDaemonClient(
      new HttpRindleDaemonClient({
        baseUrl: opts.replicatorUrl,
        headers: opts.replicatorToken
          ? { authorization: `Bearer ${opts.replicatorToken}` }
          : undefined,
        fetch: opts.fetch,
      }),
      reads,
    ),
    // Authoritative mutations execute over the versioned public SQL transport; `daemon` above keeps
    // serving query leases, SSR reads, materializations and room control.
    database: { url: opts.replicatorUrl, authToken: opts.databaseToken, fetch: opts.fetch },
    // `schema` drives the dialect SQL renderer for the LOGICAL mutator writes (tx.insert/update/…).
    schema,
    queries: apiQueries,
    mutators: apiMutators,
    authorizeQuery: ({ user }) => typeof user === "string" && user.length > 0,
    authorizeMutation: ({ user }) => typeof user === "string" && user.length > 0,
  });
}

function fromEnv(name: string): string | undefined {
  return typeof process !== "undefined" ? process.env?.[name] : undefined;
}

/** The first of `names` that is set, or a throw naming all of them.
 *
 *  There is deliberately NO fixed-port fallback anywhere in this app. The local fleet's ports are
 *  allocated per project, so `http://127.0.0.1:7600` is no longer "the daemon" — it is whatever
 *  other Rindle project happens to hold that port, and a silent default would read its data. `pnpm
 *  dev` injects every one of these from the rendered `rindle.json` bindings; `rindle render` prints
 *  the resolved URLs. */
export function requiredEnv(...names: string[]): string {
  for (const name of names) {
    const value = fromEnv(name);
    if (value) return value;
  }
  throw new Error(
    `${names.join(" or ")} is required — the fleet's ports are allocated per project, so there is ` +
      "no local port to fall back to. Run through `pnpm dev`, or take the URL from rindle.json's bindings.",
  );
}

let configuredIssueApi: IssueApiOptions | undefined;

/** Install host-provided daemon config for environments where bindings do not live on process.env
 *  (Cloudflare Workers). Safe at module scope because the values are deployment constants. */
export function configureIssueApi(opts: IssueApiOptions): void {
  configuredIssueApi = opts;
}

/** Build the API from process/env defaults for TanStack Start server routes in dev/preview. */
export function createIssueApiFromEnv(): RindleApiServer<User> {
  return createIssueApi(
    configuredIssueApi ?? {
      daemonUrl: requiredEnv("RINDLE_DAEMON_URL", "DAEMON_ORIGIN"),
      daemonToken: fromEnv("RINDLE_DAEMON_TOKEN") ?? fromEnv("DAEMON_TOKEN") ?? "dev-daemon-token",
      replicatorUrl: fromEnv("RINDLE_REPLICATOR_URL") ?? fromEnv("REPLICATOR_ORIGIN"),
      replicatorToken: fromEnv("RINDLE_REPLICATOR_TOKEN") ?? fromEnv("WRITE_TOKEN"),
      databaseToken: fromEnv("RINDLE_DATABASE_TOKEN") ?? fromEnv("SQL_TOKEN"),
    },
  );
}

/** Map an error thrown out of the API server (or body parsing) to an HTTP status + message —
 *  shared by both host shells. `RindleApiError` carries its own status (400/403/404); anything
 *  else is a 500. */
export function httpErrorOf(err: unknown): { status: number; message: string } {
  const status = typeof err === "object" && err !== null ? (err as { status?: unknown }).status : undefined;
  return {
    status: typeof status === "number" ? status : 500,
    message: String(err instanceof Error ? err.message : err),
  };
}

/** The demo policy that exercises the REJECTION path end to end (toast in the UI). Runs on the
 *  already-parsed title (zod guarantees it's a string), so it only layers on the app policy. */
function cleanTitle(title: string): string {
  const out = normalizeTitle(title);
  if (out.length === 0) throw new Error("a title is required");
  if (/\bspam\b/i.test(out)) throw new Error('the word "spam" is not allowed in titles');
  return out;
}

function requireUser(user: User): string {
  if (typeof user !== "string" || user.length === 0) throw new Error("a user is required");
  return normalizeOwner(user);
}
