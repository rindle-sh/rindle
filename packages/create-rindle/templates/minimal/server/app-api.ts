// The app authority, runtime-AGNOSTIC. The serverless-shaped tier's actual logic: it resolves named
// queries to ASTs (the daemon mints opaque leases), drives the SAME isomorphic mutators the browser
// predicted (shared/app-def.ts) with their ops rendered to approved SQL, enforces policy (identity
// required to write, a "spam" rejection demo), and talks to the daemon over the private bearer-auth'd
// control plane.
//
// It is deliberately free of any host: server/rindle-http.ts adapts it to a Web Request for the Start
// API routes (src/routes/api.rindle.*.tsx) the browser calls, and the SSR loader (src/ssr.ts) calls the
// SAME factory in-process. The only per-host inputs are the unified Rindle connection and the
// AuthProvider.

import {
  createRindleApiServer,
  defineApiMutators,
  registerQueries,
  runSharedMutation,
  scoped,
  sharedApiMutators,
} from "@rindle/api-server";
import type {
  ApiMutator,
  ApiMutators,
  MutationContext,
  MutatorCtx,
  RindleApiServer,
  SharedMutatorWithArgs,
} from "@rindle/api-server";
import {
  mutators as sharedMutators,
  normalizeBody,
  normalizeName,
  normalizeSubject,
  postMessageArgs,
  schema,
} from "../shared/app-def.ts";
import type { Identity } from "../shared/auth.ts";
import { roomsQuery } from "../src/components/RoomCard.queries.ts";
import { roomDetailQuery } from "../src/components/RoomView.queries.ts";

/** The authority's principal is the verified identity (or undefined when anonymous). */
export type User = Identity | undefined;

// The authority's query surface is just the list of co-located client queries. Each `defineQuery`
// re-runs its validator on the UNTRUSTED wire args before building the AST, so a malformed client
// can't smuggle a garbage arg in.
const apiQueries = registerQueries<User>([roomsQuery, roomDetailQuery]);

// MUTATORS ARE ISOMORPHIC — defined once in shared/app-def.ts and auto-driven here by
// `sharedApiMutators`: for each mutator it parses the untrusted wire args (the mutator's co-located
// `.args` schema), injects the AUTHENTICATED principal as `ctx.user` (`sharedCtx` — never a client
// arg), and drives the SAME body the client predicted, rendering every yielded op to SQL. A mutator
// whose server run needs no authority beyond that triad needs no entry here at all.
//
// The ONLY explicit entries are server-only AUTHORITY the client must NOT predict, each OVERRIDING
// its auto-wrapped default (spread first, override wins):
//   • createRoom — a `withGuard` arg policy: an empty or "spam" name hard-rejects BEFORE any write.
//   • postMessage — a `scoped` mutator (work-outside-tx): it runs async MODERATION *outside* the
//     transaction, then opens the ONE write via `scope.transact`, driving the SAME isomorphic body the
//     browser predicted (its room-exists guard is a `tx.row` read — no raw SQL, and the client predicts
//     it too). A scoped mutator lets server-only work that must NOT hold a DB write open (a moderation/
//     LLM/anti-abuse call, a payment) run before — or after — the atomic write.

/** The `MutatorCtx` a shared body sees on the server: the AUTHENTICATED subject (throws if absent —
 *  a business rejection). Never a client-supplied author. */
function sharedCtx(ctx: MutationContext<User>): MutatorCtx {
  return { user: requireUser(ctx.user).subject };
}

/** Layer a server-only arg policy onto a shared mutator (throw → hard reject), then drive the SAME
 *  body the client predicts. */
function withGuard<A>(gen: SharedMutatorWithArgs<A>, guard: (a: A) => void): ApiMutator<User, unknown> {
  return (tx, raw, ctx) => {
    const a = gen.args.parse(raw);
    guard(a);
    return runSharedMutation(gen, a, sharedCtx(ctx), tx);
  };
}

const apiMutators = defineApiMutators<User, ApiMutators<User>>({
  ...sharedApiMutators(sharedMutators, sharedCtx),

  // (a) a server-only policy on the shared body: an empty or "spam" name throws BEFORE any write.
  createRoom: withGuard(sharedMutators.createRoom, (a) => void cleanName(a.name)),

  // (b) a SCOPED mutator (work-outside-tx): server-only code runs OUTSIDE the write transaction, then
  //     ONE atomic write opens via `scope.transact`, then post-commit code may run.
  postMessage: scoped<User, unknown>(async (scope, raw, ctx) => {
    const a = postMessageArgs.parse(raw);

    // OUTSIDE the transaction: moderation is an async, out-of-database call (a real app would reach an
    // LLM or anti-abuse service) — the ONE thing here the client genuinely can't predict. A scoped
    // mutator exists precisely so this does NOT hold a DB write open across the network round trip. A
    // throw rejects the post before any write — the client's optimistic message snaps back. For a
    // NON-idempotent external effect, key it on the stable `ctx.envelope.mid` so a retried envelope
    // can't double-fire it.
    await moderateBody(a.body);

    // INSIDE the transaction: drive the SAME isomorphic body the browser predicted (shared/app-def.ts).
    // Its room-exists guard is a `tx.row` READ (read-your-writes) — no raw SQL, no server-only insert —
    // so the client predicts the same guard. This is the one atomic write; the daemon stamps `lmid`
    // together with it. A throw inside the body would be a business rejection surfaced as
    // `MutationRejected` (data rolls back, `lmid` still advances).
    await scope.transact(sharedMutators.postMessage, a, sharedCtx(ctx));

    // AFTER commit: fire post-commit effects here (a push notification, a webhook). They can't roll the
    // write back, so keep them best-effort (wrap your own try/catch). None are needed for this demo.
  }),
});

/** The one application-facing Rindle connection. */
export interface AppApiOptions {
  /** The unified fleet ingress for reads, SQL writes, migrations, and subscriptions. */
  url: string;
  /** The server-only public SQL bearer. Never returned to the browser. */
  token: string;
  /** Optional distinct public WebSocket ingress; normally derived from {@link url}. */
  wsUrl?: string;
}

/** Build the configured API server. Stateless: safe to construct per-request or once per process.
 *  Reads are PUBLIC; writes require a verified identity. */
export function createAppApi(opts: AppApiOptions): RindleApiServer<User> {
  return createRindleApiServer<User>({
    rindle: { url: opts.url, token: opts.token, wsUrl: opts.wsUrl },
    schema, // drives the dialect-SQL renderer for the shared mutators' logical ops
    queries: apiQueries,
    mutators: apiMutators,
    authorizeQuery: () => true, // public reads
    authorizeMutation: ({ user }) => !!user && user.subject.length > 0, // must be signed in
  });
}

/** Resolve the one connection injected by `rindle dev` (or the production host). */
export function resolveRindle(env: Record<string, string | undefined>): AppApiOptions {
  const url = env.RINDLE_URL;
  const token = env.RINDLE_DATABASE_TOKEN;
  if (!url || !token) {
    throw new Error(
      "RINDLE_URL + RINDLE_DATABASE_TOKEN are required — start the app with `rindle dev -- …`",
    );
  }
  const wsUrl = env.RINDLE_WS_URL;
  return { url, token, ...(wsUrl ? { wsUrl } : {}) };
}

/** Map an error thrown out of the API server (or body parsing) to an HTTP status + message. */
export function httpErrorOf(err: unknown): { status: number; message: string } {
  const status = typeof err === "object" && err !== null ? (err as { status?: unknown }).status : undefined;
  return {
    status: typeof status === "number" ? status : 500,
    message: String(err instanceof Error ? err.message : err),
  };
}

/** A room name policy that also exercises the REJECTION path end to end (toast in the UI). */
function cleanName(name: string): string {
  const out = normalizeName(name);
  if (out.length === 0) throw new Error("a room name is required");
  if (/\bspam\b/i.test(out)) throw new Error('the word "spam" is not allowed');
  return out;
}

/** Message moderation — a PLACEHOLDER for a real async, out-of-database check (an LLM / anti-abuse
 *  service). It runs OUTSIDE the write transaction (see `postMessage`), the whole reason a scoped
 *  mutator exists: you don't hold a DB write open across a network call. Here it just flags the "spam"
 *  demo; throwing rejects the post before any write (server-only, so the client's optimistic message
 *  snaps back). Empty/normalization is handled ISOMORPHICALLY in the shared body, so it's not here. */
async function moderateBody(body: string): Promise<void> {
  if (/\bspam\b/i.test(normalizeBody(body))) throw new Error('the word "spam" is not allowed');
}

function requireUser(user: User): Identity {
  if (!user || user.subject.length === 0) throw new Error("you must be signed in");
  return { subject: normalizeSubject(user.subject) };
}
