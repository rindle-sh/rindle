// The shared CONTRACT root: the normalized schema, the relationships the views join back over, the
// normalization helpers, and the ISOMORPHIC mutators — one generator body per mutator, driven by
// BOTH tiers. The browser drives each body synchronously (the optimistic prediction); the API server
// drives the SAME body inside an authoritative transaction, rendering its ops to SQL
// (server/app-api.ts).
//
// This module is the leaf of the contract DAG: it depends on nothing app-internal. The named root
// queries AND the per-component SELECTIONS they compose are co-located with their components in
// `src/components/*.queries.ts` (Relay-style co-location). Keeping the schema here, free of those
// imports, is what keeps that graph acyclic.

import { defineRelationships, newQueryBuilder, rel, shared } from "@rindle/client";
import type { IsoTx, MutationGen, MutatorCtx, Row } from "@rindle/client";
import type { ClientRegistry } from "@rindle/optimistic";
import { z } from "zod";

// The schema is GENERATED from migrations/*.sql into ./schema.gen.ts by `rindle schema gen` — `pnpm
// dev` regenerates it on every migration change, so the DDL is the single source of truth. We import
// the tables + `schema` here and re-export them below, keeping this contract root the one import for
// app code.
import { message, room, schema } from "./schema.gen.ts";

// --------------------------------------------------------------------------- tables (generated)

export { message, room, schema };

/** One schema-bound query builder, shared by every co-located `*.queries.ts`. Each `q.<table>` access
 *  mints a fresh builder, so sharing the single instance is safe. */
export const q = newQueryBuilder(schema);

// --------------------------------------------------------------- row types (schema-derived)

export type Room = Row<typeof room>;
export type Message = Row<typeof message>;

// --------------------------------------------------------------- relationships (joins, declared once)
//
// The one join in this app, declared ONCE: a room's messages. The home page's live `countAs` and the
// room view both spread it instead of restating the `roomId → id` correlation.
export const rels = defineRelationships({
  roomMessages: rel(room, message, { id: "roomId" }),
});

// --------------------------------------------------------------------------- normalization

export function normalizeName(name: string): string {
  return name.trim().replace(/\s+/g, " ").slice(0, 80);
}

export function normalizeBody(body: string): string {
  return body.trim().slice(0, 4000);
}

/** A login handle reduced to a stable slug, so the client's predicted author id matches the server's. */
export function normalizeSubject(raw: string): string {
  return raw.trim().replace(/\s+/g, "-").toLowerCase().slice(0, 40) || "anon";
}

// --------------------------------------------------------------------------- mutator args
//
// One zod schema per mutator, co-located with its body via `shared(args, gen)`. The SERVER parses the
// UNTRUSTED wire args through it (server/app-api.ts); BOTH tiers derive the arg TYPE from it
// (`z.infer`). NB: the AUTHOR is NOT an arg — it is the acting principal (`ctx.user`), injected by
// each tier's driver (the client's local handle for the prediction; the server's AUTHENTICATED
// identity for the authoritative run), so it is unspoofable over the wire.

export const createRoomArgs = z.object({ id: z.string(), name: z.string(), createdAt: z.number() });
export type CreateRoomArgs = z.infer<typeof createRoomArgs>;

export const postMessageArgs = z.object({
  id: z.string(),
  roomId: z.string(),
  body: z.string(),
  createdAt: z.number(),
});
export type PostMessageArgs = z.infer<typeof postMessageArgs>;

// --------------------------------------------------------------------------- mutators (ISOMORPHIC)
//
// ONE body per mutator, shared by both tiers. Each is a GENERATOR: it `yield`s logical write ops
// (`yield tx.insert(...)`) instead of touching a database, so the SAME function runs synchronously
// against the browser's wasm engine (the optimistic prediction) AND asynchronously against a live
// transaction on the server, each op rendered to SQL. Deterministic + replayable: every value that
// would otherwise come from the clock or a random source is passed in args (ids, timestamps) — the
// client RE-INVOKES the body on every rebase. Normalization AND the room-exists guard (a `tx.row`
// read) run INSIDE the body, so both tiers behave identically; the server layers only what it ALONE
// can do on top (identity required, an async "spam" moderation rejection demo — server/app-api.ts).

export const mutators = {
  createRoom: shared(createRoomArgs, function* (tx: IsoTx, a: CreateRoomArgs): MutationGen {
    const name = normalizeName(a.name);
    if (!name) return; // a no-op prediction is fine; the server's guard hard-rejects
    yield tx.insertIgnore("room", { id: a.id, name, createdAt: a.createdAt });
  }),
  postMessage: shared(postMessageArgs, function* (tx: IsoTx, a: PostMessageArgs, ctx: MutatorCtx): MutationGen {
    const body = normalizeBody(a.body);
    if (!body) return;
    // The room-exists guard, ISOMORPHIC (no raw SQL): READ the room through the mutator so a message
    // never lands in a room that doesn't exist. Read-your-writes on BOTH tiers — the browser's local
    // engine for the optimistic prediction, the server's interactive transaction for the authoritative
    // run — so the client predicts the SAME guard the server enforces, and a post into a deleted room
    // snaps back.
    if (!(yield tx.row("room", { id: a.roomId }))) return;
    yield tx.insert("message", {
      id: a.id,
      roomId: a.roomId,
      author: normalizeSubject(ctx.user),
      body,
      createdAt: a.createdAt,
    });
  }),
} satisfies ClientRegistry;
