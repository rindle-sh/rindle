// @rindle/narrator-react — React lifecycle integration for @rindle/narrator.

import { useEffect, useMemo, useRef } from "react";
import type { AnyQuery, ChangePhase } from "@rindle/client";
import { createNarrator } from "@rindle/narrator";
import type { NarrateContext, NarratorRegistry, SemanticEvent } from "@rindle/narrator";
import { queryCacheKey, useRindleStore } from "@rindle/react";

/** Options for {@link useNarration}. */
export interface UseNarrationOptions {
  /** The registry key for this query's templates. Defaults to a named query's `name`; pass it for an
   *  ad-hoc (unnamed) query, e.g. `{ as: "deckSlides" }`. */
  as?: string;
  /** Context handed to every template (e.g. a human subject label). */
  ctx?: NarrateContext;
  /** Which delivery phases to narrate. Default `["batch"]` — the initial `snapshot` is the current
   *  state (not a change), so "tell me what CHANGED" ignores it. Pass `["snapshot", "batch"]` to also
   *  narrate the initial rows. */
  phases?: ChangePhase[];
  /** Cap the retained event buffer; oldest drop first. Default 200. */
  max?: number;
}

/** A stable handle over a query's live narration buffer. */
export interface Narration {
  /** Drain the events accumulated since the last call, then clear — call this when you hand context
   *  to an agent (e.g. on chat send). */
  take(): SemanticEvent[];
  /** Discard the buffer without returning it. */
  clear(): void;
}

/** Narrate a live query's change stream into buffered {@link SemanticEvent}s — the agent-facing twin
 *  of `useQuery`. It drives off the view's OWN `onChanges` channel (net of no-op rebase cycles, so a
 *  correctly predicted optimistic write narrates nothing) and needs no store-global qid filter.
 *
 *  The returned handle is STABLE and does NOT re-render on each change — narration feeds an agent,
 *  not the DOM. Drain it with `take()` at the moment you send context. Pass a STABLE `registry` (a
 *  module const): a fresh object each render re-subscribes the view. */
export function useNarration<Q extends AnyQuery>(
  query: Q,
  registry: NarratorRegistry,
  opts: UseNarrationOptions = {},
): Narration {
  // Derive + validate the registry key BEFORE any hook runs (so a throw never executes a partial
  // hook list). An ad-hoc (unnamed) query carries no `name`, so without `opts.as` the key would fall
  // back to "" — matching no registry entry and silently narrating NOTHING, indistinguishable from
  // "no changes". Fail loud instead: the fix is one option, and a silent agent is worse than a crash.
  const key = opts.as ?? (typeof query.name === "string" ? query.name : "");
  if (key === "") {
    throw new Error(
      "useNarration: could not derive a narrator registry key from this query. Ad-hoc (unnamed) " +
        'queries have no name — pass `{ as: "<registryKey>" }` naming this query\'s narrator entry.',
    );
  }

  const store = useRindleStore();
  const viewKey = queryCacheKey(query);
  const queryRef = useRef(query);
  queryRef.current = query;

  const narrator = useMemo(() => createNarrator(registry), [registry]);

  // Latest ctx/phases/max read inside the subscription, so a changing `opts` object does not
  // re-subscribe the view (only the query/registry/key identity does).
  const optsRef = useRef(opts);
  optsRef.current = opts;
  const bufferRef = useRef<SemanticEvent[]>([]);

  useEffect(() => {
    // A dedicated view for narration: `onChanges` is wired inside `materialize` (before the backend
    // registers the query), so a synchronous backend's first `snapshot` is caught too.
    const view = store.materialize(queryRef.current, {
      onChanges: (changes, phase, schema) => {
        const { phases, ctx, max = 200 } = optsRef.current;
        const want = phases ? phases.includes(phase) : phase === "batch";
        if (!want) return;
        const buf = bufferRef.current;
        // `text === null` is the narrator's suppression signal; an empty string is a real (if terse)
        // rendered line, so filter on null explicitly, not truthiness.
        for (const event of narrator.narrate(key, schema, changes, phase, ctx)) {
          if (event.text !== null) buf.push(event);
        }
        if (buf.length > max) buf.splice(0, buf.length - max);
      },
    });
    // Clear the buffer on re-subscribe (a query/registry/key identity change, or a StrictMode
    // double-mount): the buffer outlives the effect, so without this a re-materialize replays the
    // fresh snapshot/changes on top of stale events from the previous subscription.
    return () => {
      view.destroy();
      bufferRef.current = [];
    };
  }, [store, viewKey, narrator, key]);

  return useMemo<Narration>(
    () => ({
      take: () => {
        const out = bufferRef.current;
        bufferRef.current = [];
        return out;
      },
      clear: () => {
        bufferRef.current = [];
      },
    }),
    [],
  );
}
