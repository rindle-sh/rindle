// The narration framework: per-query templates that render a resolved flat-change into prose.
//
// The position→name resolution (`resolveChange` / `subRow`) lives in @rindle/client — it is the
// pure inverse of the wire encoder and broadly useful. This module is the layer ON TOP: a small
// registry of hand-written, per-query templates keyed by `defineQuery` name + op (+ count alias),
// salience tagging (alert / info / ambient), and a `digest()` that folds a batch of rendered events
// into a salience-ranked block for an agent prompt. The templates are the only domain-specific part
// — small, because the query and relationship NAMES already carry the meaning; the app supplies the
// registry to {@link createNarrator}.

import { resolveChange, subRow, type FlatChange, type NamedRow, type ResolvedChange, type WireSchema } from "@rindle/client";

export type { NamedRow, ResolvedChange };

export type Salience = "alert" | "info" | "ambient";

/** Context a template may interpolate — e.g. a human label for the subscription's subject. */
export interface NarrateContext {
  /** A human name for what the query is scoped to ("the Smith wedding"). */
  subject?: string;
  [k: string]: unknown;
}

export interface TemplateCtx {
  row: NamedRow;
  old?: NamedRow;
  /** The parent row, for an aggregate/nested change (e.g. the `ticket_type` behind a `sold` count). */
  parent?: NamedRow;
  /** Resolve a named sub-row of the change (only populated on `add`). */
  sub: (alias: string) => NamedRow | null;
  aggregate?: ResolvedChange["aggregate"];
  context: NarrateContext;
}

export type Template = (ctx: TemplateCtx) => string | null;

/** Handlers for changes to ONE nested relationship (keyed by its alias in {@link QueryNarrator.related}),
 *  by op — plus an optional salience override for that relationship's events. A nested template reads
 *  `row` (the nested row), `old`, and `parent` (its immediate container — the deck for a slide, the
 *  slide for a component). */
export interface RelatedNarrator {
  /** Override the query's default salience for events on this relationship (else inherits it). */
  salience?: Salience;
  add?: Template;
  remove?: Template;
  edit?: Template;
}

export interface QueryNarrator {
  /** Default importance of this query's events; an op handler may override via the return tuple. */
  salience: Salience;
  /** Handlers for changes at the ROOT level, by op. */
  root?: Partial<Record<"add" | "remove" | "edit", Template>>;
  /** Handlers for changes to a NESTED relationship, keyed by op — so ONE materialized view narrates
   *  its whole tree: e.g. a deck's title at the root, slide edits under `slides`, component adds under
   *  `components`. Key by the relationship's LEAF alias (`components`) for the common case, or by the
   *  FULL dotted alias-chain from the root (`slides.components`) to disambiguate two relationships that
   *  share a leaf alias at different tree positions (e.g. `slides.components` vs `appendix.components`).
   *  A dotted key wins over a leaf key when both match; a bare leaf key still matches at any depth. */
  related?: Record<string, RelatedNarrator>;
  /** Handlers for an aggregate slot (`countAs`), keyed by the count's alias. */
  counts?: Record<string, Template>;
}

/** A registry of {@link QueryNarrator}s, keyed by `defineQuery` name. Supplied by the app. */
export type NarratorRegistry = Record<string, QueryNarrator>;

/** One rendered semantic event. `text === null` was suppressed by its template (too ambient). */
export interface SemanticEvent {
  query: string;
  phase: "snapshot" | "batch";
  salience: Salience;
  resolved: ResolvedChange;
  text: string | null;
}

export interface Narrator {
  /** Resolve + render a `FlatChange[]` for a named query into semantic events. `schema` is the
   *  query's `WireSchema`, captured from its `hello` frame (or read off `view.schema`) — the
   *  position→name source. */
  narrate(query: string, schema: WireSchema, changes: FlatChange[], phase: "snapshot" | "batch", ctx?: NarrateContext): SemanticEvent[];
  /** Format a batch of rendered events for an agent prompt (salience-marked, suppressions dropped). */
  digest(events: SemanticEvent[]): string;
}

/** Per-salience glyph for a rendered line. */
export const SALIENCE_MARK: Record<Salience, string> = { alert: "⚠️", info: "•", ambient: "·" };

/** Order salience high→low. */
export const salienceRank = (s: Salience): number => (s === "alert" ? 2 : s === "info" ? 1 : 0);

/** Build a narrator over an app-supplied {@link NarratorRegistry}. Resolution is driven entirely by
 *  the per-query `WireSchema` passed to {@link Narrator.narrate}, so the narrator holds no schema
 *  state of its own. */
export function createNarrator(narrators: NarratorRegistry): Narrator {
  return {
    narrate(query, schema, changes, phase, ctx = {}) {
      const spec = narrators[query];
      const out: SemanticEvent[] = [];
      for (const change of changes) {
        const resolved = resolveChange(schema, change);
        if (!resolved) continue;
        let text: string | null = null;
        let salience: Salience = spec?.salience ?? "info";
        if (spec) {
          const tctx: TemplateCtx = {
            row: resolved.row,
            old: resolved.old,
            parent: resolved.parent,
            sub: (alias) => subRow(resolved, alias),
            aggregate: resolved.aggregate,
            context: ctx,
          };
          // A nested relationship change (not the root, not an aggregate) matches a `related` template
          // keyed by its FULL dotted alias-chain (`slides.components`) if present, else its leaf alias
          // (`components`) — so two same-leaf relationships at different tree positions never collide,
          // while the common single-position case still keys by the bare alias.
          const relSpec =
            resolved.alias !== "" && !resolved.aggregate
              ? (spec.related?.[resolved.aliasChain.join(".")] ?? spec.related?.[resolved.alias])
              : undefined;
          const tmpl = resolved.aggregate
            ? spec.counts?.[resolved.aggregate.alias]
            : resolved.alias === ""
              ? spec.root?.[resolved.op]
              : relSpec?.[resolved.op];
          text = tmpl ? tmpl(tctx) : null;
          if (relSpec?.salience) salience = relSpec.salience;
        }
        out.push({ query, phase, salience, resolved, text });
      }
      return out;
    },
    digest(events) {
      return events
        .filter((e) => e.text !== null)
        .sort((a, b) => salienceRank(b.salience) - salienceRank(a.salience))
        .map((e) => `${SALIENCE_MARK[e.salience]} ${e.text}`)
        .join("\n");
    },
  };
}
