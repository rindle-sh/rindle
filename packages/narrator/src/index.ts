// @rindle/narrator — turn a Rindle query's flat-change stream into agent-ready prose.
//
// Pipeline: a view's per-query `FlatChange`s (delivered by `view.onChanges` / `store.materialize(
// query, { onChanges })`, net of no-op rebase cycles) → `resolveChange` (@rindle/client) lifts each
// into NAMED rows → per-query templates render salience-tagged `SemanticEvent`s → `digest()` folds a
// batch into a prompt block. Drive it off a live view's OWN change channel — the position→name source
// is that view's `schema`. In React, `@rindle/narrator-react`'s `useNarration` wires this for you.

export {
  createNarrator,
  SALIENCE_MARK,
  salienceRank,
} from "./narrator.ts";
export type {
  NamedRow,
  NarrateContext,
  Narrator,
  NarratorRegistry,
  QueryNarrator,
  RelatedNarrator,
  ResolvedChange,
  Salience,
  SemanticEvent,
  Template,
  TemplateCtx,
} from "./narrator.ts";
