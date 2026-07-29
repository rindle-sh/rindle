// One tag chip. Its data shape is declared by `TagChipFragment`; callers that already own that
// inline payload can render it directly without inspecting fragment-ref internals.

import type { TagChipData } from "./TagChip.queries.ts";

export function TagChip({ tag }: { tag: TagChipData }) {
  return <span className="it-tag">{tag.name}</span>;
}
