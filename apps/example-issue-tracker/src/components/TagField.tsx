// The tag editor: the issue's tags render as removable pills, and a typeahead "Add tag…" combobox
// suggests existing tag names (the ones not already applied) while letting you type a brand-new one.
// Presentational — the caller decides what `onAdd`/`onRemove` do (local form state vs. live
// add/remove mutations).

import { normalizeTagName } from "../../shared/app-def.ts";
import { Combobox } from "./Combobox.tsx";

export function TagField({
  tags,
  suggestions,
  onAdd,
  onRemove,
}: {
  tags: string[];
  suggestions: string[];
  onAdd: (name: string) => void;
  onRemove: (name: string) => void;
}) {
  const available = suggestions.filter((name) => !tags.includes(name)).map((name) => ({ value: name, label: name }));
  return (
    <div className="it-field">
      <span>Tags</span>
      <div className="it-tag-edit">
        <div className="it-pills">
          {tags.map((tag) => (
            <span key={tag} className="it-pill">
              {tag}
              <button type="button" aria-label={`Remove ${tag}`} onClick={() => onRemove(tag)}>
                ×
              </button>
            </span>
          ))}
          {tags.length === 0 && <span className="it-pills-empty">No tags yet</span>}
          <Combobox
            value=""
            items={available}
            placeholder="Add tag..."
            allowCustom
            formatCustom={normalizeTagName}
            onSelect={onAdd}
          />
        </div>
      </div>
    </div>
  );
}
