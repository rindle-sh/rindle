// The faceted search box: type free text (matches titles), or pick an `axis:value` facet from the
// suggestions (status / priority / owner / tag / comment). Committed tokens render as chips; the
// App ANDs them together to filter the loaded windows. All matching logic lives in lib/search.ts.

import { useMemo, useRef, useState } from "react";
import type { KeyboardEvent } from "react";

import {
  SEARCH_AXES,
  axisLabel,
  normalizeTokenValue,
  parseAxisDraft,
  tokenDisplayValue,
  valuesForAxis,
} from "../lib/search.ts";
import type { OwnerOption, SearchAxis, SearchToken } from "../lib/search.ts";

export function TokenSearch({
  tokens,
  owners,
  tags,
  onChange,
}: {
  tokens: SearchToken[];
  owners: OwnerOption[];
  tags: string[];
  onChange: (tokens: SearchToken[]) => void;
}) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [axis, setAxis] = useState<SearchAxis | null>(null);
  const [draft, setDraft] = useState("");
  const [open, setOpen] = useState(false);

  const suggestions = useMemo(() => {
    const needle = draft.trim().toLowerCase();
    if (axis) {
      const values = valuesForAxis(axis, owners, tags);
      if (axis === "title" || axis === "comment") {
        return needle
          ? [{ kind: "value" as const, value: draft.trim(), label: `contains "${draft.trim()}"` }]
          : [];
      }
      return values
        .filter((item) => item.label.toLowerCase().includes(needle) || item.value.toLowerCase().includes(needle))
        .slice(0, 8)
        .map((item) => ({ kind: "value" as const, ...item }));
    }
    return SEARCH_AXES.filter((item) => item.label.includes(needle) || item.axis.includes(needle))
      .slice(0, 8)
      .map((item) => ({ kind: "axis" as const, ...item }));
  }, [axis, draft, owners, tags]);

  const resetDraft = () => {
    setAxis(null);
    setDraft("");
    setOpen(false);
  };

  const addToken = (nextAxis: SearchAxis, rawValue: string) => {
    const value = normalizeTokenValue(nextAxis, rawValue);
    if (!value) return;
    onChange([...tokens, { id: crypto.randomUUID(), axis: nextAxis, value }]);
    resetDraft();
    inputRef.current?.focus();
  };

  const chooseSuggestion = (suggestion: (typeof suggestions)[number]) => {
    if (suggestion.kind === "axis") {
      setAxis(suggestion.axis);
      setDraft("");
      setOpen(true);
      inputRef.current?.focus();
      return;
    }
    if (axis) addToken(axis, suggestion.value);
  };

  const commit = () => {
    const parsed = axis ? null : parseAxisDraft(draft);
    if (parsed) {
      setAxis(parsed.axis);
      setDraft(parsed.value);
      setOpen(true);
      return;
    }
    const first = suggestions[0];
    if (first) {
      chooseSuggestion(first);
      return;
    }
    if (axis) addToken(axis, draft);
    else if (draft.trim()) addToken("title", draft);
  };

  const removeToken = (id: string) => onChange(tokens.filter((token) => token.id !== id));

  const onDraftChange = (value: string) => {
    if (!axis) {
      const parsed = parseAxisDraft(value);
      if (parsed) {
        setAxis(parsed.axis);
        setDraft(parsed.value);
        setOpen(true);
        return;
      }
    }
    setDraft(value);
    setOpen(true);
  };

  return (
    <div className="it-token-search">
      <div className="it-token-box" onClick={() => inputRef.current?.focus()}>
        {tokens.map((token) => (
          <span key={token.id} className="it-search-token">
            <b>{axisLabel(token.axis)}</b>
            <span>{tokenDisplayValue(token, owners)}</span>
            <button type="button" onClick={() => removeToken(token.id)} aria-label={`Remove ${axisLabel(token.axis)} filter`}>
              x
            </button>
          </span>
        ))}
        {axis && (
          <span className="it-search-token is-editing">
            <b>{axisLabel(axis)}</b>
          </span>
        )}
        <input
          ref={inputRef}
          value={draft}
          onChange={(e) => onDraftChange(e.target.value)}
          onFocus={() => setOpen(true)}
          onBlur={() => setOpen(false)}
          onKeyDown={(e: KeyboardEvent<HTMLInputElement>) => {
            if (e.key === "Tab" || e.key === "Enter") {
              e.preventDefault();
              commit();
            } else if (e.key === "Backspace" && !draft) {
              if (axis) setAxis(null);
              else if (tokens.length > 0) onChange(tokens.slice(0, -1));
            } else if (e.key === "Escape") {
              resetDraft();
            }
          }}
          placeholder={tokens.length || axis ? "" : "Search issues"}
        />
      </div>
      {open && suggestions.length > 0 && (
        <div className="it-suggest" role="listbox">
          {suggestions.map((suggestion) => (
            <button
              key={suggestion.kind === "axis" ? suggestion.axis : `${axis}:${suggestion.value}`}
              type="button"
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => chooseSuggestion(suggestion)}
            >
              <span>{suggestion.label}</span>
              <small>{suggestion.kind === "axis" ? "axis" : axisLabel(axis ?? "title")}</small>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
