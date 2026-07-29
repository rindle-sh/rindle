// A single-select typeahead dropdown: a trigger showing the current value, and (when open) a filter
// input over a scrollable option list. Keyboard-driven (↑/↓/Enter/Escape), closes on outside click.
// With `allowCustom`, a typed value that matches no option commits as-is (optionally normalized) —
// so the same control powers "pick a known user" and "type a brand-new tag/handle".

import { useEffect, useMemo, useRef, useState } from "react";
import type { KeyboardEvent } from "react";

export interface ComboItem {
  value: string;
  label: string;
}

export function Combobox({
  value,
  items,
  onSelect,
  placeholder = "Select…",
  allowCustom = false,
  formatCustom,
}: {
  /** The committed value (`""` for an always-empty "add" control). */
  value: string;
  items: ComboItem[];
  onSelect: (value: string) => void;
  placeholder?: string;
  /** Allow committing a typed value that matches no option. */
  allowCustom?: boolean;
  /** Normalize a typed custom value before committing (e.g. an owner handle). */
  formatCustom?: (raw: string) => string;
}) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState("");
  const [active, setActive] = useState(0);
  const rootRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const selected = items.find((item) => item.value === value);
  const selectedLabel = selected?.label ?? value;

  const filtered = useMemo(() => {
    const needle = draft.trim().toLowerCase();
    if (!needle) return items;
    return items.filter(
      (item) => item.label.toLowerCase().includes(needle) || item.value.toLowerCase().includes(needle),
    );
  }, [draft, items]);

  const custom = allowCustom ? (formatCustom ? formatCustom(draft) : draft.trim()) : "";
  const showCustom = Boolean(custom) && !filtered.some((item) => item.value === custom);

  useEffect(() => setActive(0), [draft, open]);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  const openMenu = () => {
    setDraft("");
    setOpen(true);
    requestAnimationFrame(() => inputRef.current?.focus());
  };

  const commit = (next: string) => {
    if (next) onSelect(next);
    setOpen(false);
    setDraft("");
  };

  const commitActive = () => {
    if (active < filtered.length) return commit(filtered[active].value);
    if (showCustom) return commit(custom);
    setOpen(false);
  };

  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    const count = filtered.length + (showCustom ? 1 : 0);
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive((i) => Math.min(i + 1, count - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter") {
      e.preventDefault();
      commitActive();
    } else if (e.key === "Escape") {
      e.preventDefault();
      setOpen(false);
    }
  };

  return (
    <div className="it-combo" ref={rootRef}>
      <button type="button" className="it-combo-trigger" onClick={() => (open ? setOpen(false) : openMenu())}>
        <span className={value ? "" : "it-combo-placeholder"}>{value ? selectedLabel : placeholder}</span>
        <span className="it-combo-caret" aria-hidden="true">
          ▾
        </span>
      </button>
      {open && (
        <div className="it-combo-menu">
          <input
            ref={inputRef}
            className="it-combo-input"
            value={draft}
            placeholder="Type to filter…"
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={onKeyDown}
          />
          <div className="it-combo-list" role="listbox">
            {filtered.map((item, i) => (
              <button
                type="button"
                key={item.value}
                role="option"
                aria-selected={item.value === value}
                className={`it-combo-option${i === active ? " is-active" : ""}${
                  item.value === value ? " is-selected" : ""
                }`}
                onMouseEnter={() => setActive(i)}
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => commit(item.value)}
              >
                <span className="it-combo-option-label">{item.label}</span>
                {item.label !== item.value && <small>{item.value}</small>}
              </button>
            ))}
            {showCustom && (
              <button
                type="button"
                className={`it-combo-option${active === filtered.length ? " is-active" : ""}`}
                onMouseEnter={() => setActive(filtered.length)}
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => commit(custom)}
              >
                <span className="it-combo-option-label">Use “{custom}”</span>
                <small>new</small>
              </button>
            )}
            {filtered.length === 0 && !showCustom && <p className="it-combo-empty">No matches</p>}
          </div>
        </div>
      )}
    </div>
  );
}
