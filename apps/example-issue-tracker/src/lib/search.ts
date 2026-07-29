// The faceted token search: the axis/token model and the pure functions the search box uses. A
// token is one `axis:value` facet. The facets are pushed to the SERVER (see `tokensToFilter` +
// `issuesPageQuery`), so the window the daemon materializes is already filtered — there is no
// client-side scan over a loaded window.

import { ISSUE_PRIORITIES, ISSUE_STATUSES, normalizeOwner, normalizeTagName } from "../../shared/app-def.ts";
import type { IssueFilter, SearchAxis } from "../../shared/app-def.ts";
import { PRIORITY_LABELS, STATUS_LABELS } from "./issue.ts";

export type { SearchAxis } from "../../shared/app-def.ts";

/** The UI token: a `SearchAxis` facet plus a stable `id` for React keys + removal. The wire filter
 *  (`tokensToFilter`) drops the `id` so equal searches share one subscription. */
export interface SearchToken {
  id: string;
  axis: SearchAxis;
  value: string;
}

/** Project the UI tokens onto the canonical server-side filter (id-free, order preserved). */
export function tokensToFilter(tokens: readonly SearchToken[]): IssueFilter {
  return tokens.map((token) => ({ axis: token.axis, value: token.value }));
}

/** An owner facet value: the handle is the identity (`value`), the display name is the `label`. */
export interface OwnerOption {
  value: string;
  label: string;
}

export const SEARCH_AXES: Array<{ axis: SearchAxis; label: string }> = [
  { axis: "title", label: "title" },
  { axis: "status", label: "status" },
  { axis: "priority", label: "priority" },
  { axis: "owner", label: "owner" },
  { axis: "tag", label: "tag" },
  { axis: "comment", label: "comment" },
];

export function parseAxisDraft(value: string): { axis: SearchAxis; value: string } | null {
  const colon = value.indexOf(":");
  if (colon === -1) return null;
  const rawAxis = value.slice(0, colon).trim().toLowerCase();
  const match = SEARCH_AXES.find((item) => item.axis === rawAxis || item.label === rawAxis);
  return match ? { axis: match.axis, value: value.slice(colon + 1).trimStart() } : null;
}

export function valuesForAxis(
  axis: SearchAxis,
  owners: OwnerOption[],
  tags: string[],
): Array<{ value: string; label: string }> {
  if (axis === "status") return ISSUE_STATUSES.map((status) => ({ value: status, label: STATUS_LABELS[status] }));
  if (axis === "priority") {
    return ISSUE_PRIORITIES.map((priority) => ({ value: priority, label: PRIORITY_LABELS[priority] }));
  }
  if (axis === "owner") return owners;
  if (axis === "tag") return tags.map((tag) => ({ value: tag, label: tag }));
  return [];
}

export function normalizeTokenValue(axis: SearchAxis, rawValue: string): string {
  const value = rawValue.trim();
  if (!value) return "";
  if (axis === "status") {
    const lower = value.toLowerCase();
    return ISSUE_STATUSES.find((status) => status === lower || STATUS_LABELS[status].toLowerCase() === lower) ?? "";
  }
  if (axis === "priority") {
    const lower = value.toLowerCase();
    return ISSUE_PRIORITIES.find((priority) => priority === lower || PRIORITY_LABELS[priority].toLowerCase() === lower) ?? "";
  }
  if (axis === "owner") return normalizeOwner(value);
  if (axis === "tag") return normalizeTagName(value);
  return value;
}

export function axisLabel(axis: SearchAxis): string {
  return SEARCH_AXES.find((item) => item.axis === axis)?.label ?? axis;
}

export function tokenDisplayValue(token: SearchToken, owners: OwnerOption[]): string {
  if (token.axis === "status") return STATUS_LABELS[token.value as keyof typeof STATUS_LABELS] ?? token.value;
  if (token.axis === "priority") return PRIORITY_LABELS[token.value as keyof typeof PRIORITY_LABELS] ?? token.value;
  if (token.axis === "owner") return owners.find((owner) => owner.value === token.value)?.label ?? token.value;
  return token.value;
}
