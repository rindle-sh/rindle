// Locale date formatters for the issue list (short) and the comment timeline (date + time).

const shortDate = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric" });
const dateTime = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
});

export function formatDate(value: number): string {
  return Number.isFinite(value) ? shortDate.format(new Date(value)) : "-";
}

export function formatDateTime(value: number): string {
  return Number.isFinite(value) ? dateTime.format(new Date(value)) : "-";
}
