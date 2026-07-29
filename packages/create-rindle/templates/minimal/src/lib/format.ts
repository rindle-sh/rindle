// Tiny date formatting helper, shared by the views. Pure + SSR-safe (same output on server and client
// for a given input, so hydration matches).

const DATE_TIME = new Intl.DateTimeFormat("en", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });

export function formatDateTime(ms: number): string {
  return DATE_TIME.format(new Date(ms));
}
