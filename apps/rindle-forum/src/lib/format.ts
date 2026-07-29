// Tiny date/number formatting helpers, shared by the views. Pure + SSR-safe (same output on server
// and client for a given input, so hydration matches).

const DATE = new Intl.DateTimeFormat("en", { month: "short", day: "numeric" });
const DATE_TIME = new Intl.DateTimeFormat("en", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });

export function formatDate(ms: number): string {
  return DATE.format(new Date(ms));
}

export function formatDateTime(ms: number): string {
  return DATE_TIME.format(new Date(ms));
}

/** "3 replies" / "1 reply" / "no replies" from a post COUNT (which includes the opening post). */
export function replyLabel(postCount: number): string {
  const replies = Math.max(0, postCount - 1);
  if (replies === 0) return "no replies";
  return `${replies} ${replies === 1 ? "reply" : "replies"}`;
}
