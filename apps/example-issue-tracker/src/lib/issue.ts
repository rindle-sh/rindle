// Pure read helpers over an issue row and its folded `sub` relationships (owner / tags / comments),
// plus the human labels. No React — shared by every view component.
//
// The row types are the FRAGMENT result shapes: a list row / board card is an `IssueCard` (owner +
// tags + a comment count), the detail pane is an `IssueDetailCard` (owner + tags + the full thread).
// The helpers below take the narrowest structural shape they read, so the same `ownerName`/`tagNames`
// serve a card, a detail row, or a feed item interchangeably.

import type { IssuePriority, IssueStatus, User } from "../../shared/app-def.ts";
import type { CommentWithAuthor } from "../components/CommentCard.queries.ts";
import type { IssueCardRef } from "../components/IssueListItem.queries.ts";
import type { IssueDetailRef } from "../components/IssueDetail.queries.ts";

/** A list row / board card: an issue with its owner, tags, and the comment-thread count. */
export type IssueRow = IssueCardRef;
/** The detail pane's row: an issue with its owner, tags, and the WHOLE comment thread. */
export type IssueDetailRow = IssueDetailRef;

export const STATUS_LABELS: Record<IssueStatus, string> = {
  backlog: "Backlog",
  todo: "Todo",
  "in-progress": "In progress",
  review: "Review",
  done: "Done",
};

export const PRIORITY_LABELS: Record<IssuePriority, string> = {
  urgent: "Urgent",
  high: "High",
  medium: "Medium",
  low: "Low",
};

export interface TagPayload {
  id: string;
  name: string;
}

// Tags are parent-owned inline payloads here because the chrome/detail views need their names for
// search suggestions, editing, and duplicate checks.
export function issueTags(issue: { tags?: TagPayload[] } | null | undefined): TagPayload[] {
  return issue && Array.isArray(issue.tags) ? issue.tags : [];
}

export function tagNames(issue: { tags?: TagPayload[] } | null | undefined): string[] {
  return issueTags(issue).map((tag) => tag.name);
}

export function issueComments(issue: { comments?: CommentWithAuthor[] } | null | undefined): CommentWithAuthor[] {
  return issue && Array.isArray(issue.comments) ? issue.comments : [];
}

/** The earliest comment doubles as the description. */
export function descriptionComment(issue: { comments?: CommentWithAuthor[] } | null | undefined): CommentWithAuthor | undefined {
  return issueComments(issue)[0];
}

export function discussionComments(issue: { comments?: CommentWithAuthor[] } | null | undefined): CommentWithAuthor[] {
  return issueComments(issue).slice(1);
}

/** The owner relationship is singular (correlated on a key), surfaced as a 0-or-1 user array. */
export function ownerName(issue: { owner?: User[]; ownerId: string }): string {
  return issue.owner?.[0]?.name ?? issue.ownerId;
}

export function authorName(comment: { author?: User[]; authorId: string }): string {
  return comment.author?.[0]?.name ?? comment.authorId;
}
