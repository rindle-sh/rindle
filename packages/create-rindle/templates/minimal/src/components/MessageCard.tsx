// A single message in a room: author, body, and time. Reads its data through the `MessageCard` fragment.

import { useFragment } from "@rindle/react";

import { MessageCardFragment } from "./MessageCard.queries.ts";
import type { MessageCardRef } from "./MessageCard.queries.ts";
import { formatDateTime } from "../lib/format.ts";

export function MessageCard({ message }: { message: MessageCardRef }) {
  const data = useFragment(MessageCardFragment, message);
  if (!data) return null;

  return (
    <article className="app-message">
      <div className="app-message-head">
        <span className="app-message-author">{data.author}</span>
        <span className="app-message-time">{formatDateTime(data.createdAt)}</span>
      </div>
      <p className="app-message-body">{data.body}</p>
    </article>
  );
}
