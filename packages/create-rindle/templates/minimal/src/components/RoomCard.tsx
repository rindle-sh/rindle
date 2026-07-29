// A single room on the home page: its name and a LIVE message count (the `countAs` stays exact as
// messages are posted — no polling). Reads its data through the `RoomCard` fragment.

import { Link } from "@tanstack/react-router";
import { useFragment } from "@rindle/react";

import { RoomCardFragment } from "./RoomCard.queries.ts";
import type { RoomCardRef } from "./RoomCard.queries.ts";

export function RoomCard({ room }: { room: RoomCardRef }) {
  const data = useFragment(RoomCardFragment, room);
  if (!data) return null;

  return (
    <li className="app-room">
      <Link to="/r/$id" params={{ id: data.id }} className="app-room-link">
        <span className="app-room-name">{data.name}</span>
        <span className="app-count" title={`${data.messageCount} messages`}>
          {data.messageCount}
          <small>messages</small>
        </span>
      </Link>
    </li>
  );
}
