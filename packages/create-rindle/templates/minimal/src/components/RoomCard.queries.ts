// Co-located with the room card on the home page. Holds the per-card SELECTION (`RoomCardFragment`)
// AND the named root query over the room table (`roomsQuery`). React-free, so the API server can
// import it for query resolution without pulling in React.

import { defineFragment, defineQuery } from "@rindle/client";
import type { FragmentRef } from "@rindle/client";

import { q, rels, room } from "../../shared/app-def.ts";

/** A room row + a LIVE count of its messages (a scalar `countAs` over the whole `message` table —
 *  maintained incrementally by the engine, never polled). */
export const RoomCardFragment = defineFragment(room, (r) =>
  r.select("id", "name", "createdAt").countAs("messageCount", rels.roomMessages),
);
export type RoomCardRef = FragmentRef<typeof RoomCardFragment>;

/** Every room, newest first. No args, so no validator. */
export const roomsQuery = defineQuery("rooms", () =>
  q.room.orderBy("createdAt", "desc").orderBy("id", "asc").include(RoomCardFragment),
);
