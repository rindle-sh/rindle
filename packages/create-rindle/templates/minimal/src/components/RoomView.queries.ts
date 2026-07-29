// Co-located with the room-detail view. Holds the detail SELECTION (`RoomDetailFragment`) AND the
// named root query that fetches it on demand (`roomDetailQuery`). React-free.

import { defineFragment, defineQuery } from "@rindle/client";
import type { FragmentRef } from "@rindle/client";
import { z } from "zod";

import { q, rels, room } from "../../shared/app-def.ts";
import { MessageCardFragment } from "./MessageCard.queries.ts";

/** What the ROOM VIEW renders: the room chrome plus its WHOLE message thread `sub`'d in oldest-first,
 *  each message via the shared `MessageCardFragment`. To cap the thread you'd add `.limit(n)` here. */
export const RoomDetailFragment = defineFragment(room, (r) =>
  r
    .select("id", "name", "createdAt")
    .sub("messages", rels.roomMessages, MessageCardFragment, (m) =>
      m.orderBy("createdAt", "asc").orderBy("id", "asc")
    ),
);
export type RoomDetailRef = FragmentRef<typeof RoomDetailFragment>;

/** The room view's only arg: a room id. */
const roomIdArgs = z.string().max(80);

/** A single room by id with its WHOLE message thread (`RoomDetailFragment`). */
export const roomDetailQuery = defineQuery("roomDetail", (raw) => roomIdArgs.parse(raw), (id) =>
  q.room.where.id(id).include(RoomDetailFragment).one(),
);
