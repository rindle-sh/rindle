// The home view (`/`): the list of rooms, each with a LIVE message count, plus a form to create a
// room. Its Rindle loader seeds the rooms query for first paint and blocks client navigation until
// the live query is ready — post a message in any room and its count here updates with no polling.

import { createFileRoute } from "@tanstack/react-router";
import { fragmentKey, useRoot } from "@rindle/react";

import { RoomCardFragment, roomsQuery } from "../components/RoomCard.queries.ts";
import { RoomCard } from "../components/RoomCard.tsx";
import { NewRoomForm } from "../components/NewRoomForm.tsx";
import { rindle } from "../rindle-tanstack.ts";

export const Route = createFileRoute("/")({
  loader: rindle.loader({ query: () => roomsQuery() }),
  component: Home,
});

function Home() {
  const [rooms, { status }] = useRoot(roomsQuery, RoomCardFragment);
  const loading = status !== "complete" && rooms.length === 0;

  return (
    <section className="app-page">
      <div className="app-page-head">
        <p className="app-eyebrow">Rindle starter</p>
        <h1>Rooms</h1>
      </div>
      <NewRoomForm />
      {loading ? (
        <p className="app-empty">Loading rooms…</p>
      ) : rooms.length === 0 ? (
        <p className="app-empty">No rooms yet — create the first one above.</p>
      ) : (
        <ul className="app-rooms">
          {rooms.map((room) => (
            <RoomCard key={fragmentKey(room)} room={room} />
          ))}
        </ul>
      )}
    </section>
  );
}
