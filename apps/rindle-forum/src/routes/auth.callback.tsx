// The Rindle Cloud sign-in landing (`/auth/callback`). headwaters' /authorize 303s the browser here
// with the minted JWT in the URL FRAGMENT (`#token=…`). The fragment never reached the server, so we
// pull it off `location.hash` on the client, stash the token, and replace into the app — from here on
// every /api/rindle/* call carries it as a Bearer (src/rindle-client.ts → cloud-auth.ts). A full
// navigation (not router push) re-reads identity everywhere and drops the token from the address bar.

import { useEffect, useState } from "react";
import { createFileRoute } from "@tanstack/react-router";

import { setCloudToken } from "../cloud-auth.ts";

export const Route = createFileRoute("/auth/callback")({
  // No first-paint data — this route only processes the fragment, then leaves.
  loader: () => ({ rindle: {} }),
  component: AuthCallback,
});

function AuthCallback() {
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const token = new URLSearchParams(location.hash.replace(/^#/, "")).get("token");
    if (!token) {
      setError("No token in the sign-in response. Try signing in again.");
      return;
    }
    setCloudToken(token);
    location.replace("/"); // full reload: re-read identity in the TopBar + client headers
  }, []);

  return (
    <section className="rf-page">
      <p className="rf-empty">{error ?? "Signing you in with Rindle Cloud…"}</p>
    </section>
  );
}
