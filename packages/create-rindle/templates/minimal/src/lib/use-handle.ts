// Hydration-safe hook for "who am I (this browser)?". The server renders under the SSR placeholder and
// so does the client's FIRST render (so the markup byte-matches); after mount it adopts the real
// persisted handle. Personalized UI reads this so it never causes a hydration mismatch.

import { useEffect, useState } from "react";

import { currentHandle, SSR_USER } from "../rindle-client.ts";

/** The current browser's handle — `SSR_USER` until mounted, then the persisted dev handle. */
export function useCurrentHandle(): string {
  const [handle, setHandle] = useState(SSR_USER);
  useEffect(() => {
    setHandle(currentHandle());
  }, []);
  return handle;
}
