// Hydration-safe hooks for "who am I (this browser)?". The server renders under the SSR placeholder
// and so does the client's FIRST render (so the markup byte-matches); after mount they adopt the real
// identity — the cloud token's `sub` when signed into Rindle Cloud, else the persisted dev handle.
// Personalized UI (own-post controls, my vote state) reads this so it never causes a hydration mismatch.

import { useEffect, useState } from "react";

import { normalizeSubject } from "../../shared/app-def.ts";
import { currentHandle, SSR_USER } from "../rindle-client.ts";
import { cloudIdentity } from "../cloud-auth.ts";

/** The current browser's normalized subject — `SSR_USER` until mounted, then the cloud `sub` (if signed
 *  into Rindle Cloud) or the dev handle. Matches `authorId`, so own-post controls work in either mode. */
export function useCurrentSubject(): string {
  const [subject, setSubject] = useState(SSR_USER);
  useEffect(() => {
    const cloud = cloudIdentity();
    setSubject(cloud ? cloud.subject : normalizeSubject(currentHandle()));
  }, []);
  return subject;
}

export type Identity =
  | { kind: "ssr"; displayName: string }
  | { kind: "dev"; displayName: string }
  | { kind: "cloud"; displayName: string; avatarUrl: string };

/** The mounted identity for the TopBar: dev handle vs. a signed-in Rindle Cloud account. SSR-safe. */
export function useIdentity(): Identity {
  const [id, setId] = useState<Identity>({ kind: "ssr", displayName: SSR_USER });
  useEffect(() => {
    const cloud = cloudIdentity();
    if (cloud) setId({ kind: "cloud", displayName: cloud.displayName, avatarUrl: cloud.avatarUrl });
    else setId({ kind: "dev", displayName: normalizeSubject(currentHandle()) });
  }, []);
  return id;
}
