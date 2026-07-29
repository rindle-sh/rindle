/// <reference types="vite/client" />

declare module "rindle-wasm-bin?url" {
  const url: string;
  export default url;
}

interface ImportMetaEnv {
  /** Deprecated direct-follower test/debug escape hatch. */
  readonly VITE_DAEMON_WS?: string;
  /** Stable local/production fleet endpoint; follower affinity is always enabled. */
  readonly VITE_FLEET_WS?: string;
  /** "oidc" turns on the "Sign in with Rindle Cloud" flow; anything else keeps the dev switcher. */
  readonly VITE_FORUM_AUTH?: string;
  /** headwaters origin the sign-in redirect points at (default http://localhost:8787). */
  readonly VITE_HEADWATERS_ORIGIN?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
