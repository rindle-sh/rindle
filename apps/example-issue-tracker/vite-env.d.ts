declare module "rindle-wasm-bin?url" {
  const url: string;
  export default url;
}

interface ImportMetaEnv {
  readonly VITE_METRICS_URL?: string;
  /** Deprecated direct-follower test/debug escape hatch. */
  readonly VITE_DAEMON_WS?: string;
  /** Stable fleet ws host for every follower count; `rindle exec` derives it locally. */
  readonly VITE_FLEET_WS?: string;
}
