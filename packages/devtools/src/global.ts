// The opt-in global registry (DEBUG-TOOLS-BROWSER-DESIGN.md §6.2). The client core stays clean —
// no global mutable singleton lives in `@rindle/client`/`@rindle/optimistic`. Instead a dev build
// calls `attachDevtools(app)`, which constructs a {@link DevtoolsCore} and registers it on
// `globalThis.__RINDLE_DEVTOOLS__`, the same discovery pattern TanStack/Redux DevTools use. A panel
// finds the hub with {@link getDevtoolsHub} and subscribes — so it works whether it mounts before or
// after the app attaches.

import { DevtoolsCore } from "./core.ts";
import type { DevtoolsCoreOptions, DevtoolsTarget } from "./types.ts";

const GLOBAL_KEY = "__RINDLE_DEVTOOLS__";

/** The discovery surface a panel binds to: the set of attached cores + a change subscription. */
export interface DevtoolsHub {
  readonly version: number;
  readonly cores: readonly DevtoolsCore[];
  /** Fires whenever a core attaches or detaches (so a panel can pick one up). */
  subscribe(listener: () => void): () => void;
  /** Register a core; returns its deregistration function. */
  register(core: DevtoolsCore): () => void;
}

class Hub implements DevtoolsHub {
  readonly version = 1;
  readonly cores: DevtoolsCore[] = [];
  private readonly listeners = new Set<() => void>();

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  register(core: DevtoolsCore): () => void {
    this.cores.push(core);
    this.emit();
    return () => {
      const i = this.cores.indexOf(core);
      if (i >= 0) {
        this.cores.splice(i, 1);
        this.emit();
      }
    };
  }

  private emit(): void {
    for (const l of this.listeners) l();
  }
}

function globalSlot(): Record<typeof GLOBAL_KEY, Hub | undefined> {
  return globalThis as unknown as Record<typeof GLOBAL_KEY, Hub | undefined>;
}

/** Get (creating on first use) the global devtools hub. A panel calls this to discover attached
 *  clients; it is safe to call before any `attachDevtools`. */
export function getDevtoolsHub(): DevtoolsHub {
  const slot = globalSlot();
  return (slot[GLOBAL_KEY] ??= new Hub());
}

/** The most recently attached core, or `undefined` — the common single-app convenience a panel uses. */
export function getDevtoolsCore(): DevtoolsCore | undefined {
  const cores = getDevtoolsHub().cores;
  return cores[cores.length - 1];
}

/** Attach a devtools pane to a running client (DEBUG-TOOLS-BROWSER-DESIGN §6.2). Call this ONLY in a
 *  dev build (e.g. behind `import.meta.env.DEV` with a dynamic `import("@rindle/devtools")`) so the
 *  pane, its core, and this registration tree-shake out of production. Returns the {@link DevtoolsCore}
 *  (also discoverable via {@link getDevtoolsHub}); call `core.detach()` to unwind. */
export function attachDevtools(target: DevtoolsTarget, opts?: DevtoolsCoreOptions): DevtoolsCore {
  // Default the safety-net poll on for real apps (a fold's debounced flush / a pending flip on an
  // already-`complete` query move state with no event); tests construct DevtoolsCore directly.
  const core = new DevtoolsCore(target, { pollMs: 500, ...opts });
  core.onDetach = getDevtoolsHub().register(core);
  return core;
}
