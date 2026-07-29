import { lazy, Suspense, useEffect, useState } from "react";

const Panel = import.meta.env.DEV
  ? lazy(() => import("@rindle/react-devtools").then((m) => ({ default: m.RindleDevtools })))
  : null;

export function DevTools() {
  const [mounted, setMounted] = useState(false);

  useEffect(() => setMounted(true), []);

  if (!Panel || !mounted) return null;

  return (
    <Suspense fallback={null}>
      <Panel />
    </Suspense>
  );
}
