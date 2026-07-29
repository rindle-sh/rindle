// Surfaces authority REJECTIONS as transient toasts. When a mutation is rejected (e.g. a "spam" room
// name or message), the optimistic layer snaps back AND `onRejected` fires — we show why.

import { useEffect, useState } from "react";

import { onRejection } from "../rindle-client.ts";

interface Toast {
  id: number;
  message: string;
}

export function Toaster() {
  const [toasts, setToasts] = useState<Toast[]>([]);
  useEffect(() => {
    return onRejection((_envelope, reason) => {
      const id = Date.now() + Math.random();
      setToasts((t) => [...t, { id, message: reason }]);
      setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 4000);
    });
  }, []);
  return (
    <div className="app-toaster" role="status" aria-live="polite">
      {toasts.map((t) => (
        <div key={t.id} className="app-toast">
          {t.message}
        </div>
      ))}
    </div>
  );
}
