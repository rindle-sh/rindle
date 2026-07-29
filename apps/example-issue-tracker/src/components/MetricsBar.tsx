// The live footprint strip — the issue-tracker twin of the wiki demo's packed-slot badge.
// While a swarm of bots hammers the tracker (see server/swarm.ts), this polls the swarm tier's
// /metrics and shows what the ONE box absorbing them is actually doing: whole-VM CPU, resident
// memory against the machine's size — plus the load side: how many bots are
// hitting it and at what rate. The numbers stay tiny while the boards you see below stay exact; that
// contrast is the whole point. Renders nothing until a metrics URL is configured and /metrics
// answers (so it's invisible when the swarm isn't running, e.g. a plain `pnpm dev`).

import { useEffect, useState } from "react";

import "./MetricsBar.css";

const METRICS_URL = (import.meta.env.VITE_METRICS_URL as string | undefined)?.trim() || null;

interface Metrics {
  bots: number;
  mutationsPerSec: number;
  mutations: number;
  applied: number;
  rejected: number;
  liveBotIssues: number;
  subscribers: number;
  subscribersConnected: number;
  viewers: number | null;
  materializations: number | null;
  leases: number | null;
  vcpus: number | null;
  cpuPercent: number | null;
  memUsedBytes: number | null;
  memLimitBytes: number | null;
}

const fmtInt = (n: number): string => n.toLocaleString();
const fmtBytes = (b: number): string => {
  const mb = b / (1024 * 1024);
  return mb >= 1024 ? `${(mb / 1024).toFixed(1)} GB` : `${Math.round(mb)} MB`;
};

export function MetricsBar() {
  const [m, setM] = useState<Metrics | null>(null);

  // Poll the swarm tier's badge telemetry. Slow (2s) — it's slowly-changing footprint data, served
  // from a TTL+single-flight cache, so many open tabs collapse to one daemon round-trip per window.
  useEffect(() => {
    if (!METRICS_URL) return;

    let alive = true;
    const poll = async () => {
      try {
        const res = await fetch(METRICS_URL);
        if (!res.ok) return;
        const next = (await res.json()) as Metrics;
        if (alive) setM(next);
      } catch {
        /* swarm not running — the strip just stays hidden */
      }
    };
    void poll();
    const id = setInterval(() => void poll(), 2000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  if (!m || m.vcpus == null) return null;

  return (
    <div
      className="it-machine"
      role="status"
      title="the entire server tier — rindled + this swarm — runs in one packed slot; this is its live CPU and resident memory while a swarm of bots writes to it"
    >
      <span className="it-machine-load">
        <span className="it-machine-dot" />
        <b>{fmtInt(m.bots)}</b> bots
        <span className="it-machine-sub">{m.mutationsPerSec.toFixed(1)}/s</span>
      </span>
      <span className="it-machine-sep" aria-hidden="true" />

      <span className="it-machine-label">one packed slot</span>
      <span className="it-machine-stat">
        <b>{m.vcpus}</b> vCPU<span className="it-machine-sub">shared</span>
      </span>
      <span className="it-machine-sep" aria-hidden="true" />
      <span className="it-machine-stat">
        CPU <b>{m.cpuPercent != null ? `${m.cpuPercent.toFixed(0)}%` : "—"}</b>
      </span>
      <span className="it-machine-sep" aria-hidden="true" />
      <span className="it-machine-stat">
        RAM <b>{m.memUsedBytes != null ? fmtBytes(m.memUsedBytes) : "—"}</b>
        {m.memLimitBytes != null && <span className="it-machine-sub">/ {fmtBytes(m.memLimitBytes)}</span>}
      </span>
      {m.viewers != null && (
        <>
          <span className="it-machine-sep" aria-hidden="true" />
          <span
            className="it-machine-stat"
            title="live public ws connections on the daemon — every subscriber bot and every real browser tab watching now"
          >
            <b>{fmtInt(m.viewers)}</b> watching
          </span>
        </>
      )}
      {m.materializations != null && (
        <>
          <span className="it-machine-sep" aria-hidden="true" />
          <span
            className="it-machine-stat"
            title="live materializations on the daemon — every viewer watches the SAME window, so thousands SHARE one pipeline; this count stays flat as viewers grow"
          >
            <b>{fmtInt(m.materializations)}</b> live queries
          </span>
        </>
      )}
    </div>
  );
}
