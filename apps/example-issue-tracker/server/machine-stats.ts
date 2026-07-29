// Whole-machine resource telemetry for the metrics badge — the point it makes is how LITTLE this
// tier uses even while a swarm of bots hammers it. CPU% is the WHOLE VM (every core, every process)
// read from /proc/stat; memory is the resident working set of the app processes (this Node swarm +
// the spawned rindled), NOT page cache, so it reads as the honest "tiny footprint" rather than
// drifting toward the limit as the kernel caches the sqlite file. Off-Linux (a dev mac) /proc is
// absent and the numbers degrade gracefully (cpuPercent stays null; memory falls back to this
// process's own RSS).
//
// Shared verbatim with apps/wiki-demo/src/machine-stats.ts — the same "tiny machine" footprint
// badge, here measuring the box that takes the write load instead of the Wikimedia ingester's.

import { readFileSync } from "node:fs";
import os from "node:os";

const PAGE_SIZE = 4096; // Linux x86-64 page size; /proc/<pid>/statm is in pages.

export interface MachineSample {
  /** Logical CPUs the process sees. */
  vcpus: number;
  /** Whole-VM CPU utilization 0..100 across all cores, or null until the first interval elapses. */
  cpuPercent: number | null;
  /** Resident memory of the app processes (this Node tier + rindled), in bytes. */
  memUsedBytes: number;
  /** The host/cgroup memory limit, in bytes. */
  memLimitBytes: number;
}

export interface MachineStats {
  read(): MachineSample;
  stop(): void;
}

/** Parse the aggregate `cpu …` line of /proc/stat into {busy,total} jiffies (all cores summed). */
function readProcStat(): { busy: number; total: number } | null {
  try {
    const line = readFileSync("/proc/stat", "utf8").split("\n", 1)[0];
    // cpu  user nice system idle iowait irq softirq steal guest guest_nice
    const parts = line.trim().split(/\s+/).slice(1).map(Number);
    if (parts.length < 5 || parts.some((n) => Number.isNaN(n))) return null;
    const total = parts.reduce((a, b) => a + b, 0);
    const idle = parts[3] + (parts[4] ?? 0); // idle + iowait
    return { busy: total - idle, total };
  } catch {
    return null;
  }
}

/** Resident set size of a pid via /proc/<pid>/statm (field 2 = resident pages). 0 if unreadable. */
function rssOf(pid: number): number {
  try {
    const fields = readFileSync(`/proc/${pid}/statm`, "utf8").trim().split(/\s+/);
    return (Number(fields[1]) || 0) * PAGE_SIZE;
  } catch {
    return 0;
  }
}

/** Begin sampling whole-VM CPU on a FIXED cadence — independent of how often the badge is polled, so
 *  many concurrent viewers hitting /metrics can't corrupt the delta. `extraPids` are processes to add
 *  to this Node process's own RSS (i.e. the spawned rindled), so the memory number is the whole app. */
export function startMachineStats(extraPids: number[] = [], sampleMs = 2000): MachineStats {
  const vcpus = os.cpus().length || 1;
  let cpuPercent: number | null = null;
  let prev = readProcStat();

  const sample = () => {
    const now = readProcStat();
    if (now && prev && now.total > prev.total) {
      const dBusy = now.busy - prev.busy;
      const dTotal = now.total - prev.total;
      cpuPercent = Math.max(0, Math.min(100, (dBusy / dTotal) * 100));
    }
    if (now) prev = now;
  };

  const timer = setInterval(sample, sampleMs);
  timer.unref?.(); // never keep the process alive for telemetry alone

  return {
    read(): MachineSample {
      let memUsedBytes = process.memoryUsage().rss;
      for (const pid of extraPids) memUsedBytes += rssOf(pid);
      return { vcpus, cpuPercent, memUsedBytes, memLimitBytes: os.totalmem() };
    },
    stop() {
      clearInterval(timer);
    },
  };
}
