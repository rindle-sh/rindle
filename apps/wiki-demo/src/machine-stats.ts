// Resource telemetry for the "tiny machine" badge and profiler. Whole-VM CPU still shows noisy
// neighbors/kernel work, while component samples make regressions attributable: the Node API +
// ingester, HCTree write-master, and wal2 read-follower each get their own CPU and RSS reading.
//
// CPU percentages use the same 0..100 whole-machine scale as `cpuPercent`. A process saturating one
// core on a 2-vCPU machine therefore reports about 50%, and the three process values can be added
// and compared directly with whole-VM CPU. Off Linux, /proc is absent and CPU degrades to null;
// Node RSS still comes from `process.memoryUsage()` while child RSS becomes zero.

import { readFileSync } from "node:fs";
import os from "node:os";

export interface MachineProcessPids {
  masterPid?: number;
  followerPid?: number;
}

export interface MachineSample {
  /** Logical CPUs the process sees. */
  vcpus: number;
  /** Whole-VM CPU utilization 0..100 across all cores, or null until the first interval elapses. */
  cpuPercent: number | null;
  /** Per-process CPU shares on that same whole-machine 0..100 scale. */
  nodeCpuPercent: number | null;
  masterCpuPercent: number | null;
  followerCpuPercent: number | null;
  /** Per-process resident sets. */
  nodeRssBytes: number;
  masterRssBytes: number;
  followerRssBytes: number;
  /** Sum of the three resident sets, retained for the existing aggregate badge. */
  memUsedBytes: number;
  /** The host/cgroup memory limit, in bytes. */
  memLimitBytes: number;
}

export interface MachineStats {
  read(): MachineSample;
  stop(): void;
}

interface CpuCounters {
  busy: number;
  total: number;
}

interface ProcessCpu {
  node: number | null;
  master: number | null;
  follower: number | null;
}

/** Parse the aggregate `cpu …` line of /proc/stat into jiffies summed across all cores. */
function readProcStat(): CpuCounters | null {
  try {
    const line = readFileSync("/proc/stat", "utf8").split("\n", 1)[0];
    // cpu  user nice system idle iowait irq softirq steal guest guest_nice
    const parts = line.trim().split(/\s+/).slice(1).map(Number);
    if (parts.length < 5 || parts.some((n) => Number.isNaN(n))) return null;
    // guest/guest_nice (fields 9/10) are already included in user/nice; summing them again makes
    // both whole-machine and per-process shares subtly wrong under virtualization.
    const total = parts.slice(0, 8).reduce((a, b) => a + b, 0);
    const idle = parts[3] + (parts[4] ?? 0); // idle + iowait
    return { busy: total - idle, total };
  } catch {
    return null;
  }
}

/** Read user + system jiffies (fields 14 + 15) without being fooled by spaces in `(comm)`. */
function processTicks(pid: number | undefined): number | null {
  if (pid === undefined) return null;
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
    const commEnd = stat.lastIndexOf(")");
    if (commEnd < 0) return null;
    // The suffix starts at proc field 3 (`state`), so utime/stime are indexes 11/12 here.
    const fields = stat
      .slice(commEnd + 1)
      .trim()
      .split(/\s+/);
    const ticks = Number(fields[11]) + Number(fields[12]);
    return Number.isFinite(ticks) ? ticks : null;
  } catch {
    return null;
  }
}

/** Resident set from /proc status (already normalized to KiB by Linux). 0 if unreadable. */
function rssOf(pid: number | undefined): number {
  if (pid === undefined) return 0;
  try {
    const match = readFileSync(`/proc/${pid}/status`, "utf8").match(
      /^VmRSS:\s+(\d+)\s+kB$/m,
    );
    return match ? Number(match[1]) * 1024 : 0;
  } catch {
    return 0;
  }
}

/**
 * Sample CPU on a fixed cadence, independent of `/metrics` polling. The deprecated array shape is
 * accepted only so callers can migrate without an all-at-once edit; it represents one follower.
 */
export function startMachineStats(
  processPids: MachineProcessPids | number[] = {},
  sampleMs = 2000,
): MachineStats {
  const pids: MachineProcessPids = Array.isArray(processPids)
    ? { followerPid: processPids[0] }
    : processPids;
  const vcpus = os.cpus().length || 1;
  let cpuPercent: number | null = null;
  let processCpu: ProcessCpu = { node: null, master: null, follower: null };
  let prevMachine = readProcStat();
  let prevTicks: ProcessCpu = {
    node: processTicks(process.pid),
    master: processTicks(pids.masterPid),
    follower: processTicks(pids.followerPid),
  };

  const sample = () => {
    const machine = readProcStat();
    const ticks: ProcessCpu = {
      node: processTicks(process.pid),
      master: processTicks(pids.masterPid),
      follower: processTicks(pids.followerPid),
    };

    if (machine && prevMachine && machine.total > prevMachine.total) {
      const dTotal = machine.total - prevMachine.total;
      cpuPercent = Math.max(
        0,
        Math.min(100, ((machine.busy - prevMachine.busy) / dTotal) * 100),
      );
      processCpu = {
        node: processShare(ticks.node, prevTicks.node, dTotal),
        master: processShare(ticks.master, prevTicks.master, dTotal),
        follower: processShare(ticks.follower, prevTicks.follower, dTotal),
      };
    } else {
      processCpu = { node: null, master: null, follower: null };
    }

    if (machine) prevMachine = machine;
    prevTicks = ticks;
  };

  const timer = setInterval(sample, sampleMs);
  timer.unref?.(); // never keep the process alive for telemetry alone

  return {
    read(): MachineSample {
      // process.memoryUsage works off Linux and is at least as current as parsing Node's proc file.
      const nodeRssBytes = process.memoryUsage().rss;
      const masterRssBytes = rssOf(pids.masterPid);
      const followerRssBytes = rssOf(pids.followerPid);
      return {
        vcpus,
        cpuPercent,
        nodeCpuPercent: processCpu.node,
        masterCpuPercent: processCpu.master,
        followerCpuPercent: processCpu.follower,
        nodeRssBytes,
        masterRssBytes,
        followerRssBytes,
        memUsedBytes: nodeRssBytes + masterRssBytes + followerRssBytes,
        memLimitBytes: os.totalmem(),
      };
    },
    stop() {
      clearInterval(timer);
    },
  };
}

function processShare(
  now: number | null,
  prev: number | null,
  machineDelta: number,
): number | null {
  if (now === null || prev === null || now < prev) return null;
  return Math.max(0, Math.min(100, ((now - prev) / machineDelta) * 100));
}
