import fs from 'node:fs';
import os from 'node:os';
import { execFileSync } from 'node:child_process';

/**
 * Test-only override, the memory-telemetry equivalent of load.js's
 * readLoadAvg / LANE_BROKER_LOADAVG_FILE: when LANE_BROKER_MEMORY_FILE is
 * set, its first line is
 * "swapUsedBytes,swapTotalBytes,compressorBytes[,availableBytes,totalMemoryBytes]"
 * and readMemorySample returns that directly instead of shelling out to
 * sysctl/vm_stat. A bad or missing file falls through to the real reading —
 * same fallback behavior as readLoadAvg. The last two fields are optional
 * (BRAIN-273 added them after the swap/compressor fields already existed);
 * an override that omits them yields `availableBytes`/`totalMemoryBytes` as
 * null, which classifyMemorySample treats as "unavailable" — never a
 * fabricated availability reading.
 */
function readMemoryOverride() {
  const file = process.env.LANE_BROKER_MEMORY_FILE;
  if (!file) return null;
  try {
    const first = fs.readFileSync(file, 'utf8').split('\n')[0].trim();
    const parts = first.split(',');
    // Number('') and Number(' ') are 0, not NaN — so a field-count-correct but
    // EMPTY override (",," ) would coerce to three zeros and be accepted as a
    // real 0-of-0 sample, rendering "HEALTHY 0.0%" instead of falling through
    // as this function's contract promises. Reject blank fields explicitly.
    const numeric = (v) => (typeof v === 'string' && v.trim() !== '' ? Number(v) : NaN);
    const [usedStr, totalStr, compressorStr, availableStr, totalMemStr] = parts;
    const swapUsedBytes = numeric(usedStr);
    const swapTotalBytes = numeric(totalStr);
    const compressorBytes = numeric(compressorStr);
    if (Number.isFinite(swapUsedBytes) && Number.isFinite(swapTotalBytes) && Number.isFinite(compressorBytes)) {
      const availableBytes = numeric(availableStr);
      const totalMemoryBytes = numeric(totalMemStr);
      return {
        swapUsedBytes,
        swapTotalBytes,
        compressorBytes,
        availableBytes: Number.isFinite(availableBytes) ? availableBytes : null,
        totalMemoryBytes: Number.isFinite(totalMemoryBytes) ? totalMemoryBytes : null,
        sampledAt: Date.now(),
      };
    }
  } catch {
    // fall through to the real reading
  }
  return null;
}

const SWAP_USAGE_RE = /total\s*=\s*([\d.]+)M\s+used\s*=\s*([\d.]+)M/;

/**
 * Pure parse of `sysctl -n vm.swapusage`'s raw text, e.g.
 * "total = 2048.00M  used = 512.00M  free = 1536.00M  (encrypted)". Returns
 * null on anything that doesn't match rather than fabricating a number.
 */
export function parseSwapUsage(text) {
  const m = SWAP_USAGE_RE.exec(text || '');
  if (!m) return null;
  const swapTotalBytes = Math.round(Number(m[1]) * 1024 * 1024);
  const swapUsedBytes = Math.round(Number(m[2]) * 1024 * 1024);
  if (!Number.isFinite(swapTotalBytes) || !Number.isFinite(swapUsedBytes)) return null;
  return { swapTotalBytes, swapUsedBytes };
}

const PAGE_SIZE_RE = /page size of (\d+) bytes/;
const COMPRESSOR_RE = /Pages occupied by compressor:\s*(\d+)\./;
const FREE_RE = /Pages free:\s*(\d+)\./;
const INACTIVE_RE = /Pages inactive:\s*(\d+)\./;
const SPECULATIVE_RE = /Pages speculative:\s*(\d+)\./;

/** Shared page-size extraction for every vm_stat parser below. */
function parseVmStatPageSize(text) {
  const match = PAGE_SIZE_RE.exec(text || '');
  return match ? Number(match[1]) : NaN;
}

/**
 * Pure parse of `vm_stat`'s raw text into `{ pageSize, compressorPages }`.
 * Either field missing/malformed yields both as null rather than a partial,
 * half-fabricated reading.
 */
export function parseVmStat(text) {
  const pageSize = parseVmStatPageSize(text);
  const compressorMatch = COMPRESSOR_RE.exec(text || '');
  const compressorPages = compressorMatch ? Number(compressorMatch[1]) : NaN;
  if (!Number.isFinite(pageSize) || !Number.isFinite(compressorPages)) {
    return { pageSize: null, compressorPages: null };
  }
  return { pageSize, compressorPages };
}

/**
 * Pure parse of `vm_stat`'s raw text into macOS's actually-available memory,
 * in bytes: (free + inactive + speculative) pages × page size (BRAIN-252).
 * `os.freemem()` on macOS counts only free pages and so undercounts
 * available memory by an order of magnitude — inactive and speculative
 * pages are just as reclaimable on demand. Purgeable pages are deliberately
 * excluded: on macOS they are a property of pages already counted in
 * inactive, so adding them would double-count. Requires the page size and
 * all three counters; any missing/malformed field returns null rather than
 * a partial, half-fabricated sum.
 */
export function parseVmStatAvailable(text) {
  const pageSize = parseVmStatPageSize(text);
  const freeMatch = FREE_RE.exec(text || '');
  const inactiveMatch = INACTIVE_RE.exec(text || '');
  const speculativeMatch = SPECULATIVE_RE.exec(text || '');
  const freePages = freeMatch ? Number(freeMatch[1]) : NaN;
  const inactivePages = inactiveMatch ? Number(inactiveMatch[1]) : NaN;
  const speculativePages = speculativeMatch ? Number(speculativeMatch[1]) : NaN;
  if (!Number.isFinite(pageSize) || !Number.isFinite(freePages) || !Number.isFinite(inactivePages) || !Number.isFinite(speculativePages)) {
    return null;
  }
  return pageSize * (freePages + inactivePages + speculativePages);
}

/**
 * Best-effort swap/compressor/availability reading. INFORMATIONAL ONLY
 * (BRAIN-211) — sampled fresh on every `lane status` call, never persisted,
 * and never fed into an admission decision. See BRAIN-207's postmortem: on
 * 2026-09-09 a runaway `coreaudiod` (nothing to do with any lane) thrashed
 * this machine for 8 hours at 42GB swap; a memory GATE would have starved
 * every lane over it, exactly the mistake BRAIN-207 already reversed for
 * load average. This is detection, not admission.
 *
 * Never throws: a non-macOS host, or any probe failure, degrades to null —
 * the same fail-safe shape as readMemoryInfo's macPressure probe in cpu.js
 * (a separate, unrelated reading).
 */
export function readMemorySample(exec = execFileSync) {
  const override = readMemoryOverride();
  if (override) return override;
  if (process.platform !== 'darwin') return null;
  try {
    // 2s bound, same precedent as every other exec-based sampler here
    // (cpu.js's sampleHostCpu/readMemoryInfo): a hung sysctl must never be
    // able to stall a status poll indefinitely.
    const swapOut = exec('sysctl', ['-n', 'vm.swapusage'], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 2000,
    });
    const swap = parseSwapUsage(swapOut);
    if (!swap) return null;
    let compressorBytes = null;
    let availableBytes = null;
    try {
      const vmStatOut = exec('vm_stat', [], {
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'ignore'],
        timeout: 2000,
      });
      const { pageSize, compressorPages } = parseVmStat(vmStatOut);
      if (pageSize != null && compressorPages != null) {
        compressorBytes = pageSize * compressorPages;
      }
      // BRAIN-252's already-proven availability parse (free + inactive +
      // speculative), reused here as the PRIMARY classification signal
      // below rather than a second, disagreeing metric.
      availableBytes = parseVmStatAvailable(vmStatOut);
    } catch {
      compressorBytes = null; // vm_stat missing/failed: the swap reading alone is still useful
      availableBytes = null;
    }
    return {
      swapUsedBytes: swap.swapUsedBytes,
      swapTotalBytes: swap.swapTotalBytes,
      compressorBytes,
      availableBytes,
      totalMemoryBytes: os.totalmem(),
      sampledAt: Date.now(),
    };
  } catch {
    return null; // sysctl missing/failed: never let this crash a status poll
  }
}

// BRAIN-273: thresholds against the fraction of total RAM that is actually
// AVAILABLE (parseVmStatAvailable / os.totalmem()), not against the swap
// ratio. A swap-used-percent threshold cannot work on macOS: the kernel
// sizes the swap file dynamically and does not shrink it back down once
// used, so swapUsed/swapTotal trends toward 100% on any long-uptime machine
// regardless of current pressure -- it measures how much swap has EVER been
// needed, not how constrained the machine is now. The incident that forced
// this rewrite: `lane status` reported EXHAUSTED (swap 95.8% used) while
// `memory_pressure` reported 44% free and vm_stat's own available-memory
// sum (free+inactive+speculative) was 17.06 GiB out of a 4d16h-uptime host
// whose pageouts (316,512) were flat against 122M pageins -- a machine
// under real pressure pages OUT, and this one wasn't. Compressor footprint
// has the identical flaw for the identical reason (a large compressor is
// macOS reclaiming memory well, not a sign of a squeeze) and was already
// demoted to display-only context alongside swap; neither is exported as a
// threshold anymore, and neither must be reinstated as the classifier.
export const AVAILABLE_TIGHT_PCT = 15;
export const AVAILABLE_EXHAUSTED_PCT = 8;

/**
 * Pure classification of a raw sample into a renderable reading — the
 * memory-telemetry equivalent of updateGateState, minus any state to carry
 * between calls: there is no hysteresis here, this is a point-in-time
 * display, never a gate. `null` in, `null` out; a caller must treat a null
 * classification as "unavailable", never coerce it into a bogus 0% or fall
 * back to the swap ratio -- see AVAILABLE_TIGHT_PCT's comment above for why
 * that fallback is exactly the bug this function used to have.
 *
 * Requires `availableBytes`/`totalMemoryBytes` (the real pressure signal);
 * swap/compressor are carried through only as unenforced display context
 * (footprint, not pressure) and default to null when absent, same pattern
 * as `compressorBytes` always used.
 */
export function classifyMemorySample(sample) {
  if (
    !sample ||
    !Number.isFinite(sample.availableBytes) ||
    !Number.isFinite(sample.totalMemoryBytes) ||
    sample.totalMemoryBytes <= 0
  ) {
    return null;
  }
  const { availableBytes, totalMemoryBytes, swapUsedBytes, swapTotalBytes, compressorBytes, sampledAt } = sample;
  const availablePct = (availableBytes / totalMemoryBytes) * 100;
  let level = 'healthy';
  if (availablePct <= AVAILABLE_EXHAUSTED_PCT) level = 'exhausted';
  else if (availablePct <= AVAILABLE_TIGHT_PCT) level = 'tight';
  const hasSwap = Number.isFinite(swapUsedBytes) && Number.isFinite(swapTotalBytes);
  const swapUsedPct = hasSwap && swapTotalBytes > 0 ? (swapUsedBytes / swapTotalBytes) * 100 : hasSwap ? 0 : null;
  return {
    level,
    availableBytes,
    totalMemoryBytes,
    availablePct,
    swapUsedBytes: hasSwap ? swapUsedBytes : null,
    swapTotalBytes: hasSwap ? swapTotalBytes : null,
    swapUsedPct,
    compressorBytes: Number.isFinite(compressorBytes) ? compressorBytes : null,
    sampledAt: sampledAt ?? null,
  };
}
