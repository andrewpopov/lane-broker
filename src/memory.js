import fs from 'node:fs';
import { execFileSync } from 'node:child_process';

/**
 * Test-only override, the memory-telemetry equivalent of load.js's
 * readLoadAvg / LANE_BROKER_LOADAVG_FILE: when LANE_BROKER_MEMORY_FILE is
 * set, its first line is "swapUsedBytes,swapTotalBytes,compressorBytes" and
 * readMemorySample returns that directly instead of shelling out to
 * sysctl/vm_stat. A bad or missing file falls through to the real reading —
 * same fallback behavior as readLoadAvg.
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
    const [usedStr, totalStr, compressorStr] = parts;
    const swapUsedBytes = numeric(usedStr);
    const swapTotalBytes = numeric(totalStr);
    const compressorBytes = numeric(compressorStr);
    if (Number.isFinite(swapUsedBytes) && Number.isFinite(swapTotalBytes) && Number.isFinite(compressorBytes)) {
      return { swapUsedBytes, swapTotalBytes, compressorBytes, sampledAt: Date.now() };
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

/**
 * Pure parse of `vm_stat`'s raw text into `{ pageSize, compressorPages }`.
 * Either field missing/malformed yields both as null rather than a partial,
 * half-fabricated reading.
 */
export function parseVmStat(text) {
  const pageSizeMatch = PAGE_SIZE_RE.exec(text || '');
  const compressorMatch = COMPRESSOR_RE.exec(text || '');
  const pageSize = pageSizeMatch ? Number(pageSizeMatch[1]) : NaN;
  const compressorPages = compressorMatch ? Number(compressorMatch[1]) : NaN;
  if (!Number.isFinite(pageSize) || !Number.isFinite(compressorPages)) {
    return { pageSize: null, compressorPages: null };
  }
  return { pageSize, compressorPages };
}

/**
 * Best-effort swap + compressor-footprint reading. INFORMATIONAL ONLY
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
    } catch {
      compressorBytes = null; // vm_stat missing/failed: the swap reading alone is still useful
    }
    return { swapUsedBytes: swap.swapUsedBytes, swapTotalBytes: swap.swapTotalBytes, compressorBytes, sampledAt: Date.now() };
  } catch {
    return null; // sysctl missing/failed: never let this crash a status poll
  }
}

// Swap-used thresholds, chosen against the 2026-09-09 incident (42GB swap on
// a thrashing host): `tight` starts well ahead of exhaustion so an operator
// sees pressure building, `exhausted` is reserved for the range that
// actually correlates with a machine that's thrashing.
export const SWAP_TIGHT_PCT = 50;
export const SWAP_EXHAUSTED_PCT = 90;

/**
 * Pure classification of a raw sample into a renderable reading — the
 * memory-telemetry equivalent of updateGateState, minus any state to carry
 * between calls: there is no hysteresis here, this is a point-in-time
 * display, never a gate. `null` in, `null` out; a caller must treat a null
 * classification as "unavailable", never coerce it into a bogus 0%.
 */
export function classifyMemorySample(sample) {
  if (!sample || !Number.isFinite(sample.swapTotalBytes) || !Number.isFinite(sample.swapUsedBytes)) {
    return null;
  }
  const { swapUsedBytes, swapTotalBytes, compressorBytes, sampledAt } = sample;
  const swapUsedPct = swapTotalBytes > 0 ? (swapUsedBytes / swapTotalBytes) * 100 : 0;
  let level = 'healthy';
  if (swapUsedPct >= SWAP_EXHAUSTED_PCT) level = 'exhausted';
  else if (swapUsedPct >= SWAP_TIGHT_PCT) level = 'tight';
  return {
    level,
    swapUsedBytes,
    swapTotalBytes,
    swapUsedPct,
    compressorBytes: Number.isFinite(compressorBytes) ? compressorBytes : null,
    sampledAt: sampledAt ?? null,
  };
}
