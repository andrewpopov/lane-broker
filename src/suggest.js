import fs from 'node:fs';
import { ensureStateDirs, paths } from './state.js';
import { OVERRUN_FACTOR, OVERRUN_MIN_MS } from './observed.js';

/**
 * BRAIN-361 `lane suggest`: from history.jsonl, compare each lane's DECLARED cpuCores with the
 * SUSTAINED cores (per-run time-weighted mean) its finished runs actually used; peak is shown, never sized from. Read-only; never edits config.
 */

export const MIN_RUNS = 5;
export const OVERBOOKED_FRACTION = 0.5;

const foldedNames = (g) => new Set(g.rows.map((r) => r.lane).filter((l) => l !== g.lane)).size;
const rowTime = (r) => (Number.isFinite(r.endedAt) ? r.endedAt : r.startedAt);

/** Nearest-rank percentile of an ascending-sorted array. */
function percentile(sorted, p) {
  return sorted[Math.max(0, Math.ceil(p * sorted.length) - 1)];
}

function readRows(file) {
  let text;
  try {
    text = fs.readFileSync(file, 'utf8');
  } catch {
    return [];
  }
  const rows = [];
  for (const line of text.split('\n')) {
    if (!line.trim()) continue;
    try {
      rows.push(JSON.parse(line));
    } catch {
      // a torn trailing line must not hide the rest of the history
    }
  }
  return rows;
}

/** Pure: rows -> { groups, skippedNoObservation }. Declared cpuCores is the group's most recent row's. */
export function buildSuggestions(rows, { repo, days = 7, now = Date.now() } = {}) {
  const since = now - days * 24 * 3600 * 1000;
  const byLane = new Map();
  let skippedNoObservation = 0;
  for (const row of rows) {
    if (!row || typeof row !== 'object' || (repo && row.repo !== repo) || !row.lane) continue;
    if (Number.isFinite(row.endedAt) && row.endedAt < since) continue;
    if (!Number.isFinite(row.startedAt)) continue; // never ran (refusal, queue timeout)
    if (!Number.isFinite(row.observedCpu?.peak) || !Number.isFinite(row.observedCpu?.mean)) {
      skippedNoObservation += 1;
      continue;
    }
    const key = `${row.repo}\u0000${row.lane}`;
    if (!byLane.has(key)) byLane.set(key, { repo: row.repo, lane: row.lane, rows: [] });
    byLane.get(key).rows.push(row);
  }
  // BRAIN-361: an ad-hoc lane (zirk812) is grouped under the declared lane it inherited (`configLane`).
  // A bucket of rows from before that field existed (none carries it) that is too small to report is
  // pooled with its repo's other such leftovers that share a resources signature.
  const buckets = new Map();
  const add = (key, init, row) => {
    if (!buckets.has(key)) buckets.set(key, { ...init, rows: [] });
    buckets.get(key).rows.push(row);
  };
  for (const g of byLane.values()) {
    for (const r of g.rows) add(`l\u0000${g.repo}\u0000${r.configLane ?? r.lane}`, { repo: g.repo, lane: r.configLane ?? r.lane, grouping: 'lane' }, r);
  }
  for (const [key, bucket] of [...buckets]) {
    if (bucket.rows.length >= MIN_RUNS || bucket.rows.some((r) => r.configLane)) continue;
    buckets.delete(key);
    for (const r of bucket.rows) {
      const { cpuCores, memoryBytes, minCpuCores } = r.resources ?? {};
      add(`s\u0000${r.repo}\u0000${cpuCores}|${memoryBytes}|${minCpuCores}`, { repo: r.repo, lane: '(by resources)', grouping: 'resources' }, r);
    }
  }
  const groups = [];
  const tooFewRuns = [];
  for (const g of buckets.values()) {
    if (g.rows.length < MIN_RUNS) {
      tooFewRuns.push({ repo: g.repo, lane: g.lane, runs: g.rows.length });
      continue;
    }
    // Runs under OVERRUN_MIN_MS are mostly process startup, so their mean says nothing about sustained need.
    const sustained = g.rows.filter((r) => !(r.endedAt - r.startedAt < OVERRUN_MIN_MS));
    const shortRunsExcluded = g.rows.length - sustained.length;
    // the bucket's rows are in lane order, not time order: the declaration is the NEWEST row's
    const newest = g.rows.reduce((a, r) => (rowTime(r) >= rowTime(a) ? r : a));
    const declared = newest.resources?.cpuCores ?? newest.weight;
    if (sustained.length < MIN_RUNS) {
      groups.push({ repo: g.repo, lane: g.lane, grouping: g.grouping, runs: sustained.length, shortRunsExcluded, foldedLaneNames: foldedNames(g), declaredCpuCores: declared, meanP50: null, meanP90: null, p50Peak: null, p90Peak: null, elasticRuns: 0, grantedBelowDeclaredPct: 0, suggestedCpuCores: null, verdict: 'insufficient' });
      continue;
    }
    // An elastic run was granted fewer cores than declared and its workers sized from the grant, so its raw
    // mean understates need: scale it back to declared-equivalent. Peak stays raw (information only).
    const rowDeclared = (r) => r.resources?.cpuCores ?? r.weight;
    const isElastic = (r) => Number.isFinite(r.grantedCpuCores) && r.grantedCpuCores > 0;
    const need = (r) => (isElastic(r) ? (r.observedCpu.mean / r.grantedCpuCores) * rowDeclared(r) : r.observedCpu.mean);
    const means = sustained.map(need).sort((a, b) => a - b);
    const peaks = sustained.map((r) => r.observedCpu.peak).sort((a, b) => a - b);
    const elastic = sustained.filter(isElastic);
    const grantedBelowDeclaredPct = Math.round((100 * elastic.filter((r) => r.grantedCpuCores < rowDeclared(r)).length) / sustained.length);
    // The label is judged on the raw ceil(p90 mean); the reported suggestion is floored at 1 whole core
    // (config accepts any positive cpuCores, so a fractional booking like 0.5 is legal but not worth suggesting).
    const needed = Math.ceil(percentile(means, 0.9));
    const suggested = Math.max(1, needed);
    // Same tolerance as overrun detection: ceil rounding alone (p90 4.15 on a booking of 4) is not under-booking.
    const verdict = percentile(means, 0.9) > OVERRUN_FACTOR * declared ? 'under-booked' : needed <= OVERBOOKED_FRACTION * declared ? 'over-booked' : 'ok';
    groups.push({ repo: g.repo, lane: g.lane, grouping: g.grouping, runs: means.length, shortRunsExcluded, foldedLaneNames: foldedNames(g), elasticRuns: elastic.length, grantedBelowDeclaredPct, declaredCpuCores: declared, meanP50: percentile(means, 0.5), meanP90: percentile(means, 0.9), p50Peak: percentile(peaks, 0.5), p90Peak: percentile(peaks, 0.9), suggestedCpuCores: suggested, verdict });
  }
  const order = { 'under-booked': 0, 'over-booked': 1, ok: 2, insufficient: 3 };
  groups.sort((a, b) => order[a.verdict] - order[b.verdict] || a.repo.localeCompare(b.repo) || a.lane.localeCompare(b.lane));
  return { days, minRuns: MIN_RUNS, groups, tooFewRuns, skippedNoObservation };
}

export function renderSuggestText(report) {
  const lines = [`lane suggest: last ${report.days}d, groups with >= ${report.minRuns} observed runs (suggested = ceil(p90 mean) over runs >= 2 min; peak is informational)`];
  if (report.groups.length === 0) lines.push('  (no lane has enough observed runs yet)');
  for (const g of report.groups) {
    if (g.verdict === 'insufficient') {
      lines.push(`  ${g.repo}:${g.lane}  declared=${g.declaredCpuCores}  insufficient sustained runs (${g.shortRunsExcluded} short excluded)`);
      continue;
    }
    const elasticNote = g.grantedBelowDeclaredPct > 0 ? `  elastic: ${g.grantedBelowDeclaredPct}% of runs granted below declared` : '';
    const note = g.verdict === 'under-booked' ? '  UNDER-BOOKED' : g.verdict === 'over-booked' ? '  over-booked (wastes capacity)' : '';
    lines.push(
      `  ${g.repo}:${g.lane}  runs=${g.runs}${g.foldedLaneNames > 0 ? ` (${g.foldedLaneNames} ad-hoc lane names folded)` : ''}  declared=${g.declaredCpuCores}  mean p50/p90=${g.meanP50}/${g.meanP90}  peak p50/p90=${g.p50Peak}/${g.p90Peak}  suggested=${g.suggestedCpuCores}${note}${elasticNote}`,
    );
  }
  lines.push(`skipped: ${report.skippedNoObservation} finished run(s) without observedCpu; ${report.tooFewRuns.length} lane(s) with < ${report.minRuns} observed runs`);
  return lines.join('\n');
}

export async function suggestCommand({ repo, days, json } = {}) {
  const report = buildSuggestions(readRows(paths(ensureStateDirs().root).history), { repo, days });
  process.stdout.write(json ? `${JSON.stringify(report, null, 2)}\n` : `${renderSuggestText(report)}\n`);
  return { exitCode: 0 };
}
