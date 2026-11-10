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

/** A history row that records a run. Rows with `event` set (BRAIN-730 `supervisor-error`) are diagnostics, never runs. */
export const isRunRow = (row) => row !== null && typeof row === 'object' && row.event === undefined;

const readText = (file) => {
  try {
    return fs.readFileSync(file, 'utf8');
  } catch {
    return null;
  }
};

/** One JSONL line -> its run row, or null when it is blank, torn, a diagnostic row, or (windowed reader) lacks `include`. The ONE
 *  definition of that tolerance, so the two history readers below cannot drift. */
function parseRunLine(line, include) {
  if (include !== null && !line.includes(include)) return null;
  const text = typeof line === 'string' ? line : line.toString('utf8');
  if (!text.trim()) return null;
  let row;
  try {
    row = JSON.parse(text);
  } catch {
    return null; // a torn trailing line must not hide the rest of the history
  }
  return isRunRow(row) ? row : null;
}

/** Tolerant JSONL reader: a torn or partial line is skipped, never fatal. */
export function readRows(file) {
  const text = readText(file);
  if (text === null) return [];
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

/** history.jsonl's run rows: diagnostic rows (`event` set) are skipped. */
export function readHistoryRows(file) {
  const text = readText(file);
  if (text === null) return [];
  const rows = [];
  for (const line of text.split('\n')) {
    const row = parseRunLine(line, null);
    if (row !== null) rows.push(row);
  }
  return rows;
}

/** How much of the tail `readRecentHistoryRows` reads at a time. The window, not this, decides coverage. */
export const RECENT_READ_CHUNK_BYTES = 1 << 20;
/** How far past the window `readRecentHistoryRows` keeps scanning for rows appended out of start order. */
export const RECENT_READ_STOP_MARGIN_MS = 24 * 3_600_000;
const NEWLINE = 0x0a;

/** A buffer's newline-delimited lines. Reading backwards, a buffer that does not start at the file's start opens mid-line, so its
 *  bytes before the first newline come back as `carry`, to be completed by the chunk before it. Splitting on the newline BYTE is
 *  safe in utf8: no multi-byte sequence contains one, so no line is ever cut inside a character. */
function splitLines(buf, atFileStart) {
  const lines = [];
  let from = 0;
  let carry = Buffer.alloc(0);
  if (!atFileStart) {
    const first = buf.indexOf(NEWLINE);
    if (first === -1) return { lines, carry: buf };
    carry = buf.subarray(0, first);
    from = first + 1;
  }
  for (let at = buf.indexOf(NEWLINE, from); at !== -1; at = buf.indexOf(NEWLINE, at + 1)) {
    lines.push(buf.subarray(from, at));
    from = at + 1;
  }
  lines.push(buf.subarray(from)); // the file's unterminated tail, or (mid-file) empty
  return { lines, carry };
}

/** `buf.length` bytes at `position`, or as many as the file has left. */
function readAt(fd, buf, position) {
  let offset = 0;
  while (offset < buf.length) {
    const got = fs.readSync(fd, buf, offset, buf.length - offset, position + offset);
    if (got === 0) break;
    offset += got;
  }
  return offset === buf.length ? buf : buf.subarray(0, offset);
}

/**
 * BRAIN-899 B2: the run rows of `file`'s tail that are at least as new as `sinceMs`, read backwards in chunks so the WINDOW, not
 * a byte cap, decides coverage. Rows come back in file order, with the same parser (so the same tolerance for torn lines and
 * diagnostic rows) as `readHistoryRows`; a row without `startedAt` is kept, as it is there. A row is appended when it ENDS, so file
 * order is not start order: a row older than the window is skipped, and the scan stops only at a row that ended (or, without
 * `endedAt`, started) more than RECENT_READ_STOP_MARGIN_MS before `sinceMs` -- a queued-then-long run, a late reconcile row or one
 * bogus timestamp cannot cut the window short. `include` is a substring a raw line must contain before it is parsed -- a placement
 * caller passes `"repo":"<id>"`, exactly as `appendHistory`'s JSON.stringify writes it -- so the parse cost scales with that repo's
 * rows, not with the whole history.
 */
export function readRecentHistoryRows(file, { sinceMs = null, include = null } = {}) {
  let fd;
  try {
    fd = fs.openSync(file, 'r');
  } catch {
    return [];
  }
  try {
    const rows = [];
    let carry = Buffer.alloc(0);
    let end = fs.fstatSync(fd).size;
    let stopped = false;
    const take = (line) => {
      const row = parseRunLine(line, include);
      if (row === null) return;
      if (sinceMs === null || !Number.isFinite(row.startedAt) || row.startedAt >= sinceMs) {
        rows.push(row);
        return;
      }
      const settledAt = Number.isFinite(row.endedAt) ? row.endedAt : row.startedAt;
      if (settledAt < sinceMs - RECENT_READ_STOP_MARGIN_MS) stopped = true;
    };
    while (end > 0 && !stopped) {
      const start = Math.max(0, end - RECENT_READ_CHUNK_BYTES);
      const chunk = splitLines(Buffer.concat([readAt(fd, Buffer.allocUnsafe(end - start), start), carry]), start === 0);
      carry = chunk.carry;
      for (let i = chunk.lines.length - 1; i >= 0 && !stopped; i -= 1) take(chunk.lines[i]);
      end = start;
    }
    return rows.reverse();
  } finally {
    fs.closeSync(fd);
  }
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
    // An elastic run was granted fewer cores than declared and its workers sized from the grant, so its raw
    // mean understates need: scale it back to declared-equivalent. Peak stays raw (information only).
    const rowDeclared = (r) => r.resources?.cpuCores ?? r.weight;
    const isElastic = (r) => Number.isFinite(r.grantedCpuCores) && r.grantedCpuCores > 0;
    const finished = g.rows.filter((r) => Number.isFinite(r.startedAt) && Number.isFinite(r.endedAt));
    const unfinishedRuns = g.rows.length - finished.length;
    // an elastic row with no usable declaration cannot be scaled; counting it raw would be wrong, scaling it NaN
    const scalable = finished.filter((r) => !isElastic(r) || (Number.isFinite(rowDeclared(r)) && rowDeclared(r) > 0));
    const malformedRuns = finished.length - scalable.length;
    // Runs under OVERRUN_MIN_MS are mostly process startup, so their mean says nothing about sustained need.
    // a remote row's endedAt is the local receipt time, so its runner-measured runMs is the run; a local row's span is
    const sustained = scalable.filter((r) => (Number.isFinite(r.runMs) ? r.runMs : r.endedAt - r.startedAt) >= OVERRUN_MIN_MS);
    const shortRunsExcluded = scalable.length - sustained.length;
    const elastic = sustained.filter(isElastic);
    const grantedBelowDeclaredPct = sustained.length === 0 ? 0 : Math.round((100 * elastic.filter((r) => r.grantedCpuCores < rowDeclared(r)).length) / sustained.length);
    // the bucket's rows are in lane order, not time order: the declaration is the NEWEST row's that has a usable
    // one; a group with none can't be judged, so it reports insufficient rather than a NaN-driven label
    const declaredRows = g.rows.filter((r) => Number.isFinite(rowDeclared(r)) && rowDeclared(r) > 0);
    const newest = declaredRows.length > 0 ? declaredRows.reduce((a, r) => (rowTime(r) >= rowTime(a) ? r : a)) : null;
    const declared = newest ? rowDeclared(newest) : null;
    if (declared === null || sustained.length < MIN_RUNS) {
      groups.push({ repo: g.repo, lane: g.lane, grouping: g.grouping, runs: sustained.length, shortRunsExcluded, unfinishedRuns, malformedRuns, foldedLaneNames: foldedNames(g), declaredCpuCores: declared, meanP50: null, meanP90: null, p50Peak: null, p90Peak: null, elasticRuns: elastic.length, grantedBelowDeclaredPct, suggestedCpuCores: null, verdict: 'insufficient' });
      continue;
    }
    const need = (r) => (isElastic(r) ? (r.observedCpu.mean / r.grantedCpuCores) * rowDeclared(r) : r.observedCpu.mean);
    const means = sustained.map(need).sort((a, b) => a - b);
    const peaks = sustained.map((r) => r.observedCpu.peak).sort((a, b) => a - b);
    // The label is judged on the raw ceil(p90 mean); the reported suggestion is floored at 1 whole core
    // (config accepts any positive cpuCores, so a fractional booking like 0.5 is legal but not worth suggesting).
    const needed = Math.ceil(percentile(means, 0.9));
    const suggested = Math.max(1, needed);
    // Same tolerance as overrun detection: ceil rounding alone (p90 4.15 on a booking of 4) is not under-booking.
    const verdict = percentile(means, 0.9) > OVERRUN_FACTOR * declared ? 'under-booked' : needed <= OVERBOOKED_FRACTION * declared ? 'over-booked' : 'ok';
    groups.push({ repo: g.repo, lane: g.lane, grouping: g.grouping, runs: means.length, shortRunsExcluded, unfinishedRuns, malformedRuns, foldedLaneNames: foldedNames(g), elasticRuns: elastic.length, grantedBelowDeclaredPct, declaredCpuCores: declared, meanP50: percentile(means, 0.5), meanP90: percentile(means, 0.9), p50Peak: percentile(peaks, 0.5), p90Peak: percentile(peaks, 0.9), suggestedCpuCores: suggested, verdict });
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
      lines.push(`  ${g.repo}:${g.lane}  declared=${g.declaredCpuCores}  insufficient sustained runs (${g.shortRunsExcluded} short excluded)${g.unfinishedRuns + g.malformedRuns > 0 ? `, ${g.unfinishedRuns + g.malformedRuns} unusable` : ''}`);
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
  const report = buildSuggestions(readHistoryRows(paths(ensureStateDirs().root).history), { repo, days });
  process.stdout.write(json ? `${JSON.stringify(report, null, 2)}\n` : `${renderSuggestText(report)}\n`);
  return { exitCode: 0 };
}
