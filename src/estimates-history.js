import { paths, stateHome } from './state.js';
import { recordObservation, estimateFor, emptyEstimator } from './estimates.js';
import { readRows } from './suggest.js';

/**
 * BRAIN-408 slice A: fold the local broker's history.jsonl through the estimator, read-only.
 * Local history only knows (repo, lane); history rows carry no job template, so the key is
 * template = `repo:lane`, class_id = the work class, and the narrower parts are empty.
 */

/** A row carries no class today, so honour `class` when a later row has one, else a lane named like a sim is a sim. */
export const workClassOf = (row) => (row.class === 'sim' || (row.class === undefined && /(^|[-_:.])sims?($|[-_:.0-9])/i.test(row.lane ?? '')) ? 'sim' : 'test');


/** `timeout`/`gtimeout` (optionally after `env [VAR=x ...]`) exits 124 when it kills its child: a timeout with no signal on the row. */
const TIMEOUT_WRAPPER = /^\s*(?:env\s+(?:-\S+\s+|\w+=\S*\s+)*)?g?timeout\b/;

/**
 * How a row counts: `ignored` (never started, cancelled, or failed on its own: a fast failure says nothing about
 * duration), `censored` (killed by a signal it did not ask for, or a `timeout` wrapper's exit 124: a lower bound), `completed` (exit 0), with its wall seconds.
 */
export function classifyRow(row) {
  if (!row || typeof row !== 'object' || !row.repo || !row.lane) return { kind: 'ignored', why: 'malformed' };
  if (!Number.isFinite(row.startedAt)) return { kind: 'ignored', why: 'never-started' };
  if (row.cancelled === true) return { kind: 'ignored', why: 'cancelled' };
  const wallS = Number.isFinite(row.runMs) && row.runMs > 0 ? row.runMs / 1000 : (row.endedAt - row.startedAt) / 1000;
  if (!(wallS > 0)) return { kind: 'ignored', why: 'no-duration' };
  if (row.signal || (row.exit === 124 && TIMEOUT_WRAPPER.test(row.command ?? ''))) return { kind: 'censored', wallS };
  if (row.exit === 0) return { kind: 'completed', wallS };
  return { kind: 'ignored', why: 'failed' };
}

const percentile = (sorted, q) => sorted[Math.max(0, Math.ceil(q * sorted.length) - 1)];

/** Pure: rows -> { keys, skipped }. `hostFactor` converts wall to ref-s (1.0: the host has no calibration). */
export function backfillEstimates(rows, { hostFactor = 1 } = {}) {
  const store = new Map();
  const groups = new Map();
  const skipped = {};
  const ordered = rows.filter((r) => r && typeof r === 'object').sort((a, b) => (a.endedAt ?? 0) - (b.endedAt ?? 0));
  for (const row of ordered) {
    const c = classifyRow(row);
    if (c.kind === 'ignored') {
      skipped[c.why] = (skipped[c.why] ?? 0) + 1;
      continue;
    }
    const workClass = workClassOf(row);
    const jobKey = { template: `${row.repo}:${row.lane}`, class_id: workClass };
    recordObservation(store, jobKey, { refS: c.wallS / hostFactor, censored: c.kind === 'censored' });
    const id = jobKey.template + '\u0000' + workClass;
    if (!groups.has(id)) groups.set(id, { repo: row.repo, lane: row.lane, class: workClass, jobKey, rss: [] });
    if (Number.isFinite(row.observedRssPeakBytes)) groups.get(id).rss.push(row.observedRssPeakBytes);
  }
  const keys = [...groups.values()].map((g) => {
    const est = estimateFor(store, g.jobKey, { workClass: g.class });
    const state = store.get(`0:template=${g.jobKey.template}|class_id=${g.class}|throughAct=|armSetHash=|codeFamily=`) ?? emptyEstimator();
    const rss = g.rss.sort((a, b) => a - b);
    return {
      repo: g.repo,
      lane: g.lane,
      class: g.class,
      n: state.n,
      censored: state.censored,
      p50: Math.round(est.est_p50_s * 10) / 10,
      p90: Math.round(est.est_p90_s * 10) / 10,
      est_source: est.est_source,
      rssP90Bytes: rss.length > 0 ? percentile(rss, 0.9) : null,
    };
  });
  keys.sort((a, b) => (a.repo + a.lane < b.repo + b.lane ? -1 : 1));
  return { keys, skipped };
}

export async function estimatesCommand({ root = stateHome(), json = false } = {}) {
  const hostFactor = 1;
  const report = { root, hostFactor, hostFactorSource: 'none: no calibration, factor 1.0', ...backfillEstimates(readRows(paths(root).history), { hostFactor }) };
  if (json) process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  else {
    process.stdout.write(`host factor ${hostFactor} (${report.hostFactorSource})\n`);
    for (const k of report.keys) process.stdout.write(`  ${k.repo}:${k.lane} [${k.class}]  n=${k.n} censored=${k.censored}  p50/p90=${k.p50}/${k.p90}s  ${k.est_source}  rss p90=${k.rssP90Bytes ?? 'n/a'}\n`);
  }
  return { exitCode: 0 };
}
