import { paths } from './state.js';
import { readHistoryRows } from './suggest.js';
import { stats, parseTime } from './cost-report.js';

/**
 * BRAIN-899 A4 `lane report placement`: how accurate the finish model's own predictions were, per runner. Each remote history row
 * carries the `placement` its FINAL dispatch recorded (supervisor.js `placementOf`); this joins it to what that same dispatch
 * actually saw -- `remotePhasesMs.queuedMs` for the wait, `runMs` for the run -- so a prediction is only ever scored against its
 * own outcome, never against the cumulative wait of the moves before it. A row whose `placement.runner` is not the runner that
 * ran it is a prediction about a dispatch that never happened here, and is not counted. The ratio is predicted/actual, so above 1
 * is an over-prediction (the model was too pessimistic) and below 1 an under-prediction. Window is [since, until) by startedAt.
 */

/** Both sides of a ratio are padded by this, so a runner that admitted a ticket instantly still has a usable denominator. */
export const RATIO_PAD_MS = 30_000;
/** A prediction this many times its actual counts as over 2x; its reciprocal as under 2x. */
export const RATIO_FACTOR = 2;

export const ratioOf = (predictedMs, actualMs) => (predictedMs + RATIO_PAD_MS) / (actualMs + RATIO_PAD_MS);

const summarise = (pairs) => {
  const ratios = pairs.map((p) => ratioOf(p.predictedMs, p.actualMs));
  const p50 = (xs) => stats(xs).p50;
  return {
    n: pairs.length,
    predictedMs: p50(pairs.map((p) => p.predictedMs)),
    actualMs: p50(pairs.map((p) => p.actualMs)),
    ratio: p50(ratios),
    over2x: ratios.filter((r) => r > RATIO_FACTOR).length,
    under2x: ratios.filter((r) => r < 1 / RATIO_FACTOR).length,
  };
};

const emptyGroup = () => ({ placements: 0, nonGreen: 0, wait: [], run: [] });

export function buildPlacementReport({ rows, since, until }) {
  const sinceMs = Date.parse(since);
  const untilMs = Date.parse(until);
  const byRunner = new Map();
  const all = emptyGroup();
  const seen = new Set();
  for (const row of Array.isArray(rows) ? rows : []) {
    const placement = row?.placement;
    if (!row || row.executor !== 'remote' || !placement || placement.runner !== row.runner) continue;
    if (!Number.isFinite(row.startedAt) || row.startedAt < sinceMs || row.startedAt >= untilMs) continue;
    if (row.id) {
      if (seen.has(row.id)) continue;
      seen.add(row.id);
    }
    const group = byRunner.get(row.runner) ?? emptyGroup();
    byRunner.set(row.runner, group);
    for (const g of [group, all]) {
      g.placements += 1;
      const queuedMs = row.remotePhasesMs?.queuedMs;
      if (Number.isFinite(placement.predictedWaitMs) && placement.predictedWaitMs >= 0 && Number.isFinite(queuedMs) && queuedMs >= 0) g.wait.push({ predictedMs: placement.predictedWaitMs, actualMs: queuedMs });
      // a run that did not exit 0 says nothing about how long the command takes, but it did wait exactly as long
      if (row.exit !== 0) g.nonGreen += 1;
      else if (Number.isFinite(placement.predictedRunMs) && placement.predictedRunMs >= 0 && Number.isFinite(row.runMs) && row.runMs >= 0) g.run.push({ predictedMs: placement.predictedRunMs, actualMs: row.runMs });
    }
  }
  const finish = (g) => ({ placements: g.placements, nonGreen: g.nonGreen, wait: summarise(g.wait), run: summarise(g.run) });
  return { since: new Date(sinceMs).toISOString(), until: new Date(untilMs).toISOString(), runners: Object.fromEntries([...byRunner].map(([name, g]) => [name, finish(g)])), total: finish(all) };
}

export function renderPlacementText(report) {
  const ms = (v) => (v === null ? '-' : String(Math.round(v)));
  const ratio = (v) => (v === null ? '-' : v.toFixed(2));
  const head = `    ${''.padEnd(6)}${'n'.padStart(5)}${'predicted-ms'.padStart(13)}${'actual-ms'.padStart(12)}${'ratio'.padStart(9)}${'over-2x'.padStart(9)}${'under-2x'.padStart(10)}`;
  const line = (label, s) => `    ${label.padEnd(6)}${String(s.n).padStart(5)}${ms(s.predictedMs).padStart(13)}${ms(s.actualMs).padStart(12)}${ratio(s.ratio).padStart(9)}${String(s.over2x).padStart(9)}${String(s.under2x).padStart(10)}`;
  const block = (g) => [head, line('wait', g.wait), line('run', g.run)];
  const out = [`lane report placement: [${report.since}, ${report.until})`];
  const names = Object.keys(report.runners).sort();
  if (names.length === 0) return [...out, '  no scored remote placements in this window'].join('\n');
  for (const name of names) {
    const g = report.runners[name];
    out.push(`  ${name}: ${g.placements} placement(s), ${g.nonGreen} non-green (out of the run stats)`, ...block(g));
  }
  out.push('  total:', ...block(report.total));
  return out.join('\n');
}

/** `lane report placement --since <iso> [--until <iso>] [--history <file>] [--json]` */
export async function placementReportCommand(args, { stdout = process.stdout, stderr = process.stderr, now = Date.now() } = {}) {
  const flag = (name) => (args.indexOf(name) === -1 ? undefined : args[args.indexOf(name) + 1]);
  try {
    const since = flag('--since');
    if (!since) throw new Error('--since is required');
    parseTime('--since', since);
    const untilArg = flag('--until');
    const until = untilArg === undefined ? new Date(now).toISOString() : untilArg;
    parseTime('--until', until);
    if (Date.parse(until) <= Date.parse(since)) throw new Error('--until must be after --since');
    const rows = readHistoryRows(flag('--history') || paths().history);
    const report = buildPlacementReport({ rows, since, until });
    stdout.write(args.includes('--json') ? `${JSON.stringify(report, null, 2)}\n` : `${renderPlacementText(report)}\n`);
    return { exitCode: 0 };
  } catch (err) {
    stderr.write(`lane report placement: ${err.message}\n`);
    return { exitCode: 2 };
  }
}
