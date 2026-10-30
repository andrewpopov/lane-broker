import { paths } from './state.js';
import { readHistoryRows } from './suggest.js';
import { repoMatches, stats, parseTime } from './cost-report.js';

/**
 * BRAIN-530 `lane report phases`: where a remote run spends its wall time, per runner. Reads the `remotePhasesMs` each remote
 * history row carries (supervisor.js `relayedPhases`) and reports median and p90 per phase, plus the overhead share
 * (snapshot + deps + setup) / (overhead + command + artifacts). A row from a runner that predates a phase simply lacks its key
 * and is left out of that phase's stats; a row with no `remotePhasesMs` at all is not counted. Window is [since, until) by startedAt.
 */

export const PHASES = ['uploadMs', 'queuedMs', 'snapshotMs', 'depsMs', 'setupMs', 'commandMs', 'artifactsMs', 'artifactReturnMs'];
const OVERHEAD = ['snapshotMs', 'depsMs', 'setupMs'];
const RUN = ['commandMs', 'artifactsMs'];
const sum = (phases, keys) => keys.reduce((t, k) => t + (Number.isFinite(phases[k]) ? phases[k] : 0), 0);

export function buildPhasesReport({ repos, rows, since, until }) {
  const sinceMs = Date.parse(since);
  const untilMs = Date.parse(until);
  const byRunner = {};
  const seen = new Set();
  for (const row of rows) {
    if (!row || row.executor !== 'remote' || !row.remotePhasesMs || !repos.some((r) => repoMatches(row.repo, r))) continue;
    if (!Number.isFinite(row.startedAt) || row.startedAt < sinceMs || row.startedAt >= untilMs) continue;
    if (row.id) {
      if (seen.has(row.id)) continue;
      seen.add(row.id);
    }
    const g = (byRunner[row.runner ?? 'unknown'] ??= { runs: 0, values: Object.fromEntries(PHASES.map((p) => [p, []])), overhead: 0, total: 0, deps: {} });
    g.runs += 1;
    for (const p of PHASES) if (Number.isFinite(row.remotePhasesMs[p])) g.values[p].push(row.remotePhasesMs[p]);
    if (Number.isFinite(row.remotePhasesMs.commandMs)) {
      g.overhead += sum(row.remotePhasesMs, OVERHEAD);
      g.total += sum(row.remotePhasesMs, OVERHEAD) + sum(row.remotePhasesMs, RUN);
    }
    if (row.depsCache) g.deps[row.depsCache] = (g.deps[row.depsCache] ?? 0) + 1;
  }
  const runners = {};
  for (const [name, g] of Object.entries(byRunner)) {
    runners[name] = {
      runs: g.runs,
      phases: Object.fromEntries(PHASES.map((p) => [p, stats(g.values[p])])),
      overheadShare: g.total > 0 ? g.overhead / g.total : null,
      depsCache: g.deps,
    };
  }
  return { repos, since, until, runners };
}

export function renderPhasesText(report) {
  const f = (v) => (v === null ? '-' : String(Math.round(v)));
  const out = [`lane report phases: ${report.repos.join(', ')}  [${report.since}, ${report.until})`];
  const names = Object.keys(report.runners).sort();
  if (names.length === 0) out.push('  no remote runs with phase timings in this window');
  for (const name of names) {
    const r = report.runners[name];
    const share = r.overheadShare === null ? '-' : `${(r.overheadShare * 100).toFixed(1)}%`;
    const deps = Object.entries(r.depsCache).map(([k, n]) => `${k} ${n}`).join(', ') || '-';
    out.push(`  ${name}: ${r.runs} runs, overhead share ${share} (snapshot+deps+setup of total), deps-cache: ${deps}`);
    out.push(`    ${'phase'.padEnd(18)}${'n'.padStart(5)}${'p50-ms'.padStart(10)}${'p90-ms'.padStart(10)}`);
    for (const p of PHASES) out.push(`    ${p.padEnd(18)}${String(r.phases[p].n).padStart(5)}${f(r.phases[p].p50).padStart(10)}${f(r.phases[p].p90).padStart(10)}`);
  }
  return out.join('\n');
}

/** `lane report phases --repo a[,b] --since <iso> --until <iso> [--history f] [--json]` */
export async function phasesReportCommand(args, { stdout = process.stdout, stderr = process.stderr } = {}) {
  const flag = (name) => (args.indexOf(name) === -1 ? undefined : args[args.indexOf(name) + 1]);
  try {
    const repoArg = flag('--repo');
    const since = flag('--since');
    const until = flag('--until');
    if (!repoArg || !since || !until) throw new Error('--repo, --since and --until are required');
    parseTime('--since', since);
    parseTime('--until', until);
    if (Date.parse(until) <= Date.parse(since)) throw new Error('--until must be after --since');
    const rows = readHistoryRows(flag('--history') || paths().history);
    const report = buildPhasesReport({ repos: repoArg.split(',').filter(Boolean), rows, since, until });
    stdout.write(args.includes('--json') ? `${JSON.stringify(report, null, 2)}\n` : `${renderPhasesText(report)}\n`);
    return { exitCode: 0 };
  } catch (err) {
    stderr.write(`lane report phases: ${err.message}\n`);
    return { exitCode: 2 };
  }
}
