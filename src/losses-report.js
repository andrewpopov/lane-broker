import { paths } from './state.js';
import { readRows } from './suggest.js';
import { repoMatches, parseTime } from './cost-report.js';

/**
 * BRAIN-350 `lane report losses`: remote work lost to a runner, per runner. Counts three things the history rows carry:
 * settled-lost rows (`reason` in LOST_REASONS, with the lane-hours spent before settling), re-dispatched rows (once per runner in
 * `redispatchedFrom`, by `redispatchReason`) and local fallbacks after a loss (`localReason: fallback:<reason>`). Rows without
 * those fields are ignored. Window is [since, until) by startedAt; `repos` empty means every repo.
 */

export const LOST_REASONS = ['runner-lost', 'runner-offline', 'runner-shutdown', 'runner-never-started', 'never-dispatched'];
const FALLBACK_REASONS = ['runner-shutdown', 'runner-lost', 'runner-offline'];
const HOUR_MS = 3_600_000;
const bump = (obj, key) => { obj[key] = (obj[key] ?? 0) + 1; };

export function buildLossesReport({ repos = [], rows, since, until }) {
  const sinceMs = Date.parse(since);
  const untilMs = Date.parse(until);
  const runners = {};
  const group = (name) => (runners[name ?? 'unknown'] ??= { lost: {}, laneMs: 0, redispatched: {}, fallbacks: {} });
  for (const row of rows) {
    if (!row || (repos.length > 0 && !repos.some((r) => repoMatches(row.repo, r)))) continue;
    if (!Number.isFinite(row.startedAt) || row.startedAt < sinceMs || row.startedAt >= untilMs) continue;
    if (row.executor === 'remote' && LOST_REASONS.includes(row.reason)) {
      const g = group(row.runner);
      bump(g.lost, row.reason);
      if (Number.isFinite(row.endedAt) && row.endedAt >= row.startedAt) g.laneMs += row.endedAt - row.startedAt;
    }
    if (Array.isArray(row.redispatchedFrom) && row.redispatchReason) {
      for (const name of row.redispatchedFrom) bump(group(name).redispatched, row.redispatchReason);
    }
    if (row.executor === 'local' && FALLBACK_REASONS.some((r) => row.localReason === `fallback:${r}`)) {
      bump(group(row.runner).fallbacks, row.localReason.slice('fallback:'.length));
    }
  }
  const total = { lost: 0, laneHours: 0, redispatched: 0, fallbacks: 0 };
  const sumOf = (o) => Object.values(o).reduce((t, n) => t + n, 0);
  const out = {};
  for (const [name, g] of Object.entries(runners)) {
    out[name] = { lost: g.lost, laneHours: Math.round((g.laneMs / HOUR_MS) * 10) / 10, redispatched: g.redispatched, fallbacks: g.fallbacks };
    total.lost += sumOf(g.lost);
    total.laneHours += g.laneMs / HOUR_MS;
    total.redispatched += sumOf(g.redispatched);
    total.fallbacks += sumOf(g.fallbacks);
  }
  total.laneHours = Math.round(total.laneHours * 10) / 10;
  return { repos, since, until, runners: out, total };
}

export function renderLossesText(report) {
  const list = (o) => Object.entries(o).sort().map(([k, n]) => `${k} ${n}`).join(', ') || 'none';
  const out = [`lane report losses: ${report.repos.join(', ') || 'all repos'}  [${report.since}, ${report.until})`];
  const names = Object.keys(report.runners).sort();
  if (names.length === 0) out.push('  no runner losses in this window');
  for (const name of names) {
    const r = report.runners[name];
    out.push(`  ${name}:`);
    out.push(`    settled lost: ${list(r.lost)} (${r.laneHours.toFixed(1)} lane-hours)`);
    out.push(`    re-dispatched: ${list(r.redispatched)}`);
    out.push(`    local fallbacks: ${list(r.fallbacks)}`);
  }
  const t = report.total;
  out.push(`  total: ${t.lost} lost (${t.laneHours.toFixed(1)} lane-hours), ${t.redispatched} re-dispatched, ${t.fallbacks} local fallbacks`);
  return out.join('\n');
}

/** `lane report losses --since <iso> --until <iso> [--repo a[,b]] [--history f] [--json]` */
export async function lossesReportCommand(args, { stdout = process.stdout, stderr = process.stderr } = {}) {
  const flag = (name) => (args.indexOf(name) === -1 ? undefined : args[args.indexOf(name) + 1]);
  try {
    const since = flag('--since');
    const until = flag('--until');
    if (!since || !until) throw new Error('--since and --until are required');
    parseTime('--since', since);
    parseTime('--until', until);
    if (Date.parse(until) <= Date.parse(since)) throw new Error('--until must be after --since');
    const rows = readRows(flag('--history') || paths().history);
    const report = buildLossesReport({ repos: (flag('--repo') ?? '').split(',').filter(Boolean), rows, since, until });
    stdout.write(args.includes('--json') ? `${JSON.stringify(report, null, 2)}\n` : `${renderLossesText(report)}\n`);
    return { exitCode: 0 };
  } catch (err) {
    stderr.write(`lane report losses: ${err.message}\n`);
    return { exitCode: 2 };
  }
}
