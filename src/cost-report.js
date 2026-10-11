import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { paths } from './state.js';
import { readRows } from './suggest.js';

/**
 * BRAIN-529 `lane report cost`: the gate-cost scorecard for the BRAIN-528 fleet-throughput program.
 * Per repo and window: what testing costs per landed PR, split by spend class, and how long a PR takes
 * from its first merge-train enqueue to landed. Read-only; every input is a file, none is fetched.
 *
 * Window is [since, until) on one frozen cutoff for history rows (by startedAt), journal events and PRs.
 */

const MIN = 60_000;
const DAY = 24 * 3600 * 1000;
export const CLASSES = ['gate', 'train-batch', 'master-verify', 'ad-hoc'];

/** A history row belongs to <repo> when its key is the repo's checkout (`..._proj_<repo>_.git`) or the train's clone of it. */
export function repoMatches(historyRepo, repo) {
  if (typeof historyRepo !== 'string') return false;
  return historyRepo.endsWith(`_proj_${repo}_.git`) || historyRepo.endsWith(`_merge-train_${repo}_clone_.git`);
}

export function readJournal(file) {
  return readRows(file).filter((e) => e && typeof e === 'object' && typeof e.event === 'string' && Date.parse(e.at) >= 0);
}

/** Nearest-rank percentile of an ascending-sorted array. */
function percentile(sorted, p) {
  if (!sorted.length) return null;
  return sorted[Math.max(0, Math.ceil(p * sorted.length) - 1)];
}

const stats = (xs) => {
  const s = [...xs].sort((a, b) => a - b);
  return { n: s.length, p50: percentile(s, 0.5), p90: percentile(s, 0.9), max: s.length ? s[s.length - 1] : null };
};

/** Wall run time of a row in ms. A remote row's `endedAt - startedAt` includes result-delivery delay, so `runMs` wins. */
export function runMsOf(row) {
  if (Number.isFinite(row.runMs)) return row.runMs;
  return row.endedAt - row.startedAt;
}

/** Cores a row reserved: what the scheduler granted, else what the lane declared, else its weight, else 1. */
export function reservedCoresOf(row) {
  return row.grantedCpuCores || row.resources?.cpuCores || row.weight || 1;
}

function journalFacts(events, untilMs) {
  const masterLanes = new Set();
  const trainLanes = new Set();
  const runPrs = new Map();
  const firstEnqueue = new Map();
  const landedAt = new Map();
  for (const e of events) {
    const at = Date.parse(e.at);
    if (at >= untilMs) continue;
    switch (e.event) {
      case 'lane-start':
        (String(e.runId ?? '').startsWith('master-') ? masterLanes : trainLanes).add(e.laneId);
        break;
      case 'enqueue':
        if (!firstEnqueue.has(e.pr)) firstEnqueue.set(e.pr, at); // an eject keeps this
        break;
      case 'batch-built':
        runPrs.set(e.runId, e.prs ?? []); // a rebuild replaces the batch
        break;
      case 'landed':
        for (const pr of runPrs.get(e.runId) ?? []) if (!landedAt.has(pr)) landedAt.set(pr, at);
        break;
      case 'remove':
      case 'drop':
        // the operator pulled it out: a later re-add starts a new wait
        if (!landedAt.has(e.pr)) firstEnqueue.delete(e.pr);
        break;
      default:
    }
  }
  return { masterLanes, trainLanes, firstEnqueue, landedAt };
}

const labelNames = (labels) => (labels ?? []).map((l) => (typeof l === 'string' ? l : l?.name));

/** Union of GitHub merged, GitHub closed+train-landed, and journal landed. One timestamp per PR (journal first). */
export function reconcilePrs(ghPrs, landedAt, sinceMs, untilMs) {
  const inWindow = (ms) => Number.isFinite(ms) && ms >= sinceMs && ms < untilMs;
  const merged = new Map();
  const trainLanded = new Map();
  for (const p of ghPrs) {
    const mergedMs = Date.parse(p.mergedAt);
    const closedMs = Date.parse(p.closedAt);
    if (inWindow(mergedMs)) merged.set(p.number, mergedMs);
    else if (!p.mergedAt && labelNames(p.labels).includes('train-landed') && inWindow(closedMs)) trainLanded.set(p.number, closedMs);
  }
  const journal = new Map([...landedAt].filter(([, ms]) => inWindow(ms)));
  const union = new Map();
  for (const m of [journal, merged, trainLanded]) for (const [pr, ms] of m) if (!union.has(pr)) union.set(pr, ms);
  const gh = new Set([...merged.keys(), ...trainLanded.keys()]);
  return {
    union,
    counts: {
      githubMerged: merged.size,
      githubTrainLanded: trainLanded.size,
      journalLanded: journal.size,
      union: union.size,
      onlyJournal: [...journal.keys()].filter((pr) => !gh.has(pr)).length,
      onlyGithub: [...gh].filter((pr) => !journal.has(pr)).length,
    },
  };
}

const emptySpend = () => ({ runs: 0, failed: 0, wallMin: 0, waitMin: 0, reservedCoreMin: 0, cpuSeconds: 0, cpuRuns: 0 });

/** Pure: history rows + journal events + PR list -> one repo's report for one window. */
export function buildRepoReport({ repo, rows, events, prs, sinceMs, untilMs }) {
  const { masterLanes, trainLanes, firstEnqueue, landedAt } = journalFacts(events, untilMs);
  const { union, counts } = reconcilePrs(prs, landedAt, sinceMs, untilMs);

  const spend = Object.fromEntries(CLASSES.map((c) => [c, emptySpend()]));
  const executors = {};
  const seen = new Set();
  for (const row of rows) {
    if (!row || !repoMatches(row.repo, repo) || !Number.isFinite(row.startedAt) || !Number.isFinite(row.endedAt)) continue;
    if (row.startedAt < sinceMs || row.startedAt >= untilMs) continue;
    if (row.id) {
      if (seen.has(row.id)) continue;
      seen.add(row.id);
    }
    let cls = 'ad-hoc';
    if (masterLanes.has(row.id)) cls = 'master-verify';
    else if (trainLanes.has(row.id) || row.lane === 'train') cls = 'train-batch';
    else if (row.lane === 'prepush') cls = 'gate';
    const runMin = runMsOf(row) / MIN;
    const s = spend[cls];
    s.runs += 1;
    s.failed += row.exit !== 0 && row.exit != null ? 1 : 0;
    s.wallMin += runMin;
    s.waitMin += (row.waitedMs || 0) / MIN;
    s.reservedCoreMin += runMin * reservedCoresOf(row);
    if (Number.isFinite(row.cpuSeconds)) {
      s.cpuSeconds += row.cpuSeconds;
      s.cpuRuns += 1;
    }
    const exKey = row.executor === 'remote' ? `remote:${row.runner ?? '?'}` : 'local';
    const ex = (executors[exKey] ??= { runs: 0, reservedCoreMin: 0 });
    ex.runs += 1;
    ex.reservedCoreMin += runMin * reservedCoresOf(row);
  }

  const total = emptySpend();
  for (const s of Object.values(spend)) for (const k of Object.keys(total)) total[k] += s[k];
  const n = union.size;
  const per = (s) => (n ? { runs: s.runs / n, wallMin: s.wallMin / n, waitMin: s.waitMin / n, reservedCoreMin: s.reservedCoreMin / n, cpuSeconds: s.cpuSeconds / n } : null);

  const latencyMin = [];
  for (const [pr, at] of landedAt) {
    if (!union.has(pr) || !firstEnqueue.has(pr)) continue;
    latencyMin.push((at - firstEnqueue.get(pr)) / MIN);
  }
  const unfinished = [];
  let unfinishedBeforeWindow = 0;
  for (const [pr, at] of firstEnqueue) {
    if (landedAt.has(pr)) continue;
    if (at < sinceMs) unfinishedBeforeWindow += 1;
    else unfinished.push({ pr, enqueuedAt: new Date(at).toISOString(), ageMin: (untilMs - at) / MIN });
  }

  const landedByDay = { weekday: 0, weekend: 0 };
  for (const ms of union.values()) landedByDay[isWeekend(ms) ? 'weekend' : 'weekday'] += 1;

  return {
    repo,
    prs: counts,
    spend,
    total,
    perPr: { total: per(total), ...Object.fromEntries(CLASSES.map((c) => [c, per(spend[c])])) },
    latency: { enqueueToLandedMin: stats(latencyMin), prsWithoutEnqueue: n - latencyMin.length },
    unfinished: { inWindow: unfinished.sort((a, b) => a.pr - b.pr), enqueuedBeforeWindow: unfinishedBeforeWindow },
    executors,
    weekday: { ...weekdayDays(sinceMs, untilMs), landedWeekday: landedByDay.weekday, landedWeekend: landedByDay.weekend },
  };
}

const isWeekend = (ms) => [0, 6].includes(new Date(ms).getUTCDay());

/** Fractional days in [since, until) that fall on a UTC weekday vs weekend. */
function weekdayDays(sinceMs, untilMs) {
  let weekday = 0;
  let weekend = 0;
  for (let t = sinceMs; t < untilMs; ) {
    const next = Math.min(untilMs, (Math.floor(t / DAY) + 1) * DAY);
    if (isWeekend(t)) weekend += (next - t) / DAY;
    else weekday += (next - t) / DAY;
    t = next;
  }
  return { weekdayDays: weekday, weekendDays: weekend };
}

export function buildCostReport({ repos, rows, journals, prsByRepo, since, until }) {
  const sinceMs = Date.parse(since);
  const untilMs = Date.parse(until);
  return {
    window: { since: new Date(sinceMs).toISOString(), until: new Date(untilMs).toISOString(), days: (untilMs - sinceMs) / DAY },
    repos: repos.map((repo) =>
      buildRepoReport({ repo, rows, events: journals[repo] ?? [], prs: prsByRepo[repo] ?? [], sinceMs, untilMs })),
  };
}

const f = (x, d = 1) => (x == null ? '-' : Number(x).toFixed(d));

export function renderCostText(report) {
  const w = report.window;
  const out = [`Gate-cost scorecard  ${w.since} .. ${w.until} (${f(w.days)} days, [since, until) UTC)`];
  for (const r of report.repos) {
    const c = r.prs;
    out.push('', `== ${r.repo}`);
    out.push(`  PRs landed ${c.union} = union of github-merged ${c.githubMerged}, github train-landed ${c.githubTrainLanded}, journal-landed ${c.journalLanded}`
      + ` (only in journal ${c.onlyJournal}, only in github ${c.onlyGithub})`);
    out.push(`  weekday note: ${f(r.weekday.weekdayDays)} weekday days / ${f(r.weekday.weekendDays)} weekend days; PRs landed ${r.weekday.landedWeekday} weekday / ${r.weekday.landedWeekend} weekend`);
    out.push('  spend by class (window totals)');
    out.push(`    ${'class'.padEnd(14)}${'runs'.padStart(6)}${'fail'.padStart(6)}${'wall-min'.padStart(10)}${'wait-min'.padStart(10)}${'resv-core-min'.padStart(15)}${'cpu-s'.padStart(10)}${'cpu-s runs'.padStart(11)}`);
    for (const [name, s] of [...CLASSES.map((k) => [k, r.spend[k]]), ['TOTAL', r.total]]) {
      out.push(`    ${name.padEnd(14)}${String(s.runs).padStart(6)}${String(s.failed).padStart(6)}${f(s.wallMin, 0).padStart(10)}${f(s.waitMin, 0).padStart(10)}${f(s.reservedCoreMin, 0).padStart(15)}${f(s.cpuSeconds, 0).padStart(10)}${String(s.cpuRuns).padStart(11)}`);
    }
    out.push('  per landed PR');
    out.push(`    ${'class'.padEnd(14)}${'runs'.padStart(7)}${'wall-min'.padStart(10)}${'wait-min'.padStart(10)}${'resv-core-min'.padStart(15)}${'cpu-s'.padStart(10)}`);
    for (const name of ['total', ...CLASSES]) {
      const p = r.perPr[name];
      if (!p) { out.push(`    ${name.padEnd(14)}  (no PRs)`); continue; }
      out.push(`    ${name.padEnd(14)}${f(p.runs, 2).padStart(7)}${f(p.wallMin).padStart(10)}${f(p.waitMin).padStart(10)}${f(p.reservedCoreMin).padStart(15)}${f(p.cpuSeconds, 0).padStart(10)}`);
    }
    const l = r.latency.enqueueToLandedMin;
    out.push(l.n
      ? `  latency first-enqueue -> landed (min): n ${l.n} p50 ${f(l.p50)} p90 ${f(l.p90)} max ${f(l.max)}; ${r.latency.prsWithoutEnqueue} landed PRs have no journal enqueue`
      : `  latency first-enqueue -> landed: n/a (no journal enqueue for any landed PR in window)`);
    const u = r.unfinished;
    out.push(`  unfinished (enqueued in window, not landed by cutoff): ${u.inWindow.length}${u.inWindow.length ? ' -> ' + u.inWindow.map((x) => `#${x.pr} (${f(x.ageMin / 60)}h)`).join(', ') : ''}; enqueued before window: ${u.enqueuedBeforeWindow}`);
    out.push('  executor mix: ' + (Object.entries(r.executors).sort().map(([k, e]) => `${k} ${e.runs} runs / ${f(e.reservedCoreMin, 0)} core-min`).join('; ') || '-'));
  }
  return out.join('\n');
}

const parseTime = (flag, v) => {
  const ms = Date.parse(v);
  if (!Number.isFinite(ms)) throw new Error(`${flag} must be an ISO date/time (got "${v}")`);
  return ms;
};

/** `lane report cost --repo a[,b] --since <iso> --until <iso> --prs <file|repo=file,...> [--journal-root d] [--history f] [--json]` */
export async function costReportCommand(args, { stdout = process.stdout, stderr = process.stderr } = {}) {
  const flag = (name) => (args.indexOf(name) === -1 ? undefined : args[args.indexOf(name) + 1]);
  try {
    const repoArg = flag('--repo');
    const since = flag('--since');
    const until = flag('--until');
    if (!repoArg || !since || !until) throw new Error('--repo, --since and --until are required');
    parseTime('--since', since);
    parseTime('--until', until);
    if (Date.parse(until) <= Date.parse(since)) throw new Error('--until must be after --since');
    const repos = repoArg.split(',').filter(Boolean);
    const prsArg = flag('--prs');
    const prsByRepo = {};
    if (prsArg) {
      for (const part of prsArg.split(',')) {
        const [name, file] = part.includes('=') ? part.split('=') : [repos[0], part];
        if (!part.includes('=') && repos.length > 1) throw new Error('--prs with several repos needs repo=file pairs');
        const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
        prsByRepo[name] = [...(prsByRepo[name] ?? []), ...parsed];
      }
    }
    const journalRoot = flag('--journal-root') || path.join(os.homedir(), '.cache', 'merge-train');
    const journals = Object.fromEntries(repos.map((r) => [r, readJournal(path.join(journalRoot, r, 'journal.jsonl'))]));
    const rows = readRows(flag('--history') || paths().history);
    const report = buildCostReport({ repos, rows, journals, prsByRepo, since, until });
    stdout.write(args.includes('--json') ? `${JSON.stringify(report, null, 2)}\n` : `${renderCostText(report)}\n`);
    return { exitCode: 0 };
  } catch (err) {
    stderr.write(`lane report cost: ${err.message}\n`);
    return { exitCode: 2 };
  }
}
