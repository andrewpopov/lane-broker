import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { buildRepoReport, buildCostReport, costReportCommand, renderCostText } from '../src/cost-report.js';

const SINCE = Date.parse('2026-10-09T00:00:00Z'); // Friday
const UNTIL = Date.parse('2026-10-12T00:00:00Z'); // 3 days: Fri, Sat, Sun
const T = (iso) => Date.parse(iso);
const REPO = '_Volumes_Lexar_proj_foo_.git';

// History, journal and GitHub deliberately disagree.
const rows = [
  { id: 'g1', repo: REPO, lane: 'prepush', startedAt: T('2026-10-09T01:00Z'), endedAt: T('2026-10-09T01:10Z'), waitedMs: 120000, grantedCpuCores: 2, exit: 0, executor: 'local', cpuSeconds: 100 },
  // remote: endedAt-startedAt is 15 min (delivery delay) but runMs is 10 min
  { id: 'b1', repo: REPO, lane: 'train', startedAt: T('2026-10-09T02:00Z'), endedAt: T('2026-10-09T02:15Z'), runMs: 600000, waitedMs: 60000, grantedCpuCores: 4, resources: { cpuCores: 9 }, exit: 1, executor: 'remote', runner: 'wintop' },
  // master-verify lane is named `train` too; only the journal tells it apart from a batch lane
  { id: 'm1', repo: REPO, lane: 'train', startedAt: T('2026-10-09T03:00Z'), endedAt: T('2026-10-09T03:20Z'), resources: { cpuCores: 3 }, exit: 0, executor: 'remote', runner: 'skybox' },
  { id: 'a1', repo: REPO, lane: 'foo-adhoc', startedAt: T('2026-10-10T03:00Z'), endedAt: T('2026-10-10T03:05Z'), weight: 2, exit: 0 },
  { id: 'old', repo: REPO, lane: 'prepush', startedAt: T('2026-10-08T23:00Z'), endedAt: T('2026-10-08T23:10Z'), grantedCpuCores: 2, exit: 0 },
  { id: 'other', repo: '_Volumes_Lexar_proj_bar_.git', lane: 'prepush', startedAt: T('2026-10-09T01:00Z'), endedAt: T('2026-10-09T01:10Z'), grantedCpuCores: 2, exit: 0 },
];
const ev = (at, event, rest) => ({ at, event, ...rest });
const events = [
  ev('2026-10-09T01:00:00Z', 'enqueue', { pr: 3 }),
  ev('2026-10-09T01:05:00Z', 'enqueue', { pr: 5 }),
  ev('2026-10-09T01:30:00Z', 'eject', { pr: 5, runId: 'r0' }),
  ev('2026-10-09T05:00:00Z', 'enqueue', { pr: 5 }), // re-enqueue after the eject
  ev('2026-10-09T05:00:00Z', 'enqueue', { pr: 4 }),
  ev('2026-10-09T05:30:00Z', 'enqueue', { pr: 6 }), // never lands
  ev('2026-10-09T02:00:01Z', 'lane-start', { runId: 'r1', laneId: 'b1' }),
  ev('2026-10-09T03:00:01Z', 'lane-start', { runId: 'master-abcd1234', laneId: 'm1' }),
  ev('2026-10-09T02:30:00Z', 'batch-built', { runId: 'r1', prs: [3] }),
  ev('2026-10-09T02:40:00Z', 'landed', { runId: 'r1' }),
  ev('2026-10-09T06:00:00Z', 'batch-built', { runId: 'r2', prs: [4, 5] }),
  ev('2026-10-09T07:00:00Z', 'landed', { runId: 'r2' }),
  ev('2026-10-12T01:00:00Z', 'landed', { runId: 'r2' }), // after the cutoff: ignored
];
const prs = [
  { number: 1, mergedAt: '2026-10-09T09:00:00Z', closedAt: '2026-10-09T09:00:00Z', labels: [] },
  { number: 2, mergedAt: '2026-10-10T09:00:00Z', closedAt: '2026-10-10T09:00:00Z', labels: [] },
  { number: 3, mergedAt: null, closedAt: '2026-10-09T02:41:00Z', labels: [{ name: 'train-landed' }] },
  { number: 7, mergedAt: null, closedAt: '2026-10-09T09:00:00Z', labels: [{ name: 'wontfix' }] }, // closed, not landed
  { number: 8, mergedAt: '2026-10-08T09:00:00Z', closedAt: '2026-10-08T09:00:00Z', labels: [] }, // before window
];

const report = buildRepoReport({ repo: 'foo', rows, events, prs, sinceMs: SINCE, untilMs: UNTIL });

test('PR denominator is the reconciled union, including a PR present only in the journal', () => {
  assert.deepEqual(report.prs, { githubMerged: 2, githubTrainLanded: 1, journalLanded: 3, union: 5, onlyJournal: 2, onlyGithub: 2 });
});

test('a remote row uses runMs, not endedAt - startedAt', () => {
  assert.equal(report.spend['train-batch'].wallMin, 10);
  assert.equal(report.spend['train-batch'].reservedCoreMin, 40); // 10 min x granted 4 (not declared 9)
});

test('reserved falls back to declared cores, then weight; CPU seconds are reported separately', () => {
  assert.equal(report.spend['master-verify'].reservedCoreMin, 60); // 20 min x declared 3
  assert.equal(report.spend['ad-hoc'].reservedCoreMin, 10); // 5 min x weight 2
  assert.equal(report.spend.gate.cpuSeconds, 100);
  assert.equal(report.spend['train-batch'].cpuRuns, 0);
});

test('spend classes: master lane ids come from the journal; other repos and out-of-window rows are excluded', () => {
  assert.equal(report.spend.gate.runs, 1);
  assert.equal(report.spend['master-verify'].runs, 1);
  assert.equal(report.total.runs, 4);
  assert.equal(report.total.waitMin, 3);
  assert.equal(report.perPr.total.reservedCoreMin, (20 + 40 + 60 + 10) / 5);
});

test('latency keeps the first enqueue across an eject and lists the unfinished PR', () => {
  // #5: first enqueue 01:05, landed 07:00 => 355 min (not 60 from the re-enqueue); #3: 100; #4: 120
  assert.deepEqual(report.latency.enqueueToLandedMin, { n: 3, p50: 120, p90: 355, max: 355 });
  assert.equal(report.latency.prsWithoutEnqueue, 2);
  assert.deepEqual(report.unfinished.inWindow.map((u) => u.pr), [6]);
});

test('executor mix and weekday split', () => {
  assert.deepEqual(Object.keys(report.executors).sort(), ['local', 'remote:skybox', 'remote:wintop']);
  assert.equal(report.weekday.weekdayDays, 1);
  assert.equal(report.weekday.weekendDays, 2);
  assert.equal(report.weekday.landedWeekend, 1); // PR 2, Saturday
});

test('CLI reads files, honours --json, and rejects a missing cutoff', async (t) => {
  const dir = makeTmpDir('cost-report-', t);
  fs.mkdirSync(path.join(dir, 'j', 'foo'), { recursive: true });
  fs.writeFileSync(path.join(dir, 'h.jsonl'), `${rows.map((r) => JSON.stringify(r)).join('\n')}\n{torn`);
  fs.writeFileSync(path.join(dir, 'j', 'foo', 'journal.jsonl'), `${events.map((e) => JSON.stringify(e)).join('\n')}\n`);
  fs.writeFileSync(path.join(dir, 'prs.json'), JSON.stringify(prs));
  const out = [];
  const err = [];
  const sink = (a) => ({ write: (s) => a.push(s) });
  const base = ['--repo', 'foo', '--since', '2026-10-09', '--until', '2026-10-12', '--prs', path.join(dir, 'prs.json'), '--journal-root', path.join(dir, 'j'), '--history', path.join(dir, 'h.jsonl')];
  assert.equal((await costReportCommand([...base, '--json'], { stdout: sink(out), stderr: sink(err) })).exitCode, 0);
  assert.equal(JSON.parse(out.join('')).repos[0].prs.union, 5);
  assert.equal((await costReportCommand(['--repo', 'foo', '--since', '2026-10-09'], { stdout: sink(out), stderr: sink(err) })).exitCode, 2);
  assert.match(renderCostText(buildCostReport({ repos: ['foo'], rows, journals: { foo: events }, prsByRepo: { foo: prs }, since: '2026-10-09', until: '2026-10-12' })), /PRs landed 5/);
  fs.rmSync(dir, { recursive: true });
});
