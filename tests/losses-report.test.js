import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { buildLossesReport, renderLossesText, lossesReportCommand } from '../src/losses-report.js';

const REPO = '_Volumes_Lexar_proj_foo_.git';
const T = (iso) => Date.parse(iso);
const H = 3_600_000;
const start = T('2026-10-09T02:00Z');
const base = { repo: REPO, executor: 'remote', startedAt: start };
const win = { since: '2026-10-09T00:00:00Z', until: '2026-10-10T00:00:00Z' };

const rows = [
  { ...base, runner: 'wintop', reason: 'runner-lost', exitCode: 75, endedAt: start + 2 * H, lostDetail: 'x' },
  { ...base, runner: 'wintop', reason: 'runner-lost', exitCode: 75, endedAt: start + 1.5 * H },
  { ...base, runner: 'wintop', reason: 'runner-shutdown', exitCode: 75, endedAt: start + 0.5 * H },
  { ...base, runner: 'skybox', reason: 'runner-offline', exitCode: 75, endedAt: start },
  { ...base, runner: 'skybox', reason: 'never-dispatched', exitCode: 75, endedAt: start },
  { ...base, runner: 'skybox', reason: 'runner-never-started', exitCode: 75, endedAt: start + H },
  { ...base, runner: 'skybox', exitCode: 0, redispatchedFrom: ['wintop', 'skybox'], redispatchReason: 'runner-lost' },
  { ...base, runner: 'skybox', exitCode: 0, redispatchedFrom: ['wintop'], redispatchReason: 'runner-shutdown' },
  { ...base, executor: 'local', runner: 'wintop', exitCode: 0, localReason: 'fallback:runner-shutdown' },
  // ignored: no loss fields, other repo, outside the window, an ordinary local reason
  { ...base, runner: 'wintop', exitCode: 0 },
  { ...base, executor: 'local', exitCode: 0, localReason: 'needs-this-machine' },
  { ...base, executor: 'local', exitCode: 0, localReason: 'fallback:could not persist the dispatch binding' },
  { ...base, repo: '_Volumes_Lexar_proj_bar_.git', runner: 'wintop', reason: 'runner-lost', endedAt: start + H },
  { ...base, startedAt: T('2026-10-08T23:00Z'), runner: 'wintop', reason: 'runner-lost', endedAt: T('2026-10-09T01:00Z') },
  { ...base, startedAt: T('2026-10-10T00:00Z'), runner: 'wintop', reason: 'runner-lost', endedAt: T('2026-10-10T01:00Z') },
];

test('per runner counts by reason, lane-hours, redispatches and fallbacks; window and repo filter', () => {
  const r = buildLossesReport({ ...win, repos: ['foo'], rows });
  assert.deepEqual(Object.keys(r.runners).sort(), ['skybox', 'wintop']);
  assert.deepEqual(r.runners.wintop, {
    lost: { 'runner-lost': 2, 'runner-shutdown': 1 },
    laneHours: 4,
    redispatched: { 'runner-lost': 1, 'runner-shutdown': 1 },
    fallbacks: { 'runner-shutdown': 1 },
  });
  assert.deepEqual(r.runners.skybox, {
    lost: { 'runner-offline': 1, 'never-dispatched': 1, 'runner-never-started': 1 },
    laneHours: 1,
    redispatched: { 'runner-lost': 1 },
    fallbacks: {},
  });
  assert.deepEqual(r.total, { lost: 6, laneHours: 5, redispatched: 3, fallbacks: 1 });
  const text = renderLossesText(r);
  assert.match(text, /wintop:\n {4}settled lost: runner-lost 2, runner-shutdown 1 \(4\.0 lane-hours\)/);
  assert.match(text, /total: 6 lost \(5\.0 lane-hours\), 3 re-dispatched, 1 local fallbacks/);
});

test('command: --json and text from a history file, repo optional', async (testCtx) => {
  const dir = makeTmpDir('losses-', testCtx);
  const file = path.join(dir, 'history.jsonl');
  fs.writeFileSync(file, `${rows.map((r) => JSON.stringify(r)).join('\n')}\n`);
  const run = async (extra) => {
    let out = '';
    const res = await lossesReportCommand(['--since', win.since, '--until', win.until, '--history', file, ...extra], { stdout: { write: (s) => { out += s; } } });
    return { res, out };
  };
  const j = await run(['--repo', 'foo', '--json']);
  assert.equal(j.res.exitCode, 0);
  assert.equal(JSON.parse(j.out).total.lost, 6);
  const all = await run(['--json']);
  assert.equal(JSON.parse(all.out).total.lost, 7); // bar's in-window row counts when no --repo
  const t = await run(['--repo', 'foo']);
  assert.match(t.out, /total: 6 lost/);
  let err = '';
  const bad = await lossesReportCommand(['--since', win.since], { stderr: { write: (s) => { err += s; } } });
  assert.equal(bad.exitCode, 2);
  assert.match(err, /required/);
});
