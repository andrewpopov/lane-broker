import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { buildPlacementReport, renderPlacementText, placementReportCommand, ratioOf, RATIO_PAD_MS } from '../src/placement-report.js';

// BRAIN-899 A4: the accuracy report over the `placement` each remote history row carries.

const T = (iso) => Date.parse(iso);
const win = { since: '2026-11-01T00:00:00Z', until: '2026-11-08T00:00:00Z' };
const placement = (over = {}) => ({ runner: 'skybox', rawWaitMs: 300_000, factor: 1, predictedWaitMs: 300_000, predictedRunMs: 600_000, tier: 'repo-lane', n: 3, slots: 2, queued: 1, ...over });
const row = (id, over = {}) => ({
  id, repo: '_Volumes_Lexar_proj_foo_.git', lane: 'pr', executor: 'remote', runner: 'skybox', exit: 0,
  startedAt: T('2026-11-02T00:00:00Z'), endedAt: T('2026-11-02T00:20:00Z'), runMs: 600_000,
  remotePhasesMs: { queuedMs: 300_000 }, placement: placement(), ...over,
});

const rows = [
  row('a'),
  row('b', { remotePhasesMs: { queuedMs: 60_000 }, runMs: 1_500_000 }),
  row('c', { remotePhasesMs: { queuedMs: 900_000 }, runMs: 300_000 }),
  row('d', { exit: 1, runMs: 999_000 }),
  row('e', { runner: 'wintop', placement: placement({ runner: 'wintop', predictedWaitMs: 0 }), remotePhasesMs: { queuedMs: 0 } }),
];

test('per runner: median predicted and actual wait and run, the median predicted/actual ratio, and the 2x tails', () => {
  const r = buildPlacementReport({ ...win, rows });
  assert.deepEqual(Object.keys(r.runners).sort(), ['skybox', 'wintop']);
  const s = r.runners.skybox;
  assert.equal(s.placements, 4);
  assert.equal(s.nonGreen, 1, 'the exit-1 row is counted apart');
  assert.deepEqual(s.wait, { n: 4, predictedMs: 300_000, actualMs: 300_000, ratio: 1, over2x: 1, under2x: 1 }, 'a non-green row still waited');
  assert.deepEqual(s.run, { n: 3, predictedMs: 600_000, actualMs: 600_000, ratio: 1, over2x: 0, under2x: 1 }, 'but says nothing about how long the command takes');
  assert.deepEqual(r.runners.wintop.placements, 1);
  assert.deepEqual(r.total, { placements: 5, nonGreen: 1, wait: { n: 5, predictedMs: 300_000, actualMs: 300_000, ratio: 1, over2x: 1, under2x: 1 }, run: { n: 4, predictedMs: 600_000, actualMs: 600_000, ratio: 1, over2x: 0, under2x: 1 } });
});

test('the ratio is predicted/actual and both sides are padded, so a runner that admitted a ticket instantly is still measurable', () => {
  assert.equal(ratioOf(300_000, 300_000), 1);
  assert.equal(ratioOf(0, 0), 1, 'no zero denominator');
  assert.equal(ratioOf(0, 0), (0 + RATIO_PAD_MS) / (0 + RATIO_PAD_MS));
  const r = buildPlacementReport({ ...win, rows: [row('e', { runner: 'wintop', placement: placement({ runner: 'wintop', predictedWaitMs: 0 }), remotePhasesMs: { queuedMs: 0 } })] });
  assert.deepEqual(r.runners.wintop.wait, { n: 1, predictedMs: 0, actualMs: 0, ratio: 1, over2x: 0, under2x: 0 }, 'an instant admission is neither tail');
  const over = buildPlacementReport({ ...win, rows: [row('f', { placement: placement({ predictedWaitMs: 600_000 }), remotePhasesMs: { queuedMs: 0 } })] });
  assert.deepEqual([over.total.wait.ratio, over.total.wait.over2x], [(600_000 + RATIO_PAD_MS) / RATIO_PAD_MS, 1], 'predicting 600s for a wait of 0 is over 2x');
});

test('a row counts only when it is remote, in the window, carries a placement for the runner that ran it, and is not a repeat id', () => {
  const ignored = [
    row('a'),
    row('x1', { executor: 'local' }),
    row('x2', { placement: undefined }),
    row('x3', { placement: placement({ runner: 'wintop' }) }),
    row('x4', { startedAt: T('2026-10-31T23:59:59Z') }),
    row('x5', { startedAt: T('2026-11-08T00:00:00Z') }),
    row('x6', { startedAt: undefined }),
    null,
  ];
  const r = buildPlacementReport({ ...win, rows: [row('a'), ...ignored] });
  assert.deepEqual(Object.keys(r.runners), ['skybox']);
  assert.equal(r.total.placements, 1, 'the repeated id is deduplicated and nothing else qualifies');
});

test('a row whose runner reported no queuedMs or no runMs is still a placement, but leaves that stat out', () => {
  const r = buildPlacementReport({ ...win, rows: [row('a', { remotePhasesMs: undefined }), row('b', { runMs: undefined })] });
  assert.equal(r.total.placements, 2);
  assert.equal(r.total.wait.n, 1, 'only the row without remotePhasesMs drops out of the wait stats');
  assert.equal(r.total.run.n, 1);
});

test('renders one block per runner plus a total, and says so when the window holds nothing', () => {
  const text = renderPlacementText(buildPlacementReport({ ...win, rows }));
  const lines = text.split('\n');
  assert.match(lines[0], /^lane report placement: \[2026-11-01T00:00:00\.000Z, 2026-11-08T00:00:00\.000Z\)$/);
  assert.match(text, /^ {2}skybox: 4 placement\(s\), 1 non-green \(out of the run stats\)$/m);
  assert.match(text, /^ {2}wintop: 1 placement\(s\), 0 non-green \(out of the run stats\)$/m);
  assert.match(text, /^ {2}total:$/m);
  const columns = (label, after) => lines.slice(lines.indexOf(after) + 1).find((l) => l.trim().startsWith(label)).split(/\s+/).filter(Boolean);
  assert.deepEqual(columns('wait', '  skybox: 4 placement(s), 1 non-green (out of the run stats)'), ['wait', '4', '300000', '300000', '1.00', '1', '1']);
  assert.deepEqual(columns('run', '  skybox: 4 placement(s), 1 non-green (out of the run stats)'), ['run', '3', '600000', '600000', '1.00', '0', '1']);
  assert.deepEqual(columns('wait', '  total:'), ['wait', '5', '300000', '300000', '1.00', '1', '1']);
  assert.match(renderPlacementText(buildPlacementReport({ ...win, rows: [] })), /no scored remote placements in this window/);
});

test('command: --since is required and validated, --until defaults to now, --history and --json are honoured', async (testCtx) => {
  const file = path.join(makeTmpDir('placement-', testCtx), 'history.jsonl');
  fs.writeFileSync(file, `${rows.map((r) => JSON.stringify(r)).join('\n')}\n`);
  const run = async (args) => {
    let out = '';
    let err = '';
    const res = await placementReportCommand(args, { stdout: { write: (s) => { out += s; } }, stderr: { write: (s) => { err += s; } } });
    return { res, out, err };
  };
  const bad = [
    [[], /--since is required/],
    [['--since', 'nope'], /--since must be an ISO date/],
    [['--since', win.until, '--until', win.since], /--until must be after --since/],
    [['--since', win.since, '--until', 'nope'], /--until must be an ISO date/],
  ];
  for (const [args, message] of bad) {
    const got = await run(args);
    assert.equal(got.res.exitCode, 2, String(args));
    assert.match(got.err, message);
    assert.match(got.err, /^lane report placement: /);
  }
  const json = await run(['--since', win.since, '--history', file, '--json']);
  assert.equal(json.res.exitCode, 0);
  const parsed = JSON.parse(json.out);
  assert.equal(parsed.total.placements, 5);
  assert.ok(Date.parse(parsed.until) >= Date.now() - 60_000, '--until defaults to now');
  const text = await run(['--since', win.since, '--until', win.until, '--history', file]);
  assert.match(text.out, /skybox: 4 placement/);
});
