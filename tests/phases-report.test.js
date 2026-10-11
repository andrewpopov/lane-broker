import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildPhasesReport, renderPhasesText } from '../src/phases-report.js';

const REPO = '_Volumes_Lexar_proj_foo_.git';
const T = (iso) => Date.parse(iso);
const base = { repo: REPO, executor: 'remote', startedAt: T('2026-10-09T02:00Z') };
const window = { repos: ['foo'], since: '2026-10-09T00:00:00Z', until: '2026-10-10T00:00:00Z' };

const rows = [
  { ...base, id: 'a', runner: 'wintop', depsCache: 'miss', remotePhasesMs: { snapshotMs: 1000, depsMs: 3000, setupMs: 1000, commandMs: 5000, artifactsMs: 0 } },
  { ...base, id: 'b', runner: 'wintop', depsCache: 'hit', remotePhasesMs: { snapshotMs: 1000, depsMs: 1000, setupMs: 0, commandMs: 8000 } },
  // an older runner: no depsMs (its setupMs holds deps + setup); still counted, absent from the depsMs stats
  { ...base, id: 'c', runner: 'skybox', remotePhasesMs: { snapshotMs: 500, setupMs: 1500, commandMs: 8000 } },
  // an old row with no phases at all, a local row, another repo and an out-of-window row are not counted
  { ...base, id: 'd', runner: 'skybox' },
  { ...base, id: 'e', executor: 'local', remotePhasesMs: { commandMs: 1 } },
  { ...base, id: 'f', repo: '_Volumes_Lexar_proj_bar_.git', runner: 'skybox', remotePhasesMs: { commandMs: 1 } },
  { ...base, id: 'g', runner: 'skybox', startedAt: T('2026-10-08T23:00Z'), remotePhasesMs: { commandMs: 1 } },
];

test('per runner: median and p90 of each phase, overhead share, and an old runner without depsMs', () => {
  const r = buildPhasesReport({ ...window, rows });
  assert.deepEqual(Object.keys(r.runners).sort(), ['skybox', 'wintop']);
  const w = r.runners.wintop;
  assert.equal(w.runs, 2);
  assert.equal(w.phases.depsMs.n, 2);
  assert.equal(w.phases.depsMs.p50, 1000);
  assert.equal(w.phases.depsMs.p90, 3000);
  assert.equal(w.phases.commandMs.p90, 8000);
  // (5000 + 2000) / (10000 + 10000)
  assert.equal(w.overheadShare, 0.35);
  assert.deepEqual(w.depsCache, { miss: 1, hit: 1 });
  const s = r.runners.skybox;
  assert.equal(s.runs, 1);
  assert.equal(s.phases.depsMs.n, 0);
  assert.equal(s.phases.depsMs.p50, null);
  assert.equal(s.overheadShare, 2000 / 10000);
  assert.match(renderPhasesText(r), /wintop: 2 runs, overhead share 35\.0%/);
});

test('a window with no remote phase rows renders and reports an empty runner map', () => {
  const r = buildPhasesReport({ ...window, rows: [] });
  assert.deepEqual(r.runners, {});
  assert.match(renderPhasesText(r), /no remote runs with phase timings/);
});

test('the client copies the runner phases and queuedMs into the row; an old-format result adds nothing and does not throw', async () => {
  const { relayedPhases } = await import('../src/supervisor.js');
  const fresh = relayedPhases({ uploadMs: 5, artifactReturnMs: 2, result: { queuedMs: 40, phasesMs: { snapshotMs: 10, depsMs: 20, setupMs: 0, commandMs: 30, bogus: 1, artifactsMs: -1 } } });
  assert.deepEqual(fresh, { remotePhasesMs: { uploadMs: 5, artifactReturnMs: 2, queuedMs: 40, snapshotMs: 10, depsMs: 20, setupMs: 0, commandMs: 30 } });
  assert.deepEqual(relayedPhases({ result: { kind: 'exit', exit: 0, runMs: 10 } }), {});
  assert.deepEqual(relayedPhases({ uploadMs: 7, result: {} }), { remotePhasesMs: { uploadMs: 7 } });
});
