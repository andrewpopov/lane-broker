import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, laneRun } from './helpers.js';
import { classifyRow, workClassOf, backfillEstimates } from '../src/estimates-history.js';

const T0 = 1_700_000_000_000;
const row = (over) => ({ id: 'x', repo: 'r', lane: 'unit', startedAt: T0, endedAt: T0 + 100_000, exit: 0, signal: null, ...over });
const GIB = 1024 ** 3;

function fixtureRoot(rows, { torn = false } = {}) {
  const { state, env } = freshEnv();
  const text = rows.map((r) => JSON.stringify(r)).join('\n') + '\n' + (torn ? '{"id":"torn' : '');
  fs.writeFileSync(path.join(state, 'history.jsonl'), text);
  return { state, env };
}

const HISTORY = [
  row({ id: 'a', endedAt: T0 + 100_000, observedRssPeakBytes: 1 * GIB }),
  row({ id: 'b', endedAt: T0 + 200_000, observedRssPeakBytes: 2 * GIB }),
  row({ id: 'c', endedAt: T0 + 300_000, startedAt: T0, signal: 'SIGKILL', exit: null, observedRssPeakBytes: 3 * GIB }), // 300 s, killed: censored
  row({ id: 'd', startedAt: null, exit: 75, reason: 'queue-timeout' }), // never started
  row({ id: 'e', cancelled: true, exit: 130 }),
  row({ id: 'f', exit: 1 }), // failed on its own
  row({ id: 'g', repo: 'rouge', lane: 'sim-batch', startedAt: T0, endedAt: T0 + 900_000, observedRssPeakBytes: 5 * GIB }),
  { id: 'dead', key: 'k', error: 'supervisor died while queued', dequeuedDeadSupervisor: true, endedAt: T0 },
];

test('classifyRow: completed, censored (unrequested signal), and the ignored kinds', () => {
  assert.equal(classifyRow(row({})).kind, 'completed');
  assert.deepEqual(classifyRow(row({ signal: 'SIGKILL', exit: null })), { kind: 'censored', wallS: 100 });
  assert.equal(classifyRow(row({ startedAt: null })).why, 'never-started');
  assert.equal(classifyRow(row({ cancelled: true })).why, 'cancelled');
  assert.equal(classifyRow(row({ exit: 2 })).why, 'failed');
  assert.equal(classifyRow(row({ exit: 124, command: 'npm test' })).why, 'failed');
  for (const command of ['timeout 300 npm test', 'gtimeout -k 5 300 npm test', 'env CI=1 timeout 300 npm test']) {
    assert.deepEqual(classifyRow(row({ exit: 124, command })), { kind: 'censored', wallS: 100 }, command);
  }
  assert.equal(classifyRow(row({ runMs: 7000 })).wallS, 7);
});

test('workClassOf: a lane named like a sim is a sim; an explicit class wins; everything else is a test', () => {
  assert.equal(workClassOf(row({ lane: 'sim-batch' })), 'sim');
  assert.equal(workClassOf(row({ lane: 'unit', class: 'sim' })), 'sim');
  assert.equal(workClassOf(row({ lane: 'sim', class: 'test' })), 'test');
  assert.equal(workClassOf(row({ lane: 'simple' })), 'test');
  assert.equal(workClassOf(row({ lane: 'unit' })), 'test');
});

test('backfillEstimates: folds completed and censored runs per (repo, lane)', () => {
  const { keys, skipped } = backfillEstimates(HISTORY);
  assert.equal(keys.length, 2);
  const unit = keys.find((k) => k.lane === 'unit');
  assert.equal(unit.class, 'test');
  assert.equal(unit.n, 3);
  assert.equal(unit.censored, 1);
  assert.equal(unit.est_source, 'exact');
  assert.ok(unit.p50 >= 300, `a 300 s censored run must pull p50 up to at least 300, got ${unit.p50}`);
  assert.ok(unit.p90 > unit.p50);
  assert.equal(unit.rssP90Bytes, 3 * GIB);
  const sim = keys.find((k) => k.lane === 'sim-batch');
  assert.equal(sim.class, 'sim');
  assert.equal(sim.p50, 900);
  assert.deepEqual(skipped, { malformed: 1, 'never-started': 1, cancelled: 1, failed: 1 });
});

test('backfillEstimates: a host factor divides wall into ref-s', () => {
  const { keys } = backfillEstimates([row({})], { hostFactor: 2 });
  assert.equal(keys[0].p50, 50);
});

test('lane estimates --json reads a temp root read-only and prints per-key estimates and the factor used', async () => {
  const { state, env } = fixtureRoot(HISTORY, { torn: true });
  const before = fs.readFileSync(path.join(state, 'history.jsonl'), 'utf8');
  const r = await laneRun(['estimates', '--json', '--root', state], { env });
  assert.equal(r.code, 0, r.stderr);
  const out = JSON.parse(r.stdout);
  assert.equal(out.root, state);
  assert.equal(out.hostFactor, 1);
  assert.match(out.hostFactorSource, /no calibration/);
  const unit = out.keys.find((k) => k.lane === 'unit');
  assert.deepEqual([unit.n, unit.censored, unit.est_source, unit.rssP90Bytes], [3, 1, 'exact', 3 * GIB]);
  assert.equal(fs.readFileSync(path.join(state, 'history.jsonl'), 'utf8'), before);
  assert.deepEqual(fs.readdirSync(state), ['history.jsonl']);
});

test('lane estimates with an empty root prints no keys and creates nothing', async () => {
  const { state, env } = freshEnv();
  const r = await laneRun(['estimates', '--json', '--root', state], { env });
  assert.equal(r.code, 0, r.stderr);
  assert.deepEqual(JSON.parse(r.stdout).keys, []);
  assert.deepEqual(fs.readdirSync(state), []);
});
