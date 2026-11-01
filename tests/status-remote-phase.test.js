import test from 'node:test';
import assert from 'node:assert/strict';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { createAttempt, updateAttempt } from '../src/attempts.js';
import { collectStatus, renderStatusText } from '../src/status.js';

// BRAIN-770: a remote attempt waiting in the runner's queue reads queued, not `phase=running`; wait and run time are split.

async function withStatusEnv(fn) {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const prev = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    return await fn(state);
  } finally {
    for (const [name, value] of [['LANE_BROKER_HOME', prev.home], ['LANE_BROKER_STATE', prev.state]]) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

const MIN = 60_000;

async function remoteAttempt(state, patch) {
  const created = await createAttempt(state, 'att-1', { runner: 'skybox' });
  await updateAttempt(state, 'att-1', created.generation, { phase: 'running', runner: 'skybox', startedAt: Date.now() - 10 * MIN, ...patch });
  const status = await collectStatus({});
  return { row: status.remote.find((r) => r.id === 'att-1'), text: renderStatusText(status) };
}

test('a remote attempt the runner still has queued shows queued@runner with its wait, not phase=running', async () => {
  await withStatusEnv(async (state) => {
    const { row, text } = await remoteAttempt(state, { runnerPhase: 'queued' });
    assert.equal(row.runnerPhase, 'queued');
    assert.equal(row.commandStartedAt, null);
    assert.equal(row.runMs, null);
    assert.ok(row.waitedMs >= 10 * MIN);
    assert.match(text, /att-1 {2}queued@skybox {2}waited=10m/);
    assert.doesNotMatch(text, /att-1.*phase=running/);
  });
});

test('once the runner started the command, run and wait are split', async () => {
  await withStatusEnv(async (state) => {
    const startedAt = Date.now() - 10 * MIN;
    const { row, text } = await remoteAttempt(state, { startedAt, runnerPhase: 'admitted', commandStartedAt: startedAt + 3 * MIN });
    assert.equal(row.waitedMs, 3 * MIN);
    assert.ok(row.runMs >= 7 * MIN && row.runMs < 8 * MIN);
    assert.match(text, /att-1 {2}running@skybox {2}run=7m\d+s {2}waited=3m0s/);
  });
});

test('a runner that reports no phase keeps the old line and null fields', async () => {
  await withStatusEnv(async (state) => {
    const { row, text } = await remoteAttempt(state, {});
    assert.deepEqual([row.runnerPhase, row.commandStartedAt, row.waitedMs, row.runMs], [null, null, null, null]);
    assert.match(text, /att-1 {2}runner=skybox {2}phase=running {2}elapsed=10m/);
  });
});
