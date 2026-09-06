import test from 'node:test';
import assert from 'node:assert/strict';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { paths, atomicWriteJson } from '../src/state.js';
import { collectStatus, renderStatusText } from '../src/status.js';

test('lane status surfaces a persisted config reload failure', async () => {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const prevHome = process.env.LANE_BROKER_HOME;
  const prevState = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    atomicWriteJson(paths(state).configWarning, {
      message: 'config.json: invalid JSON (status-test)',
      firstAt: Date.now() - 60_000,
      lastAt: Date.now(),
    });

    const status = await collectStatus();
    assert.ok(status.configWarning, 'a persisted warning must be surfaced in the status object');
    assert.equal(status.configWarning.message, 'config.json: invalid JSON (status-test)');

    const text = renderStatusText(status);
    assert.match(text, /config: WARNING/);
    assert.match(text, /status-test/);
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
    process.env.LANE_BROKER_STATE = prevState;
  }
});

test('lane status shows no config warning when none is recorded', async () => {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const prevHome = process.env.LANE_BROKER_HOME;
  const prevState = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    const status = await collectStatus();
    assert.equal(status.configWarning, null);
    assert.doesNotMatch(renderStatusText(status), /config: WARNING/);
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
    process.env.LANE_BROKER_STATE = prevState;
  }
});
