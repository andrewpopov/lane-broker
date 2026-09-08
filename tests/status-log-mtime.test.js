import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';
import { collectStatus, renderStatusText, LOG_STALE_MS } from '../src/status.js';

/**
 * BRAIN-202: the incident that motivated the restricted-backfill fix in
 * scheduler.js was invisible in `lane status` for nine hours -- `elapsed`
 * and `heartbeat-age` both describe the SUPERVISOR, which looked healthy
 * the whole time; nothing described the CHILD's own output. This is a
 * REPORT-ONLY signal off the lease's logPath mtime: it must never claim the
 * lane is "hung" (a legitimately quiet compile looks identical) and it must
 * never crash `lane status` when the log is missing or unreadable.
 */

function baseStatus(overrides = {}) {
  return {
    capacity: 4,
    used: 0,
    paused: null,
    configWarning: null,
    loadGate: { closed: false, lastLoad: null, sampleAgeMs: null, consecutiveUnder: 0, loadClose: 15, loadOpen: 11, loadOpenSamples: 3 },
    running: [],
    queued: [],
    ...overrides,
  };
}

function runningRow(overrides = {}) {
  return {
    id: 'lane-1',
    key: 'rouge:e2e',
    state: 'RUNNING',
    pid: 123,
    elapsedMs: 60_000,
    heartbeatAgeMs: 1_000,
    logAgeMs: null,
    log: '/tmp/lane-1.log',
    weight: 1,
    ...overrides,
  };
}

test('lane status flags a stale log with "no log output for <duration>"', () => {
  const text = renderStatusText(baseStatus({ running: [runningRow({ logAgeMs: LOG_STALE_MS + 1000 })] }));
  assert.match(text, /no log output for \d+m\d+s/);
});

test('lane status does not flag a fresh log', () => {
  const text = renderStatusText(baseStatus({ running: [runningRow({ logAgeMs: 5_000 })] }));
  assert.doesNotMatch(text, /no log output for/);
});

test('lane status never calls a quiet log "hung" -- report-only wording', () => {
  const text = renderStatusText(baseStatus({ running: [runningRow({ logAgeMs: LOG_STALE_MS + 1000 })] }));
  assert.doesNotMatch(text, /hung/i);
});

test('a missing log file does not flag as stale and does not crash collectStatus', async () => {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const prevHome = process.env.LANE_BROKER_HOME;
  const prevState = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;

  try {
    writeLease(state, {
      id: 'no-log-lease',
      key: 'rouge:default',
      bootId: bootId(),
      supervisorPid: process.pid,
      supervisorStart: null,
      childPgid: null,
      heartbeatAt: Date.now(),
      startedAt: Date.now(),
      logPath: path.join(state, 'does-not-exist.log'),
      weight: 1,
      state: LEASE_STATE.RUNNING,
    });

    const status = await collectStatus();
    const row = status.running.find((r) => r.id === 'no-log-lease');
    assert.ok(row, 'the lease must still be reported');
    assert.equal(row.logAgeMs, null, 'a missing log must report null age, not throw');

    const text = renderStatusText(status);
    assert.doesNotMatch(text, /no log output for/, 'null age must never be treated as stale');
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
    process.env.LANE_BROKER_STATE = prevState;
  }
});

test('an unreadable log path (a directory, not a file) does not crash collectStatus', async () => {
  const { home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const prevHome = process.env.LANE_BROKER_HOME;
  const prevState = process.env.LANE_BROKER_STATE;
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;

  const weirdLogPath = path.join(state, 'a-directory.log');
  fs.mkdirSync(weirdLogPath);

  try {
    writeLease(state, {
      id: 'dir-log-lease',
      key: 'rouge:default',
      bootId: bootId(),
      supervisorPid: process.pid,
      supervisorStart: null,
      childPgid: null,
      heartbeatAt: Date.now(),
      startedAt: Date.now(),
      logPath: weirdLogPath,
      weight: 1,
      state: LEASE_STATE.RUNNING,
    });

    const status = await collectStatus();
    const row = status.running.find((r) => r.id === 'dir-log-lease');
    // fs.statSync succeeds on a directory (it has a real mtime), so this is
    // not expected to be null -- the point of this test is that collectStatus
    // never throws regardless of what logPath points at.
    assert.ok(row);
  } finally {
    process.env.LANE_BROKER_HOME = prevHome;
    process.env.LANE_BROKER_STATE = prevState;
  }
});
