import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { runnerWatchOnce } from '../src/runner-watch.js';
import {
  readUnreachable, readHeartbeat, downRunnerReasons, heartbeatUpRunners, collectRunnerStates, renderRunnersLines,
  pingIntervalMs, MIN_PING_INTERVAL_MS, MAX_PING_INTERVAL_MS,
} from '../src/runner-unreachable.js';
import { selectRunner } from '../src/remote-client.js';
import { loadGlobalConfig, DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { paths } from '../src/state.js';
import { tmpDir } from './remote-harness.js';
import { freshEnv, writeGlobalConfig, laneRun } from './helpers.js';

/** BRAIN-1057: exponential backoff of the watcher's pings for an unreachable runner, and `lane runner offline|online` (a state-file overlay). */

const MIN = 60_000;
const NOW = Date.parse('2026-10-15T12:00:00Z');
const SKYBOX = { name: 'skybox', ssh: 'skybox-runner' };
const WINTOP = { name: 'wintop', ssh: 'wintop-runner' };
const CFG = { ...DEFAULT_GLOBAL_CONFIG, runnerDownAfterMs: 2 * MIN, runners: [SKYBOX, WINTOP] };

/** A fake ssh that logs each call's argv and fails every call naming a destination listed in `<dir>/failing`. */
function fakeSsh() {
  const dir = tmpDir('backoff-ssh');
  const log = path.join(dir, 'calls.log');
  const failing = path.join(dir, 'failing');
  const bin = path.join(dir, 'ssh');
  fs.writeFileSync(
    bin,
    `#!/usr/bin/env node
const fs = require('fs');
const argv = process.argv.slice(2);
fs.appendFileSync(${JSON.stringify(log)}, argv.join(' ') + '\\n');
const down = fs.existsSync(${JSON.stringify(failing)}) ? fs.readFileSync(${JSON.stringify(failing)}, 'utf8').split('\\n') : [];
process.exit(down.some((d) => d && argv.includes(d)) ? 255 : 0);
`,
  );
  fs.chmodSync(bin, 0o755);
  const calls = () => (fs.existsSync(log) ? fs.readFileSync(log, 'utf8').split('\n').filter(Boolean) : []);
  return {
    bin,
    callsTo: (ssh) => calls().filter((c) => c.split(' ').includes(ssh)).length,
    fail: (...destinations) => fs.writeFileSync(failing, destinations.join('\n')),
  };
}

test('pingIntervalMs: the time already failing, clamped to [1 min, 1 h]', () => {
  assert.equal(pingIntervalMs(NOW, NOW), MIN_PING_INTERVAL_MS);
  assert.equal(pingIntervalMs(NOW, NOW + 8 * MIN), 8 * MIN);
  assert.equal(pingIntervalMs(NOW, NOW + 10 * 60 * MIN), MAX_PING_INTERVAL_MS);
});

test('watch backoff: an unreachable runner is pinged at 0, 1, 2, 4, 8, 16, 32, 64 min, then hourly; success resets it', async () => {
  const root = tmpDir('backoff-schedule');
  const ssh = fakeSsh();
  ssh.fail('skybox-runner');
  const pingedAt = [];
  let seen = 0;
  const tick = async (minute) => {
    await runnerWatchOnce(root, { cfg: CFG, sshBin: ssh.bin, now: NOW + minute * MIN });
    if (ssh.callsTo('skybox-runner') > seen) pingedAt.push(minute);
    seen = ssh.callsTo('skybox-runner');
  };
  for (let minute = 0; minute <= 65; minute += 1) await tick(minute);
  assert.deepEqual(pingedAt, [0, 1, 2, 4, 8, 16, 32, 64]);
  assert.equal(readUnreachable(root).skybox, NOW, 'the streak start never moves');

  for (let minute = 66; minute < 124; minute += 1) await tick(minute);
  assert.equal(pingedAt.at(-1), 64, 'capped at an hour: nothing before minute 124');
  ssh.fail();
  await tick(124);
  assert.deepEqual(readUnreachable(root), {}, 'the ok ping at the cap clears the clock');
  await tick(125);
  assert.equal(pingedAt.at(-1), 125, 'no clock again: pinged on the very next tick');
});

test('watch backoff: a skipped runner keeps its heartbeat entry, stays down for placement, never up, and shows its next check', async () => {
  const root = tmpDir('backoff-skipped');
  const ssh = fakeSsh();
  ssh.fail('skybox-runner');
  for (const minute of [0, 1, 2, 3]) await runnerWatchOnce(root, { cfg: CFG, sshBin: ssh.bin, now: NOW + minute * MIN });
  assert.equal(ssh.callsTo('skybox-runner'), 3, 'minute 3 is inside the 2 min wait');
  const now = NOW + 3 * MIN;
  const beat = readHeartbeat(root);
  assert.equal(beat.at, now, 'the tick time still advances');
  assert.deepEqual(beat.runners.skybox, { lastOkAt: null, lastPingAt: NOW + 2 * MIN }, 'the skipped runner keeps its entry');
  assert.deepEqual(beat.runners.wintop, { lastOkAt: now, lastPingAt: now });
  assert.deepEqual(downRunnerReasons(root, CFG, now), { skybox: `unreachable since ${new Date(NOW).toTimeString().slice(0, 5)}` });
  assert.deepEqual(heartbeatUpRunners(root, CFG, now), ['wintop']);
  const [skybox, wintop] = collectRunnerStates(root, CFG, now);
  assert.equal(skybox.state, 'down');
  assert.equal(skybox.nextPingAt, NOW + 4 * MIN);
  assert.equal(skybox.lostSettleAt, NOW + CFG.runnerLostAfterMs);
  assert.equal(wintop.nextPingAt, null);
  assert.match(renderRunnersLines([skybox, wintop], now).join('\n'), /skybox {2}down .*next check \d\d:\d\d/);
});

/** Run the real CLI against a fresh home and state root whose config lists skybox and wintop. */
async function cliFixture(configExtra = {}) {
  const fx = freshEnv();
  writeGlobalConfig(fx.home, { version: 1, runners: [SKYBOX, WINTOP], ...configExtra });
  const lane = (...args) => laneRun(['runner', ...args], { env: fx.env });
  const load = () => {
    const previous = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
    Object.assign(process.env, { LANE_BROKER_HOME: fx.home, LANE_BROKER_STATE: fx.state });
    try {
      return loadGlobalConfig();
    } finally {
      for (const [key, value] of [['LANE_BROKER_HOME', previous.home], ['LANE_BROKER_STATE', previous.state]]) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  };
  return { ...fx, lane, load };
}

test('runner offline/online: round trip through the state file, idempotent, config.json untouched', async () => {
  const fx = await cliFixture();
  const configBefore = fs.readFileSync(path.join(fx.home, 'config.json'), 'utf8');

  const first = await fx.lane('offline', 'skybox');
  assert.equal(first.code, 0, first.stderr);
  assert.equal(first.stdout, 'runner skybox: offline (changed)\n');
  assert.equal((await fx.lane('offline', 'skybox')).stdout, 'runner skybox: offline (unchanged, already marked)\n');
  assert.deepEqual(Object.keys(JSON.parse(fs.readFileSync(paths(fx.state).runnerOffline, 'utf8'))), ['skybox']);
  assert.equal(fs.readFileSync(path.join(fx.home, 'config.json'), 'utf8'), configBefore);
  const [skybox, wintop] = fx.load().runners;
  assert.deepEqual([skybox.offline, skybox.offlineSource, wintop.offline], [true, 'cli', undefined]);

  const status = JSON.parse((await fx.lane('status', '--json')).stdout);
  assert.deepEqual(status.map((r) => [r.name, r.state, r.offlineSource]), [['skybox', 'offline', 'cli'], ['wintop', 'up', null]]);
  assert.match((await fx.lane('status')).stdout, /skybox {2}offline \(lane runner offline, since \d\d:\d\d\)/);

  assert.equal((await fx.lane('online', 'skybox')).stdout, 'runner skybox: online (changed)\n');
  assert.equal((await fx.lane('online', 'skybox')).stdout, 'runner skybox: online (unchanged, was not marked)\n');
  assert.equal(fx.load().runners[0].offline, undefined);
});

test('runner offline/online: an unknown name is refused with exit 2 listing the valid names, and writes nothing', async () => {
  const fx = await cliFixture();
  for (const sub of ['offline', 'online']) {
    const res = await fx.lane(sub, 'nope');
    assert.equal(res.code, 2);
    assert.match(res.stderr, /no runner named "nope" \(configured: skybox, wintop\)/);
  }
  assert.equal(fs.existsSync(paths(fx.state).runnerOffline), false);
  assert.equal((await fx.lane('offline')).code, 2, 'a name is required');
});

test('runner online: a runner the config itself marks offline is not pretended online (exit 1, says so)', async () => {
  const fx = await cliFixture({ runners: [{ ...SKYBOX, offline: true }, WINTOP] });
  const res = await fx.lane('online', 'skybox');
  assert.equal(res.code, 1);
  assert.match(res.stderr, /still offline: the global config sets offline: true/);
  assert.match((await fx.lane('status')).stdout, /skybox {2}offline \(config\)/);
});

test('overlay: a marked runner is skipped by placement ("marked offline") and not pinged by the watcher', async () => {
  const fx = await cliFixture();
  await fx.lane('offline', 'skybox');
  const cfg = fx.load();
  const ssh = fakeSsh();

  const picked = await selectRunner(cfg.runners, { sshBin: ssh.bin, deadlineMs: 3000 });
  assert.deepEqual(picked.skipped.find((x) => x.name === 'skybox'), { name: 'skybox', reason: 'marked offline' });
  assert.equal(ssh.callsTo('skybox-runner'), 0);

  await runnerWatchOnce(fx.state, { cfg, sshBin: ssh.bin, now: NOW });
  assert.equal(ssh.callsTo('skybox-runner'), 0, 'the watcher does not ping an overlay-offline runner');
  assert.equal(ssh.callsTo('wintop-runner') > 0, true);
});
