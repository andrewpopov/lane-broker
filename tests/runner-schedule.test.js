import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { runnerAvailability, scheduleProblem } from '../src/runner-schedule.js';
import { selectRunner } from '../src/remote-client.js';
import { loadGlobalConfig, DEFAULT_GLOBAL_CONFIG, ConfigError } from '../src/config.js';
import { expectedRunMs } from '../src/estimates-history.js';
import { remoteSelectOptions } from '../src/supervisor.js';
import { tmpDir } from './remote-harness.js';
import { freshEnv, writeGlobalConfig } from './helpers.js';

const LA = 'America/Los_Angeles';
const ALL = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];
const daytime = { timezone: LA, available: [{ days: ALL, from: '06:00', to: '22:30' }] };
const overnight = { timezone: LA, available: [{ days: ['fri'], from: '20:00', to: '04:00' }] };
// 2026-10-11 is a Sunday; PDT (UTC-7) until 2026-11-01.
const at = (iso) => Date.parse(iso);

test('runnerAvailability: no schedule is always available', () => {
  assert.deepEqual(runnerAvailability(undefined, 0), { available: true, closesAt: null });
});

test('runnerAvailability: inside the window reports when it closes', () => {
  const r = runnerAvailability(daytime, at('2026-10-11T20:00:00Z')); // 13:00 PDT
  assert.equal(r.available, true);
  assert.equal(r.closesAt, at('2026-10-12T05:30:00Z')); // 22:30 PDT
});

test('runnerAvailability: outside the window is unavailable', () => {
  assert.deepEqual(runnerAvailability(daytime, at('2026-10-12T06:30:00Z')), { available: false, closesAt: null }); // 23:30 PDT
  assert.equal(runnerAvailability(daytime, at('2026-10-11T12:59:00Z')).available, false); // 05:59 PDT
  assert.equal(runnerAvailability(daytime, at('2026-10-11T13:00:00Z')).available, true); // 06:00 PDT, inclusive
  assert.equal(runnerAvailability(daytime, at('2026-10-12T05:30:00Z')).available, false); // 22:30 PDT, exclusive
});

test('runnerAvailability: a window crossing midnight belongs to the day it starts on', () => {
  const lateFri = runnerAvailability(overnight, at('2026-10-10T05:00:00Z')); // Fri 2026-10-09 22:00 PDT
  assert.equal(lateFri.available, true);
  assert.equal(lateFri.closesAt, at('2026-10-10T11:00:00Z')); // Sat 04:00 PDT
  assert.equal(runnerAvailability(overnight, at('2026-10-10T09:00:00Z')).available, true); // Sat 02:00 PDT
  assert.equal(runnerAvailability(overnight, at('2026-10-10T12:00:00Z')).available, false); // Sat 05:00 PDT
  assert.equal(runnerAvailability(overnight, at('2026-10-11T09:00:00Z')).available, false); // Sun 02:00: Sat has no window
  assert.equal(runnerAvailability(overnight, at('2026-10-09T09:00:00Z')).available, false); // Fri 02:00: Thu has no window
});

test('runnerAvailability: DST transition days use the real wall clock', () => {
  const night = { timezone: LA, available: [{ days: ALL, from: '22:00', to: '07:00' }] };
  // Fall back 2026-11-01 02:00 PDT -> 01:00 PST: the Sat-night window is 9 hours long, ending 07:00 PST = 15:00Z.
  const fall = runnerAvailability(night, at('2026-11-01T08:30:00Z'));
  assert.equal(fall.available, true);
  assert.equal(fall.closesAt, at('2026-11-01T15:00:00Z'));
  // Spring forward 2026-03-08: 07:00 PDT = 14:00Z.
  const spring = runnerAvailability(night, at('2026-03-08T10:00:00Z')); // 03:00 PDT
  assert.equal(spring.available, true);
  assert.equal(spring.closesAt, at('2026-03-08T14:00:00Z'));
  assert.equal(runnerAvailability(daytime, at('2026-03-08T13:00:00Z')).available, true); // 06:00 PDT
  assert.equal(runnerAvailability(daytime, at('2026-03-08T12:59:00Z')).available, false);
});

test('scheduleProblem: names each kind of bad schedule', () => {
  const ok = (over) => scheduleProblem({ ...daytime, ...over });
  assert.equal(scheduleProblem(daytime), null);
  assert.match(ok({ timezone: 'Mars/Olympus' }), /not a known timezone/);
  assert.match(ok({ available: [] }), /non-empty array of windows/);
  assert.match(ok({ available: [{ days: ['mon'], from: '6:00', to: '22:30' }] }), /"HH:MM"/);
  assert.match(ok({ available: [{ days: ['mon'], from: '06:00', to: '24:00' }] }), /"HH:MM"/);
  assert.match(ok({ available: [{ days: ['monday'], from: '06:00', to: '22:30' }] }), /unknown day/);
  assert.match(ok({ available: [{ days: [], from: '06:00', to: '22:30' }] }), /days.*non-empty/);
  assert.match(ok({ available: [{ days: ['mon'], from: '06:00', to: '06:00' }] }), /from equal to to/);
  assert.match(scheduleProblem('x'), /must be an object/);
});

test('loadGlobalConfig: a bad runner schedule is refused by name; a good one and none load', () => {
  const load = (runner) => {
    const { home } = freshEnv();
    writeGlobalConfig(home, { runners: [{ name: 'skybox', ssh: 'skybox-runner', ...runner }] });
    const prev = process.env.LANE_BROKER_HOME;
    process.env.LANE_BROKER_HOME = home;
    try {
      return loadGlobalConfig();
    } finally {
      process.env.LANE_BROKER_HOME = prev;
    }
  };
  assert.deepEqual(load({ schedule: daytime }).runners[0].schedule, daytime);
  assert.equal(load({}).runners[0].schedule, undefined);
  assert.throws(() => load({ schedule: { ...daytime, timezone: 'Nope/Zone' } }), (e) => e instanceof ConfigError && /runner "skybox".*not a known timezone/.test(e.message));
});

/** A fake ssh that records every probe it receives and answers an idle runner. */
function recordingSsh() {
  const dir = tmpDir('schedule-ssh');
  const log = path.join(dir, 'probes.log');
  const bin = path.join(dir, 'ssh');
  fs.writeFileSync(bin, `#!/usr/bin/env node\nrequire('node:fs').appendFileSync(${JSON.stringify(log)}, 'probe\\n');\nprocess.stdout.write(JSON.stringify({protocol:1,protocols:[1,2],paused:false,queued:0,running:0,capacity:{}})+'\\n');\n`);
  fs.chmodSync(bin, 0o755);
  return { bin, probes: () => (fs.existsSync(log) ? fs.readFileSync(log, 'utf8').split('\n').filter(Boolean).length : 0) };
}

test('selectRunner: a runner outside its window is skipped without being probed', async () => {
  const ssh = recordingSsh();
  const runners = [{ name: 'skybox', ssh: 'skybox', schedule: daytime }];
  const off = await selectRunner(runners, { sshBin: ssh.bin, deadlineMs: 3000, now: at('2026-10-12T07:00:00Z') });
  assert.equal(off.runner, null);
  assert.deepEqual(off.skipped, [{ name: 'skybox', reason: 'outside schedule' }]);
  assert.equal(ssh.probes(), 0);
  const on = await selectRunner(runners, { sshBin: ssh.bin, deadlineMs: 3000, now: at('2026-10-11T20:00:00Z') });
  assert.equal(on.runner.name, 'skybox');
  assert.equal(ssh.probes(), 1);
});

test('selectRunner: a window closing before the expected run skips the runner; no estimate places normally', async () => {
  const ssh = recordingSsh();
  const runners = [{ name: 'skybox', ssh: 'skybox', schedule: daytime }, { name: 'always', ssh: 'always' }];
  const now = at('2026-10-12T04:30:00Z'); // 21:30 PDT, window closes in an hour
  const long = await selectRunner(runners, { sshBin: ssh.bin, deadlineMs: 3000, now, expectedRunMs: 2 * 3600_000 });
  assert.equal(long.runner.name, 'always');
  assert.match(long.skipped[0].reason, /window closes at 2026-10-12T05:30:00.000Z, before the expected 7200s run/);
  const short = await selectRunner(runners, { sshBin: ssh.bin, deadlineMs: 3000, now, expectedRunMs: 600_000 });
  assert.equal(short.runner.name, 'skybox');
  const unknown = await selectRunner(runners, { sshBin: ssh.bin, deadlineMs: 3000, now });
  assert.equal(unknown.runner.name, 'skybox');
});

test('expectedRunMs: the p90 wall time of the lane, null with no usable run', () => {
  const row = (lane, s) => ({ repo: 'r', lane, startedAt: 1000, endedAt: 1000 + s * 1000, runMs: s * 1000, exit: 0 });
  const rows = [row('t', 100), row('t', 100), row('t', 100), row('x', 5)];
  const p90 = expectedRunMs(rows, 'r', 't'); // the estimator's p90 sits above the observed 100s
  assert.ok(p90 >= 100_000 && p90 <= 300_000, String(p90));
  assert.equal(expectedRunMs(rows, 'r', 'none'), null);
});

test('remoteSelectOptions: carries the lane\'s history p90 only when a runner is scheduled', async () => {
  const root = tmpDir('schedule-root');
  const row = { repo: 'r', lane: 't', startedAt: 1000, endedAt: 61_000, runMs: 60_000, exit: 0 };
  fs.writeFileSync(path.join(root, 'history.jsonl'), `${JSON.stringify(row)}\n`);
  const enriched = { repoId: 'r', lane: 't', remote: {}, resources: {}, weight: 1 };
  const scheduled = remoteSelectOptions(enriched, { ...DEFAULT_GLOBAL_CONFIG, runners: [{ name: 'a', ssh: 'a', schedule: daytime }] }, root);
  assert.ok(scheduled.expectedRunMs >= 60_000 && scheduled.expectedRunMs <= 180_000, String(scheduled.expectedRunMs));
  const plain = remoteSelectOptions(enriched, { ...DEFAULT_GLOBAL_CONFIG, runners: [{ name: 'a', ssh: 'a' }] }, root);
  assert.equal('expectedRunMs' in plain, false);
});
