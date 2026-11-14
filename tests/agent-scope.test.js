import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { spawn, spawnSync } from 'node:child_process';
import { makeTmpDir } from './helpers/tmp.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { sleep, waitFor } from './helpers.js';
import { CLAIM_ANSWERS, renderAgentMetrics, writeAgentMetrics } from '../src/agent/metrics.js';
import { SUBPHASES } from '../src/agent/store.js';
import { userManagerEnv } from '../src/cpu-limit.js';

const EXEC_ID = '11111111-2222-4333-8444-555555555555.e3';
const ARGV = ['/usr/bin/node', '/opt/lane/bin/lane.js', 'remote-exec', '--epoch', '3'];
const named = (failure, pattern) => assert.match(failure.message, pattern);

const launchScenarios = {
  'a passing probe spawns the supervisor in its own quota-free transient scope, a failing one spawns it bare': async (load) => {
    const { supervisorLaunch } = await load('cpu-limit.js');
    const scoped = supervisorLaunch(EXEC_ID, ARGV, { available: true, reason: 'ok' });
    assert.equal(scoped.scoped, true);
    assert.equal(scoped.cmd, 'systemd-run');
    assert.deepEqual(scoped.args, ['--user', '--scope', '--expand-environment=no', `--unit=lane-exec-${EXEC_ID}`, '--quiet', '--collect', '--', ...ARGV]);
    const bare = supervisorLaunch(EXEC_ID, ARGV, { available: false, reason: 'systemd-run-missing' });
    assert.deepEqual(bare, { cmd: ARGV[0], args: ARGV.slice(1), scoped: false });
  },
  'the unit name keeps systemd-safe characters only': async (load) => {
    const { supervisorScopeUnit } = await load('cpu-limit.js');
    assert.equal(supervisorScopeUnit(EXEC_ID), `lane-exec-${EXEC_ID}`);
    assert.equal(supervisorScopeUnit('a/b c\n'), 'lane-exec-a-b-c-');
  },
  'XDG_RUNTIME_DIR is supplied on linux only when the user manager dir exists and the env has none': async (load) => {
    const { userManagerEnv: env } = await load('cpu-limit.js');
    const present = { platform: 'linux', uid: 1001, exists: () => true };
    assert.deepEqual(env({ PATH: '/bin' }, present), { XDG_RUNTIME_DIR: '/run/user/1001' });
    assert.deepEqual(env({ XDG_RUNTIME_DIR: '/custom' }, present), {});
    assert.deepEqual(env({}, { ...present, exists: () => false }), {});
    assert.deepEqual(env({}, { ...present, platform: 'darwin' }), {});
  },
};
const LIMIT = 'cpu-limit.js';
const launchCanaries = [
  ['a passing probe spawns the supervisor in its own quota-free transient scope, a failing one spawns it bare', ['if (!scope.available) return', 'if (false) return'], null],
  ['a passing probe spawns the supervisor in its own quota-free transient scope, a failing one spawns it bare', ['const uncapped = grantCores === null;', 'const uncapped = false;'], /grantCores must be positive/],
  ['the unit name keeps systemd-safe characters only', ["replace(/[^A-Za-z0-9:_.-]/g, '-')", "replace(/[^A-Za-z0-9:_.\\/ \\n-]/g, '-')"], null],
  ['XDG_RUNTIME_DIR is supplied on linux only when the user manager dir exists and the env has none', ['!env.XDG_RUNTIME_DIR && ', ''], null],
  ['XDG_RUNTIME_DIR is supplied on linux only when the user manager dir exists and the env has none', ["platform === 'linux' && uid !== undefined", 'uid !== undefined'], null],
];

for (const [name, scenario] of Object.entries(launchScenarios)) test(name, () => scenario(realSrc));
for (const [name, mutation, expected] of launchCanaries) {
  test(`canary (${LIMIT}): with "${mutation[0]}" removed, "${name}" fails on its own assertion`, async (t) => {
    const failure = await assertCanaryFails(LIMIT, mutation, launchScenarios[name], expected);
    t.diagnostic(`failed as expected: ${failure.message.split('\n')[0]}`);
  });
}

const SNAPSHOT = {
  now: 1_700_000_123_456,
  records: [{ subphase: 'RUNNING' }, { subphase: 'RUNNING' }, { subphase: 'OUTBOX' }],
  claims: { claimed: 2, none: 5 },
  listening: true,
  registered: false,
  version: '1.2"3',
};
const sample = (text, series) => text.split('\n').find((line) => line.startsWith(`${series} `))?.split(' ')[1];

const metricsScenarios = {
  'lane-agent.prom carries every series, with 0 for an empty subphase or claim answer and the label escaped': async (load) => {
    const { renderAgentMetrics: render } = await load('agent/metrics.js');
    const text = render(SNAPSHOT);
    assert.equal(sample(text, 'lane_agent_last_tick_timestamp_seconds'), '1700000123.456');
    for (const [subphase, n] of Object.entries({ RUNNING: 2, OUTBOX: 1, FETCHING: 0, KILLING: 0, DONE: 0 })) assert.equal(sample(text, `lane_agent_executions{subphase="${subphase}"}`), String(n), subphase);
    assert.equal(SUBPHASES.filter((s) => sample(text, `lane_agent_executions{subphase="${s}"}`) === undefined).length, 0, 'every subphase has a series');
    assert.deepEqual(CLAIM_ANSWERS.map((a) => sample(text, `lane_agent_claims_total{answer="${a}"}`)), ['2', '5', '0', '0']);
    assert.equal(sample(text, 'lane_agent_listen_up'), '1');
    assert.equal(sample(text, 'lane_agent_registered'), '0');
    assert.equal(sample(text, 'lane_agent_info{version="1.2\\"3"}'), '1');
    assert.match(text, /# TYPE lane_agent_claims_total counter\n/);
    assert.equal(text.endsWith('\n'), true);
  },
  'the file is replaced by rename: a reader holding the old file still sees it whole, and no temp file is left': async (load) => {
    const { writeAgentMetrics: write } = await load('agent/metrics.js');
    const dir = makeTmpDir('agent-prom-');
    const file = path.join(dir, 'lane-agent.prom');
    write(dir, SNAPSHOT);
    const before = fs.readFileSync(file, 'utf8');
    const held = fs.openSync(file, 'r');
    try {
      write(dir, { ...SNAPSHOT, now: SNAPSHOT.now + 15_000, listening: false });
      assert.equal(fs.readFileSync(held, 'utf8'), before, 'the replaced file is untouched');
      assert.equal(sample(fs.readFileSync(file, 'utf8'), 'lane_agent_listen_up'), '0', 'the path holds the new content');
    } finally {
      fs.closeSync(held);
    }
    assert.deepEqual(fs.readdirSync(dir), ['lane-agent.prom']);
  },
};
const METRICS = 'agent/metrics.js';
const metricsCanaries = [
  ['lane-agent.prom carries every series, with 0 for an empty subphase or claim answer and the label escaped', ["const CLAIM_ANSWERS = ['claimed', 'none', 'upgrade', 'error']".replace('const ', 'export const '), "export const CLAIM_ANSWERS = ['claimed', 'none', 'upgrade']"]],
  ['lane-agent.prom carries every series, with 0 for an empty subphase or claim answer and the label escaped', ["replace(/\\\\/g, '\\\\\\\\').replace(/\"/g, '\\\\\"')", "replace(/\\\\/g, '\\\\\\\\')"]],
  ['the file is replaced by rename: a reader holding the old file still sees it whole, and no temp file is left', ['atomicWriteFile(path.join(dir, METRICS_FILE), renderAgentMetrics(snapshot))', "process.getBuiltinModule('node:fs').writeFileSync(path.join(dir, METRICS_FILE), renderAgentMetrics(snapshot))"]],
];
for (const [name, scenario] of Object.entries(metricsScenarios)) test(name, () => scenario(realSrc));
for (const [name, mutation] of metricsCanaries) {
  test(`canary (${METRICS}): with the guard removed, "${name}" fails on its own assertion`, async (t) => {
    const failure = await assertCanaryFails(METRICS, mutation, metricsScenarios[name]);
    t.diagnostic(`failed as expected: ${failure.message.split('\n')[0]}`);
  });
}
test('metrics helpers are the ones the agent uses', () => {
  assert.equal(typeof renderAgentMetrics, 'function');
  assert.equal(typeof writeAgentMetrics, 'function');
});

const stopScenario = async (load) => {
  const { agentCommand } = await load('agent/cli.js');
  const signals = new EventEmitter();
  const events = [];
  const start = async () => ({ stop: async () => { await sleep(20); events.push('stopped'); } });
  const running = agentCommand([], { loadConfig: () => ({}), signals, start, err: { write: () => {} } });
  await sleep(20);
  assert.deepEqual(events, [], 'nothing stops before the signal');
  signals.emit('SIGTERM');
  const { exitCode } = await running;
  assert.deepEqual(events, ['stopped'], 'agent.stop() settled before the command returned');
  assert.equal(exitCode, 0, 'a clean stop exits 0');
};
test('SIGTERM stops the agent and the command exits 0 only after stop() settled', () => stopScenario(realSrc));
for (const mutation of [['    await agent.stop();\n', ''], ['return { exitCode: 0 };\n  } catch', 'return { exitCode: 1 };\n  } catch']]) {
  test(`canary (agent/cli.js): with ${JSON.stringify(mutation[0].trim())} changed, the SIGTERM scenario fails on its own assertion`, async (t) => {
    const failure = await assertCanaryFails('agent/cli.js', mutation, stopScenario);
    t.diagnostic(`failed as expected: ${failure.message.split('\n')[0]}`);
  });
}

const systemdEnv = { ...process.env, ...userManagerEnv() };
const systemdUserWorks = process.platform === 'linux' && spawnSync('systemd-run', ['--user', '--scope', '--quiet', '--collect', 'true'], { env: systemdEnv, timeout: 10_000 }).status === 0;
const readCgroup = (pid) => fs.readFileSync(`/proc/${pid}/cgroup`, 'utf8').trim();
const alive = (pid) => { try { process.kill(pid, 0); return true; } catch { return false; } };

test('a supervisor in its transient user scope survives SIGKILL of the process that spawned it, execs in place and runs under user@UID.service', {
  skip: systemdUserWorks ? false : `systemd-run --user does not work here (platform ${process.platform}; needs a user manager with linger)`,
  timeout: 60_000,
}, async () => {
  const dir = makeTmpDir('agent-scope-');
  const parent = path.join(dir, 'parent.mjs');
  const limit = new URL('../src/cpu-limit.js', import.meta.url).href;
  fs.writeFileSync(parent, `import { spawn } from 'node:child_process';
import { supervisorLaunch } from ${JSON.stringify(limit)};
const { cmd, args } = supervisorLaunch(${JSON.stringify(EXEC_ID)}, ['sleep', '300'], { available: true });
const child = spawn(cmd, args, { detached: true, stdio: 'ignore', env: process.env });
child.unref();
process.stdout.write(child.pid + '\\n');
setInterval(() => {}, 1000);
`);
  const agent = spawn(process.execPath, [parent], { env: systemdEnv, stdio: ['ignore', 'pipe', 'inherit'] });
  const unit = `lane-exec-${EXEC_ID}.scope`;
  try {
    const pid = await new Promise((resolve, reject) => {
      agent.stdout.once('data', (d) => resolve(Number(String(d).trim())));
      agent.once('exit', () => reject(new Error('the parent exited before it printed a pid')));
    });
    await waitFor(() => fs.readFileSync(`/proc/${pid}/comm`, 'utf8').trim() === 'sleep', { timeoutMs: 10_000 });
    assert.match(readCgroup(pid), new RegExp(`user@\\d+\\.service/.*${unit.replace(/\./g, '\\.')}$`), 'the supervisor sits in its own scope under the user manager');
    agent.kill('SIGKILL');
    await new Promise((resolve) => agent.once('exit', resolve));
    await sleep(500);
    assert.equal(alive(pid), true, 'the supervisor outlived the process that spawned it');
    assert.match(readCgroup(pid), new RegExp(unit.replace(/\./g, '\\.')));
  } finally {
    agent.kill('SIGKILL');
    spawnSync('systemctl', ['--user', 'stop', unit], { env: systemdEnv, timeout: 15_000 });
  }
});
