import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { writeRepoConfig, laneRun } from './helpers.js';
import { tmpDir, setup } from './remote-harness.js';
import { parseScrubCommandEnv } from '../src/scrub-command-env.js';
import { execFileSync } from 'node:child_process';
import { childEnv } from '../src/supervisor.js';

/** TPWR-674: LANE_BROKER_SCRUB_COMMAND_ENV names variables a locally exec'd command must not inherit. */

const dumpEnv = (file) => [
  process.execPath,
  '-e',
  `require('fs').writeFileSync(${JSON.stringify(file)}, JSON.stringify(process.env));`,
];
const read = (file) => JSON.parse(fs.readFileSync(file, 'utf8'));
const AGENT = { SSH_AUTH_SOCK: '/tmp/agent.sock', SSH_AGENT_PID: '123', KEEP_ME: 'yes' };

async function localRun(extraEnv) {
  const { env, repoDir } = setup();
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const file = path.join(tmpDir('scrub-env'), 'command.json');
  const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...dumpEnv(file)], { env: { ...env, ...AGENT, ...extraEnv }, cwd: repoDir });
  assert.equal(res.code, 0, res.stderr);
  return read(file);
}

test('local run: listed variables and the control variable are gone from the command env', async () => {
  const seen = await localRun({ LANE_BROKER_SCRUB_COMMAND_ENV: 'SSH_AUTH_SOCK,SSH_AGENT_PID' });
  assert.equal(seen.SSH_AUTH_SOCK, undefined);
  assert.equal(seen.SSH_AGENT_PID, undefined);
  assert.equal(seen.LANE_BROKER_SCRUB_COMMAND_ENV, undefined);
  assert.equal(seen.KEEP_ME, 'yes');
});

test('local run without the variable: SSH_AUTH_SOCK reaches the command', async () => {
  const seen = await localRun({});
  assert.equal(seen.SSH_AUTH_SOCK, '/tmp/agent.sock');
  assert.equal(seen.SSH_AGENT_PID, '123');
});

test('invalid names are ignored and nothing else is stripped', async () => {
  const seen = await localRun({ LANE_BROKER_SCRUB_COMMAND_ENV: 'bad name,1BAD,=x,,SSH_AGENT_PID' });
  assert.equal(seen.SSH_AUTH_SOCK, '/tmp/agent.sock');
  assert.equal(seen.SSH_AGENT_PID, undefined);
  assert.equal(seen.KEEP_ME, 'yes');
});

test('nested direct-exec path scrubs too', async () => {
  const { env, repoDir } = setup();
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const file = path.join(tmpDir('scrub-env-nested'), 'command.json');
  const laneBin = path.resolve('bin/lane.js');
  const inner = [process.execPath, laneBin, 'run', '--repo', 'r', '--lane', 'default', '--', ...dumpEnv(file)];
  const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...inner], { env: { ...env, ...AGENT }, cwd: repoDir });
  assert.equal(res.code, 0, res.stderr);
  assert.equal(read(file).SSH_AUTH_SOCK, '/tmp/agent.sock', 'control: nested run without the list keeps it');
  const scrubbed = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', process.execPath, '-e',
    `const {spawnSync}=require('child_process');const r=spawnSync(process.execPath,${JSON.stringify(inner.slice(1))},{stdio:'inherit',env:{...process.env,LANE_BROKER_SCRUB_COMMAND_ENV:'SSH_AUTH_SOCK'}});process.exit(r.status)`],
  { env: { ...env, ...AGENT }, cwd: repoDir });
  assert.equal(scrubbed.code, 0, scrubbed.stderr);
  const seen = read(file);
  assert.equal(seen.SSH_AUTH_SOCK, undefined);
  assert.equal(seen.LANE_BROKER_SCRUB_COMMAND_ENV, undefined);
  assert.equal(seen.SSH_AGENT_PID, '123');
});

test('parseScrubCommandEnv keeps valid names only; childEnv leaves env alone without a list', () => {
  assert.deepEqual(parseScrubCommandEnv(' A_1, b ,bad-name,9x,A_1,'), ['A_1', 'b']);
  assert.deepEqual(parseScrubCommandEnv(undefined), []);
  const ticket = { id: 't', key: 'k' };
  assert.equal(childEnv(ticket, { SSH_AUTH_SOCK: 's' }).SSH_AUTH_SOCK, 's');
  assert.equal(childEnv({ ...ticket, scrubCommandEnv: ['SSH_AUTH_SOCK'] }, { SSH_AUTH_SOCK: 's', LANE_BROKER_SCRUB_COMMAND_ENV: 'SSH_AUTH_SOCK' }).LANE_BROKER_SCRUB_COMMAND_ENV, undefined);
});

test('lane capabilities --json advertises scrub-command-env/1', () => {
  const out = JSON.parse(execFileSync(process.execPath, [path.resolve('bin/lane.js'), 'capabilities', '--json'], { encoding: 'utf8' }));
  assert.ok(out.capabilities.includes('scrub-command-env/1'));
});
