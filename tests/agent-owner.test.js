import test, { beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { makeTmpDir } from './helpers/tmp.js';
import { AgentOwnedError, agentPaths, claimOwner, ensureAgentDirs, readAgentStatus, readOwner, writeStatus } from '../src/agent/store.js';
import { processStartTime } from '../src/state.js';

let remoteRoot;
let savedEnv;
const children = [];

beforeEach(() => {
  const base = makeTmpDir('agent-owner-');
  remoteRoot = path.join(base, 'remote');
  savedEnv = { state: process.env.LANE_BROKER_STATE, boot: process.env.LANE_BROKER_KERNEL_BOOT_ID };
  process.env.LANE_BROKER_STATE = path.join(base, 'state');
  process.env.LANE_BROKER_KERNEL_BOOT_ID = 'boot-1';
  ensureAgentDirs(remoteRoot);
});

afterEach(async () => {
  for (const child of children.splice(0)) {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
      await new Promise((resolve) => child.once('exit', resolve));
    }
  }
  for (const [name, value] of [['LANE_BROKER_STATE', savedEnv.state], ['LANE_BROKER_KERNEL_BOOT_ID', savedEnv.boot]]) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

async function liveForeignOwner() {
  const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
  children.push(child);
  await new Promise((resolve) => child.once('spawn', resolve));
  for (let i = 0; i < 100 && processStartTime(child.pid) === null; i++) await new Promise((resolve) => setTimeout(resolve, 20));
  await claimOwner(remoteRoot, { pid: child.pid });
  return child;
}

test('T25 a second agent refuses to own the root while the recorded owner is alive', async () => {
  const child = await liveForeignOwner();
  await assert.rejects(claimOwner(remoteRoot), (err) => err instanceof AgentOwnedError && err.message.includes(`pid ${child.pid}`));
  assert.equal(readOwner(remoteRoot).owner.pid, child.pid, 'the refused agent left the owner record alone');
});

test('T25 a second agent takes over once the owner has died, and the record names the new owner', async () => {
  const child = await liveForeignOwner();
  child.kill('SIGKILL');
  await new Promise((resolve) => child.once('exit', resolve));
  await claimOwner(remoteRoot);
  const { owner, live } = readOwner(remoteRoot);
  assert.deepEqual([owner.pid, owner.start, owner.kernelBootId, live], [process.pid, processStartTime(process.pid), 'boot-1', true]);
});

test('T25 an owner recorded under another kernel boot is dead even if its pid is alive', async () => {
  await liveForeignOwner();
  process.env.LANE_BROKER_KERNEL_BOOT_ID = 'boot-2';
  assert.equal(readOwner(remoteRoot).live, false);
  await claimOwner(remoteRoot);
  assert.equal(readOwner(remoteRoot).owner.pid, process.pid);
});

test('T25 the owner may claim again (an agent restarting under its own pid)', async () => {
  await claimOwner(remoteRoot);
  await claimOwner(remoteRoot);
  assert.equal(readOwner(remoteRoot).owner.pid, process.pid);
});

test('--status reads the live owner\'s last status.json tick, and reports no-owner before any owner exists', async () => {
  assert.deepEqual(readAgentStatus(remoteRoot), { state: 'no-owner' });
  await claimOwner(remoteRoot);
  writeStatus(remoteRoot, { gen: 3, executions: [] });
  const status = readAgentStatus(remoteRoot);
  assert.deepEqual([status.state, status.gen, status.pid], ['running', 3, process.pid]);
  assert.equal(JSON.parse(fs.readFileSync(agentPaths(remoteRoot).status, 'utf8')).gen, 3);
});

test('--status reports no-owner once the owner that wrote the last tick has died', async () => {
  const child = await liveForeignOwner();
  writeStatus(remoteRoot, { gen: 1, executions: [] });
  assert.equal(readAgentStatus(remoteRoot).state, 'running');
  child.kill('SIGKILL');
  await new Promise((resolve) => child.once('exit', resolve));
  assert.deepEqual(readAgentStatus(remoteRoot), { state: 'no-owner' });
});
