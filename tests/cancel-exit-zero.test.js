import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, laneRun, waitFor } from './helpers.js';
import { paths } from '../src/state.js';
import { readLease } from '../src/lease.js';

// BRAIN-349: a child that traps SIGTERM and exits 0 must still publish a cancelled result.
const TRAP_SCRIPT = `trap 'exit 0' TERM; echo ready; while :; do sleep 0.1; done`;

function setup() {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  return { state, env, repoDir };
}

async function startForeground(command, { state, env, repoDir }) {
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'bash', '-c', command], { env, cwd: repoDir });
  const exited = new Promise((resolve) => child.on('close', (code) => resolve(code)));
  const id = await waitFor(() => {
    let names;
    try {
      names = fs.readdirSync(paths(state).leases).filter((n) => n.endsWith('.json'));
    } catch {
      return null;
    }
    const leaseId = names[0] ? names[0].replace(/\.json$/, '') : null;
    const l = leaseId ? readLease(state, leaseId) : null;
    return l && l.childPgid ? leaseId : null;
  });
  return { exited, id, lease: readLease(state, id) };
}

// Wait until the child's trap is installed (it has printed "ready" into the log).
async function waitReady(lease) {
  await waitFor(() => (fs.existsSync(lease.logPath) && fs.readFileSync(lease.logPath, 'utf8').includes('ready') ? true : null));
}

function readResult(lease) {
  return JSON.parse(fs.readFileSync(lease.resultPath, 'utf8'));
}

test('lane cancel on a TERM-trapping exit-0 child publishes cancelled; lane wait exits 130', async () => {
  const ctx = setup();
  const detached = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'bash', '-c', TRAP_SCRIPT], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(detached.code, 0, detached.stderr);
  const id = detached.stderr.match(/detached (\S+?);/)?.[1];
  assert.ok(id, `no id in: ${detached.stderr}`);
  const lease = await waitFor(() => {
    const l = readLease(ctx.state, id);
    return l && l.childPgid ? l : null;
  });
  await waitReady(lease);
  const waiter = laneRun(['wait', id], { env: ctx.env, cwd: ctx.repoDir });
  const cancel = await laneRun(['cancel', id], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(cancel.code, 0, cancel.stderr);
  const waited = await waiter;
  const result = readResult(lease);
  assert.equal(result.cancelled, true);
  assert.equal(result.exit, 130);
  assert.equal(waited.code, 130, waited.stderr);
});

test('lane cancel on a foreground TERM-trapping exit-0 run makes lane run exit 130', async () => {
  const ctx = setup();
  const fg = await startForeground(TRAP_SCRIPT, ctx);
  await waitReady(fg.lease);
  const cancel = await laneRun(['cancel', fg.id], { env: ctx.env, cwd: ctx.repoDir });
  assert.equal(cancel.code, 0, cancel.stderr);
  assert.equal(await fg.exited, 130);
  assert.equal(readResult(fg.lease).cancelled, true);
});

test('SIGTERM to the supervisor of a TERM-trapping exit-0 child publishes cancelled; foreground run exits 130', async () => {
  const ctx = setup();
  const fg = await startForeground(TRAP_SCRIPT, ctx);
  await waitReady(fg.lease);
  process.kill(fg.lease.supervisorPid, 'SIGTERM');
  assert.equal(await fg.exited, 130);
  const result = readResult(fg.lease);
  assert.equal(result.cancelled, true);
  assert.equal(result.exit, 130);
});

test('an uncancelled exit-0 child still yields exit 0 and no cancelled marker', async () => {
  const ctx = setup();
  const fg = await startForeground('echo ready; sleep 0.5; exit 0', ctx);
  assert.equal(await fg.exited, 0);
  const result = readResult(fg.lease);
  assert.equal(result.exit, 0);
  assert.equal(result.cancelled, undefined);
});
