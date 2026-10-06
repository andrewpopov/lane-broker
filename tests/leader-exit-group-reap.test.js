import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, laneRun, waitFor, sleep } from './helpers.js';
import { paths } from '../src/state.js';
import { readLease, isGroupAlive } from '../src/lease.js';

/** ROG-2181 T1b: a leader that dies on its own must not leave descendants running unleased. */

function setupRun(shellScript) {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const pidFile = path.join(base, 'grandchild.pid');
  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'sh', '-c', shellScript.replaceAll('PIDFILE', pidFile)], { env, cwd: repoDir });
  return { state, child, pidFile };
}

async function leaseOf(state) {
  const leaseId = await waitFor(() => {
    try {
      const names = fs.readdirSync(paths(state).leases).filter((n) => n.endsWith('.json'));
      return names[0] ? names[0].replace(/\.json$/, '') : null;
    } catch {
      return null;
    }
  });
  const lease = await waitFor(() => {
    const l = readLease(state, leaseId);
    return l && l.childPgid ? l : null;
  });
  return { leaseId, lease };
}

function historyRow(state, id) {
  return fs
    .readFileSync(paths(state).history, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((l) => JSON.parse(l))
    .find((r) => r.id === id);
}

async function killLeaderAlone(script, { timeoutMs }) {
  const { state, child, pidFile } = setupRun(script);
  const { leaseId, lease } = await leaseOf(state);
  await waitFor(() => fs.existsSync(pidFile) && fs.readFileSync(pidFile, 'utf8').trim());
  await sleep(300); // let the grandchild's trap install
  const grandchild = Number(fs.readFileSync(pidFile, 'utf8'));
  assert.notEqual(grandchild, lease.childPgid, 'the grandchild is a different process than the leader');

  process.kill(lease.childPgid, 'SIGKILL'); // the leader only
  await waitFor(() => !fs.existsSync(path.join(paths(state).leases, `${leaseId}.json`)), { timeoutMs });
  // the lease is gone: by then the whole group must be too
  assert.equal(isGroupAlive(lease.childPgid), false, 'the lease was released while a group member survived');
  await new Promise((resolve) => (child.exitCode !== null ? resolve() : child.on('exit', resolve)));
  const row = historyRow(state, leaseId);
  assert.equal(row.reason, 'leader-exited-group-reaped');
  return row;
}

test('SIGKILL of the leader alone reaps its surviving child before the lease is released', async () => {
  await killLeaderAlone("sleep 30 & echo $! > PIDFILE; wait", { timeoutMs: 8000 });
});

test('a TERM-trapping grandchild of a dead leader is escalated to KILL before release', async () => {
  const started = Date.now();
  await killLeaderAlone("sh -c 'trap \"\" TERM; echo $$ > PIDFILE; sleep 30' & wait", { timeoutMs: 20000 });
  assert.ok(Date.now() - started >= 5000, 'a TERM-ignoring member can only have been reaped after the grace period');
});

test('a normal exit with no survivors is unchanged (no reap reason)', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'sh', '-c', 'exit 0'], { env, cwd: repoDir });
  assert.equal(res.code, 0, res.stderr);
  const rows = fs.readFileSync(paths(state).history, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
  assert.equal(rows.length, 1);
  assert.equal(rows[0].reason, undefined);
  assert.equal(rows[0].exit, 0);
});
