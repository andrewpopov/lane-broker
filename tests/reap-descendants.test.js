import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import net from 'node:net';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, laneRun, waitFor, sleep } from './helpers.js';
import { paths } from '../src/state.js';
import { readLease } from '../src/lease.js';
import { DescendantTracker, parsePsTable, parseProcStat } from '../src/descendants.js';

/** BRAIN-419: cancel and leader exit must reap descendants that left the leader's process group. */

const SERVER = `
const net = require('node:net');
const fs = require('node:fs');
const srv = net.createServer().listen(0, '127.0.0.1', () => {
  fs.writeFileSync(process.argv[1], process.pid + ' ' + srv.address().port);
  setInterval(() => {}, 1000);
});
`;

// The leader starts the server in a NEW session (what Playwright's webServer does), then either idles or exits.
const LEADER = `
const { spawn } = require('node:child_process');
const fs = require('node:fs');
spawn(process.execPath, ['-e', fs.readFileSync(process.argv[2], 'utf8'), process.argv[3]], { detached: true, stdio: 'ignore' }).unref();
if (process.argv[4] === 'exit') {
  const wait = setInterval(() => {
    if (fs.existsSync(process.argv[3])) { clearInterval(wait); setTimeout(() => process.exit(0), 700); }
  }, 50);
} else setInterval(() => {}, 1000);
`;

function alive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function portFree(port) {
  return new Promise((resolve) => {
    const s = net.createServer();
    s.once('error', () => resolve(false));
    s.listen(port, '127.0.0.1', () => s.close(() => resolve(true)));
  });
}

async function startLane(t, mode) {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const serverJs = path.join(base, 'server.js');
  const leaderJs = path.join(base, 'leader.js');
  const portFile = path.join(base, 'server.pid-port');
  fs.writeFileSync(serverJs, SERVER);
  fs.writeFileSync(leaderJs, LEADER);
  const lane = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', process.execPath, leaderJs, serverJs, portFile, mode], { env, cwd: repoDir });
  let server = null;
  t.after(async () => {
    if (server) {
      try {
        process.kill(server.pid, 'SIGKILL');
      } catch {
        // already reaped, the expected case
      }
    }
    if (lane.exitCode === null && lane.signalCode === null) {
      lane.kill('SIGKILL');
      await new Promise((resolve) => lane.on('exit', resolve));
    }
  });
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
  await waitFor(() => fs.existsSync(portFile) && fs.readFileSync(portFile, 'utf8').includes(' '));
  const [pid, port] = fs.readFileSync(portFile, 'utf8').split(' ').map(Number);
  server = { pid, port };
  assert.notEqual(server.pid, lease.childPgid);
  assert.equal(await portFree(port), false, 'the grandchild holds its port');
  return { env, repoDir, state, lane, lease, leaseId, server };
}

async function exited(lane) {
  if (lane.exitCode === null && lane.signalCode === null) await new Promise((resolve) => lane.on('exit', resolve));
}

test('lane cancel reaps a grandchild that left the process group, freeing its port', async (t) => {
  const { env, repoDir, lane, leaseId, server } = await startLane(t, 'idle');
  const cancel = await laneRun(['cancel', leaseId], { env, cwd: repoDir });
  assert.equal(cancel.code, 0, cancel.stderr);
  await exited(lane);
  assert.equal(alive(server.pid), false, 'the detached grandchild survived the cancel');
  assert.equal(await portFree(server.port), true, 'its port is still held');
});

test('a leader that exits normally does not leave its detached grandchild running', async (t) => {
  const { state, lane, leaseId, server } = await startLane(t, 'exit');
  await exited(lane);
  assert.equal(alive(server.pid), false, 'the detached grandchild outlived the lease');
  assert.equal(await portFree(server.port), true, 'its port is still held');
  const log = fs.readFileSync(paths(state).admissionLog, 'utf8');
  assert.match(log, new RegExp(`lane-broker-reap id=${leaseId} descendants-reaped=1\\b`));
});

test('a process outside the lease is left alone by a cancel', async (t) => {
  const bystander = spawn('sleep', ['60'], { detached: true, stdio: 'ignore' }); // a sibling of the supervisor's tree, started by the test
  t.after(() => {
    try {
      process.kill(bystander.pid, 'SIGKILL');
    } catch {
      // already gone
    }
  });
  const { env, repoDir, lane, leaseId, server } = await startLane(t, 'idle');
  const cancel = await laneRun(['cancel', leaseId], { env, cwd: repoDir });
  assert.equal(cancel.code, 0, cancel.stderr);
  await exited(lane);
  assert.equal(alive(server.pid), false);
  await sleep(200);
  assert.equal(alive(bystander.pid), true, 'a process outside the lease was killed');
});

const table = (...rows) => () => rows.map(([pid, ppid, token]) => ({ pid, ppid, token }));

test('a sibling that merely shares an ancestor with the leader is never recorded', () => {
  const rows = [[10, 1, 'a'], [20, 10, 'b'], [30, 10, 'c'], [40, 20, 'd'], [50, 1, 'e']]; // 50 is the root's sibling; 30 is protected (supervisor)
  const tracker = new DescendantTracker(20, { readTable: table(...rows), protectedPids: [10] });
  tracker.scan();
  assert.deepEqual([...tracker.recorded.keys()], [40]);
});

test('a reparented descendant is still reaped once recorded; a reused pid is not killed', async () => {
  let rows = [[20, 10, 'L'], [40, 20, 'd1'], [41, 40, 'd2']];
  const tracker = new DescendantTracker(20, { readTable: () => rows.map(([pid, ppid, token]) => ({ pid, ppid, token })) });
  tracker.scan();
  // leader gone, 40 reparented to init, 41 died and its pid was taken by an unrelated process
  rows = [[40, 1, 'd1'], [41, 1, 'unrelated'], [99, 1, 'x']];
  const killed = [];
  const reaped = await tracker.reap({
    graceMs: 50,
    kill: (pid, sig) => {
      killed.push([pid, sig]);
      if (sig === 'SIGTERM') rows = rows.filter(([p]) => p !== pid);
    },
    sleep: () => sleep(5),
  });
  assert.deepEqual(killed, [[40, 'SIGTERM']]);
  assert.equal(reaped, 1);
});

test('a reused leader pid is never walked', () => {
  let rows = [[20, 10, 'L'], [40, 20, 'd']];
  const tracker = new DescendantTracker(20, { readTable: () => rows.map(([pid, ppid, token]) => ({ pid, ppid, token })) });
  tracker.scan();
  rows = [[20, 10, 'someone-else'], [60, 20, 'theirs']];
  tracker.scan();
  assert.deepEqual([...tracker.recorded.keys()], [40]);
});

test('process table parsers read ps lstart and /proc stat starttime', () => {
  assert.deepEqual(parsePsTable('  12   1 Mon Oct  6 12:00:00 2026\nbad\n'), [{ pid: 12, ppid: 1, token: 'Mon Oct 6 12:00:00 2026' }]);
  const stat = '77 (we ird) name)) S 5 77 77 0 -1 4194560 1 0 0 0 0 0 0 0 20 0 1 0 987654 1 1 18446744073709551615';
  assert.deepEqual(parseProcStat(77, stat), { pid: 77, ppid: 5, token: '987654' });
});
