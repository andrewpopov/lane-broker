import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import net from 'node:net';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, laneRun, waitFor, sleep } from './helpers.js';
import { paths, bootId } from '../src/state.js';
import { readLease, writeLease, reapIfStale } from '../src/lease.js';
import { DescendantTracker, parsePsTable, parseProcStat, readProcessTable, readProcRow, hasLiveMembers } from '../src/descendants.js';

/** BRAIN-419: cancel and leader exit must reap descendants that left the leader's process group. */

const SERVER = `
const net = require('node:net');
const fs = require('node:fs');
const srv = net.createServer().listen(0, '127.0.0.1', () => {
  fs.writeFileSync(process.argv[1], process.pid + ' ' + srv.address().port);
  setInterval(() => {}, 1000);
});
`;

// A server that, on TERM, starts a detached replacement and exits (its pid goes in <file>.replacement).
const FORKER = SERVER + `
process.on('SIGTERM', () => {
  const { spawn } = require('node:child_process');
  const c = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { detached: true, stdio: 'ignore' });
  c.unref();
  fs.writeFileSync(process.argv[1] + '.replacement', String(c.pid));
  process.exit(0);
});
`;

// The leader starts the server in a NEW session (what Playwright's webServer does), then either idles or exits.
const LEADER = `
const { spawn } = require('node:child_process');
const fs = require('node:fs');
spawn(process.execPath, ['-e', fs.readFileSync(process.argv[2], 'utf8'), process.argv[3]], { detached: true, stdio: 'ignore' }).unref();
if (process.argv[4] === 'now') {
  const wait = setInterval(() => {
    if (fs.existsSync(process.argv[3])) process.exit(0);
  }, 20);
} else if (process.argv[4] === 'exit') {
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

async function startLane(t, mode, { serverSource = SERVER, sampleMs = 100 } = {}) {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const serverJs = path.join(base, 'server.js');
  const leaderJs = path.join(base, 'leader.js');
  const portFile = path.join(base, 'server.pid-port');
  fs.writeFileSync(serverJs, serverSource);
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
  if (mode === 'now') {
    // the leader exits as soon as the server is up, so there is no stable lease to read
    await waitFor(() => fs.existsSync(portFile) && fs.readFileSync(portFile, 'utf8').includes(' '));
    const [pid, port] = fs.readFileSync(portFile, 'utf8').split(' ').map(Number);
    server = { pid, port };
    return { env, repoDir, state, lane, server, portFile };
  }
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
  return { env, repoDir, state, lane, lease, leaseId, server, portFile };
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

test('a leader that exits right after spawning a detached grandchild leaves nothing behind, even when no heartbeat saw it', async (t) => {
  // sampleMs 5000 (the default): the leader lives ~1 s, so no heartbeat ever scans the tree; only the lease marker can find the child
  const { lane, server } = await startLane(t, 'now', { sampleMs: 5000 });
  await exited(lane);
  assert.equal(alive(server.pid), false, 'the detached grandchild outlived its short-lived leader');
  assert.equal(await portFree(server.port), true, 'its port is still held');
});

test('a replacement spawned from a TERM handler is reaped too', async (t) => {
  const { env, repoDir, lane, leaseId, server, portFile } = await startLane(t, 'idle', { serverSource: FORKER, sampleMs: 5000 });
  let replacement = null;
  t.after(() => {
    if (replacement) {
      try {
        process.kill(replacement, 'SIGKILL');
      } catch {
        // already reaped, the expected case
      }
    }
  });
  const cancel = await laneRun(['cancel', leaseId], { env, cwd: repoDir });
  assert.equal(cancel.code, 0, cancel.stderr);
  await exited(lane);
  replacement = Number(fs.readFileSync(`${portFile}.replacement`, 'utf8'));
  assert.equal(alive(server.pid), false);
  assert.equal(alive(replacement), false, 'the replacement started by the TERM handler survived');
});

const row = (pid, ppid, token, marked = false) => ({ pid, ppid, pgid: 0, pcpu: 0, rss: 0, token, marked });
const fixed = (rows) => () => rows();

test('a sibling that merely shares an ancestor with the leader is never recorded', () => {
  const rows = [row(10, 1, 'a'), row(20, 10, 'b'), row(30, 10, 'c'), row(40, 20, 'd'), row(50, 1, 'e')]; // 50 is the root's sibling
  const tracker = new DescendantTracker(20, { readTable: () => rows, protectedPids: [10] });
  tracker.scan();
  assert.deepEqual([...tracker.recorded.keys()], [40]);
});

test('a process carrying the lease marker is recorded without any ancestry', () => {
  const tracker = new DescendantTracker(20, { readTable: () => [row(20, 10, 'L'), row(70, 1, 'm', true), row(71, 1, 'other')] });
  tracker.scan();
  assert.deepEqual([...tracker.recorded.keys()], [70]);
});

test('a reparented descendant is still reaped once recorded; a reused pid is not killed', async () => {
  let rows = [row(20, 10, 'L'), row(40, 20, 'd1'), row(41, 40, 'd2')];
  const tracker = new DescendantTracker(20, { readTable: fixed(() => rows) });
  tracker.scan();
  // leader gone, 40 reparented to init, 41 died and its pid was taken by an unrelated process
  rows = [row(40, 1, 'd1'), row(41, 1, 'unrelated'), row(99, 1, 'x')];
  const killed = [];
  const result = await tracker.reap({
    graceMs: 50,
    kill: (pid, sig) => {
      killed.push([pid, sig]);
      if (sig === 'SIGTERM') rows = rows.filter((r) => r.pid !== pid);
    },
    sleep: () => sleep(5),
  });
  assert.deepEqual(killed, [[40, 'SIGTERM']]);
  assert.deepEqual(result, { signalled: 1, survivors: [], complete: true });
});

test('a reused pid that is again a real descendant has its stale token replaced', async () => {
  let rows = [row(20, 10, 'L'), row(40, 20, 'old')];
  const tracker = new DescendantTracker(20, { readTable: fixed(() => rows) });
  tracker.scan();
  rows = [row(20, 10, 'L'), row(40, 20, 'new')]; // pid 40 was reused by a new child of the leader
  tracker.scan();
  assert.equal(tracker.recorded.get(40), 'new');
  const killed = [];
  await tracker.reap({ graceMs: 20, killWaitMs: 20, kill: (pid, sig) => killed.push([pid, sig]), sleep: () => sleep(5) });
  assert.deepEqual(killed, [[40, 'SIGTERM'], [40, 'SIGKILL']]);
});

test('a reused leader pid is never walked', () => {
  let rows = [row(20, 10, 'L'), row(40, 20, 'd')];
  const tracker = new DescendantTracker(20, { readTable: () => rows });
  tracker.scan();
  rows = [row(20, 10, 'someone-else'), row(60, 20, 'theirs')];
  tracker.scan();
  assert.deepEqual([...tracker.recorded.keys()], [40]);
});

test('an unreadable table is an incomplete reap, never "all gone"', async () => {
  let reads = 0;
  const tracker = new DescendantTracker(20, {
    readTable: () => {
      reads += 1;
      throw new Error('ps cannot fork');
    },
  });
  tracker.recorded.set(40, 'd');
  const result = await tracker.reap({ graceMs: 30, killWaitMs: 30, kill: () => assert.fail('nothing readable, nothing to signal'), sleep: () => sleep(5) });
  assert.deepEqual(result, { signalled: 0, survivors: [], complete: false });
  assert.ok(reads >= 3, 'each failed read is retried');
});

test('members that survive KILL are listed and the reap is incomplete', async () => {
  const rows = [row(20, 10, 'L'), row(40, 20, 'stubborn')];
  const tracker = new DescendantTracker(20, { readTable: () => rows });
  tracker.scan();
  const result = await tracker.reap({ graceMs: 20, killWaitMs: 20, kill: () => {}, sleep: () => sleep(5) });
  assert.deepEqual(result, { signalled: 1, survivors: [40], complete: false });
});

test('a snapshot naming the canceller\'s own parent is dropped and never signalled', async () => {
  const snapshot = [{ pid: process.ppid, token: 'parent' }, { pid: process.pid, token: 'me' }, { pid: 4242, token: 'member' }];
  const rows = [row(process.ppid, 1, 'parent'), row(process.pid, process.ppid, 'me'), row(4242, 1, 'member')];
  const tracker = DescendantTracker.fromSnapshot(null, snapshot, { readTable: () => rows });
  assert.deepEqual([...tracker.recorded.keys()], [4242]);
  const killed = new Set();
  await tracker.reap({ graceMs: 20, killWaitMs: 20, kill: (pid) => killed.add(pid), sleep: () => sleep(5) });
  assert.deepEqual([...killed], [4242]);
});

test('signalling itself refuses a protected pid even if one was recorded', async () => {
  const rows = [row(20, 10, 'L'), row(40, 20, 'd'), row(process.ppid, 20, 'parent')];
  const tracker = new DescendantTracker(20, { readTable: () => rows });
  tracker.recorded.set(process.ppid, 'parent');
  tracker.recorded.set(40, 'd');
  const killed = new Set();
  await tracker.reap({ graceMs: 20, killWaitMs: 20, kill: (pid) => killed.add(pid), sleep: () => sleep(5) });
  assert.deepEqual([...killed], [40]);
});

test('process table parsers read ps lstart and /proc stat starttime', () => {
  const ps = [
    '  12     1    12   0.5  1024 Mon Oct  6 12:00:00 2026 /usr/bin/node a.js',
    '  13     1    13   0.0    10 Mon Oct  6 12:00:01 2026 /bin/grep LANE_BROKER_LEASE=abc',
    '  14     1    14   0.0    10 Mon Oct  6 12:00:02 2026 /bin/sleep 5',
    'bad',
  ].join('\n');
  const envPs = [
    '  12 /usr/bin/node a.js HOME=/h LANE_BROKER_LEASE=abc LANE_BROKER_KEY=k',
    '  13 /bin/grep LANE_BROKER_LEASE=abc HOME=/h', // the marker is only an ARGUMENT here
    '  14 /bin/sleep 5',
  ].join('\n');
  assert.deepEqual(parsePsTable(ps, 'abc', envPs), [
    { pid: 12, ppid: 1, pgid: 12, pcpu: 0.5, rss: 1024, token: 'Mon Oct 6 12:00:00 2026', marked: true },
    { pid: 13, ppid: 1, pgid: 13, pcpu: 0, rss: 10, token: 'Mon Oct 6 12:00:01 2026', marked: false },
    { pid: 14, ppid: 1, pgid: 14, pcpu: 0, rss: 10, token: 'Mon Oct 6 12:00:02 2026', marked: false },
  ]);
  const stat = '77 (we ird) name)) S 5 66 77 0 -1 4194560 1 0 0 0 100 100 0 0 20 0 1 0 987654 1 25 18446744073709551615';
  assert.deepEqual(parseProcStat(77, stat, 9876.54 + 20, { pageKib: 4, clkTck: 100 }), { pid: 77, ppid: 5, pgid: 66, pcpu: 10, rss: 100, token: '987654', marked: false });
});

test('/proc units come from the injected page size and tick rate, not 4096 / 100', () => {
  const stat = '77 (x) S 5 66 77 0 -1 4194560 1 0 0 0 100 100 0 0 20 0 1 0 1000 1 25 18446744073709551615';
  // 16 KiB pages (arm64), 250 ticks: rss 25 pages = 400 KiB; cpu 200/250 s over (20 s - 4 s)
  assert.deepEqual(parseProcStat(77, stat, 20, { pageKib: 16, clkTck: 250 }), { pid: 77, ppid: 5, pgid: 66, pcpu: 5, rss: 400, token: '1000', marked: false });
});

test('only ENOENT/ESRCH mean a /proc entry is gone; any other read error is an unknown row', () => {
  const fail = (code) => ({ readFileSync: () => { throw Object.assign(new Error(code), { code }); } });
  assert.equal(readProcRow('5', undefined, 0, fail('ENOENT')), null);
  assert.equal(readProcRow('5', undefined, 0, fail('ESRCH')), null);
  assert.deepEqual(readProcRow('5', undefined, 0, fail('EACCES')), { pid: 5, ppid: -1, pgid: -1, pcpu: 0, rss: 0, token: null, marked: false, unknown: true });
  const stat = '5 (x) S 1 5 5 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 900 1 1 0';
  const noEnviron = { readFileSync: (f) => { if (f.endsWith('environ')) throw Object.assign(new Error('x'), { code: 'EACCES' }); return stat; } };
  const row = readProcRow('5', 'abc', 0, noEnviron, { pageKib: 4, clkTck: 100 });
  assert.equal(row.pid, 5);
  assert.equal(row.ppid, 1);
  assert.equal(row.marked, false);
});

test('an unknown row still counts as alive, is never signalled, and keeps the reap incomplete', async () => {
  const rows = [row(20, 10, 'L'), row(40, 20, 'd')];
  const tracker = new DescendantTracker(20, { readTable: () => rows });
  tracker.scan();
  rows[1] = { ...row(40, -1, null), unknown: true };
  assert.deepEqual(tracker.live(rows), [40]);
  const killed = [];
  const result = await tracker.reap({ graceMs: 20, killWaitMs: 20, kill: (pid) => killed.push(pid), sleep: () => sleep(5) });
  assert.deepEqual(killed, []);
  assert.deepEqual(result, { signalled: 0, survivors: [40], complete: false });
});

test('an unreadable table at leader exit counts as "members may be alive", not "none"', () => {
  const tracker = new DescendantTracker(20, { readTable: () => { throw new Error('ps cannot fork'); } });
  assert.equal(hasLiveMembers(tracker), true);
  assert.equal(hasLiveMembers(new DescendantTracker(20, { readTable: () => [row(20, 10, 'L')] })), false);
});

async function staleLeaseWithLiveMember(t) {
  const { state } = freshEnv();
  const member = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { detached: true, stdio: 'ignore' });
  t.after(() => {
    try {
      process.kill(member.pid, 'SIGKILL');
    } catch {
      // already gone
    }
  });
  const token = (await waitFor(() => readProcessTable().find((r) => r.pid === member.pid)))?.token;
  const gone = spawn('true');
  await new Promise((resolve) => gone.on('exit', resolve));
  const lease = { id: 'stale-1', bootId: bootId(), supervisorPid: gone.pid, childPgid: gone.pid, state: 'RUNNING', descendants: [{ pid: member.pid, token }] };
  writeLease(state, lease);
  return { state, member };
}

test('a stale lease heals itself: TERM on the first pass, KILL after the grace period, released on the next', async (t) => {
  const { state, member } = await staleLeaseWithLiveMember(t);
  const signals = [];
  const kill = (pid, sig) => signals.push([pid, sig]); // injected: the member is never really signalled
  const pass = (now) => reapIfStale(state, readLease(state, 'stale-1'), bootId(), { now, kill });

  assert.equal(pass(1000), 'orphaned');
  assert.deepEqual(signals, [[member.pid, 'SIGTERM']]);
  assert.equal(readLease(state, 'stale-1').orphanReap.termAt, 1000);

  assert.equal(pass(5000), 'orphaned'); // inside the grace period: no further signal
  assert.equal(signals.length, 1);

  assert.equal(pass(11_000), 'orphaned');
  assert.deepEqual(signals[1], [member.pid, 'SIGKILL']);

  process.kill(member.pid, 'SIGKILL'); // now it really dies
  await waitFor(() => !alive(member.pid));
  assert.equal(pass(12_000), 'reaped');
  assert.equal(readLease(state, 'stale-1'), null);
  assert.match(fs.readFileSync(paths(state).admissionLog, 'utf8'), /lane-broker-reap id=stale-1 descendants-reaped=2\b/);
});

test('an unreadable table does not hold a stale lease forever', async (t) => {
  const { state } = await staleLeaseWithLiveMember(t);
  const lease = readLease(state, 'stale-1');
  lease.descendants = [{ pid: 4242, token: 'x' }];
  const realPlatform = Object.getOwnPropertyDescriptor(process, 'platform');
  // an unreadable table: no `ps` and no /proc to read
  const savedPath = process.env.PATH;
  process.env.PATH = '/nonexistent';
  Object.defineProperty(process, 'platform', { value: 'darwin' });
  try {
    assert.equal(reapIfStale(state, lease, bootId(), { now: 1000 }), 'orphaned');
    assert.equal(reapIfStale(state, readLease(state, 'stale-1'), bootId(), { now: 30_000 }), 'orphaned');
    assert.equal(reapIfStale(state, readLease(state, 'stale-1'), bootId(), { now: 62_000 }), 'reaped');
  } finally {
    process.env.PATH = savedPath;
    Object.defineProperty(process, 'platform', realPlatform);
  }
  assert.equal(readLease(state, 'stale-1'), null);
  assert.match(fs.readFileSync(paths(state).admissionLog, 'utf8'), /descendants-reap-incomplete survivors=unknown/);
});

// ---- orphan state machine, driven with an injected table, clock and kill ----

function orphanHarness(t, members = [row(40, 1, 'a')]) {
  const { state } = freshEnv();
  let rows = members;
  const signals = [];
  const lease = { id: 'orph-1', bootId: bootId(), supervisorPid: 2 ** 22 - 3, childPgid: 2 ** 22 - 3, state: 'RUNNING', descendants: members.map((r) => ({ pid: r.pid, token: r.token })) };
  writeLease(state, lease);
  return {
    state,
    signals,
    setRows: (next) => { rows = next; },
    pass: (now) => reapIfStale(state, readLease(state, 'orph-1'), bootId(), { now, kill: (pid, sig) => signals.push([pid, sig]), readTable: () => rows }),
  };
}

test('a stale lease never gives up before a KILL was sent: late first KILL, release only after the bound or death', () => {
  const h = orphanHarness();
  assert.equal(h.pass(1000), 'orphaned');
  assert.equal(h.pass(62_000), 'orphaned'); // far past 60 s, but no KILL has gone out yet
  assert.deepEqual(h.signals, [[40, 'SIGTERM'], [40, 'SIGKILL']]);
  assert.ok(readLease(h.state, 'orph-1'), 'the lease is held');
  assert.equal(h.pass(80_000), 'orphaned');
  assert.equal(h.pass(92_500), 'reaped'); // 30 s after killAt, the member being unkillable
  assert.match(fs.readFileSync(paths(h.state).admissionLog, 'utf8'), /descendants-reap-incomplete survivors=40/);
});

test('a stale lease persists and signals members found on any later pass, including reparented ones', () => {
  const h = orphanHarness();
  h.pass(1000); // TERM 40
  h.setRows([row(40, 1, 'a'), row(41, 40, 'b')]); // 41 appears mid-grace as 40's child
  h.pass(5000);
  assert.deepEqual(h.signals.at(-1), [41, 'SIGTERM']);
  assert.ok(readLease(h.state, 'orph-1').descendants.some((d) => d.pid === 41), 'the discovery is persisted');
  h.setRows([row(41, 1, 'b')]); // 40 died, 41 reparented to init
  h.pass(11_000);
  assert.deepEqual(h.signals.at(-1), [41, 'SIGKILL']);
});

test('a marked replacement that appears after KILL was sent is KILLed on its own pass', () => {
  const h = orphanHarness();
  h.pass(1000);
  h.pass(11_000); // killAt
  h.setRows([row(40, 1, 'a'), row(90, 1, 'fresh', true)]);
  h.pass(12_000);
  assert.deepEqual(h.signals.at(-1), [90, 'SIGKILL']);
});
