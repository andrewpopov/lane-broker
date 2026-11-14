import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, waitFor } from './helpers.js';
import { enqueue, reapOrphanedQueue } from '../src/scheduler.js';
import { writeLease, isPidAlive } from '../src/lease.js';
import { paths, readJsonSafe, withLock, bootId, processStartTime } from '../src/state.js';

const liveLease = (id, key) => ({ id, key, state: 'RUNNING', bootId: bootId(), supervisorPid: process.pid, supervisorStart: processStartTime(process.pid) ?? null, startedAt: Date.now() });

/**
 * BRAIN-1035 S2a (T39): `neverStarted` on a persisted result is explicit pre-spawn provenance. This audits how persisted results
 * are built (which function carries the flag, in which kind of object) and checks the two broker writers' lease guard by behaviour.
 */

const SRC = fileURLToPath(new URL('../src/', import.meta.url));

/** file -> functions in which the identifier may appear, by what they build */
const ALLOWED = {
  // broker structured results (`results/<id>.json`): the only two writers that finalize a queued, never-leased ticket
  'scheduler.js': ['reapOrphanedQueue'],
  'supervisor.js': ['finalizeQueuedAndExit', /* dispatch-control, never persisted as a result (the pull hooks nest in runRemoteAttempt): */ 'abandonRebind', 'fallbackOrRefuse', 'runRemoteAttempt', 'dispatchViaPull', 'prepareFailed'],
  // remote-exec's own result (`tickets/<id>/result.json`) and its classification
  'remote-runner.js': ['buildResult', 'remoteExecCommand', 'shutdownStopReason'],
  // the client's dispatch outcome objects
  'remote-client.js': null,
};

/** Every non-comment line naming `neverStarted`, with the nearest preceding function declaration. */
function occurrences() {
  const found = [];
  for (const file of fs.readdirSync(SRC).filter((n) => n.endsWith('.js'))) {
    let fn = null;
    fs.readFileSync(path.join(SRC, file), 'utf8').split('\n').forEach((text, i) => {
      const decl = /^\s*(?:export\s+)?(?:async\s+)?function\s+(\w+)/.exec(text);
      if (decl) fn = decl[1];
      if (/\bneverStarted\b/.test(text) && !/^\s*(\/\/|\*|\/\*)/.test(text)) found.push({ file, line: i + 1, fn, text: text.trim() });
    });
  }
  return found;
}

test('neverStarted appears in src/ only in the persisted-result builders and dispatch-control code the audit names', () => {
  const stray = occurrences().filter((o) => {
    if (!(o.file in ALLOWED)) return true;
    return ALLOWED[o.file] !== null && !ALLOWED[o.file].includes(o.fn);
  });
  assert.deepEqual(stray.map((o) => `${o.file}:${o.line} (${o.fn}) ${o.text}`), [], 'a result writer outside the audited set carries neverStarted');
});

test('the dispatch-control uses in supervisor.js never sit on a line that writes a persisted result', () => {
  const writes = occurrences().filter((o) => o.file === 'supervisor.js' && o.fn !== 'finalizeQueuedAndExit' && /atomicWriteJson|resultPath|publish/.test(o.text));
  assert.deepEqual(writes, []);
});

test('the cancel, reconcile and pending result writers never name neverStarted: they can follow admission', () => {
  const names = occurrences().map((o) => o.file);
  for (const file of ['cancel.js', 'local-reconcile.js', 'remote-reconcile.js', 'pending.js', 'attempts.js']) assert.equal(names.includes(file), false, file);
});

function queuedTicket(base, repoDir, id, supervisorPid) {
  return {
    id,
    key: 'k',
    conflicts: [],
    weight: 1,
    supervisorPid,
    supervisorStart: null,
    cwd: repoDir,
    cmd: ['true'],
    logPath: path.join(base, `${id}.log`),
    resultPath: path.join(base, `${id}.json`),
    createdAt: Date.now(),
  };
}

test('reapOrphanedQueue sets neverStarted only while no lease exists under the lock', async () => {
  const { base, home, state } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, sampleMs: 100 });
  const dead = spawnSync(process.execPath, ['-e', 'process.stdout.write(String(process.pid))']).stdout.toString();
  const repoDir = path.join(base, 'repo');
  fs.mkdirSync(repoDir, { recursive: true });
  const [plain, leased] = [crypto.randomUUID(), crypto.randomUUID()];
  for (const id of [plain, leased]) await enqueue(state, queuedTicket(base, repoDir, id, Number(dead)));
  writeLease(state, liveLease(leased, 'k'));
  await withLock(state, () => reapOrphanedQueue(state));
  assert.equal(readJsonSafe(path.join(base, `${plain}.json`)).neverStarted, true);
  const withLease = readJsonSafe(path.join(base, `${leased}.json`));
  assert.ok(withLease, 'still published');
  assert.equal('neverStarted' in withLease, false);
});

test('finalizeQueuedAndExit (a runner-side ticket cancelled while queued) sets neverStarted only while no lease exists under the lock', async () => {
  const run = async (withLease) => {
    const { base, home, state, env } = freshEnv({ LANE_BROKER_EXECUTOR: 'remote', LANE_BROKER_LOCAL_REASON: 'remote-exec runner' });
    writeGlobalConfig(home, { schedulerMode: 'shadow', sampleMs: 50 });
    fs.writeFileSync(path.join(state, 'PAUSE'), 'kept busy for the test');
    const repoDir = path.join(base, 'repo');
    writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
    const child = spawn(process.execPath, [fileURLToPath(new URL('../bin/lane.js', import.meta.url)), 'run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'true'], { env, cwd: repoDir });
    let out = '';
    child.stdout.on('data', (d) => {
      out += d;
    });
    await new Promise((resolve) => child.on('exit', resolve));
    const id = out.trim();
    await waitFor(() => fs.existsSync(paths(state).queue) && fs.readdirSync(paths(state).queue).some((n) => n.includes(id)), { timeoutMs: 30_000 });
    const queued = readJsonSafe(path.join(paths(state).queue, fs.readdirSync(paths(state).queue).find((n) => n.includes(id))));
    if (withLease) writeLease(state, liveLease(id, queued.key));
    assert.equal((await laneRun(['cancel', id], { env, cwd: repoDir })).code, 0);
    await waitFor(() => !isPidAlive(queued.supervisorPid), { timeoutMs: 30_000 });
    return readJsonSafe(path.join(paths(state).results, `${id}.json`));
  };
  const plain = await run(false);
  assert.ok(plain, 'a runner-side cancelled queued ticket publishes a result');
  assert.equal(plain.cancelled, true);
  assert.equal(plain.neverStarted, true);
  const leased = await run(true);
  assert.ok(leased);
  assert.equal('neverStarted' in leased, false);
});
