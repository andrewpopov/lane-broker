import test, { afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { waitFor, BIN } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import { reapSpawned, setupRunner, makeHeader, execRemote, ticketPath, leaseOf, whenRunning, SLEEP } from './helpers/remote-exec-fixture.js';

/**
 * BRAIN-1035 S2a (T29): `lane drain --shutdown --wait-ms W` is one absolute deadline. Real subprocesses: it must EXIT by W plus slack
 * when a supervisor is stopped, when the broker lock is held, and when its stdout is a pipe nobody reads.
 */

const W = 3000;
const SLACK = 1500;
const stateModule = fileURLToPath(new URL('../src/state.js', import.meta.url));
const stopped = [];
const helpers = [];

afterEach(() => {
  for (const pid of stopped.splice(0)) {
    try {
      process.kill(pid, 'SIGCONT');
    } catch {
      // gone
    }
  }
  for (const child of helpers.splice(0)) child.kill('SIGKILL');
  reapSpawned();
});

/** `lane drain --shutdown` whose stdout nobody reads. Resolves `{code, elapsedMs}`, or `{timedOut: true}` after `limitMs`. */
function drainSubprocess(ctx, { env = {}, limitMs }) {
  const startedAt = Date.now();
  const child = spawn(process.execPath, [BIN, 'drain', '--shutdown', 'test shutdown', '--root', ctx.root, '--wait-ms', String(W)], { env: { ...ctx.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  child.stderr.on('data', () => {});
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      child.kill('SIGKILL');
      resolve({ timedOut: true, elapsedMs: Date.now() - startedAt });
    }, limitMs);
    child.on('exit', (code) => {
      clearTimeout(timer);
      resolve({ code, elapsedMs: Date.now() - startedAt });
    });
  });
}

test('a supervisor that is SIGSTOPped does not hold the drain past W: it exits, and the ticket stays marked shutdown', async () => {
  const ctx = setupRunner({ LANE_BROKER_TEST_CANCEL_GRACE_MS: '10000' });
  const h = makeHeader(SLEEP);
  execRemote(ctx, h);
  await whenRunning(ctx, h.ticketId);
  const { supervisorPid } = leaseOf(ctx);
  process.kill(supervisorPid, 'SIGSTOP');
  stopped.push(supervisorPid);
  const out = await drainSubprocess(ctx, { limitMs: W + 8000 });
  assert.equal(out.timedOut, undefined, `the drain outlived its deadline (${out.elapsedMs} ms)`);
  assert.ok(out.elapsedMs < W + SLACK, `exited after ${out.elapsedMs} ms, deadline ${W} ms`);
  assert.equal(out.code, 0);
  assert.equal(fs.existsSync(ticketPath(ctx, h.ticketId, 'shutdown')), true, 'unfinished work keeps its shutdown marker');
});

test('a broker lock held by another process does not hold the drain past W', async () => {
  const ctx = setupRunner();
  const ready = path.join(makeTmpDir('deadline-lock-'), 'ready');
  const holder = spawn(process.execPath, ['--input-type=module', '-e', `import { withLock } from ${JSON.stringify(stateModule)}; await withLock(${JSON.stringify(ctx.state)}, async () => { (await import('node:fs')).writeFileSync(${JSON.stringify(ready)}, 'x'); await new Promise((r) => setTimeout(r, 15000)); });`], { env: ctx.env, stdio: 'ignore' });
  helpers.push(holder);
  await waitFor(() => fs.existsSync(ready), { timeoutMs: 15_000 });
  const out = await drainSubprocess(ctx, { limitMs: W + 9000 });
  assert.equal(out.timedOut, undefined, `the drain outlived its deadline (${out.elapsedMs} ms)`);
  assert.ok(out.elapsedMs < W + SLACK, `exited after ${out.elapsedMs} ms, deadline ${W} ms`);
  assert.equal(out.code, 75, 'the documented "try again" exit');
});

test('a stdout pipe nobody reads, with 1 MiB written before the flush, does not hold the drain past W', async () => {
  const ctx = setupRunner();
  const out = await drainSubprocess(ctx, { env: { LANE_BROKER_TEST_STDOUT_FILL_BYTES: String(1 << 20) }, limitMs: W + 9000 });
  assert.equal(out.timedOut, undefined, `the flush outlived the deadline (${out.elapsedMs} ms)`);
  assert.ok(out.elapsedMs < W + SLACK, `exited after ${out.elapsedMs} ms, deadline ${W} ms`);
  assert.equal(out.code, 0);
});
