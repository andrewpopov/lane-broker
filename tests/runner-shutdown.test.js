import test, { afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig, BIN, laneRun, waitFor } from './helpers.js';
import { encodeSnapshot } from '../src/remote-stream.js';
import { paths, readJsonSafe, readRemoteDrain, writeRemoteDrain, clearRemoteDrain, RUNNER_SHUTDOWN_REASON } from '../src/state.js';
import { makeTmpDir } from './helpers/tmp.js';
import { shutdownStopReason } from '../src/remote-runner.js';

/**
 * BRAIN-350 S1 (runner side): `lane drain --shutdown` stops new remote work and stops every in-flight remote ticket with an
 * `unfinished` / `runner-shutdown` record. Real `lane remote-exec` processes against a real runner state root.
 */

const spawned = [];
afterEach(() => {
  for (const { child, env } of spawned.splice(0)) {
    if (child.exitCode !== null || child.signalCode !== null) continue;
    const leasesDir = paths(env.LANE_BROKER_STATE).leases;
    let names = [];
    try {
      names = fs.readdirSync(leasesDir);
    } catch {
      // none
    }
    for (const n of names) {
      const lease = readJsonSafe(path.join(leasesDir, n));
      for (const [pid, sig] of [[lease?.supervisorPid, 'SIGCONT'], [lease?.childPgid && -lease.childPgid, 'SIGKILL'], [lease?.supervisorPid, 'SIGKILL']]) {
        if (!pid) continue;
        try {
          process.kill(pid, sig);
        } catch {
          // gone
        }
      }
    }
    child.kill('SIGKILL');
  }
});

function setup() {
  const f = freshEnv({ LANE_BROKER_TEST_CANCEL_GRACE_MS: '1000' });
  writeGlobalConfig(f.home, {});
  return { ...f, root: makeTmpDir('shutdown-root-') };
}

function header(argv) {
  return { ticketId: crypto.randomUUID(), generation: 0, repoKey: 'shutdown-test-repo', lane: 'default', argv: [process.execPath, '-e', argv], relCwd: '' };
}

function exec(ctx, h) {
  const dir = makeTmpDir('shutdown-src-');
  const buf = Buffer.from('x');
  fs.writeFileSync(path.join(dir, 'a.txt'), buf);
  const entries = [{ path: 'a.txt', type: 'file', exec: false, size: 1, sha256: crypto.createHash('sha256').update(buf).digest('hex') }];
  const child = spawn(process.execPath, [BIN, 'remote-exec', '--root', ctx.root], { env: ctx.env });
  child.stdout.on('data', () => {});
  child.stderr.on('data', () => {});
  encodeSnapshot(dir, h, entries).pipe(child.stdin);
  spawned.push({ child, env: ctx.env });
  const closed = new Promise((resolve) => child.on('close', resolve));
  return { child, closed };
}

const resultFile = (ctx, h) => path.join(ctx.root, 'tickets', h.ticketId, 'result.json');
const ticketFile = (ctx, h, name) => path.join(ctx.root, 'tickets', h.ticketId, name);
const running = (ctx, h) => waitFor(() => fs.existsSync(ticketFile(ctx, h, 'remote-id')) && fs.readdirSync(paths(ctx.state).leases).some((n) => n.endsWith('.json')), { timeoutMs: 30_000 });
const shutdown = (ctx, extra = []) => laneRun(['drain', '--shutdown', 'test shutdown', '--root', ctx.root, '--wait-ms', '30000', ...extra], { env: ctx.env });
const SLEEP = 'setInterval(() => {}, 1000)';

test('a shutdown drain is reported as draining and refuses dispatch in the probe like a drain', async () => {
  const ctx = setup();
  const { code } = await shutdown(ctx);
  assert.equal(code, 0);
  const probe = JSON.parse((await laneRun(['remote-probe', '--root', ctx.root], { env: ctx.env })).stdout.trim());
  assert.equal(probe.draining, true);
  assert.equal(probe.remoteDrain.reason, 'test shutdown');
  assert.equal(readRemoteDrain(ctx.state).shutdown, true);
});

test('a RUNNING remote ticket ends unfinished/runner-shutdown bound to its header, not cancelled', async () => {
  const ctx = setup();
  const h = header(SLEEP);
  const { closed } = exec(ctx, h);
  await running(ctx, h);
  const out = await shutdown(ctx);
  assert.equal(out.code, 0, out.stderr);
  assert.match(out.stdout, /stopped 1, published 0, still pending 0|published 1, still pending 0/);
  await closed;
  const result = readJsonSafe(resultFile(ctx, h));
  assert.equal(result.kind, 'unfinished');
  assert.equal(result.reason, RUNNER_SHUTDOWN_REASON);
  assert.equal(result.ticketId, h.ticketId);
  assert.equal(fs.existsSync(ticketFile(ctx, h, 'cancelled')), false, 'no user-cancel marker');
  assert.equal(fs.existsSync(ticketFile(ctx, h, 'shutdown')), true);
});

test('an exec that arrives after the shutdown drain never starts its command and gets the same record', async () => {
  const ctx = setup();
  assert.equal((await shutdown(ctx)).code, 0);
  const marker = path.join(makeTmpDir('shutdown-mark-'), 'ran');
  const h = header(`require('fs').writeFileSync(${JSON.stringify(marker)}, '1')`);
  const { closed } = exec(ctx, h);
  await closed;
  const result = readJsonSafe(resultFile(ctx, h));
  assert.equal(result.kind, 'unfinished');
  assert.equal(result.reason, RUNNER_SHUTDOWN_REASON);
  assert.equal(fs.existsSync(marker), false);
});

test('a ticket that completed before the shutdown keeps completed; one that finishes on its own despite the marker does too', async () => {
  const ctx = setup();
  const done = header('process.exit(0)');
  await exec(ctx, done).closed;
  assert.equal((await shutdown(ctx)).code, 0);
  assert.equal(readJsonSafe(resultFile(ctx, done)).kind, 'completed');
  assert.equal(fs.existsSync(ticketFile(ctx, done, 'shutdown')), false, 'a published ticket is not marked');

  const ctx2 = setup();
  const own = header('setTimeout(() => {}, 1500)');
  const { closed } = exec(ctx2, own);
  await running(ctx2, own);
  fs.writeFileSync(ticketFile(ctx2, own, 'shutdown'), '1');
  await closed;
  assert.equal(readJsonSafe(resultFile(ctx2, own)).kind, 'completed');
});

test('a user cancel still wins over a shutdown marker', async () => {
  const ctx = setup();
  const h = header(SLEEP);
  const { closed } = exec(ctx, h);
  await running(ctx, h);
  fs.writeFileSync(ticketFile(ctx, h, 'shutdown'), '1');
  await laneRun(['remote-cancel', h.ticketId, '--root', ctx.root], { env: ctx.env });
  await closed;
  assert.equal(readJsonSafe(resultFile(ctx, h)).kind, 'cancelled');
});

test('a shutdown marker from a previous boot reads as not draining; the current boot reads as draining', async () => {
  const ctx = setup();
  const prev = process.env.LANE_BROKER_BOOT_ID;
  try {
    process.env.LANE_BROKER_BOOT_ID = 'boot-1';
    const marker = await writeRemoteDrain(ctx.state, 'going down', { shutdown: true });
    assert.equal(marker.shutdown, true);
    assert.equal(readRemoteDrain(ctx.state).shutdown, true);
    process.env.LANE_BROKER_BOOT_ID = 'boot-2';
    assert.equal(readRemoteDrain(ctx.state), null);
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_BOOT_ID;
    else process.env.LANE_BROKER_BOOT_ID = prev;
  }
});

test('plain drain is not a shutdown drain, survives a boot change, and undrain clears either kind', async () => {
  const ctx = setup();
  const prev = process.env.LANE_BROKER_BOOT_ID;
  try {
    process.env.LANE_BROKER_BOOT_ID = 'boot-1';
    await writeRemoteDrain(ctx.state, 'maint');
    assert.equal(readRemoteDrain(ctx.state).shutdown, undefined);
    process.env.LANE_BROKER_BOOT_ID = 'boot-2';
    assert.equal(readRemoteDrain(ctx.state).reason, 'maint');
    await writeRemoteDrain(ctx.state, 'going down', { shutdown: true });
    assert.equal(await clearRemoteDrain(ctx.state), true);
    assert.equal(readRemoteDrain(ctx.state), null);
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_BOOT_ID;
    else process.env.LANE_BROKER_BOOT_ID = prev;
  }
});

test('shutdownStopReason: only a proven stop yields runner-shutdown (Codex P1s on BRAIN-350)', () => {
  const base = { cancelled: false, shutdownMarked: true, refusedAtStart: false };
  const proven = 'runner-shutdown';
  const unproven = 'runner-shutdown-unproven';
  assert.equal(shutdownStopReason({ ...base, refusedAtStart: true, structured: null }), proven, 'refused before anything ran');
  assert.equal(shutdownStopReason({ ...base, unstarted: true, structured: null }), proven, 'found unleased by the sweep: dequeued or fenced');
  assert.equal(shutdownStopReason({ ...base, structured: { id: 'x', exit: null, signal: 'SIGKILL', startedAt: null, cancelled: true } }), unproven, 'an orphan-lease kill result is not proof');
  assert.equal(shutdownStopReason({ ...base, structured: { cancelled: true, startedAt: 1, exit: 130, stopProven: true } }), proven, 'cancel ended it, reap complete');
  assert.equal(shutdownStopReason({ ...base, structured: null }), unproven, 'supervisor died with no result: the child may live');
  assert.equal(shutdownStopReason({ ...base, structured: { cancelled: true, startedAt: 1, exit: 130 } }), unproven, 'incomplete reap, or the command had already exited');
  assert.equal(shutdownStopReason({ ...base, structured: { cancelled: false, startedAt: 1, exit: 0 } }), null, 'completed on its own keeps its record');
  assert.equal(shutdownStopReason({ ...base, cancelled: true, refusedAtStart: true, structured: null }), null, 'a user cancel wins');
  assert.equal(shutdownStopReason({ ...base, shutdownMarked: false, unstarted: true, structured: null }), null, 'no shutdown, no say');
});
