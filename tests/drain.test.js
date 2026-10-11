import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, laneSpawn, waitFor } from './helpers.js';
import { setup as remoteSetup, tmpDir, markerCmd, makeRunner, detachAndWait, resultOf } from './remote-harness.js';
import { paths, readRemoteDrain, writeRemoteDrain, clearRemoteDrain, sanitizeDrainReason } from '../src/state.js';
import { selectRunner } from '../src/remote-client.js';
import { CAPABILITIES } from '../src/capabilities.js';

/**
 * BRAIN-520 slice 1: `lane drain` / `lane undrain`. Every test uses a temporary state root; none touches ~/.cache/lane-broker.
 */

const UNREADABLE = { reason: 'drain marker unreadable', since: null, unreadable: true };
const writeRaw = (state, text) => fs.writeFileSync(paths(state).remoteDrain, text);

// ---- the marker ----

test('sanitizeDrainReason strips ANSI and control characters, collapses whitespace and caps at 200 chars', () => {
  assert.equal(sanitizeDrainReason('\u001b[31mred\u001b[0m\u0007 and\n\tbold'), 'red and bold');
  assert.equal(sanitizeDrainReason('\u001b]0;title\u0007hi'), 'hi');
  assert.equal(sanitizeDrainReason('x'.repeat(500)).length, 200);
  assert.equal(sanitizeDrainReason(undefined), '');
});

test('readRemoteDrain: no file is not draining; a written marker round-trips with version, reason, since and host', async () => {
  const { state } = freshEnv();
  assert.equal(readRemoteDrain(state), null);
  const before = Date.now();
  const m = await writeRemoteDrain(state, 'disk swap');
  assert.equal(m.reason, 'disk swap');
  assert.ok(m.since >= before);
  assert.deepEqual(readRemoteDrain(state), { reason: 'disk swap', since: m.since, unreadable: false });
  const raw = JSON.parse(fs.readFileSync(paths(state).remoteDrain, 'utf8'));
  assert.equal(raw.version, 1);
  assert.equal(typeof raw.host, 'string');
  assert.equal(raw.pid, undefined, 'operator-owned: no pid');
});

test('a drain with no reason gets a default reason', async () => {
  const { state } = freshEnv();
  assert.equal((await writeRemoteDrain(state, '')).reason, 'drained by operator');
});

for (const [name, content] of [
  ['unparseable', '{not json'],
  ['empty file', ''],
  ['JSON null', 'null'],
  ['an array', '[]'],
  ['a string', '"draining"'],
  ['wrong version', JSON.stringify({ version: 2, reason: 'x', since: 1 })],
  ['missing version', JSON.stringify({ reason: 'x', since: 1 })],
  ['non-string reason', JSON.stringify({ version: 1, reason: 5, since: 1 })],
  ['missing since', JSON.stringify({ version: 1, reason: 'x' })],
  ['string since', JSON.stringify({ version: 1, reason: 'x', since: 'yesterday' })],
  ['null since', JSON.stringify({ version: 1, reason: 'x', since: null })],
]) {
  test(`malformed drain marker (${name}) fails closed: draining, reason "drain marker unreadable", since unknown`, () => {
    const { state } = freshEnv();
    writeRaw(state, content);
    assert.deepEqual(readRemoteDrain(state), UNREADABLE);
  });
}

test('a marker that is a directory (unreadable) fails closed', () => {
  const { state } = freshEnv();
  fs.mkdirSync(paths(state).remoteDrain);
  assert.deepEqual(readRemoteDrain(state), UNREADABLE);
});

test('re-drain keeps the original since and updates the reason; re-draining an unreadable marker restarts since', async () => {
  const { state } = freshEnv();
  const first = await writeRemoteDrain(state, 'one');
  await new Promise((r) => setTimeout(r, 15));
  const second = await writeRemoteDrain(state, 'two');
  assert.equal(second.since, first.since);
  assert.deepEqual(readRemoteDrain(state), { reason: 'two', since: first.since, unreadable: false });
  writeRaw(state, 'garbage');
  const third = await writeRemoteDrain(state, 'three');
  assert.ok(Number.isFinite(third.since));
  assert.equal(readRemoteDrain(state).reason, 'three');
});

test('clearRemoteDrain removes the marker, is idempotent, and clears an unreadable one', async () => {
  const { state } = freshEnv();
  assert.equal(await clearRemoteDrain(state), false);
  await writeRemoteDrain(state, 'x');
  assert.equal(await clearRemoteDrain(state), true);
  assert.equal(readRemoteDrain(state), null);
  writeRaw(state, 'garbage');
  await clearRemoteDrain(state);
  assert.equal(fs.existsSync(paths(state).remoteDrain), false);
});

test('lane undrain exits 1 naming the path when the marker cannot be removed', async () => {
  const { state, env } = freshEnv();
  fs.mkdirSync(paths(state).remoteDrain);
  fs.writeFileSync(path.join(paths(state).remoteDrain, 'child'), 'x'); // unlink of a directory fails (EPERM/EISDIR)
  const r = await laneRun(['undrain'], { env });
  assert.equal(r.code, 1, r.stdout + r.stderr);
  assert.ok(r.stderr.includes(paths(state).remoteDrain), r.stderr);
});

test('lane drain --bogus is an unknown option: exit 2 and no drain marker is written', async () => {
  const { state, env } = freshEnv();
  const r = await laneRun(['drain', '--bogus'], { env });
  assert.equal(r.code, 2, r.stdout + r.stderr);
  assert.match(r.stderr, /unknown option "--bogus"/);
  assert.equal(readRemoteDrain(state), null);
});

test('the capability list advertises remote-drain/1', () => {
  assert.ok(CAPABILITIES.includes('remote-drain/1'));
});

// ---- CLI ----

test('lane drain / undrain: exit codes, output, counts, bad arguments exit 2, the migration marker is untouched', async () => {
  const { state, env } = freshEnv();
  const d = await laneRun(['drain', 'disk swap'], { env });
  assert.equal(d.code, 0, d.stderr);
  assert.match(d.stdout, /drain: draining \(disk swap\) since \d{4}-\d\d-\d\dT.*; remaining here: 0 running, 0 queued/);
  assert.equal(readRemoteDrain(state).reason, 'disk swap');
  assert.equal(fs.existsSync(paths(state).draining), false, "the migrate-scheduler marker is not this feature's");

  assert.equal((await laneRun(['drain', 'a', 'b'], { env })).code, 2);
  assert.equal((await laneRun(['undrain', 'extra'], { env })).code, 2);
  assert.equal(readRemoteDrain(state).reason, 'disk swap', 'bad arguments change nothing');

  const u = await laneRun(['undrain'], { env });
  assert.equal(u.code, 0, u.stderr);
  assert.equal(readRemoteDrain(state), null);
  assert.equal((await laneRun(['undrain'], { env })).code, 0, 'idempotent');
});

test('resume does not clear a drain and says so; undrain does not clear a pause and says so', async () => {
  const { state, env } = freshEnv();
  await laneRun(['pause', 'maintenance'], { env });
  await laneRun(['drain', 'disk swap'], { env });

  const undrain = await laneRun(['undrain'], { env });
  assert.match(undrain.stdout, /still paused: maintenance; lane resume/);
  assert.equal(fs.existsSync(paths(state).pause), true, 'undrain left the pause');

  await laneRun(['drain', 'disk swap'], { env });
  const resume = await laneRun(['resume'], { env });
  assert.match(resume.stdout, /still draining: disk swap; lane undrain/);
  assert.equal(readRemoteDrain(state).reason, 'disk swap', 'resume left the drain');
  assert.equal(fs.existsSync(paths(state).pause), false);

  await laneRun(['undrain'], { env });
  assert.doesNotMatch((await laneRun(['resume'], { env })).stdout, /still/);
  assert.doesNotMatch((await laneRun(['undrain'], { env })).stdout, /still/);
});

test('lane status renders the drain line and --json carries remoteDrain without disturbing `draining`', async () => {
  const { env } = freshEnv();
  const off = await laneRun(['status'], { env });
  assert.match(off.stdout, /pause: not paused\ndrain: not draining\n/);
  const offJson = JSON.parse((await laneRun(['status', '--json'], { env })).stdout);
  assert.equal(offJson.remoteDrain, null);

  await laneRun(['drain', 'disk swap'], { env });
  const on = await laneRun(['status'], { env });
  assert.match(on.stdout, /pause: not paused\ndrain: draining \(disk swap\) since \d{4}-.*Z; remaining here: 0 running, 0 queued\n/);
  const onJson = JSON.parse((await laneRun(['status', '--json'], { env })).stdout);
  assert.equal(onJson.remoteDrain.reason, 'disk swap');
  assert.equal(onJson.draining, null, 'the migrate-scheduler key is unchanged');
});

test('lane status on an unreadable marker says since unknown', async () => {
  const { state, env } = freshEnv();
  writeRaw(state, '[]');
  const r = await laneRun(['status'], { env });
  assert.match(r.stdout, /drain: draining \(drain marker unreadable\) since unknown; remaining here: 0 running, 0 queued/);
});

// ---- runner side: probe and result ----

test('remote-probe: remoteDrain, draining and paused (compat bit) are set only while drained', async () => {
  const { state, env } = freshEnv();
  const off = JSON.parse((await laneRun(['remote-probe'], { env })).stdout);
  assert.equal(off.remoteDrain, undefined);
  assert.equal(off.draining, false);
  assert.equal(off.paused, false);

  const m = await writeRemoteDrain(state, 'disk swap');
  const on = JSON.parse((await laneRun(['remote-probe'], { env })).stdout);
  assert.deepEqual(on.remoteDrain, { reason: 'disk swap', since: m.since });
  assert.equal(on.draining, true);
  assert.equal(on.paused, true);
  assert.ok(on.capabilities.includes('remote-drain/1'));

  await clearRemoteDrain(state);
  assert.equal(JSON.parse((await laneRun(['remote-probe'], { env })).stdout).draining, false);
});

test('remote-probe on an unreadable marker reports the sentinel reason', async () => {
  const { state, env } = freshEnv();
  writeRaw(state, 'garbage');
  const p = JSON.parse((await laneRun(['remote-probe'], { env })).stdout);
  assert.deepEqual(p.remoteDrain, { reason: 'drain marker unreadable', since: null });
  assert.equal(p.draining, true);
});

test('remote-result: the missing-record reply carries remoteDrain only while draining', async () => {
  const { state, env } = freshEnv();
  const id = crypto.randomUUID();
  const off = JSON.parse((await laneRun(['remote-result', id], { env })).stdout);
  assert.equal(off.missing, true);
  assert.equal(off.remoteDrain, undefined);
  const m = await writeRemoteDrain(state, 'disk swap');
  const on = JSON.parse((await laneRun(['remote-result', id], { env })).stdout);
  assert.equal(on.missing, true);
  assert.deepEqual(on.remoteDrain, { reason: 'disk swap', since: m.since });
});

// ---- submitter side: selectRunner ----

function fakeProbeSsh(probe) {
  const binDir = tmpDir('drain-ssh');
  const sshBin = path.join(binDir, 'ssh');
  fs.writeFileSync(sshBin, `#!/usr/bin/env node\nprocess.stdout.write(${JSON.stringify(`${JSON.stringify(probe)}\n`)});\n`);
  fs.chmodSync(sshBin, 0o755);
  return sshBin;
}
const baseProbe = { protocol: 1, protocols: [1, 2], version: '0.0.0', draining: false, paused: false, queued: 0, running: 0, capacity: {} };
const select = (probe) => selectRunner([makeRunner({ ssh: 'a' })], { sshBin: fakeProbeSsh(probe), deadlineMs: 3000, maxRemoteQueue: 2 });

test('selectRunner skips a drained runner naming the reason, and sanitizes the reason it was sent', async () => {
  const res = await select({ ...baseProbe, draining: true, paused: true, remoteDrain: { reason: 'maint', since: 1 } });
  assert.equal(res.runner, null);
  assert.deepEqual(res.skipped.map((s) => s.reason), ['draining: maint']);
  const dirty = await select({ ...baseProbe, draining: true, paused: true, remoteDrain: { reason: '\u001b[31mmaint\u001b[0m\nnow', since: 1 } });
  assert.deepEqual(dirty.skipped.map((s) => s.reason), ['draining: maint now']);
});

test('selectRunner still skips a paused-only probe (an old runner or client) and selects an undrained idle one', async () => {
  const paused = await select({ ...baseProbe, paused: true });
  assert.deepEqual(paused.skipped.map((s) => s.reason), ['paused']);
  const ok = await select(baseProbe);
  assert.equal(ok.runner?.name, 'a');
});

test('selectRunner: a probe carrying only draining:true (the migration kind) keeps the plain "draining" reason', async () => {
  assert.deepEqual((await select({ ...baseProbe, draining: true, paused: true })).skipped.map((s) => s.reason), ['draining']);
});

// ---- end to end: a REAL marker on the runner's state root, the real probe, a fresh dispatch ----

test('end to end: a real drain marker makes a fresh dispatch skip the runner (falls back local), and undrain restores it', async () => {
  const { env, state, repoDir } = remoteSetup();
  await writeRemoteDrain(state, 'maint');
  const marker = path.join(tmpDir('marker'), 'where');
  const drained = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker, 0)], env, repoDir);
  assert.equal(drained.waited.code, 0, drained.waited.stderr);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'local', 'never dispatched to the drained runner');
  assert.match(resultOf(state, drained.id).fallbackReason, /draining: maint/);

  await clearRemoteDrain(state);
  const marker2 = path.join(tmpDir('marker'), 'where');
  const back = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(marker2, 0)], env, repoDir);
  assert.equal(back.waited.code, 0, back.waited.stderr);
  assert.equal(fs.readFileSync(marker2, 'utf8'), 'remote', 'dispatched once the drain was lifted');
});

// ---- a drained host keeps admitting its own work ----

test('a drained host still admits a queued local ticket and a running lease completes with its normal exit code', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 1, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  const gate = path.join(base, 'gate');
  const ran = path.join(base, 'ran');
  const holder = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'sh', '-c', `while [ ! -f ${gate} ]; do sleep 0.05; done; exit 7`], { env, cwd: repoDir });
  const holderClosed = new Promise((resolve) => holder.on('close', resolve));
  await waitFor(() => fs.existsSync(paths(state).leases) && fs.readdirSync(paths(state).leases).length === 1, { timeoutMs: 30_000 });
  const queued = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', 'touch', ran], { env, cwd: repoDir });
  const queuedClosed = new Promise((resolve) => queued.on('close', resolve));
  await waitFor(() => fs.readdirSync(paths(state).queue).length === 1, { timeoutMs: 30_000 });

  const d = await laneRun(['drain', 'maint'], { env });
  assert.match(d.stdout, /remaining here: 1 running, 1 queued/);
  assert.equal(fs.existsSync(ran), false);
  fs.writeFileSync(gate, '');
  assert.equal(await holderClosed, 7, 'the running lease kept its own exit code');
  const queuedCode = await Promise.race([queuedClosed, new Promise((r) => setTimeout(() => r('not admitted within 30s'), 30_000))]);
  if (queuedCode !== 0) queued.kill('SIGTERM');
  assert.equal(queuedCode, 0, 'the queued ticket was admitted and completed while drained');
  assert.equal(fs.existsSync(ran), true);
  const fresh = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', 'true'], { env, cwd: repoDir });
  assert.equal(fresh.code, 0, 'a new local run is admitted too');
  assert.equal(readRemoteDrain(state).reason, 'maint');
});
