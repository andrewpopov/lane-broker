import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun, laneSpawn, waitFor, BIN } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import { gcLeaseTmpDirs, retireLeaseTmpDir, createLeaseTmpDir, tmpBase, TMP_OWNER_FILE } from '../src/gc.js';
import { writeLease } from '../src/lease.js';
import { paths, atomicWriteFile, processStartTime } from '../src/state.js';

/**
 * BRAIN-937: every local run gets its own `lb-*` TMPDIR/TMP/TEMP dir under the tmp base, retired (renamed, then a detached rm)
 * before the result is published, and a sweep collects what a dead supervisor left. Each test has its own base.
 */

const HOUR = 3_600_000;
const GREEN = [0, 0];
const lbEntries = (base) => fs.readdirSync(base).filter((n) => n.startsWith('lb-'));

/** One run's environment with its own tmp base, also installed in this process for the in-process helpers. */
function setup({ extraEnv = {}, lane = {} } = {}) {
  const base = makeTmpDir('lease-tmp-base-');
  process.env.LANE_BROKER_TMP_BASE = base;
  const f = freshEnv({ LANE_BROKER_TMP_BASE: base, TMPDIR: makeTmpDir('lease-tmp-ambient-'), ...extraEnv });
  writeGlobalConfig(f.home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(f.base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, ...lane } } });
  return { ...f, tmpBase: base, repoDir, probe: path.join(makeTmpDir('lease-tmp-probe-'), 'probe.json') };
}

/**
 * A command that records what it sees in its TMPDIR, leaves a scratch file and a file written by a detached grandchild, then does
 * `mode` ('exit:N' or 'hang'). The probe is written last, atomically, so a reader never sees half of it.
 */
function probeCommand(file, mode) {
  const script = `
    const fs = require('fs'), path = require('path'), { spawn } = require('child_process');
    const t = process.env.TMPDIR;
    fs.writeFileSync(path.join(t, 'scratch'), 'x');
    const grand = path.join(t, 'grand');
    spawn('sh', ['-c', 'echo y > "$TMPDIR/grand"; sleep 60'], { detached: true, stdio: 'ignore' }).unref();
    while (!fs.existsSync(grand));
    const marker = fs.readFileSync(path.join(t, '${TMP_OWNER_FILE}'), 'utf8');
    fs.writeFileSync(${JSON.stringify(`${file}.part`)}, JSON.stringify({ TMPDIR: t, TMP: process.env.TMP, TEMP: process.env.TEMP, mode: fs.statSync(t).mode & 0o777, marker, lease: process.env.LANE_BROKER_LEASE }));
    fs.renameSync(${JSON.stringify(`${file}.part`)}, ${JSON.stringify(file)});
    const [kind, code] = process.argv[1].split(':');
    if (kind === 'exit') process.exit(Number(code));
    setInterval(() => {}, 1000);
  `;
  return ['node', '-e', script, mode];
}

const readProbe = (file) => JSON.parse(fs.readFileSync(file, 'utf8'));
const runArgs = (cmd) => ['run', '--repo', 'r', '--lane', 'default', '--', ...cmd];

test('LEASE_TMP_ENV: the child sees TMPDIR == TMP == TEMP == a 0700 lb-* dir under the base, whose marker names this root and lease; it is gone afterwards', async () => {
  const s = setup();
  const res = await laneRun(runArgs(probeCommand(s.probe, 'exit:0')), { env: s.env, cwd: s.repoDir });
  assert.equal(res.code, 0, res.stderr);
  const seen = readProbe(s.probe);
  assert.equal(seen.TMP, seen.TMPDIR);
  assert.equal(seen.TEMP, seen.TMPDIR);
  assert.equal(path.dirname(fs.realpathSync(path.dirname(seen.TMPDIR))), path.dirname(fs.realpathSync(s.tmpBase)));
  assert.equal(path.dirname(seen.TMPDIR), s.tmpBase);
  assert.match(path.basename(seen.TMPDIR), /^lb-/);
  assert.notEqual(seen.TMPDIR, s.env.TMPDIR, 'the ambient TMPDIR is overridden');
  assert.equal(seen.mode, 0o700);
  const marker = JSON.parse(seen.marker);
  assert.equal(marker.kind, 'lease');
  assert.equal(marker.root, fs.realpathSync(s.state));
  assert.equal(marker.id, seen.lease);
  assert.ok(Number.isInteger(marker.pid) && marker.pid > 0);
  assert.equal(fs.existsSync(seen.TMPDIR), false, 'the original path is gone when lane run returns');
});

test('LEASE_TMP_CLEANUP: the original path is gone when lane run returns after a non-zero exit, a cancel and a watchdog kill, grandchild files included', async () => {
  const failing = setup();
  const res = await laneRun(runArgs(probeCommand(failing.probe, 'exit:3')), { env: failing.env, cwd: failing.repoDir });
  assert.equal(res.code, 3, res.stderr);
  assert.equal(fs.existsSync(readProbe(failing.probe).TMPDIR), false, 'non-zero exit');

  const cancelled = setup();
  const child = laneSpawn(runArgs(probeCommand(cancelled.probe, 'hang')), { env: cancelled.env, cwd: cancelled.repoDir });
  const closed = new Promise((resolve) => child.on('close', resolve));
  const seen = await waitFor(() => (fs.existsSync(cancelled.probe) ? readProbe(cancelled.probe) : null), { timeoutMs: 15_000 });
  assert.equal(fs.existsSync(seen.TMPDIR), true, 'the dir exists while the run is live');
  const cancel = await laneRun(['cancel', seen.lease], { env: cancelled.env, cwd: cancelled.repoDir });
  assert.equal(cancel.code, 0, cancel.stderr);
  assert.equal(await closed, 130);
  assert.equal(fs.existsSync(seen.TMPDIR), false, 'cancel');

  const silent = setup({ lane: { noProgressTimeoutMs: 2000 } });
  const killed = await laneRun(runArgs(probeCommand(silent.probe, 'hang')), { env: silent.env, cwd: silent.repoDir });
  assert.equal(killed.code, 124, killed.stderr);
  assert.equal(fs.existsSync(readProbe(silent.probe).TMPDIR), false, 'watchdog kill');
});

test('LEASE_TMP_PUBLISH: the result publishes without waiting for a slow rm of the retired dir', async (t) => {
  const s = setup();
  const shims = makeTmpDir('lease-tmp-shims-');
  fs.writeFileSync(path.join(shims, 'rm'), '#!/bin/sh\nsleep 4\nexec /bin/rm "$@"\n', { mode: 0o755 });
  const env = { ...s.env, PATH: `${shims}${path.delimiter}${s.env.PATH}` };
  t.after(() => waitFor(() => lbEntries(s.tmpBase).length === 0, { timeoutMs: 15_000 }).catch(() => {}));
  const res = await laneRun(runArgs(probeCommand(s.probe, 'exit:0')), { env, cwd: s.repoDir });
  assert.equal(res.code, 0, res.stderr);
  const seen = readProbe(s.probe);
  assert.equal(fs.existsSync(seen.TMPDIR), false, 'renamed away before publishing');
  const dead = lbEntries(s.tmpBase);
  assert.equal(dead.length, 1, 'the retired dir is still being removed when lane run returns, so the rm did not delay the publish');
  assert.match(dead[0], /^lb-dead-/);
  await waitFor(() => lbEntries(s.tmpBase).length === 0, { timeoutMs: 15_000 });
});

test('LEASE_TMP_SPAWN_FAILURE: a refused spawn (CPU_LIMIT_UNAVAILABLE) leaves no dir', async () => {
  const s = setup();
  writeGlobalConfig(s.home, { version: 1, cpuLimit: 'off', capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1 });
  writeRepoConfig(s.repoDir, { version: 1, lanes: { default: { weight: 1, exclusive: true, exclusivePurpose: 'upgrade', exclusiveDeadlineMs: 60_000, maxExecutionCpuCores: 6 } } });
  const res = await laneRun(runArgs(['node', '-e', '']), { env: s.env, cwd: s.repoDir });
  assert.equal(res.code, 69, res.stderr);
  await waitFor(() => lbEntries(s.tmpBase).length === 0);
});

test('LEASE_TMP_MKDTEMP_FAILURE: an unusable base still runs the command, with the inherited TMPDIR, and logs why', async () => {
  const s = setup();
  const notADir = path.join(makeTmpDir('lease-tmp-file-'), 'file');
  fs.writeFileSync(notADir, '');
  const env = { ...s.env, LANE_BROKER_TMP_BASE: notADir };
  const out = path.join(path.dirname(s.probe), 'tmpdir.txt');
  const res = await laneRun(runArgs(['node', '-e', `require('fs').writeFileSync(${JSON.stringify(out)}, process.env.TMPDIR)`]), { env, cwd: s.repoDir });
  assert.equal(res.code, 0, res.stderr);
  assert.equal(fs.readFileSync(out, 'utf8'), s.env.TMPDIR);
  assert.match(fs.readFileSync(paths(s.state).admissionLog, 'utf8'), /lease-tmp: cannot create a temp dir/);
});

test('LEASE_TMP_RUNNER: a supervisor marked as a remote-exec runner makes no second dir and leaves TMPDIR alone', async () => {
  const s = setup();
  const env = { ...s.env, LANE_BROKER_EXECUTOR: 'remote', LANE_BROKER_LOCAL_REASON: 'remote-exec runner' };
  const out = path.join(path.dirname(s.probe), 'tmpdir.txt');
  const res = await laneRun(runArgs(['node', '-e', `require('fs').writeFileSync(${JSON.stringify(out)}, process.env.TMPDIR)`]), { env, cwd: s.repoDir });
  assert.equal(res.code, 0, res.stderr);
  assert.equal(fs.readFileSync(out, 'utf8'), s.env.TMPDIR);
  assert.deepEqual(lbEntries(s.tmpBase), []);
});

test('LEASE_TMP_NESTED: a nested `lane run` sees the parent\'s dir, and the dir survives the nested exit', async () => {
  const s = setup();
  const nestedOut = path.join(path.dirname(s.probe), 'nested.json');
  const nested = `require('fs').writeFileSync(${JSON.stringify(nestedOut)}, process.env.TMPDIR)`;
  const parent = `
    const { spawnSync } = require('child_process'), fs = require('fs');
    const r = spawnSync(process.execPath, [${JSON.stringify(BIN)}, 'run', '--repo', 'r', '--lane', 'default', '--', 'node', '-e', ${JSON.stringify(nested)}], { stdio: 'inherit' });
    fs.writeFileSync(${JSON.stringify(s.probe)}, JSON.stringify({ TMPDIR: process.env.TMPDIR, status: r.status, survived: fs.existsSync(process.env.TMPDIR) }));
  `;
  const res = await laneRun(runArgs(['node', '-e', parent]), { env: s.env, cwd: s.repoDir });
  assert.equal(res.code, 0, res.stderr);
  const seen = readProbe(s.probe);
  assert.equal(seen.status, 0);
  assert.equal(seen.survived, true, 'the nested exit did not retire the parent\'s dir');
  assert.equal(fs.readFileSync(nestedOut, 'utf8'), seen.TMPDIR);
  assert.equal(fs.existsSync(seen.TMPDIR), false, 'the parent retired it at its own end');
});

test('LEASE_TMP_RETIRE_GUARDS: only a real lb-* dir under the base whose marker names this root and id is retired', () => {
  const s = setup();
  const owner = { root: s.state, id: 'lease-a', pid: process.pid, start: null };
  const mine = createLeaseTmpDir(owner);
  assert.ok(mine);

  retireLeaseTmpDir(mine, { root: s.state, id: 'lease-b' });
  assert.equal(fs.existsSync(mine), true, 'another id');
  retireLeaseTmpDir(mine, { root: makeTmpDir('lease-tmp-root-'), id: 'lease-a' });
  assert.equal(fs.existsSync(mine), true, 'another root');

  const link = path.join(s.tmpBase, 'lb-link');
  fs.symlinkSync(mine, link);
  retireLeaseTmpDir(link, { root: s.state, id: 'lease-a' });
  assert.equal(fs.existsSync(mine), true, 'a symlink');

  const outside = makeTmpDir('lease-tmp-outside-');
  const outsideLb = path.join(outside, 'lb-x');
  fs.mkdirSync(outsideLb);
  atomicWriteFile(path.join(outsideLb, TMP_OWNER_FILE), JSON.stringify({ kind: 'lease', root: fs.realpathSync(s.state), id: 'lease-a', pid: 1, start: null }));
  retireLeaseTmpDir(outsideLb, { root: s.state, id: 'lease-a' });
  assert.equal(fs.existsSync(outsideLb), true, 'outside the base');

  const remoteMarked = fs.mkdtempSync(path.join(s.tmpBase, 'lb-'));
  atomicWriteFile(path.join(remoteMarked, TMP_OWNER_FILE), 'lease-a');
  retireLeaseTmpDir(remoteMarked, { root: s.state, id: 'lease-a' });
  assert.equal(fs.existsSync(remoteMarked), true, 'a remote-format marker');

  retireLeaseTmpDir(mine, { root: s.state, id: 'lease-a' });
  assert.equal(fs.existsSync(mine), false, 'the owner retires it');
});

// ---- the orphan sweep ----

const deadPid = () => spawnSync(process.execPath, ['-e', 'process.exit(0)']).pid;

/** A lease dir as a supervisor leaves it, its marker aged `ageMs`. */
function leaseDir(base, root, { id, pid = deadPid(), start = null, ageMs = 2 * HOUR, markerRoot = fs.realpathSync(root), name } = {}) {
  const dir = fs.mkdtempSync(path.join(base, name ?? 'lb-'));
  const marker = path.join(dir, TMP_OWNER_FILE);
  atomicWriteFile(marker, JSON.stringify({ kind: 'lease', root: markerRoot, id, pid, start }));
  const when = new Date(Date.now() - ageMs);
  fs.utimesSync(marker, when, when);
  fs.writeFileSync(path.join(dir, 'scratch'), 'x');
  return dir;
}

const settle = (dirs) => waitFor(() => dirs.every((d) => !fs.existsSync(d)), { timeoutMs: 15_000 });

test('LEASE_TMP_SWEEP: a dead, 2 h-old local orphan is removed; a young one is kept', async () => {
  const s = setup();
  const orphan = leaseDir(s.tmpBase, s.state, { id: 'gone' });
  const young = leaseDir(s.tmpBase, s.state, { id: 'gone-too', ageMs: HOUR / 2 });
  const summary = await gcLeaseTmpDirs(s.state);
  assert.equal(summary.tmpDirs, 1);
  assert.equal(fs.existsSync(orphan), false);
  assert.equal(fs.existsSync(young), true);
  await settle(lbEntries(s.tmpBase).filter((n) => n.startsWith('lb-dead-')).map((n) => path.join(s.tmpBase, n)));
});

test('LEASE_TMP_SWEEP_ROOT: root B\'s sweep leaves root A\'s dirs alone', async () => {
  const a = setup();
  const b = freshEnv();
  const aDir = leaseDir(a.tmpBase, a.state, { id: 'a-orphan' });
  const summary = await gcLeaseTmpDirs(b.state);
  assert.equal(summary.tmpDirs, 0);
  assert.equal(fs.existsSync(aDir), true);
  assert.equal((await gcLeaseTmpDirs(a.state)).tmpDirs, 1, 'its own root does collect it');
});

test('LEASE_TMP_SWEEP_LIVE: a live lease, an ORPHANED lease, a live supervisor pid and a remote-format marker all keep their dir', async () => {
  const s = setup();
  const leased = leaseDir(s.tmpBase, s.state, { id: 'leased' });
  writeLease(s.state, { id: 'leased', state: 'RUNNING' });
  const orphaned = leaseDir(s.tmpBase, s.state, { id: 'orphaned' });
  writeLease(s.state, { id: 'orphaned', state: 'ORPHANED' });
  const alive = leaseDir(s.tmpBase, s.state, { id: 'no-lease-but-alive', pid: process.pid, start: processStartTime(process.pid) });
  const remote = fs.mkdtempSync(path.join(s.tmpBase, 'lb-'));
  atomicWriteFile(path.join(remote, TMP_OWNER_FILE), 'some-remote-ticket');
  const when = new Date(Date.now() - 2 * HOUR);
  fs.utimesSync(path.join(remote, TMP_OWNER_FILE), when, when);

  const summary = await gcLeaseTmpDirs(s.state);
  assert.equal(summary.tmpDirs, 0);
  assert.equal(summary.tmpSkippedLive, 3);
  for (const dir of [leased, orphaned, alive, remote]) assert.equal(fs.existsSync(dir), true, dir);
});

test('LEASE_TMP_SWEEP_UNREADABLE: an unreadable lease record aborts the sweep and removes nothing', async () => {
  const s = setup();
  const orphan = leaseDir(s.tmpBase, s.state, { id: 'gone' });
  fs.mkdirSync(paths(s.state).leases, { recursive: true });
  fs.writeFileSync(path.join(paths(s.state).leases, 'broken.json'), '{not json');
  await assert.rejects(gcLeaseTmpDirs(s.state), /broken\.json/);
  assert.equal(fs.existsSync(orphan), true);
});

test('LEASE_TMP_SWEEP_SYMLINK: a symlink named lb-* is never followed', async () => {
  const s = setup();
  const target = leaseDir(makeTmpDir('lease-tmp-elsewhere-'), s.state, { id: 'gone' });
  fs.symlinkSync(target, path.join(s.tmpBase, 'lb-link'));
  const summary = await gcLeaseTmpDirs(s.state);
  assert.equal(summary.tmpDirs, 0);
  assert.equal(fs.existsSync(path.join(target, 'scratch')), true);
});

test('LEASE_TMP_SWEEP_DEAD: an lb-dead-* dir whose marker is over an hour old is removed whatever its owner; a fresher one is kept', async () => {
  const s = setup();
  const stale = leaseDir(s.tmpBase, s.state, { id: 'x', pid: process.pid, name: 'lb-dead-' });
  const fresh = leaseDir(s.tmpBase, s.state, { id: 'y', pid: process.pid, name: 'lb-dead-', ageMs: 60_000 });
  const summary = await gcLeaseTmpDirs(s.state);
  assert.equal(summary.tmpDead, 1);
  assert.equal(fs.existsSync(fresh), true);
  await settle([stale]);
});

test('LEASE_TMP_SWEEP_DRY_RUN: reports and removes nothing', async () => {
  const s = setup();
  const orphan = leaseDir(s.tmpBase, s.state, { id: 'gone' });
  assert.equal((await gcLeaseTmpDirs(s.state, { dryRun: true })).tmpDirs, 1);
  assert.equal(fs.existsSync(orphan), true);
  assert.equal(tmpBase(), s.tmpBase);
});
