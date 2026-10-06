import test, { mock } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { startStore, fakeClock, snapshotOf, publish, sha, token, tmpDir } from './store-harness.js';
import { readJournal } from '../src/store/journal.js';
import { Replicator, readReplicationState } from '../src/store/replicate.js';
import { sweep } from '../src/store/retention.js';
import { acquireStoreLock, lockMode } from '../src/store/lock.js';

// The R3 cases exercise the macOS file lock; on Linux it refuses by design (the kernel-held lock is used instead).
const fileLockOnly = { skip: lockMode() === 'file' ? false : 'macOS file-lock fallback; Linux uses the kernel-held lock' };
import { StoreClient } from '../src/store/client.js';
import { blobRelPath, manifestRelPath } from '../src/store/ids.js';

const DAY = 24 * 3600 * 1000;
const BIN = fileURLToPath(new URL('../bin/lane-store.js', import.meta.url));
const put = (srv, text) => srv.submit.putBlob(sha(text), Buffer.from(text));
const replicator = (p, replica = p.replica.replicaPeer) => new Replicator({ store: p.primary.store, root: p.primary.root, replica, now: p.clock.now });

async function pair({ replicaGraceHours = 168 } = {}) { // explicit: these tests never depend on the production default
  const clock = fakeClock();
  const primary = await startStore({ clock });
  const replica = await startStore({ clock, replicaMode: true, replicaGraceHours });
  return { clock, primary, replica, close: () => Promise.all([primary.close(), replica.close()]) };
}

// ---- 1 + 2: deletions never replicate; the replica's retention is local and time-based ----
test('R1 the replica is never told to delete: a primary-side deletion leaves the replica copy intact', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  await put(p.primary, 'shared');
  assert.equal((await replicator(p).runOnce()).ok, true);
  assert.equal(p.primary.store.deleteObject(blobRelPath(sha('shared'))), true);
  const spy = Object.create(p.replica.replicaPeer);
  spy.deleteObject = async () => { throw new Error('delete must not be shipped'); };
  const r = await replicator(p, spy).runOnce();
  assert.equal(r.ok, true, r.error);
  assert.equal(p.replica.store.hasBlob(sha('shared')), true);
});

test('R2 a replica stall shorter than the grace deletes nothing; a terminal manifest older than the grace is swept on the replica only', async (t) => {
  const p = await pair({ replicaGraceHours: 168 });
  t.after(() => p.close());
  const snap = snapshotOf({ 'a.txt': 'replicated input' });
  await publish(p.primary, 'job-1', snap);
  assert.equal((await replicator(p).runOnce()).ok, true);
  p.clock.advance(6 * DAY);
  sweep(p.replica.store);
  assert.ok(p.replica.store.readManifest('job-1'), 'non-terminal manifests are never swept');
  await p.primary.admin.json('PUT', '/jobs/job-1/terminal');
  assert.equal((await replicator(p).runOnce()).ok, true);
  p.clock.advance(6 * DAY);
  sweep(p.replica.store);
  assert.ok(p.replica.store.readManifest('job-1'), 'terminal for 6 days, grace is 7');
  p.clock.advance(2 * DAY);
  sweep(p.replica.store);
  assert.equal(p.replica.store.readManifest('job-1'), null);
  for (const h of snap.blobs.keys()) assert.equal(p.replica.store.hasBlob(h), false);
  assert.ok(p.primary.store.readManifest('job-1'), 'the primary keeps its own 14-day retention');
});

test('R2 a pinned manifest is never swept on the replica', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  await publish(p.primary, 'job-1', snapshotOf({ 'a.txt': 'pinned input' }));
  await p.primary.admin.json('PUT', '/jobs/job-1/terminal');
  await p.primary.admin.json('PUT', '/pins/job-1');
  assert.equal((await replicator(p).runOnce()).ok, true);
  p.clock.advance(30 * DAY);
  sweep(p.replica.store);
  assert.ok(p.replica.store.readManifest('job-1'));
});

// ---- 3: lock ----
test('R3 the lock file never exists empty: it is created complete by a hard link', fileLockOnly, () => {
  const root = tmpDir('lock');
  const lockPath = path.join(root, 'store.lock');
  let sawEmpty = false;
  const real = fs.writeSync;
  const m = mock.method(fs, 'writeSync', (...a) => {
    if (fs.existsSync(lockPath) && fs.statSync(lockPath).size === 0) sawEmpty = true;
    return real(...a);
  });
  const lock = acquireStoreLock(root);
  m.mock.restore();
  assert.equal(sawEmpty, false);
  const owner = JSON.parse(fs.readFileSync(lockPath, 'utf8'));
  assert.equal(owner.pid, process.pid);
  assert.equal(typeof owner.token, 'string');
  assert.throws(() => acquireStoreLock(root), /in use/);
  fs.writeFileSync(lockPath, JSON.stringify({ pid: process.pid, token: 'someone else' }));
  lock.release();
  assert.ok(fs.existsSync(lockPath), 'a release never removes a lock it does not own');
});

test('R3 concurrent reclaimers of a stale lock: exactly one process still HOLDS it once the race settles (the rest are fenced)', fileLockOnly, async () => {
  const root = tmpDir('stale');
  const dead = spawnSync(process.execPath, ['-e', '0']).pid;
  fs.writeFileSync(path.join(root, 'store.lock'), JSON.stringify({ pid: dead, token: 'stale' }));
  // Rename-based reclaim can transiently let two processes believe they own the lock (B renames A's fresh lock aside while C
  // links a new one). The guarantee is the self-check fence: after the race settles only one passes assertHeld(), and a
  // displaced owner throws LockLostError before it can write. Children hold until the parent has heard from all of them, so a
  // slow start under machine load cannot masquerade as a second owner.
  const script = `import('${new URL('../src/store/lock.js', import.meta.url).href}').then(({acquireStoreLock})=>{let l=null;try{l=acquireStoreLock(${JSON.stringify(root)});console.log('OWNER')}catch(e){console.log('REFUSED')}let step=0;process.stdin.on('data',()=>{step+=1;if(step===1){let r='NONE';if(l){try{l.assertHeld();r='HELD'}catch(e){r=e.name}}console.log(r)}else{l?.release();process.exit(0)}})})`;
  const children = Array.from({ length: 6 }, () => {
    const c = spawn(process.execPath, ['-e', script]);
    const lines = [];
    const waiters = [];
    c.stdout.on('data', (d) => String(d).split('\n').filter(Boolean).forEach((l) => { const w = waiters.shift(); if (w) w(l); else lines.push(l); }));
    const next = () => (lines.length ? Promise.resolve(lines.shift()) : new Promise((r) => waiters.push(r)));
    return { c, next };
  });
  const verdicts = await Promise.all(children.map((x) => x.next()));
  children.forEach((x) => x.c.stdin.write('check\n'));
  const held = await Promise.all(children.map((x) => x.next()));
  children.forEach((x) => { x.c.stdin.write('release\n'); setTimeout(() => x.c.kill(), 2000).unref(); });
  assert.equal(held.filter((h) => h === 'HELD').length, 1, `verdicts ${verdicts.join(',')} held ${held.join(',')}`);
  for (const h of held) assert.ok(['HELD', 'LockLostError', 'NONE'].includes(h), h);
});

// ---- 4: cancel, and per-path exclusion ----
test('R4 registering a manifest that needs a blob with a pending delete-intent cancels it, so a later accidental loss is a loss, not a tombstone', async (t) => {
  const clock = fakeClock();
  let primary = await startStore({ clock });
  const snap = snapshotOf({ 'f.txt': 'needed again' });
  const [h, buf] = [...snap.blobs][0];
  await primary.submit.putBlob(h, buf);
  const m = mock.method(fs, 'unlinkSync', () => { throw new Error('EBUSY'); }, { times: 1 });
  assert.throws(() => primary.store.deleteObject(blobRelPath(h)), /EBUSY/);
  m.mock.restore();
  await primary.submit.putManifest('job-1', snap.manifest);
  assert.ok(readJournal(primary.root).some((r) => r.kind === 'delete-cancel' && r.path === blobRelPath(h)));
  const root = primary.root;
  await primary.close();
  fs.rmSync(path.join(root, blobRelPath(h))); // accidental loss afterwards
  primary = await startStore({ clock, root });
  t.after(() => primary.close());
  assert.deepEqual(primary.store.reconcileReport.deleted, []);
  assert.deepEqual(primary.store.reconcileReport.lost, [blobRelPath(h)]);
});

test('R4 a successful duplicate upload cancels a pending delete-intent too', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  await put(srv, 'again');
  const m = mock.method(fs, 'unlinkSync', () => { throw new Error('EBUSY'); }, { times: 1 });
  assert.throws(() => srv.store.deleteObject(blobRelPath(sha('again'))), /EBUSY/);
  m.mock.restore();
  assert.deepEqual(await put(srv, 'again'), { stored: false });
  assert.ok(readJournal(srv.root).some((r) => r.kind === 'delete-cancel'));
});

test('R4 retention cannot delete an object while rejournal is hashing it, so no put is ever journaled for an absent path', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const big = Buffer.alloc(8 * 1024 * 1024, 3);
  await srv.submit.putBlob(sha(big), big);
  const rel = blobRelPath(sha(big));
  const pending = srv.store.rejournal([rel]);
  const deleted = srv.store.deleteObject(rel); // retention fires while the async hash is in flight
  await pending;
  assert.equal(deleted, false, 'deferred, not performed');
  assert.ok(fs.existsSync(path.join(srv.root, rel)));
  const kinds = readJournal(srv.root).map((r) => r.kind);
  assert.ok(!kinds.includes('delete-intent') && !kinds.includes('delete'), kinds.join(','));
  assert.equal(srv.store.deleteObject(rel), true, 'the next sweep deletes it normally');
});

// ---- 5: directory fsync ----
test('R5 the containing directory is fsynced after the unlink and before the delete record', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const snap = snapshotOf({ 'a.txt': 'durable delete' });
  await publish(srv, 'job-1', snap);
  const events = [];
  const rUnlink = fs.unlinkSync;
  const rFsync = fs.fsyncSync;
  const mu = mock.method(fs, 'unlinkSync', (f) => { events.push('unlink'); return rUnlink(f); });
  const mf = mock.method(fs, 'fsyncSync', (fd) => { if (fs.fstatSync(fd).isDirectory()) events.push('fsync-dir'); return rFsync(fd); });
  const realAppend = srv.store.journal.append.bind(srv.store.journal);
  const ma = mock.method(srv.store.journal, 'append', (f) => { events.push(f.kind); return realAppend(f); });
  srv.store.deleteObject(manifestRelPath('job-1'));
  [mu, mf, ma].forEach((x) => x.mock.restore());
  const iUnlink = events.indexOf('unlink');
  const iDelete = events.indexOf('delete');
  assert.ok(iUnlink >= 0 && iDelete > iUnlink);
  assert.ok(events.slice(iUnlink, iDelete).includes('fsync-dir'), events.join(','));
  assert.ok(events.slice(events.indexOf('unlink'), iDelete).filter((e) => e === 'fsync-dir').length >= 2, 'manifest and its meta removal are both made durable');
});

// ---- 6: client deadline ----
test('R6 a replica that never answers fails the round within the deadline and does not wedge later rounds', async (t) => {
  const p = await pair();
  t.after(() => p.close());
  await put(p.primary, 'x');
  const silent = http.createServer(() => {});
  await new Promise((r) => silent.listen(0, '127.0.0.1', r));
  t.after(() => { silent.closeAllConnections(); silent.close(); });
  const stalled = new StoreClient({ baseUrl: `http://127.0.0.1:${silent.address().port}`, token: token({ role: 'replica' }), timeoutMs: 200 });
  const rep = replicator(p, stalled);
  const first = await Promise.race([rep.runOnce(), new Promise((r) => setTimeout(() => r('wedged'), 3000))]);
  assert.notEqual(first, 'wedged');
  assert.equal(first.ok, false);
  assert.equal(readReplicationState(p.primary.root).failedRounds, 1);
  rep.replica = p.replica.replicaPeer;
  const second = await rep.runOnce();
  assert.equal(second.ok, true, second.error);
});

// ---- 7: close drains the round ----
test('R7 close() waits for the in-flight replication round before the journal closes and the lock is released', async (t) => {
  const slow = http.createServer((req, res) => setTimeout(() => { res.writeHead(200, { 'content-type': 'application/json' }); res.end('{}'); }, 300));
  await new Promise((r) => slow.listen(0, '127.0.0.1', r));
  t.after(() => { slow.closeAllConnections(); slow.close(); });
  const clock = fakeClock();
  const primary = await startStore({
    clock,
    replicateIntervalMs: 40,
    replicateTo: new StoreClient({ baseUrl: `http://127.0.0.1:${slow.address().port}`, token: token({ role: 'replica' }) }),
  });
  await put(primary, 'in flight');
  await new Promise((r) => setTimeout(r, 120)); // the first round is now waiting on the slow replica
  const root = primary.root;
  await primary.close();
  const stateFile = path.join(root, 'replication.json');
  const atClose = fs.existsSync(stateFile) ? fs.readFileSync(stateFile, 'utf8') : null;
  await new Promise((r) => setTimeout(r, 700));
  const later = fs.existsSync(stateFile) ? fs.readFileSync(stateFile, 'utf8') : null;
  assert.equal(later, atClose, 'nothing writes the watermark after close() resolved');
  const reopened = await startStore({ clock, root });
  await reopened.close();
});

// ---- 8: recovery message ----
test('R8 the recovery command the refusal prints is a real command and recovers the store', async () => {
  const first = await startStore();
  await put(first, 'a');
  await put(first, 'b');
  const root = first.root;
  await first.close();
  fs.writeFileSync(path.join(root, 'replication.json'), JSON.stringify({ replicatedSeq: 1 }));
  let message = '';
  await startStore({ root }).catch((e) => { message = e.message; });
  const printed = /lane-store (rebuild-watermark .*)$/.exec(message);
  assert.ok(printed, message);
  const args = printed[1].replace('<last verified seq>', '1').split(' ');
  const ran = spawnSync(process.execPath, [BIN, ...args], { encoding: 'utf8' });
  assert.equal(ran.status, 0, ran.stderr);
  const again = await startStore({ root });
  await again.close();
});
