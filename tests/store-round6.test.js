import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { startStore, snapshotOf, publish, sha, tmpDir } from './store-harness.js';
import { lockMode } from '../src/store/lock.js';
import { ObjectStore } from '../src/store/objects.js';
import { StoreHttpError } from '../src/store/client.js';
import { blobRelPath } from '../src/store/ids.js';

const onLinux = process.platform === 'linux';
const LINUX_ONLY = onLinux ? false : 'abstract Unix sockets are Linux-only: this runs on the Linux hosts (wintop), not macOS';

test('the lock is selected by platform: kernel-held abstract socket on Linux, the best-effort file lock elsewhere', () => {
  assert.equal(lockMode('linux'), 'abstract-socket');
  assert.equal(lockMode('darwin'), 'file');
  assert.equal(lockMode('win32'), 'file');
});

test('N11 (Linux) a second store on the same root is refused while the first lives; after SIGKILL a new store acquires at once', { skip: LINUX_ONLY }, async () => {
  const root = tmpDir('kernel-lock');
  const storeUrl = new URL('../src/store/objects.js', import.meta.url).href;
  const child = spawn(process.execPath, ['-e', `import('${storeUrl}').then(async({ObjectStore})=>{await ObjectStore.open(${JSON.stringify(root)});console.log('UP');setInterval(()=>{},1000)})`]);
  await new Promise((resolve, reject) => {
    child.stdout.once('data', resolve);
    child.once('exit', (c) => reject(new Error(`child exited ${c}`)));
  });
  await assert.rejects(ObjectStore.open(root), /in use/);
  child.kill('SIGKILL');
  await new Promise((r) => child.once('exit', r));
  const reopened = await ObjectStore.open(root); // no stale state, no reclaim: the kernel released the name
  await reopened.close();
});

test('N11 (Linux) the held lock leaves no lock file behind and release frees it for a re-open', { skip: LINUX_ONLY }, async () => {
  const root = tmpDir('kernel-lock2');
  const a = await ObjectStore.open(root);
  assert.equal(fs.existsSync(path.join(root, 'store.lock')), false);
  await a.close();
  await (await ObjectStore.open(root)).close();
});

test('N20 a replica-role DELETE /objects is 403; an admin DELETE of a blob a non-terminal or pinned manifest references is 409', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const snap = snapshotOf({ 'a.txt': 'referenced input' });
  const [h] = snap.blobs.keys();
  await publish(srv, 'job-1', snap);
  const rel = blobRelPath(h);
  await assert.rejects(srv.replicaPeer.deleteObject(rel), (e) => e instanceof StoreHttpError && e.status === 403);
  await assert.rejects(srv.admin.deleteObject(rel), (e) => e instanceof StoreHttpError && e.status === 409);
  assert.equal(srv.store.hasBlob(h), true);
  await srv.admin.json('PUT', '/jobs/job-1/terminal');
  await srv.admin.json('PUT', '/pins/job-1');
  await assert.rejects(srv.admin.deleteObject(rel), (e) => e.status === 409, 'pinned still protects it');
  await srv.admin.json('DELETE', '/pins/job-1');
  assert.deepEqual(await srv.admin.deleteObject(rel), { deleted: true }, 'terminal and unpinned: no longer protected');
});

test('N20 deleteObject itself refuses a protected blob, whoever calls it', async (t) => {
  const srv = await startStore();
  t.after(() => srv.close());
  const snap = snapshotOf({ 'a.txt': 'in use' });
  await publish(srv, 'job-1', snap);
  assert.throws(() => srv.store.deleteObject(blobRelPath([...snap.blobs.keys()][0])), (e) => e.status === 409);
});
