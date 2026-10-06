import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { SECRET, sha, token } from './store-harness.js';
import { StoreClient } from '../src/store/client.js';
import { makeTmpDir } from './helpers/tmp.js';

const BIN = fileURLToPath(new URL('../bin/lane-store.js', import.meta.url));
const env = { ...process.env, LANE_STORE_SECRET: SECRET, LANE_STORE_REPLICA_TOKEN: token({ role: 'replica' }) };

function serve(root, ...extra) {
  const child = spawn(process.execPath, [BIN, 'serve', '--root', root, '--listen', '127.0.0.1:0', ...extra], { env });
  const ready = new Promise((resolve, reject) => {
    let out = '';
    child.stdout.on('data', (d) => {
      out += d;
      const m = /listening on 127\.0\.0\.1:(\d+)/.exec(out);
      if (m) resolve(`http://127.0.0.1:${m[1]}`);
    });
    child.on('exit', (code) => reject(new Error(`lane-store exited ${code}`)));
  });
  return { child, ready };
}

test('lane-store CLI: in-process replication (serve --replicate-to), compare via the running primary, offline lock', async (t) => {
  const dirs = [0, 1].map(() => makeTmpDir('lane-store-cli-'));
  const replica = serve(dirs[1], '--replica');
  t.after(() => replica.child.kill('SIGTERM'));
  const rUrl = await replica.ready;
  const primary = serve(dirs[0], '--replicate-to', rUrl, '--replicate-interval-s', '1');
  t.after(() => primary.child.kill('SIGTERM'));
  const pUrl = await primary.ready;
  const submit = new StoreClient({ baseUrl: pUrl, token: token({ role: 'submit' }) });
  await submit.putBlob(sha('cli blob'), Buffer.from('cli blob'));
  const admin = new StoreClient({ baseUrl: rUrl, token: token({ role: 'admin' }) });
  for (let i = 0; i < 60 && (await admin.has([sha('cli blob')])).length; i += 1) await new Promise((r) => setTimeout(r, 200));
  assert.deepEqual(await admin.has([sha('cli blob')]), [], 'the primary replicated by itself, with no timer or second process');

  const adminEnv = { ...env, LANE_STORE_ADMIN_TOKEN: token({ role: 'admin' }) };
  const run = (...args) => spawnSync(process.execPath, [BIN, ...args], { env: adminEnv, encoding: 'utf8' });
  assert.equal(run('compare', '--url', pUrl, '--deep').status, 0);
  fs.rmSync(path.join(dirs[1], 'blobs'), { recursive: true });
  const cmp = run('compare', '--url', pUrl);
  assert.equal(cmp.status, 2);
  assert.equal(JSON.parse(cmp.stdout).missingAtReplica.length, 1);

  const locked = run('rebuild-watermark', '--root', dirs[0], '--seq', '0');
  assert.notEqual(locked.status, 0);
  assert.match(locked.stderr, /in use/);

  const wild = spawnSync(process.execPath, [BIN, 'serve', '--root', makeTmpDir('ls-w-'), '--listen', '0.0.0.0:0'], { env, encoding: 'utf8' });
  assert.notEqual(wild.status, 0);
  assert.match(wild.stderr, /wildcard/);
});
