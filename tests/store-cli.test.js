import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { SECRET, sha, token } from './store-harness.js';
import { StoreClient } from '../src/store/client.js';

const BIN = fileURLToPath(new URL('../bin/lane-store.js', import.meta.url));
const env = { ...process.env, LANE_STORE_SECRET: SECRET };

function serve(root) {
  const child = spawn(process.execPath, [BIN, 'serve', '--root', root, '--listen', '127.0.0.1:0'], { env });
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

test('lane-store CLI: serve, replicate (exit 0) and compare (exit 0, then 2 on a planted divergence)', async (t) => {
  const dirs = [0, 1].map(() => fs.mkdtempSync(path.join(os.tmpdir(), 'lane-store-cli-')));
  const [p, r] = dirs.map(serve);
  t.after(() => [p, r].forEach((s) => s.child.kill('SIGTERM')));
  const [pUrl, rUrl] = await Promise.all([p.ready, r.ready]);
  const submit = new StoreClient({ baseUrl: pUrl, token: token({ role: 'submit' }) });
  await submit.putBlob(sha('cli blob'), Buffer.from('cli blob'));

  const replEnv = { ...env, LANE_STORE_REPLICA_TOKEN: token({ role: 'replica' }) };
  const run = (...args) => spawnSync(process.execPath, [BIN, ...args], { env: replEnv, encoding: 'utf8' });
  const rep = run('replicate', '--root', dirs[0], '--replica-url', rUrl);
  assert.equal(rep.status, 0, rep.stderr);
  assert.equal(JSON.parse(rep.stdout).replicatedSeq, 1);
  assert.equal(run('compare', '--root', dirs[0], '--replica-url', rUrl, '--deep').status, 0);

  fs.rmSync(path.join(dirs[1], 'blobs'), { recursive: true });
  const cmp = run('compare', '--root', dirs[0], '--replica-url', rUrl);
  assert.equal(cmp.status, 2);
  assert.equal(JSON.parse(cmp.stdout).missingAtReplica.length, 1);

  const wild = spawnSync(process.execPath, [BIN, 'serve', '--root', dirs[0], '--listen', '0.0.0.0:0'], { env, encoding: 'utf8' });
  assert.notEqual(wild.status, 0);
  assert.match(wild.stderr, /wildcard/);
});
