import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { PgCluster, PG_SKIP_REASON } from './lane-db-harness.js';

/**
 * BRAIN-1373: the Postgres harness must not leak clusters when its test process is killed. SIGTERM stops the cluster
 * before the process dies; a SIGKILLed owner leaves a cluster that the next PgCluster.start() reaps; a live owner's
 * cluster is never touched.
 */
const harnessUrl = pathToFileURL(path.join(path.dirname(fileURLToPath(import.meta.url)), 'lane-db-harness.js')).href;
const CHILD_SOURCE = `
import { PgCluster } from ${JSON.stringify(harnessUrl)};
const cluster = PgCluster.start();
console.log(cluster.dir);
setInterval(() => {}, 1000);
`;

const alive = (pid) => {
  try { process.kill(pid, 0); return true; } catch (err) { return err.code !== 'ESRCH'; }
};
const postmasterPid = (dir) => Number(fs.readFileSync(path.join(dir, 'data', 'postmaster.pid'), 'utf8').split('\n')[0]);

async function startChild() {
  const child = spawn(process.execPath, ['--input-type=module', '-e', CHILD_SOURCE], { stdio: ['ignore', 'pipe', 'inherit'] });
  const exited = new Promise((resolve) => child.once('exit', (code, signal) => resolve({ code, signal })));
  const dir = await new Promise((resolve, reject) => {
    let out = '';
    child.stdout.on('data', (d) => { out += d; if (out.includes('\n')) resolve(out.trim()); });
    child.once('exit', () => reject(new Error(`child exited before printing its cluster dir: ${out}`)));
  });
  return { child, dir, exited, postmaster: postmasterPid(dir) };
}

const waitGone = async (pid) => {
  for (let i = 0; i < 100 && alive(pid); i++) await new Promise((r) => setTimeout(r, 100));
};

describe('PgCluster leak protection', { skip: PG_SKIP_REASON ?? false, timeout: 120000 }, () => {
  test('SIGTERM to the owning process stops the postmaster and removes the dir', async () => {
    const { child, dir, exited, postmaster } = await startChild();
    assert.ok(alive(postmaster), 'postmaster should be running before the signal');
    child.kill('SIGTERM');
    const { signal } = await exited;
    await waitGone(postmaster);
    assert.equal(signal, 'SIGTERM', 'the process must still die from the signal');
    assert.equal(alive(postmaster), false, 'postmaster survived SIGTERM of its owner');
    assert.equal(fs.existsSync(dir), false, 'cluster dir survived SIGTERM of its owner');
  });

  test('a cluster whose owner was SIGKILLed is reaped by the next start()', async () => {
    const { child, dir, exited, postmaster } = await startChild();
    child.kill('SIGKILL');
    await exited;
    assert.ok(alive(postmaster), 'SIGKILL should have orphaned the postmaster');
    const next = PgCluster.start();
    try {
      await waitGone(postmaster);
      assert.equal(alive(postmaster), false, 'orphaned postmaster was not stopped');
      assert.equal(fs.existsSync(dir), false, 'orphaned cluster dir was not removed');
    } finally {
      next.stop();
    }
  });

  test('a cluster whose owner is alive is not reaped', () => {
    const dir = fs.mkdtempSync('/tmp/lbpg-');
    fs.writeFileSync(path.join(dir, 'owner.pid'), String(process.pid));
    const next = PgCluster.start();
    try {
      assert.equal(fs.existsSync(dir), true, 'a live owner cluster was reaped');
    } finally {
      next.stop();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
