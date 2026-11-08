import test, { afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import zlib from 'node:zlib';
import net from 'node:net';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig, BIN } from './helpers.js';
import { encodeSnapshot } from '../src/remote-stream.js';
import { makeTmpDir } from './helpers/tmp.js';
import { paths } from '../src/state.js';

/** BRAIN-850 S1: `lane remote-exec` exits once its result is published, even with a live handle holding the event loop. */

const TAIL_BYTES = 200_000;
const EXIT_WITHIN_MS = 15_000;
const children = [];
const sockets = [];
const servers = [];
afterEach(() => {
  for (const child of children.splice(0)) child.kill('SIGKILL');
  for (const s of sockets.splice(0)) s.destroy();
  for (const s of servers.splice(0)) s.close();
});

/**
 * A stdout nobody reads whose kernel buffer is already full, so everything remote-exec writes stays pending.
 * Sizing the job's output cannot do this: a payload past the socket buffer blocks the job itself (macOS and Linux).
 */
async function fullUnreadStdout() {
  const server = net.createServer({ pauseOnConnect: true }); // a paused-later reader still drains a highWaterMark of the fill
  await new Promise((resolve) => server.listen(path.join(makeTmpDir('exec-exits-sock-'), 's'), resolve));
  const accepted = new Promise((resolve) => server.once('connection', resolve));
  const writer = net.connect(server.address());
  await new Promise((resolve) => writer.once('connect', resolve));
  const reader = await accepted;
  sockets.push(writer, reader);
  servers.push(server);
  // Raw writes until the kernel refuses: nothing stays queued in this process to flush later.
  const chunk = Buffer.alloc(64 * 1024);
  for (;;) {
    try {
      fs.writeSync(writer._handle.fd, chunk);
    } catch (err) {
      if (err.code !== 'EAGAIN') throw err;
      return writer;
    }
  }
}

async function runExec(gzip, { env = {}, readStdout = true } = {}) {
  const f = freshEnv({ LANE_BROKER_TEST_RETAIN_HANDLE: '1', LANE_BROKER_TEST_CANCEL_GRACE_MS: '1000', ...env });
  writeGlobalConfig(f.home, {});
  const root = makeTmpDir('exec-exits-root-');
  const dir = makeTmpDir('exec-exits-src-');
  const buf = Buffer.from('x');
  fs.writeFileSync(path.join(dir, 'a.txt'), buf);
  const entries = [{ path: 'a.txt', type: 'file', exec: false, size: 1, sha256: crypto.createHash('sha256').update(buf).digest('hex') }];
  const header = {
    ticketId: crypto.randomUUID(),
    generation: 0,
    repoKey: 'exec-exits-repo',
    lane: 'default',
    argv: [process.execPath, '-e', `process.stdout.write('y'.repeat(${readStdout ? TAIL_BYTES : 1}) + 'END')`],
    relCwd: '',
  };
  const args = [BIN, 'remote-exec', '--root', root];
  if (gzip) args.push('--transfer', 'gzip');
  const child = spawn(process.execPath, args, { env: f.env, stdio: ['pipe', readStdout ? 'pipe' : await fullUnreadStdout(), 'pipe'] });
  children.push(child);
  let out = '';
  if (readStdout) child.stdout.on('data', (d) => (out += d));
  child.stderr.on('data', () => {});
  const snapshot = encodeSnapshot(dir, header, entries);
  (gzip ? snapshot.pipe(zlib.createGzip()) : snapshot).pipe(child.stdin);
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve({ timedOut: true, out, root, header }), EXIT_WITHIN_MS);
    child.on('close', (code) => {
      clearTimeout(timer);
      resolve({ timedOut: false, code, out, root, header });
    });
  });
}

for (const gzip of [false, true]) {
  test(`remote-exec exits after publishing its result despite a retained handle (${gzip ? 'gzip' : 'raw'})`, { timeout: 60_000 }, async () => {
    const r = await runExec(gzip);
    assert.equal(r.timedOut, false, 'remote-exec still running after its result was published');
    assert.equal(r.code, 0);
    const result = JSON.parse(fs.readFileSync(path.join(r.root, 'tickets', r.header.ticketId, 'result.json'), 'utf8'));
    assert.equal(result.exitCode ?? result.exit, 0);
    assert.ok(r.out.includes('y'.repeat(TAIL_BYTES) + 'END'), `stdout lost the ${TAIL_BYTES}-byte tail (got ${r.out.length} bytes)`);
  });
}

test('remote-exec whose stdout is never read records the flush timeout in the broker log and keeps its exit code', { timeout: 60_000 }, async () => {
  const r = await runExec(false, { env: { LANE_BROKER_TEST_FLUSH_MS: '500' }, readStdout: false });
  assert.equal(r.timedOut, false, 'remote-exec did not exit after the flush bound');
  assert.equal(r.code, 0);
  assert.match(fs.readFileSync(paths(r.root).admissionLog, 'utf8'), /stdout\/stderr did not flush within 500ms/);
});
