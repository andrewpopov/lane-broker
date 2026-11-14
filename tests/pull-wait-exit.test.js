import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { spawn } from 'node:child_process';
import test from 'node:test';
import { createAttempt, updateAttempt } from '../src/attempts.js';
import { createEnvelope, recordSnapshot } from '../src/pull-envelope.js';
import { ensureStateDirs } from '../src/state.js';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { CA_TEXT } from './helpers/queue-fixture.js';

/** BRAIN-1036 R6: `lane wait --timeout` ends the process at the deadline even while a pull step is still inside a network call. */

const WAIT_TIMEOUT_MS = 1500;
const CONNECT_TIMEOUT_MS = 5000;

async function silentQueue() {
  const sockets = new Set();
  const server = net.createServer((socket) => { sockets.add(socket); socket.on('error', () => {}); });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return { port: server.address().port, close: () => { for (const s of sockets) s.destroy(); server.close(); } };
}

async function seedStuckPullAttempt(state, home, port) {
  const secrets = path.join(home, 'secrets');
  fs.mkdirSync(secrets);
  const files = { password: 'pw\n', 'ca.pem': CA_TEXT, 'rustfs.env': 'AWS_ACCESS_KEY_ID=a\nAWS_SECRET_ACCESS_KEY=b\n' };
  for (const [name, text] of Object.entries(files)) fs.writeFileSync(path.join(secrets, name), text, { mode: 0o600 });
  writeGlobalConfig(home, {
    version: 1,
    laneQueueUrl: `postgres://lane_submit_w@127.0.0.1:${port}/lane_queue?sslmode=verify-full`,
    lanePull: { passwordFile: path.join(secrets, 'password'), caFile: path.join(secrets, 'ca.pem'), rustfs: { endpoint: 'http://127.0.0.1:9', credentialsEnvFile: path.join(secrets, 'rustfs.env') } },
  });
  ensureStateDirs(state);
  const id = crypto.randomUUID();
  const attempt = await createAttempt(state, id);
  await createEnvelope(state, {
    logicalId: id, binding: { protocol: 2, generation: 0, manifestHash: 'm'.repeat(64) }, header: {}, repoKey: 'r', lane: 'test', laneKey: 'r:test',
    cpuCores: 1, memoryBytes: 1, requiresTags: [], priority: 0,
  });
  await recordSnapshot(state, id, { key: 'k', sha256: 'x', bytes: 1, blobs: [] });
  await updateAttempt(state, id, attempt.generation, { transport: 'pull', phase: 'running', supervisor: { pid: 2 ** 22 + 9, startTime: null, bootId: 'dead' }, pull: { stage: 'QUEUED', enqueue: { seenRowAt: Date.now(), attempts: [] } } });
  return id;
}

async function laneWaitProcess(load, env, id) {
  const script = 'const m = await import(process.argv[1]); await m.waitAndExit(process.argv[2], { timeoutMs: Number(process.argv[3]) });';
  const started = Date.now();
  const child = spawn(process.execPath, ['--input-type=module', '-e', script, load.url('wait.js'), id, String(WAIT_TIMEOUT_MS)], { env });
  child.stdout.resume();
  child.stderr.resume();
  const code = await new Promise((resolve) => child.on('close', resolve));
  return { code, ms: Date.now() - started };
}

async function waitEndsAtItsDeadline(load) {
  const { home, state, env } = freshEnv();
  const queue = await silentQueue();
  try {
    const id = await seedStuckPullAttempt(state, home, queue.port);
    const { code, ms } = await laneWaitProcess(load, env, id);
    assert.equal(code, 75, 'waited, not failed');
    assert.ok(ms < CONNECT_TIMEOUT_MS - 1000, `the process lived ${ms}ms: it waited for the abandoned queue call instead of ending at the deadline`);
  } finally {
    queue.close();
  }
}

test('lane wait --timeout exits 75 at its deadline while a pull step is stuck connecting to the queue', async () => {
  await waitEndsAtItsDeadline(realSrc);
});

test('canary: with process.exit replaced by exitCode, the abandoned step keeps lane wait alive past its deadline', async () => {
  await assertCanaryFails('wait.js', ['process.exit(exitCode);', 'process.exitCode = exitCode;'], waitEndsAtItsDeadline);
});
