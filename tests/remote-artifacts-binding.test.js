import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { Readable } from 'node:stream';
import { spawn } from 'node:child_process';
import { collectArtifacts, receiveArtifacts } from '../src/remote-artifacts.js';
import { makeReader } from '../src/remote-stream.js';
import { freshEnv, BIN } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';

/** BRAIN-1035 S2a (T26): `remote-artifacts <execId>` reads the execution's dir but binds the stream to the logical ticket uuid. */

const LIMITS = { maxFileBytes: 1024, maxTotalBytes: 4096, maxCount: 10 };
const PATTERNS = ['out/*.json'];

function storeArtifacts(remoteRoot, execId) {
  const work = makeTmpDir('binding-work-');
  fs.mkdirSync(path.join(work, 'out'));
  fs.writeFileSync(path.join(work, 'out', 'a.json'), '{"ok":true}');
  const res = collectArtifacts(work, PATTERNS, path.join(remoteRoot, 'tickets', execId, 'artifacts'), LIMITS);
  assert.equal(res.ok, true, JSON.stringify(res));
}

function printArtifacts(env, remoteRoot, id) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [BIN, 'remote-artifacts', id, '--root', remoteRoot], { env });
    const chunks = [];
    child.stdout.on('data', (d) => chunks.push(d));
    child.stderr.on('data', () => {});
    child.on('close', (code) => resolve({ code, bytes: Buffer.concat(chunks) }));
  });
}

const receive = (bytes, ticketId) => receiveArtifacts(makeReader(Readable.from([bytes])), { ticketId, patterns: PATTERNS, limits: LIMITS });

test('a bare-uuid push ticket and a uuid.e2 execution both stream bound to the logical uuid, which receiveArtifacts accepts', async () => {
  const { env } = freshEnv();
  const remoteRoot = makeTmpDir('binding-root-');
  const uuid = crypto.randomUUID();
  const execId = `${uuid}.e2`;
  const bare = crypto.randomUUID();
  storeArtifacts(remoteRoot, execId);
  storeArtifacts(remoteRoot, bare);

  const fromExec = await printArtifacts(env, remoteRoot, execId);
  assert.equal(fromExec.code, 0);
  const received = await receive(fromExec.bytes, uuid);
  assert.equal(received.ok, true, JSON.stringify(received));
  assert.deepEqual(received.files.map((f) => f.path), ['out/a.json']);
  assert.equal((await receive(fromExec.bytes, execId)).ok, false, 'binding to the execution id is refused: the receiver knows only the uuid');

  const fromBare = await printArtifacts(env, remoteRoot, bare);
  assert.equal(fromBare.code, 0);
  assert.equal((await receive(fromBare.bytes, bare)).ok, true);
});

test('an unknown execution id prints nothing and exits 1; a malformed id exits 2', async () => {
  const { env } = freshEnv();
  const remoteRoot = makeTmpDir('binding-root-');
  assert.equal((await printArtifacts(env, remoteRoot, `${crypto.randomUUID()}.e5`)).code, 1);
  assert.equal((await printArtifacts(env, remoteRoot, `${crypto.randomUUID()}.e0`)).code, 2);
});
