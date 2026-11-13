import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv } from './helpers.js';
import { makeGitWorktree } from './remote-harness.js';
import { makeTmpDir } from './helpers/tmp.js';
import { recordFsyncs } from './helpers/fsync-recorder.js';
import { ensureDurableDir, paths, atomicWriteFile } from '../src/state.js';
import { installArtifactsDurable } from '../src/remote-artifacts.js';
import { createAttempt, readAttempt, updateAttempt, publishTerminal } from '../src/attempts.js';
import { CappedLogWriter, publishAttemptTerminal } from '../src/supervisor.js';

const withRecorder = async (fn) => {
  const recorder = recordFsyncs();
  try {
    await fn(recorder);
  } finally {
    recorder.stop();
  }
  return recorder;
};

const parentsOfEveryComponent = (dir) => {
  const parents = [];
  for (let d = path.resolve(dir); path.dirname(d) !== d; d = path.dirname(d)) parents.push(path.dirname(d));
  return parents;
};

// ---- ensureDurableDir (T33) ----

test('T33: ensureDurableDir fsyncs the parent of every component of a fresh path', async () => {
  const base = fs.realpathSync(makeTmpDir('durable-dir-'));
  const target = path.join(base, 'pull', 'ticket-1', 'stage');
  const recorder = await withRecorder(() => ensureDurableDir(target));
  assert.ok(fs.statSync(target).isDirectory());
  for (const parent of parentsOfEveryComponent(target)) assert.ok(recorder.fsyncedPaths().includes(parent), `no fsync of ${parent}`);
});

test('T33: an existing but unsynced component still gets its parent fsynced', async () => {
  const base = fs.realpathSync(makeTmpDir('durable-dir-'));
  const existing = path.join(base, 'pull', 'ticket-1');
  fs.mkdirSync(existing, { recursive: true });
  const recorder = await withRecorder(() => ensureDurableDir(existing));
  for (const parent of parentsOfEveryComponent(existing)) assert.ok(recorder.fsyncedPaths().includes(parent), `no fsync of ${parent}`);
  assert.ok(recorder.fsyncedPaths().includes(path.join(base, 'pull')), 'the parent of the pre-existing ticket directory was not fsynced');
});

// ---- installArtifactsDurable (T37) ----

const artifact = (relPath, text) => ({ path: relPath, bytes: Buffer.from(text) });

test('T37: a new nested destination is durable up through the worktree root before the installed marker', async () => {
  const wt = fs.realpathSync(makeGitWorktree({ 'a.txt': 'x' }));
  const markerDir = fs.realpathSync(makeTmpDir('installed-marker-'));
  const markerPath = path.join(markerDir, 'installed.json');
  let result;
  const recorder = await withRecorder(() => {
    result = installArtifactsDurable(wt, [artifact('reports/run/output.json', '{"ok":1}')], ['reports/run/output.json'], { installedMarkerPath: markerPath });
  });
  assert.deepEqual(result.written, ['reports/run/output.json']);
  assert.deepEqual(JSON.parse(fs.readFileSync(markerPath, 'utf8')), { paths: ['reports/run/output.json'] });
  const marker = recorder.indexOf('rename', markerPath);
  assert.ok(marker >= 0);
  for (const barrier of [path.join(wt, 'reports/run/output.json'), path.join(wt, 'reports/run'), path.join(wt, 'reports'), wt]) {
    const at = recorder.indexOf('fsync', barrier);
    assert.ok(at >= 0 && at < marker, `no fsync of ${barrier} before the installed marker`);
  }
});

// ---- updateAttempt {fsync} and publishTerminal cancelPrecedence ----

test('updateAttempt: {fsync:true} fsyncs the attempt file and its directory; the default does not', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'abc', { runner: 'r' });
  const file = path.join(paths(state).attempts, 'abc.json');
  const quiet = await withRecorder(async () => assert.equal((await updateAttempt(state, 'abc', 0, { phase: 'running' })).ok, true));
  assert.deepEqual(quiet.fsyncedPaths(), []);
  const durable = await withRecorder(async () => assert.equal((await updateAttempt(state, 'abc', 0, { phase: 'done' }, { fsync: true })).ok, true));
  assert.ok(durable.fsyncedPaths().some((p) => path.dirname(p) === path.dirname(file) && path.basename(p).startsWith('.tmp-')));
  assert.ok(durable.fsyncedPaths().includes(path.dirname(file)));
  assert.equal(readAttempt(state, 'abc').phase, 'done');
});

const writeCancelMarker = (root, id) => atomicWriteFile(path.join(paths(root).cancel, id), String(Date.now()));

test('publishTerminal: the default cancelPrecedence lets the cancel marker override the writer', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'abc', { runner: 'r' });
  writeCancelMarker(state, 'abc');
  const seen = [];
  const out = await publishTerminal(state, 'abc', 0, (info) => seen.push(info));
  assert.deepEqual(out, { ok: true, cancelled: true });
  assert.deepEqual(seen, [{ cancelled: true }]);
  assert.equal(readAttempt(state, 'abc'), null);
});

test('publishTerminal: cancelPrecedence "decision" ignores the marker and publishes the decision', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'abc', { runner: 'r' });
  writeCancelMarker(state, 'abc');
  const seen = [];
  const out = await publishTerminal(state, 'abc', 0, (info) => seen.push(info), { cancelPrecedence: 'decision' });
  assert.deepEqual(out, { ok: true, cancelled: false });
  assert.deepEqual(seen, [{ cancelled: false }]);
  assert.equal(readAttempt(state, 'abc'), null);
});

test('publishTerminal: cancelPrecedence "decision" still refuses a stale generation', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'abc', { runner: 'r' });
  writeCancelMarker(state, 'abc');
  const out = await publishTerminal(state, 'abc', 7, () => assert.fail('must not write'), { cancelPrecedence: 'decision' });
  assert.deepEqual(out, { ok: false });
  assert.ok(readAttempt(state, 'abc'));
});

// ---- publishAttemptTerminal ----

test('publishAttemptTerminal: publishes without exiting; durable fsyncs the result before the attempt is removed', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'abc', { runner: 'r' });
  const resultPath = path.join(state, 'result-abc.json');
  let drained = false;
  let attemptPresentAtFsync = null;
  let out;
  const recorder = recordFsyncs();
  const attemptFile = path.join(paths(state).attempts, 'abc.json');
  try {
    out = await publishAttemptTerminal(state, 'abc', 0, {
      drain: async () => {
        drained = true;
      },
      buildConfirmedResult: () => ({ exit: 3 }),
      buildCancelledResult: () => ({ exit: 130 }),
      writeResult: (result, { fsync }) => {
        atomicWriteFile(resultPath, JSON.stringify(result), { fsync });
        attemptPresentAtFsync = fs.existsSync(attemptFile);
      },
      durable: true,
    });
  } finally {
    recorder.stop();
  }
  assert.equal(drained, true);
  assert.deepEqual(out.published, { ok: true, cancelled: false });
  assert.deepEqual(out.finalResult, { exit: 3 });
  assert.equal(attemptPresentAtFsync, true);
  assert.ok(recorder.indexOf('rename', resultPath) >= 0);
  assert.ok(recorder.fsyncedPaths().some((p) => path.dirname(p) === path.dirname(resultPath)));
  assert.equal(readAttempt(state, 'abc'), null);
});

test('publishAttemptTerminal: cancelPrecedence "decision" reaches publishTerminal', async () => {
  const { state } = freshEnv();
  await createAttempt(state, 'abc', { runner: 'r' });
  writeCancelMarker(state, 'abc');
  const out = await publishAttemptTerminal(state, 'abc', 0, {
    drain: async () => {},
    buildConfirmedResult: () => ({ exit: 0 }),
    buildCancelledResult: () => ({ exit: 130 }),
    writeResult: () => {},
    cancelPrecedence: 'decision',
  });
  assert.deepEqual(out.finalResult, { exit: 0 });
});

// ---- CappedLogWriter.durable and byte accounting ----

const MARKER = '\n[lane-broker] log truncated at 50MB\n';
const closeWriter = (writer) => writer.finish();

test('CappedLogWriter.durable: waits for queued writes, fsyncs the log fd, and bytes equals the file size', async () => {
  const dir = makeTmpDir('capped-durable-');
  const logPath = path.join(dir, 'out.log');
  const writer = new CappedLogWriter(logPath);
  const recorder = recordFsyncs();
  let ok;
  try {
    writer.write(Buffer.from('hello\n'));
    ok = await writer.durable();
  } finally {
    recorder.stop();
  }
  assert.equal(ok, true);
  assert.equal(fs.readFileSync(logPath, 'utf8'), 'hello\n');
  assert.ok(recorder.events.some((e) => e.op === 'fsync' && e.ino === fs.statSync(logPath).ino), 'the log fd was not fsynced');
  assert.equal(writer.bytes, fs.statSync(logPath).size);
  await closeWriter(writer);
});

test('CappedLogWriter: bytes stays equal to the file size across the cap and its truncation marker, with exactly one marker', async () => {
  const dir = makeTmpDir('capped-cap-');
  const logPath = path.join(dir, 'out.log');
  const writer = new CappedLogWriter(logPath, { capBytes: 100 });
  writer.write(Buffer.alloc(60, 'a'));
  writer.write(Buffer.alloc(60, 'b'));
  writer.write(Buffer.alloc(60, 'c'));
  await writer.durable();
  const text = fs.readFileSync(logPath, 'utf8');
  assert.equal(writer.bytes, Buffer.byteLength(text));
  assert.equal(writer.bytes, fs.statSync(logPath).size);
  assert.equal(text.split(MARKER).length - 1, 1);
  assert.equal(text.length, 100 + MARKER.length);
  await closeWriter(writer);
});

test('CappedLogWriter: a writer built from a reconciled file counts that file and sets truncated only when it ends with the marker', async () => {
  const dir = makeTmpDir('capped-reconcile-');
  const open = path.join(dir, 'open.log');
  fs.writeFileSync(open, 'x'.repeat(10));
  const fromOpen = new CappedLogWriter(open, { capBytes: 100 });
  assert.equal(fromOpen.bytes, 10);
  assert.equal(fromOpen.truncated, false);
  await closeWriter(fromOpen);

  const capped = path.join(dir, 'capped.log');
  fs.writeFileSync(capped, `${'x'.repeat(100)}${MARKER}`);
  const fromCapped = new CappedLogWriter(capped, { capBytes: 100 });
  assert.equal(fromCapped.truncated, true);
  fromCapped.write(Buffer.from('more'));
  await fromCapped.durable();
  assert.equal(fs.statSync(capped).size, 100 + MARKER.length);
  await closeWriter(fromCapped);
});

test('CappedLogWriter: a reconciled tail replayed after truncating an uncommitted append stays under the cap with one marker', async () => {
  const dir = makeTmpDir('capped-replay-');
  const logPath = path.join(dir, 'out.log');
  const first = new CappedLogWriter(logPath, { capBytes: 100 });
  first.write(Buffer.alloc(10, 'a'));
  first.write(Buffer.alloc(80, 'b'));
  first.write(Buffer.alloc(80, 'c'));
  await first.durable();
  await closeWriter(first);
  fs.truncateSync(logPath, 10);
  const second = new CappedLogWriter(logPath, { capBytes: 100 });
  second.write(Buffer.alloc(30, 'd'));
  await second.durable();
  const text = fs.readFileSync(logPath, 'utf8');
  assert.equal(text, `${'a'.repeat(10)}${'d'.repeat(30)}`);
  assert.equal(second.bytes, 40);
  await closeWriter(second);
});
