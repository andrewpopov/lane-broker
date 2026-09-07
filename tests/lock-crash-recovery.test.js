import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { freshEnv, sleep } from './helpers.js';
import { paths, withLock, __test } from '../src/state.js';

/** Spawn a child that exits immediately, and return its (now-dead) pid. */
async function spawnDeadPid() {
  const child = spawn(process.execPath, ['-e', 'process.exit(0)']);
  const pid = child.pid;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}

function writeRecord(file, record) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(record));
}

test('crash after step 2: a leftover tomb from a stealer that died mid-takeover is left alone and the lock acquires normally', async () => {
  const { state } = freshEnv();
  const lockDir = paths(state).lock;
  const ownerFile = path.join(lockDir, 'owner.json');

  const deadPid = await spawnDeadPid();
  const deadToken = 'dead-T';
  // Simulate: a stealer completed step 2 (renamed lockDir -> tomb) and then
  // crashed before its own next loop iteration could re-acquire. So lockDir
  // is ABSENT and a tomb sits at the deterministic name, still carrying the
  // dead owner's own record (the stealer never touches the tomb's contents,
  // only its own claim/promotion, which under this protocol is just the
  // next withLock loop's normal acquire).
  assert.equal(fs.existsSync(lockDir), false, 'lockDir must be absent for this scenario');
  const tomb = __test.tombPath(state, deadPid, deadToken);
  writeRecord(path.join(tomb, 'owner.json'), { pid: deadPid, token: deadToken, start: null, at: Date.now() });

  const result = await withLock(state, async () => 'ran', { timeoutMs: 5_000, pollMs: 10 });
  assert.equal(result, 'ran', 'withLock must acquire normally: lockDir was absent, no dead owner to recover');
  assert.equal(fs.existsSync(tomb), true, 'the leftover tomb must survive — nothing in a normal acquire touches it');
  assert.equal(fs.existsSync(ownerFile), false, 'lock must have been released again after withLock returned');
});

test('stale-contender safety: B, still holding the same stale observation, can never displace A after A recovers and re-acquires', async () => {
  const { state } = freshEnv();
  const lockDir = paths(state).lock;
  const ownerFile = path.join(lockDir, 'owner.json');
  fs.mkdirSync(lockDir, { recursive: true });

  const deadPid = await spawnDeadPid();
  const observedOwner = { pid: deadPid, token: 'dead-T', start: null, at: Date.now() };
  writeRecord(ownerFile, observedOwner);

  // A moves the dead lock dir aside...
  const aMoved = __test.recoverDeadLock(state, lockDir, ownerFile, observedOwner);
  assert.equal(aMoved, true, 'A must successfully move the dead lock dir to its tomb');
  const tomb = __test.tombPath(state, deadPid, observedOwner.token);
  assert.equal(fs.existsSync(tomb), true, 'the tomb must exist after A moves the dead lock aside');
  assert.equal(
    fs.existsSync(path.join(tomb, 'stolen-at')),
    true,
    'the tomb A created must contain a stolen-at stamp so GC can age it correctly',
  );

  // ...then re-acquires normally (lockDir is now free) and holds it open.
  let release;
  const held = new Promise((resolve) => {
    release = resolve;
  });
  const aHolding = withLock(state, async () => {
    await held;
    return 'a-done';
  });
  // Give A's acquire a moment to land before B races against the stale state.
  await sleep(50);
  const ownerAfterA = JSON.parse(fs.readFileSync(ownerFile, 'utf8'));
  assert.notEqual(ownerAfterA.token, observedOwner.token, 'A must now hold the lock under its own fresh token');

  // B, still holding the SAME stale observation of the original dead owner,
  // runs the recovery step. It must lose: the tomb it would move lockDir
  // onto already exists (non-empty), so its rename fails.
  const bMoved = __test.recoverDeadLock(state, lockDir, ownerFile, observedOwner);
  assert.equal(bMoved, false, 'B must lose the race and never move the live lock aside');

  const ownerAfterB = JSON.parse(fs.readFileSync(ownerFile, 'utf8'));
  assert.equal(ownerAfterB.token, ownerAfterA.token, "owner.json must still show A's token after B loses");
  assert.equal(fs.existsSync(tomb), true, 'the original tomb must be untouched by B losing the race');

  release();
  const result = await aHolding;
  assert.equal(result, 'a-done');
});

test('MUTATION: deleting the tomb after a successful takeover lets a stale contender displace the current owner', async () => {
  const { state } = freshEnv();
  const lockDir = paths(state).lock;
  const ownerFile = path.join(lockDir, 'owner.json');
  fs.mkdirSync(lockDir, { recursive: true });

  const deadPid = await spawnDeadPid();
  const observedOwner = { pid: deadPid, token: 'dead-T', start: null, at: Date.now() };
  writeRecord(ownerFile, observedOwner);
  const tomb = __test.tombPath(state, deadPid, observedOwner.token);

  // Mutant recovery step: same tomb-named rename as the real protocol, but
  // (the bug) deletes the tomb right after a successful move instead of
  // leaving it in place. Deliberately skips the real function's own
  // re-read-and-compare (step 1) and verify-and-restore (step 3) guards, so
  // this isolates exactly what tomb PERSISTENCE (step 4) is responsible
  // for: a stale contender's rename onto the same tomb name, once nothing
  // occupies that name, moves whatever currently sits at lockDir — live or
  // not — with no other check standing in the way.
  function mutantRecover(root, lockDirArg, tombPathForOwner) {
    fs.renameSync(lockDirArg, tombPathForOwner);
    fs.rmSync(tombPathForOwner, { recursive: true, force: true }); // BUG: deletes the tomb instead of keeping it
    return true;
  }

  // A "steals" the dead lock via the buggy variant and immediately re-acquires normally.
  const aMoved = mutantRecover(state, lockDir, tomb);
  assert.equal(aMoved, true);
  assert.equal(fs.existsSync(tomb), false, 'mutant: the tomb was deleted, unlike the real protocol');

  const token = crypto.randomUUID();
  fs.mkdirSync(lockDir, { recursive: true });
  writeRecord(ownerFile, { pid: process.pid, token, start: null, at: Date.now() });

  // B, still holding the exact same stale observation, retries the SAME
  // buggy recovery step against the SAME tomb name. Under the real
  // protocol this rename would fail (the tomb still exists, non-empty); with
  // the tomb gone, B's raw rename succeeds and moves A's live lockDir aside.
  const bMoved = mutantRecover(state, lockDir, tomb);
  assert.equal(bMoved, true, 'mutant: B wrongly succeeds in moving the live lock aside once the tomb is gone');

  assert.throws(
    () => assert.equal(fs.existsSync(lockDir), true, 'lockDir must still exist with A as owner'),
    /lockDir must still exist with A as owner/,
    'the stale-contender safety assertion must fail by name against the tomb-deleted mutant',
  );
});

test('GC ages tombs by the stolen-at stamp INSIDE the dir, never by directory mtime', async () => {
  const { state } = freshEnv();
  fs.mkdirSync(state, { recursive: true });

  // Directory mtime is 30 days old — a rename keeps the OLD mtime, so this
  // simulates a lock that was acquired long before it was ever stolen. Its
  // stolen-at stamp says "just now", so GC must keep it: mtime must never
  // enter the decision.
  const freshByStamp = path.join(state, '.lock-tomb-111-fresh-token');
  writeRecord(path.join(freshByStamp, 'owner.json'), { pid: 111, token: 'fresh-token', start: null, at: Date.now() });
  fs.writeFileSync(path.join(freshByStamp, 'stolen-at'), String(Date.now()));
  const oldMtime = (Date.now() - 30 * 24 * 60 * 60 * 1000) / 1000;
  fs.utimesSync(freshByStamp, oldMtime, oldMtime);

  // stolen-at is 8 days ago (past the 7-day TTL) — must be removed.
  const oldByStamp = path.join(state, '.lock-tomb-222-old-token');
  writeRecord(path.join(oldByStamp, 'owner.json'), { pid: 222, token: 'old-token', start: null, at: Date.now() });
  fs.writeFileSync(path.join(oldByStamp, 'stolen-at'), String(Date.now() - 8 * 24 * 60 * 60 * 1000));

  // No stolen-at stamp at all — must be kept (fail safe: never delete what
  // we cannot date).
  const noStamp = path.join(state, '.lock-tomb-333-no-stamp-token');
  writeRecord(path.join(noStamp, 'owner.json'), { pid: 333, token: 'no-stamp-token', start: null, at: Date.now() });

  // An EMPTY stamp (a truncated or half-written file) must also be kept:
  // Number('') is 0, and a naive parser would read that as 1970 and GC a
  // brand-new tomb.
  const emptyStamp = path.join(state, '.lock-tomb-444-empty-stamp-token');
  writeRecord(path.join(emptyStamp, 'owner.json'), { pid: 444, token: 'empty-stamp-token', start: null, at: Date.now() });
  fs.writeFileSync(path.join(emptyStamp, 'stolen-at'), '');

  __test.gcTombs(state);

  assert.equal(fs.existsSync(emptyStamp), true, 'a tomb with an EMPTY stolen-at stamp must be kept (Number("") is 0, not 1970)');
  assert.equal(fs.existsSync(freshByStamp), true, 'a tomb whose stolen-at is recent must survive GC despite an old directory mtime');
  assert.equal(fs.existsSync(oldByStamp), false, 'a tomb whose stolen-at is past TOMB_TTL_MS must be GC-ed');
  assert.equal(fs.existsSync(noStamp), true, 'a tomb with no stolen-at stamp must be kept, fail safe');
});

test('MUTATION: GC-by-mtime instead of stolen-at wrongly removes the fresh-by-stamp tomb', async () => {
  const { state } = freshEnv();
  fs.mkdirSync(state, { recursive: true });

  const freshByStamp = path.join(state, '.lock-tomb-111-fresh-token');
  writeRecord(path.join(freshByStamp, 'owner.json'), { pid: 111, token: 'fresh-token', start: null, at: Date.now() });
  fs.writeFileSync(path.join(freshByStamp, 'stolen-at'), String(Date.now()));
  const oldMtime = (Date.now() - 30 * 24 * 60 * 60 * 1000) / 1000;
  fs.utimesSync(freshByStamp, oldMtime, oldMtime);

  function mutantGcByMtime(root) {
    const names = fs.readdirSync(root);
    const now = Date.now();
    const ttl = 7 * 24 * 60 * 60 * 1000;
    for (const name of names) {
      if (!name.startsWith('.lock-tomb-')) continue;
      const full = path.join(root, name);
      const stat = fs.statSync(full);
      if (now - stat.mtimeMs > ttl) fs.rmSync(full, { recursive: true, force: true });
    }
  }

  mutantGcByMtime(state);

  assert.throws(
    () =>
      assert.equal(
        fs.existsSync(freshByStamp),
        true,
        'a tomb whose stolen-at is recent must survive GC despite an old directory mtime',
      ),
    /a tomb whose stolen-at is recent must survive GC despite an old directory mtime/,
    'GC-by-mtime must wrongly remove a tomb that stolen-at-based GC would have kept',
  );
});

test('third-contender race: C acquires the freed lock path while A is still mid-recovery, and A waits rather than holding concurrently', async () => {
  const { state } = freshEnv();
  const lockDir = paths(state).lock;
  const ownerFile = path.join(lockDir, 'owner.json');
  const marker = path.join(state, '..', 'contention-marker');
  fs.mkdirSync(lockDir, { recursive: true });

  const deadPid = await spawnDeadPid();
  const observedOwner = { pid: deadPid, token: 'dead-T', start: null, at: Date.now() };
  writeRecord(ownerFile, observedOwner);

  // A moves the dead lock dir aside (finishing the recovery step) but does
  // NOT yet re-acquire — simulating the gap between recoverDeadLock
  // returning and A's next loop iteration attempting the staging rename.
  const aMoved = __test.recoverDeadLock(state, lockDir, ownerFile, observedOwner);
  assert.equal(aMoved, true);
  assert.equal(fs.existsSync(lockDir), false, 'lockDir must be free after A moves the dead lock aside');

  // C acquires the now-free path first and holds it open.
  let releaseC;
  const heldC = new Promise((resolve) => {
    releaseC = resolve;
  });
  let overlap = false;
  const cHolding = withLock(state, async () => {
    fs.writeFileSync(marker, 'c');
    await heldC;
    fs.unlinkSync(marker);
    return 'c-done';
  });
  await sleep(50); // let C's acquire land

  // A now attempts to acquire (its next loop iteration after recovery) —
  // must wait, not run concurrently with C.
  const aHolding = withLock(
    state,
    async () => {
      if (fs.existsSync(marker)) overlap = true;
      fs.writeFileSync(marker, 'a');
      await sleep(20);
      fs.unlinkSync(marker);
      return 'a-done';
    },
    { timeoutMs: 5_000, pollMs: 10 },
  );

  await sleep(100);
  releaseC();
  const [cResult, aResult] = await Promise.all([cHolding, aHolding]);

  assert.equal(cResult, 'c-done');
  assert.equal(aResult, 'a-done');
  assert.equal(overlap, false, 'A must never observe holding the lock while C is still inside it');
});

test('a persistently failing recovery sleeps and honours the deadline instead of spinning', async () => {
  const { state } = freshEnv();
  const lockDir = paths(state).lock;
  const ownerFile = path.join(lockDir, 'owner.json');
  fs.mkdirSync(lockDir, { recursive: true });

  const deadPid = await spawnDeadPid();
  const observedOwner = { pid: deadPid, token: 'dead-T', start: null, at: Date.now() };
  writeRecord(ownerFile, observedOwner);

  // Pre-create a regular FILE at the tomb path recoverDeadLock will target,
  // so its rename(lockDir, tomb) fails every time (rename of a dir onto a
  // file is ENOTDIR) — recovery keeps failing for the whole call.
  const tomb = __test.tombPath(state, deadPid, observedOwner.token);
  fs.writeFileSync(tomb, 'not a directory');

  const startedAt = Date.now();
  await assert.rejects(
    withLock(state, async () => 'ran', { timeoutMs: 300, pollMs: 25 }),
    /lane-broker: timed out waiting for the global lock held by dead pid \d+ \(recovery kept failing\)/,
  );
  const elapsed = Date.now() - startedAt;
  assert.ok(elapsed >= 300, `must have slept through at least the timeout window, got ${elapsed}ms`);
  assert.ok(elapsed < 3000, `must not spin far past the deadline, got ${elapsed}ms`);
});
