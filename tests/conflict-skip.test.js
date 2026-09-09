import test from 'node:test';
import assert from 'node:assert/strict';
import { freshEnv } from './helpers.js';
import { enqueue, tryStart } from '../src/scheduler.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { writeLease, removeLease, LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';

/**
 * Tests for the second, separate body of work on this branch: a
 * conflict-blocked queue head is skipped in favor of the first
 * non-conflicting ticket, instead of "not the literal head" stalling
 * everyone behind it for the whole duration of the conflicting job. See
 * selectCandidate() in src/scheduler.js. This is NOT part of phase 1's
 * shadow-mode admission work above (tests/admission.test.js,
 * tests/cpu.test.js) — it changes real admission behavior today,
 * independent of schedulerMode.
 */

function baseCfg(overrides = {}) {
  return { ...DEFAULT_GLOBAL_CONFIG, capacity: 10, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, ...overrides };
}

function baseTicket(id, overrides = {}) {
  return {
    id,
    key: 'r:default',
    weight: 1,
    cwd: process.cwd(),
    cmd: ['true'],
    supervisorPid: process.pid,
    supervisorStart: null,
    logPath: '/dev/null',
    resultPath: '/dev/null',
    ...overrides,
  };
}

function heldLease(id, key, weight = 1) {
  return {
    id,
    key,
    bootId: bootId(),
    supervisorPid: process.pid,
    supervisorStart: null,
    childPgid: null,
    heartbeatAt: Date.now(),
    weight,
    state: LEASE_STATE.RUNNING,
  };
}

test('a conflict-blocked head is skipped so a later, non-conflicting ticket can start', async () => {
  const { state } = freshEnv();
  const globalCfg = baseCfg();
  writeLease(state, heldLease('holder', 'rouge:prepush'));

  const head = baseTicket('head-conflicting', { key: 'rouge:prepush' });
  const behind = baseTicket('behind-free', { key: 'zirkbot:default' });
  await enqueue(state, head);
  await enqueue(state, behind);

  const headResult = await tryStart(state, head, globalCfg);
  assert.equal(headResult.started, false);
  assert.equal(headResult.reason, 'not-head', 'the conflicting head is simply not selected this poll, not specially blocked');

  const behindResult = await tryStart(state, behind, globalCfg);
  assert.equal(behindResult.started, true, 'a later, non-conflicting ticket must be allowed to start ahead of a conflict-blocked head');
});

test('a ticket is NOT skipped merely for not fitting capacity', async () => {
  const { state } = freshEnv();
  const globalCfg = baseCfg({ capacity: 2 });
  // Uses the whole capacity but conflicts with no one.
  writeLease(state, heldLease('holder', 'other:key', 2));

  const head = baseTicket('heavy-head', { key: 'rouge:default', weight: 2 }); // no conflict, but 2 (held) + 2 > capacity 2
  const light = baseTicket('light-behind', { key: 'zirkbot:default', weight: 1 }); // would fit alone
  await enqueue(state, head);
  await enqueue(state, light);

  const headResult = await tryStart(state, head, globalCfg);
  assert.equal(headResult.started, false);
  assert.equal(headResult.reason, 'capacity');

  const lightResult = await tryStart(state, light, globalCfg);
  assert.equal(lightResult.started, false, 'capacity must never let a later ticket skip ahead');
  assert.equal(lightResult.reason, 'not-head');
});

test('FIFO order is preserved among runnable (non-conflicting) tickets', async () => {
  const { state } = freshEnv();
  const globalCfg = baseCfg();
  const first = baseTicket('first', { key: 'a:default' });
  const second = baseTicket('second', { key: 'b:default' });
  await enqueue(state, first);
  await enqueue(state, second);

  const secondResult = await tryStart(state, second, globalCfg);
  assert.equal(secondResult.started, false);
  assert.equal(secondResult.reason, 'not-head', 'an earlier non-conflicting ticket must still go first');

  const firstResult = await tryStart(state, first, globalCfg);
  assert.equal(firstResult.started, true);
});

test('the skipped head still starts as soon as its conflict clears', async () => {
  const { state } = freshEnv();
  const globalCfg = baseCfg();
  const holder = heldLease('holder', 'rouge:prepush');
  writeLease(state, holder);

  const head = baseTicket('head-conflicting', { key: 'rouge:prepush' });
  const behind = baseTicket('behind-free', { key: 'zirkbot:default' });
  await enqueue(state, head);
  await enqueue(state, behind);

  const behindResult = await tryStart(state, behind, globalCfg);
  assert.equal(behindResult.started, true);

  // Only `head` remains queued now, still blocked by the same holder.
  let headResult = await tryStart(state, head, globalCfg);
  assert.equal(headResult.started, false);
  assert.equal(headResult.reason, 'conflict');

  // The conflicting job ends.
  removeLease(state, holder.id);

  headResult = await tryStart(state, head, globalCfg);
  assert.equal(headResult.started, true, 'once the conflict clears, the previously-skipped head must start');
});

// --- Codex review finding #6: the starvation bound is not automatic with
// three or more conflicting keys. Head A conflicts with held B; a later C
// conflicts with A but not B, so C starts; when B finishes and a fresh
// B-like ticket arrives, it doesn't conflict with C (only with A), so it
// starts too — alternating B/C tickets can keep skipping past A forever
// unless something explicitly bounds it. ---

test('starvation bound: once the skip allowance is exhausted, a ticket that could BLOCK the head may no longer jump ahead — an unrelated one still may', async () => {
  const { state } = freshEnv();
  const limit = 3;
  const globalCfg = baseCfg({ conflictSkipLimit: limit });

  // A conflicts with both alternating keys; the alternating tickets conflict
  // with no one (selection only ever checks a candidate against currently
  // HELD leases, so it's irrelevant whether they "declare" a conflict back).
  const A = baseTicket('A', { key: 'a-key', conflicts: ['b-key', 'c-key'] });
  await enqueue(state, A);

  for (let i = 0; i < limit; i += 1) {
    const blockerKey = i % 2 === 0 ? 'b-key' : 'c-key';
    writeLease(state, heldLease(`blocker-${i}`, blockerKey));
    const other = baseTicket(`other-${i}`, { key: `other-key-${i}` });
    await enqueue(state, other);

    const aResult = await tryStart(state, A, globalCfg);
    assert.equal(aResult.started, false);
    assert.equal(aResult.reason, 'not-head', `cycle ${i}: A is skipped, not specially blocked, while under the skip limit`);

    const otherResult = await tryStart(state, other, globalCfg);
    assert.equal(otherResult.started, true, `cycle ${i}: skip count ${i} < limit ${limit}, so the non-conflicting ticket may still jump ahead`);

    removeLease(state, `blocker-${i}`); // that cycle's conflicting job "finishes"
  }

  // The skip allowance is now exhausted. One more conflicting job arrives...
  writeLease(state, heldLease('blocker-final', 'b-key'));

  const aResult2 = await tryStart(state, A, globalCfg);
  assert.equal(aResult2.started, false);
  assert.equal(aResult2.reason, 'conflict', 'once the allowance is exhausted, A is reported as conflict-blocked, not merely skipped — even though a restricted backfill candidate exists behind it');

  // A candidate whose key IS one of A's declared conflicts must still be
  // refused once exhausted: admitting it would renew A's blocker set
  // (Codex's rotation counterexample) — exactly what the skip limit exists
  // to bound, and rule (a) must keep bounding it forever, not just for the
  // first conflictSkipLimit skips. It is enqueued directly behind A, with
  // nothing else queued yet, so it is the very first candidate
  // selectBackfillCandidate examines — its rejection can only be rule (a)'s
  // doing. Without rule (a), the ordinary held-conflict check (selectCandidate)
  // finds nothing currently held conflicts with 'c-key' (only 'b-key' is
  // held right now), so this ticket WOULD be selected and started; enqueuing
  // it behind some other, already-eligible ticket would let that other
  // ticket win the slot first and make the assertion pass for the wrong
  // reason, same as the bug this test previously had (Codex review).
  const renewer = baseTicket('renewer', { key: 'c-key' });
  await enqueue(state, renewer);
  const renewerResult = await tryStart(state, renewer, globalCfg);
  assert.equal(renewerResult.started, false);
  assert.equal(renewerResult.reason, 'not-head', "a candidate that could become one of A's own blockers must never be admitted, exhausted or not");

  // An UNRELATED candidate cannot renew A's blocker set (BRAIN-202): the set
  // of things that could ever block A can only shrink from here, so
  // refusing it buys no safety and only idles the machine — this is the
  // exact 9-hour stall the restricted-backfill rule exists to fix.
  const another = baseTicket('another', { key: 'other-key-final' });
  await enqueue(state, another);
  const anotherResult = await tryStart(state, another, globalCfg);
  assert.equal(anotherResult.started, true, 'an unrelated candidate cannot block A, so it is still admitted once the skip allowance is exhausted');

  // ...and once that conflict actually clears, A finally starts.
  removeLease(state, 'blocker-final');
  const aResult3 = await tryStart(state, A, globalCfg);
  assert.equal(aResult3.started, true, 'once truly nothing conflicts, the previously-starved head starts');
});

// --- Codex's second vet finding: the test above only ever injects blockers
// via writeLease, so it never exercises the actual rotation scenario rule
// (a) exists for — a candidate carrying one of the head's declared conflict
// keys, held-conflict-free RIGHT NOW because nothing is currently holding
// that key, arriving through tryStart itself. This is the counterexample
// that got the first version of this fix rejected: admit it now, and a
// later admission at the OTHER declared key would alternate forever,
// letting A's conflict "clear" in name only. ---

test('rotation counterexample: a candidate at one of the head\'s declared conflict keys is refused even when nothing currently held conflicts with it', async () => {
  const { state } = freshEnv();
  const limit = 1;
  const globalCfg = baseCfg({ conflictSkipLimit: limit });

  const A = baseTicket('A', { key: 'a-key', conflicts: ['b-key', 'c-key'] });
  await enqueue(state, A);

  // A lease on A's OWN key keeps headConflicted true for the rest of the
  // test without ever touching b-key/c-key, so any refusal of a b-key/c-key
  // candidate below can only be rule (a) — never the ordinary held-conflict
  // check, which would trivially explain it away.
  writeLease(state, heldLease('self-blocker', 'a-key'));

  // Reach exhaustion via one ordinary skip (limit=1), same shape as the
  // pre-exhaustion loop in the test above.
  const filler = baseTicket('filler', { key: 'filler-key' });
  await enqueue(state, filler);
  const fillerResult = await tryStart(state, filler, globalCfg);
  assert.equal(fillerResult.started, true, 'ordinary skip-ahead still works before exhaustion');

  // b-key and c-key are genuinely free right now — nothing holds either.
  // Under the OLD unrestricted selectCandidate, a ticket at either key would
  // be perfectly eligible (nothing held conflicts with it); admitting it is
  // exactly the rotation Codex's counterexample describes.
  const atB = baseTicket('at-b', { key: 'b-key' });
  const atC = baseTicket('at-c', { key: 'c-key' });
  await enqueue(state, atB);
  await enqueue(state, atC);

  const atBResult = await tryStart(state, atB, globalCfg);
  assert.equal(atBResult.started, false);
  assert.equal(
    atBResult.reason,
    'not-head',
    "a candidate at one of A's declared conflict keys is refused even though nothing currently held conflicts with it",
  );

  const atCResult = await tryStart(state, atC, globalCfg);
  assert.equal(atCResult.started, false);
  assert.equal(
    atCResult.reason,
    'not-head',
    'same for the other declared conflict key — rule (a) excludes the whole declared set, not just whatever happens to be held right now',
  );
});

test('starvation bound (fail-closed): if the skip count cannot be durably recorded, the conflicted head is never skipped at all', async () => {
  const { state } = freshEnv();
  const globalCfg = baseCfg();
  writeLease(state, heldLease('holder', 'a-key'));

  // Make conflict-skip-state.json permanently unwritable: pre-create it as
  // a directory, so atomicWriteJson's final rename(tmp, file) always fails
  // (EISDIR) — a persistent failure, never healing, exactly the case Codex
  // review flagged (as opposed to one dropped write).
  const fs = await import('node:fs');
  const { paths } = await import('../src/state.js');
  fs.mkdirSync(paths(state).conflictSkipState);

  const head = baseTicket('head', { key: 'a-key' });
  const other = baseTicket('other', { key: 'other-key' });
  await enqueue(state, head);
  await enqueue(state, other);

  // `other` doesn't conflict with anything, so under a working recorder it
  // would be allowed to skip ahead (see the very first test above). With
  // recording permanently broken, it must be refused every single time,
  // not just once — a fail-open bug here specifically only shows up after
  // repeated attempts, not the first.
  for (let i = 0; i < 5; i += 1) {
    const otherResult = await tryStart(state, other, globalCfg);
    assert.equal(otherResult.started, false, `attempt ${i}: other must never be allowed to skip ahead while recording is broken`);
    assert.equal(otherResult.reason, 'not-head');
  }

  // The head itself is not punished by this: once its own conflict clears,
  // it starts normally (strict FIFO, not stuck).
  removeLease(state, 'holder');
  const headResult = await tryStart(state, head, globalCfg);
  assert.equal(headResult.started, true, 'the head must still start on its own once its conflict clears');
});

test('a corrupt/unreadable queue record behind a conflict-blocked head stops selection rather than being skipped over', async () => {
  const { state } = freshEnv();
  const globalCfg = baseCfg();
  writeLease(state, heldLease('holder', 'blocked:key'));

  const head = baseTicket('head-conflicting', { key: 'blocked:key' }); // queue[0], conflicts with holder
  const willBeCorrupted = baseTicket('will-be-corrupted', { key: 'other:key' }); // queue[1]
  const behind = baseTicket('behind-free', { key: 'zirkbot:default' }); // queue[2], no conflict
  await enqueue(state, head);
  await enqueue(state, willBeCorrupted);
  await enqueue(state, behind);

  // Corrupt the middle record directly on disk so listQueue() represents it
  // as `null`, simulating a torn/unreadable record — same effect as
  // Codex review finding #7 describes, but away from position 0 so it
  // actually exercises selectCandidate's own null-handling rather than
  // tryStart's separate "corrupt literal head" guard.
  const fs = await import('node:fs');
  const path = await import('node:path');
  const { paths } = await import('../src/state.js');
  const dir = paths(state).queue;
  const files = fs.readdirSync(dir).sort();
  fs.writeFileSync(path.join(dir, files[1]), 'not valid json{{{');

  const result = await tryStart(state, behind, globalCfg);
  assert.equal(result.started, false, 'a corrupt record must block everything behind it, never be silently skipped over');
  assert.equal(result.reason, 'not-head');
});
