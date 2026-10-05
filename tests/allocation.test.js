import test from 'node:test';
import assert from 'node:assert/strict';
import { classOf, simArmed, classLocks, usedByClass, evaluateCandidate, evaluateQueue, headReservationOf, DEFAULT_SIM_ARM_WINDOW_MS } from '../src/allocation.js';
import { LEASE_STATE } from '../src/lease.js';

const NOW = 1_000_000;
const CFG = { simArmWindowMs: DEFAULT_SIM_ARM_WINDOW_MS };

const lease = (id, klass, cores, state = LEASE_STATE.RUNNING) => ({ id, class: klass, state, weight: cores, resources: { cpuCores: cores, memoryBytes: 1 } });
const ticket = (id, klass, cores, extra = {}) => ({ id, class: klass, key: id, weight: cores, resources: { cpuCores: cores, ...extra } });

/** Admit 1-core tickets of one class one at a time against the live locks until the first refusal. */
function fillOneCoreClass(klass, B, armed, start) {
  let usedT = start.test;
  let usedS = start.sim;
  let admitted = 0;
  for (;;) {
    const v = evaluateCandidate({ candidate: ticket(`${klass}${admitted}`, klass, 1), cfg: CFG, B, usedT, usedS, armed });
    if (!v.eligible) return { admitted, usedT, usedS };
    admitted += 1;
    if (klass === 'test') usedT += 1;
    else usedS += 1;
  }
}

test('classOf defaults to test for old leases and tickets', () => {
  assert.equal(classOf({}), 'test');
  assert.equal(classOf(undefined), 'test');
  assert.equal(classOf({ class: 'sim' }), 'sim');
  assert.equal(classOf({ class: 'bogus' }), 'test');
});

test('fit test rounds: a fractional lock reserves its full real value', () => {
  const near = (actual, expected) => assert.ok(Math.abs(actual - expected) < 1e-9, `${actual} != ${expected}`);
  for (const [B, lt, ls] of [[15, 2.25, 4.5], [7.5, 1.125, 2.25], [3, 0.45, 0.9]]) {
    const locks = classLocks({ B, armed: true });
    near(locks.L_t, lt);
    near(locks.L_s, ls);
  }
});

test('sim lock is armed by a queued sim, a charged sim, or the 5-min tail after the last demand', () => {
  assert.equal(simArmed({ now: NOW }), false);
  assert.equal(simArmed({ now: NOW, simQueued: true }), true);
  assert.equal(simArmed({ now: NOW, simCharged: true }), true);
  assert.equal(simArmed({ now: NOW, lastSimDemandAt: NOW - 299_999 }), true);
  assert.equal(simArmed({ now: NOW, lastSimDemandAt: NOW - 300_000 }), true);
  assert.equal(simArmed({ now: NOW, lastSimDemandAt: NOW - 300_001 }), false);
  assert.equal(simArmed({ now: NOW, lastSimDemandAt: NOW - 10_000, simArmWindowMs: 5_000 }), false);
});

test('a disarmed sim lock is zero and the test lock stays permanent', () => {
  assert.deepEqual(classLocks({ B: 15, armed: false }), { L_t: 2.25, L_s: 0 });
});

test('usedByClass charges RUNNING and ORPHANED leases per class and never a DONE or synthetic one', () => {
  const head = ticket('head', 'sim', 4);
  const used = usedByClass([lease('a', 'test', 3), lease('b', 'sim', 1), lease('c', undefined, 2, LEASE_STATE.ORPHANED), lease('d', 'test', 9, LEASE_STATE.DONE), headReservationOf(head)], NOW, CFG);
  assert.deepEqual(used, { test: 5, sim: 1 });
});

test('a synthetic head reservation carries the head class but is never counted in used_c', () => {
  const head = ticket('head', 'sim', 4);
  const reservation = headReservationOf(head);
  assert.equal(reservation.class, 'sim');
  assert.equal(reservation.synthetic, true);
  assert.deepEqual(usedByClass([reservation], NOW, CFG), { test: 0, sim: 0 });
  // even if a caller forgets to mark it synthetic, a reservation has no lease state and is not charged
  assert.deepEqual(usedByClass([{ ...reservation, synthetic: false }], NOW, CFG), { test: 0, sim: 0 });
});

test('usedByClass reuses leaseDemandBasis: a fresh observation above the booking is charged', () => {
  const observed = { ...lease('a', 'test', 2), observedCpuCores: 5, observedAt: NOW - 1000 };
  assert.deepEqual(usedByClass([observed], NOW, CFG), { test: 5, sim: 0 });
});

test('trace fixture: B=15, sims only, 1-core sims fill to exactly 12 (1 <= 15 - used_s - 2.25 fails at 12)', () => {
  const sims = fillOneCoreClass('sim', 15, true, { test: 0, sim: 0 });
  assert.equal(sims.usedS, 12);
});

test('trace fixture: B=15, sims armed, 1-core tests only fill to exactly 10 (L_s = 4.5)', () => {
  const tests = fillOneCoreClass('test', 15, true, { test: 0, sim: 0 });
  assert.equal(tests.usedT, 10);
});

test('trace fixture: with sims disarmed tests fill the whole budget', () => {
  assert.equal(fillOneCoreClass('test', 15, false, { test: 0, sim: 0 }).usedT, 15);
});

test('the permanent test lock blocks a sim claim and names the class lock', () => {
  const fits = evaluateCandidate({ candidate: ticket('s', 'sim', 1), cfg: CFG, B: 15, usedT: 0, usedS: 11, armed: true });
  assert.equal(fits.eligible, true);
  assert.equal(fits.reservedOther, 2.25);
  const blocked = evaluateCandidate({ candidate: ticket('s', 'sim', 1), cfg: CFG, B: 15, usedT: 0, usedS: 12, armed: true });
  assert.equal(blocked.eligible, false);
  assert.equal(blocked.reason, 'class-lock');
  // once tests hold their lock the reservation releases the sim
  const released = evaluateCandidate({ candidate: ticket('s', 'sim', 1), cfg: CFG, B: 15, usedT: 3, usedS: 11, armed: true });
  assert.equal(released.eligible, true);
  assert.equal(released.reservedOther, 0);
});

test('an oversize test claim is clamped to B - L_s when sims can arm, and is eligible at the clamp', () => {
  const v = evaluateCandidate({ candidate: ticket('t', 'test', 14), cfg: CFG, B: 15, usedT: 0, usedS: 0, armed: true });
  assert.equal(v.claim, 14);
  assert.equal(v.clampedClaim, 10); // floor(15 - 4.5), a whole-core grant
  assert.equal(v.eligible, true);
  const unclamped = evaluateCandidate({ candidate: ticket('t', 'test', 14), cfg: CFG, B: 15, usedT: 0, usedS: 0, armed: false, simsCanArm: false });
  assert.equal(unclamped.clampedClaim, 14);
  const simBig = evaluateCandidate({ candidate: ticket('s', 'sim', 14), cfg: CFG, B: 15, usedT: 0, usedS: 0, armed: true });
  assert.equal(simBig.clampedClaim, 14);
});

test('a test claim is held back by an armed sim lock that is not yet used, and not when disarmed', () => {
  const armed = evaluateCandidate({ candidate: ticket('t', 'test', 12), cfg: CFG, B: 15, usedT: 0, usedS: 0, armed: true });
  assert.equal(armed.reservedOther, 4.5);
  assert.equal(armed.clampedClaim, 10);
  const heavy = evaluateCandidate({ candidate: ticket('t', 'test', 2), cfg: CFG, B: 15, usedT: 10, usedS: 0, armed: true });
  assert.equal(heavy.eligible, false);
  assert.equal(heavy.reason, 'class-lock');
  const disarmed = evaluateCandidate({ candidate: ticket('t', 'test', 2), cfg: CFG, B: 15, usedT: 10, usedS: 0, armed: false });
  assert.equal(disarmed.eligible, true);
});

test('externalBusy composes with reserved_other: the smaller limit wins', () => {
  const base = { cfg: CFG, B: 15, usedT: 8, usedS: 0, armed: true };
  // reservedOther = 4.5 binds over externalBusy = 1: limit = 15 - 8 - 4.5 = 2.5
  const classBinds = evaluateCandidate({ ...base, candidate: ticket('t', 'test', 2), externalBusy: 1 });
  assert.equal(classBinds.limit, 2.5);
  assert.equal(classBinds.eligible, true);
  assert.equal(evaluateCandidate({ ...base, candidate: ticket('t', 'test', 3), externalBusy: 1 }).reason, 'class-lock');
  // externalBusy = 6 binds over reservedOther = 4.5: limit = 15 - 8 - 6 = 1, and the denial is not the class's fault
  const externalBinds = evaluateCandidate({ ...base, candidate: ticket('t', 'test', 2), externalBusy: 6 });
  assert.equal(externalBinds.limit, 1);
  assert.equal(externalBinds.eligible, false);
  assert.equal(externalBinds.reason, 'over-free');
});

test('the idle overshoot exemption never bypasses a class lock', () => {
  // a claim over the whole budget is refused without the exemption
  const over = evaluateCandidate({ candidate: ticket('s', 'sim', 16), cfg: CFG, B: 15, usedT: 0, usedS: 0, armed: true, idleExempt: false });
  assert.equal(over.eligible, false);
  // exemption waives over-free only when the class lock would still have allowed it
  const exempt = evaluateCandidate({ candidate: ticket('s', 'sim', 12), cfg: CFG, B: 15, usedT: 0, usedS: 0, armed: true, externalBusy: 5, idleExempt: true });
  assert.equal(exempt.eligible, true);
  const locked = evaluateCandidate({ candidate: ticket('s', 'sim', 13), cfg: CFG, B: 15, usedT: 0, usedS: 0, armed: true, idleExempt: true });
  assert.equal(locked.reason, 'class-lock');
  assert.equal(locked.eligible, false);
});

test('an elastic ticket is judged at its ceil(minCpuCores) floor', () => {
  const base = { cfg: CFG, B: 15, usedT: 4, usedS: 0, armed: true };
  assert.equal(evaluateCandidate({ ...base, candidate: ticket('t', 'test', 8) }).eligible, false); // limit 15 - 4 - 4 = 7
  const elastic = evaluateCandidate({ ...base, candidate: ticket('t', 'test', 8, { minCpuCores: 2.5 }) });
  assert.equal(elastic.eligible, true); // floor 3 <= 7
  assert.equal(elastic.claim, 8);
});

const SKIP = { limit: 2, used: 0 };
const run = (over) => evaluateQueue({ cfg: CFG, B: 15, now: NOW, held: [], skipBudget: SKIP, ...over });

test('backfill past a class-ineligible head picks the smallest eligible claim and consumes the skip budget', () => {
  const held = [lease('r', 'test', 9), lease('s', 'sim', 4)];
  const queue = [ticket('head', 'test', 3), ticket('big', 'sim', 3), ticket('small', 'sim', 1), ticket('mid', 'test', 1)];
  const out = run({ held, queue });
  // head claim 3 > free 2: a cpu denial, so backfill past it is the BRAIN-346 shape
  assert.equal(out.decisions[0].eligible, false);
  assert.equal(out.selection, 'small');
  assert.equal(out.skipBudget.consumed, 1);
  assert.equal(out.skipBudget.remaining, 1);
});

test('a class-locked head (not a cpu denial) is skipped and the skip budget is the same one cpu skips use', () => {
  const held = [lease('r', 'test', 10)];
  const queue = [ticket('head', 'test', 2), ticket('s1', 'sim', 1), ticket('s0', 'sim', 1)];
  const out = run({ held, queue });
  assert.equal(out.armed, true);
  assert.equal(out.decisions[0].reason, 'class-lock');
  assert.equal(out.selection, 's1'); // earliest on equal claims
  assert.equal(out.decisions[0].skipped, true);
  assert.equal(out.skipBudget.consumed, 1);
  const exhausted = run({ held, queue, skipBudget: { limit: 2, used: 2 } });
  assert.equal(exhausted.selection, null);
  assert.equal(exhausted.selectionReason, 'skip-budget-exhausted');
  assert.equal(exhausted.skipBudget.consumed, 0);
});

test('class-ineligible candidates are filtered out before the smallest claim is chosen', () => {
  // 11 test cores held; the armed sim lock keeps tests out, so the smaller TEST claim must not win
  const held = [lease('r', 'test', 11)];
  const queue = [ticket('head', 'test', 4), ticket('t1', 'test', 1), ticket('s2', 'sim', 2)];
  const out = run({ held, queue });
  assert.equal(out.decisions[1].eligible, false);
  assert.equal(out.decisions[1].reason, 'class-lock');
  assert.equal(out.selection, 's2');
});

test('an eligible head is selected with no skip consumed', () => {
  const out = run({ queue: [ticket('head', 'test', 2), ticket('x', 'sim', 1)] });
  assert.equal(out.selection, 'head');
  assert.equal(out.skipBudget.consumed, 0);
});

test('a live reservation wins over class eligibility: no backfill past a reserved head', () => {
  const held = [lease('r', 'test', 10)];
  const queue = [ticket('head', 'test', 2), ticket('s1', 'sim', 1)];
  const out = run({ held, queue, reservation: { headId: 'head', reserved: true } });
  assert.equal(out.decisions[1].eligible, true);
  assert.equal(out.selection, null);
  assert.equal(out.selectionReason, 'reserved-head');
});

test('BRAIN-355 safe backfill stays uncounted, reserves the head claim, and works with an exhausted budget', () => {
  const held = [lease('r', 'test', 6)];
  const queue = [ticket('head', 'test', 6), ticket('safe', 'test', 1), ticket('toobig', 'test', 4)];
  const out = run({
    held,
    queue,
    skipBudget: { limit: 2, used: 2 },
    conflictBlocked: (t) => t.id === 'head',
    safeBackfill: (t) => t.id !== 'head',
  });
  assert.equal(out.decisions[0].reason, 'conflict');
  // free 9, minus head reservation 6 = 3: 'safe' fits, 'toobig' does not; used_c never includes the reservation
  assert.equal(out.decisions[2].eligible, false);
  assert.equal(out.selection, 'safe');
  assert.equal(out.selectionReason, 'safe-backfill');
  assert.equal(out.skipBudget.consumed, 0);
  assert.equal(out.used.test, 6);
});

test('an unreadable queue entry stops the walk', () => {
  const held = [lease('r', 'test', 10)];
  const out = run({ held, queue: [ticket('head', 'test', 2), null, ticket('s1', 'sim', 1)] });
  assert.equal(out.selection, null);
  assert.equal(out.decisions.length, 2);
});

test('a sim ticket queued arms the lock for tests in the same evaluation', () => {
  const held = [lease('r', 'test', 10)];
  const noSim = run({ held, queue: [ticket('head', 'test', 2)] });
  assert.equal(noSim.armed, false);
  assert.equal(noSim.selection, 'head');
  const withSim = run({ held, queue: [ticket('head', 'test', 2), ticket('s', 'sim', 1)] });
  assert.equal(withSim.armed, true);
  assert.equal(withSim.selection, 's');
});

test('evaluateQueue never mutates its inputs', () => {
  const held = [lease('r', 'test', 10)];
  const queue = [ticket('head', 'test', 2), ticket('s1', 'sim', 1)];
  const skipBudget = { limit: 2, used: 0 };
  const snapshot = JSON.stringify({ held, queue, skipBudget });
  run({ held, queue, skipBudget });
  assert.equal(JSON.stringify({ held, queue, skipBudget }), snapshot);
});

test('the arm window decides whether tests are held back: 4m59s ago is armed, 5m01s ago is disarmed, same B', () => {
  const testAtTen = (lastSimDemandAt) => {
    const out = run({ held: [lease('r', 'test', 10)], queue: [ticket('head', 'test', 2)], lastSimDemandAt });
    return { armed: out.armed, head: out.decisions[0] };
  };
  const recent = testAtTen(NOW - (4 * 60 + 59) * 1000);
  assert.equal(recent.armed, true);
  assert.equal(recent.head.eligible, false);
  assert.equal(recent.head.reason, 'class-lock');
  const lapsed = testAtTen(NOW - (5 * 60 + 1) * 1000);
  assert.equal(lapsed.armed, false);
  assert.equal(lapsed.head.eligible, true);
  assert.equal(lapsed.head.reservedOther, 0);
});

test('only an OVERSIZED test claim is clamped: fractional and in-range claims are left untouched', () => {
  const clampOf = (claim, B) => evaluateCandidate({ candidate: ticket('t', 'test', claim), cfg: CFG, B, usedT: 0, usedS: 0, armed: true }).clampedClaim;
  assert.equal(clampOf(1.5, 15), 1.5, 'a fractional in-range claim is not floored to 1');
  assert.equal(clampOf(10.4, 15), 10.4, 'at B - L_s = 10.5 and below: untouched');
  assert.equal(clampOf(11, 15), 10, 'above B - L_s: clamped to floor(B - L_s)');
  assert.equal(clampOf(3, 1), 1, 'tiny-B edge: B - L_s = 0.7 < 1, the grant is 1 core, never 0');
  assert.equal(evaluateCandidate({ candidate: ticket('t', 'test', 3), cfg: CFG, B: 1, usedT: 0, usedS: 0, armed: true }).reason, 'class-lock', 'and it is class-locked while sims are armed (it runs once they disarm)');
  assert.equal(evaluateCandidate({ candidate: ticket('t', 'test', 3), cfg: CFG, B: 1, usedT: 0, usedS: 0, armed: false }).eligible, true);
});

test('a safe backfill leaves room for the head\'s FULL claim (as BRAIN-355 live does), not its elastic floor', () => {
  // B=15, 6 held, head wants 8 (floor 2), the backfill candidate wants 2: 15 - 6 - 8 = 1 < 2
  const held = [lease('r', 'test', 6)];
  const head = ticket('head', 'test', 8, { minCpuCores: 2 });
  const out = run({ held, queue: [head, ticket('bf', 'test', 2)], conflictBlocked: (t) => t.id === 'head', safeBackfill: (t) => t.id === 'bf', skipBudget: { limit: 3, used: 3 } });
  assert.equal(out.decisions[1].reservedHead, 8);
  assert.equal(out.decisions[1].eligible, false);
  assert.equal(out.selection, null);
  const fits = run({ held, queue: [head, ticket('bf', 'test', 1)], conflictBlocked: (t) => t.id === 'head', safeBackfill: (t) => t.id === 'bf', skipBudget: { limit: 3, used: 3 } });
  assert.equal(fits.selection, 'bf');
});

test('existing guards (weight capacity, memory, pause...) veto a candidate the class locks would admit', () => {
  const guarded = run({ queue: [ticket('head', 'test', 3)], existingGuards: (t) => (t.id === 'head' ? ['capacity'] : []) });
  assert.equal(guarded.selection, null);
  assert.equal(guarded.decisions[0].reason, 'capacity');
  assert.deepEqual(guarded.decisions[0].guards, ['capacity']);
  assert.equal(run({ queue: [ticket('head', 'test', 3)] }).selection, 'head');
});

test('charges supplied by the live evaluation replace the recomputed per-lease demand', () => {
  const held = [lease('r', 'test', 2), lease('s', 'sim', 1)];
  assert.deepEqual(usedByClass(held, NOW, CFG), { test: 2, sim: 1 });
  assert.deepEqual(usedByClass(held, NOW, CFG, new Map([['r', 5.5]])), { test: 5.5, sim: 1 }, 'a lease absent from the map is still recomputed');
  assert.equal(run({ held, queue: [], charges: new Map([['r', 5.5]]) }).used.test, 5.5);
});
