import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { logins, call, enqueue, claim, tid } from './lane-dispatch-fixture.js';
import { agentPg } from './agent-harness.js';
import { scenarioSuite, happens, sleep } from './agent-run-harness.js';

const open = () => null;
const offer = () => ({ freeCpu: 8, freeMem: 1e9 });

async function coreFor(rw, load, calls, onCall) {
  const { openAgentCore } = await load('agent/agent.js');
  const core = await openAgentCore({
    remoteRoot: rw.root, config: { pg: agentPg(rw.w), schedule: null }, version: 't', log: () => {}, backoff: { minMs: 20, maxMs: 100 },
    onCall: (name, args) => { calls.push([name, args]); onCall?.(name, args); },
  });
  rw.agents.push({ stop: () => core.close(), core });
  return core;
}

const SCHEDULE = { timezone: 'UTC', available: [{ days: ['mon'], from: '09:00', to: '10:00' }] };
const MONDAY = (hh, mm) => Date.UTC(2026, 10, 16, hh, mm);

const scenarios = {
  'the req is persisted before ld_claim is called, and a claimed answer leaves a FETCHING record and no pending req': async (w, rw, load) => {
    const { claimOnce } = await load('agent/claims.js');
    const { readPendingClaim } = await load('agent/store.js');
    await enqueue(await w.as(logins.s1), tid(1));
    const calls = [];
    const core = await coreFor(rw, load, calls, (name, args) => { if (name === 'ld_claim') assert.equal(readPendingClaim(rw.root)?.req, args[0], 'the pending req is on disk before the call'); });
    const answer = await claimOnce({ core, remoteRoot: rw.root, figures: offer, block: open });
    assert.equal(answer.status, 'claimed');
    assert.equal(core.table.get(answer.execId).subphase, 'FETCHING');
    assert.equal(readPendingClaim(rw.root), null);
  },

  'a claim whose answer was lost is replayed under the same req however old it is, and adopts exactly one ticket': async (w, rw, load) => {
    const { claimOnce } = await load('agent/claims.js');
    const { writePendingClaim } = await load('agent/store.js');
    const [s1, a1] = [await w.as(logins.s1), await w.as(logins.a1)];
    await enqueue(s1, tid(1));
    await enqueue(s1, tid(2));
    const req = randomUUID();
    const lost = await claim(a1, { req });
    writePendingClaim(rw.root, req);
    const calls = [];
    const core = await coreFor(rw, load, calls);
    const answer = await claimOnce({ core, remoteRoot: rw.root, figures: offer, block: open, now: () => Date.now() + 24 * 3_600_000 });
    assert.deepEqual(calls.filter(([n]) => n === 'ld_claim').map(([, a]) => a[0]), [req]);
    assert.equal(answer.execId, `${lost.id}.e${lost.epoch}`);
    assert.equal(core.table.get(answer.execId).token, lost.token);
    assert.equal((await w.ticket(tid(2))).state, 'queued', 'the second ticket was not claimed');
  },

  'a crash after the record and before the pending delete: the replay adopts the record unchanged': async (w, rw, load) => {
    const { claimOnce } = await load('agent/claims.js');
    const { newExecutionRecord, readPendingClaim, writePendingClaim } = await load('agent/store.js');
    await enqueue(await w.as(logins.s1), tid(1));
    const req = randomUUID();
    const lost = await claim(await w.as(logins.a1), { req });
    const core = await coreFor(rw, load, []);
    const execId = `${lost.id}.e${lost.epoch}`;
    core.table.put({ ...newExecutionRecord(lost), subphase: 'STARTING', claimedAt: 12345 });
    writePendingClaim(rw.root, req);
    await claimOnce({ core, remoteRoot: rw.root, figures: offer, block: open });
    assert.deepEqual([core.table.get(execId).subphase, core.table.get(execId).claimedAt], ['STARTING', 12345], 'the record was not reset');
    assert.equal(readPendingClaim(rw.root), null);
  },

  'null headroom makes no claim and leaves no req; shadow mode claims a 64 GiB ticket with finite figures sent as a decimal string': async (w, rw, load) => {
    const { claimOnce, claimFigures, MAX_FREE_MEM } = await load('agent/claims.js');
    const { readPendingClaim } = await load('agent/store.js');
    await call(await w.as(logins.s1), 'ld_enqueue', tid(1), 'repoA', 'default', 'lk', { protocol: 1 }, {}, { key: 'k', sha256: 'x', bytes: 1, blobs: [] }, [], 1, 64 * 2 ** 30, 0);
    const calls = [];
    const core = await coreFor(rw, load, calls);
    const none = await claimOnce({ core, remoteRoot: rw.root, figures: () => null, block: open });
    assert.deepEqual(none, { status: 'skipped', reason: 'no-headroom' });
    assert.deepEqual([calls.filter(([n]) => n === 'ld_claim').length, readPendingClaim(rw.root)], [0, null]);
    const figures = () => claimFigures({ cfg: { schedulerMode: 'shadow', cpuReserveCores: 1, cpuAdmissionPercent: 100, memoryReserveBytes: 2 ** 31 }, host: { cpuCores: 16, memoryBytes: Number.MAX_VALUE } });
    const offered = await figures();
    assert.deepEqual([Number.isFinite(offered.freeCpu), offered.freeMem], [true, MAX_FREE_MEM]);
    const claimed = await claimOnce({ core, remoteRoot: rw.root, figures, block: open });
    assert.equal(claimed.status, 'claimed');
    assert.equal(typeof calls.find(([n]) => n === 'ld_claim')[1][2], 'string');
  },

  'none leaves no pending req, and upgrade is answered, clears the req and is not an error': async (w, rw, load) => {
    const { claimOnce } = await load('agent/claims.js');
    const { readPendingClaim } = await load('agent/store.js');
    const core = await coreFor(rw, load, []);
    assert.deepEqual(await claimOnce({ core, remoteRoot: rw.root, figures: offer, block: open }), { status: 'none' });
    assert.equal(readPendingClaim(rw.root), null);
    await (await w.su()).query("DELETE FROM lane_dispatch.runners WHERE host = 'a1'");
    assert.deepEqual(await claimOnce({ core, remoteRoot: rw.root, figures: offer, block: open }), { status: 'upgrade' });
    assert.equal(readPendingClaim(rw.root), null);
  },

  'an upgrade answer stops the claim loop for good: no further ld_claim, and every tick logs it': async (w, rw, load) => {
    let armed = false;
    const agent = await rw.agent({ load, config: { pollMs: 100 }, figures: () => (armed ? offer() : null) });
    await (await w.su()).query("DELETE FROM lane_dispatch.runners WHERE host = 'a1'");
    armed = true;
    await happens('the upgrade answer is logged', () => rw.logs.some((l) => /UPGRADE REQUIRED/.test(l)));
    const claimsThen = agent.callsTo('ld_claim').length;
    await sleep(600);
    assert.equal(agent.callsTo('ld_claim').length, claimsThen, 'no claim after the upgrade answer');
    assert.ok(rw.logs.filter((l) => /UPGRADE REQUIRED/.test(l)).length >= 2, 'logged on every tick');
  },

  'the schedule window is respected: outside it, or too close to its close, no ld_claim is made': async (w, rw, load) => {
    const { claimBlock, claimOnce } = await load('agent/claims.js');
    const block = (now) => claimBlock({ schedule: SCHEDULE, stopClaimingBeforeCloseMs: 30 * 60_000, now, draining: () => false });
    assert.equal(block(MONDAY(9, 10)), null);
    assert.equal(block(MONDAY(9, 45)), 'closing-soon');
    assert.equal(block(MONDAY(11, 0)), 'outside-schedule');
    assert.equal(block(MONDAY(9, 10) + 24 * 3_600_000), 'outside-schedule');
    await enqueue(await w.as(logins.s1), tid(1));
    const calls = [];
    const core = await coreFor(rw, load, calls);
    const answer = await claimOnce({ core, remoteRoot: rw.root, figures: offer, block: () => block(MONDAY(11, 0)) });
    assert.deepEqual(answer, { status: 'skipped', reason: 'outside-schedule' });
    assert.equal(calls.some(([n]) => n === 'ld_claim'), false);
  },
};

const CLAIMS = 'agent/claims.js';
const canaries = [
  ['the req is persisted before ld_claim is called, and a claimed answer leaves a FETCHING record and no pending req', CLAIMS, ['if (!pending) writePendingClaim(remoteRoot, req);', '']],
  ['a claim whose answer was lost is replayed under the same req however old it is, and adopts exactly one ticket', CLAIMS, ['const req = pending?.req ?? randomUUID();', 'const req = randomUUID();']],
  ['a crash after the record and before the pending delete: the replay adopts the record unchanged', CLAIMS, ['if (!core.table.get(execId)) core.table.put(', 'core.table.put(']],
  ['null headroom makes no claim and leaves no req; shadow mode claims a 64 GiB ticket with finite figures sent as a decimal string', CLAIMS,
    ["const offered = await figures();\n  if (!offered) return { status: 'skipped', reason: 'no-headroom' };", 'const offered = (await figures()) ?? { freeCpu: Infinity, freeMem: Infinity };'], /invalid input syntax for type bigint/],
  ['none leaves no pending req, and upgrade is answered, clears the req and is not an error', CLAIMS, ["a.status !== 'none' && a.status !== 'upgrade'", "a.status !== 'none'"], /ld_claim answered/],
  ['an upgrade answer stops the claim loop for good: no further ld_claim, and every tick logs it', 'agent/run.js', ["if (answer.status === 'upgrade') upgradeRequired = true;", 'if (false) upgradeRequired = true;']],
  ['the schedule window is respected: outside it, or too close to its close, no ld_claim is made', CLAIMS, ["if (!available) return 'outside-schedule';", '']],
  ['the schedule window is respected: outside it, or too close to its close, no ld_claim is made', CLAIMS, ["if (reason) return { status: 'skipped', reason };", '']],
];

scenarioSuite('agent claim handoff on a disposable Postgres', { scenarios, canaries });
