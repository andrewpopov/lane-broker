import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { randomUUID } from 'node:crypto';
import { logins, call, enqueue, claim, tid } from './lane-dispatch-fixture.js';
import { agentPg } from './agent-harness.js';
import { scenarioSuite, happens, sleep } from './agent-run-harness.js';
import { readAgentStatus } from '../src/agent/store.js';
import { writeGlobalConfig } from './helpers.js';

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

const brokerGates = (rw, deviceGates) => writeGlobalConfig(rw.ctx.home, { remoteBlobCache: true, deviceGates });

/** A device sampler that answers only when the test feeds it a reading, so every sample is one deliberate step. */
function scriptedSampler() {
  const waiting = [];
  const sampler = async () => {
    const reading = await new Promise((resolve) => waiting.push(resolve));
    return { thermal: reading, power: {} };
  };
  const asked = () => happens('the agent asked for a sample', () => waiting.length > 0);
  const feed = async (reading) => {
    await asked();
    waiting.shift()(reading);
  };
  return { sampler, feed, asked, releaseAll: () => waiting.splice(0).forEach((resolve) => resolve({})) };
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
    const su = await w.su();
    const agent = await rw.agent({ load, config: { pollMs: 300 } });
    // every ping re-registers the runner, so the row is deleted again and again until the queue has answered `upgrade`
    const remove = setInterval(() => su.query("DELETE FROM lane_dispatch.runners WHERE host = 'a1'").catch(() => {}), 15);
    try {
      await happens(`the upgrade answer is logged (calls ${agent.names().join(',')}; logs ${rw.logs.join(' | ')})`, () => rw.logs.some((l) => /UPGRADE REQUIRED/.test(l)), 4000);
    } finally {
      clearInterval(remove);
    }
    const claimsThen = agent.callsTo('ld_claim').length;
    await sleep(900);
    assert.equal(agent.callsTo('ld_claim').length, claimsThen, 'no claim after the upgrade answer');
    assert.ok(rw.logs.filter((l) => /UPGRADE REQUIRED/.test(l)).length >= 2, 'logged on every tick');
  },

  'the drain marker is read from the configured stateDir, not from a hostile LANE_BROKER_STATE': async (w, rw, load) => {
    const saved = process.env.LANE_BROKER_STATE;
    process.env.LANE_BROKER_STATE = makeTmpDir('hostile-state-');
    try {
      fs.writeFileSync(path.join(rw.ctx.state, 'remote-drain.json'), JSON.stringify({ version: 1, reason: 'test', since: Date.now() }));
      await enqueue(await w.as(logins.s1), tid(1));
      const agent = await rw.agent({ load, config: { pollMs: 100 } });
      await sleep(600);
      assert.equal(agent.names().includes('ld_claim'), false, 'the drain marker in the configured state dir is honoured');
    } finally {
      process.env.LANE_BROKER_STATE = saved;
    }
  },

  'a closed thermal gate takes no claim and shows paused:thermal; it reopens only after openSamples clean samples, then the claim goes through': async (w, rw, load) => {
    const readings = { pressureLevel: 2 };
    let release;
    const secondCleanSample = new Promise((resolve) => { release = resolve; });
    let clean = 0;
    const deviceSampler = async () => {
      if (readings.pressureLevel === 0 && clean++ >= 1) await secondCleanSample;
      return { thermal: { ...readings }, power: {} };
    };
    const ticket = await rw.submit({ code: 'process.exit(0)' });
    const deviceGates = { thermal: { maxPressureLevel: 1, openSamples: 2 } };
    brokerGates(rw, deviceGates);
    try {
      const agent = await rw.agent({ load, deviceSampler, config: { deviceGates } });
      const gate = () => readAgentStatus(rw.root).deviceGates?.thermal;
      await happens('the gate closed and the status says why', () => gate()?.closed === true && readAgentStatus(rw.root).claims === 'paused:thermal');
      await sleep(400);
      assert.equal(agent.names().includes('ld_claim'), false, 'no claim while the gate is closed');
      assert.equal((await w.ticket(ticket.id)).epoch, 0);
      readings.pressureLevel = 0;
      await happens('one clean sample is counted and the gate stays closed', () => gate()?.consecutiveOk === 1);
      assert.deepEqual([gate().closed, gate().reading.pressureLevel, agent.names().includes('ld_claim')], [true, 0, false]);
      release();
      await rw.claimed(ticket);
      await happens('the gate shows open', () => gate()?.closed === false);
    } finally {
      release();
    }
  },

  'a closed thermal gate holds while the sampler fails or reads nothing: status stays paused:thermal and the countdown does not move': async (w, rw, load) => {
    let mode = 'hot';
    let failures = 0;
    const deviceSampler = async () => {
      if (mode === 'fail') {
        failures += 1;
        throw new Error('sensor exploded');
      }
      return { thermal: { pressureLevel: mode === 'hot' ? 2 : 0 }, power: {} };
    };
    const deviceGates = { thermal: { maxPressureLevel: 1, openSamples: 2 } };
    brokerGates(rw, deviceGates);
    const ticket = await rw.submit({ code: 'process.exit(0)' });
    const agent = await rw.agent({ load, deviceSampler, config: { deviceGates } });
    const gate = () => readAgentStatus(rw.root).deviceGates?.thermal;
    await happens('the gate closed', () => gate()?.closed === true);
    mode = 'fail';
    await happens('the sampler failed more often than openSamples', () => failures >= 4);
    await sleep(150);
    assert.deepEqual([gate().closed, gate().consecutiveOk, readAgentStatus(rw.root).claims], [true, 0, 'paused:thermal']);
    assert.equal(agent.names().includes('ld_claim'), false);
    assert.equal(rw.logs.filter((line) => /sampling failed/.test(line)).length, 1, 'one log line for one distinct error');
    mode = 'cool';
    await rw.claimed(ticket);
  },

  'thresholds are re-read from the broker config on each sample: an edit restarts the countdown and leaves the gate closed': async (w, rw, load) => {
    const { sampler, feed, asked, releaseAll } = scriptedSampler();
    const gates = (openSamples) => ({ thermal: { maxPressureLevel: 1, openSamples } });
    brokerGates(rw, gates(3));
    const gate = () => readAgentStatus(rw.root).deviceGates?.thermal;
    const hot = { pressureLevel: 2 };
    const cool = { pressureLevel: 0 };
    try {
      await rw.agent({ load, deviceSampler: sampler, config: { deviceGates: gates(3) } });
      await feed(hot);
      await happens('closed', () => gate()?.closed === true);
      await feed(cool);
      await happens('one clean sample', () => gate()?.consecutiveOk === 1);
      await asked();
      brokerGates(rw, gates(4));
      await feed(cool);
      await happens('two clean samples under the old thresholds', () => gate()?.consecutiveOk === 2);
      await feed(cool);
      await happens('the edit restarted the countdown, and the gate is still closed', () => gate()?.consecutiveOk === 1 && gate().closed === true);
      await asked();
      brokerGates(rw, { thermal: { maxPressureLevel: 9 } });
      await feed(cool);
      await feed(cool);
      await happens('the unreadable edit is logged once and the last good thresholds keep counting', () => gate()?.consecutiveOk === 3);
      assert.equal(rw.logs.filter((line) => /broker config unreadable/.test(line)).length, 1);
    } finally {
      releaseAll();
    }
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
const gateName = 'a closed thermal gate takes no claim and shows paused:thermal; it reopens only after openSamples clean samples, then the claim goes through';
const holdName = 'a closed thermal gate holds while the sampler fails or reads nothing: status stays paused:thermal and the countdown does not move';
const liveName = 'thresholds are re-read from the broker config on each sample: an edit restarts the countdown and leaves the gate closed';
const canaries = [
  [holdName, 'device-gates.js', ['state.closed && GATES[name].informative(gateCfg, reading)', 'state.closed']],
  [liveName, 'agent/run.js', ['    reloadDeviceGatesConfig();\n    if (!deviceGatesConfig)', '    if (!deviceGatesConfig)']],
  [gateName, CLAIMS, ['return closedGateReason(deviceGates);', 'return null;']],
  [gateName, 'agent/run.js', ['        await refreshDeviceGates();\n', '']],
  [gateName, 'device-gates.js', ['if (state.consecutiveOk >= openSamples) {', 'if (state.consecutiveOk >= 1) {']],
  ['the req is persisted before ld_claim is called, and a claimed answer leaves a FETCHING record and no pending req', CLAIMS, ['if (!pending) writePendingClaim(remoteRoot, req);', '']],
  ['a claim whose answer was lost is replayed under the same req however old it is, and adopts exactly one ticket', CLAIMS, ['const req = pending?.req ?? randomUUID();', 'const req = randomUUID();']],
  ['a crash after the record and before the pending delete: the replay adopts the record unchanged', CLAIMS, ['if (!core.table.get(execId)) core.table.put(', 'core.table.put(']],
  ['null headroom makes no claim and leaves no req; shadow mode claims a 64 GiB ticket with finite figures sent as a decimal string', CLAIMS,
    ["const offered = await figures({ executions: () => liveExecutions(core.table.all(), remoteRoot) });\n  if (!offered) return { status: 'skipped', reason: 'no-headroom' };", 'const offered = (await figures()) ?? { freeCpu: Infinity, freeMem: Infinity };'], /invalid input syntax for type bigint/],
  ['none leaves no pending req, and upgrade is answered, clears the req and is not an error', CLAIMS, ["a.status !== 'none' && a.status !== 'upgrade'", "a.status !== 'none'"], /ld_claim answered/],
  ['an upgrade answer stops the claim loop for good: no further ld_claim, and every tick logs it', 'agent/run.js', ["if (answer.status === 'upgrade') upgradeRequired = true;", 'if (false) upgradeRequired = true;']],
  ['the schedule window is respected: outside it, or too close to its close, no ld_claim is made', CLAIMS, ["if (!available) return 'outside-schedule';", '']],
  ['the schedule window is respected: outside it, or too close to its close, no ld_claim is made', CLAIMS, ["if (reason) return { status: 'skipped', reason };", '']],
];

scenarioSuite('agent claim handoff on a disposable Postgres', { scenarios, canaries });
