// PKG-470: the memory a pull runner offers when it claims must be what its own admission would accept, or it claims tickets it then refuses.
import test from 'node:test';
import assert from 'node:assert/strict';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { realSrc, assertCanaryFails } from './helpers/mutant-module.js';
import { loadGlobalConfig } from '../src/config.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';
import { evaluateMemoryAdmission } from '../src/resources.js';

const CLAIMS = 'agent/claims.js';
const GIB = 2 ** 30;
const MIB = 2 ** 20;
const GLOBAL = { schedulerMode: 'active', capacity: 10, maxLaneWeight: 4, cpuAdmissionPercent: 100, cpuReserveCores: 1 };

const lease = (id, { requested, observed, observedAgeMs = 0 }) => ({
  id, key: `r:${id}`, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 1,
  state: LEASE_STATE.RUNNING, resources: { cpuCores: 1, memoryBytes: requested },
  ...(observed === undefined ? {} : { observedMemoryBytes: observed, observedAt: Date.now() - observedAgeMs }),
});

async function figuresFor(load, { total, available, reserve, leases }, executions = () => []) {
  const { claimFigures } = await load(CLAIMS);
  const { state, home } = freshEnv();
  const saved = [process.env.LANE_BROKER_HOME, process.env.LANE_BROKER_STATE];
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  try {
    writeGlobalConfig(home, { ...GLOBAL, memoryReserveBytes: reserve });
    const held = leases.map((spec, i) => lease(`held${i}`, spec));
    held.forEach((l) => writeLease(state, l));
    const cfg = loadGlobalConfig({ home });
    const offered = await claimFigures({ cfg, executions, memoryReader: () => ({ totalBytes: total, availableBytes: available, macPressure: null }) });
    assert.notEqual(offered, null, 'headroom could be measured');
    const admits = (candidateBytes) => evaluateMemoryAdmission({
      memoryInfo: { totalBytes: total, availableBytes: available }, heldLeases: held, candidateResources: { cpuCores: 1, memoryBytes: candidateBytes }, cfg,
    });
    return { freeMem: offered.freeMem, admits };
  } finally {
    for (const [key, value] of [['LANE_BROKER_HOME', saved[0]], ['LANE_BROKER_STATE', saved[1]]]) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

const MAC_GRANDY = {
  total: 8 * GIB, available: 5 * GIB, reserve: GIB,
  leases: [{ requested: 2 * GIB, observed: GIB / 2 }, { requested: 2 * GIB, observed: GIB / 2 }],
};

async function growthReserveScenario(load) {
  const { freeMem, admits } = await figuresFor(load, MAC_GRANDY);
  const candidate = 2 * GIB;
  assert.equal(admits(candidate).admit, false, 'admission refuses 2 GiB: held leases still have 3 GiB of growth to claim');
  assert.equal(admits(candidate).reason, 'memory-headroom');
  assert.ok(freeMem < candidate, `the claim must not offer ${freeMem} for a ${candidate} ticket admission refuses`);
}

test('a claim does not offer the memory held leases have requested but not yet used', () => growthReserveScenario(realSrc));

test('canary: offering the OS-available memory less the reserve fails the growth-reserve test', () => assertCanaryFails(CLAIMS,
  ['clampMem(admissibleMem - pendingMem)', 'clampMem(headroom.memoryBytes)'], growthReserveScenario));

const FIXTURES = [
  MAC_GRANDY,
  { total: 64 * GIB, available: 40 * GIB, reserve: 2 * GIB, leases: [] },
  { total: 16 * GIB, available: 14 * GIB, reserve: GIB, leases: [{ requested: 6 * GIB, observed: 6 * GIB }, { requested: 6 * GIB, observed: 7 * GIB }] },
  { total: 16 * GIB, available: 15 * GIB, reserve: GIB, leases: [{ requested: 6 * GIB, observed: GIB }, { requested: 6 * GIB, observed: GIB, observedAgeMs: 60_000 }] },
  { total: 32 * GIB, available: 30 * GIB, reserve: 4 * GIB, leases: [{ requested: 12 * GIB }, { requested: 12 * GIB, observed: 2 * GIB }] },
];

async function agreementScenario(load) {
  for (const [i, fixture] of FIXTURES.entries()) {
    const { freeMem, admits } = await figuresFor(load, fixture);
    assert.ok(freeMem > 0, `fixture ${i} leaves room to test`);
    assert.equal(admits(freeMem).admit, true, `fixture ${i}: admission takes exactly what the claim offers`);
    assert.equal(admits(freeMem + MIB).admit, false, `fixture ${i}: admission refuses anything more`);
  }
}

test('what a claim offers is the largest memory the same host admits, over varied leases', () => agreementScenario(realSrc));

test('canary: offering the OS-available memory less the reserve fails the agreement test', () => assertCanaryFails(CLAIMS,
  ['clampMem(admissibleMem - pendingMem)', 'clampMem(headroom.memoryBytes)'], agreementScenario));

async function pendingScenario(load) {
  const fixture = FIXTURES[1];
  const executions = () => [{ weight: 1, memoryBytes: 3 * GIB, laneId: null }, { weight: 1, memoryBytes: 5 * GIB, laneId: 'held0' }];
  const { freeMem } = await figuresFor(load, { ...fixture, leases: [{ requested: GIB, observed: GIB }] }, executions);
  const withLease = (await figuresFor(load, { ...fixture, leases: [{ requested: GIB, observed: GIB }] })).freeMem;
  assert.equal(freeMem, withLease - 3 * GIB, 'the unadmitted execution is charged once; the one whose lane is leased is already counted by its lease');
}

test('a claimed execution not yet admitted is charged against the offered memory once', () => pendingScenario(realSrc));

test('canary: not charging unadmitted executions fails the pending-memory test', () => assertCanaryFails(CLAIMS,
  ['clampMem(admissibleMem - pendingMem)', 'clampMem(admissibleMem)'], pendingScenario));
