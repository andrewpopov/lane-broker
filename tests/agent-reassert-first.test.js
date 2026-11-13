import { describe, test, before, after, afterEach, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, enqueue, claim, tid, simulateRestore } from './lane-dispatch-fixture.js';
import { openAgent, closeAgents, agentPg, backendPids, terminateBackend, sleep, takeTicket } from './agent-harness.js';
import { openAgentCore } from '../src/agent/agent.js';
import { LdClient } from '../src/agent/ld-client.js';
import { ExecutionTable, ensureAgentDirs, newExecutionRecord } from '../src/agent/store.js';
import { importMutant } from './helpers/mutant-module.js';

const FIRST_CALLS = ['ld_reassert', 'ld_register'];

const recordFor = (claimed, subphase) => ({ ...newExecutionRecord({ repo_key: 'repoA', lane: 'test', binding: {}, header: {}, snapshot: {}, ...claimed }), subphase });

const SCENARIOS = {
  'T7 a client that has never connected sends ld_reassert then ld_register before the claim it was asked for': async (w, { LdClient }) => {
    await enqueue(await w.as(logins.s1), tid(1));
    const { client, names } = await openAgent(w, { open: false, LdClient });
    assert.equal((await client.claim(randomUUID(), 8, 1e9)).status, 'claimed');
    assert.deepEqual(names(), [...FIRST_CALLS, 'ld_claim']);
  },

  'T7 a heartbeat that answers reassert closes the gate: the next calls are ld_reassert, ld_register, ld_heartbeat, and the row ends running': async (w, { LdClient }) => {
    const held = [];
    const { client, names } = await openAgent(w, { snapshot: () => held, LdClient });
    const c = await takeTicket(w, client, held);
    await simulateRestore(w);
    assert.equal((await client.heartbeat(c.id, c.epoch, c.token)).reassert, true);
    assert.equal((await w.ticket(c.id)).state, 'quarantined');
    const before = names().length;
    const next = await client.heartbeat(c.id, c.epoch, c.token);
    assert.deepEqual(names().slice(before), [...FIRST_CALLS, 'ld_heartbeat']);
    assert.equal(next.reassert, undefined);
    assert.equal((await w.ticket(c.id)).state, 'running');
  },

  'T7 after the guard terminates the call backend the next calls are ld_reassert then ld_register': async (w) => {
    const held = [];
    const { client, names } = await openAgent(w, { snapshot: () => held });
    const c = await takeTicket(w, client, held);
    const [pid] = await backendPids(w, 'SELECT lane_dispatch%');
    const before = names().length;
    await terminateBackend(w, pid);
    await sleep(100);
    await client.heartbeat(c.id, c.epoch, c.token);
    const after = names().slice(before);
    assert.deepEqual(after.slice(-3), [...FIRST_CALLS, 'ld_heartbeat']);
    assert.ok(after.indexOf('ld_reassert') < after.indexOf('ld_register'), 'nothing registers before the reassert');
  },

  'T7 a running-phase record whose row the queue still holds claimed is an anomaly: logged, judged as fenced, never resumed': async (w, { remoteRoot, cores }) => {
    await enqueue(await w.as(logins.s1), tid(1));
    const claimed = await claim(await w.as(logins.a1));
    ensureAgentDirs(remoteRoot);
    new ExecutionTable(remoteRoot).put(recordFor(claimed, 'RUNNING'));
    const log = [];
    const calls = [];
    const core = await openAgentCore({ remoteRoot, config: { pg: agentPg(w), schedule: null }, version: 'test', log: (line) => log.push(line), onCall: (name, args) => calls.push([name, args]), backoff: { minMs: 20, maxMs: 100 } });
    cores.push(core);
    assert.equal(log.length, 1);
    assert.match(log[0], new RegExp(`anomaly: ${claimed.id}\\.e1 reasserted running but the queue holds it claimed`));
    const record = core.table.get(`${claimed.id}.e1`);
    assert.deepEqual([record.subphase, record.from], ['KILLING', 'RUNNING']);
    assert.deepEqual(new ExecutionTable(remoteRoot).get(record.execId).subphase, 'KILLING', 'the kill decision is on disk');
    assert.equal((await w.ticket(claimed.id)).state, 'claimed', 'the queue row was not touched');

    const testsOwnConnection = (await w.as(logins.a1)).processID;
    const pid = (await backendPids(w, 'SELECT lane_dispatch%')).find((p) => p !== testsOwnConnection);
    await terminateBackend(w, pid);
    await core.ld.claim(randomUUID(), 8, 1e9);
    const reasserts = calls.filter(([name]) => name === 'ld_reassert').map(([, args]) => JSON.parse(args[0]));
    assert.deepEqual(reasserts.map((items) => items.length), [1, 0], 'a KILLING record is not reasserted again');
    assert.deepEqual(calls.map(([name]) => name).filter((n) => ['ld_heartbeat', 'ld_finish', 'ld_release', 'ld_start'].includes(n)), []);
  },

  'T7 an item the queue does not know ends a pre-start record DONE with the tombstone flag, and kills a running one': async (w, { remoteRoot, cores }) => {
    ensureAgentDirs(remoteRoot);
    const table = new ExecutionTable(remoteRoot);
    const ghost = (n, subphase) => recordFor({ id: tid(n), epoch: 1, token: randomUUID(), lane_key: `lk${n}` }, subphase);
    for (const [n, subphase] of [[1, 'FETCHING'], [2, 'STARTING'], [3, 'SPAWNING']]) table.put(ghost(n, subphase));
    const core = await openAgentCore({ remoteRoot, config: { pg: agentPg(w), schedule: null }, version: 'test', log: () => {}, backoff: { minMs: 20, maxMs: 100 } });
    cores.push(core);
    const phase = (n) => [core.table.get(`${tid(n)}.e1`).subphase, core.table.get(`${tid(n)}.e1`).tombstone];
    assert.deepEqual([phase(1), phase(2), phase(3)], [['DONE', undefined], ['DONE', true], ['KILLING', undefined]]);
  },
};

const GATE_CANARIES = [
  'T7 a client that has never connected sends ld_reassert then ld_register before the claim it was asked for',
  'T7 a heartbeat that answers reassert closes the gate: the next calls are ld_reassert, ld_register, ld_heartbeat, and the row ends running',
];

describe('agent reassert-first gate on a disposable Postgres', { skip: pgSkipOption, timeout: 300000 }, () => {
  let cluster;
  let remoteRoot;
  let savedState;
  const cores = [];
  before(async () => { cluster = await startDispatchCluster(); });
  after(async () => { await cluster?.closeClients(); cluster?.stop(); });
  beforeEach(() => {
    const base = makeTmpDir('agent-reassert-');
    remoteRoot = path.join(base, 'remote');
    savedState = process.env.LANE_BROKER_STATE;
    process.env.LANE_BROKER_STATE = path.join(base, 'state');
  });
  afterEach(async () => {
    await Promise.all(cores.splice(0).map((c) => c.close()));
    await closeAgents();
    await cluster.closeClients();
    if (savedState === undefined) delete process.env.LANE_BROKER_STATE;
    else process.env.LANE_BROKER_STATE = savedState;
  });

  for (const [name, scenario] of Object.entries(SCENARIOS)) {
    test(name, async () => scenario(await World.create(cluster), { remoteRoot, cores, LdClient }));
  }

  for (const name of GATE_CANARIES) {
    test(`canary: with the gate bypassed, "${name}" fails on its own call-order assertion`, async (t) => {
      const bypassed = ['        await this.#gate();\n', "        this.registered ??= { host: 'a1' };\n"];
      const mutant = await importMutant(path.join(import.meta.dirname, '..', 'src', 'agent', 'ld-client.js'), bypassed);
      let failure;
      await SCENARIOS[name](await World.create(cluster), { remoteRoot, cores, LdClient: mutant.LdClient }).catch((err) => { failure = err; });
      assert.ok(failure, `"${name}" still passed with the gate bypassed, so it proves nothing`);
      assert.equal(failure.code, 'ERR_ASSERTION', `it must fail on its assertion, not crash: ${failure.stack}`);
      assert.match(failure.message, /ld_reassert/);
      t.diagnostic(`failed as expected: ${failure.message.split('\n').join(' ').slice(0, 200)}`);
    });
  }
});
