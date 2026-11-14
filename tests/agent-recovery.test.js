import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { logins, call, claim } from './lane-dispatch-fixture.js';
import { agentPg } from './agent-harness.js';
import { agentPaths } from '../src/agent/store.js';
import { recordingFetch, happens, scenarioSuite } from './agent-run-harness.js';

const BIG = Buffer.alloc(20_000, 'b');
const READS_BIG = `process.exit(require('fs').readFileSync('big.bin').length === ${BIG.length} ? 0 : 3)`;
const oneOffer = () => { let given = false; return () => (given++ ? null : { freeCpu: 8, freeMem: 4e9 }); };

/** A record on disk for the next agent to adopt, written the way a crashed one would have left it. */
async function seed(w, rw, load, make) {
  const { openAgentCore } = await load('agent/agent.js');
  const { newExecutionRecord } = await load('agent/store.js');
  const core = await openAgentCore({ remoteRoot: rw.root, config: { pg: agentPg(w), schedule: null }, version: 't', log: () => {} });
  const record = make(newExecutionRecord);
  core.table.put(record);
  await core.close();
  return record;
}

const scenarios = {
  'a spool that no longer hashes at STARTING sends the record back to FETCHING, and the stream is fetched again': async (w, rw, load) => {
    const ticket = await rw.submit({ code: READS_BIG, files: [{ path: 'big.bin', bytes: BIG }] });
    const spool = path.join(agentPaths(rw.root).spool, `${ticket.id}.e1.gz`);
    const fetchImpl = recordingFetch((url, _init, count) => {
      if (url.includes('/lane-snap/blobs/') && count === 1) fs.appendFileSync(spool, 'torn');
      return undefined;
    });
    const agent = await rw.agent({ load, fetchImpl, figures: oneOffer() });
    const row = await rw.claimed(ticket);
    await rw.untilGone(agent, rw.execId(row));
    assert.equal(fetchImpl.count('GET', '/lane-snap/streams/'), 2, 'the torn spool was fetched again');
    const final = await w.ticket(ticket.id);
    assert.deepEqual([final.result.kind, final.result.exit], ['completed', 0]);
  },

  'SPAWNING recovery with a real result.json settles that result instead of declaring the publisher gone': async (w, rw, load) => {
    const ticket = await rw.submit({ code: 'setTimeout(() => process.exit(0), 1200)' });
    const first = await rw.agent({ load, figures: oneOffer(), timing: { heartbeatMs: 60_000 } });
    const row = await rw.claimed(ticket);
    const execId = rw.execId(row);
    await happens('the lane is running', () => first.core.table.get(execId)?.subphase === 'RUNNING');
    await first.stop();
    await happens('the lane published its result', () => fs.existsSync(path.join(rw.ticketDir(execId), 'result.json')), 10_000);
    const { pid: _pid, pidStart: _start, ...rest } = first.core.table.get(execId);
    first.core.table.put({ ...rest, subphase: 'SPAWNING' });
    const second = await rw.agent({ load, figures: oneOffer() });
    await rw.untilGone(second, execId);
    const final = await w.ticket(ticket.id);
    assert.deepEqual([final.state, final.result.kind, final.result.exit], ['finished', 'completed', 0]);
    assert.notEqual(final.result.reason, 'publisher-gone');
  },

  'SPAWNING recovery with no result and no execution directory finishes rejected never-admitted': async (w, rw, load) => {
    await rw.submit();
    const a1 = await w.as(logins.a1);
    const claimed = await claim(a1);
    assert.equal(await call(a1, 'ld_start', claimed.id, claimed.epoch, claimed.token), true);
    const record = await seed(w, rw, load, (make) => ({ ...make(claimed), subphase: 'SPAWNING', kernelBootId: 'x' }));
    const agent = await rw.agent({ load, figures: () => null });
    await rw.untilGone(agent, record.execId);
    const final = await w.ticket(claimed.id);
    assert.deepEqual([final.state, final.result.kind, final.result.reason, final.result.neverStarted], ['finished', 'rejected', 'never-admitted', true]);
  },

  'KILLING ends when the boot id has changed, sends the queue nothing, and logs why': async (w, rw, load) => {
    const fake = { id: randomUUID(), epoch: 1, token: randomUUID(), repo_key: 'r', lane: 'l', lane_key: 'k', binding: {}, header: {}, snapshot: {} };
    const record = await seed(w, rw, load, (make) => ({ ...make(fake), subphase: 'KILLING', kernelBootId: 'a-boot-that-is-gone', killingSince: Date.now() }));
    const saved = process.env.LANE_BROKER_KERNEL_BOOT_ID;
    process.env.LANE_BROKER_KERNEL_BOOT_ID = 'this-boot';
    try {
      const agent = await rw.agent({ load, figures: () => null });
      await rw.untilGone(agent, record.execId);
      assert.ok(rw.logs.some((l) => /boot id changed/.test(l)), `logged why: ${rw.logs.join(' | ')}`);
      assert.deepEqual(agent.names().filter((n) => !['ld_register', 'ld_claim'].includes(n)), [], 'nothing about it went to the queue');
    } finally {
      if (saved === undefined) delete process.env.LANE_BROKER_KERNEL_BOOT_ID;
      else process.env.LANE_BROKER_KERNEL_BOOT_ID = saved;
    }
  },
};

const EXEC = 'agent/execution.js';
const canaries = [
  ['a spool that no longer hashes at STARTING sends the record back to FETCHING, and the stream is fetched again', EXEC, ['if (!intact) {', 'if (false) {']],
  ['SPAWNING recovery with a real result.json settles that result instead of declaring the publisher gone', EXEC, ["if (this.#resultExists(execId)) return this.#cas(execId, ['SPAWNING'], { subphase: 'SETTLING' });", '']],
  ['KILLING ends when the boot id has changed, sends the queue nothing, and logs why', EXEC, ['const rebooted = Boolean(record.kernelBootId && bootNow && record.kernelBootId !== bootNow);', 'const rebooted = false;']],
];

scenarioSuite('agent recovery on a disposable Postgres', { scenarios, canaries });
