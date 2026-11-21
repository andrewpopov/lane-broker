import assert from 'node:assert/strict';
import path from 'node:path';
import { logins, call } from './lane-dispatch-fixture.js';
import { happens, scenarioSuite, sleep } from './agent-run-harness.js';
import { wait, groupAlive, runningLane } from './helpers/agent-guard-fixture.js';

const EXEC = 'agent/execution.js';

const scenarios = {
  'the guard keeps enforcing the run limit while a heartbeat never answers': async (w, rw, load) => {
    const { ExecutionDriver } = await load('agent/execution.js');
    let release;
    let armed = false;
    const stuck = new Promise((resolve) => { release = () => resolve({ fenced: false, cancel: false }); });
    class StuckHeartbeat extends ExecutionDriver {
      constructor(options) {
        super(options);
        const real = this.ld;
        const bound = (key) => (typeof real[key] === 'function' ? real[key].bind(real) : real[key]);
        this.ld = new Proxy(real, { get: (_target, key) => (key === 'heartbeat' ? (...args) => (armed ? stuck : real.heartbeat(...args)) : bound(key)) });
      }
    }
    try {
      const run = await runningLane(w, rw, load, { config: { maxRunMs: 1500 }, timing: { guardMs: 100, heartbeatMs: 100 }, Driver: StuckHeartbeat });
      armed = true;
      await happens('the command group was killed although every heartbeat hangs', () => !groupAlive(run.lease.childPgid), wait(load));
    } finally {
      release();
    }
  },

  'a cancel the queue recorded before an agent guard kill finishes is a cancelled ticket, not exit 124': async (w, rw, load) => {
    const run = await runningLane(w, rw, load, { config: { maxRunMs: 1500 }, timing: { guardMs: 100, heartbeatMs: 3_600_000 } });
    await happens('the guard decided to kill', () => run.agent.core.table.get(run.execId)?.guardKill, wait(load));
    await call(await w.as(logins.s1), 'ld_cancel', run.ticket.id);
    await rw.untilGone(run.agent, run.execId, wait(load));
    const final = await w.ticket(run.ticket.id);
    assert.deepEqual([final.state, final.result.kind, final.result.guard], ['cancelled', 'cancelled', undefined]);
  },

  'an unproven stop whose result exists does not renew the queue lease while it uploads': async (w, rw, load) => {
    const failPut = (url, init) => (init?.method === 'PUT' ? Promise.reject(new Error('storage down')) : fetch(url, init));
    const run = await runningLane(w, rw, load, { code: 'setTimeout(() => process.exit(0), 700); setInterval(() => {}, 1000)', fetchImpl: failPut, timing: { guardMs: 60_000, heartbeatMs: 100, uploadRetryMs: 1500 } });
    run.agent.core.table.put({ ...run.agent.core.table.get(run.execId), guardStop: { unproven: true } });
    await happens('the upload started', () => run.agent.core.table.get(run.execId)?.uploadSince !== undefined, wait(load));
    await sleep(200);
    const beats = run.agent.timesOf('ld_heartbeat').length;
    await sleep(600);
    assert.equal(run.agent.timesOf('ld_heartbeat').length, beats, 'the upload path sent no heartbeat');
  },
};

const canaries = [
  ['an unproven stop whose result exists does not renew the queue lease while it uploads', EXEC, [['if (!this.table.get(execId)?.guardStop?.unproven && (context.beatNow', 'if ((context.beatNow'], ['BEATING.has(this.table.get(execId)?.subphase) && !this.table.get(execId)?.guardStop?.unproven && !context.escalating,', 'BEATING.has(this.table.get(execId)?.subphase),']]],
  ['the guard keeps enforcing the run limit while a heartbeat never answers', EXEC, ['    this.#guardTimer = setTimeout(async () => {', '    this.#guardTimer = setTimeout(async () => {\n      await this.#contexts.values().next().value.promise;']],
];

scenarioSuite('agent guard suppression and cancel arbitration (2) on a disposable Postgres', { timeout: 300_000, scenarios, canaries });
