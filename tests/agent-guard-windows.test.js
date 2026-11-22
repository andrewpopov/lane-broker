import assert from 'node:assert/strict';
import { SLEEP, scenarioSuite } from './agent-run-harness.js';
import { wait, runningLane, skewClock } from './helpers/agent-guard-fixture.js';

const EXEC = 'agent/execution.js';

const scenarios = {
  'a pull lane that hangs idle is killed by the agent\'s own no-progress guard and finishes red with exit 124': async (w, rw, load) => {
    const ticket = await rw.submit({ code: SLEEP });
    const agent = await rw.agent({ load, config: { noProgressTimeoutMs: 600 }, timing: { guardMs: 100 } });
    const row = await rw.claimed(ticket);
    await rw.untilGone(agent, rw.execId(row), wait(load));
    const final = await w.ticket(ticket.id);
    assert.equal(final.state, 'finished');
    assert.deepEqual([final.result.kind, final.result.exit, final.result.reason, final.result.noProgressTimeoutMs], ['completed', 124, 'no-progress', 600]);
    assert.match(rw.s3.objects.get(`lane-out/a1/${ticket.id}/e${row.epoch}/log`).body.toString(), /no progress for/);
  },

  'a pull lane that burns CPU past the no-progress window is not killed': async (w, rw, load) => {
    const ticket = await rw.submit({ code: 'const end = Date.now() + 1500; while (Date.now() < end) {}' });
    const agent = await rw.agent({ load, config: { noProgressTimeoutMs: 600 }, timing: { guardMs: 100 } });
    await rw.untilGone(agent, rw.execId(await rw.claimed(ticket)), wait(load));
    const final = await w.ticket(ticket.id);
    assert.deepEqual([final.result.kind, final.result.exit], ['completed', 0]);
  },

  'a pull lane still running at maxRunMs is killed by the wall-clock backstop even while it burns CPU': async (w, rw, load) => {
    const clock = skewClock();
    const { ticket, agent, row, execId } = await runningLane(w, rw, load, { code: 'while (true) {}', config: { maxRunMs: 60_000 }, timing: { guardMs: 100 }, now: clock.now });
    clock.advance(60_000);
    await rw.untilGone(agent, execId, wait(load));
    const final = await w.ticket(ticket.id);
    assert.deepEqual([final.result.kind, final.result.exit, final.result.reason, final.result.runLimitMs], ['completed', 124, 'run-limit', 60_000]);
    assert.match(rw.s3.objects.get(`lane-out/a1/${ticket.id}/e${row.epoch}/log`).body.toString(), /run limit/);
  },
};

const canaries = [
  ['a pull lane that hangs idle is killed by the agent\'s own no-progress guard and finishes red with exit 124', EXEC, ['const { message, ...guardKill } = guard.check(cpuRows, hostTable) ?? {};', 'const { message, ...guardKill } = {};']],
  ['a pull lane still running at maxRunMs is killed by the wall-clock backstop even while it burns CPU', 'agent/run-guard.js', ['if (this.now() - this.startedAt >= this.maxRunMs)', 'if (false)']],
];

scenarioSuite('agent guard windows and workload sampling (1) on a disposable Postgres', { timeout: 300_000, scenarios, canaries });
