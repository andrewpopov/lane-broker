import assert from 'node:assert/strict';
import { SLEEP, scenarioSuite, happens } from './agent-run-harness.js';
import { scaledMs } from './helpers.js';
import { DEFAULT_MAX_RUN_MS } from '../src/agent/config.js';
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

  'a pull lane whose output keeps growing past 2 h is not killed under the default run limit': async (w, rw, load) => {
    const clock = skewClock();
    const code = 'let n = 0; const t = setInterval(() => { console.log(`test ${++n} ok`); if (n === 25) { clearInterval(t); process.exit(0); } }, 100);';
    const { ticket, agent, execId } = await runningLane(w, rw, load, { code, config: { maxRunMs: DEFAULT_MAX_RUN_MS }, timing: { guardMs: 100 }, now: clock.now });
    clock.advance(2 * 3_600_000 + 60_000);
    await rw.untilGone(agent, execId, wait(load));
    const final = await w.ticket(ticket.id);
    assert.deepEqual([final.result.kind, final.result.exit], ['completed', 0]);
  },

  'a pull lane that stops writing is killed by silence at its window, long before the run limit': async (w, rw, load) => {
    // The silence window runs on the watchdog's own monotonic clock (not the injected `now`), from the first guard tick on a RUNNING record:
    // a window shorter than a starved runner's spawn would kill the lane before it exists (never-admitted). So the window is generous and scaled.
    const silenceTimeoutMs = scaledMs(4_000);
    const ticket = await rw.submit({ code: SLEEP });
    const agent = await rw.agent({ load, config: { maxRunMs: DEFAULT_MAX_RUN_MS, silenceTimeoutMs }, timing: { guardMs: 100 } });
    const execId = rw.execId(await rw.claimed(ticket));
    let killed;
    await happens('the silence kill is recorded', () => (killed = agent.core.table.get(execId))?.guardKillAt !== undefined, wait(load));
    await rw.untilGone(agent, execId, wait(load));
    const final = await w.ticket(ticket.id);
    assert.deepEqual([final.result.kind, final.result.exit, final.result.reason, final.result.silenceTimeoutMs, final.result.runLimitMs ?? null], ['completed', 124, 'silence', silenceTimeoutMs, null]);
    // The reported window is the agent's own claim; this is the observed one: silent from the spawn, the kill may not come early (nor anywhere near the run limit).
    const silentMs = killed.guardKillAt - killed.runningSince;
    assert.ok(silentMs >= silenceTimeoutMs - 250, `killed after ${silentMs} ms of silence, before the ${silenceTimeoutMs} ms window`);
    assert.ok(silentMs < DEFAULT_MAX_RUN_MS / 2, `killed after ${silentMs} ms, nowhere near the window`);
  },
};

const canaries = [
  ['a pull lane that hangs idle is killed by the agent\'s own no-progress guard and finishes red with exit 124', EXEC, ['const { message, ...guardKill } = guard.check(cpuRows, hostTable) ?? {};', 'const { message, ...guardKill } = {};']],
  ['a pull lane still running at maxRunMs is killed by the wall-clock backstop even while it burns CPU', 'agent/run-guard.js', ['if (this.now() - this.startedAt >= this.maxRunMs)', 'if (false)']],
];

scenarioSuite('agent guard windows and workload sampling (1) on a disposable Postgres', { timeout: 300_000, scenarios, canaries });
