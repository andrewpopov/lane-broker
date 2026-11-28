import assert from 'node:assert/strict';
import fs from 'node:fs';
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
    // The silence window runs on the watchdog's own monotonic clock (not the injected `now`), from the first guard tick on a RUNNING record
    // and restarting whenever the log grows. A window shorter than a starved runner's spawn would kill the lane before it exists (never-admitted),
    // so the window is generous and stretches on a slow runner. It never shrinks below 4 s: LANE_BROKER_TEST_TIME_SCALE < 1 models a slow
    // runner, whose spawn takes longer, not shorter (at 0.25 a 1 s window killed the lane mid-spawn: 'cancelled' instead of 'silence').
    const silenceTimeoutMs = Math.max(4_000, scaledMs(4_000));
    const stats = [];
    const realStatSync = fs.statSync;
    fs.statSync = (target, ...rest) => {
      const stat = realStatSync(target, ...rest);
      stats.push({ target: String(target), size: stat.size, at: Date.now() });
      return stat;
    };
    try {
      const ticket = await rw.submit({ code: SLEEP });
      const agent = await rw.agent({ load, config: { maxRunMs: DEFAULT_MAX_RUN_MS, silenceTimeoutMs }, timing: { guardMs: 100 } });
      const execId = rw.execId(await rw.claimed(ticket));
      let killed;
      await happens('the silence kill is recorded', () => (killed = agent.core.table.get(execId))?.guardKillAt !== undefined, wait(load));
      await rw.untilGone(agent, execId, wait(load));
      const final = await w.ticket(ticket.id);
      assert.deepEqual([final.result.kind, final.result.exit, final.result.reason, final.result.silenceTimeoutMs, final.result.runLimitMs ?? null], ['completed', 124, 'silence', silenceTimeoutMs, null]);
      // The reported window is the agent's own claim. The observed one: the guard stats the lane's log on every tick, so the first such stat
      // is where the silent stretch begins and a tick that saw the log grow restarts it. The kill must come on the first tick whose silence has
      // reached the window (one tick of tolerance for Date.now() against the watchdog's monotonic clock); how long the lane then takes to
      // finish is the scenario's own, generous deadline, not part of this check.
      const ticks = stats.filter((stat) => stat.target.includes(execId) && stat.target.endsWith('log') && stat.at >= killed.runningSince);
      assert.ok(ticks.length > 1, `the guard never stat the lane's log: ${JSON.stringify(stats.map((stat) => stat.target))}`);
      const tickMs = 100;
      let sinceMs = ticks[0].at;
      const silences = ticks.map((tick, i) => {
        if (i > 0 && tick.size > ticks[i - 1].size) sinceMs = tick.at;
        return tick.at - sinceMs;
      });
      const killTick = ticks.findLastIndex((tick) => tick.at <= killed.guardKillAt);
      const killStretchStart = Math.max(0, ticks.findLastIndex((tick, i) => i > 0 && i <= killTick && tick.size > ticks[i - 1].size));
      const firstEligible = silences.findIndex((silent, i) => i >= killStretchStart && silent >= silenceTimeoutMs - tickMs);
      assert.ok(silences[killTick] >= silenceTimeoutMs - tickMs, `killed after ${silences[killTick]} ms of observed silence, before the ${silenceTimeoutMs} ms window`);
      assert.ok(firstEligible !== -1 && killTick <= firstEligible + 1, `killed on guard tick ${killTick}, but tick ${firstEligible} had already been silent for ${silences[firstEligible]} ms of the ${silenceTimeoutMs} ms window`);
    } finally {
      fs.statSync = realStatSync;
    }
  },
};

const canaries = [
  ['a pull lane that hangs idle is killed by the agent\'s own no-progress guard and finishes red with exit 124', EXEC, ['const { message, ...guardKill } = guard.check(cpuRows, hostTable) ?? {};', 'const { message, ...guardKill } = {};']],
  ['a pull lane still running at maxRunMs is killed by the wall-clock backstop even while it burns CPU', 'agent/run-guard.js', ['if (this.now() - this.startedAt >= this.maxRunMs)', 'if (false)']],
];

scenarioSuite('agent guard windows and workload sampling (1) on a disposable Postgres', { timeout: 300_000, scenarios, canaries });
