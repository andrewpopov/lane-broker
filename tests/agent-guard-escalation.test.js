import assert from 'node:assert/strict';
import path from 'node:path';
import { happens, scenarioSuite, sleep } from './agent-run-harness.js';
import { wait, groupAlive, runningLane, skewClock } from './helpers/agent-guard-fixture.js';

const EXEC = 'agent/execution.js';

const scenarios = {
  'a SIGSTOPped supervisor that ignores the graceful cancel is SIGKILLed after the grace, the stop is verified, and the run finishes completed 124': async (w, rw, load) => {
    const graceVar = 'LANE_BROKER_TEST_CANCEL_GRACE_MS';
    const savedGrace = process.env[graceVar];
    process.env[graceVar] = '100';
    const { isProcessAlive } = await load('process-liveness.js');
    const clock = skewClock();
    const run = await runningLane(w, rw, load, { config: { maxRunMs: 60_000 }, timing: { guardMs: 100, guardEscalateMs: 400 }, now: clock.now });
    const { supervisorPid, supervisorStart, childPgid } = run.lease;
    process.kill(supervisorPid, 'SIGSTOP');
    clock.advance(60_000);
    try {
      await rw.untilGone(run.agent, run.execId, wait(load));
    } finally {
      for (const signal of ['SIGKILL', 'SIGCONT']) { try { process.kill(supervisorPid, signal); } catch { /* gone */ } }
      if (savedGrace === undefined) delete process.env[graceVar]; else process.env[graceVar] = savedGrace;
    }
    const final = await w.ticket(run.ticket.id);
    assert.deepEqual([final.state, final.result.kind, final.result.exit, final.result.reason, final.result.stopProven], ['finished', 'completed', 124, 'run-limit', true]);
    assert.equal(isProcessAlive(supervisorPid, supervisorStart), false, 'the supervisor is gone by identity');
    assert.equal(groupAlive(childPgid), false, 'the command group is gone');
  },

  'a stop that never completes hits its deadline, is persisted unproven, and the RUNNING path stops heartbeating': async (w, rw, load) => {
    const graceVar = 'LANE_BROKER_TEST_CANCEL_GRACE_MS';
    process.env[graceVar] = '100';
    const clock = skewClock();
    const run = await runningLane(w, rw, load, { config: { maxRunMs: 60_000 }, timing: { guardMs: 100, guardEscalateMs: 200, guardStopMs: 200, guardStopSlackMs: 200, heartbeatMs: 100 }, driverOptions: { forceStop: () => new Promise(() => {}) }, now: clock.now });
    process.kill(run.lease.supervisorPid, 'SIGSTOP');
    clock.advance(60_000);
    try {
      await happens('the stop was persisted unproven after its deadline', () => run.agent.core.table.get(run.execId)?.guardStop?.unproven === true, wait(load));
      await sleep(300);
      const beats = run.agent.timesOf('ld_heartbeat').length;
      await sleep(600);
      assert.equal(run.agent.timesOf('ld_heartbeat').length, beats, 'no heartbeat is sent once the stop is unproven');
    } finally {
      delete process.env[graceVar];
      for (const signal of ['SIGKILL', 'SIGCONT']) { try { process.kill(run.lease.supervisorPid, signal); } catch { /* gone */ } }
      try { process.kill(-run.lease.childPgid, 'SIGKILL'); } catch { /* gone */ }
    }
  },

  'a legacy guard-kill record without guardKillAt is stamped at first sight and still escalates': async (w, rw, load) => {
    const graceVar = 'LANE_BROKER_TEST_CANCEL_GRACE_MS';
    process.env[graceVar] = '100';
    const run = await runningLane(w, rw, load, { config: { maxRunMs: 3_600_000 }, timing: { guardMs: 100, guardEscalateMs: 300 } });
    process.kill(run.lease.supervisorPid, 'SIGSTOP');
    run.agent.core.table.put({ ...run.agent.core.table.get(run.execId), guardKill: { reason: 'run-limit', runLimitMs: 1 } });
    try {
      await rw.untilGone(run.agent, run.execId, wait(load));
    } finally {
      delete process.env[graceVar];
      for (const signal of ['SIGKILL', 'SIGCONT']) { try { process.kill(run.lease.supervisorPid, signal); } catch { /* gone */ } }
    }
    const final = await w.ticket(run.ticket.id);
    assert.deepEqual([final.result.kind, final.result.exit, final.result.stopProven], ['completed', 124, true]);
  },
};

const canaries = [
  ['a stop that never completes hits its deadline, is persisted unproven, and the RUNNING path stops heartbeating', EXEC, ['resolve(false), this.timing.guardStopMs + this.timing.guardStopSlackMs', 'resolve(new Promise(() => {})), this.timing.guardStopMs + this.timing.guardStopSlackMs']],
  ['a stop that never completes hits its deadline, is persisted unproven, and the RUNNING path stops heartbeating', EXEC, [['if (!context.escalating && !this.table.get(execId)?.guardStop?.unproven && (context.beatNow', 'if ((context.beatNow'], ['BEATING.has(this.table.get(execId)?.subphase) && !this.table.get(execId)?.guardStop?.unproven && !context.escalating,', 'BEATING.has(this.table.get(execId)?.subphase),']]],
  ['a legacy guard-kill record without guardKillAt is stamped at first sight and still escalates', EXEC, ["record.guardKillAt === undefined ? this.#cas(execId, ['RUNNING'], { guardKillAt: this.now() }) ?? record : record", 'record']],
  ['a SIGSTOPped supervisor that ignores the graceful cancel is SIGKILLed after the grace, the stop is verified, and the run finishes completed 124', EXEC, ['      context.escalatedAt = this.now();', '      return;\n      context.escalatedAt = this.now();']],
];

scenarioSuite('agent guard escalation and suppression (1) on a disposable Postgres', { timeout: 300_000, scenarios, canaries });
