import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { logins, call } from './lane-dispatch-fixture.js';
import { makeTmpDir } from './helpers/tmp.js';
import { SLEEP, happens, scenarioSuite, sleep } from './agent-run-harness.js';
import { RUNNER_SHUTDOWN_REASON, paths, readJsonSafe } from '../src/state.js';

const leaseOf = (rw) => {
  const dir = paths(rw.ctx.state).leases;
  const name = fs.existsSync(dir) ? fs.readdirSync(dir).find((n) => n.endsWith('.json')) : undefined;
  return name ? readJsonSafe(path.join(dir, name)) : null;
};
const groupAlive = (pgid) => { try { process.kill(-pgid, 0); return true; } catch { return false; } };

async function runningLane(w, rw, load, { code = SLEEP, ...agentOptions } = {}) {
  const ticket = await rw.submit({ code });
  const agent = await rw.agent({ load, ...agentOptions });
  const row = await rw.claimed(ticket);
  const execId = rw.execId(row);
  await happens('the lane is running', () => agent.core.table.get(execId)?.subphase === 'RUNNING' && leaseOf(rw)?.childPgid);
  return { ticket, agent, row, execId, lease: leaseOf(rw) };
}

const scenarios = {
  'a heartbeat answering fenced kills the lane and the record, and no ld_finish is ever sent': async (w, rw, load) => {
    const run = await runningLane(w, rw, load);
    await (await w.su()).query('UPDATE lane_dispatch.tickets SET claim_token = gen_random_uuid() WHERE id = $1', [run.ticket.id]);
    await rw.untilGone(run.agent, run.execId);
    assert.equal(groupAlive(run.lease.childPgid), false, 'the lane process group is gone');
    assert.equal(run.agent.names().includes('ld_finish'), false);
    assert.equal(run.agent.names().includes('ld_release'), false);
    assert.equal(readJsonSafe(path.join(rw.ticketDir(run.execId), 'result.json')).stopProven, true, 'the stop was proven by the runner');
  },

  'the lane\'s command sees the agent\'s configured speedFactor as LANE_BROKER_RUNNER_SPEED_FACTOR, so a repo can scale its time budgets': async (w, rw, load) => {
    const out = path.join(makeTmpDir('agent-speed-factor-'), 'speed');
    const ticket = await rw.submit({ code: `require('fs').writeFileSync(${JSON.stringify(out)}, String(process.env.LANE_BROKER_RUNNER_SPEED_FACTOR))` });
    const agent = await rw.agent({ load, config: { speedFactor: 2.5 } });
    await rw.untilGone(agent, rw.execId(await rw.claimed(ticket)));
    assert.equal(fs.readFileSync(out, 'utf8'), '2.5');
  },

  'ld_cancel on a running ticket reaches the lane within a heartbeat and finishes cancelled with cancelConfirmed': async (w, rw, load) => {
    const run = await runningLane(w, rw, load);
    await call(await w.as(logins.s1), 'ld_cancel', run.ticket.id);
    await rw.untilGone(run.agent, run.execId, 8000);
    const final = await w.ticket(run.ticket.id);
    assert.equal(final.state, 'cancelled');
    assert.equal(final.result.kind, 'cancelled');
    assert.equal(final.result.cancelConfirmed, true);
    assert.equal(groupAlive(run.lease.childPgid), false);
    assert.equal(run.agent.names().includes('ld_release'), false, 'nothing after start releases');
  },

  'an agent killed mid-run leaves the lane running, and the next agent adopts it and finishes it without starting it again': async (w, rw, load) => {
    const release = path.join(makeTmpDir('agent-adopt-'), 'release');
    const code = `const fs = require('fs'); const t = setInterval(() => { if (!fs.existsSync(${JSON.stringify(release)})) return; clearInterval(t); process.stdout.write('done after the agent died\\n'); process.exit(0); }, 50); setTimeout(() => process.exit(3), 600_000);`;
    const run = await runningLane(w, rw, load, { code });
    await run.agent.stop();
    assert.equal(groupAlive(run.lease.childPgid), true, 'the lane outlived its agent');
    const second = await rw.agent({ load });
    fs.writeFileSync(release, '');
    await happens('the adopted lane finished', async () => (await w.ticket(run.ticket.id)).state === 'finished', 20_000);
    await rw.untilGone(second, run.execId);
    const final = await w.ticket(run.ticket.id);
    assert.deepEqual([final.result.kind, final.result.exit], ['completed', 0]);
    assert.equal(second.names().includes('ld_start'), false, 'the lane was adopted, not started again');
    const log = rw.s3.objects.get(`lane-out/a1/${run.ticket.id}/e${run.row.epoch}/log`).body;
    assert.match(log.toString(), /done after the agent died/);
  },

  'under a hostile inherited LANE_BROKER_STATE, the agent and the lane it spawns use the configured stateDir': async (w, rw, load) => {
    const { stateHome } = await load('state.js');
    const hostile = makeTmpDir('hostile-state-');
    const saved = process.env.LANE_BROKER_STATE;
    process.env.LANE_BROKER_STATE = hostile;
    try {
      const run = await runningLane(w, rw, load, { spawnEnv: { ...rw.ctx.env, LANE_BROKER_STATE: hostile, LANE_BROKER_HOME: hostile } });
      assert.equal(stateHome(), rw.ctx.state, 'a module resolving the state home from the environment sees the configured one');
      const leases = (dir) => (fs.existsSync(paths(dir).leases) ? fs.readdirSync(paths(dir).leases).filter((n) => n.endsWith('.json')) : []);
      assert.equal(leases(rw.ctx.state).length, 1, 'the spawned lane took its lease in the configured state dir');
      assert.equal(leases(hostile).length, 0, 'and none in the inherited one');
      assert.ok(run.lease.childPgid);
    } finally {
      process.env.LANE_BROKER_STATE = saved;
    }
  },

  'a pull lane that hangs idle is killed by the agent\'s own no-progress guard and finishes red with exit 124': async (w, rw, load) => {
    const ticket = await rw.submit({ code: SLEEP });
    const agent = await rw.agent({ load, config: { noProgressTimeoutMs: 1500 }, timing: { guardMs: 200 } });
    const row = await rw.claimed(ticket);
    await rw.untilGone(agent, rw.execId(row), 30_000);
    const final = await w.ticket(ticket.id);
    assert.equal(final.state, 'finished');
    assert.deepEqual([final.result.kind, final.result.exit, final.result.reason, final.result.noProgressTimeoutMs], ['completed', 124, 'no-progress', 1500]);
    assert.match(rw.s3.objects.get(`lane-out/a1/${ticket.id}/e${row.epoch}/log`).body.toString(), /no progress for/);
  },

  'a pull lane that burns CPU past the no-progress window is not killed': async (w, rw, load) => {
    const ticket = await rw.submit({ code: 'const end = Date.now() + 4000; while (Date.now() < end) {}' });
    const agent = await rw.agent({ load, config: { noProgressTimeoutMs: 1500 }, timing: { guardMs: 200 } });
    await rw.untilGone(agent, rw.execId(await rw.claimed(ticket)), 30_000);
    const final = await w.ticket(ticket.id);
    assert.deepEqual([final.result.kind, final.result.exit], ['completed', 0]);
  },

  'a pull lane still running at maxRunMs is killed by the wall-clock backstop even while it burns CPU': async (w, rw, load) => {
    const ticket = await rw.submit({ code: 'while (true) {}' });
    const agent = await rw.agent({ load, config: { maxRunMs: 2000 }, timing: { guardMs: 200 } });
    const row = await rw.claimed(ticket);
    await rw.untilGone(agent, rw.execId(row), 30_000);
    const final = await w.ticket(ticket.id);
    assert.deepEqual([final.result.kind, final.result.exit, final.result.reason, final.result.runLimitMs], ['completed', 124, 'run-limit', 2000]);
    assert.match(rw.s3.objects.get(`lane-out/a1/${ticket.id}/e${row.epoch}/log`).body.toString(), /run limit/);
  },

  'a SIGSTOPped supervisor that ignores the graceful cancel is SIGKILLed after the grace, the stop is verified, and the run finishes completed 124': async (w, rw, load) => {
    const graceVar = 'LANE_BROKER_TEST_CANCEL_GRACE_MS';
    const savedGrace = process.env[graceVar];
    process.env[graceVar] = '200';
    const { isProcessAlive } = await load('process-liveness.js');
    const run = await runningLane(w, rw, load, { config: { maxRunMs: 4000 }, timing: { guardMs: 200, guardEscalateMs: 1500 } });
    const { supervisorPid, supervisorStart, childPgid } = run.lease;
    process.kill(supervisorPid, 'SIGSTOP');
    try {
      await rw.untilGone(run.agent, run.execId, 60_000);
    } finally {
      for (const signal of ['SIGKILL', 'SIGCONT']) { try { process.kill(supervisorPid, signal); } catch { /* gone */ } }
      if (savedGrace === undefined) delete process.env[graceVar]; else process.env[graceVar] = savedGrace;
    }
    const final = await w.ticket(run.ticket.id);
    assert.deepEqual([final.state, final.result.kind, final.result.exit, final.result.reason, final.result.stopProven], ['finished', 'completed', 124, 'run-limit', true]);
    assert.equal(isProcessAlive(supervisorPid, supervisorStart), false, 'the supervisor is gone by identity');
    assert.equal(groupAlive(childPgid), false, 'the command group is gone');
  },

  'an explicit 0 in the dispatch header turns the guard off for the lane, while an absent value takes the runner\'s global': async (w, rw, load) => {
    const ticket = await rw.submit({ code: 'setTimeout(() => process.exit(0), 4500); setInterval(() => {}, 1000)', headerOverrides: { noProgressTimeoutMs: 0, silenceTimeoutMs: 0 } });
    const agent = await rw.agent({ load, config: { noProgressTimeoutMs: 1500, silenceTimeoutMs: 1500 }, timing: { guardMs: 200 } });
    await rw.untilGone(agent, rw.execId(await rw.claimed(ticket)), 30_000);
    assert.deepEqual([(await w.ticket(ticket.id)).result.kind, (await w.ticket(ticket.id)).result.exit], ['completed', 0]);
  },

  'a worker that daemonised and reparented to PID 1 keeps the lane alive while it burns CPU, even though the command itself is idle': async (w, rw, load) => {
    const done = path.join(makeTmpDir('agent-daemon-'), 'done');
    const grandchild = `const end = Date.now() + 5000; while (Date.now() < end) {} require('fs').writeFileSync(${JSON.stringify(done)}, 'x')`;
    const middle = `require('child_process').spawn(process.execPath, ['-e', ${JSON.stringify(grandchild)}], { detached: true, stdio: 'ignore' }).unref()`;
    const code = `require('child_process').spawn(process.execPath, ['-e', ${JSON.stringify(middle)}], { detached: true, stdio: 'ignore' }).unref(); const t = setInterval(() => { if (require('fs').existsSync(${JSON.stringify(done)})) process.exit(0); }, 100);`;
    const ticket = await rw.submit({ code });
    const agent = await rw.agent({ load, config: { noProgressTimeoutMs: 1500 }, timing: { guardMs: 200 } });
    await rw.untilGone(agent, rw.execId(await rw.claimed(ticket)), 40_000);
    const final = await w.ticket(ticket.id);
    assert.deepEqual([final.result.kind, final.result.exit], ['completed', 0]);
  },

  'a hung workload is killed even while the supervisor itself burns CPU every interval': async (w, rw, load) => {
    const { readProcCpuRows } = await load('preemptible.js');
    let reads = 0;
    const busySupervisor = () => {
      reads += 1;
      return readProcCpuRows().map((row) => (row.pid === leaseOf(rw)?.supervisorPid ? { ...row, cpuSec: row.cpuSec + reads } : row));
    };
    const ticket = await rw.submit({ code: SLEEP });
    const agent = await rw.agent({ load, config: { noProgressTimeoutMs: 1500 }, timing: { guardMs: 200 }, driverOptions: { readRows: busySupervisor } });
    await rw.untilGone(agent, rw.execId(await rw.claimed(ticket)), 30_000);
    const final = await w.ticket(ticket.id);
    assert.deepEqual([final.result.exit, final.result.reason], [124, 'no-progress']);
  },

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
      const run = await runningLane(w, rw, load, { config: { maxRunMs: 2500 }, timing: { guardMs: 200, heartbeatMs: 100 }, Driver: StuckHeartbeat });
      armed = true;
      await happens('the command group was killed although every heartbeat hangs', () => !groupAlive(run.lease.childPgid), 30_000);
    } finally {
      release();
    }
  },

  'a cancel the queue recorded before an agent guard kill finishes is a cancelled ticket, not exit 124': async (w, rw, load) => {
    const run = await runningLane(w, rw, load, { config: { maxRunMs: 1500 }, timing: { guardMs: 200, heartbeatMs: 3_600_000 } });
    await happens('the guard decided to kill', () => run.agent.core.table.get(run.execId)?.guardKill, 30_000);
    await call(await w.as(logins.s1), 'ld_cancel', run.ticket.id);
    await rw.untilGone(run.agent, run.execId, 30_000);
    const final = await w.ticket(run.ticket.id);
    assert.deepEqual([final.state, final.result.kind, final.result.guard], ['cancelled', 'cancelled', undefined]);
  },

  'a record without runningSince gets its first observation persisted once, and two agent restarts never reset the wall-clock anchor': async (w, rw, load) => {
    const run = await runningLane(w, rw, load, { config: { maxRunMs: 3_600_000 }, timing: { guardMs: 1500 } });
    const { runningSince: _dropped, ...legacy } = run.agent.core.table.get(run.execId);
    run.agent.core.table.put(legacy);
    await happens('the first observation was persisted', () => run.agent.core.table.get(run.execId)?.runningSince !== undefined);
    const anchor = run.agent.core.table.get(run.execId).runningSince;
    await run.agent.stop();
    for (const restart of [1, 2]) {
      await sleep(600);
      const next = await rw.agent({ load, config: { maxRunMs: 3_600_000 }, timing: { guardMs: 300 } });
      await sleep(900);
      assert.equal(next.core.table.get(run.execId)?.runningSince, anchor, `restart ${restart} kept the anchor`);
      await next.stop();
    }
    for (const target of [run.lease.supervisorPid, -run.lease.childPgid]) { try { process.kill(target, 'SIGKILL'); } catch { /* gone */ } }
  },

  'a stop that never completes hits its deadline, is persisted unproven, and the RUNNING path stops heartbeating': async (w, rw, load) => {
    const graceVar = 'LANE_BROKER_TEST_CANCEL_GRACE_MS';
    process.env[graceVar] = '200';
    const run = await runningLane(w, rw, load, { config: { maxRunMs: 1500 }, timing: { guardMs: 200, guardEscalateMs: 400, guardStopMs: 500, heartbeatMs: 100 }, driverOptions: { forceStop: () => new Promise(() => {}) } });
    process.kill(run.lease.supervisorPid, 'SIGSTOP');
    try {
      await happens('the stop was persisted unproven after its deadline', () => run.agent.core.table.get(run.execId)?.guardStop?.unproven === true, 30_000);
      await sleep(500);
      const beats = run.agent.timesOf('ld_heartbeat').length;
      await sleep(1500);
      assert.equal(run.agent.timesOf('ld_heartbeat').length, beats, 'no heartbeat is sent once the stop is unproven');
    } finally {
      delete process.env[graceVar];
      for (const signal of ['SIGKILL', 'SIGCONT']) { try { process.kill(run.lease.supervisorPid, signal); } catch { /* gone */ } }
      try { process.kill(-run.lease.childPgid, 'SIGKILL'); } catch { /* gone */ }
    }
  },

  'an unproven stop whose result exists does not renew the queue lease while it uploads': async (w, rw, load) => {
    const failPut = (url, init) => (init?.method === 'PUT' ? Promise.reject(new Error('storage down')) : fetch(url, init));
    const run = await runningLane(w, rw, load, { code: 'setTimeout(() => process.exit(0), 1500); setInterval(() => {}, 1000)', fetchImpl: failPut, timing: { guardMs: 60_000, heartbeatMs: 100, uploadRetryMs: 3000 } });
    run.agent.core.table.put({ ...run.agent.core.table.get(run.execId), guardStop: { unproven: true } });
    await happens('the upload started', () => run.agent.core.table.get(run.execId)?.uploadSince !== undefined, 30_000);
    await sleep(300);
    const beats = run.agent.timesOf('ld_heartbeat').length;
    await sleep(1500);
    assert.equal(run.agent.timesOf('ld_heartbeat').length, beats, 'the upload path sent no heartbeat');
  },

  'a legacy guard-kill record without guardKillAt is stamped at first sight and still escalates': async (w, rw, load) => {
    const graceVar = 'LANE_BROKER_TEST_CANCEL_GRACE_MS';
    process.env[graceVar] = '200';
    const run = await runningLane(w, rw, load, { config: { maxRunMs: 3_600_000 }, timing: { guardMs: 200, guardEscalateMs: 1000 } });
    process.kill(run.lease.supervisorPid, 'SIGSTOP');
    run.agent.core.table.put({ ...run.agent.core.table.get(run.execId), guardKill: { reason: 'run-limit', runLimitMs: 1 } });
    try {
      await rw.untilGone(run.agent, run.execId, 60_000);
    } finally {
      delete process.env[graceVar];
      for (const signal of ['SIGKILL', 'SIGCONT']) { try { process.kill(run.lease.supervisorPid, signal); } catch { /* gone */ } }
    }
    const final = await w.ticket(run.ticket.id);
    assert.deepEqual([final.result.kind, final.result.exit, final.result.stopProven], ['completed', 124, true]);
  },

  'two running lanes share one host table read and one CPU read per guard tick': async (w, rw, load) => {
    const { readProcCpuRows } = await load('preemptible.js');
    const { readProcessTable } = await load('descendants.js');
    const cpuReads = [];
    const tableReads = [];
    const tickA = await rw.submit({ code: SLEEP });
    const tickB = await rw.submit({ code: SLEEP });
    const agent = await rw.agent({
      load, config: { noProgressTimeoutMs: 600_000 }, timing: { guardMs: 300 },
      driverOptions: { readRows: () => { cpuReads.push(1); return readProcCpuRows(); }, readTable: (ids) => { tableReads.push(ids.length); return readProcessTable(ids); } },
    });
    await rw.claimed(tickA);
    await rw.claimed(tickB);
    await happens('both lanes were sampled in one tick', () => tableReads.includes(2), 30_000);
    await sleep(1500);
    assert.ok(tableReads.length <= cpuReads.length, `at most one table read per CPU read, whatever the lane count (${tableReads.length} table, ${cpuReads.length} cpu)`);
    assert.ok(Math.max(...tableReads) === 2);
    for (const target of [tickA, tickB]) await call(await w.as(logins.s1), 'ld_cancel', target.id);
    void agent;
  },

  'the very first lease that names the command group already names its leader, so a supervisor killed right away leaves a provable orphan': async (w, rw, load) => {
    const ticket = await rw.submit({ code: SLEEP });
    await rw.agent({ load, laneBin: load.laneBin });
    let first = null;
    for (const deadline = Date.now() + 30_000; first === null && Date.now() < deadline;) {
      const lease = leaseOf(rw);
      if (lease?.childPgid) first = lease;
      else await sleep(1);
    }
    assert.ok(first, 'a lease with a command group appeared');
    assert.equal(first.leader?.pid, first.childPgid, 'the leader pid is in that same lease');
    assert.equal(typeof first.leader?.token, 'string', 'with its start token');
    void ticket;
  },

  'a post-start shutdown result is finished with its proof, and nothing is released': async (w, rw, load) => {
    const { ExecutionDriver } = await load('agent/execution.js');
    const { readRemoteResultRecord } = await load('remote-runner.js');
    const shutdown = async (execId, options) => {
      const real = await readRemoteResultRecord(execId, options);
      return real.missing ? real : { ...real, kind: 'unfinished', reason: RUNNER_SHUTDOWN_REASON, shutdownProof: { at: Date.now() } };
    };
    class ShutdownDriver extends ExecutionDriver {
      constructor(options) { super({ ...options, readResult: shutdown }); }
    }
    const ticket = await rw.submit({ code: 'process.exit(0)' });
    const agent = await rw.agent({ load, Driver: ShutdownDriver });
    const row = await rw.claimed(ticket);
    await rw.untilGone(agent, rw.execId(row));
    assert.equal(agent.names().includes('ld_release'), false, 'a started run is never released');
    const final = await w.ticket(ticket.id);
    assert.equal(final.state, 'finished');
    assert.equal(final.result.reason, RUNNER_SHUTDOWN_REASON);
    assert.ok(final.result.shutdownProof);
  },
};

const EXEC = 'agent/execution.js';
const canaries = [
  ['the very first lease that names the command group already names its leader, so a supervisor killed right away leaves a provable orphan', 'supervisor.js', ['    leader: leaderIdentityOf(child.pid),', '    leader: null,']],
  ['a stop that never completes hits its deadline, is persisted unproven, and the RUNNING path stops heartbeating', EXEC, ['resolve(false), this.timing.guardStopMs + 5000', 'resolve(new Promise(() => {})), this.timing.guardStopMs + 5000']],
  ['a stop that never completes hits its deadline, is persisted unproven, and the RUNNING path stops heartbeating', EXEC, [['if (!context.escalating && !this.table.get(execId)?.guardStop?.unproven && (context.beatNow', 'if ((context.beatNow'], ['BEATING.has(this.table.get(execId)?.subphase) && !this.table.get(execId)?.guardStop?.unproven && !context.escalating,', 'BEATING.has(this.table.get(execId)?.subphase),']]],
  ['an unproven stop whose result exists does not renew the queue lease while it uploads', EXEC, [['if (!this.table.get(execId)?.guardStop?.unproven && (context.beatNow', 'if ((context.beatNow'], ['BEATING.has(this.table.get(execId)?.subphase) && !this.table.get(execId)?.guardStop?.unproven && !context.escalating,', 'BEATING.has(this.table.get(execId)?.subphase),']]],
  ['a legacy guard-kill record without guardKillAt is stamped at first sight and still escalates', EXEC, ["record.guardKillAt === undefined ? this.#cas(execId, ['RUNNING'], { guardKillAt: this.now() }) ?? record : record", 'record']],
  ['two running lanes share one host table read and one CPU read per guard tick', EXEC, ['this.#attempt(() => this.readTable(leaseIds)) : null', '(leaseIds.map((id) => this.#attempt(() => this.readTable([id]))), this.#attempt(() => this.readTable(leaseIds))) : null']],

  ['a SIGSTOPped supervisor that ignores the graceful cancel is SIGKILLed after the grace, the stop is verified, and the run finishes completed 124', EXEC, ['      context.escalatedAt = this.now();', '      return;\n      context.escalatedAt = this.now();']],
  ['an explicit 0 in the dispatch header turns the guard off for the lane, while an absent value takes the runner\'s global', 'agent/run-guard.js', ['fromHeader ?? fromConfig ?? 0', 'fromHeader > 0 ? fromHeader : fromConfig ?? 0']],
  ['a worker that daemonised and reparented to PID 1 keeps the lane alive while it burns CPU, even though the command itself is idle', 'agent/run-guard.js', ['[...new Set([this.#tracker.rootPid, ...this.#tracker.live(rows)])]', '[this.#tracker.rootPid]']],
  ['a hung workload is killed even while the supervisor itself burns CPU every interval', 'agent/run-guard.js', ['[...new Set([this.#tracker.rootPid, ...this.#tracker.live(rows)])]', '[...new Set([this.#tracker.rootPid, ...this.#tracker.live(rows), readLease(this.root, this.readLeaseId()).supervisorPid])]']],
  ['the guard keeps enforcing the run limit while a heartbeat never answers', EXEC, ['    this.#guardTimer = setTimeout(async () => {', '    this.#guardTimer = setTimeout(async () => {\n      await this.#contexts.values().next().value.promise;']],
  ['a record without runningSince gets its first observation persisted once, and two agent restarts never reset the wall-clock anchor', EXEC, ["if (record.runningSince === undefined) this.#cas(record.execId, ['RUNNING'], { runningSince: startedAt });", '']],

  ['a pull lane that hangs idle is killed by the agent\'s own no-progress guard and finishes red with exit 124', EXEC, ['const { message, ...guardKill } = guard.check(cpuRows, hostTable) ?? {};', 'const { message, ...guardKill } = {};']],
  ['a pull lane still running at maxRunMs is killed by the wall-clock backstop even while it burns CPU', 'agent/run-guard.js', ['if (this.now() - this.startedAt >= this.maxRunMs)', 'if (false)']],

  ['the lane\'s command sees the agent\'s configured speedFactor as LANE_BROKER_RUNNER_SPEED_FACTOR, so a repo can scale its time budgets', EXEC, ['String(this.config.speedFactor)', "'1'"]],
  ['a heartbeat answering fenced kills the lane and the record, and no ld_finish is ever sent', EXEC, ['if (answer.fenced === true) {', 'if (false) {']],
  ['ld_cancel on a running ticket reaches the lane within a heartbeat and finishes cancelled with cancelConfirmed', EXEC, ['} else if (answer.cancel === true) {', '} else if (false) {']],
  ['an agent killed mid-run leaves the lane running, and the next agent adopts it and finishes it without starting it again', 'agent/run.js', ['driver.adoptAll();', '']],
  ['under a hostile inherited LANE_BROKER_STATE, the agent and the lane it spawns use the configured stateDir', 'agent/run.js', ['Object.assign(process.env, brokerEnv);', '']],
  ['under a hostile inherited LANE_BROKER_STATE, the agent and the lane it spawns use the configured stateDir', 'agent/run.js', ['const laneEnv = { ...spawnEnv, ...brokerEnv };', 'const laneEnv = spawnEnv;']],
  ['a post-start shutdown result is finished with its proof, and nothing is released', EXEC,
    ['return this.#upload(record, context, result);', "if (result.reason === 'runner-shutdown') return this.#cas(execId, ['SETTLING'], { subphase: 'OUTBOX', outbox: { kind: 'release', proof: 'never-started' } });\n    return this.#upload(record, context, result);"]],
];

scenarioSuite('agent running and settling on a disposable Postgres', { timeout: 600_000, scenarios, canaries });
