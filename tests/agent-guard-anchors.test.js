import assert from 'node:assert/strict';
import { logins, call } from './lane-dispatch-fixture.js';
import { SLEEP, happens, scenarioSuite, sleep } from './agent-run-harness.js';
import { wait, leaseOf, runningLane } from './helpers/agent-guard-fixture.js';

const EXEC = 'agent/execution.js';

const scenarios = {
  'a record without runningSince gets its first observation persisted once, and two agent restarts never reset the wall-clock anchor': async (w, rw, load) => {
    const run = await runningLane(w, rw, load, { config: { maxRunMs: 3_600_000 }, timing: { guardMs: 60_000 } });
    const { runningSince: _dropped, ...legacy } = run.agent.core.table.get(run.execId);
    run.agent.core.table.put(legacy);
    await run.agent.stop();
    let anchor;
    for (const restart of [1, 2]) {
      const next = await rw.agent({ load, config: { maxRunMs: 3_600_000 }, timing: { guardMs: 100 } });
      await happens(`restart ${restart} has a runningSince`, () => next.core.table.get(run.execId)?.runningSince !== undefined, wait(load));
      anchor ??= next.core.table.get(run.execId).runningSince;
      await sleep(300);
      assert.equal(next.core.table.get(run.execId)?.runningSince, anchor, `restart ${restart} kept the anchor`);
      await next.stop();
    }
    for (const target of [run.lease.supervisorPid, -run.lease.childPgid]) { try { process.kill(target, 'SIGKILL'); } catch { /* gone */ } }
  },

  'two running lanes share one host table read and one CPU read per guard tick': async (w, rw, load) => {
    const { readProcCpuRows } = await load('preemptible.js');
    const { readProcessTable } = await load('descendants.js');
    const cpuReads = [];
    const tableReads = [];
    const tickA = await rw.submit({ code: SLEEP });
    const tickB = await rw.submit({ code: SLEEP });
    const agent = await rw.agent({
      load, config: { noProgressTimeoutMs: 600_000 }, timing: { guardMs: 100 },
      driverOptions: { readRows: () => { cpuReads.push(1); return readProcCpuRows(); }, readTable: (ids) => { tableReads.push(ids.length); return readProcessTable(ids); } },
    });
    await rw.claimed(tickA);
    await rw.claimed(tickB);
    await happens('both lanes were sampled in one tick', () => tableReads.includes(2), wait(load));
    await sleep(600);
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
};

const canaries = [
  ['the very first lease that names the command group already names its leader, so a supervisor killed right away leaves a provable orphan', 'supervisor.js', ['    leader: leaderIdentityOf(child.pid),', '    leader: null,']],
  ['two running lanes share one host table read and one CPU read per guard tick', EXEC, ['this.#attempt(() => this.readTable(leaseIds)) : null', '(leaseIds.map((id) => this.#attempt(() => this.readTable([id]))), this.#attempt(() => this.readTable(leaseIds))) : null']],
  ['a record without runningSince gets its first observation persisted once, and two agent restarts never reset the wall-clock anchor', EXEC, ["if (record.runningSince === undefined) this.#cas(record.execId, ['RUNNING'], { runningSince: startedAt });", '']],
];

scenarioSuite('agent guard anchors, shared scans and the first lease on a disposable Postgres', { timeout: 300_000, scenarios, canaries });
