import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { SLEEP, scenarioSuite } from './agent-run-harness.js';
import { wait, leaseOf } from './helpers/agent-guard-fixture.js';

const EXEC = 'agent/execution.js';

const scenarios = {
  'an explicit 0 in the dispatch header turns the guard off for the lane, while an absent value takes the runner\'s global': async (w, rw, load) => {
    const ticket = await rw.submit({ code: 'setTimeout(() => process.exit(0), 700); setInterval(() => {}, 1000)', headerOverrides: { noProgressTimeoutMs: 0, silenceTimeoutMs: 0 } });
    const agent = await rw.agent({ load, config: { noProgressTimeoutMs: 600, silenceTimeoutMs: 600 }, timing: { guardMs: 100 } });
    await rw.untilGone(agent, rw.execId(await rw.claimed(ticket)), wait(load));
    assert.deepEqual([(await w.ticket(ticket.id)).result.kind, (await w.ticket(ticket.id)).result.exit], ['completed', 0]);
  },

  'a worker that daemonised and reparented to PID 1 keeps the lane alive while it burns CPU, even though the command itself is idle': async (w, rw, load) => {
    const done = path.join(makeTmpDir('agent-daemon-'), 'done');
    const grandchild = `const end = Date.now() + 1800; while (Date.now() < end) {} require('fs').writeFileSync(${JSON.stringify(done)}, 'x')`;
    const middle = `require('child_process').spawn(process.execPath, ['-e', ${JSON.stringify(grandchild)}], { detached: true, stdio: 'ignore' }).unref()`;
    const code = `require('child_process').spawn(process.execPath, ['-e', ${JSON.stringify(middle)}], { detached: true, stdio: 'ignore' }).unref(); const t = setInterval(() => { if (require('fs').existsSync(${JSON.stringify(done)})) process.exit(0); }, 100);`;
    const ticket = await rw.submit({ code });
    const agent = await rw.agent({ load, config: { noProgressTimeoutMs: 600 }, timing: { guardMs: 100 } });
    await rw.untilGone(agent, rw.execId(await rw.claimed(ticket)), wait(load));
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
    const agent = await rw.agent({ load, config: { noProgressTimeoutMs: 600 }, timing: { guardMs: 100 }, driverOptions: { readRows: busySupervisor } });
    await rw.untilGone(agent, rw.execId(await rw.claimed(ticket)), wait(load));
    const final = await w.ticket(ticket.id);
    assert.deepEqual([final.result.exit, final.result.reason], [124, 'no-progress']);
  },
};

const canaries = [
  ['an explicit 0 in the dispatch header turns the guard off for the lane, while an absent value takes the runner\'s global', 'agent/run-guard.js', ['fromHeader ?? fromConfig ?? 0', 'fromHeader > 0 ? fromHeader : fromConfig ?? 0']],
  ['a worker that daemonised and reparented to PID 1 keeps the lane alive while it burns CPU, even though the command itself is idle', 'agent/run-guard.js', ['[...new Set([this.#tracker.rootPid, ...this.#tracker.live(rows)])]', '[this.#tracker.rootPid]']],
  ['a hung workload is killed even while the supervisor itself burns CPU every interval', 'agent/run-guard.js', ['[...new Set([this.#tracker.rootPid, ...this.#tracker.live(rows)])]', '[...new Set([this.#tracker.rootPid, ...this.#tracker.live(rows), readLease(this.root, this.readLeaseId()).supervisorPid])]']],
];

scenarioSuite('agent guard workload sampling (2) on a disposable Postgres', { timeout: 300_000, scenarios, canaries });
