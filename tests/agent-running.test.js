import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { logins, call } from './lane-dispatch-fixture.js';
import { SLEEP, happens, scenarioSuite } from './agent-run-harness.js';
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
    const run = await runningLane(w, rw, load, { code: "setTimeout(() => { process.stdout.write('done after the agent died\\n'); process.exit(0); }, 2500)" });
    await run.agent.stop();
    assert.equal(groupAlive(run.lease.childPgid), true, 'the lane outlived its agent');
    const second = await rw.agent({ load });
    await happens('the adopted lane finished', async () => (await w.ticket(run.ticket.id)).state === 'finished', 20_000);
    await rw.untilGone(second, run.execId);
    const final = await w.ticket(run.ticket.id);
    assert.deepEqual([final.result.kind, final.result.exit], ['completed', 0]);
    assert.equal(second.names().includes('ld_start'), false, 'the lane was adopted, not started again');
    const log = rw.s3.objects.get(`lane-out/a1/${run.ticket.id}/e${run.row.epoch}/log`).body;
    assert.match(log.toString(), /done after the agent died/);
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
  ['a heartbeat answering fenced kills the lane and the record, and no ld_finish is ever sent', EXEC, ['if (answer.fenced === true) {', 'if (false) {']],
  ['ld_cancel on a running ticket reaches the lane within a heartbeat and finishes cancelled with cancelConfirmed', EXEC, ['} else if (answer.cancel === true) {', '} else if (false) {']],
  ['an agent killed mid-run leaves the lane running, and the next agent adopts it and finishes it without starting it again', 'agent/run.js', ['driver.adoptAll();', '']],
  ['a post-start shutdown result is finished with its proof, and nothing is released', EXEC,
    ['return this.#upload(record, context, result);', "if (result.reason === 'runner-shutdown') return this.#cas(execId, ['SETTLING'], { subphase: 'OUTBOX', outbox: { kind: 'release', proof: 'never-started' } });\n    return this.#upload(record, context, result);"]],
];

scenarioSuite('agent running and settling on a disposable Postgres', { timeout: 600_000, scenarios, canaries });
