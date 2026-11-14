import assert from 'node:assert/strict';
import fs from 'node:fs';
import { call, claim, holdLock, logins, waitBlocked } from './lane-dispatch-fixture.js';
import { agentPg, backendPids } from './agent-harness.js';
import { recordingFetch, happens, scenarioSuite } from './agent-run-harness.js';

const oneOffer = () => { let given = false; return () => (given++ ? null : { freeCpu: 8, freeMem: 4e9 }); };
const STREAM = '/lane-snap/streams/';
const noBeats = { heartbeatMs: 60_000 };

/** `ld_start` blocked on the row lock, then the agent gone: the call commits with nobody to hear the answer. The record is left STARTING. */
async function abandonedStart(w, rw, load) {
  const ticket = await rw.submit();
  let hold;
  const holdAtStream = recordingFetch(async (url) => {
    if (url.includes(STREAM)) hold = await holdLock(w, `SELECT 1 FROM lane_dispatch.tickets WHERE id = '${ticket.id}' FOR UPDATE`);
    return undefined;
  });
  const first = await rw.agent({ load, fetchImpl: holdAtStream, timing: noBeats, figures: oneOffer() });
  const row = await rw.claimed(ticket);
  await happens('ld_start is in flight', () => first.names().includes('ld_start'));
  const [callPid] = await backendPids(w, 'SELECT lane_dispatch%');
  await waitBlocked(w, callPid);
  await first.stop();
  await hold.release();
  await happens('the abandoned ld_start committed', async () => (await w.ticket(ticket.id)).state === 'running');
  const execId = rw.execId(row);
  assert.equal(first.core.table.get(execId).subphase, 'STARTING');
  return { ticket, execId };
}

const scenarios = {
  'a cancel that lands after the last heartbeat and before ld_start: ld_start answers false, nothing is spawned, the row ends cancelled': async (w, rw, load) => {
    const ticket = await rw.submit();
    const su = await w.su();
    const cancelBeforeStart = recordingFetch(async (url) => {
      if (url.includes(STREAM)) await su.query('UPDATE lane_dispatch.tickets SET cancel_requested = true WHERE id = $1', [ticket.id]);
      return undefined;
    });
    const agent = await rw.agent({ load, fetchImpl: cancelBeforeStart, timing: noBeats, figures: oneOffer() });
    const row = await rw.claimed(ticket);
    await rw.untilGone(agent, rw.execId(row));
    assert.equal((await w.ticket(ticket.id)).state, 'cancelled');
    assert.deepEqual(agent.callsTo('ld_release').map((a) => a[3]), ['never-started']);
    assert.equal(agent.names().includes('ld_finish'), false);
    assert.equal(fs.existsSync(rw.ticketDir(rw.execId(row))), false, 'no execution directory: nothing was spawned');
  },

  'a drain marker after the claim releases never-started before ld_start is ever called': async (w, rw, load) => {
    const ticket = await rw.submit();
    let drain = false;
    const drainAtStream = recordingFetch(async (url) => { if (url.includes(STREAM)) drain = true; return undefined; });
    const agent = await rw.agent({ load, fetchImpl: drainAtStream, timing: noBeats, figures: oneOffer(), draining: () => drain });
    const row = await rw.claimed(ticket);
    await rw.untilGone(agent, rw.execId(row));
    assert.equal(agent.names().includes('ld_start'), false, 'the drain check comes before ld_start');
    assert.deepEqual(agent.callsTo('ld_release').map((a) => a[3]), ['never-started']);
    assert.equal((await w.ticket(ticket.id)).state, 'queued');
    assert.equal(fs.existsSync(rw.ticketDir(rw.execId(row))), false);
  },

  'a STARTING record found on restart whose ld_start answers true finishes rejected start-uncertain and spawns nothing': async (w, rw, load) => {
    const { ticket, execId } = await abandonedStart(w, rw, load);
    const second = await rw.agent({ load, figures: oneOffer() });
    await rw.untilGone(second, execId);
    const final = await w.ticket(ticket.id);
    assert.equal(final.state, 'finished');
    assert.equal(final.result.kind, 'rejected');
    assert.equal(final.result.reason, 'start-uncertain');
    assert.equal(final.result.neverStarted, true);
    assert.equal(second.names().filter((n) => n === 'ld_start').length, 1, 'ld_start was called again');
    assert.equal(fs.existsSync(rw.ticketDir(execId)), false, 'nothing was spawned after the restart');
    assert.equal(second.names().includes('ld_release'), false);
  },

  'a recovered STARTING release refused as started is converted to a finish, never acknowledged: cancelled with its stop proven when a cancel is pending': async (w, rw, load) => {
    const { ticket, execId } = await abandonedStart(w, rw, load);
    await (await w.su()).query('UPDATE lane_dispatch.tickets SET cancel_requested = true WHERE id = $1', [ticket.id]);
    const second = await rw.agent({ load, figures: oneOffer() });
    await rw.untilGone(second, execId);
    const names = second.names();
    assert.deepEqual(second.callsTo('ld_release').map((a) => a[3]), ['never-started']);
    assert.ok(names.indexOf('ld_release') < names.indexOf('ld_heartbeat') && names.indexOf('ld_heartbeat') < names.indexOf('ld_finish'), `the refused release, one heartbeat for the cancel, then a finish: ${names.join(',')}`);
    const final = await w.ticket(ticket.id);
    assert.deepEqual([final.state, final.result.kind, final.result.reason], ['cancelled', 'cancelled', 'start-uncertain']);
    assert.deepEqual([final.result.stopProven, final.result.neverStarted, final.result.cancelConfirmed], [true, true, true]);
    assert.equal(fs.existsSync(rw.ticketDir(execId)), false);
  },

  'a release whose first response was lost is acknowledged when the retry answers fenced: the record ends DONE, no kill': async (w, rw, load) => {
    const { newExecutionRecord } = await load('agent/store.js');
    const ticket = await rw.submit();
    const claimed = await claim(await w.as(logins.a1));
    assert.equal(claimed.id, ticket.id);
    await call(await w.as(logins.a1), 'ld_release', claimed.id, claimed.epoch, claimed.token, 'never-started');
    const { openAgentCore } = await load('agent/agent.js');
    const seed = await openAgentCore({ remoteRoot: rw.root, config: { pg: agentPg(w), schedule: null }, version: 't', log: () => {} });
    seed.table.put({ ...newExecutionRecord(claimed), subphase: 'OUTBOX', outbox: { kind: 'release', proof: 'never-started' } });
    await seed.close();
    const agent = await rw.agent({ load, figures: () => null });
    const execId = `${claimed.id}.e${claimed.epoch}`;
    await rw.untilGone(agent, execId);
    assert.deepEqual(agent.callsTo('ld_release').map((a) => a[3]), ['never-started']);
    assert.equal(agent.names().includes('ld_finish'), false);
    assert.equal(fs.existsSync(rw.ticketDir(execId)), false, 'nothing was killed or spawned');
    assert.equal((await w.ticket(ticket.id)).state, 'queued');
  },
};

const EXEC = 'agent/execution.js';
const canaries = [
  ['a cancel that lands after the last heartbeat and before ld_start: ld_start answers false, nothing is spawned, the row ends cancelled', EXEC, ['if (started !== true) this.#releasing(record);', 'if (false) this.#releasing(record);']],
  ['a drain marker after the claim releases never-started before ld_start is ever called', EXEC, ['if (this.draining() || this.scopeBlock()) return this.#releasing(record);', '']],
  ['a STARTING record found on restart whose ld_start answers true finishes rejected start-uncertain and spawns nothing', EXEC, ["else if (!context.mayStart) this.#rejected(record, ['STARTING'], 'start-uncertain');", "else if (false) this.#rejected(record, ['STARTING'], 'start-uncertain');"]],
  ['a recovered STARTING release refused as started is converted to a finish, never acknowledged: cancelled with its stop proven when a cancel is pending', EXEC, ["(answer.refused === 'started' ? this.#cas(execId, ['OUTBOX'], { outbox: { ...outbox, refused: true } }) : ack())", 'ack()']],
  ['a recovered STARTING release refused as started is converted to a finish, never acknowledged: cancelled with its stop proven when a cancel is pending', EXEC, ['|| beat.cancel === true;', ';']],
  ['a release whose first response was lost is acknowledged when the retry answers fenced: the record ends DONE, no kill', EXEC, [": ack())", ": answer.fenced === true ? null : ack())"]],
];

scenarioSuite('agent start gate on a disposable Postgres', { scenarios, canaries });
