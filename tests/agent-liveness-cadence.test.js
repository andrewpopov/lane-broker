import assert from 'node:assert/strict';
import { logins } from './lane-dispatch-fixture.js';
import { SLEEP, happens, sleep, scenarioSuite } from './agent-run-harness.js';

const INTERVAL = 200;
const SLACK = 300;
const LIVENESS = ['ld_claim', 'ld_heartbeat'];

/** Every liveness call within `[from, to]` (and the edges themselves) is at most INTERVAL + SLACK after the one before it. */
const assertCadence = (agent, from, to, what) => {
  const times = [from, ...agent.timesOf(...LIVENESS).filter((t) => t >= from && t <= to), to];
  const gaps = times.slice(1).map((t, i) => t - times[i]);
  assert.ok(Math.max(...gaps) <= INTERVAL + SLACK, `${what}: a liveness call gap of ${Math.max(...gaps)} ms exceeds ${INTERVAL + SLACK} (gaps ${gaps.join(',')})`);
  assert.ok(times.length >= 5, `${what}: only ${times.length - 2} liveness calls`);
};

const scenarios = {
  'a liveness call is made every interval while idle': async (w, rw, load) => {
    const agent = await rw.agent({ load, config: { pollMs: INTERVAL } });
    const from = Date.now();
    await sleep(1500);
    assertCadence(agent, from, Date.now(), 'idle');
  },

  'at max_claims, with the claim poll far away, heartbeats keep the liveness calls coming': async (w, rw, load) => {
    const ticket = await rw.submit({ code: SLEEP });
    const agent = await rw.agent({ load, login: logins.x, config: { runnerName: 'x', pollMs: 60_000 }, timing: { heartbeatMs: INTERVAL } });
    const row = await rw.claimed(ticket);
    await happens('the lane is running', () => agent.core.table.get(rw.execId(row))?.subphase === 'RUNNING');
    const from = Date.now();
    await sleep(1500);
    assertCadence(agent, from, Date.now(), 'at max_claims');
  },

  'while settling, with an upload that never answers, heartbeats keep the liveness calls coming': async (w, rw, load) => {
    rw.s3.hangMatching = (p) => p.startsWith('/lane-out/');
    const ticket = await rw.submit({ code: 'process.exit(0)' });
    const agent = await rw.agent({ load, config: { pollMs: 60_000 }, timing: { heartbeatMs: INTERVAL } });
    const row = await rw.claimed(ticket);
    await happens('the record is settling', () => agent.core.table.get(rw.execId(row))?.subphase === 'SETTLING');
    const from = Date.now();
    await sleep(1500);
    assert.equal(agent.core.table.get(rw.execId(row))?.subphase, 'SETTLING');
    assertCadence(agent, from, Date.now(), 'settling');
  },
};

const canaries = [
  ['a liveness call is made every interval while idle', 'agent/run.js', ['await waker.wait(Math.max(0, config.pollMs - (now() - startedAt)));', 'await waker.wait(1e9);']],
  ['at max_claims, with the claim poll far away, heartbeats keep the liveness calls coming', 'agent/execution.js', ['>= this.timing.heartbeatMs) await this.#beat(execId, context);\n    const current', '>= 1e9) await this.#beat(execId, context);\n    const current']],
  ['while settling, with an upload that never answers, heartbeats keep the liveness calls coming', 'agent/execution.js',
    ['context.abort = abort;\n    const stopBeating = this.#keepBeating(execId, context, abort.signal);\n    try {\n      const since', 'context.abort = abort;\n    const stopBeating = async () => {};\n    try {\n      const since']],
];

scenarioSuite('agent liveness cadence on a disposable Postgres', { scenarios, canaries });
