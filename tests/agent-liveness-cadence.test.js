import assert from 'node:assert/strict';
import { logins } from './lane-dispatch-fixture.js';
import { SLEEP, happens, sleep, scenarioSuite } from './agent-run-harness.js';

const INTERVAL = 200;
const SLACK = 300;
const LIVENESS = ['ld_claim', 'ld_heartbeat', 'ld_register'];
const DAYS = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];

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

  'a draining runner holding nothing still makes a call every interval': async (w, rw, load) => {
    const agent = await rw.agent({ load, config: { pollMs: INTERVAL }, draining: () => true });
    const from = Date.now();
    await sleep(1500);
    assertCadence(agent, from, Date.now(), 'draining');
    assert.equal(agent.names().includes('ld_claim'), false, 'a draining runner claims nothing');
  },

  'a runner outside its schedule window holding nothing still makes a call every interval': async (w, rw, load) => {
    const tomorrow = DAYS[(new Date().getUTCDay() + 1) % 7];
    const agent = await rw.agent({ load, config: { pollMs: INTERVAL, schedule: { timezone: 'UTC', available: [{ days: [tomorrow], from: '00:00', to: '23:59' }] } } });
    const from = Date.now();
    await sleep(1500);
    assertCadence(agent, from, Date.now(), 'outside the window');
    assert.equal(agent.names().includes('ld_claim'), false, 'outside its window it claims nothing');
  },

  'a NOTIFY wake mid-interval does not stretch the gap to the next call: the wait is capped by the liveness deadline': async (w, rw, load) => {
    const period = 400;
    const agent = await rw.agent({ load, config: { pollMs: period }, draining: () => true });
    const su = await w.su();
    const from = Date.now();
    for (let round = 0; round < 3; round++) {
      const seen = agent.timesOf('ld_register').length;
      await happens('a ping was sent', () => agent.timesOf('ld_register').length > seen, 3000);
      await sleep(Math.round(period * 0.45));
      await su.query("SELECT pg_notify('ld_work', '')");
    }
    await sleep(period);
    const times = [from, ...agent.timesOf(...LIVENESS).filter((t) => t >= from), Date.now()];
    const gaps = times.slice(1).map((t, i) => t - times[i]);
    assert.ok(Math.max(...gaps) <= period + 100, `a NOTIFY mid-interval stretched a gap to ${Math.max(...gaps)} ms (gaps ${gaps.join(',')})`);
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
  ['a liveness call is made every interval while idle', 'agent/run.js', ['await waker.wait(Math.max(0, Math.min(pollAt, lastSignOfLife() + config.pollMs) - now()));', 'await waker.wait(1e9);']],
  ['a draining runner holding nothing still makes a call every interval', 'agent/run.js', ['now() - lastSignOfLife() >= config.pollMs', 'false']],
  ['a runner outside its schedule window holding nothing still makes a call every interval', 'agent/run.js', ['now() - lastSignOfLife() >= config.pollMs', 'false']],
  ['a NOTIFY wake mid-interval does not stretch the gap to the next call: the wait is capped by the liveness deadline', 'agent/run.js', ['Math.min(pollAt, lastSignOfLife() + config.pollMs)', 'pollAt']],
  ['at max_claims, with the claim poll far away, heartbeats keep the liveness calls coming', 'agent/execution.js', ['>= this.timing.heartbeatMs) await this.#beat(execId, context);\n    const current', '>= 1e9) await this.#beat(execId, context);\n    const current']],
  ['while settling, with an upload that never answers, heartbeats keep the liveness calls coming', 'agent/execution.js',
    ['context.abort = abort;\n    const stopBeating = this.#keepBeating(execId, context, abort.signal);\n    try {\n      const since', 'context.abort = abort;\n    const stopBeating = async () => {};\n    try {\n      const since']],
];

scenarioSuite('agent liveness cadence on a disposable Postgres', { scenarios, canaries });
