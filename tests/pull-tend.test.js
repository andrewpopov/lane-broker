import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { paths } from '../src/state.js';
import { definePullSuite, pullFixture } from './helpers/pull-fixture.js';
import { tid } from './lane-dispatch-fixture.js';

/** BRAIN-1036 R6: `lane pull tend` steps the pull attempts whose supervisor is dead, in rank order, and rebuilds results the ledger holds (T11, T37b). */

const CRASH = new Error('killed after the ledger entry');
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function until(check, ms = 8000) {
  for (const deadline = Date.now() + ms; Date.now() < deadline;) {
    const value = await check();
    if (value) return value;
    await sleep(100);
  }
  return null;
}

const SCENARIOS = {
  'T11: a claimed ticket whose supervisor died gets URLs from one tender pass, a live supervisor is left alone, and the runner proceeds': async (load, w, t) => {
    const { tendOnce } = await load('pull-tend.js');
    const fx = await pullFixture(t, w, { load });
    await fx.enqueue();
    await fx.runner.claim();
    assert.deepEqual(await tendOnce(fx.ctx()), [], 'the supervisor is alive: it owns the ticket');
    assert.equal((await fx.runner.heartbeat()).urls, null);
    await fx.killSupervisor();
    const results = await tendOnce(fx.ctx());
    assert.deepEqual(results.map((r) => [r.id, r.state, r.next]), [[fx.id, 'claimed', 2000]]);
    assert.equal((await fx.runner.heartbeat()).urls?.epoch, 1, 'the runner has URLs inside its 60 s window');
    await fx.runner.start();
    await fx.runner.putOutputs({ log: 'tended\n' });
    await fx.runner.finish();
    assert.equal((await tendOnce(fx.ctx()))[0].state, 'settled');
    assert.equal(fx.published().exit, 0);
  },

  'a claimed ticket without URLs is stepped before a settling one, whatever order the attempts are listed in': async (load, w, t) => {
    const { tendOnce } = await load('pull-tend.js');
    const settling = await pullFixture(t, w, { load, id: tid(1) });
    await settling.hold();
    await settling.runner.finish();
    await settling.killSupervisor();
    const claimed = await pullFixture(t, w, { load, id: tid(2), stub: settling.stub, state: settling.state });
    await claimed.enqueue();
    await claimed.runner.claim();
    await claimed.killSupervisor();
    const results = await tendOnce({ ...settling.ctx(), concurrency: 1 });
    assert.deepEqual(results.map((r) => r.id), [claimed.id, settling.id]);
    assert.equal(settling.published().exit, 0);
  },

  'T37b: a ledger entry with neither an attempt nor a result is rebuilt by one tender pass, once': async (load, w, t) => {
    const { tendOnce } = await load('pull-tend.js');
    const fx = await pullFixture(t, w, { load });
    await fx.hold();
    await fx.runner.finish({ exit: 4 });
    fx.hooks.afterLedger = () => { throw CRASH; };
    await assert.rejects(fx.step(), CRASH);
    delete fx.hooks.afterLedger;
    fs.rmSync(path.join(paths(fx.state).attempts, `${fx.id}.json`));
    assert.equal(fx.published(), null);
    const results = await tendOnce(fx.ctx());
    assert.deepEqual(results.map((r) => [r.id, r.state]), [[fx.id, 'settled']]);
    assert.equal(fx.published().exit, 4, 'the ledgered exit, never 1');
    assert.equal(fx.writes.length, 1);
    assert.deepEqual(await tendOnce(fx.ctx()), [], 'nothing is left to rebuild');
    assert.equal(fx.writes.length, 1, 'rebuilt once');
  },

  'the serve loop keeps passing: a ticket that becomes orphaned after the first pass is tended without a restart': async (load, w, t) => {
    const { tendServe } = await load('pull-tend.js');
    const fx = await pullFixture(t, w, { load, cfg: { pollMs: 1200 } });
    const stop = new AbortController();
    const serving = tendServe({ ...fx.ctx(), signal: stop.signal });
    await sleep(300);
    await fx.enqueue();
    await fx.runner.claim();
    await fx.killSupervisor();
    const urls = await until(async () => (await fx.runner.heartbeat()).urls);
    stop.abort();
    await serving;
    assert.equal(urls?.epoch, 1, 'a later pass attached the URLs');
  },
};

const CANARIES = [
  ['T11: a claimed ticket whose supervisor died gets URLs from one tender pass, a live supervisor is left alone, and the runner proceeds', 'pull-tend.js', ["a.transport === 'pull' && !supervisorAlive(a)", "a.transport === 'pull' && supervisorAlive(a)"]],
  ['a claimed ticket without URLs is stepped before a settling one, whatever order the attempts are listed in', 'pull-tend.js', ['.sort((a, b) => a.rank - b.rank)', '.sort((a, b) => b.rank - a.rank)']],
  ['T37b: a ledger entry with neither an attempt nor a result is rebuilt by one tender pass, once', 'pull-tend.js', [', ...resultlessLedgerTickets(root, new Set(attempts.map((a) => a.id)))]', ']']],
  ['the serve loop keeps passing: a ticket that becomes orphaned after the first pass is tended without a restart', 'pull-tend.js', ['while (!signal?.aborted) {', 'if (!signal?.aborted) {']],
];

definePullSuite('lane pull tend against a disposable queue, a stub runner and the S3 stub', SCENARIOS, CANARIES);

test('the delay to the next pass follows the soonest step, within one second and the poll interval', async () => {
  const { nextDelayMs } = await import('../src/pull-tend.js');
  assert.equal(nextDelayMs([], 15_000), 15_000);
  assert.equal(nextDelayMs([{ next: 2000 }, { next: 15_000 }], 15_000), 2000);
  assert.equal(nextDelayMs([{ next: 0 }], 15_000), 1000);
  assert.equal(nextDelayMs([{ error: 'down' }, { next: 2000 }], 15_000), 2000);
  assert.equal(nextDelayMs([{ error: 'down' }], 15_000), 15_000);
  assert.equal(nextDelayMs([{ next: 60_000 }], 15_000), 15_000);
});
