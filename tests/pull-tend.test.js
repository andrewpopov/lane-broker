import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { readAttempt } from '../src/attempts.js';
import { ledgerAccept } from '../src/pull-ledger.js';
import { paths, writeCancelMarkerFile } from '../src/state.js';
import { definePullSuite, pullFixture } from './helpers/pull-fixture.js';
import { tid } from './lane-dispatch-fixture.js';

/** BRAIN-1036 R6: `lane pull tend` steps the pull attempts whose supervisor is dead, in rank order, and rebuilds results the ledger holds (T11, T37b). */

const CRASH = new Error('killed after the ledger entry');
async function silenced(fn) {
  const [out, err] = [process.stdout.write, process.stderr.write];
  process.stdout.write = () => true;
  process.stderr.write = () => true;
  try {
    return await fn();
  } finally {
    Object.assign(process.stdout, { write: out });
    Object.assign(process.stderr, { write: err });
  }
}
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
  'T11: a claimed ticket whose supervisor died gets URLs from one tender pass inside the runner 60 s window, and the runner proceeds': async (load, w, t) => {
    const { tendOnce } = await load('pull-tend.js');
    const fx = await pullFixture(t, w, { load });
    await fx.enqueue();
    await fx.runner.claim();
    const claimedAt = Date.now();
    await fx.killSupervisor();
    assert.equal((await fx.runner.heartbeat()).urls, null);
    const results = await tendOnce(fx.ctx());
    assert.deepEqual(results.map((r) => [r.id, r.state, r.next]), [[fx.id, 'claimed', 2000]]);
    const urls = (await fx.runner.heartbeat()).urls;
    assert.equal(urls?.epoch, 1, 'the dead supervisor\'s ticket was recovered by the tender');
    assert.ok(Date.now() - claimedAt < 60_000, 'inside the window before the runner releases a claim with no URLs');
    await fx.runner.start();
    await fx.runner.putOutputs({ log: 'tended\n' });
    await fx.runner.finish();
    assert.equal((await tendOnce(fx.ctx()))[0].state, 'settled');
    assert.equal(fx.published().exit, 0);
  },

  'a live supervisor owns its ticket: the tender steps nothing for it': async (load, w, t) => {
    const { tendOnce } = await load('pull-tend.js');
    const fx = await pullFixture(t, w, { load });
    await fx.enqueue();
    await fx.runner.claim();
    assert.deepEqual(await tendOnce(fx.ctx()), []);
    assert.equal((await fx.runner.heartbeat()).urls, null);
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

  'a hung ticket cannot hold the tender: past its hard deadline its slot frees, and a ticket claimed meanwhile gets URLs inside the 60 s window': async (load, w, t) => {
    const { tendServe } = await load('pull-tend.js');
    const slow = await pullFixture(t, w, { load, id: tid(1) });
    await slow.enqueue();
    await slow.runner.claim();
    await slow.killSupervisor();
    let hung = false;
    const queue = { ...slow.queue, get: (id) => (id === slow.id ? (hung = true, new Promise(() => {})) : slow.queue.get(id)) };
    const stop = new AbortController();
    const serving = tendServe({ ...slow.ctx({ queue }), concurrency: 1, hardDeadlineMs: 700, signal: stop.signal });
    assert.ok(await until(() => hung), 'the slow ticket is being stepped and holds the only slot');
    const fast = await pullFixture(t, w, { load, id: tid(2), stub: slow.stub, state: slow.state });
    await fast.enqueue();
    await fast.runner.claim();
    const claimedAt = Date.now();
    await fast.killSupervisor();
    const urls = await until(async () => (await fast.runner.heartbeat()).urls);
    stop.abort();
    await serving;
    assert.equal(urls?.epoch, 1);
    assert.ok(Date.now() - claimedAt < 60_000);
  },

  'a cancel recorded before the process died is sent by the tender, and so is a cancel marker with no supervisor left': async (load, w, t) => {
    const { tendOnce } = await load('pull-tend.js');
    const { cancelAttempt } = await load('cancel.js');
    const fx = await pullFixture(t, w, { load });
    await fx.enqueue();
    await fx.killSupervisor();
    const dying = { ...fx.queue, cancel: async () => { throw CRASH; } };
    const died = await silenced(() => cancelAttempt(fx.state, fx.id, readAttempt(fx.state, fx.id), { orphanOnly: true, pullContext: (ticket) => fx.ctx({ generation: ticket.generation, queue: dying }) }));
    assert.equal(died.exitCode, 1);
    assert.equal(typeof fx.pull().cancel.requestedAt, 'number');
    assert.equal(fx.pull().cancel.answer, undefined, 'the intent is durable and no answer was recorded');
    assert.equal((await fx.row()).state, 'queued');
    await tendOnce(fx.ctx());
    assert.equal((await fx.row()).state, 'cancelled');
    assert.deepEqual([fx.published().exit, fx.published().remoteCancelConfirmed], [130, true]);

    const marked = await pullFixture(t, w, { load, id: tid(2), stub: fx.stub });
    await marked.enqueue();
    writeCancelMarkerFile(marked.state, marked.id);
    await marked.killSupervisor();
    await tendOnce(marked.ctx());
    assert.equal((await marked.row()).state, 'cancelled', 'a cancel marker the supervisor never acted on is sent by the tender');
  },

  'the tender rebuilds only ledger decisions younger than the result retention, and a bounded batch per pass': async (load, w, t) => {
    const { createTender } = await load('pull-tend.js');
    const fx = await pullFixture(t, w, { load });
    const decision = { kind: 'lost', exit: 75, reason: 'x' };
    const entry = (n, ageMs) => ledgerAccept(fx.state, { logicalId: tid(n), decision, epoch: null, generation: 0, acceptedAt: Date.now() - ageMs });
    for (const n of [10, 11, 12, 13, 14]) entry(n, 1000);
    entry(20, 30 * 24 * 3_600_000);
    const tender = createTender({ ...fx.ctx({ writeResult: undefined }), ledgerTicketsPerPass: 2 });
    await Promise.all(await tender.round());
    assert.equal(tender.results.length, 2, 'one pass looks at a bounded batch');
    while ((await Promise.all(await tender.round())).length > 0);
    const published = (n) => fs.existsSync(path.join(paths(fx.state).results, `${tid(n)}.json`));
    assert.deepEqual([10, 11, 12, 13, 14].map(published), [true, true, true, true, true]);
    assert.equal(published(20), false, 'a result lane gc expired stays expired');
  },

  'the ledger scan window rotates, so a bounded window never starves later entries': async (load, w, t) => {
    const { createTender } = await load('pull-tend.js');
    const fx = await pullFixture(t, w, { load });
    for (const n of [10, 11, 12, 13, 14]) ledgerAccept(fx.state, { logicalId: tid(n), decision: { kind: 'lost', exit: 75, reason: 'x' }, epoch: null, generation: 0 });
    const tender = createTender({ ...fx.ctx({ writeResult: undefined }), ledgerNamesPerPass: 3 });
    for (let pass = 0; pass < 3; pass++) await Promise.all(await tender.round());
    assert.deepEqual([10, 11, 12, 13, 14].map((n) => fs.existsSync(path.join(paths(fx.state).results, `${tid(n)}.json`))), [true, true, true, true, true]);
  },

  'outstanding steps are capped at a small multiple of the slots: abandoned steps and an abandoned listing are not piled up round after round': async (load, w, t) => {
    const { createTender } = await load('pull-tend.js');
    const first = await pullFixture(t, w, { load, id: tid(1) });
    await first.enqueue();
    await first.killSupervisor();
    for (const n of [2, 3, 4]) {
      const more = await pullFixture(t, w, { load, id: tid(n), stub: first.stub, state: first.state });
      await more.enqueue();
      await more.killSupervisor();
    }
    const counts = { gets: 0, lists: 0 };
    const slowly = (name, fn) => async (...args) => { counts[name]++; await sleep(2500); return fn(...args); };
    const queue = { ...first.queue, get: slowly('gets', (id) => first.queue.get(id)), listMine: slowly('lists', (opts) => first.queue.listMine(opts)) };
    const tender = createTender({ ...first.ctx({ queue }), concurrency: 1, hardDeadlineMs: 150 });
    for (let round = 0; round < 6; round++) await Promise.all(await tender.round());
    assert.equal(counts.gets, 3, 'one slot, three outstanding: the fourth ticket waits for an abandoned step to finish');
    assert.equal(counts.lists, 1, 'one abandoned listing, never another on top of it');
    await sleep(3000);
  },

  'the ledger scan counts only tickets it could step: an entry already stepped does not take the place of a fresh one': async (load, w, t) => {
    const { createTender } = await load('pull-tend.js');
    const fx = await pullFixture(t, w, { load });
    const accept = (n) => ledgerAccept(fx.state, { logicalId: tid(n), decision: { kind: 'lost', exit: 75, reason: 'x' }, epoch: null, generation: 0 });
    const failing = fx.ctx({ writeResult: () => { throw new Error('disk full'); } });
    accept(11);
    const tender = createTender({ ...failing, ledgerTicketsPerPass: 1 });
    await Promise.all(await tender.round());
    assert.deepEqual(tender.results.map((r) => r.id), [tid(11)]);
    accept(10);
    await Promise.all(await tender.round());
    assert.deepEqual(tender.results.map((r) => r.id), [tid(11), tid(10)], 'the very next round steps the new entry, not the one still waiting out its error');
  },

  'tendOnce steps each ticket once per invocation even when the clock says every ticket is due again': async (load, w, t) => {
    const { tendOnce } = await load('pull-tend.js');
    const fx = await pullFixture(t, w, { load });
    await fx.enqueue();
    await fx.killSupervisor();
    let gets = 0;
    let calls = 0;
    let at = Date.now();
    const now = () => {
      if (++calls > 2000) throw new Error('too many rounds');
      return (at += 600_000);
    };
    const queue = { ...fx.queue, get: (id) => { gets++; return fx.queue.get(id); } };
    const results = await tendOnce({ ...fx.ctx({ queue, now }) });
    assert.deepEqual([results.length, gets], [1, 1]);
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
  ['T11: a claimed ticket whose supervisor died gets URLs from one tender pass inside the runner 60 s window, and the runner proceeds', 'pull-tend.js', ["a.transport === 'pull' && !supervisorAlive(a)", "a.transport === 'pull' && !supervisorAlive(a) && false"]],
  ['a live supervisor owns its ticket: the tender steps nothing for it', 'pull-tend.js', ["a.transport === 'pull' && !supervisorAlive(a)", "a.transport === 'pull'"]],
  ['a hung ticket cannot hold the tender: past its hard deadline its slot frees, and a ticket claimed meanwhile gets URLs inside the 60 s window', 'pull-tend.js', ["    return withDeadline(step, hardDeadlineMs, () => record(id, { error: `no answer within the ${hardDeadlineMs}ms step deadline` })).finally(() => {", '    return step.finally(() => {']],
  ['a cancel recorded before the process died is sent by the tender, and so is a cancel marker with no supervisor left', 'pull-ticket.js', ['(pull?.cancel?.requestedAt !== undefined && pull.cancel.answer === undefined) || ', '']],
  ['a cancel recorded before the process died is sent by the tender, and so is a cancel marker with no supervisor left', 'pull-ticket.js', [' || isCancelled(run.root, run.id);', ';']],
  ['the tender rebuilds only ledger decisions younger than the result retention, and a bounded batch per pass', 'pull-tend.js', ['now - (entry.acceptedAt ?? 0) < resultRetentionMs', 'true']],
  ['the tender rebuilds only ledger decisions younger than the result retention, and a bounded batch per pass', 'pull-tend.js', ['while (examined < Math.min(names.length, namesPerPass) && found.length < ticketsPerPass) {', 'while (examined < Math.min(names.length, namesPerPass)) {']],
  ['the ledger scan window rotates, so a bounded window never starves later entries', 'pull-tend.js', ['  ledgerScanOffsets.set(root, start + examined);\n', '']],
  ['a claimed ticket without URLs is stepped before a settling one, whatever order the attempts are listed in', 'pull-tend.js', ['.sort((a, b) => a.rank - b.rank)', '.sort((a, b) => b.rank - a.rank)']],
  ['T37b: a ledger entry with neither an attempt nor a result is rebuilt by one tender pass, once', 'pull-tend.js', ['const tickets = [...attempts, ...ledgered];', 'const tickets = [...attempts];']],
  ['outstanding steps are capped at a small multiple of the slots: abandoned steps and an abandoned listing are not piled up round after round', 'pull-tend.js', ['Math.min(free, concurrency * OUTSTANDING_PER_SLOT - inFlight.size)', 'free']],
  ['outstanding steps are capped at a small multiple of the slots: abandoned steps and an abandoned listing are not piled up round after round', 'pull-tend.js', ['    if (listing) return new Map();\n', '']],
  ['the ledger scan counts only tickets it could step: an entry already stepped does not take the place of a fresh one', 'pull-tend.js', ['eligible: (id) => due(id) && !attempts.some((a) => a.id === id),', 'eligible: () => true,']],
  ['tendOnce steps each ticket once per invocation even when the clock says every ticket is due again', 'pull-tend.js', ['    for (const r of tender.results) stepped.add(r.id);\n', ''], /too many rounds/],
  ['the serve loop keeps passing: a ticket that becomes orphaned after the first pass is tended without a restart', 'pull-tend.js', ['while (!signal?.aborted) {', 'if (!signal?.aborted) {']],
];

definePullSuite('lane pull tend against a disposable queue, a stub runner and the S3 stub', SCENARIOS, CANARIES);

test('the delay to the next pass follows the soonest due ticket, within one second and the poll interval', async () => {
  const { dueDelayMs } = await import('../src/pull-tend.js');
  assert.equal(dueDelayMs([], 1000, 15_000), 15_000);
  assert.equal(dueDelayMs([3000, 20_000], 1000, 15_000), 2000);
  assert.equal(dueDelayMs([1000], 1000, 15_000), 1000);
  assert.equal(dueDelayMs([61_000], 1000, 15_000), 15_000);
});
