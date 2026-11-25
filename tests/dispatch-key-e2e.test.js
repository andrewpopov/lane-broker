import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { after as afterAll, afterEach, before, describe, test } from 'node:test';
import { paths } from '../src/state.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { pullE2E } from './helpers/pull-e2e.js';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, logins, enqueue, downgradeToV9 } from './lane-dispatch-fixture.js';
import { scaledMs } from './helpers.js';

/** PKG-438: `lane run --dispatch-key` through a real supervisor against a disposable queue. */

const KEY = 'train:r:3:1';
const keyed = ['--dispatch-key', KEY];
const ticketCount = async (w) => (await (await w.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.tickets')).rows[0].n;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const SCENARIOS = {
  'a key the queue holds for a ticket this machine knows is that ticket: a second detach prints its id and starts nothing': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const id = await fx.detach(['true'], keyed);
    await fx.enqueued();
    const second = fx.lane(['run', '--repo', 'r', '--lane', 'default', '--detach', ...keyed, '--', 'true']);
    const again = await Promise.race([second, sleep(30_000).then(() => assert.fail('the second detach did not return: it started a second run instead of reporting the first'))]);
    assert.equal(again.code, 0, again.stderr);
    assert.equal(again.stdout.trim(), id);
    assert.doesNotMatch(again.stderr, /detached/, 'no second detached run was started');
    assert.equal(await ticketCount(w), 1);
  },

  'once the keyed ticket finished a foreground run reports its exit and does not run again, and lane status finds it by key': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const id = await fx.detach(['true'], keyed);
    await fx.enqueued();
    await fx.run1({ exit: 3 });
    assert.equal((await fx.wait(id)).code, 3);
    await sleep(1500);
    assert.deepEqual(fx.supervisorExits(), [3]);
    const rerun = await fx.run(fx.markLocalRun, keyed);
    assert.equal(rerun.code, 3, rerun.stderr);
    assert.match(rerun.stderr, new RegExp(`dispatch key ${KEY} is ticket ${id}`));
    assert.equal(fx.ranLocally(), false);
    assert.equal(await ticketCount(w), 1, 'the resubmit queued nothing');
    await sleep(1500);
    assert.deepEqual(fx.supervisorExits(), [3], 'no second supervisor was started');

    const status = await fx.lane(['status', '--dispatch-key', KEY, '--json']);
    assert.equal(status.code, 0, status.stderr);
    assert.deepEqual(JSON.parse(status.stdout), { dispatchKey: KEY, found: true, id, state: 'finished', host: 'a2' });
    assert.equal((await fx.lane(['status', '--dispatch-key', 'train:none'])).code, 1);
    assert.equal((await fx.lane(['status', '--dispatch-key', 'bad key'])).code, 64);
    assert.equal((await fx.run(['true'], ['--dispatch-key', 'bad key'])).code, 64);
    assert.equal((await fx.run(['true'], [...keyed, '--detach', '--id', crypto.randomUUID()])).code, 64);
  },

  'a key the queue holds for a ticket this machine has never seen is adopted: the run takes its id and queues no second ticket': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const held = crypto.randomUUID();
    await enqueue(await w.as(logins.s1), held, { dispatchKey: KEY });
    const started = await fx.lane(['run', '--repo', 'r', '--lane', 'default', '--detach', ...keyed, '--', 'true']);
    assert.equal(started.code, 0, started.stderr);
    t.after(() => fx.lane(['cancel', held]));
    assert.equal(started.stdout.trim(), held);
    for (let i = 0; i < 100 && !fx.attempt(held); i++) await sleep(100);
    assert.ok(fx.attempt(held), 'the local attempt is keyed by the held ticket');
    await sleep(1500);
    assert.equal(await ticketCount(w), 1);
  },

  'a keyed run that would run locally is refused 76 "dispatch key needs the pull queue" and nothing runs or queues': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load });
    const res = await fx.run(fx.markLocalRun, keyed);
    assert.equal(res.code, 76, res.stderr);
    assert.match(res.stderr, /dispatch key needs the pull queue/);
    assert.equal(fx.ranLocally(), false);
    assert.equal(await ticketCount(w), 0);
  },

  'a keyed run a push runner would take is refused 76 and the runner is never used': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, pushRunner: true, ssh: 'normal' });
    const res = await fx.run(fx.markLocalRun, keyed);
    assert.equal(res.code, 76, res.stderr);
    assert.match(res.stderr, /dispatch key needs the pull queue/);
    assert.equal(fx.ranLocally(), false);
    assert.equal(await ticketCount(w), 0);
  },

  'a keyed run with pull dispatch off is refused 76 before a supervisor starts': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, lanePull: { enabled: false } });
    const res = await fx.run(fx.markLocalRun, keyed);
    assert.equal(res.code, 76, res.stderr);
    assert.match(res.stderr, /dispatch key needs the pull queue.*pull dispatch is not configured here/);
    assert.equal(fx.ranLocally(), false);
    assert.deepEqual(fx.supervisorExits(), [], 'no supervisor was started');
  },

  'a queue that predates the dispatch key refuses a keyed run 76 and never enqueues it unkeyed': async (load, w, t) => {
    await downgradeToV9(w);
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const res = await fx.run(['true'], keyed);
    assert.equal(res.code, 76, res.stderr);
    assert.match(res.stderr, /dispatch key needs the pull queue with this version/);
    assert.match(res.stderr, /dispatch key lookup failed/, 'the lookup against the old queue is not fatal');
    assert.equal(await ticketCount(w), 0, 'nothing was enqueued without its key');
  },
};

const run = (from, to) => ['run.js', [from, to]];
const supervisor = (from, to) => ['supervisor.js', [from, to]];
const pullEnqueue = (from, to) => ['pull-enqueue.js', [from, to]];
const DETACH = 'a key the queue holds for a ticket this machine knows is that ticket: a second detach prints its id and starts nothing';
const FINISHED = 'once the keyed ticket finished a foreground run reports its exit and does not run again, and lane status finds it by key';

const CANARIES = [
  [FINISHED, ...run('      if (known) return waitCommand(held.id, { timeoutMs });', '      if (false) return waitCommand(held.id, { timeoutMs });')],
  [DETACH, ...run('      if (known && detach) {', '      if (false) {')],
  [FINISHED, ...run('const known = readAttempt(root, held.id) !== null || ', 'const known = false && ')],
  ['a key the queue holds for a ticket this machine has never seen is adopted: the run takes its id and queues no second ticket', ...run('      adoptedId = held.id;', '      adoptedId = undefined;')],
  ['a keyed run that would run locally is refused 76 "dispatch key needs the pull queue" and nothing runs or queues', ...supervisor('    if (enriched.dispatchKey) {\n      const refusal', '    if (false) {\n      const refusal')],
  ['a keyed run a push runner would take is refused 76 and the runner is never used', ...supervisor("if (enriched.dispatchKey && route.route !== 'pull')", 'if (false)')],
  ['a keyed run with pull dispatch off is refused 76 before a supervisor starts', ...run('if (!remoteEligible || !pullConfigured(globalCfg)) {', 'if (false) {')],
  ['a queue that predates the dispatch key refuses a keyed run 76 and never enqueues it unkeyed', ...pullEnqueue('const refusal = args.dispatchKey === undefined ? null : dispatchKeyRefusal(err);', 'const refusal = null;')],
];

describe('dispatch keys through a real supervisor', { skip: pgSkipOption, timeout: scaledMs(1_200_000) }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  afterAll(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());
  for (const [name, scenario] of Object.entries(SCENARIOS)) test(name, { timeout: scaledMs(150_000) }, async (t) => scenario(realSrc, await World.create(cluster), t));
  for (const [name, file, mutation] of CANARIES) {
    test(`canary: with "${mutation[0].trim().slice(0, 70)}" removed, "${name.slice(0, 60)}" fails`, { timeout: scaledMs(180_000) }, async (t) => {
      const world = await World.create(cluster);
      const failure = await assertCanaryFails(file, mutation, (load) => SCENARIOS[name](load, world, t));
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0].slice(0, 160)}`);
    });
  }
});
