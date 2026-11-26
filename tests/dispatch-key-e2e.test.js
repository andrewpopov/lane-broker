import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { after as afterAll, afterEach, before, describe, test } from 'node:test';
import { paths, readJsonSafe } from '../src/state.js';
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
    assert.doesNotMatch(rerun.stderr, /finished with exit/, 'this machine answered from its own record, without attaching to the queue');
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

  'a key the queue holds for a ticket this machine has never seen is observed, not run again: a detach prints its id, and no attempt or supervisor starts here': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const held = crypto.randomUUID();
    await enqueue(await w.as(logins.s1), held, { dispatchKey: KEY });
    const started = await fx.lane(['run', '--repo', 'r', '--lane', 'default', '--detach', ...keyed, '--', 'true']);
    assert.equal(started.code, 0, started.stderr);
    assert.equal(started.stdout.trim(), held);
    await sleep(1500);
    assert.equal(fx.attempt(held), null, 'no local attempt was made for the held ticket');
    assert.deepEqual(fx.supervisorExits(), []);
    assert.equal(fs.existsSync(paths(fx.state).attempts) ? fs.readdirSync(paths(fx.state).attempts).length : 0, 0, 'no supervisor was started for anything');
    assert.equal(await ticketCount(w), 1);
  },

  'a finished ticket adopted from an empty state directory with a different checkout reports the original result, queues nothing and starts no supervisor': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const id = await fx.detach(['true'], keyed);
    await fx.enqueued();
    await fx.run1({ exit: 3 });
    assert.equal((await fx.wait(id)).code, 3);
    await sleep(1500);
    assert.deepEqual(fx.supervisorExits(), [3]);
    // this machine forgets the ticket, and the checkout is no longer the one that was submitted
    for (const file of [path.join(paths(fx.state).results, `${id}.json`), path.join(paths(fx.state).attempts, `${id}.json`), path.join(paths(fx.state).pull, id)]) fs.rmSync(file, { recursive: true, force: true });
    fs.writeFileSync(path.join(fx.repoDir, 'changed.txt'), 'a different checkout\n');
    const rerun = await fx.run(fx.markLocalRun, keyed);
    assert.equal(rerun.code, 3, rerun.stderr);
    assert.match(rerun.stderr, new RegExp(`ticket ${id} finished with exit 3`));
    assert.equal(fx.ranLocally(), false);
    assert.equal(await ticketCount(w), 1);
    await sleep(1500);
    assert.deepEqual(fx.supervisorExits(), [3], 'no second supervisor was started');
    assert.equal(fx.attempt(id), null, 'no attempt was made for it');
    assert.equal(readJsonSafe(path.join(paths(fx.state).results, `${id}.json`)).exit, 3, 'the result is kept here, so a later run answers from it');
    assert.equal((await fx.run(fx.markLocalRun, keyed)).code, 3);
  },

  'two detached callers racing on one key print the same ticket id, and the command is queued once': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const detach = () => fx.lane(['run', '--repo', 'r', '--lane', 'default', '--detach', ...keyed, '--', 'true']);
    const [a, b] = await Promise.all([detach(), detach()]);
    assert.deepEqual([a.code, b.code], [0, 0], `${a.stderr}\n${b.stderr}`);
    const [{ id: queued }] = (await (await w.su()).query("SELECT id FROM lane_dispatch.tickets WHERE dispatch_key = $1", [KEY])).rows;
    assert.deepEqual([a.stdout.trim(), b.stdout.trim()], [queued, queued], 'both callers were told the ticket the queue holds');
    assert.equal(await ticketCount(w), 1);
    t.after(() => fx.lane(['cancel', queued]));
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
const UNSEEN = 'a key the queue holds for a ticket this machine has never seen is observed, not run again: a detach prints its id, and no attempt or supervisor starts here';
const ADOPTED = 'a finished ticket adopted from an empty state directory with a different checkout reports the original result, queues nothing and starts no supervisor';
const DETACH = 'a key the queue holds for a ticket this machine knows is that ticket: a second detach prints its id and starts nothing';
const FINISHED = 'once the keyed ticket finished a foreground run reports its exit and does not run again, and lane status finds it by key';

const CANARIES = [
  [FINISHED, ...run('if (readAttempt(root, held.id) !== null || fs.existsSync(path.join(paths(root).results, `${held.id}.json`))) return waitCommand(held.id, { timeoutMs });', 'if (false) return waitCommand(held.id, { timeoutMs });')],
  [DETACH, ...run('      if (detach) {\n        process.stdout.write(`${held.id}\\n`);', '      if (false) {\n        process.stdout.write(`${held.id}\\n`);')],
  [UNSEEN, ...run('    if (held !== null) {\n      process.stderr.write(`lane run: dispatch key', '    if (false) {\n      process.stderr.write(`lane run: dispatch key')],
  [ADOPTED, ...run('    if (held !== null) {\n      process.stderr.write(`lane run: dispatch key', '    if (false) {\n      process.stderr.write(`lane run: dispatch key')],
  ['two detached callers racing on one key print the same ticket id, and the command is queued once', ...run('      printedId = answer.id;', '      printedId = id;')],
  ['two detached callers racing on one key print the same ticket id, and the command is queued once', ...run('    if (enqueue?.heldTicketId) return { id: enqueue.heldTicketId };\n', '')],
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
