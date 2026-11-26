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
import { laneRun, scaledMs } from './helpers.js';

/** PKG-438: `lane run --dispatch-key` through a real supervisor against a disposable queue. */

const KEY = 'train:r:3:1';
const keyed = ['--dispatch-key', KEY];
const ticketCount = async (w) => (await (await w.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.tickets')).rows[0].n;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(fn, timeoutMs = 60_000) {
  for (const deadline = Date.now() + scaledMs(timeoutMs); !(await fn()); await sleep(100)) if (Date.now() > deadline) assert.fail('timed out waiting');
}

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

  'a key the queue holds for a ticket this machine never submitted ends 73 with its id, state and host printed, and writes, polls and starts nothing': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const held = crypto.randomUUID();
    await enqueue(await w.as(logins.s1), held, { dispatchKey: KEY });
    const nothingHere = () => {
      for (const dir of [paths(fx.state).attempts, paths(fx.state).results, paths(fx.state).pull]) assert.deepEqual(fs.existsSync(dir) ? fs.readdirSync(dir).filter((f) => f !== 'readiness.json') : [], [], `${dir} was written`);
    };
    const listing = (dir) => (fs.existsSync(dir) ? fs.readdirSync(dir, { recursive: true }).sort().join('|') : 'absent');
    const before = listing(fx.state);
    for (const extra of [[], ['--detach']]) {
      const res = await fx.lane(['run', '--repo', 'r', '--lane', 'default', ...extra, ...keyed, '--', 'true']);
      assert.equal(res.code, 73, res.stderr);
      assert.equal(res.stdout, `${held} queued -\n`);
      assert.match(res.stderr, /did not submit/);
    }
    const json = await fx.lane(['run', '--repo', 'r', '--lane', 'default', '--json', ...keyed, '--', 'true']);
    assert.deepEqual([json.code, JSON.parse(json.stdout)], [73, { dispatchKey: KEY, id: held, state: 'queued', host: null }]);
    nothingHere();
    assert.equal(listing(fx.state), before, 'the state directory is exactly as it was: nothing was created or stamped');
    assert.deepEqual(fx.supervisorExits(), []);
    assert.equal(await ticketCount(w), 1);
  },

  'a finished ticket this machine never submitted, with a different checkout here, ends 73 naming its state and host: the original result is not re-validated, cached or re-run': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const id = await fx.detach(['true'], keyed);
    await fx.enqueued();
    await fx.run1({ exit: 3 });
    assert.equal((await fx.wait(id)).code, 3);
    await sleep(1500);
    assert.deepEqual(fx.supervisorExits(), [3]);
    // this machine forgets the ticket, and the checkout is no longer the one that was submitted
    for (const file of [path.join(paths(fx.state).results, `${id}.json`), path.join(paths(fx.state).attempts, `${id}.json`), path.join(paths(fx.state).pull, id), path.join(paths(fx.state).pullLedger, `${id}.json`)]) fs.rmSync(file, { recursive: true, force: true });
    fs.writeFileSync(path.join(fx.repoDir, 'changed.txt'), 'a different checkout\n');
    const rerun = await fx.run(fx.markLocalRun, keyed);
    assert.equal(rerun.code, 73, rerun.stderr);
    assert.equal(rerun.stdout, `${id} finished a2\n`);
    assert.equal(fx.ranLocally(), false);
    assert.equal(await ticketCount(w), 1);
    await sleep(1500);
    assert.deepEqual(fx.supervisorExits(), [3], 'no second supervisor was started');
    assert.equal(fx.attempt(id), null);
    assert.equal(fs.existsSync(path.join(paths(fx.state).results, `${id}.json`)), false, 'nothing was written for the ticket');
    const status = await fx.lane(['status', '--dispatch-key', KEY, '--json']);
    assert.deepEqual(JSON.parse(status.stdout), { dispatchKey: KEY, found: true, id, state: 'finished', host: 'a2' }, 'the caller follows it through status');
  },

  'a caller, detached or foreground, that loses the key to a ticket another machine submitted exits 73 with that holder printed (id, state, host), and prints no id of its own': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    fx.stub.delayMatching = (method, pathname) => (method === 'PUT' && pathname.includes('/streams/') ? 3000 : 0);
    for (const [n, extra] of [[1, ['--detach']], [2, []], [3, ['--json']]]) {
      const key = `${KEY}:${n}`;
      const held = crypto.randomUUID();
      const attemptFiles = () => (fs.existsSync(paths(fx.state).attempts) ? fs.readdirSync(paths(fx.state).attempts) : []);
      const attemptsBefore = new Set(attemptFiles());
      const running = fx.lane(['run', '--repo', 'r', '--lane', 'default', ...extra, '--dispatch-key', key, '--', 'true']);
      // the lookup missed and the snapshot is uploading: another machine's submit takes the key now, and its runner already has it
      await until(async () => attemptFiles().some((file) => !attemptsBefore.has(file)));
      await enqueue(await w.as(logins.s1), held, { dispatchKey: key });
      await (await w.su()).query("UPDATE lane_dispatch.tickets SET state = 'running', host = 'pit', claim_token = gen_random_uuid(), lease_until = now() + interval '1 hour' WHERE id = $1", [held]);
      const res = await running;
      assert.equal(res.code, 73, `${extra}: ${res.stderr}`);
      if (extra[0] === '--json') assert.deepEqual(JSON.parse(res.stdout), { dispatchKey: key, id: held, state: 'running', host: 'pit' });
      else assert.equal(res.stdout, `${held} running pit\n`, `${extra}`);
    }
    assert.equal(await ticketCount(w), 3);
  },

  'a keyed refusal is published from the attempt as it is, never after the move to local: a supervisor parked at the refusal still holds a non-local attempt': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load });
    const dir = path.dirname(fx.state);
    const [ready, go] = [path.join(dir, 'keyed-ready'), path.join(dir, 'keyed-go')];
    const env = { ...fx.env, LANE_BROKER_TEST_HOLD_AT: 'keyed-refusal', LANE_BROKER_TEST_HOLD_READY: ready, LANE_BROKER_TEST_HOLD_GO: go };
    t.after(() => fs.writeFileSync(go, 'x'));
    const running = laneRun(['run', '--repo', 'r', '--lane', 'default', ...keyed, '--', ...fx.markLocalRun], { env, cwd: fx.repoDir, bin: load.laneBin });
    await until(async () => fs.existsSync(ready));
    const [file] = fs.readdirSync(paths(fx.state).attempts);
    const attempt = fx.attempt(file.replace('.json', ''));
    assert.notEqual(attempt.executor, 'local', 'the attempt was not moved to local before the refusal');
    fs.writeFileSync(go, 'x');
    const res = await running;
    assert.equal(res.code, 76, res.stderr);
    assert.equal(fx.ranLocally(), false);
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
const UNSEEN = 'a key the queue holds for a ticket this machine never submitted ends 73 with its id, state and host printed, and writes, polls and starts nothing';
const ADOPTED = 'a finished ticket this machine never submitted, with a different checkout here, ends 73 naming its state and host: the original result is not re-validated, cached or re-run';
const REMOTE_LOSER = 'a caller, detached or foreground, that loses the key to a ticket another machine submitted exits 73 with that holder printed (id, state, host), and prints no id of its own';
const KEYED_REFUSAL = 'a keyed refusal is published from the attempt as it is, never after the move to local: a supervisor parked at the refusal still holds a non-local attempt';
const DETACH = 'a key the queue holds for a ticket this machine knows is that ticket: a second detach prints its id and starts nothing';
const FINISHED = 'once the keyed ticket finished a foreground run reports its exit and does not run again, and lane status finds it by key';

const CANARIES = [
  [FINISHED, ...run('      return waitCommand(held.id, {', '      return { exitCode: 99 }; waitCommand(held.id, {')],
  [DETACH, ...run('      if (detach) {\n        process.stdout.write(`${held.id}\\n`);', '      if (false) {\n        process.stdout.write(`${held.id}\\n`);')],
  [UNSEEN, ...run('      if (!heldLocally(probeRoot, held.id)) {', '      if (false) {')],
  [ADOPTED, ...run('      if (!heldLocally(probeRoot, held.id)) {', '      if (false) {')],
  [REMOTE_LOSER, ...run('      return heldLocally(root, ack.ticketId) ? { id: ack.ticketId } : {', '      return true ? { id: ack.ticketId } : {')],
  [REMOTE_LOSER, ...run('      if (collided?.ticketId && collided.ticketId !== id) {', '      if (false) {')],
  [UNSEEN, ...run('    const probeRoot = stateHome();', '    const probeRoot = ensureStateDirs().root;')],
  [KEYED_REFUSAL, ...supervisor('    await testHoldAt(\'keyed-refusal\');\n    await publishAndExit(gen, () => localRefusalResult(enriched, refusal));', '    await fallbackToLocal(root, enriched.id, reason);\n    await testHoldAt(\'keyed-refusal\');\n    await publishAndExit(gen + 1, () => localRefusalResult(enriched, refusal));')],
  ['two detached callers racing on one key print the same ticket id, and the command is queued once', ...run('      printedId = answer.id;', '      printedId = id;')],
  ['two detached callers racing on one key print the same ticket id, and the command is queued once', ...run('      return heldLocally(root, ack.ticketId) ? { id: ack.ticketId } : {', '      return heldLocally(root, ack.ticketId) ? { id } : {')],
  ['a keyed run that would run locally is refused 76 "dispatch key needs the pull queue" and nothing runs or queues', ...supervisor('    if (isPullOnly(enriched)) return refusePullOnly(reason);', '    if (false) return refusePullOnly(reason);')],
  ['a keyed run a push runner would take is refused 76 and the runner is never used', ...supervisor("if (enriched.dispatchKey && route.route !== 'pull')", 'if (false)')],
  ['a keyed run with pull dispatch off is refused 76 before a supervisor starts', ...run('if (!remoteEligible || !pullConfigured(globalCfg)) {', 'if (false) {')],
  ['a queue that predates the dispatch key refuses a keyed run 76 and never enqueues it unkeyed', ...pullEnqueue('const refusal = (envelope.placementRequest === undefined ? null : placementRefusal(err)) ?? (args.dispatchKey === undefined ? null : dispatchKeyRefusal(err));', 'const refusal = null;')],
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
