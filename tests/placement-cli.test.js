import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { after as afterAll, afterEach, before, describe, test } from 'node:test';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { pullE2E } from './helpers/pull-e2e.js';
import { pgSkipOption } from './lane-db-harness.js';
import { startDispatchCluster, World, downgradeToV9 } from './lane-dispatch-fixture.js';
import { gitFixture, scaledMs } from './helpers.js';

/** PKG-439: `lane run --class / --avoid-host / --target-host` through a real supervisor against a disposable queue. */

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const ticketCount = async (w) => (await (await w.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.tickets')).rows[0].n;
const ticketRow = async (w, id) => (await (await w.su()).query('SELECT * FROM lane_dispatch.tickets WHERE id = $1', [id])).rows[0];
const target = (host) => ['--target-host', host];
const nothingRan = async (fx, w) => {
  assert.equal(fx.ranLocally(), false, 'the command was run on this machine');
  assert.equal(await ticketCount(w), 0, 'a ticket was queued');
};

const SCENARIOS = {
  'a target-host run is queued for pull whatever else could take it: a healthy push runner and free local capacity are never used, and the target runner runs it': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, pushRunner: true, ssh: 'normal' });
    const running = fx.run(fx.markLocalRun, [...target('A2')]);
    const id = await Promise.race([fx.enqueued(), running.then((res) => assert.fail(`the run ended ${res.code} without queueing: ${res.stderr}`))]);
    const row = await ticketRow(w, id);
    assert.deepEqual([row.target_host, row.avoid_hosts, row.capability_class], ['a2', [], null], 'stored canonical');
    await fx.run1({ exit: 0 });
    const res = await running;
    assert.equal(res.code, 0, res.stderr);
    assert.doesNotMatch(res.stderr, /running on skybox/);
    assert.equal(fx.ranLocally(), false);
  },

  'a placement nothing in the fleet satisfies is refused 76 with nothing run locally and nothing queued: a class, every host avoided, an unknown host': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, pushRunner: true, ssh: 'normal' });
    for (const [args, pattern] of [
      [['--class', 'win32'], /no queue principal satisfies the requested placement/],
      [['--avoid-host', 'A1', '--avoid-host', 'a2', '--avoid-host', 'x'], /no queue principal satisfies/],
      [['--target-host', 'nobody'], /unknown host nobody/],
      [['--class', 'linux+nothing-has-this'], /no queue principal satisfies/],
    ]) {
      const res = await fx.run(fx.markLocalRun, args);
      assert.equal(res.code, 76, `${args.join(' ')}: ${res.stderr}`);
      assert.match(res.stderr, pattern);
      await nothingRan(fx, w);
    }
  },

  'a malformed placement is a usage error, exit 64, before any supervisor starts': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load });
    for (const args of [['--class', 'plan9'], ['--class', ''], ['--class', 'linux+Bad Tag'], ['--target-host', ''], ['--target-host', 'a1', '--avoid-host', 'A1'], ['--avoid-host', 'not a host']]) {
      const res = await fx.run(fx.markLocalRun, args);
      assert.equal(res.code, 64, `${args.join(' ')}: ${res.stderr}`);
    }
    assert.deepEqual(fx.supervisorExits(), []);
    await nothingRan(fx, w);
  },

  'with pull dispatch off a placed run is refused 76 before a supervisor starts, never run locally': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, lanePull: { enabled: false } });
    const res = await fx.run(fx.markLocalRun, target('a2'));
    assert.equal(res.code, 76, res.stderr);
    assert.match(res.stderr, /placement constraints need the pull queue.*pull dispatch is not configured here/);
    assert.deepEqual(fx.supervisorExits(), []);
    await nothingRan(fx, w);
  },

  'a queue that cannot be reached before anything is sent is an infra failure, exit 75, with nothing queued or run': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, queueFail: 'ld_queue_ref' });
    const res = await fx.run(fx.markLocalRun, target('a2'));
    assert.equal(res.code, 75, res.stderr);
    assert.match(res.stderr, /nothing was enqueued/);
    await nothingRan(fx, w);
  },

  'a queue that predates migration 013 refuses a placed run 76 and never enqueues it without its placement': async (load, w, t) => {
    await downgradeToV9(w);
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const res = await fx.run(fx.markLocalRun, target('a2'));
    assert.equal(res.code, 76, res.stderr);
    assert.match(res.stderr, /queue too old for placement constraints/);
    await nothingRan(fx, w);
  },

  'a detached placed run answers with the supervisor outcome, not an id: an unknown host exits 76 and prints no id': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const res = await fx.lane(['run', '--repo', 'r', '--lane', 'default', '--detach', ...target('nobody'), '--', 'true']);
    assert.equal(res.code, 76, res.stderr);
    assert.equal(res.stdout, '', 'no id was printed for a refused submission');
    assert.equal(await ticketCount(w), 0);
  },

  'hostAliases map a spelling to the runner id before the queue sees it, and an alias that is itself a runner id is refused 64': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true, globalExtra: { hostAliases: { 'a2-fleet': 'a2' } } });
    const id = await fx.detach(['true'], target('A2-Fleet'));
    await fx.enqueued();
    assert.equal((await ticketRow(w, id)).target_host, 'a2');
    const shadowing = await pullE2E(t, w, { load, localPaused: true, globalExtra: { hostAliases: { a1: 'a2' } } });
    const res = await shadowing.run(['true'], target('a1'));
    assert.equal(res.code, 64, res.stderr);
    assert.match(res.stderr, /alias must never map one real runner to another/);
    assert.equal(await ticketCount(w), 1, 'only the first ticket exists');
  },

  'a placed run bypasses the result cache: a cached pass of the same content does not answer a canary on a named host': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    gitFixture(['add', '-A'], fx.repoDir);
    gitFixture(['commit', '-q', '-m', 'init'], fx.repoDir);
    const first = fx.run(['true']);
    const firstId = await fx.enqueued();
    await fx.run1({ exit: 0 });
    assert.equal((await first).code, 0);
    await sleep(1500);
    const cached = await Promise.race([fx.run(['true']), sleep(40_000).then(() => assert.fail('the identical run was not a cache hit'))]);
    assert.match(cached.stderr, /cached pass from/, 'the same run is a cache hit, so the bypass below means something');
    const placed = fx.run(['true'], target('a2'));
    const id = await Promise.race([fx.enqueued([firstId]), placed.then((res) => assert.fail(`the placed run ended ${res.code} without queueing: ${res.stderr}`))]);
    assert.equal((await ticketRow(w, id)).target_host, 'a2');
    await fx.run1({ exit: 0 });
    const res = await placed;
    assert.equal(res.code, 0, res.stderr);
    assert.doesNotMatch(res.stderr, /cached pass/);
  },

  'a dispatch key adopts only a ticket placed as asked: another placement is refused 76 and queues nothing, the same placement is the same ticket': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const key = ['--dispatch-key', 'train:r:9:1'];
    const detached = (extra) => fx.lane(['run', '--repo', 'r', '--lane', 'default', '--detach', ...key, ...extra, '--', 'true']);
    const plain = await detached([]);
    assert.equal(plain.code, 0, plain.stderr);
    const id = plain.stdout.trim();
    t.after(() => fx.lane(['cancel', id]));
    const other = await detached(target('a2'));
    assert.equal(other.code, 76, other.stderr);
    assert.match(other.stderr, /different placement/);
    assert.equal(other.stdout, '');
    assert.equal(await ticketCount(w), 1);
    await sleep(1500);
    assert.deepEqual(fx.supervisorExits(), [], 'refused by the client before a supervisor was started');
    const same = await detached([]);
    assert.equal(same.stdout.trim(), id);
    const placedKey = ['--dispatch-key', 'train:r:9:2'];
    const placed = await fx.lane(['run', '--repo', 'r', '--lane', 'default', '--detach', ...placedKey, ...target('a2'), '--avoid-host', 'x', '--', 'true']);
    assert.equal(placed.code, 0, placed.stderr);
    const again = await fx.lane(['run', '--repo', 'r', '--lane', 'default', '--detach', ...placedKey, ...target('a2'), '--avoid-host', 'X', '--', 'true']);
    assert.equal(again.stdout.trim(), placed.stdout.trim(), 'the same canonical placement adopts');
    const elsewhere = await fx.lane(['run', '--repo', 'r', '--lane', 'default', '--detach', ...placedKey, ...target('a2'), '--avoid-host', 'a1', '--', 'true']);
    assert.equal(elsewhere.code, 76, elsewhere.stderr);
    assert.equal(await ticketCount(w), 2);
    t.after(() => fx.lane(['cancel', placed.stdout.trim()]));
  },

  'an id reused with another placement is refused 64 and the same placement is the same ticket': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    const id = crypto.randomUUID();
    const detached = (extra) => fx.lane(['run', '--repo', 'r', '--lane', 'default', '--detach', '--id', id, ...extra, '--', 'true']);
    const first = await detached(target('a2'));
    assert.equal(first.code, 0, first.stderr);
    t.after(() => fx.lane(['cancel', id]));
    assert.equal(first.stdout.trim(), id);
    await fx.enqueued();
    const again = await detached(target('a2'));
    assert.equal(again.stdout.trim(), id, `${again.code}: ${again.stderr} ${JSON.stringify((await ticketRow(w, id))?.target_host)} ${fx.supervisorExits()}`);
    const other = await detached(['--avoid-host', 'a1']);
    assert.equal(other.code, 64, other.stderr);
    assert.match(other.stderr, /different placement/);
    assert.equal(await ticketCount(w), 1);
  },

  'a placed ticket nobody claims in time is withdrawn and refused 76: the committed withdrawal never runs it locally, though this machine is free': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, lanePull: { queuedTimeoutMs: 1500 } });
    const res = await fx.run(fx.markLocalRun, target('x'));
    assert.equal(res.code, 76, res.stderr);
    assert.match(res.stderr, /queued-timeout/);
    assert.equal(fx.ranLocally(), false, 'the command ran on this machine after the withdrawal');
    assert.equal((await (await w.su()).query('SELECT state FROM lane_dispatch.tickets')).rows[0].state, 'withdrawn');
  },

  'a detached placed run that the queue has not answered in time prints its id and says wait, exits 75, and the ticket still ends with its real outcome': async (load, w, t) => {
    const fx = await pullE2E(t, w, { load, localPaused: true });
    fx.stub.delayMatching = (method, pathname) => (method === 'PUT' && pathname.includes('/streams/') ? 4000 : 0);
    const res = await fx.lane(['run', '--repo', 'r', '--lane', 'default', '--detach', '--timeout', '1s', ...target('a2'), '--', 'true']);
    assert.equal(res.code, 75, res.stderr);
    const id = res.stdout.trim();
    assert.match(id, /^[0-9a-f-]{36}$/);
    assert.match(res.stderr, new RegExp(`lane wait ${id}`));
    assert.doesNotMatch(res.stderr, /retry/);
    await fx.enqueued();
    await fx.run1({ exit: 0 });
    assert.equal((await fx.wait(id)).code, 0, 'the late enqueue was acknowledged and the run finished');
    const json = await fx.lane(['run', '--repo', 'r', '--lane', 'default', '--detach', '--json', '--timeout', '1s', ...target('a2'), '--', 'true']);
    assert.equal(json.code, 75);
    assert.deepEqual(Object.keys(JSON.parse(json.stdout)).sort(), ['id', 'pending']);
    await sleep(100);
  },
};

const run = (from, to) => ['run.js', [from, to]];
const supervisor = (from, to) => ['supervisor.js', [from, to]];
const MIXED = 'a target-host run is queued for pull whatever else could take it: a healthy push runner and free local capacity are never used, and the target runner runs it';
const UNREACHABLE = 'a queue that cannot be reached before anything is sent is an infra failure, exit 75, with nothing queued or run';
const OFF = 'with pull dispatch off a placed run is refused 76 before a supervisor starts, never run locally';
const DETACHED = 'a detached placed run answers with the supervisor outcome, not an id: an unknown host exits 76 and prints no id';
const CACHE = 'a placed run bypasses the result cache: a cached pass of the same content does not answer a canary on a named host';
const ADOPT = 'a dispatch key adopts only a ticket placed as asked: another placement is refused 76 and queues nothing, the same placement is the same ticket';
const WITHDRAWN = 'a placed ticket nobody claims in time is withdrawn and refused 76: the committed withdrawal never runs it locally, though this machine is free';
const IDREUSE = 'an id reused with another placement is refused 64 and the same placement is the same ticket';

const CANARIES = [
  [WITHDRAWN, ...supervisor('    if (isPullOnly(enriched)) return refusePullOnly(reason);', '    if (false) return refusePullOnly(reason);')],
  [MIXED, ...supervisor('const pullOnlyPlacement = isConstrained(enriched.placementRequest);', 'const pullOnlyPlacement = false;')],
  [UNREACHABLE, ...supervisor("if (pullOnlyPlacement && route.route !== 'pull') {", 'if (false) {')],
  [OFF, ...run('if (placement !== null && (!remoteEligible || !pullConfigured(globalCfg))) {', 'if (false) {')],
  [DETACHED, ...run('if (dispatchKey !== undefined || placement !== null) {', 'if (dispatchKey !== undefined) {')],
  [CACHE, ...run('dispatchKey === undefined && placement === null) {', 'dispatchKey === undefined) {')],
  [ADOPT, 'run.js', [
    ['      const refusal = placement === null ? null : placementRefusal(err);', '      const refusal = null;'],
    ['    if (held !== null && placement !== null && !samePlacement(placementKeyOfView(held), placement)) {', '    if (false) {'],
  ]],
  [IDREUSE, ...run('if (placement !== null && ticketId !== undefined && heldLocally(root, ticketId) && !samePlacement(localPlacementOf(root, ticketId), placement)) {', 'if (false) {')],
];

describe('placement constraints through a real supervisor', { skip: pgSkipOption, timeout: scaledMs(1_800_000) }, () => {
  let cluster;
  before(async () => { cluster = await startDispatchCluster(); });
  afterAll(async () => { await cluster?.closeClients(); cluster?.stop(); });
  afterEach(() => cluster.closeClients());
  for (const [name, scenario] of Object.entries(SCENARIOS)) test(name, { timeout: scaledMs(150_000) }, async (t) => scenario(realSrc, await World.create(cluster), t));
  for (const [name, file, mutation] of CANARIES) {
    test(`canary: with "${(Array.isArray(mutation[0]) ? mutation[0][0] : mutation[0]).trim().slice(0, 70)}" removed, "${name.slice(0, 60)}" fails`, { timeout: scaledMs(180_000) }, async (t) => {
      const world = await World.create(cluster);
      const failure = await assertCanaryFails(file, mutation, (load) => SCENARIOS[name](load, world, t));
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0].slice(0, 160)}`);
    });
  }
});
