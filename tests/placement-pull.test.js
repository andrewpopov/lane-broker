import assert from 'node:assert/strict';
import { freshEnv } from './helpers.js';
import { definePullSuite, pullFixture } from './helpers/pull-fixture.js';
import { tid } from './lane-dispatch-fixture.js';

/** PKG-439: a placed envelope through the pull poller against a disposable queue: what is stored, and how an unplaceable ticket is reported. */

const PLACEMENT = { capabilityClass: 'win32', classPlatform: 'win32', classTags: [], avoidHosts: ['a2'], targetHost: null };

async function withBrokerEnv(state, fn) {
  const { home } = freshEnv();
  const previous = { home: process.env.LANE_BROKER_HOME, state: process.env.LANE_BROKER_STATE };
  Object.assign(process.env, { LANE_BROKER_HOME: home, LANE_BROKER_STATE: state });
  try {
    return await fn();
  } finally {
    for (const [name, value] of [['LANE_BROKER_HOME', previous.home], ['LANE_BROKER_STATE', previous.state]]) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

const SCENARIOS = {
  'a placed envelope is enqueued with its placement, and the poller copies why no runner can take it into the attempt until one can': async (load, w, t) => {
    const { collectStatus, renderStatusText } = await load('status.js');
    const fx = await pullFixture(t, w, { load, id: tid(1) });
    fx.envelopeFields.placementRequest = PLACEMENT;
    await fx.enqueue();
    const row = await fx.row();
    assert.deepEqual([row.capability_class, row.class_platform, row.avoid_hosts, row.target_host], ['win32', 'win32', ['a2'], null]);
    await fx.step();
    assert.equal(fx.pull().unplaceable, 'class', 'no principal runs win32');
    const status = await withBrokerEnv(fx.state, () => collectStatus({}));
    assert.equal(status.remote.find((r) => r.id === fx.id).pullUnplaceable, 'class');
    assert.match(renderStatusText(status), new RegExp(`${fx.id}.*pull.*stage=QUEUED queued \\(unplaceable: class\\)`));
    await (await w.su()).query("UPDATE lane_dispatch.principals SET platform = 'win32' WHERE host = 'a1'");
    await fx.step();
    assert.equal(fx.pull().unplaceable, null, 'a principal of the class exists now');
    const after = await withBrokerEnv(fx.state, () => collectStatus({}));
    assert.doesNotMatch(renderStatusText(after), /unplaceable/);
    assert.equal((await fx.row()).state, 'queued', 'the ticket stayed queued throughout');
  },

  'a committed enqueue whose reply was dropped is recovered as the one row it made, with its placement': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load, id: tid(5) });
    fx.envelopeFields.placementRequest = { capabilityClass: 'linux', classPlatform: 'linux', classTags: [], avoidHosts: ['x'], targetHost: 'a2' };
    const sends = [];
    const dropping = { ...fx.queue, enqueueFenced: async (args) => { sends.push('dropping'); await fx.queue.enqueueFenced(args); throw Object.assign(new Error('connection reset'), { name: 'QueueError' }); } };
    const counting = { ...fx.queue, enqueueFenced: async (args) => { sends.push('real'); return fx.queue.enqueueFenced(args); } };
    await fx.prepare();
    await fx.step({ queue: dropping }).catch(() => {});
    const rows = async () => (await (await w.su()).query('SELECT id, capability_class, avoid_hosts, target_host FROM lane_dispatch.tickets')).rows;
    const expected = [{ id: tid(5), capability_class: 'linux', avoid_hosts: ['x'], target_host: 'a2' }];
    assert.deepEqual(sends, ['dropping'], 'the first send was the one whose reply was lost');
    assert.deepEqual(await rows(), expected, 'it had committed');
    for (let i = 0; i < 3; i++) await fx.step({ queue: counting });
    assert.deepEqual(sends, ['dropping', 'real'], 'the recovery replayed the send once');
    assert.deepEqual(await rows(), expected, 'and found the row it had made');
    assert.ok(fx.pull().enqueue.seenRowAt);
  },

  'recovery after a proven never-enqueued placed ticket publishes 75, as the live path does': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load, id: tid(7) });
    fx.envelopeFields.placementRequest = { capabilityClass: null, classPlatform: null, classTags: [], avoidHosts: [], targetHost: 'a2' };
    fx.envelopeFields.publication = { refusalExit: 76 };
    await fx.prepare();
    assert.deepEqual(await fx.proveNeverEnqueued({ neverEnqueuedExit: 75 }), { proof: true });
    await fx.killSupervisor();
    await fx.step();
    assert.equal(fx.published().exit, 75);
    assert.equal((await (await w.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.tickets')).rows[0].n, 0);
  },

  'an unconstrained ticket keeps no placement and reports no reason while a runner can take it': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load, id: tid(2) });
    await fx.enqueue();
    const row = await fx.row();
    assert.deepEqual([row.capability_class, row.avoid_hosts, row.target_host], [null, [], null]);
    await fx.step();
    assert.equal(fx.pull().unplaceable ?? null, null);
  },
};

const NEVER = 'recovery after a proven never-enqueued placed ticket publishes 75, as the live path does';
const REPLY = 'a committed enqueue whose reply was dropped is recovered as the one row it made, with its placement';
const FIRST = 'a placed envelope is enqueued with its placement, and the poller copies why no runner can take it into the attempt until one can';
const CANARIES = [
  [FIRST, 'status.js', ["${r.pullUnplaceable ? ` queued (unplaceable: ${r.pullUnplaceable})` : ''}", ''], null, null],
  [REPLY, 'pull-enqueue.js', ['view = await queue.enqueueFenced({ expectRef: ref, ...args });', 'view = await queue.enqueueFenced({ expectRef: ref, ...args, ...(intent.attempts.length > 0 ? { id: \'00000000-0000-4000-8000-0000000000ff\' } : {}) });'], null, null],
  [NEVER, 'pull-ticket.js', ['exit: pull.neverEnqueuedExit ?? ', 'exit: '], null, null],
  ['a placed envelope is enqueued with its placement, and the poller copies why no runner can take it into the attempt until one can', 'pull-ticket.js',
    ["if (unplaceable !== (attempt.pull.unplaceable ?? null)) await persistPull(run, { unplaceable });", ''], null, /no principal runs win32|unplaceable|expected/i],
];

definePullSuite('placed pull tickets: stored placement and the unplaceable reason', SCENARIOS, CANARIES.map(([name, file, mutation, expected, pattern]) => [name, file, mutation, expected, pattern]));
