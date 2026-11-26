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
    await fx.prepare();
    const dropping = { ...fx.queue, enqueueFenced: async (args) => { await fx.queue.enqueueFenced(args); throw Object.assign(new Error('connection reset'), { name: 'QueueError' }); } };
    await fx.step();
    await fx.step({ queue: dropping }).catch(() => {});
    const rows = async () => (await (await w.su()).query('SELECT id, capability_class, avoid_hosts, target_host FROM lane_dispatch.tickets')).rows;
    assert.deepEqual(await rows(), [{ id: tid(5), capability_class: 'linux', avoid_hosts: ['x'], target_host: 'a2' }], 'the first send committed');
    for (let i = 0; i < 3; i++) await fx.step();
    assert.deepEqual(await rows(), [{ id: tid(5), capability_class: 'linux', avoid_hosts: ['x'], target_host: 'a2' }], 'the recovery found it and sent nothing else');
    assert.ok(fx.pull().enqueue.seenRowAt, 'the recovery saw the row');
  },

  'a retained envelope answers its own placement only: preparing for another is refused, the same one reuses it': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load, id: tid(6) });
    fx.envelopeFields.placementRequest = { capabilityClass: null, classPlatform: null, classTags: [], avoidHosts: [], targetHost: 'a2' };
    await fx.prepare();
    await fx.prepare();
    fx.envelopeFields.placementRequest = { ...fx.envelopeFields.placementRequest, targetHost: 'x' };
    await assert.rejects(fx.prepare(), /different placement/);
    fx.envelopeFields.placementRequest = undefined;
    await fx.prepare();
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

const REPLY = 'a committed enqueue whose reply was dropped is recovered as the one row it made, with its placement';
const RETAINED = 'a retained envelope answers its own placement only: preparing for another is refused, the same one reuses it';
const FIRST = 'a placed envelope is enqueued with its placement, and the poller copies why no runner can take it into the attempt until one can';
const CANARIES = [
  [RETAINED, 'pull-ticket.js', ['!adoptsPlacement(existing.placementRequest, envelopeFields.placementRequest)', 'false'], null, null],
  [FIRST, 'status.js', ["${r.pullUnplaceable ? ` queued (unplaceable: ${r.pullUnplaceable})` : ''}", ''], null, null],
  [REPLY, 'pull-enqueue.js', ['...(envelope.placementRequest === undefined ? {} : placementArgs(envelope.placementRequest)),', ''], null, null],
  ['a placed envelope is enqueued with its placement, and the poller copies why no runner can take it into the attempt until one can', 'pull-ticket.js',
    ["if (unplaceable !== (attempt.pull.unplaceable ?? null)) await persistPull(run, { unplaceable });", ''], null, /no principal runs win32|unplaceable|expected/i],
];

definePullSuite('placed pull tickets: stored placement and the unplaceable reason', SCENARIOS, CANARIES.map(([name, file, mutation, expected, pattern]) => [name, file, mutation, expected, pattern]));
