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

  'an unconstrained ticket keeps no placement and reports no reason while a runner can take it': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load, id: tid(2) });
    await fx.enqueue();
    const row = await fx.row();
    assert.deepEqual([row.capability_class, row.avoid_hosts, row.target_host], [null, [], null]);
    await fx.step();
    assert.equal(fx.pull().unplaceable ?? null, null);
  },
};

const CANARIES = [
  ['a placed envelope is enqueued with its placement, and the poller copies why no runner can take it into the attempt until one can', 'pull-ticket.js',
    ["if (unplaceable !== (attempt.pull.unplaceable ?? null)) await persistPull(run, { unplaceable });", ''], null, /no principal runs win32|unplaceable|expected/i],
];

definePullSuite('placed pull tickets: stored placement and the unplaceable reason', SCENARIOS, CANARIES.map(([name, file, mutation, expected, pattern]) => [name, file, mutation, expected, pattern]));
