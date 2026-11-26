import assert from 'node:assert/strict';
import { refusalExitOf } from '../src/pull-publication.js';
import { definePullSuite, pullFixture } from './helpers/pull-fixture.js';

/** PKG-438: what a recovery publishes for a keyed ticket whose supervisor died: a refusal the queue already gave, and a withdrawal it already committed. */

const KEY = 'train:repoA:7:2';
const LOST_RESPONSE = new Error('response lost');
const missingFunction = Object.assign(new Error('function lane_dispatch.ld_enqueue_fenced does not exist'), { code: '42883' });
const rowCount = async (fx) => (await (await fx.su()).query('SELECT count(*)::int AS n FROM lane_dispatch.tickets')).rows[0].n;
const counting = (fx, replace) => {
  const sends = [];
  return { sends, queue: { ...fx.queue, enqueueFenced: async (args) => { sends.push(args); return replace ? replace(args) : fx.queue.enqueueFenced(args); } } };
};

const SCENARIOS = {
  'a refusal the queue gave is ledgered and published with its exit, and a recovery after the supervisor died never sends the enqueue again': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    fx.envelopeFields.dispatchKey = KEY;
    await fx.prepare();
    const refusing = counting(fx, async () => { throw missingFunction; });
    const first = await fx.step({ queue: refusing.queue });
    assert.deepEqual([first.state, first.decision.kind, first.decision.reason, first.decision.exit], ['settled', 'lost', 'dispatch-key-refused', 76]);
    assert.equal(fx.published().exit, 76);
    await fx.killSupervisor();
    const recovery = counting(fx);
    await fx.step({ queue: recovery.queue });
    assert.deepEqual(recovery.sends, [], 'the queue would accept it now, and it is still not sent');
    assert.equal(await rowCount(fx), 0);
    assert.equal(fx.published().exit, 76);
  },

  'a supervisor that died between recording the refusal and publishing it is recovered to the same published refusal, without a second send': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    fx.envelopeFields.dispatchKey = KEY;
    await fx.prepare();
    const refusing = counting(fx, async () => { throw missingFunction; });
    fx.hooks.afterEnqueueStage = () => { throw LOST_RESPONSE; };
    await assert.rejects(fx.step({ queue: refusing.queue }), (err) => err === LOST_RESPONSE);
    assert.equal(fx.published(), null, 'nothing was published before the death');
    delete fx.hooks.afterEnqueueStage;
    await fx.killSupervisor();
    const recovery = counting(fx);
    const recovered = await fx.step({ queue: recovery.queue });
    assert.deepEqual([recovered.state, recovered.decision.reason, recovered.decision.exit], ['settled', 'dispatch-key-refused', 76]);
    assert.deepEqual(recovery.sends, []);
    assert.equal(await rowCount(fx), 0);
    assert.equal(fx.published().exit, 76);
  },

  'a keyed row withdrawn after a lost enqueue response is a refusal on replay, never a queued ticket': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    fx.envelopeFields.dispatchKey = KEY;
    await fx.prepare();
    const losing = counting(fx, async (args) => { await fx.queue.enqueueFenced(args); throw LOST_RESPONSE; });
    await assert.rejects(fx.step({ queue: losing.queue }), (err) => err === LOST_RESPONSE);
    await fx.setRow("state = 'withdrawn', finished_at = now()");
    const replay = await fx.step();
    assert.deepEqual([replay.state, replay.decision.reason, replay.decision.exit], ['settled', 'dispatch-key-refused', 76]);
    assert.match(replay.decision.detail, /withdrawn/);
    assert.equal(fx.published().exit, 76);
  },

  'a proven withdrawal of a keyed ticket publishes the same exit whether the live supervisor or a recovery does': async (load, w, t) => {
    const fx = await pullFixture(t, w, { load });
    const { refusalExitOf: refusalExit } = await load('pull-publication.js');
    assert.equal(refusalExit({ dispatchKey: KEY }, 76), 76);
    assert.equal(refusalExit({}, 76), undefined);
    assert.equal(refusalExit({ dispatchOnly: true }, 76), 76);
    fx.envelopeFields.dispatchKey = KEY;
    fx.envelopeFields.publication = { refusalExit: refusalExit({ dispatchKey: KEY }, 76) };
    await fx.enqueue();
    assert.equal((await fx.withdraw()).proof, true);
    await fx.killSupervisor();
    const recovered = await fx.step();
    assert.deepEqual([recovered.state, recovered.decision.reason, recovered.decision.exit], ['settled', 'withdrawn-never-started', 76]);
    assert.equal(fx.published().exit, 76);
  },
};

const CANARIES = [
  ['a refusal the queue gave is ledgered and published with its exit, and a recovery after the supervisor died never sends the enqueue again', 'pull-ticket.js', ['{ ...lost(DISPATCH_KEY_REFUSED_REASON, sent.reason), exit: sent.exitCode }', '{ ...lost(DISPATCH_KEY_REFUSED_REASON, sent.reason), exit: 1 }']],
  ['a refusal the queue gave is ledgered and published with its exit, and a recovery after the supervisor died never sends the enqueue again', 'pull-ticket.js', ['    if (sent.outcome === DISPATCH_KEY_REFUSED) return await afterStep', '    if (false) return await afterStep'], /Cannot read properties of undefined/],
  ['a supervisor that died between recording the refusal and publishing it is recovered to the same published refusal, without a second send', 'pull-enqueue.js', ['      return { outcome: DISPATCH_KEY_REFUSED, ...intent.refusal };\n', '      return { outcome: \'queued\', view: null };\n'], /Cannot read properties of undefined/],
  ['a keyed row withdrawn after a lost enqueue response is a refusal on replay, never a queued ticket', 'pull-enqueue.js', ["args.dispatchKey !== undefined && view.state === 'withdrawn'", 'false'], /Cannot read properties of undefined/],
  ['a proven withdrawal of a keyed ticket publishes the same exit whether the live supervisor or a recovery does', 'pull-publication.js', ['(dispatchOnly || dispatchKey || pullOnly ? noRunnerExit : undefined)', '(dispatchOnly || pullOnly ? noRunnerExit : undefined)']],
];

assert.equal(refusalExitOf({ dispatchKey: KEY }, 76), 76);
definePullSuite('keyed recovery on a disposable Postgres', SCENARIOS, CANARIES);
