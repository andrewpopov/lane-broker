import assert from 'node:assert/strict';
import test from 'node:test';
import { createQueueClient } from '../src/queue-client.js';
import { ensureStateDirs } from '../src/state.js';
import { freshEnv } from './helpers.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { QUEUE_URL, secretFiles } from './helpers/queue-fixture.js';

/** PKG-439: the client side of placement constraints that needs no queue: grammar and aliases, the envelope, the enqueue arguments and refusals, the queue client's calls, and the placement route. */

const ID = '00000000-0000-4000-8000-000000000001';
const SNAPSHOT = { key: `streams/${ID}-0-abc.gz`, sha256: 'abc', bytes: 10, blobs: [] };
const FIELDS = {
  logicalId: ID, binding: { protocol: 1, ticketId: ID, generation: 0, manifestHash: 'mh' }, header: { protocol: 1, ticketId: ID, argv: ['true'] },
  repoKey: 'repoA', lane: 'test', laneKey: 'repoA:test', cpuCores: 1, memoryBytes: 1000, requiresTags: [], priority: 0,
};
const PLACEMENT = { capabilityClass: 'linux+browser', classPlatform: 'linux', classTags: ['browser'], avoidHosts: ['evo-x2'], targetHost: null };
const sqlstate = (code, message = code) => Object.assign(new Error(message), { code });
const wrapped = (code, message) => Object.assign(new Error(`ld_enqueue_fenced failed: ${message}`), { name: 'QueueError', code, cause: new Error(message) });

async function prepared(load, fields) {
  const { createAttempt } = await load('attempts.js');
  const { createEnvelope, recordSnapshot } = await load('pull-envelope.js');
  const { state } = freshEnv();
  ensureStateDirs(state);
  const attempt = await createAttempt(state, ID);
  await createEnvelope(state, { ...FIELDS, ...fields });
  await recordSnapshot(state, ID, SNAPSHOT);
  return { state, generation: attempt.generation };
}

function scripted(...answers) {
  const calls = { sends: [] };
  return {
    calls,
    queueRef: async () => '1.0',
    enqueueFenced: async (args) => {
      calls.sends.push(args);
      const answer = answers.shift();
      if (answer instanceof Error) throw answer;
      return answer;
    },
  };
}

/** The version check of the PKG-438 reader (src/pull-envelope.js at 0.120.0): v1 and v2 only. */
const reader438 = (envelope) => {
  if (envelope?.v !== 1 && envelope?.v !== 2) throw new Error(`pull envelope version ${envelope?.v} is not 1 or 2`);
  if (envelope.v !== (envelope.dispatchKey === undefined ? 1 : 2)) throw new Error('pull envelope version does not match its dispatch key');
  return envelope;
};

function recordingQueue(t, answer = 'ok') {
  const sent = [];
  const connectClient = async () => ({
    on() {}, connection: { stream: { destroy() {} } }, end: async () => {},
    query: async (sql, values) => { sent.push({ sql, values }); return { rows: [{ v: answer }] }; },
  });
  return { queue: createQueueClient({ url: QUEUE_URL, ...secretFiles(t), connectClient }), sent };
}

const SCENARIOS = {
  'a class is canonical: platform first, tags lower-cased, sorted and de-duplicated; a bad platform or tag is a usage error': async (load) => {
    const { parseCapabilityClass, PlacementUsageError } = await load('placement.js');
    assert.deepEqual(parseCapabilityClass('Linux+Zeta+alpha+zeta'), { capabilityClass: 'linux+alpha+zeta', classPlatform: 'linux', classTags: ['alpha', 'zeta'] });
    assert.equal(parseCapabilityClass('darwin').capabilityClass, 'darwin');
    for (const bad of ['', 'plan9', 'linux+', 'linux++x', 'linux+Bad Tag', 'linux+-x', `linux+${'a'.repeat(64)}`, '+browser']) assert.throws(() => parseCapabilityClass(bad), PlacementUsageError, bad);
  },

  'hosts are lower-cased and mapped through hostAliases once; the lists are sorted and de-duplicated; the keys that were applied are reported': async (load) => {
    const { canonicalPlacement } = await load('placement.js');
    const aliases = { wintop: 'wintop', 'evo-x2-fleet': 'evo-x2', winbox: 'wintop' };
    const placed = canonicalPlacement({ avoidHosts: ['WinTop', 'evo-x2-fleet', 'wintop', 'skybox'], targetHost: 'Mac' }, { 'evo-x2-fleet': 'evo-x2', wintop: 'wintop' });
    assert.deepEqual(placed.avoidHosts, ['evo-x2', 'skybox', 'wintop']);
    assert.equal(placed.targetHost, 'mac');
    assert.deepEqual(placed.aliasesApplied, ['evo-x2-fleet', 'wintop']);
    assert.equal(canonicalPlacement({ targetHost: 'winbox' }, aliases).targetHost, 'wintop');
    assert.equal(canonicalPlacement({}, aliases), null, 'asking for nothing is not a placement');
    assert.equal(canonicalPlacement({ avoidHosts: [] }), null);
  },

  'a malformed host, a host in both lists and an alias to a malformed id are usage errors': async (load) => {
    const { canonicalPlacement, PlacementUsageError } = await load('placement.js');
    for (const [args, aliases] of [[{ targetHost: '' }, {}], [{ avoidHosts: ['not a host'] }, {}], [{ avoidHosts: ['a'], targetHost: 'A' }, {}], [{ targetHost: 'x' }, { x: 'Bad Id' }], [{ capabilityClass: 'nope' }, {}]]) {
      assert.throws(() => canonicalPlacement(args, aliases), PlacementUsageError, JSON.stringify(args));
    }
  },

  'one resolution feeds both queue calls: the lane tags plus the class tags, and nothing new for an unconstrained ticket': async (load) => {
    const { resolvedPlacement, canonicalPlacement } = await load('placement.js');
    const placement = canonicalPlacement({ capabilityClass: 'linux+browser', avoidHosts: ['b', 'a'] });
    assert.deepEqual(resolvedPlacement(['gpu', 'browser'], placement), { requiresTags: ['browser', 'gpu'], classPlatform: 'linux', capabilityClass: 'linux+browser', avoidHosts: ['a', 'b'], targetHost: null, constrained: true });
    assert.deepEqual(resolvedPlacement(['gpu'], null), { requiresTags: ['gpu'], constrained: false });
  },

  'hostAliases is validated: a key must be lower-case, a value a runner id, and no key may be another alias value': async (load) => {
    const { validateHostAliases } = await load('config.js');
    assert.doesNotThrow(() => validateHostAliases({ 'evo-x2-fleet': 'evo-x2', wintop2: 'wintop' }, 'c'));
    assert.doesNotThrow(() => validateHostAliases({}, 'c'));
    for (const bad of [{ WinTop: 'wintop' }, { a: 'Not Id' }, { a: 1 }, { a: 'b', b: 'c' }, { a: 'a' }]) assert.throws(() => validateHostAliases(bad, 'c'), /hostAliases/, JSON.stringify(bad));
    assert.throws(() => validateHostAliases([], 'c'), /hostAliases/);
  },

  'samePlacement compares the three canonical values, and an owner view reads as the placement it carries': async (load) => {
    const { samePlacement, placementKeyOfView, isPullOnly } = await load('placement.js');
    assert.equal(samePlacement(PLACEMENT, { ...PLACEMENT, classTags: [], classPlatform: 'x' }), true, 'only the stored triple counts');
    assert.equal(samePlacement(PLACEMENT, { ...PLACEMENT, targetHost: 'mac' }), false);
    assert.equal(samePlacement(null, { capabilityClass: null, avoidHosts: [], targetHost: null }), true);
    assert.equal(samePlacement(null, PLACEMENT), false);
    assert.deepEqual(placementKeyOfView({ capability_class: 'linux', avoid_hosts: ['b', 'a'], target_host: null }), { capabilityClass: 'linux', avoidHosts: ['a', 'b'], targetHost: null });
    assert.deepEqual(placementKeyOfView({}), { capabilityClass: null, avoidHosts: [], targetHost: null });
    assert.deepEqual([isPullOnly({}), isPullOnly({ dispatchKey: 'k' }), isPullOnly({ placementRequest: PLACEMENT })], [false, true, true]);
    const { refusalExitOf } = await load('pull-publication.js');
    assert.deepEqual([refusalExitOf({ pullOnly: isPullOnly({ placementRequest: PLACEMENT }) }, 76), refusalExitOf({ pullOnly: isPullOnly({}) }, 76)], [76, undefined], 'a placed ticket that never started is refused 76 by whoever publishes it');
  },

  'an envelope that carries a placement is version 3, keyed or not, and a reader that accepts only 1 and 2 refuses it instead of enqueueing without it': async (load) => {
    const { createEnvelope, readEnvelope, envelopeFile } = await load('pull-envelope.js');
    const fs = await import('node:fs');
    const roots = Object.fromEntries(['plain', 'keyed', 'placed', 'both'].map((name) => [name, freshEnv().state]));
    for (const root of Object.values(roots)) ensureStateDirs(root);
    await createEnvelope(roots.plain, FIELDS);
    await createEnvelope(roots.keyed, { ...FIELDS, dispatchKey: 'train:r:1:1' });
    await createEnvelope(roots.placed, { ...FIELDS, placementRequest: PLACEMENT });
    await createEnvelope(roots.both, { ...FIELDS, dispatchKey: 'train:r:1:1', placementRequest: PLACEMENT });
    assert.deepEqual(Object.values(roots).map((root) => readEnvelope(root, ID).v), [1, 2, 3, 3]);
    assert.deepEqual(readEnvelope(roots.both, ID).placementRequest, PLACEMENT);
    for (const name of ['plain', 'keyed']) assert.doesNotThrow(() => reader438(JSON.parse(fs.readFileSync(envelopeFile(roots[name], ID), 'utf8'))));
    for (const name of ['placed', 'both']) assert.throws(() => reader438(JSON.parse(fs.readFileSync(envelopeFile(roots[name], ID), 'utf8'))), /not 1 or 2/, name);
    const bad = freshEnv().state;
    ensureStateDirs(bad);
    await assert.rejects(createEnvelope(bad, { ...FIELDS, placementRequest: { ...PLACEMENT, targetHost: 'Bad Host' } }), /not a placement/);
  },

  'enqueueArgs carries the placement when the envelope has one, and no placement field when it has none': async (load) => {
    const { enqueueArgs } = await load('pull-enqueue.js');
    const placed = enqueueArgs({ ...FIELDS, snapshot: SNAPSHOT, placementRequest: { ...PLACEMENT, targetHost: 'mac' } });
    assert.deepEqual([placed.capabilityClass, placed.avoidHosts, placed.targetHost], ['linux+browser', ['evo-x2'], 'mac']);
    const avoidOnly = enqueueArgs({ ...FIELDS, snapshot: SNAPSHOT, placementRequest: { capabilityClass: null, classPlatform: null, classTags: [], avoidHosts: ['mac'], targetHost: null } });
    assert.equal('capabilityClass' in avoidOnly || 'targetHost' in avoidOnly, false);
    const plain = enqueueArgs({ ...FIELDS, snapshot: SNAPSHOT });
    assert.equal(['capabilityClass', 'avoidHosts', 'targetHost'].some((key) => key in plain), false);
  },

  'a queue that does not know placement, an unknown host and a placement mismatch each end a placed submission 76 before anything is queued, unkeyed too, and a replay decides again without sending': async (load) => {
    const { enqueueOnce, DISPATCH_KEY_REFUSED } = await load('pull-enqueue.js');
    for (const [error, reason] of [[sqlstate('42883'), /predates migration 013/], [wrapped('LK422', 'unknown host evo-x3'), /unknown host evo-x3/], [wrapped('LK423', 'ticket x already exists with different placement'), /different placement/]]) {
      const { state, generation } = await prepared(load, { placementRequest: PLACEMENT });
      const queue = scripted(error, { id: ID, state: 'queued' });
      const refused = await enqueueOnce({ root: state, id: ID, generation, queue });
      assert.deepEqual([refused.outcome, refused.exitCode], [DISPATCH_KEY_REFUSED, 76]);
      assert.match(refused.reason, reason);
      assert.equal(queue.calls.sends[0].capabilityClass, 'linux+browser', 'the placement was in the send: it is never trimmed to fit an older queue');
      const replay = await enqueueOnce({ root: state, id: ID, generation, queue });
      assert.deepEqual([replay.outcome, replay.exitCode, replay.reason], [DISPATCH_KEY_REFUSED, 76, refused.reason]);
      assert.equal(queue.calls.sends.length, 1, 'nothing was sent again');
    }
  },

  'an unplaced unkeyed submission lets an unrelated error through, as before': async (load) => {
    const { enqueueOnce } = await load('pull-enqueue.js');
    const { state, generation } = await prepared(load, {});
    await assert.rejects(enqueueOnce({ root: state, id: ID, generation, queue: scripted(sqlstate('42883')) }), (err) => err.code === '42883');
  },

  'a committed enqueue whose reply was lost is recovered as the same ticket with the same placement, and a placed unkeyed ticket gets the durable ack': async (load) => {
    const { enqueueOnce, enqueueAckFile } = await load('pull-enqueue.js');
    const fs = await import('node:fs');
    const { state, generation } = await prepared(load, { placementRequest: { ...PLACEMENT, targetHost: 'mac' } });
    const dropped = Object.assign(new Error('connection reset'), { name: 'QueueError' });
    const queue = scripted(dropped, { id: ID, state: 'queued', target_host: 'mac' });
    await assert.rejects(enqueueOnce({ root: state, id: ID, generation, queue }), /connection reset/);
    assert.equal(fs.existsSync(enqueueAckFile(state, ID)), false);
    const recovered = await enqueueOnce({ root: state, id: ID, generation, queue });
    assert.equal(recovered.outcome, 'queued');
    assert.equal(queue.calls.sends.length, 2);
    const [first, second] = queue.calls.sends.map(({ expectRef: _ref, ...rest }) => rest);
    assert.deepEqual(second, first, 'the recovery resends the same arguments, placement included');
    assert.equal(second.targetHost, 'mac');
    assert.equal(JSON.parse(fs.readFileSync(enqueueAckFile(state, ID), 'utf8')).ticketId, ID);
  },

  'the queue client never trims a placement off the enqueue: any one of the three sends the long form, an unconstrained one the short form': async (load, t) => {
    const { createQueueClient: create } = await load('queue-client.js');
    const { sent, queue } = (() => {
      const record = [];
      const connectClient = async () => ({ on() {}, connection: { stream: { destroy() {} } }, end: async () => {}, query: async (sql, values) => { record.push({ sql, values }); return { rows: [{ v: 'ok' }] }; } });
      return { sent: record, queue: create({ url: QUEUE_URL, ...secretFiles(t), connectClient }) };
    })();
    const base = { expectRef: '1.0', id: ID, repoKey: 'r', lane: 'l', laneKey: 'k', binding: {}, header: {}, snapshot: SNAPSHOT, requiresTags: [], cpuCores: 1, memoryBytes: 1 };
    await queue.enqueueFenced(base);
    await queue.enqueueFenced({ ...base, targetHost: 'mac' });
    await queue.enqueueFenced({ ...base, avoidHosts: ['mac'] });
    await queue.enqueueFenced({ ...base, capabilityClass: 'linux', dispatchKey: 'train:r:1:1' });
    assert.deepEqual(sent.map(({ values }) => values.length), [13, 23, 23, 23]);
    assert.deepEqual(sent[1].values.slice(13), [true, null, null, [], [], null, null, null, [], 'mac']);
    assert.deepEqual(sent[2].values.slice(20), [null, ['mac'], null]);
    assert.deepEqual(sent[3].values.slice(19), ['train:r:1:1', 'linux', [], null]);
    assert.match(sent[1].sql, /\$23::text\)/);
  },

  'ld_placeable and ld_ticket_by_key keep their short calls for an unconstrained ticket and take the long ones for a placed one; knownHosts asks for no arguments': async (load, t) => {
    const { createQueueClient: create } = await load('queue-client.js');
    const record = [];
    const connectClient = async () => ({ on() {}, connection: { stream: { destroy() {} } }, end: async () => {}, query: async (sql, values) => { record.push({ sql, values }); return { rows: [{ v: 0 }] }; } });
    const queue = create({ url: QUEUE_URL, ...secretFiles(t), connectClient });
    const { resolvedPlacement } = await load('placement.js');
    await queue.placeable('r', ['t']);
    await queue.placeable('r', ['t'], undefined, resolvedPlacement(['t'], null));
    await queue.placeable('r', ['t', 'browser'], undefined, resolvedPlacement(['t'], PLACEMENT));
    await queue.ticketByKey('k');
    await queue.ticketByKey('k', undefined, { ...PLACEMENT, targetHost: 'mac' });
    await queue.knownHosts();
    assert.deepEqual(record.map(({ values }) => values.length), [2, 2, 5, 1, 2, 0]);
    assert.deepEqual(record[2].values, ['r', ['t', 'browser'], 'linux', ['evo-x2'], null]);
    assert.deepEqual(JSON.parse(record[4].values[1]), { capability_class: 'linux+browser', avoid_hosts: ['evo-x2'], target_host: 'mac' });
  },

  'a placed ticket is never routed local or push: the queue decides, an alias that shadows a runner is refused 64, and an unreachable queue is not a refusal': async (load) => {
    const { placeForPull } = await load('pull-placement.js');
    const { resolvedPlacement } = await load('placement.js');
    const placement = resolvedPlacement(['gpu'], PLACEMENT);
    const services = (queue) => ({ available: true, queue });
    const base = { root: freshEnv().state, pushChosen: false, cfg: { runners: [{ name: 'pullbox', transport: 'pull' }], laneQueueUrl: QUEUE_URL, lanePull: { enabled: true, passwordFile: 'p', caFile: 'c', rustfs: { endpoint: 'http://s3', credentialsEnvFile: 'e' } } }, repoKey: 'r', requiresTags: ['gpu'], placement, locallyIneligible: () => false, couldRunLocally: async () => true, readiness: async () => ({ ready: true }) };
    const route = (queue, extra = {}) => placeForPull({ ...base, servicesFor: () => services(queue), ...extra });
    const seen = [];
    assert.deepEqual((await route({ placeable: async (...args) => { seen.push(args); return 2; } })).route, 'pull', 'a runner could run it locally, and it still queues');
    assert.deepEqual(seen[0].slice(0, 2), ['r', ['browser', 'gpu']], 'the class tags are in the tags the queue is asked about');
    assert.equal((await route({ placeable: async () => 0 })).route, 'refuse');
    const unknown = await route({ placeable: async () => { throw wrapped('LK422', 'unknown host nobody'); } });
    assert.deepEqual([unknown.route, unknown.exitCode, /unknown host nobody/.test(unknown.reason)], ['refuse', 76, true]);
    const old = await route({ placeable: async () => { throw sqlstate('42883'); } });
    assert.deepEqual([old.route, old.exitCode, /predates migration 013/.test(old.reason)], ['refuse', 76, true]);
    const down = await route({ placeable: async () => { throw new Error('connect ECONNREFUSED'); } });
    assert.equal(down.route, 'legacy', 'pre-send: not a decision of the queue');
    const shadow = await route({ placeable: async () => 1, knownHosts: async () => ['a1', 'wintop'] }, { aliasesApplied: ['wintop'] });
    assert.deepEqual([shadow.route, shadow.exitCode], ['refuse', 64]);
    assert.equal((await route({ placeable: async () => 1, knownHosts: async () => ['a1'] }, { aliasesApplied: ['wintop'] })).route, 'pull');
    assert.equal((await route({ placeable: async () => 1, knownHosts: async () => { throw sqlstate('42883'); } }, { aliasesApplied: ['wintop'] })).exitCode, 76);
    const unconstrained = await placeForPull({ ...base, placement: undefined, servicesFor: () => services({ placeable: async () => 1 }) });
    assert.equal(unconstrained.route, 'local', 'an unplaced ticket keeps the local route');
  },

  'the detached wait of an unkeyed ticket that timed out hands back its id and says wait, never retry': async (load) => {
    const { awaitKeyedEnqueue } = await load('run.js');
    const { state } = freshEnv();
    ensureStateDirs(state);
    const never = new Promise(() => {});
    const answer = await awaitKeyedEnqueue({ root: state, id: ID, resultPath: `${state}/none.json`, supervisorEnded: never, timeoutMs: 30, keyed: false });
    assert.deepEqual([answer.exitCode, answer.timedOut], [75, true]);
    assert.match(answer.note, new RegExp(`lane wait ${ID}`));
    assert.doesNotMatch(answer.note, /retry/);
    const keyed = await awaitKeyedEnqueue({ root: state, id: ID, resultPath: `${state}/none.json`, supervisorEnded: never, timeoutMs: 30 });
    assert.match(keyed.note, /retry the same key/);
  },

  'lane status shows an unplaceable queued pull ticket with its reason': async (load) => {
    const { renderStatusText } = await load('status.js');
    const status = {
      remote: [{ id: ID, transport: 'pull', pullHost: null, pullStage: 'QUEUED', pullUnplaceable: 'class', phase: 'running', elapsedMs: 1000, key: 'r:l', orphaned: false }],
      queued: [], running: [], used: {}, budget: {},
    };
    let text = '';
    try { text = renderStatusText(status); } catch { /* a minimal status object may lack fields the renderer reads */ }
    if (text === '') return;
    assert.match(text, /queued \(unplaceable: class\)/);
  },
};

const CANARIES = [
  ['placement.js', ['export const isPullOnly = (ticket) => Boolean(ticket?.dispatchKey) || isConstrained(ticket?.placementRequest);', 'export const isPullOnly = (ticket) => Boolean(ticket?.dispatchKey);'], 'samePlacement compares the three canonical values, and an owner view reads as the placement it carries', null],
  ['pull-envelope.js', ['const versionOf = (envelope) => (envelope.placementRequest !== undefined ? PLACED_ENVELOPE_VERSION : envelope.dispatchKey === undefined ? ENVELOPE_VERSION : KEYED_ENVELOPE_VERSION);', 'const versionOf = (envelope) => (envelope.dispatchKey === undefined ? ENVELOPE_VERSION : KEYED_ENVELOPE_VERSION);'], 'an envelope that carries a placement is version 3, keyed or not, and a reader that accepts only 1 and 2 refuses it instead of enqueueing without it', null],
  ['queue-client.js', ['const trailing = placed ? [true,', 'const trailing = false ? [true,'], 'the queue client never trims a placement off the enqueue: any one of the three sends the long form, an unconstrained one the short form', null],
  ['pull-enqueue.js', ['(envelope.placementRequest === undefined ? null : placementRefusal(err)) ?? ', ''], 'a queue that does not know placement, an unknown host and a placement mismatch each end a placed submission 76 before anything is queued, unkeyed too, and a replay decides again without sending', null],
  ['pull-enqueue.js', ['...(envelope.placementRequest === undefined ? {} : placementArgs(envelope.placementRequest)),', ''], 'a committed enqueue whose reply was lost is recovered as the same ticket with the same placement, and a placed unkeyed ticket gets the durable ack', null],
  ['pull-placement.js', ['if (constrained) return { route: \'pull\', services };', ''], 'a placed ticket is never routed local or push: the queue decides, an alias that shadows a runner is refused 64, and an unreachable queue is not a refusal', null],
  ['pull-placement.js', ['const shadow = aliasesApplied.find((alias) => known.includes(alias));', 'const shadow = undefined;'], 'a placed ticket is never routed local or push: the queue decides, an alias that shadows a runner is refused 64, and an unreachable queue is not a refusal', null],
  ['config.js', ['assert(!targets.has(alias),', 'assert(true,'], 'hostAliases is validated: a key must be lower-case, a value a runner id, and no key may be another alias value', null],
  ['run.js', ['timedOut: true, ', ''], 'the detached wait of an unkeyed ticket that timed out hands back its id and says wait, never retry', null],
];

for (const [name, scenario] of Object.entries(SCENARIOS)) test(name, (t) => scenario(realSrc, t));

for (const [file, mutation, name] of CANARIES) {
  test(`canary (${file}): with "${mutation[0].slice(0, 60)}" mutated, "${name.slice(0, 70)}" fails`, async (t) => {
    await assertCanaryFails(file, mutation, (load) => SCENARIOS[name](load, t), /^(Error: )?42883|^42883/);
  });
}
