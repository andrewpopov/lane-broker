import assert from 'node:assert/strict';
import test from 'node:test';
import { ensureStateDirs } from '../src/state.js';
import { freshEnv } from './helpers.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';

/** PKG-438: the client side of a dispatch key that needs no queue: the envelope, the enqueue arguments, and how an enqueue answer ends a keyed submission. */

const ID = '00000000-0000-4000-8000-000000000001';
const OTHER = '00000000-0000-4000-8000-000000000002';
const KEY = 'train:repoA:7:2';
const SNAPSHOT = { key: `streams/${ID}-0-abc.gz`, sha256: 'abc', bytes: 10, blobs: [] };
const FIELDS = {
  logicalId: ID, binding: { protocol: 1, ticketId: ID, generation: 0, manifestHash: 'mh' }, header: { protocol: 1, ticketId: ID, argv: ['true'] },
  repoKey: 'repoA', lane: 'test', laneKey: 'repoA:test', cpuCores: 1, memoryBytes: 1000, requiresTags: [], priority: 0,
};
const missingFunction = Object.assign(new Error('function does not exist'), { code: '42883' });
const heldElsewhere = Object.assign(new Error('held by another owner'), { code: 'LK409' });

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

/** A queue that answers each send with the next of `answers` (a value, or an Error to throw) and counts what it was asked. */
function scripted(...answers) {
  const calls = { refs: 0, sends: [] };
  return {
    calls,
    queueRef: async () => { calls.refs += 1; return '1.0'; },
    enqueueFenced: async (args) => {
      calls.sends.push(args);
      const answer = answers.shift();
      if (answer instanceof Error) throw answer;
      return answer;
    },
  };
}

const intentAnswers = async (load, state) => (await load('attempts.js')).readAttempt(state, ID).pull.enqueue.attempts.map((attempt) => attempt.answer);

const SCENARIOS = {
  'the envelope round-trips a dispatch key, reads one written without it, and refuses an invalid one': async (load) => {
    const { createEnvelope, readEnvelope } = await load('pull-envelope.js');
    const { state } = freshEnv();
    ensureStateDirs(state);
    await createEnvelope(state, { ...FIELDS, dispatchKey: KEY });
    assert.equal(readEnvelope(state, ID).dispatchKey, KEY);
    const plain = freshEnv().state;
    ensureStateDirs(plain);
    await createEnvelope(plain, FIELDS);
    assert.equal(readEnvelope(plain, ID).dispatchKey, undefined, 'an envelope from before the key still reads');
    const bad = freshEnv().state;
    ensureStateDirs(bad);
    await assert.rejects(createEnvelope(bad, { ...FIELDS, dispatchKey: 'has space' }), /not a valid dispatch key/);
  },

  'enqueueArgs sends the key when the envelope has one, and no key field when it has none': async (load) => {
    const { enqueueArgs } = await load('pull-enqueue.js');
    assert.equal(enqueueArgs({ ...FIELDS, snapshot: SNAPSHOT, dispatchKey: KEY }).dispatchKey, KEY);
    assert.equal('dispatchKey' in enqueueArgs({ ...FIELDS, snapshot: SNAPSHOT }), false);
  },

  'a queue without the keyed signature refuses the submission 76 before anything is queued, and the refusal is recorded so a replay decides it again and never sends': async (load) => {
    const { enqueueOnce, DISPATCH_KEY_REFUSED } = await load('pull-enqueue.js');
    const { state, generation } = await prepared(load, { dispatchKey: KEY });
    const queue = scripted(missingFunction, { id: ID, state: 'queued' });
    const refused = await enqueueOnce({ root: state, id: ID, generation, queue });
    assert.deepEqual([refused.outcome, refused.exitCode], [DISPATCH_KEY_REFUSED, 76]);
    assert.match(refused.reason, /dispatch key needs the pull queue/);
    assert.equal(queue.calls.sends[0].dispatchKey, KEY, 'the key was in the send; it is never trimmed off');
    assert.deepEqual(await intentAnswers(load, state), ['refused']);
    // the supervisor died before it published: the recovery decides the same refusal from the record, even though this queue would now take the send
    const replay = await enqueueOnce({ root: state, id: ID, generation, queue });
    assert.deepEqual([replay.outcome, replay.exitCode, replay.reason], [DISPATCH_KEY_REFUSED, 76, refused.reason]);
    assert.equal(queue.calls.sends.length, 1, 'nothing was sent again');
  },

  'a key another owner holds is refused 64, a definite refusal': async (load) => {
    const { enqueueOnce } = await load('pull-enqueue.js');
    const { state, generation } = await prepared(load, { dispatchKey: KEY });
    const refused = await enqueueOnce({ root: state, id: ID, generation, queue: scripted(heldElsewhere) });
    assert.deepEqual([refused.outcome, refused.exitCode], ['dispatch-key-refused', 64]);
    assert.deepEqual(await intentAnswers(load, state), ['refused']);
  },

  'an enqueue answered with another ticket is a retryable refusal naming it, so this id never shadows the held ticket': async (load) => {
    const { enqueueOnce } = await load('pull-enqueue.js');
    const { state, generation } = await prepared(load, { dispatchKey: KEY });
    const refused = await enqueueOnce({ root: state, id: ID, generation, queue: scripted({ id: OTHER, state: 'queued' }) });
    assert.deepEqual([refused.outcome, refused.exitCode, refused.ticketId], ['dispatch-key-refused', 76, OTHER]);
    assert.match(refused.reason, new RegExp(OTHER));
    assert.deepEqual(await intentAnswers(load, state), ['refused']);
  },

  'a keyed enqueue whose row was withdrawn meanwhile is refused 76, not reported queued; an unkeyed one keeps its answer': async (load) => {
    const { enqueueOnce, DISPATCH_KEY_REFUSED } = await load('pull-enqueue.js');
    const keyed = await prepared(load, { dispatchKey: KEY });
    const refused = await enqueueOnce({ root: keyed.state, id: ID, generation: keyed.generation, queue: scripted({ id: ID, state: 'withdrawn' }) });
    assert.deepEqual([refused.outcome, refused.exitCode], [DISPATCH_KEY_REFUSED, 76]);
    assert.match(refused.reason, /withdrawn/);
    const plain = await prepared(load, {});
    assert.equal((await enqueueOnce({ root: plain.state, id: ID, generation: plain.generation, queue: scripted({ id: ID, state: 'withdrawn' }) })).outcome, 'queued');
  },

  'a keyed envelope is version 2 and an unkeyed one version 1; a reader that checks for version 1, as every release before the key did, refuses the keyed one instead of enqueueing it without its key': async (load) => {
    const { createEnvelope, readEnvelope, envelopeFile } = await load('pull-envelope.js');
    const fs = await import('node:fs');
    const keyed = freshEnv().state;
    const plain = freshEnv().state;
    for (const root of [keyed, plain]) ensureStateDirs(root);
    await createEnvelope(keyed, { ...FIELDS, dispatchKey: KEY });
    await createEnvelope(plain, FIELDS);
    assert.deepEqual([readEnvelope(keyed, ID).v, readEnvelope(plain, ID).v], [2, 1]);
    // the pre-0.119 validate, verbatim in what it checks: the version must be exactly 1
    const oldReader = (file) => {
      const envelope = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (envelope?.v !== 1) throw new Error(`pull envelope version ${envelope?.v} is not 1`);
      return envelope;
    };
    assert.doesNotThrow(() => oldReader(envelopeFile(plain, ID)));
    assert.throws(() => oldReader(envelopeFile(keyed, ID)), /version 2 is not 1/);
    // and a keyed envelope that claims version 1 is not trusted either
    const forged = JSON.parse(fs.readFileSync(envelopeFile(keyed, ID), 'utf8'));
    fs.writeFileSync(envelopeFile(keyed, ID), JSON.stringify({ ...forged, v: 1 }));
    assert.throws(() => readEnvelope(keyed, ID), /does not match its dispatch key/);
  },

  'a keyed enqueue answered with its own ticket is queued; an unkeyed one answered with a missing function throws it': async (load) => {
    const { enqueueOnce } = await load('pull-enqueue.js');
    const keyed = await prepared(load, { dispatchKey: KEY });
    assert.equal((await enqueueOnce({ root: keyed.state, id: ID, generation: keyed.generation, queue: scripted({ id: ID, state: 'queued' }) })).outcome, 'queued');
    const plain = await prepared(load, {});
    await assert.rejects(enqueueOnce({ root: plain.state, id: ID, generation: plain.generation, queue: scripted(missingFunction) }), (err) => err === missingFunction);
  },

};

const CANARIES = [
  ['a keyed enqueue whose row was withdrawn meanwhile is refused 76, not reported queued; an unkeyed one keeps its answer', 'pull-enqueue.js', ["args.dispatchKey !== undefined && view.state === 'withdrawn'", 'false']],
  ['a keyed envelope is version 2 and an unkeyed one version 1; a reader that checks for version 1, as every release before the key did, refuses the keyed one instead of enqueueing it without its key', 'pull-envelope.js', ['v: versionOf(fields), createdAt', 'v: ENVELOPE_VERSION, createdAt']],
  ['a keyed envelope is version 2 and an unkeyed one version 1; a reader that checks for version 1, as every release before the key did, refuses the keyed one instead of enqueueing it without its key', 'pull-envelope.js', ["  if (envelope.v !== versionOf(envelope)) throw new Error(`pull envelope version ${envelope.v} does not match its dispatch key`);\n", '']],
  ['the envelope round-trips a dispatch key, reads one written without it, and refuses an invalid one', 'pull-envelope.js', ["  if (envelope.dispatchKey !== undefined && !isDispatchKey(envelope.dispatchKey)) throw new Error('pull envelope dispatchKey is not a valid dispatch key');\n", '']],
  ['enqueueArgs sends the key when the envelope has one, and no key field when it has none', 'pull-enqueue.js', ["    ...(envelope.dispatchKey === undefined ? {} : { dispatchKey: envelope.dispatchKey }),\n", '']],
  ['a queue without the keyed signature refuses the submission 76 before anything is queued, and the refusal is recorded so a replay decides it again and never sends', 'pull-enqueue.js', ["const refusal = args.dispatchKey === undefined ? null : dispatchKeyRefusal(err);", 'const refusal = null;']],
  ['a key another owner holds is refused 64, a definite refusal', 'dispatch-key.js', ["err?.code === DISPATCH_KEY_HELD_SQLSTATE", 'false']],
  ['an enqueue answered with another ticket is a retryable refusal naming it, so this id never shadows the held ticket', 'pull-enqueue.js', ['view.id !== undefined && view.id !== id', 'false']],
  ['a keyed enqueue answered with its own ticket is queued; an unkeyed one answered with a missing function throws it', 'pull-enqueue.js', ['if (refusal === null) throw err;', 'if (refusal === null) return refuse({ reason: String(err), exitCode: 76 });']],
];

for (const [name, scenario] of Object.entries(SCENARIOS)) test(name, async () => scenario(realSrc));

for (const [name, file, mutation] of CANARIES) {
  test(`canary: with "${mutation[0].trim().slice(0, 70)}" removed, "${name}" fails`, async () => {
    await assertCanaryFails(file, mutation, (load) => SCENARIOS[name](load), /function does not exist|held by another owner|does not match its dispatch key/);
  });
}
