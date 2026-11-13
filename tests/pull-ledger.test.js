import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { ledgerDir, ledgerFile } from '../src/pull-ledger.js';
import { freshEnv } from './helpers.js';
import { recordFsyncs } from './helpers/fsync-recorder.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { tid } from './lane-dispatch-fixture.js';

/** BRAIN-1036 R5: the acceptance ledger on its own, T24, T25b and the ledger half of T33. */

const ID = tid(1);
const entry = (over = {}) => ({ logicalId: ID, epoch: 1, attemptRef: 'ref-a', resultSha256: 'sha-a', decision: { kind: 'finished', exit: 0 }, ...over });
const LINK = 'fs.linkSync(tmp, ledgerFile(root, entry.logicalId));';
const PRE_ADOPT_SYNC = '  ensureDurableDir(ledgerDir(root), { boundary: root });\n  return validate(JSON.parse(text));';
const PARENT_SYNC = '  ensureDurableDir(dir, { boundary: root });\n  const tmp';

async function recorded(fn) {
  const recorder = recordFsyncs();
  try {
    await fn();
  } finally {
    recorder.stop();
  }
  return recorder;
}

const SCENARIOS = {
  'T24: the same epoch and result body under a different attempt ref is refused, and the stored entry stands': async (load) => {
    const { ledgerAccept, readLedger } = await load('pull-ledger.js');
    const { state } = freshEnv();
    assert.equal(ledgerAccept(state, entry()).created, true);
    const other = ledgerAccept(state, entry({ attemptRef: 'ref-b' }));
    assert.deepEqual([other.created, other.same], [false, false]);
    assert.equal(other.entry.attemptRef, 'ref-a');
    assert.equal(readLedger(state, ID).attemptRef, 'ref-a');
    assert.equal(ledgerAccept(state, entry()).same, true, 'the identical acceptance is the same one');
  },

  'a queue-authored decision is keyed by kind and source, so a different source is refused': async (load) => {
    const { ledgerAccept } = await load('pull-ledger.js');
    const { state } = freshEnv();
    const cancelled = (source) => entry({ attemptRef: null, resultSha256: null, decision: { kind: 'cancelled', exit: 130, source } });
    assert.equal(ledgerAccept(state, cancelled('queued')).created, true);
    assert.equal(ledgerAccept(state, cancelled('queued')).same, true);
    assert.equal(ledgerAccept(state, cancelled('operator')).same, false);
  },

  'T25b: many processes racing with different entries yield exactly one created': async (load, t) => {
    const { state } = freshEnv();
    const ledgerUrl = load.url('pull-ledger.js');
    const go = path.join(state, 'go');
    const code = `
      import fs from 'node:fs';
      import { ledgerAccept } from ${JSON.stringify(ledgerUrl)};
      const [root, id, tag, go] = process.argv.slice(1);
      while (!fs.existsSync(go)) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 5);
      const out = ledgerAccept(root, { logicalId: id, epoch: 1, attemptRef: 'ref-' + tag, resultSha256: 'sha-' + tag, decision: { kind: 'finished', exit: 0 } });
      console.log(JSON.stringify({ created: out.created, stored: out.entry.attemptRef }));
    `;
    const runs = Array.from({ length: 6 }, (_, i) => new Promise((resolve, reject) => {
      const child = spawn(process.execPath, ['--input-type=module', '-e', code, state, ID, String(i), go], { stdio: ['ignore', 'pipe', 'inherit'] });
      let out = '';
      child.stdout.on('data', (chunk) => { out += chunk; });
      child.on('exit', (status) => (status === 0 ? resolve(JSON.parse(out)) : reject(new Error(`child exited ${status}`))));
    }));
    await new Promise((resolve) => setTimeout(resolve, 400));
    fs.writeFileSync(go, '');
    const answers = await Promise.all(runs);
    assert.equal(answers.filter((a) => a.created).length, 1, `${JSON.stringify(answers)}`);
    assert.equal(new Set(answers.map((a) => a.stored)).size, 1, 'every caller sees the one stored entry');
    t.diagnostic(`stored ${answers[0].stored}`);
  },

  'T33: an entry whose creator died right after the link is made durable by the reader before it is returned': async (load) => {
    const { ledgerAccept, readLedger, ledgerDir: dirOf } = await load('pull-ledger.js');
    const { state } = freshEnv();
    assert.throws(() => ledgerAccept(state, entry(), { hooks: { afterLink: () => { throw new Error('killed after link'); } } }), /killed after link/);
    let read;
    const recorder = await recorded(() => { read = readLedger(state, ID); });
    assert.equal(read.attemptRef, 'ref-a');
    assert.ok(recorder.fsyncedPaths().includes(dirOf(state)), 'the ledger directory was not fsynced before the entry was returned');
  },

  'T33: creating the ledger in a fresh root fsyncs every parent of its directory': async (load) => {
    const { ledgerAccept, ledgerDir: dirOf } = await load('pull-ledger.js');
    const { state } = freshEnv();
    const recorder = await recorded(() => ledgerAccept(state, entry()));
    assert.ok(recorder.fsyncedPaths().includes(dirOf(state)));
    assert.ok(recorder.fsyncedPaths().includes(state), 'the parent of the ledger directory was not fsynced');
  },
};

const SAME = 'const sameAcceptance = (a, b) => isDeepStrictEqual(identityOf(a), identityOf(b)) && isDeepStrictEqual(a.decision, b.decision);';

const CANARIES = [
  ['T24: the same epoch and result body under a different attempt ref is refused, and the stored entry stands', 'pull-ledger.js', [SAME, 'const sameAcceptance = () => true;']],
  ['T25b: many processes racing with different entries yield exactly one created', 'pull-ledger.js', [LINK, LINK.replace('linkSync', 'renameSync')]],
  ['T33: an entry whose creator died right after the link is made durable by the reader before it is returned', 'pull-ledger.js', [PRE_ADOPT_SYNC, '  return validate(JSON.parse(text));']],
  ['T33: creating the ledger in a fresh root fsyncs every parent of its directory', 'pull-ledger.js', [PARENT_SYNC, '  fs.mkdirSync(dir, { recursive: true });\n  const tmp']],
];

describe('pull ledger', () => {
  test('the ledger lives under the state root, one file per ticket', () => {
    assert.equal(ledgerFile('/r', ID), path.join(ledgerDir('/r'), `${ID}.json`));
  });
  for (const [name, scenario] of Object.entries(SCENARIOS)) test(name, async (t) => scenario(realSrc, t));
  for (const [name, file, mutation] of CANARIES) {
    test(`canary: with its guard removed, "${name}" fails`, async (t) => {
      const failure = await assertCanaryFails(file, mutation, (load) => SCENARIOS[name](load, t));
      t.diagnostic(`failed as expected: ${String(failure.message).split('\n')[0]}`);
    });
  }
});
