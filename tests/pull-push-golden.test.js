import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import zlib from 'node:zlib';
import { fileURLToPath } from 'node:url';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { laneRun } from './helpers.js';
import { setup } from './remote-harness.js';
import { makeTmpDir } from './helpers/tmp.js';

/**
 * BRAIN-1036 R7 T14: with no pull runner configured, a real supervisor's placement is what it was before the pull hook existed.
 * `fixtures/push-placement-golden.json` was recorded from lane-broker 3fafcb3 (R6, before R7), never from this tree: per fixture it holds
 * the supervisor's `updateAttempt` patches, its `fallbackOrRefuse` arguments, its `lane:` lines, its exit and, where a runner got it,
 * the exec header. Re-record against another checkout with `T14_GOLDEN_BIN=<checkout>/bin/lane.js T14_GOLDEN_RECORD=<file>`.
 */

const PRELOAD = fileURLToPath(new URL('./helpers/push-golden-preload.js', import.meta.url));
const GOLDEN = fileURLToPath(new URL('./fixtures/push-placement-golden.json', import.meta.url));
const DISPATCH_ONLY = { localExecution: 'refused' };
const NODE_EXIT_0 = ['node', '-e', 'process.exit(0)'];

const FIXTURES = {
  'an idle push runner takes the ticket': { ssh: 'tee' },
  'every runner busy and the ticket locally eligible': { ssh: 'queued-one' },
  'every runner busy on a dispatch-only host': { ssh: 'queued-one', globalExtra: DISPATCH_ONLY },
  'the only runner down on a dispatch-only host': { ssh: 'down', globalExtra: DISPATCH_ONLY },
  'the only runner down and the ticket locally eligible': { ssh: 'down' },
};

const normalize = (text) => text
  .replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g, '<id>')
  .replace(/ts=\S+?(?=[ ",])/g, 'ts=<time>')
  .replace(/"priorityAccruedMs":\d+/g, '"priorityAccruedMs":"<ms>"')
  .replace(/_(?:private_)?(?:var|tmp)_[^"\s]*\.git/g, '<repoKey>')
  .replace(/\b\d+(\.\d+)?\s?(ms|s)\b/g, '<duration>')
  .replace(/\/(?:private\/)?(?:var|tmp)\/[^\s"',;)]+/g, '<path>');

const jsonLines = (file) => (fs.existsSync(file) ? fs.readFileSync(file, 'utf8').split('\n').filter(Boolean).map((line) => JSON.parse(line)) : []);

export async function observeFixture(spec, laneBin) {
  const base = setup(spec);
  const log = path.join(makeTmpDir('push-golden-'), 'golden.jsonl');
  const env = { ...base.env, NODE_OPTIONS: `--import=${PRELOAD}`, LANE_TEST_GOLDEN_LOG: log };
  const res = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...NODE_EXIT_0], { env, cwd: base.repoDir, bin: laneBin });
  const execStdin = fs.existsSync(base.teePath) && fs.statSync(base.teePath).size > 0 ? fs.readFileSync(base.teePath) : null;
  const header = execStdin === null ? null : (execStdin[0] === 0x7b ? execStdin : zlib.gunzipSync(execStdin)).toString('utf8').split('\n')[0];
  const recorded = jsonLines(log);
  return JSON.parse(normalize(JSON.stringify({
    exit: res.code,
    laneLines: res.stderr.split('\n').filter((line) => line.startsWith('lane')),
    patches: recorded.filter((r) => 'patch' in r).map((r) => r.patch),
    fallbackOrRefuse: recorded.filter((r) => 'fallbackOrRefuse' in r).map((r) => r.fallbackOrRefuse),
    header: header === null ? null : JSON.parse(header),
  })));
}

const observeAll = async (laneBin) => Object.fromEntries(await Promise.all(Object.entries(FIXTURES).map(async ([name, spec]) => [name, await observeFixture(spec, laneBin)])));

if (process.env.T14_GOLDEN_RECORD) {
  test('record the push placement golden', { timeout: 300_000 }, async () => {
    fs.writeFileSync(process.env.T14_GOLDEN_RECORD, `${JSON.stringify(await observeAll(process.env.T14_GOLDEN_BIN), null, 2)}\n`);
  });
} else {
  const golden = JSON.parse(fs.readFileSync(GOLDEN, 'utf8'));
  const scenario = (name) => async (load) => assert.deepEqual((await observeAll(load.laneBin))[name], golden[name], name);
  for (const name of Object.keys(FIXTURES)) {
    test(`T14 golden: ${name}: patches, fallback arguments, lane lines, exit and header equal the pre-R7 recording`, { timeout: 180_000 }, async () => scenario(name)(realSrc));
  }
  const dispatchOnlyDown = 'the only runner down on a dispatch-only host';
  test('canary: with the no-pull-runner early return of the placement hook removed, the golden fails by name', { timeout: 180_000 }, async () => {
    const mutation = ["  if (!pullConfigured(cfg)) return { route: 'legacy' };\n", ''];
    const failure = await assertCanaryFails('pull-placement.js', mutation, scenario(dispatchOnlyDown));
    assert.match(failure.message, new RegExp(dispatchOnlyDown));
  });
}
