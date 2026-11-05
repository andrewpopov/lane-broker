import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { sleep } from './helpers.js';
import { tmpDir, makeRunner, makeGitWorktree, makeDispatchArgs } from './remote-harness.js';
import { dispatchRemote } from '../src/remote-client.js';
import { buildManifest } from '../src/remote-manifest.js';

/**
 * BRAIN-850: an ssh exec channel that never closes must not outlive a result the runner has already published. The fake ssh keeps
 * `remote-exec` open (optionally closing it after `closeAfterMs`) while `remote-result` serves whatever `record.json` holds.
 */

const GRACE_MS = 300;
const T = { timeout: 30_000 };

function makeHungSsh({ closeAfterMs = null, stdoutText = '', resultCallsMax = null, holdPipesMs = null } = {}) {
  const dir = tmpDir('hung-channel-ssh');
  const recordPath = path.join(dir, 'record.json');
  const logPath = path.join(dir, 'calls.log');
  const sshBin = path.join(dir, 'ssh');
  fs.writeFileSync(
    sshBin,
    `#!/usr/bin/env node
const fs = require('fs');
const cmd = process.argv.slice(2).filter((a) => !a.startsWith('-o') && a !== '-o').join(' ');
const log = (w) => fs.appendFileSync(${JSON.stringify(logPath)}, w + '\\n');
if (cmd.includes('remote-exec')) {
  log('exec');
  // a grandchild holding the inherited pipes open: SIGKILL ends ssh itself but its close event does not arrive until the grandchild exits
  if (${holdPipesMs === null ? 'false' : 'true'}) fs.writeFileSync(${JSON.stringify(path.join(dir, 'holder.pid'))}, String(require('child_process').spawn('sleep', ['${Math.ceil((holdPipesMs ?? 0) / 1000)}'], { stdio: 'inherit' }).pid));
  process.stdin.resume();
  process.stdin.on('end', () => {
    if (${JSON.stringify(stdoutText)}) process.stdout.write(${JSON.stringify(stdoutText)});
    if (${closeAfterMs === null ? 'false' : 'true'}) setTimeout(() => process.exit(255), ${closeAfterMs ?? 0});
    else setTimeout(() => process.exit(255), 40_000);
  });
} else if (cmd.includes('remote-result')) {
  log('result');
  // after this many served fetches every further result query fails like a dead transport
  if (${resultCallsMax === null ? 'false' : 'true'} && fs.readFileSync(${JSON.stringify(logPath)}, 'utf8').split('\\n').filter((l) => l === 'result').length > ${resultCallsMax ?? 0}) process.exit(255);
  const body = fs.existsSync(${JSON.stringify(recordPath)}) ? fs.readFileSync(${JSON.stringify(recordPath)}, 'utf8') : '';
  if (body) process.stdout.write(body + '\\n'); else process.stdout.write(JSON.stringify({ protocol: 1, missing: true, state: 'running', phase: 'admitted' }) + '\\n');
} else if (cmd.includes('remote-cancel')) {
  log('cancel');
  process.stdout.write(JSON.stringify({ protocol: 1, cancelConfirmed: true }) + '\\n');
}
`,
  );
  fs.chmodSync(sshBin, 0o755);
  const calls = () => (fs.existsSync(logPath) ? fs.readFileSync(logPath, 'utf8').split('\n').filter(Boolean) : []);
  const releaseHolder = () => {
    try {
      process.kill(Number(fs.readFileSync(path.join(dir, 'holder.pid'), 'utf8')), 'SIGKILL');
    } catch {
      // never started or gone
    }
  };
  return { sshBin, calls, releaseHolder, publish: (record) => fs.writeFileSync(recordPath, JSON.stringify(record)) };
}

/** Starts a dispatch against the hung ssh. `record` is a function of the bound result; it is published before the dispatch starts unless null. */
function startDispatch({ record, ssh = makeHungSsh(), rebalance, canWithdraw = false, graceMs = GRACE_MS } = {}) {
  const src = makeGitWorktree({ 'a.txt': 'hello' });
  const args = makeDispatchArgs();
  const { manifestHash } = buildManifest(src);
  const bound = { protocol: 1, ticketId: args.ticketId, generation: args.generation, kind: 'completed', exit: 0, signal: null, phase: 'command', manifestHash };
  if (record) ssh.publish(record(bound));
  const abort = new AbortController();
  let stdout = '';
  let stderr = '';
  let settled = null;
  const done = dispatchRemote({
    ...args,
    runner: makeRunner({ ssh: 'scripted', root: tmpDir('hung-runner-root') }),
    worktreeRoot: src,
    sshBin: ssh.sshBin,
    env: process.env,
    abortSignal: abort.signal,
    canWithdraw,
    channelGraceMs: graceMs,
    rebalance: { minQueuedMs: 60_000, intervalMs: 50, pickTarget: () => null, ...rebalance },
    deadlines: { resultMs: 5000, resultAttempts: 3 },
    onStdout: (d) => { stdout += d; },
    onStderr: (d) => { stderr += d; },
  }).then((result) => {
    settled = result;
    return result;
  });
  return { done, abort, ssh, bound, out: () => stdout, err: () => stderr, settled: () => settled };
}

/** Always releases the dispatch (kills the hung ssh) so a regression fails by name instead of hanging the run. */
async function finish(run, { expectSettled = true } = {}) {
  const guard = setTimeout(() => run.abort.abort(), 15_000);
  try {
    return await run.done;
  } finally {
    clearTimeout(guard);
    if (!expectSettled) run.abort.abort();
  }
}

test('hung channel + published exit 0 settles confirmed 0 within the grace, with the one-line notice', T, async () => {
  const run = startDispatch({ record: (b) => b });
  const startedAt = Date.now();
  const result = await finish(run);
  assert.equal(result.outcome, 'confirmed', JSON.stringify(result));
  assert.equal(result.exitCode, 0);
  assert.ok(Date.now() - startedAt < 10_000, 'settled long before the 40s channel timeout');
  assert.match(run.err(), /lane: remote channel from scripted did not close \d+s after its result was published; settled from the published result \(output may be truncated\)/);
});

test('hung channel + published nonzero exit settles with that exit', T, async () => {
  const result = await finish(startDispatch({ record: (b) => ({ ...b, exit: 7 }) }));
  assert.equal(result.outcome, 'confirmed', JSON.stringify(result));
  assert.equal(result.exitCode, 7);
});

test('hung channel + signal-terminated record settles with the classified code, not the raw exit', T, async () => {
  const result = await finish(startDispatch({ record: (b) => ({ ...b, exit: null, signal: 'SIGKILL' }) }));
  assert.equal(result.outcome, 'confirmed', JSON.stringify(result));
  assert.equal(result.exitCode, 1);
});

test('hung channel recovers with rebalance disabled and with an active rebalance watcher', T, async () => {
  const withWatcher = await finish(startDispatch({ record: (b) => b, canWithdraw: true }));
  assert.equal(withWatcher.outcome, 'confirmed', JSON.stringify(withWatcher));
  const noRebalanceWiring = await finish(startDispatch({ record: (b) => b, canWithdraw: false }));
  assert.equal(noRebalanceWiring.outcome, 'confirmed', JSON.stringify(noRebalanceWiring));
});

for (const [name, mutate] of [
  ['wrong generation', (b) => ({ ...b, generation: b.generation + 1 })],
  ['wrong manifestHash', (b) => ({ ...b, manifestHash: 'f'.repeat(64) })],
  ['wrong ticketId', (b) => ({ ...b, ticketId: 'someone-else' })],
  ['contradictory exit 0 with a signal on protocol 2', (b) => ({ ...b, protocol: 2, exit: 0, signal: 'SIGTERM' })],
  ['non-terminal kind', (b) => ({ ...b, kind: 'unfinished', reason: 'queue-timeout' })],
]) {
  test(`hung channel + published record with ${name} does NOT settle; abort resolves cancelled`, T, async () => {
    const run = startDispatch({ record: mutate });
    await sleep(GRACE_MS * 4 + 500);
    assert.equal(run.settled(), null, `settled early: ${JSON.stringify(run.settled())}`);
    assert.doesNotMatch(run.err(), /did not close/);
    run.abort.abort();
    const result = await finish(run, { expectSettled: false });
    assert.equal(result.outcome, 'cancelled', JSON.stringify(result));
  });
}

test('hung channel with a non-terminal (admitted) record keeps waiting', T, async () => {
  const run = startDispatch({ record: null });
  await sleep(GRACE_MS * 4 + 500);
  assert.equal(run.settled(), null);
  run.abort.abort();
  assert.equal((await finish(run, { expectSettled: false })).outcome, 'cancelled');
});

test('channelGraceMs 0 disables the fallback', T, async () => {
  const run = startDispatch({ record: (b) => b, graceMs: 0 });
  await sleep(1500);
  assert.equal(run.settled(), null);
  run.abort.abort();
  assert.equal((await finish(run, { expectSettled: false })).outcome, 'cancelled');
});

test('a channel that closes late but within the grace keeps its full output and does not use the fallback', T, async () => {
  const ssh = makeHungSsh({ closeAfterMs: 400, stdoutText: 'full-output-line\n' });
  const run = startDispatch({ record: (b) => b, ssh, graceMs: 5000 });
  const result = await finish(run);
  assert.equal(result.outcome, 'confirmed', JSON.stringify(result));
  assert.equal(run.out(), 'full-output-line\n');
  assert.doesNotMatch(run.err(), /did not close/);
});

test('an abort during the grace is the cancelled path, not the fallback', T, async () => {
  const run = startDispatch({ record: (b) => b, graceMs: 5000 });
  await sleep(700);
  assert.equal(run.settled(), null);
  run.abort.abort();
  const result = await finish(run, { expectSettled: false });
  assert.equal(result.outcome, 'cancelled', JSON.stringify(result));
  assert.doesNotMatch(run.err(), /did not close/);
});

test('an observer-settled completion stays confirmed with its exit when every later fetch fails', T, async () => {
  const ssh = makeHungSsh({ resultCallsMax: 2 });
  const result = await finish(startDispatch({ record: (b) => ({ ...b, exit: 7 }), ssh }));
  assert.equal(result.outcome, 'confirmed', JSON.stringify(result));
  assert.equal(result.exitCode, 7);
});

test('an abort whose ssh close never arrives still returns cancelled within the post-abort bound', T, async () => {
  const ssh = makeHungSsh({ holdPipesMs: 20_000 });
  const run = startDispatch({ record: null, ssh });
  try {
    await sleep(800);
    const abortedAt = Date.now();
    run.abort.abort();
    const result = await finish(run, { expectSettled: false });
    assert.equal(result.outcome, 'cancelled', JSON.stringify(result));
    assert.ok(Date.now() - abortedAt < 8000, `cancelled only after ${Date.now() - abortedAt}ms, waiting on a close that never came`);
  } finally {
    ssh.releaseHolder();
  }
});
