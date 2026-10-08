import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { freshEnv, writeGlobalConfig, writeRepoConfig, gitFixture } from './helpers.js';
import { runCommand } from '../src/run.js';
import { atomicWriteJson } from '../src/state.js';

// BRAIN-453: `lane run` waits for the supervisor's output tail BEFORE it unpipes stdout/stderr. Returning the drain promise
// from inside the try ran the finally (unpipe) first, so any tail still in flight lost its destination.

test('output that arrives after the result is written is still forwarded before runCommand returns', { timeout: 20_000 }, async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  gitFixture(['init', '-q'], repoDir);

  const stdout = new PassThrough();
  const spawnSupervisor = (execPath, args, opts) => {
    const ticket = JSON.parse(Buffer.from(opts.env.LANE_BROKER_TICKET, 'base64').toString('utf8'));
    atomicWriteJson(ticket.resultPath, { id: ticket.id, exit: 0, signal: null, startedAt: Date.now(), endedAt: Date.now(), waitedMs: 0 });
    setTimeout(() => stdout.write('late-tail\n'), 300);
    setTimeout(() => stdout.end(), 450);
    return Object.assign(new EventEmitter(), { pid: process.pid, unref() {}, stdout, stderr: new PassThrough().end() });
  };

  const names = ['LANE_BROKER_HOME', 'LANE_BROKER_STATE', 'LANE_BROKER_LEASE', 'LANE_BROKER_KEY', 'LANE_BROKER_LOCAL', 'LANE_BROKER_PRIORITY'];
  const prev = Object.fromEntries(names.map((n) => [n, process.env[n]]));
  for (const n of names) delete process.env[n];
  process.env.LANE_BROKER_HOME = env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_STATE = env.LANE_BROKER_STATE;
  let seen = '';
  const origWrite = process.stdout.write.bind(process.stdout);
  process.stdout.write = (chunk, ...rest) => {
    seen += chunk;
    return true;
  };
  try {
    const result = await runCommand({ repo: 'r', lane: 'default', cmd: ['true'], cwd: repoDir, spawnSupervisor });
    assert.equal(result.exitCode, 0);
  } finally {
    process.stdout.write = origWrite;
    for (const n of names) {
      if (prev[n] === undefined) delete process.env[n];
      else process.env[n] = prev[n];
    }
  }
  stdout.destroy();
  assert.match(seen, /late-tail/);
});
