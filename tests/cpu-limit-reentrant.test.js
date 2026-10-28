import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, gitFixture } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import { writeLease, LEASE_STATE } from '../src/lease.js';
import { bootId } from '../src/state.js';
import { resolveTicketConfig } from '../src/config.js';
import { runCommand } from '../src/run.js';

/** BRAIN-695: a nested `lane run` reusing an inherited lease runs inside the parent's scope; it must never open a second one. */

test('a reentrant nested run spawns its command directly: no second systemd-run scope, no supervisor', async (t) => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, cpuLimit: 'require', loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });
  gitFixture(['init', '-q'], repoDir);
  const ownKey = resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }).key;
  writeLease(state, { id: 'parent', key: ownKey, bootId: bootId(), supervisorPid: process.pid, supervisorStart: null, childPgid: null, heartbeatAt: Date.now(), weight: 1, state: LEASE_STATE.RUNNING, resources: { cpuCores: 2, memoryBytes: 1024 ** 3 } });

  const bin = makeTmpDir('lane-broker-reentrant-bin-', t);
  const seen = path.join(bin, 'systemd-run.seen');
  fs.writeFileSync(path.join(bin, 'systemd-run'), `#!/bin/sh\necho "$@" >> "${seen}"\nwhile [ "$1" != "--" ]; do shift; done\nshift\nexec "$@"\n`, { mode: 0o755 });

  const names = ['LANE_BROKER_HOME', 'LANE_BROKER_STATE', 'LANE_BROKER_LEASE', 'LANE_BROKER_KEY', 'LANE_BROKER_LOCAL', 'LANE_BROKER_PRIORITY', 'PATH'];
  const prev = Object.fromEntries(names.map((n) => [n, process.env[n]]));
  Object.assign(process.env, { LANE_BROKER_HOME: env.LANE_BROKER_HOME, LANE_BROKER_STATE: env.LANE_BROKER_STATE, LANE_BROKER_LEASE: 'parent', LANE_BROKER_KEY: ownKey, PATH: `${bin}:${process.env.PATH}` });
  let supervisorSpawned = false;
  try {
    const result = await runCommand({ repo: 'r', lane: 'default', cmd: ['true'], cwd: repoDir, spawnSupervisor: () => { supervisorSpawned = true; throw new Error('a reentrant run must not spawn a supervisor'); } });
    assert.equal(result.exitCode, 0);
  } finally {
    for (const n of names) {
      if (prev[n] === undefined) delete process.env[n];
      else process.env[n] = prev[n];
    }
  }
  assert.equal(supervisorSpawned, false);
  assert.equal(fs.existsSync(seen), false, 'systemd-run was invoked for a nested run under an inherited lease');
});
