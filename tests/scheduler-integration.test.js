import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneRun } from './helpers.js';

function markerScript(mark, viol, sleepSecs) {
  // Records a violation if it finds another run's marker still present.
  return [
    'sh',
    '-c',
    `if [ -e "${mark}" ]; then echo VIOLATION >> "${viol}"; fi; : > "${mark}"; sleep ${sleepSecs}; rm -f "${mark}"`,
  ];
}

test('two runs on the same key serialize: the second waits until the first releases', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const mark = path.join(base, 'marker');
  const viol = path.join(base, 'violations');

  const [r1, r2] = await Promise.all([
    laneRun(['run', '--repo', 'same', '--lane', 'default', '--', ...markerScript(mark, viol, 0.4)], { env, cwd: repoDir }),
    laneRun(['run', '--repo', 'same', '--lane', 'default', '--', ...markerScript(mark, viol, 0.4)], { env, cwd: repoDir }),
  ]);

  assert.equal(r1.code, 0);
  assert.equal(r2.code, 0);
  assert.equal(fs.existsSync(viol), false, 'the second run must never see the first run\'s marker still present');
});

test('capacity holds: two keys whose combined weight exceeds capacity do not run concurrently', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 2, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2 }, lint: { weight: 2 } } });

  const mark = path.join(base, 'marker');
  const viol = path.join(base, 'violations');

  const [r1, r2] = await Promise.all([
    laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...markerScript(mark, viol, 0.4)], { env, cwd: repoDir }),
    laneRun(['run', '--repo', 'r', '--lane', 'lint', '--', ...markerScript(mark, viol, 0.4)], { env, cwd: repoDir }),
  ]);

  assert.equal(r1.code, 0);
  assert.equal(r2.code, 0);
  assert.equal(fs.existsSync(viol), false, 'combined weight 4 > capacity 2 must serialize');
});

test('capacity allows: two different keys whose combined weight fits capacity run concurrently', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 3, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 2 }, lint: { weight: 1 } } });

  const startFile = path.join(base, 'starts');
  const script = (name) => ['sh', '-c', `echo ${name} $(date +%s%N) >> "${startFile}"; sleep 0.3`];

  const [r1, r2] = await Promise.all([
    laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...script('a')], { env, cwd: repoDir }),
    laneRun(['run', '--repo', 'r', '--lane', 'lint', '--', ...script('b')], { env, cwd: repoDir }),
  ]);

  assert.equal(r1.code, 0);
  assert.equal(r2.code, 0);
  const lines = fs.readFileSync(startFile, 'utf8').trim().split('\n');
  assert.equal(lines.length, 2, 'both should have started (not one skipped)');
});

test('conflicting rouge-style default + sim lanes never overlap even though their keys differ', async () => {
  const { base, home, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    lanes: { default: { weight: 1 }, sim: { weight: 1 } },
    conflicts: [['sim', '*']],
  });

  const mark = path.join(base, 'marker');
  const viol = path.join(base, 'violations');

  const [r1, r2] = await Promise.all([
    laneRun(['run', '--repo', 'rouge', '--lane', 'default', '--', ...markerScript(mark, viol, 0.4)], { env, cwd: repoDir }),
    laneRun(['run', '--repo', 'rouge', '--lane', 'sim', '--allow-local-sim', '--', ...markerScript(mark, viol, 0.4)], { env, cwd: repoDir }),
  ]);

  assert.equal(r1.code, 0);
  assert.equal(r2.code, 0);
  assert.equal(fs.existsSync(viol), false, 'default and sim declared as conflicting must never run at the same time');
});
