import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { gitFixture, laneRun, writeGlobalConfig, writeRepoConfig } from './helpers.js';
import { setup, detachAndWait, resultOf, tmpDir } from './remote-harness.js';
import { paths } from '../src/state.js';

/**
 * BRAIN-398: remoteArtifacts end to end -- the REAL `lane run` CLI over the fake-ssh transport (tests/remote-harness.js).
 * The command writes its files into its own cwd, which is the runner's work dir when it runs remotely and the
 * submitter's worktree when it runs locally.
 */

const STAMP = 'artifacts/test-lane/default-latest.json';

/** argv writing `files` (rel -> content) under cwd, then exiting `exitCode`. */
function writerCmd(files, exitCode = 0) {
  const body = `
const fs = require('fs'); const path = require('path');
for (const [rel, content] of Object.entries(${JSON.stringify(files)})) {
  fs.mkdirSync(path.dirname(rel), { recursive: true });
  fs.writeFileSync(rel, content);
}
process.exit(${exitCode});
`;
  return [process.execPath, '-e', body];
}

function configure(repoDir, lane) {
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1, remote: true, ...lane } } });
}

async function runLane(env, repoDir, cmd, extraArgs = []) {
  return detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', ...extraArgs, '--', ...cmd], env, repoDir);
}

const admissionLog = (state) => fs.readFileSync(paths(state).admissionLog, 'utf8');

test('a declared file comes back on success: written into the submitter worktree, logged with its name, recorded in the result, released on the runner', async () => {
  const { env, state, repoDir, runnerRoot } = setup();
  configure(repoDir, { remoteArtifacts: [STAMP] });
  const { id, waited } = await runLane(env, repoDir, writerCmd({ [STAMP]: '{"ok":true}' }));
  assert.equal(waited.code, 0, waited.stderr);
  assert.equal(fs.readFileSync(path.join(repoDir, STAMP), 'utf8'), '{"ok":true}');
  assert.match(admissionLog(state), new RegExp(`remote artifacts: 1 file\\(s\\) returned: ${STAMP}`));
  const result = resultOf(state, id);
  assert.equal(result.executor, 'remote');
  assert.deepEqual(result.remoteArtifacts, [STAMP]);
  assert.equal(result.remoteArtifactsWarning, undefined);
  const ticketDirs = fs.readdirSync(path.join(runnerRoot, 'tickets'));
  assert.equal(ticketDirs.length, 1);
  assert.equal(fs.existsSync(path.join(runnerRoot, 'tickets', ticketDirs[0], 'artifacts')), false, 'the runner copies are deleted once fetched');
});

test('under the default policy a failing command returns nothing, and its exit code passes through', async () => {
  const { env, state, repoDir } = setup();
  configure(repoDir, { remoteArtifacts: [STAMP] });
  const { id, waited } = await runLane(env, repoDir, writerCmd({ [STAMP]: '{"ok":false}' }, 3));
  assert.equal(waited.code, 3, waited.stderr);
  assert.equal(fs.existsSync(path.join(repoDir, STAMP)), false);
  assert.equal(resultOf(state, id).remoteArtifacts, undefined);
});

test('remoteArtifactsOn "always" returns the files even when the command fails, and the exit code is still the command\'s', async () => {
  const { env, repoDir } = setup();
  configure(repoDir, { remoteArtifacts: [STAMP], remoteArtifactsOn: 'always' });
  const { waited } = await runLane(env, repoDir, writerCmd({ [STAMP]: '{"ok":false}' }, 3));
  assert.equal(waited.code, 3, waited.stderr);
  assert.equal(fs.readFileSync(path.join(repoDir, STAMP), 'utf8'), '{"ok":false}');
});

test('a glob returns every match', async () => {
  const { env, state, repoDir } = setup();
  configure(repoDir, { remoteArtifacts: ['reports/*.json'] });
  const { id, waited } = await runLane(env, repoDir, writerCmd({ 'reports/a.json': 'A', 'reports/b.json': 'B', 'reports/c.txt': 'C' }));
  assert.equal(waited.code, 0, waited.stderr);
  assert.equal(fs.readFileSync(path.join(repoDir, 'reports/a.json'), 'utf8'), 'A');
  assert.equal(fs.readFileSync(path.join(repoDir, 'reports/b.json'), 'utf8'), 'B');
  assert.equal(fs.existsSync(path.join(repoDir, 'reports/c.txt')), false);
  assert.deepEqual(resultOf(state, id).remoteArtifacts, ['reports/a.json', 'reports/b.json']);
});

test('an artifact failure on the runner (cap breached) is a warning: the lane still exits 0 and nothing is written', async () => {
  const { env, state, repoDir, runnerHome } = setup();
  writeGlobalConfig(runnerHome, { sampleMs: 50, capacity: 4, remoteArtifactMaxFileBytes: 4 });
  configure(repoDir, { remoteArtifacts: [STAMP] });
  const { id, waited } = await runLane(env, repoDir, writerCmd({ [STAMP]: 'much longer than four bytes' }));
  assert.equal(waited.code, 0, waited.stderr);
  assert.equal(fs.existsSync(path.join(repoDir, STAMP)), false);
  const result = resultOf(state, id);
  assert.equal(result.exit, 0);
  assert.match(result.remoteArtifactsWarning, /per-file cap/);
  assert.match(admissionLog(state), /warning: remote artifacts: .*per-file cap/);
});

test('an artifact the submitter cannot accept (its own cap) is a warning too, and the exit code is unaffected', async () => {
  const { env, state, home, repoDir } = setup();
  const cfg = JSON.parse(fs.readFileSync(path.join(home, 'config.json'), 'utf8'));
  writeGlobalConfig(home, { ...cfg, remoteArtifactMaxTotalBytes: 3 });
  configure(repoDir, { remoteArtifacts: [STAMP] });
  const { id, waited } = await runLane(env, repoDir, writerCmd({ [STAMP]: 'more than three bytes' }));
  assert.equal(waited.code, 0, waited.stderr);
  assert.equal(fs.existsSync(path.join(repoDir, STAMP)), false);
  assert.match(resultOf(state, id).remoteArtifactsWarning, /total cap/);
});

test('a declared file the command never produced is a warning, not a failure', async () => {
  const { env, state, repoDir } = setup();
  configure(repoDir, { remoteArtifacts: [STAMP] });
  const { id, waited } = await runLane(env, repoDir, writerCmd({}));
  assert.equal(waited.code, 0, waited.stderr);
  assert.match(resultOf(state, id).remoteArtifactsWarning, new RegExp(`declared but not produced: ${STAMP}`));
});

test('a git-tracked file is never overwritten, whether declared by glob or by exact path; the lane still exits 0 with a warning', async () => {
  const { env, state, repoDir } = setup();
  fs.mkdirSync(path.join(repoDir, 'out'));
  fs.writeFileSync(path.join(repoDir, 'out/tracked.json'), 'committed');
  configure(repoDir, { remoteArtifacts: ['out/*.json'] });
  gitFixture(['add', '-A'], repoDir);
  gitFixture(['commit', '-q', '-m', 'x'], repoDir);

  for (const patterns of [['out/*.json'], ['out/tracked.json']]) {
    configure(repoDir, { remoteArtifacts: patterns });
    const run = await runLane(env, repoDir, writerCmd({ 'out/tracked.json': 'from-runner' }));
    assert.equal(run.waited.code, 0, run.waited.stderr);
    assert.equal(fs.readFileSync(path.join(repoDir, 'out/tracked.json'), 'utf8'), 'committed');
    assert.match(resultOf(state, run.id).remoteArtifactsWarning, /git tracks/);
  }
});

test('the runner deletes its stored copies even when the submitter refuses what it was sent', async () => {
  const { env, home, repoDir, runnerRoot } = setup();
  const cfg = JSON.parse(fs.readFileSync(path.join(home, 'config.json'), 'utf8'));
  writeGlobalConfig(home, { ...cfg, remoteArtifactMaxTotalBytes: 3 });
  configure(repoDir, { remoteArtifacts: [STAMP] });
  const { waited } = await runLane(env, repoDir, writerCmd({ [STAMP]: 'more than three bytes' }));
  assert.equal(waited.code, 0, waited.stderr);
  const [ticket] = fs.readdirSync(path.join(runnerRoot, 'tickets'));
  assert.equal(fs.existsSync(path.join(runnerRoot, 'tickets', ticket, 'artifacts')), false);
});

test('nothing is stored on the runner for a run that produced no artifacts', async () => {
  const { env, repoDir, runnerRoot } = setup();
  configure(repoDir, { remoteArtifacts: ['reports/*.json'] });
  const { waited } = await runLane(env, repoDir, writerCmd({}));
  assert.equal(waited.code, 0, waited.stderr);
  const [ticket] = fs.readdirSync(path.join(runnerRoot, 'tickets'));
  assert.equal(fs.existsSync(path.join(runnerRoot, 'tickets', ticket, 'artifacts')), false);
});

test('a command that makes the runner\'s artifact directory undeletable cannot change the result: the true exit is published and the submitter does not fall back', async () => {
  const { env, state, repoDir } = setup();
  configure(repoDir, { remoteArtifacts: [STAMP], remoteArtifactsOn: 'always' });
  // `..` of the work dir is the ticket dir, where the runner stores artifacts: leave a read-only dir with a file in it.
  const body = `
const fs = require('fs');
fs.mkdirSync('../artifacts');
fs.writeFileSync('../artifacts/blocker', 'x');
fs.chmodSync('../artifacts', 0o555);
fs.mkdirSync('artifacts/test-lane', { recursive: true });
fs.writeFileSync(${JSON.stringify(STAMP)}, 'x');
process.exit(3);
`;
  const { id, waited } = await runLane(env, repoDir, [process.execPath, '-e', body]);
  assert.equal(waited.code, 3, waited.stderr);
  const result = resultOf(state, id);
  assert.equal(result.executor, 'remote', 'a published result means no local fallback');
  assert.equal(result.exit, 3);
  assert.match(result.remoteArtifactsWarning, /could not collect artifacts/);
  assert.equal(fs.existsSync(path.join(repoDir, STAMP)), false);
});

test('a runner prunes artifacts older than a day on every probe, not only on artifact jobs', async () => {
  const { env, runnerRoot } = setup();
  const stale = path.join(runnerRoot, 'tickets', 'old-ticket', 'artifacts');
  fs.mkdirSync(stale, { recursive: true });
  const old = (Date.now() - 25 * 60 * 60_000) / 1000;
  fs.utimesSync(stale, old, old);
  const res = await laneRun(['remote-probe', '--root', runnerRoot], { env });
  assert.equal(res.code, 0, res.stderr);
  assert.equal(fs.existsSync(stale), false);
});

test('a runner without artifacts/1 runs the command, warns, and returns nothing', async () => {
  const { env, state, repoDir } = setup({ ssh: 'no-artifacts' });
  configure(repoDir, { remoteArtifacts: [STAMP] });
  const marker = path.join(tmpDir('marker'), 'where');
  const body = `require('fs').writeFileSync(${JSON.stringify(marker)}, process.env.LANE_FAKE_RUNNER === '1' ? 'remote' : 'local');`;
  const { id, waited } = await runLane(env, repoDir, [process.execPath, '-e', body]);
  assert.equal(waited.code, 0, waited.stderr);
  assert.equal(fs.readFileSync(marker, 'utf8'), 'remote');
  assert.equal(fs.existsSync(path.join(repoDir, STAMP)), false);
  assert.match(resultOf(state, id).remoteArtifactsWarning, /does not support artifacts\/1/);
  assert.match(admissionLog(state), /warning: remote artifacts: runner skybox does not support artifacts\/1/);
});

test('a local run ignores remoteArtifacts: the command\'s own file is simply there, with no artifact line or warning', async () => {
  const { env, state, repoDir } = setup();
  configure(repoDir, { remoteArtifacts: [STAMP] });
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--local', '--', ...writerCmd({ [STAMP]: 'local' })], { env, cwd: repoDir });
  assert.equal(result.code, 0, result.stderr);
  assert.equal(fs.readFileSync(path.join(repoDir, STAMP), 'utf8'), 'local');
  assert.doesNotMatch(result.stderr, /remote artifacts/);
  const log = fs.existsSync(paths(state).admissionLog) ? admissionLog(state) : '';
  assert.doesNotMatch(log, /remote artifacts/);
});
