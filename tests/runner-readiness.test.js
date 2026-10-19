import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { selectRunner } from '../src/remote-client.js';
import { remoteSelectOptions } from '../src/supervisor.js';
import { resolveTicketConfig, ConfigError, DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { isOnPath, readinessGap, readinessNeeds, readinessProbeArgs, parseReadinessArgs, reportReadiness, scanZeroLength } from '../src/runner-readiness.js';
import { recordReadiness, readReadiness, collectRunnerStates, renderRunnersLines, READINESS_TTL_MS } from '../src/runner-unreachable.js';
import { noRunnerRefusal, NO_RUNNER_EXIT_CODE } from '../src/resources.js';
import { tmpDir, makeRunner, setup as remoteSetup, markerCmd, detachAndWait } from './remote-harness.js';
import { paths, readJsonSafe } from '../src/state.js';
import { freshEnv, writeRepoConfig, writeGlobalConfig, laneRun } from './helpers.js';
import { loadGlobalConfig } from '../src/config.js';

/** BRAIN-623 slice 1: `requires` (executables a runner must have), its probe round trip, placement, config and status. */

/**
 * A fake ssh answering `remote-probe` per destination. `tools` maps destination -> the executables that runner "has"; it answers
 * `tools` only for names the command line asks for (via --requires), like the real runner, and answers none at all when the
 * destination is listed in `old` (a runner that predates `requires`).
 */
function makeToolSsh({ tools = {}, old = [] }) {
  const dir = tmpDir('readiness-ssh');
  const bin = path.join(dir, 'ssh');
  fs.writeFileSync(
    bin,
    `#!/usr/bin/env node
const tools = ${JSON.stringify(tools)};
const old = ${JSON.stringify(old)};
const argv = process.argv.slice(2);
const rest = [];
for (let i = 0; i < argv.length; i += 1) { if (argv[i] === '-o') { i += 1; continue; } rest.push(argv[i]); }
const dest = rest[0];
const m = /--requires ([^ ]+)/.exec((rest[1] || '').replace(/['\\\\]/g, ''));
const asked = m ? m[1].split(',') : [];
const probe = { protocol: 1, protocols: [1, 2], paused: false, queued: 0, running: 0, capacity: {}, version: 't' };
if (!old.includes(dest) && asked.length) probe.tools = Object.fromEntries(asked.map((n) => [n, (tools[dest] || []).includes(n)]));
process.stdout.write(JSON.stringify(probe) + '\\n');
fs.appendFileSync(${JSON.stringify(path.join(dir, 'calls.log'))}, (rest[1] || '').replace(/['\\\\]/g, '') + '\\n');
`.replace("#!/usr/bin/env node\n", "#!/usr/bin/env node\nconst fs = require('fs');\n"),
  );
  fs.chmodSync(bin, 0o755);
  return { bin, calls: () => (fs.existsSync(path.join(dir, 'calls.log')) ? fs.readFileSync(path.join(dir, 'calls.log'), 'utf8').split('\n').filter(Boolean) : []) };
}

const runners = (...names) => names.map((n) => makeRunner({ ssh: n }));
const NEEDS = { tools: ['rg', 'poetry'] };

test('a runner without the tool is not chosen; one with it is, whatever the config order', async () => {
  const ssh = makeToolSsh({ tools: { bare: ['poetry'], full: ['rg', 'poetry'] } });
  const res = await selectRunner(runners('bare', 'full'), { sshBin: ssh.bin, deadlineMs: 3000, readinessNeeds: NEEDS });
  assert.equal(res.runner.name, 'full');
  assert.deepEqual(res.skipped, [{ name: 'bare', reason: 'missing tool rg (required by the lane)' }]);
  assert.deepEqual(res.readiness, { bare: { check: 'tools', missing: ['rg'] } });
});

test('the submitter sends the names it needs in the probe command', async () => {
  const ssh = makeToolSsh({ tools: { a: ['rg', 'poetry'] } });
  await selectRunner(runners('a'), { sshBin: ssh.bin, deadlineMs: 3000, readinessNeeds: NEEDS });
  assert.match(ssh.calls()[0], /remote-probe --requires rg,poetry /);
});

test('an older runner that answers no tools is not ready for a lane with requires', async () => {
  const ssh = makeToolSsh({ tools: { old: ['rg'] }, old: ['old'] });
  const res = await selectRunner(runners('old'), { sshBin: ssh.bin, deadlineMs: 3000, readinessNeeds: { tools: ['rg'] } });
  assert.equal(res.runner, null);
  assert.match(res.skipped[0].reason, /does not report tools \(older lane-broker\).*rg/);
});

test('requires absent behaves exactly as today: the probe command is unchanged and an old runner is chosen', async () => {
  const ssh = makeToolSsh({ old: ['old'] });
  for (const needs of [undefined, {}, { tools: [] }, readinessNeeds(undefined)]) {
    const res = await selectRunner(runners('old'), { sshBin: ssh.bin, deadlineMs: 3000, ...(needs ? { readinessNeeds: needs } : {}) });
    assert.equal(res.runner.name, 'old');
    assert.equal('readiness' in res, false);
  }
  assert.ok(ssh.calls().every((c) => !c.includes('--requires')));
});

test('no eligible runner: the refusal (exit 76) names the missing tool and each runner checked', async () => {
  const ssh = makeToolSsh({ tools: { a: [], b: ['poetry'] } });
  const res = await selectRunner(runners('a', 'b'), { sshBin: ssh.bin, deadlineMs: 3000, readinessNeeds: NEEDS });
  assert.equal(res.runner, null);
  const reason = res.skipped.map((s) => `${s.name}: ${s.reason}`).join('; '); // as the supervisor builds it
  const refusal = noRunnerRefusal('sim', reason);
  assert.equal(refusal.exitCode, NO_RUNNER_EXIT_CODE);
  assert.equal(NO_RUNNER_EXIT_CODE, 76);
  assert.match(refusal.message, /a: missing tool rg, poetry/);
  assert.match(refusal.message, /b: missing tool rg/);
});

test('remoteSelectOptions derives the needs from the ticket payload (empty without requires)', () => {
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, runners: [] };
  const base = { id: 'x', lane: 'l', repoId: 'r', weight: 1, resources: {}, remote: {} };
  assert.deepEqual(remoteSelectOptions(base, cfg, tmpDir('opts')).readinessNeeds, { tools: [], tags: [], version: null, integrity: true });
  assert.deepEqual(remoteSelectOptions({ ...base, remote: { requires: ['rg', 'rg', 'jq'] } }, cfg, tmpDir('opts')).readinessNeeds, { tools: ['rg', 'jq'], tags: [], version: null, integrity: true });
  assert.equal(remoteSelectOptions(base, { ...cfg, runnerVersionFloor: '0.49.0' }, tmpDir('opts')).readinessNeeds.version, '0.49.0');
});

test('probe args round trip between submitter and runner', () => {
  const args = readinessProbeArgs({ tools: ['rg', 'jq'], integrity: true });
  assert.deepEqual(args, ['--requires', 'rg,jq']);
  assert.deepEqual(parseReadinessArgs(['--requires', 'rg,jq', '--root', '/x']), { tools: ['rg', 'jq'], tags: [], integrity: true });
  assert.deepEqual(readinessProbeArgs({ tools: [] }), []);
  assert.equal(readinessGap({ tools: { rg: true } }, { tools: ['rg'] }), null);
});

test('the real runner side: remote-probe --requires reports which executables are on PATH, and ignores non-names', async () => {
  const { env } = freshEnv();
  const bin = tmpDir('readiness-path');
  fs.writeFileSync(path.join(bin, 'fake-tool-xyz'), '#!/bin/sh\n', { mode: 0o755 });
  fs.writeFileSync(path.join(bin, 'not-executable-xyz'), 'x', { mode: 0o644 });
  const probe = JSON.parse((await laneRun(['remote-probe', '--requires', 'fake-tool-xyz,not-executable-xyz,absent-xyz,../evil'], { env: { ...env, PATH: `${bin}${path.delimiter}${env.PATH}` } })).stdout);
  assert.deepEqual(probe.tools, { 'fake-tool-xyz': true, 'not-executable-xyz': false, 'absent-xyz': false });
  const plain = JSON.parse((await laneRun(['remote-probe'], { env })).stdout);
  assert.equal('tools' in plain, false);
  assert.equal(isOnPath('fake-tool-xyz', bin), true);
});

test('config: requires at repo and lane level merge, dedupe, and are inherited by an undeclaredLanes template', () => {
  const { base } = freshEnv();
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, {
    version: 1,
    requires: ['rg'],
    undeclaredLanes: { as: 'shard' },
    lanes: { default: { weight: 1 }, shard: { weight: 1, requires: ['poetry', 'rg'] } },
  });
  assert.deepEqual(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }).requires, ['rg']);
  assert.deepEqual(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'shard' }).requires, ['rg', 'poetry']);
  assert.deepEqual(resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'adhoc' }).requires, ['rg', 'poetry']);
  const bare = path.join(base, 'bare');
  fs.mkdirSync(bare);
  assert.deepEqual(resolveTicketConfig({ cwd: bare, repo: 'b', lane: 'default' }).requires, []);
});

test('config: a non-array or non-string requires is a clear config error', () => {
  const { base } = freshEnv();
  const cases = [
    [{ lanes: { default: { weight: 1, requires: 'rg' } } }, /lane "default"\.requires must be an array of executable names/],
    [{ lanes: { default: { weight: 1, requires: ['rg', 3] } } }, /lane "default"\.requires must be an array of executable names/],
    [{ lanes: { default: { weight: 1, requires: ['../bin/rg'] } } }, /requires must be an array of executable names/],
    [{ requires: { rg: true }, lanes: { default: { weight: 1 } } }, /"requires" must be an array of executable names/],
    [{ requires: [''], lanes: { default: { weight: 1 } } }, /"requires" must be an array of executable names/],
  ];
  cases.forEach(([body, pattern], i) => {
    const repoDir = path.join(base, `bad${i}`);
    writeRepoConfig(repoDir, { version: 1, ...body });
    assert.throws(() => resolveTicketConfig({ cwd: repoDir, repo: 'r', lane: 'default' }), (err) => err instanceof ConfigError && pattern.test(err.message));
  });
});

test('status: the runner block names the missing tool, and a runner that passes clears it', async () => {
  const root = tmpDir('readiness-status');
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, runners: [{ name: 'skybox', ssh: 's' }, { name: 'wintop', ssh: 'w' }] };
  const now = Date.now();
  await recordReadiness(root, { skybox: { check: 'tools', missing: ['rg'] } }, [], now - 120_000);
  let lines = renderRunnersLines(collectRunnerStates(root, cfg, now), now).join('\n');
  assert.match(lines, /skybox .*not ready: missing rg \(2m ago\)/);
  assert.doesNotMatch(lines, /wintop .*not ready/);
  await recordReadiness(root, {}, ['skybox'], now);
  lines = renderRunnersLines(collectRunnerStates(root, cfg, now), now).join('\n');
  assert.doesNotMatch(lines, /not ready/);
});

/** BRAIN-623 slice 2a: the version floor and the install-integrity check. */
const gapOf = (probe, needs) => readinessGap(probe, { tools: [], version: null, integrity: true, ...needs });

test('version floor: unset is no gap; below, missing and unparsable versions are gaps; equal and above are not', () => {
  assert.equal(gapOf({ version: '0.1.0' }, {}), null);
  assert.equal(gapOf({}, {}), null);
  const gap = gapOf({ version: '0.47.0' }, { version: '0.49.0' });
  assert.deepEqual(gap, { check: 'version', reason: 'lane-broker 0.47.0 is below the floor 0.49.0', missing: ['lane-broker>=0.49.0'] });
  assert.equal(gapOf({ version: '0.49.0' }, { version: '0.49.0' }), null);
  assert.equal(gapOf({ version: '0.50.0' }, { version: '0.49.0' }), null);
  assert.equal(gapOf({ version: '1.0.0' }, { version: '0.49.0' }), null);
  assert.equal(gapOf({ version: '0.9.0' }, { version: '0.10.0' })?.check, 'version', 'numeric, not lexical');
  assert.equal(gapOf({ version: '0.49.1' }, { version: '0.49.0' }), null);
  assert.match(gapOf({}, { version: '0.49.0' }).reason, /version not reported/);
  assert.equal(gapOf({ version: 'junk' }, { version: '0.49.0' }).check, 'version');
});

test('version floor: flows from the global config into the needs, and sends no probe flag', () => {
  assert.equal(readinessNeeds({}, { runnerVersionFloor: '0.49.0' }).version, '0.49.0');
  assert.equal(readinessNeeds({}, {}).version, null);
  assert.deepEqual(readinessProbeArgs({ tools: [], version: '0.49.0', integrity: true }), []);
});

test('integrity: a non-empty zeroLength list is a gap (reason capped at 3 paths); absent or empty is not', () => {
  const gap = gapOf({ integrity: { zeroLength: ['src/a.js', 'src/b.js'] } }, {});
  assert.deepEqual(gap, { check: 'integrity', reason: 'truncated install: 2 zero-length file(s) (src/a.js, src/b.js)', missing: ['src/a.js', 'src/b.js'] });
  const many = gapOf({ integrity: { zeroLength: ['a', 'b', 'c', 'd'] } }, {});
  assert.match(many.reason, /4 zero-length file\(s\) \(a, b, c, \.\.\.\)/);
  assert.equal(gapOf({}, {}), null, 'an older runner reports no integrity: not a gap (the version floor excludes it)');
  assert.equal(gapOf({ integrity: { zeroLength: [] } }, {}), null);
  assert.equal(readinessGap({ integrity: { zeroLength: ['x'] } }, {})?.check, 'integrity', 'judged even when the submitter passed no needs');
});

test('integrity: the runner reports unasked, and the scan finds zero-length files under bin/, src/ and package.json only', () => {
  const root = tmpDir('integrity-pkg');
  for (const d of ['bin', 'src/store', 'node_modules/x', 'tests']) fs.mkdirSync(path.join(root, d), { recursive: true });
  fs.writeFileSync(path.join(root, 'package.json'), '{}');
  fs.writeFileSync(path.join(root, 'bin/lane.js'), 'ok');
  fs.writeFileSync(path.join(root, 'src/full.js'), 'ok');
  fs.writeFileSync(path.join(root, 'src/store/empty.js'), '');
  fs.writeFileSync(path.join(root, 'node_modules/x/empty.js'), '');
  fs.writeFileSync(path.join(root, 'tests/empty.js'), '');
  assert.deepEqual(scanZeroLength(root), [path.join('src', 'store', 'empty.js')]);
  fs.writeFileSync(path.join(root, 'package.json'), '');
  assert.deepEqual(scanZeroLength(root), ['package.json', path.join('src', 'store', 'empty.js')]);
  assert.deepEqual(scanZeroLength(path.join(root, 'missing')), []);
  const needs = parseReadinessArgs([]);
  assert.deepEqual(reportReadiness(needs, { packageRoot: root }).integrity.zeroLength, ['package.json', path.join('src', 'store', 'empty.js')]);
  assert.equal('version' in reportReadiness(needs, { packageRoot: root }), false);
  assert.deepEqual(scanZeroLength(new URL('..', import.meta.url).pathname), [], 'this checkout is intact');
});

test('config: runnerVersionFloor defaults to null, accepts X.Y.Z, rejects anything else', () => {
  const { home } = freshEnv();
  const load = (patch) => {
    writeGlobalConfig(home, { version: 1, ...patch });
    const prev = process.env.LANE_BROKER_HOME;
    process.env.LANE_BROKER_HOME = home;
    try {
      return loadGlobalConfig();
    } finally {
      if (prev === undefined) delete process.env.LANE_BROKER_HOME;
      else process.env.LANE_BROKER_HOME = prev;
    }
  };
  assert.equal(load({}).runnerVersionFloor, null);
  assert.equal(load({ runnerVersionFloor: '0.49.0' }).runnerVersionFloor, '0.49.0');
  for (const bad of ['0.49', 'v0.49.0', '0.49.0-rc1', 49, '', true]) assert.throws(() => load({ runnerVersionFloor: bad }), ConfigError, String(bad));
});

/** BRAIN-623 slice 2b: a stored "not ready" note clears when a placement sees the runner pass, and expires. */
test('selectRunner lists every runner that passed the readiness checks, chosen or not', async () => {
  const ssh = makeToolSsh({ tools: { a: [], b: ['rg', 'poetry'], c: ['rg', 'poetry'] } });
  const res = await selectRunner(runners('a', 'b', 'c'), { sshBin: ssh.bin, deadlineMs: 3000, readinessNeeds: NEEDS });
  assert.deepEqual(res.ready, ['b', 'c']);
  assert.deepEqual(Object.keys(res.readiness), ['a']);
  assert.deepEqual((await selectRunner(runners('a'), { sshBin: ssh.bin, deadlineMs: 3000, readinessNeeds: NEEDS })).ready, [], 'present on the no-runner return too');
});

test('a placement clears the gap of every runner that passed (not only the chosen one) and keeps the gap of one it could not probe', async () => {
  const { env, state, home, repoDir } = remoteSetup({ ssh: 'normal' });
  const cfg = JSON.parse(fs.readFileSync(path.join(home, 'config.json'), 'utf8'));
  cfg.runners.push({ ...cfg.runners[0], name: 'wintop' }, { ...cfg.runners[0], name: 'ghost', ssh: 'down' });
  fs.writeFileSync(path.join(home, 'config.json'), JSON.stringify(cfg));
  const stale = { check: 'tools', missing: ['zzz'] };
  await recordReadiness(state, { skybox: stale, wintop: stale, ghost: stale }, [], Date.now());
  const { waited } = await detachAndWait(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', ...markerCmd(path.join(tmpDir('marker'), 'where'), 0)], env, repoDir);
  assert.equal(waited.code, 0, waited.stderr);
  assert.deepEqual(Object.keys(readReadiness(state)), ['ghost'], 'skybox and wintop passed (one was chosen, one was not); ghost was never probed ready');
});

test('readiness entries expire after the TTL: not shown, pruned on the next write; a fresh one is kept', async () => {
  const root = tmpDir('readiness-ttl');
  const cfg = { ...DEFAULT_GLOBAL_CONFIG, runners: [{ name: 'skybox', ssh: 's' }, { name: 'wintop', ssh: 'w' }] };
  const now = Date.now();
  const gap = { check: 'tools', missing: ['rg'] };
  await recordReadiness(root, { skybox: gap }, [], now - READINESS_TTL_MS - 1);
  await recordReadiness(root, { wintop: gap }, [], now - READINESS_TTL_MS + 60_000);
  assert.deepEqual(Object.keys(readReadiness(root, now)), ['wintop']);
  const lines = renderRunnersLines(collectRunnerStates(root, cfg, now), now).join('\n');
  assert.doesNotMatch(lines, /skybox .*not ready/);
  assert.match(lines, /wintop .*not ready/);
  await recordReadiness(root, {}, [], now);
  assert.deepEqual(Object.keys(readJsonSafe(paths(root).runnerUnreachable).__readiness), ['wintop'], 'the expired entry is pruned from the file');
  const raw = readJsonSafe(paths(root).runnerUnreachable);
  raw.__readiness.bad = { check: 'tools', missing: ['x'] };
  fs.writeFileSync(paths(root).runnerUnreachable, JSON.stringify(raw));
  assert.deepEqual(Object.keys(readReadiness(root, now)), ['wintop'], 'an entry without a finite at is dropped');
});
