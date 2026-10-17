import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { selectRunner } from '../src/remote-client.js';
import { remoteSelectOptions } from '../src/supervisor.js';
import { resolveTicketConfig, ConfigError, DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { isOnPath, readinessGap, readinessNeeds, readinessProbeArgs, parseReadinessArgs } from '../src/runner-readiness.js';
import { recordReadiness, collectRunnerStates, renderRunnersLines } from '../src/runner-unreachable.js';
import { noRunnerRefusal, NO_RUNNER_EXIT_CODE } from '../src/resources.js';
import { tmpDir, makeRunner } from './remote-harness.js';
import { freshEnv, writeRepoConfig, laneRun } from './helpers.js';

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
  assert.deepEqual(remoteSelectOptions(base, cfg, tmpDir('opts')).readinessNeeds, { tools: [] });
  assert.deepEqual(remoteSelectOptions({ ...base, remote: { requires: ['rg', 'rg', 'jq'] } }, cfg, tmpDir('opts')).readinessNeeds, { tools: ['rg', 'jq'] });
});

test('probe args round trip between submitter and runner', () => {
  const args = readinessProbeArgs({ tools: ['rg', 'jq'] });
  assert.deepEqual(args, ['--requires', 'rg,jq']);
  assert.deepEqual(parseReadinessArgs(['--requires', 'rg,jq', '--root', '/x']), { tools: ['rg', 'jq'] });
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
