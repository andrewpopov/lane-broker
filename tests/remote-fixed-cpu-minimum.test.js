import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { writeRepoConfig, laneRun, gitFixture } from './helpers.js';
import { setup, markerCmd, readCapturedHeader } from './remote-harness.js';
import { remoteMinCpuCores } from '../src/run.js';
import { resolveTicketResources } from '../src/resources.js';

/** BRAIN-675: a fixed CPU minimum (floor == claim) is explicit intent and must reach the runner; an absent floor sends nothing. */

test('remoteMinCpuCores: fixed floor is carried as the claim, elastic as itself, absent as nothing', () => {
  const res = (cpuCores, minCpuCores) => resolveTicketResources({ weight: 1, cpuCores, minCpuCores, memoryBytes: 1 });
  assert.equal(remoteMinCpuCores(res(6, 6), 6), 6);
  assert.equal(remoteMinCpuCores(res(6, 2), 2), 2);
  assert.equal(remoteMinCpuCores(res(6, undefined), undefined), undefined);
  assert.equal(remoteMinCpuCores(res(6, null), null), undefined);
});

async function dispatch(lane) {
  const ctx = setup({ ssh: 'tee' });
  writeRepoConfig(ctx.repoDir, { version: 1, lanes: { default: { weight: 1, memoryBytes: 1048576, remote: true, remotePolicy: 'remote-first', ...lane } } });
  gitFixture(['add', '-A'], ctx.repoDir);
  gitFixture(['commit', '-q', '-m', 'x'], ctx.repoDir);
  const result = await laneRun(['run', '--repo', 'r', '--lane', 'default', '--', ...markerCmd(`${ctx.repoDir}/../marker-${Date.now()}`, 0)], { env: ctx.env, cwd: ctx.repoDir });
  assert.ok(fs.existsSync(ctx.teePath), result.stderr);
  return readCapturedHeader(ctx.teePath);
}

test('end to end: a lane with cpuCores 2 / minCpuCores 2 sends the fixed minimum', async () => {
  const header = await dispatch({ cpuCores: 2, minCpuCores: 2 });
  assert.equal(header.minCpuCores, 2);
});

test('end to end: a lane without a floor sends no minCpuCores (legacy header unchanged)', async () => {
  const header = await dispatch({ cpuCores: 2 });
  assert.equal('minCpuCores' in header, false);
});
