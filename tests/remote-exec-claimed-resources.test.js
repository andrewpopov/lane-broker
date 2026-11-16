import test from 'node:test';
import assert from 'node:assert/strict';
import { claimedResources } from '../src/remote-runner.js';

test('a queue grant below the declaration becomes the claim, a floor at or above it is dropped, and anything else changes nothing', () => {
  assert.deepEqual(claimedResources({ cpuCores: 8, minCpuCores: 4 }, 6), { cpuCores: 6, minCpuCores: 4 });
  assert.deepEqual(claimedResources({ cpuCores: 8, minCpuCores: 4 }, 4), { cpuCores: 4, minCpuCores: undefined });
  assert.deepEqual(claimedResources({ cpuCores: 8 }, 6), { cpuCores: 6, minCpuCores: undefined });
  for (const grant of [undefined, 8, 9, 0, -1, Number.NaN]) assert.deepEqual(claimedResources({ cpuCores: 8, minCpuCores: 4 }, grant), {}, String(grant));
});
