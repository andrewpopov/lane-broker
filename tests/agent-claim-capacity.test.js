import test from 'node:test';
import assert from 'node:assert/strict';
import { claimCapacity } from '../src/agent/claims.js';
import { claimCpuArgs } from '../src/agent/execution.js';
import { newExecutionRecord } from '../src/agent/store.js';

const GIB = 2 ** 30;
const record = (claim, subphase = 'RUNNING') => ({ subphase, claim });
const reserving = { head_reserve: true };

test('capacity is the free figures plus what this runner\'s own live claims hold, CPU capped at the budget', () => {
  const records = [
    record({ cpuCores: 6, header: { cpuCores: 8, memoryBytes: 4 * GIB } }),
    record({ header: { cpuCores: 2, memoryBytes: GIB } }),
    record({ cpuCores: 4, header: { cpuCores: 4, memoryBytes: 8 * GIB } }, 'DONE'),
  ];
  assert.deepEqual(claimCapacity({ freeCpu: 1, freeMem: 2 * GIB, budgetCpu: 12 }, records, reserving), { cpu: 9, mem: 7 * GIB });
  assert.equal(claimCapacity({ freeCpu: 6, freeMem: 0, budgetCpu: 12 }, records, reserving).cpu, 12);
});

test('a queue that does not offer the reservation (v8, no head_reserve) gets no capacity, so the claim keeps its old arguments', () => {
  const offered = { freeCpu: 4, freeMem: GIB, budgetCpu: 12 };
  assert.equal(claimCapacity(offered, [], { host: 'wintop' }), null);
  assert.equal(claimCapacity(offered, [], null), null);
  assert.equal(claimCapacity({ freeCpu: 4, freeMem: GIB }, [], reserving), null);
});

test('the queue grant reaches the record and the remote-exec argv, and a claim answered without one passes no flag', () => {
  const answer = { id: '11111111-2222-3333-4444-555555555555', epoch: 1, token: '11111111-2222-3333-4444-666666666666', repo_key: 'r', lane: 'ci', lane_key: 'k',
    binding: {}, header: { cpuCores: 8 }, snapshot: {} };
  const granted = newExecutionRecord({ ...answer, cpu_cores: 6 });
  assert.equal(granted.claim.cpuCores, 6);
  assert.deepEqual(claimCpuArgs(granted.claim), ['--claim-cpu-cores', '6']);
  const old = newExecutionRecord(answer);
  assert.equal('cpuCores' in old.claim, false);
  assert.deepEqual(claimCpuArgs(old.claim), []);
});
