import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { runSoak, soakPassed } from './lane-soak.js';
import { PG_SKIP_REASON } from './lane-db-harness.js';

/** The 30 minute run is `node tests/lane-soak.js --minutes 30`; this is the same harness for a few seconds. */
describe('lane lock-order soak harness', { skip: PG_SKIP_REASON ?? false, timeout: 120000 }, () => {
  test('eight claimers alongside completions, cancels, policy edits and a fleet tier show no deadlock, double claim or lost job', async () => {
    const r = await runSoak({ minutes: 0.15, claimers: 8, log: () => {} });
    assert.ok(r.claims > 100 && r.completed > 50 && r.cancelled > 0 && r.started > 0 && r.policyEdits > 0, `actors did too little: ${JSON.stringify(r)}`);
    assert.deepEqual({ deadlocks: r.deadlocks, doubleClaims: r.doubleClaims, lostJobs: r.lostJobs, anomalies: r.anomalies, otherErrors: r.otherErrors },
      { deadlocks: 0, doubleClaims: 0, lostJobs: 0, anomalies: 0, otherErrors: 0 }, JSON.stringify(r.samples));
    assert.ok(soakPassed(r));
  });

  test('the soak reports a deadlock when one actor locks job before group, so a clean run means something', async () => {
    const r = await runSoak({ minutes: 0.5, claimers: 8, injectBadOrder: true, log: () => {} });
    assert.ok(r.deadlocks >= 1, `expected a deadlock, got ${JSON.stringify(r)}`);
    assert.equal(soakPassed(r), false);
  });

  test('the soak reports a double claim when a re-claim does not advance the epoch', async () => {
    const r = await runSoak({ minutes: 0.1, claimers: 8, mutate: ['SET state = \'claimed\', epoch = epoch + 1,', 'SET state = \'claimed\', epoch = epoch,'], log: () => {} });
    assert.ok(r.doubleClaims > 0, `expected double claims, got ${JSON.stringify(r)}`);
    assert.equal(soakPassed(r), false);
  });

  test('the soak reports lost jobs when a claim leaves the job somewhere nobody completes it', async () => {
    const r = await runSoak({ minutes: 0.1, claimers: 8, mutate: ['SET state = \'claimed\', epoch = epoch + 1,', 'SET state = \'lost\', epoch = epoch + 1,'], log: () => {} });
    assert.ok(r.lostJobs > 0, `expected lost jobs, got ${JSON.stringify(r)}`);
    assert.equal(soakPassed(r), false);
  });
});
