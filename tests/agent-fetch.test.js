import assert from 'node:assert/strict';
import fs from 'node:fs';
import { REPO_IDENTITY, recordingFetch, cutShort, happens, scenarioSuite } from './agent-run-harness.js';
import { blobNamespace, hasBlob } from '../src/remote-blobs.js';

const BIG = Buffer.alloc(20_000, 'b');
const READS_BIG = `process.exit(require('fs').readFileSync('big.bin').length === ${BIG.length} ? 0 : 3)`;
const oneOffer = () => { let given = false; return () => (given++ ? null : { freeCpu: 8, freeMem: 4e9 }); };
const snap = (t) => `lane-snap/${t.streamKey}`;

async function runToEnd(w, rw, load, { fetchImpl, tamper, ticketOptions, config } = {}) {
  const ticket = await rw.submit(ticketOptions);
  await tamper?.(ticket);
  const agent = await rw.agent({ load, fetchImpl, config, figures: oneOffer() });
  const row = await rw.claimed(ticket);
  const execId = rw.execId(row);
  return { ticket, agent, row, execId, finish: async () => { await rw.untilGone(agent, execId); return w.ticket(ticket.id); } };
}

const neverSpawned = (rw, run) => assert.deepEqual([fs.existsSync(rw.ticketDir(run.execId)), run.agent.names().includes('ld_start')], [false, false], 'nothing was started or spawned');

const rejectedWith = async (run, reason) => {
  const final = await run.finish();
  assert.equal(final.state, 'finished');
  assert.equal(final.result.kind, 'rejected');
  assert.match(final.result.reason, reason);
  assert.equal(final.result.neverStarted, true);
  assert.equal(run.agent.names().includes('ld_release'), false, 'a store verdict is a finish, never a release');
};

const releasedNeverStarted = async (w, rw, run) => {
  await happens('the claim was released', () => run.agent.names().includes('ld_release'), 8000);
  assert.deepEqual(run.agent.callsTo('ld_release').map((a) => a[3]), ['never-started']);
  await happens('the row is queued again', async () => (await w.ticket(run.ticket.id)).state === 'queued', 8000);
  assert.equal(run.agent.names().includes('ld_finish'), false);
  neverSpawned(rw, run);
};

const scenarios = {
  'a stream the store does not have (404) finishes rejected and releases nothing': async (w, rw, load) => {
    const run = await runToEnd(w, rw, load, { tamper: (t) => rw.s3.objects.delete(snap(t)) });
    await rejectedWith(run, /^stream-missing/);
    neverSpawned(rw, run);
  },

  'a 403 for a missing key is a 404 while the readable probe object still reads': async (w, rw, load) => {
    rw.s3.missingAs403 = true;
    const run = await runToEnd(w, rw, load, { tamper: (t) => rw.s3.objects.delete(snap(t)) });
    await rejectedWith(run, /^stream-missing/);
  },

  'a 403 with the probe unreadable is a store problem: retried for the budget, then released never-started': async (w, rw, load) => {
    rw.s3.missingAs403 = true;
    const run = await runToEnd(w, rw, load, { config: { fetchBudgetMs: 800 }, tamper: (t) => { rw.s3.objects.delete(snap(t)); rw.s3.objects.delete('lane-snap/.probe'); } });
    await releasedNeverStarted(w, rw, run);
  },

  'a stream that arrives whole with the wrong sha256 finishes rejected and nothing is spawned': async (w, rw, load) => {
    const run = await runToEnd(w, rw, load, { tamper: (t) => { const body = Buffer.from(rw.s3.objects.get(snap(t)).body); body[body.length - 20] ^= 0xff; rw.s3.objects.set(snap(t), { body, lastModified: Date.now() }); } });
    await rejectedWith(run, /^stream-corrupt/);
    neverSpawned(rw, run);
  },

  'a stream that is not the ticket the queue bound finishes rejected with the mismatched field': async (w, rw, load) => {
    const run = await runToEnd(w, rw, load, { tamper: async (t) => (await w.su()).query("UPDATE lane_dispatch.tickets SET binding = jsonb_set(binding, '{manifestHash}', '\"deadbeef\"') WHERE id = $1", [t.id]) });
    await rejectedWith(run, /^binding-mismatch: manifestHash/);
    neverSpawned(rw, run);
  },

  'a blob the store does not have finishes rejected cache-miss, with no release': async (w, rw, load) => {
    const run = await runToEnd(w, rw, load, { ticketOptions: { files: [{ path: 'big.bin', bytes: BIG }] }, tamper: (t) => rw.s3.objects.delete(`lane-snap/blobs/${t.blobs[0].ns}/${t.blobs[0].sha256}`) });
    await rejectedWith(run, /^cache-miss: 1$/);
    neverSpawned(rw, run);
  },

  'a blob whose bytes do not hash to the manifest finishes rejected cache-corrupt at once, with no release': async (w, rw, load) => {
    const fetchImpl = recordingFetch();
    const run = await runToEnd(w, rw, load, { fetchImpl, ticketOptions: { files: [{ path: 'big.bin', bytes: BIG }] }, tamper: (t) => rw.s3.objects.set(`lane-snap/blobs/${t.blobs[0].ns}/${t.blobs[0].sha256}`, { body: Buffer.alloc(BIG.length, 'X'), lastModified: Date.now() }) });
    await rejectedWith(run, /^cache-corrupt: 1$/);
    assert.equal(fetchImpl.count('GET', '/lane-snap/blobs/'), 1, 'a mismatch is not retried');
    neverSpawned(rw, run);
  },

  'a store that is down is retried with heartbeats for the fetch budget, then the claim is released never-started': async (w, rw, load) => {
    const run = await runToEnd(w, rw, load, { config: { fetchBudgetMs: 1500 }, tamper: () => rw.s3.close() });
    await releasedNeverStarted(w, rw, run);
    const beats = run.agent.names().filter((n) => n === 'ld_heartbeat').length;
    assert.ok(beats >= 2, `heartbeats continued while the fetch retried (saw ${beats})`);
  },

  'a stream cut short in transit is retried and the lane then runs to completion': async (w, rw, load) => {
    const fetchImpl = recordingFetch((url, _init, count) => (url.includes('/lane-snap/streams/') && count === 1 ? cutShort() : undefined));
    const run = await runToEnd(w, rw, load, { fetchImpl });
    const final = await run.finish();
    assert.equal(fetchImpl.count('GET', '/lane-snap/streams/'), 2, 'one cut-short GET and one retry');
    assert.deepEqual([final.result.kind, final.result.exit], ['completed', 0]);
  },

  'a missing blob is fetched, verified and published to the runner cache, and the lane reads it through the cached frame': async (w, rw, load) => {
    const run = await runToEnd(w, rw, load, { ticketOptions: { code: READS_BIG, files: [{ path: 'big.bin', bytes: BIG }] } });
    const final = await run.finish();
    assert.deepEqual([final.result.kind, final.result.exit], ['completed', 0]);
    assert.equal(hasBlob(rw.root, blobNamespace(REPO_IDENTITY), run.ticket.blobs[0].sha256, BIG.length), 'hit');
  },
};

const FETCH = 'agent/fetch.js';
const EXEC = 'agent/execution.js';
const REJECT = ["if (err instanceof VerdictError) this.#rejected(record, ['FETCHING'], err.message);", 'if (err instanceof VerdictError) this.#releasing(record);'];
const canaries = [
  ['a stream the store does not have (404) finishes rejected and releases nothing', FETCH, ['if (!got.exists) throw new VerdictError(`stream-missing: ${key}`);', '']],
  ['a stream the store does not have (404) finishes rejected and releases nothing', EXEC, REJECT],
  ['a 403 with the probe unreadable is a store problem: retried for the budget, then released never-started', FETCH, ['if (err.status === 403 && await probeReads(s3, bucket, dest, signal)) return', 'if (err.status === 403) return']],
  ['a stream that arrives whole with the wrong sha256 finishes rejected and nothing is spawned', FETCH, ['if (got.size !== bytes || got.sha256 !== sha256) {', 'if (false) {']],
  ['a stream that is not the ticket the queue bound finishes rejected with the mismatched field', FETCH, ['return bad ?', 'return null && bad ?']],
  ['a blob the store does not have finishes rejected cache-miss, with no release', FETCH, ["if (!got.exists) throw new VerdictError('cache-miss: 1');", '']],
  ['a blob the store does not have finishes rejected cache-miss, with no release', EXEC, REJECT],
  ['a blob whose bytes do not hash to the manifest finishes rejected cache-corrupt at once, with no release', FETCH, ["if (got.size !== blob.size || got.sha256 !== blob.sha256) throw new VerdictError('cache-corrupt: 1');", '']],
  ['a store that is down is retried with heartbeats for the fetch budget, then the claim is released never-started', EXEC, ['if (this.now() - record.claimedAt >= this.config.fetchBudgetMs) throw new FetchBudgetError(err.message);', '']],
];

scenarioSuite('agent snapshot fetch and verification on a disposable Postgres', { scenarios, canaries });
