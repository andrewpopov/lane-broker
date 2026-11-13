import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { recordingFetch, happens, scenarioSuite } from './agent-run-harness.js';
import { agentPaths } from '../src/agent/store.js';
import { sha256Hex } from '../src/s3-presign.js';

const ARTIFACT_BYTES = Buffer.from('artifact bytes\n');
const storeArtifacts = (dir) => {
  fs.mkdirSync(path.join(dir, 'files'), { recursive: true });
  fs.writeFileSync(path.join(dir, 'files', 'out.txt'), ARTIFACT_BYTES);
  fs.writeFileSync(path.join(dir, 'manifest.json'), JSON.stringify({ files: [{ path: 'out.txt', size: ARTIFACT_BYTES.length, sha256: sha256Hex(ARTIFACT_BYTES) }] }));
};

const scenarios = {
  'a pulled ticket runs end to end, and its outputs are PUT under the host prefix the queue and the runner both derive': async (w, rw, load) => {
    const ticket = await rw.submit({ code: "process.stdout.write('hello from the lane\\n')" });
    const agent = await rw.agent({ load });
    const row = await rw.claimed(ticket);
    const execId = rw.execId(row);
    await rw.untilGone(agent, execId);
    const final = await w.ticket(ticket.id);
    assert.equal(final.state, 'finished');
    assert.equal(final.result.kind, 'completed');
    const key = `lane-out/a1/${ticket.id}/e${row.epoch}/log`;
    assert.deepEqual([...rw.s3.objects.keys()].filter((k) => k.startsWith('lane-out/')), [key], 'the log is the only output, under the runner host prefix');
    const log = rw.s3.objects.get(key).body;
    assert.match(log.toString(), /hello from the lane/);
    assert.equal(final.result.logSha256, sha256Hex(log));
    assert.equal(final.result.logBytes, log.length);
    const puts = rw.s3.requests.filter((r) => r.method === 'PUT').map((r) => r.path);
    assert.deepEqual(puts, [`/lane-out/a1/${ticket.id}/e${row.epoch}/log`]);
    const names = agent.names();
    assert.deepEqual(names.filter((n) => ['ld_start', 'ld_finish'].includes(n)), ['ld_start', 'ld_finish']);
    assert.ok(names.indexOf('ld_claim') < names.indexOf('ld_start'), 'claimed before starting');
    assert.equal(names.includes('ld_log'), false);
    const p = agentPaths(rw.root);
    assert.deepEqual([fs.readdirSync(p.spool), fs.readdirSync(p.out), fs.readdirSync(p.executions)], [[], [], []]);
    await happens('no claim is in flight', () => !fs.existsSync(p.pendingClaim), 3000);
  },

  'stored artifacts are spooled to a temp file and PUT under the same prefix, with their hash and size in the finish': async (w, rw, load) => {
    const { uploadOutputs, outputKey } = await load('agent/upload.js');
    const { createS3Client } = await load('s3-client.js');
    const dir = makeTmpDir('upload-');
    fs.writeFileSync(path.join(dir, 'log'), 'the log\n');
    storeArtifacts(path.join(dir, 'artifacts'));
    const s3 = createS3Client({ endpoint: rw.rustfs.endpoint, region: rw.rustfs.region, creds: rw.rustfs.creds });
    const record = { id: '11111111-1111-4111-8111-111111111111', epoch: 3 };
    const out = await uploadOutputs({ s3, bucket: 'lane-out', host: 'a1', record, logFile: path.join(dir, 'log'), artifactsDir: path.join(dir, 'artifacts'), artifactsFile: path.join(dir, 'spool') });
    const stored = rw.s3.objects.get(`lane-out/a1/${record.id}/e3/artifacts`).body;
    assert.deepEqual([out.artifactsSha256, out.artifactsBytes], [sha256Hex(stored), stored.length]);
    assert.match(stored.toString(), /artifact bytes/);
    assert.equal(fs.readFileSync(path.join(dir, 'spool')).equals(stored), true, 'the PUT body is the spooled file');
    assert.equal(outputKey('a1', record.id, 3, 'log'), `a1/${record.id}/e3/log`);
  },

  'an upload that never answers is cut off at the upload budget with uploadError, and uploadSince is on disk before the first PUT is answered': async (w, rw, load) => {
    rw.s3.hangMatching = (p) => p.startsWith('/lane-out/');
    const ticket = await rw.submit({ code: 'process.exit(0)' });
    const agent = await rw.agent({ load, timing: { uploadRetryMs: 1500 } });
    const row = await rw.claimed(ticket);
    const execId = rw.execId(row);
    await happens('uploadSince is persisted while the first PUT hangs', () => agent.core.table.get(execId)?.subphase === 'SETTLING' && typeof agent.core.table.get(execId)?.uploadSince === 'number', 4000);
    await happens('the ticket finishes with uploadError within the budget', async () => (await w.ticket(ticket.id)).state === 'finished', 1500 + 3000);
    assert.ok((await w.ticket(ticket.id)).result.uploadError);
  },

  'a log PUT that keeps failing finishes with uploadError once uploadRetryMs has passed, and still finishes the ticket': async (w, rw, load) => {
    const fetchImpl = recordingFetch((url, init) => (init?.method === 'PUT' ? new Response('no', { status: 500 }) : undefined));
    const ticket = await rw.submit({ code: 'process.exit(0)' });
    const agent = await rw.agent({ load, fetchImpl, timing: { uploadRetryMs: 600 } });
    const row = await rw.claimed(ticket);
    await happens('the ticket is finished', async () => (await w.ticket(ticket.id)).state === 'finished', 8000);
    const final = await w.ticket(ticket.id);
    assert.match(final.result.uploadError, /PUT lane-out\/a1\/.*answered 500/);
    assert.equal(final.result.logSha256, undefined);
    assert.ok(fetchImpl.count('PUT', '/log') >= 2, 'it retried before giving up');
    await rw.untilGone(agent, rw.execId(row));
  },
};

const UPLOAD = 'agent/upload.js';
const canaries = [
  ['an upload that never answers is cut off at the upload budget with uploadError, and uploadSince is on disk before the first PUT is answered', 'agent/execution.js', ["if (record.uploadSince === undefined) this.#cas(execId, ['SETTLING'], { uploadSince: since });", '']],
  ['an upload that never answers is cut off at the upload budget with uploadError, and uploadSince is on disk before the first PUT is answered', UPLOAD, ['{ signal, deadlineMs: Math.max(1, remainingMs()) }', '{ signal }']],
  ['a pulled ticket runs end to end, and its outputs are PUT under the host prefix the queue and the runner both derive', UPLOAD, ['`${host}/${ticketId}/e${epoch}/${name}`', '`${ticketId}/e${epoch}/${name}`']],
  ['stored artifacts are spooled to a temp file and PUT under the same prefix, with their hash and size in the finish', UPLOAD, ['await put(\'artifacts\', artifactsFile)', 'await put(\'artifacts\', artifactsFile.replace(/spool$/, "none"))'], /ENOENT/],
  ['a log PUT that keeps failing finishes with uploadError once uploadRetryMs has passed, and still finishes the ticket', 'agent/execution.js', ['if (remainingMs() <= 0) uploadError = err.message;', 'if (false) uploadError = err.message;']],
];

scenarioSuite('agent outputs and uploads on a disposable Postgres', { scenarios, canaries });
