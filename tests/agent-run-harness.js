import assert from 'node:assert/strict';
import { describe, test, before, after, afterEach } from 'node:test';
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import crypto, { randomUUID } from 'node:crypto';
import { BIN, sleep, waitFor } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import { startS3Stub, STUB_CREDS, STUB_REGION } from './helpers/s3-stub.js';
import { setupRunner, makeHeader } from './helpers/remote-exec-fixture.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { World, call, logins, startDispatchCluster } from './lane-dispatch-fixture.js';
import { agentPg, closeAgents } from './agent-harness.js';
import { pgSkipOption } from './lane-db-harness.js';
import { encodeSnapshot } from '../src/remote-stream.js';
import { manifestHashOf } from '../src/remote-manifest.js';
import { blobNamespace, isCacheable } from '../src/remote-blobs.js';
import { executionIdOf, paths, readJsonSafe } from '../src/state.js';

export { sleep, waitFor };
export const SLEEP = 'setInterval(() => {}, 1000)';
export const REPO_IDENTITY = '/fake/repo/.git';
export const HOST = 'a1';

export const FAST_TIMING = {
  tickMs: 50, heartbeatMs: 300, killRetryMs: 200, errorRetryMs: 200, uploadRetryMs: 2500, backoff: { minMs: 20, maxMs: 100 },
};

const sha256Of = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex');

async function collect(readable) {
  const chunks = [];
  for await (const chunk of readable) chunks.push(chunk);
  return Buffer.concat(chunks);
}

/** A snapshot stream as the submitter would encode it: gzipped, with every cacheable file as a body-less cached frame. */
async function buildSnapshot({ id, code, files, headerOverrides }) {
  const dir = makeTmpDir('agent-src-');
  const entries = files.map(({ path: rel, bytes }) => {
    fs.writeFileSync(path.join(dir, rel), bytes);
    return { path: rel, type: 'file', exec: false, size: bytes.length, sha256: sha256Of(bytes) };
  });
  const header = makeHeader(code, { ticketId: id, repoIdentity: REPO_IDENTITY, ...headerOverrides });
  const cached = new Set(entries.filter(isCacheable).map((e) => e.path));
  const gz = zlib.gzipSync(await collect(encodeSnapshot(dir, header, entries, undefined, { cached })));
  const blobs = entries.filter(isCacheable).map((e) => ({ ns: blobNamespace(REPO_IDENTITY), sha256: e.sha256, size: e.size, bytes: files.find((f) => f.path === e.path).bytes }));
  const binding = { protocol: header.protocol ?? 1, ticketId: id, generation: header.generation, manifestHash: manifestHashOf(entries) };
  return { header, gz, blobs, binding };
}

/**
 * A runner with its own broker state and remote root, a fake S3 that verifies signatures (with the readable `lane-snap/.probe` an admin
 * writes once), and the submitter-side enqueue a pull agent needs answered: a ticket whose stream is really in the store.
 */
export class RunnerWorld {
  agents = [];
  logs = [];

  static async create(w, { runnerConfig = {} } = {}) {
    const ctx = setupRunner({}, { remoteBlobCache: true, ...runnerConfig });
    const s3 = await startS3Stub();
    s3.objects.set('lane-snap/.probe', { body: Buffer.from('probe'), lastModified: Date.now() });
    const world = new RunnerWorld(w, ctx, s3);
    world.saved = { state: process.env.LANE_BROKER_STATE, home: process.env.LANE_BROKER_HOME };
    process.env.LANE_BROKER_STATE = ctx.state;
    process.env.LANE_BROKER_HOME = ctx.home;
    return world;
  }

  constructor(w, ctx, s3) {
    Object.assign(this, { w, ctx, s3 });
  }

  get root() {
    return this.ctx.root;
  }

  get rustfs() {
    return { endpoint: this.s3.endpoint, region: STUB_REGION, snapBucket: 'lane-snap', outBucket: 'lane-out', creds: STUB_CREDS };
  }

  async submit({ code = 'process.exit(0)', files = [{ path: 'a.txt', bytes: Buffer.from('x') }], headerOverrides = {}, repo = 'repoA' } = {}) {
    const id = randomUUID();
    const { header, gz, blobs, binding } = await buildSnapshot({ id, code, files, headerOverrides });
    const streamKey = `streams/${id}-${binding.generation}-${sha256Of(gz)}.gz`;
    this.s3.objects.set(`lane-snap/${streamKey}`, { body: gz, lastModified: Date.now() });
    for (const blob of blobs) this.s3.objects.set(`lane-snap/blobs/${blob.ns}/${blob.sha256}`, { body: blob.bytes, lastModified: Date.now() });
    const { manifest: _manifest, ...rowHeader } = header;
    const snapshot = { key: streamKey, sha256: sha256Of(gz), bytes: gz.length, blobs: blobs.map(({ ns, sha256, size }) => ({ ns, sha256, size })) };
    await call(await this.w.as(logins.s1), 'ld_enqueue', id, repo, 'default', `lk-${id}`, binding, rowHeader, snapshot, [], 1, 1000, 0);
    return { id, header, gz, blobs, binding, snapshot, streamKey };
  }

  /** The agent under `load` (the real tree, or a mutated copy: its S3 client and error classes must come from the same tree). */
  async agent({ load = realSrc, config = {}, timing = {}, fetchImpl, s3, Driver, coreOptions, login, ...rest } = {}) {
    const calls = [];
    const { startAgent } = await load('agent/run.js');
    const { createS3Client } = await load('s3-client.js');
    const rustfs = { ...this.rustfs, ...config.rustfs };
    const agent = await startAgent({
      remoteRoot: this.root, version: 'test', figures: () => ({ freeCpu: 8, freeMem: 4e9 }), spawnEnv: this.ctx.env, laneBin: BIN, log: (line) => this.logs.push(line),
      config: { schedule: null, pollMs: 100, stopClaimingBeforeCloseMs: 0, speedFactor: 1, runnerName: HOST, brokerHome: this.ctx.home, stateDir: this.ctx.state, fetchBudgetMs: 20_000, ...config, pg: agentPg(this.w, login), rustfs },
      s3: s3 ?? createS3Client({ endpoint: rustfs.endpoint, region: rustfs.region, creds: rustfs.creds, ...(fetchImpl ? { fetchImpl } : {}) }),
      timing: { ...FAST_TIMING, ...timing },
      coreOptions: { onCall: (name, args) => calls.push([name, args, Date.now()]), backoff: { minMs: 20, maxMs: 100 }, ...coreOptions },
      ...(Driver ? { Driver } : {}),
      ...rest,
    });
    this.agents.push(agent);
    return Object.assign(agent, { calls, names: () => calls.map(([name]) => name), callsTo: (name) => calls.filter(([n]) => n === name).map(([, args]) => args), timesOf: (...names) => calls.filter(([n]) => names.includes(n)).map(([, , at]) => at) });
  }

  execId(row) {
    return executionIdOf(row.id, row.epoch);
  }

  ticketDir(execId) {
    return path.join(this.root, 'tickets', execId);
  }

  /** The row once the queue has handed `ticket` to the agent. */
  claimed(ticket) {
    return waitFor(async () => {
      const row = await this.w.ticket(ticket.id);
      return row.epoch > 0 ? row : null;
    }, { timeoutMs: 20_000, intervalMs: 25 }).catch(() => assert.fail(`ticket ${ticket.id} was never claimed; log ${this.logs.join(' | ')}`));
  }

  async untilGone(agent, execId, timeoutMs = 30_000) {
    await waitFor(() => !agent.core.table.get(execId), { timeoutMs, intervalMs: 25 }).catch((err) => {
      assert.fail(`${err.message}: record ${JSON.stringify(agent.core.table.get(execId))}; calls ${agent.names().join(',')}; log ${this.logs.join(' | ')}`);
    });
  }

  /** Kill what the agents spawned (detached remote-exec groups) and the broker's leased supervisors and commands. */
  killLeftovers() {
    const kill = (pid, signal) => { try { process.kill(pid, signal); } catch { /* gone */ } };
    for (const agent of this.agents) for (const r of agent.core.table.all()) if (r.pid) kill(-r.pid, 'SIGKILL');
    const leases = paths(this.ctx.state).leases;
    for (const name of fs.existsSync(leases) ? fs.readdirSync(leases) : []) {
      const lease = readJsonSafe(path.join(leases, name));
      for (const [pid, signal] of [[lease?.supervisorPid, 'SIGCONT'], [lease?.childPgid && -lease.childPgid, 'SIGKILL'], [lease?.supervisorPid, 'SIGKILL']]) if (pid) kill(pid, signal);
    }
  }

  async close() {
    this.killLeftovers();
    await Promise.allSettled(this.agents.splice(0).map((a) => a.stop()));
    await this.s3.close();
    for (const [key, name] of [['state', 'LANE_BROKER_STATE'], ['home', 'LANE_BROKER_HOME']]) {
      if (this.saved[key] === undefined) delete process.env[name];
      else process.env[name] = this.saved[key];
    }
  }
}

/** A fetch for `fetchImpl` that records every request and lets `intercept(url, init, count)` answer some of them (a Response), else passes through. */
export function recordingFetch(intercept = async () => undefined) {
  const requests = [];
  const wrapped = async (url, init) => {
    const method = init?.method ?? 'GET';
    requests.push({ url: String(url), method });
    const count = requests.filter((r) => r.url === String(url) && r.method === method).length;
    return (await intercept(String(url), init, count)) ?? fetch(url, init);
  };
  return Object.assign(wrapped, { requests, count: (method, includes) => requests.filter((r) => r.method === method && r.url.includes(includes)).length });
}

export const cutShort = () => new Response(new ReadableStream({ start(c) { c.enqueue(new Uint8Array(16)); c.error(new Error('terminated')); } }), { status: 200 });

export const happens = (what, check, timeoutMs = 15_000) => waitFor(check, { timeoutMs, intervalMs: 25 }).then(() => true, () => false).then((ok) => assert.ok(ok, what));

/**
 * One describe of scenarios `(w, rw, load)` on a disposable Postgres. Each canary `[scenario name, file under src/, [from, to], expected?]` reruns
 * its scenario against a copy of the tree with that one guard removed, and the scenario must fail on its own assertion.
 */
export function scenarioSuite(title, { timeout = 300_000, scenarios, canaries = [] }) {
  describe(title, { skip: pgSkipOption, timeout }, () => {
    let cluster;
    let rw;
    before(async () => { cluster = await startDispatchCluster(); });
    after(async () => { await cluster?.closeClients(); cluster?.stop(); });
    afterEach(async () => { await rw?.close(); await closeAgents(); await cluster.closeClients(); });
    const world = async () => {
      const w = await World.create(cluster);
      rw = await RunnerWorld.create(w);
      return [w, rw];
    };
    for (const [name, scenario] of Object.entries(scenarios)) test(name, async () => scenario(...await world(), realSrc));
    for (const [name, file, mutation, expected] of canaries) {
      test(`canary (${file}): with the guard removed, "${name}" fails on its own assertion`, async (t) => {
        const failure = await assertCanaryFails(file, mutation, async (load) => scenarios[name](...await world(), load), expected);
        t.diagnostic(`failed as expected: ${failure.message.split('\n')[0]}`);
      });
    }
  });
}
