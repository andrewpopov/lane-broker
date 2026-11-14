import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { readAttempt, supervisorAlive } from '../../src/attempts.js';
import { laneRun, laneSpawn, writeGlobalConfig } from '../helpers.js';
import { logins } from '../lane-dispatch-fixture.js';
import { setup } from '../remote-harness.js';
import { CA_TEXT, QUEUE_URL } from './queue-fixture.js';
import { createS3Client } from '../../src/s3-client.js';
import { startS3Stub, STUB_CREDS, STUB_REGION } from './s3-stub.js';
import { StubRunner } from './stub-runner.js';
import { makeTmpDir } from './tmp.js';

const PRELOAD = fileURLToPath(new URL('./queue-redirect-preload.js', import.meta.url));
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
export const PULL_RUNNER = { name: 'pullbox', ssh: 'pullbox', shell: 'sh -c', transport: 'pull' };

async function until(fn, what, { timeoutMs = 40_000 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  for (;;) {
    const value = await fn().catch((err) => { lastError = err; return null; });
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}${lastError ? `: ${lastError.message}` : ''}`);
    await sleep(100);
  }
}

// a real `lane run` supervisor whose queue connections land on the harness database and whose object store is an in-process stub
export async function pullE2E(t, w, { load, pullRunner = true, pushRunner = false, localPaused = false, ssh, globalExtra = {}, lanePull = {}, queueFail = '' } = {}) {
  const stub = await startS3Stub();
  t.after(() => stub.close());
  const secrets = makeTmpDir('pull-e2e-secrets-', t);
  const file = (name, text) => {
    fs.writeFileSync(path.join(secrets, name), text, { mode: 0o600 });
    return path.join(secrets, name);
  };
  const rustfs = { endpoint: stub.endpoint, credentialsEnvFile: file('rustfs.env', `AWS_ACCESS_KEY_ID=${STUB_CREDS.accessKeyId}\nAWS_SECRET_ACCESS_KEY=${STUB_CREDS.secretAccessKey}\n`) };
  const base = setup({
    ...(ssh === undefined ? {} : { ssh }),
    globalExtra: {
      laneQueueUrl: QUEUE_URL,
      lanePull: { enabled: true, pollMs: 100, passwordFile: file('password', 'pw\n'), caFile: file('ca.pem', CA_TEXT), rustfs, ...lanePull },
      ...globalExtra,
    },
  });
  const config = JSON.parse(fs.readFileSync(path.join(base.home, 'config.json'), 'utf8'));
  config.runners = [...(pullRunner ? [PULL_RUNNER] : []), ...(pushRunner ? config.runners : [])];
  writeGlobalConfig(base.home, config);
  const pause = path.join(base.state, 'PAUSE');
  if (localPaused) fs.writeFileSync(pause, 'the local queue is closed for the test');
  const calls = path.join(secrets, 'queue-calls');
  const exits = path.join(secrets, 'exits');
  const env = {
    ...base.env,
    NODE_OPTIONS: `--import=${PRELOAD}`,
    LANE_TEST_QUEUE_CONN: JSON.stringify(w.cluster.conn(w.db, logins.s1)),
    LANE_TEST_QUEUE_CALLS: calls,
    LANE_TEST_QUEUE_FAIL: queueFail,
    LANE_TEST_EXIT_LOG: exits,
  };
  const runner = new StubRunner(w, { login: logins.a2, s3: createS3Client({ endpoint: stub.endpoint, region: STUB_REGION, creds: STUB_CREDS }), tmpDir: makeTmpDir('pull-e2e-runner-', t) });
  const ran = path.join(secrets, 'ran-locally');
  const markLocalRun = ['node', '-e', `require('fs').appendFileSync(${JSON.stringify(ran)}, 'ran\\n')`];
  const lane = (args) => laneRun(args, { env, cwd: base.repoDir, bin: load.laneBin });
  const fx = {
    ...base,
    env,
    stub,
    runner,
    markLocalRun,
    ranLocally: () => fs.existsSync(ran),
    supervisorExits: () => (fs.existsSync(exits) ? fs.readFileSync(exits, 'utf8').split('\n').filter((line) => line.includes('supervisor.js')).map((line) => Number(line.split(' ').at(-1))) : []),
    unpauseLocal: () => fs.rmSync(pause, { force: true }),
    queueCalls: () => (fs.existsSync(calls) ? fs.readFileSync(calls, 'utf8').split('\n').filter(Boolean).length : 0),
    lane,
    // a foreground `lane run` the test can signal; killed with the test so a failing canary leaves nothing behind
    spawnRun(cmd = ['true']) {
      const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--', ...cmd], { env, cwd: base.repoDir, bin: load.laneBin });
      let stderr = '';
      child.stdout.resume();
      child.stderr.on('data', (chunk) => { stderr += chunk; });
      t.after(() => child.kill('SIGKILL'));
      return { child, stderr: () => stderr, exited: new Promise((resolve) => child.on('close', (code) => resolve(code))) };
    },
    run: (cmd = ['true'], extra = []) => lane(['run', '--repo', 'r', '--lane', 'default', ...extra, '--', ...cmd]),
    async detach(cmd = ['true'], extra = []) {
      const started = await lane(['run', '--repo', 'r', '--lane', 'default', '--detach', ...extra, '--', ...cmd]);
      if (started.code !== 0) throw new Error(`lane run --detach failed: ${started.stderr}`);
      const id = started.stdout.trim();
      t.after(() => lane(['cancel', id]));
      return id;
    },
    wait: (id, timeout = '60s') => lane(['wait', id, '--timeout', timeout]),
    ticket: async (id) => (await (await w.su()).query('SELECT * FROM lane_dispatch.tickets WHERE id = $1', [id])).rows[0],
    async enqueued(known = []) {
      return until(async () => (await (await w.su()).query('SELECT id FROM lane_dispatch.tickets WHERE id <> ALL($1::uuid[]) ORDER BY created_at LIMIT 1', [known])).rows[0]?.id, 'the ticket to reach the queue');
    },
    async killSupervisor(id) {
      process.kill(readAttempt(base.state, id).supervisor.pid, 'SIGKILL');
      await until(async () => !supervisorAlive(readAttempt(base.state, id)), 'the supervisor to die');
    },
    async claim() {
      await until(async () => runner.claim({ mem: 4e9 }).then(() => true), 'a claimable ticket');
    },
    async run1({ exit = 0, log = 'ran on the stub\n' } = {}) {
      await fx.claim();
      await runner.start();
      await runner.finish({ exit, ...(await runner.putOutputs({ log })) });
    },
    attempt: (id) => readAttempt(base.state, id),
  };
  return fx;
}
