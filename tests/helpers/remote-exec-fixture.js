import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { freshEnv, writeGlobalConfig, BIN, waitFor } from '../helpers.js';
import { encodeSnapshot } from '../../src/remote-stream.js';
import { paths, readJsonSafe } from '../../src/state.js';
import { makeTmpDir } from './tmp.js';

/** A runner (state + remote root) driven by real `lane remote-exec` processes, shared by the BRAIN-1035 runner-side tests. */

const spawned = [];

/** Kill what a test left running: leased supervisors and their groups, then the remote-exec children. Call from `afterEach`. */
export function reapSpawned() {
  for (const { child, env } of spawned.splice(0)) {
    if (child.exitCode !== null || child.signalCode !== null) continue;
    let names = [];
    try {
      names = fs.readdirSync(paths(env.LANE_BROKER_STATE).leases);
    } catch {
      // none
    }
    for (const n of names) {
      const lease = readJsonSafe(path.join(paths(env.LANE_BROKER_STATE).leases, n));
      for (const [pid, sig] of [[lease?.supervisorPid, 'SIGCONT'], [lease?.childPgid && -lease.childPgid, 'SIGKILL'], [lease?.supervisorPid, 'SIGKILL']]) {
        if (!pid) continue;
        try {
          process.kill(pid, sig);
        } catch {
          // gone
        }
      }
    }
    child.kill('SIGKILL');
  }
}

export function setupRunner(extraEnv = {}, globalConfig = {}) {
  const f = freshEnv({ LANE_BROKER_TEST_CANCEL_GRACE_MS: '1000', ...extraEnv });
  writeGlobalConfig(f.home, globalConfig);
  return { ...f, root: makeTmpDir('exec-root-') };
}

export function makeHeader(argv, overrides = {}) {
  return { ticketId: crypto.randomUUID(), generation: 0, repoKey: 'exec-test-repo', lane: 'default', argv: [process.execPath, '-e', argv], relCwd: '', ...overrides };
}

/** Run `lane remote-exec --root <root> <extraArgs>` with `header`'s snapshot on stdin. Resolves `closed` with `{code, stderr}`. */
export function execRemote(ctx, header, { extraArgs = [], env = ctx.env } = {}) {
  const dir = makeTmpDir('exec-src-');
  const buf = Buffer.from('x');
  fs.writeFileSync(path.join(dir, 'a.txt'), buf);
  const entries = [{ path: 'a.txt', type: 'file', exec: false, size: 1, sha256: crypto.createHash('sha256').update(buf).digest('hex') }];
  const child = spawn(process.execPath, [BIN, 'remote-exec', '--root', ctx.root, ...extraArgs], { env });
  let stderr = '';
  child.stdout.on('data', () => {});
  child.stderr.on('data', (d) => {
    stderr += d;
  });
  encodeSnapshot(dir, header, entries).pipe(child.stdin);
  spawned.push({ child, env });
  const closed = new Promise((resolve) => child.on('close', (code) => resolve({ code, stderr })));
  return { child, closed };
}

export const ticketPath = (ctx, execId, name) => path.join(ctx.root, 'tickets', execId, name);
export const readExecResult = (ctx, execId) => readJsonSafe(ticketPath(ctx, execId, 'result.json'));
export const hasLease = (ctx) => fs.existsSync(paths(ctx.state).leases) && fs.readdirSync(paths(ctx.state).leases).some((n) => n.endsWith('.json'));
export const leaseOf = (ctx) => {
  const name = fs.readdirSync(paths(ctx.state).leases).find((n) => n.endsWith('.json'));
  return readJsonSafe(path.join(paths(ctx.state).leases, name));
};
export const whenRunning = (ctx, execId) => waitFor(() => fs.existsSync(ticketPath(ctx, execId, 'remote-id')) && hasLease(ctx), { timeoutMs: 30_000 });
export const SLEEP = 'setInterval(() => {}, 1000)';
