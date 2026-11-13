import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { freshEnv, writeGlobalConfig, laneRun } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import { gcRemoteTickets } from '../src/gc.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';

/** BRAIN-1035 S2a (T2): every runner-side sweep sees a `<uuid>.e<N>` execution dir, not only a bare uuid. */

const DAY = 86_400_000;
const NOW = Date.now();

function setup() {
  const f = freshEnv();
  writeGlobalConfig(f.home, {});
  return { ...f, remoteRoot: makeTmpDir('gc-exec-remote-') };
}

function makeExecDir(remoteRoot, name, { ageMs = 0 } = {}) {
  const dir = path.join(remoteRoot, 'tickets', name);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'result.json'), '{}');
  const t = new Date(NOW - ageMs);
  fs.utimesSync(dir, t, t);
  return dir;
}

test('gc removes an expired uuid.e<N> ticket dir like a bare uuid, and leaves a dir that is not an execution id alone', async () => {
  const ctx = setup();
  const uuid = crypto.randomUUID();
  const old = DEFAULT_GLOBAL_CONFIG.remoteTicketRetentionMs + DAY;
  const exec = makeExecDir(ctx.remoteRoot, `${uuid}.e2`, { ageMs: old });
  const bare = makeExecDir(ctx.remoteRoot, crypto.randomUUID(), { ageMs: old });
  const stranger = makeExecDir(ctx.remoteRoot, `${uuid}.x2`, { ageMs: old });
  const fresh = makeExecDir(ctx.remoteRoot, `${crypto.randomUUID()}.e1`);
  const prev = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = ctx.home;
  try {
    await gcRemoteTickets({ remoteRoot: ctx.remoteRoot, brokerRoot: ctx.state, retentionMs: DEFAULT_GLOBAL_CONFIG.remoteTicketRetentionMs, now: NOW });
  } finally {
    if (prev === undefined) delete process.env.LANE_BROKER_HOME;
    else process.env.LANE_BROKER_HOME = prev;
  }
  assert.equal(fs.existsSync(exec), false, 'expired execution dir is swept');
  assert.equal(fs.existsSync(bare), false);
  assert.equal(fs.existsSync(stranger), true, 'not an execution id: never touched');
  assert.equal(fs.existsSync(fresh), true, 'within retention');
});

test('drain --shutdown marks an unpublished uuid.e<N> ticket dir', async () => {
  const ctx = setup();
  const execId = `${crypto.randomUUID()}.e3`;
  const dir = path.join(ctx.remoteRoot, 'tickets', execId);
  fs.mkdirSync(dir, { recursive: true });
  const out = await laneRun(['drain', '--shutdown', 'test', '--root', ctx.remoteRoot, '--wait-ms', '500'], { env: ctx.env });
  assert.equal(out.code, 0, out.stderr);
  assert.equal(fs.existsSync(path.join(dir, 'shutdown')), true, 'the shutdown sweep saw the execution dir');
  assert.match(out.stdout, /still pending|stopped|published/);
});
