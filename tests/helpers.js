import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export const BIN = fileURLToPath(new URL('../bin/lane.js', import.meta.url));

let counter = 0;

/** Fresh, isolated $LANE_BROKER_HOME / $LANE_BROKER_STATE for one test. */
export function freshEnv(extra = {}) {
  counter += 1;
  const base = fs.mkdtempSync(path.join(os.tmpdir(), `lane-broker-test-${counter}-`));
  const home = path.join(base, 'home');
  const state = path.join(base, 'state');
  fs.mkdirSync(home, { recursive: true });
  fs.mkdirSync(state, { recursive: true });
  return {
    base,
    home,
    state,
    env: {
      ...process.env,
      LANE_BROKER_HOME: home,
      LANE_BROKER_STATE: state,
      ...extra,
    },
  };
}

export function writeGlobalConfig(home, cfg) {
  fs.writeFileSync(path.join(home, 'config.json'), JSON.stringify(cfg, null, 2));
}

export function writeRepoConfig(dir, cfg) {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, '.lane-broker.json'), JSON.stringify(cfg, null, 2));
}

export function writeLoadFile(base, value) {
  const file = path.join(base, 'load');
  fs.writeFileSync(file, String(value));
  return file;
}

/** Spawn `lane <args>` and resolve with { code, stdout, stderr } on exit. */
export function laneRun(args, { env, cwd } = {}) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [BIN, ...args], { env, cwd: cwd || process.cwd() });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => {
      stdout += d;
    });
    child.stderr.on('data', (d) => {
      stderr += d;
    });
    child.on('exit', (code) => resolve({ code, stdout, stderr }));
  });
}

/** Spawn `lane <args>` without waiting — caller controls lifecycle (signals, timing). */
export function laneSpawn(args, { env, cwd } = {}) {
  return spawn(process.execPath, [BIN, ...args], { env, cwd: cwd || process.cwd() });
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function waitFor(predicate, { timeoutMs = 5000, intervalMs = 20 } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await predicate();
    if (v) return v;
    if (Date.now() > deadline) throw new Error('waitFor: timed out');
    await sleep(intervalMs);
  }
}
