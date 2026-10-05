import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { freshEnv } from './helpers.js';
import { paths } from '../src/state.js';

const stateJsPath = fileURLToPath(new URL('../src/state.js', import.meta.url));

const WORKERS = 6;
const ITERATIONS = 50;
// Default sweep is 4 rounds (24 spawned processes) rather than the original
// 10 (60 spawned processes): under host contention each round costs 20-40s,
// so 10 rounds could exceed a 180s budget on a loaded box. Override with
// LANE_BROKER_TEST_ROUNDS for a fuller sweep (e.g. `LANE_BROKER_TEST_ROUNDS=10
// npm test`) — the MUTATION test below is unaffected and remains the
// sensitivity proof that this suite still catches the bug it guards.
const ROUNDS = Number(process.env.LANE_BROKER_TEST_ROUNDS) || 4;

function workerSource(stateJsUrl, root, resultFile, iterations = ITERATIONS, rendezvous = false) {
  return `
import { withLock } from ${JSON.stringify(stateJsUrl)};
import fs from 'node:fs';
import path from 'node:path';

const root = ${JSON.stringify(root)};
const marker = ${JSON.stringify(path.join(root, '..', 'contention-marker'))};
let overlaps = 0;
let escaped = 0;
for (let i = 0; i < ${iterations}; i++) {
  try {
    await withLock(root, async () => {
      if (fs.existsSync(marker)) overlaps += 1;
      fs.writeFileSync(marker, String(process.pid));
      if (${rendezvous}) {
        // Hold the critical section until BOTH contenders have entered it, so
        // an admitted second holder is observed as an overlap every run.
        const dir = path.dirname(marker);
        fs.writeFileSync(path.join(dir, 'entered-' + process.pid), '');
        const stop = Date.now() + 10000;
        while (fs.readdirSync(dir).filter((f) => f.startsWith('entered-')).length < 2 && Date.now() < stop) {
          await new Promise((r) => setTimeout(r, 1));
        }
      }
      await new Promise((r) => setTimeout(r, 5));
      fs.unlinkSync(marker);
    });
  } catch (err) {
    escaped += 1;
    process.stderr.write('ESCAPED: ' + ((err && err.stack) || err) + '\\n');
  }
}
fs.writeFileSync(${JSON.stringify(resultFile)}, JSON.stringify({ overlaps, escaped }));
`;
}

/** Spawn a child that exits immediately, and return its (now-dead) pid. */
async function spawnDeadPid() {
  const child = spawn(process.execPath, ['-e', 'process.exit(0)']);
  const pid = child.pid;
  await new Promise((resolve) => child.on('exit', resolve));
  return pid;
}

/** Plant a dead lock: a lock dir whose owner.json names a confirmed-dead pid. */
function plantDeadLock(state, deadPid, token) {
  const lockDir = paths(state).lock;
  fs.mkdirSync(lockDir, { recursive: true });
  fs.writeFileSync(
    path.join(lockDir, 'owner.json'),
    JSON.stringify({ pid: deadPid, token, start: null, at: Date.now() }),
  );
}

async function runRound({ base, state }, stateJsUrl, deadPid, round, { workers = WORKERS, iterations = ITERATIONS, rendezvous = false } = {}) {
  plantDeadLock(state, deadPid, `dead-${round}`);

  const runs = [];
  for (let w = 0; w < workers; w += 1) {
    const scriptFile = path.join(base, `worker-${round}-${w}.mjs`);
    const resultFile = path.join(base, `result-${round}-${w}.json`);
    fs.writeFileSync(scriptFile, workerSource(stateJsUrl, state, resultFile, iterations, rendezvous));
    runs.push({ scriptFile, resultFile });
  }

  const children = runs.map(({ scriptFile }) =>
    new Promise((resolve) => {
      const child = spawn(process.execPath, [scriptFile], { stdio: ['ignore', 'ignore', 'pipe'] });
      let stderr = '';
      child.stderr.on('data', (d) => {
        stderr += d;
      });
      child.on('exit', (code) => resolve({ code, stderr }));
    }),
  );

  const results = await Promise.all(children);
  for (const r of results) {
    assert.equal(r.code, 0, `worker must exit 0; stderr: ${r.stderr}`);
  }

  let overlaps = 0;
  let escaped = 0;
  for (const { resultFile } of runs) {
    const r = JSON.parse(fs.readFileSync(resultFile, 'utf8'));
    overlaps += r.overlaps;
    escaped += r.escaped;
  }
  return { overlaps, escaped };
}

test('dead-owner lock recovery: many contenders observing the same dead owner never double-admit', async () => {
  const env = freshEnv();
  const deadPid = await spawnDeadPid();
  const stateJsUrl = pathToFileURL(stateJsPath).href;

  let totalOverlaps = 0;
  let totalEscaped = 0;
  for (let round = 0; round < ROUNDS; round += 1) {
    const { overlaps, escaped } = await runRound(env, stateJsUrl, deadPid, round);
    totalOverlaps += overlaps;
    totalEscaped += escaped;
  }

  assert.equal(totalEscaped, 0, `expected zero escaped errors across ${ROUNDS} rounds`);
  assert.equal(totalOverlaps, 0, `expected zero overlaps across ${ROUNDS} rounds of a freshly-planted dead lock`);
});

test('MUTATION: the pre-fix blind rmSync reintroduces overlap on the same dead-owner scenario', async () => {
  // Recreate the pre-fix behaviour: swap the atomic-takeover call for the
  // blind, unconditional rmSync it replaced. This proves the new guard in
  // state.js is actually load-bearing, not decoration.
  // The race is driven explicitly, not by timing: both contenders observe the
  // same dead owner and meet at a barrier (files in the shared parent dir).
  // The leader (lowest pid) removes the dead lock and acquires; only once
  // that live owner is visible does the follower perform its stale blind rm,
  // which deletes the leader's LIVE lock. The follower then acquires while
  // the leader is still inside its critical section (the worker holds it
  // until both have entered), so the overlap occurs on every run.
  const source = fs.readFileSync(stateJsPath, 'utf8');
  const guarded = 'const won = recoverDeadLock(root, lockDir, ownerFile, owner);';
  assert.ok(source.includes(guarded), 'expected the atomic-takeover call site in state.js; test needs updating');
  const mutated = source.replace(
    guarded,
    `const gate = path.join(root, '..');
      fs.writeFileSync(path.join(gate, 'observed-' + process.pid), '');
      const observed = () => fs.readdirSync(gate).filter((f) => f.startsWith('observed-'));
      while (observed().length < 2) await sleep(1);
      const leader = Math.min(...observed().map((f) => Number(f.slice('observed-'.length)))) === process.pid;
      if (!leader) {
        for (;;) {
          const now = readJsonSafe(ownerFile);
          if (now && now.token !== owner.token) break;
          await sleep(1);
        }
      }
      try {
        fs.rmSync(lockDir, { recursive: true, force: true });
      } catch {
        // lost the race to another recoverer; loop and try again
      }
      const won = true;`,
  );
  assert.notEqual(mutated, source, 'mutation must actually change the source');

  const mutantDir = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'lane-broker-mutant-state-'));
  const mutantPath = path.join(mutantDir, 'state.js');
  fs.writeFileSync(mutantPath, mutated);
  fs.copyFileSync(fileURLToPath(new URL('../src/process-liveness.js', import.meta.url)), path.join(mutantDir, 'process-liveness.js')); // state.js imports it
  const stateJsUrl = pathToFileURL(mutantPath).href;

  const env = freshEnv();
  const deadPid = await spawnDeadPid();

  const MUTANT_WORKERS = 2;
  const MUTANT_ITERATIONS = 1;

  const { overlaps: totalOverlaps, escaped: totalEscaped } = await runRound(env, stateJsUrl, deadPid, 0, {
    workers: MUTANT_WORKERS,
    iterations: MUTANT_ITERATIONS,
    rendezvous: true,
  });

  fs.rmSync(mutantDir, { recursive: true, force: true });

  assert.ok(
    totalOverlaps > 0,
    `expected the pre-fix blind rmSync to reproduce an overlap, got 0 — escaped=${totalEscaped}`,
  );
});
