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
const ROUNDS = 10;

function workerSource(stateJsUrl, root, resultFile, iterations = ITERATIONS) {
  return `
import { withLock } from ${JSON.stringify(stateJsUrl)};
import fs from 'node:fs';

const root = ${JSON.stringify(root)};
const marker = ${JSON.stringify(path.join(root, '..', 'contention-marker'))};
let overlaps = 0;
let escaped = 0;
for (let i = 0; i < ${iterations}; i++) {
  try {
    await withLock(root, async () => {
      if (fs.existsSync(marker)) overlaps += 1;
      fs.writeFileSync(marker, String(process.pid));
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

async function runRound({ base, state }, stateJsUrl, deadPid, round, { workers = WORKERS, iterations = ITERATIONS } = {}) {
  plantDeadLock(state, deadPid, `dead-${round}`);

  const runs = [];
  for (let w = 0; w < workers; w += 1) {
    const scriptFile = path.join(base, `worker-${round}-${w}.mjs`);
    const resultFile = path.join(base, `result-${round}-${w}.json`);
    fs.writeFileSync(scriptFile, workerSource(stateJsUrl, state, resultFile, iterations));
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
  // A widened race window (a random sleep between "observed dead" and the
  // blind rm) is added so the pre-fix bug reproduces reliably on this
  // machine instead of depending on luck: it still exercises exactly the
  // same defect (two contenders that both observed the SAME dead owner both
  // acting on that stale observation), just makes the window wide enough to
  // hit deterministically rather than needing thousands of iterations.
  const source = fs.readFileSync(stateJsPath, 'utf8');
  const guarded = 'recoverDeadLock(root, lockDir, ownerFile, owner);';
  assert.ok(source.includes(guarded), 'expected the atomic-takeover call site in state.js; test needs updating');
  const mutated = source.replace(
    guarded,
    `await sleep(Math.random() * 15);\n      try {\n        fs.rmSync(lockDir, { recursive: true, force: true });\n      } catch {\n        // lost the race to another recoverer; loop and try again\n      }`,
  );
  assert.notEqual(mutated, source, 'mutation must actually change the source');

  const mutantDir = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'lane-broker-mutant-state-'));
  const mutantPath = path.join(mutantDir, 'state.js');
  fs.writeFileSync(mutantPath, mutated);
  const stateJsUrl = pathToFileURL(mutantPath).href;

  const env = freshEnv();
  const deadPid = await spawnDeadPid();

  const MUTANT_WORKERS = 16;
  const MUTANT_ITERATIONS = 1;
  const MUTANT_ROUNDS = 10;

  let totalOverlaps = 0;
  let totalEscaped = 0;
  let round = 0;
  // Escalate rounds a bit if the machine is fast enough not to reproduce it
  // immediately; report whatever settings it took.
  for (; round < MUTANT_ROUNDS && totalOverlaps === 0; round += 1) {
    const { overlaps, escaped } = await runRound(env, stateJsUrl, deadPid, round, {
      workers: MUTANT_WORKERS,
      iterations: MUTANT_ITERATIONS,
    });
    totalOverlaps += overlaps;
    totalEscaped += escaped;
  }

  fs.rmSync(mutantDir, { recursive: true, force: true });

  process.stdout.write(
    `[lock-dead-owner-recovery] mutant reproduced overlaps=${totalOverlaps} within round=${round} ` +
      `(WORKERS=${MUTANT_WORKERS}, ITERATIONS=${MUTANT_ITERATIONS})\n`,
  );

  assert.ok(
    totalOverlaps > 0,
    `expected the pre-fix blind rmSync to reproduce at least one overlap within ${round} round(s) ` +
      `(WORKERS=${MUTANT_WORKERS}, ITERATIONS=${MUTANT_ITERATIONS}, widened race sleep 0-15ms), got 0 — escaped=${totalEscaped}`,
  );
});
