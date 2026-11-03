import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { isPidAlive, isProcessAlive, processStartTime, procStatState } from '../src/process-liveness.js';
import { makeTmpDir } from './helpers/tmp.js';

function withFailingPs(t, fn) {
  const bin = makeTmpDir('lane-ps-stub-', t);
  fs.writeFileSync(path.join(bin, 'ps'), '#!/bin/sh\nexit 1\n', { mode: 0o755 });
  const saved = process.env.PATH;
  process.env.PATH = `${bin}${path.delimiter}${saved}`;
  try {
    return fn();
  } finally {
    process.env.PATH = saved;
  }
}

test('a live pid whose ps exits non-zero is still alive: ps alone cannot declare it dead', (t) => {
  withFailingPs(t, () => {
    assert.equal(processStartTime(process.pid), null);
    assert.equal(isProcessAlive(process.pid, 'Mon Jan  1 00:00:00 2024'), true);
  });
});

test('a pid that exited is not alive, even with a failing ps', async (t) => {
  const child = spawn(process.execPath, ['-e', '']);
  await new Promise((resolve) => child.on('exit', resolve));
  withFailingPs(t, () => assert.equal(isProcessAlive(child.pid, 'Mon Jan  1 00:00:00 2024'), false));
});

test('real ps with a different start time means pid reuse: not alive', () => {
  assert.equal(isProcessAlive(process.pid, 'Mon Jan  1 00:00:00 2000'), false);
  assert.equal(isProcessAlive(process.pid, processStartTime(process.pid)), true);
});

test('procStatState reads the state after the LAST paren of comm, which may hold spaces and parens', () => {
  assert.equal(procStatState('123 (sleep) S 1 123 123 0'), 'S');
  assert.equal(procStatState('123 (a) b) Z 1 2 3'), 'Z');
  assert.equal(procStatState('123 (we ird) Z 1 123'), 'Z');
  assert.equal(procStatState('garbage'), null);
  assert.equal(procStatState('123 (x)'), null);
});

test('a zombie is not alive on Linux (kill(0) succeeds on one)', { skip: process.platform !== 'linux' }, async (t) => {
  // sh forks a short-lived child then execs a long sleep: the child exits and is never reaped by its non-waiting parent
  const parent = spawn('sh', ['-c', 'sh -c "exit 0" & exec sleep 30'], { stdio: 'ignore' });
  t.after(() => parent.kill('SIGKILL'));
  await new Promise((resolve) => setTimeout(resolve, 300));
  const zombiePid = fs.readdirSync('/proc').filter((n) => /^\d+$/.test(n)).map(Number).find((pid) => {
    try {
      const stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
      return procStatState(stat) === 'Z' && Number(stat.slice(stat.lastIndexOf(')') + 2).split(' ')[1]) === parent.pid;
    } catch {
      return false;
    }
  });
  assert.ok(zombiePid, 'expected a zombie child of the sleeping parent');
  process.kill(zombiePid, 0);
  assert.equal(isPidAlive(zombiePid), false);
  assert.equal(isPidAlive(parent.pid), true);
});
