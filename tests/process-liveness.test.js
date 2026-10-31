import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { isProcessAlive, processStartTime } from '../src/process-liveness.js';
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
