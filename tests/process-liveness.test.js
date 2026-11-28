import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { isConfirmedGone, isPidAlive, isProcessAlive, isVerifiedProcess, processStartTime, procStatState, psStartTime, sameProcessStart } from '../src/process-liveness.js';
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
    if (process.platform === 'linux') assert.match(processStartTime(process.pid), /^ticks:\d+$/, 'Linux reads /proc, not ps');
    else assert.equal(processStartTime(process.pid), null);
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

test('a stuck ps that ignores SIGTERM is killed at the timeout and reads as indeterminate, so a liveness check fails closed instead of hanging', { timeout: 10_000 }, (t) => {
  const bin = makeTmpDir('lane-ps-stuck-', t);
  fs.writeFileSync(path.join(bin, 'ps'), "#!/bin/sh\ntrap '' TERM\ni=0\nwhile [ $i -lt 20 ]; do sleep 1; i=$((i+1)); done\n", { mode: 0o755 });
  const saved = process.env.PATH;
  process.env.PATH = `${bin}${path.delimiter}${saved}`;
  try {
    const started = Date.now();
    assert.equal(psStartTime(process.pid, { timeoutMs: 200 }), undefined);
    assert.ok(Date.now() - started < 5_000, 'the probe ended at its timeout');
  } finally {
    process.env.PATH = saved;
  }
});

function withPsPrinting(t, line, fn) {
  const bin = makeTmpDir('lane-ps-print-', t);
  fs.writeFileSync(path.join(bin, 'ps'), `#!/bin/sh\necho '${line}'\n`, { mode: 0o755 });
  const saved = process.env.PATH;
  process.env.PATH = `${bin}${path.delimiter}${saved}`;
  try {
    return fn();
  } finally {
    process.env.PATH = saved;
  }
}

test('a ps start token is still compared exactly: one second off is a different process', (t) => {
  const recorded = 'Tue Nov 24 14:30:17 2026';
  withPsPrinting(t, recorded, () => assert.equal(sameProcessStart(process.pid, recorded), true));
  withPsPrinting(t, 'Tue Nov 24 14:30:16 2026', () => assert.equal(sameProcessStart(process.pid, recorded), false));
});

test('a ticks token whose /proc read fails is indeterminate, never a mismatch', { skip: process.platform !== 'linux' }, () => {
  assert.equal(sameProcessStart(2 ** 22 + 12345, 'ticks:1'), null, 'a pid with no /proc entry is gone');
  assert.equal(isVerifiedProcess(process.pid, processStartTime(process.pid)), true);
  assert.equal(isConfirmedGone(process.pid, processStartTime(process.pid)), false);
  assert.equal(isConfirmedGone(process.pid, 'ticks:1'), true, 'a different tick count is a different process');
});

test('on Linux the start token comes from /proc and ignores whatever ps reports', { skip: process.platform !== 'linux' }, (t) => {
  const token = processStartTime(process.pid);
  assert.match(token, /^ticks:\d+$/);
  assert.equal(token.slice(6), fs.readFileSync('/proc/self/stat', 'utf8').slice(fs.readFileSync('/proc/self/stat', 'utf8').lastIndexOf(')') + 2).split(' ')[19]);
  withPsPrinting(t, 'Mon Jan  1 00:00:00 2000', () => {
    assert.equal(processStartTime(process.pid), token);
    assert.equal(isProcessAlive(process.pid, token), true);
  });
  assert.equal(isProcessAlive(process.pid, 'ticks:1'), false, 'a different tick count is pid reuse');
});
