import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { CappedLogWriter } from '../src/supervisor.js';

/**
 * If the log stream errors while a source (the child's stdout/stderr) is
 * paused waiting for backpressure to clear, 'drain' will never fire on an
 * errored stream. Without an explicit resume on error, the source stays
 * paused forever and the child never finishes -- the run hangs instead of
 * failing.
 */
test('a log stream error while a source is paused resumes it immediately and switches to discard mode', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lane-broker-log-writer-'));
  const logPath = path.join(dir, 'out.log');
  const writer = new CappedLogWriter(logPath);

  let paused = false;
  let resumeCalls = 0;
  const fakeSource = {
    pause() {
      paused = true;
    },
    resume() {
      resumeCalls += 1;
      paused = false;
    },
  };

  writer.pauseUntilDrain(fakeSource);
  assert.equal(paused, true, 'the source must be paused while waiting for drain');

  writer.stream.emit('error', new Error('simulated disk failure'));

  assert.equal(writer.failed, true, 'the writer must switch to failed/discard mode on a stream error');
  assert.equal(paused, false, 'a source paused on backpressure must never stay paused after the stream errors');
  assert.equal(resumeCalls, 1, 'resume must be called exactly once for the pending source');

  // Once failed, writes are discarded (return true) instead of pausing again.
  assert.equal(writer.write(Buffer.from('more output')), true);

  await writer.finish();
  fs.rmSync(dir, { recursive: true, force: true });
});
