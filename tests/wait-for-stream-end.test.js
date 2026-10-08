import test from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { waitForStreamEnd } from '../src/run.js';

// BRAIN-453: the tail wait after the supervisor exits is bounded by idle time, so a stream still delivering bytes is
// never abandoned with output unread, while a stream that goes quiet without ending still cannot hang `lane run`.

test('a stream that keeps delivering past the idle bound is waited for until it ends', async () => {
  const stream = new PassThrough();
  stream.resume();
  const started = Date.now();
  const pending = waitForStreamEnd(stream, 150);
  for (let i = 0; i < 5; i += 1) {
    await new Promise((resolve) => setTimeout(resolve, 100));
    stream.write('x');
  }
  stream.end();
  await pending;
  assert.ok(Date.now() - started >= 450, 'waited past 3x the idle bound because bytes kept arriving');
  assert.equal(stream.readableEnded, true, 'resolved on end, not on the idle bound');
});

test('a stream that goes quiet without ending is abandoned after the idle bound', async () => {
  const stream = new PassThrough();
  stream.resume();
  const started = Date.now();
  await waitForStreamEnd(stream, 100);
  assert.ok(Date.now() - started < 2000);
  assert.equal(stream.readableEnded, false);
});
