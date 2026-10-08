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
  let resolved = false;
  const pending = waitForStreamEnd(stream, 300).then(() => {
    resolved = true;
  });
  await new Promise((resolve) => setTimeout(resolve, 150));
  assert.equal(resolved, false, 'still inside the idle window: an immediate return must not pass');
  await pending;
  assert.ok(Date.now() - started >= 280, 'waited out the idle bound');
  assert.equal(stream.readableEnded, false);
});

test('time spent paused under backpressure is not idle time', async () => {
  const stream = new PassThrough();
  stream.resume();
  stream.pause();
  let resolved = false;
  const pending = waitForStreamEnd(stream, 100).then(() => {
    resolved = true;
  });
  await new Promise((resolve) => setTimeout(resolve, 500));
  assert.equal(resolved, false, 'a paused stream is waiting on its consumer, not quiet');
  stream.resume();
  await pending;
  assert.equal(resolved, true, 'once flowing and quiet it is abandoned');
});
