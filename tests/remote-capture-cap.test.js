import test from 'node:test';
import assert from 'node:assert/strict';
import { runWithDeadline } from '../src/remote-client.js';

// BRAIN-398: what a runner prints is bounded; a flood keeps only its tail. The retained text IS the memory the submitter
// holds for it (RSS is too noisy to assert on), so its length is what is checked.

const FLOOD_BYTES = 40 * 1024 * 1024;
const flood = (stream) => `
const chunk = Buffer.alloc(1024 * 1024, 'a');
for (let i = 0; i < ${FLOOD_BYTES / (1024 * 1024)}; i += 1) process.${stream}.write(chunk);
process.${stream}.write('THE-END');
`;

for (const stream of ['stderr', 'stdout']) {
  test(`${stream} flood: the capture keeps only its tail, marks truncation, and stays under the cap`, async () => {
    const cap = 64 * 1024;
    const res = await runWithDeadline(process.execPath, ['-e', flood(stream)], 60_000, process.env, cap);
    assert.equal(res.code, 0);
    const text = res[stream];
    assert.ok(text.length <= cap + 32, `captured ${text.length} chars`);
    assert.match(text, /^\[truncated\]/);
    assert.ok(text.endsWith('THE-END'), 'the tail is what is kept');
  });
}

test('a small capture is returned whole and unmarked', async () => {
  const res = await runWithDeadline(process.execPath, ['-e', "process.stdout.write('hello')"], 10_000, process.env, 1024);
  assert.equal(res.stdout, 'hello');
});
