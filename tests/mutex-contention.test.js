import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { freshEnv } from './helpers.js';

const stateJsPath = fileURLToPath(new URL('../src/state.js', import.meta.url));

const WORKERS = 8;
const ITERATIONS = 200;

function workerSource(root, resultFile) {
  return `
import { withLock } from ${JSON.stringify(stateJsPath)};
import fs from 'node:fs';

const root = ${JSON.stringify(root)};
const marker = ${JSON.stringify(path.join(root, '..', 'contention-marker'))};
let overlaps = 0;
let escaped = 0;
for (let i = 0; i < ${ITERATIONS}; i++) {
  try {
    await withLock(root, async () => {
      if (fs.existsSync(marker)) overlaps += 1;
      fs.writeFileSync(marker, String(process.pid));
      await new Promise((r) => setTimeout(r, 1));
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

test('8 processes x 200 iterations of withLock: zero overlaps and zero escaped errors', async () => {
  const { base, state } = freshEnv();
  fs.mkdirSync(state, { recursive: true });

  const runs = [];
  for (let w = 0; w < WORKERS; w += 1) {
    const scriptFile = path.join(base, `worker-${w}.mjs`);
    const resultFile = path.join(base, `result-${w}.json`);
    fs.writeFileSync(scriptFile, workerSource(state, resultFile));
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

  let totalOverlaps = 0;
  let totalEscaped = 0;
  for (const { resultFile } of runs) {
    const { overlaps, escaped } = JSON.parse(fs.readFileSync(resultFile, 'utf8'));
    totalOverlaps += overlaps;
    totalEscaped += escaped;
  }
  if (totalEscaped > 0) {
    for (const r of results) if (r.stderr) process.stderr.write(r.stderr);
  }

  assert.equal(totalOverlaps, 0, `expected zero overlaps across ${WORKERS * ITERATIONS} iterations`);
  assert.equal(totalEscaped, 0, `expected zero escaped errors across ${WORKERS * ITERATIONS} iterations`);
});
