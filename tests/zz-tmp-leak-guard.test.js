import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { manifestPath } from './helpers/tmp.js';

/**
 * BRAIN-420: the suite leaves nothing in os.tmpdir(). Runs last (name sorts after every other
 * tests/*.test.js, and the suite is --test-concurrency=1), so by now every earlier test-file
 * process has exited and run its cleanup hook.
 *
 * Two halves, because each alone has a hole: the manifest proves every dir made through
 * makeTmpDir is gone, and the source scan proves no test bypasses makeTmpDir.
 */
const testsDir = path.dirname(fileURLToPath(import.meta.url));

test('every directory made through makeTmpDir has been removed', () => {
  const manifest = manifestPath();
  if (!fs.existsSync(manifest)) return; // run alone, not as part of one suite run
  const dirs = fs.readFileSync(manifest, 'utf8').split('\n').filter(Boolean);
  const leaked = dirs.filter((d) => fs.existsSync(d));
  fs.rmSync(manifest, { force: true });
  assert.deepEqual(leaked, [], `${leaked.length} of ${dirs.length} test temp dirs leaked: ${leaked.slice(0, 5).join(', ')}`);
});

test('no test creates a temp directory except through makeTmpDir', () => {
  const offenders = [];
  for (const file of fs.readdirSync(testsDir).filter((f) => f.endsWith('.js'))) {
    if (file === 'zz-tmp-leak-guard.test.js') continue;
    fs.readFileSync(path.join(testsDir, file), 'utf8').split('\n').forEach((line, i) => {
      if (/\bmkdtemp(Sync)?\(/.test(line) && !/^\s*(\/\/|\*)/.test(line) && !/orig\.apply|lbpg-/.test(line) && !/(join|mkdtempSync)\((path\.dirname\(home\)|base),/.test(line)) {
        offenders.push(`${file}:${i + 1}: ${line.trim()}`);
      }
    });
  }
  assert.deepEqual(offenders, [], `raw mkdtemp in tests (use makeTmpDir from tests/helpers/tmp.js):\n${offenders.join('\n')}`);
});
