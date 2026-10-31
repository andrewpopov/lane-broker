import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { makeTmpDir } from './helpers/tmp.js';

const tmpHelper = fileURLToPath(new URL('./helpers/tmp.js', import.meta.url));

test('teardown kills a straggler that names a temp dir and would recreate it', async (t) => {
  const stage = makeTmpDir('lane-teardown-stage-', t);
  const dirFile = path.join(stage, 'dir');
  const driver = `
    import { spawn } from 'node:child_process';
    import fs from 'node:fs';
    import { makeTmpDir } from ${JSON.stringify(tmpHelper)};
    const dir = makeTmpDir('lane-teardown-victim-');
    const script = 'setTimeout(() => { require("fs").mkdirSync(process.argv[1], { recursive: true }); require("fs").writeFileSync(process.argv[1] + "/late", "x"); }, 4000)';
    spawn(process.execPath, ['-e', script, dir], { detached: true, stdio: 'ignore' }).unref();
    fs.writeFileSync(${JSON.stringify(dirFile)}, dir);
  `;
  const run = spawnSync(process.execPath, ['--input-type=module', '-e', driver], { encoding: 'utf8' });
  assert.equal(run.status, 0, run.stderr);
  const dir = fs.readFileSync(dirFile, 'utf8');
  assert.equal(fs.existsSync(dir), false, 'removed at teardown');
  await new Promise((resolve) => setTimeout(resolve, 5000));
  assert.equal(fs.existsSync(dir), false, 'not recreated by the straggler');
});
