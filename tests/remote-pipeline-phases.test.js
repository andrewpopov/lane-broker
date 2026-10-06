import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { tmpDir } from './remote-harness.js';
import { remotePipelineCommand } from '../src/remote-pipeline.js';

/** BRAIN-425: `phases.json` is telemetry; failing to write it must not fail the pipeline or leak deps-cache leases. */

const REAL_NPM = execFileSync('which', ['npm'], { encoding: 'utf8' }).trim();

/** An app depending on a local tarball, installed with real npm offline: enough for `npm ci` to need no network. */
function appFixture(dir) {
  const npmEnv = { ...process.env, HOME: dir, npm_config_cache: path.join(dir, 'cache'), npm_config_userconfig: path.join(dir, 'userrc') };
  const tool = path.join(dir, 'tool');
  const app = path.join(dir, 'work');
  fs.mkdirSync(path.join(tool, 'bin'), { recursive: true });
  fs.mkdirSync(app);
  fs.writeFileSync(path.join(tool, 'package.json'), JSON.stringify({ name: 'hello-tool', version: '1.0.0', bin: { 'hello-tool': 'bin/hello.js' } }));
  fs.writeFileSync(path.join(tool, 'bin', 'hello.js'), '#!/usr/bin/env node\n', { mode: 0o755 });
  execFileSync(REAL_NPM, ['pack', '--silent', '--pack-destination', app], { cwd: tool, env: npmEnv });
  fs.writeFileSync(path.join(app, 'package.json'), JSON.stringify({ name: 'app', version: '1.0.0', private: true, dependencies: { 'hello-tool': 'file:./hello-tool-1.0.0.tgz' } }));
  execFileSync(REAL_NPM, ['install', '--offline', '--ignore-scripts', '--no-audit', '--no-fund'], { cwd: app, env: npmEnv });
  fs.rmSync(path.join(app, 'node_modules'), { recursive: true });
  fs.writeFileSync(path.join(app, '.npmrc'), 'registry=http://127.0.0.1:9\nfetch-retries=0\nfetch-timeout=2000\n');
  return app;
}

test('a phases.json write failure neither fails a finished pipeline nor leaks deps-cache leases', async () => {
  const dir = tmpDir('pipeline-phases');
  const workDir = appFixture(dir);
  const ticketDir = path.join(dir, 'ticket');
  const store = path.join(dir, 'deps-cache');
  fs.mkdirSync(ticketDir);
  fs.writeFileSync(
    path.join(ticketDir, 'pipeline.json'),
    JSON.stringify({
      workDir,
      relCwd: '',
      remoteDeps: ['.'],
      remoteSetup: [],
      argv: [process.execPath, '-e', 'process.exit(0)'],
      npmCacheDir: path.join(dir, 'npm-cache'),
      npmUserConfig: path.join(dir, 'npmrc'),
      depsCache: { enabled: true, rootScriptsSafe: false, root: store, maxBytes: 10 * 1024 ** 3 },
    }),
  );
  // a directory where the file goes: the atomic rename onto it fails, with no seam in the code under test
  fs.mkdirSync(path.join(ticketDir, 'phases.json'));
  const result = await remotePipelineCommand(ticketDir);
  assert.equal(result.exitCode, 0);
  const keys = fs.readdirSync(store).filter((n) => /^[0-9a-f]{64}$/.test(n));
  assert.equal(keys.length, 1, 'the install was published, so there is a lease dir to inspect');
  const leasesDir = path.join(store, keys[0], 'leases');
  assert.deepEqual(fs.existsSync(leasesDir) ? fs.readdirSync(leasesDir) : [], [], 'every lease released');
});
