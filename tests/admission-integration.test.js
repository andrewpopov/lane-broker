import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs from 'node:fs';
import { freshEnv, writeGlobalConfig, writeRepoConfig, laneSpawn, waitFor } from './helpers.js';
import { paths } from '../src/state.js';

/**
 * Codex review finding #2, closed end-to-end: production supervisors are
 * spawned with `stdio: 'ignore'` (run.js) — a stderr-only "log" is silently
 * discarded on every real run. tests/admission.test.js's unit-level checks
 * write from an in-process call with a stubbed/real stream, which cannot
 * distinguish "the file gets written" from "the write silently goes
 * nowhere" — that gap is exactly what let the original stderr-only version
 * pass its own tests while being worthless in production.
 *
 * This spawns a REAL `lane run` through laneSpawn, exactly as run.js does
 * (detached supervisor, stdio ignored), lets it be admitted normally, and
 * reads the admission log back off disk from the fresh broker home — the
 * only way to prove the write actually reaches a file a stubbed stream
 * can't fake.
 */
test('a real, stdio-ignored detached supervisor writes a readable admission-decision line to disk', async () => {
  const { base, home, state, env } = freshEnv();
  writeGlobalConfig(home, { version: 1, capacity: 4, loadClose: 1000, loadOpen: 900, loadOpenSamples: 1, sampleMs: 100 });
  const repoDir = path.join(base, 'repo');
  writeRepoConfig(repoDir, { version: 1, lanes: { default: { weight: 1 } } });

  const child = laneSpawn(['run', '--repo', 'r', '--lane', 'default', '--detach', '--', 'sleep', '0.3'], { env, cwd: repoDir });
  const id = await new Promise((resolve) => {
    let out = '';
    child.stdout.on('data', (d) => {
      out += d;
    });
    child.on('exit', () => resolve(out.trim()));
  });
  assert.ok(id, 'lane run --detach should print an id');

  // Nothing blocks this ticket, so it should be admitted; the lease file
  // appearing confirms tryStart (and therefore the admission-logging call
  // inside it) actually ran inside the real, stdio-ignored supervisor. A
  // generous timeout here (matching no-backfill.test.js's own 30000ms
  // precedent for the same kind of wait): under heavy machine load, just
  // spawning and admitting a detached supervisor can itself take many
  // seconds, independent of anything this change does.
  await waitFor(() => fs.existsSync(path.join(paths(state).leases, `${id}.json`)), { timeoutMs: 60000 });

  // The log write happens synchronously, inside the same tryStart call,
  // BEFORE the lease file is written — so once the lease exists, the log
  // line is already on disk. This wait is just a formality against
  // filesystem-visibility ordering, not a real race.
  const logged = await waitFor(() => {
    let content;
    try {
      content = fs.readFileSync(paths(state).admissionLog, 'utf8');
    } catch {
      return null;
    }
    return content.includes(`candidate=${id}`) ? content : null;
  }, { timeoutMs: 5000 });

  assert.match(logged, new RegExp(`candidate=${id}`));
  assert.match(logged, /mode=(shadow|active)/, 'the schedulerMode must be present');
  assert.match(logged, /current=\w[\w-]*:\w[\w-]*/, 'the current rule\'s decision:reason must be present');
  assert.match(logged, /new=(admit|deny):\S+/, 'the new predicate\'s decision:reason must be present');
});
