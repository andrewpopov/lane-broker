import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { makeTmpDir } from './tmp.js';

/**
 * Imports `file` with one text mutation applied: a canary that runs a scenario against code with its guard removed.
 * `mutate` is [from, to] and must match exactly once. The copy lives in a temp dir, so its `pg` import points at the repo's own.
 */
export async function importMutant(file, [from, to]) {
  const source = fs.readFileSync(file, 'utf8');
  if (source.split(from).length !== 2) throw new Error(`mutation target must occur exactly once in ${file}: ${from}`);
  const pgEntry = pathToFileURL(createRequire(import.meta.url).resolve('pg')).href;
  const mutated = source.replace(from, () => to).replace("from 'pg'", () => `from '${pgEntry}'`);
  const copy = path.join(makeTmpDir('mutant-'), path.basename(file));
  fs.writeFileSync(copy, mutated);
  return import(pathToFileURL(copy).href);
}

/**
 * Canary for a module that imports its neighbours: copies `src/` beside the repo's package.json and node_modules, applies one exact-once
 * mutation to `file` (relative to src/) and returns a loader for any module of the mutated tree, so the scenario runs wholly against it.
 */
export async function mutateSrcTree(file, mutation) {
  // one [from, to] pair, or a list of pairs for a guard that is one structure spread over several lines
  const pairs = Array.isArray(mutation[0]) ? mutation : [mutation];
  const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
  const tree = makeTmpDir('mutant-tree-');
  fs.cpSync(path.join(repo, 'src'), path.join(tree, 'src'), { recursive: true });
  fs.cpSync(path.join(repo, 'bin'), path.join(tree, 'bin'), { recursive: true });
  fs.copyFileSync(path.join(repo, 'package.json'), path.join(tree, 'package.json'));
  fs.symlinkSync(path.join(repo, 'node_modules'), path.join(tree, 'node_modules'));
  const target = path.join(tree, 'src', file);
  let mutated = fs.readFileSync(target, 'utf8');
  for (const [edit, replacement] of pairs) {
    if (mutated.split(edit).length !== 2) throw new Error(`mutation target must occur exactly once in ${file}: ${edit}`);
    mutated = mutated.replace(edit, () => replacement);
  }
  fs.writeFileSync(target, mutated);
  const url = (module) => pathToFileURL(path.join(tree, 'src', module)).href;
  return Object.assign((module) => import(url(module)), { url, laneBin: path.join(tree, 'bin', 'lane.js') });
}

const realUrl = (module) => new URL(`../../src/${module}`, import.meta.url).href;
export const realSrc = Object.assign((module) => import(realUrl(module)), { url: realUrl, laneBin: fileURLToPath(new URL('../../bin/lane.js', import.meta.url)) });

/**
 * Runs `run(load)` against the mutated tree and demands that it fail on an assertion, or on an error whose message matches `expected`
 * (the one named failure a guard's absence causes), and, when `assertionPattern` is given, on the assertion whose message matches it: a guard that survives its own removal, or a scenario that dies of something else, proves nothing.
 */
export async function assertCanaryFails(file, mutation, run, expected = null, assertionPattern = null) {
  const removed = Array.isArray(mutation[0]) ? mutation[0][0] : mutation[0];
  const load = await mutateSrcTree(file, mutation);
  let failure;
  await run(load).catch((err) => { failure = err; });
  assert.ok(failure, `with "${removed}" removed from ${file} the scenario still passed, so it proves nothing`);
  const named = failure.code === 'ERR_ASSERTION' || (expected !== null && expected.test(String(failure.message)));
  assert.ok(named, `with "${removed}" removed from ${file} the scenario failed, but not on an assertion or ${expected}: ${failure.stack}`);
  if (assertionPattern !== null) assert.match(String(failure.message), assertionPattern, `with "${removed}" removed from ${file} the scenario failed on a different assertion than the one this guard is for`);
  return failure;
}
