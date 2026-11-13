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
export async function mutateSrcTree(file, [from, to]) {
  const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
  const tree = makeTmpDir('mutant-tree-');
  fs.cpSync(path.join(repo, 'src'), path.join(tree, 'src'), { recursive: true });
  fs.copyFileSync(path.join(repo, 'package.json'), path.join(tree, 'package.json'));
  fs.symlinkSync(path.join(repo, 'node_modules'), path.join(tree, 'node_modules'));
  const target = path.join(tree, 'src', file);
  const source = fs.readFileSync(target, 'utf8');
  if (source.split(from).length !== 2) throw new Error(`mutation target must occur exactly once in ${file}: ${from}`);
  fs.writeFileSync(target, source.replace(from, () => to));
  const url = (module) => pathToFileURL(path.join(tree, 'src', module)).href;
  return Object.assign((module) => import(url(module)), { url });
}

const realUrl = (module) => new URL(`../../src/${module}`, import.meta.url).href;
export const realSrc = Object.assign((module) => import(realUrl(module)), { url: realUrl });

/** Runs `run(load)` against the mutated tree and fails unless it throws: a guard that survives its own removal proves nothing. */
export async function assertCanaryFails(file, mutation, run) {
  const load = await mutateSrcTree(file, mutation);
  let failure;
  await run(load).catch((err) => { failure = err; });
  assert.ok(failure, `with "${mutation[0]}" removed from ${file} the scenario still passed, so it proves nothing`);
  return failure;
}
