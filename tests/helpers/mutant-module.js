import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
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
