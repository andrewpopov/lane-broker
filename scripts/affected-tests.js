import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const MAX_SELECTED = 20;
const IMPORT_SPECIFIER = /(?:\bfrom\s*|^\s*import\s*)['"]([^'"]+)['"]/gm;
const TEST_FILE = /^tests\/[^/]+\.test\.js$/;

const git = (cwd, args) => execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();

function mergeBase(cwd) {
  try {
    return git(cwd, ['merge-base', 'HEAD', 'origin/main']);
  } catch {
    return null;
  }
}

function dependenciesAt(cwd, ref) {
  try {
    const { dependencies, devDependencies } = JSON.parse(git(cwd, ['show', `${ref}:package.json`]));
    return JSON.stringify({ dependencies, devDependencies });
  } catch {
    return null;
  }
}

function dependenciesChanged(cwd, base, changed) {
  if (changed.includes('package-lock.json')) return true;
  if (!changed.includes('package.json')) return false;
  return dependenciesAt(cwd, base) !== dependenciesAt(cwd, 'HEAD');
}

function importedRepoPaths(cwd, testFile) {
  const source = fs.readFileSync(path.join(cwd, testFile), 'utf8');
  return [...source.matchAll(IMPORT_SPECIFIER)]
    .map((match) => match[1])
    .filter((specifier) => specifier.startsWith('.'))
    .map((specifier) => path.posix.normalize(path.posix.join(path.posix.dirname(testFile), specifier)));
}

function selectTests(cwd, changed) {
  const allTests = fs.readdirSync(path.join(cwd, 'tests')).map((name) => `tests/${name}`).filter((file) => TEST_FILE.test(file));
  const changedSources = changed.filter((file) => /^(src|bin)\/.+\.js$/.test(file));
  const changedBasenames = changed.filter((file) => /^src\/.+\.js$/.test(file)).map((file) => path.posix.basename(file, '.js'));
  const selected = new Set(changed.filter((file) => TEST_FILE.test(file) && fs.existsSync(path.join(cwd, file))));
  for (const test of allTests) {
    const importsChanged = importedRepoPaths(cwd, test).some((imported) => changedSources.includes(imported));
    const nameMatches = changedBasenames.some((base) => test.startsWith(`tests/${base}`));
    if (importsChanged || nameMatches) selected.add(test);
  }
  return [...selected].sort();
}

/** Returns { skip: reason } when the change is unbounded, otherwise { files } (possibly empty). */
export function selectAffected(cwd = process.cwd()) {
  const base = mergeBase(cwd);
  if (!base) return { skip: 'no merge-base with origin/main' };
  const changed = git(cwd, ['diff', '--name-only', `${base}...HEAD`]).split('\n').filter(Boolean);
  if (dependenciesChanged(cwd, base, changed)) return { skip: 'package dependencies changed' };
  const files = selectTests(cwd, changed);
  if (files.length > MAX_SELECTED) return { skip: `${files.length} tests selected (limit ${MAX_SELECTED})` };
  return { files };
}

export function renderSelection(selection) {
  if (selection.skip) return `affected: ${selection.skip}, skipping unit tests (the train runs them)`;
  if (selection.files.length === 0) return 'affected: no related tests';
  return selection.files.join('\n');
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  console.log(renderSelection(selectAffected()));
}
