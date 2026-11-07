#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { run } from 'node:test';
import { finished } from 'node:stream/promises';
import { spec } from 'node:test/reporters';

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const testsDir = path.join(root, 'tests');
const weightsFile = path.join(root, 'scripts', 'test-weights.json');

export const GUARD = 'zz-tmp-leak-guard.test.js';
export const HEAVY = ['lane-claim-scale.test.js', 'lane-soak.test.js', 'mutex-contention.test.js'];
export const TIERS = ['pr', 'heavy', 'all'];
const GUARD_SECONDS = 1;

const USAGE = `usage: node scripts/test-tier.js <pr|heavy|all> [--dry-run] [--concurrency N] [--weights-from <logfile>]
  pr     every test file except the heavy ones
  heavy  ${HEAVY.join(', ')}
  all    every test file
  --only <file,...>  (hidden filter) restrict the pool to these test files`;

export const listTestFiles = (dir = testsDir) =>
  fs.readdirSync(dir).filter((name) => name.endsWith('.test.js')).sort();

export function selectFiles(tier, universe) {
  const pool = universe.filter((name) => name !== GUARD);
  if (tier === 'pr') return pool.filter((name) => !HEAVY.includes(name));
  if (tier === 'heavy') return pool.filter((name) => HEAVY.includes(name));
  if (tier === 'all') return pool;
  throw new Error(`unknown tier "${tier}"`);
}

export function resolveConcurrency(flag, env = process.env) {
  const fromFlag = Number.parseInt(flag, 10);
  if (fromFlag >= 1) return fromFlag;
  const grant = Math.floor(Number(env.LANE_BROKER_CPU_CORES));
  return grant >= 1 ? grant : 1;
}

const median = (values) => {
  if (values.length === 0) return 1;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
};

export function weighFiles(files, weights) {
  const fallback = median(Object.values(weights));
  return files.map((file) => ({ file, seconds: weights[file] ?? fallback }));
}

export function orderLongestFirst(files, weights) {
  return weighFiles(files, weights).sort((a, b) => b.seconds - a.seconds || a.file.localeCompare(b.file));
}

export function predictMakespan(ordered, concurrency) {
  const workers = new Array(concurrency).fill(0);
  for (const { seconds } of ordered) {
    const idle = workers.indexOf(Math.min(...workers));
    workers[idle] += seconds;
  }
  return Math.max(...workers) + GUARD_SECONDS;
}

export function parseFileTimings(log) {
  const timings = {};
  for (const match of log.matchAll(/^test-tier file (\S+) ([\d.]+)s$/gm)) timings[match[1]] = Number(match[2]);
  return timings;
}

const loadWeights = () => JSON.parse(fs.readFileSync(weightsFile, 'utf8'));

function parseArgs(argv) {
  const options = { tier: argv[0], dryRun: false, concurrency: undefined, only: null, weightsFrom: null };
  for (let i = 1; i < argv.length; i += 1) {
    if (argv[i] === '--dry-run') options.dryRun = true;
    else if (argv[i] === '--concurrency') options.concurrency = argv[++i];
    else if (argv[i] === '--only') options.only = argv[++i].split(',');
    else if (argv[i] === '--weights-from') options.weightsFrom = argv[++i];
    else throw new Error(`unknown argument "${argv[i]}"`);
  }
  return options;
}

async function runFiles(files, concurrency, seconds) {
  let failed = false;
  const addDuration = (data) => {
    if (data.nesting !== 0) return;
    const name = path.basename(data.file);
    seconds[name] = (seconds[name] ?? 0) + data.details.duration_ms / 1000;
  };
  const stream = run({ files: files.map((name) => path.join(testsDir, name)), concurrency });
  stream.on('test:pass', addDuration);
  stream.on('test:fail', (data) => {
    failed = true;
    addDuration(data);
  });
  const output = stream.compose(new spec());
  output.pipe(process.stdout, { end: false });
  await finished(output);
  return !failed;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.weightsFrom) {
    const timings = parseFileTimings(fs.readFileSync(options.weightsFrom, 'utf8'));
    const rounded = Object.fromEntries(Object.entries(timings).sort().map(([file, s]) => [file, Math.round(s * 10) / 10]));
    process.stdout.write(`${JSON.stringify(rounded, null, 2)}\n`);
    return 0;
  }
  if (!TIERS.includes(options.tier)) throw new Error(USAGE);
  const concurrency = resolveConcurrency(options.concurrency);
  let pool = selectFiles(options.tier, listTestFiles());
  if (options.only) pool = pool.filter((name) => options.only.includes(name));
  const ordered = orderLongestFirst(pool, loadWeights());

  if (options.dryRun) {
    console.log(`tier ${options.tier}, concurrency ${concurrency}, ${ordered.length} files`);
    for (const { file, seconds } of ordered) console.log(`  ${seconds.toFixed(1)}s ${file}`);
    console.log(`predicted makespan ${(predictMakespan(ordered, concurrency) / 60).toFixed(1)} min (greedy longest-first over ${concurrency} workers, plus the leak guard)`);
    return 0;
  }

  const started = Date.now();
  const seconds = {};
  const poolPassed = await runFiles(ordered.map(({ file }) => file), concurrency, seconds);
  const guardPassed = await runFiles([GUARD], 1, seconds);
  for (const file of Object.keys(seconds).sort()) console.log(`test-tier file ${file} ${seconds[file].toFixed(1)}s`);
  console.log(`test-tier total ${((Date.now() - started) / 1000).toFixed(1)}s`);
  return poolPassed && guardPassed ? 0 : 1;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().then((code) => { process.exitCode = code; }, (error) => {
    console.error(error.message);
    process.exitCode = 2;
  });
}
