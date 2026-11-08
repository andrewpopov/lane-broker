#!/usr/bin/env node
// zz-tmp-leak-guard checks what the files before it left behind, so every shard runs it last (README "Running the tests").
import fs from 'node:fs';
import path from 'node:path';

const LAST = 'tests/zz-tmp-leak-guard.test.js';

export function shardFiles(files, durations, shard, total) {
  const known = Object.values(durations).sort((a, b) => a - b);
  const fallback = known.length > 0 ? known[Math.floor(known.length / 2)] : 1;
  const cost = (file) => durations[file] ?? fallback;
  const bins = Array.from({ length: total }, () => ({ seconds: 0, files: [] }));
  const dealt = files.filter((f) => f !== LAST).sort((a, b) => cost(b) - cost(a) || a.localeCompare(b));
  for (const file of dealt) {
    const bin = bins.reduce((min, b) => (b.seconds < min.seconds ? b : min));
    bin.seconds += cost(file);
    bin.files.push(file);
  }
  const mine = bins[shard - 1].files.sort();
  return files.includes(LAST) ? [...mine, LAST] : mine;
}

export function parseShard(value) {
  if (value === undefined || value === '') return { shard: 1, total: 1 };
  const match = /^(\d+)\/(\d+)$/.exec(value);
  const [shard, total] = match ? [Number(match[1]), Number(match[2])] : [0, 0];
  if (!(total >= 1 && shard >= 1 && shard <= total)) throw new Error(`TRAIN_SHARD must be i/N with 1 <= i <= N, got ${JSON.stringify(value)}`);
  return { shard, total };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { shard, total } = parseShard(process.env.TRAIN_SHARD);
  const files = fs.readdirSync('tests').filter((n) => n.endsWith('.test.js')).map((n) => path.posix.join('tests', n)).sort();
  const durations = JSON.parse(fs.readFileSync('tests/durations.json', 'utf8'));
  process.stdout.write(`${shardFiles(files, durations, shard, total).join('\n')}\n`);
}
