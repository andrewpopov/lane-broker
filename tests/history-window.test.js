import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { readHistoryRows, readRecentHistoryRows, RECENT_READ_CHUNK_BYTES, RECENT_READ_STOP_MARGIN_MS } from '../src/suggest.js';

/**
 * BRAIN-899 B2: the windowed history read a placement ranks on -- the tail of history.jsonl, cut by the WINDOW rather than by a
 * byte cap, with one repo's raw lines prefiltered before any of them is parsed, through the same parser `readHistoryRows` uses.
 */

const NOW = Date.parse('2026-11-09T12:00:00Z');
const DAY = 24 * 3_600_000;
const REPO = 'lane-broker';
const LANE = 'pr';
const INCLUDE = `"repo":"${REPO}"`;

const row = (id, extra = {}) => ({
  id, repo: REPO, lane: LANE, command: 'sh scripts/test-run.sh tests/a.test.js', executor: 'remote', runner: 'skybox', exit: 0,
  startedAt: NOW - 3_600_000, endedAt: NOW - 3_500_000, runMs: 100_000, ...extra,
});

/** A history file of raw lines, written the way `appendHistory` writes rows. */
function writeLines(lines, prefix = 'history-window') {
  const file = path.join(makeTmpDir(`${prefix}-`), 'history.jsonl');
  fs.writeFileSync(file, lines.map((line) => `${line}\n`).join(''));
  return file;
}

const fileOf = (rows, prefix) => writeLines(rows.map((r) => JSON.stringify(r)), prefix);

test('readRecentHistoryRows: a file smaller than one chunk is read whole, in file order', () => {
  const rows = [row('a'), row('b'), row('c')];
  const file = fileOf(rows);
  assert.ok(fs.statSync(file).size < RECENT_READ_CHUNK_BYTES, 'the fixture is one chunk');
  assert.deepEqual(readRecentHistoryRows(file, { sinceMs: NOW - DAY }), rows);
  assert.deepEqual(readRecentHistoryRows(file), readHistoryRows(file), 'with no window it is the full read');
  assert.deepEqual(readRecentHistoryRows(path.join(path.dirname(file), 'absent.jsonl'), { sinceMs: NOW - DAY }), [], 'a missing history is no history');
});

test('readRecentHistoryRows: torn, blank, null and diagnostic lines are skipped, exactly as readHistoryRows skips them', () => {
  const file = writeLines([
    'null',
    '',
    '{ torn',
    JSON.stringify({ id: 'diag', event: 'supervisor-error', startedAt: NOW }),
    JSON.stringify(row('ok')),
  ]);
  fs.appendFileSync(file, '{ a torn tail with no newline');
  const got = readRecentHistoryRows(file, { sinceMs: NOW - DAY });
  assert.deepEqual(got.map((r) => r.id), ['ok']);
  assert.deepEqual(got, readHistoryRows(file), 'one parser, so one tolerance');
});

test('readRecentHistoryRows: a row older than the window is skipped, and only one that settled past the margin ends the scan', () => {
  const sinceMs = NOW - DAY;
  const lateRow = row('started-before-ended-inside', { startedAt: sinceMs - 2 * 3_600_000, endedAt: sinceMs + 3_600_000 });
  const justOld = row('just-old', { startedAt: sinceMs - 3_600_000, endedAt: sinceMs - 1 });
  const file = fileOf([row('in-window-1'), lateRow, justOld, row('in-window-2')]);
  assert.deepEqual(readRecentHistoryRows(file, { sinceMs }).map((r) => r.id), ['in-window-1', 'in-window-2'], 'old rows inside the margin are skipped, not a stop');
  const farOld = row('far-old', { startedAt: sinceMs - RECENT_READ_STOP_MARGIN_MS - 2, endedAt: sinceMs - RECENT_READ_STOP_MARGIN_MS - 1 });
  assert.deepEqual(readRecentHistoryRows(fileOf([row('before-stop'), farOld, row('after-stop')]), { sinceMs }).map((r) => r.id), ['after-stop'], 'a row that ended past the margin ends the scan');
  const undatedEnd = row('no-endedAt', { startedAt: sinceMs - RECENT_READ_STOP_MARGIN_MS - 1, endedAt: undefined });
  assert.deepEqual(readRecentHistoryRows(fileOf([row('before-stop'), undatedEnd, row('after-stop')]), { sinceMs }).map((r) => r.id), ['after-stop'], 'without endedAt, startedAt decides');
  assert.deepEqual(readRecentHistoryRows(fileOf([row('at', { startedAt: sinceMs })]), { sinceMs }).map((r) => r.id), ['at'], 'the window is inclusive');
});

test('readRecentHistoryRows: a row without startedAt is kept and never ends the scan', () => {
  const sinceMs = NOW - DAY;
  const file = fileOf([row('old', { startedAt: sinceMs - 1 }), row('never-ran', { startedAt: undefined }), row('new')]);
  const got = readRecentHistoryRows(file, { sinceMs });
  assert.deepEqual(got.map((r) => r.id), ['never-ran', 'new']);
  assert.deepEqual(got, readHistoryRows(file).filter((r) => !Number.isFinite(r.startedAt) || r.startedAt >= sinceMs), 'today\'s treatment, windowed');
});

test('readRecentHistoryRows: include keeps one repo\'s lines, and the closing quote keeps a longer repo id out', () => {
  const file = fileOf([row('a1'), row('o1', { repo: 'rouge' }), row('a2'), row('prefix', { repo: `${REPO}-x` }), row('o2', { repo: 'rouge' })]);
  assert.deepEqual(readRecentHistoryRows(file, { sinceMs: NOW - DAY, include: INCLUDE }).map((r) => r.id), ['a1', 'a2']);
  assert.deepEqual(readRecentHistoryRows(file, { include: INCLUDE }).map((r) => r.id), ['a1', 'a2'], 'include alone filters without a window');
  assert.deepEqual(readRecentHistoryRows(file, { include: '"repo":"rouge"' }).map((r) => r.id), ['o1', 'o2']);
});

test('readRecentHistoryRows: a multi-byte line cut by a chunk boundary comes back whole', () => {
  const command = `${'漢字✓ü'.repeat(120_000)} sh scripts/test-run.sh`;
  const file = fileOf([row('a', { command }), row('b'), row('c', { command })]);
  assert.ok(fs.statSync(file).size > 2 * RECENT_READ_CHUNK_BYTES, 'the fixture spans several chunks');
  assert.deepEqual(readRecentHistoryRows(file, { sinceMs: NOW - DAY }), readHistoryRows(file));
});

test('readRecentHistoryRows: the window, not a byte cap, decides coverage', () => {
  const count = 90_000;
  const file = fileOf(Array.from({ length: count }, (_, i) => row(`r-${i}`, { startedAt: NOW - 3_600_000 - i })), 'history-window-big');
  const size = fs.statSync(file).size;
  assert.ok(size > 16 * 1024 * 1024, `${size} bytes: a 16 MiB cap could not cover this window`);
  const got = readRecentHistoryRows(file, { sinceMs: NOW - DAY, include: INCLUDE });
  assert.equal(got.length, count, 'every in-window row, from the first chunk to the last');
  assert.deepEqual([got[0].id, got.at(-1).id], ['r-0', `r-${count - 1}`], 'in file order');
  assert.equal(got.every((r) => r.repo === REPO), true);
});

test('readRecentHistoryRows: a sparse repo in a large history reads only the window share of the file, and returns the same rows', () => {
  const count = 100_000;
  const span = 60 * DAY;
  const at = (i) => Math.round(NOW - span + (i / count) * span);
  const file = fileOf(
    Array.from({ length: count }, (_, i) => (i % 500 === 0 ? row(`t-${i}`, { startedAt: at(i), endedAt: at(i) + 100_000 }) : row(`b-${i}`, { repo: 'bulk', startedAt: at(i), endedAt: at(i) + 100_000 }))),
    'history-window-cost',
  );
  const sinceMs = NOW - 14 * DAY;
  const realReadSync = fs.readSync;
  let bytesRead = 0;
  fs.readSync = (...args) => {
    const got = realReadSync(...args);
    bytesRead += got;
    return got;
  };
  let windowed;
  try {
    windowed = readRecentHistoryRows(file, { sinceMs, include: INCLUDE });
  } finally {
    fs.readSync = realReadSync;
  }
  const full = readHistoryRows(file);
  assert.deepEqual(windowed, full.filter((r) => r.repo === REPO && r.startedAt >= sinceMs), 'the same rows a full read filtered the same way gives');
  assert.ok(windowed.length > 0 && windowed.length < count, `${windowed.length} of ${count} rows`);
  const size = fs.statSync(file).size;
  assert.ok(bytesRead < size * 0.4, `read ${bytesRead} of ${size} bytes: about 15 of 60 days, not the whole file`);
});
