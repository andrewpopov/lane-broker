import test from 'node:test';
import assert from 'node:assert/strict';
import { processStartTime } from '../src/lease.js';

test('processStartTime is pinned to a fixed locale: the caller\'s ambient LC_TIME must not change the recorded value', () => {
  const prevLcTime = process.env.LC_TIME;
  const prevLang = process.env.LANG;
  try {
    process.env.LC_TIME = 'C';
    process.env.LANG = 'C';
    const c = processStartTime(process.pid);

    process.env.LC_TIME = 'de_DE.UTF-8';
    process.env.LANG = 'de_DE.UTF-8';
    const de = processStartTime(process.pid);

    assert.equal(c, de, 'a supervisor spawned under one LC_TIME and reaped under another must compare equal, not mismatch');
  } finally {
    process.env.LC_TIME = prevLcTime;
    process.env.LANG = prevLang;
  }
});
