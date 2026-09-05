import fs from 'node:fs';
import os from 'node:os';
import { paths, atomicWriteJson, readJsonSafe } from './state.js';

/**
 * Current 1-minute load average. Tests inject a value via
 * LANE_BROKER_LOADAVG_FILE (a path whose first line is the load to report),
 * so the hysteresis logic can be driven deterministically.
 */
export function readLoadAvg() {
  const file = process.env.LANE_BROKER_LOADAVG_FILE;
  if (file) {
    try {
      const first = fs.readFileSync(file, 'utf8').split('\n')[0].trim();
      const val = Number(first);
      if (Number.isFinite(val)) return val;
    } catch {
      // fall through to the real loadavg
    }
  }
  return os.loadavg()[0];
}

/**
 * Pure hysteresis transition: closes immediately above loadClose; once
 * closed, reopens only after `loadOpenSamples` consecutive samples below
 * loadOpen. Samples at or above loadOpen (but below loadClose) reset the
 * consecutive-under counter without reopening.
 */
export function updateGateState(prev, load, { loadClose, loadOpen, loadOpenSamples }) {
  const state = prev ? { ...prev } : { closed: false, consecutiveUnder: 0 };
  if (load > loadClose) {
    state.closed = true;
    state.consecutiveUnder = 0;
  } else if (state.closed) {
    if (load < loadOpen) {
      state.consecutiveUnder += 1;
      if (state.consecutiveUnder >= loadOpenSamples) {
        state.closed = false;
        state.consecutiveUnder = 0;
      }
    } else {
      state.consecutiveUnder = 0;
    }
  }
  state.lastLoad = load;
  state.lastSampleAt = Date.now();
  return state;
}

/** Sample the current load, update the persisted gate state, and return it. Caller must hold the global lock. */
export function sampleAndUpdateGate(root, cfg, sampler = readLoadAvg) {
  const file = paths(root).loadGate;
  const prev = readJsonSafe(file);
  const load = sampler();
  const next = updateGateState(prev, load, cfg);
  atomicWriteJson(file, next);
  return next;
}

export function readGateState(root) {
  return readJsonSafe(paths(root).loadGate) || { closed: false, consecutiveUnder: 0 };
}
