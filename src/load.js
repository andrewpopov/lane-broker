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

/** Fingerprint of the threshold fields that govern hysteresis, so a config
 *  change can be detected against the shared, unversioned gate state. */
function thresholdFingerprint({ loadClose, loadOpen, loadOpenSamples }) {
  return `${loadClose}|${loadOpen}|${loadOpenSamples}`;
}

/**
 * Pure hysteresis transition: closes immediately above loadClose; once
 * closed, reopens only after `loadOpenSamples` consecutive samples below
 * loadOpen. Samples at or above loadOpen (but below loadClose) reset the
 * consecutive-under counter without reopening.
 *
 * Concurrent supervisors reload their own config independently and all write
 * this one shared state, so a threshold edit lands mid-countdown for some of
 * them and not others. Blending a countdown that started under the old
 * thresholds with samples judged under the new ones would let the decision
 * satisfy neither config that was ever installed. So whenever the incoming
 * thresholds don't match the fingerprint stamped on the stored state, the
 * countdown restarts from zero under the new thresholds. `closed` is left
 * untouched: a config edit is not evidence the machine got quieter.
 */
export function updateGateState(prev, load, cfg) {
  const { loadClose, loadOpen, loadOpenSamples } = cfg;
  const fingerprint = thresholdFingerprint(cfg);
  const state = prev ? { ...prev } : { closed: false, consecutiveUnder: 0 };
  if (state.fingerprint !== fingerprint) {
    state.consecutiveUnder = 0;
    state.fingerprint = fingerprint;
  }
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
