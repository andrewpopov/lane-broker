import fs from 'node:fs';
import os from 'node:os';
import { fmtMB } from './memory.js';

export const RAN_ON_TRANSPORTS = Object.freeze(['local', 'push', 'pull']);

const positive = (n) => (Number.isFinite(n) && n > 0 ? n : null);
const named = (s) => (typeof s === 'string' && s !== '' ? s : null);

// PKG-402: `ticket` is the submitter's id, never a runner's own lane id, so the line names what the reader submitted.
export function ranOnRecord({ host = os.hostname(), transport, ticket, cpuCores, memoryBytes, runner = null }) {
  return { host, transport, ticket, cpuCores: positive(cpuCores), memoryBytes: positive(memoryBytes), runner: named(runner) };
}

export function ranOnLine({ host, transport, ticket, cpuCores, memoryBytes, runner }) {
  const mem = memoryBytes === null ? 'unset' : fmtMB(memoryBytes);
  return `lane: executing on ${host} via ${transport} (ticket ${ticket}, cpu ${cpuCores ?? 'unset'}, mem ${mem}${runner ? `, runner ${runner}` : ''})\n`;
}

const RAN_ON_LINE = /^lane: executing on (\S+) via (local|push|pull) \(ticket (\S+), cpu (\S+), mem (?:unset|(\d+)MB)(?:, runner (\S+))?\)$/m;
const LOG_HEAD_BYTES = 64 * 1024;

function logHead(logPath) {
  try {
    const fd = fs.openSync(logPath, 'r');
    try {
      const buf = Buffer.alloc(LOG_HEAD_BYTES);
      return buf.subarray(0, fs.readSync(fd, buf, 0, LOG_HEAD_BYTES, 0)).toString('utf8');
    } finally {
      fs.closeSync(fd);
    }
  } catch {
    return '';
  }
}

// PKG-402: a cancelled push run has no runner result to relay, but the runner's line, written before any command output, reached our log.
export function ranOnFromLog(logPath, ticket) {
  const m = RAN_ON_LINE.exec(logHead(logPath));
  if (!m || m[3] !== ticket) return {};
  return { ranOn: ranOnRecord({ host: m[1], transport: m[2], ticket, cpuCores: Number(m[4]), memoryBytes: Number(m[5]) * 1024 * 1024, runner: m[6] }) };
}

export function relayedRanOn(result) {
  const ranOn = result?.ranOn;
  if (!named(ranOn?.host) || !RAN_ON_TRANSPORTS.includes(ranOn.transport) || !named(ranOn.ticket)) return {};
  return { ranOn: ranOnRecord(ranOn) };
}
