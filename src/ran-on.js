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

export function relayedRanOn(result) {
  const ranOn = result?.ranOn;
  if (!named(ranOn?.host) || !RAN_ON_TRANSPORTS.includes(ranOn.transport) || !named(ranOn.ticket)) return {};
  return { ranOn: ranOnRecord(ranOn) };
}
