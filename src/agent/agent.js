import { executionIdOf } from '../state.js';
import { LdClient } from './ld-client.js';
import { ExecutionTable, claimOwner, ensureAgentDirs, reassertPhase, reassertVerdict, writeStatus } from './store.js';

const TRANSITION = { killing: 'KILLING', done: 'DONE' };

function applyReassert(table, items, results, log) {
  items.forEach((item, i) => {
    const record = table.get(executionIdOf(item.id, item.epoch));
    if (!record || reassertPhase(record) === null) return;
    const verdict = reassertVerdict(record, results[i]);
    if (verdict.anomaly) log(`anomaly: ${record.execId} reasserted ${item.phase} but the queue holds it ${results[i].state}; treating it as fenced (${verdict.next})`);
    if (verdict.next !== 'resume') table.put({ ...record, subphase: TRANSITION[verdict.next], from: record.subphase, ...(verdict.tombstone && { tombstone: true }) });
  });
}

/** Directories durable, then the single-owner claim, then the records, then the first database call: nothing reaches the queue before its local state is safe. */
export async function openAgentCore({ remoteRoot, config, version, log = (line) => process.stderr.write(`${line}\n`), onWake, onCall, ...clientOptions }) {
  const paths = ensureAgentDirs(remoteRoot);
  await claimOwner(remoteRoot);
  const table = new ExecutionTable(remoteRoot);
  const ld = new LdClient({
    pg: config.pg, version, schedule: config.schedule, snapshot: () => table.reassertItems(), onWake, onCall,
    onReassert: (items, results) => applyReassert(table, items, results, log), ...clientOptions,
  });
  try {
    await ld.open();
  } catch (err) {
    await ld.close();
    throw err;
  }
  return {
    paths, table, ld,
    tick: (extra = {}) => writeStatus(remoteRoot, { gen: ld.gen, host: ld.registered.host, executions: table.all().map((r) => ({ execId: r.execId, subphase: r.subphase })), ...extra }),
    close: () => ld.close(),
  };
}
