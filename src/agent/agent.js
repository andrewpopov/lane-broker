import { LdClient } from './ld-client.js';
import { ExecutionTable, claimOwner, ensureAgentDirs, writeStatus } from './store.js';

/** Directories durable, then the single-owner claim, then the records, then the first database call: nothing reaches the queue before its local state is safe. */
export async function openAgentCore({ remoteRoot, config, version, log = (line) => process.stderr.write(`${line}\n`), onWake, onCall, ...clientOptions }) {
  const paths = ensureAgentDirs(remoteRoot);
  await claimOwner(remoteRoot);
  const table = new ExecutionTable(remoteRoot);
  let listenError = null;
  const status = (extra = {}) => ({
    host: ld.registered?.host, executions: table.all().map((r) => ({ execId: r.execId, subphase: r.subphase })), listenError, ...extra,
  });
  const wake = (event) => {
    if (event.reason === 'listen-failed') {
      const what = event.error?.code ?? event.error?.message;
      listenError = { at: Date.now(), error: what };
      log(`listen-failed: ${what}`);
      writeStatus(remoteRoot, status());
    }
    onWake?.(event);
  };
  const ld = new LdClient({
    pg: config.pg, version, schedule: config.schedule, onWake: wake, onCall, ...clientOptions,
  });
  try {
    await ld.open();
  } catch (err) {
    await ld.close();
    throw err;
  }
  return {
    paths, table, ld,
    tick: (extra = {}) => writeStatus(remoteRoot, status(extra)),
    close: () => ld.close(),
  };
}
