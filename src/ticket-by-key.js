import { isDispatchKey } from './dispatch-key.js';
import { loadGlobalConfigOrError } from './config.js';
import { pullServices } from './pull-services.js';
import { ensureStateDirs } from './state.js';

/** The caller's own live ticket under `dispatchKey` as the queue shows it, or null when there is none. Throws when the queue cannot answer. */
export async function lookupTicketByKey(cfg, root, dispatchKey, { servicesFor = pullServices } = {}) {
  const services = servicesFor(cfg, root);
  if (!services.available) throw new Error(`pull dispatch is not available here: ${services.reason}`);
  return (await services.queue.ticketByKey(dispatchKey)) ?? null;
}

/** `lane status --dispatch-key <k>`: the ticket's id, state and host. Exit 0 found, 1 none (or the queue cannot answer), 64 an invalid key. */
export async function ticketByKeyCommand({ dispatchKey, json = false, servicesFor } = {}) {
  if (!isDispatchKey(dispatchKey)) {
    process.stderr.write('lane status: --dispatch-key must be 1-200 characters of [A-Za-z0-9:._/-]\n');
    return { exitCode: 64 };
  }
  const { config, error } = loadGlobalConfigOrError();
  if (error) {
    process.stderr.write(`lane status: global config unreadable (${error})\n`);
    return { exitCode: 1 };
  }
  let ticket;
  try {
    ticket = await lookupTicketByKey(config, ensureStateDirs().root, dispatchKey, { servicesFor });
  } catch (err) {
    process.stderr.write(`lane status: dispatch key lookup failed: ${err.message}\n`);
    return { exitCode: 1 };
  }
  if (ticket === null) {
    process.stdout.write(json ? `${JSON.stringify({ dispatchKey, found: false })}\n` : `no ticket for dispatch key ${dispatchKey}\n`);
    return { exitCode: 1 };
  }
  const { id, state, host } = ticket;
  process.stdout.write(json ? `${JSON.stringify({ dispatchKey, found: true, id, state, host })}\n` : `${id} ${state} ${host ?? '-'}\n`);
  return { exitCode: 0 };
}
