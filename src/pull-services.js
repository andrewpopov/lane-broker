import path from 'node:path';
import { loadGlobalConfigOrError } from './config.js';
import { publicationBindingsFor } from './pull-publication.js';
import { createQueueClient } from './queue-client.js';
import { createS3Client, loadS3Credentials } from './s3-client.js';
import { paths } from './state.js';

const unavailable = (reason) =>
  new Proxy({}, { get: () => async () => { throw new Error(`pull dispatch is not available here: ${reason}`); } });

// no `enabled` check on purpose: tickets in flight must stay recoverable after the switch is turned off
export function pullServices(cfg) {
  const pull = cfg?.lanePull;
  const missing = [['laneQueueUrl', cfg?.laneQueueUrl], ['lanePull.passwordFile', pull?.passwordFile], ['lanePull.caFile', pull?.caFile],
    ['lanePull.rustfs.endpoint', pull?.rustfs?.endpoint], ['lanePull.rustfs.credentialsEnvFile', pull?.rustfs?.credentialsEnvFile]].filter(([, value]) => typeof value !== 'string');
  if (missing.length > 0) return { available: false, reason: `config lacks ${missing.map(([name]) => name).join(', ')}`, queue: unavailable('no queue'), s3: unavailable('no store') };
  const { endpoint, region, credentialsEnvFile } = pull.rustfs;
  try {
    const creds = loadS3Credentials(credentialsEnvFile);
    return {
      available: true,
      queue: createQueueClient({ url: cfg.laneQueueUrl, passwordFile: pull.passwordFile, caFile: pull.caFile }),
      s3: createS3Client({ endpoint, region, creds }),
    };
  } catch (err) {
    return { available: false, reason: err.message, queue: unavailable(err.message), s3: unavailable(err.message) };
  }
}

export function pullContextFor(root, cfg, services, ticket, extra = {}) {
  const { id, generation } = ticket;
  return {
    ...extra,
    root,
    id,
    generation,
    queue: services.queue,
    s3: services.s3,
    cfg: cfg?.lanePull,
    logPath: path.join(paths(root).logs, `${id}.log`),
    resultPath: path.join(paths(root).results, `${id}.json`),
    ...publicationBindingsFor(root, id),
  };
}

export function pullContextFactory(root, cfg, extra = {}) {
  const services = pullServices(cfg);
  return (ticket) => pullContextFor(root, cfg, services, ticket, extra);
}

export const configuredPullContext = (root, extra = {}) => pullContextFactory(root, loadGlobalConfigOrError().config, extra);
