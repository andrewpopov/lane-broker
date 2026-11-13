import fs from 'node:fs';
import path from 'node:path';
import { connect } from '../../src/lane-db.js';
import { createQueueClient } from '../../src/queue-client.js';
import { logins } from '../lane-dispatch-fixture.js';
import { makeTmpDir } from './tmp.js';

export const QUEUE_URL = 'postgres://lane_submit_s1@queue.example:5432/lane_queue?sslmode=verify-full';
export const CA_TEXT = '-----BEGIN CERTIFICATE-----\nnot-a-real-ca\n-----END CERTIFICATE-----\n';

export function secretFiles(t, password = 'queue-pass\n') {
  const dir = makeTmpDir('queue-client-', t);
  const passwordFile = path.join(dir, 'pw');
  const caFile = path.join(dir, 'ca.pem');
  fs.writeFileSync(passwordFile, password);
  fs.writeFileSync(caFile, CA_TEXT);
  return { passwordFile, caFile };
}

/** A queue client whose connections reach the harness database as `login`; `onConnect(client, n)` may break the connection it just opened. */
export function harnessClient(t, world, { login = logins.s1, onConnect, sleep = async () => {}, random = () => 0.5 } = {}) {
  const seen = { configs: [], sleeps: [] };
  const queue = createQueueClient({
    url: QUEUE_URL,
    ...secretFiles(t),
    connectClient: async (config) => {
      seen.configs.push(config);
      const client = await connect(world.cluster.conn(world.db, login));
      client.on('error', () => {});
      await onConnect?.(client, seen.configs.length);
      return client;
    },
    sleep: async (ms) => { seen.sleeps.push(ms); await sleep(ms); },
    random,
  });
  return { queue, seen };
}
