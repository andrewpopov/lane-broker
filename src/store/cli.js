import { parseArgs } from 'node:util';
import { parseByteSize } from '../resources.js';
import { createStore } from './server.js';
import { createHmacVerifier } from './auth.js';
import { Replicator } from './replicate.js';
import { compareStores } from './compare.js';
import { StoreClient } from './client.js';

const USAGE = `usage: lane-store <command> [options]
  serve      --root DIR --listen HOST:PORT [--replica] [--max-blob-bytes N] [--cap-bytes N] [--sweep-interval-s N]
             secret: LANE_STORE_SECRET
  replicate  --root DIR --replica-url URL            one journal-ordered round; token: LANE_STORE_REPLICA_TOKEN
  compare    --root DIR --replica-url URL [--deep] [--repair --primary-url URL]
             weekly backstop; exit 2 on divergence; --repair re-journals objects the journal lacks (tokens: LANE_STORE_REPLICA_TOKEN, LANE_STORE_ADMIN_TOKEN)`;

function need(value, name) {
  if (!value) throw new Error(`missing ${name}\n${USAGE}`);
  return value;
}

function splitListen(listen) {
  const i = need(listen, '--listen').lastIndexOf(':');
  if (i <= 0) throw new Error('--listen must be HOST:PORT');
  return [listen.slice(0, i).replace(/^\[|\]$/g, ''), Number(listen.slice(i + 1))];
}

function byteOption(value, name) {
  if (value === undefined) return undefined;
  const n = parseByteSize(value);
  if (n === null) throw new Error(`bad ${name}: ${value}`);
  return n;
}

export async function storeCli(argv, env = process.env) {
  const [command, ...rest] = argv;
  const { values } = parseArgs({
    args: rest,
    options: {
      root: { type: 'string' },
      listen: { type: 'string' },
      'max-blob-bytes': { type: 'string' },
      'cap-bytes': { type: 'string' },
      'sweep-interval-s': { type: 'string' },
      'replica-url': { type: 'string' },
      'primary-url': { type: 'string' },
      deep: { type: 'boolean' },
      replica: { type: 'boolean' },
      repair: { type: 'boolean' },
    },
  });
  const root = need(values.root, '--root');

  if (command === 'serve') {
    const secret = need(env.LANE_STORE_SECRET, 'LANE_STORE_SECRET');
    const [host, port] = splitListen(values.listen);
    const s = createStore({
      root,
      replicaMode: !!values.replica,
      verifier: createHmacVerifier(secret),
      maxBlobBytes: byteOption(values['max-blob-bytes'], '--max-blob-bytes'),
      capBytes: byteOption(values['cap-bytes'], '--cap-bytes'),
      sweepIntervalMs: Number(values['sweep-interval-s'] ?? 3600) * 1000,
    });
    const addr = await s.listen(host, port);
    console.log(`lane-store listening on ${addr.address}:${addr.port} root=${root}`);
    const stop = () => s.close().then(() => process.exit(0));
    process.on('SIGTERM', stop);
    process.on('SIGINT', stop);
    return 0;
  }

  const replica = new StoreClient({ baseUrl: need(values['replica-url'], '--replica-url'), token: need(env.LANE_STORE_REPLICA_TOKEN, 'LANE_STORE_REPLICA_TOKEN') });

  if (command === 'replicate') {
    const result = await new Replicator({ root, replica }).runOnce();
    console.log(JSON.stringify(result));
    return result.ok ? 0 : 1;
  }

  if (command === 'compare') {
    const diff = await compareStores({ root, replica, deep: !!values.deep });
    if (!diff.ok && values.repair) {
      const primary = new StoreClient({ baseUrl: need(values['primary-url'], '--primary-url'), token: need(env.LANE_STORE_ADMIN_TOKEN, 'LANE_STORE_ADMIN_TOKEN') });
      const paths = [...new Set([...diff.notInJournal, ...diff.missingAtReplica, ...diff.mismatched])];
      diff.rejournaled = (await primary.rejournal(paths)).added;
    }
    console.log(JSON.stringify(diff));
    return diff.ok ? 0 : 2;
  }

  throw new Error(USAGE);
}
