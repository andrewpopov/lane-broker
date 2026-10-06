import { parseArgs } from 'node:util';
import { parseByteSize } from '../resources.js';
import { createStore } from './server.js';
import { createHmacVerifier } from './auth.js';
import { rebuildWatermark } from './replicate.js';
import { StoreClient } from './client.js';

const USAGE = `usage: lane-store <command> [options]
  serve      --root DIR --listen HOST:PORT [--replica [--replica-grace-hours N] | --replicate-to URL [--replicate-interval-s N]]
             [--max-blob-bytes N] [--cap-bytes N] [--sweep-interval-s N] [--maintenance-interval-s N]
             secret: LANE_STORE_SECRET; with --replicate-to also LANE_STORE_REPLICA_TOKEN (role=replica)
             replication runs INSIDE this process, one round every --replicate-interval-s (default 300)
  compare    --url URL [--deep] [--repair]          asks the running primary to compare itself with its replica;
             token LANE_STORE_ADMIN_TOKEN; exit 2 on divergence or a lost object
  rebuild-watermark --root DIR --seq N              OFFLINE: recompute replication.json (format 2); refuses while a server holds the store`;

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
      url: { type: 'string' },
      'max-blob-bytes': { type: 'string' },
      'cap-bytes': { type: 'string' },
      'sweep-interval-s': { type: 'string' },
      'maintenance-interval-s': { type: 'string' },
      'replicate-to': { type: 'string' },
      'replicate-interval-s': { type: 'string' },
      replica: { type: 'boolean' },
      'replica-grace-hours': { type: 'string' },
      deep: { type: 'boolean' },
      repair: { type: 'boolean' },
      seq: { type: 'string' },
    },
  });

  if (command === 'serve') {
    const secret = need(env.LANE_STORE_SECRET, 'LANE_STORE_SECRET');
    const [host, port] = splitListen(values.listen);
    const target = values['replicate-to'];
    const s = createStore({
      root: need(values.root, '--root'),
      replicaMode: !!values.replica,
      replicaGraceHours: values['replica-grace-hours'] === undefined ? undefined : Number(values['replica-grace-hours']),
      verifier: createHmacVerifier(secret),
      maxBlobBytes: byteOption(values['max-blob-bytes'], '--max-blob-bytes'),
      capBytes: byteOption(values['cap-bytes'], '--cap-bytes'),
      sweepIntervalMs: Number(values['sweep-interval-s'] ?? 3600) * 1000,
      maintenanceIntervalMs: Number(values['maintenance-interval-s'] ?? 30) * 1000,
      onLockLost: (err) => {
        console.error(`lane-store: ${err.message}; another process owns this store, shutting down`);
        s.close().finally(() => process.exit(70));
      },
      replicateTo: target ? new StoreClient({ baseUrl: target, token: need(env.LANE_STORE_REPLICA_TOKEN, 'LANE_STORE_REPLICA_TOKEN') }) : undefined,
      replicateIntervalMs: Number(values['replicate-interval-s'] ?? 300) * 1000,
    });
    const addr = await s.listen(host, port);
    console.log(`lane-store listening on ${addr.address}:${addr.port} root=${values.root}`);
    const stop = () => s.close().then(() => process.exit(0));
    process.on('SIGTERM', stop);
    process.on('SIGINT', stop);
    return 0;
  }

  if (command === 'compare') {
    const admin = new StoreClient({ baseUrl: need(values.url, '--url'), token: need(env.LANE_STORE_ADMIN_TOKEN, 'LANE_STORE_ADMIN_TOKEN') });
    const diff = await admin.compare({ deep: !!values.deep, repair: !!values.repair });
    console.log(JSON.stringify(diff));
    return diff.ok ? 0 : 2;
  }

  if (command === 'rebuild-watermark') {
    const seq = Number(need(values.seq, '--seq'));
    if (!Number.isInteger(seq) || seq < 0) throw new Error('--seq must be a non-negative integer');
    console.log(JSON.stringify(rebuildWatermark(need(values.root, '--root'), seq)));
    return 0;
  }

  throw new Error(USAGE);
}
