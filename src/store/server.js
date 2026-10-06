import http from 'node:http';
import fs from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { ObjectStore, StoreError, MAX_MANIFEST_BYTES, listObjects } from './objects.js';
import { ROLES } from './auth.js';
import { sweep } from './retention.js';
import { renderMetrics } from './metrics.js';
import { compareStores } from './compare.js';
import { startReplication } from './replicate.js';
import { isSha256, isJobId, blobRelPath, parseObjectPath } from './ids.js';

const { SUBMIT, READ, REPLICA, ADMIN } = ROLES;
const WRITERS = [SUBMIT, REPLICA, ADMIN];
const PEERS = [REPLICA, ADMIN];

async function readBody(req, max) {
  const chunks = [];
  let n = 0;
  for await (const c of req) {
    n += c.length;
    if (n > max) throw new StoreError(413, 'request body too large');
    chunks.push(c);
  }
  return Buffer.concat(chunks);
}

async function readJson(req, max) {
  try {
    return JSON.parse((await readBody(req, max)).toString('utf8'));
  } catch (err) {
    if (err instanceof StoreError) throw err;
    throw new StoreError(400, 'body is not JSON');
  }
}

function send(res, status, body, headers = {}) {
  const isText = typeof body === 'string' || Buffer.isBuffer(body);
  res.writeHead(status, { 'content-type': isText ? 'text/plain; charset=utf-8' : 'application/json', ...headers });
  res.end(isText ? body : JSON.stringify(body));
}

/** Per-kind concurrency gate (spec 8.2: 3 uploads / 4 downloads). */
function gate(limit) {
  let active = 0;
  return async (fn) => {
    if (active >= limit) throw new StoreError(503, 'too many concurrent transfers');
    active += 1;
    try {
      return await fn();
    } finally {
      active -= 1;
    }
  };
}

/**
 * `createStore({root, verifier, ...})` returns `{store, server, listen(host, port), close()}`. `verifier` is
 * `async (bearerToken) => claims | null` (see auth.js). Reads are job-scoped: a `read` token may fetch only the
 * manifest of `claims.job` and the blobs that manifest references.
 */
export function createStore({ root, verifier, maxUploads = 3, maxDownloads = 4, downloadIdleMs = 30_000, sweepIntervalMs = 0, maintenanceIntervalMs = 30_000, replicateTo, replicateIntervalMs = 300_000, ...storeOpts }) {
  if (replicateTo && storeOpts.replicaMode) throw new Error('a replica does not replicate onward (--replica and --replicate-to are exclusive)');
  const store = new ObjectStore(root, storeOpts);
  const upload = gate(maxUploads);
  const download = gate(maxDownloads);

  async function authorize(req, roles) {
    const m = /^Bearer (.+)$/.exec(req.headers.authorization ?? '');
    const claims = m ? await verifier(m[1]) : null;
    if (!claims) throw new StoreError(401, 'missing or invalid token');
    if (!roles.includes(claims.role)) throw new StoreError(403, 'role not permitted');
    return claims;
  }

  const requireJobScope = (claims, job) => {
    if (claims.role === READ || (claims.role === SUBMIT && claims.job !== undefined)) {
      if (claims.job !== job) throw new StoreError(403, 'token is scoped to another job');
    }
  };

  async function route(req, res, url, parts) {
    const method = req.method;
    if (method === 'GET' && url.pathname === '/healthz') return send(res, 200, 'ok\n');
    if (method === 'GET' && url.pathname === '/metrics') return send(res, 200, renderMetrics(store, store.now()));

    if (method === 'POST' && url.pathname === '/has') {
      await authorize(req, WRITERS);
      const { shas } = await readJson(req, 8 * 1024 * 1024);
      if (!Array.isArray(shas) || !shas.every(isSha256)) throw new StoreError(400, 'shas must be sha256 hex strings');
      return send(res, 200, { missing: shas.filter((s) => !store.hasBlob(s)) });
    }

    if (parts[0] === 'blobs' && parts.length === 2) {
      const sha = parts[1];
      if (!isSha256(sha)) throw new StoreError(400, 'bad sha256');
      if (method === 'PUT') {
        await authorize(req, WRITERS);
        const len = Number(req.headers['content-length']);
        if (!Number.isInteger(len) || len < 0) throw new StoreError(411, 'Content-Length required (whole-file uploads only)');
        return upload(async () => send(res, 200, await store.putBlob(sha, req, len)));
      }
      if (method === 'GET') {
        const claims = await authorize(req, [READ, REPLICA, ADMIN]);
        if (claims.role === READ) {
          const allowed = claims.job === undefined ? null : store.jobBlobs(claims.job);
          if (!allowed || !allowed.has(sha)) throw new StoreError(403, 'blob is not part of this job');
        }
        return download(async () => {
          const file = store.openRead(blobRelPath(sha));
          if (!file) throw new StoreError(404, 'no such blob');
          const src = fs.createReadStream(null, { fd: file.fd });
          res.writeHead(200, { 'content-type': 'application/octet-stream', 'content-length': file.size });
          res.setTimeout(downloadIdleMs, () => res.destroy()); // a client that stops reading is evicted, freeing its slot
          await pipeline(src, res).catch(() => {}); // pipeline destroys src (closing the fd) when res closes or errors
        });
      }
    }

    if (parts[0] === 'manifests' && parts.length === 2) {
      const job = parts[1];
      if (!isJobId(job)) throw new StoreError(400, 'bad job id');
      if (method === 'PUT') {
        const claims = await authorize(req, WRITERS);
        requireJobScope(claims, job);
        return upload(async () => send(res, 200, store.registerManifest(job, await readBody(req, MAX_MANIFEST_BYTES))));
      }
      if (method === 'GET') {
        const claims = await authorize(req, [READ, REPLICA, ADMIN]);
        requireJobScope(claims, job);
        const doc = store.readManifest(job);
        if (!doc) throw new StoreError(404, 'no such manifest');
        return send(res, 200, doc);
      }
    }

    if (method === 'GET' && parts[0] === 'verify') {
      await authorize(req, PEERS);
      const seen = await store.verifyObject(decodeURIComponent(url.pathname.slice('/verify/'.length)));
      if (!seen) throw new StoreError(404, 'no such object');
      return send(res, 200, seen);
    }
    if (method === 'GET' && url.pathname === '/list') {
      await authorize(req, PEERS);
      return send(res, 200, { objects: await listObjects(store.root, { deep: url.searchParams.get('deep') === '1' }) });
    }

    if (parts[0] === 'jobs' && parts[2] === 'terminal' && method === 'PUT') {
      const claims = await authorize(req, PEERS);
      const at = claims.role === REPLICA && url.searchParams.has('at') ? Number(url.searchParams.get('at')) : store.now();
      if (!Number.isFinite(at)) throw new StoreError(400, 'bad at');
      return send(res, 200, { changed: store.markTerminal(parts[1], at) });
    }
    if (parts[0] === 'jobs' && parts[2] === 'meta' && method === 'GET') {
      await authorize(req, PEERS);
      if (!store.readManifest(parts[1])) throw new StoreError(404, 'no such job');
      return send(res, 200, store.meta(parts[1]));
    }
    if (parts[0] === 'pins' && parts.length === 2 && (method === 'PUT' || method === 'DELETE')) {
      await authorize(req, PEERS);
      return send(res, 200, { changed: store.setPinned(parts[1], method === 'PUT') });
    }
    if (method === 'DELETE' && parts[0] === 'objects') {
      await authorize(req, PEERS);
      return send(res, 200, { deleted: store.deleteObject(decodeURIComponent(url.pathname.slice('/objects/'.length))) });
    }
    if (method === 'POST' && url.pathname === '/admin/rejournal') {
      await authorize(req, [ADMIN]);
      const { paths } = await readJson(req, 8 * 1024 * 1024);
      if (!Array.isArray(paths) || !paths.every((p) => parseObjectPath(p))) throw new StoreError(400, 'paths must be object paths');
      return send(res, 200, { added: (await store.rejournal(paths)).length });
    }
    if (method === 'POST' && url.pathname === '/admin/compare') {
      await authorize(req, [ADMIN]);
      if (!replicateTo) throw new StoreError(400, 'no replica target is configured (--replicate-to)');
      const diff = await compareStores({ store, replica: replicateTo, deep: url.searchParams.get('deep') === '1' });
      if (!diff.ok && url.searchParams.get('repair') === '1') {
        const paths = [...new Set([...diff.notInJournal, ...diff.missingAtReplica, ...diff.mismatched])];
        diff.rejournaled = (await store.rejournal(paths)).length;
      }
      return send(res, 200, diff);
    }
    if (method === 'POST' && url.pathname === '/admin/sweep') {
      await authorize(req, [ADMIN]);
      return send(res, 200, sweep(store));
    }
    throw new StoreError(404, 'no such route');
  }

  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url, 'http://store.invalid');
      await route(req, res, url, url.pathname.split('/').filter(Boolean));
    } catch (err) {
      if (!(err instanceof StoreError)) {
        console.error('lane-store: unexpected error', err);
        if (!res.headersSent) send(res, 500, { error: 'internal error' });
        return;
      }
      const headers = { connection: 'close', ...(err.status === 503 ? { 'retry-after': '1' } : {}) };
      if (!res.headersSent) send(res, err.status, { error: err.message, ...err.extra }, headers);
      req.resume();
    }
  });

  const timers = [];
  if (sweepIntervalMs > 0) timers.push(setInterval(() => sweep(store), sweepIntervalMs));
  if (maintenanceIntervalMs > 0) {
    timers.push(setInterval(() => {
      try {
        store.maintain();
      } catch (err) {
        console.error(`lane-store: maintenance failed: ${err.message}`);
      }
    }, maintenanceIntervalMs));
  }
  timers.forEach((t) => t.unref());
  const replication = replicateTo ? startReplication({ store, replica: replicateTo, intervalMs: replicateIntervalMs, now: store.now }) : null;

  return {
    store,
    server,
    replicator: replication?.replicator,
    /** Bind exactly one interface; a wildcard address is refused so the store can never listen on every NIC. */
    listen(host, port = 0) {
      if (!host || host === '0.0.0.0' || host === '::' || host === '::0') {
        return Promise.reject(new Error(`refusing to bind wildcard address "${host}": give one interface address`));
      }
      return new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(port, host, () => resolve(server.address()));
      });
    },
    async close() {
      timers.forEach((t) => clearInterval(t));
      await replication?.stop(); // drain the in-flight round BEFORE the journal closes and the lock is released
      return new Promise((resolve) => {
        server.close(() => {
          store.close();
          resolve();
        });
        server.closeAllConnections();
      });
    },
  };
}
