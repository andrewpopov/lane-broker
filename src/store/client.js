import http from 'node:http';
import fs from 'node:fs';
import { parseObjectPath } from './ids.js';

export class StoreHttpError extends Error {
  constructor(status, body) {
    super(`lane-store ${status}: ${body && body.error ? body.error : JSON.stringify(body)}`);
    this.name = 'StoreHttpError';
    this.status = status;
    this.body = body;
  }
}

/** Minimal client over `http` (zero dependencies). `baseUrl` is `http://host:port`; `token` is a bearer token. */
export class StoreClient {
  constructor({ baseUrl, token }) {
    this.baseUrl = new URL(baseUrl);
    this.token = token;
  }

  request(method, pathname, { body, contentLength } = {}) {
    return new Promise((resolve, reject) => {
      const headers = { authorization: `Bearer ${this.token}` };
      if (body !== undefined) headers['content-length'] = contentLength ?? Buffer.byteLength(body);
      const req = http.request({ host: this.baseUrl.hostname, port: this.baseUrl.port, method, path: pathname, headers }, (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
        res.on('error', reject);
      });
      req.on('error', reject);
      if (body && typeof body.pipe === 'function') body.pipe(req);
      else req.end(body);
    });
  }

  async json(method, pathname, opts) {
    const res = await this.request(method, pathname, opts);
    let parsed = null;
    try {
      parsed = JSON.parse(res.body.toString('utf8'));
    } catch {
      parsed = res.body.toString('utf8');
    }
    if (res.status >= 300) throw new StoreHttpError(res.status, parsed);
    return parsed;
  }

  async has(shas) {
    return (await this.json('POST', '/has', { body: JSON.stringify({ shas }) })).missing;
  }

  putBlob(sha, data) {
    return this.json('PUT', `/blobs/${sha}`, { body: data });
  }

  putBlobFile(sha, file) {
    return this.json('PUT', `/blobs/${sha}`, { body: fs.createReadStream(file), contentLength: fs.statSync(file).size });
  }

  putManifest(job, doc) {
    return this.json('PUT', `/manifests/${job}`, { body: JSON.stringify(doc) });
  }

  getManifest(job) {
    return this.json('GET', `/manifests/${job}`);
  }

  async getBlob(sha) {
    const res = await this.request('GET', `/blobs/${sha}`);
    if (res.status !== 200) throw new StoreHttpError(res.status, JSON.parse(res.body.toString('utf8')));
    return res.body;
  }

  /** Replication shipping: a journal entry's local file goes to the endpoint for its kind. */
  putObject(entry, file) {
    const parsed = parseObjectPath(entry.path);
    return parsed.kind === 'blob' ? this.putBlobFile(parsed.sha, file) : this.json('PUT', `/manifests/${parsed.job}`, { body: fs.readFileSync(file) });
  }

  /** `{sha256, size}` the peer computed by re-reading the object from its own disk, or null if it has none. */
  async verifyObject(relPath) {
    try {
      return await this.json('GET', `/verify/${relPath}`);
    } catch (err) {
      if (err instanceof StoreHttpError && err.status === 404) return null;
      throw err;
    }
  }

  async list({ deep = false } = {}) {
    return (await this.json('GET', `/list${deep ? '?deep=1' : ''}`)).objects;
  }

  rejournal(paths) {
    return this.json('POST', '/admin/rejournal', { body: JSON.stringify({ paths }) });
  }
}

/**
 * Spec 8.1 submit flow: `POST /has`, upload only the missing blobs, register the manifest. `readFor(entry)` yields the
 * bytes of a file entry (a Buffer, or a `{file}` path to stream).
 */
export async function publishSnapshot(client, job, manifest, readFor) {
  const files = manifest.entries.filter((e) => e.type === 'file');
  const missing = new Set(await client.has([...new Set(files.map((e) => e.sha256))]));
  for (const entry of files) {
    if (!missing.delete(entry.sha256)) continue;
    const data = readFor(entry);
    await (Buffer.isBuffer(data) ? client.putBlob(entry.sha256, data) : client.putBlobFile(entry.sha256, data.file));
  }
  return client.putManifest(job, { manifestHash: manifest.manifestHash, entries: manifest.entries });
}
