import crypto from 'node:crypto';
import fs from 'node:fs';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { amzTimestamp, awsUriEncode, canonicalQueryString, credentialScope, objectPath, sha256Hex, signatureFor } from './s3-presign.js';

/** The S3 operations the pull transport needs (path-style, header-signed SigV4) over `fetch`. Every operation has a deadline. */

export const DEFAULT_S3_DEADLINE_MS = 30_000;
export const DEFAULT_S3_PUT_DEADLINE_MS = 10 * 60_000;
const EMPTY_SHA256 = sha256Hex('');
const ACCESS_KEY_VAR = 'AWS_ACCESS_KEY_ID';
const SECRET_KEY_VAR = 'AWS_SECRET_ACCESS_KEY';

export class S3Error extends Error {
  constructor(message, { status = null, code = null, cause } = {}) {
    super(message, { cause });
    this.name = 'S3Error';
    this.status = status;
    this.code = code;
  }
}

/** Credentials from the env file named in config: `KEY=value` lines, 0600 and ours, both variables required. */
export function loadS3Credentials(file) {
  let stat;
  let text;
  try {
    stat = fs.statSync(file);
    text = fs.readFileSync(file, 'utf8');
  } catch (err) {
    throw new S3Error(`s3 credentials file ${file} unreadable (${err.code ?? err.message})`);
  }
  if ((stat.mode & 0o077) !== 0 || (typeof process.getuid === 'function' && stat.uid !== process.getuid())) {
    throw new S3Error(`s3 credentials file ${file} must be mode 0600 and owned by the current user`);
  }
  const vars = {};
  for (const line of text.split('\n')) {
    const match = /^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(line);
    if (match) vars[match[1]] = match[2].trim().replace(/^(['"])(.*)\1$/, '$2');
  }
  if (!vars[ACCESS_KEY_VAR] || !vars[SECRET_KEY_VAR]) throw new S3Error(`s3 credentials file ${file} must define ${ACCESS_KEY_VAR} and ${SECRET_KEY_VAR}`);
  return { accessKeyId: vars[ACCESS_KEY_VAR], secretAccessKey: vars[SECRET_KEY_VAR] };
}

const XML_ENTITIES = { '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', '&apos;': "'" };
const xmlText = (raw) => raw.replace(/&(amp|lt|gt|quot|apos);/g, (e) => XML_ENTITIES[e]);
const xmlTag = (xml, tag) => {
  const match = new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`).exec(xml);
  return match ? xmlText(match[1]) : null;
};
const xmlBlocks = (xml, tag) => [...xml.matchAll(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, 'g'))].map((m) => m[1]);

async function sha256AndSize(file) {
  const hash = crypto.createHash('sha256');
  let size = 0;
  for await (const chunk of fs.createReadStream(file)) {
    hash.update(chunk);
    size += chunk.length;
  }
  return { sha256: hash.digest('hex'), size };
}

async function spoolBody(body, file) {
  const hash = crypto.createHash('sha256');
  let size = 0;
  const tap = new Transform({
    transform(chunk, _encoding, done) {
      hash.update(chunk);
      size += chunk.length;
      done(null, chunk);
    },
  });
  const part = `${file}.${crypto.randomBytes(6).toString('hex')}.part`;
  try {
    await pipeline(Readable.fromWeb(body), tap, fs.createWriteStream(part));
    fs.renameSync(part, file);
  } catch (err) {
    fs.rmSync(part, { force: true });
    throw err;
  }
  return { sha256: hash.digest('hex'), size };
}

export function createS3Client({ endpoint, region, credentialsEnvFile, creds = loadS3Credentials(credentialsEnvFile), fetchImpl = globalThis.fetch, now = Date.now, deadlineMs = DEFAULT_S3_DEADLINE_MS }) {
  const base = new URL(endpoint);

  async function send(op, { method, bucket, key, query = {}, payloadSha = EMPTY_SHA256, body, contentLength, deadline, toFile }) {
    const path = key === undefined ? `/${awsUriEncode(bucket)}` : objectPath(bucket, key);
    const queryString = canonicalQueryString(query);
    const stamp = amzTimestamp(now());
    const signedHeaders = 'host;x-amz-content-sha256;x-amz-date';
    const canonicalRequest = [method, path, queryString, `host:${base.host}\nx-amz-content-sha256:${payloadSha}\nx-amz-date:${stamp}\n`, signedHeaders, payloadSha].join('\n');
    const signature = signatureFor({ secretAccessKey: creds.secretAccessKey, stamp, region, canonicalRequest });
    const headers = {
      'x-amz-content-sha256': payloadSha,
      'x-amz-date': stamp,
      authorization: `AWS4-HMAC-SHA256 Credential=${creds.accessKeyId}/${credentialScope(stamp, region)}, SignedHeaders=${signedHeaders}, Signature=${signature}`,
      ...(contentLength === undefined ? {} : { 'content-length': String(contentLength) }),
    };
    const url = `${base.protocol}//${base.host}${path}${queryString ? `?${queryString}` : ''}`;
    const signal = AbortSignal.timeout(deadline ?? deadlineMs);
    try {
      const res = await fetchImpl(url, { method, headers, signal, ...(body === undefined ? {} : { body, duplex: 'half' }) });
      const streamed = toFile && res.status === 200 ? await spoolBody(res.body, toFile) : null;
      const text = method === 'HEAD' || streamed ? '' : await res.text();
      return { res, text, streamed };
    } catch (err) {
      if (err?.name === 'TimeoutError' || err?.name === 'AbortError') throw new S3Error(`s3 ${op} timed out after ${deadline ?? deadlineMs}ms`, { code: 'Timeout' });
      throw new S3Error(`s3 ${op} failed: ${err?.cause?.code ?? err?.message}`, { code: 'Network', cause: err });
    }
  }

  const fail = (op, { res, text }) => new S3Error(`s3 ${op} answered ${res.status}${xmlTag(text, 'Code') ? ` ${xmlTag(text, 'Code')}` : ''}`, { status: res.status, code: xmlTag(text, 'Code') });

  return {
    async headObject(bucket, key, { deadlineMs: deadline } = {}) {
      const reply = await send(`HEAD ${bucket}/${key}`, { method: 'HEAD', bucket, key, deadline });
      if (reply.res.status === 404) return { exists: false };
      if (reply.res.status !== 200) throw fail(`HEAD ${bucket}/${key}`, reply);
      const modified = Date.parse(reply.res.headers.get('last-modified') ?? '');
      return { exists: true, size: Number(reply.res.headers.get('content-length')), lastModified: Number.isNaN(modified) ? null : modified, etag: reply.res.headers.get('etag') };
    },

    async putObjectFromFile(bucket, key, file, { deadlineMs: deadline = DEFAULT_S3_PUT_DEADLINE_MS } = {}) {
      const { sha256, size } = await sha256AndSize(file);
      const reply = await send(`PUT ${bucket}/${key}`, { method: 'PUT', bucket, key, payloadSha: sha256, body: Readable.toWeb(fs.createReadStream(file)), contentLength: size, deadline });
      if (reply.res.status !== 200) throw fail(`PUT ${bucket}/${key}`, reply);
      return { sha256, size };
    },

    /** Streams the object to `file` (via `file.part`, renamed once whole) and reports the sha256 and size it saw; the caller compares them. */
    async getObjectToFile(bucket, key, file, { deadlineMs: deadline } = {}) {
      const reply = await send(`GET ${bucket}/${key}`, { method: 'GET', bucket, key, deadline, toFile: file });
      if (reply.res.status === 404) return { exists: false };
      if (reply.res.status !== 200) throw fail(`GET ${bucket}/${key}`, reply);
      return { exists: true, ...reply.streamed };
    },

    async deleteObject(bucket, key, { deadlineMs: deadline } = {}) {
      const reply = await send(`DELETE ${bucket}/${key}`, { method: 'DELETE', bucket, key, deadline });
      if (reply.res.status !== 204 && reply.res.status !== 200 && reply.res.status !== 404) throw fail(`DELETE ${bucket}/${key}`, reply);
    },

    async listObjects(bucket, { prefix = '', pageSize, deadlineMs: deadline } = {}) {
      const objects = [];
      let token;
      do {
        const query = { 'list-type': 2, ...(prefix ? { prefix } : {}), ...(pageSize ? { 'max-keys': pageSize } : {}), ...(token ? { 'continuation-token': token } : {}) };
        const reply = await send(`LIST ${bucket}`, { method: 'GET', bucket, query, deadline });
        if (reply.res.status !== 200) throw fail(`LIST ${bucket}`, reply);
        for (const block of xmlBlocks(reply.text, 'Contents')) {
          objects.push({ key: xmlTag(block, 'Key'), size: Number(xmlTag(block, 'Size')), lastModified: Date.parse(xmlTag(block, 'LastModified')) });
        }
        token = xmlTag(reply.text, 'IsTruncated') === 'true' ? xmlTag(reply.text, 'NextContinuationToken') : undefined;
      } while (token);
      return objects;
    },

    /** `{rules: [{id, status, prefix}]}`; a bucket with no lifecycle configuration has none. Anything else unreadable throws. */
    async getBucketLifecycle(bucket, { deadlineMs: deadline } = {}) {
      const reply = await send(`GET lifecycle ${bucket}`, { method: 'GET', bucket, query: { lifecycle: '' }, deadline });
      if (reply.res.status === 404 && xmlTag(reply.text, 'Code') === 'NoSuchLifecycleConfiguration') return { rules: [] };
      if (reply.res.status !== 200) throw fail(`GET lifecycle ${bucket}`, reply);
      return {
        rules: xmlBlocks(reply.text, 'Rule').map((rule) => ({ id: xmlTag(rule, 'ID'), status: xmlTag(rule, 'Status'), prefix: xmlTag(rule, 'Prefix') ?? '' })),
      };
    },
  };
}
