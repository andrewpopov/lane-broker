import http from 'node:http';
import { canonicalQueryString, credentialScope, sha256Hex, signatureFor } from '../../src/s3-presign.js';

/**
 * An in-process S3 (path-style) that VERIFIES header-signed SigV4, so a wrong signature or payload hash fails the
 * request the way the real service does. Objects live in `stub.objects` keyed `bucket/key`.
 */
export const STUB_CREDS = { accessKeyId: 'AKIASTUBSTUBSTUB', secretAccessKey: 'stub/secret/key' };
export const STUB_REGION = 'us-east-1';

const xmlEscape = (s) => s.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');

export async function startS3Stub({ creds = STUB_CREDS, region = STUB_REGION, now = () => Date.now(), emitLastModified = true, putRefreshesLastModified = true } = {}) {
  const stub = {
    objects: new Map(),
    lifecycle: new Map(),
    requests: [],
    hang: false,
    emitLastModified,
    putRefreshesLastModified,
  };

  const verify = (req, url, bodySha) => {
    const query = Object.fromEntries(url.searchParams);
    const path = url.pathname;
    const auth = /^AWS4-HMAC-SHA256 Credential=([^,]+), SignedHeaders=([^,]+), Signature=([0-9a-f]+)$/.exec(req.headers.authorization ?? '');
    if (!auth) return 'no authorization';
    const [, credential, signedHeaders, given] = auth;
    const stamp = req.headers['x-amz-date'];
    if (credential !== `${creds.accessKeyId}/${credentialScope(stamp, region)}`) return 'credential scope';
    if (req.headers['x-amz-content-sha256'] !== bodySha) return 'payload hash mismatch';
    const canonicalHeaders = signedHeaders.split(';').map((h) => `${h}:${req.headers[h]}\n`).join('');
    const canonicalRequest = [req.method, path, canonicalQueryString(query), canonicalHeaders, signedHeaders, req.headers['x-amz-content-sha256']].join('\n');
    return signatureFor({ secretAccessKey: creds.secretAccessKey, stamp, region, canonicalRequest }) === given ? null : 'signature mismatch';
  };

  const server = http.createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const body = Buffer.concat(chunks);
    const url = new URL(req.url, 'http://stub');
    const reply = (status, text = '', headers = {}) => {
      res.writeHead(status, { 'content-type': 'application/xml', ...headers });
      res.end(req.method === 'HEAD' ? undefined : text);
    };
    const error = (status, code, message = code) => reply(status, `<?xml version="1.0"?><Error><Code>${code}</Code><Message>${xmlEscape(message)}</Message></Error>`);
    if (stub.hang || stub.hangMatching?.(url.pathname)) return;
    const delayMs = stub.delayMatching?.(req.method, url.pathname);
    if (delayMs) await new Promise((resolve) => setTimeout(resolve, delayMs));
    const injected = stub.failMatching?.(req.method, url.pathname);
    if (injected) return error(injected, 'InjectedFailure');
    const problem = verify(req, url, req.method === 'PUT' ? sha256Hex(body) : req.headers['x-amz-content-sha256']);
    stub.requests.push({ method: req.method, path: url.pathname, query: url.search, problem });
    if (problem) return error(403, 'SignatureDoesNotMatch', problem);

    const [, bucket, ...rest] = decodeURIComponent(url.pathname).split('/');
    const key = rest.join('/');
    const name = `${bucket}/${key}`;
    if (key === '' && url.searchParams.has('lifecycle')) {
      const config = stub.lifecycle.get(bucket);
      if (config === 'unreadable') return error(501, 'NotImplemented');
      if (config === undefined) return error(404, 'NoSuchLifecycleConfiguration');
      const rules = config.map((r) => `<Rule><ID>${r.id}</ID><Filter><Prefix>${xmlEscape(r.prefix ?? '')}</Prefix></Filter><Status>${r.status ?? 'Enabled'}</Status></Rule>`).join('');
      return reply(200, `<LifecycleConfiguration>${rules}</LifecycleConfiguration>`);
    }
    if (key === '' && req.method === 'GET') {
      const prefix = url.searchParams.get('prefix') ?? '';
      const max = Number(url.searchParams.get('max-keys') ?? 1000);
      const start = url.searchParams.get('continuation-token') ?? '';
      const keys = [...stub.objects.keys()].filter((n) => n.startsWith(`${bucket}/${prefix}`)).map((n) => n.slice(bucket.length + 1)).sort().filter((k) => k > start);
      const delimiter = url.searchParams.get('delimiter');
      if (delimiter) {
        const common = [...new Set(keys.filter((k) => k.slice(prefix.length).includes(delimiter)).map((k) => k.slice(0, prefix.length + k.slice(prefix.length).indexOf(delimiter) + 1)))];
        const own = keys.filter((k) => !k.slice(prefix.length).includes(delimiter));
        return reply(200, `<ListBucketResult><IsTruncated>false</IsTruncated>${own.map((k) => `<Contents><Key>${xmlEscape(k)}</Key></Contents>`).join('')}${common.map((c) => `<CommonPrefixes><Prefix>${xmlEscape(c)}</Prefix></CommonPrefixes>`).join('')}</ListBucketResult>`);
      }
      const page = keys.slice(0, max);
      const more = keys.length > page.length;
      const contents = page.map((k) => `<Contents><Key>${xmlEscape(k)}</Key><Size>${stub.objects.get(`${bucket}/${k}`).body.length}</Size><LastModified>${new Date(stub.objects.get(`${bucket}/${k}`).lastModified).toISOString()}</LastModified></Contents>`).join('');
      return reply(200, `<ListBucketResult><IsTruncated>${more}</IsTruncated>${more ? `<NextContinuationToken>${xmlEscape(page.at(-1))}</NextContinuationToken>` : ''}${contents}</ListBucketResult>`);
    }
    if (req.method === 'PUT') {
      const prior = stub.objects.get(name);
      stub.objects.set(name, { body, lastModified: prior && !stub.putRefreshesLastModified ? prior.lastModified : now() });
      return reply(200);
    }
    const object = stub.objects.get(name);
    if (req.method === 'DELETE') {
      if (stub.denyDelete) return error(403, 'AccessDenied');
      stub.objects.delete(name);
      return reply(204);
    }
    if (!object) return stub.missingAs403 ? error(403, 'AccessDenied') : error(404, 'NoSuchKey');
    const headers = { 'content-length': String(object.body.length), etag: `"${sha256Hex(object.body).slice(0, 32)}"`, ...(stub.emitLastModified ? { 'last-modified': new Date(object.lastModified).toUTCString() } : {}) };
    if (req.method === 'HEAD') return reply(200, '', headers);
    res.writeHead(200, headers);
    return res.end(object.body);
  });

  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  stub.endpoint = `http://127.0.0.1:${server.address().port}`;
  stub.close = () => new Promise((resolve) => {
    server.closeAllConnections();
    server.close(resolve);
  });
  return stub;
}
