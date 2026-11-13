import http from 'node:http';
import { canonicalQueryString, credentialScope, sha256Hex, signatureFor, UNSIGNED_PAYLOAD } from '../../src/s3-presign.js';

/**
 * An in-process S3 (path-style) that VERIFIES SigV4, header-signed and presigned, so a wrong signature or payload hash fails the
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
    const host = req.headers.host;
    const path = url.pathname;
    if (query['X-Amz-Signature'] !== undefined) {
      const { 'X-Amz-Signature': given, ...signedParams } = query;
      const stamp = query['X-Amz-Date'];
      if (query['X-Amz-Credential'] !== `${creds.accessKeyId}/${credentialScope(stamp, region)}`) return 'credential scope';
      if (Date.parse(stamp.replace(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/, '$1-$2-$3T$4:$5:$6Z')) + Number(query['X-Amz-Expires']) * 1000 < now()) return 'expired';
      const canonicalRequest = [req.method, path, canonicalQueryString(signedParams), `host:${host}\n`, 'host', UNSIGNED_PAYLOAD].join('\n');
      return signatureFor({ secretAccessKey: creds.secretAccessKey, stamp, region, canonicalRequest }) === given ? null : 'presigned signature mismatch';
    }
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
    if (!object) return error(404, 'NoSuchKey');
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
