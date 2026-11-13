import crypto from 'node:crypto';

/** SigV4 for S3, hand-rolled (the package has one dependency). Pure: the clock is injected, nothing here does I/O. */

export const UNSIGNED_PAYLOAD = 'UNSIGNED-PAYLOAD';
const ALGORITHM = 'AWS4-HMAC-SHA256';
const SERVICE = 's3';

export const sha256Hex = (data) => crypto.createHash('sha256').update(data).digest('hex');
const hmac = (key, data) => crypto.createHmac('sha256', key).update(data).digest();

/** RFC 3986 unreserved characters stay; everything else is %XX. `keepSlash` is for a path, never a query value. */
export function awsUriEncode(value, { keepSlash = false } = {}) {
  const encoded = encodeURIComponent(value).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
  return keepSlash ? encoded.replaceAll('%2F', '/') : encoded;
}

/** Sorted, encoded `k=v&k=v`; the same string goes into the URL and into the canonical request. */
export function canonicalQueryString(params) {
  return Object.entries(params)
    .map(([k, v]) => [awsUriEncode(k), awsUriEncode(String(v))])
    .sort(([a, av], [b, bv]) => (a < b ? -1 : a > b ? 1 : av < bv ? -1 : av > bv ? 1 : 0))
    .map(([k, v]) => `${k}=${v}`)
    .join('&');
}

export const amzTimestamp = (now) => new Date(now).toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '');

export function credentialScope(stamp, region) {
  return `${stamp.slice(0, 8)}/${region}/${SERVICE}/aws4_request`;
}

export function signatureFor({ secretAccessKey, stamp, region, canonicalRequest }) {
  const date = stamp.slice(0, 8);
  const key = hmac(hmac(hmac(hmac(`AWS4${secretAccessKey}`, date), region), SERVICE), 'aws4_request');
  const stringToSign = [ALGORITHM, stamp, credentialScope(stamp, region), sha256Hex(canonicalRequest)].join('\n');
  return crypto.createHmac('sha256', key).update(stringToSign).digest('hex');
}

/**
 * Presign `method` for `host` + `path` (path already URI-encoded). Returns `{host, path, query}` where `query` is the full signed
 * query string. Split from `presignUrl` so the AWS documented vector (virtual-hosted style) and our path-style URLs share one signer.
 */
export function presignRequest({ method, host, path, expiresSec, creds, region, now }) {
  const stamp = amzTimestamp(now);
  const params = {
    'X-Amz-Algorithm': ALGORITHM,
    'X-Amz-Credential': `${creds.accessKeyId}/${credentialScope(stamp, region)}`,
    'X-Amz-Date': stamp,
    'X-Amz-Expires': expiresSec,
    'X-Amz-SignedHeaders': 'host',
    ...(creds.sessionToken ? { 'X-Amz-Security-Token': creds.sessionToken } : {}),
  };
  const query = canonicalQueryString(params);
  const canonicalRequest = [method, path, query, `host:${host}\n`, 'host', UNSIGNED_PAYLOAD].join('\n');
  const signature = signatureFor({ secretAccessKey: creds.secretAccessKey, stamp, region, canonicalRequest });
  return { host, path, query: `${query}&X-Amz-Signature=${signature}` };
}

export const objectPath = (bucket, key) => `/${awsUriEncode(bucket)}/${awsUriEncode(key, { keepSlash: true })}`;

/** Path-style presigned URL (`<endpoint>/<bucket>/<key>?...`), `UNSIGNED-PAYLOAD`. */
export function presignUrl({ method, bucket, key, expiresSec, creds, endpoint, region, now = Date.now() }) {
  const base = new URL(endpoint);
  const signed = presignRequest({ method, host: base.host, path: objectPath(bucket, key), expiresSec, creds, region, now });
  return `${base.protocol}//${signed.host}${signed.path}?${signed.query}`;
}
