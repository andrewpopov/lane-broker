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

export const objectPath = (bucket, key) => `/${awsUriEncode(bucket)}/${awsUriEncode(key, { keepSlash: true })}`;
