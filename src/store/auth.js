import crypto from 'node:crypto';

/**
 * Token verification for lane-store is a plug point: `createStore({ verifier })` takes any
 * `async (token) => claims | null`. P1 swaps in the DB-backed principal check (spec 4.2); until
 * then this HMAC verifier stands in. Claims: `{ role, job?, exp }` with `exp` in epoch seconds.
 *
 * Roles: `submit` (upload blobs, register a manifest), `read` (job-scoped: only blobs referenced by
 * the manifest of `claims.job`), `replica` (replication peer: write and read any object, verify,
 * list), `admin` (everything, including pins and terminal marks).
 */
export const ROLES = Object.freeze({ SUBMIT: 'submit', READ: 'read', REPLICA: 'replica', ADMIN: 'admin' });

const b64 = (buf) => Buffer.from(buf).toString('base64url');

function mac(secret, payload) {
  return crypto.createHmac('sha256', secret).update(payload).digest();
}

export function signToken(secret, claims) {
  const payload = b64(JSON.stringify(claims));
  return `${payload}.${b64(mac(secret, payload))}`;
}

export function createHmacVerifier(secret, { now = Date.now } = {}) {
  return async (token) => {
    if (typeof token !== 'string') return null;
    const [payload, tag, extra] = token.split('.');
    if (!payload || !tag || extra !== undefined) return null;
    const expected = mac(secret, payload);
    const given = Buffer.from(tag, 'base64url');
    if (given.length !== expected.length || !crypto.timingSafeEqual(given, expected)) return null;
    let claims;
    try {
      claims = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
    } catch {
      return null;
    }
    if (!claims || typeof claims.role !== 'string' || !Object.values(ROLES).includes(claims.role)) return null;
    if (typeof claims.exp !== 'number' || claims.exp * 1000 <= now()) return null;
    return claims;
  };
}
