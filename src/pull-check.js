import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createS3Client } from './s3-client.js';
import { loadGlobalConfigOrError } from './config.js';

/**
 * `lane pull check`: may pull dispatch be switched on against this storage. Only the sweeper may delete objects (by ticket state), so a
 * bucket lifecycle rule of any kind, or one we cannot read, refuses. It also probes where an object's age comes from, because blob GC
 * trusts HEAD `Last-Modified` and a re-PUT refreshing it; if either fails, blob GC must stay off.
 */

// Last-Modified has one-second resolution
const REPUT_SETTLE_MS = 1_100;
const PROBE_PREFIX = 'probes/age-source-';

const hasAge = (head) => head.exists && Number.isFinite(head.lastModified);

async function probeAgeSource({ s3, bucket, key, file, settleMs, sleep }) {
  await s3.putObjectFromFile(bucket, key, file);
  const first = await s3.headObject(bucket, key);
  if (!hasAge(first)) return { verdict: 'off', reason: 'HEAD returns no Last-Modified' };
  await sleep(settleMs);
  await s3.putObjectFromFile(bucket, key, file);
  const second = await s3.headObject(bucket, key);
  if (!hasAge(second) || second.lastModified <= first.lastModified) return { verdict: 'off', reason: 'a re-PUT does not refresh Last-Modified' };
  return { verdict: 'on', reason: 'HEAD returns Last-Modified and a re-PUT refreshes it' };
}

/** Throws when the probe cannot run or its object cannot be removed: a leftover under the prefix is a refusal, never a quiet success. */
export async function blobGcVerdict({ s3, bucket, settleMs = REPUT_SETTLE_MS, sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)) }) {
  for (const leftover of await s3.listObjects(bucket, { prefix: PROBE_PREFIX })) await s3.deleteObject(bucket, leftover.key);
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lane-pull-check-'));
  const file = path.join(dir, 'probe');
  const key = `${PROBE_PREFIX}${crypto.randomBytes(8).toString('hex')}`;
  fs.writeFileSync(file, 'lane pull check age probe\n');
  let verdict;
  let probeError = null;
  try {
    verdict = await probeAgeSource({ s3, bucket, key, file, settleMs, sleep });
  } catch (err) {
    probeError = err;
  }
  fs.rmSync(dir, { recursive: true, force: true });
  try {
    await s3.deleteObject(bucket, key);
  } catch (err) {
    throw new Error(`probe object ${bucket}/${key} could not be deleted (${err.message}); remove it by hand`);
  }
  if (probeError) throw probeError;
  return verdict;
}

/** `{refusals, lifecycle}`: an enabled lifecycle rule on either bucket, or a bucket whose rules cannot be read, is a refusal. Reads only. */
export async function lifecycleVerdict({ cfg, s3, deadlineMs }) {
  const { snapBucket, outBucket } = cfg.lanePull.rustfs;
  const refusals = [];
  const lifecycle = {};
  for (const bucket of [snapBucket, outBucket]) {
    try {
      const { rules } = await s3.getBucketLifecycle(bucket, { deadlineMs });
      lifecycle[bucket] = rules;
      for (const rule of rules.filter((r) => String(r.status).toLowerCase() === 'enabled')) {
        refusals.push(`bucket ${bucket} has an enabled lifecycle rule (${rule.id ?? 'unnamed'}${rule.prefix ? `, prefix ${rule.prefix}` : ''}); remove it before activation`);
      }
    } catch (err) {
      lifecycle[bucket] = null;
      refusals.push(`bucket ${bucket} lifecycle configuration is unreadable (${err.message}); cannot prove nothing expires`);
    }
  }
  return { refusals, lifecycle };
}

/** `{ok, refusals, lifecycle, blobGc}`; `ok` is false on any refusal, and an unreadable bucket or failed probe is a refusal. */
export async function pullCheck({ cfg, s3, settleMs, sleep }) {
  const { snapBucket } = cfg.lanePull.rustfs;
  const { refusals, lifecycle } = await lifecycleVerdict({ cfg, s3 });
  let blobGc;
  try {
    blobGc = await blobGcVerdict({ s3, bucket: snapBucket, settleMs, sleep });
  } catch (err) {
    blobGc = { verdict: 'off', reason: `age probe failed (${err.message})` };
    refusals.push(`age-source probe failed on ${snapBucket}: ${err.message}`);
  }
  return { ok: refusals.length === 0, refusals, lifecycle, blobGc };
}

export async function pullCheckCommand(args = []) {
  const json = args.includes('--json');
  const unknown = args.find((a) => a !== '--json');
  if (unknown !== undefined) {
    process.stderr.write(`lane pull check: unknown argument "${unknown}"\n`);
    return { exitCode: 2 };
  }
  const { config, error } = loadGlobalConfigOrError();
  if (error) {
    process.stderr.write(`lane pull check: ${error}\n`);
    return { exitCode: 1 };
  }
  const { rustfs } = config.lanePull;
  const missing = ['endpoint', 'credentialsEnvFile'].filter((k) => rustfs[k] === undefined);
  if (missing.length > 0) {
    process.stderr.write(`lane pull check: config needs ${missing.map((k) => `"lanePull.rustfs.${k}"`).join(' and ')}\n`);
    return { exitCode: 1 };
  }
  let report;
  try {
    report = await pullCheck({ cfg: config, s3: createS3Client({ endpoint: rustfs.endpoint, region: rustfs.region, credentialsEnvFile: rustfs.credentialsEnvFile }) });
  } catch (err) {
    process.stderr.write(`lane pull check: ${err.message}\n`);
    return { exitCode: 1 };
  }
  const gcNote = report.blobGc.verdict === 'off' && config.lanePull.blobGc ? ' (config has lanePull.blobGc true: set it to false)' : '';
  if (json) process.stdout.write(`${JSON.stringify({ ...report, configBlobGc: config.lanePull.blobGc })}\n`);
  else {
    for (const refusal of report.refusals) process.stdout.write(`refused: ${refusal}\n`);
    process.stdout.write(`blobGc ${report.blobGc.verdict}: ${report.blobGc.reason}${gcNote}\n`);
    process.stdout.write(report.ok ? 'lane pull check: ready (no lifecycle rule on either bucket)\n' : 'lane pull check: NOT ready\n');
  }
  return { exitCode: report.ok ? 0 : 1 };
}
