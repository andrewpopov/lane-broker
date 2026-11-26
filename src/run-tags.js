import { pullConfigured } from './config.js';
import { effectiveRequiresTags, hostTag, isValidRunTag, HOST_TAG_PREFIX } from './runner-readiness.js';
import { pullServices } from './pull-services.js';
import { QUEUE_PROBE } from './queue-client.js';

/** PKG-468: a runner counts as online when it polled the queue within this many seconds (the queue stamps `runners.last_seen` on every claim poll). */
export const RUNNER_ONLINE_WITHIN_S = 120;
const RUNNER_NAME_RE = /^[a-z0-9][a-z0-9-]{0,54}$/;

/** The tag one `lane run --tag <t>` / `--runner <name>` flag stands for; throws a usage message on a bad value. */
export function runTagFromFlag(flag, raw) {
  if (flag === '--runner') {
    if (!RUNNER_NAME_RE.test(raw ?? '')) throw new Error(`--runner needs a runner name (lowercase letters, digits and "-"), got "${raw ?? ''}"`);
    return hostTag(raw);
  }
  if (!isValidRunTag(raw)) throw new Error(`--tag needs a tag name (letters, digits, "." "_" "+" "-", optionally "${HOST_TAG_PREFIX}<runner>"), got "${raw ?? ''}"`);
  return raw;
}

const ago = (s) => (s === null || s === undefined ? 'never registered' : `last seen ${s < 90 ? `${s}s` : s < 5400 ? `${Math.round(s / 60)}m` : `${Math.round(s / 3600)}h`} ago`);

/** The refusal for a run whose tags no online runner satisfies, or null when one can claim it. `detail` is `ld_placeable_detail`'s answer. */
export function noOnlineRunnerMessage(detail, tags) {
  const wanted = `[${tags.join(', ')}]`;
  const runners = Array.isArray(detail?.runners) ? detail.runners : [];
  if (runners.length === 0) return `waiting: no runner has ${wanted}; nothing registered with the queue can claim it (check the runner name, or tags the runner declares)`;
  if (runners.some((r) => Number.isFinite(r.seenAgoS) && r.seenAgoS <= RUNNER_ONLINE_WITHIN_S)) return null;
  return `waiting: no online runner has ${wanted}; offline: ${runners.map((r) => `${r.host} (${ago(r.seenAgoS ?? null)})`).join(', ')}`;
}

/**
 * Before a run with per-run tags enqueues anything: `{ ok: true }` when a runner that polls the queue can claim it, else the named refusal.
 * A queue without `ld_placeable_detail` (migration 012 not applied) is asked `ld_placeable` instead, which still catches "no runner has the tag".
 * Best effort beyond that: pull not configured or a queue that cannot be reached never blocks the run.
 */
export async function checkRunnable({ cfg, root, repoKey, tags, servicesFor = pullServices }) {
  if (!pullConfigured(cfg)) return { ok: true };
  try {
    const services = servicesFor(cfg, root);
    if (!services.available) return { ok: true };
    const detail = await services.queue.placeableDetail(repoKey, tags, QUEUE_PROBE).catch(async () => ({ eligible: await services.queue.placeable(repoKey, tags, QUEUE_PROBE), runners: null }));
    const message = detail.runners === null ? (detail.eligible === 0 ? noOnlineRunnerMessage({ runners: [] }, tags) : null) : noOnlineRunnerMessage(detail, tags);
    return message === null ? { ok: true } : { ok: false, message };
  } catch {
    return { ok: true };
  }
}

export { effectiveRequiresTags };
