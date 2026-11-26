// PKG-439: placement constraints; the queue owns the claim-time rule (migration 013), this is the client's one canonicalisation of the request.
export const PLATFORMS = Object.freeze(['darwin', 'linux', 'win32']);
export const CLASS_TAG_RE = /^[a-z0-9][a-z0-9-]{0,62}$/;
export const HOST_ID_RE = /^[a-z0-9][a-z0-9-]{0,54}$/;

export const PLACEMENT_REFUSED_EXIT = 76;
export const PLACEMENT_USAGE_EXIT = 64;
export const PLACEMENT_UNREACHABLE_EXIT = 75;

const UNDEFINED_FUNCTION_SQLSTATE = '42883';
export const UNKNOWN_HOST_SQLSTATE = 'LK422';
export const PLACEMENT_MISMATCH_SQLSTATE = 'LK423';
export const QUEUE_TOO_OLD_REASON = 'queue too old for placement constraints (it predates migration 013)';

export class PlacementUsageError extends Error {}

const byCodepoint = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
const sortedUnique = (items) => [...new Set(items)].sort(byCodepoint);

export function parseCapabilityClass(raw) {
  const [platform, ...tags] = String(raw).trim().toLowerCase().split('+');
  if (!PLATFORMS.includes(platform)) throw new PlacementUsageError(`--class must start with ${PLATFORMS.join(', ')} (got "${raw}")`);
  const bad = tags.find((tag) => !CLASS_TAG_RE.test(tag));
  if (bad !== undefined) throw new PlacementUsageError(`--class tag "${bad}" must match ${CLASS_TAG_RE} (got "${raw}")`);
  const classTags = sortedUnique(tags);
  return { capabilityClass: [platform, ...classTags].join('+'), classPlatform: platform, classTags };
}

export function canonicalHostId(raw, hostAliases = {}, what = 'host') {
  const lower = String(raw).trim().toLowerCase();
  const alias = Object.hasOwn(hostAliases, lower) ? lower : null;
  const id = alias === null ? lower : hostAliases[alias];
  if (!HOST_ID_RE.test(id)) throw new PlacementUsageError(`${what} "${raw}" is not a runner id (${HOST_ID_RE})`);
  return { id, alias };
}

export function canonicalPlacement({ capabilityClass, avoidHosts = [], targetHost } = {}, hostAliases = {}) {
  const wantsClass = capabilityClass !== undefined;
  const wantsTarget = targetHost !== undefined;
  if (!wantsClass && avoidHosts.length === 0 && !wantsTarget) return null;
  const parsed = wantsClass ? parseCapabilityClass(capabilityClass) : { capabilityClass: null, classPlatform: null, classTags: [] };
  const aliasesApplied = [];
  const resolve = (raw, what) => {
    const { id, alias } = canonicalHostId(raw, hostAliases, what);
    if (alias !== null) aliasesApplied.push(alias);
    return id;
  };
  const avoid = sortedUnique(avoidHosts.map((raw) => resolve(raw, '--avoid-host')));
  const target = wantsTarget ? resolve(targetHost, '--target-host') : null;
  if (target !== null && avoid.includes(target)) throw new PlacementUsageError(`--target-host ${target} is also in --avoid-host`);
  return { ...parsed, avoidHosts: avoid, targetHost: target, aliasesApplied: sortedUnique(aliasesApplied) };
}

export function resolvedPlacement(laneRequiresTags = [], placement = null) {
  if (placement == null) return { requiresTags: laneRequiresTags, constrained: false };
  return {
    requiresTags: sortedUnique([...laneRequiresTags, ...placement.classTags]),
    classPlatform: placement.classPlatform,
    capabilityClass: placement.capabilityClass,
    avoidHosts: placement.avoidHosts,
    targetHost: placement.targetHost,
    constrained: true,
  };
}

export const isConstrained = (placement) => placement != null;

export const isPullOnly = (ticket) => Boolean(ticket?.dispatchKey) || isConstrained(ticket?.placementRequest);

export const placementKey = (placement) => ({ capabilityClass: placement?.capabilityClass ?? null, avoidHosts: placement?.avoidHosts ?? [], targetHost: placement?.targetHost ?? null });
export const placementKeyOfView = (view) => ({ capabilityClass: view?.capability_class ?? null, avoidHosts: [...(view?.avoid_hosts ?? [])].sort(byCodepoint), targetHost: view?.target_host ?? null });
export const samePlacement = (a, b) => {
  const [x, y] = [placementKey(a), placementKey(b)];
  return x.capabilityClass === y.capabilityClass && x.targetHost === y.targetHost && x.avoidHosts.length === y.avoidHosts.length && x.avoidHosts.every((host, i) => host === y.avoidHosts[i]);
};

export const placementArgument = (placement) => ({ capability_class: placement?.capabilityClass ?? null, avoid_hosts: placement?.avoidHosts ?? [], target_host: placement?.targetHost ?? null });

// 42883 is a queue without the placement signatures (arguments are never trimmed to fit it), LK422 an unknown host, LK423 a key or id held with other placement
export function placementRefusal(err) {
  if (err?.code === UNDEFINED_FUNCTION_SQLSTATE) return { reason: QUEUE_TOO_OLD_REASON, exitCode: PLACEMENT_REFUSED_EXIT };
  if (err?.code === UNKNOWN_HOST_SQLSTATE || err?.code === PLACEMENT_MISMATCH_SQLSTATE) return { reason: err.cause?.message ?? err.message, exitCode: PLACEMENT_REFUSED_EXIT };
  return null;
}

export const placementPullRefusal = (lane, reason, exitCode = PLACEMENT_REFUSED_EXIT) => ({
  ok: false,
  exitCode,
  message: `lane run: placement constraints need the pull queue, so lane "${lane}" was not run${reason ? ` (${reason})` : ''}.\n`,
});

export function isPlacementRecord(value) {
  if (value === null || typeof value !== 'object' || !Array.isArray(value.avoidHosts) || !Array.isArray(value.classTags)) return false;
  const { capabilityClass, classPlatform, classTags, avoidHosts, targetHost } = value;
  let parsed = { capabilityClass: null, classPlatform: null, classTags: [] };
  if (capabilityClass !== null) {
    try {
      parsed = parseCapabilityClass(capabilityClass);
    } catch {
      return false;
    }
  }
  const sameList = (a, b) => a.length === b.length && a.every((item, i) => item === b[i]);
  const hostsOk = avoidHosts.every((host) => typeof host === 'string' && HOST_ID_RE.test(host)) && sameList(avoidHosts, sortedUnique(avoidHosts));
  const targetOk = targetHost === null || (typeof targetHost === 'string' && HOST_ID_RE.test(targetHost) && !avoidHosts.includes(targetHost));
  const constrained = capabilityClass !== null || avoidHosts.length > 0 || targetHost !== null;
  return capabilityClass === parsed.capabilityClass && classPlatform === parsed.classPlatform && sameList(classTags, parsed.classTags) && hostsOk && targetOk && constrained;
}

/** What a holder placed as `held` may answer: an unconstrained request adopts any holder (it ran under a placement at least as strict), a placed one needs the exact canonical placement. */
export const adoptsPlacement = (held, requested) => requested == null || samePlacement(held, requested);

