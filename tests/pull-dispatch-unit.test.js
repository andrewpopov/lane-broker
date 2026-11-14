import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { createAttempt, updateAttempt } from '../src/attempts.js';
import { DEFAULT_GLOBAL_CONFIG } from '../src/config.js';
import { QUEUE_PROBE } from '../src/queue-client.js';
import { ensureStateDirs, paths } from '../src/state.js';
import { freshEnv, writeGlobalConfig } from './helpers.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';
import { makeTmpDir } from './helpers/tmp.js';

/** BRAIN-1036 R7: the placement decision (T14, D6), readiness, the exit mapping, the withdraw guard (T2), the queued timeout keys and the history writer. */

const QUEUE_URL = 'postgres://lane_submit_train@pitelite:5432/lane_queue?sslmode=verify-full';
const PULL_RUNNER = { name: 'pullbox', ssh: 'pullbox', transport: 'pull' };
const CFG = {
  ...DEFAULT_GLOBAL_CONFIG,
  runners: [PULL_RUNNER, { name: 'skybox', ssh: 'skybox' }],
  laneQueueUrl: QUEUE_URL,
  lanePull: { ...DEFAULT_GLOBAL_CONFIG.lanePull, enabled: true, passwordFile: '/p', caFile: '/c', rustfs: { ...DEFAULT_GLOBAL_CONFIG.lanePull.rustfs, endpoint: 'http://s', credentialsEnvFile: '/e' } },
};

/** Services and callbacks that record every touch, so a test can say "nothing was called". */
function spies({ placeable = 2, ready = { ready: true }, lifecycleRules = [], queueRef = 'ref' } = {}) {
  const calls = [];
  const probes = { lifecycleDeadlines: [], queueRef: [], placeable: [] };
  const services = {
    available: true,
    s3: { getBucketLifecycle: async (bucket, options) => { calls.push(`lifecycle ${bucket}`); probes.lifecycleDeadlines.push(options?.deadlineMs); return { rules: lifecycleRules }; } },
    queue: {
      queueRef: async (options) => { calls.push('queueRef'); probes.queueRef.push(options); if (queueRef instanceof Error) throw queueRef; return queueRef; },
      placeable: async (_repoKey, _tags, options) => { calls.push('placeable'); probes.placeable.push(options); if (placeable instanceof Error) throw placeable; return placeable; },
    },
  };
  return {
    calls,
    probes,
    services,
    input: {
      cfg: CFG,
      repoKey: 'r',
      requiresTags: [],
      servicesFor: () => { calls.push('servicesFor'); return services; },
      readiness: async () => { calls.push('readiness'); return ready; },
      locallyIneligible: () => { calls.push('locallyIneligible'); return false; },
      couldRunLocally: async () => { calls.push('couldRunLocally'); return false; },
    },
  };
}

const PLACEMENT = {
  'D6: a chosen push runner returns before any pull config, file, store or queue is touched': async (load) => {
    const { placeForPull } = await load('pull-placement.js');
    const s = spies();
    assert.deepEqual(await placeForPull({ ...s.input, pushChosen: true }), { route: 'push' });
    assert.deepEqual(s.calls, []);
  },

  'T14: pull off, no pull runner, or an incomplete pull config leaves today\'s flow untouched with no call': async (load) => {
    const { placeForPull } = await load('pull-placement.js');
    for (const cfg of [{ ...CFG, lanePull: { ...CFG.lanePull, enabled: false } }, { ...CFG, runners: [{ name: 'skybox', ssh: 'skybox' }] }, { ...CFG, laneQueueUrl: undefined }]) {
      const s = spies();
      assert.deepEqual(await placeForPull({ ...s.input, cfg, pushChosen: false }), { route: 'legacy' });
      assert.deepEqual(s.calls, []);
    }
  },

  'an unready or unreachable pull side is today\'s flow with the reason, never an enqueue': async (load) => {
    const { placeForPull } = await load('pull-placement.js');
    const unready = spies({ ready: { ready: false, reason: 'lifecycle rule' } });
    assert.deepEqual(await placeForPull({ ...unready.input, pushChosen: false }), { route: 'legacy', reason: 'pull dispatch unavailable: lifecycle rule' });
    assert.ok(!unready.calls.includes('placeable'));
    const down = spies({ placeable: new Error('connection refused') });
    const placed = await placeForPull({ ...down.input, pushChosen: false });
    assert.deepEqual([placed.route, /ld_placeable failed: connection refused/.test(placed.reason)], ['legacy', true]);
  },

  'N9: no eligible principal refuses before the enqueue decision, even for a ticket this machine could run': async (load) => {
    const { placeForPull } = await load('pull-placement.js');
    const s = spies({ placeable: 0 });
    s.input.couldRunLocally = async () => true;
    assert.equal((await placeForPull({ ...s.input, pushChosen: false })).route, 'refuse');
    assert.ok(!s.calls.includes('locallyIneligible'));
  },

  'a ticket this machine could never run is enqueued without asking whether it could run now': async (load) => {
    const { placeForPull } = await load('pull-placement.js');
    const s = spies();
    s.input.locallyIneligible = () => true;
    s.input.couldRunLocally = async () => assert.fail('a locally ineligible ticket asked whether it could run locally');
    const placed = await placeForPull({ ...s.input, pushChosen: false });
    assert.deepEqual([placed.route, placed.services === s.services], ['pull', true]);
    assert.deepEqual(s.probes.placeable, [QUEUE_PROBE], 'ld_placeable decides placement, so it is one short attempt too');
  },

  'a ticket this machine can admit or bypass now runs locally; otherwise it is enqueued': async (load) => {
    const { placeForPull } = await load('pull-placement.js');
    const now = spies();
    now.input.couldRunLocally = async () => true;
    assert.equal((await placeForPull({ ...now.input, pushChosen: false })).route, 'local');
    assert.equal((await placeForPull({ ...spies().input, pushChosen: false })).route, 'pull');
  },

  'readiness is kept under the state root: another process within five minutes reuses the verdict, a failure included, and does no I/O': async (load) => {
    const { createPullReadiness, PULL_READY_TTL_MS } = await load('pull-placement.js');
    const { state } = freshEnv();
    const clock = { now: 1_000 };
    const processOf = () => createPullReadiness({ root: state, now: () => clock.now });
    const first = spies();
    assert.deepEqual(await processOf()(CFG, first.services), { ready: true });
    assert.deepEqual(first.calls, ['lifecycle lane-snap', 'lifecycle lane-out', 'queueRef']);
    clock.now += PULL_READY_TTL_MS - 1;
    const second = spies({ queueRef: new Error('would fail') });
    assert.deepEqual(await processOf()(CFG, second.services), { ready: true });
    assert.deepEqual(second.calls, [], 'the second process probed again');
    clock.now += 1;
    const expired = spies();
    await processOf()(CFG, expired.services);
    assert.equal(expired.calls.filter((c) => c === 'queueRef').length, 1);

    const failing = freshEnv().state;
    const down = spies({ queueRef: new Error('connection refused') });
    const verdict = await createPullReadiness({ root: failing, now: () => clock.now })(CFG, down.services);
    assert.equal(verdict.ready, false);
    const retry = spies();
    assert.deepEqual(await createPullReadiness({ root: failing, now: () => clock.now })(CFG, retry.services), verdict);
    assert.deepEqual(retry.calls, [], 'a failure is cached like a success');
    const moved = spies();
    await createPullReadiness({ root: failing, now: () => clock.now })({ ...CFG, laneQueueUrl: `${QUEUE_URL}x`, lanePull: { ...CFG.lanePull } }, moved.services);
    assert.ok(moved.calls.includes('queueRef'), 'a verdict for another queue is not reused');
  },

  'a verdict is not reused after the credentials, the queue password or CA, or the region change': async (load) => {
    const { createPullReadiness } = await load('pull-placement.js');
    const dir = makeTmpDir('readiness-key-');
    const files = Object.fromEntries(['pw', 'ca', 'env'].map((name) => [name, path.join(dir, name)]));
    for (const file of Object.values(files)) fs.writeFileSync(file, 'x\n');
    const cfg = { ...CFG, lanePull: { ...CFG.lanePull, passwordFile: files.pw, caFile: files.ca, rustfs: { ...CFG.lanePull.rustfs, credentialsEnvFile: files.env } } };
    const { state } = freshEnv();
    const probes = async (config) => {
      const s = spies();
      await createPullReadiness({ root: state })(config, s.services);
      return s.calls.length;
    };
    assert.ok((await probes(cfg)) > 0);
    assert.equal(await probes(cfg), 0, 'unchanged inputs reuse the verdict');
    for (const file of Object.values(files)) {
      fs.utimesSync(file, new Date(5_000_000), new Date(5_000_000));
      assert.ok((await probes(cfg)) > 0, `touching ${path.basename(file)} must invalidate the verdict`);
      assert.equal(await probes(cfg), 0);
    }
    assert.ok((await probes({ ...cfg, lanePull: { ...cfg.lanePull, rustfs: { ...cfg.lanePull.rustfs, region: 'eu-west-1' } } })) > 0);
  },

  'the readiness probes are one short attempt each: lifecycle reads carry a deadline and the queue login does not retry': async (load) => {
    const { createPullReadiness, PULL_PROBE_DEADLINE_MS } = await load('pull-placement.js');
    const s = spies();
    await createPullReadiness({ root: freshEnv().state })(CFG, s.services);
    assert.deepEqual(s.probes.lifecycleDeadlines, [PULL_PROBE_DEADLINE_MS, PULL_PROBE_DEADLINE_MS]);
    assert.deepEqual(s.probes.queueRef, [QUEUE_PROBE]);
  },

  'a probe option of one attempt makes the queue client send once; without it a transient failure is retried': async (load) => {
    const { createQueueClient } = await load('queue-client.js');
    const dir = makeTmpDir('queue-probe-');
    fs.writeFileSync(path.join(dir, 'pw'), 'pw\n');
    fs.writeFileSync(path.join(dir, 'ca'), 'ca\n');
    const connects = [];
    const client = createQueueClient({
      url: QUEUE_URL, passwordFile: path.join(dir, 'pw'), caFile: path.join(dir, 'ca'), sleep: async () => {},
      connectClient: async (config) => { connects.push(config); throw Object.assign(new Error('too many connections'), { code: '53300' }); },
    });
    await assert.rejects(client.queueRef(QUEUE_PROBE));
    assert.equal(connects.length, 1);
    assert.deepEqual([connects[0].connectionTimeoutMillis, connects[0].query_timeout], [QUEUE_PROBE.connectTimeoutMs, QUEUE_PROBE.queryTimeoutMs]);
    connects.length = 0;
    await assert.rejects(client.queueRef());
    assert.equal(connects.length, 6);
  },

  'readiness refuses a missing credential file, an enabled lifecycle rule and a queue login that fails': async (load) => {
    const { createPullReadiness } = await load('pull-placement.js');
    const verdict = (services) => createPullReadiness({ root: freshEnv().state })(CFG, services);
    assert.deepEqual(await verdict({ available: false, reason: 'config lacks lanePull.caFile' }), { ready: false, reason: 'config lacks lanePull.caFile' });
    const rule = (await verdict(spies({ lifecycleRules: [{ id: 'expire', status: 'Enabled' }] }).services));
    assert.deepEqual([rule.ready, /enabled lifecycle rule \(expire/.test(rule.reason)], [false, true]);
    const login = await verdict(spies({ queueRef: new Error('password authentication failed') }).services);
    assert.deepEqual([login.ready, /queue login failed: password authentication failed/.test(login.reason)], [false, true]);
  },
};

const EXIT_MAPPING = {
  'every settled outcome ends the supervisor with its own exit and an unresolved one with 1; a withdraw is not an exit': async (load) => {
    const { pullExitCode } = await load('pull-ticket.js');
    assert.deepEqual(
      [{ outcome: 'confirmed', exitCode: 7 }, { outcome: 'confirmed', exitCode: 0 }, { outcome: 'cancelled', exitCode: 130 }, { outcome: 'lost' }, { outcome: 'lost', exitCode: 76 }, { outcome: 'unresolved' }, { outcome: 'withdrawn' }, { outcome: 'gone' }].map(pullExitCode),
      [7, 0, 130, 75, 76, 1, undefined, undefined],
    );
  },
};

const GUARD = {
  'T2: a pull attempt may not fall back until the queue committed the withdraw or no enqueue was ever recorded; push and absent attempts are not held': async (load) => {
    const { assertWithdrawCommitted } = await load('pull-ticket.js');
    const { state } = freshEnv();
    ensureStateDirs(state);
    const id = '00000000-0000-4000-8000-0000000000a1';
    const { generation } = await createAttempt(state, id);
    assert.doesNotThrow(() => assertWithdrawCommitted(state, id), 'a push attempt is not held');
    assert.doesNotThrow(() => assertWithdrawCommitted(state, '00000000-0000-4000-8000-0000000000a2'), 'an absent attempt is not held');
    await updateAttempt(state, id, generation, { transport: 'pull', pull: { stage: 'QUEUED' } });
    assert.throws(() => assertWithdrawCommitted(state, id), /before the queue has committed a withdraw/);
    await updateAttempt(state, id, generation, (current) => ({ pull: { ...current.pull, withdrawProof: false } }));
    assert.throws(() => assertWithdrawCommitted(state, id), /committed a withdraw/);
    await updateAttempt(state, id, generation, (current) => ({ pull: { ...current.pull, withdrawProof: true } }));
    assert.doesNotThrow(() => assertWithdrawCommitted(state, id));
    await updateAttempt(state, id, generation, { pull: { stage: 'PREPARING', neverEnqueued: true } });
    assert.doesNotThrow(() => assertWithdrawCommitted(state, id), 'a proof that nothing was ever enqueued allows the fallback too');
  },
};

const HISTORY = {
  'a result rebuilt from the ledger adds no second history row; one never counted gets its row; a push write always appends': async (load) => {
    const { recordSettledResult: record } = await load('supervisor.js');
    const { state } = freshEnv();
    ensureStateDirs(state);
    const ticket = (id) => ({ id, key: 'r:default', repoId: 'r', lane: 'default', weight: 1, resources: {}, command: 'true', headTree: 'abc', resultPath: path.join(paths(state).results, `${id}.json`) });
    const rows = (id) => fs.readFileSync(paths(state).history, 'utf8').split('\n').filter((line) => line.includes(id));
    const published = '00000000-0000-4000-8000-0000000000b1';
    record(state, ticket(published), { id: published, exit: 3, executor: 'remote' }, {});
    fs.rmSync(ticket(published).resultPath);
    record(state, ticket(published), { id: published, exit: 3, executor: 'remote' }, { rebuilt: true });
    assert.equal(rows(published).length, 1, 'the rebuild must not count the ticket twice');
    assert.equal(JSON.parse(fs.readFileSync(ticket(published).resultPath, 'utf8')).exit, 3, 'the result file is rewritten');
    const neverCounted = '00000000-0000-4000-8000-0000000000b2';
    record(state, ticket(neverCounted), { id: neverCounted, exit: 0, executor: 'remote' }, { rebuilt: true });
    assert.equal(rows(neverCounted).length, 1);
    record(state, ticket(neverCounted), { id: neverCounted, exit: 0, executor: 'remote' }, {});
    assert.equal(rows(neverCounted).length, 2, 'a first write is never deduplicated');
  },
};

const loadedConfig = async (loader, extra) => {
  const { loadGlobalConfig } = await loader('config.js');
  const { home } = freshEnv();
  writeGlobalConfig(home, { version: 1, ...extra });
  const prior = process.env.LANE_BROKER_HOME;
  process.env.LANE_BROKER_HOME = home;
  try {
    return loadGlobalConfig();
  } finally {
    process.env.LANE_BROKER_HOME = prior;
  }
};

const TIMEOUT = {
  'the queued timeout is unbounded by default, the global one when set, and a lane\'s own over it': async (load) => {
    const { pullQueuedTimeoutMs } = await load('config.js');
    assert.equal(pullQueuedTimeoutMs(DEFAULT_GLOBAL_CONFIG, 'train'), undefined, 'BRAIN-509: a train/pr lane has no queue timeout unless one is configured');
    const cfg = { lanePull: { queuedTimeoutMs: 600_000, queuedTimeoutMsByLane: { sim: 60_000 } } };
    assert.deepEqual(['default', 'sim'].map((lane) => pullQueuedTimeoutMs(cfg, lane)), [600_000, 60_000]);
    assert.equal((await loadedConfig(load, { lanePull: { queuedTimeoutMsByLane: { sim: 5000 } } })).lanePull.queuedTimeoutMsByLane.sim, 5000);
  },

  'a queued timeout that is not a positive integer, or a lane map that is not lane to milliseconds, is refused naming the key': async (load) => {
    const { ConfigError } = await load('config.js');
    for (const [lanePull, key] of [[{ queuedTimeoutMs: 0 }, 'lanePull.queuedTimeoutMs'], [{ queuedTimeoutMs: 1.5 }, 'lanePull.queuedTimeoutMs'], [{ queuedTimeoutMsByLane: [] }, 'lanePull.queuedTimeoutMsByLane'], [{ queuedTimeoutMsByLane: { sim: -1 } }, 'lanePull.queuedTimeoutMsByLane.sim']]) {
      await assert.rejects(async () => loadedConfig(load, { lanePull }), (err) => err instanceof ConfigError && err.message.includes(`"${key}"`), JSON.stringify(lanePull));
    }
  },
};

const PRIORITY = {
  'a ticket\'s requested tier maps onto the queue\'s 0-9 priority column, higher claimed first, unrequested as medium': async (load) => {
    const { pullQueuePriority } = await load('priority.js');
    const mapped = ['low', 'medium', 'high', undefined, 'bogus'].map(pullQueuePriority);
    assert.deepEqual(mapped, [0, 4, 7, 4, 4]);
    assert.ok(mapped.every((p) => p >= 0 && p <= 9));
  },
};

const PUBLICATION = {
  'the finished result is a pure function of the publication block and the ledger entry, and the writer records one history row': async (load) => {
    const { publicationBindings, publicationOf, pullFinishedResult } = await load('pull-publication.js');
    const { state } = freshEnv();
    ensureStateDirs(state);
    const id = '00000000-0000-4000-8000-0000000000c1';
    const enriched = { id, key: 'r:default', repoId: 'r', lane: 'default', weight: 1, resources: {}, command: 'true', headTree: 'abc', cwd: state, createdAt: 1_000, logPath: path.join(state, 'custom.log'), resultPath: path.join(paths(state).results, `${id}.json`), ignored: 'not copied' };
    const publication = publicationOf(enriched, { placement: null, attemptStartedAt: 900, refusalExit: 76 });
    assert.equal('ignored' in publication.enriched, false);
    assert.deepEqual(JSON.parse(JSON.stringify(publication)), publication, 'the block survives the envelope\'s JSON');
    const entry = { decision: { kind: 'finished', exit: 3 }, host: 'agentbox', acceptedAt: 5_000, result: { kind: 'completed', runMs: 2_000 } };
    const built = pullFinishedResult(publication, entry, null, { remoteLogWarning: 'w' });
    assert.deepEqual(pullFinishedResult(publication, entry, null, { remoteLogWarning: 'w' }), built);
    assert.deepEqual([built.endedAt, built.runMs, built.waitedMs, built.startedAt, built.runner, built.exit, built.remoteLogWarning], [5_000, 2_000, 2_000, 3_000, 'agentbox', 3, 'w']);
    const bindings = publicationBindings(state, JSON.parse(JSON.stringify(publication)));
    assert.deepEqual([bindings.logPath, bindings.resultPath], [enriched.logPath, enriched.resultPath]);
    bindings.writeResult(built, { fsync: false });
    bindings.writeResult(built, { fsync: false, rebuilt: true });
    const rows = fs.readFileSync(paths(state).history, 'utf8').split('\n').filter(Boolean).map((line) => JSON.parse(line));
    assert.deepEqual(rows.map((row) => [row.id, row.exit, row.logPath, row.runner]), [[id, 3, enriched.logPath, 'agentbox']]);
  },
};

const ALL = { ...PLACEMENT, ...EXIT_MAPPING, ...GUARD, ...HISTORY, ...TIMEOUT, ...PRIORITY, ...PUBLICATION };

const CANARIES = [
  ['D6: a chosen push runner returns before any pull config, file, store or queue is touched', 'pull-placement.js', ["  if (pushChosen) return { route: 'push' };\n", '']],
  ['T14: pull off, no pull runner, or an incomplete pull config leaves today\'s flow untouched with no call', 'pull-placement.js', ["  if (!pullConfigured(cfg)) return { route: 'legacy' };\n", '']],
  ['an unready or unreachable pull side is today\'s flow with the reason, never an enqueue', 'pull-placement.js', ["  if (!verdict.ready) return { route: 'legacy', reason: `pull dispatch unavailable: ${verdict.reason}` };\n", '']],
  ['N9: no eligible principal refuses before the enqueue decision, even for a ticket this machine could run', 'pull-placement.js', ["  if (placeable === 0) return { route: 'refuse', reason: 'no queue principal is eligible for this ticket' };\n", '']],
  ['a ticket this machine could never run is enqueued without asking whether it could run now', 'pull-placement.js', ["  if (locallyIneligible()) return { route: 'pull', services };\n", '']],
  ['a ticket this machine can admit or bypass now runs locally; otherwise it is enqueued', 'pull-placement.js', ["return (await couldRunLocally()) ? { route: 'local' } : { route: 'pull', services };", "return { route: 'pull', services };"]],
  ['readiness is kept under the state root: another process within five minutes reuses the verdict, a failure included, and does no I/O', 'pull-placement.js', ['if (stored?.key === key && age >= 0 && age < ttlMs) return stored.verdict;', '']],
  ['readiness is kept under the state root: another process within five minutes reuses the verdict, a failure included, and does no I/O', 'pull-placement.js', ['stored?.key === key && ', '']],
  ['the readiness probes are one short attempt each: lifecycle reads carry a deadline and the queue login does not retry', 'pull-placement.js', ['deadlineMs: PULL_PROBE_DEADLINE_MS', 'deadlineMs: undefined']],
  ['the readiness probes are one short attempt each: lifecycle reads carry a deadline and the queue login does not retry', 'pull-placement.js', ['services.queue.queueRef(QUEUE_PROBE)', 'services.queue.queueRef()']],
  ['a ticket this machine could never run is enqueued without asking whether it could run now', 'pull-placement.js', ['services.queue.placeable(repoKey, requiresTags, QUEUE_PROBE)', 'services.queue.placeable(repoKey, requiresTags)']],
  ['a probe option of one attempt makes the queue client send once; without it a transient failure is retried', 'queue-client.js', ['if (attempt < attempts && retryable', 'if (attempt < MAX_ATTEMPTS && retryable']],
  ['a ticket\'s requested tier maps onto the queue\'s 0-9 priority column, higher claimed first, unrequested as medium', 'priority.js', ['PULL_QUEUE_BAND_START[tierRank(tier)]', '0']],
  ['the finished result is a pure function of the publication block and the ledger entry, and the writer records one history row', 'pull-publication.js', ['const endedAt = acceptedAt ?? Date.now();', 'const endedAt = Date.now();']],
  ['the finished result is a pure function of the publication block and the ledger entry, and the writer records one history row', 'pull-publication.js', ['rebuilt, placement: publication.placement', 'placement: publication.placement']],
  ['readiness refuses a missing credential file, an enabled lifecycle rule and a queue login that fails', 'pull-placement.js', ["  if (refusals.length > 0) return { ready: false, reason: refusals[0] };\n", '']],
  ['every settled outcome ends the supervisor with its own exit and an unresolved one with 1; a withdraw is not an exit', 'pull-ticket.js', ['outcome.exitCode : LOST_EXIT;', 'outcome.exitCode : 1;']],
  ['every settled outcome ends the supervisor with its own exit and an unresolved one with 1; a withdraw is not an exit', 'pull-ticket.js', ['return Number.isInteger(outcome.exitCode) ? outcome.exitCode : LOST_EXIT;', 'return LOST_EXIT;']],
  ['a verdict is not reused after the credentials, the queue password or CA, or the region change', 'pull-placement.js', ['...[cfg.lanePull.passwordFile, cfg.lanePull.caFile, credentialsEnvFile].map(fileStamp)', '']],
  ['a verdict is not reused after the credentials, the queue password or CA, or the region change', 'pull-placement.js', ['[cfg.laneQueueUrl, endpoint, region,', '[cfg.laneQueueUrl, endpoint,']],
  ['T2: a pull attempt may not fall back until the queue committed the withdraw or no enqueue was ever recorded; push and absent attempts are not held', 'pull-ticket.js', ['pull?.withdrawProof === true || ', '']],
  ['T2: a pull attempt may not fall back until the queue committed the withdraw or no enqueue was ever recorded; push and absent attempts are not held', 'pull-ticket.js', [' || pull?.neverEnqueued === true;', ';']],
  ['a result rebuilt from the ledger adds no second history row; one never counted gets its row; a push write always appends', 'supervisor.js', ['if (rebuilt && historyHasRow(root, enriched.id)) return;', '']],
  ['the queued timeout is unbounded by default, the global one when set, and a lane\'s own over it', 'config.js', ['cfg?.lanePull?.queuedTimeoutMsByLane?.[lane] ?? ', '']],
  ['a queued timeout that is not a positive integer, or a lane map that is not lane to milliseconds, is refused naming the key', 'config.js', ['assert(pull.queuedTimeoutMs === undefined ||', 'assert(true ||']],
];

for (const [name, scenario] of Object.entries(ALL)) test(name, () => scenario(realSrc));
for (const [name, file, mutation] of CANARIES) {
  test(`canary: with "${mutation[0].trim().slice(0, 70)}" removed, "${name}" fails`, async () => {
    await assertCanaryFails(file, mutation, ALL[name]);
  });
}

