import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { loadGlobalConfig, loadEffectiveGlobalConfig, reloadGlobalConfig, isDispatchOnly, ConfigError, DEFAULT_GLOBAL_CONFIG, LIVE_RELOAD_KEYS, CONFIG_OVERLAY_DIR } from '../src/config.js';
import { effectiveWeightCapacity, checkResourceBudget } from '../src/resources.js';
import { resolveLaneNice } from '../src/priority.js';
import { claimFigures, brokerConfigReader } from '../src/agent/claims.js';
import { noteOverlayWarnings } from '../src/supervisor.js';
import { paths } from '../src/state.js';
import { freshEnv, writeGlobalConfig, laneRun } from './helpers.js';

/** BRAIN-1141: `<home>/config.d/*.json` overlays, `lane config effective`, and the live-reload contract bosun relies on. */

const writeOverlay = (home, name, body) => {
  fs.mkdirSync(path.join(home, CONFIG_OVERLAY_DIR), { recursive: true });
  fs.writeFileSync(path.join(home, CONFIG_OVERLAY_DIR, name), typeof body === 'string' ? body : JSON.stringify(body, null, 2));
};

function inEnv({ home, state }, fn) {
  const prev = [process.env.LANE_BROKER_HOME, process.env.LANE_BROKER_STATE];
  process.env.LANE_BROKER_HOME = home;
  process.env.LANE_BROKER_STATE = state;
  const restore = () => {
    for (const [name, value] of [['LANE_BROKER_HOME', prev[0]], ['LANE_BROKER_STATE', prev[1]]]) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  };
  try {
    const result = fn();
    if (result?.then) return result.finally(restore);
    restore();
    return result;
  } catch (err) {
    restore();
    throw err;
  }
}

// ---- (a) overlays ----

test('overlays apply in lexical filename order, the later file winning per top-level key', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { capacity: 4, laneNice: 3, sampleMs: 7000 });
  writeOverlay(env.home, '90-human.json', { capacity: 9, laneNice: 12 });
  writeOverlay(env.home, '50-bosun.json', { capacity: 6, laneNice: 8, cpuReserveCores: 2 });
  const cfg = loadGlobalConfig({ home: env.home });
  assert.deepEqual([cfg.capacity, cfg.laneNice, cfg.cpuReserveCores, cfg.sampleMs], [9, 12, 2, 7000]);
});

test('a nested object in a later file replaces the earlier one whole, with no deep merge', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { deviceGates: { thermal: { maxPressureLevel: 1 }, power: { requireAc: true } } });
  writeOverlay(env.home, '50-bosun.json', { deviceGates: { thermal: { maxPressureLevel: 2 } } });
  assert.deepEqual(loadGlobalConfig({ home: env.home }).deviceGates, { thermal: { maxPressureLevel: 2 } });
});

test('overlays apply with no config.json at all, and a missing or empty config.d is fine', () => {
  const env = freshEnv();
  assert.deepEqual(loadGlobalConfig({ home: env.home }), DEFAULT_GLOBAL_CONFIG);
  fs.mkdirSync(path.join(env.home, CONFIG_OVERLAY_DIR));
  assert.deepEqual(loadGlobalConfig({ home: env.home }), DEFAULT_GLOBAL_CONFIG);
  writeOverlay(env.home, '50-bosun.json', { capacity: 3 });
  assert.equal(loadGlobalConfig({ home: env.home }).capacity, 3);
});

test('only *.json files are overlays', () => {
  const env = freshEnv();
  writeOverlay(env.home, '50-bosun.json.tmp', { capacity: 3 });
  writeOverlay(env.home, 'README', { capacity: 3 });
  assert.equal(loadGlobalConfig({ home: env.home }).capacity, DEFAULT_GLOBAL_CONFIG.capacity);
});

test('top-level keys starting with _ are ignored in config.json and in overlays; nested keys are real data', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { _note: 'human', capacity: 4, lanePull: { queuedTimeoutMsByLane: { _lint: 5000 } } });
  writeOverlay(env.home, '50-bosun.json', { _writtenBy: 'bosun', _capacity: 99, laneNice: 12 });
  const cfg = loadGlobalConfig({ home: env.home });
  assert.deepEqual([cfg._note, cfg._writtenBy, cfg._capacity, cfg.laneNice], [undefined, undefined, undefined, 12]);
  assert.deepEqual(cfg.lanePull.queuedTimeoutMsByLane, { _lint: 5000 });
});

test('an overlay may set only LIVE_RELOAD_KEYS: anything else is dropped whole with a warning naming the file and the key', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { capacity: 4, runners: [{ name: 'a-box', ssh: 'a-box' }] });
  for (const key of ['runners', 'lanePull', 'laneQueueUrl', 'classes', 'localWindow', 'remoteBlobCache']) {
    writeOverlay(env.home, '50-bosun.json', { laneNice: 12, [key]: {} });
    const cfg = loadGlobalConfig({ home: env.home });
    assert.equal(cfg.laneNice, DEFAULT_GLOBAL_CONFIG.laneNice, `${key}: the whole overlay is set aside`);
    assert.deepEqual(cfg.runners.map((r) => r.name), ['a-box'], `${key}: config.json keeps its own`);
    assert.match(cfg.overlayWarnings[0], new RegExp(`^${path.join(env.home, CONFIG_OVERLAY_DIR, '50-bosun.json').replace(/[.]/g, '\\.')}: "${key}" cannot be set in an overlay`));
  }
});

test('a failed overlay costs only its own tuning: the rest of config.json and the other overlays stay in force, and reload never falls back to defaults', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { capacity: 4, cpuReserveCores: 3, runners: [{ name: 'a-box', ssh: 'a-box' }] });
  writeOverlay(env.home, '50-bosun.json', { capacity: -1 });
  writeOverlay(env.home, '60-other.json', { laneNice: 12 });
  inEnv(env, () => {
    const cfg = reloadGlobalConfig(undefined);
    assert.deepEqual([cfg.capacity, cfg.cpuReserveCores, cfg.laneNice, cfg.runners.length], [4, 3, 12, 1]);
    assert.equal(cfg.overlayWarnings.length, 1);
    assert.ok(cfg.overlayWarnings[0].startsWith(path.join(env.home, CONFIG_OVERLAY_DIR, '50-bosun.json')), cfg.overlayWarnings[0]);
    assert.match(cfg.overlayWarnings[0], /"capacity" must be/);
    writeOverlay(env.home, '50-bosun.json', { capacity: 6 });
    const healed = reloadGlobalConfig(cfg);
    assert.deepEqual([healed.capacity, healed.overlayWarnings], [6, undefined]);
  });
});

test('an unparseable, non-object or unreadable overlay is set aside with a warning naming its path; other layers load', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { capacity: 4 });
  writeOverlay(env.home, '50-bosun.json', '{ nope');
  writeOverlay(env.home, '60-other.json', '[1]');
  writeOverlay(env.home, '70-ok.json', { laneNice: 12 });
  const cfg = loadGlobalConfig({ home: env.home });
  assert.deepEqual([cfg.capacity, cfg.laneNice], [4, 12]);
  assert.match(cfg.overlayWarnings[0], new RegExp(`${path.join(env.home, CONFIG_OVERLAY_DIR, '50-bosun.json').replace(/[.]/g, '\\.')}: invalid JSON`));
  assert.match(cfg.overlayWarnings[1], /60-other\.json: overlay must be a JSON object/);
});

test('a config.json fault is still an error that starts with the config.json path, whatever the overlays say', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { capacity: -1 });
  writeOverlay(env.home, '50-bosun.json', { laneNice: 12 });
  assert.throws(() => loadGlobalConfig({ home: env.home }), (err) => err instanceof ConfigError && err.message.startsWith(path.join(env.home, 'config.json')));
});

test('reloadGlobalConfig covers overlays: an edit is picked up and removing the overlay reverts to config.json', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { capacity: 4 });
  inEnv(env, () => {
    const first = loadGlobalConfig();
    writeOverlay(env.home, '50-bosun.json', { capacity: 6 });
    const second = reloadGlobalConfig(first);
    assert.equal(second.capacity, 6);
    fs.rmSync(path.join(env.home, CONFIG_OVERLAY_DIR, '50-bosun.json'));
    assert.equal(reloadGlobalConfig(second).capacity, 4);
  });
});

test('the supervisor records a config warning for a dropped overlay and none for a clean config', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { capacity: 4 });
  writeOverlay(env.home, '50-bosun.json', { capacity: -1 });
  inEnv(env, () => {
    assert.equal(noteOverlayWarnings(env.state, loadGlobalConfig()), true);
    const warning = JSON.parse(fs.readFileSync(paths(env.state).configWarning, 'utf8'));
    assert.match(warning.message, /50-bosun\.json: "capacity" must be .*; ignored/);
    assert.equal(noteOverlayWarnings(env.state, { capacity: 4 }), false);
  });
});

test('the agent keeps its last good broker config when the file breaks, and says so once', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { cpuReserveCores: 3 });
  const logs = [];
  inEnv(env, () => {
    const read = brokerConfigReader({ log: (line) => logs.push(line) });
    assert.equal(read().cpuReserveCores, 3);
    fs.writeFileSync(path.join(env.home, 'config.json'), '{ torn');
    assert.equal(read().cpuReserveCores, 3);
    assert.equal(read().cpuReserveCores, 3);
    assert.equal(logs.length, 1);
    assert.match(logs[0], /unreadable, keeping the last good config/);
    writeGlobalConfig(env.home, { cpuReserveCores: 5 });
    writeOverlay(env.home, '50-bosun.json', { capacity: -1 });
    assert.equal(read().cpuReserveCores, 5);
    assert.equal(logs.length, 2);
    assert.match(logs[1], /50-bosun\.json/);
  });
});

// ---- (b) lane config effective ----

test('loadEffectiveGlobalConfig names the file that set each top-level key', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { capacity: 4, laneNice: 3 });
  writeOverlay(env.home, '50-bosun.json', { capacity: 6, cpuReserveCores: 2 });
  writeOverlay(env.home, '90-human.json', { cpuReserveCores: 1 });
  const { sources, files } = loadEffectiveGlobalConfig({ home: env.home });
  assert.deepEqual([sources.capacity, sources.laneNice, sources.cpuReserveCores, sources.sampleMs], ['config.d/50-bosun.json', 'config.json', 'config.d/90-human.json', 'default']);
  assert.deepEqual(files, ['config.json', 'config.d/50-bosun.json', 'config.d/90-human.json']);
});

test('lane config effective --json prints the merged config, per-key sources and the live-reload list', async () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { capacity: 4 });
  writeOverlay(env.home, '50-bosun.json', { capacity: 6, _by: 'bosun' });
  const result = await laneRun(['config', 'effective', '--json'], { env: env.env });
  assert.equal(result.code, 0, result.stderr);
  const out = JSON.parse(result.stdout);
  assert.equal(out.config.capacity, 6);
  assert.equal(out.sources.capacity, 'config.d/50-bosun.json');
  assert.equal(out.sources.schedulerMode, 'config.json');
  assert.equal(out.sources.laneNice, 'default');
  assert.deepEqual(out.liveReload, [...LIVE_RELOAD_KEYS]);
  assert.equal(out.config._by, undefined);
});

test('lane config effective prints one line per key with its source, and changes nothing', async () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { capacity: 4 });
  writeOverlay(env.home, '50-bosun.json', { cpuReserveCores: 2 });
  const before = fs.readdirSync(env.home, { recursive: true }).sort();
  const result = await laneRun(['config', 'effective'], { env: env.env });
  assert.equal(result.code, 0, result.stderr);
  assert.match(result.stdout, /^files: config\.json, config\.d\/50-bosun\.json$/m);
  assert.match(result.stdout, /^capacity = 4 {2}\[config\.json\]$/m);
  assert.match(result.stdout, /^cpuReserveCores = 2 {2}\[config\.d\/50-bosun\.json\]$/m);
  assert.match(result.stdout, /^laneNice = 10 {2}\[default\]$/m);
  assert.match(result.stdout, /^live reload \(no restart\): capacity, /m);
  assert.deepEqual(fs.readdirSync(env.home, { recursive: true }).sort(), before);
});

test('lane config effective: a dropped overlay is reported as a warning and the config shown is the one in force; bad arguments exit 2', async () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { capacity: 4 });
  writeOverlay(env.home, '50-bosun.json', { capacity: -1 });
  const shown = await laneRun(['config', 'effective', '--json'], { env: env.env });
  assert.equal(shown.code, 0, shown.stderr);
  const out = JSON.parse(shown.stdout);
  assert.deepEqual([out.config.capacity, out.sources.capacity, out.files], [4, 'config.json', ['config.json']]);
  assert.match(out.warnings[0], /50-bosun\.json: "capacity" must be/);
  assert.match((await laneRun(['config', 'effective'], { env: env.env })).stdout, /^WARNING: .*50-bosun\.json/m);
  const broken = freshEnv();
  writeGlobalConfig(broken.home, { capacity: -1 });
  assert.equal((await laneRun(['config', 'effective'], { env: broken.env })).code, 1);
  for (const args of [['config'], ['config', 'show'], ['config', 'effective', '--yaml']]) assert.equal((await laneRun(args, { env: env.env })).code, 2, args.join(' '));
});

// ---- (c) the live-reload list ----

const LIVE_VALUES = {
  capacity: [4, 7],
  cpuAdmissionPercent: [90, 60],
  cpuReserveCores: [1, 3],
  memoryReserveBytes: [1_000_000_000, 2_000_000_000],
  schedulerMode: ['active', 'shadow'],
  sampleMs: [5000, 1500],
  laneNice: [10, 15],
  priorityNice: [{ high: 5 }, { high: 2, low: 19 }],
  localExecution: ['allowed', 'refused'],
  deviceGates: [{ thermal: { maxPressureLevel: 1 } }, { thermal: { maxPressureLevel: 2 } }],
};

test('the live-reload table here covers exactly LIVE_RELOAD_KEYS', () => {
  assert.deepEqual(Object.keys(LIVE_VALUES).sort(), [...LIVE_RELOAD_KEYS].sort());
});

for (const key of LIVE_RELOAD_KEYS) {
  test(`live reload: changing "${key}" in config.json or an overlay reaches a running process on its next reload`, () => {
    const env = freshEnv();
    const [first, second] = LIVE_VALUES[key];
    writeGlobalConfig(env.home, { [key]: first });
    inEnv(env, () => {
      const loaded = loadGlobalConfig();
      assert.deepEqual(loaded[key], first);
      writeGlobalConfig(env.home, { [key]: second });
      const afterEdit = reloadGlobalConfig(loaded);
      assert.deepEqual(afterEdit[key], second, 'a config.json edit');
      writeOverlay(env.home, '50-bosun.json', { [key]: first });
      assert.deepEqual(reloadGlobalConfig(afterEdit)[key], first, 'an overlay added later');
    });
  });
}

const HOST = { cpuCores: 8, memoryBytes: 16_000_000_000 };

test('live reload, admission: capacity, schedulerMode and the CPU and memory budgets change what the next poll decides', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { capacity: 'auto', cpuReserveCores: 1, schedulerMode: 'active', cpuAdmissionPercent: 100, memoryReserveBytes: 0 });
  inEnv(env, () => {
    const request = { resources: { cpuCores: 6, memoryBytes: 1_000_000_000 }, host: HOST };
    let cfg = loadGlobalConfig();
    assert.equal(effectiveWeightCapacity(cfg, HOST.cpuCores), 7);
    assert.equal(checkResourceBudget({ ...request, globalCfg: cfg }).ok, true);

    writeOverlay(env.home, '50-bosun.json', { capacity: 3, cpuReserveCores: 4 });
    cfg = reloadGlobalConfig(cfg);
    assert.equal(effectiveWeightCapacity(cfg, HOST.cpuCores), 3);
    assert.equal(checkResourceBudget({ ...request, globalCfg: cfg }).ok, false, 'the larger reserve leaves 4 cores');

    writeOverlay(env.home, '90-human.json', { schedulerMode: 'shadow' });
    cfg = reloadGlobalConfig(cfg);
    assert.equal(checkResourceBudget({ ...request, globalCfg: cfg }).ok, true, 'shadow mode admits whatever the budget says');

    writeOverlay(env.home, '90-human.json', { memoryReserveBytes: 16_000_000_000 });
    cfg = reloadGlobalConfig(cfg);
    assert.equal(checkResourceBudget({ ...request, globalCfg: { ...cfg, schedulerMode: 'active', cpuReserveCores: 0 } }).ok, false, 'the memory reserve takes it all');
  });
});

test('live reload, agent claims: the figures a claim offers follow the reserves in config.json and overlays on every call, with no restart', async () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { schedulerMode: 'shadow', cpuAdmissionPercent: 100, cpuReserveCores: 1, memoryReserveBytes: 1_000_000_000 });
  await inEnv(env, async () => {
    assert.deepEqual(await claimFigures({ host: HOST }), { freeCpu: 7, freeMem: 15_000_000_000, budgetCpu: 7 });
    writeOverlay(env.home, '50-bosun.json', { cpuReserveCores: 4, memoryReserveBytes: 6_000_000_000 });
    assert.deepEqual(await claimFigures({ host: HOST }), { freeCpu: 4, freeMem: 10_000_000_000, budgetCpu: 4 });
    writeOverlay(env.home, '90-human.json', { cpuAdmissionPercent: 25 });
    assert.deepEqual(await claimFigures({ host: HOST }), { freeCpu: 2, freeMem: 10_000_000_000, budgetCpu: 2 });
  });
});

test('live reload, nice: laneNice and priorityNice decide the nice of the next lane to start', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { laneNice: 10, priorityNice: { high: 5 } });
  inEnv(env, () => {
    let cfg = loadGlobalConfig();
    assert.deepEqual([resolveLaneNice({}, 'medium', cfg), resolveLaneNice({}, 'high', cfg)], [10, 5]);
    writeOverlay(env.home, '50-bosun.json', { laneNice: 14, priorityNice: { high: 2 } });
    cfg = reloadGlobalConfig(cfg);
    assert.deepEqual([resolveLaneNice({}, 'medium', cfg), resolveLaneNice({}, 'high', cfg)], [14, 2]);
  });
});

test('live reload, dispatch-only: localExecution flips a running supervisor to refusing local execution', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, {});
  inEnv(env, () => {
    let cfg = loadGlobalConfig();
    assert.equal(isDispatchOnly(cfg), false);
    writeOverlay(env.home, '50-bosun.json', { localExecution: 'refused' });
    cfg = reloadGlobalConfig(cfg);
    assert.equal(isDispatchOnly(cfg), true);
  });
});

test('live reload, device gates: the agent reads deviceGates through loadGlobalConfig({ home }), so an overlay edit reaches its next sample', () => {
  const env = freshEnv();
  writeGlobalConfig(env.home, { deviceGates: { thermal: { maxPressureLevel: 1 } } });
  assert.equal(loadGlobalConfig({ home: env.home }).deviceGates.thermal.maxPressureLevel, 1);
  writeOverlay(env.home, '50-bosun.json', { deviceGates: { thermal: { maxPressureLevel: 3 } } });
  assert.equal(loadGlobalConfig({ home: env.home }).deviceGates.thermal.maxPressureLevel, 3);
});
