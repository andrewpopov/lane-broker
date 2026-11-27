import { openAgentCore } from './agent.js';
import { AgentConfigError } from './config.js';
import { brokerConfigReader, claimBlock, claimFigures, claimOnce, drainOwners, drainsOf, isDraining, presentTools } from './claims.js';
import { ExecutionDriver } from './execution.js';
import { createS3Client } from '../s3-client.js';
import { Waker } from './waker.js';
import { writeAgentMetrics } from './metrics.js';
import { capByGrant, openBudgetShadow } from './budget.js';
import { userManagerEnv } from '../cpu-limit.js';
import { loadApproaching, sampleDevice, updateDeviceGates } from '../device-gates.js';

export const STOP_SETTLE_MS = 45_000;

const UNKNOWN_DEVICE_SAMPLE = { thermal: {}, power: {}, memory: {}, load: {} };

export const s3ClientOf = ({ endpoint, region, creds }, options = {}) => createS3Client({ endpoint, region, creds, ...options });

const LIVENESS_CALLS = new Set(['ld_claim', 'ld_heartbeat', 'ld_register']);

/**
 * Open the agent core (directories durable, owner taken, registered), adopt every record on disk, and keep claiming: on a NOTIFY hint,
 * every `config.pollMs` (at most 15 s) as the fallback, and again straight after each claim until the queue answers `none`. Whatever wakes
 * the loop, no more than `pollMs` passes between two liveness calls (`ld_claim`, `ld_heartbeat`, `ld_register`): a runner that is draining,
 * outside its window or short of headroom, holding nothing, pings with `ld_register`. An `upgrade` answer stops claiming for good and is
 * logged on every tick; running executions carry on. Stopping leaves running executions running: they outlive the agent. With a textfile
 * directory (`config.metricsTextfileDir`, else bosun's `BOSUN_TEXTFILE_DIR`) a tick writes `lane-agent.prom` there, at most once per `pollMs`.
 *
 * The broker's home and state directory come from `config` and replace whatever the environment says, here and in the lane processes this
 * agent spawns, so a supervisor's minimal environment and an operator's shell behave the same.
 */
export async function startAgent({ remoteRoot, config, version, log = (line) => process.stderr.write(`${line}\n`), figures, draining, now = Date.now, timing, s3 = s3ClientOf(config.rustfs), spawnEnv = process.env, laneBin, Driver = ExecutionDriver, coreOptions = {}, textfileDir = config.metricsTextfileDir ?? process.env.BOSUN_TEXTFILE_DIR, stopSettleMs = STOP_SETTLE_MS, platform = process.platform, driverOptions = {}, tools, deviceSampler = (gates) => sampleDevice(gates, { platform }), budgetOptions = {} }) {
  const brokerEnv = { LANE_BROKER_HOME: config.brokerHome, LANE_BROKER_STATE: config.stateDir, ...userManagerEnv({ ...process.env, ...spawnEnv }, { platform }) };
  Object.assign(process.env, brokerEnv);
  const readBrokerConfig = brokerConfigReader({ log });
  let budget;
  const claimFiguresOf = figures ?? (async (pending) => {
    const cfg = readBrokerConfig();
    return capByGrant(await claimFigures({ cfg, ...pending }), budget.enforced(cfg));
  });
  const isDrainingNow = draining ?? (() => isDraining(config.stateDir));
  const waker = new Waker();
  let driver;
  let poked = false;
  let lastLivenessAt = now();
  const onCall = (name, args) => {
    if (LIVENESS_CALLS.has(name)) lastLivenessAt = now();
    coreOptions.onCall?.(name, args);
  };
  const core = await openAgentCore({ remoteRoot, config, version, log, onWake: (event) => { poked = true; driver?.poke(event.channel); waker.poke(); }, ...coreOptions, onCall });
  if (core.ld.registered.host !== config.runnerName) {
    await core.close();
    throw new AgentConfigError(`"runnerName" is ${config.runnerName} but the queue registers this login as ${core.ld.registered.host}`);
  }
  budget = openBudgetShadow({ readConfig: readBrokerConfig, records: () => core.table.all(), log, now, ...budgetOptions });
  const laneEnv = { ...spawnEnv, ...brokerEnv };
  const claimTools = tools ?? (() => presentTools({ ld: core.ld, envPath: laneEnv.PATH }));
  driver = new Driver({ core, remoteRoot, config, s3, timing, waker, log, spawnEnv: laneEnv, laneBin, draining: isDrainingNow, now, onScopeChange: () => { poked = true; waker.poke(); }, ...driverOptions });
  driver.adoptAll();
  driver.watchScope();
  let stopped = false;
  let upgradeRequired = false;
  const claimAttempts = {};
  let claimed = 0;
  const countClaim = (answer) => { claimAttempts[answer] = (claimAttempts[answer] ?? 0) + 1; };
  let deviceGates = {};
  let deviceSampledAt = -Infinity;
  let deviceSampleError = null;
  let deviceGatesConfig = config.deviceGates;
  let metricsWrittenAt = -Infinity;
  const writeMetrics = (tickAt) => {
    if (!textfileDir || tickAt - metricsWrittenAt < config.pollMs) return;
    metricsWrittenAt = tickAt;
    try {
      writeAgentMetrics(textfileDir, { now: now(), records: core.table.all(), claimed, claimAttempts, listening: core.ld.listening, registered: core.ld.registeredOnConnection, scopeAvailable: driver.scopeAvailable, version, deviceGates, budgetShadow: budget.shadow() });
    } catch (err) {
      log(`metrics: ${err.message}`);
    }
  };
  const block = () => stopped ? 'stopping' : driver.scopeBlock() ?? claimBlock({ schedule: config.schedule, stopClaimingBeforeCloseMs: config.stopClaimingBeforeCloseMs, now: now(), draining: isDrainingNow, owners: () => drainOwners(config.stateDir), deviceGates, idle: !core.table.all().some((r) => r.subphase !== 'DONE') });

  const reloadDeviceGatesConfig = () => { deviceGatesConfig = readBrokerConfig().deviceGates ?? null; };

  async function refreshDeviceGates() {
    if (now() - deviceSampledAt < config.pollMs) return;
    deviceSampledAt = now();
    reloadDeviceGatesConfig();
    let sample = UNKNOWN_DEVICE_SAMPLE;
    try {
      sample = await deviceSampler(deviceGatesConfig);
      deviceSampleError = null;
    } catch (err) {
      if (err.message !== deviceSampleError) log(`device gates: sampling failed, readings unknown: ${err.message}`);
      deviceSampleError = err.message;
    }
    const sensorWas = deviceGates.memory?.sensor;
    deviceGates = updateDeviceGates(deviceGates, deviceGatesConfig, sample);
    if (deviceGates.memory?.sensor === 'unavailable' && sensorWas !== 'unavailable') log('device gates: memory sensor-unavailable; the gate cannot close on unknown readings and reopens after 3 of them');
  }

  const deviceGateStatus = () => Object.fromEntries(Object.entries(deviceGates).map(([name, { closed, consecutiveOk, reading, sensor }]) => [name, { closed, consecutiveOk, reading, ...(sensor ? { sensor } : {}) }]));

  // PKG-440: a NOTIFY (including the one our own claim raises) starts a tick at once, so while load approaches its limit the pace is one claim per pollMs, not per tick.
  let hotClaimAt = -Infinity;
  async function claimRound() {
    while (!stopped && !upgradeRequired) {
      if (loadApproaching(deviceGates, deviceGatesConfig) && now() - hotClaimAt < config.pollMs) return;
      let answer;
      try {
        answer = await claimOnce({ core, remoteRoot, figures: claimFiguresOf, block, now, tools: claimTools, log });
      } catch (err) {
        if (!stopped) log(`claim: ${err.code ?? ''} ${err.message}`);
        countClaim('error');
        return;
      }
      if (answer.status !== 'skipped') countClaim(answer.status);
      if (answer.status === 'upgrade') upgradeRequired = true;
      else if (answer.status === 'claimed') {
        claimed += 1;
        driver.adopt(answer.execId);
        const claimedAt = now();
        await refreshDeviceGates(); // self-gating: a sample that fell due during a long round is taken before the next claim, so block() and the pacing see it
        if (loadApproaching(deviceGates, deviceGatesConfig)) {
          hotClaimAt = claimedAt;
          return;
        }
      }
      else return;
    }
  }

  const done = (async () => {
    let pollAt = now();
    let pingAt = 0;
    while (!stopped) {
      if (poked || now() >= pollAt) {
        poked = false;
        const tickAt = now();
        pollAt = tickAt + config.pollMs;
        driver.watchScope();
        await refreshDeviceGates();
        if (upgradeRequired) log('UPGRADE REQUIRED: the queue refuses this agent\'s protocol, so no claims are made; update lane-broker and restart the agent');
        else await claimRound();
        const blocked = block();
        core.tick({ claims: upgradeRequired ? 'stopped: upgrade required' : blocked ? `paused:${blocked}` : 'running', drains: drainsOf(config.stateDir), ...(Object.keys(deviceGates).length ? { deviceGates: deviceGateStatus() } : {}) });
        writeMetrics(tickAt);
        budget.heartbeat();
      }
      const lastSignOfLife = () => Math.max(lastLivenessAt, pingAt);
      if (!stopped && now() - lastSignOfLife() >= config.pollMs) {
        pingAt = now();
        await core.ld.register().catch((err) => log(`register ping: ${err.code ?? ''} ${err.message}`));
      }
      if (!stopped && !poked) await waker.wait(Math.max(0, Math.min(pollAt, lastSignOfLife() + config.pollMs) - now()));
    }
  })();

  return {
    core, driver, done,
    /** Claims stop at once; in-flight claims and outbox finishes get `stopSettleMs` to settle; running lanes are left running. */
    async stop() {
      stopped = true;
      waker.poke();
      await driver.close({ settleMs: stopSettleMs });
      await core.close();
      await done;
      await budget.close();
    },
  };
}
