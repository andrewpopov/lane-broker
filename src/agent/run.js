import { openAgentCore } from './agent.js';
import { AgentConfigError } from './config.js';
import { loadGlobalConfig } from '../config.js';
import { claimBlock, claimFigures, claimOnce, isDraining } from './claims.js';
import { ExecutionDriver } from './execution.js';
import { createS3Client } from '../s3-client.js';
import { Waker } from './waker.js';
import { writeAgentMetrics } from './metrics.js';
import { userManagerEnv } from '../cpu-limit.js';

export const STOP_SETTLE_MS = 45_000;

export const s3ClientOf = ({ endpoint, region, creds }, options = {}) => createS3Client({ endpoint, region, creds, ...options });

const LIVENESS_CALLS = new Set(['ld_claim', 'ld_heartbeat', 'ld_register']);

/**
 * Open the agent core (directories durable, owner taken, registered), adopt every record on disk, and keep claiming: on a NOTIFY hint,
 * every `config.pollMs` (at most 15 s) as the fallback, and again straight after each claim until the queue answers `none`. Whatever wakes
 * the loop, no more than `pollMs` passes between two liveness calls (`ld_claim`, `ld_heartbeat`, `ld_register`): a runner that is draining,
 * outside its window or short of headroom, holding nothing, pings with `ld_register`. An `upgrade` answer stops claiming for good and is
 * logged on every tick; running executions carry on. Stopping leaves running executions running: they outlive the agent. With `textfileDir`
 * (bosun's `BOSUN_TEXTFILE_DIR`) each tick also writes `lane-agent.prom` there.
 *
 * The broker's home and state directory come from `config` and replace whatever the environment says, here and in the lane processes this
 * agent spawns, so a supervisor's minimal environment and an operator's shell behave the same.
 */
export async function startAgent({ remoteRoot, config, version, log = (line) => process.stderr.write(`${line}\n`), figures, draining, now = Date.now, timing, s3 = s3ClientOf(config.rustfs), spawnEnv = process.env, laneBin, Driver = ExecutionDriver, coreOptions = {}, textfileDir = process.env.BOSUN_TEXTFILE_DIR, stopSettleMs = STOP_SETTLE_MS, platform = process.platform, driverOptions = {} }) {
  const brokerEnv = { LANE_BROKER_HOME: config.brokerHome, LANE_BROKER_STATE: config.stateDir, ...userManagerEnv({ ...process.env, ...spawnEnv }, { platform }) };
  Object.assign(process.env, brokerEnv);
  const claimFiguresOf = figures ?? (() => claimFigures({ cfg: loadGlobalConfig({ home: config.brokerHome }) }));
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
  driver = new Driver({ core, remoteRoot, config, s3, timing, waker, log, spawnEnv: { ...spawnEnv, ...brokerEnv }, laneBin, draining: isDrainingNow, now, onScopeChange: () => { poked = true; waker.poke(); }, ...driverOptions });
  driver.adoptAll();
  driver.watchScope();
  let stopped = false;
  let upgradeRequired = false;
  const claims = {};
  const countClaim = (answer) => { claims[answer] = (claims[answer] ?? 0) + 1; };
  const writeMetrics = () => {
    if (!textfileDir) return;
    try {
      writeAgentMetrics(textfileDir, { now: now(), records: core.table.all(), claims, listening: core.ld.listening, registered: core.ld.registeredOnConnection, scopeAvailable: driver.scopeAvailable, version });
    } catch (err) {
      log(`metrics: ${err.message}`);
    }
  };
  const block = () => stopped ? 'stopping' : driver.scopeBlock() ?? claimBlock({ schedule: config.schedule, stopClaimingBeforeCloseMs: config.stopClaimingBeforeCloseMs, now: now(), draining: isDrainingNow });

  async function claimRound() {
    while (!stopped && !upgradeRequired) {
      let answer;
      try {
        answer = await claimOnce({ core, remoteRoot, figures: claimFiguresOf, block, now });
      } catch (err) {
        if (!stopped) log(`claim: ${err.code ?? ''} ${err.message}`);
        countClaim('error');
        return;
      }
      if (answer.status !== 'skipped') countClaim(answer.status);
      if (answer.status === 'upgrade') upgradeRequired = true;
      else if (answer.status === 'claimed') driver.adopt(answer.execId);
      else return;
    }
  }

  const done = (async () => {
    let pollAt = now();
    let pingAt = 0;
    while (!stopped) {
      if (poked || now() >= pollAt) {
        poked = false;
        pollAt = now() + config.pollMs;
        driver.watchScope();
        if (upgradeRequired) log('UPGRADE REQUIRED: the queue refuses this agent\'s protocol, so no claims are made; update lane-broker and restart the agent');
        else await claimRound();
        core.tick({ claims: upgradeRequired ? 'stopped: upgrade required' : 'running' });
        writeMetrics();
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
    },
  };
}
