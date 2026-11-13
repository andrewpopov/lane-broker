import { openAgentCore } from './agent.js';
import { AgentConfigError } from './config.js';
import { claimBlock, claimFigures, claimOnce } from './claims.js';
import { ExecutionDriver } from './execution.js';
import { createS3Client } from '../s3-client.js';
import { Waker } from './waker.js';

export const s3ClientOf = ({ endpoint, region, creds }, options = {}) => createS3Client({ endpoint, region, creds, ...options });

/**
 * Open the agent core (directories durable, owner taken, registered), adopt every record on disk, and keep claiming: on a NOTIFY hint,
 * every `config.pollMs` (at most 15 s) as the fallback and liveness call, and again straight after each claim until the queue answers
 * `none`. An `upgrade` answer stops claiming for good and is logged on every tick; running executions carry on. Stopping leaves running
 * executions running: they outlive the agent.
 */
export async function startAgent({ remoteRoot, config, version, log = (line) => process.stderr.write(`${line}\n`), figures = claimFigures, draining, now = Date.now, timing, s3 = s3ClientOf(config.rustfs), spawnEnv, laneBin, Driver = ExecutionDriver, coreOptions = {} }) {
  const waker = new Waker();
  let driver;
  const core = await openAgentCore({ remoteRoot, config, version, log, onWake: (event) => { driver?.poke(event.channel); waker.poke(); }, ...coreOptions });
  if (core.ld.registered.host !== config.runnerName) {
    await core.close();
    throw new AgentConfigError(`"runnerName" is ${config.runnerName} but the queue registers this login as ${core.ld.registered.host}`);
  }
  driver = new Driver({ core, remoteRoot, config, s3, timing, waker, log, spawnEnv, laneBin, draining, now });
  driver.adoptAll();
  let stopped = false;
  let upgradeRequired = null;
  const block = () => claimBlock({ schedule: config.schedule, stopClaimingBeforeCloseMs: config.stopClaimingBeforeCloseMs, now: now(), ...(draining ? { draining } : {}) });

  async function claimRound() {
    while (!stopped && upgradeRequired === null) {
      let answer;
      try {
        answer = await claimOnce({ core, remoteRoot, figures, block, now });
      } catch (err) {
        if (!stopped) log(`claim: ${err.code ?? ''} ${err.message}`);
        return;
      }
      if (answer.status === 'upgrade') upgradeRequired = true;
      else if (answer.status === 'claimed') driver.adopt(answer.execId);
      else return;
    }
  }

  const done = (async () => {
    while (!stopped) {
      const startedAt = now();
      if (upgradeRequired) log('UPGRADE REQUIRED: the queue refuses this agent\'s protocol, so no claims are made; update lane-broker and restart the agent');
      else await claimRound();
      core.tick({ claims: upgradeRequired ? 'stopped: upgrade required' : 'running' });
      if (!stopped) await waker.wait(Math.max(0, config.pollMs - (now() - startedAt)));
    }
  })();

  return {
    core, driver, done,
    async stop() {
      stopped = true;
      waker.poke();
      const closing = driver.close();
      await core.close();
      await Promise.all([done, closing]);
    },
  };
}
