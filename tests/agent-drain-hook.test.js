import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { SLEEP, gatedQuery, happens, scenarioSuite, sleep } from './agent-run-harness.js';
import { readAgentStatus } from '../src/agent/store.js';

const DRAIN = { BOSUN_DRAIN_ID: 'drain-1', BOSUN_DRAIN_MODE: 'interrupt', BOSUN_DRAIN_REASON: 'deploy' };
const hookOf = (load, rw) => async (verb, env = DRAIN) => (await load('agent/drain-hook.js')).drainHookCommand([verb], { remoteRoot: rw.root, env, loadConfig: () => ({ stateDir: rw.ctx.state, brokerHome: rw.ctx.home }) }).then((r) => r.exitCode);
const drainsOf = (rw) => readAgentStatus(rw.root).drains;

const scenarios = {
  'under a bosun drain no ld_claim is sent while a running lane still finishes; after the lift claims resume': async (w, rw, load) => {
    const hook = hookOf(load, rw);
    const release = path.join(makeTmpDir('drain-hook-release-'), 'release');
    const running = await rw.submit({ code: `const fs = require('fs'); setInterval(() => { if (fs.existsSync(${JSON.stringify(release)})) process.exit(0); }, 50);` });
    const agent = await rw.agent({ load });
    const runningId = rw.execId(await rw.claimed(running));
    await happens('the lane is running', () => agent.core.table.get(runningId)?.subphase === 'RUNNING');

    assert.equal(await hook('--apply'), 0);
    assert.equal(await hook('--status'), 75, 'the agent has not ticked since the marker');
    await happens('the agent status lists the drain id', () => drainsOf(rw)?.some((d) => d.owner === 'bosun' && d.id === 'drain-1'));
    assert.equal(await hook('--status'), 0);

    const queued = await rw.submit({ code: SLEEP });
    const claimsBefore = agent.callsTo('ld_claim').length;
    await sleep(700);
    assert.equal(agent.callsTo('ld_claim').length, claimsBefore, 'no ld_claim is sent under the drain');
    assert.equal((await w.ticket(queued.id)).state, 'queued');

    fs.writeFileSync(release, '');
    await rw.untilGone(agent, runningId);
    assert.equal((await w.ticket(running.id)).state, 'finished', 'the running lane finished and published under the drain');
    assert.ok(agent.names().includes('ld_finish'), 'its finish was published during the drain');

    assert.equal(await hook('--lift'), 0);
    await rw.claimed(queued);
  },
};

const scenariosMore = {
  'a drain reapplied under the same id while a claim is held is not acknowledged until a tick after the claim settles': async (w, rw, load) => {
    const hook = hookOf(load, rw);
    const gate = gatedQuery('ld_claim', { armed: false });
    await rw.agent({ load, coreOptions: { connect: gate.connect } });
    assert.equal(await hook('--apply'), 0);
    await happens('the first application is acknowledged', async () => await hook('--status') === 0);
    gate.armed = true;
    await rw.submit({ code: SLEEP });
    await sleep(300);
    assert.equal(gate.sent, false, 'a drained agent sends no claim');
    assert.equal(await hook('--lift'), 0);
    await happens('a claim is held in flight', () => gate.sent);
    assert.equal(await hook('--apply'), 0);
    await sleep(500);
    assert.equal(await hook('--status'), 75, 'the old acknowledgement is not this application');
    gate.armed = false;
    gate.release();
    await happens('the tick after the claim settles acknowledges the new application', async () => await hook('--status') === 0);
  },
};

const RUN = 'agent/run.js';
const canaries = [
  ['under a bosun drain no ld_claim is sent while a running lane still finishes; after the lift claims resume', RUN, ['(() => isDraining(config.stateDir))', '(() => false)']],
  ['under a bosun drain no ld_claim is sent while a running lane still finishes; after the lift claims resume', RUN, ['drains: drainsOf(config.stateDir), ', '']],
];

const DRAIN_HOOK = 'agent/drain-hook.js';
canaries.push(['a drain reapplied under the same id while a claim is held is not acknowledged until a tick after the claim settles', DRAIN_HOOK, [' && d.applied === marker.applied', '']]);

scenarioSuite('agent drain hook on a disposable Postgres', { scenarios: { ...scenarios, ...scenariosMore }, canaries });
