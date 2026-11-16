import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeTmpDir } from './helpers/tmp.js';
import { happens, scenarioSuite, sleep } from './agent-run-harness.js';
import { downgradeToV6 } from './lane-dispatch-fixture.js';

const TOOL = 'brain1135-tool';

const scenarios = {
  'a runner without a required tool on its lanes\' PATH never claims the ticket, and claims and runs it once the tool is there': async (w, rw, load) => {
    const bin = makeTmpDir('agent-requires-bin-');
    const ticket = await rw.submit({ requires: [TOOL] });
    const agent = await rw.agent({ load, spawnEnv: { ...rw.ctx.env, PATH: `${rw.ctx.env.PATH}${path.delimiter}${bin}` } });
    await happens('the agent asked the queue which tools to check, several times', () => agent.callsTo('ld_needed_tools').length >= 3);
    await sleep(300);
    assert.equal((await w.ticket(ticket.id)).epoch, 0, 'never claimed while the tool is missing');
    assert.ok(agent.callsTo('ld_claim').every((args) => Array.isArray(args[4]) && !args[4].includes(TOOL)), 'each claim named the tools it found, without the missing one');
    fs.writeFileSync(path.join(bin, TOOL), '#!/bin/sh\nexit 0\n', { mode: 0o755 });
    const row = await rw.claimed(ticket);
    await rw.untilGone(agent, rw.execId(row));
    assert.equal((await w.ticket(ticket.id)).state, 'finished');
  },

  'a claimed ticket whose tool is missing when it would start is released never-started, and nothing is spawned': async (w, rw, load) => {
    const ticket = await rw.submit({ requires: [TOOL] });
    const agent = await rw.agent({ load, tools: async () => [TOOL] });
    await rw.claimed(ticket);
    await happens('the claim was released', () => agent.names().includes('ld_release'));
    assert.equal(agent.names().includes('ld_start'), false, 'ld_start was never sent');
    assert.deepEqual(agent.callsTo('ld_release')[0].slice(3), ['never-started']);
    assert.ok(rw.logs.some((line) => line.includes(`missing tool ${TOOL}`)), rw.logs.join(' | '));
  },

  'on a queue older than migration 008 the agent claims with the old 4-argument call and runs a ticket': async (w, rw, load) => {
    await downgradeToV6(w);
    const ticket = await rw.submit();
    const agent = await rw.agent({ load });
    await rw.untilGone(agent, rw.execId(await rw.claimed(ticket)));
    assert.equal((await w.ticket(ticket.id)).state, 'finished');
    assert.ok(agent.callsTo('ld_claim').every((args) => args.length === 4), 'no has_tools argument for a queue that has no such parameter');
  },
};

const canaries = [
  ['a runner without a required tool on its lanes\' PATH never claims the ticket, and claims and runs it once the tool is there', 'agent/claims.js', ['return toolsOnPath(needed ?? [], envPath);', 'return needed ?? [];']],
  ['a runner without a required tool on its lanes\' PATH never claims the ticket, and claims and runs it once the tool is there', 'agent/run.js', ['envPath: laneEnv.PATH', 'envPath: process.env.PATH']],
  ['a claimed ticket whose tool is missing when it would start is released never-started, and nothing is spawned', 'agent/execution.js', ['if (missing.length > 0) {', 'if (false) {']],
  ['on a queue older than migration 008 the agent claims with the old 4-argument call and runs a ticket', 'agent/claims.js', ['if (err.code === UNDEFINED_FUNCTION) return null;', '']],
];

scenarioSuite('lane agent: requires (BRAIN-1135)', { scenarios, canaries });
