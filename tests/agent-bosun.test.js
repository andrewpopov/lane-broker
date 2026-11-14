import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import pg from 'pg';
import { makeTmpDir } from './helpers/tmp.js';
import { SLEEP, happens, recordingFetch, scenarioSuite, sleep } from './agent-run-harness.js';
import { METRICS_FILE } from '../src/agent/metrics.js';
import { paths, readJsonSafe } from '../src/state.js';
import { readAgentStatus } from '../src/agent/store.js';
import { holdLock, waitBlocked } from './lane-dispatch-fixture.js';
import { backendPids } from './agent-harness.js';

const groupAlive = (pgid) => { try { process.kill(-pgid, 0); return true; } catch { return false; } };
const leaseOf = (rw) => {
  const dir = paths(rw.ctx.state).leases;
  const name = fs.existsSync(dir) ? fs.readdirSync(dir).find((n) => n.endsWith('.json')) : undefined;
  return name ? readJsonSafe(path.join(dir, name)) : null;
};
const series = (text, name) => text.split('\n').find((line) => line.startsWith(`${name} `))?.split(' ')[1];
const readProm = (dir) => { try { return fs.readFileSync(path.join(dir, METRICS_FILE), 'utf8'); } catch { return ''; } };

/** Driver options that run the real command through a recording spawn and a stubbed scope probe: systemd is not needed to see which argv the agent chose. */
const scopeOptions = (calls, probe, extra = {}) => ({
  probeScope: probe,
  spawnProcess: (cmd, args, spawnOptions) => {
    const child = cmd === 'systemd-run' ? spawn(args[args.indexOf('--') + 1], args.slice(args.indexOf('--') + 2), spawnOptions) : spawn(cmd, args, spawnOptions);
    calls.push({ cmd, args, pid: child.pid, env: spawnOptions.env });
    return child;
  },
  ...extra,
});
const stubProbe = (scope) => async () => scope;

/** `ld_finish` queries held at a gate until `release()`, or failed when their connection is ended (as pg does to an in-flight query). */
function gatedFinish() {
  const gate = { sent: false };
  const open = new Promise((resolve) => { gate.release = resolve; });
  gate.connect = async (config) => {
    const client = new pg.Client(config);
    client.on('error', () => {});
    await client.connect();
    const query = client.query.bind(client);
    const end = client.end.bind(client);
    let ended;
    const closed = new Promise((_, reject) => { ended = reject; });
    closed.catch(() => {});
    client.query = async (...args) => {
      if (String(args[0]).includes('ld_finish')) { gate.sent = true; await Promise.race([open, closed]); }
      return query(...args);
    };
    client.end = () => { ended(new Error('Connection terminated')); return end(); };
    return client;
  };
  return gate;
}

const scenarios = {
  'a passing scope probe starts remote-exec as systemd-run --scope with a lane-exec unit and no quota, and the record keeps the supervisor pid': async (w, rw, load) => {
    const calls = [];
    const ticket = await rw.submit({ code: SLEEP });
    const agent = await rw.agent({ load, driverOptions: scopeOptions(calls, stubProbe({ available: true, reason: 'ok' }), { scopeRequired: true }) });
    const execId = rw.execId(await rw.claimed(ticket));
    await happens('the lane is running', () => agent.core.table.get(execId)?.subphase === 'RUNNING');
    assert.equal(calls.length, 1);
    assert.equal(calls[0].cmd, 'systemd-run');
    const { args } = calls[0];
    assert.deepEqual(args.slice(0, args.indexOf('--') + 1), ['--user', '--scope', '--expand-environment=no', `--unit=lane-exec-${execId}`, '--quiet', '--collect', '--']);
    assert.equal(args.includes('-p'), false, 'the supervisor scope carries no quota');
    assert.equal(args[args.indexOf('--') + 1], process.execPath);
    assert.ok(args.includes('remote-exec'));
    const record = agent.core.table.get(execId);
    assert.equal(record.pid, calls[0].pid, 'the recorded pid is the spawned (exec-in-place) process');
    assert.ok(record.pidStart, 'pidStart was read for it');
    assert.equal(rw.logs.some((line) => /no scope of their own/.test(line)), false);
    assert.equal(agent.driver.scopeAvailable, true);
    await happens('the lane process group is alive', () => leaseOf(rw)?.childPgid && groupAlive(leaseOf(rw).childPgid));
  },

  'a failing scope probe off Linux starts remote-exec bare and detached, re-probing without repeating its log line': async (w, rw, load) => {
    const calls = [];
    let probes = 0;
    const probe = async () => { probes += 1; return { available: false, reason: 'systemd-run-missing' }; };
    const agent = await rw.agent({ load, driverOptions: scopeOptions(calls, probe, { scopeRequired: false, scopeRetryMs: 50 }) });
    for (const n of [1, 2]) {
      const row = await rw.claimed(await rw.submit({ code: 'process.exit(0)' }));
      await rw.untilGone(agent, rw.execId(row));
      assert.equal(calls.length, n);
    }
    await happens('the probe ran again', () => probes >= 3);
    assert.deepEqual(calls.map((c) => c.cmd), [process.execPath, process.execPath]);
    assert.equal(calls[0].args.includes('--scope'), false);
    assert.equal(rw.logs.filter((line) => /supervisor scope: unavailable \(systemd-run-missing\).*no scope of their own/.test(line)).length, 1);
  },

  'on Linux a failing scope probe pauses claims, logging once per change, and they resume when a re-probe passes': async (w, rw, load) => {
    const calls = [];
    const dir = makeTmpDir('bosun-scope-');
    const results = [{ available: false, reason: 'systemd-run-exit-1: no bus' }, { available: false, reason: 'systemd-run-exit-1: no bus' }];
    let probes = 0;
    const probe = async () => { probes += 1; return results[probes - 1] ?? { available: true, reason: 'ok' }; };
    const ticket = await rw.submit({ code: 'process.exit(0)' });
    const agent = await rw.agent({ load, textfileDir: dir, driverOptions: scopeOptions(calls, probe, { scopeRequired: true, scopeRetryMs: 300 }) });
    await happens('the first probes failed', () => probes >= 2);
    assert.equal(agent.names().includes('ld_claim'), false, 'no claim while there is no scope');
    assert.equal(series(readProm(dir), 'lane_agent_scope_available'), '0');
    assert.equal(readAgentStatus(rw.root).claims, 'paused:no-scope', 'status says why claims are paused');
    assert.deepEqual((await w.ticket(ticket.id)).epoch, 0);
    const row = await rw.claimed(ticket);
    await rw.untilGone(agent, rw.execId(row));
    assert.ok(probes >= 3);
    assert.equal(calls[0].cmd, 'systemd-run', 'the resumed claim started its supervisor in a scope');
    await happens('status goes back to running', () => readAgentStatus(rw.root).claims === 'running');
    await happens('the metric follows', () => series(readProm(dir), 'lane_agent_scope_available') === '1');
    assert.equal(rw.logs.filter((line) => /supervisor scope: unavailable/.test(line)).length, 1, 'one line for the failure, however many probes');
    assert.equal(rw.logs.filter((line) => /supervisor scope: available/.test(line)).length, 1);
  },

  'XDG_RUNTIME_DIR missing at start is filled in for the agent and its supervisors, whether or not the directory exists yet': async (w, rw, load) => {
    const saved = process.env.XDG_RUNTIME_DIR;
    const savedSpawn = rw.ctx.env.XDG_RUNTIME_DIR;
    delete process.env.XDG_RUNTIME_DIR;
    delete rw.ctx.env.XDG_RUNTIME_DIR;
    try {
      const calls = [];
      const agent = await rw.agent({ load, platform: 'linux', driverOptions: scopeOptions(calls, stubProbe({ available: false, reason: 'tests' })) });
      await rw.claimed(await rw.submit({ code: 'process.exit(0)' }));
      await happens('the supervisor was spawned', () => calls.length === 1);
      const want = `/run/user/${process.getuid()}`;
      assert.equal(calls[0].env.XDG_RUNTIME_DIR, want);
      assert.equal(process.env.XDG_RUNTIME_DIR, want);
      assert.ok(agent.core);
    } finally {
      if (saved === undefined) delete process.env.XDG_RUNTIME_DIR;
      else process.env.XDG_RUNTIME_DIR = saved;
      if (savedSpawn !== undefined) rw.ctx.env.XDG_RUNTIME_DIR = savedSpawn;
    }
  },

  'with BOSUN_TEXTFILE_DIR set each tick writes lane-agent.prom: ticking, every subphase, claim counters, listen and registered up, version': async (w, rw, load) => {
    const dir = makeTmpDir('bosun-textfile-');
    const saved = process.env.BOSUN_TEXTFILE_DIR;
    process.env.BOSUN_TEXTFILE_DIR = dir;
    try {
      const agent = await rw.agent({ load });
      await happens('a tick with an empty queue was written', () => Number(series(readProm(dir), 'lane_agent_claims_total{answer="none"}')) >= 1);
      const idle = readProm(dir);
      assert.equal(series(idle, 'lane_agent_listen_up'), '1');
      assert.equal(series(idle, 'lane_agent_registered'), '1');
      assert.equal(series(idle, 'lane_agent_info{version="test"}'), '1');
      assert.equal(series(idle, 'lane_agent_scope_available'), '0');
      assert.equal(series(idle, 'lane_agent_executions{subphase="RUNNING"}'), '0');
      assert.ok(Math.abs(Number(series(idle, 'lane_agent_last_tick_timestamp_seconds')) * 1000 - Date.now()) < 10_000);
      const ticket = await rw.submit({ code: SLEEP });
      await rw.claimed(ticket);
      await happens('the running execution and the claim are counted', () => series(readProm(dir), 'lane_agent_executions{subphase="RUNNING"}') === '1' && series(readProm(dir), 'lane_agent_claims_total{answer="claimed"}') === '1');
      assert.deepEqual(fs.readdirSync(dir), [METRICS_FILE], 'only the finished file is ever left');
      assert.equal(agent.core.table.all().length, 1);
    } finally {
      if (saved === undefined) delete process.env.BOSUN_TEXTFILE_DIR;
      else process.env.BOSUN_TEXTFILE_DIR = saved;
    }
  },

  'with no textfile directory the agent writes no file and logs no metrics error': async (w, rw, load) => {
    const saved = process.env.BOSUN_TEXTFILE_DIR;
    delete process.env.BOSUN_TEXTFILE_DIR;
    const before = fs.readdirSync(process.cwd());
    try {
      const agent = await rw.agent({ load, textfileDir: undefined });
      await happens('ticks ran', () => agent.names().filter((n) => n === 'ld_claim').length >= 2);
      assert.deepEqual(rw.logs.filter((line) => line.startsWith('metrics:')), []);
      assert.deepEqual(fs.readdirSync(process.cwd()), before, 'nothing is written relative to the working directory either');
    } finally {
      if (saved !== undefined) process.env.BOSUN_TEXTFILE_DIR = saved;
    }
  },

  'once stop begins no ld_claim is sent, even by a claim already past its gate': async (w, rw, load) => {
    let release;
    const gate = new Promise((resolve) => { release = resolve; });
    let entered = false;
    const figures = async () => { entered = true; await gate; return { freeCpu: 8, freeMem: 4e9 }; };
    const { ExecutionDriver } = await load('agent/execution.js');
    class SlowClose extends ExecutionDriver {
      async close(options) { await sleep(300); return super.close(options); }
    }
    const agent = await rw.agent({ load, figures, Driver: SlowClose });
    await happens('a claim is waiting on its figures', () => entered);
    const ticket = await rw.submit({ code: 'process.exit(0)' });
    const stopping = agent.stop();
    release();
    await stopping;
    assert.equal(agent.names().includes('ld_claim'), false);
    const row = await w.ticket(ticket.id);
    assert.deepEqual([row.state, row.epoch], ['queued', 0]);
  },

  'a stop waits for an in-flight ld_finish to settle, leaves the running lane running, and returns': async (w, rw, load) => {
    const gate = gatedFinish();
    const running = await rw.submit({ code: SLEEP });
    const agent = await rw.agent({ load, coreOptions: { connect: gate.connect }, config: { pollMs: 5000 } });
    const runningExec = rw.execId(await rw.claimed(running));
    await happens('the sleeping lane is running', () => agent.core.table.get(runningExec)?.subphase === 'RUNNING' && leaseOf(rw)?.childPgid);
    const lane = leaseOf(rw);
    const finishing = await rw.submit({ code: 'process.exit(0)', headerOverrides: { repoKey: 'other-repo' } });
    await rw.claimed(finishing);
    await happens('the short lane is waiting on its ld_finish', () => gate.sent);
    let stopped = false;
    const stopping = agent.stop().then(() => { stopped = true; });
    await sleep(400);
    assert.equal(stopped, false, 'stop is still waiting for the in-flight finish');
    gate.release();
    await stopping;
    assert.equal((await w.ticket(finishing.id)).state, 'finished', 'the in-flight finish was acknowledged');
    assert.equal(groupAlive(lane.childPgid), true, 'the running lane outlived the stop');
    assert.equal((await w.ticket(running.id)).state, 'running');
    assert.equal(fs.existsSync(path.join(agent.core.paths.executions, `${runningExec}.json`)), true, 'its record is still on disk for the next agent');
  },

  'a finish that never answers holds stop for about settleMs only, and its record stays in OUTBOX for the next agent': async (w, rw, load) => {
    const gate = gatedFinish();
    const ticket = await rw.submit({ code: 'process.exit(0)' });
    const agent = await rw.agent({ load, coreOptions: { connect: gate.connect }, stopSettleMs: 600 });
    const execId = rw.execId(await rw.claimed(ticket));
    await happens('the finish is in flight', () => gate.sent);
    const started = Date.now();
    await agent.stop();
    const elapsed = Date.now() - started;
    assert.ok(elapsed >= 500 && elapsed < 6000, `stop took ${elapsed} ms`);
    assert.equal(agent.core.table.get(execId).subphase, 'OUTBOX');
    assert.equal(agent.core.table.get(execId).outbox.kind, 'finish');
    assert.equal((await w.ticket(ticket.id)).state, 'running');
  },

  'a stop while a record is STARTING with its ld_start unsent releases it never-started: the row requeues and nothing spawns': async (w, rw, load) => {
    const gate = gatedFinish();
    let letFetch;
    const fetchHeld = new Promise((resolve) => { letFetch = resolve; });
    const quick = await rw.submit({ code: 'process.exit(0)' });
    const held = await rw.submit({ code: 'process.exit(0)', headerOverrides: { repoKey: 'other-repo' } });
    const holdStream = recordingFetch(async (url) => { if (url.includes(`/lane-snap/streams/${held.id}`)) await fetchHeld; return undefined; });
    const agent = await rw.agent({ load, fetchImpl: holdStream, coreOptions: { connect: gate.connect }, config: { pollMs: 5000 } });
    const heldExec = rw.execId(await rw.claimed(held));
    await happens('the other ticket is finishing, which holds the queue connection', () => gate.sent);
    letFetch();
    await happens('the held record reached STARTING', () => ['STARTING', 'OUTBOX'].includes(agent.core.table.get(heldExec)?.subphase));
    const stopping = agent.stop();
    await sleep(200);
    gate.release();
    await stopping;
    assert.equal(agent.callsTo('ld_start').some(([id]) => id === held.id), false, 'ld_start was never sent for the held ticket');
    assert.equal(fs.existsSync(rw.ticketDir(heldExec)), false, 'nothing was spawned');
    const row = await w.ticket(held.id);
    assert.deepEqual([row.state], ['queued'], 'the claim went back to the queue');
    assert.equal((await w.ticket(quick.id)).state, 'finished');
  },

  'an adopted FETCHING record is released never-started when no scope is available on Linux, never started bare': async (w, rw, load) => {
    const ticket = await rw.submit({ code: 'process.exit(0)' });
    const stuck = recordingFetch(async (url) => { if (url.includes('/lane-snap/streams/')) await new Promise(() => {}); return undefined; });
    const first = await rw.agent({ load, fetchImpl: stuck, stopSettleMs: 200 });
    const execId = rw.execId(await rw.claimed(ticket));
    await happens('the record is FETCHING', () => first.core.table.get(execId)?.subphase === 'FETCHING');
    await first.stop();
    const calls = [];
    const second = await rw.agent({ load, driverOptions: scopeOptions(calls, stubProbe({ available: false, reason: 'systemd-run-exit-1: no bus' }), { scopeRequired: true, scopeRetryMs: 60_000 }) });
    await rw.untilGone(second, execId);
    assert.equal(calls.length, 0, 'nothing was spawned');
    assert.equal(second.callsTo('ld_start').length, 0, 'ld_start was never sent');
    assert.equal(second.names().includes('ld_claim'), false, 'and no new claim was made');
    assert.equal((await w.ticket(ticket.id)).state, 'queued');
  },

  'a stop while an ld_start is already in flight leaves the record STARTING for the next agent to recover': async (w, rw, load) => {
    const ticket = await rw.submit();
    let hold;
    const holdAtStream = recordingFetch(async (url) => {
      if (url.includes('/lane-snap/streams/')) hold = await holdLock(w, `SELECT 1 FROM lane_dispatch.tickets WHERE id = '${ticket.id}' FOR UPDATE`);
      return undefined;
    });
    const agent = await rw.agent({ load, fetchImpl: holdAtStream, timing: { heartbeatMs: 60_000 }, stopSettleMs: 300 });
    const execId = rw.execId(await rw.claimed(ticket));
    await happens('ld_start is in flight', () => agent.names().includes('ld_start'));
    const [callPid] = await backendPids(w, 'SELECT lane_dispatch%');
    await waitBlocked(w, callPid);
    await agent.stop();
    await hold.release();
    assert.equal(agent.core.table.get(execId).subphase, 'STARTING', 'a start whose outcome is unknown is not released');
  },

  'a textfile directory that is a regular file logs a metrics error each failure and claims carry on': async (w, rw, load) => {
    const notDir = path.join(makeTmpDir('bosun-notdir-'), 'file');
    fs.writeFileSync(notDir, 'x');
    const agent = await rw.agent({ load, textfileDir: notDir });
    await happens('the write failure is logged', () => rw.logs.some((line) => line.startsWith('metrics:')));
    const row = await rw.claimed(await rw.submit({ code: 'process.exit(0)' }));
    await rw.untilGone(agent, rw.execId(row));
  },
};

const RUN = 'agent/run.js';
const EXEC = 'agent/execution.js';
const spawnName = 'a passing scope probe starts remote-exec as systemd-run --scope with a lane-exec unit and no quota, and the record keeps the supervisor pid';
const bareName = 'a failing scope probe off Linux starts remote-exec bare and detached, re-probing without repeating its log line';
const pauseName = 'on Linux a failing scope probe pauses claims, logging once per change, and they resume when a re-probe passes';
const xdgName = 'XDG_RUNTIME_DIR missing at start is filled in for the agent and its supervisors, whether or not the directory exists yet';
const propName = 'with BOSUN_TEXTFILE_DIR set each tick writes lane-agent.prom: ticking, every subphase, claim counters, listen and registered up, version';
const inflightName = 'a stop waits for an in-flight ld_finish to settle, leaves the running lane running, and returns';
const canaries = [
  [spawnName, EXEC, ['supervisorLaunch(execId, argv, this.#scope)', 'supervisorLaunch(execId, argv, { available: false })']],
  [bareName, 'cpu-limit.js', ['if (!scope.available) return', 'if (false) return']],
  [bareName, EXEC, ['const changed = !this.#scopeKnown || result.available !== this.#scope.available;', 'const changed = true;']],
  ['an adopted FETCHING record is released never-started when no scope is available on Linux, never started bare', EXEC, ['this.draining() || this.scopeBlock()', 'this.draining()']],
  ['a stop while an ld_start is already in flight leaves the record STARTING for the next agent to recover', EXEC, ['context.startSent !== true', 'true']],
  [pauseName, RUN, ["blocked ? `paused:${blocked}` : 'running'", "'running'"]],
  [pauseName, EXEC, ["return this.scopeRequired && !this.#scope.available ? 'no-scope' : null;", 'return null;']],
  [pauseName, EXEC, ['this.now() - this.#scopeProbedAt < this.scopeRetryMs', 'this.#scopeKnown']],
  [xdgName, RUN, [', ...userManagerEnv({ ...process.env, ...spawnEnv }, { platform })', '']],
  [propName, RUN, ['        writeMetrics();\n', '']],
  [propName, RUN, ['listening: core.ld.listening', 'listening: false']],
  ['with no textfile directory the agent writes no file and logs no metrics error', RUN, ['if (!textfileDir) return;', 'if (false) return;'], /./],
  ['a textfile directory that is a regular file logs a metrics error each failure and claims carry on', RUN, ['log(`metrics: ${err.message}`);', '']],
  ['once stop begins no ld_claim is sent, even by a claim already past its gate', RUN, ["const block = () => stopped ? 'stopping' : driver.scopeBlock() ?? claimBlock(", 'const block = () => driver.scopeBlock() ?? claimBlock(']],
  [inflightName, EXEC, ['setTimeout(resolve, settleMs)', 'setTimeout(resolve, 0)']],
  ['a finish that never answers holds stop for about settleMs only, and its record stays in OUTBOX for the next agent', EXEC, ['setTimeout(resolve, settleMs)', 'setTimeout(resolve, settleMs + 8000)']],
  ['a stop while a record is STARTING with its ld_start unsent releases it never-started: the row requeues and nothing spawns', EXEC, ["if (record?.subphase === 'STARTING' && context.startSent !== true) this.#releasing(record);", '']],
];

scenarioSuite('agent under bosun on a disposable Postgres', { timeout: 600_000, scenarios, canaries });
