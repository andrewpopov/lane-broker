import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import pg from 'pg';
import { makeTmpDir } from './helpers/tmp.js';
import { SLEEP, happens, scenarioSuite, sleep } from './agent-run-harness.js';
import { METRICS_FILE } from '../src/agent/metrics.js';
import { paths, readJsonSafe } from '../src/state.js';

const groupAlive = (pgid) => { try { process.kill(-pgid, 0); return true; } catch { return false; } };
const leaseOf = (rw) => {
  const dir = paths(rw.ctx.state).leases;
  const name = fs.existsSync(dir) ? fs.readdirSync(dir).find((n) => n.endsWith('.json')) : undefined;
  return name ? readJsonSafe(path.join(dir, name)) : null;
};
const series = (text, name) => text.split('\n').find((line) => line.startsWith(`${name} `))?.split(' ')[1];
const readProm = (dir) => { try { return fs.readFileSync(path.join(dir, METRICS_FILE), 'utf8'); } catch { return ''; } };

/** A Driver that runs the real command, but through a recording spawn and a stubbed scope probe: systemd is not needed to see which argv the agent chose. */
const recordingDriver = (Base, scope, calls) => class extends Base {
  constructor(options) {
    super({
      ...options,
      probeScope: () => scope,
      spawnProcess: (cmd, args, spawnOptions) => {
        const child = cmd === 'systemd-run' ? spawn(args[args.indexOf('--') + 1], args.slice(args.indexOf('--') + 2), spawnOptions) : spawn(cmd, args, spawnOptions);
        calls.push({ cmd, args, pid: child.pid });
        return child;
      },
    });
  }
};

const scenarios = {
  'a passing scope probe starts remote-exec as systemd-run --scope with a lane-exec unit and no quota, and the record keeps the supervisor pid': async (w, rw, load) => {
    const { ExecutionDriver } = await load('agent/execution.js');
    const calls = [];
    const ticket = await rw.submit({ code: SLEEP });
    const agent = await rw.agent({ load, Driver: recordingDriver(ExecutionDriver, { available: true, reason: 'ok' }, calls) });
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
    await happens('the lane process group is alive', () => leaseOf(rw)?.childPgid && groupAlive(leaseOf(rw).childPgid));
  },

  'a failing scope probe starts remote-exec bare and detached, logging why once however many are spawned': async (w, rw, load) => {
    const { ExecutionDriver } = await load('agent/execution.js');
    const calls = [];
    const agent = await rw.agent({ load, Driver: recordingDriver(ExecutionDriver, { available: false, reason: 'systemd-run-missing' }, calls) });
    for (const n of [1, 2]) {
      const row = await rw.claimed(await rw.submit({ code: 'process.exit(0)' }));
      await rw.untilGone(agent, rw.execId(row));
      assert.equal(calls.length, n);
    }
    assert.deepEqual(calls.map((c) => c.cmd), [process.execPath, process.execPath]);
    assert.equal(calls[0].args.includes('--scope'), false);
    assert.equal(rw.logs.filter((line) => /no scope of their own \(systemd-run-missing\)/.test(line)).length, 1);
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
    let release;
    const gate = new Promise((resolve) => { release = resolve; });
    let finishSent = false;
    const connect = async (config) => {
      const client = new pg.Client(config);
      client.on('error', () => {});
      await client.connect();
      const query = client.query.bind(client);
      client.query = async (...args) => {
        if (String(args[0]).includes('ld_finish')) { finishSent = true; await gate; }
        return query(...args);
      };
      return client;
    };
    const running = await rw.submit({ code: SLEEP });
    const agent = await rw.agent({ load, coreOptions: { connect }, config: { pollMs: 5000 } });
    const runningExec = rw.execId(await rw.claimed(running));
    await happens('the sleeping lane is running', () => agent.core.table.get(runningExec)?.subphase === 'RUNNING' && leaseOf(rw)?.childPgid);
    const lane = leaseOf(rw);
    const finishing = await rw.submit({ code: 'process.exit(0)', headerOverrides: { repoKey: 'other-repo' } });
    await rw.claimed(finishing);
    await happens('the short lane is waiting on its ld_finish', () => finishSent);
    let stopped = false;
    const stopping = agent.stop().then(() => { stopped = true; });
    await sleep(400);
    assert.equal(stopped, false, 'stop is still waiting for the in-flight finish');
    release();
    await stopping;
    assert.equal((await w.ticket(finishing.id)).state, 'finished', 'the in-flight finish was acknowledged');
    assert.equal(groupAlive(lane.childPgid), true, 'the running lane outlived the stop');
    assert.equal((await w.ticket(running.id)).state, 'running');
    assert.equal(fs.existsSync(path.join(agent.core.paths.executions, `${runningExec}.json`)), true, 'its record is still on disk for the next agent');
  },
};

const RUN = 'agent/run.js';
const EXEC = 'agent/execution.js';
const spawnName = 'a passing scope probe starts remote-exec as systemd-run --scope with a lane-exec unit and no quota, and the record keeps the supervisor pid';
const bareName = 'a failing scope probe starts remote-exec bare and detached, logging why once however many are spawned';
const propName = 'with BOSUN_TEXTFILE_DIR set each tick writes lane-agent.prom: ticking, every subphase, claim counters, listen and registered up, version';
const canaries = [
  [spawnName, EXEC, ['supervisorLaunch(execId, argv, this.#supervisorScope())', 'supervisorLaunch(execId, argv, { available: false })']],
  [bareName, 'cpu-limit.js', ['if (!scope.available) return', 'if (false) return']],
  [bareName, EXEC, ['if (!this.#scope) {', 'if (true) {']],
  [propName, RUN, ['        writeMetrics();\n', '']],
  [propName, RUN, ['listening: core.ld.listening', 'listening: false']],
  ['with no textfile directory the agent writes no file and logs no metrics error', RUN, ['if (!textfileDir) return;', 'if (false) return;'], /./],
  ['once stop begins no ld_claim is sent, even by a claim already past its gate', RUN, ["const block = () => stopped ? 'stopping' : claimBlock(", 'const block = () => claimBlock(']],
  ['a stop waits for an in-flight ld_finish to settle, leaves the running lane running, and returns', EXEC, ['setTimeout(resolve, settleMs)', 'setTimeout(resolve, 0)']],
];

scenarioSuite('agent under bosun on a disposable Postgres', { timeout: 600_000, scenarios, canaries });
