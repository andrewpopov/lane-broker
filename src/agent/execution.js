import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { probeLifetimeScope, supervisorLaunch } from '../cpu-limit.js';
import { kernelBootId, processStartTime, readJsonSafe } from '../state.js';
import { isProcessAlive } from '../process-liveness.js';
import { childExitSafeFlags } from '../node-exit-safe.js';
import { buildResult, cancelRemoteExecution, laneBinPath, readRemoteResultRecord, remoteArtifactsReleaseCommand, tombstoneAbsentTicket } from '../remote-runner.js';
import { LdClosedError, backoffMs } from './ld-client.js';
import { artifactsSpoolPath, clearExecutionFiles, logPath, spoolPath } from './store.js';
import { publishBlob } from '../remote-blobs.js';
import { BlobLease, VerdictError, bindingProblem, downloadVerified, hashFile, prefetchBlobs, readSnapshotHeader } from './fetch.js';
import { artifactsDirOf, uploadOutputs } from './upload.js';
import { stopEvidence } from './stop-proof.js';
import { isDraining } from './claims.js';
import { Waker } from './waker.js';

const PRE_START = new Set(['FETCHING', 'STARTING']);
const BEATING = new Set(['FETCHING', 'SPAWNING', 'RUNNING', 'SETTLING']);
const POST_START = ['SPAWNING', 'RUNNING', 'SETTLING'];

export const DEFAULT_TIMING = {
  tickMs: 1000, heartbeatMs: 15_000, killRetryMs: 5000, killGiveUpMs: 24 * 3_600_000, errorRetryMs: 15_000,
  uploadRetryMs: 10 * 60_000, blobLeaseRenewMs: 5 * 60_000, backoff: { minMs: 250, maxMs: 15_000 },
};

class FetchBudgetError extends Error {}

/** The header `buildResult` reads, rebuilt from what the queue bound this execution to (the stream is not trusted when it is the thing refused). */
const headerOf = ({ id, claim: { binding, header } }) => ({ ...header, ticketId: id, generation: binding.generation, protocol: binding.protocol, manifest: { manifestHash: binding.manifestHash } });

/** After the queue accepts this finish, may the local execution still be running? Then the record waits in KILLING for proof. */
const mayStillRun = (result) => result.reason === 'publisher-gone' || (result.kind === 'cancelled' && result.stopProven !== true && result.neverStarted !== true)
  || (result.kind === 'unfinished' && result.neverStarted !== true && !result.shutdownProof);

/**
 * The per-execution state machines of a `lane agent`: one loop per record, advancing it through FETCHING, STARTING, SPAWNING, RUNNING,
 * SETTLING, OUTBOX, KILLING and DONE. Every transition is persisted through the record table before its side effect. Before `ld_start`
 * the exits are a release (the claim goes back to the queue) or a `rejected` finish; after it every exit is a finish.
 */
export class ExecutionDriver {
  #contexts = new Map();
  #leases = new Map();
  #closed = false;
  #stopping = false;
  #scope = { available: false, reason: 'not probed yet' };
  #scopeKnown = false;
  #scopeProbing = false;
  #scopeProbedAt = -Infinity;

  constructor({ core, remoteRoot, config, s3, timing = {}, waker = new Waker(), log = () => {}, spawnEnv = process.env, laneBin = laneBinPath, draining = isDraining,
    cancelExecution = cancelRemoteExecution, readResult = readRemoteResultRecord, now = Date.now, publish = publishBlob, makeLease = (root, hashes) => new BlobLease(root, hashes),
    probeScope = probeLifetimeScope, spawnProcess = spawn, scopeRequired = process.platform === 'linux', scopeRetryMs = 60_000, onScopeChange = () => {} }) {
    Object.assign(this, { core, remoteRoot: path.resolve(remoteRoot), config, s3, waker, log, spawnEnv, laneBin, draining, cancelExecution, readResult, now, publish, makeLease, probeScope, spawnProcess, scopeRequired, scopeRetryMs, onScopeChange });
    this.timing = { ...DEFAULT_TIMING, ...timing };
    this.ld = core.ld;
    this.table = core.table;
  }

  adopt(execId) {
    if (this.#stopping || this.#contexts.has(execId) || !this.table.get(execId)) return;
    const context = { abort: null, beatNow: false, lastBeatAt: this.now(), mayStart: false };
    this.#contexts.set(execId, context);
    context.promise = this.#drive(execId, context).finally(() => this.#contexts.delete(execId));
  }

  adoptAll() {
    for (const record of this.table.all()) this.adopt(record.execId);
  }

  poke(channel) {
    if (channel?.startsWith('ld_host_')) for (const context of this.#contexts.values()) context.beatNow = true;
    this.waker.poke();
  }

  settled() {
    return Promise.allSettled([...this.#contexts.values()].map((c) => c.promise));
  }

  get scopeAvailable() {
    return this.#scope.available;
  }

  /** Why this runner may not claim for its supervisors' sake, or null: on Linux a supervisor outside its own user scope dies with the agent's cgroup, so no scope means no claims. */
  scopeBlock() {
    return this.scopeRequired && !this.#scope.available ? 'no-scope' : null;
  }

  /** Starts a scope probe in the background when none has succeeded and the last try is older than `scopeRetryMs`; only a success is kept. Cheap to call every tick. */
  watchScope() {
    if (this.#scope.available || this.#scopeProbing || this.#stopping || this.now() - this.#scopeProbedAt < this.scopeRetryMs) return;
    this.#scopeProbing = true;
    this.#scopeProbedAt = this.now();
    this.probeScope()
      .catch((err) => ({ available: false, reason: `probe-error: ${err.message}` }))
      .then((result) => {
        const changed = !this.#scopeKnown || result.available !== this.#scope.available;
        this.#scope = result;
        this.#scopeKnown = true;
        this.#scopeProbing = false;
        if (!changed) return;
        if (result.available) this.log('supervisor scope: available; remote-exec supervisors start in their own user scope');
        else this.log(`supervisor scope: unavailable (${result.reason}); ${this.scopeRequired ? `claims are paused, re-probing every ${Math.round(this.scopeRetryMs / 1000)} s` : 'remote-exec supervisors start detached with no scope of their own'}`);
        this.onScopeChange();
      });
  }

  /**
   * Stop driving, leaving running lanes alone: in-flight fetches and uploads are aborted, and a record already in OUTBOX keeps retrying its
   * `ld_finish` or `ld_release` until it is acknowledged or `settleMs` passes. Whatever is left is picked up from disk by the next agent.
   * A STARTING record whose `ld_start` was not yet sent is released as never started, so a start that never landed requeues cleanly.
   * Only those finishes and releases are waited for; any other call in flight, and any left at the deadline, fails when the caller closes the queue.
   */
  async close({ settleMs = 0 } = {}) {
    this.#stopping = true;
    for (const [execId, context] of this.#contexts) {
      const record = this.table.get(execId);
      if (record?.subphase === 'STARTING' && context.startSent !== true) this.#releasing(record);
    }
    const finishing = [...this.#contexts].filter(([execId]) => this.table.get(execId)?.subphase === 'OUTBOX').map(([, context]) => context.promise);
    for (const context of this.#contexts.values()) context.abort?.abort();
    this.waker.poke();
    let timer;
    await Promise.race([Promise.allSettled(finishing), new Promise((resolve) => { timer = setTimeout(resolve, settleMs); })]);
    clearTimeout(timer);
    this.#closed = true;
    this.waker.poke();
    await Promise.all([...this.#leases.values()].map((lease) => lease.release()));
  }

  async #drive(execId, context) {
    while (!this.#closed) {
      const record = this.table.get(execId);
      if (!record || (this.#stopping && record.subphase !== 'OUTBOX')) return;
      try {
        if (await this.#step(record, context) === 'end') return;
      } catch (err) {
        if (err instanceof LdClosedError || this.#closed || (this.#stopping && record.subphase !== 'OUTBOX')) return;
        this.log(`${execId}: ${record.subphase}: ${err.message}`);
        await this.waker.wait(this.timing.errorRetryMs);
      }
    }
  }

  #step(record, context) {
    switch (record.subphase) {
      case 'FETCHING': return this.#fetching(record, context);
      case 'STARTING': return this.#starting(record, context);
      case 'SPAWNING': return this.#spawning(record);
      case 'RUNNING': return this.#running(record, context);
      case 'SETTLING': return this.#settling(record, context);
      case 'OUTBOX': return this.#outbox(record);
      case 'KILLING': return this.#killing(record);
      default: return this.#done(record);
    }
  }

  /** Compare-and-set one record, synchronously. */
  #cas(execId, from, next) {
    const record = this.table.get(execId);
    if (!record || !from.includes(record.subphase)) return null;
    const updated = { ...record, ...next };
    this.table.put(updated);
    return updated;
  }

  #releasing(record, proof = 'never-started') {
    return this.#cas(record.execId, [...PRE_START], { subphase: 'OUTBOX', outbox: { kind: 'release', proof } });
  }

  #finishing(record, from, result) {
    return this.#cas(record.execId, from, { subphase: 'OUTBOX', outbox: { kind: 'finish', result, postAck: mayStillRun(result) ? 'KILLING' : 'DONE' } });
  }

  #rejected(record, from, reason) {
    return this.#finishing(record, from, buildResult(headerOf(record), this.#ticketDir(record.execId), { kind: 'rejected', reason, neverStarted: true }));
  }

  #ticketDir(execId) {
    return path.join(this.remoteRoot, 'tickets', execId);
  }

  // ---- heartbeats ----

  async #beat(execId, context) {
    const record = this.table.get(execId);
    if (!record) return null;
    context.beatNow = false;
    context.lastBeatAt = this.now();
    return this.ld.heartbeat(record.id, record.epoch, record.token, {
      live: () => BEATING.has(this.table.get(execId)?.subphase),
      persist: (answer) => this.#applyBeat(execId, context, answer),
    });
  }

  #applyBeat(execId, context, answer) {
    const record = this.table.get(execId);
    if (!record) return;
    if (answer.fenced === true) {
      context.abort?.abort();
      this.#cas(execId, [record.subphase], { subphase: PRE_START.has(record.subphase) ? 'DONE' : 'KILLING' });
    } else if (answer.cancel === true) {
      if (record.subphase === 'FETCHING') {
        context.abort?.abort();
        this.#releasing(record, 'cancel-confirmed');
      } else if (!record.cancel?.requested) {
        this.#cas(execId, [record.subphase], { cancel: { requested: true } });
      }
    }
  }

  /** Heartbeats in the background while a long step (a download, an upload) runs, until `signal` aborts. */
  #keepBeating(execId, context, signal) {
    const loop = (async () => {
      while (!signal.aborted && !this.#closed) {
        await this.waker.wait(this.timing.tickMs, signal);
        if (signal.aborted || this.#closed) break;
        if (context.beatNow || this.now() - context.lastBeatAt >= this.timing.heartbeatMs) await this.#beat(execId, context).catch((err) => this.log(`${execId}: heartbeat: ${err.message}`));
      }
    })();
    return async () => { context.abort?.abort(); await loop; };
  }

  // ---- pre-start ----

  #leaseFor(record) {
    if (!this.#leases.has(record.execId)) this.#leases.set(record.execId, this.makeLease(this.remoteRoot, (record.claim.snapshot.blobs ?? []).map((b) => b.sha256)));
    return this.#leases.get(record.execId);
  }

  async #fetching(record, context) {
    const { execId } = record;
    const abort = new AbortController();
    context.abort = abort;
    const stopBeating = this.#keepBeating(execId, context, abort.signal);
    try {
      await this.#fetchWithinBudget(record, abort.signal);
      context.mayStart = true;
      this.#cas(execId, ['FETCHING'], { subphase: 'STARTING' });
    } catch (err) {
      if (abort.signal.aborted) return;
      if (err instanceof VerdictError) this.#rejected(record, ['FETCHING'], err.message);
      else if (err instanceof FetchBudgetError) this.#releasing(record);
      else throw err;
    } finally {
      await stopBeating();
      context.abort = null;
    }
  }

  /**
   * The stream, its binding and its blobs, inside one absolute deadline (`claimedAt + fetchBudgetMs`) that every GET and probe is capped to.
   * A store that is unreachable and a runner that cannot keep a blob or lease are retried with backoff until it passes; only a store that
   * answers wrongly is a verdict.
   */
  async #fetchWithinBudget(record, signal) {
    const { snapshot } = record.claim;
    const { rustfs } = this.config;
    const deadlineAt = record.claimedAt + this.config.fetchBudgetMs;
    const remainingMs = () => deadlineAt - this.now();
    let header = null;
    for (let attempt = 0; ; attempt++) {
      try {
        if (header === null) {
          clearExecutionFiles(this.remoteRoot, record.execId, ['spool']);
          await downloadVerified(this.s3, rustfs.snapBucket, snapshot, spoolPath(this.remoteRoot, record.execId), { signal, remainingMs });
          header = await readSnapshotHeader(spoolPath(this.remoteRoot, record.execId), { compressedBytes: snapshot.bytes });
          const problem = bindingProblem(header, record);
          if (problem) throw new VerdictError(problem);
        }
        await this.#leaseFor(record).renew();
        return await prefetchBlobs({ s3: this.s3, bucket: rustfs.snapBucket, root: this.remoteRoot, record, header, signal, remainingMs, publish: this.publish });
      } catch (err) {
        if (err instanceof VerdictError || signal.aborted) throw err;
        if (remainingMs() <= 0) throw new FetchBudgetError(err.message);
        this.log(`${record.execId}: fetch: ${err.message}`);
        await this.waker.wait(Math.min(backoffMs(attempt, this.timing.backoff), remainingMs()), signal);
      }
    }
  }

  /**
   * A STARTING record this process just made re-hashes its spool, checks the drain marker, and calls `ld_start`. One found on restart
   * is "start attempted, outcome unknown": `ld_start` runs again, and a `true` means the run committed with nothing spawned.
   */
  async #starting(record, context) {
    const { execId } = record;
    if (context.mayStart) {
      const { sha256, bytes } = record.claim.snapshot;
      const intact = await hashFile(spoolPath(this.remoteRoot, execId)).then((got) => got.sha256 === sha256 && got.bytes === bytes, () => false);
      if (!intact) {
        context.mayStart = false;
        return this.#cas(execId, ['STARTING'], { subphase: 'FETCHING' });
      }
      if (this.draining()) return this.#releasing(record);
    }
    await this.ld.start(record.id, record.epoch, record.token, {
      live: () => {
        const live = this.table.get(execId)?.subphase === 'STARTING';
        context.startSent ||= live;
        return live;
      },
      persist: (started) => this.#applyStart(execId, context, started),
    });
  }

  #applyStart(execId, context, started) {
    const record = this.table.get(execId);
    if (record?.subphase !== 'STARTING') return;
    if (started !== true) this.#releasing(record);
    else if (!context.mayStart) this.#rejected(record, ['STARTING'], 'start-uncertain');
    else this.#spawn(this.#cas(execId, ['STARTING'], { subphase: 'SPAWNING', kernelBootId: kernelBootId() }));
  }

  /** `ld_start` answered true: the record says SPAWNING, then the detached `remote-exec` starts on the spooled stream, then RUNNING names it. */
  #spawn(record) {
    const { execId } = record;
    const log = fs.openSync(logPath(this.remoteRoot, execId), 'a');
    const input = fs.openSync(spoolPath(this.remoteRoot, execId), 'r');
    try {
      const argv = [process.execPath, ...childExitSafeFlags(), this.laneBin, 'remote-exec', '--root', this.remoteRoot, '--transfer', 'gzip', '--transfer-cache', '--epoch', String(record.epoch),
        '--runner-name', this.ld.registered.host, '--runner-speed-factor', String(this.config.speedFactor)];
      const { cmd, args } = supervisorLaunch(execId, argv, this.#scope);
      const child = this.spawnProcess(cmd, args, { detached: true, stdio: [input, log, log], env: this.spawnEnv });
      child.on('error', (err) => this.log(`${execId}: spawn failed: ${err.message}`));
      child.unref();
      if (child.pid !== undefined) this.#cas(execId, ['SPAWNING'], { subphase: 'RUNNING', pid: child.pid, pidStart: processStartTime(child.pid) ?? null });
    } finally {
      fs.closeSync(log);
      fs.closeSync(input);
    }
  }

  /** SPAWNING survives only a crash between the spawn and its record, or a failed spawn; the broker lock decides which. */
  async #spawning(record) {
    const { execId } = record;
    const dir = this.#ticketDir(execId);
    if (!fs.existsSync(dir) && await tombstoneAbsentTicket(this.remoteRoot, execId)) return this.#rejected(record, ['SPAWNING'], 'never-admitted');
    if (this.#resultExists(execId)) return this.#cas(execId, ['SPAWNING'], { subphase: 'SETTLING' });
    const publisher = readJsonSafe(path.join(dir, 'publisher.json'));
    if (publisher && isProcessAlive(publisher.pid, publisher.start)) return this.#cas(execId, ['SPAWNING'], { subphase: 'RUNNING', pid: publisher.pid, pidStart: publisher.start });
    return this.#publisherGone(record);
  }

  // ---- running ----

  #childAlive(record) {
    return record.pid !== undefined && isProcessAlive(record.pid, record.pidStart ?? undefined);
  }

  #resultExists(execId) {
    return fs.existsSync(path.join(this.#ticketDir(execId), 'result.json'));
  }

  async #running(record, context) {
    const { execId } = record;
    await this.#renewBlobLease(record, context);
    if (context.beatNow || this.now() - context.lastBeatAt >= this.timing.heartbeatMs) await this.#beat(execId, context);
    const current = this.table.get(execId);
    if (current?.subphase !== 'RUNNING') return;
    if (current.cancel?.requested && this.now() - (context.lastCancelAt ?? 0) >= this.timing.killRetryMs) {
      context.lastCancelAt = this.now();
      await this.cancelExecution(execId, { root: this.remoteRoot });
    }
    if (this.#resultExists(execId) || !this.#childAlive(current)) return this.#cas(execId, ['RUNNING'], { subphase: 'SETTLING' });
    await this.waker.wait(this.timing.tickMs);
  }

  async #renewBlobLease(record, context) {
    if ((record.claim.snapshot.blobs ?? []).length === 0) return;
    const acknowledged = fs.existsSync(path.join(this.#ticketDir(record.execId), 'blob-stats.json')) || this.#resultExists(record.execId);
    if (acknowledged) {
      await this.#leases.get(record.execId)?.release();
      this.#leases.delete(record.execId);
    } else if (this.now() - (context.leaseAt ?? 0) >= this.timing.blobLeaseRenewMs) {
      context.leaseAt = this.now();
      await this.#leaseFor(record).renew();
    }
  }

  // ---- settling ----

  async #settling(record, context) {
    const { execId } = record;
    const result = await this.readResult(execId, { root: this.remoteRoot });
    if (result.missing) return this.#childAlive(record) ? this.#cas(execId, ['SETTLING'], { subphase: 'RUNNING' }) : this.#publisherGone(record);
    return this.#upload(record, context, result);
  }

  /** The process that publishes `result.json` is gone without one. The broker-locked fence in `cancelRemoteExecution` proves a never-admitted run; otherwise it may still run. */
  async #publisherGone(record) {
    const cancel = await this.cancelExecution(record.execId, { root: this.remoteRoot });
    if (stopEvidence(cancel, null)) return this.#rejected(record, POST_START, 'never-admitted');
    const result = buildResult(headerOf(record), this.#ticketDir(record.execId), { kind: 'unfinished', reason: 'publisher-gone', remoteLaneId: cancel.remoteLaneId });
    return this.#finishing(record, POST_START, result);
  }

  async #upload(record, context, result) {
    const { execId } = record;
    const abort = new AbortController();
    context.abort = abort;
    const stopBeating = this.#keepBeating(execId, context, abort.signal);
    try {
      const since = record.uploadSince ?? this.now();
      if (record.uploadSince === undefined) this.#cas(execId, ['SETTLING'], { uploadSince: since });
      const remainingMs = () => since + this.timing.uploadRetryMs - this.now();
      let outputs;
      let uploadError;
      for (let attempt = 0; outputs === undefined && uploadError === undefined; attempt++) {
        try {
          outputs = await uploadOutputs({
            s3: this.s3, bucket: this.config.rustfs.outBucket, host: this.ld.registered.host, record, logFile: logPath(this.remoteRoot, execId),
            artifactsDir: artifactsDirOf(this.remoteRoot, execId), artifactsFile: artifactsSpoolPath(this.remoteRoot, execId), signal: abort.signal, remainingMs,
          });
        } catch (err) {
          if (abort.signal.aborted) return;
          if (remainingMs() <= 0) uploadError = err.message;
          else await this.waker.wait(Math.min(backoffMs(attempt, this.timing.backoff), remainingMs()), abort.signal);
        }
      }
      const cancelled = this.table.get(execId)?.cancel?.requested === true || result.kind === 'cancelled';
      const payload = { ...result, ...outputs, ...(uploadError === undefined ? {} : { uploadError }), ...(cancelled ? { cancelConfirmed: result.stopProven === true } : {}) };
      this.#finishing(record, ['SETTLING'], payload);
    } finally {
      await stopBeating();
      context.abort = null;
    }
  }

  // ---- outbox, killing, done ----

  async #outbox(record) {
    const { execId, outbox } = record;
    if (outbox.refused) return this.#startRefused(record);
    const ack = () => this.#cas(execId, ['OUTBOX'], { subphase: outbox.postAck === 'KILLING' ? 'KILLING' : 'DONE' });
    if (outbox.kind === 'finish') await this.ld.finish(record.id, record.epoch, record.token, outbox.result, { persist: ack });
    else await this.ld.release(record.id, record.epoch, record.token, outbox.proof, { persist: (answer) => (answer.refused === 'started' ? this.#cas(execId, ['OUTBOX'], { outbox: { ...outbox, refused: true } }) : ack()) });
    const after = this.table.get(execId);
    if (after?.subphase === 'OUTBOX' && JSON.stringify(after.outbox) === JSON.stringify(outbox)) await this.waker.wait(this.timing.errorRetryMs);
  }

  /**
   * The queue refused a release because the run had already started (an `ld_start` whose answer was lost), and nothing was ever spawned.
   * One heartbeat learns whether a cancel is pending: then the run finishes `cancelled` with its stop proven, otherwise `rejected`.
   */
  async #startRefused(record) {
    const { execId } = record;
    const beat = await this.ld.heartbeat(record.id, record.epoch, record.token);
    if (beat.fenced === true) return this.#cas(execId, ['OUTBOX'], { subphase: 'DONE' });
    const cancelled = record.cancel?.requested === true || beat.cancel === true;
    const base = { reason: 'start-uncertain', neverStarted: true };
    const result = cancelled
      ? { ...buildResult(headerOf(record), this.#ticketDir(execId), { kind: 'cancelled', stopProven: true, ...base }), cancelConfirmed: true }
      : buildResult(headerOf(record), this.#ticketDir(execId), { kind: 'rejected', ...base });
    return this.#finishing(record, ['OUTBOX'], result);
  }

  /** The queue is told nothing from here: this only ends a lane the queue already settled, on proof it stopped, a reboot, or a day. */
  async #killing(record) {
    const { execId } = record;
    if (record.killingSince === undefined) {
      this.#cas(execId, ['KILLING'], { killingSince: this.now() });
      return;
    }
    const bootNow = kernelBootId();
    const rebooted = Boolean(record.kernelBootId && bootNow && record.kernelBootId !== bootNow);
    const cancel = rebooted ? null : await this.cancelExecution(execId, { root: this.remoteRoot });
    const evidence = rebooted ? null : stopEvidence(cancel, await this.readResult(execId, { root: this.remoteRoot }));
    const expired = this.now() - record.killingSince >= this.timing.killGiveUpMs;
    if (!evidence && !rebooted && !expired) {
      await this.waker.wait(this.timing.killRetryMs);
      return;
    }
    if (!evidence) this.log(`${execId}: KILLING ended with no stop evidence (${rebooted ? 'the boot id changed' : 'gave up after 24 h'})`);
    this.#cas(execId, ['KILLING'], { subphase: 'DONE' });
  }

  async #done(record) {
    const { execId } = record;
    await this.#leases.get(execId)?.release();
    this.#leases.delete(execId);
    clearExecutionFiles(this.remoteRoot, execId);
    await remoteArtifactsReleaseCommand(execId, { root: this.remoteRoot });
    this.table.remove(execId);
    return 'end';
  }
}
