import { call, claim, finish, heartbeat, logins, release, start } from '../lane-dispatch-fixture.js';
import { sha256Hex } from '../../src/s3-presign.js';

// PUTs only through the URLs the supervisor attached, so a wrong key, epoch or signature fails at the S3 stub
export class StubRunner {
  constructor(world, { login = logins.a1, fetchImpl = globalThis.fetch } = {}) {
    Object.assign(this, { world, login, fetchImpl, claimed: null, urls: null, nextSeq: 0 });
  }

  client() {
    return this.world.as(this.login);
  }

  async claim(options) {
    const claimed = await claim(await this.client(), options);
    if (claimed.status !== 'claimed') throw new Error(`stub runner claimed nothing: ${JSON.stringify(claimed)}`);
    this.claimed = claimed;
    this.urls = null;
    this.nextSeq = 0;
    return claimed;
  }

  async start() {
    return start(await this.client(), this.claimed);
  }

  async heartbeat() {
    const answer = await heartbeat(await this.client(), this.claimed);
    this.urls = answer.urls ?? null;
    return answer;
  }

  async log(text, seq = this.nextSeq++) {
    return call(await this.client(), 'ld_log', this.claimed.id, this.claimed.epoch, this.claimed.token, seq, Buffer.from(text));
  }

  async put(name, body) {
    const res = await this.fetchImpl(this.urls[name], { method: 'PUT', body });
    if (res.status !== 200) throw new Error(`stub runner PUT ${name} answered ${res.status}: ${await res.text()}`);
    return { sha256: sha256Hex(body), bytes: body.length };
  }

  async putOutputs({ log, artifacts }) {
    const fields = {};
    if (log !== undefined) {
      const { sha256, bytes } = await this.put('log', Buffer.from(log));
      Object.assign(fields, { logSha256: sha256, logBytes: bytes });
    }
    if (artifacts !== undefined) {
      const { sha256, bytes } = await this.put('artifacts', artifacts);
      Object.assign(fields, { artifactsSha256: sha256, artifactsBytes: bytes });
    }
    return fields;
  }

  result(extra = {}) {
    const { protocol, ticketId, generation, manifestHash } = this.claimed.binding;
    return { protocol, ticketId, generation, manifestHash, kind: 'completed', exit: 0, signal: null, ...(protocol === 2 ? { phase: 'command' } : {}), ...extra };
  }

  async finish(extra = {}) {
    return finish(await this.client(), this.claimed, this.result(extra));
  }

  async finishCancelled(cancelConfirmed) {
    return finish(await this.client(), this.claimed, this.result({ kind: 'cancelled', exit: undefined, cancelConfirmed }));
  }

  async release(proof = 'never-started') {
    return release(await this.client(), this.claimed, proof);
  }
}
