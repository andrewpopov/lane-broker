import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { call, claim, finish, heartbeat, liftBackoff, logins, release, start } from '../lane-dispatch-fixture.js';
import { sha256Hex } from '../../src/s3-presign.js';

// Learns its host from the queue at registration, as an agent does, and derives its output keys the way a real runner does (`<host>/<ticket>/e<epoch>/<name>` in `lane-out`) and PUTs with its own signed request, so a wrong host, epoch or signature fails at the S3 stub
export class StubRunner {
  constructor(world, { login = logins.a1, s3, tmpDir } = {}) {
    Object.assign(this, { world, login, s3, tmpDir, host: null, claimed: null });
  }

  client() {
    return this.world.as(this.login);
  }

  async claim(options) {
    await liftBackoff(this.world);
    this.host = (await call(await this.client(), 'ld_register', 'test', 1, null)).host;
    const claimed = await claim(await this.client(), options);
    if (claimed.status !== 'claimed') throw new Error(`stub runner claimed nothing: ${JSON.stringify(claimed)}`);
    this.claimed = claimed;
    return claimed;
  }

  async start() {
    return start(await this.client(), this.claimed);
  }

  async heartbeat() {
    return heartbeat(await this.client(), this.claimed);
  }

  outputKey(name) {
    return `${this.host}/${this.claimed.id}/e${this.claimed.epoch}/${name}`;
  }

  async put(name, body) {
    const file = path.join(this.tmpDir, `${name}-${crypto.randomBytes(4).toString('hex')}`);
    fs.writeFileSync(file, body);
    await this.s3.putObjectFromFile('lane-out', this.outputKey(name), file);
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
