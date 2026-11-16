import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { freshEnv, laneRun } from './helpers.js';
import { paths, readRemoteDrain, readDrainMarkers, writeRemoteDrain, clearRemoteDrain, pruneExpiredDrains } from '../src/state.js';
import { claimBlock, drainOwners, isDraining } from '../src/agent/claims.js';

/** BRAIN-1141: owned, expiring drain markers. The ownerless `lane drain` / `lane undrain` stay the `human` owner. */

const HOUR = 3_600_000;
const ownerFile = (state, owner) => path.join(paths(state).drainOwners, `${owner}.json`);
const writeRawOwner = (state, owner, marker) => {
  fs.mkdirSync(paths(state).drainOwners, { recursive: true });
  fs.writeFileSync(ownerFile(state, owner), JSON.stringify({ version: 1, host: 'h', ...marker }));
};
const owners = (state) => readDrainMarkers(state).map((m) => m.owner);

test('markers of several owners coexist: the host drains while any is unexpired, human listed first', async () => {
  const { state } = freshEnv();
  await writeRemoteDrain(state, 'maintenance window', { owner: 'bosun', expiresAt: Date.now() + HOUR });
  await writeRemoteDrain(state, 'disk swap');
  await writeRemoteDrain(state, 'thermal', { owner: 'a-tool' });
  assert.deepEqual(owners(state), ['human', 'a-tool', 'bosun']);
  assert.equal(readRemoteDrain(state).reason, 'disk swap', 'the legacy single-marker reader answers with the first');
  assert.equal(isDraining(state), true);
  assert.deepEqual(drainOwners(state), ['human', 'a-tool', 'bosun']);
});

test('the ownerless marker keeps the old file and shape, so an older reader still sees it', async () => {
  const { state } = freshEnv();
  await writeRemoteDrain(state, 'disk swap');
  const raw = JSON.parse(fs.readFileSync(paths(state).remoteDrain, 'utf8'));
  assert.deepEqual(Object.keys(raw).sort(), ['host', 'reason', 'since', 'version']);
  assert.equal(fs.existsSync(paths(state).drainOwners), false);
});

test('an expired marker is ignored; the others still drain; once all are expired the host is not draining', async () => {
  const { state } = freshEnv();
  writeRawOwner(state, 'bosun', { reason: 'old', since: 1, expiresAt: Date.now() - 1000 });
  assert.deepEqual(readDrainMarkers(state), []);
  assert.equal(readRemoteDrain(state), null);
  assert.equal(isDraining(state), false);
  await writeRemoteDrain(state, 'keep', { owner: 'other' });
  writeRawOwner(state, 'bosun', { reason: 'old', since: 1, expiresAt: Date.now() - 1000 });
  assert.deepEqual(owners(state), ['other']);
  assert.equal(isDraining(state), true);
});

test('a marker lapses by the clock: unexpired now, ignored at its expiry', async () => {
  const { state } = freshEnv();
  const expiresAt = Date.now() + 5000;
  await writeRemoteDrain(state, 'brief', { owner: 'bosun', expiresAt });
  assert.equal(readDrainMarkers(state, expiresAt - 1).length, 1);
  assert.equal(readDrainMarkers(state, expiresAt).length, 0);
});

test('expired markers are pruned under the lock by pruneExpiredDrains, a write and a clear, and live ones are kept', async () => {
  const { state } = freshEnv();
  const expired = () => writeRawOwner(state, 'bosun', { reason: 'old', since: 1, expiresAt: Date.now() - 1000 });
  await writeRemoteDrain(state, 'live', { owner: 'live', expiresAt: Date.now() + HOUR });
  expired();
  pruneExpiredDrains(state);
  assert.equal(fs.existsSync(ownerFile(state, 'bosun')), false);
  assert.equal(fs.existsSync(ownerFile(state, 'live')), true);
  expired();
  await writeRemoteDrain(state, 'other', { owner: 'other' });
  assert.equal(fs.existsSync(ownerFile(state, 'bosun')), false, 'a write prunes');
  expired();
  await clearRemoteDrain(state, 'other');
  assert.equal(fs.existsSync(ownerFile(state, 'bosun')), false, 'a clear prunes');
});

test('re-draining after an expiry starts a fresh since; re-draining a live marker keeps it and replaces reason and expiry', async () => {
  const { state } = freshEnv();
  writeRawOwner(state, 'bosun', { reason: 'old', since: 1, expiresAt: Date.now() - 1000 });
  const fresh = await writeRemoteDrain(state, 'again', { owner: 'bosun', expiresAt: Date.now() + HOUR });
  assert.ok(fresh.since > 1);
  await new Promise((resolve) => setTimeout(resolve, 15));
  const later = await writeRemoteDrain(state, 'updated', { owner: 'bosun' });
  assert.equal(later.since, fresh.since);
  const [marker] = readDrainMarkers(state);
  assert.deepEqual([marker.reason, marker.expiresAt], ['updated', undefined]);
});

test('an unreadable owner marker, or a malformed expiry, fails closed as draining', () => {
  const { state } = freshEnv();
  fs.mkdirSync(paths(state).drainOwners, { recursive: true });
  fs.writeFileSync(ownerFile(state, 'bosun'), '{nope');
  assert.deepEqual(readDrainMarkers(state).map((m) => [m.owner, m.reason, m.unreadable]), [['bosun', 'drain marker unreadable', true]]);
  writeRawOwner(state, 'bosun', { reason: 'x', since: 1, expiresAt: 'tomorrow' });
  assert.equal(readDrainMarkers(state)[0].unreadable, true);
  assert.equal(isDraining(state), true);
});

test('clearRemoteDrain removes only the named owner and is idempotent', async () => {
  const { state } = freshEnv();
  await writeRemoteDrain(state, 'a', { owner: 'bosun' });
  await writeRemoteDrain(state, 'b');
  assert.equal(await clearRemoteDrain(state, 'bosun'), true);
  assert.equal(await clearRemoteDrain(state, 'bosun'), false);
  assert.deepEqual(owners(state), ['human']);
});

test('lane drain --owner --reason --expires, then lane undrain --owner removes only that owner', async () => {
  const { state, env } = freshEnv();
  const drain = await laneRun(['drain', '--owner', 'bosun', '--reason', 'tuning', '--expires', '30m'], { env });
  assert.equal(drain.code, 0, drain.stderr);
  assert.match(drain.stdout, /drain marker: bosun: tuning \(expires \d{4}-\d\d-\d\dT.*Z\)/);
  const [marker] = readDrainMarkers(state);
  assert.equal(marker.owner, 'bosun');
  assert.ok(Math.abs(marker.expiresAt - (Date.now() + 30 * 60_000)) < 60_000);

  assert.equal((await laneRun(['drain', 'disk swap'], { env })).code, 0);
  assert.deepEqual(owners(state), ['human', 'bosun']);

  const undrain = await laneRun(['undrain', '--owner', 'bosun'], { env });
  assert.equal(undrain.code, 0, undrain.stderr);
  assert.match(undrain.stdout, /still draining: human: disk swap/);
  assert.deepEqual(owners(state), ['human']);
});

test('the ownerless lane undrain removes only the human marker and says another owner still drains', async () => {
  const { state, env } = freshEnv();
  await laneRun(['drain', '--owner', 'bosun', '--reason', 'tuning'], { env });
  await laneRun(['drain', 'disk swap'], { env });
  const undrain = await laneRun(['undrain'], { env });
  assert.equal(undrain.code, 0, undrain.stderr);
  assert.match(undrain.stdout, /still draining: bosun: tuning/);
  assert.deepEqual(owners(state), ['bosun']);
});

test('--expires takes an ISO time too, and applies to the human marker', async () => {
  const { state, env } = freshEnv();
  const at = new Date(Date.now() + 2 * HOUR).toISOString();
  assert.equal((await laneRun(['drain', 'disk swap', '--expires', at], { env })).code, 0);
  assert.equal(readDrainMarkers(state)[0].expiresAt, Date.parse(at));
});

test('bad drain and undrain arguments exit 2 and change nothing', async () => {
  const { state, env } = freshEnv();
  await laneRun(['drain', 'keep'], { env });
  for (const args of [
    ['drain', '--owner'],
    ['drain', '--owner', 'Bad Owner'],
    ['drain', '--owner', 'bosun', '--expires', 'soon'],
    ['drain', '--owner', 'bosun', '--expires', '2020-01-01T00:00:00Z'],
    ['drain', '--owner', 'bosun', '--expires'],
    ['drain', 'a', 'b'],
    ['drain', '--bogus'],
    ['undrain', '--owner', 'Bad Owner'],
    ['undrain', '--expires', '1h'],
    ['undrain', 'extra'],
  ]) assert.equal((await laneRun(args, { env })).code, 2, args.join(' '));
  assert.deepEqual(owners(state), ['human']);
});

test('lane status lists every active marker with owner, reason and expiry, and prunes expired ones; --json carries drainMarkers', async () => {
  const { state, env } = freshEnv();
  await laneRun(['drain', '--owner', 'bosun', '--reason', 'tuning', '--expires', '2h'], { env });
  await laneRun(['drain', 'disk swap'], { env });
  writeRawOwner(state, 'gone', { reason: 'lapsed', since: 1, expiresAt: Date.now() - 1000 });
  const text = (await laneRun(['status'], { env })).stdout;
  assert.match(text, /^ {2}drain marker: human: disk swap \(no expiry\)$/m);
  assert.match(text, /^ {2}drain marker: bosun: tuning \(expires \d{4}-.*Z\)$/m);
  assert.doesNotMatch(text, /lapsed/);
  assert.equal(fs.existsSync(ownerFile(state, 'gone')), false, 'status pruned it');
  const json = JSON.parse((await laneRun(['status', '--json'], { env })).stdout);
  assert.deepEqual(json.drainMarkers.map((m) => m.owner), ['human', 'bosun']);
  assert.equal(json.remoteDrain.reason, 'disk swap');
});

test('the agent pauses claims with every owner in the reason, and resumes when the last marker lapses', async () => {
  const { state } = freshEnv();
  const block = () => claimBlock({ schedule: null, stopClaimingBeforeCloseMs: 0, draining: () => isDraining(state), owners: () => drainOwners(state) });
  assert.equal(block(), null);
  await writeRemoteDrain(state, 'tuning', { owner: 'bosun' });
  assert.equal(block(), 'draining (bosun)');
  await writeRemoteDrain(state, 'disk swap');
  assert.equal(block(), 'draining (human, bosun)');
  await clearRemoteDrain(state, 'human');
  await clearRemoteDrain(state, 'bosun');
  assert.equal(block(), null);
  writeRawOwner(state, 'bosun', { reason: 'old', since: 1, expiresAt: Date.now() - 1000 });
  assert.equal(block(), null, 'an expired marker blocks nothing');
});

test('a claim block with no owners named keeps the plain reason', () => {
  assert.equal(claimBlock({ schedule: null, stopClaimingBeforeCloseMs: 0, draining: () => true }), 'draining');
});
