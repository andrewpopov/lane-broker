import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { PG_LIVENESS } from '../src/agent/config.js';
import { assertCanaryFails, mutateSrcTree } from './helpers/mutant-module.js';

const CONFIG = 'agent/config.js';
const REAL = 'connectionTimeoutMillis: 10_000, query_timeout: 20_000';

const AUTH_OK_READY = Buffer.from([0x52, 0, 0, 0, 8, 0, 0, 0, 0, 0x5a, 0, 0, 0, 5, 0x49]);

// Accepts, finishes the startup handshake, then never answers a query: the shape of a peer that died without a FIN.
async function stallingServer() {
  const server = { sockets: new Set(), queried: 0 };
  const tcp = net.createServer((socket) => {
    server.sockets.add(socket);
    socket.on('error', () => {});
    let started = false;
    socket.on('data', () => {
      if (started) server.queried += 1;
      else { started = true; socket.write(AUTH_OK_READY); }
    });
  });
  await new Promise((resolve) => tcp.listen(0, '127.0.0.1', resolve));
  server.port = tcp.address().port;
  server.close = () => { for (const socket of server.sockets) socket.destroy(); tcp.close(); };
  return server;
}

async function detectsDeadServer(load) {
  const { PG_LIVENESS: liveness } = await load(CONFIG);
  const { LdClient } = await load('agent/ld-client.js');
  const server = await stallingServer();
  const client = new LdClient({ pg: { host: '127.0.0.1', port: server.port, user: 'u', database: 'd', ...liveness }, version: 't', backoff: { minMs: 5, maxMs: 20 } });
  const pending = client.claim('44444444-4444-4444-8444-444444444444', 8, 1e9).catch(() => {});
  try {
    for (let i = 0; i < 300 && server.queried < 2; i++) await new Promise((resolve) => setTimeout(resolve, 20));
    assert.ok(server.queried >= 2, `the stalled query was never given up on and retried (queries seen: ${server.queried})`);
  } finally {
    await client.close();
    server.close();
    await pending;
  }
}

test('the agent pg connection carries the liveness bounds', () => {
  assert.deepEqual(PG_LIVENESS, { keepAlive: true, connectionTimeoutMillis: 10_000, query_timeout: 20_000 });
});

test('a server that stops answering is detected within the timeouts and the call is retried on a new connection', async () => {
  await detectsDeadServer(await mutateSrcTree(CONFIG, [REAL, 'connectionTimeoutMillis: 300, query_timeout: 300']));
});

test('canary: with query_timeout dropped, a stalled server is never detected', async () => {
  const failure = await assertCanaryFails(CONFIG, [REAL, 'connectionTimeoutMillis: 300'], detectsDeadServer);
  assert.match(failure.message, /never given up on/);
});
