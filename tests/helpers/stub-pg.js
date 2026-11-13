import { EventEmitter } from 'node:events';

/**
 * A pg stand-in for LdClient's `connect`: every lane_dispatch call is recorded in `sqlLog` and answered by `answers[name](args)`
 * (default: empty reassert, a host-a1 register, null otherwise). `failConnects` is a queue of errors the next connects throw.
 */
export function makeStubPg({ answers = {} } = {}) {
  const stub = { clients: [], sqlLog: [], answers, failConnects: [], onListen: null, ended: 0 };
  const defaults = { ld_reassert: () => [], ld_register: () => ({ host: 'a1', max_claims: 2 }) };
  stub.connect = async () => {
    const failure = stub.failConnects.shift();
    if (failure) throw failure;
    const client = Object.assign(new EventEmitter(), { listens: [], ended: false, queries: 0 });
    client.escapeIdentifier = (name) => `"${name}"`;
    client.end = async () => { client.ended = true; stub.ended += 1; };
    client.query = async (sql, args) => {
      client.queries += 1;
      if (sql.startsWith('LISTEN')) {
        client.listens.push(sql);
        await stub.onListen?.(client, client.listens.length);
        return { rows: [] };
      }
      const name = /lane_dispatch\.(\w+)/.exec(sql)[1];
      stub.sqlLog.push({ name, args });
      return { rows: [{ v: await (answers[name] ?? defaults[name] ?? (() => null))(args) }] };
    };
    stub.clients.push(client);
    return client;
  };
  stub.names = () => stub.sqlLog.map((c) => c.name);
  stub.listenClient = () => stub.clients.filter((c) => c.listens.length && !c.ended).at(-1);
  return stub;
}

export const sqlError = (code, message = `sql error ${code}`) => Object.assign(new Error(message), { code });
