import fs from 'node:fs';
import * as real from '../../src/lane-db.js?real';

export * from '../../src/lane-db.js?real';

/**
 * The real `connect` aimed at the harness database (`LANE_TEST_QUEUE_CONN`, a pg config as JSON). A connection counts in
 * `LANE_TEST_QUEUE_CALLS` and any statement naming a function in `LANE_TEST_QUEUE_FAIL` (comma separated) fails, so a test sees
 * what the queue was asked and can make one call unreachable.
 */
export async function connect(config) {
  if (process.env.LANE_TEST_QUEUE_CALLS) fs.appendFileSync(process.env.LANE_TEST_QUEUE_CALLS, 'connect\n');
  const client = await real.connect({ ...JSON.parse(process.env.LANE_TEST_QUEUE_CONN), connectionTimeoutMillis: config.connectionTimeoutMillis, query_timeout: config.query_timeout });
  const failing = (process.env.LANE_TEST_QUEUE_FAIL ?? '').split(',').filter(Boolean);
  const query = client.query.bind(client);
  client.query = (sql, ...rest) => (typeof sql === 'string' && failing.some((name) => sql.includes(name)) ? Promise.reject(new Error(`injected failure of ${failing.join(',')}`)) : query(sql, ...rest));
  return client;
}
