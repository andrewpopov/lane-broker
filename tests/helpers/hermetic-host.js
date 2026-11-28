import os from 'node:os';
import { INHERITED_BROKER_VARS } from '../helpers.js';

// BRAIN-860: the suite runs inside a 1-core runner lane; capacity must be the machine's, as on a bare host.
process.env.LANE_BROKER_TEST_HOST_CORES ??= String(os.cpus().length);

// PKG-404: the memory gate reads this host's paging; the suite's own load must never close it mid-run.
process.env.LANE_BROKER_TEST_MEMORY_PRESSURE ??= ',,';

// A runner lane exports its own LOCAL/EXECUTOR/RUNNER vars; in-process runCommand tests must not inherit them.
for (const key of INHERITED_BROKER_VARS) delete process.env[key];

for (const key of Object.keys(process.env)) if (key.startsWith('BOSUN_')) delete process.env[key];
