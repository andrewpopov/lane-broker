import os from 'node:os';

// BRAIN-860: the suite runs inside a 1-core runner lane; capacity must be the machine's, as on a bare host.
process.env.LANE_BROKER_TEST_HOST_CORES ??= String(os.cpus().length);
