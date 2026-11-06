import os from 'node:os';

// BRAIN-860: a lane's cpuset shrinks availableParallelism to its 1-core grant; the CPU sampler counts os.cpus().
const machineCores = os.cpus().length;
os.availableParallelism = () => machineCores;

const self = `--import=${import.meta.url}`;
const inherited = process.env.NODE_OPTIONS ?? '';
if (!inherited.includes(self)) process.env.NODE_OPTIONS = `${inherited} ${self}`.trim();
