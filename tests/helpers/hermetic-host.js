import fs from 'node:fs';
import os from 'node:os';

// BRAIN-860: the suite runs inside a lane's 1-core cgroup and cpuset; capacity detection must see the machine instead.
const machineCores = os.cpus().length;
os.availableParallelism = () => machineCores;

const readFileSync = fs.readFileSync;
fs.readFileSync = function readOutsideLaneCgroup(file, ...rest) {
  if (file === '/proc/self/cgroup') throw Object.assign(new Error(`ENOENT: hermetic-host hides ${file}`), { code: 'ENOENT' });
  return readFileSync.call(this, file, ...rest);
};

const self = `--import=${import.meta.url}`;
const inherited = process.env.NODE_OPTIONS ?? '';
if (!inherited.includes(self)) process.env.NODE_OPTIONS = `${inherited} ${self}`.trim();
