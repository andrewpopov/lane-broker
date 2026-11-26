#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { LOCK_CONFLICT_EXIT, lockedReexec, parseCli, runCli, spawnExec } from './runner-admin-lib.js';

const argv = process.argv.slice(2);
let reexec = null;
try { const opts = parseCli(argv); if (opts.apply) reexec = lockedReexec({ argv, host: opts.host }); } catch { /* runCli reports usage errors */ }
if (reexec) {
  const child = spawnSync(reexec.cmd, reexec.args, { stdio: 'inherit', env: reexec.env });
  if (child.error) { process.stderr.write(`lane-runner-admin: could not run flock (${child.error.message}); install it with brew install flock\n`); process.exitCode = 1; }
  else if (child.status === LOCK_CONFLICT_EXIT) { process.stderr.write(`lane-runner-admin: another --apply run for this host holds the lock; only one admin may run it at a time\n`); process.exitCode = LOCK_CONFLICT_EXIT; }
  else process.exitCode = child.status ?? 1;
} else {
  process.exitCode = await runCli(argv, { exec: spawnExec });
}
