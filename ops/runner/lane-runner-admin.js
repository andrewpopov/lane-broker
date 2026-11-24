#!/usr/bin/env node
import { runCli, spawnExec } from './runner-admin-lib.js';

process.exitCode = await runCli(process.argv.slice(2), { exec: spawnExec });
