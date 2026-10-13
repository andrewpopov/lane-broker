#!/usr/bin/env node
import '../src/node-exit-safe-boot.js';
import { storeCli } from '../src/store/cli.js';

storeCli(process.argv.slice(2)).then(
  (code) => {
    if (code !== 0) process.exit(code);
  },
  (err) => {
    console.error(err.message);
    process.exit(1);
  },
);
