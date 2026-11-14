import fs from 'node:fs';
import { register } from 'node:module';

register('./queue-redirect-hooks.js', import.meta.url);

if (process.env.LANE_TEST_EXIT_LOG) {
  process.on('exit', (code) => fs.appendFileSync(process.env.LANE_TEST_EXIT_LOG, `${process.argv[1]} ${code}\n`));
}
