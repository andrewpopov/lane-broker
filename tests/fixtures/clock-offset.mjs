// Preloaded with `node --import`: shifts this process's wall clock by LANE_TEST_CLOCK_OFFSET_MS, so a test can run one
// "host" (a fake runner) with a clock that disagrees with the submitter's. Every child inherits it through NODE_OPTIONS.
const offset = Number(process.env.LANE_TEST_CLOCK_OFFSET_MS || 0);
if (offset !== 0) {
  const realNow = Date.now;
  Date.now = () => realNow() + offset;
}
