const SUPERVISOR_SIGNATURE = '  async function fallbackOrRefuse(runner, reason, { rebindable = false, retryWhenBlocked = false, neverStarted = false, noRunner = false, retryable = noRunner, failureKind } = {}) {\n';
const RECORDER = `
function __goldenRecord(entry) {
  fs.appendFileSync(process.env.LANE_TEST_GOLDEN_LOG, JSON.stringify(entry) + '\\n');
}
`;

function replaceOnce(source, from, to, file) {
  if (source.split(from).length !== 2) throw new Error(`golden recorder: ${file} lacks exactly one of: ${from.trim()}`);
  return source.replace(from, () => to);
}

/**
 * Records, as JSON lines in `LANE_TEST_GOLDEN_LOG`, every `updateAttempt` patch and every `fallbackOrRefuse` call of a real supervisor,
 * by wrapping those two functions as the module loads. The same wrapping applies to any checkout, so a golden recorded on the code before
 * a change can be compared with the code after it.
 */
export async function load(url, context, nextLoad) {
  const loaded = await nextLoad(url, context);
  if (url.endsWith('/src/attempts.js')) {
    const wrapped = replaceOnce(String(loaded.source), 'export async function updateAttempt(', 'async function __updateAttempt(', 'attempts.js');
    const source = `${wrapped}${RECORDER}
export async function updateAttempt(root, id, generation, patch, ...rest) {
  __goldenRecord({ patch: typeof patch === 'function' ? '<function>' : patch });
  return __updateAttempt(root, id, generation, patch, ...rest);
}
`;
    return { ...loaded, source };
  }
  if (url.endsWith('/src/supervisor.js')) {
    const record = `${SUPERVISOR_SIGNATURE}    __goldenRecord({ fallbackOrRefuse: { runner: runner?.name ?? null, reason, rebindable, retryWhenBlocked, neverStarted, noRunner, retryable, failureKind } });\n`;
    return { ...loaded, source: `${replaceOnce(String(loaded.source), SUPERVISOR_SIGNATURE, record, 'supervisor.js')}${RECORDER}` };
  }
  return loaded;
}
