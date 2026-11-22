import { isAbsolute } from 'node:path';

/**
 * A structural check of a bosun module manifest (contract 1, store-loaded) written from bosun's documented schema (agent-fleet scripts/bosun/README.md,
 * "Module manifest" and "Package store"). It is not bosun's parser and shares no code with it; the parser's own vector over bosun/lane.yaml belongs in agent-fleet.
 */
const TOP = ['schema', 'contract', 'name', 'version', 'kind', 'platforms', 'user', 'privileges', 'required', 'compat', 'params', 'packages', 'requires', 'setup', 'check', 'run', 'job', 'schedule', 'after', 'readiness', 'stopped', 'health', 'logs', 'drain', 'exitCodes', 'resources'];
const SECTIONS = {
  compat: ['bosun', 'capabilities'],
  setup: ['argv', 'timeoutSeconds', 'requiresStop', 'platforms'],
  run: ['argv', 'cwd', 'envFile', 'restart', 'stopSignal', 'stopDeadlineSeconds', 'requiresUserManager', 'runtime', 'platforms'],
  readiness: ['kind', 'graceSeconds', 'url', 'expectStatus', 'file', 'maxAgeSeconds', 'argv', 'timeoutSeconds'],
  drain: ['kind', 'signal', 'ack', 'ackTimeoutSeconds', 'modes', 'interruptSignal', 'argv', 'hookTimeoutSeconds'],
  resources: ['class', 'reserve', 'elastic'],
};
const PARAM_TYPES = ['string', 'path', 'integer', 'argv'];
const FIXED_TEMPLATES = ['home', 'stateDir', 'libDir', 'textfileDir', 'envFile', 'self', 'node'];
const BOSUN_RANGE = /^(>=|\^)\d+\.\d+\.\d+$/;
const insideSelf = (text) => /^\$\{self\}\//.test(text) && !text.split('/').includes('..');
const isMapping = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);

export function validateBosunManifest(manifest, expectedName) {
  const problems = [];
  const refuse = (message) => problems.push(message);
  const section = (path, value) => {
    if (!isMapping(value)) { refuse(`${path} must be a mapping`); return {}; }
    for (const key of Object.keys(value)) if (!(SECTIONS[path] ?? TOP).includes(key)) refuse(`unknown key ${path}.${key}`);
    return value;
  };
  const top = section('manifest', manifest);
  if (top.schema !== 1) refuse('manifest.schema must be 1');
  if (top.contract !== 1) refuse('manifest.contract must be 1');
  if (top.name !== expectedName) refuse(`manifest.name is ${top.name} but the manifest is for ${expectedName}`);
  if (top.kind !== 'service') refuse('manifest.kind must be service for a long-running agent');
  if (!Array.isArray(top.platforms) || top.platforms.length === 0) refuse('manifest.platforms must be a non-empty list');
  const compat = section('compat', top.compat ?? {});
  if (compat.bosun !== undefined && !BOSUN_RANGE.test(compat.bosun)) refuse('manifest.compat.bosun must be >=x.y.z or ^x.y.z');

  const params = isMapping(top.params) ? top.params : {};
  for (const [name, param] of Object.entries(params)) {
    if (!PARAM_TYPES.includes(param?.type)) refuse(`params.${name}.type must be one of ${PARAM_TYPES.join(', ')}`);
    if (typeof param?.overridable !== 'boolean') refuse(`params.${name}.overridable must be true or false`);
    if (typeof param?.default === 'string' && param.default.includes('${')) refuse(`params.${name}.default must not contain \${`);
  }

  const templates = (path, text) => {
    for (const [, token] of text.matchAll(/\$\{([^}]*)\}/g)) {
      const known = FIXED_TEMPLATES.includes(token) || (token.startsWith('param:') && Object.hasOwn(params, token.slice(6)));
      if (!known) refuse(`${path} uses \${${token}}, which is not a template this manifest may use`);
    }
  };
  // A store package's command never resolves through PATH: ${node}, an absolute path, or a path inside ${self}/; node runs a script inside ${self}/.
  const hook = (path, argv) => {
    if (!Array.isArray(argv) || argv.length === 0 || argv.some((arg) => typeof arg !== 'string' || arg === '')) { refuse(`${path} must be a non-empty list of strings`); return; }
    argv.forEach((arg, index) => templates(`${path}[${index}]`, arg));
    const [command, script] = argv;
    if (!(command === '${node}' || isAbsolute(command) || insideSelf(command))) refuse(`${path}[0] is ${command}: a store package's command must be \${node}, an absolute path, or start with \${self}/, so it never resolves through PATH`);
    if (/^\$\{self\}/.test(command) && !insideSelf(command)) refuse(`${path}[0] is ${command}: a path into the package must continue with / and no .. component`);
    if (command === '${node}' && !insideSelf(script ?? '')) refuse(`${path} runs ${script} under \${node}: the script must be a path inside \${self}/`);
  };

  const run = section('run', top.run);
  hook('run.argv', run.argv);
  if (top.setup !== undefined) hook('setup.argv', section('setup', top.setup).argv);
  if (top.drain !== undefined) {
    const drain = section('drain', top.drain);
    if ((drain.kind === 'exec') !== (drain.argv !== undefined)) refuse('drain.argv goes with, and is required by, drain.kind exec');
    if (drain.kind === 'exec') hook('drain.argv', drain.argv);
    if (drain.interruptSignal !== undefined && drain.kind === 'exec') refuse('drain.interruptSignal does not go with drain.kind exec: the hook is told the mode');
  }
  if (top.readiness !== undefined) {
    const readiness = section('readiness', top.readiness);
    if (readiness.kind === 'textfile-fresh') templates('readiness.file', readiness.file ?? '');
    if (readiness.kind === 'exec') hook('readiness.argv', readiness.argv);
  }
  if (top.run?.cwd !== undefined) templates('run.cwd', top.run.cwd);
  for (const capability of compat.capabilities ?? []) if (!/^[a-z][a-z0-9-]{0,63}$/.test(capability)) refuse(`manifest.compat.capabilities has ${capability}`);
  return problems;
}

export const expandSelf = (text, self, node, params = {}) => text.replace(/\$\{([^}]*)\}/g, (_, token) => (token === 'self' ? self : token === 'node' ? node : token.startsWith('param:') ? params[token.slice(6)] : _));
