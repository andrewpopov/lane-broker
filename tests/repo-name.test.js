import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { gitFixture } from './helpers.js';
import { makeTmpDir } from './helpers/tmp.js';
import { assertCanaryFails, realSrc } from './helpers/mutant-module.js';

/** BRAIN-1116 S3: the canonical owner/name a submitter stamps on a pull ticket, and the version stamped beside it. */

const PACKAGE_VERSION = JSON.parse(fs.readFileSync(new URL('../package.json', import.meta.url), 'utf8')).version;

const URL_FORMS = [
  ['an scp-style origin through an ssh host alias', 'git@github-personal:popovlabs/towerpower.git', 'popovlabs/towerpower'],
  ['an https origin', 'https://github.com/popovlabs/towerpower.git', 'popovlabs/towerpower'],
  ['an https origin with a user', 'https://andrewpopov@github.com/andrewpopov/lane-broker.git', 'andrewpopov/lane-broker'],
  ['an ssh:// origin with a port', 'ssh://git@github.com:22/popovlabs/towerpower.git', 'popovlabs/towerpower'],
  ['a git:// origin', 'git://github.com/popovlabs/towerpower.git', 'popovlabs/towerpower'],
  ['an scp-style origin with no user', 'github.com:popovlabs/towerpower', 'popovlabs/towerpower'],
  ['an origin with no .git', 'https://github.com/popovlabs/towerpower', 'popovlabs/towerpower'],
  ['a trailing slash', 'https://github.com/popovlabs/towerpower.git/', 'popovlabs/towerpower'],
  ['a trailing slash with no .git', 'https://github.com/popovlabs/towerpower/', 'popovlabs/towerpower'],
  ['upper case', 'git@github.com:PopovLabs/TowerPower.GIT', 'popovlabs/towerpower'],
  ['a fork keeps its own owner', 'https://github.com/andrewpopov/towerpower.git', 'andrewpopov/towerpower'],
  ['nested groups keep the last two segments', 'https://gitlab.example/group/sub/proj.git', 'sub/proj'],
  ['a bracketed IPv6 scp host', 'git@[2001:db8::1]:Owner/Repo.git', 'owner/repo'],
  ['a bracketed IPv6 scp host with no user', '[::1]:owner/repo.git', 'owner/repo'],
  ['a slug no runner lists is still the slug, never adhoc', 'https://github.com/nobody/never-heard-of-it.git', 'nobody/never-heard-of-it'],
];

const ADHOC_FORMS = [
  ['no origin', undefined],
  ['a null origin', null],
  ['an empty origin', '  '],
  ['one path segment', 'https://github.com/towerpower.git'],
  ['no path', 'https://github.com'],
  ['an unparseable url', 'https://'],
  ['an unknown scheme', 'ftp://example.com/a/b.git'],
  ['a segment that is not a slug', 'https://github.com/a b/c.git'],
  ['a local path that does not exist', '/no/such/dir/anywhere'],
  ['a file:// url with a host, which is no path on this machine', 'file://server/share/repo.git'],
  ['a malformed percent escape', 'ssh://host/owner/100%done.git'],
  ['a Windows drive path with forward slashes', 'C:/repos/cache.git'],
  ['a Windows drive path with backslashes', 'C:\\repos\\cache.git'],
];

const NAMING_SCENARIO = async (load) => {
  const { repoName } = await load('repo-name.js');
  for (const [what, origin, expected] of URL_FORMS) assert.equal(repoName(origin), expected, what);
  for (const [what, origin] of ADHOC_FORMS) assert.equal(repoName(origin), 'adhoc', what);
};

test('every origin form maps to its lowercase owner/name, and an origin it cannot read maps to adhoc', async () => {
  await NAMING_SCENARIO(realSrc);
  await assertCanaryFails('repo-name.js', ['return found.slug ?? ADHOC_REPO;', "return 'adhoc';"], NAMING_SCENARIO);
});

const UNKNOWN_SLUG = async (load) => {
  const { repoName } = await load('repo-name.js');
  assert.equal(repoName('https://github.com/nobody/never-heard-of-it.git'), 'nobody/never-heard-of-it');
};

test('an unknown valid slug is returned as it is; whether a runner may claim it is the runner\'s allowlist', async () => {
  await UNKNOWN_SLUG(realSrc);
  await assertCanaryFails('repo-name.js', ['return found.slug ?? ADHOC_REPO;', "return found.slug === 'nobody/never-heard-of-it' ? ADHOC_REPO : found.slug ?? ADHOC_REPO;"], UNKNOWN_SLUG);
});

const GIT_ORIGIN = 'git@github-personal:popovlabs/towerpower.git';

/** `length` repos, each naming the next as its origin; the last one's origin is `end`. Reading all of them is `length` hops from the first path. */
function chain(length, end, t) {
  const dir = makeTmpDir('repo-name-chain-', t);
  const repos = Array.from({ length }, (_, i) => path.join(dir, `r${i}`));
  repos.forEach((repo, i) => {
    fs.mkdirSync(repo);
    gitFixture(['init', '-q'], repo);
    const origin = i + 1 < length ? repos[i + 1] : end;
    if (origin !== null) gitFixture(['remote', 'add', 'origin', origin], repo);
  });
  return repos;
}

const HOPS_SCENARIO = (t) => async (load) => {
  const { repoName } = await load('repo-name.js');
  const [one] = chain(1, GIT_ORIGIN, t);
  assert.equal(repoName(one), 'popovlabs/towerpower', 'one hop');
  const [three] = chain(3, GIT_ORIGIN, t);
  assert.equal(repoName(three), 'popovlabs/towerpower', 'three hops');
  const [four] = chain(4, GIT_ORIGIN, t);
  assert.equal(repoName(four), 'adhoc', 'four hops is past the limit');
  const [dead] = chain(2, null, t);
  assert.equal(repoName(dead), 'adhoc', 'a chain that ends with no origin');
};

test('a local path origin is followed through that repo\'s own origin for at most three hops', async (t) => {
  await HOPS_SCENARIO(t)(realSrc);
  await assertCanaryFails('repo-name.js', ['export const MAX_LOCAL_HOPS = 3;', 'export const MAX_LOCAL_HOPS = 9;'], HOPS_SCENARIO(t));
});

test('a relative local origin resolves against the repo it was read from, and a cycle ends at adhoc', async (t) => {
  const { repoName } = await realSrc('repo-name.js');
  const dir = makeTmpDir('repo-name-cycle-', t);
  const [a, b] = ['a', 'b'].map((name) => path.join(dir, name));
  for (const repo of [a, b]) {
    fs.mkdirSync(repo);
    gitFixture(['init', '-q'], repo);
  }
  gitFixture(['remote', 'add', 'origin', a], b);
  gitFixture(['remote', 'add', 'origin', b], a);
  assert.equal(repoName(a), 'adhoc', 'a repo whose origin leads back to itself');
  const [rel] = chain(2, GIT_ORIGIN, t);
  assert.equal(repoName('./r1', { baseDir: path.dirname(rel) }), 'popovlabs/towerpower');
});

const STAMP_SCENARIO = async (load) => {
  const { withSubmitterStamp } = await load('repo-name.js');
  const header = { protocol: 1, argv: ['true'] };
  const stamped = withSubmitterStamp(header, 'popovlabs/towerpower');
  assert.deepEqual(stamped, { ...header, repoName: 'popovlabs/towerpower', brokerVersion: PACKAGE_VERSION });
  assert.deepEqual(header, { protocol: 1, argv: ['true'] }, 'the header it was given is not touched');
  assert.equal('repoName' in withSubmitterStamp(header, undefined), false, 'no name computed means none stamped');
};

test('the submitter stamp carries the name and this package\'s version', async () => {
  await STAMP_SCENARIO(realSrc);
  await assertCanaryFails('repo-name.js', ['brokerVersion: BROKER_VERSION }', '}'], STAMP_SCENARIO);
});

const ALIAS_SCENARIO = (t) => async (load) => {
  const { repoName } = await load('repo-name.js');
  const dir = fs.realpathSync(makeTmpDir('repo-name-alias-', t));
  const [a, b] = ['a', 'b'].map((name) => path.join(dir, 'repos', name));
  for (const repo of [a, b]) {
    fs.mkdirSync(repo, { recursive: true });
    gitFixture(['init', '-q'], repo);
  }
  gitFixture(['remote', 'add', 'origin', '../b'], a);
  gitFixture(['remote', 'add', 'origin', GIT_ORIGIN], b);
  fs.mkdirSync(path.join(dir, 'aliases'));
  fs.symlinkSync(a, path.join(dir, 'aliases', 'a'));
  assert.equal(repoName(path.join(dir, 'aliases', 'a')), 'popovlabs/towerpower', 'a relative origin resolves against the real directory, not the symlink');
};

test('a symlinked local origin resolves its relative origin against the link target', async (t) => {
  await ALIAS_SCENARIO(t)(realSrc);
  await assertCanaryFails('repo-name.js', ['base = fs.realpathSync(path.resolve(base, found.localPath));', 'base = path.resolve(base, found.localPath);'], ALIAS_SCENARIO(t));
});

test('the url forms that used to throw, split wrongly or misread are caught by their own guards', async () => {
  const scenario = async (load) => {
    const { repoName } = await load('repo-name.js');
    for (const origin of ['file://server/share/repo.git', 'ssh://host/owner/100%done.git']) assert.equal(repoName(origin), 'adhoc', origin);
    for (const origin of ['C:/repos/cache.git', 'C:\\repos\\cache.git']) assert.equal(repoName(origin), 'adhoc', origin);
    assert.equal(repoName('git@[2001:db8::1]:Owner/Repo.git'), 'owner/repo');
  };
  await scenario(realSrc);
  await assertCanaryFails('repo-name.js', [
    ['    try {\n      const url = new URL(originUrl);', '    if (true) {\n      const url = new URL(originUrl);'],
    ['    } catch {\n      return null;\n    }\n  }\n  if (DRIVE_PATH', '    }\n  }\n  if (DRIVE_PATH'],
  ], scenario, /URI malformed|file URL|must be/i);
  await assertCanaryFails('repo-name.js', ['  if (DRIVE_PATH.test(originUrl)) return { localPath: originUrl };\n', ''], scenario);
  await assertCanaryFails('repo-name.js', ['(?:\\[[0-9a-f:.]+\\]|[^:/\\s]+)', '[^:/\\s]+'], scenario);
});
