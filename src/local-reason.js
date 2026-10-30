// BRAIN-724: a forced-local reason must name a real need for this machine. A runner gets a snapshot of the worktree,
// installs and builds its own deps and returns declared remoteArtifacts, so "the worktree is here" is not a need.
const BROWSER = /\b(browser|chromium|webkit|safari|headed)\b/i;
const HERE = /\b(here|this mac|this machine|this screen|this host|local|locally)\b/i;

export const LOCAL_REASON_CATEGORIES = [
  { label: 'ssh or scp', accepts: (t) => /\bssh\b|\bscp\b/i.test(t) },
  { label: 'the gh CLI', accepts: (t) => /\bgh\b|github cli/i.test(t) },
  {
    label: 'the lane CLI',
    accepts: (t) => /\blane (cli|command|status|run|wait|cancel|pause|resume)\b|\b(needs?|runs?|calls?|invokes?|uses?) lane\b/i.test(t),
  },
  {
    label: 'a browser rendering on this machine',
    accepts: (t) => (BROWSER.test(t) && HERE.test(t)) || /\brenders? (here|on this)\b/i.test(t),
  },
  { label: 'an input not in the snapshot (gitignored, local-only, unpublished)', accepts: (t) => /git-?ignored|\blocal-only\b|\bunpublished\b/i.test(t) },
  { label: 'a path outside the worktree', accepts: (t) => /outside (of )?(the |this )?(worktree|repo)/i.test(t) },
  { label: 'git history', accepts: (t) => /git history|\.git\b/i.test(t) },
  { label: 'docker', accepts: (t) => /\bdocker\b/i.test(t) },
  {
    label: 'Mac hardware or toolchain (Xcode, iOS simulator, macOS, iPhone, USB)',
    accepts: (t) => /\bxcode(build)?\b|\bxcrun\b|\bios simulator\b|\bmacos\b|\bdarwin\b|\biphones?\b|\bifones?\b|\busb\b/i.test(t),
  },
  { label: "Andrew's approval", accepts: (t) => /\bandrew\b[^.;]*\b(approv|asked|request)|\bandrew: standing\b|\bstanding (approval|exception)\b/i.test(t) },
];

export const acceptedLocalReason = (text) => LOCAL_REASON_CATEGORIES.some((c) => c.accepts(text));

export function localReasonRefusal(lane, text) {
  return [
    `lane run: ${lane} may run on a remote runner; --local-reason "${text}" names no need for this machine. A runner gets a snapshot of the worktree and installs and builds its own deps.`,
    'Drop --local: a missing build step belongs in the lane\'s remoteSetup, and output that must come back belongs in its remoteArtifacts.',
    `Accepted reasons name one of: ${LOCAL_REASON_CATEGORIES.map((c) => c.label).join('; ')}.`,
  ];
}
