/**
 * BRAIN-533: a runner's optional `schedule` -- the wall-clock windows in which it is powered on (a workstation shut down
 * every night). Pure and clock-injected: nothing here reads the system time or the filesystem.
 */

export const SCHEDULE_DAYS = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];
const TIME_RE = /^([01]\d|2[0-3]):([0-5]\d)$/;
const DAY_MS = 86_400_000;

const minutesOf = (hhmm) => {
  const m = TIME_RE.exec(hhmm);
  return Number(m[1]) * 60 + Number(m[2]);
};

/** Returns the first problem with a runner's `schedule` as a message, or null when it is valid. */
export function scheduleProblem(schedule) {
  if (!schedule || typeof schedule !== 'object' || Array.isArray(schedule)) return '"schedule" must be an object';
  if (typeof schedule.timezone !== 'string' || schedule.timezone.length === 0) return '"schedule.timezone" must be a non-empty IANA timezone name';
  try {
    new Intl.DateTimeFormat('en-US', { timeZone: schedule.timezone });
  } catch {
    return `"schedule.timezone" is not a known timezone: ${JSON.stringify(schedule.timezone)}`;
  }
  if (!Array.isArray(schedule.available) || schedule.available.length === 0) return '"schedule.available" must be a non-empty array of windows';
  for (const [i, win] of schedule.available.entries()) {
    const at = `schedule.available[${i}]`;
    if (!win || typeof win !== 'object' || Array.isArray(win)) return `"${at}" must be an object`;
    if (!Array.isArray(win.days) || win.days.length === 0) return `"${at}.days" must be a non-empty array`;
    for (const day of win.days) {
      if (!SCHEDULE_DAYS.includes(day)) return `"${at}.days" has an unknown day ${JSON.stringify(day)} (use ${SCHEDULE_DAYS.join(', ')})`;
    }
    if (new Set(win.days).size !== win.days.length) return `"${at}.days" lists a day twice`;
    for (const field of ['from', 'to']) {
      if (typeof win[field] !== 'string' || !TIME_RE.test(win[field])) return `"${at}.${field}" must be a 24-hour "HH:MM" time, got ${JSON.stringify(win[field])}`;
    }
    if (win.from === win.to) return `"${at}" has from equal to to (an empty or ambiguous window)`;
  }
  return null;
}

const formatters = new Map();
function formatterFor(timeZone) {
  if (!formatters.has(timeZone)) {
    formatters.set(
      timeZone,
      new Intl.DateTimeFormat('en-US', { timeZone, hourCycle: 'h23', year: 'numeric', month: 'numeric', day: 'numeric', hour: 'numeric', minute: 'numeric', second: 'numeric' }),
    );
  }
  return formatters.get(timeZone);
}

/** The wall-clock reading of instant `ms` in `timeZone`, as a UTC-based ms count (so calendar math is plain arithmetic). */
function wallMs(ms, timeZone) {
  const p = Object.fromEntries(formatterFor(timeZone).formatToParts(new Date(ms)).map((x) => [x.type, Number(x.value)]));
  return Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second);
}

/** The instant at which `timeZone`'s wall clock reads `wall` (a UTC-based ms count); a nonexistent DST-gap time lands just after the gap. */
function instantOf(wall, timeZone) {
  let guess = wall;
  for (let i = 0; i < 3; i += 1) guess = wall - (wallMs(guess, timeZone) - guess);
  return guess;
}

/**
 * Whether a runner is inside one of its windows at `now` (ms). No schedule: always available, `closesAt: null`. Outside every
 * window: `{ available: false, closesAt: null }`. Inside: `closesAt` is the end (ms) of the latest-ending window containing `now`.
 * A window whose `to` is not after its `from` crosses midnight and belongs to the day it starts on.
 */
export function runnerAvailability(schedule, now) {
  if (!schedule) return { available: true, closesAt: null };
  const wallNow = wallMs(now, schedule.timezone);
  const todayStart = wallNow - (((wallNow % DAY_MS) + DAY_MS) % DAY_MS);
  let closesAt = null;
  for (const win of schedule.available) {
    const from = minutesOf(win.from);
    const to = minutesOf(win.to);
    for (const offset of [-1, 0]) {
      const dayStart = todayStart + offset * DAY_MS;
      const day = SCHEDULE_DAYS[(new Date(dayStart).getUTCDay() + 6) % 7];
      if (!win.days.includes(day)) continue;
      const start = instantOf(dayStart + from * 60_000, schedule.timezone);
      const end = instantOf(dayStart + (to > from ? to : to + 1440) * 60_000, schedule.timezone);
      if (start <= now && now < end && (closesAt === null || end > closesAt)) closesAt = end;
    }
  }
  return { available: closesAt !== null, closesAt };
}
