#!/bin/bash
# BRAIN-1150/BRAIN-1157/BRAIN-1145, bosun README "Installing"; also the lane child's bosun setup (--dry-run/--apply). Keep /bin/bash 3.2 compatible: it runs before brew bash exists.
# Darwin installs system tools with Homebrew; Linux never uses root and only reports a missing system tool with its apt fix.
# RUNNER_PREREQS_UNAME overrides `uname -s`; tests use it to force a platform branch.
set -u

usage() {
  echo "usage: $0 (--check | --install | --dry-run | --apply) [--path <dir>:<dir>...]" >&2
  exit 2
}

here=$(cd "$(dirname "$0")" && pwd)
brewfile="$here/Brewfile.lane-runner"
wheels="$here/requirements.lane-runner.txt"
playwrights="$here/playwright.lane-runner.txt"
browsers="chromium chromium-headless-shell webkit"
mode=""
os=${RUNNER_PREREQS_UNAME:-$(uname -s)}
case "$os" in
  Darwin)
    path="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
    hint_bash="brew bash"
    hint_timeout="brew coreutils"
    hint_flock="brew flock"
    hint_python="brew python"
    hint_node=""
    ;;
  Linux)
    path="/usr/local/bin:/usr/bin:/bin"
    hint_bash="apt install bash"
    hint_timeout="apt install coreutils"
    hint_flock="apt install util-linux"
    hint_python="apt install python3 python3-pip"
    hint_node=" (apt install nodejs npm)"
    ;;
  *)
    echo "unsupported platform $os: the lane runner prerequisites cover Darwin and Linux" >&2
    exit 2
    ;;
esac
while [ $# -gt 0 ]; do
  case "$1" in
    --check | --dry-run) mode=check ;;
    --install | --apply) mode=install ;;
    --path)
      [ $# -ge 2 ] || usage
      path=$2
      shift
      ;;
    *) usage ;;
  esac
  shift
done
[ -n "$mode" ] || usage

on_path() { env PATH="$path" "$@"; }

playwright_versions() { grep -Ev '^[[:space:]]*(#|$)' "$playwrights"; }

# Every browser directory one Playwright version installs is present (its own `install --dry-run` names them).
has_browsers() {
  local locations dir
  locations=$(cd /tmp && on_path npx -y "playwright@$1" install --dry-run $browsers 2> /dev/null | sed -n 's/^ *Install location: *//p')
  [ -n "$locations" ] || return 1
  while IFS= read -r dir; do [ -d "$dir" ] || return 1; done <<< "$locations"
}

is_on_path() {
  local IFS=: dir
  for dir in $path; do [ -x "$dir/$1" ] && return 0; done
  return 1
}

system_lacks() {
  on_path bash -c '[ "${BASH_VERSINFO[0]}" -ge 5 ]' > /dev/null 2>&1 || echo "bash >= 5 ($hint_bash)"
  on_path timeout -k 1 5 true > /dev/null 2>&1 || echo "timeout with -k ($hint_timeout)"
  local lock
  lock=$(mktemp "${TMPDIR:-/tmp}/runner-prereqs.XXXXXX")
  on_path flock "$lock" true > /dev/null 2>&1 || echo "flock ($hint_flock)"
  rm -f "$lock"
  on_path python3 -c 'import tomllib' > /dev/null 2>&1 || echo "python3 >= 3.11 with tomllib ($hint_python)"
}

check() {
  missing=0
  miss() {
    echo "missing: $1" >&2
    missing=$((missing + 1))
  }
  while IFS= read -r item; do miss "$item"; done < <(system_lacks)
  on_path python3 -c 'import numpy' > /dev/null 2>&1 || miss "python3 numpy (pip numpy)"
  on_path python3 -c 'import PIL' > /dev/null 2>&1 || miss "python3 Pillow (pip pillow)"
  if is_on_path npx; then
    for version in $(playwright_versions); do
      has_browsers "$version" || miss "playwright $version browsers ($browsers)"
    done
  else
    miss "npx (node), for the playwright browsers$hint_node"
  fi
  if [ "$missing" -gt 0 ]; then
    echo "$missing lane-runner prerequisite(s) missing on PATH=$path" >&2
    return 1
  fi
  echo "ok: every lane-runner prerequisite is on PATH=$path"
}

# Neither `brew bundle check` nor `brew list` works for a user who does not own Homebrew, so the capability checks decide.
install_system_darwin() {
  if ! is_on_path brew; then
    echo "brew is not on PATH=$path: install Homebrew first, or pass --path" >&2
    exit 2
  fi
  export HOMEBREW_NO_AUTO_UPDATE=1
  local brew_missing cellar locks owner item
  brew_missing=$(system_lacks)
  [ -n "$brew_missing" ] || return 0
  cellar=$(on_path brew --cellar)
  locks="$(on_path brew --prefix)/var/homebrew/locks"
  if [ -w "$cellar" ] && { [ ! -d "$locks" ] || [ -w "$locks" ]; }; then
    on_path brew bundle --no-upgrade --file="$brewfile" || failed=1
  else
    failed=1
    while IFS= read -r item; do echo "missing from Homebrew: $item" >&2; done <<< "$brew_missing"
    owner=$(ls -ld "$cellar" | awk '{print $3}')
    echo "Homebrew is not writable by $(id -un): its owner $owner must run: brew bundle --file=$brewfile" >&2
  fi
}

if [ "$mode" = install ]; then
  if [ "$(id -u)" -eq 0 ]; then
    echo "refusing to run as root: the runner prerequisites install as the service user" >&2
    exit 2
  fi
  failed=0
  [ "$os" = Darwin ] && install_system_darwin
  if on_path python3 -c 'import tomllib' > /dev/null 2>&1; then
    on_path python3 -m pip install --quiet --disable-pip-version-check --user --break-system-packages \
      --only-binary=:all: -r "$wheels" || failed=1
  else
    echo "skipping the PyPI wheels: no python3 >= 3.11 yet" >&2
  fi
  for version in $(playwright_versions); do
    (cd /tmp && on_path npx -y "playwright@$version" install $browsers) || failed=1
  done
  check || exit 1
  exit "$failed"
fi
check
