#!/bin/sh
# Drives spike.c inside interactive R sessions on a pty (script(1)), so R's
# terminal SIGINT handler is the one installed when the wait runs. Three cases:
#
#   1. timed wait (10 s), SIGINT at ~1.5 s  -> PASS iff ret == -EINTR (-4)
#                                              and elapsed ~1.5 s
#   2. timed wait (2 s), no signal          -> sanity: ret == -ETIMEDOUT (-60)
#   3. untimed wait (0 = forever), SIGINT   -> informational: EINTR or silently
#      at ~1.5 s, SIGTERM backstop at +6 s     restarted (no result file)
#
# Usage: sh spike.sh   (from this directory; macOS only)

set -eu
cd "$(dirname "$0")"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

R CMD SHLIB spike.c >/dev/null

run_case() {
  name=$1 timeout=$2 signal=$3
  pidfile="$work/$name.pid" result="$work/$name.result"

  if [ "$signal" = "yes" ]; then
    (
      while [ ! -s "$pidfile" ]; do sleep 0.1; done
      sleep 1.5
      kill -INT "$(cat "$pidfile")" 2>/dev/null || true
      sleep 6
      if kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        kill -TERM "$(cat "$pidfile")" 2>/dev/null || true
        sleep 3
        kill -KILL "$(cat "$pidfile")" 2>/dev/null || true
      fi
    ) &
    watcher=$!
  fi

  {
    printf 'dyn.load("spike.so")\n'
    printf 'writeLines(as.character(Sys.getpid()), "%s")\n' "$pidfile"
    printf 'invisible(.Call("spike_ulock", %s, "%s"))\n' "$timeout" "$result"
    printf 'q("no")\n'
    sleep 20
  } | script -q /dev/null R --no-save -q >/dev/null 2>&1 || true

  if [ "$signal" = "yes" ]; then wait "$watcher" 2>/dev/null || true; fi

  if [ -s "$result" ]; then
    printf '%s: %s\n' "$name" "$(cat "$result")"
  else
    printf '%s: NO RESULT (wait never returned before backstop)\n' "$name"
  fi
}

run_case timed-sigint 10 yes
run_case timed-quiet 2 no
run_case untimed-sigint 0 yes
