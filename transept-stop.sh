#!/usr/bin/env bash
# Stop the subtitle server and close the Tailscale Funnel.
# transept-start.sh runs this first, so it has to be safe when nothing is
# running. No -e: every step is worth attempting even when an earlier one
# failed, because the point is to leave nothing running and nothing exposed.
set -uo pipefail
cd "$(dirname "$0")"

# reset rather than naming a port: the per-port "off" form was dropped from
# the Tailscale CLI, and these two scripts own the funnel config anyway.
echo "Closing the Tailscale Funnel..."
tailscale funnel reset ||
    echo "Could not close it. Check 'tailscale funnel status'."

# A zombie counts as gone: it is finished and holds no port, its parent
# has simply not reaped it yet, and kill -0 alone would call it alive and
# spend ten seconds waiting for a process that already stopped.
gone() {
    case "$(ps -p "$1" -o state= 2>/dev/null)" in ""|Z*) return 0 ;; esac
    return 1
}

stopped=""
for pid in $(pgrep -u "$USER" -f 'server\.py'); do
    # Ours is a python running out of this directory. Matching the command
    # line alone would also match an editor or a shell with the same words
    # in it, and this loop is about to send that process a signal.
    case "$(ps -p "$pid" -o comm=)" in python*) ;; *) continue ;; esac
    [ "$(readlink -f "/proc/$pid/cwd" 2>/dev/null)" = "$PWD" ] || continue
    # A plain kill is SIGTERM, which server.py handles like Ctrl-C: the
    # session stops and the last lines drain.
    echo "Stopping the subtitle server (PID $pid)..."
    kill "$pid" 2>/dev/null
    for _ in $(seq 1 20); do
        gone "$pid" && break
        sleep 0.5
    done
    if ! gone "$pid"; then
        echo "It ignored the stop signal for ten seconds, so it was forced."
        kill -9 "$pid" 2>/dev/null
    fi
    stopped=1
done
[ -n "$stopped" ] || echo "No subtitle server was running here."
