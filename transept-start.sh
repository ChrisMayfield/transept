#!/usr/bin/env bash
# Start the subtitle server and open a Tailscale Funnel in front of it.
# Run it from anywhere: server.py reads config.toml, .env, and static/ out
# of the directory it starts in, so move there first.
set -euo pipefail
cd "$(dirname "$0")"

LOG_FILE="/tmp/transept-server.log"
if [ -x .venv/bin/python3 ]; then
    PYTHON=.venv/bin/python3
else
    PYTHON=python3
fi

# The ports and the address on the card come from config.toml, because a
# copy of the port here would one day disagree with the one server.py binds.
read -r PORT OPERATOR_PORT PUBLIC_URL <<<"$("$PYTHON" - <<'PY'
import tomllib
try:
    server = tomllib.load(open("config.toml", "rb")).get("server", {})
except FileNotFoundError:
    server = {}
print(server.get("port", 8080), server.get("operator_port", 8081),
      server.get("public_url", ""))
PY
)"

# A server left from a previous meeting holds both ports, and by then there
# is rarely a terminal left to press Ctrl-C in. Clearing it here also means
# the funnel below is opened in front of a server this pair can stop.
echo "Clearing anything left over from a previous run:"
./transept-stop.sh | sed 's/^/  /'

busy=$(ss -ltnpH "sport = :$PORT or sport = :$OPERATOR_PORT")
if [ -n "$busy" ]; then
    echo "Port $PORT or $OPERATOR_PORT is held by something that is not"
    echo "Transept, so the server has nowhere to listen:"
    echo "$busy"
    exit 1
fi

# -u because the operator address is printed once at startup and Python
# buffers stdout when it is a file, which would hold that line back until
# the server exits.
echo "Starting the subtitle server..."
"$PYTHON" -u server.py >"$LOG_FILE" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 30); do
    if (: </dev/tcp/127.0.0.1/"$PORT") 2>/dev/null; then break; fi
    sleep 0.5
done
if ! (: </dev/tcp/127.0.0.1/"$PORT") 2>/dev/null; then
    echo "The server never listened on port $PORT:"
    tail -n 3 "$LOG_FILE"
    kill "$SERVER_PID" 2>/dev/null || true
    exit 1
fi

# --bg so the funnel outlives this script, --yes because nobody is at a
# prompt: these two scripts own the funnel config.
echo "Opening the Tailscale Funnel on port $PORT..."
if ! tailscale funnel --bg --yes "$PORT"; then
    echo "The funnel did not open, so no phone off this laptop can reach the"
    echo "subtitles. If the message above was about access, run this once:"
    echo "    sudo tailscale set --operator=$USER"
    ./transept-stop.sh >/dev/null
    exit 1
fi

FUNNEL_URL="https://$(tailscale status --json | "$PYTHON" -c "import json, sys
print(json.load(sys.stdin)['Self']['DNSName'].rstrip('.'))")/"
echo
# The QR code the room scans is built from public_url, so a stale value
# there is a printed card that opens nothing.
if [ "${PUBLIC_URL%/}" != "${FUNNEL_URL%/}" ]; then
    echo "Warning: public_url in config.toml is \"$PUBLIC_URL\","
    echo "but this funnel serves $FUNNEL_URL"
fi
echo "Readers:  $FUNNEL_URL"
grep -m1 '^Operator: ' "$LOG_FILE" || true
echo "Log:      $LOG_FILE"
echo "Stop the server, and the funnel, with ./transept-stop.sh"
