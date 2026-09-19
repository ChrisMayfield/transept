#!/usr/bin/env python3
"""
Transept: the subtitle server and the Tailscale Funnel in front of it.

    ./transept start | stop | restart | status

The weekly command for a room behind Tailscale Funnel. Starting clears
anything left from last time, which is why restarting is only another name
for starting.

Run it from anywhere. server.py reads config.toml, the glossary, the key
terms, and the recording database out of the directory it starts in, so
everything here is anchored to the directory this file sits in.

The server is found through the file server.py writes on its way up, since
no portable command answers "which python is running server.py out of this
directory". Nothing else in the project imports this file, and server.py
stays runnable on its own, which is also the form a systemd unit would take.
"""

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import tomllib
from pathlib import Path

HERE = Path(__file__).resolve().parent
# Beside the code rather than in /tmp: Windows has no /tmp, and two checkouts
# on one machine should not share a log that carries this run's tokens.
LOG_FILE = HERE / "transept.log"
# Written by server.py into its working directory, which do_start makes this
# one. The name is spelled out again rather than imported, because importing
# server.py would pull aiohttp and the whole pipeline into a script that only
# needs to send a signal.
PID_FILE = HERE / "transept.pid"
# Seconds to wait for each half of the work. A server on its way down has
# lines to drain; a server on its way up has two ports to bind.
STOP_GRACE = 10
START_GRACE = 15


# -- the machine this runs on ------------------------------------------------


def listening(port):
    """True when something answers on that loopback port.

    The question every step here actually cares about. A port that answers
    is a port the next server cannot have, and a port that has gone quiet
    is a server that has finished letting go.
    """
    try:
        with socket.create_connection(("127.0.0.1", port), 0.25):
            return True
    except OSError:
        return False


def alive(pid):
    """True when that process id still exists."""
    if os.name == "nt":
        # Not os.kill(pid, 0), which is no liveness probe on Windows: every
        # signal but the two console events is TerminateProcess there, so
        # asking would kill the process being asked about.
        found = subprocess.run(["tasklist", "/FI", f"PID eq {pid}",
                                "/NH", "/FO", "CSV"],
                               capture_output=True, text=True)
        return f'"{pid}"' in found.stdout
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Somebody else's process, which is not ours to stop but is alive.
        return True
    return True


def wait_until(settled, seconds):
    """Poll until settled() is true, and report whether it got there."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if settled():
            return True
        time.sleep(0.25)
    return settled()


def server_python():
    """The interpreter with the packages, which is the virtual one.

    SETUP.md has the operator create one in .venv, and the point of this
    script is that a meeting starts without activating anything first.
    """
    for candidate in (HERE / ".venv" / "bin" / "python3",
                      HERE / ".venv" / "Scripts" / "python.exe"):
        if candidate.exists():
            return str(candidate)
    return sys.executable


def detached():
    """Popen arguments for a server that outlives this script."""
    if os.name == "nt":
        return {"creationflags": (subprocess.DETACHED_PROCESS
                                  | subprocess.CREATE_NEW_PROCESS_GROUP)}
    return {"start_new_session": True}


# -- the server ---------------------------------------------------------------


def read_config(path=None):
    """The two ports and the address on the card, from config.toml.

    Never copied into this file, since a second copy of the port is a copy
    that will one day disagree with the one server.py binds.
    """
    try:
        with open(path or HERE / "config.toml", "rb") as handle:
            server = tomllib.load(handle).get("server", {})
    except FileNotFoundError:
        server = {}
    except (OSError, tomllib.TOMLDecodeError) as failure:
        sys.exit(f"Could not read config.toml: {failure}")
    try:
        return (int(server.get("reader_port", 8080)),
                int(server.get("operator_port", 8081)),
                str(server.get("public_url", "")))
    except (TypeError, ValueError):
        sys.exit("The ports in config.toml are not numbers. "
                 "Check the [server] section.")


def server_record():
    """What server.py left behind, or None when nothing is serving here.

    The file outlives a server that was killed outright, so the ports
    decide rather than the file alone: a recorded process that is alive and
    still holding one of its ports is the server, and anything else is a
    leftover to clear away. Checking both is also what keeps a process id
    the system has since handed to something else from being signalled.
    """
    try:
        record = json.loads(PID_FILE.read_text(encoding="utf-8"))
        pid = int(record["pid"])
    except (OSError, ValueError, KeyError, TypeError):
        return None
    ports = [port for port in (record.get("reader_port"),
                               record.get("operator_port"))
             if isinstance(port, int)]
    if alive(pid) and any(listening(port) for port in ports):
        return {"pid": pid, "ports": ports}
    return None


def stop_process(pid):
    """Ask the server to stop, the way Ctrl-C does.

    SIGTERM reaches the handler server.py installs, which stops the session
    and lets the last lines drain. Windows has no such signal, and the
    server is started with no console for a Ctrl event to reach, so there
    the stop is abrupt: the meeting ends either way, and what is lost is
    the drain and the tidy goodbye to phones still connected.
    """
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                       capture_output=True)
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass


def force_process(pid):
    """The second attempt, for a server that ignored the first."""
    if os.name == "nt":
        stop_process(pid)      # already the forceful one there
        return
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass


def banner(prefix):
    """The Reader or Operator line server.py printed when it started.

    Read back out of the log rather than built here, because each address
    carries a token that run minted, and the funnel hostname on its own
    now opens nothing.
    """
    for line in log_lines():
        if line.startswith(prefix):
            return line
    return ""


def log_lines():
    """Whatever is in the log, or nothing when there is no log to read."""
    try:
        return LOG_FILE.read_text(encoding="utf-8",
                                  errors="replace").splitlines()
    except OSError:
        return []


# -- the funnel ---------------------------------------------------------------


def tailscale_binary():
    """The CLI, which a macOS application bundle keeps off the PATH."""
    found = shutil.which("tailscale")
    if found:
        return found
    for guess in ("/Applications/Tailscale.app/Contents/MacOS/Tailscale",
                  r"C:\Program Files\Tailscale\tailscale.exe"):
        if Path(guess).exists():
            return guess
    return ""


def tailscale(*args):
    """Run one tailscale command, as (worked, output).

    A missing CLI is reported as a command that failed, because from the
    room's point of view there is no difference between tailscale being
    absent and tailscale refusing. The output is held rather than printed,
    so that the first funnel of a tailnet, which answers with a link to the
    admin console, reaches the operator as part of the failure.
    """
    binary = tailscale_binary()
    if not binary:
        return False, "Tailscale is not installed, or is not on the PATH."
    try:
        done = subprocess.run([binary, *args], text=True, capture_output=True)
    except OSError as failure:
        return False, str(failure)
    return done.returncode == 0, (done.stdout or "") + (done.stderr or "")


def funnel_url():
    """The address this machine answers on, or empty when unknown.

    Empty rather than a guess, since every caller would rather say nothing
    than print half an address.
    """
    worked, output = tailscale("status", "--json")
    if not worked:
        return ""
    try:
        name = json.loads(output)["Self"]["DNSName"].rstrip(".")
    except (ValueError, KeyError, AttributeError, TypeError):
        return ""
    return f"https://{name}/" if name else ""


def funnel_open(port):
    """True when a funnel is serving that port right now."""
    worked, output = tailscale("funnel", "status")
    return worked and f"127.0.0.1:{port}" in output


# -- the commands -------------------------------------------------------------


def do_stop(indent=""):
    """Close the funnel, then stop the server. Safe when neither is up."""
    def say(text):
        print(indent + text)

    # reset rather than naming a port: the per-port "off" form was dropped
    # from the Tailscale CLI, and this script owns the funnel config anyway.
    say("Closing the Tailscale Funnel...")
    worked, output = tailscale("funnel", "reset")
    if not worked:
        say(f"Could not close the funnel. {output.strip()}")
        say("Check 'tailscale funnel status'.")

    found = server_record()
    if found is None:
        if PID_FILE.exists():
            PID_FILE.unlink(missing_ok=True)
            say("Cleared the record of a server that had already stopped.")
        say("No subtitle server was running here.")
        return

    pid, ports = found["pid"], found["ports"]
    say(f"Stopping the subtitle server (PID {pid})...")
    stop_process(pid)
    # Waiting on the ports rather than on the process. The ports are what
    # the next server needs back, they are the last thing server.py lets go
    # of, and a process that has finished but has not been reaped by its
    # parent yet still answers every liveness test there is, which would
    # spend the whole grace period waiting for something already stopped.

    def quiet():
        return not any(listening(port) for port in ports)

    if not wait_until(quiet, STOP_GRACE):
        say(f"It ignored the stop signal for {STOP_GRACE} seconds "
            "and was forced.")
        force_process(pid)
        wait_until(quiet, 5)
    PID_FILE.unlink(missing_ok=True)


def do_start(port, operator_port, public_url):
    """Clear what is left of last time, start the server, open the funnel."""
    # A server left from a previous meeting holds both ports, and by then
    # there is rarely a terminal left to press Ctrl-C in, so refusing would
    # send a volunteer hunting for a process five minutes before a meeting.
    print("Clearing anything left over from a previous run:")
    do_stop(indent="  ")

    # Anything still holding a port now is genuinely not Transept, which is
    # the one case worth stopping for.
    for name, number in (("reader", port), ("operator", operator_port)):
        if listening(number):
            print(f"The {name} port {number} is held by something that is "
                  "not Transept,")
            print("so the server has nowhere to listen. Find what is using "
                  "the port and stop it first.")
            return 1

    # -u because both addresses are printed once at startup and Python
    # block buffers stdout when it is a file, which would hold them back
    # until the server exits, which is when they stop being worth having.
    print("Starting the subtitle server...")
    try:
        with open(LOG_FILE, "wb") as log:
            process = subprocess.Popen(
                [server_python(), "-u", str(HERE / "server.py")],
                cwd=HERE, stdout=log, stderr=subprocess.STDOUT, **detached())
    except OSError as failure:
        print(f"Could not start server.py: {failure}")
        return 1
    if not wait_until(lambda: listening(port), START_GRACE):
        print(f"The server never listened on port {port}:")
        for line in log_lines()[-3:]:
            print("  " + line)
        process.terminate()
        return 1

    # --bg so the funnel outlives this script, --yes because nobody is at a
    # prompt: this script owns the funnel configuration.
    print(f"Opening the Tailscale Funnel on port {port}...")
    worked, output = tailscale("funnel", "--bg", "--yes", str(port))
    if not worked:
        print(output.strip() or "The funnel did not open.")
        print("No phone off this laptop can reach the subtitles.")
        if sys.platform.startswith("linux"):
            print("If the message above was about access, run this once:")
            user = os.environ.get("USER", "$USER")
            print(f"    sudo tailscale set --operator={user}")
        else:
            print("Check that Tailscale is signed in and that Funnel is "
                  "enabled for this tailnet.")
        # A failed start leaves nothing running, so that half a meeting is
        # never left behind a tunnel that did not open.
        do_stop(indent="  ")
        return 1

    print()
    url = funnel_url()
    # The QR code the room scans is built from public_url, so a stale value
    # there is a printed card that opens nothing.
    if url and public_url.rstrip("/") != url.rstrip("/"):
        print(f'Warning: public_url in config.toml is "{public_url}",')
        print(f"but this funnel serves {url}")
    print(banner("Reader:") or f"Reader:   {url or public_url}")
    operator_line = banner("Operator:")
    if operator_line:
        print(operator_line)
    print(f"Log:      {LOG_FILE}")
    print("Stop the server, and the funnel, with ./transept stop")
    return 0


def do_status(port, operator_port, public_url):
    """Answer the two questions worth asking mid-meeting.

    Is the room being subtitled, and what address do I hand somebody.
    Nonzero when the server is not running, the way systemctl status
    answers.
    """
    found = server_record()
    if found is None:
        print("Server:   not running")
    else:
        print(f"Server:   running (PID {found['pid']})")
        print(f"          reader port {port}, "
              f"operator port {operator_port}")

    # The funnel is a separate thing that can outlive the server, which is
    # the state worth naming out loud: an address still answering all week.
    print(f"Funnel:   open on port {port}" if funnel_open(port)
          else "Funnel:   closed")

    # The addresses carry a token minted for the run, so a stopped server
    # has none to give and the hostname alone is all there is to say.
    if found is None:
        where = funnel_url() or public_url or "unknown"
        print(f"Reader:   {where}, once a server is up")
        return 1
    print(banner("Reader:")
          or "Reader:   unknown, since this server was not started here")
    print(banner("Operator:")
          or "Operator: unknown, since this server was not started here")
    print(f"Log:      {LOG_FILE}")
    return 0


def main():
    parser = argparse.ArgumentParser(
        prog="./transept", description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "command", choices=("start", "restart", "stop", "status", "help"),
        help="start: stop leftovers, start the server, open the funnel. "
             "restart: the same thing, spelled the way you meant it. "
             "stop: close the funnel, then stop the server. "
             "status: whether either is up, and the addresses.")
    command = parser.parse_args().command
    if command == "help":
        parser.print_help()
        return 0
    # Stopping has to work when config.toml does not parse, because the
    # point of stopping is to leave nothing running and nothing exposed.
    # The ports come out of the record server.py left instead.
    if command == "stop":
        do_stop()
        return 0
    port, operator_port, public_url = read_config()
    if command == "status":
        return do_status(port, operator_port, public_url)
    return do_start(port, operator_port, public_url)


if __name__ == "__main__":
    sys.exit(main())
