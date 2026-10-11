#!/usr/bin/env python3
"""
Transept: the weekly command, for either way a room can run.

    ./transept start | stop | restart | status

A room behind Tailscale Funnel runs the subtitle server here, with the
funnel in front of it. A room with a hosted server runs sender.py here,
which carries the microphone to that server. Which one is decided by
local/config.toml rather than by a flag: a control_token under [keys] is
what a sender needs and what nothing else on a laptop uses, so a laptop
holding one sends to the server that token belongs to.

Starting clears anything left from last time, which is why restarting is
only another name for starting.

Run it from anywhere. server.py and sender.py read config.toml, the
glossary, the key terms, and the recording database out of the directory
they start in, so everything here is anchored to local/ beside src/.

The process is found through the file it writes on its way up, since no
portable command answers "which python is running server.py out of this
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
ROOT = HERE.parent
# The room's own files, which git ignores, and the working directory every
# process started here runs in.
LOCAL = ROOT / "local"
CONFIG_FILE = LOCAL / "config.toml"
# In the checkout rather than in /tmp: Windows has no /tmp, and two checkouts
# on one machine should not share a log that carries this run's tokens.
LOG_FILE = LOCAL / "transept.log"
# Written by server.py or sender.py into its working directory, which
# do_start makes local/. The name is spelled out again rather than imported,
# because importing pipeline.py would pull httpx and websockets into a script
# that only needs to send a signal.
PID_FILE = LOCAL / "transept.pid"
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
    for candidate in (ROOT / ".venv" / "bin" / "python3",
                      ROOT / ".venv" / "Scripts" / "python.exe"):
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
    """The ports, the address on the card, and which half this laptop is.

    The ports are never copied into this file, since a second copy of the
    port is a copy that will one day disagree with the one server.py binds.

    sends is true when this laptop should run sender.py. The token can come
    from the environment as well as the file, the way load_keys reads it,
    and a backend of "remote" is the hosted server's own file, which holds
    a control token too but is never the laptop in the room.
    """
    try:
        with open(path or CONFIG_FILE, "rb") as handle:
            parsed = tomllib.load(handle)
    except FileNotFoundError:
        parsed = {}
    except (OSError, tomllib.TOMLDecodeError) as failure:
        sys.exit(f"Could not read config.toml: {failure}")
    server = parsed.get("server", {})
    token = os.environ.get("CONTROL_TOKEN",
                           parsed.get("keys", {}).get("control_token", ""))
    backend = parsed.get("audio", {}).get("backend", "auto")
    try:
        return {"reader_port": int(server.get("reader_port", 8080)),
                "operator_port": int(server.get("operator_port", 8081)),
                "public_url": str(server.get("public_url", "")),
                "sends": bool(token) and backend != "remote"}
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
        return {"pid": pid, "ports": ports,
                "role": record.get("role", "server")}
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

# What each kind of record is called when talking to the operator.
DESCRIBED = {"server": "subtitle server", "sender": "sender"}


def do_stop(indent=""):
    """Close the funnel, then stop the server. Safe when neither is up."""
    def say(text):
        print(indent + text)

    # reset rather than naming a port: the per-port "off" form was dropped
    # from the Tailscale CLI, and this script owns the funnel config anyway.
    # A laptop with no Tailscale at all, which is the usual sender, has no
    # funnel to close, and saying it could not close one would be a false
    # alarm every week.
    if tailscale_binary():
        say("Closing the Tailscale Funnel...")
        worked, output = tailscale("funnel", "reset")
        if not worked:
            say(f"Could not close the funnel. {output.strip()}")
            say("Check 'tailscale funnel status'.")

    found = server_record()
    if found is None:
        if PID_FILE.exists():
            PID_FILE.unlink(missing_ok=True)
            say("Cleared the record of a process that had already stopped.")
        say("Nothing was running here.")
        return

    pid, ports = found["pid"], found["ports"]
    say(f"Stopping the {DESCRIBED[found['role']]} (PID {pid})...")
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


def launch(script, port):
    """Start one of the two programs in local/, and wait for it to listen.

    -u because the addresses are printed once at startup and Python block
    buffers stdout when it is a file, which would hold them back until the
    process exits, which is when they stop being worth having.
    """
    try:
        LOCAL.mkdir(exist_ok=True)
        with open(LOG_FILE, "wb") as log:
            process = subprocess.Popen(
                [server_python(), "-u", str(HERE / script)],
                cwd=LOCAL, stdout=log, stderr=subprocess.STDOUT,
                **detached())
    except OSError as failure:
        print(f"Could not start {script}: {failure}")
        return False
    if not wait_until(lambda: listening(port), START_GRACE):
        print(f"{script} never listened on port {port}:")
        for line in log_lines()[-3:]:
            print("  " + line)
        process.terminate()
        return False
    return True


def do_start(config):
    """Clear what is left of last time, then start whichever half this is."""
    if not CONFIG_FILE.exists():
        print(f"There is no {CONFIG_FILE.relative_to(ROOT)} yet. Copy "
              "examples/config.example.toml there")
        print("and fill it in, as docs/SETUP.md describes.")
        return 1

    # A process left from a previous meeting holds the ports, and by then
    # there is rarely a terminal left to press Ctrl-C in, so refusing would
    # send a volunteer hunting for a process five minutes before a meeting.
    print("Clearing anything left over from a previous run:")
    do_stop(indent="  ")

    # Anything still holding a port now is genuinely not Transept, which is
    # the one case worth stopping for. A sender binds only the operator one.
    ports = [("operator", config["operator_port"])]
    if not config["sends"]:
        ports.insert(0, ("reader", config["reader_port"]))
    for name, number in ports:
        if listening(number):
            print(f"The {name} port {number} is held by something that is "
                  "not Transept,")
            print("so there is nowhere to listen. Find what is using "
                  "the port and stop it first.")
            return 1

    if config["sends"]:
        return start_sender(config)
    return start_server(config)


def start_sender(config):
    """The laptop half of a hosted room: no server here and no funnel."""
    print("Starting the sender...")
    if not launch("sender.py", config["operator_port"]):
        return 1
    print()
    print(banner("Operator:"))
    print(banner("Sending:"))
    print("The address to hand the room appears on the operator page once "
          "the sender")
    print("has reached the server.")
    print(f"Log:      {LOG_FILE}")
    print("Stop the sender with ./transept stop")
    return 0


def start_server(config):
    """The whole room on this laptop: the server, then the funnel."""
    port, public_url = config["reader_port"], config["public_url"]
    print("Starting the subtitle server...")
    if not launch("server.py", port):
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


def do_status(config):
    """Answer the two questions worth asking mid-meeting.

    Is the room being subtitled, and what address do I hand somebody.
    Nonzero when nothing is running, the way systemctl status answers.
    """
    port, operator_port = config["reader_port"], config["operator_port"]
    public_url = config["public_url"]
    found = server_record()
    if found is not None and found["role"] == "sender":
        print(f"Sender:   running (PID {found['pid']})")
        print(banner("Sending:") or "Sending:  unknown")
        print(banner("Operator:")
              or "Operator: unknown, since this sender was not started here")
        print(f"Log:      {LOG_FILE}")
        return 0
    if found is None and config["sends"]:
        print("Sender:   not running")
        return 1
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
        help="start: stop leftovers, then start the server and open the "
             "funnel, or start the sender when config.toml holds a "
             "control_token. "
             "restart: the same thing, spelled the way you meant it. "
             "stop: close the funnel, then stop whichever is running. "
             "status: whether anything is up, and the addresses.")
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
    config = read_config()
    if command == "status":
        return do_status(config)
    return do_start(config)


if __name__ == "__main__":
    sys.exit(main())
