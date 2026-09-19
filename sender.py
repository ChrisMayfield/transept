#!/usr/bin/env python3
"""
The laptop in the room, for a server that is somewhere else.

Captures audio here and sends it to a hosted server over that room's
control socket, and serves the operator page on 127.0.0.1 so the volunteer
starts and stops the meeting from the machine the microphone is plugged
into. Everything else, recognition and translation and the phones, happens
on the server. HOSTED.md describes the deployment and why it exists.

It is a pipe, not a second pipeline. There is no Segmenter here, no
Translator, and no Hub, so this is not a second copy of the loops in
pipeline.py. What it holds is the microphone, one websocket, and a proxy
that makes the server look to controls.py like the Session it serves the
same page against in the default deployment.

    python3 sender.py                  the whole thing
    python3 sender.py --list-devices   find an audio source

Three settings and one key, in the same config.toml as everything else:

    [audio]   backend, encoding
    [server]  control_url, room, operator_port
    [keys]    control_token

There is no Deepgram key and no translation key on this machine. Those
live on the server, which is the point of hosting them.
"""

import argparse
import asyncio
import base64
import json
import sys
import time
from urllib.parse import quote

try:
    import websockets
    from aiohttp import web
except ImportError:
    sys.exit("Missing dependencies: pip install -r requirements.txt")

import capture
import controls
from controls import OPERATOR_HOST, mint_token
from pipeline import (add_settings_arguments, install_stop_handler,
                      load_config, load_keys, open_encoder, resolve)

# Seconds between attempts to reach the server, and how long a connection
# has to last before the next drop starts counting from the beginning
# again. A meeting that blips twice early should not pay twenty seconds of
# silence for a third blip an hour later.
BACKOFF = [1, 2, 5, 10, 20]
HEALTHY_LINK = 60
# Seconds to wait for the socket, and then for the ready that answers a
# hello. Bounded because a server that accepts the connection and says
# nothing would otherwise hold this sender forever with no way to say so.
OPEN_TIMEOUT = 10
READY_TIMEOUT = 10
# How long after pressing Start a server that still says "stopped" is
# taken at its word. The hello is what begins a session, so the first
# statuses after one describe the session that had not begun yet.
START_GRACE = 5.0
# How long the hello waits for the encoder's first pages, and how often it
# looks. The pump reads them a page or two into the stream, so this is
# ordinarily a wait of milliseconds; it is bounded because a hello that
# never goes out is a sender that never says anything at all.
HEADER_WAIT = 3.0
HEADER_POLL = 0.05


def blank_status():
    """The shape the operator page reads, before a server has filled it.

    Every field the page touches, because it renders what it is given
    without checking, and a missing number shows up in the room as the
    word undefined.
    """
    return {
        "state": "stopped", "error": None, "device": "", "model": "",
        "channels": ["English"], "listeners": {}, "refused": 0, "uptime": 0,
        "units": 0, "translated": 0, "failures": 0, "timeouts": 0,
        "reconnects": 0, "corrections": 0, "reader_url": "", "idle_stop": 0,
        "quiet_for": 0, "skipped": 0, "capped": 0, "recording": False,
        "dropped": 0, "languages": [], "english_listeners": 0,
        "median": None, "recent": [],
    }


class Remote:
    """The room's server, one socket away, in the shape the page needs.

    controls.py serves the operator page against an object with four
    methods: start, stop, set_override, and status. Session is that object
    in the default deployment. Here the same four travel over the control
    socket, so one page works either way and there is no second copy of it
    to drift.
    """

    def __init__(self, config, room, encoding):
        # The page reads two things off a config: which backend to list
        # devices from, and the reader address for the QR code. The second
        # arrives from the server when the socket opens, because a sender
        # holds neither the reader token nor public_url.
        self.config = config
        self.room = room
        # What to ask the encoder for. What it actually produces is what
        # the hello declares, because a laptop with no ffmpeg sends PCM.
        self.encoding = encoding
        self.device = ""
        self.source = None
        self.capturing = asyncio.Event()
        self.socket = None
        self.outbox = None
        self.languages = []
        self.intent = "attach"
        self.redial = asyncio.Event()
        self.snapshot = {}
        self.error = None
        self.started = 0.0
        # A device that would not reopen, so a meeting running without this
        # laptop is not retried once a second for the rest of the hour.
        self.declined = ""

    # -- what the operator page calls --------------------------------------

    async def start(self, device):
        """Open the microphone here, then ask the server for a session."""
        if self.source is not None:
            return False, "Already running."
        device = device or ""
        if not device:
            return False, "Choose an audio source first."
        try:
            source = await self.open_source(device)
        except capture.CaptureError as exc:
            self.error = str(exc)
            return False, str(exc)
        self.device = device
        self.source = source
        self.error = None
        self.declined = ""
        self.started = time.monotonic()
        # A session begins with a hello carrying an intent, because a
        # sender that reconnects has to say whether it means to begin a
        # meeting or to rejoin one. So starting one means dialing again.
        self.intent = "start"
        self.capturing.set()
        self.redial.set()
        return True, "Starting."

    async def stop(self):
        if self.source is None and not self.live():
            return False, "Not running."
        await self.release()
        self.intent = "attach"
        if not self.tell({"type": "stop"}):
            return True, ("Stopped capturing here. The server cannot be "
                          "reached, so it stops by itself shortly.")
        return True, "Stopped."

    def set_override(self, language, mode):
        """Forward a language button. Synchronous, as Session's is.

        Checked here as far as this laptop can check it, against the list
        the server sent when the socket opened. Anything the server refuses
        comes back as an error frame and reaches the page as its alert.
        """
        if self.languages and language not in self.languages:
            return False, "Unknown language."
        if mode not in ("auto", "on", "off"):
            return False, "Mode must be auto, on, or off."
        if not self.tell({"type": "language", "name": language,
                          "mode": mode}):
            return False, "Not connected to the server."
        return True, f"{language} set to {mode}."

    def status(self):
        """What the page renders: the server's own snapshot, plus this room.

        The snapshot is passed through rather than rebuilt, because the
        page has to go on rendering exactly what it renders in the default
        deployment. Laid over it is what only this laptop knows: which
        microphone is open, and whether the socket carrying the rest of it
        is up at all.
        """
        status = dict(self.snapshot) if self.snapshot else blank_status()
        if self.socket is None:
            status["state"] = "reconnecting" if self.source else "stopped"
            status["error"] = self.error or "Not connected to the server."
        elif self.error:
            status["error"] = self.error
            if self.declined and self.source is None:
                # A meeting running that this laptop is not feeding. Left
                # as running, the page paints a healthy session and hides
                # the message, because it shows errors only while a session
                # is not running.
                status["state"] = "reconnecting"
        status["device"] = self.device or status.get("device", "")
        status["reader_url"] = self.config.get("reader_url", "")
        return status

    # -- the microphone ----------------------------------------------------

    async def open_source(self, device):
        """This laptop's audio, compressed if this laptop can compress it.

        The encoder is opened here rather than with the socket, because
        what it falls back to is what the hello has to declare: the server
        builds its recognizer URL from that and cannot be told later.
        """
        source = await capture.open_capture(device, self.config["capture"])
        source, note = await open_encoder(source, self.encoding)
        if note:
            print(note)
        return source

    async def release(self):
        self.capturing.clear()
        source, self.source = self.source, None
        if source is not None:
            await source.close()

    async def pump(self):
        """This laptop's audio up the socket, for as long as both exist.

        A chunk read and dropped while there is no socket, rather than one
        left in the encoder's pipe: the pipe would fill during a blip and
        then deliver a minute of stale audio to a meeting that had moved
        on. It is the rule the server keeps at the other end, where audio
        arriving between recognizer runs has no socket to go up.
        """
        while True:
            await self.capturing.wait()
            source = self.source
            if source is None:
                self.capturing.clear()
                continue
            chunk = await source.read()
            if chunk is None:
                continue
            if not chunk:
                await self.lost("The audio source stopped. Press Start "
                                "again once it is back.")
                continue
            socket = self.socket
            if socket is None:
                continue
            try:
                await socket.send(chunk)
            except (websockets.WebSocketException, RuntimeError):
                # Reconnecting belongs to the link; this task simply stops
                # writing until there is a socket again.
                pass

    async def lost(self, message):
        """The microphone stopped, which is not something to hide.

        The session on the server would otherwise stay up on silence until
        its grace period ran out, and the operator page would say running
        with nothing arriving.
        """
        self.error = message
        await self.release()
        self.tell({"type": "stop"})

    # -- the socket --------------------------------------------------------

    async def hello(self):
        """The first message on every connection, which says what this is.

        An Opus stream is not self-describing from the middle, and the
        middle is where the server always joins: the encoder starts with
        the microphone here, and a recognizer run starts over there. So
        the hello carries the stream's first pages along with its name,
        which is the same reason the encoding is here rather than settled
        later, and they are dropped from the audio itself in the ordinary
        way, unread, while this connection is still being made.
        """
        return {"type": "hello", "room": self.room,
                "encoding": (self.source.encoding if self.source
                             else self.encoding),
                "headers": await self.stream_headers(),
                "device": self.device, "intent": self.intent}

    async def stream_headers(self):
        """The encoder's first pages as text, once it has written them.

        Empty for PCM, which has no pages to wait for, and empty when
        there is no microphone open here, which is a sender rejoining a
        room it is not feeding: the server keeps the ones it was given
        rather than taking that silence for a new stream.
        """
        source = self.source
        if source is None or source.encoding != "opus":
            return ""
        for _ in range(int(HEADER_WAIT / HEADER_POLL)):
            if source.headers:
                return base64.b64encode(source.headers).decode()
            await asyncio.sleep(HEADER_POLL)
        return ""

    def welcome(self, ready):
        """What the server tells a sender when the socket opens."""
        self.config["reader_url"] = ready.get("reader_url", "")
        self.languages = list(ready.get("languages") or [])

    def attach(self, socket):
        self.socket = socket
        self.outbox = asyncio.Queue()
        # Whatever went wrong before, this connection is past it, and a
        # stale message on the page is worse than none.
        self.error = None

    def detach(self):
        self.socket = None
        # Dropped rather than held for the next connection: a stop queued
        # during a blip and delivered a reconnect later would stop whatever
        # the operator had started in between.
        self.outbox = None

    def tell(self, message):
        """Queue one control message, or say there is nothing to carry it."""
        if self.socket is None or self.outbox is None:
            return False
        self.outbox.put_nowait(message)
        return True

    def live(self):
        """Whether the server said, last time it said anything, it was on."""
        return self.snapshot.get("state", "stopped") != "stopped"

    async def update(self, status):
        """One pushed status: keep it, and keep the two halves agreeing.

        Two ways they drift. A session can end without this laptop asking,
        because the idle watchdog stopped it or somebody stopped it from
        the server, and the microphone here would go on running for
        nothing. And a sender restarted mid-meeting finds a session already
        running with nobody feeding it, which is what attaching is for:
        reopen the device the server names and carry on.
        """
        self.snapshot = status
        running = status.get("state", "stopped") != "stopped"
        if running:
            # The session is up, so the next connection rejoins it rather
            # than asking for a second one.
            self.intent = "attach"
        if running and self.source is None:
            device = status.get("device") or ""
            if not device or device == self.declined:
                return
            try:
                self.source = await self.open_source(device)
            except capture.CaptureError as exc:
                # Once per device, not once a second: all this can offer
                # the operator is the message and the Stop button.
                self.declined = device
                self.error = f"This meeting is running, but {exc}"
                return
            self.device = device
            self.started = time.monotonic()
            self.capturing.set()
            # A new encoder is a new stream, and the pages that say what it
            # is have already gone past. The hello is what carries them, so
            # rejoining means dialing again rather than feeding this socket
            # a stream the server has no way to read.
            self.redial.set()
        elif not running and self.source is not None:
            if time.monotonic() - self.started < START_GRACE:
                return
            await self.release()


# -- the connection ----------------------------------------------------------


async def receive(remote, socket):
    """Everything the server says, until the socket closes."""
    async for raw in socket:
        if isinstance(raw, bytes):
            # Nothing comes up this socket as binary. Audio goes the other
            # way, and a frame that is not JSON is not for this program.
            continue
        message = parse(raw)
        kind = message.get("type")
        if kind == "status":
            await remote.update(message.get("status") or {})
        elif kind == "error":
            remote.error = message.get("message") or "The server refused."
        elif kind == "ready":
            remote.welcome(message)


async def deliver(remote, socket):
    """Control messages out, one at a time, while the socket is up."""
    while True:
        message = await remote.outbox.get()
        await socket.send(json.dumps(message))


async def restart(remote):
    """Wait for a start, which needs a hello and so a new connection."""
    await remote.redial.wait()


def parse(raw):
    """One text frame as a dict, or an empty one. Never a raised error."""
    try:
        message = json.loads(raw)
    except ValueError:
        return {}
    return message if isinstance(message, dict) else {}


async def link(remote, url):
    """One connection to the room's control socket, until it drops."""
    async with websockets.connect(url, open_timeout=OPEN_TIMEOUT) as socket:
        # Cleared before the hello rather than after: a start pressed while
        # this sender was offline set it, and this connection is the answer.
        remote.redial.clear()
        await socket.send(json.dumps(await remote.hello()))
        while True:
            frame = parse(await asyncio.wait_for(socket.recv(),
                                                 READY_TIMEOUT))
            if frame.get("type") == "ready":
                break
            if frame.get("type") == "error":
                # Refused before it began: the wrong room, a second sender,
                # or an encoding this meeting is not carrying.
                remote.error = frame.get("message") or "The server refused."
                return
        remote.welcome(frame)
        remote.attach(socket)
        tasks = [asyncio.create_task(receive(remote, socket)),
                 asyncio.create_task(deliver(remote, socket)),
                 asyncio.create_task(restart(remote))]
        try:
            # Whichever finishes first ends this connection: the socket
            # closed, or a start is waiting on a hello only a new one can
            # carry.
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            remote.detach()


async def supervise(remote, url):
    """Keep a connection to the server, across whatever the room's wifi does.

    A blip is a gap in the audio rather than the end of a meeting: the
    server holds the session open for control_grace seconds, so what this
    has to do is come back inside it.
    """
    attempt = 0
    while True:
        began = time.monotonic()
        try:
            await link(remote, url)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            remote.error = f"{type(exc).__name__}: {exc}"
        finally:
            remote.detach()
        if remote.redial.is_set():
            # A start is waiting, and waiting is the one thing it must not
            # do: the operator pressed a button and the room is quiet.
            continue
        if not remote.error:
            # A connection that ended with nothing to say ended at the far
            # end. attach clears the error, so anything still here is this
            # drop rather than a message from an older one.
            remote.error = "The server closed the connection."
        if time.monotonic() - began >= HEALTHY_LINK:
            attempt = 0
        await asyncio.sleep(BACKOFF[min(attempt, len(BACKOFF) - 1)])
        attempt += 1


# -- entry point -------------------------------------------------------------


def dial(url, token):
    """The control address with this sender's token on it.

    In the query string, which is the one rule every route on the server
    reads, and where the reader and operator addresses already carry
    theirs.
    """
    joiner = "&" if "?" in url else "?"
    return f"{url}{joiner}token={quote(token)}"


async def run(remote, url, token, settings):
    """The operator page here, the audio and the socket over there."""
    runner = web.AppRunner(controls.build_operator_app(remote, token))
    await runner.setup()
    tasks = []
    try:
        await web.TCPSite(runner, OPERATOR_HOST,
                          settings["operator_port"]).start()
        tasks = [asyncio.create_task(remote.pump()),
                 asyncio.create_task(supervise(remote, url))]
        stop = asyncio.Event()
        install_stop_handler(stop)
        await stop.wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # Before the runner, because closing the capture closes an encoder
        # subprocess and that needs the loop still running.
        await remote.release()
        await runner.cleanup()


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Any setting may live in config.toml instead, which is the "
               "point: a weekly run should be just `python3 sender.py`.")
    parser.add_argument("--list-devices", action="store_true",
                        help="list audio input devices and exit")
    # Repeatable, and merged left to right, so a hosted room passes the
    # tuning its server shares and then the file naming only itself.
    parser.add_argument("--config", action="append", metavar="FILE",
                        help="config file; repeat for a shared file and "
                             "then a room's own")
    add_settings_arguments(parser, [
        "capture", "encoding",
        "control_url", "room",
        "operator_port",
    ])
    args = parser.parse_args()

    if args.list_devices:
        capture.print_devices(args.capture or "auto")
        return

    parsed = load_config(*(args.config or []))
    settings = resolve(args, parsed)
    keys = load_keys(parsed)
    if not settings["control_url"]:
        sys.exit("No server to send to. Set control_url under [server] in "
                 "config.toml, for example\n"
                 "control_url = \"wss://transept.example.org/chapel/control\"")
    if not keys["control_token"]:
        sys.exit("Set control_token under [keys] in config.toml. It is what "
                 "the server knows\nthis room by, and it is the same token "
                 "that room's server was given.")
    if settings["capture"] == "remote":
        # The two halves read one config.toml format, so the mistake worth
        # catching is a server's file copied onto the laptop.
        sys.exit("[audio] backend is \"remote\", which is the server's half "
                 "of this. A sender\ncaptures from a device on the machine "
                 "it runs on, so set it to auto.")

    token = mint_token(keys["operator_token"])
    remote = Remote({"capture": settings["capture"], "reader_url": ""},
                    settings["room"], settings["encoding"])
    room = f" as {settings['room']}" if settings["room"] else ""
    print(f"Operator: http://{OPERATOR_HOST}:{settings['operator_port']}"
          f"/operator?token={token}")
    print(f"Sending:  {settings['control_url']}{room}")
    print("The address to hand the room is the server's, and it appears on "
          "the page\nabove once this sender has reached it.")
    try:
        asyncio.run(run(remote, dial(settings["control_url"],
                                     keys["control_token"]),
                        token, settings))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
