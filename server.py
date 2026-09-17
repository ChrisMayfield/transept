#!/usr/bin/env python3
"""
Live subtitling and translation server.

Runs the capture pipeline behind an HTTP server, so a volunteer can start and
stop it from a browser and listeners can read subtitles on their phones.

Two listeners. The reader port carries the pages phones use, and is the
one to point a tunnel at; the operator port stays on 127.0.0.1.

    /read               reader view with a language picker
    /stream/<channel>   server-sent events for one language
    /operator           start and stop controls, status, language toggles

Both addresses carry a token, so a tunnel open to the internet serves the
meeting rather than whoever finds it. The bare / is a 404: a visitor who
was not handed the address gets nothing.

Speech recognition runs continuously while a session is on. Translation runs
per language, only while somebody is reading that language or the operator
has forced it on, so an unread language costs nothing.

Both addresses are printed at startup with tokens minted for that run,
unless reader_token and operator_token in config.toml pin them.

Usage:
    python3 server.py --model gemini-3.8-flash --reasoning-effort low \\
        --languages "French,Swahili,Spanish" --correct-english \\
        --glossary glossary.txt --keyterms keyterms.txt

The language list is fixed when the server starts. Readers and the operator
choose from it at runtime, but the set itself does not change, so a printed
QR code or a bookmarked link keeps working from week to week.
"""

import argparse
import asyncio
import hashlib
import hmac
import io
import json
import os
import secrets
import sys
import time
from collections import deque
from pathlib import Path

try:
    import websockets
    from aiohttp import web
except ImportError:
    sys.exit("Missing dependencies: pip install websockets httpx aiohttp")

import capture
import record
from pipeline import (SYSTEM_PROMPT, Segmenter, Translator,
                      add_settings_arguments,
                      build_asr_url, install_stop_handler, listen,
                      load_config, load_file_lines, load_glossary,
                      load_keys, publish, pump_audio,
                      resolve)

STATIC = Path(__file__).parent / "static"
# Where this process records itself so the transept script can stop it
# later. In the working directory rather than beside the code, because that
# is where config.toml, the glossary, and sessions.db already live, and "a
# server running out of this directory" is the thing being identified.
# transept.py spells the name out again rather than importing it, since
# importing this file would pull aiohttp and the whole pipeline into a
# script that only needs to send a signal.
PID_FILE = Path("transept.pid")
# Not a setting: the point of the second listener is that a tunnel cannot
# be pointed at it by mistake.
OPERATOR_HOST = "127.0.0.1"
HISTORY = 60
RECONNECT_BACKOFF = [1, 2, 5, 10, 20]
# Seconds a run must last to count as healthy and reset the backoff. Without
# it, a few blips early in a meeting make a later one cost twenty seconds.
HEALTHY_RUN = 60
# Queued in place of a subtitle to tell a stream handler its reader has
# moved on. A sentinel object rather than None, which json.dumps would
# happily turn into a subtitle reading "null".
LEAVING = object()


class Hub:
    """Ring buffer plus live subscribers, one channel per language."""

    def __init__(self, languages, max_readers=0):
        self.channels = ["English"] + list(languages)
        self.max_readers = max_readers         # 0 means no cap
        self.refused = 0
        self.buffers = {name: deque(maxlen=HISTORY) for name in self.channels}
        self.subscribers = {name: set() for name in self.channels}
        # When a reader was last cut off a channel without meaning to be, so
        # a language does not shut off the instant a phone drops and comes
        # back. Set only by an involuntary departure: a reader who picks a
        # different language has not lost anything to wait out.
        self.last_seen = dict.fromkeys(self.channels)
        # reader id -> the one queue that reader is holding.
        self.streams = {}

    def publish(self, channel, seq, text):
        """Add a line, or revise one already sent under the same seq.

        The English channel emits the raw transcript immediately and may
        replace it a second later with a glossary-corrected version. Clients
        key on seq, so the line sharpens in place rather than repeating.
        """
        buffer = self.buffers[channel]
        entry = None
        for existing in buffer:
            if existing["seq"] == seq:
                existing["text"] = text
                existing["revised"] = True
                entry = existing
                break
        if entry is None:
            entry = {"seq": seq, "text": text, "at": time.time(),
                     "revised": False}
            buffer.append(entry)
        payload = dict(entry)
        for queue in list(self.subscribers[channel]):
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                # A phone that has fallen far behind is better served by the
                # replay buffer on its next reconnect than by a stalled feed.
                pass

    def subscribe(self, channel, reader=None):
        """Open a stream, retiring the one this reader already had.

        One stream per reader: a phone that switches language would
        otherwise hold both until the old handler noticed at its next
        keepalive write, up to fifteen seconds later, and the language
        nobody is reading any more would keep being paid for.
        """
        queue = asyncio.Queue(maxsize=200)
        self.subscribers[channel].add(queue)
        if reader:
            self.retire(reader)
            self.streams[reader] = queue
        return queue

    def retire(self, reader):
        """Wake this reader's previous stream so its handler returns."""
        previous = self.streams.pop(reader, None)
        if previous is None:
            return
        try:
            previous.put_nowait(LEAVING)
        except asyncio.QueueFull:
            # A queue this far behind belongs to a phone that stopped
            # reading long ago; the keepalive write will end it.
            pass

    def unsubscribe(self, channel, queue, reader=None, dropped=True):
        """Close a stream. dropped is False when the reader chose to leave.

        Grace exists for a phone that locks its screen, not for somebody
        who just picked a different language, so a deliberate departure
        leaves last_seen alone and the old language can stop at once.
        """
        self.subscribers[channel].discard(queue)
        if reader and self.streams.get(reader) is queue:
            del self.streams[reader]
        if dropped:
            self.last_seen[channel] = time.monotonic()

    def close(self):
        """End every open stream, so the handlers holding them return.

        A phone keeps its stream open for as long as the page is up, and
        AppRunner.cleanup waits on the connections a site still has, so one
        reader who never closes the tab keeps the whole process alive after
        the operator has pressed Ctrl-C.
        """
        for queues in self.subscribers.values():
            for queue in list(queues):
                try:
                    queue.put_nowait(LEAVING)
                except asyncio.QueueFull:
                    # Far enough behind that the handler is not reading;
                    # closing the socket under it ends that one instead.
                    pass
        self.streams.clear()

    def wanted(self, channel, grace):
        """True while somebody is reading this channel, or just was."""
        if self.subscribers[channel]:
            return True
        last = self.last_seen[channel]
        return last is not None and (time.monotonic() - last) < grace

    def listener_count(self):
        return {name: len(queues) for name, queues in self.subscribers.items()}

    def listener_total(self):
        return sum(len(queues) for queues in self.subscribers.values())

    def full(self):
        """True once the reader cap is reached, across all channels."""
        return (self.max_readers > 0
                and self.listener_total() >= self.max_readers)

    def clear(self):
        for buffer in self.buffers.values():
            buffer.clear()


class Session:
    """The capture pipeline, startable and stoppable at runtime."""

    def __init__(self, config, hub):
        self.config = config
        self.hub = hub
        self.state = "stopped"
        self.error = None
        self.device = ""
        self.started_at = None
        self.last_activity = None
        self.idle_task = None
        # language -> "on" | "off"; absent means follow demand
        self.overrides = {}
        self.task = None
        self.translator = None
        # Highest sequence number published so far, kept here rather than in
        # the segmenter because a reconnect builds a new segmenter and the
        # numbering has to run on across the whole meeting.
        self.last_seq = 0
        self.recorder = None
        # Which stream a line came from, and when that stream began.
        # audio_end is relative to the websocket, so both restart on every
        # reconnect and the timeline only reassembles with the run beside it.
        self.run = 0
        self.run_started = None
        self.stats = {"units": 0, "translated": 0, "failures": 0,
                      "timeouts": 0, "reconnects": 0, "corrections": 0,
                      "skipped": 0, "capped": 0, "latencies": []}

    # -- lifecycle ---------------------------------------------------------

    async def start(self, device):
        # Anything other than stopped means a supervisor task is alive.
        # Checking only for starting and running would let a second one spawn
        # while the first was in its reconnect backoff.
        if self.state != "stopped":
            return False, "Already running."
        # Whatever the operator picked, kept on the session rather than in
        # the config: the source is chosen fresh every meeting because the
        # name changes with a reboot or a replugged cable.
        self.device = device or ""
        if not self.device:
            return False, "Choose an audio source first."
        self.error = None
        self.stats = {"units": 0, "translated": 0, "failures": 0,
                      "timeouts": 0, "reconnects": 0, "corrections": 0,
                      "skipped": 0, "capped": 0, "latencies": []}
        self.hub.clear()
        self.last_seq = 0
        self.run = 0
        self.state = "starting"
        self.started_at = time.time()
        self.last_activity = time.monotonic()
        await self._open_recorder()
        self.translator = Translator(
            self.config["llm_base"], self.config["llm_key"],
            self.config["model"], self.config["languages"],
            self.config["glossary"], self.config["max_tokens"],
            self.config["timeout"], self.config["reasoning_effort"],
            self.config["correct_english"])
        self.task = asyncio.create_task(self._supervise())
        self.idle_task = asyncio.create_task(self._watch_idle())
        return True, "Starting."

    async def stop(self):
        if self.state == "stopped":
            return False, "Not running."
        self.state = "stopped"
        # The idle watchdog calls this, so it must not cancel itself and
        # deadlock waiting on its own completion.
        if self.idle_task and self.idle_task is not asyncio.current_task():
            self.idle_task.cancel()
        self.idle_task = None
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None
        if self.translator:
            await self.translator.close()
            self.translator = None
        if self.recorder:
            await self.recorder.close()
            self.recorder = None
        return True, "Stopped."

    async def _open_recorder(self):
        """Start recording, or carry on without it.

        A database that will not open is a note on the operator page, never
        a refusal to start. The meeting matters more than the record of it.
        """
        if not self.config.get("record"):
            return
        config = self.config
        header = (
            self.started_at, self.device, config["asr_model"],
            config["model"], json.dumps(config["languages"]),
            int(bool(config["correct_english"])),
            json.dumps({name: config[name] for name in
                        ("ceiling", "gap", "hold", "endpointing", "grace",
                         "timeout", "max_tokens", "reasoning_effort")}),
            # The glossary and keyterms verbatim, not a path and not a
            # digest. Both files are gitignored and both get edited between
            # meetings, so a path recorded here would not describe the text
            # that produced these lines.
            config["glossary"], json.dumps(config["keyterms"]),
            hashlib.sha256(SYSTEM_PROMPT.encode()).hexdigest()[:12],
        )
        recorder = record.Recorder(config.get("database") or "sessions.db")
        error = await recorder.open(header)
        if error:
            self.error = f"Not recording: {error}"
            return
        self.recorder = recorder

    def recording(self):
        """True while a session is being written to disk."""
        return self.recorder is not None and self.recorder.session_id

    def _record(self, method, **row):
        """Hand one row to the recorder, and never let it reach the subtitles.

        The Recorder is built not to raise, but the guard belongs here as
        well, because these calls sit in the loop that drains the Deepgram
        socket. A disk that has stopped answering costs the record of the
        meeting and must not cost the meeting.
        """
        if self.recorder is None:
            return
        try:
            getattr(self.recorder, method)(**row)
        except Exception as exc:
            self.error = f"Recording stopped: {type(exc).__name__}: {exc}"
            self.recorder = None

    def demand(self):
        """(translated, capped): languages wanted, split by max_languages.

        Demand needs no authentication, so one client opening every channel
        could otherwise make every sentence pay for the whole list. Ranking
        puts operator overrides first and the most-read languages next, so
        a stranger holding every channel loses to a language with readers.
        """
        grace = self.config["grace"]
        wanted = []
        for language in self.config["languages"]:
            override = self.overrides.get(language)
            if override == "off":
                continue
            if override == "on" or self.hub.wanted(language, grace):
                wanted.append(language)
        cap = self.config.get("max_languages") or 0
        if cap <= 0 or len(wanted) <= cap:
            return wanted, []
        order = self.config["languages"]
        ranked = sorted(wanted, key=lambda name: (
            self.overrides.get(name) != "on",      # forced on wins
            -len(self.hub.subscribers[name]),      # then most readers
            order.index(name)))                    # then config order
        keep = set(ranked[:cap])
        # Both lists stay in configured order, which the prompt and the
        # operator page both rely on.
        return ([name for name in wanted if name in keep],
                [name for name in wanted if name not in keep])

    def set_override(self, language, mode):
        if language not in self.config["languages"]:
            return False, "Unknown language."
        if mode == "auto":
            self.overrides.pop(language, None)
        elif mode in ("on", "off"):
            self.overrides[language] = mode
        else:
            return False, "Mode must be auto, on, or off."
        return True, f"{language} set to {mode}."

    def language_report(self):
        """The translated channels, which are the ones with a mode to set.

        English is not here. It is never a model call, so it has nothing
        the buttons on this list could switch; its reader count goes to
        the operator page as english_listeners instead.
        """
        translated, capped = self.demand()
        translated, capped = set(translated), set(capped)
        return [{
            "name": language,
            "listeners": len(self.hub.subscribers[language]),
            "override": self.overrides.get(language, "auto"),
            "active": language in translated,
            "capped": language in capped,
        } for language in self.config["languages"]]

    async def _watch_idle(self):
        """Stop a session that nobody is using.

        Recognition is billed by audio duration, and silence is audio. A
        session left running after a meeting would stream an empty room for
        days, which is the most likely way this project costs real money.
        """
        limit = self.config["idle_stop"] * 60
        if limit <= 0:
            return
        while True:
            await asyncio.sleep(15)
            if self.state == "stopped":
                return
            if time.monotonic() - self.last_activity >= limit:
                minutes = int(limit // 60) or 1
                self.error = (f"Stopped automatically after {minutes} "
                              f"minutes with nothing said. Press Start to "
                              f"begin again.")
                await self.stop()
                return

    def status(self):
        latencies = self.stats["latencies"]
        return {
            "state": self.state,
            "error": self.error,
            "device": self.device,
            "model": self.config["model"],
            "channels": self.hub.channels,
            "listeners": self.hub.listener_count(),
            "refused": self.hub.refused,
            "max_readers": self.hub.max_readers,
            "uptime": (time.time() - self.started_at
                       if self.started_at and self.state != "stopped" else 0),
            "units": self.stats["units"],
            "translated": self.stats["translated"],
            "failures": self.stats["failures"],
            "timeouts": self.stats["timeouts"],
            "reconnects": self.stats["reconnects"],
            "corrections": self.stats["corrections"],
            # The address on the card, token and all, because that is what
            # the operator hands somebody who cannot scan the code.
            "reader_url": self.config.get("reader_url", ""),
            "idle_stop": self.config["idle_stop"],
            "quiet_for": (time.monotonic() - self.last_activity
                          if self.last_activity and self.state != "stopped"
                          else 0),
            "skipped": self.stats["skipped"],
            "capped": self.stats["capped"],
            "max_languages": self.config.get("max_languages") or 0,
            # On the page, not just in a config file somebody edited six
            # weeks ago. The operator is the person who has to tell the room
            # a transcript is being kept.
            "recording": bool(self.recording()),
            "dropped": self.recorder.dropped if self.recorder else 0,
            "languages": self.language_report(),
            # Beside the total on the page: a room where everybody reads
            # English and a room where nobody does need different things
            # from the operator, and the total alone cannot tell them apart.
            "english_listeners": len(self.hub.subscribers["English"]),
            "median": (sorted(latencies)[len(latencies) // 2]
                       if latencies else None),
            "recent": list(self.hub.buffers["English"])[-6:],
        }

    # -- the pipeline ------------------------------------------------------

    async def _supervise(self):
        """Keep the capture running across dropped connections."""
        attempt = 0
        try:
            while True:
                began = time.monotonic()
                try:
                    await self._run_once()
                    # A clean return means the far end closed the stream.
                    self.error = "Audio stream ended."
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.error = f"{type(exc).__name__}: {exc}"

                if self.state == "stopped":
                    return
                if time.monotonic() - began >= HEALTHY_RUN:
                    attempt = 0
                delay = RECONNECT_BACKOFF[min(attempt,
                                              len(RECONNECT_BACKOFF) - 1)]
                attempt += 1
                self.stats["reconnects"] += 1
                self.state = "reconnecting"
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise

    async def _run_once(self):
        config = self.config
        source = await capture.open_capture(self.device,
                                            config["capture"])
        # Only the counter carries over. Reusing one segmenter would carry
        # the buffered fragments and audio_end too, and audio_end is relative
        # to the stream, so on the next stream the gap check would compare
        # against a larger number, never fire, and glue the sentence stranded
        # by the drop onto whatever the next person says.
        segmenter = Segmenter(config["ceiling"], config["gap"],
                              start_seq=self.last_seq)
        queue = asyncio.Queue()
        try:
            async with websockets.connect(
                build_asr_url(config, config["keyterms"]),
                additional_headers={
                    "Authorization": f"Token {config['deepgram_key']}"},
            ) as socket:
                self.state = "running"
                self.error = None
                self.run += 1
                self.run_started = time.monotonic()
                tasks = [
                    asyncio.create_task(pump_audio(source, socket)),
                    asyncio.create_task(
                        listen(socket, segmenter, self.translator, self,
                               queue)),
                    asyncio.create_task(publish(queue, self, config["hold"])),
                ]
                try:
                    # Whichever leg finishes first ends the session. gather
                    # would propagate its result but leave the other two
                    # running against a socket nobody is reading.
                    done, pending = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                for task in done:
                    # Surface a real failure to the supervisor rather than
                    # treating every exit as a clean end of stream.
                    if task.exception():
                        raise task.exception()
        finally:
            await source.close()

    # -- sink: what the shared pipeline does with finished text ------------

    def fragment(self, text):
        """Raw fragments are half sentences. Only whole ones reach a phone."""

    def unit(self, unit):
        """Publish the English line and say which outputs are worth buying."""
        self.stats["units"] += 1
        self.last_seq = max(self.last_seq, unit.seq)
        self.last_activity = time.monotonic()
        # The raw transcript goes out immediately either way.
        self.hub.publish("English", unit.seq, unit.text)

        languages, capped = self.demand()
        # Recording asks for the English correction whether or not anybody
        # is reading that channel. Without it, a Sunday where everyone reads
        # French stores no corrected line to compare against, and the two
        # things the record exists for, the keyterms worklist and the check
        # for an invented name, are both empty.
        wants_english = (self.config["correct_english"]
                         and (self.recording()
                              or self.hub.wanted("English",
                                                 self.config["grace"])))
        outputs = (["English"] if wants_english else []) + languages
        if not outputs:
            # Nobody is reading a translated channel and English needs no
            # correction, so this sentence costs no model tokens at all.
            self.stats["skipped"] += 1
        if self.recorder:
            # What a reader actually waited: time since this stream opened,
            # less how far into the stream the words were spoken.
            lag = None
            if self.run_started is not None:
                lag = ((time.monotonic() - self.run_started)
                       - unit.audio_end)
                # A negative value means the stream clock and this clock
                # disagree, which is not a measurement of anything a reader
                # experienced. Better absent than reported.
                lag = lag if lag >= 0 else None
            self._record(
                "line",
                seq=unit.seq, run=self.run, at=time.time(),
                audio_end=unit.audio_end, lag=lag,
                confidence=unit.confidence,
                heard=unit.text, reason=unit.reason,
                # Set here rather than left for a completion method: when
                # outputs is empty, publish sees task is None and skips, so
                # none of translated, timed_out or failed ever runs.
                outcome="skipped" if not outputs else "pending",
                requested=list(outputs))
        for language in capped:
            # English rather than a gap, as with any failed translation.
            self.stats["capped"] += 1
            self.hub.publish(language, unit.seq, unit.text)
            self._record("translation", seq=unit.seq, language=language,
                         text=unit.text, source="capped")
        return outputs

    def translated(self, unit, languages, translations, elapsed):
        self.stats["translated"] += 1
        self.stats["latencies"].append(round(elapsed, 2))
        revised = translations.get("English")
        if revised and revised != unit.text:
            self.stats["corrections"] += 1
            self.hub.publish("English", unit.seq, revised)
        if self.recorder:
            self._record("correction",
                         seq=unit.seq, english=revised or unit.text,
                         latency=elapsed, outcome="translated")
            for language in languages:
                text = translations.get(language)
                self._record(
                    "translation",
                    seq=unit.seq, language=language, text=text or unit.text,
                    # An empty answer is not a failure anywhere else, so
                    # this is the only place it can be told apart from a
                    # translation the model actually produced.
                    source="model" if text else "empty")
        for language in languages:
            # parse_translations fills every requested language, empty
            # where the model dropped one, so a get() default never fires
            # and the empty string is what has to trigger the fallback.
            self.hub.publish(language, unit.seq,
                             translations.get(language) or unit.text)

    def timed_out(self, unit, languages):
        self.stats["timeouts"] += 1
        self._fall_back_to_english(unit, languages, "timeout")

    def failed(self, unit, languages, error):
        self.stats["failures"] += 1
        self._fall_back_to_english(unit, languages, "failure")

    def _fall_back_to_english(self, unit, languages, source):
        """Show the English text rather than let a channel stall.

        A reader of a translated channel cannot hear the room, so a line they
        cannot read beats a gap they cannot explain.
        """
        for language in languages:
            self.hub.publish(language, unit.seq, unit.text)
        if self.recorder:
            # Recorded as what the reader saw, not as an absence. A missing
            # row would be indistinguishable from a language nobody had
            # open, and the difference is the whole point of the table.
            self._record("correction", seq=unit.seq, english=None,
                         latency=None, outcome=source)
            for language in languages:
                self._record("translation", seq=unit.seq, language=language,
                             text=unit.text, source=source)


# -- web layer -------------------------------------------------------------


def authorized(request):
    """True if this request carries the token its listener was given.

    One rule, two tokens: each application holds its own under app["token"],
    so a reader link opens nothing on the operator port and the reverse.
    No tokenless mode on either. The reader port is on the public internet
    through the tunnel, and on the operator port loopback alone would still
    let a page in another browser tab post a cross-origin form at these
    routes.
    """
    token = request.app["token"]
    supplied = (request.headers.get("X-Subtitle-Token")
                or request.query.get("token") or "")
    # Bytes, because compare_digest on two str raises TypeError outside
    # ASCII and a query string can carry that.
    return hmac.compare_digest(supplied.encode(), token.encode())


async def page(request, filename):
    return web.FileResponse(STATIC / filename)


async def nothing_here(request):
    """The bare root of the reader port, which is what a bot reaches.

    A tunnel puts this address on the public internet, so the front door
    says only that there is no page here. The meeting is at /read, behind
    the token on the card the room was handed.
    """
    return web.Response(status=404, text="Not found.")


async def reader_page(request):
    if not authorized(request):
        # A reader who mistyped a link or kept last week's, told what to do
        # about it. It names nothing a visitor without the token could use.
        return web.Response(
            status=403,
            text="This link is not for this meeting. Scan the QR code or "
                 "use the link for today.")
    return await page(request, "reader.html")


async def operator_page(request):
    if not authorized(request):
        return web.Response(status=403, text="Add ?token=... to this address.")
    return await page(request, "operator.html")


async def api_status(request):
    """Everything the operator page shows, which is more than a reader sees.

    Behind the token: it names the audio device and the model and carries
    raw exception text.
    """
    if not authorized(request):
        return web.json_response({"ok": False, "message": "Not authorized."},
                                 status=403)
    return web.json_response(request.app["session"].status())


async def api_devices(request):
    """Input devices, so the operator picks from a list rather than typing."""
    if not authorized(request):
        # The devices-and-error shape this route already returns, not the
        # ok-and-message shape: the page destructures the list and would
        # throw on a missing one.
        return web.json_response({"devices": [], "error": "Not authorized."},
                                 status=403)
    config = request.app["session"].config
    try:
        # In a thread: list_devices shells out to pactl with a five second
        # timeout, and this loop is also feeding the subtitles.
        devices = await asyncio.to_thread(capture.list_devices,
                                          config["capture"])
    except capture.CaptureError as exc:
        return web.json_response({"devices": [], "error": str(exc)})
    # Which one to pre-select, decided here rather than on the page: the
    # "default" flag is whatever the backend considers default and parec
    # marks nothing at all, and the page sorts the list by name anyway, so
    # it no longer knows the order the backend offered them in.
    return web.json_response({"devices": devices,
                              "suggested": capture.choose_default(devices)})


async def read_body(request):
    """The posted JSON object, or an empty one.

    A malformed or absent body is not a server error. The operator page
    renders the {"ok": false} shape and renders a 500 traceback as nothing.
    """
    if not request.can_read_body:
        return {}
    try:
        body = await request.json()
    except (ValueError, json.JSONDecodeError):
        return {}
    return body if isinstance(body, dict) else {}


async def api_start(request):
    if not authorized(request):
        return web.json_response({"ok": False, "message": "Not authorized."},
                                 status=403)
    body = await read_body(request)
    ok, message = await request.app["session"].start(body.get("device"))
    return web.json_response({"ok": ok, "message": message})


async def api_stop(request):
    if not authorized(request):
        return web.json_response({"ok": False, "message": "Not authorized."},
                                 status=403)
    ok, message = await request.app["session"].stop()
    return web.json_response({"ok": ok, "message": message})


async def api_language(request):
    if not authorized(request):
        return web.json_response({"ok": False, "message": "Not authorized."},
                                 status=403)
    body = await read_body(request)
    ok, message = request.app["session"].set_override(
        body.get("language"), body.get("mode"))
    return web.json_response({"ok": ok, "message": message})


async def stream(request):
    """Server-sent events for one language channel."""
    if not authorized(request):
        # First, before the channel is even looked up: opening a stream is
        # what makes a language be translated, and this is the route that
        # spends money on behalf of whoever calls it.
        return web.Response(status=403, text="Not authorized.")
    hub = request.app["hub"]
    channel = request.match_info["channel"]
    if channel not in hub.channels:
        return web.Response(status=404, text="No such channel.")
    if hub.full():
        # Before prepare, so this is an ordinary response a browser retries
        # rather than a stream that opens and then says nothing.
        hub.refused += 1
        return web.Response(status=503, text="Too many readers just now.",
                            headers={"Retry-After": "10"})

    response = web.StreamResponse(headers={
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    })
    await response.prepare(request)
    await response.write(b"retry: 3000\n\n")

    # Unguessable and made fresh by each page load, so it identifies one
    # phone's stream without naming anybody. Absent is fine: the stream
    # then simply cannot be retired early.
    reader = request.query.get("reader", "")
    queue = hub.subscribe(channel, reader)
    dropped = True
    try:
        # Replay recent history so someone arriving late has context. The
        # client dedupes on seq, so overlap with the live feed is harmless.
        for entry in list(hub.buffers[channel]):
            await response.write(
                f"data: {json.dumps(entry)}\n\n".encode())
        while True:
            try:
                entry = await asyncio.wait_for(queue.get(), timeout=15)
                if entry is LEAVING:
                    # This reader opened another channel. Chosen, not lost,
                    # so the channel being left gets no grace period.
                    dropped = False
                    break
                await response.write(
                    f"data: {json.dumps(entry)}\n\n".encode())
            except TimeoutError:
                # Phones and proxies drop idle connections; this keeps the
                # socket warm through long silences.
                await response.write(b": keepalive\n\n")
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        hub.unsubscribe(channel, queue, reader, dropped)
    return response


async def qr_code(request):
    """QR for the reader address, rendered on demand as SVG.

    Drawn from public_url rather than the bind address, because the address
    this server listens on is not the one a phone can reach, and carrying
    the reader token, because without it that address answers nothing.

    Behind the operator token like the rest of this listener, now that the
    image encodes the reader token: a page in another tab of the operator's
    browser can point an <img> at a loopback port without asking anybody.
    """
    if not authorized(request):
        return web.Response(status=403, text="Not authorized.")
    url = request.app["session"].config.get("reader_url")
    if not url:
        return web.Response(status=404, text="No public_url configured.")
    try:
        import segno
    except ImportError:
        return web.Response(status=501, text="pip install segno")

    buffer = io.BytesIO()
    # Medium error correction, which survives a printed card getting scuffed.
    segno.make(url, error="m").save(
        buffer, kind="svg", scale=8, border=2, dark="#16181d", light="#ffffff")
    return web.Response(body=buffer.getvalue(),
                        content_type="image/svg+xml",
                        headers={"Cache-Control": "max-age=600"})


async def api_channels(request):
    if not authorized(request):
        # The channels-and-error shape the reader page destructures, so a
        # refusal leaves it with an empty picker rather than a thrown script.
        return web.json_response({"channels": [], "error": "Not authorized."},
                                 status=403)
    return web.json_response({"channels": request.app["hub"].channels})


def build_reader_app(hub, session, token):
    """What a phone needs, and nothing else.

    A tunnel points here, so this listener is on the public internet. No
    route can change anything, and every route that answers with a subtitle
    or with what the channels are is behind the reader token, so a meeting
    is read by the room it was handed to rather than by whoever finds the
    address. The bare / is the one exception, and it is a 404.
    """
    app = web.Application()
    app["hub"] = hub
    app["session"] = session
    app["token"] = token
    app.add_routes([
        web.get("/", nothing_here),
        web.get("/read", reader_page),
        web.get("/api/channels", api_channels),
        web.get("/stream/{channel}", stream),
    ])
    return app


def build_operator_app(hub, session, token):
    """The controls, on a listener the tunnel never sees.

    A separate listener rather than a check in the handlers: the tunnel
    daemon connects from this machine, so a public visitor and the
    operator both arrive from 127.0.0.1 and no check can tell them apart.
    A token of its own, not the reader's: the link the whole room is given
    must not be the link that can press Start.
    """
    app = web.Application()
    app["hub"] = hub
    app["session"] = session
    app["token"] = token
    app.add_routes([
        web.get("/operator", operator_page),
        web.get("/api/status", api_status),
        web.get("/api/devices", api_devices),
        web.post("/api/start", api_start),
        web.post("/api/stop", api_stop),
        web.post("/api/language", api_language),
        web.get("/qr.svg", qr_code),
    ])
    return app


def mint_token(pinned=""):
    """A token for one of the two addresses.

    Minted for the run unless [keys] pins one, so a link that leaks expires
    when the server does. The overrides exist for development, where a new
    address every restart is a new link to click every restart, and for a
    room that prints its reader card once and wants it to keep working; a
    pinned operator token should be left empty for a meeting.
    """
    return pinned or secrets.token_urlsafe(32)


def write_pid_file(settings):
    """Record this process, once it is really listening.

    Written after the ports are bound, so a second server that fails on
    "address already in use" cannot overwrite the record of the one that
    holds them. The ports are in the file as well as the pid, which is how
    stopping works without reading config.toml, and how a recycled pid is
    told from a server that is still serving.

    A directory that will not take the file costs the operator a convenient
    stop, not the meeting, so the failure is reported and swallowed.
    """
    record = {"pid": os.getpid(), "port": settings["port"],
              "operator_port": settings["operator_port"],
              "started_at": time.time()}
    try:
        PID_FILE.write_text(json.dumps(record) + "\n", encoding="utf-8")
    except OSError as failure:
        print(f"Could not write {PID_FILE}, so ./transept stop will not "
              f"find this server: {failure}")
        return False
    return True


def remove_pid_file():
    """Take the record away again, but only if it is still ours.

    A server that started after this one owns the file by then, and
    deleting that record would leave the running server unstoppable.
    """
    try:
        if json.loads(PID_FILE.read_text(encoding="utf-8"))["pid"] \
                == os.getpid():
            PID_FILE.unlink()
    except (OSError, ValueError, KeyError, TypeError):
        pass


def reader_address(base, token):
    """The whole address a phone opens: a reachable base, path, and token.

    Built here rather than typed into config.toml, because the token half
    of it changes every run unless reader_token pins one. An empty base
    means no public address is configured yet, and stays empty rather than
    becoming a link to nowhere.
    """
    return f"{base.rstrip('/')}/read?token={token}" if base else ""


async def serve(config, tokens, settings):
    """Run both listeners until Ctrl-C, then stop the session.

    AppRunner rather than web.run_app, which takes a single application.
    The two apps share one Hub and one Session: two doors, one room, and a
    key cut for each door.
    """
    hub = Hub(config["languages"], config.get("max_readers") or 0)
    session = Session(config, hub)
    listeners = (
        (build_reader_app(hub, session, tokens["reader"]),
         settings["host"], settings["port"]),
        (build_operator_app(hub, session, tokens["operator"]), OPERATOR_HOST,
         settings["operator_port"]),
    )
    runners = []
    recorded = False
    try:
        for app, host, port in listeners:
            runner = web.AppRunner(app)
            await runner.setup()
            runners.append(runner)
            await web.TCPSite(runner, host, port).start()
        recorded = write_pid_file(settings)
        stop = asyncio.Event()
        install_stop_handler(stop)
        await stop.wait()
    finally:
        if recorded:
            remove_pid_file()
        # Before the runners: stopping the session closes the Deepgram
        # socket and the translator, which needs the loop still running.
        await session.stop()
        # Then let go of the phones, for the reason Hub.close explains.
        hub.close()
        for runner in runners:
            await runner.cleanup()


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Any setting may live in config.toml instead, which is the "
               "point: a weekly run should be just `python3 server.py`.")
    parser.add_argument("--list-devices", action="store_true",
                        help="list audio input devices and exit")
    parser.add_argument("--config", default="config.toml")
    add_settings_arguments(parser, [
        "capture",
        "asr_model", "endpointing",
        "ceiling", "gap",
        "model", "reasoning_effort", "correct_english", "max_tokens",
        "timeout", "hold",
        "languages", "grace", "max_languages",
        "glossary", "keyterms",
        "idle_stop", "record", "database",
        "host", "port", "operator_port", "max_readers",
    ])
    args = parser.parse_args()

    if args.list_devices:
        capture.print_devices(args.capture or "auto")
        return

    parsed = load_config(args.config)
    settings = resolve(args, parsed)
    keys = load_keys(parsed)
    if not keys["deepgram_key"] or not keys["llm_key"] or not keys["llm_base"]:
        sys.exit("Set deepgram_api_key, llm_api_key, and llm_base_url under "
                 "[keys] in config.toml")
    if not settings["model"]:
        sys.exit("No translation model. Set translation.model in config.toml "
                 "or pass --model.")

    config = dict(settings)
    config["keyterms"] = load_file_lines(settings["keyterms"])
    config["glossary"] = load_glossary(settings["glossary"])
    config["deepgram_key"] = keys["deepgram_key"]
    config["llm_key"] = keys["llm_key"]
    config["llm_base"] = keys["llm_base"]

    tokens = {"reader": mint_token(keys["reader_token"]),
              "operator": mint_token(keys["operator_token"])}
    # What the QR code and the operator page show, which is the address on
    # the card rather than the one this process binds. Empty until a tunnel
    # is configured, and the banner falls back to the local address so that
    # a first run on one machine still has something to open.
    config["reader_url"] = reader_address(settings["public_url"],
                                          tokens["reader"])
    print("Reader:   " + (config["reader_url"] or reader_address(
        f"http://{settings['host']}:{settings['port']}", tokens["reader"])))
    if keys["reader_token"]:
        print("That address carries reader_token from config.toml, so a "
              "printed card\nkeeps working. Clear it to have one minted per "
              "run instead.")
    else:
        print("That address carries a token minted for this run, so any "
              "card or link\nfrom a previous run stops working. Nothing "
              "answers without it.")
    print(f"Operator: http://{OPERATOR_HOST}:{settings['operator_port']}"
          f"/operator?token={tokens['operator']}")
    if keys["operator_token"]:
        print("That address carries operator_token from config.toml, so it "
              "is the same\nevery restart. Clear it to have one minted per "
              "run.")
    else:
        print("That address carries a token minted for this run, so it "
              "changes\nevery restart. Copy it rather than saving a bookmark.")
    if settings["host"] in ("127.0.0.1", "localhost", "::1"):
        print("Bound to this machine only. Readers reach it through your "
              "tunnel;\nset server.host to 0.0.0.0 to allow direct "
              "connections on this network.")
    try:
        asyncio.run(serve(config, tokens, settings))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
