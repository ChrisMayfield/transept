#!/usr/bin/env python3
"""
Live subtitling and translation server.

Runs the capture pipeline behind an HTTP server, so a volunteer can start and
stop it from a browser and listeners can read subtitles on their phones.

Two listeners. The reader port carries the pages phones use, and is the
one to point a tunnel at; the operator port stays on 127.0.0.1.

    /reader             reader view with a language picker
    /stream/<channel>   server-sent events for one language
    /operator           start and stop controls, status, language toggles

With [audio] backend set to "remote" the audio arrives over a websocket
from a sender on a laptop in the room rather than from a sound card here,
and the second listener is the control socket that sender connects to,
bound where it can reach it. The operator page then belongs to the sender.
HOSTED.md describes that deployment; everything below is the same either
way, because only where the audio comes from has changed.

    /control            one sender: audio up, controls and status down

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
import base64
import hashlib
import json
import os
import sys
import time
from collections import deque
from pathlib import Path

try:
    import websockets
    from aiohttp import web
except ImportError:
    sys.exit("Missing dependencies: pip install -r requirements.txt")

import capture
import record
from controls import (OPERATOR_HOST, authorized, build_operator_app,
                      mint_token, page)
from pipeline import (SYSTEM_PROMPT, Segmenter, Translator,
                      add_settings_arguments,
                      build_asr_url, install_stop_handler, listen,
                      load_config, load_file_lines, load_glossary,
                      load_keys, open_encoder, publish, pump_audio,
                      resolve)

# Where this process records itself so the transept script can stop it
# later. In the working directory rather than beside the code, because that
# is where config.toml, the glossary, and sessions.db already live, and "a
# server running out of this directory" is the thing being identified.
PID_FILE = Path("transept.pid")
HISTORY = 60
# How long a reader's stream waits for a line before it looks at the session
# state again, and how long it may stay silent before a keepalive is written.
# The state is polled rather than pushed because a stream handler is the only
# thing that knows what it last told its phone, and a reader who opened the
# page before the meeting began has to be told when it starts without waiting
# out a keepalive.
STATE_POLL = 2
KEEPALIVE = 15
RECONNECT_BACKOFF = [1, 2, 5, 10, 20]
# Seconds a run must last to count as healthy and reset the backoff. Without
# it, a few blips early in a meeting make a later one cost twenty seconds.
HEALTHY_RUN = 60
# Seconds a control socket may stay silent before it is pinged, how long a
# sender has to say hello once connected, and how often the status the
# sender's page renders is pushed down to it. The hello is bounded because
# a socket that connects and says nothing would otherwise hold the room's
# one slot against the sender that means to use it.
HEARTBEAT = 30
HELLO_TIMEOUT = 10
STATUS_INTERVAL = 1.0
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

    def __init__(self, config, hub, source=None):
        self.config = config
        self.hub = hub
        # How a run gets its audio, when it is not a device on this
        # machine: an async callable returning an open capture object,
        # which is what a ControlRoom hands over for a remote sender.
        self.source = source
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
            # The setting rather than the name of the directory this runs
            # in, although the two usually match. A WorkingDirectory edited
            # in a unit file, or a run started from somewhere else, would
            # otherwise silently relabel a room's recorded meetings.
            config.get("room", ""),
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

    async def _open_source(self):
        """The audio for one run: a local device, or the room's sender."""
        if self.source is not None:
            return await self.source()
        source = await capture.open_capture(self.device,
                                            self.config["capture"])
        # Compressed on the way out, because the leg that fails in a real
        # building is audio leaving it. A machine with no encoder says so
        # here rather than on the operator page, which has a meeting to
        # report on and nothing to do about ffmpeg.
        source, note = await open_encoder(source, self.config["encoding"])
        if note:
            print(note)
        return source

    async def _run_once(self):
        config = self.config
        source = await self._open_source()
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
                # The source says what it yields, and the URL has to
                # declare that rather than what a local device would have
                # produced, because a sender may be sending encoded audio.
                build_asr_url(config, config["keyterms"], source.encoding),
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


def decode_headers(value):
    """A hello's stream pages as bytes, or none at all.

    Anything unreadable is treated as absent rather than raised on. What a
    bad value costs is caught a line later, where an Opus sender that
    declared nothing is refused with a sentence the operator can act on.
    """
    if not isinstance(value, str) or not value:
        return b""
    try:
        return base64.b64decode(value, validate=True)
    except ValueError:
        return b""


class ControlRoom:
    """The one sender for this room: its socket, its audio, and its grace.

    One sender at a time, because two laptops feeding one session would
    interleave two rooms into one transcript. The socket may come and go:
    a hotspot blinking is a gap in the audio rather than the end of a
    meeting, so a session outlives its socket by control_grace seconds and
    only then does the server decide the sender has gone home.
    """

    def __init__(self, config, hub):
        self.config = config
        self.hub = hub
        # The session reads whatever this room hands it, rather than
        # opening a sound card the rented machine does not have.
        self.session = Session(config, hub, source=self.open_source)
        self.socket = None
        self.source = None
        self.encoding = "pcm"
        # What the sender's hello said its Opus stream is. Empty for PCM,
        # which any run can read from wherever it joins.
        self.headers = b""
        self.watch = None

    async def open_source(self):
        """A fresh source for one recognizer run.

        Per run rather than per sender, because a run that ends closes its
        source, and audio that arrives between runs has no socket to go up.
        Dropping it is the whole of the right behavior for audio.

        The stream's own first pages are the exception, because they are
        not audio. Every run here joins an Opus stream already in progress,
        the first one a second or two after the encoder started and a later
        one an hour in, and a recognizer given such a stream with nothing
        in front of it holds the socket open and returns nothing at all.
        """
        self.source = capture.RemoteCapture(self.config["control_grace"],
                                            self.encoding)
        if self.headers:
            self.source.feed(self.headers)
        return self.source

    def feed(self, chunk):
        """Audio from the socket, dropped while no run is reading."""
        if self.source is not None:
            self.source.feed(chunk)

    def attach(self, socket, hello):
        """Give this sender the room, or say why not."""
        name = str(hello.get("room") or "")
        mine = self.config.get("room") or ""
        if mine and name != mine:
            return False, (f"This server serves {mine}, "
                           f"not {name or 'an unnamed room'}.")
        encoding = str(hello.get("encoding") or "pcm")
        if encoding not in ("pcm", "opus"):
            return False, f"Unknown encoding {encoding!r}."
        headers = decode_headers(hello.get("headers"))
        if encoding == "opus" and hello.get("intent") == "start" \
                and not headers:
            # Refused rather than started, because the alternative is a
            # meeting that runs for an hour with every light green and not
            # one subtitle, which is the one failure nobody in the room can
            # tell from a quiet speaker.
            return False, ("This sender's Opus stream did not say what it "
                           "is. Restart the sender, or set [audio] encoding "
                           "to pcm on the laptop.")
        if self.socket is not None:
            return False, "Another sender is connected to this room."
        running = self.session.state != "stopped"
        if running and hello.get("intent") == "start":
            # A sender that reconnects to a meeting already in progress
            # means to rejoin it. Starting would be a second laptop taking
            # a running session over, which is worth refusing out loud.
            return False, ("This room is already running. Attach to rejoin "
                           "it, or stop it first.")
        if running and encoding != self.encoding:
            # The recognizer was told the format when its socket opened and
            # has no way to be told another, so a sender that has since
            # fallen back has to rejoin as what this session is carrying.
            return False, (f"This room is running on {self.encoding}. "
                           f"Reconnect with the same encoding.")
        if headers or encoding != self.encoding:
            # Kept when a hello carries none and nothing about the stream
            # changed, which is a sender rejoining a room it is not feeding
            # yet. Replaced outright when the encoding changed, since pages
            # belonging to the format this room was carrying before are
            # worse than none at all.
            self.headers = headers
        self.socket = socket
        self.encoding = encoding
        if self.watch is not None:
            self.watch.cancel()
            self.watch = None
        return True, "Attached."

    async def release(self, socket):
        """This sender's socket closed; hold the room open for it briefly."""
        if socket is not self.socket:
            return
        self.socket = None
        if self.session.state == "stopped":
            return
        self.watch = asyncio.create_task(self._watch_gone())

    async def _watch_gone(self):
        """Stop a session whose sender did not come back.

        The audio stops the moment the socket does, and the recognizer
        socket is held open with KeepAlives meanwhile, so a blip costs a
        gap in the transcript rather than the meeting. What must not
        survive is a session left running on a sender that went home: it
        would hold the room against the next sender and go on paying for a
        stream of nothing.
        """
        try:
            await asyncio.sleep(self.config["control_grace"])
        except asyncio.CancelledError:
            return
        if self.socket is not None or self.session.state == "stopped":
            return
        self.session.error = ("The sender disconnected and did not come "
                              "back.")
        await self.session.stop()


# -- web layer -------------------------------------------------------------
#
# The reader half only. The operator page and its routes live in
# controls.py, because sender.py serves the same page beside the
# microphone when the audio comes from a laptop in another building.


async def nothing_here(request):
    """The bare root of the reader port, which is what a bot reaches.

    A tunnel puts this address on the public internet, so the front door
    says only that there is no page here. The meeting is at /reader, behind
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
    session = request.app["session"]
    dropped = True
    # What this stream last told the phone about the session, and how long
    # it has been since anything at all was written. A named event rather
    # than another subtitle, because the page reads subtitles as text to
    # show and a state word is not something anybody said.
    state = None
    quiet = 0.0
    try:
        # Replay recent history so someone arriving late has context. The
        # client dedupes on seq, so overlap with the live feed is harmless.
        for entry in list(hub.buffers[channel]):
            await response.write(
                f"data: {json.dumps(entry)}\n\n".encode())
        while True:
            if session.state != state:
                state = session.state
                await response.write(
                    f"event: state\ndata: {json.dumps({'state': state})}"
                    f"\n\n".encode())
                quiet = 0.0
            try:
                entry = await asyncio.wait_for(queue.get(),
                                               timeout=STATE_POLL)
                if entry is LEAVING:
                    # This reader opened another channel. Chosen, not lost,
                    # so the channel being left gets no grace period.
                    dropped = False
                    break
                await response.write(
                    f"data: {json.dumps(entry)}\n\n".encode())
                quiet = 0.0
            except TimeoutError:
                # Phones and proxies drop idle connections; this keeps the
                # socket warm through long silences. Counted rather than
                # timed out on directly, because the wait above is now short
                # enough to notice a session starting.
                quiet += STATE_POLL
                if quiet >= KEEPALIVE:
                    await response.write(b": keepalive\n\n")
                    quiet = 0.0
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        hub.unsubscribe(channel, queue, reader, dropped)
    return response


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
        web.get("/reader", reader_page),
        web.get("/api/channels", api_channels),
        web.get("/stream/{channel}", stream),
    ])
    return app


# -- the control socket, for a sender in another building --------------------


async def first_message(socket):
    """The sender's hello, or None if what arrived was not one."""
    try:
        frame = await socket.receive(timeout=HELLO_TIMEOUT)
    except TimeoutError:
        return None
    if frame.type is not web.WSMsgType.TEXT:
        return None
    try:
        hello = json.loads(frame.data)
    except ValueError:
        return None
    if not isinstance(hello, dict) or hello.get("type") != "hello":
        return None
    return hello


async def send_error(socket, message):
    """Say what went wrong, if the socket is still there to hear it."""
    try:
        await socket.send_json({"type": "error", "message": message})
    except (ConnectionResetError, RuntimeError):
        pass


async def push_status(socket, session):
    """Keep the sender's copy of the operator page fed.

    Session.status() unchanged rather than a shape of its own: the page
    the sender serves is the page this server serves today, and it has to
    go on rendering what it renders now.
    """
    try:
        while True:
            await socket.send_json({"type": "status",
                                    "status": session.status()})
            await asyncio.sleep(STATUS_INTERVAL)
    except (ConnectionResetError, RuntimeError):
        pass


async def handle_message(room, socket, data):
    """One text frame from the sender: stop, or a language override.

    Start is not here. It is the intent on the hello, because a sender
    that reconnects has to say whether it means to begin a session or to
    rejoin the one it was already feeding.

    Only failures are answered. What happened reaches the sender a moment
    later in the status it is already being pushed, and a second path
    saying the same thing is a second path that can disagree.
    """
    try:
        message = json.loads(data)
    except ValueError:
        message = None
    if not isinstance(message, dict):
        await send_error(socket, "Not a JSON object.")
        return
    kind = message.get("type")
    if kind == "stop":
        ok, text = await room.session.stop()
    elif kind == "language":
        ok, text = room.session.set_override(message.get("name"),
                                             message.get("mode"))
    else:
        ok, text = False, f"Unknown message {kind!r}."
    if not ok:
        await send_error(socket, text)


async def control(request):
    """The sender's socket: this room's audio, and its controls.

    Binary frames are audio and nothing else. Text frames are JSON, and
    the first of them has to be a hello naming the room, the encoding, and
    whether this sender means to start a session or rejoin one.
    """
    if not authorized(request):
        return web.Response(status=403, text="Not authorized.")
    if "Origin" in request.headers:
        # Any Origin at all, rather than a list of allowed ones. Websockets
        # are not subject to the same-origin policy, so a page on any site
        # the operator visits can open one to any host with no CORS
        # preflight standing in the way. The only legitimate client here is
        # a Python program, which sends no Origin, while a browser stamps
        # every handshake with its own and cannot be made not to. So a page
        # that somehow learned the control token still cannot use it.
        return web.Response(status=403, text="Not a route for a browser.")

    room = request.app["room"]
    socket = web.WebSocketResponse(heartbeat=HEARTBEAT)
    await socket.prepare(request)
    hello = await first_message(socket)
    if hello is None:
        await send_error(socket, "The first message has to be a hello.")
        await socket.close()
        return socket
    taken, message = room.attach(socket, hello)
    if not taken:
        await send_error(socket, message)
        await socket.close()
        return socket

    await socket.send_json({
        "type": "ready",
        "room": room.config.get("room", ""),
        # The address on the card, which this server knows and the sender
        # does not: it holds no reader token and no public_url.
        "reader_url": room.config.get("reader_url", ""),
        "languages": room.config["languages"],
        "state": room.session.state,
    })
    if hello.get("intent") == "start":
        started, message = await room.session.start(hello.get("device"))
        if not started:
            await send_error(socket, message)

    pushing = asyncio.create_task(push_status(socket, room.session))
    try:
        async for frame in socket:
            if frame.type is web.WSMsgType.BINARY:
                room.feed(frame.data)
            elif frame.type is web.WSMsgType.TEXT:
                await handle_message(room, socket, frame.data)
    finally:
        pushing.cancel()
        # Not the session: a drop is usually a blip, and the room decides
        # how long to wait for this sender before giving up on it.
        await room.release(socket)
    return socket


def build_control_app(room, token):
    """One route, for the one program allowed to reach it.

    This listener has to bind publicly, because the sender is in another
    building, so what guards it is the control token and the refusal of
    any handshake a browser would send, rather than the address it binds
    to. It serves no page and nothing a reader link could reach, which is
    what keeps a publicly bound control port a small thing to guard.
    """
    app = web.Application()
    app["room"] = room
    app["hub"] = room.hub
    app["session"] = room.session
    app["token"] = token
    app.add_routes([web.get("/control", control)])
    return app


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
    return f"{base.rstrip('/')}/reader?token={token}" if base else ""


async def serve(config, tokens, settings):
    """Run both listeners until Ctrl-C, then stop the session.

    AppRunner rather than web.run_app, which takes a single application.
    The reader listener is always the same one. Which listener joins it
    follows from where the audio comes from, and from nothing else: a
    local device means the operator page on loopback, and a sender means
    the control socket, bound where that sender can reach it. Both apps
    share one Hub and one Session: two doors, one room, and a key cut for
    each door.
    """
    hub = Hub(config["languages"], config.get("max_readers") or 0)
    if config["capture"] == "remote":
        room = ControlRoom(config, hub)
        session = room.session
        second = (build_control_app(room, tokens["control"]),
                  settings["host"], settings["control_port"])
    else:
        session = Session(config, hub)
        second = (build_operator_app(session, tokens["operator"]),
                  OPERATOR_HOST, settings["operator_port"])
    listeners = (
        (build_reader_app(hub, session, tokens["reader"]),
         settings["host"], settings["port"]),
        second,
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
        "capture", "encoding",
        "asr_model", "endpointing",
        "ceiling", "gap",
        "model", "reasoning_effort", "correct_english", "max_tokens",
        "timeout", "hold",
        "languages", "grace", "max_languages",
        "glossary", "keyterms",
        "idle_stop", "record", "database",
        "host", "port", "operator_port", "control_port", "max_readers",
        "room",
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
              "operator": mint_token(keys["operator_token"]),
              "control": mint_token(keys["control_token"])}
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
    if settings["capture"] == "remote":
        # No operator page here at all: the audio comes from a sender, and
        # the page that starts and stops a meeting belongs beside the
        # microphone, on the laptop in the room.
        print(f"Control:  ws://{settings['host']}:{settings['control_port']}"
              f"/control?token={tokens['control']}")
        if keys["control_token"]:
            print("That address carries control_token from config.toml, "
                  "which is what the\nsender is configured with.")
        else:
            print("No control_token is set, so that token was minted for "
                  "this run and the\nsender has to be given it again. Pin "
                  "one under [keys] for a real room.")
    else:
        print(f"Operator: http://{OPERATOR_HOST}:{settings['operator_port']}"
              f"/operator?token={tokens['operator']}")
        if keys["operator_token"]:
            print("That address carries operator_token from config.toml, so "
                  "it is the same\nevery restart. Clear it to have one "
                  "minted per run.")
        else:
            print("That address carries a token minted for this run, so it "
                  "changes\nevery restart. Copy it rather than saving a "
                  "bookmark.")
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
