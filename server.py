#!/usr/bin/env python3
"""
Live captioning and translation server.

Runs the capture pipeline behind an HTTP server, so a volunteer can start and
stop it from a browser and listeners can read captions on their phones.

    /                   reader view with a language picker
    /operator           start and stop controls, status, language toggles
    /stream/<channel>   server-sent events for one language

Speech recognition runs continuously while a session is on. Translation runs
per language, only while somebody is reading that language or the operator
has forced it on, so an unread language costs nothing.

Usage:
    python3 server.py --model gemini-3.8-flash --reasoning-effort low \\
        --languages "French,Swahili,Spanish" --correct-english \\
        --glossary glossary.txt --keyterms keyterms.txt --token <secret>

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
                      load_config, load_env,
                      load_file_lines, load_glossary, publish, pump_audio,
                      resolve)

STATIC = Path(__file__).parent / "static"
# The operator listener never leaves this machine. Not a setting, because
# the whole point of the second listener is that a tunnel cannot be
# pointed at it by mistake.
OPERATOR_HOST = "127.0.0.1"
HISTORY = 60
RECONNECT_BACKOFF = [1, 2, 5, 10, 20]
# Seconds a run must last to count as healthy and reset the backoff. Without
# it, a few blips early in a meeting make a later one cost twenty seconds.
HEALTHY_RUN = 60


class Hub:
    """Ring buffer plus live subscribers, one channel per language."""

    def __init__(self, languages, max_listeners=0):
        self.channels = ["English"] + list(languages)
        # Nothing authenticates to open an event stream, so the count is
        # bounded here rather than left to whatever the machine will bear.
        self.max_listeners = max_listeners
        self.refused = 0
        self.buffers = {name: deque(maxlen=HISTORY) for name in self.channels}
        self.subscribers = {name: set() for name in self.channels}
        # When a channel last had somebody on it, so a language does not shut
        # off the instant a phone drops and reconnects.
        self.last_seen = dict.fromkeys(self.channels)

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

    def subscribe(self, channel):
        queue = asyncio.Queue(maxsize=200)
        self.subscribers[channel].add(queue)
        self.last_seen[channel] = time.monotonic()
        return queue

    def unsubscribe(self, channel, queue):
        self.subscribers[channel].discard(queue)
        self.last_seen[channel] = time.monotonic()

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
        """True once the listener cap is reached.

        Every stream costs a task, a queue and a socket, and each published
        line is written to all of them, so an unbounded count is a way for
        one client to make the fan-out slower for the whole room.
        """
        return (self.max_listeners > 0
                and self.listener_total() >= self.max_listeners)

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
        self.config["device"] = device or self.config["device"]
        if not self.config["device"]:
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
            self.started_at, config["device"], config["asr_model"],
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
        """Hand one row to the recorder, and never let it reach the captions.

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

    def _demand(self):
        """(translated, capped): languages wanted, split by max_languages.

        Demand is unauthenticated. One client can open every channel and
        make every sentence cost the whole language list, in tokens and in
        the latency that more output tokens adds for the people actually
        reading. The cap keeps what the operator forced on, then what has
        the most readers, so a stranger holding eight channels loses to
        the two languages somebody is really reading.
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
            self.overrides.get(name) != "on",
            -len(self.hub.subscribers[name]),
            order.index(name)))
        keep = set(ranked[:cap])
        # Both lists come back in configured order, which is the order the
        # prompt names them in and the order the operator page shows.
        return ([name for name in wanted if name in keep],
                [name for name in wanted if name not in keep])

    def active_languages(self):
        """Languages worth spending tokens on right now."""
        return self._demand()[0]

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
        translated, capped = self._demand()
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
            "device": self.config["device"],
            "model": self.config["model"],
            "channels": self.hub.channels,
            "listeners": self.hub.listener_count(),
            "refused": self.hub.refused,
            "max_listeners": self.hub.max_listeners,
            "uptime": (time.time() - self.started_at
                       if self.started_at and self.state != "stopped" else 0),
            "units": self.stats["units"],
            "translated": self.stats["translated"],
            "failures": self.stats["failures"],
            "timeouts": self.stats["timeouts"],
            "reconnects": self.stats["reconnects"],
            "corrections": self.stats["corrections"],
            "public_url": self.config.get("public_url", ""),
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
        source = await capture.open_capture(config["device"],
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

        languages, capped = self._demand()
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
            # A language the cap dropped still gets the English line, the
            # same as one whose translation failed. A gap is worse: a
            # reader who cannot hear the room cannot tell it from silence.
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
    """True if this request carries the token minted for this run.

    There is deliberately no tokenless mode. The operator routes are meant
    to be reached from the operator's own machine, but a page open in any
    tab of the operator's browser can post a cross-origin form to
    127.0.0.1 with no CORS preflight, and without a secret it can guess
    that reaches Session.start and Session.stop.
    """
    token = request.app["token"]
    supplied = (request.headers.get("X-Caption-Token")
                or request.query.get("token") or "")
    # Bytes and compare_digest rather than ==, which returns sooner for a
    # token that shares a prefix. The encode is not decoration: given two
    # str, compare_digest raises TypeError on anything outside ASCII, and
    # a request can carry that, so == would become a 500 here.
    return hmac.compare_digest(supplied.encode(), token.encode())


async def page(request, filename):
    return web.FileResponse(STATIC / filename)


async def reader_page(request):
    return await page(request, "reader.html")


async def operator_page(request):
    if not authorized(request):
        return web.Response(status=403, text="Add ?token=... to this address.")
    return await page(request, "operator.html")


async def api_status(request):
    """Everything the operator page shows, which is more than a reader sees.

    Behind the token because it names the audio device and the model, and
    carries the raw error text and the last lines spoken. The transcript is
    public by design on the reader page; the rest of this is host detail.
    """
    if not authorized(request):
        return web.json_response({"ok": False, "message": "Not authorized."},
                                 status=403)
    return web.json_response(request.app["session"].status())


async def api_devices(request):
    """Input devices, so the operator picks from a list rather than typing.

    Behind the operator token like every other route under /api, because
    listing devices runs a subprocess and names the sound hardware, and
    neither is something a reader on the tunnel needs.
    """
    if not authorized(request):
        # The devices-and-error shape the operator page already renders,
        # not the ok-and-message shape of the other routes: the page
        # destructures this answer and would throw on a missing list.
        return web.json_response({"devices": [], "error": "Not authorized."},
                                 status=403)
    config = request.app["session"].config
    try:
        # In a thread, because list_devices shells out to pactl with a five
        # second timeout and this coroutine shares its event loop with the
        # captions. Called inline, one slow listing stalls the fan-out to
        # every phone in the room for as long as pactl takes to answer.
        devices = await asyncio.to_thread(capture.list_devices,
                                          config["capture"])
    except capture.CaptureError as exc:
        return web.json_response({"devices": [], "error": str(exc)})
    # The page pre-selects the configured source, which has to be sent
    # separately: the "default" flag on a device is whatever the backend
    # considers default, and parec marks nothing at all.
    return web.json_response({"devices": devices,
                              "configured": config["device"]})


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
    hub = request.app["hub"]
    channel = request.match_info["channel"]
    if channel not in hub.channels:
        return web.Response(status=404, text="No such channel.")
    if hub.full():
        # Refused before prepare, so this is an ordinary response a browser
        # will retry rather than a stream that opens and then goes quiet.
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

    queue = hub.subscribe(channel)
    try:
        # Replay recent history so someone arriving late has context. The
        # client dedupes on seq, so overlap with the live feed is harmless.
        for entry in list(hub.buffers[channel]):
            await response.write(
                f"data: {json.dumps(entry)}\n\n".encode())
        while True:
            try:
                entry = await asyncio.wait_for(queue.get(), timeout=15)
                await response.write(
                    f"data: {json.dumps(entry)}\n\n".encode())
            except TimeoutError:
                # Phones and proxies drop idle connections; this keeps the
                # socket warm through long silences.
                await response.write(b": keepalive\n\n")
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        hub.unsubscribe(channel, queue)
    return response


async def qr_code(request):
    """QR for the reader address, rendered on demand as SVG.

    Drawn from public_url rather than the bind address, because the address
    this server listens on is not the one a phone can reach.
    """
    url = request.app["session"].config.get("public_url")
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
    return web.json_response({"channels": request.app["hub"].channels})


def build_reader_app(hub, session):
    """What a phone needs, and nothing else.

    This is the app a tunnel points at, so every route on it is reachable
    from the public internet and none of them can change anything.
    """
    app = web.Application()
    app["hub"] = hub
    app["session"] = session
    app.add_routes([
        web.get("/", reader_page),
        web.get("/api/channels", api_channels),
        web.get("/stream/{channel}", stream),
    ])
    return app


def build_operator_app(hub, session, token):
    """The controls, served on their own listener.

    A second listener rather than a check inside the handlers, because a
    check cannot tell the two audiences apart: the tunnel daemon runs on
    this machine and connects to the local port, so a visitor from the
    public internet and the operator at the keyboard both arrive from
    127.0.0.1. Keeping these routes on a port the tunnel does not know
    about needs no trust in the tunnel at all.
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


async def serve(config, token, settings):
    """Run both listeners until Ctrl-C, then stop the session.

    web.run_app serves one application, and these are two: the reader app
    on the port a tunnel points at, and the operator app on loopback. They
    share one Hub and one Session, so this is two doors onto one room
    rather than two servers.
    """
    hub = Hub(config["languages"], config.get("max_listeners") or 0)
    session = Session(config, hub)
    listeners = (
        (build_reader_app(hub, session), settings["host"], settings["port"]),
        (build_operator_app(hub, session, token), OPERATOR_HOST,
         settings["operator_port"]),
    )
    runners = []
    try:
        for app, host, port in listeners:
            runner = web.AppRunner(app)
            await runner.setup()
            runners.append(runner)
            await web.TCPSite(runner, host, port).start()
        stop = asyncio.Event()
        install_stop_handler(stop)
        await stop.wait()
    finally:
        # Before the runners, because stopping the session closes the
        # Deepgram socket and the translator, and those should shut down
        # while the loop is still serving rather than after.
        await session.stop()
        for runner in runners:
            await runner.cleanup()


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Any setting may live in config.toml instead, which is the "
               "point: a weekly run should be just `python3 server.py`.")
    parser.add_argument("--list-devices", action="store_true",
                        help="show PipeWire sources and exit")
    parser.add_argument("--config", default="config.toml")
    add_settings_arguments(parser, [
        "device", "capture", "model", "languages", "grace", "idle_stop",
        "glossary", "keyterms", "ceiling", "gap", "hold", "asr_model",
        "endpointing", "max_tokens", "timeout", "reasoning_effort",
        "correct_english", "host", "port", "operator_port", "record",
        "database", "max_languages", "max_listeners",
    ])
    args = parser.parse_args()

    if args.list_devices:
        capture.print_devices(args.capture or "auto")
        return

    load_env()
    settings = resolve(args, load_config(args.config))
    deepgram_key = os.environ.get("DEEPGRAM_API_KEY")
    llm_key = os.environ.get("LLM_API_KEY")
    llm_base = os.environ.get("LLM_BASE_URL")
    if not deepgram_key or not llm_key or not llm_base:
        sys.exit("Set DEEPGRAM_API_KEY, LLM_API_KEY, and LLM_BASE_URL in .env")
    if not settings["model"]:
        sys.exit("No translation model. Set translation.model in config.toml "
                 "or pass --model.")

    config = dict(settings)
    config["keyterms"] = load_file_lines(settings["keyterms"])
    config["glossary"] = load_glossary(settings["glossary"])
    config["deepgram_key"] = deepgram_key
    config["llm_key"] = llm_key
    config["llm_base"] = llm_base

    # Minted every run rather than configured. A settable token invites a
    # weak one and a forgotten one, and the volunteer never types this:
    # the banner below prints the address with the token already in it.
    # Per run also means a link that leaks, by screen share or a photo of
    # this terminal, stops working at the next restart.
    token = secrets.token_urlsafe(32)
    reader = settings["public_url"] or (
        f"http://{settings['host']}:{settings['port']}/")
    print(f"Reader:   {reader}")
    print(f"Operator: http://{OPERATOR_HOST}:{settings['operator_port']}"
          f"/operator?token={token}")
    print("That address carries a token minted for this run, so it "
          "changes\nevery restart. Copy it rather than saving a bookmark.")
    if settings["host"] in ("127.0.0.1", "localhost", "::1"):
        print("Bound to this machine only. Readers reach it through your "
              "tunnel;\nset server.host to 0.0.0.0 to allow direct "
              "connections on this network.")
    try:
        asyncio.run(serve(config, token, settings))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
