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

Setup:
    sudo apt install pulseaudio-utils
    pip install -r requirements.txt
    cp .env.example .env   and fill it in

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
import io
import json
import os
import sys
import time
from collections import deque
from pathlib import Path
from urllib.parse import urlencode

try:
    import httpx
    import websockets
    from aiohttp import web
except ImportError:
    sys.exit("Missing dependencies: pip install websockets httpx aiohttp")

import capture
from pipeline import (CONTEXT_UNITS, Segmenter, Translator, Unit,
                      add_settings_arguments, load_config, load_env,
                      load_file_lines, load_glossary, resolve)

STATIC = Path(__file__).parent / "static"
HISTORY = 60
RECONNECT_BACKOFF = [1, 2, 5, 10, 20]


class Hub:
    """Ring buffer plus live subscribers, one channel per language."""

    def __init__(self, languages):
        self.channels = ["English"] + list(languages)
        self.buffers = {name: deque(maxlen=HISTORY) for name in self.channels}
        self.subscribers = {name: set() for name in self.channels}
        # When a channel last had somebody on it, so a language does not shut
        # off the instant a phone drops and reconnects.
        self.last_seen = {name: None for name in self.channels}

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
        self.stats = {"units": 0, "translated": 0, "failures": 0,
                      "timeouts": 0, "reconnects": 0, "corrections": 0,
                      "skipped": 0, "latencies": []}

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
                      "skipped": 0, "latencies": []}
        self.hub.clear()
        self.state = "starting"
        self.started_at = time.time()
        self.last_activity = time.monotonic()
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
        return True, "Stopped."

    def active_languages(self):
        """Languages worth spending tokens on right now."""
        grace = self.config["grace"]
        active = []
        for language in self.config["languages"]:
            override = self.overrides.get(language)
            if override == "off":
                continue
            if override == "on" or self.hub.wanted(language, grace):
                active.append(language)
        return active

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
        grace = self.config["grace"]
        active = set(self.active_languages())
        return [{
            "name": language,
            "listeners": len(self.hub.subscribers[language]),
            "override": self.overrides.get(language, "auto"),
            "active": language in active,
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
        segmenter = Segmenter(config["ceiling"], config["gap"])
        queue = asyncio.Queue()
        try:
            async with websockets.connect(
                self._asr_url(),
                additional_headers={
                    "Authorization": f"Token {config['deepgram_key']}"},
            ) as socket:
                self.state = "running"
                self.error = None
                tasks = [
                    asyncio.create_task(self._pump(source, socket)),
                    asyncio.create_task(
                        self._listen(socket, segmenter, queue)),
                    asyncio.create_task(self._publish(queue)),
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

    def _asr_url(self):
        config = self.config
        params = [
            ("model", config["asr_model"]), ("language", "en"),
            ("encoding", "linear16"), ("sample_rate", str(SAMPLE_RATE)),
            ("channels", str(CHANNELS)), ("interim_results", "false"),
            ("smart_format", "true"), ("punctuate", "true"),
            ("endpointing", str(config["endpointing"])),
        ]
        params.extend(("keyterm", term) for term in config["keyterms"])
        return "wss://api.deepgram.com/v1/listen?" + urlencode(params)

    async def _pump(self, source, socket):
        """Feed captured audio to the recognizer until one end stops.

        Closing the stream on the way out matters: without it, a capture
        device that dies mid-meeting leaves the websocket open, the reader
        task waits forever on a socket that will never produce another
        result, and the session sits in "running" with no audio and no
        reconnect.
        """
        try:
            while True:
                chunk = await source.read()
                if not chunk:
                    return
                await socket.send(chunk)
        finally:
            try:
                await socket.send(json.dumps({"type": "CloseStream"}))
            except (websockets.ConnectionClosed, RuntimeError):
                pass

    async def _listen(self, socket, segmenter, queue):
        context = deque(maxlen=CONTEXT_UNITS)

        async def emit(taken):
            text, reason, audio_end = taken
            unit = Unit(segmenter.seq, text, audio_end, time.monotonic(),
                        reason)
            self.stats["units"] += 1
            self.last_activity = time.monotonic()
            # The raw transcript goes out immediately either way.
            self.hub.publish("English", unit.seq, text)

            languages = self.active_languages()
            wants_english = (self.config["correct_english"]
                             and self.hub.wanted("English",
                                                 self.config["grace"]))
            outputs = (["English"] if wants_english else []) + languages

            task = None
            if outputs:
                task = asyncio.create_task(
                    self.translator.translate(text, list(context), outputs))
            else:
                # Nobody is reading a translated channel and English needs no
                # correction, so this sentence costs no model tokens at all.
                self.stats["skipped"] += 1
            context.append(text)
            await queue.put((unit, task, languages))

        async def watch_ceiling():
            while True:
                await asyncio.sleep(0.25)
                taken = segmenter.check_ceiling()
                if taken:
                    await emit(taken)

        ceiling = asyncio.create_task(watch_ceiling())
        try:
            async for message in socket:
                if isinstance(message, bytes):
                    continue
                payload = json.loads(message)
                if (payload.get("type") != "Results"
                        or not payload.get("is_final")):
                    continue
                alternatives = payload.get("channel", {}).get("alternatives",
                                                              [])
                if not alternatives:
                    continue
                text = alternatives[0].get("transcript", "").strip()
                if not text:
                    continue
                start = payload.get("start", 0.0)
                audio_end = start + payload.get("duration", 0.0)
                for taken in segmenter.add(
                        text, payload.get("speech_final", False), start,
                        audio_end):
                    await emit(taken)
        finally:
            ceiling.cancel()
            taken = segmenter.drain()
            if taken:
                await emit(taken)
            await queue.put(None)

    async def _publish(self, queue):
        hold = self.config["hold"]
        while True:
            item = await queue.get()
            if item is None:
                return
            unit, task, languages = item
            if task is None:
                continue
            try:
                translations, elapsed = await asyncio.wait_for(
                    asyncio.shield(task), timeout=hold)
                self.stats["translated"] += 1
                self.stats["latencies"].append(round(elapsed, 2))
                revised = translations.get("English")
                if revised and revised != unit.text:
                    self.stats["corrections"] += 1
                    self.hub.publish("English", unit.seq, revised)
                for language in languages:
                    self.hub.publish(language, unit.seq,
                                     translations.get(language, unit.text))
            except asyncio.TimeoutError:
                task.cancel()
                self.stats["timeouts"] += 1
                for language in languages:
                    self.hub.publish(language, unit.seq, unit.text)
            except (RuntimeError, asyncio.CancelledError, httpx.HTTPError):
                self.stats["failures"] += 1
                for language in languages:
                    self.hub.publish(language, unit.seq, unit.text)


# -- web layer -------------------------------------------------------------


def authorized(request):
    token = request.app["token"]
    if not token:
        return True
    supplied = (request.headers.get("X-Caption-Token")
                or request.query.get("token"))
    return supplied == token


async def page(request, filename):
    return web.FileResponse(STATIC / filename)


async def reader_page(request):
    return await page(request, "reader.html")


async def operator_page(request):
    if not authorized(request):
        return web.Response(status=403, text="Add ?token=... to this address.")
    return await page(request, "operator.html")


async def api_status(request):
    return web.json_response(request.app["session"].status())


async def api_devices(request):
    """Input devices, so the operator picks from a list rather than typing."""
    try:
        devices = capture.list_devices(request.app["session"].config["capture"])
    except capture.CaptureError as exc:
        return web.json_response({"devices": [], "error": str(exc)})
    return web.json_response({"devices": devices})


async def api_start(request):
    if not authorized(request):
        return web.json_response({"ok": False, "message": "Not authorized."},
                                 status=403)
    body = await request.json() if request.can_read_body else {}
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
    body = await request.json()
    ok, message = request.app["session"].set_override(
        body.get("language"), body.get("mode"))
    return web.json_response({"ok": ok, "message": message})


async def stream(request):
    """Server-sent events for one language channel."""
    hub = request.app["hub"]
    channel = request.match_info["channel"]
    if channel not in hub.channels:
        return web.Response(status=404, text="No such channel.")

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
                f"data: {json.dumps(entry)}\n\n".encode("utf-8"))
        while True:
            try:
                entry = await asyncio.wait_for(queue.get(), timeout=15)
                await response.write(
                    f"data: {json.dumps(entry)}\n\n".encode("utf-8"))
            except asyncio.TimeoutError:
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


def build_app(config, token):
    hub = Hub(config["languages"])
    app = web.Application()
    app["hub"] = hub
    app["token"] = token
    app["session"] = Session(config, hub)
    app.add_routes([
        web.get("/", reader_page),
        web.get("/operator", operator_page),
        web.get("/api/status", api_status),
        web.get("/api/devices", api_devices),
        web.get("/api/channels", api_channels),
        web.post("/api/start", api_start),
        web.post("/api/stop", api_stop),
        web.post("/api/language", api_language),
        web.get("/stream/{channel}", stream),
        web.get("/qr.svg", qr_code),
    ])

    async def shutdown(app):
        await app["session"].stop()

    app.on_cleanup.append(shutdown)
    return app


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
        "correct_english", "host", "port",
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

    # The operator secret belongs with the other secrets, not in config.toml.
    token = os.environ.get("OPERATOR_TOKEN")
    app = build_app(config, token)
    suffix = f"?token={token}" if token else ""
    reader = settings["public_url"] or (
        f"http://{settings['host']}:{settings['port']}/")
    print(f"Reader:   {reader}")
    print(f"Operator: http://{settings['host']}:{settings['port']}"
          f"/operator{suffix}")
    if settings["host"] in ("127.0.0.1", "localhost", "::1"):
        print("Bound to this machine only. Readers reach it through your "
              "tunnel;\nset server.host to 0.0.0.0 to allow direct "
              "connections on this network.")
    elif not token:
        print("No OPERATOR_TOKEN set, and this server is reachable from the "
              "network:\nanyone who can reach it can start and stop it.")
    web.run_app(app, host=settings["host"], port=settings["port"], print=None)


if __name__ == "__main__":
    main()
