#!/usr/bin/env python3
"""
Live subtitling and translation, terminal edition.

Captures audio, streams it to Deepgram for speech recognition, buffers
finalized fragments into whole sentences, translates each sentence into every
target language concurrently, and prints the result in source order.

This is the same pipeline server.py runs, without the web layer. Use it to
check a microphone feed, tune segmentation, or measure translation latency,
because a terminal is a better place to see what is happening than a browser.

Usage:
    python3 pipeline.py --model gemini-3.8-flash
    python3 pipeline.py --device <source> --model gemini-3.8-flash \\
        --languages "French,Congolese Swahili" \\
        --keyterms keyterms.txt --glossary glossary.txt --correct-english

Without --device it opens whatever the backend calls the default input, so
run --list-devices first if the wrong thing ends up being captured.

Output format:
    A dim line starting with a middle dot is a raw recognition fragment,
    printed as it arrives so the buffering is visible. A numbered block is a
    completed sentence with its translations, tagged with the reason the
    sentence closed: endpoint, punctuation, gap, or ceiling. A line marked
    en* is the transcript after glossary correction.

Ctrl-C to stop.
"""

import argparse
import asyncio
import json
import os
import re
import signal
import statistics
import sys
import time
import tomllib
from collections import deque
from urllib.parse import urlencode

try:
    import httpx
    import websockets
except ImportError:
    sys.exit("Missing dependencies: pip install -r requirements.txt")

import capture
from capture import CHANNELS, SAMPLE_RATE

# Every setting, in one place: attribute, config.toml section and key, type,
# and built-in default. Precedence is command line, then config.toml, then
# the default here. Adding a setting means adding one row. The order here
# follows config.example.toml, so the two can be read side by side.
SETTINGS = [
    ("capture", "audio", "backend", str, "auto"),
    ("endpointing", "audio", "endpointing", int, 400),

    ("languages", "languages", "available", list, ["French", "Swahili"]),
    ("grace", "languages", "grace", float, 90.0),
    ("max_languages", "languages", "max_active", int, 0),

    ("model", "translation", "model", str, None),
    ("reasoning_effort", "translation", "reasoning_effort", str, None),
    ("correct_english", "translation", "correct_english", bool, False),
    ("max_tokens", "translation", "max_tokens", int, 2000),
    ("timeout", "translation", "timeout", float, 15.0),
    ("hold", "translation", "hold", float, 8.0),

    ("ceiling", "segmentation", "ceiling", float, 4.0),
    ("gap", "segmentation", "gap", float, 0.6),

    ("idle_stop", "session", "idle_stop_minutes", float, 10.0),
    # Off by default. Recording a meeting is a decision a congregation makes,
    # not something that should happen because nobody set anything.
    ("record", "session", "record", bool, False),
    ("database", "session", "database", str, "sessions.db"),

    ("asr_model", "recognition", "model", str, "nova-3"),

    ("glossary", "files", "glossary", str, "glossary.txt"),
    ("keyterms", "files", "keyterms", str, "keyterms.txt"),

    # Loopback by default: opening a service to a shared network should be
    # a deliberate edit, not what happens when nobody sets anything.
    ("host", "server", "host", str, "127.0.0.1"),
    ("port", "server", "port", int, 8080),
    ("operator_port", "server", "operator_port", int, 8081),
    ("max_listeners", "server", "max_listeners", int, 500),
    ("public_url", "server", "public_url", str, ""),
]


def load_config(path="config.toml"):
    """Read settings from config.toml. Missing file is not an error."""
    try:
        with open(path, "rb") as handle:
            return tomllib.load(handle)
    except FileNotFoundError:
        return {}
    except tomllib.TOMLDecodeError as exc:
        sys.exit(f"{path} is not valid TOML: {exc}")


def resolve(args, config):
    """Merge command line over config file over defaults.

    Every argparse default is None so that "not given" is distinguishable
    from "given the same value as the default", which is what makes the
    three-layer precedence work.
    """
    settings = {}
    for attr, section, key, kind, default in SETTINGS:
        value = getattr(args, attr, None)
        if value is None:
            value = config.get(section, {}).get(key)
        if value is None:
            value = default
        if kind is list and isinstance(value, str):
            # Command line gives a comma-separated string; TOML gives a list.
            value = [item.strip() for item in value.split(",") if item.strip()]
        settings[attr] = value
    return settings


def load_env(path=".env"):
    """Read KEY=value lines from .env without adding a dependency.

    Real environment variables win, so a systemd unit or a shell export can
    override the file without editing it.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            lines = handle.readlines()
    except OSError:
        return
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


CONTEXT_UNITS = 3
TERMINAL_PUNCTUATION = re.compile(r"[.!?]['\"]?$")

DIM = "\033[2m"
RESET = "\033[0m"

SYSTEM_PROMPT = """\
You are producing live subtitles for a Sunday school class. The source text \
comes from automatic speech recognition of people speaking aloud, so it may \
contain recognition errors, false starts, and incomplete sentences.

Return only a JSON object whose keys are exactly the output names listed in \
the request. No preamble, no markdown fences, no commentary.

These rules apply to every output without exception:

1. Never introduce a proper noun, a personal name, a place, a number, a date, \
or a scripture reference that is not present in the source. This is absolute. \
A reader of a translated channel cannot hear the room and has no way to catch \
an invented name, so a confident wrong name is worse than visible confusion.
2. When a garbled span looks like a name you cannot identify, carry the \
English words through unchanged rather than guessing at them.
3. Work on the CURRENT LINE only. The preceding lines are context for \
pronouns and continuity; do not translate them or repeat their content.
4. Preserve sentence boundaries. A reader sees one line at a time on a phone, \
so do not merge or split sentences.
5. A line may be the back half of a sentence the recognizer cut early. If it \
continues the previous line, render it so it reads as a natural continuation, \
and do not repeat what was already covered.

The English output is a correction of the transcript, not a rewrite. Start \
from the source text and change only what is clearly a recognition error: \
names and terms that appear in the glossary, and obvious mis-hearings of \
common words. Keep false starts, repetitions, filler words, and informal \
grammar exactly as transcribed, because for a reader who cannot hear well \
this line is the record of what was said. If nothing needs correcting, return \
the source text unchanged.

Every other output is a translation. A faithful translation has to be fluent \
in its own language, which means supplying grammatical elements that English \
left implicit: subjects, verbs, case, gender, agreement. That latitude covers \
grammar and phrasing only and never extends to content. Keep the spoken \
register of a conversational class rather than formal writing. Keep proper \
names in their original form unless the target language has a well \
established equivalent. Render scripture references using the conventional \
book names and formatting of the target language.\
"""


class Unit:
    """One sentence-ish span of English, ready to translate."""

    def __init__(self, seq, text, audio_end, emitted_at, reason,
                 confidence=None):
        self.seq = seq
        self.text = text
        self.audio_end = audio_end
        self.emitted_at = emitted_at
        self.reason = reason
        # Lowest recognizer confidence among the fragments that formed this
        # sentence. The shakiest lines are where a mis-heard name lives, so
        # this is what points at the next entry for keyterms.txt.
        self.confidence = confidence


class Segmenter:
    """Accumulates finalized ASR fragments into whole sentences.

    Deepgram finalizes text before the speaker finishes a sentence, so
    translating each result in isolation produces fragments like "on the
    heater" with no subject. Fragments are held until a silent gap opens,
    the recognizer reports an endpoint, the text ends in terminal
    punctuation, or the ceiling expires.

    The gap check is what stops an abandoned half-sentence from capturing
    whatever the next person says. Without it, "you all can see if it" waits
    in the buffer and gets glued onto the following turn.
    """

    def __init__(self, ceiling_seconds, gap_seconds, start_seq=0):
        self.ceiling = ceiling_seconds
        self.gap = gap_seconds
        self.parts = []
        self.first_seen = None
        self.audio_end = 0.0
        self.confidence = None
        # Counts on from where the last stream left off. A reconnect builds a
        # new segmenter, and a sequence number that restarted at 1 would make
        # the Hub and the reader page revise the opening lines of the meeting
        # instead of publishing the new ones.
        self.seq = start_seq

    def add(self, text, speech_final, start, audio_end, confidence=None):
        """Feed one finalized fragment. Returns a list of units to emit.

        Two units can come out of one fragment: the gap check may close the
        buffered sentence before the new fragment opens the next one.
        """
        emits = []
        if self.parts and (start - self.audio_end) > self.gap:
            # Before the new fragment is folded in, so the sentence that
            # just closed keeps its own confidence rather than this one's.
            taken = self._take("gap")
            if taken:
                emits.append(taken)

        if self.first_seen is None:
            self.first_seen = time.monotonic()
        self.parts.append(text)
        self.audio_end = audio_end
        if confidence is not None:
            self.confidence = (confidence if self.confidence is None
                               else min(self.confidence, confidence))

        joined = " ".join(self.parts).strip()
        if speech_final or TERMINAL_PUNCTUATION.search(joined):
            taken = self._take("endpoint" if speech_final else "punctuation")
            if taken:
                emits.append(taken)
        return emits

    def check_ceiling(self):
        """Returns a unit if the buffer has been held too long."""
        if not self.parts or self.first_seen is None:
            return None
        if time.monotonic() - self.first_seen < self.ceiling:
            return None
        return self._take("ceiling")

    def drain(self):
        return self._take("drain") if self.parts else None

    def _take(self, reason):
        """Closes the buffer and returns the unit, or None if it was empty.

        The sequence number travels with the text rather than being read off
        the segmenter afterwards. One fragment can close two sentences, and
        both takes run before either unit is built, so a caller reading
        self.seq later would stamp both with the second number.
        """
        text = " ".join(self.parts).strip()
        confidence = self.confidence
        self.parts = []
        self.first_seen = None
        self.confidence = None
        if not text:
            return None
        self.seq += 1
        return self.seq, text, reason, self.audio_end, confidence



class Translator:
    """Concurrent translation calls over one pooled HTTP client."""

    def __init__(self, base_url, api_key, model, languages, glossary,
                 max_tokens, timeout, reasoning_effort=None,
                 correct_english=False, max_attempts=2, retry_delay=0.4):
        self.url = base_url.rstrip("/") + "/chat/completions"
        self.model = model
        self.languages = languages
        self.correct_english = correct_english
        # English first when enabled, so the model settles what was actually
        # said before deciding how to render it in another language.
        self.outputs = (["English"] if correct_english else []) + languages
        self.glossary = glossary
        self.max_tokens = max_tokens
        self.reasoning_effort = reasoning_effort
        # Live subtitling gets one fast retry, because a class cannot wait out
        # a long backoff and the caller falls back to English. Offline review
        # raises this, where waiting beats losing the line.
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay
        self.client = httpx.AsyncClient(
            timeout=timeout,
            headers={"Authorization": f"Bearer {api_key}"},
            limits=httpx.Limits(max_keepalive_connections=8,
                                max_connections=16),
        )

    async def close(self):
        await self.client.aclose()

    def _messages(self, text, context, outputs):
        context_block = "\n".join(context) if context else "(start of session)"
        return [
            {"role": "system", "content": SYSTEM_PROMPT + self.glossary},
            {"role": "user", "content": (
                f"Outputs: {', '.join(outputs)}\n\n"
                f"PRECEDING LINES (context only):\n{context_block}\n\n"
                f"CURRENT LINE:\n{text}"
            )},
        ]

    async def translate(self, text, context, outputs=None):
        """Returns (translations, elapsed). Raises on unrecoverable failure.

        Pass outputs to request a subset, which is how the server avoids
        paying for a language nobody is currently reading.
        """
        # An explicit None check, not a truthiness test: an empty list means
        # "nothing to do" and must never silently expand to every output.
        outputs = self.outputs if outputs is None else outputs
        started = time.monotonic()
        body = {
            "model": self.model,
            "messages": self._messages(text, context, outputs),
            "temperature": 0,
            "max_tokens": self.max_tokens,
        }
        if self.reasoning_effort:
            # Translation is mechanical and does not benefit from thinking.
            # Gemini 3 cannot disable it outright, but "low" floors the
            # budget. Thinking tokens count against max_tokens, which is
            # what truncated responses earlier.
            body["reasoning_effort"] = self.reasoning_effort
        last_error = None
        for attempt in range(self.max_attempts):
            try:
                response = await self.client.post(self.url, json=body)
                if response.status_code != 200:
                    last_error = f"HTTP {response.status_code}"
                    if response.status_code not in (408, 429, 500, 502, 503,
                                                    504):
                        break
                else:
                    choice = response.json()["choices"][0]
                    if choice.get("finish_reason") == "length":
                        raise ValueError("truncated: raise --max-tokens")
                    parsed = parse_translations(
                        choice["message"]["content"], outputs)
                    return parsed, time.monotonic() - started
            # IndexError covers an empty choices array, which is what a
            # filtered or truncated response looks like. Letting it escape
            # kills the publisher task and, in server.py, the session.
            except (httpx.HTTPError, KeyError, IndexError, ValueError,
                    json.JSONDecodeError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            if attempt + 1 < self.max_attempts:
                await asyncio.sleep(self.retry_delay * (2 ** attempt))
        raise RuntimeError(last_error or "unknown translation failure")


def parse_translations(text, languages):
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("```")[1]
        if cleaned.startswith("json"):
            cleaned = cleaned[4:]
    start, end = cleaned.find("{"), cleaned.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"no JSON object in response: {text[:120]}")
    parsed = json.loads(cleaned[start:end + 1])
    return {lang: str(parsed.get(lang, "")).strip() for lang in languages}


def load_file_lines(path):
    """Read a plain list file, ignoring blanks and comments.

    A missing file returns nothing rather than raising, so glossary.txt and
    keyterms.txt can be picked up automatically when present and simply
    skipped when they are not.
    """
    if not path:
        return []
    try:
        with open(path, encoding="utf-8") as handle:
            return [line.strip() for line in handle
                    if line.strip() and not line.startswith("#")]
    except OSError:
        return []


def load_glossary(path):
    terms = load_file_lines(path)
    if not terms:
        return ""
    return ("\n\nGlossary of names and terms used in this congregation. "
            "Follow it exactly:\n\n" + "\n".join(terms))


def build_asr_url(settings, keyterms):
    params = [
        ("model", settings["asr_model"]),
        ("language", "en"),
        ("encoding", "linear16"),
        ("sample_rate", str(SAMPLE_RATE)),
        ("channels", str(CHANNELS)),
        ("interim_results", "false"),
        ("smart_format", "true"),
        ("punctuate", "true"),
        ("endpointing", str(settings["endpointing"])),
    ]
    params.extend(("keyterm", term) for term in keyterms)
    return "wss://api.deepgram.com/v1/listen?" + urlencode(params)


ARGUMENT_HELP = {
    "capture": "auto, sounddevice, or parec",
    "endpointing": "milliseconds of silence that ends an utterance",
    "languages": "comma separated language names",
    "grace": "seconds a language runs on after its last reader leaves",
    "max_languages": "most languages to translate at once, 0 for no cap",
    "model": "translation model, e.g. gemini-3.8-flash",
    "reasoning_effort": "low is usually right; translation needs no thinking",
    "correct_english": "fix recognition errors on the English channel using "
                       "the glossary",
    "max_tokens": "raise if responses truncate; thinking counts against this",
    "timeout": "seconds before a translation request is abandoned",
    "hold": "seconds to wait for a translation before showing English",
    "ceiling": "seconds to hold fragments before forcing a sentence",
    "gap": "seconds of silence that closes the buffered sentence",
    "idle_stop": "minutes of silence before the session stops itself; "
                 "0 disables",
    "record": "keep a transcript of the session; see record.py",
    "database": "where a recorded session is kept",
    "asr_model": "Deepgram model name",
    "glossary": "file of names and terms for the translation model",
    "keyterms": "file of terms to bias speech recognition toward",
    "host": "address to bind",
    "port": "port to bind, the one a tunnel points at",
    "operator_port": "port for the operator controls, always loopback",
    "max_listeners": "most event streams to hold at once, 0 for no cap",
    "public_url": "the address readers use, for the QR code",
}


def install_stop_handler(stop):
    """Set Ctrl-C to raise the stop flag, on whatever platform this is.

    loop.add_signal_handler raises NotImplementedError on Windows, so fall
    back to the plain signal module there.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, AttributeError, ValueError):
            signal.signal(sig, lambda *_: stop.set())


def add_settings_arguments(parser, names):
    """Add the named settings as command line flags.

    Defaults are deliberately None rather than the real default, so that
    resolve() can tell an explicit flag from an absent one and fall through
    to config.toml.
    """
    spec = {attr: (kind,) for attr, _, _, kind, _ in SETTINGS}
    for name in names:
        kind = spec[name][0]
        flag = "--" + name.replace("_", "-")
        help_text = ARGUMENT_HELP.get(name)
        if kind is bool:
            parser.add_argument(flag, dest=name, default=None,
                                action=argparse.BooleanOptionalAction,
                                help=help_text)
        elif kind in (int, float):
            parser.add_argument(flag, dest=name, default=None, type=kind,
                                help=help_text)
        else:
            parser.add_argument(flag, dest=name, default=None,
                                help=help_text)


# -- the pipeline ------------------------------------------------------------
#
# server.py and pipeline.py both run these three tasks over one websocket: a
# pump feeding audio in, a listener segmenting results, and a publisher
# emitting in source order. They differ only in where finished text goes, so
# that difference lives behind a sink and the loops themselves are shared.
# Keep it that way: these loops were duplicated once and the copies drifted
# until one was building a Deepgram URL from names it had never imported.
#
# A sink provides:
#     fragment(text)          a raw recognition result arrived
#     unit(unit) -> outputs   a sentence closed; returns the output names
#                             worth requesting, where an empty list means do
#                             not call the model for this sentence at all
#     translated(unit, languages, translations, elapsed)
#     timed_out(unit, languages)
#     failed(unit, languages, error)


async def pump_audio(source, socket, stop=None):
    """Captured audio to the recognizer, until either end stops.

    Closing the stream on the way out matters: without it, a capture device
    that dies mid-meeting leaves the websocket open, the reader task waits
    forever on a socket that will never produce another result, and the
    session sits in "running" with no audio and no reconnect.
    """
    try:
        while stop is None or not stop.is_set():
            chunk = await source.read()
            if not chunk:
                return
            await socket.send(chunk)
    finally:
        try:
            await socket.send(json.dumps({"type": "CloseStream"}))
        except (websockets.ConnectionClosed, RuntimeError):
            pass


async def listen(socket, segmenter, translator, sink, queue):
    """Read ASR results, segment them, and launch translations."""
    context = deque(maxlen=CONTEXT_UNITS)

    async def emit(taken):
        seq, text, reason, audio_end, confidence = taken
        unit = Unit(seq, text, audio_end, time.monotonic(), reason,
                    confidence)
        outputs = sink.unit(unit)
        task = None
        if outputs and translator is not None:
            task = asyncio.create_task(
                translator.translate(text, list(context), outputs))
        # English among the outputs is a correction of the transcript, not a
        # channel to fill; the publisher wants the translated names only.
        languages = [name for name in outputs if name != "English"]
        context.append(text)
        await queue.put((unit, task, languages))

    async def watch_ceiling():
        while True:
            await asyncio.sleep(0.25)
            taken = segmenter.check_ceiling()
            if taken:
                await emit(taken)

    ceiling_task = asyncio.create_task(watch_ceiling())
    try:
        async for message in socket:
            if isinstance(message, bytes):
                continue
            payload = json.loads(message)
            if payload.get("type") != "Results" or not payload.get("is_final"):
                continue
            alternatives = payload.get("channel", {}).get("alternatives", [])
            if not alternatives:
                continue
            text = alternatives[0].get("transcript", "").strip()
            if not text:
                continue

            sink.fragment(text)
            start = payload.get("start", 0.0)
            audio_end = start + payload.get("duration", 0.0)
            speech_final = payload.get("speech_final", False)
            confidence = alternatives[0].get("confidence")
            for taken in segmenter.add(text, speech_final, start, audio_end,
                                       confidence):
                await emit(taken)
    finally:
        ceiling_task.cancel()
        taken = segmenter.drain()
        if taken:
            await emit(taken)
        await queue.put(None)


async def publish(queue, sink, hold_seconds):
    """Hand translations to the sink in source order as calls complete.

    Calls run concurrently, but output is ordered, so a slow call delays the
    ones behind it rather than scrambling the transcript. The hold timeout
    bounds that delay: past it, the English text stands in so a reader never
    sees a gap.
    """
    while True:
        item = await queue.get()
        if item is None:
            return
        unit, task, languages = item
        if task is None:
            continue
        try:
            translations, elapsed = await asyncio.wait_for(
                asyncio.shield(task), timeout=hold_seconds)
        except TimeoutError:
            task.cancel()
            sink.timed_out(unit, languages)
        except asyncio.CancelledError:
            # The shield means this is our own cancellation: the session is
            # stopping. An English fallback here would reach every channel
            # after the operator pressed Stop.
            task.cancel()
            raise
        except (RuntimeError, httpx.HTTPError) as exc:
            sink.failed(unit, languages, exc)
        else:
            sink.translated(unit, languages, translations, elapsed)


def latency_summary(values):
    """Median and p95 over a list of seconds, for an end of run report."""
    ordered = sorted(values)
    p95 = ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]
    return f"median {statistics.median(ordered):.2f}s, p95 {p95:.2f}s"


class TerminalSink:
    """Prints the transcript as it forms, in the format __doc__ describes."""

    def __init__(self, outputs, started, color):
        self.outputs = list(outputs)
        self.started = started
        self.dim, self.reset = (DIM, RESET) if color else ("", "")
        self.latencies = []
        self.failures = 0
        self.timeouts = 0

    def fragment(self, text):
        print(f"{self.dim}  . {text}{self.reset}")

    def unit(self, unit):
        lag = (time.monotonic() - self.started) - unit.audio_end
        print(f"\n[{unit.seq}] en ({lag:.1f}s, {unit.reason}): {unit.text}")
        return self.outputs

    def translated(self, unit, languages, translations, elapsed):
        self.latencies.append(elapsed)
        revised = translations.get("English")
        if revised and revised != unit.text:
            print(f"     en*: {revised}")
        for name in languages:
            # An empty string means the model omitted that language, which
            # parse_translations cannot distinguish from an empty answer.
            print(f"     {name[:2].lower()}: "
                  f"{translations.get(name) or '[en] ' + unit.text}"
                  f"  (+{elapsed:.1f}s)")

    def timed_out(self, unit, languages):
        self.timeouts += 1
        for name in languages:
            print(f"     {name[:2].lower()}: [slow] {unit.text}")

    def failed(self, unit, languages, error):
        self.failures += 1
        print(f"     !! translation failed: {error}")
        for name in languages:
            print(f"     {name[:2].lower()}: [en] {unit.text}")

    def summary(self):
        if not self.latencies:
            return None
        return (f"{len(self.latencies)} translated, {self.failures} failed, "
                f"{self.timeouts} too slow. {latency_summary(self.latencies)}")


async def run(args, settings):
    load_env()
    deepgram_key = os.environ.get("DEEPGRAM_API_KEY")
    if not deepgram_key:
        sys.exit("Set DEEPGRAM_API_KEY in .env")

    translator = None
    if not args.no_translate:
        llm_key = os.environ.get("LLM_API_KEY")
        llm_base = os.environ.get("LLM_BASE_URL")
        if not llm_key or not llm_base:
            sys.exit("Set LLM_API_KEY and LLM_BASE_URL in .env, "
                     "or pass --no-translate to check the audio path alone.")
        if not settings["model"]:
            sys.exit("No translation model. Set translation.model in "
                     "config.toml or pass --model.")
        translator = Translator(
            llm_base, llm_key, settings["model"], settings["languages"],
            load_glossary(settings["glossary"]), settings["max_tokens"],
            settings["timeout"], settings["reasoning_effort"],
            settings["correct_english"])

    keyterms = load_file_lines(settings["keyterms"])
    segmenter = Segmenter(settings["ceiling"], settings["gap"])
    queue = asyncio.Queue()

    try:
        # No --device means the default input, so the usual check of the
        # audio path is one command with nothing to look up first.
        device = args.device or capture.default_device(settings["capture"])
        source = await capture.open_capture(device, settings["capture"])
    except capture.CaptureError as exc:
        sys.exit(str(exc))

    stop = asyncio.Event()
    install_stop_handler(stop)

    describe = (", ".join(translator.languages) if translator
                else "transcription only")
    print(f"Listening on {device}. {describe}. Ctrl-C to stop.",
          file=sys.stderr)
    started = time.monotonic()
    sink = TerminalSink(translator.outputs if translator else [], started,
                        not args.no_color)

    try:
        async with websockets.connect(
            build_asr_url(settings, keyterms),
            additional_headers={"Authorization": f"Token {deepgram_key}"},
        ) as socket:
            pump = asyncio.create_task(pump_audio(source, socket, stop))
            reader = asyncio.create_task(
                listen(socket, segmenter, translator, sink, queue))
            writer = asyncio.create_task(
                publish(queue, sink, settings["hold"]))
            await stop.wait()
            pump.cancel()
            try:
                # Ctrl-C should still print the sentence left in the buffer
                # and the translations already in flight, so the reader and
                # the writer get a bounded chance to drain.
                await asyncio.wait_for(asyncio.gather(reader, writer),
                                       timeout=settings["hold"] + 2)
            except (TimeoutError, asyncio.CancelledError):
                reader.cancel()
                writer.cancel()
            finally:
                # Collect the pump, so a socket that closed under it does not
                # surface later as an unretrieved task exception.
                await asyncio.gather(pump, return_exceptions=True)
    finally:
        await source.close()
        if translator:
            await translator.close()

    summary = sink.summary()
    if summary:
        print("\n" + summary, file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Any setting may live in config.toml instead. The command "
               "line wins where both are given.")
    parser.add_argument("--list-devices", action="store_true",
                        help="show PipeWire sources and exit")
    parser.add_argument("--no-translate", action="store_true",
                        help="transcribe only, no translation model needed")
    parser.add_argument("--no-color", action="store_true")
    parser.add_argument("--config", default="config.toml")
    # Not a setting: the source changes with every reboot, so a name kept
    # in config.toml would be stale more often than it was right.
    parser.add_argument("--device",
                        help="audio input device name; see --list-devices")
    add_settings_arguments(parser, [
        "capture", "endpointing",
        "languages",
        "model", "reasoning_effort", "correct_english", "max_tokens",
        "timeout", "hold",
        "ceiling", "gap",
        "asr_model",
        "glossary", "keyterms",
    ])
    args = parser.parse_args()

    if args.list_devices:
        capture.print_devices(args.capture or "auto")
        return

    settings = resolve(args, load_config(args.config))
    asyncio.run(run(args, settings))


if __name__ == "__main__":
    main()
