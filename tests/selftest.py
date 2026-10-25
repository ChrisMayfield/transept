#!/usr/bin/env python3
"""
Self test for the shared pipeline, a whole session, the recording it keeps,
the running server, and the script that starts it.

Needs no API keys, no audio device, and no network. A fake recognizer and a
fake translator stand in where the real ones would go, so the whole thing
runs in a few seconds and costs nothing.

This is not a substitute for listening to a real room, where recognition
accuracy is decided. What it catches is the wiring between pipeline.py and
server.py coming apart, which once put every session start into a NameError
that a supervisor caught and retried forever.

Usage, from the top of the checkout:
    python3 tests/selftest.py            everything
    python3 tests/selftest.py lint       ruff, with the rules in ruff.toml
    python3 tests/selftest.py pipeline   the shared loops, both sinks
    python3 tests/selftest.py session    a whole Session start and stop
    python3 tests/selftest.py store      recording a session, read back
    python3 tests/selftest.py server     boot server.py, exercise routes
    python3 tests/selftest.py script     the operator script, no funnel

Exits non-zero if any check fails.
"""

import array
import asyncio
import base64
import collections
import contextlib
import io
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace

import websockets
from aiohttp import web
from websockets.sync.client import connect as ws_connect

# The code under test is in src/, beside this directory rather than in it,
# and nothing is installed, so the path is the only way to reach it.
ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import capture
import controls
import pipeline
import record
import sender
import server
import transept


class Checks:
    """Counts passes and failures and prints one line per check."""

    def __init__(self):
        self.passed = 0
        self.failed = 0

    def section(self, title):
        print(f"\n{title}")

    def check(self, label, condition, detail=""):
        if condition:
            self.passed += 1
            print(f"  [PASS] {label}")
        else:
            self.failed += 1
            print(f"  [FAIL] {label}")
            if detail:
                print(f"         got: {detail}")

    def report(self):
        total = self.passed + self.failed
        print(f"\n{self.passed} of {total} checks passed.")
        return 1 if self.failed else 0


# -- fakes -------------------------------------------------------------------


def asr_result(text, speech_final, start, duration, is_final=True,
               from_finalize=False, quiet=0.3):
    """One result, shaped the way Deepgram sends it, finalized by default.

    Its words share the audio evenly and the last ends quiet seconds
    before its audio does.
    """
    spoken = text.split()
    step = (duration - quiet) / max(1, len(spoken))
    words = [{"word": word.lower().strip(".,?!"), "punctuated_word": word,
              "start": start + index * step,
              "end": start + (index + 1) * step}
             for index, word in enumerate(spoken)]
    return json.dumps({
        "type": "Results",
        "is_final": is_final,
        "speech_final": speech_final,
        "from_finalize": from_finalize,
        "start": start,
        "duration": duration,
        "channel": {"alternatives": [{"transcript": text,
                                      "words": words}]},
    })


def ogg_page(granule, payload):
    """One Ogg page with this granule position and a one-segment payload."""
    return (b"OggS" + bytes(2) + granule.to_bytes(8, "little", signed=True)
            + bytes(12) + bytes([1, len(payload)]) + payload)


# Three sentences over four fragments, one for each way the buffer closes.
# Nothing is punctuated until the third sentence on purpose: the first has to
# survive in the buffer until the gap closes it. An earlier fixture punctuated
# fragment two, so the gap branch was never reached under a check that claimed
# to test it. The third fragment then does double duty, arriving after the gap
# and ending a sentence itself, which is the case where one sequence number
# for two units would make the Hub revise the first line away.
FRAGMENTS = [
    asr_result("the meeting will start", False, 0.0, 1.0),
    asr_result("at nine o'clock", False, 1.0, 1.0),
    asr_result("Brother Kalema will pray.", True, 5.0, 1.5),
    asr_result("Please turn to page ten.", False, 6.5, 1.5),
]

FIRST_SENTENCE = "the meeting will start at nine o'clock"
SECOND_SENTENCE = "Brother Kalema will pray."
THIRD_SENTENCE = "Please turn to page ten."
SENTENCES = [FIRST_SENTENCE, SECOND_SENTENCE, THIRD_SENTENCE]


class FakeSocket:
    """Stands in for the Deepgram websocket.

    Yields the given results, then either ends the stream or stays open the
    way a live socket does between turns. Which one matters: an ended stream
    is how the session supervisor learns to reconnect.
    """

    def __init__(self, messages, stay_open=False):
        self.messages = messages
        self.stay_open = stay_open
        self.audio_chunks = 0
        self.keepalives = 0
        self.finalizes = 0
        self.close_stream_sent = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def send(self, data):
        if isinstance(data, str) and "CloseStream" in data:
            self.close_stream_sent = True
        elif isinstance(data, str) and "KeepAlive" in data:
            # Counted apart from audio, because the whole point of it is
            # that it holds the socket open without being audio.
            self.keepalives += 1
        elif isinstance(data, str) and "Finalize" in data:
            self.finalizes += 1
        else:
            self.audio_chunks += 1

    def __aiter__(self):
        async def results():
            for message in self.messages:
                await asyncio.sleep(0)
                yield message
            if self.stay_open:
                await asyncio.Event().wait()
        return results()


class FakeSource:
    """A capture device that never runs out of silence."""

    encoding = "pcm"

    def __init__(self):
        self.reads = 0
        self.closed = False

    async def read(self):
        self.reads += 1
        # A real device paces itself. Without a yield here the pump would
        # starve every other task in the session.
        await asyncio.sleep(0.01)
        return b"\x00\x00" * capture.CHUNK_FRAMES

    async def close(self):
        self.closed = True


class FakeOpusSource:
    """An encoder whose stream, like a real one, opens with its own pages.

    The pages appear on the first read rather than at construction,
    because that is the ordering the hello has to cope with: ffmpeg writes
    them a moment after the microphone opens, while the socket that will
    carry them is still being made.
    """

    encoding = "opus"
    # Shape enough to be recognized: a page whose one segment is an
    # OpusHead. Nothing here decodes it, only carries it.
    headers = ogg_page(0, b"OpusHead" + bytes(11))

    def __init__(self):
        self.reads = 0
        self.closed = False
        # Shadows the class attribute, so nothing is known about this
        # stream until something has actually read from it.
        self.headers = b""

    async def read(self):
        await asyncio.sleep(0.01)
        self.reads += 1
        if self.reads == 1:
            self.headers = type(self).headers
            return type(self).headers
        return b"\x11\x22" * capture.CHUNK_FRAMES

    async def close(self):
        self.closed = True


class FakeTranslator:
    """Returns a marker per output, or misbehaves on demand.

    A marker rather than a plausible translation, so a line published to the
    wrong channel is obvious in the failure message.
    """

    def __init__(self, *args, mode="ok", delay=0.0, **kwargs):
        self.mode = mode
        self.delay = delay
        self.calls = []

    async def translate(self, text, context, outputs=None):
        self.calls.append({"text": text, "context": list(context),
                           "outputs": list(outputs or [])})
        await asyncio.sleep(self.delay)
        if self.mode == "fail":
            raise RuntimeError("provider exploded")
        rendered = {name: (f"CORRECTED {text}" if name == "English"
                           else f"<{name}> {text}")
                    for name in outputs}
        if self.mode == "empty":
            # What parse_translations returns when the model answered but
            # left a language out. The call succeeded, so no failure path
            # runs and only the sink can save the reader.
            rendered = {name: ("" if name != "English" else rendered[name])
                        for name in rendered}
        # Counts that differ per sentence, so a row stored against the
        # wrong line would show. From the text rather than a call counter,
        # because calls run concurrently and finish in any order.
        rendered = pipeline.Translations(rendered)
        rendered.usage = {"prompt_tokens": 600 + len(text),
                          "output_tokens": 40, "reasoning_tokens": 12,
                          "cached_tokens": 0}
        return rendered, 0.5

    async def close(self):
        pass


@contextlib.contextmanager
def cleared_secrets():
    """No key or token from this machine's environment, for the duration.

    This machine may have a shell that exports any of them, and a check
    that reads a real key would pass for the wrong reason.
    """
    kept = {name: os.environ.pop(name, None)
            for _, _, name in pipeline.SECRETS}
    try:
        yield
    finally:
        for name, value in kept.items():
            os.environ.pop(name, None)
            if value is not None:
                os.environ[name] = value


class StubRequest:
    """Just enough of an aiohttp request for a handler to read."""

    def __init__(self, token, query=None, header=None, session=None):
        self.app = {"token": token, "session": session}
        self.query = {"token": query} if query else {}
        self.headers = {"X-Subtitle-Token": header} if header else {}


def fake_config(**overrides):
    """A Session config carrying every key the pipeline reads."""
    config = {
        "languages": ["French", "Swahili"], "grace": 90.0,
        "correct_english": True, "ceiling": 4.0, "gap": 0.6, "min_words": 6,
        "hold": 8.0,
        "capture": "auto", "encoding": "pcm", "asr_model": "nova-3",
        "endpointing": 400, "early_finalize": True,
        "keyterms": ["Kalema"], "glossary": "",
        "model": "fake-model", "max_tokens": 2000, "timeout": 15.0,
        "reasoning_effort": "low", "idle_stop": 0, "public_url": "",
        "reader_url": "", "max_languages": 0, "room": "",
        "control_grace": 30.0,
        "deepgram_key": "fake", "llm_key": "fake",
        "llm_base": "http://invalid/v1",
    }
    config.update(overrides)
    return config


def make_session(languages, correct_english, readers, **overrides):
    """A Session and its Hub, with the named channels already subscribed."""
    hub = server.Hub(languages)
    session = server.Session(
        fake_config(languages=languages, correct_english=correct_english,
                    **overrides), hub)
    for channel in readers:
        hub.subscribe(channel)
    return session, hub


async def drive(sink, translator, hold=5.0):
    """Run the shared loops over one fake socket until the results run out."""
    segmenter = pipeline.Segmenter(ceiling_seconds=99, gap_seconds=0.6)
    queue = asyncio.Queue()
    reader = asyncio.create_task(
        pipeline.listen(FakeSocket(FRAGMENTS), segmenter, translator, sink,
                        queue))
    writer = asyncio.create_task(pipeline.publish(queue, sink, hold))
    await asyncio.gather(reader, writer)


# -- the shared loops, against both sinks ------------------------------------


class FragmentSink:
    """Keeps what listen hands a sink, and asks for no translations."""

    def __init__(self):
        self.results = []
        self.fragments = []
        self.words = []
        self.guesses = []
        self.units = []

    def recognized(self, words):
        self.results.append(words)

    def fragment(self, text, start, audio_end, speech_final, confidence,
                 finalized=False, words=None):
        self.fragments.append((text, finalized))
        self.words.append(words)

    def guess(self, audio_end, quiet, unseen, decision):
        self.guesses.append((audio_end, unseen, decision))

    def unit(self, unit):
        self.units.append(unit.text)
        return []


class SentClock:
    """A source's clock, with this much audio already gone up."""

    def __init__(self, position):
        self.position = position


async def listen_to(messages, finalize, clock=None):
    """Run listen alone over these messages; the sink and the socket."""
    socket = FakeSocket(messages)
    sink = FragmentSink()
    await pipeline.listen(socket, pipeline.Segmenter(99, 0.6), None, sink,
                          asyncio.Queue(), finalize, clock)
    return sink, socket


async def check_early_finalize(checks):
    """Guesses decide when to finish, and never become text themselves.

    On the second field test most pieces ended on the recognizer's own
    clock, four seconds or more after their first word, because a pause
    long enough for endpointing is rare in a room.
    """
    checks.section("Asking the recognizer to finish a sentence")
    guesses = [
        asr_result("and so we", False, 0.0, 0.8, is_final=False),
        asr_result("and so we came to the chapel.", False, 0.0, 2.0,
                   is_final=False),
        # The next guess, before the finished result answers the request,
        # which ends a sentence too and must not ask a second time.
        asr_result("and so we came to the chapel today.", False, 0.0, 2.3,
                   is_final=False),
        asr_result("and so we came to the chapel.", False, 0.0, 2.0,
                   from_finalize=True),
        asr_result("Right?", False, 2.4, 0.4, is_final=False),
    ]
    sink, socket = await listen_to(guesses, True)
    checks.check("a guess ending a sentence asks the recognizer to finish",
                 socket.finalizes >= 1, socket.finalizes)
    checks.check("once, not again for every guess before the answer",
                 socket.finalizes == 1, socket.finalizes)
    checks.check("guesses never become fragments or lines",
                 sink.fragments == [("and so we came to the chapel.", True)]
                 and sink.units == ["and so we came to the chapel."],
                 (sink.fragments, sink.units))
    checks.check("and are not counted as results the recognizer returned",
                 sink.results == [True], sink.results)
    sink, socket = await listen_to([
        asr_result("and so we came to the chapel.", False, 0.0, 2.0,
                   is_final=False, quiet=0.05)], True)
    checks.check("nor while the next word may already be arriving",
                 socket.finalizes == 0, socket.finalizes)
    checks.check("a short sentence on its own is not worth finishing",
                 not pipeline.ends_sentence("Right?", 0, 6))
    checks.check("but is when the segmenter holds the rest of it",
                 pipeline.ends_sentence("Right?", 5, 6))
    checks.check("a guess still in its sentence is not finished",
                 not pipeline.ends_sentence(
                     "and so we came to the chapel and", 0, 6))
    # What Deepgram sent on a live test: a Finalize answered with
    # speech_final set, here on a piece that stops mid-sentence.
    halfway = [
        asr_result("We hope you feel at home. Our opening", True, 0.0, 2.0,
                   from_finalize=True),
        asr_result("hymn will be number two.", False, 2.0, 1.5),
    ]
    sink, socket = await listen_to(halfway, True)
    checks.check("an answer to a Finalize is not taken for a pause",
                 sink.units == ["We hope you feel at home.",
                                "Our opening hymn will be number two."],
                 sink.units)
    sink, socket = await listen_to(guesses, False)
    checks.check("with early_finalize off nothing is asked",
                 socket.finalizes == 0, socket.finalizes)
    checks.check("and the guesses are still ignored",
                 [text for text, _ in sink.fragments]
                 == ["and so we came to the chapel."], sink.fragments)
    checks.check("but the finish it would have asked for is kept, once",
                 [decision for _, _, decision in sink.guesses] == ["off"],
                 sink.guesses)

    # On the third field test a speaker already into the next sentence
    # lost its first word to a Finalize asked on a guess that had not
    # heard it begin. The first guess here has 0.8 seconds of audio after
    # it, the second 0.2.
    stale = [
        asr_result("and so we came to the chapel.", False, 0.0, 2.0,
                   is_final=False),
        asr_result("and so we came to the chapel.", False, 0.0, 2.6,
                   is_final=False),
    ]
    sink, socket = await listen_to(stale, True, SentClock(2.8))
    checks.check("a guess with too much audio after it is not finished on",
                 [decision for _, _, decision in sink.guesses]
                 == ["stale", "sent"], sink.guesses)
    checks.check("and the next guess, which heard more of it, is",
                 socket.finalizes == 1, socket.finalizes)
    checks.check("what each guess had not heard is measured off the clock",
                 [round(unseen, 2) for _, unseen, _ in sink.guesses]
                 == [0.8, 0.2], sink.guesses)

    sink, socket = await listen_to(FRAGMENTS[:1], False)
    timed = sink.words[0] or []
    checks.check("each word reaches the sink with its start and end",
                 [word for word, _, _ in timed]
                 == ["the", "meeting", "will", "start"]
                 and all(start < end for _, start, end in timed), timed)

    settings = {"asr_model": "nova-3", "endpointing": 400}
    checks.check("guesses are requested with early_finalize off as well",
                 "interim_results=true"
                 in pipeline.build_asr_url(settings, [], "pcm")
                 and "interim_results=true" in pipeline.build_asr_url(
                     dict(settings, early_finalize=True), [], "pcm"))


async def check_stream_clock(checks):
    """Lag measured from when the audio was captured.

    The numbers are the two ways the old measurement failed on a real
    Sunday: a line that arrived promptly measured negative and was thrown
    away, and after a microphone muted by a locked screen every line
    measured late by the length of the silence.
    """
    checks.section("How far behind the room a line is")
    clock = pipeline.StreamClock()
    # Ten seconds of audio in tenths, the last of it read at t=100.
    for step in range(1, 101):
        clock.advance(0.1, now=90 + step * 0.1)
    checks.check("a line that arrived promptly measures a small lag",
                 abs(clock.lag(10.0, now=100.4) - 0.4) < 1e-6,
                 clock.lag(10.0, now=100.4))
    checks.check("audio early in a chunk was captured before the chunk "
                 "was read", abs(clock.lag(9.95, now=100.0) - 0.05) < 1e-6,
                 clock.lag(9.95, now=100.0))
    # Twenty seconds with nothing delivered, then two more of audio.
    for step in range(1, 21):
        clock.advance(0.1, now=120 + step * 0.1)
    checks.check("a silence the microphone never delivered is not lag",
                 abs(clock.lag(12.0, now=122.5) - 0.5) < 1e-6,
                 clock.lag(12.0, now=122.5))
    checks.check("and the audio before it is still placed where it was",
                 abs(clock.lag(5.0, now=100.0) - 5.0) < 1e-6,
                 clock.lag(5.0, now=100.0))
    short = pipeline.StreamClock()
    short.marks = collections.deque(maxlen=3)
    for step in range(1, 11):
        short.advance(0.1, now=step * 0.1)
    checks.check("audio older than the marks kept is unmeasured, not "
                 "guessed", short.lag(0.2, now=2.0) is None)
    checks.check("the clock never reports a line ahead of the room",
                 clock.lag(12.0, now=121.0) == 0.0,
                 clock.lag(12.0, now=121.0))

    stamped = pipeline.Stamped(FakeSource())
    for _ in range(3):
        await stamped.read()
    checks.check("the capture wrapper counts the audio it reads",
                 abs(stamped.clock.position - 0.3) < 1e-9,
                 stamped.clock.position)
    checks.check("and answers for the device about everything else",
                 stamped.encoding == "pcm" and stamped.reads == 3)
    checks.check("between runs there is no source and so no lag",
                 pipeline.stream_lag(None, 1.0) is None)

    checks.section("How loud the audio was")
    meter = pipeline.LevelMeter()
    checks.check("a meter nobody fed has measured nothing, not silence",
                 meter.take() == (None, 0.0, 0.0))
    # A tenth of a second alternating at half scale: RMS and peak 0.5.
    meter.add(array.array("h", [16384, -16384] * 800).tobytes())
    seconds, rms, peak = meter.take()
    checks.check("a chunk at half scale reads -6 dBFS",
                 abs(seconds - 0.1) < 1e-9
                 and pipeline.decibels(rms) == -6.0
                 and pipeline.decibels(peak) == -6.0,
                 (seconds, rms, peak))
    checks.check("and taking a reading starts the next one empty",
                 meter.take() == (None, 0.0, 0.0))
    # One loud second and three silent ones are quiet on average, which
    # is the weighting a sender's one-second reports depend on.
    meter.merge(1.0, 0.5, 0.9)
    meter.merge(3.0, 0.0, 0.0)
    seconds, rms, peak = meter.take()
    checks.check("readings from a sender are weighted by their length",
                 seconds == 4.0 and abs(rms - 0.25) < 1e-9 and peak == 0.9,
                 (seconds, rms, peak))
    meter.merge(0.0, 0.0, 0.0)
    checks.check("a sender reporting no audio at all is a reading of zero",
                 meter.take() == (0.0, 0.0, 0.0))
    checks.check("silence is floored rather than minus infinity",
                 pipeline.decibels(0.0) == pipeline.LEVEL_FLOOR)

    stamped = pipeline.Stamped(FakeSource())
    await stamped.read()
    checks.check("the capture wrapper measures what it reads",
                 stamped.meter.take()[0] == 0.1)

    meter = pipeline.LevelMeter()
    meter.add(array.array("h", [16384, -16384] * 4000).tobytes())
    checks.check("the page has no level until a second has been heard",
                 meter.current() is None, meter.current())
    meter.add(array.array("h", [16384, -16384] * 4000).tobytes())
    checks.check("and then shows that second", meter.current() == -6.0,
                 meter.current())
    meter.take()
    checks.check("which the recording taking its totals leaves alone",
                 meter.current() == -6.0, meter.current())
    meter.merge(1.0, 0.01, 0.02)
    checks.check("a sender's report is a second the page can show",
                 meter.current() == -40.0, meter.current())
    checks.check("and an old reading shows nothing rather than a level",
                 meter.current(now=meter.live_at + 4.0) is None)

    checks.section("The server's own clock for a sender's audio")
    # Twenty milliseconds a page, the encoder's own page duration, after
    # the two pages that say what the stream is.
    stream = (ogg_page(0, b"OpusHead") + ogg_page(0, b"OpusTags")
              + b"".join(ogg_page(960 * step, bytes(60))
                         for step in range(100, 151)))
    pages = pipeline.OggGranules()
    # Split badly on purpose, the way a sender's reads fall.
    counted = sum(pages.feed(stream[at:at + 37])
                  for at in range(0, len(stream), 37))
    checks.check("Opus pages split across chunks are counted as audio",
                 abs(counted - 1.0) < 1e-9, counted)
    checks.check("a page that finishes no packet adds nothing",
                 pages.feed(ogg_page(-1, bytes(60))) == 0.0)
    checks.check("and a new encoder starting again is not counted backwards",
                 pages.feed(ogg_page(960, bytes(60))) == 0.0
                 and abs(pages.feed(ogg_page(1920, bytes(60))) - 0.02)
                 < 1e-9)

    arriving = capture.RemoteCapture(1.0, "opus")
    received = pipeline.Received(arriving)
    arriving.feed(stream)
    await received.read()
    checks.check("a sender's audio is clocked as it reaches the server",
                 abs(received.clock.position - 1.0) < 1e-9,
                 received.clock.position)
    checks.check("so lag on a hosted server is measured, not guessed",
                 pipeline.stream_lag(received, 1.0) is not None)
    raw = capture.RemoteCapture(1.0)
    received = pipeline.Received(raw)
    raw.feed(b"\x00\x00" * 1600)
    await received.read()
    checks.check("as is raw audio, by its length",
                 abs(received.clock.position - 0.1) < 1e-9
                 and received.encoding == "pcm",
                 received.clock.position)


def check_sentence_splitting(checks):
    """Where a line ends when a sentence finishes inside a fragment.

    The texts are shaped like the ones a recorded meeting produced, where
    the recognizer handed over a sentence and the start of the next in one
    result and the whole of it waited for the next endpoint.
    """
    checks.section("Sentences that finish inside a fragment")

    def segmenter(**overrides):
        return pipeline.Segmenter(ceiling_seconds=overrides.get("ceiling", 99),
                                  gap_seconds=0.6)

    split = segmenter()
    emitted = split.add("And you probably have some favorite scriptures "
                        "from this week. So I just", False, 0.0, 3.0)
    checks.check("a finished sentence goes out as soon as it arrives",
                 [unit[1] for unit in emitted]
                 == ["And you probably have some favorite scriptures "
                     "from this week."], emitted)
    checks.check("and says it was split out",
                 [unit[2] for unit in emitted] == ["sentence"], emitted)
    checks.check("the unfinished rest stays for its own ending",
                 split.parts == ["So I just"], split.parts)
    emitted = split.add("wanted to begin with a thought.", True, 3.0, 2.0)
    checks.check("and goes out whole when that ending comes",
                 [unit[1] for unit in emitted]
                 == ["So I just wanted to begin with a thought."], emitted)
    checks.check("every line has a number of its own",
                 split.seq == 2, split.seq)

    short = segmenter()
    emitted = short.add("Right? So when we are sustained in", False, 0.0, 2.0)
    checks.check("a sentence too short to be worth a call waits with the "
                 "next", emitted == [] and short.parts
                 == ["Right? So when we are sustained in"], emitted)

    several = segmenter()
    emitted = several.add(
        "This is a very interesting chapter indeed. This is where we learn "
        "about his calling or commission. Right?", True, 0.0, 6.0)
    checks.check("an endpoint closing several sentences gives a line each",
                 [unit[1] for unit in emitted]
                 == ["This is a very interesting chapter indeed.",
                     "This is where we learn about his calling or "
                     "commission. Right?"], emitted)
    checks.check("numbered in order, none sharing a number",
                 [unit[0] for unit in emitted] == [1, 2], emitted)

    title = segmenter()
    emitted = title.add("I went to see my good friend Dr. Smith about it and",
                        False, 0.0, 3.0)
    checks.check("an abbreviation is not the end of a sentence",
                 emitted == [], emitted)

    initial = segmenter()
    emitted = initial.add("In a talk from this last general conference by "
                          "sister Tamara W.", False, 0.0, 3.0)
    checks.check("a middle initial is not the end of a sentence",
                 emitted == [], emitted)
    emitted = initial.add("Runia, it's called The Sculpting of Our Souls.",
                          False, 3.0, 2.0)
    checks.check("so the surname stays on the line with the rest of the "
                 "name", [unit[1] for unit in emitted]
                 == ["In a talk from this last general conference by sister "
                     "Tamara W. Runia, it's called The Sculpting of Our "
                     "Souls."], emitted)
    inside = segmenter()
    emitted = inside.add("A talk by sister Tamara W. Runia. It's called the",
                         False, 0.0, 3.0)
    checks.check("nor is one inside a fragment",
                 [unit[1] for unit in emitted]
                 == ["A talk by sister Tamara W. Runia."], emitted)

    paused = segmenter()
    emitted = paused.add("you can go to that teacher council meeting the",
                         True, 0.0, 3.0)
    checks.check("an endpoint after a word no clause ends on holds the line",
                 emitted == [] and paused.parts, emitted)
    emitted = paused.add("half hour that you don't teach.", False, 3.0, 2.0)
    checks.check("until the rest of it arrives",
                 [unit[1] for unit in emitted]
                 == ["you can go to that teacher council meeting the half "
                     "hour that you don't teach."], emitted)
    brief = segmenter()
    emitted = brief.add("So right", True, 0.0, 1.0)
    checks.check("so does an endpoint after too few words to translate",
                 emitted == [], emitted)
    whole = segmenter()
    emitted = whole.add("we'll sing our closing hymn number one zero six",
                        True, 0.0, 3.0)
    checks.check("an endpoint after what could be a whole clause closes it",
                 [unit[2] for unit in emitted] == ["endpoint"], emitted)

    lowered = segmenter()
    emitted = lowered.add("we read about it in verse 16 of chapter one. and "
                          "then", False, 0.0, 3.0)
    checks.check("a full stop before a lowercase word is not a boundary",
                 emitted == [], emitted)

    ceiling = segmenter(ceiling=0)
    ceiling.add("not far north where the Samaritans are from, and then "
                "you have", False, 0.0, 3.0)
    emitted = ceiling.check_ceiling()
    checks.check("the ceiling cuts at the last comma",
                 [unit[1] for unit in emitted]
                 == ["not far north where the Samaritans are from,"],
                 emitted)
    checks.check("and keeps the clause after it",
                 ceiling.parts == ["and then you have"], ceiling.parts)

    bare = segmenter(ceiling=0)
    bare.add("the history of the house of Israel and Judah at", False,
             0.0, 3.0)
    emitted = bare.check_ceiling()
    checks.check("with no comma the ceiling takes everything",
                 [unit[1] for unit in emitted]
                 == ["the history of the house of Israel and Judah at"]
                 and bare.parts == [], emitted)

    early = segmenter(ceiling=0)
    early.add("you know, the history of the house of Israel", False,
              0.0, 3.0)
    emitted = early.check_ceiling()
    checks.check("a comma too early to leave a real clause is not a cut",
                 [unit[1] for unit in emitted]
                 == ["you know, the history of the house of Israel"],
                 emitted)


async def check_pipeline(checks):
    checks.section("Segmenter and the shared loops, printing to a terminal")
    translator = FakeTranslator()
    sink = pipeline.TerminalSink(["English", "French"], color=False)
    captured = io.StringIO()
    with contextlib.redirect_stdout(captured):
        await drive(sink, translator)
    printed = captured.getvalue()

    checks.check("two fragments become one sentence",
                 "[1] en" in printed and FIRST_SENTENCE in printed, printed)
    checks.check("a silent gap closes the sentence before the next turn",
                 ", gap): " + FIRST_SENTENCE in printed, printed)
    checks.check("the next turn is a sentence of its own",
                 "[2] en" in printed and SECOND_SENTENCE in printed, printed)
    checks.check("one fragment can close two sentences",
                 ", gap): " in printed and ", endpoint): " in printed, printed)
    checks.check("terminal punctuation closes a sentence too",
                 ", punctuation): " + THIRD_SENTENCE in printed, printed)
    checks.check("raw fragments are printed as they arrive",
                 printed.count("  . ") == 4, printed.count("  . "))
    checks.check("the corrected English shows as en*",
                 "en*: CORRECTED" in printed, printed)
    checks.check("the French line is printed",
                 "<French>" in printed and "fr: " in printed, printed)
    checks.check("English is never printed as a translated channel",
                 "\n     en: " not in printed, printed)
    checks.check("both outputs were requested in one call",
                 all(call["outputs"] == ["English", "French"]
                     for call in translator.calls), translator.calls)
    checks.check("the second call carries the first sentence as context",
                 translator.calls[1]["context"] == [FIRST_SENTENCE],
                 translator.calls[1]["context"])
    checks.check("the summary counts every call",
                 sink.summary().startswith("3 translated, 0 failed"),
                 sink.summary())

    check_sentence_splitting(checks)
    await check_stream_clock(checks)
    await check_early_finalize(checks)

    checks.section("The same loops, publishing to a Hub, one French reader")
    translator = FakeTranslator()
    session, hub = make_session(["French", "Swahili"], True,
                                ["English", "French"])
    session.translator = translator
    await drive(session, translator)
    english = [entry["text"] for entry in hub.buffers["English"]]
    french = [entry["text"] for entry in hub.buffers["French"]]

    checks.check("the English channel carries every sentence",
                 len(english) == 3, english)
    checks.check("the corrected English replaced the raw line",
                 english[0] == f"CORRECTED {FIRST_SENTENCE}", english)
    checks.check("the replacement reused the sequence number",
                 [entry["seq"] for entry in hub.buffers["English"]]
                 == [1, 2, 3],
                 [entry["seq"] for entry in hub.buffers["English"]])
    checks.check("the sentence closed by the gap survived the one behind it",
                 english[0].endswith(FIRST_SENTENCE)
                 and english[1].endswith(SECOND_SENTENCE), english)
    checks.check("the replacement is flagged as revised",
                 all(entry["revised"] for entry in hub.buffers["English"]))
    checks.check("the French channel got its translations",
                 french and french[0] == f"<French> {FIRST_SENTENCE}", french)
    checks.check("Swahili has no reader, so nothing was published to it",
                 len(hub.buffers["Swahili"]) == 0,
                 list(hub.buffers["Swahili"]))
    checks.check("Swahili was never requested from the model",
                 all("Swahili" not in call["outputs"]
                     for call in translator.calls), translator.calls)
    checks.check("the counters add up",
                 (session.stats["units"], session.stats["translated"],
                  session.stats["corrections"]) == (3, 3, 3), session.stats)

    checks.section("Nobody reading, and English needs no correction")
    translator = FakeTranslator()
    session, hub = make_session(["French"], False, [])
    session.translator = translator
    await drive(session, translator)
    checks.check("no model call was made at all", translator.calls == [],
                 translator.calls)
    checks.check("every sentence counted as skipped",
                 session.stats["skipped"] == 3, session.stats)
    checks.check("English is still published for whoever arrives later",
                 len(hub.buffers["English"]) == 3,
                 list(hub.buffers["English"]))

    checks.section("Grace, and leaving on purpose")
    hub = server.Hub(["French", "Swahili"])
    first = hub.subscribe("French", "phone")
    checks.check("a channel with a reader on it is wanted",
                 hub.wanted("French", 90))
    # The switch: the same phone opens another channel.
    hub.subscribe("Swahili", "phone")
    checks.check("switching language wakes the stream being left",
                 first.qsize() == 1 and first.get_nowait() is server.LEAVING)
    hub.unsubscribe("French", first, "phone", dropped=False)
    checks.check("the language left behind stops with no grace period",
                 not hub.wanted("French", 90), hub.last_seen["French"])
    checks.check("the language moved to is wanted", hub.wanted("Swahili", 90))
    # The drop: a phone whose screen locked, which grace exists for.
    gone = hub.subscribe("French", "other")
    hub.unsubscribe("French", gone, "other", dropped=True)
    checks.check("a reader who was cut off still gets the grace period",
                 hub.wanted("French", 90), hub.last_seen["French"])
    checks.check("grace runs out", not hub.wanted("French", 0))

    checks.section("The reader cap")
    hub = server.Hub(["French"], max_readers=2)
    checks.check("an empty hub is not full", not hub.full())
    hub.subscribe("English")
    second = hub.subscribe("French")
    checks.check("the cap counts across channels, not per channel",
                 hub.full() and hub.listener_total() == 2,
                 hub.listener_count())
    hub.unsubscribe("French", second)
    checks.check("a reader leaving frees the slot", not hub.full())
    unlimited = server.Hub(["French"])
    for _ in range(50):
        unlimited.subscribe("French")
    checks.check("no cap means no ceiling",
                 not unlimited.full(), unlimited.listener_total())

    checks.section("The language cap")
    everything = ["French", "Swahili", "Spanish", "Kurdish"]
    translator = FakeTranslator()
    # One client holding every channel, which needs no authentication and
    # is what the cap exists for.
    session, hub = make_session(everything, False, everything,
                                max_languages=2)
    session.translator = translator
    await drive(session, translator)
    checks.check("only the capped number of languages was ever requested",
                 all(len(call["outputs"]) <= 2
                     for call in translator.calls),
                 [call["outputs"] for call in translator.calls])
    checks.check("the languages over the cap still show English, not a gap",
                 all([entry["text"] for entry in hub.buffers[name]]
                     == SENTENCES for name in everything[2:]),
                 {name: list(hub.buffers[name]) for name in everything[2:]})
    checks.check("the cap is counted, so the operator can see it bite",
                 session.stats["capped"] == 3 * 2, session.stats)
    report = {row["name"]: row for row in session.language_report()}
    checks.check("the report marks which languages the cap dropped",
                 [name for name in everything if report[name]["capped"]]
                 == everything[2:], report)

    # A reader beats a stranger: the two languages with the most readers
    # win the slots, whatever order the language list is in.
    hub = server.Hub(everything)
    session = server.Session(
        fake_config(languages=everything, max_languages=2), hub)
    for channel in everything:
        hub.subscribe(channel)          # the stranger, on all four
    hub.subscribe("Kurdish")            # two real readers
    hub.subscribe("Spanish")
    checks.check("the languages with the most readers keep the slots",
                 session.demand()[0] == ["Spanish", "Kurdish"],
                 session.demand())

    # An operator override outranks reader counts, since it is the one
    # deliberate signal in the room.
    session.set_override("French", "on")
    checks.check("a language forced on keeps its slot regardless",
                 "French" in session.demand()[0],
                 session.demand())

    # The operator page shows this beside the total, and the language list
    # cannot carry it: every row there is a translated channel with a mode
    # to set, and English has no model call to switch off.
    hub.subscribe("English")
    hub.subscribe("English")
    status = session.status()
    checks.check("the operator page is told what the server loaded",
                 (status["glossary"], status["keyterms"]) == (0, 1),
                 (status["glossary"], status["keyterms"]))
    checks.check("the readers on English are counted for the operator",
                 status["english_listeners"] == 2
                 and all(row["name"] != "English"
                         for row in status["languages"]),
                 status["english_listeners"])

    checks.section("Translation failure falls back to English")
    translator = FakeTranslator(mode="fail")
    session, hub = make_session(["French"], False, ["French"])
    session.translator = translator
    await drive(session, translator)
    french = [entry["text"] for entry in hub.buffers["French"]]
    checks.check("French shows the English text rather than a gap",
                 french == SENTENCES, french)
    checks.check("every sentence counted as a failure",
                 session.stats["failures"] == 3, session.stats)

    checks.section("A translation slower than hold falls back too")
    translator = FakeTranslator(delay=0.4)
    session, hub = make_session(["French"], False, ["French"])
    session.translator = translator
    await drive(session, translator, hold=0.05)
    french = [entry["text"] for entry in hub.buffers["French"]]
    checks.check("French falls back to English on timeout",
                 french == SENTENCES, french)
    checks.check("every sentence counted as a timeout",
                 session.stats["timeouts"] == 3, session.stats)

    checks.section("A model answer that left a language out")
    translator = FakeTranslator(mode="empty")
    session, hub = make_session(["French"], False, ["French"])
    session.translator = translator
    await drive(session, translator)
    french = [entry["text"] for entry in hub.buffers["French"]]
    checks.check("an empty translation shows the English text, not a blank",
                 french == SENTENCES, french)
    checks.check("nothing was counted as a failure, because the call worked",
                 session.stats["failures"] == 0, session.stats)

    checks.section("A provider answer with no choices in it")
    translator = pipeline.Translator(
        base_url="http://invalid.invalid/v1", api_key="selftest",
        model="fake-model", languages=["French"], glossary="",
        max_tokens=100, timeout=1.0)
    posts = []

    async def empty_choices(url, json=None):
        posts.append(url)
        return SimpleNamespace(status_code=200, json=lambda: {"choices": []})

    translator.client.post = empty_choices
    try:
        raised = None
        try:
            await translator.translate("hello", [], ["French"])
        except BaseException as exc:
            raised = exc
    finally:
        await translator.close()
    checks.check("an empty choices array is a translation failure, "
                 "not a crash the sink cannot catch",
                 isinstance(raised, RuntimeError), repr(raised))
    checks.check("it was retried like any other bad answer",
                 len(posts) == 2, len(posts))

    checks.section("What a translation call reports it cost")
    translator = pipeline.Translator(
        base_url="http://invalid.invalid/v1", api_key="selftest",
        model="fake-model", languages=["French"], glossary="",
        max_tokens=100, timeout=1.0)
    # Gemini's compatible endpoint names no thinking tokens, and counts
    # them only in the total.
    reply = {"choices": [{"message": {"content": '{"French": "Bonjour"}'}}],
             "usage": {"prompt_tokens": 650, "completion_tokens": 20,
                       "total_tokens": 790}}

    async def answered(url, json=None):
        return SimpleNamespace(status_code=200, json=lambda: reply)

    translator.client.post = answered
    try:
        translations, _ = await translator.translate("hello", [], ["French"])
    finally:
        await translator.close()
    checks.check("the answer still reads like a dict of languages",
                 translations["French"] == "Bonjour", translations)
    checks.check("and carries the tokens it cost",
                 translations.usage == {"prompt_tokens": 650,
                                        "output_tokens": 20,
                                        "reasoning_tokens": 120,
                                        "cached_tokens": 0},
                 translations.usage)
    named = pipeline.usage_of({"usage": {
        "prompt_tokens": 650, "completion_tokens": 20, "total_tokens": 790,
        "completion_tokens_details": {"reasoning_tokens": 90},
        "prompt_tokens_details": {"cached_tokens": 512}}})
    checks.check("thinking and cached tokens are read where a provider "
                 "names them", named["reasoning_tokens"] == 90
                 and named["cached_tokens"] == 512, named)
    checks.check("a reply with no usage records none rather than zeros",
                 pipeline.usage_of({"choices": []}) is None)

    checks.section("Stopping the session mid translation")
    translator = FakeTranslator(delay=5.0)
    session, hub = make_session(["French"], False, ["French"])
    session.translator = translator
    queue = asyncio.Queue()
    unit = pipeline.Unit(1, FIRST_SENTENCE, 1.0, "gap")
    task = asyncio.create_task(
        translator.translate(FIRST_SENTENCE, [], ["French"]))
    await queue.put((unit, task, ["French"]))
    writer = asyncio.create_task(pipeline.publish(queue, session, 30.0))
    # Long enough for the writer to reach the shielded wait, which is the
    # only place the cancellation is interesting.
    await asyncio.sleep(0.05)
    writer.cancel()
    # Bounded rather than a plain await: a writer that swallows its own
    # cancellation goes straight back to waiting on the queue, and that
    # should fail this check rather than hang the whole suite.
    await asyncio.wait([writer], timeout=1.0)
    checks.check("the writer ended cancelled rather than reporting a failure",
                 writer.cancelled(), writer)
    if not writer.done():
        await queue.put(None)
        await asyncio.wait([writer], timeout=1.0)
    checks.check("no English fallback was pushed out after the stop",
                 list(hub.buffers["French"]) == [],
                 list(hub.buffers["French"]))
    checks.check("stopping is not counted as a translation failure",
                 session.stats["failures"] == 0, session.stats)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


    checks.section("Audio that arrives over a control socket")
    socket = FakeSocket([])
    # A grace and a tick measured in fractions of a second rather than the
    # thirty seconds and three a meeting uses, so the gap and the giving up
    # both happen while the suite is still running.
    remote = capture.RemoteCapture(0.5, tick=0.05)
    for _ in range(3):
        remote.feed(b"\x00\x00" * capture.CHUNK_FRAMES)
    pump = asyncio.create_task(pipeline.pump_audio(remote, socket))
    await asyncio.sleep(0.2)
    checks.check("what the sender sent reaches the recognizer",
                 socket.audio_chunks == 3, socket.audio_chunks)
    checks.check("a gap is a KeepAlive, not silence and not an ending",
                 socket.keepalives >= 1 and not pump.done(),
                 f"{socket.keepalives} keepalives, ended={pump.done()}")
    checks.check("and no silence was invented to fill it",
                 socket.audio_chunks == 3, socket.audio_chunks)
    await asyncio.wait([pump], timeout=2.0)
    checks.check("a gap longer than the grace ends the stream",
                 pump.done(), "the pump is still waiting on a sender "
                              "that stopped sending")
    checks.check("and the recognizer is told the stream is over",
                 socket.close_stream_sent, socket.close_stream_sent)

    checks.section("A local microphone that stops delivering")

    class StalledSource:
        """A device that has gone quiet the way a muted one does."""

        encoding = "pcm"

        def __init__(self):
            self.release = asyncio.Event()
            self.reads = 0
            self.cancelled = False

        async def read(self):
            self.reads += 1
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            self.release.clear()
            return b"\x00\x00" * capture.CHUNK_FRAMES

    stalled = StalledSource()
    socket = FakeSocket([])
    original_stall = pipeline.STALL_KEEPALIVE
    pipeline.STALL_KEEPALIVE = 0.05
    try:
        pump = asyncio.create_task(pipeline.pump_audio(stalled, socket))
        await asyncio.sleep(0.2)
        checks.check("a stalled read sends KeepAlives to hold the socket",
                     socket.keepalives >= 2 and socket.audio_chunks == 0,
                     f"{socket.keepalives} keepalives, "
                     f"{socket.audio_chunks} chunks")
        checks.check("while waiting on the same read, not a new one",
                     stalled.reads == 1, stalled.reads)
        stalled.release.set()
        await asyncio.sleep(0.02)
        checks.check("and the audio that read returns still goes up",
                     socket.audio_chunks == 1, socket.audio_chunks)
        pump.cancel()
        await asyncio.gather(pump, return_exceptions=True)
        checks.check("stopping cancels the read left waiting",
                     stalled.cancelled, "the read was left pending")
        checks.check("and still tells the recognizer the stream is over",
                     socket.close_stream_sent)
    finally:
        pipeline.STALL_KEEPALIVE = original_stall

    clock = pipeline.StreamClock()
    checks.check("a device that never delivered is silent since it opened",
                 clock.silent_for(now=clock.opened + 4.0) == 4.0,
                 clock.silent_for(now=clock.opened + 4.0))
    clock.advance(0.1, now=100.0)
    checks.check("and one that did is silent since its last chunk",
                 clock.silent_for(now=107.5) == 7.5,
                 clock.silent_for(now=107.5))

    session, _ = make_session(["French"], True, [])
    session.run_source = SimpleNamespace(clock=pipeline.StreamClock())
    session.run_source.clock.advance(0.1, now=time.monotonic() - 9.0)
    session.state = "running"
    checks.check("the operator page is told how long the microphone has "
                 "been silent", 8.5 < session.status()["no_audio_for"] < 10,
                 session.status()["no_audio_for"])
    session.run_source.meter = pipeline.LevelMeter()
    session.run_source.meter.merge(1.0, 0.5, 0.5)
    checks.check("the operator page is told the feed's level",
                 session.status()["level"] == -6.0,
                 session.status()["level"])
    session.last_words = time.monotonic() - 30.0
    checks.check("and how long since the recognizer returned words",
                 29 < session.status()["no_words_for"] < 31,
                 session.status()["no_words_for"])
    session.recognized(False)
    checks.check("which a result with no words does not reset",
                 session.status()["no_words_for"] > 29,
                 session.status()["no_words_for"])
    session.recognized(True)
    checks.check("and one with words does",
                 session.status()["no_words_for"] < 1,
                 session.status()["no_words_for"])
    session.state = "stopped"
    checks.check("and nothing once the session has stopped",
                 session.status()["no_audio_for"] == 0
                 and session.status()["level"] is None
                 and session.status()["no_words_for"] == 0,
                 session.status())
    page = (ROOT / "src" / "static" / "operator.html").read_text()
    found = re.search(r"const QUIET_LEVEL = (-?\d+);", page)
    checks.check("the page calls a feed quiet where the report does",
                 found and int(found.group(1)) == record.QUIET_DB,
                 found and found.group(1))

    checks.section("What the recognizer is told it is being sent")
    settings = {"asr_model": "nova-3", "endpointing": 400}
    pcm = pipeline.build_asr_url(settings, ["Kalema"], "pcm")
    opus = pipeline.build_asr_url(settings, ["Kalema"], "opus")
    checks.check("a local device is declared as linear16 at 16 kHz",
                 "encoding=linear16" in pcm and "sample_rate=16000" in pcm,
                 pcm)
    # A container carries its own rate, and a second declaration here is a
    # second thing to get wrong.
    checks.check("compressed audio is not declared as raw samples",
                 "linear16" not in opus and "sample_rate" not in opus, opus)
    checks.check("everything else about the URL is the same either way",
                 "endpointing=400" in opus and "keyterm=Kalema" in opus
                 and "interim_results=true" in opus, opus)
    declared = (capture.ParecCapture.encoding,
                capture.SoundDeviceCapture.encoding,
                capture.RemoteCapture(1.0).encoding,
                capture.RemoteCapture(1.0, "opus").encoding)
    checks.check("every backend says what it yields",
                 declared == ("pcm", "pcm", "pcm", "opus"), declared)

    checks.section("Compressing the audio on the way out")
    plain = FakeSource()
    same, note = await pipeline.open_encoder(plain, "pcm")
    checks.check("asking for pcm starts no encoder at all",
                 same is plain and not note, note)
    remote = capture.RemoteCapture(1.0, "opus")
    same, note = await pipeline.open_encoder(remote, "opus")
    # Audio from a sender arrives already compressed, and a second codec
    # pass on the server would cost latency to save nothing.
    checks.check("audio that arrived encoded is not encoded again",
                 same is remote and not note, note)

    original = pipeline.ENCODER_COMMAND
    try:
        pipeline.ENCODER_COMMAND = ["transept-no-such-encoder"]
        source, note = await pipeline.open_encoder(FakeSource(), "opus")
        checks.check("a machine with no ffmpeg falls back to raw audio",
                     source.encoding == "pcm", source.encoding)
        checks.check("and says so rather than failing to start",
                     "ffmpeg" in note, note or "nothing was said")
        # An ffmpeg built without libopus exits during setup rather than on
        # the first chunk, which is why the fallback is decided by watching
        # the process rather than by finding the binary.
        pipeline.ENCODER_COMMAND = [sys.executable, "-c",
                                    "raise SystemExit(1)"]
        source, note = await pipeline.open_encoder(FakeSource(), "opus")
        checks.check("an encoder that will not run falls back too",
                     source.encoding == "pcm" and note, note or "no note")
    finally:
        pipeline.ENCODER_COMMAND = original

    if shutil.which("ffmpeg"):
        source, note = await pipeline.open_encoder(FakeSource(), "opus")
        checks.check("where ffmpeg exists, the source is wrapped in Opus",
                     source.encoding == "opus" and not note, note)
        first = b""
        try:
            first = await asyncio.wait_for(source.read(), timeout=5)
        except TimeoutError:
            pass
        finally:
            await source.close()
        # The Ogg capture pages Deepgram reads the rate and channel count
        # out of, which is why build_asr_url declares neither for Opus.
        checks.check("and what comes out of it is an Ogg stream",
                     first[:4] == b"OggS", first[:16])
        # Kept because a sender's socket does not exist when ffmpeg writes
        # them, and a recognizer handed the rest without them returns
        # nothing at all while every light stays green.
        checks.check("and the encoder keeps the pages that say what it is",
                     b"OpusHead" in source.headers, source.headers[:32])
        checks.check("which is the front of the stream, not a copy of it",
                     source.headers and first.startswith(source.headers),
                     f"{len(source.headers)} of {len(first)} bytes")
    else:
        # Not a pass dressed up as one: nothing was exercised, and the
        # machine that runs a meeting is the one that has to be checked.
        print("  [SKIP] the Opus path needs ffmpeg, which is not installed")

    checks.section("Reading the front of an Ogg stream")
    # Two pages and the beginning of a third, built here rather than taken
    # from ffmpeg, so this runs on a machine that has none.
    head = ogg_page(0, b"OpusHead" + bytes(11))
    tags = ogg_page(0, b"OpusTags" + bytes(20))
    audio = ogg_page(960, b"\x01\x02\x03\x04")
    pages = pipeline.OggHeaders()
    pages.feed(head + tags + audio)
    checks.check("the two pages that describe a stream are kept",
                 pages.headers == head + tags, pages.headers[:24])
    checks.check("and the audio behind them is not",
                 not pages.headers.endswith(audio), len(pages.headers))
    # A page is whatever length its segment table says, and a socket splits
    # a stream wherever it likes, so the two boundaries do not line up.
    split = pipeline.OggHeaders()
    whole = head + tags + audio
    for at in range(0, len(whole), 7):
        split.feed(whole[at:at + 7])
    checks.check("pages split across chunks are read all the same",
                 split.headers == head + tags, split.headers[:24])
    partial = pipeline.OggHeaders()
    partial.feed(head)
    checks.check("one page alone is not yet an answer",
                 partial.headers == b"", partial.headers)
    noise = pipeline.OggHeaders()
    for _ in range(50):
        noise.feed(b"\x00\x11\x22\x33" * 256)
    checks.check("a stream that is not Ogg at all is not accumulated",
                 noise.headers == b"" and len(noise.buffer) <= 3,
                 len(noise.buffer))
    pages.reset()
    checks.check("and a new encoder is a new stream, not these pages",
                 pages.headers == b"", pages.headers)

    checks.section("Choosing the remote backend")
    checks.check("remote is a backend, and never the one auto picks",
                 capture.resolve_backend("remote") == "remote"
                 and capture.resolve_backend("auto") != "remote",
                 capture.resolve_backend("auto"))
    raised = ""
    try:
        await capture.open_capture("some device", "remote")
    except capture.CaptureError as exc:
        raised = str(exc)
    checks.check("a remote source cannot be opened by naming a device",
                 "control socket" in raised, raised or "nothing was raised")
    raised = ""
    try:
        capture.list_devices("remote")
    except capture.CaptureError as exc:
        raised = str(exc)
    # The sound hardware a hosted server could enumerate would be the
    # rented machine's, which is nobody's microphone.
    checks.check("and a hosted server offers no devices of its own",
                 "on the sender" in raised, raised or "a list was returned")


# -- a whole session, start to stop ------------------------------------------


async def check_session(checks):
    """Start and stop a real Session against a fake device and recognizer.

    This is the path that once raised NameError on every start. It reaches
    build_asr_url, the websocket, all three tasks, and the teardown, so a
    break anywhere in the wiring between the two files shows up here.
    """
    checks.section("A whole Session, start to stop")
    source = FakeSource()
    sockets = []

    def fake_connect(url, **kwargs):
        sock = FakeSocket(FRAGMENTS, stay_open=True)
        sock.url = url
        sockets.append(sock)
        return sock

    async def fake_open_capture(device, backend):
        return source

    original = (capture.open_capture, server.websockets.connect,
                server.Translator)
    capture.open_capture = fake_open_capture
    server.websockets.connect = fake_connect
    server.Translator = FakeTranslator
    try:
        hub = server.Hub(["French"])
        session = server.Session(fake_config(languages=["French"]), hub)
        hub.subscribe("English")
        hub.subscribe("French")

        started, message = await session.start("fake")
        checks.check("start was accepted", started, message)
        refused, message = await session.start("fake")
        checks.check("a second start is refused while one is alive",
                     refused is False, message)

        # Long enough for the socket to yield both results and for the
        # translation to come back, short enough not to slow the suite.
        await asyncio.sleep(0.6)

        checks.check("the session reached running",
                     session.state == "running", session.state)
        checks.check("no error was recorded", session.error is None,
                     session.error)
        # What the socket saw, not what the source produced: source.reads
        # alone stays above zero even if pump_audio forwards nothing.
        checks.check("audio reached the recognizer",
                     bool(sockets) and sockets[0].audio_chunks > 0,
                     f"{source.reads} read, "
                     f"{sockets[0].audio_chunks if sockets else 0} sent")
        # A failure to reach the recognizer at all is the interesting case,
        # so report it as failed checks rather than crashing on an empty list
        # and hiding everything after it.
        url = sockets[0].url if sockets else ""
        reached = url or "the recognizer was never reached"
        checks.check("the Deepgram URL carries the sample rate",
                     "sample_rate=16000" in url, reached)
        checks.check("the Deepgram URL carries the key term",
                     "keyterm=Kalema" in url, reached)
        checks.check("guesses are requested, to finish sentences early",
                     "interim_results=true" in url, reached)

        english = [entry["text"] for entry in hub.buffers["English"]]
        french = [entry["text"] for entry in hub.buffers["French"]]
        checks.check("English was published and then corrected in place",
                     english == [f"CORRECTED {name}" for name in SENTENCES],
                     english)
        checks.check("French was published",
                     french == [f"<French> {name}" for name in SENTENCES],
                     french)
        status = session.status()
        checks.check("status reports what the operator page needs",
                     status["state"] == "running" and status["units"] == 3
                     and status["median"] is not None, status)
        checks.check("nothing reconnected", session.stats["reconnects"] == 0,
                     session.stats["reconnects"])

        stopped, message = await session.stop()
        checks.check("stop was accepted", stopped, message)
        checks.check("the state is stopped", session.state == "stopped",
                     session.state)
        checks.check("the capture device was released", source.closed)
        checks.check("CloseStream was sent to the recognizer",
                     bool(sockets) and sockets[0].close_stream_sent,
                     reached)
        checks.check("a second stop is refused",
                     (await session.stop())[0] is False)

        # The same start again, on a source that says it is sending
        # something else. What the recognizer is told has to follow the
        # source rather than what a local device would have produced.
        sockets.clear()
        source.encoding = "opus"
        session = server.Session(fake_config(languages=["French"]),
                                 server.Hub(["French"]))
        await session.start("fake")
        await asyncio.sleep(0.2)
        url = sockets[0].url if sockets else "the recognizer was never reached"
        checks.check("a session declares the encoding its source yields",
                     "linear16" not in url and "sample_rate" not in url, url)
        await session.stop()
        source.encoding = "pcm"
    finally:
        (capture.open_capture, server.websockets.connect,
         server.Translator) = original

    checks.section("A reconnect keeps counting where the last stream ended")
    # What the supervisor does: a fresh Segmenter per _run_once, one Hub for
    # the whole meeting. A segmenter that restarted at 1 would make the Hub
    # revise the opening lines, and reader.html would rewrite those lines in
    # place and return without appending, so the feed stops growing while the
    # operator page still shows units climbing.
    hub = server.Hub(["French"])
    session = server.Session(fake_config(languages=["French"]), hub)
    for texts in (["first line of the meeting.", "second line."],
                  ["something said after the reconnect."]):
        segmenter = pipeline.Segmenter(4.0, 0.6, start_seq=session.last_seq)
        for text in texts:
            for taken in segmenter.add(text, True, 0.0, 1.0):
                seq, body = taken[0], taken[1]
                session.last_seq = max(session.last_seq, seq)
                hub.publish("English", seq, body)
    english = [(entry["seq"], entry["text"])
               for entry in hub.buffers["English"]]
    checks.check("the line from before the drop is still there",
                 english[0][1] == "first line of the meeting.", english)
    checks.check("the line after the drop was added, not substituted",
                 len(english) == 3, english)
    checks.check("sequence numbers run on across the reconnect",
                 [seq for seq, _ in english] == [1, 2, 3], english)

    checks.section("Reconnect backoff after a run that was working")
    hub = server.Hub(["French"])
    session = server.Session(fake_config(languages=["French"]), hub)

    async def fails_at_once():
        raise RuntimeError("the far end hung up")

    session._run_once = fails_at_once
    session.state = "running"
    # A long second step, so a backoff that keeps climbing across a healthy
    # run stalls here and the count below stays low. HEALTHY_RUN of zero
    # makes every run count as healthy without waiting a real minute.
    backoff = (server.RECONNECT_BACKOFF, server.HEALTHY_RUN)
    server.RECONNECT_BACKOFF, server.HEALTHY_RUN = [0.01, 5.0], 0
    try:
        supervisor = asyncio.create_task(session._supervise())
        await asyncio.sleep(0.2)
        supervisor.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await supervisor
    finally:
        server.RECONNECT_BACKOFF, server.HEALTHY_RUN = backoff
    checks.check("a healthy run puts the backoff back to the short delay",
                 session.stats["reconnects"] >= 5,
                 session.stats["reconnects"])


    checks.section("One room, one sender")

    def hello(**fields):
        return {"type": "hello", "room": "chapel", "encoding": "pcm",
                "device": "a laptop in the room", "intent": "attach",
                **fields}

    def make_room(**overrides):
        hub = server.Hub(["French"])
        return server.ControlRoom(
            fake_config(languages=["French"], capture="remote",
                        room="chapel", control_grace=0.4, **overrides), hub)

    room = make_room()
    first, second = object(), object()
    taken, message = room.attach(first, hello())
    checks.check("the first sender gets the room", taken, message)
    taken, message = room.attach(second, hello())
    checks.check("a second sender is refused while the first holds it",
                 taken is False and "Another sender" in message, message)

    # A config copied from the room next door and not edited, which is the
    # mistake that would otherwise put one room's audio in another's
    # transcript.
    taken, message = make_room().attach(first, hello(room="classroom"))
    checks.check("a sender pointed at the wrong room is refused",
                 taken is False and "chapel" in message, message)
    taken, message = make_room().attach(first, hello(encoding="flac"))
    checks.check("and so is an encoding the recognizer is never told about",
                 taken is False and "flac" in message, message)

    source = await room.open_source()
    room.feed(b"\x01\x02")
    chunk = await asyncio.wait_for(source.read(), 1.0)
    checks.check("audio off the socket reaches the session's source",
                 chunk == b"\x01\x02", chunk)
    checks.check("which carries the encoding the sender declared",
                 source.encoding == "pcm", source.encoding)

    room.session.state = "running"
    await room.release(first)
    await asyncio.sleep(0.1)
    checks.check("a dropped socket does not stop the meeting at once",
                 room.session.state == "running", room.session.state)
    # A laptop that would take a running meeting over rather than rejoin
    # it, which is worth refusing out loud even with the token in hand.
    taken, message = room.attach(object(), hello(intent="start"))
    checks.check("a sender that means to start a running room is refused",
                 taken is False and "already running" in message, message)
    taken, message = room.attach(second, hello())
    checks.check("the sender that comes back rejoins the session it left",
                 taken and room.session.state == "running", message)

    # Two blips in a row, which one bad hotspot produces all morning. The
    # second drop has to be given a whole grace of its own: the countdown
    # the first one armed is still running otherwise, and it would stop the
    # session a fraction of the way into the second window.
    await asyncio.sleep(0.1)
    await room.release(second)
    await asyncio.sleep(0.3)
    checks.check("a second blip gets a grace of its own, not what is left "
                 "of the first one's",
                 room.session.state == "running", room.session.state)

    await asyncio.sleep(0.3)
    checks.check("a sender that does not come back stops the session",
                 room.session.state == "stopped", room.session.state)
    checks.check("and the operator is told why",
                 "disconnected" in (room.session.error or ""),
                 room.session.error)


async def check_local_encoder(checks):
    """A session capturing its own audio compresses it on the way out.

    Compression is not only for the hosted deployment: the leg that failed
    in a real building is audio leaving it, and that leg is the same one a
    laptop with a sound card uses.
    """
    checks.section("A local device on its way to the recognizer")
    hub = server.Hub(["French"])
    session = server.Session(fake_config(encoding="opus"), hub)
    session.device = "a microphone"
    captured = FakeSource()

    async def opens(device, backend):
        return captured

    original_open = capture.open_capture
    original_command = pipeline.ENCODER_COMMAND
    try:
        capture.open_capture = opens
        # A stand-in for ffmpeg rather than ffmpeg, so this check runs the
        # same on a machine that has one and a machine that does not.
        pipeline.ENCODER_COMMAND = [
            sys.executable, "-c",
            "import shutil, sys; "
            "shutil.copyfileobj(sys.stdin.buffer, sys.stdout.buffer)"]
        source = await session._open_source()
        checks.check("a session opening a local device wraps it for the way "
                     "out", source.encoding == "opus", source.encoding)
        checks.check("and still reads lag off the device, not the encoder",
                     isinstance(source.clock, pipeline.StreamClock),
                     source.clock)
        await source.close()
        checks.check("and closing it closes the device underneath",
                     captured.closed, "the device was left open")

        session.config["encoding"] = "pcm"
        plain = await session._open_source()
        checks.check("asking for raw audio hands the device over unencoded",
                     plain.encoding == "pcm" and plain.source is captured,
                     plain)
        checks.check("with its capture clock around it",
                     isinstance(plain.clock, pipeline.StreamClock),
                     plain.clock)
    finally:
        capture.open_capture = original_open
        pipeline.ENCODER_COMMAND = original_command


async def check_sender_proxy(checks):
    """The sender's proxy, which is the other implementation of Session.

    controls.py serves one operator page against an object with four
    methods, and this is the object when the audio is captured in one
    building and recognized in another. Nothing here touches a socket:
    what is checked is what the page gets before there is a server, and
    the two ways this laptop and that server can come to disagree.
    """
    checks.section("The sender's half of the operator page")
    remote = sender.Remote({"capture": "auto", "reader_url": ""},
                           "chapel", "pcm")
    status = remote.status()
    # Against a real Session rather than against the sender's own table,
    # which would be a check comparing one thing with itself. The page
    # renders whatever Session produces, without checking, so a field that
    # exists there and not here shows up in the room as the word undefined.
    expected = make_session(["French"], True, [])[0].status()
    missing = [key for key in expected if key not in status]
    checks.check("a sender with no server still answers the page in full",
                 not missing, missing)
    checks.check("and says the server is not there rather than saying "
                 "nothing", status["state"] == "stopped"
                 and "connect" in (status["error"] or ""), status)
    remote.source = SimpleNamespace(clock=pipeline.StreamClock())
    remote.source.clock.advance(0.1, now=time.monotonic() - 12.0)
    checks.check("a sender reports its own microphone going silent",
                 remote.status()["no_audio_for"] > 11,
                 remote.status()["no_audio_for"])
    remote.source.meter = pipeline.LevelMeter()
    remote.source.meter.merge(1.0, 0.1, 0.1)
    checks.check("and its own level rather than the server's",
                 remote.status()["level"] == -20.0,
                 remote.status()["level"])
    remote.source = None
    ok, message = remote.set_override("French", "on")
    checks.check("a language button with no socket is refused, not dropped",
                 ok is False and "connect" in message, message)
    ok, message = await remote.start("")
    checks.check("starting with no device chosen asks for one",
                 ok is False, message)
    ok, message = await remote.stop()
    checks.check("stopping what never started says so", ok is False, message)

    opened = []

    async def opens(device, backend):
        opened.append(device)
        return FakeSource()

    async def refuses(device, backend):
        opened.append(device)
        raise capture.CaptureError("no such device")

    original = capture.open_capture
    capture.open_capture = opens
    try:
        ok, message = await remote.start("a microphone")
        checks.check("starting opens the microphone on this laptop first",
                     ok and opened == ["a microphone"], message)
        hello = await remote.hello()
        # Start is not a message. A sender says what it means in its
        # hello, because a reconnecting one has to say whether it is
        # beginning a meeting or rejoining one.
        checks.check("and asks for the session in a hello, not a message",
                     (hello["intent"], hello["device"], hello["room"])
                     == ("start", "a microphone", "chapel"), hello)
        checks.check("which is why a start needs a connection of its own",
                     remote.redial.is_set())
        checks.check("a sender capturing with no socket reads as "
                     "reconnecting",
                     remote.status()["state"] == "reconnecting",
                     remote.status())

        await remote.update({"state": "stopped", "device": "a microphone"})
        checks.check("a stopped status just after Start is the session that "
                     "had not begun yet",
                     remote.source is not None, "the microphone was closed")
        await remote.update({"state": "running", "device": "a microphone"})
        checks.check("once it is running the next hello is an attach",
                     (await remote.hello())["intent"] == "attach",
                     await remote.hello())

        remote.started = time.monotonic() - sender.START_GRACE - 1
        await remote.update({"state": "stopped", "device": "a microphone"})
        # The idle watchdog stops sessions, and a microphone left open
        # here would go on reading a room nobody is listening to.
        checks.check("a session that stopped on its own closes the "
                     "microphone here", remote.source is None, remote.source)

        await remote.update({"state": "running", "device": "a microphone"})
        checks.check("a sender that finds a meeting already running "
                     "rejoins it",
                     remote.source is not None
                     and opened[-1] == "a microphone", opened)
        await remote.release()

        capture.open_capture = refuses
        await remote.update({"state": "running", "device": "gone"})
        attempts = len(opened)
        await remote.update({"state": "running", "device": "gone"})
        checks.check("a device that will not reopen is reported once, not "
                     "once a second",
                     len(opened) == attempts
                     and "no such device" in (remote.error or ""),
                     f"{len(opened) - attempts} more attempts, "
                     f"{remote.error}")
        remote.socket = object()
        # The page shows an error only while a session is not running, so
        # a meeting this laptop is not feeding has to read as one.
        checks.check("and a meeting nobody here is feeding does not read "
                     "as a healthy one",
                     remote.status()["state"] == "reconnecting",
                     remote.status())
        remote.socket = None
    finally:
        capture.open_capture = original
        await remote.release()

    checks.section("The address this sender dials")
    # Built from public_url and the room rather than configured outright,
    # so the hostname and the room name are each written down once.
    checks.check("the control address is the room's prefix and /control",
                 sender.control_address("https://transept.example/", "chapel")
                 == "wss://transept.example/chapel/control",
                 sender.control_address("https://transept.example/",
                                        "chapel"))
    checks.check("however the room and the base are punctuated",
                 sender.control_address("https://transept.example",
                                        "/chapel/")
                 == "wss://transept.example/chapel/control")
    # One room at the root of a hostname, which is what a deployment looks
    # like before a second room joins it.
    checks.check("an unnamed room dials the root",
                 sender.control_address("https://chapel.example/", "")
                 == "wss://chapel.example/control")
    # The scheme a phone opens the page with is the one the proxy
    # terminates, so the socket follows it rather than being spelled out.
    checks.check("a plain http server is dialed as ws",
                 sender.control_address("http://127.0.0.1:8080/", "chapel")
                 == "ws://127.0.0.1:8080/chapel/control")
    # Rather than dialing "/control" on nowhere, which the sender would
    # report as a server refusing it rather than as a setting nobody filled
    # in, and would go on reporting once a backoff for the rest of the hour.
    checks.check("no public_url means no address at all",
                 sender.control_address("", "chapel") == "")


# -- the running server ------------------------------------------------------


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def wait_for_port(port, process, timeout=20.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return False
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def request(port, path, method="GET", body=None, timeout=5.0, token=None):
    """Returns (status, body text). A 4xx is an answer here, not an error."""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    if token:
        headers["X-Subtitle-Token"] = token
    call = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data,
                                  headers=headers, method=method)
    try:
        with urllib.request.urlopen(call, timeout=timeout) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8")


def open_stream(port, token, channel="English", timeout=5.0):
    """A raw event stream held open, so the cap can be reached on purpose."""
    sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    sock.sendall(f"GET /stream/{channel}?token={token} HTTP/1.1\r\n"
                 f"Host: x\r\n"
                 f"Accept: text/event-stream\r\n\r\n".encode())
    sock.recv(64)
    return sock


def sse_preamble(port, token, channel, writes=2, timeout=2.0):
    """The stream's first writes, plus its content type.

    read1 rather than read, because each write is one chunk of the response
    and a write that never comes has to be a short read rather than a hang:
    read(n) on a stream that stays open blocks until the socket times out and
    then throws away what did arrive, which would turn a failed check into a
    traceback that takes the rest of the section with it.
    """
    url = f"http://127.0.0.1:{port}/stream/{channel}?token={token}"
    with urllib.request.urlopen(url, timeout=timeout) as response:
        head = ""
        try:
            for _ in range(writes):
                head += response.read1(4096).decode("utf-8")
        except TimeoutError:
            pass
        return response.headers.get("Content-Type"), head


def drain(process):
    """(lines, thread): collect the server's output in the background.

    One reader has to own the stream. readline here plus communicate later
    loses the lines between them, since readline buffers from the pipe and
    communicate reads the raw descriptor.
    """
    lines = []
    thread = threading.Thread(
        target=lambda: lines.extend(iter(process.stdout.readline, "")),
        daemon=True)
    thread.start()
    return lines, thread


@contextlib.contextmanager
def running_server(arguments, **tokens):
    """server.py in a directory of its own, stopped again on the way out.

    A directory of its own because server.py records itself in the one it
    starts in, and a selftest that overwrote the record of a real server
    would leave a running meeting with nothing able to stop it. The keys
    are fakes and the tokens empty, so each is minted, unless named here.
    Yields (process, output lines, home), and afterwards sets forced on the
    process if it had to be killed rather than stopping when asked.
    """
    home = Path(tempfile.mkdtemp())
    environment = dict(
        os.environ, DEEPGRAM_API_KEY="selftest", LLM_API_KEY="selftest",
        LLM_BASE_URL="http://invalid.invalid/v1", READER_TOKEN="",
        OPERATOR_TOKEN="", CONTROL_TOKEN="")
    environment.update(tokens)
    process = subprocess.Popen(
        [sys.executable, "-u", str(SRC / "server.py"),
         "--model", "selftest-model", "--host", "127.0.0.1", *arguments],
        cwd=home, env=environment, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True)
    lines, reader = drain(process)
    process.forced = False
    try:
        yield process, lines, home
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.forced = True
            process.kill()
            process.wait()
        reader.join(timeout=2)


def banner_address(lines, label, timeout=10.0):
    """One of the two addresses, once the banner prints it.

    Both tokens are minted per run, so the banner is the only place to get
    them, and reading it here is what the volunteer does.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for line in list(lines):
            if line.startswith(label + ":"):
                return line.strip()
        time.sleep(0.05)
    return ""


def check_authorization(checks):
    """The token rule, and the shape a refusal keeps for the page.

    Which routes apply it is checked against the running server, where a
    route left out of add_routes would show.
    """
    checks.section("The token rule")
    checks.check("a configured token refuses a request without one",
                 not controls.authorized(StubRequest("secret")))
    checks.check("the token is accepted in the query string",
                 controls.authorized(StubRequest("secret", query="secret")))
    checks.check("the token is accepted in a header",
                 controls.authorized(StubRequest("secret", header="secret")))
    checks.check("a wrong token is refused",
                 not controls.authorized(StubRequest("secret", query="wrong")))
    # The two listeners hold different tokens under the same key, which is
    # the whole of what keeps a card handed to the room off the controls.
    checks.check("the token for the other listener is just a wrong token",
                 not controls.authorized(StubRequest("operator-secret",
                                                   query="reader-secret")))

    def refuses(query):
        # Report a raise as a failed check, not as a crashed run.
        try:
            return not controls.authorized(StubRequest("secret", query=query))
        except TypeError:
            return False

    # Guards the encode, not the constant-time comparison: reverting to ==
    # also passes this, since == was never the unsafe part.
    checks.check("a token outside ASCII is refused rather than raising",
                 refuses("caf\u00e9"))

    # The page polls this every two seconds and reads ok and message off
    # a refusal; without the message it shows a blank panel.
    session = make_session(["French"], True, [])[0]
    refused = asyncio.run(controls.api_status(
        StubRequest("secret", session=session)))
    payload = json.loads(refused.text)
    checks.check("a refused status says why, for the page to show",
                 payload.get("ok") is False and payload.get("message"),
                 payload)
    # The reader page destructures channels off this, so a refusal that
    # dropped the key would throw before the page could show anything.
    refused = asyncio.run(server.api_channels(StubRequest("secret")))
    payload = json.loads(refused.text)
    checks.check("a refused channel list keeps the shape the page reads",
                 payload.get("channels") == [] and "error" in payload,
                 payload)


def check_example_config(checks):
    """The tables in pipeline.py and config.example.toml still agree.

    They are read side by side whenever a setting is added, so a key in one
    and not the other, or the same keys in a different order, is a defect
    even though nothing breaks at runtime.
    """
    checks.section("config.example.toml against SETTINGS and SECRETS")
    section, listed = None, []
    example = ROOT / "examples" / "config.example.toml"
    for line in example.read_text().splitlines():
        if line.startswith("["):
            section = line.strip("[]")
        elif line[:1].isalpha() and "=" in line:
            listed.append((section, line.split("=", 1)[0].strip()))

    keys = [key for where, key in listed if where == "keys"]
    checks.check("every key in [keys] is a SECRETS row, in the same order",
                 keys == [key for _, key, _ in pipeline.SECRETS],
                 f"{keys} against SECRETS")
    settings = [pair for pair in listed if pair[0] != "keys"]
    expected = [(where, key) for _, where, key, _, _ in pipeline.SETTINGS]
    only_file = [pair for pair in settings if pair not in expected]
    only_table = [pair for pair in expected if pair not in settings]
    if only_file or only_table:
        detail = (f"only in the file: {only_file}, "
                  f"only in SETTINGS: {only_table}")
    else:
        # Same rows in a different order, which the lists above cannot show.
        detail = next((f"SETTINGS has {table} where the file has {found}"
                       for table, found in zip(expected, settings, strict=True)
                       if table != found), "")
    checks.check("every other setting is a SETTINGS row, in the same order",
                 settings == expected, detail)


def check_config_merge(checks):
    """Several config files, merged left to right.

    What a hosted server runs on: one file of tuning every room shares, and
    a file per room holding only what that room has to say for itself. The
    merge is key by key inside a section, because a room naming its own
    ports must not lose the shared [server] settings beside them.
    """
    checks.section("Config files merging left to right")
    home = Path(tempfile.mkdtemp())
    shared = home / "shared.toml"
    shared.write_text('[server]\nhost = "0.0.0.0"\nreader_port = 8080\n'
                      'public_url = "https://transept.example/"\n'
                      '[keys]\nllm_api_key = "shared-key"\n'
                      '[languages]\navailable = ["French", "Swahili"]\n')
    room = home / "room.toml"
    room.write_text('[server]\nreader_port = 8090\nroom = "chapel"\n'
                    '[keys]\nreader_token = "room-token"\n')

    merged = pipeline.load_config(shared, room)
    settings = pipeline.resolve(SimpleNamespace(), merged)
    checks.check("the room's port beats the shared one",
                 settings["reader_port"] == 8090, settings["reader_port"])
    checks.check("and the shared host survives beside it, so a section is "
                 "merged key by key rather than replaced",
                 settings["host"] == "0.0.0.0", settings["host"])
    checks.check("a value only the shared file names is inherited",
                 settings["public_url"] == "https://transept.example/",
                 settings["public_url"])
    checks.check("a value only the room names is read",
                 settings["room"] == "chapel", settings["room"])
    checks.check("a list is taken whole rather than merged item by item",
                 settings["languages"] == ["French", "Swahili"],
                 settings["languages"])

    with cleared_secrets():
        keys = pipeline.load_keys(merged)
    checks.check("the keys merge the same way, each file's own rows",
                 (keys["llm_key"], keys["reader_token"])
                 == ("shared-key", "room-token"), keys)

    # Precedence is the command line, then the last file, then earlier ones,
    # and a flag given is still a flag that wins over every file.
    flagged = pipeline.resolve(SimpleNamespace(reader_port=9999), merged)
    checks.check("a flag still beats every file",
                 flagged["reader_port"] == 9999, flagged["reader_port"])
    checks.check("the order is the order given, not the order on disk",
                 pipeline.resolve(SimpleNamespace(),
                                  pipeline.load_config(room, shared)
                                  )["reader_port"] == 8080)
    # The usual run names one file that may not exist yet, which has to
    # fall through to the defaults rather than stop the meeting.
    checks.check("a missing file among them is skipped, not an error",
                 pipeline.resolve(SimpleNamespace(), pipeline.load_config(
                     home / "no-such-file.toml", room))["reader_port"] == 8090)
    # A local run passes one file, or none, and the merge has to leave that
    # file exactly as it found it.
    checks.check("one file is that file and nothing besides",
                 pipeline.load_config(shared) == {
                     "server": {"host": "0.0.0.0", "reader_port": 8080,
                                "public_url": "https://transept.example/"},
                     "keys": {"llm_api_key": "shared-key"},
                     "languages": {"available": ["French", "Swahili"]}},
                 pipeline.load_config(shared))

    # The hosted layout: the shared file and the glossary it names one
    # level up, the room's file in a directory of its own, and the process
    # started somewhere else again. Read against the working directory, the
    # path found nothing, and a whole field test ran without the glossary.
    (home / "glossary.txt").write_text("Rivanna Ward\n")
    tuned = home / "tuned.toml"
    tuned.write_text('[files]\nglossary = "glossary.txt"\n'
                     'keyterms = "/etc/absolute.txt"\n'
                     '[session]\ndatabase = "sessions.db"\n')
    (home / "chapel").mkdir()
    chapel = home / "chapel" / "config.toml"
    chapel.write_text('[server]\nroom = "chapel"\n')
    files = pipeline.resolve(SimpleNamespace(),
                             pipeline.load_config(tuned, chapel))
    checks.check("a [files] path is read beside the file that names it",
                 pipeline.load_file_lines(files["glossary"])
                 == ["Rivanna Ward"], files["glossary"])
    checks.check("an absolute [files] path is left alone",
                 files["keyterms"] == "/etc/absolute.txt", files["keyterms"])
    checks.check("and sessions.db stays the room's, not the shared file's",
                 files["database"] == "sessions.db", files["database"])
    chapel.write_text('[files]\nglossary = "own.txt"\n')
    own = pipeline.resolve(SimpleNamespace(),
                           pipeline.load_config(tuned, chapel))["glossary"]
    checks.check("a room naming its own glossary gets the one beside it",
                 Path(own) == home / "chapel" / "own.txt", own)
    checks.check("a missing glossary says where it was looked for",
                 pipeline.describe_file("Glossary", own, [])
                 == f"Glossary: not found at {os.path.abspath(own)}",
                 pipeline.describe_file("Glossary", own, []))
    shutil.rmtree(home, ignore_errors=True)


def check_reader_page(checks):
    """The reader page asks beside itself rather than at the root.

    A hosted room lives under a path prefix that the proxy in front strips,
    so the page is /chapel/reader to the phone and /reader to the server.
    Root-absolute URLs would leave the room and reach whichever room
    answers the root, with that room's token in the query string.

    There is no JavaScript engine here, so the expression is lifted out of
    the page and run as a Python pattern rather than restated: a check that
    spelled the regex out again would pass against a page that had lost it.
    """
    checks.section("The reader page under a path prefix")
    page = (SRC / "static" / "reader.html").read_text(encoding="utf-8")
    found = re.search(r'location\.pathname\.replace\(/(.+?)/, "(.*?)"\)',
                      page)
    checks.check("the page derives a base from its own address",
                 found is not None, "no location.pathname in reader.html")
    if not found:
        return
    pattern, replacement = found.group(1), found.group(2)
    base = {where: re.sub(pattern, replacement, where)
            for where in ("/reader", "/chapel/reader")}
    checks.check("at the root the base is empty, so nothing changes there",
                 base["/reader"] == "", base)
    checks.check("under a prefix the stream stays inside the room",
                 base["/chapel/reader"] + "/stream/English"
                 == "/chapel/stream/English", base)
    # The derivation is only worth having if nothing goes around it, so
    # every address the page builds has to start from it.
    astray = [route for route in ('"/stream/', '"/api/')
              if page.count(route) != page.count("base + " + route)]
    checks.check("and every address the page builds starts from that base",
                 not astray, f"built at the root instead: {astray}")


def check_keys(checks):
    """Keys come out of config.toml, and the environment still wins."""
    checks.section("Keys in config.toml")
    config = {"keys": {"deepgram_api_key": "file-deepgram",
                       "llm_base_url": "  http://file/v1  ",
                       "llm_api_key": "file-llm",
                       "operator_token": "file-token"}}
    checks.check("every variable is its key in capitals, as the file claims",
                 all(variable == key.upper()
                     for _, key, variable in pipeline.SECRETS),
                 pipeline.SECRETS)
    with cleared_secrets():
        keys = pipeline.load_keys(config)
        checks.check("a key set in config.toml is read",
                     keys["deepgram_key"] == "file-deepgram", keys)
        checks.check("whitespace around a pasted key is trimmed off",
                     keys["llm_base"] == "http://file/v1", keys)
        checks.check("a file with no [keys] section is empty, not a crash",
                     pipeline.load_keys({}) == {attr: "" for attr, _, _
                                                in pipeline.SECRETS})
        os.environ["LLM_API_KEY"] = "from-environment"
        checks.check("an environment variable overrides the file",
                     pipeline.load_keys(config)["llm_key"]
                     == "from-environment")
        os.environ["OPERATOR_TOKEN"] = ""
        checks.check("an empty variable overrides a pinned token, which is "
                     "how a run asks for a minted one",
                     pipeline.load_keys(config)["operator_token"] == "")


def check_device_listing(checks):
    """The device list names one source to pre-select, so the page can.

    The page sorts by name and so has lost the backend's order, and without
    a suggestion from here the browser falls to the first option, which on a
    PulseAudio machine is the playback monitor rather than the microphone.
    capture.choose_default holds the rule; this checks that api_devices
    hands the answer over.
    """
    session = SimpleNamespace(config=fake_config())

    def listed(backend):
        return [{"name": "monitor", "detail": "", "monitor": True,
                 "default": False},
                {"name": "fake", "detail": "", "monitor": False,
                 "default": False}]

    def raises(backend):
        raise capture.CaptureError("pactl failed")

    def slow(backend):
        # Stands in for pactl on a machine that has stopped answering.
        time.sleep(0.2)
        return []

    async def ticks_while_listing(request):
        """How many turns the event loop got during one listing.

        Zero means api_devices ran the subprocess inline.
        """
        counted = 0

        async def tick():
            nonlocal counted
            while True:
                await asyncio.sleep(0.005)
                counted += 1

        ticker = asyncio.create_task(tick())
        try:
            await controls.api_devices(request)
        finally:
            ticker.cancel()
        return counted

    checks.section("The device list")
    request = StubRequest("secret", query="secret", session=session)
    original = capture.list_devices
    try:
        capture.list_devices = listed
        payload = json.loads(asyncio.run(controls.api_devices(request)).text)
        checks.check("the playback monitor is not what gets pre-selected",
                     payload.get("suggested") == "fake", payload)
        capture.list_devices = raises
        payload = json.loads(asyncio.run(controls.api_devices(request)).text)
        checks.check("a backend that cannot list is reported, not raised",
                     payload["devices"] == [] and "pactl" in payload["error"],
                     payload)

        capture.list_devices = listed
        locked = StubRequest("secret", session=session)
        payload = json.loads(asyncio.run(controls.api_devices(locked)).text)
        # The page destructures the list, so a refusal needs the same
        # shape or it throws before it can show the message.
        checks.check("a refused listing keeps the shape the page reads",
                     payload.get("devices") == [] and "error" in payload,
                     payload)

        capture.list_devices = slow
        counted = asyncio.run(ticks_while_listing(request))
        checks.check("a slow device listing leaves the event loop running",
                     counted >= 5, f"{counted} turns during a 0.2s listing")
    finally:
        capture.list_devices = original


def check_server(checks):
    """Boot server.py for real and exercise every route.

    A missing config file is deliberate: resolve falls through to the
    defaults, so the checks below do not depend on whatever config.toml
    happens to say on this machine.
    """
    check_authorization(checks)
    check_example_config(checks)
    check_config_merge(checks)
    check_keys(checks)
    check_reader_page(checks)
    check_device_listing(checks)
    checks.section("Minting the two tokens")
    minted = {controls.mint_token() for _ in range(3)}
    checks.check("a token with nothing set is minted fresh every run",
                 len(minted) == 3 and all(len(one) >= 32
                                          for one in minted), minted)
    checks.check("a pinned token is kept across restarts",
                 controls.mint_token("bench") == "bench")

    checks.section("The address on the card")
    checks.check("the reader address carries a path and a token",
                 server.reader_address("https://chapel.example/", "", "abc")
                 == "https://chapel.example/reader?token=abc",
                 server.reader_address("https://chapel.example/", "", "abc"))
    checks.check("a base without a trailing slash builds the same address",
                 server.reader_address("https://chapel.example", "", "abc")
                 == "https://chapel.example/reader?token=abc")
    # One hostname, several rooms, told apart by the prefix the proxy in
    # front strips before it forwards.
    checks.check("a named room is a path segment of its own",
                 server.reader_address("https://transept.example/", "chapel",
                                       "abc")
                 == "https://transept.example/chapel/reader?token=abc",
                 server.reader_address("https://transept.example/", "chapel",
                                       "abc"))
    checks.check("however the room and the base are punctuated",
                 server.reader_address("https://transept.example", "/chapel/",
                                       "abc")
                 == "https://transept.example/chapel/reader?token=abc")
    # Rather than a link to /reader?token= on nowhere, which the operator
    # page would render as something to hand somebody.
    checks.check("no public_url means no address at all",
                 server.reader_address("", "chapel", "abc") == "")
    # Which rooms get that segment, since a laptop behind a tunnel serves
    # the root and may still name its room for the recorded transcript.
    checks.check("a hosted room is the one that carries its name",
                 server.card_room({"room": "chapel", "capture": "remote"})
                 == "chapel")
    checks.check("a laptop naming its room keeps the address it always had",
                 server.card_room({"room": "chapel", "capture": "auto"})
                 == "")

    checks.section("The running server")
    port, operator = free_port(), free_port()
    lingering = None
    with running_server([
            "--config", "selftest-no-such-config.toml",
            "--languages", "French,Swahili", "--reader-port", str(port),
            "--operator-port", str(operator), "--max-readers", "3",
    ]) as (process, lines, home):
        pid_file = home / "transept.pid"
        if not wait_for_port(port, process) or not wait_for_port(
                operator, process):
            checks.check(f"server.py came up on ports {port} and {operator}",
                         False, "".join(lines))
            return

        # Everything below needs these, so a bad banner fails here first.
        address = banner_address(lines, "Operator")
        TOKEN = address.split("token=", 1)[-1] if "token=" in address else ""
        checks.check("the banner prints an operator address with a token",
                     len(TOKEN) >= 32, address or "".join(lines))
        card = banner_address(lines, "Reader")
        READER = card.split("token=", 1)[-1] if "token=" in card else ""
        checks.check("the banner prints a reader address with a token",
                     len(READER) >= 32, card or "".join(lines))
        # Two doors, two keys. One token for both would mean the card the
        # room is handed also opens the controls.
        checks.check("the two addresses carry different tokens",
                     READER != TOKEN, f"{READER} {TOKEN}")

        # The record ./transept finds the server by, on all three
        # platforms, since no portable command answers "which python is
        # running server.py out of this directory".
        written = {}
        if pid_file.exists():
            written = json.loads(pid_file.read_text(encoding="utf-8"))
        checks.check("a listening server left a record of itself",
                     written.get("pid") == process.pid, written)
        checks.check("the record carries both ports, so stopping one needs "
                     "no config file",
                     (written.get("reader_port"),
                      written.get("operator_port")) == (port, operator),
                     written)
        checks.check("and says it is a server, which status names",
                     written.get("role") == "server", written)

        status, body = request(port, "/api/channels", token=READER)
        checks.check("channels are fixed at startup",
                     json.loads(body)["channels"] == ["English", "French",
                                                      "Swahili"], body)

        status, body = request(operator, "/api/status", token=TOKEN)
        state = json.loads(body)
        checks.check("status starts stopped", state["state"] == "stopped",
                     state["state"])
        checks.check("no language is active before anyone reads one",
                     all(not language["active"]
                         for language in state["languages"]),
                     state["languages"])

        status, _ = request(port, "/reader", token=READER)
        checks.check("/reader answers 200 on the reader port",
                     status == 200, status)
        for path in ("/operator", "/api/devices"):
            status, _ = request(operator, path, token=TOKEN)
            checks.check(f"{path} answers 200 on the operator port",
                         status == 200, status)

        # The split is the boundary: the tunnelled port must carry no
        # route that can change anything, token or not.
        for path in ("/operator", "/api/status", "/api/devices",
                     "/api/start", "/api/stop", "/api/language", "/qr.svg"):
            status, _ = request(port, path, token=TOKEN)
            checks.check(f"{path} is not served on the reader port",
                         status == 404, status)

        # The token rule through real routing. Every other check of it calls
        # the handlers directly, so none of them would notice a route left
        # out of add_routes or a decorator that never ran.
        for path in ("/operator", "/api/status", "/api/devices", "/qr.svg"):
            status, _ = request(operator, path)
            checks.check(f"{path} is refused without the token",
                         status == 403, status)

        def answered(path, token=None):
            """The status for one reader route, or what went wrong instead.

            A stream that opens has no status to return: urllib blocks on
            it, and a gate missing from that route is exactly that hang, so
            it has to come back as a failed check rather than a stuck run.
            """
            try:
                return request(port, path, timeout=3, token=token)[0]
            except Exception as exc:
                return f"held open: {type(exc).__name__}"

        # The reader port is the one on the public internet, so this is the
        # loop that decides whether a stranger who finds the address gets a
        # meeting. The wrong-token pass is what a link from last week is.
        for path in ("/reader", "/api/channels", "/stream/English"):
            status = answered(path)
            checks.check(f"{path} is refused without the reader token",
                         status == 403, status)
            status = answered(path, token=TOKEN)
            checks.check(f"{path} is refused with the wrong token",
                         status == 403, status)

        status, body = request(port, "/")
        checks.check("the bare address gives a bot nothing",
                     status == 404, f"{status} {body}")

        status, body = request(operator, "/qr.svg", token=TOKEN)
        checks.check("the QR endpoint explains a missing public_url",
                     status == 404 and "public_url" in body,
                     f"{status} {body}")

        status, _ = request(port, "/stream/Klingon", token=READER)
        checks.check("an unknown channel is a 404", status == 404, status)

        state_event = 'event: state\ndata: {"state": "stopped"}\n\n'
        content_type, preamble = sse_preamble(port, READER, "English")
        checks.check("the event stream announces itself correctly",
                     content_type == "text/event-stream", content_type)
        checks.check("the event stream opens with a reconnect interval",
                     preamble.startswith("retry:"), repr(preamble))
        # What the second dot on the reader page shows. A reader who opens
        # the page before the meeting starts sees an empty feed either way,
        # and this is what tells that apart from a feed that has stalled.
        checks.check("and says whether anything is being captured",
                     preamble.endswith(state_event), repr(preamble))

        status, body = request(operator, "/api/start", "POST", {}, token=TOKEN)
        checks.check("starting without a device is refused with a reason",
                     json.loads(body)["ok"] is False
                     and "audio source" in json.loads(body)["message"], body)

        status, body = request(operator, "/api/stop", "POST", {}, token=TOKEN)
        checks.check("stopping when idle is refused",
                     json.loads(body)["ok"] is False, body)

        status, body = request(operator, "/api/language", "POST",
                               {"language": "French", "mode": "on"},
                               token=TOKEN)
        checks.check("a language can be forced on", json.loads(body)["ok"],
                     body)
        state = json.loads(request(operator, "/api/status", token=TOKEN)[1])
        french = [item for item in state["languages"]
                  if item["name"] == "French"][0]
        checks.check("the override shows in status and makes it active",
                     french["override"] == "on" and french["active"], french)

        status, body = request(operator, "/api/language", "POST",
                               {"language": "Klingon", "mode": "on"},
                               token=TOKEN)
        checks.check("an unknown language is refused",
                     json.loads(body)["ok"] is False, body)

        # The operator page reads the {"ok": false} shape and shows the
        # message. A 500 traceback reaches it as nothing at all.
        status, body = request(operator, "/api/language", "POST", body=None,
                               token=TOKEN)
        checks.check("a language request with no body is an answer, "
                     "not a 500",
                     status == 200 and json.loads(body)["ok"] is False,
                     f"{status} {body}")

        # Last, because these hold the cap full for as long as they are
        # open and every check above wants a slot.
        held = [open_stream(port, READER) for _ in range(3)]
        try:
            try:
                status, body = request(port, "/stream/English", timeout=3,
                                       token=READER)
            except Exception as exc:
                # A stream that opened is the failure this is looking for,
                # and it arrives here as a timeout rather than a status.
                status, body = 200, f"held open: {type(exc).__name__}"
            checks.check("a reader past the cap is refused, not left hanging",
                         status == 503, f"{status} {body}")
            state = json.loads(request(operator, "/api/status",
                                       token=TOKEN)[1])
            checks.check("the operator page can see readers turned away",
                         state["refused"] >= 1, state["refused"])
        finally:
            for sock in held:
                sock.close()

        # Held open across the shutdown below, because a phone with the
        # page still up is the normal state of the room when the operator
        # presses Ctrl-C, and a stream nobody ends is a connection the
        # runner will wait on.
        lingering = open_stream(port, READER)
    if lingering:
        lingering.close()
    output = "".join(lines)

    checks.check("the startup banner prints the reader address",
                 f"http://127.0.0.1:{port}/reader?token=" in output, output)
    checks.check("the banner sends the operator to the loopback port",
                 f"http://127.0.0.1:{operator}/operator" in output, output)
    checks.check("the banner says the operator address is not a bookmark",
                 "changes" in output and "every restart" in output, output)
    checks.check("binding to loopback says so, since a phone cannot reach it",
                 "Bound to this machine only" in output, output)
    checks.check("the server exited while a phone was still reading",
                 not process.forced,
                 "it ignored the signal and had to be killed")
    # A record left behind would send the next stop after whatever process
    # the system has since given that number to.
    checks.check("and took its record away on the way out",
                 not pid_file.exists(), pid_file)


def check_control(checks):
    """Boot the hosted half for real and knock on its control socket.

    The listener a sender reaches has to bind publicly, because the sender
    is in another building, so what it serves and what it turns away is the
    whole of its guard. Loopback to loopback here: no network, no keys, and
    no session, since starting one would reach for a recognizer.
    """
    checks.section("The hosted server and its control socket")
    port, control = free_port(), free_port()
    with running_server([
            "--config", "selftest-no-such-config.toml",
            "--languages", "French", "--capture", "remote",
            "--room", "chapel", "--reader-port", str(port),
            "--control-port", str(control),
    ]) as (process, lines, _):
        if not wait_for_port(port, process) or not wait_for_port(
                control, process):
            checks.check(f"server.py came up on ports {port} and {control}",
                         False, "".join(lines))
            return

        address = banner_address(lines, "Control")
        TOKEN = address.split("token=", 1)[-1] if "token=" in address else ""
        checks.check("the banner prints a control address with a token",
                     len(TOKEN) >= 32, address or "".join(lines))
        READER = banner_address(lines, "Reader").split("token=", 1)[-1]
        checks.check("the two addresses carry different tokens",
                     READER and READER != TOKEN, f"{READER} {TOKEN}")
        # There is no operator page on a hosted server: the page that
        # starts a meeting belongs beside the microphone, on the sender.
        checks.check("and no operator address at all",
                     banner_address(lines, "Operator", timeout=1.0) == "",
                     "".join(lines))

        base = f"ws://127.0.0.1:{control}/control"

        def handshake(token, headers=None):
            """The status a handshake is refused with, or the open socket."""
            try:
                return ws_connect(f"{base}?token={token}", open_timeout=5,
                                  additional_headers=headers)
            except websockets.InvalidStatus as exc:
                return exc.response.status_code

        refused = handshake("nonsense")
        checks.check("the control socket is refused without the token",
                     refused == 403, refused)
        refused = handshake(READER)
        checks.check("and refused with the reader's token, which the whole "
                     "room holds", refused == 403, refused)
        # A browser stamps every handshake with an Origin and cannot be
        # made not to, so refusing the header refuses the browser. A page
        # that somehow learned this token still cannot open the socket.
        refused = handshake(TOKEN, {"Origin": "https://not.this.example"})
        checks.check("a handshake carrying a browser's Origin is refused",
                     refused == 403, refused)

        def say(socket, **fields):
            socket.send(json.dumps({
                "type": "hello", "room": "chapel", "encoding": "pcm",
                "device": "a laptop in the room", "intent": "attach",
                **fields}))
            return json.loads(socket.recv(timeout=5))

        def next_status(socket):
            while True:
                message = json.loads(socket.recv(timeout=5))
                if message.get("type") == "status":
                    return message["status"]

        sender = handshake(TOKEN)
        if not hasattr(sender, "send"):
            checks.check("the sender's socket opened", False, sender)
            return
        try:
            ready = say(sender)
            checks.check("a sender that says hello is told it is ready",
                         ready.get("type") == "ready", ready)
            checks.check("and told the room, the languages, and the state",
                         (ready.get("room"), ready.get("languages"),
                          ready.get("state"))
                         == ("chapel", ["French"], "stopped"), ready)
            status = next_status(sender)
            checks.check("the status the operator page renders is pushed "
                         "down the same socket",
                         status.get("state") == "stopped"
                         and "languages" in status, status)

            sender.send(json.dumps({"type": "stop"}))
            answer = json.loads(sender.recv(timeout=5))
            while answer.get("type") == "status":
                answer = json.loads(sender.recv(timeout=5))
            checks.check("stopping what is not running says so rather than "
                         "going quiet",
                         answer.get("type") == "error", answer)
        finally:
            sender.close()

        # The control listener serves one route. Everything the operator
        # port carries is absent here, token or not.
        for path in ("/operator", "/api/status", "/api/devices", "/qr.svg",
                     "/reader", "/api/channels"):
            status, _ = request(control, path, token=TOKEN)
            checks.check(f"{path} is not served on the control port",
                         status == 404, status)
        status, _ = request(port, "/control", token=READER)
        checks.check("/control is not served on the reader port",
                     status == 404, status)
        status, _ = request(port, "/reader", token=READER)
        checks.check("and the reader port is the same one phones already "
                     "use", status == 200, status)


async def settle(ready, timeout=5.0):
    """Wait for something the other end of a socket has to do first."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if ready():
            return True
        await asyncio.sleep(0.05)
    return ready()


async def drive_sender(checks, control, operator):
    """One sender, one server, and the page the volunteer actually uses."""
    remote = sender.Remote({"capture": "auto", "reader_url": ""},
                           "chapel", "pcm")
    url = sender.dial(f"ws://127.0.0.1:{control}/control", "shared")
    runner = web.AppRunner(controls.build_operator_app(remote, "bench"))
    await runner.setup()
    link = None
    try:
        await web.TCPSite(runner, "127.0.0.1", operator).start()
        link = asyncio.create_task(sender.supervise(remote, url))
        reached = await settle(lambda: bool(remote.snapshot))
        # French rather than Swahili because the room's file is read after
        # the shared one, which is the whole of how a room differs from the
        # server it runs on. No --languages flag here for that reason.
        checks.check("the sender reaches the server and is told it is ready",
                     reached and remote.languages == ["French"],
                     remote.error or remote.languages)
        # The sender holds no reader token and no public_url, so the
        # address to hand the room can only come down this socket. The room
        # is the path segment in it, which is how several rooms share one
        # hostname, and the base it is added to came from the shared file
        # while the room name came from the command line.
        checks.check("and told the address to hand the room, which only "
                     "the server knows",
                     remote.config["reader_url"]
                     == "https://transept.example/chapel/reader?token=card",
                     remote.config["reader_url"])

        status, body = await asyncio.to_thread(
            request, operator, "/api/status", token="bench")
        payload = json.loads(body)
        checks.check("the page on this laptop renders the server's own "
                     "status", status == 200
                     and payload["channels"] == ["English", "French"]
                     and payload["state"] == "stopped", payload)
        checks.check("including the address, which is what the QR code on "
                     "this page encodes",
                     payload["reader_url"].endswith("/reader?token=card"),
                     payload["reader_url"])

        status, body = await asyncio.to_thread(
            request, operator, "/api/language", "POST",
            {"language": "French", "mode": "on"}, token="bench")
        checks.check("a language button on this page is accepted here",
                     status == 200 and json.loads(body)["ok"], body)

        def forced():
            rows = remote.snapshot.get("languages") or []
            return any(row["name"] == "French" and row["override"] == "on"
                       for row in rows)

        # The whole path in one check: the page, the proxy, the socket, the
        # session on the server, and the status pushed back down.
        checks.check("and reaches the session on the other side of the "
                     "socket", await settle(forced),
                     remote.snapshot.get("languages"))

        second = sender.Remote({"capture": "auto", "reader_url": ""},
                               "chapel", "pcm")
        await sender.link(second, url)
        checks.check("a second laptop is turned away, and told why",
                     "Another sender" in (second.error or ""), second.error)
        checks.check("and the room's own sender still holds the socket",
                     remote.socket is not None, "the first sender was cut off")
    finally:
        if link is not None:
            link.cancel()
            await asyncio.gather(link, return_exceptions=True)
        await runner.cleanup()


async def drive_audio(checks):
    """The audio path itself, both halves of it in one process.

    The one thing a sender exists to do: a chunk captured on the laptop
    reaching the source the recognizer reads on the server. Both ends are
    built here rather than in a subprocess, because what has to be looked
    at is the object on the far side of the socket.
    """
    checks.section("Audio from the room to the server that recognizes it")
    hub = server.Hub(["French"])
    room = server.ControlRoom(fake_config(languages=["French"],
                                          room="chapel"), hub)

    async def without_a_recognizer(device):
        # A real start opens a Deepgram socket, and this suite needs no
        # network. What is under test is everything up to that socket.
        room.session.device = device
        room.session.state = "running"
        return True, "Starting."

    room.session.start = without_a_recognizer
    port = free_port()
    runner = web.AppRunner(server.build_control_app(room, "shared"))
    await runner.setup()
    remote = sender.Remote({"capture": "auto", "reader_url": ""},
                           "chapel", "pcm")
    link = None
    captured = FakeSource()
    original = capture.open_capture
    try:
        await web.TCPSite(runner, "127.0.0.1", port).start()
        # Before the sender sends anything: a run that has not begun has no
        # source, and audio arriving meanwhile is dropped on purpose.
        source = await room.open_source()
        link = asyncio.create_task(sender.supervise(
            remote, sender.dial(f"ws://127.0.0.1:{port}/control", "shared")))
        pump = asyncio.create_task(remote.pump())

        async def opens(device, backend):
            return captured

        capture.open_capture = opens
        ok, message = await remote.start("a microphone")
        checks.check("the operator page starts a meeting from the laptop",
                     ok, message)
        checks.check("and the server is told which microphone it is",
                     await settle(lambda: room.session.device
                                  == "a microphone"), room.session.device)
        chunk = None
        try:
            # None rather than a raised timeout is the likely failure: a
            # source with nothing arriving reports a gap, which is exactly
            # what a sender that is not sending looks like.
            chunk = await asyncio.wait_for(source.read(), timeout=5)
        except TimeoutError:
            pass
        checks.check("audio captured in the room reaches the source the "
                     "recognizer reads", chunk == await FakeSource().read(),
                     "a gap, so nothing arrived" if not chunk
                     else f"{len(chunk)} bytes")
        # The recognizer is told the format when its socket opens and can
        # not be told another, so what the sender declared has to be what
        # the sender is sending.
        checks.check("and the server holds the encoding the sender declared",
                     room.encoding == remote.source.encoding,
                     f"{room.encoding} against {remote.source.encoding}")
        # By getattr, so a source with no clock fails here rather than
        # ending the suite in a traceback.
        clock = getattr(source, "clock", None)
        checks.check("the server counts the audio as it arrives",
                     clock is not None and clock.position > 0,
                     clock and clock.position)
        checks.check("and tells the sender it records levels",
                     remote.levels)
        reporting = asyncio.create_task(remote.report_levels())
        checks.check("the levels measured on the laptop reach the server",
                     await settle(lambda: getattr(
                         getattr(source, "meter", None), "samples", 0) > 0))
        reporting.cancel()
        pump.cancel()
        await asyncio.gather(pump, reporting, return_exceptions=True)
    finally:
        capture.open_capture = original
        if link is not None:
            link.cancel()
            await asyncio.gather(link, return_exceptions=True)
        await remote.release()
        await runner.cleanup()

    # An Opus stream is not readable from the middle, and the middle is
    # where every run here joins: the first a second after the encoder
    # started, a later one an hour in. Without the pages in front, Deepgram
    # holds the socket open and returns nothing, which in a room is
    # indistinguishable from nobody speaking.
    checks.section("An Opus stream a recognizer run joins partway through")

    async def opening(source):
        """What a run would read first, or nothing if it would wait.

        Nothing is the failure this section is about, so it is reported as
        a failed check rather than left to end the suite in a traceback.
        """
        try:
            return await asyncio.wait_for(source.read(), timeout=2)
        except TimeoutError:
            return b""

    room = server.ControlRoom(fake_config(languages=["French"],
                                          room="chapel"), hub)
    headers = FakeOpusSource.headers
    hello = {"type": "hello", "room": "chapel", "encoding": "opus",
             "intent": "start", "device": "a microphone",
             "headers": base64.b64encode(headers).decode()}
    taken, message = room.attach(object(), dict(hello, headers=""))
    checks.check("an Opus sender that declared no stream is refused",
                 not taken and "did not say what it is" in message, message)
    checks.check("and the room is left with no session and no sender",
                 room.socket is None and room.headers == b"", room.headers)
    taken, message = room.attach(object(), hello)
    checks.check("one that declared its stream is taken",
                 taken and room.headers == headers, message)
    source = await room.open_source()
    first = await opening(source)
    checks.check("and a run that opens later starts with those pages",
                 first == headers, first[:24])
    room.socket = None
    later = await room.open_source()
    first = await opening(later)
    checks.check("as does the run after a reconnect an hour in",
                 first == headers, first[:24])
    # Pages belonging to a format this room is no longer carrying are worse
    # than none: they would be prepended to raw samples.
    room.attach(object(), {"type": "hello", "room": "chapel",
                           "encoding": "pcm", "intent": "attach"})
    checks.check("a sender that fell back to raw audio drops them",
                 room.headers == b"", room.headers)
    room.socket = None
    room.encoding, room.headers = "opus", headers
    room.attach(object(), {"type": "hello", "room": "chapel",
                           "encoding": "opus", "intent": "attach"})
    # A sender rejoining a room it is not feeding has no encoder open and
    # so nothing to declare, which is not the same as a stream that changed.
    checks.check("but a sender rejoining with nothing open keeps them",
                 room.headers == headers, room.headers)

    room = server.ControlRoom(fake_config(languages=["French"],
                                          room="chapel"), hub)
    remote = sender.Remote({"capture": "auto", "reader_url": ""},
                           "chapel", "opus")
    port = free_port()
    runner = web.AppRunner(server.build_control_app(room, "shared"))
    await runner.setup()
    link = None
    room.session.start = without_a_recognizer
    original = capture.open_capture
    try:
        await web.TCPSite(runner, "127.0.0.1", port).start()

        async def opens_opus(device, backend):
            return FakeOpusSource()

        capture.open_capture = opens_opus
        link = asyncio.create_task(sender.supervise(
            remote, sender.dial(f"ws://127.0.0.1:{port}/control", "shared")))
        # Before the start, because the pages reach the hello by way of the
        # pump: nothing has read the encoder until this task does.
        pump = asyncio.create_task(remote.pump())
        ok, message = await remote.start("a microphone")
        checks.check("a real sender carries its pages across the socket",
                     ok and await settle(lambda: room.headers
                                         == FakeOpusSource.headers),
                     room.headers[:24] or "nothing arrived")
        source = await room.open_source()
        first = await opening(source)
        checks.check("so the recognizer is handed a stream it can read",
                     first == FakeOpusSource.headers, first[:24])
        pump.cancel()
        await asyncio.gather(pump, return_exceptions=True)
    finally:
        capture.open_capture = original
        if link is not None:
            link.cancel()
            await asyncio.gather(link, return_exceptions=True)
        await remote.release()
        await runner.cleanup()


def check_sender(checks):
    """The sender against a real server, over a real control socket.

    No session is started, for the reason check_control starts none: that
    would reach for a recognizer, and this suite needs no network. What is
    checked is everything the two halves do before any audio flows, which
    is where they have to agree about the room, the languages, and the
    address the room is handed.
    """
    checks.section("The sender in the room and the server somewhere else")
    port, control, operator = free_port(), free_port(), free_port()
    # Real config files, unlike the other sections, and two of them: the
    # shared one a hosted server reads first and the room's own after it,
    # which is the arrangement systemd starts every room with. reader_url
    # is built from public_url, the room, and the reader token, and the
    # sender showing the right one is half of what this section is for.
    configs = Path(tempfile.mkdtemp())
    (configs / "shared.toml").write_text(
        '[server]\npublic_url = "https://transept.example/"\n'
        '[languages]\navailable = ["Swahili"]\n', encoding="utf-8")
    (configs / "room.toml").write_text(
        '[languages]\navailable = ["French"]\n', encoding="utf-8")
    # The reader token pinned, so the address the sender is handed is one
    # this check can write down rather than read back out of the banner.
    with running_server([
            "--config", str(configs / "shared.toml"),
            "--config", str(configs / "room.toml"),
            "--capture", "remote", "--room", "chapel",
            "--reader-port", str(port), "--control-port", str(control),
    ], READER_TOKEN="card", CONTROL_TOKEN="shared") as (process, lines, _):
        if not wait_for_port(control, process):
            checks.check(f"server.py came up on port {control}", False,
                         "".join(lines))
            return
        asyncio.run(drive_sender(checks, control, operator))


# -- recording a session -----------------------------------------------------


def check_spoken(checks):
    """Lag from the words themselves, and finishes that would cut one.

    The lag a line records runs from the end of the recognizer's window,
    which after a Finalize is the moment it was sent, so on the third field
    test it read 0.10 seconds at the median while readers waited about
    five for a line's first word.
    """
    checks.section("When a line's words were spoken")
    path = Path(tempfile.mkdtemp()) / "spoken.db"
    handle = record.connect(str(path))
    handle.execute("INSERT INTO sessions (started_at, languages, settings)"
                   " VALUES (1000, '[]', '{\"early_finalize\": false}')")
    # Each piece's audio ended lag seconds before it arrived, so its
    # words sit that far back from its arrival, less their own distance
    # from the end of the piece.
    for at, start, end, text, words, lag in (
            (1010.0, 0.0, 4.0, "we came to the",
             [["we", 0.5, 0.8], ["came", 0.9, 1.2], ["to", 1.3, 1.5],
              ["the", 1.6, 1.8]], 0.5),
            (1012.0, 4.0, 6.0, "chapel. Each of",
             [["chapel.", 4.0, 4.4], ["Each", 5.6, 5.8], ["of", 5.85, 5.95]],
             0.25)):
        handle.execute(
            "INSERT INTO fragments (session_id, run, at, start, audio_end,"
            " speech_final, text, finalized, words, lag)"
            " VALUES (1, 1, ?, ?, ?, 0, ?, 0, ?, ?)",
            (at, start, end, text, json.dumps(words), lag))
    # A second run, whose one piece smart_format wrote as two words while
    # the recognizer's own list has three.
    handle.execute(
        "INSERT INTO fragments (session_id, run, at, start, audio_end,"
        " speech_final, text, finalized, words, lag)"
        " VALUES (1, 2, 1020.0, 0.0, 2.0, 0, '5PM tonight.', 0, ?, 0.5)",
        (json.dumps([["5", 0.2, 0.4], ["PM", 0.4, 0.6],
                     ["tonight.", 0.7, 1.2]]),))
    for seq, run, heard, at in ((1, 1, "we came to the chapel.", 1012.1),
                                (2, 1, "Each of", 1013.0),
                                (3, 2, "5PM tonight.", 1021.0)):
        handle.execute(
            "INSERT INTO lines (session_id, seq, run, at, heard, reason,"
            " latency) VALUES (1, ?, ?, ?, ?, 'punctuation', 1.5)",
            (seq, run, at, heard))
    # The first guess had 1.1 seconds after it, in which "Each" began at
    # 5.6. The second reported 0.3 seconds of quiet after "to", in which
    # "the" had already begun. The third ended on "came" with "to" still
    # to come after its cut.
    for end, quiet, unseen in ((4.6, 0.2, 1.1), (1.85, 0.3, 0.02),
                               (1.25, 0.03, 0.02)):
        handle.execute("INSERT INTO guesses (session_id, run, at, audio_end,"
                       " quiet, unseen, decision)"
                       " VALUES (1, 1, 0, ?, ?, ?, 'off')",
                       (end, quiet, unseen))
    handle.close()

    handle = record.open_read_only(str(path))
    lines = record.lines_of(handle, 1)
    spoken = record.spoken_times(handle, 1, lines)
    rounded = {seq: tuple(round(value, 2) for value in pair)
               for seq, pair in spoken.items()}
    checks.check("each line's first and last words are placed on the clock",
                 rounded.get(1) == (1006.0, 1010.15)
                 and rounded.get(2) == (1011.35, 1011.7), rounded)
    checks.check("a piece written with fewer words than it was spoken in "
                 "spreads them over its span, and keeps its run",
                 rounded.get(3) == (1017.7, 1018.7), rounded)
    extra = [*map(dict, lines),
             {"seq": 4, "run": 1, "heard": "and more", "at": 1014.0}]
    left = record.spoken_times(handle, 1, extra)
    checks.check("a run whose words do not add up is left out, not guessed",
                 set(left) == {3}, left)
    report = Path(tempfile.mkdtemp()) / "spoken.md"
    record.write_report(handle, record.resolve_session(handle, "last"),
                        str(report))
    handle.close()
    text = report.read_text(encoding="utf-8")
    checks.check("the report measures lag from a line's first word",
                 "From a line's first word being spoken to the English line "
                 "appearing: median 3.30s" in text,
                 text[text.find("## Latency"):][:300])
    checks.check("and says how often a finish would have cut a word",
                 "not asked, early_finalize off: 3" in text
                 and "a word already begun at the cut 2 times, 1 of them "
                 "inside the guess's own quiet" in text,
                 text[text.find("## Recognizer"):][:600])


def check_names(checks):
    """Where each name a correction introduced could have come from.

    On the third field test a name the model borrowed from the line before
    was reported as invented, because nothing said the line before had it.
    """
    checks.section("Where an introduced name came from")
    path = Path(tempfile.mkdtemp()) / "names.db"
    handle = record.connect(str(path))
    handle.execute("INSERT INTO sessions (started_at, languages, glossary)"
                   " VALUES (1000, '[]', 'Leaders: President Eyring')")
    for seq, heard, english in (
            (1, "we sustain brother kalema as clerk",
             "we sustain Brother Kalema as clerk"),
            (2, "join me in sustaining t bone in his calling",
             "join me in sustaining Kalema in his calling"),
            (3, "from president eyrie", "from President Eyring"),
            (4, "meet on mount prophet", "meet on Mount Crawford"),
            (5, "of us can do this.", "Each of us can do this."),
            (6, "rina, it's called the sculpting",
             "Runia, it's called the sculpting"),
            (7, "so my name is elders stockett",
             "So my name is Elder Stockett."),
            (8, "in the young women's presidency",
             "in the Young Women presidency")):
        handle.execute(
            "INSERT INTO lines (session_id, seq, run, heard, english, reason)"
            " VALUES (1, ?, 1, ?, ?, 'punctuation')", (seq, heard, english))
    handle.close()
    handle = record.open_read_only(str(path))
    lines = record.lines_of(handle, 1)
    found = {line["seq"]: record.name_sources(
                 line, [other["heard"] for other in lines[:index]],
                 {"leaders", "president", "eyring"})
             for index, line in enumerate(lines)}
    checks.check("a name in the lines before is marked as borrowed",
                 found[2] == {"Kalema": "context"}, found[2])
    checks.check("a name in the glossary is marked as the glossary's",
                 found[3] == {"Eyring": "glossary"}, found[3])
    checks.check("a name in neither is marked as the model's own",
                 found[4] == {"Crawford": "neither"}, found[4])
    checks.check("a capital that only opens the sentence is set apart",
                 found[5] == {"Each": "opening"}, found[5])
    checks.check("which is where a name opening the line now shows",
                 found[6] == {"Runia": "opening"}, found[6])
    checks.check("a plural or possessive dropped is not a new name",
                 found[7] == {} and found[8] == {}, (found[7], found[8]))
    report = Path(tempfile.mkdtemp()) / "names.md"
    record.write_report(handle, record.resolve_session(handle, "last"),
                        str(report))
    handle.close()
    text = report.read_text(encoding="utf-8")
    section = text[text.find("## Names"):text.find("## Terms")]
    checks.check("the report lists the model's own names first",
                 0 <= section.find("Crawford") < section.find("Kalema")
                 < section.find("Eyring"), section)


def check_levels(checks):
    """A stretch with no words, judged against the speech around it.

    On the third field test the opening hymn and the prayer after it made
    one stretch with one median, and the hymns, the sacrament being passed,
    and the prayer were all reported alike as speaking level.
    """
    checks.section("Telling music from a speaker away from the microphone")

    def window(index, level, words):
        return {"run": 1, "at": 5.0 * index, "captured": 5.0,
                "received": 5.0, "rms_db": level, "peak_db": level + 10,
                "results": 2 if words else 1, "empty": 0 if words else 1}

    levels = ([(-25, True)] * 4 + [(-10, False)] * 6 + [(-32, False)] * 6
              + [(-25, True)] * 2)
    rows = [window(index, level, words)
            for index, (level, words) in enumerate(levels, 1)]
    speech = record.speech_level(rows)
    checks.check("speech is the level of the windows with words in them",
                 speech == -25, speech)
    parts = record.silent_stretches(rows)
    checks.check("a hymn and the prayer after it are two stretches",
                 [len(windows) for _, _, windows in parts] == [6, 6],
                 [len(windows) for _, _, windows in parts])
    # Read from the parts there are, so one stretch fails the checks below
    # rather than ending the section.
    hymn, prayer = ([record.explain_silence(30.0, windows, speech)
                     for _, _, windows in parts] + ["", ""])[:2]
    checks.check("louder than the speech is music",
                 "singing or music" in hymn, hymn)
    checks.check("quieter than it is someone away from the microphone",
                 "away from the microphone" in prayer, prayer)
    bang = [(-10, False)] * 3 + [(-40, False)] + [(-10, False)] * 3
    rows = [window(index, level, words)
            for index, (level, words) in enumerate(bang, 1)]
    checks.check("one odd window does not split a stretch",
                 len(record.silent_stretches(rows)) == 1,
                 len(record.silent_stretches(rows)))


async def check_store(checks):
    """Record a session to a real file, then read it back.

    The last section here is the one that matters most. A disk that has
    stopped answering must cost the record and nothing else, so a Recorder
    that raises on every call has to leave the subtitles untouched.
    """
    checks.section("Recording a session to disk")
    path = Path(tempfile.mkdtemp()) / "selftest.db"
    translator = FakeTranslator()
    session, hub = make_session(["French", "Swahili"], True,
                                ["English", "French"])
    session.config["record"] = True
    session.config["database"] = str(path)
    session.config["room"] = "chapel"
    session.started_at = time.time()
    session.run = 1
    # Eight seconds of audio captured just now, which covers every
    # fragment, the way a device would have been read ahead of the lines.
    clock = pipeline.StreamClock()
    for step in range(1, 81):
        clock.advance(0.1, now=time.monotonic() - 8.0 + step * 0.1)
    session.run_source = SimpleNamespace(clock=clock)
    await session._open_recorder()
    session.translator = translator
    checks.check("the recorder opened", session.recording(), session.error)
    await drive(session, translator)
    # One piece finished on request, as early_finalize asks for one.
    session.fragment("Let us pray.", 8.0, 9.0, False, 0.9, True)
    session.guess(9.0, 0.3, 0.1, "sent")
    # What a run's watcher writes as each window closes: the recognizer
    # answered four times, the meter heard five seconds at -6 dBFS, and the
    # clock moved on from where the last window left it.
    session.recognized(False)
    meter = pipeline.LevelMeter()
    meter.merge(5.0, 0.5, 0.5)
    place = session._audio_row(clock, meter, 3.0)
    checks.check("a window ends where the clock is",
                 abs(place - clock.position) < 1e-9, place)
    # Then thirty seconds in which nothing with words came back, written
    # as the rows they would have been, at a level of -60 dBFS.
    began = time.time()
    for step in range(1, 8):
        session.recorder.audio(run=1, at=began + 5 * step, captured=5.0,
                               received=5.0, rms_db=-60.0, peak_db=-40.0,
                               results=1 if step < 7 else 2, empty=1)
    await session.recorder.close()

    handle = record.open_read_only(str(path))
    stored = record.lines_of(handle, 1)
    checks.check("every sentence was recorded once",
                 [row["seq"] for row in stored] == [1, 2, 3],
                 [row["seq"] for row in stored])
    checks.check("the raw transcript was kept",
                 stored[0]["heard"] == FIRST_SENTENCE, stored[0]["heard"])
    checks.check("the correction filled the same row, not a second one",
                 stored[0]["english"] == f"CORRECTED {FIRST_SENTENCE}",
                 stored[0]["english"])
    checks.check("the reason the sentence closed was kept",
                 [row["reason"] for row in stored]
                 == ["gap", "endpoint", "punctuation"],
                 [row["reason"] for row in stored])
    checks.check("the requested outputs were recorded",
                 json.loads(stored[0]["requested"]) == ["English", "French"],
                 stored[0]["requested"])
    # The clock's audio ended just now, so a line's lag is how far before
    # the end of it the line's own words were: the last line none at all.
    checks.check("every line has a lag, the prompt ones included",
                 all(row["lag"] is not None
                     and abs(row["lag"] - (8.0 - row["audio_end"])) < 0.5
                     for row in stored), [row["lag"] for row in stored])
    checks.check("each line records the tokens its own call cost",
                 [row["prompt_tokens"] for row in stored]
                 == [600 + len(sentence) for sentence in SENTENCES]
                 and stored[0]["reasoning_tokens"] == 12,
                 [dict(row) for row in stored])
    fragments = handle.execute(
        "SELECT * FROM fragments WHERE session_id = 1 ORDER BY id").fetchall()
    checks.check("every fragment the recognizer sent was kept, in order",
                 [row["text"] for row in fragments]
                 == [json.loads(message)["channel"]["alternatives"][0]
                     ["transcript"] for message in FRAGMENTS]
                 + ["Let us pray."],
                 [row["text"] for row in fragments])
    checks.check("noting which were finished on request",
                 [row["finalized"] for row in fragments] == [0, 0, 0, 0, 1],
                 [row["finalized"] for row in fragments])
    checks.check("with its timing, so the meeting can be replayed",
                 [(row["start"], row["audio_end"], row["speech_final"])
                  for row in fragments[2:3]] == [(5.0, 6.5, 1)]
                 and all(row["run"] == 1 and row["at"] for row in fragments),
                 [dict(row) for row in fragments])
    words = json.loads(fragments[2]["words"] or "[]")
    checks.check("and each word's own start and end",
                 [word for word, _, _ in words]
                 == ["Brother", "Kalema", "will", "pray."]
                 and words[0][1] == 5.0, words)
    checks.check("and how far behind its audio's end was as it arrived",
                 all(row["lag"] is not None for row in fragments),
                 [row["lag"] for row in fragments])
    guessed = handle.execute("SELECT * FROM guesses").fetchall()
    checks.check("a guess that asked for a finish is kept, with its unseen "
                 "audio", [(row["audio_end"], row["unseen"], row["decision"])
                           for row in guessed] == [(9.0, 0.1, "sent")],
                 [dict(row) for row in guessed])
    grouped = record.translations_of(handle, 1)
    checks.check("the translation was stored against its language",
                 grouped[1][0]["language"] == "French"
                 and grouped[1][0]["text"] == f"<French> {FIRST_SENTENCE}",
                 [dict(row) for row in grouped[1]])
    checks.check("a translation the model produced is marked as its own",
                 grouped[1][0]["source"] == "model", grouped[1][0]["source"])
    checks.check("Swahili had no reader, so nothing was recorded for it",
                 all(row["language"] != "Swahili"
                     for rows in grouped.values() for row in rows), grouped)
    recorded = record.resolve_session(handle, "last")
    checks.check("the room was written down with the session",
                 record.room_of(recorded) == "chapel", dict(recorded))
    report = Path(tempfile.mkdtemp()) / "chapel.md"
    record.write_report(handle, recorded, str(report))
    heading = report.read_text(encoding="utf-8").splitlines()[0]
    checks.check("and the document read back afterwards names it",
                 heading.startswith("# Session 1, chapel, "), heading)
    checks.check("and reports what the calls cost",
                 "## Tokens" in report.read_text(encoding="utf-8"))
    windows = record.audio_rows(handle, 1)
    checks.check("each audio window records what reached the recognizer",
                 windows[0]["results"] == 5 and windows[0]["empty"] == 1
                 and windows[0]["captured"] == 5.0
                 and windows[0]["rms_db"] == -6.0
                 and abs(windows[0]["received"] - 5.0) < 1e-6,
                 dict(windows[0]))
    stretches = record.silent_stretches(windows)
    checks.check("windows with only empty results make one stretch",
                 len(stretches) == 1 and len(stretches[0][2]) == 6
                 and abs(stretches[0][1] - stretches[0][0] - 30) < 0.5,
                 [(start, end, len(rows)) for start, end, rows in stretches])
    text = report.read_text(encoding="utf-8")
    checks.check("the report says what ended the recognizer's pieces",
                 "5 pieces, early_finalize on" in text
                 and "finished on request at a sentence end: 1" in text
                 and "ended by a pause: 1" in text
                 and "ended on the recognizer's own clock: 3" in text,
                 text[text.find("## Recognizer"):][:400])
    checks.check("and the report says it was a quiet feed, not lost audio",
                 "## Stretches with no words" in text
                 and "30s, level -60 dBFS" in text
                 and "nobody was speaking into it" in text,
                 text[text.find("## Stretches"):][:300])
    lost = [{"run": 1, "at": 5.0 * step, "captured": 5.0,
             "received": 1.0, "rms_db": -20.0, "peak_db": -3.0,
             "results": 0, "empty": 0} for step in range(1, 6)]
    checks.check("audio that did not reach the recognizer is named so",
                 "reached the recognizer"
                 in record.explain_silence(20.0, lost[1:]))
    loud = [dict(row, received=5.0) for row in lost]
    checks.check("and audio at speaking level with nothing back is too",
                 "sent nothing back" in record.explain_silence(20.0, loud[1:]))
    muted = [dict(row, captured=0.0, received=0.0) for row in lost]
    checks.check("a microphone that delivered nothing is named first",
                 "microphone delivered" in record.explain_silence(20.0,
                                                                  muted[1:]))
    handle.close()

    # The handle main passes it, which reads rows by name.
    handle = record.open_read_only(str(path))
    # A negative age puts the cutoff in the future, so everything goes.
    record.purge(handle, -1)
    left = [handle.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("sessions", "lines", "translations", "fragments",
                          "audio", "guesses")]
    checks.check("a purge takes the fragments, audio, and guesses with the "
                 "lines", left == [0, 0, 0, 0, 0, 0], left)
    handle.close()

    check_spoken(checks)
    check_names(checks)
    check_levels(checks)

    checks.section("A recording made before the room column existed")
    path = Path(tempfile.mkdtemp()) / "earlier.db"
    earlier = sqlite3.connect(str(path))
    earlier.execute(
        "CREATE TABLE sessions (id INTEGER PRIMARY KEY, started_at REAL,"
        " ended_at REAL, device TEXT, asr_model TEXT, model TEXT,"
        " languages TEXT, correct_english INTEGER, settings TEXT,"
        " glossary TEXT, keyterms TEXT, prompt_hash TEXT)")
    # And lines from before the calls reported their tokens.
    earlier.execute(
        "CREATE TABLE lines (id INTEGER PRIMARY KEY,"
        " session_id INTEGER NOT NULL, seq INTEGER NOT NULL, run INTEGER,"
        " at REAL, audio_end REAL, lag REAL, confidence REAL, heard TEXT,"
        " english TEXT, reason TEXT, latency REAL, outcome TEXT,"
        " requested TEXT, UNIQUE (session_id, seq))")
    # And fragments from before a piece could be finished on request.
    earlier.execute(
        "CREATE TABLE fragments (id INTEGER PRIMARY KEY,"
        " session_id INTEGER NOT NULL, run INTEGER, at REAL, start REAL,"
        " audio_end REAL, speech_final INTEGER, confidence REAL, text TEXT)")
    earlier.execute(
        "INSERT INTO fragments (session_id, start, audio_end, speech_final,"
        " text) VALUES (1, 0.0, 4.0, 0, 'brothers and sisters')")
    # The rest of the file as it is today, which is what a database
    # recorded by an earlier version actually looks like.
    earlier.executescript(record.SCHEMA)
    earlier.execute("INSERT INTO sessions (started_at, device) VALUES (?,?)",
                    (time.time(), "a microphone from last year"))
    earlier.commit()
    earlier.close()

    handle = record.open_read_only(str(path))
    old_session = record.resolve_session(handle, "last")
    checks.check("reading it back says no room rather than raising",
                 record.room_of(old_session) == "", dict(old_session))
    earlier_report = Path(tempfile.mkdtemp()) / "earlier.md"
    record.write_report(handle, old_session, str(earlier_report))
    handle.close()
    checks.check("and its pieces are counted as never finished on request",
                 "finished on request at a sentence end: 0"
                 in earlier_report.read_text(encoding="utf-8"))

    handle = record.connect(str(path))
    columns = [row[1] for row in handle.execute("PRAGMA table_info(sessions)")]
    checks.check("opening it to record gains the column",
                 "room" in columns, columns)
    columns = [row[1] for row in handle.execute("PRAGMA table_info(lines)")]
    checks.check("and the token columns on its lines",
                 all(column in columns for column in record.TOKEN_COLUMNS),
                 columns)
    columns = [row[1] for row in
               handle.execute("PRAGMA table_info(fragments)")]
    checks.check("and the finished-on-request column on its fragments",
                 "finalized" in columns, columns)
    kept = [row[0] for row in
            handle.execute("SELECT device FROM sessions")]
    checks.check("and the session already in the file is still there",
                 kept == ["a microphone from last year"], kept)
    handle.close()

    checks.section("English is corrected for the record even with no reader")
    path = Path(tempfile.mkdtemp()) / "noreader.db"
    translator = FakeTranslator()
    session, hub = make_session(["French"], True, ["French"])
    session.config["record"] = True
    session.config["database"] = str(path)
    session.started_at = time.time()
    await session._open_recorder()
    session.translator = translator
    await drive(session, translator)
    await session.recorder.close()
    checks.check("English was requested although nobody was reading it",
                 all("English" in call["outputs"]
                     for call in translator.calls), translator.calls)
    handle = record.open_read_only(str(path))
    stored = record.lines_of(handle, 1)
    checks.check("so there is a corrected line to compare against",
                 all(row["english"] for row in stored),
                 [row["english"] for row in stored])
    handle.close()

    checks.section("A failed translation is recorded as what readers saw")
    path = Path(tempfile.mkdtemp()) / "failed.db"
    translator = FakeTranslator(mode="fail")
    session, hub = make_session(["French"], False, ["French"])
    session.config["record"] = True
    session.config["database"] = str(path)
    session.started_at = time.time()
    await session._open_recorder()
    session.translator = translator
    await drive(session, translator)
    await session.recorder.close()
    handle = record.open_read_only(str(path))
    grouped = record.translations_of(handle, 1)
    rows = [row for rows in grouped.values() for row in rows]
    checks.check("the fallback was stored, not left as a missing row",
                 len(rows) == 3, len(rows))
    checks.check("and it says why the reader saw English",
                 all(row["source"] == "failure" for row in rows),
                 [row["source"] for row in rows])
    handle.close()

    checks.section("A database that will not answer costs only the record")
    translator = FakeTranslator()
    session, hub = make_session(["French"], False, ["French"])
    session.translator = translator

    class BrokenRecorder:
        session_id = 1
        dropped = 0

        def line(self, **row):
            raise OSError("no space left on device")

        def correction(self, **row):
            raise OSError("no space left on device")

        def translation(self, **row):
            raise OSError("no space left on device")

    session.recorder = BrokenRecorder()
    raised = None
    try:
        await drive(session, translator)
    except Exception as exc:
        raised = exc
    checks.check("the pipeline did not raise", raised is None, repr(raised))
    french = [entry["text"] for entry in hub.buffers["French"]]
    checks.check("every subtitle still reached the readers",
                 french == [f"<French> {name}" for name in SENTENCES], french)

    checks.section("Reading a recording back")
    repair = record.changed_words("brother kalema will pray",
                                  "Brother Kalema will pray")
    checks.check("a recapitalized name is a term for keyterms.txt",
                 repair == (["Kalema"], ["kalema"]), repair)
    checks.check("but it is not reported as an invented name",
                 record.changed_words("brother kalema will pray",
                                      "Brother Kalema will pray",
                                      fold=True)[0] == [], repair)
    invented = record.changed_words("and then he read from the book",
                                    "and then he read from Moroni",
                                    fold=True)[0]
    checks.check("a name with no counterpart in the audio is reported",
                 invented == ["moroni"], invented)
    opening = record.changed_words("we are reading today",
                                   "We are reading today")
    checks.check("capitalizing the first word is not a finding",
                 opening == ([], []), opening)

    worth = record.worth_a_keyterm
    checks.check("a common word the correction changed is not a key term",
                 not worth("The", ["the", "prince"])
                 and not worth("of", ["or"]), "common words listed")
    checks.check("nor is a number written as digits",
                 not worth("2", ["two"]))
    checks.check("nor a name that only lost its possessive",
                 not worth("Isaiah", ["Isaiah's", "chapter"]))
    checks.check("but a name the recognizer missed is",
                 worth("Immanuel", ["Emmanuel"]))
    checks.check("and so is one it heard without its capital",
                 worth("Kalema", ["kalema"]))

    worklist = Path(tempfile.mkdtemp()) / "worklist.db"
    handle = record.connect(str(worklist))
    handle.execute("INSERT INTO sessions (started_at, languages) VALUES "
                   "(?, '[]')", (time.time(),))
    handle.execute(
        "INSERT INTO lines (session_id, seq, heard, english, reason) VALUES"
        " (1, 1, 'read chapter two. the words of brother kalema on"
        " emmanuel', 'Read chapter 2. The words of Brother Kalema on"
        " Immanuel', 'endpoint')")
    handle.close()
    handle = record.open_read_only(str(worklist))
    listed = Path(tempfile.mkdtemp()) / "worklist.md"
    record.write_report(handle, record.resolve_session(handle, "last"),
                        str(listed))
    handle.close()
    written = listed.read_text(encoding="utf-8")
    table = [row.split("|")[1].strip() for row in written.splitlines()
             if row.startswith("| ") and not row.startswith("| term")]
    checks.check("the worklist in the report keeps the misheard name and "
                 "drops the rest", table == ["Immanuel"], table)
    # Half the third field test's table was words like "savior" heard right
    # and only capitalized, above the terms the recognizer missed.
    checks.check("a name heard right and only capitalized is listed apart",
                 "only capitalized, so a key term is needed only for a "
                 "name: Brother (1), Kalema (1)" in written,
                 written[written.find("## Terms"):][:900])

    report = Path(tempfile.mkdtemp()) / "review.md"
    handle = record.open_read_only(str(path))
    record.write_report(handle, record.resolve_session(handle, "last"),
                        str(report))
    handle.close()
    written = report.read_text(encoding="utf-8")
    missing = [heading for heading in (
        "## Names to check", "## Terms to add to keyterms.txt",
        "## How sentences closed", "## Latency",
        "## Where readers saw English instead", "## Transcript")
        if heading not in written]
    checks.check("the report has every section", not missing, missing)
    checks.check("the transcript carries a Comments block per line",
                 written.count("> Comments:") == 3,
                 written.count("> Comments:"))

    checks.section("A database that cannot be opened does not stop a meeting")
    session, hub = make_session(["French"], False, [])
    session.config["record"] = True
    # A directory that does not exist, which is what a mistyped path or an
    # unplugged drive looks like on a Sunday morning.
    session.config["database"] = "/nonexistent-directory/sessions.db"
    session.started_at = time.time()
    await session._open_recorder()
    checks.check("recording is off", not session.recording(), session.error)
    checks.check("and the operator is told why",
                 bool(session.error) and "Not recording" in session.error,
                 session.error)


# -- the operator script -----------------------------------------------------


def check_script(checks):
    """transept.py, which is how most Sundays start and end.

    No funnel and no server here: what these check is the reasoning the
    script does on its own, and above all the rule that keeps a stale
    record from costing somebody an unrelated process.
    """
    home = Path(tempfile.mkdtemp())
    kept = (transept.PID_FILE, transept.LOG_FILE)

    checks.section("Reading the ports out of config.toml")
    missing = home / "no-such-config.toml"
    checks.check("a missing config leaves the defaults in place",
                 transept.read_config(missing)
                 == {"reader_port": 8080, "operator_port": 8081,
                     "public_url": "", "sends": False},
                 transept.read_config(missing))
    written = home / "config.toml"
    written.write_text('[server]\nreader_port = 9000\n'
                       'operator_port = 9001\n'
                       'public_url = "https://chapel.example/"\n')
    checks.check("the ports and the address come out of the file",
                 transept.read_config(written)
                 == {"reader_port": 9000, "operator_port": 9001,
                     "public_url": "https://chapel.example/",
                     "sends": False},
                 transept.read_config(written))

    # Which half this laptop runs is read off the control token, so the
    # weekly command is one command whichever way the room is set up.
    with cleared_secrets():
        written.write_text('[keys]\ncontrol_token = "abc"\n')
        checks.check("a laptop holding a control token is a sender",
                     transept.read_config(written)["sends"])
        written.write_text('[keys]\ncontrol_token = "abc"\n'
                           '[audio]\nbackend = "remote"\n')
        checks.check("but not the hosted server's own file, which has one",
                     not transept.read_config(written)["sends"])
        written.write_text('[keys]\ncontrol_token = ""\n')
        checks.check("an empty token is no token",
                     not transept.read_config(written)["sends"])
        os.environ["CONTROL_TOKEN"] = "abc"
        checks.check("and the environment counts, as load_keys reads it",
                     transept.read_config(written)["sends"])
    # A config that will not parse and a port that is not a number both have
    # to stop here, rather than be passed on to something that probes ports
    # and sends signals.
    for label, text in (("a port that is not a number",
                         '[server]\nreader_port = "eight"\n'),
                        ("a file that is not TOML", "[server\n")):
        broken = home / "broken.toml"
        broken.write_text(text)
        refused = False
        try:
            transept.read_config(broken)
        except SystemExit:
            refused = True
        checks.check(f"{label} is refused, not passed on", refused)

    checks.section("Finding the server to stop")
    transept.PID_FILE = home / "transept.pid"
    checks.check("no record at all means no server",
                 transept.server_record() is None)
    holder = socket.socket()
    holder.bind(("127.0.0.1", 0))
    holder.listen(5)
    held = holder.getsockname()[1]
    quiet = free_port()
    finished = subprocess.Popen([sys.executable, "-c", "pass"])
    finished.wait()
    try:
        transept.PID_FILE.write_text(json.dumps(
            {"pid": os.getpid(), "reader_port": held,
             "operator_port": quiet}))
        found = transept.server_record()
        checks.check("a live process still holding a port is the server",
                     found is not None and found["pid"] == os.getpid(), found)
        checks.check("a record with no role is a server",
                     found is not None and found["role"] == "server", found)

        # A sender binds only the operator port, so its record names only
        # that one, and that has to be enough to find it.
        transept.PID_FILE.write_text(json.dumps(
            {"pid": os.getpid(), "role": "sender", "operator_port": held}))
        found = transept.server_record()
        checks.check("a sender is found by its one port, and named",
                     found is not None and found["role"] == "sender", found)

        transept.PID_FILE.write_text(json.dumps(
            {"pid": os.getpid(), "reader_port": quiet,
             "operator_port": quiet}))
        checks.check("a record whose ports have gone quiet is a leftover",
                     transept.server_record() is None)

        # The whole reason the ports are consulted at all: process ids come
        # around again, and the next thing the caller does is send a signal.
        transept.PID_FILE.write_text(json.dumps(
            {"pid": finished.pid, "reader_port": held,
             "operator_port": quiet}))
        checks.check("and so is one naming a process that has finished",
                     transept.server_record() is None)

        transept.PID_FILE.write_text("half a { file")
        checks.check("an unreadable record is a leftover too",
                     transept.server_record() is None)
    finally:
        holder.close()

    checks.section("Reading the addresses back out of the log")
    transept.LOG_FILE = home / "transept.log"
    transept.LOG_FILE.write_text(
        "Reader:   https://chapel.example/reader?token=abc\n"
        "That address carries a token minted for this run.\n"
        "Operator: http://127.0.0.1:8081/operator?token=xyz\n")
    checks.check("the reader address comes back whole",
                 transept.banner("Reader:")
                 == "Reader:   https://chapel.example/reader?token=abc",
                 transept.banner("Reader:"))
    checks.check("and the operator address with its own token",
                 transept.banner("Operator:").endswith("token=xyz"),
                 transept.banner("Operator:"))
    checks.check("a line that is not there is empty, not half an address",
                 transept.banner("Funnel:") == "")
    checks.check("there is an interpreter to start the server with",
                 Path(transept.server_python()).exists(),
                 transept.server_python())
    transept.PID_FILE, transept.LOG_FILE = kept


# -- the linter --------------------------------------------------------------


def check_lint(checks):
    """Run ruff over the project, using the rules in ruff.toml.

    Linting lives here because there is no CI and no build step, so a check
    nobody is reminded to run is a check nobody runs.
    """
    checks.section("Ruff")
    try:
        result = subprocess.run(
            [sys.executable, "-m", "ruff", "check", "."],
            cwd=ROOT, capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.SubprocessError) as exc:
        checks.check("ruff is installed",
                     False, f"{exc}. pip install -r requirements.txt")
        return
    output = (result.stdout + result.stderr).strip()
    checks.check("ruff reports nothing", result.returncode == 0,
                 output or f"exit {result.returncode}")


# -- entry point -------------------------------------------------------------


SECTIONS = ("lint", "pipeline", "session", "store", "server", "script")


def main():
    wanted = sys.argv[1:] or list(SECTIONS)
    unknown = [name for name in wanted if name not in SECTIONS]
    if unknown:
        sys.exit(f"Unknown section {unknown[0]!r}. "
                 f"Choose from {', '.join(SECTIONS)}.")

    checks = Checks()
    if "lint" in wanted:
        check_lint(checks)
    if "pipeline" in wanted:
        asyncio.run(check_pipeline(checks))
    if "session" in wanted:
        asyncio.run(check_session(checks))
        asyncio.run(check_local_encoder(checks))
        asyncio.run(check_sender_proxy(checks))
    if "store" in wanted:
        asyncio.run(check_store(checks))
    if "server" in wanted:
        check_server(checks)
        check_control(checks)
        check_sender(checks)
        asyncio.run(drive_audio(checks))
    if "script" in wanted:
        check_script(checks)
    return checks.report()


if __name__ == "__main__":
    sys.exit(main())
