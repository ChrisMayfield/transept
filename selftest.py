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

Usage:
    python3 selftest.py             everything
    python3 selftest.py lint        ruff, with the rules in ruff.toml
    python3 selftest.py pipeline    the shared loops, against both sinks
    python3 selftest.py session     a whole Session start and stop
    python3 selftest.py store       recording a session and reading it back
    python3 selftest.py server      boot server.py and exercise the routes
    python3 selftest.py script      the operator script, without a funnel

Exits non-zero if any check fails.
"""

import asyncio
import contextlib
import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace

import capture
import pipeline
import record
import server
import transept

HERE = Path(__file__).parent


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


def asr_result(text, speech_final, start, duration):
    """One finalized result, shaped the way Deepgram sends it."""
    return json.dumps({
        "type": "Results",
        "is_final": True,
        "speech_final": speech_final,
        "start": start,
        "duration": duration,
        "channel": {"alternatives": [{"transcript": text}]},
    })


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
        self.close_stream_sent = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def send(self, data):
        if isinstance(data, str) and "CloseStream" in data:
            self.close_stream_sent = True
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
        return rendered, 0.5

    async def close(self):
        pass


def fake_config(**overrides):
    """A Session config carrying every key the pipeline reads."""
    config = {
        "languages": ["French", "Swahili"], "grace": 90.0,
        "correct_english": True, "ceiling": 4.0, "gap": 0.6, "hold": 8.0,
        "capture": "auto", "asr_model": "nova-3",
        "endpointing": 400, "keyterms": ["Kalema"], "glossary": "",
        "model": "fake-model", "max_tokens": 2000, "timeout": 15.0,
        "reasoning_effort": "low", "idle_stop": 0, "public_url": "",
        "reader_url": "", "max_languages": 0,
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


async def check_pipeline(checks):
    checks.section("Segmenter and the shared loops, printing to a terminal")
    translator = FakeTranslator()
    sink = pipeline.TerminalSink(["English", "French"], started=0.0,
                                 color=False)
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
    checks.check("no cap means no ceiling, which is the old behaviour",
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
        checks.check("interim results are off",
                     "interim_results=false" in url, reached)

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


def sse_preamble(port, token, channel, size=13, timeout=5.0):
    """The first bytes of an event stream, plus its content type."""
    url = f"http://127.0.0.1:{port}/stream/{channel}?token={token}"
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return (response.headers.get("Content-Type"),
                response.read(size).decode("utf-8"))


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
    """The token rule, without needing a second server on a second port."""

    class StubRequest:
        def __init__(self, token, query=None, header=None, session=None):
            self.app = {"token": token, "session": session}
            self.query = {"token": query} if query else {}
            self.headers = {"X-Subtitle-Token": header} if header else {}

    checks.section("The token rule")
    checks.check("a configured token refuses a request without one",
                 not server.authorized(StubRequest("secret")))
    checks.check("the token is accepted in the query string",
                 server.authorized(StubRequest("secret", query="secret")))
    checks.check("the token is accepted in a header",
                 server.authorized(StubRequest("secret", header="secret")))
    checks.check("a wrong token is refused",
                 not server.authorized(StubRequest("secret", query="wrong")))
    # The two listeners hold different tokens under the same key, which is
    # the whole of what keeps a card handed to the room off the controls.
    checks.check("the token for the other listener is just a wrong token",
                 not server.authorized(StubRequest("operator-secret",
                                                   query="reader-secret")))

    def refuses(query):
        # Report a raise as a failed check, not as a crashed run.
        try:
            return not server.authorized(StubRequest("secret", query=query))
        except TypeError:
            return False

    # Guards the encode, not the constant-time comparison: reverting to ==
    # also passes this, since == was never the unsafe part.
    checks.check("a token outside ASCII is refused rather than raising",
                 refuses("caf\u00e9"))

    # The rule is worth only as much as the routes that apply it.
    session = make_session(["French"], True, [])[0]
    refused = asyncio.run(server.api_status(
        StubRequest("secret", session=session)))
    payload = json.loads(refused.text)
    checks.check("status without the token is refused",
                 refused.status == 403 and payload["ok"] is False,
                 f"{refused.status} {payload}")
    # The page polls this every two seconds and reads ok and message off
    # a refusal; without the message it shows a blank panel.
    checks.check("a refused status says why, for the page to show",
                 payload.get("message"), payload)
    allowed = asyncio.run(server.api_status(
        StubRequest("secret", header="secret", session=session)))
    checks.check("status with the token is allowed",
                 allowed.status == 200
                 and json.loads(allowed.text)["state"] == "stopped",
                 allowed.status)

    # The reader routes answer with subtitles and with what the channels
    # are, and opening a stream is what makes a language be paid for, so
    # the same rule applies to them.
    refused = asyncio.run(server.api_channels(StubRequest("secret")))
    payload = json.loads(refused.text)
    checks.check("the channel list without the token is refused",
                 refused.status == 403 and payload["channels"] == [],
                 f"{refused.status} {payload}")
    # The reader page destructures channels off this, so a refusal that
    # dropped the key would throw before the page could show anything.
    checks.check("a refused channel list keeps the shape the page reads",
                 "channels" in payload and "error" in payload, payload)
    refused = asyncio.run(server.reader_page(StubRequest("secret")))
    checks.check("the reader page without the token is refused",
                 refused.status == 403, refused.status)
    blank = asyncio.run(server.nothing_here(StubRequest("secret")))
    checks.check("the bare root is a 404 even with the token",
                 blank.status == 404, blank.status)


def check_example_config(checks):
    """The tables in pipeline.py and config.example.toml still agree.

    They are read side by side whenever a setting is added, so a key in one
    and not the other, or the same keys in a different order, is a defect
    even though nothing breaks at runtime.
    """
    checks.section("config.example.toml against SETTINGS and SECRETS")
    section, listed = None, []
    for line in (HERE / "config.example.toml").read_text().splitlines():
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


def check_keys(checks):
    """Keys come out of config.toml, and the environment still wins.

    The variables are popped for the duration, because this machine has a
    working config.toml and a shell that may export any of them, and a check
    that reads a real key would pass for the wrong reason.
    """
    checks.section("Keys in config.toml")
    config = {"keys": {"deepgram_api_key": "file-deepgram",
                       "llm_base_url": "  http://file/v1  ",
                       "llm_api_key": "file-llm",
                       "operator_token": "file-token"}}
    checks.check("every variable is its key in capitals, as the file claims",
                 all(variable == key.upper()
                     for _, key, variable in pipeline.SECRETS),
                 pipeline.SECRETS)
    kept = {name: os.environ.pop(name, None)
            for _, _, name in pipeline.SECRETS}
    try:
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
    finally:
        for name, value in kept.items():
            os.environ.pop(name, None)
            if value is not None:
                os.environ[name] = value


def check_device_listing(checks):
    """The device list names one source to pre-select, so the page can.

    The page sorts by name and so has lost the backend's order, and without
    a suggestion from here the browser falls to the first option, which on a
    PulseAudio machine is the playback monitor rather than the microphone.
    capture.choose_default holds the rule; this checks that api_devices
    hands the answer over.
    """

    class StubRequest:
        """Authorized unless supplied is withheld; no tokenless mode."""

        def __init__(self, config, supplied="secret"):
            self.app = {"session": SimpleNamespace(config=config),
                        "token": "secret"}
            self.query = {"token": supplied} if supplied else {}
            self.headers = {}

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
            await server.api_devices(request)
        finally:
            ticker.cancel()
        return counted

    checks.section("The device list")
    request = StubRequest(fake_config())
    original = server.capture.list_devices
    try:
        server.capture.list_devices = listed
        payload = json.loads(asyncio.run(server.api_devices(request)).text)
        checks.check("the playback monitor is not what gets pre-selected",
                     payload.get("suggested") == "fake", payload)
        server.capture.list_devices = raises
        payload = json.loads(asyncio.run(server.api_devices(request)).text)
        checks.check("a backend that cannot list is reported, not raised",
                     payload["devices"] == [] and "pactl" in payload["error"],
                     payload)

        server.capture.list_devices = listed
        locked = StubRequest(fake_config(), supplied=None)
        response = asyncio.run(server.api_devices(locked))
        payload = json.loads(response.text)
        checks.check("listing devices without the token is refused",
                     response.status == 403 and payload["devices"] == [],
                     f"{response.status} {payload}")
        # The page destructures the list, so a refusal needs the same
        # shape or it throws before it can show the message.
        checks.check("a refused listing still tells the operator page why",
                     "error" in payload, payload)
        opened = StubRequest(fake_config())
        payload = json.loads(asyncio.run(server.api_devices(opened)).text)
        checks.check("listing devices with the token is allowed",
                     payload.get("suggested") == "fake", payload)

        server.capture.list_devices = slow
        counted = asyncio.run(ticks_while_listing(request))
        checks.check("a slow device listing leaves the event loop running",
                     counted >= 5, f"{counted} turns during a 0.2s listing")
    finally:
        server.capture.list_devices = original


def check_server(checks):
    """Boot server.py for real and exercise every route.

    A missing config file is deliberate: resolve falls through to the
    defaults, so the checks below do not depend on whatever config.toml
    happens to say on this machine.
    """
    check_authorization(checks)
    check_example_config(checks)
    check_keys(checks)
    check_device_listing(checks)
    checks.section("Minting the two tokens")
    minted = {server.mint_token() for _ in range(3)}
    checks.check("a token with nothing set is minted fresh every run",
                 len(minted) == 3 and all(len(one) >= 32
                                          for one in minted), minted)
    checks.check("an empty setting still mints one",
                 len(server.mint_token("")) >= 32)
    checks.check("a pinned token is kept across restarts",
                 server.mint_token("bench") == "bench")

    checks.section("The address on the card")
    checks.check("the reader address carries a path and a token",
                 server.reader_address("https://chapel.example/", "abc")
                 == "https://chapel.example/reader?token=abc",
                 server.reader_address("https://chapel.example/", "abc"))
    checks.check("a base without a trailing slash builds the same address",
                 server.reader_address("https://chapel.example", "abc")
                 == "https://chapel.example/reader?token=abc")
    # Rather than a link to /reader?token= on nowhere, which the operator
    # page would render as something to hand somebody.
    checks.check("no public_url means no address at all",
                 server.reader_address("", "abc") == "")

    checks.section("The running server")
    port, operator = free_port(), free_port()
    environment = dict(
        os.environ,
        DEEPGRAM_API_KEY="selftest",
        LLM_API_KEY="selftest",
        LLM_BASE_URL="http://invalid.invalid/v1",
        # Explicitly empty, so the minted-token checks below test minting
        # rather than whatever this machine happens to have in config.toml.
        READER_TOKEN="",
        OPERATOR_TOKEN="",
    )
    # In a directory of its own, because server.py records itself in the
    # one it starts in, and a selftest that overwrote the record of a real
    # server would leave a running meeting with nothing able to stop it.
    home = Path(tempfile.mkdtemp())
    pid_file = home / "transept.pid"
    process = subprocess.Popen(
        [sys.executable, "-u", str(HERE / "server.py"),
         "--config", "selftest-no-such-config.toml",
         "--model", "selftest-model", "--languages", "French,Swahili",
         "--host", "127.0.0.1", "--port", str(port),
         "--operator-port", str(operator), "--max-readers", "3"],
        cwd=home, env=environment, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True)
    lines, reader = drain(process)
    lingering = None
    try:
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
                     (written.get("port"), written.get("operator_port"))
                     == (port, operator), written)

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

        content_type, preamble = sse_preamble(port, READER, "English")
        checks.check("the event stream announces itself correctly",
                     content_type == "text/event-stream", content_type)
        checks.check("the event stream opens with a reconnect interval",
                     preamble.startswith("retry:"), repr(preamble))

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
    finally:
        process.terminate()
        forced = False
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            forced = True
            process.kill()
            process.wait()
        if lingering:
            lingering.close()
        reader.join(timeout=2)
        output = "".join(lines)

    checks.check("the startup banner prints the reader address",
                 f"http://127.0.0.1:{port}/reader?token=" in output, output)
    checks.check("the banner sends the operator to the loopback port",
                 f"http://127.0.0.1:{operator}/operator" in output, output)
    checks.check("the banner says the operator address is not a bookmark",
                 "changes" in output and "every restart" in output, output)
    checks.check("a minted token is not the empty OPERATOR_TOKEN",
                 len(TOKEN) >= 32, TOKEN)
    checks.check("a minted token is not the empty READER_TOKEN",
                 len(READER) >= 32, READER)
    checks.check("binding to loopback says so, since a phone cannot reach it",
                 "Bound to this machine only" in output, output)
    checks.check("the server exited while a phone was still reading",
                 not forced, "it ignored the signal and had to be killed")
    # A record left behind would send the next stop after whatever process
    # the system has since given that number to.
    checks.check("and took its record away on the way out",
                 not pid_file.exists(), pid_file)


# -- recording a session -----------------------------------------------------


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
    session.started_at = time.time()
    session.run = 1
    session.run_started = time.monotonic()
    await session._open_recorder()
    session.translator = translator
    checks.check("the recorder opened", session.recording(), session.error)
    await drive(session, translator)
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

    report = Path(tempfile.mkdtemp()) / "review.md"
    handle = record.open_read_only(str(path))
    record.write_report(handle, record.resolve_session(handle, "last"),
                        str(report))
    handle.close()
    written = report.read_text(encoding="utf-8")
    for heading in ("## Names to check", "## Terms to add to keyterms.txt",
                    "## How sentences closed", "## Latency",
                    "## Where readers saw English instead", "## Transcript"):
        checks.check(f"the report has its {heading.strip('# ')!r} section",
                     heading in written, written[:120])
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
                 transept.read_config(missing) == (8080, 8081, ""),
                 transept.read_config(missing))
    written = home / "config.toml"
    written.write_text('[server]\nport = 9000\noperator_port = 9001\n'
                       'public_url = "https://chapel.example/"\n')
    checks.check("the ports and the address come out of the file",
                 transept.read_config(written)
                 == (9000, 9001, "https://chapel.example/"),
                 transept.read_config(written))
    # A config that will not parse and a port that is not a number both have
    # to stop here, rather than be passed on to something that probes ports
    # and sends signals.
    for label, text in (("a port that is not a number",
                         '[server]\nport = "eight thousand"\n'),
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
            {"pid": os.getpid(), "port": held, "operator_port": quiet}))
        found = transept.server_record()
        checks.check("a live process still holding a port is the server",
                     found is not None and found["pid"] == os.getpid(), found)

        transept.PID_FILE.write_text(json.dumps(
            {"pid": os.getpid(), "port": quiet, "operator_port": quiet}))
        checks.check("a record whose ports have gone quiet is a leftover",
                     transept.server_record() is None)

        # The whole reason the ports are consulted at all: process ids come
        # around again, and the next thing the caller does is send a signal.
        transept.PID_FILE.write_text(json.dumps(
            {"pid": finished.pid, "port": held, "operator_port": quiet}))
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
            cwd=HERE, capture_output=True, text=True, timeout=120)
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
    if "store" in wanted:
        asyncio.run(check_store(checks))
    if "server" in wanted:
        check_server(checks)
    if "script" in wanted:
        check_script(checks)
    return checks.report()


if __name__ == "__main__":
    sys.exit(main())
