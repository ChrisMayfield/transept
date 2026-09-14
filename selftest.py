#!/usr/bin/env python3
"""
Self test for the shared pipeline, a whole session, and the running server.

Needs no API keys, no audio device, and no network. A fake recognizer and a
fake translator stand in where the real ones would go, so the whole thing
runs in a few seconds and costs nothing.

This is not a substitute for listening to a real room. Recognition accuracy
is decided by the microphone feed and can only be judged there. What this
catches is the wiring between pipeline.py and server.py coming apart, which
is worth catching automatically because of how it failed last time: the two
files carried separate copies of the same loops, one copy drifted until it
referenced names it had never imported, and every session start raised
NameError into a supervisor that caught it and retried forever. The operator
page showed "reconnecting" and nothing else.

Usage:
    python3 selftest.py             everything
    python3 selftest.py pipeline    the shared loops, against both sinks
    python3 selftest.py session     a whole Session start and stop
    python3 selftest.py server      boot server.py and exercise the routes

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
import time
import urllib.error
import urllib.request
from pathlib import Path

import capture
import pipeline
import server

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


# Two fragments of one sentence, then a new sentence after a silent gap.
# The gap is what stops an abandoned half sentence from swallowing the next
# speaker's turn, so every run exercises it.
FRAGMENTS = [
    asr_result("the meeting will start", False, 0.0, 1.0),
    asr_result("at nine o'clock.", False, 1.0, 1.0),
    asr_result("Brother Kalema will pray", True, 5.0, 1.5),
]

FIRST_SENTENCE = "the meeting will start at nine o'clock."
SECOND_SENTENCE = "Brother Kalema will pray"


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
        return rendered, 0.5

    async def close(self):
        pass


def fake_config(**overrides):
    """A Session config carrying every key the pipeline reads."""
    config = dict(
        languages=["French", "Swahili"], grace=90.0, correct_english=True,
        ceiling=4.0, gap=0.6, hold=8.0, device="fake", capture="auto",
        asr_model="nova-3", endpointing=400, keyterms=["Kalema"],
        glossary="", model="fake-model", max_tokens=2000, timeout=15.0,
        reasoning_effort="low", idle_stop=0, public_url="",
        deepgram_key="fake", llm_key="fake", llm_base="http://invalid/v1",
    )
    config.update(overrides)
    return config


def make_session(languages, correct_english, readers):
    """A Session and its Hub, with the named channels already subscribed."""
    hub = server.Hub(languages)
    session = server.Session(
        fake_config(languages=languages, correct_english=correct_english), hub)
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
                 f"[1] en" in printed and FIRST_SENTENCE in printed, printed)
    checks.check("a silent gap closes the sentence before the next turn",
                 "[2] en" in printed and SECOND_SENTENCE in printed, printed)
    checks.check("raw fragments are printed as they arrive",
                 printed.count("  . ") == 3, printed.count("  . "))
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
    checks.check("the summary counts both calls",
                 sink.summary().startswith("2 translated, 0 failed"),
                 sink.summary())

    checks.section("The same loops, publishing to a Hub, one French reader")
    translator = FakeTranslator()
    session, hub = make_session(["French", "Swahili"], True,
                                ["English", "French"])
    session.translator = translator
    await drive(session, translator)
    english = [entry["text"] for entry in hub.buffers["English"]]
    french = [entry["text"] for entry in hub.buffers["French"]]

    checks.check("the English channel carries both sentences",
                 len(english) == 2, english)
    checks.check("the corrected English replaced the raw line",
                 english[0] == f"CORRECTED {FIRST_SENTENCE}", english)
    checks.check("the replacement reused the sequence number",
                 [entry["seq"] for entry in hub.buffers["English"]] == [1, 2],
                 [entry["seq"] for entry in hub.buffers["English"]])
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
                  session.stats["corrections"]) == (2, 2, 2), session.stats)

    checks.section("Nobody reading, and English needs no correction")
    translator = FakeTranslator()
    session, hub = make_session(["French"], False, [])
    session.translator = translator
    await drive(session, translator)
    checks.check("no model call was made at all", translator.calls == [],
                 translator.calls)
    checks.check("both sentences counted as skipped",
                 session.stats["skipped"] == 2, session.stats)
    checks.check("English is still published for whoever arrives later",
                 len(hub.buffers["English"]) == 2,
                 list(hub.buffers["English"]))

    checks.section("Translation failure falls back to English")
    translator = FakeTranslator(mode="fail")
    session, hub = make_session(["French"], False, ["French"])
    session.translator = translator
    await drive(session, translator)
    french = [entry["text"] for entry in hub.buffers["French"]]
    checks.check("French shows the English text rather than a gap",
                 french == [FIRST_SENTENCE, SECOND_SENTENCE], french)
    checks.check("both counted as failures", session.stats["failures"] == 2,
                 session.stats)

    checks.section("A translation slower than hold falls back too")
    translator = FakeTranslator(delay=0.4)
    session, hub = make_session(["French"], False, ["French"])
    session.translator = translator
    await drive(session, translator, hold=0.05)
    french = [entry["text"] for entry in hub.buffers["French"]]
    checks.check("French falls back to English on timeout",
                 french == [FIRST_SENTENCE, SECOND_SENTENCE], french)
    checks.check("both counted as timeouts", session.stats["timeouts"] == 2,
                 session.stats)


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
        checks.check("audio reached the recognizer", source.reads > 0,
                     source.reads)
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
                     english == [f"CORRECTED {FIRST_SENTENCE}",
                                 f"CORRECTED {SECOND_SENTENCE}"], english)
        checks.check("French was published",
                     french == [f"<French> {FIRST_SENTENCE}",
                                f"<French> {SECOND_SENTENCE}"], french)
        status = session.status()
        checks.check("status reports what the operator page needs",
                     status["state"] == "running" and status["units"] == 2
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


def request(port, path, method="GET", body=None, timeout=5.0):
    """Returns (status, body text). A 4xx is an answer here, not an error."""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    call = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data,
                                  headers=headers, method=method)
    try:
        with urllib.request.urlopen(call, timeout=timeout) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8")


def sse_preamble(port, channel, size=13, timeout=5.0):
    """The first bytes of an event stream, plus its content type."""
    url = f"http://127.0.0.1:{port}/stream/{channel}"
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return (response.headers.get("Content-Type"),
                response.read(size).decode("utf-8"))


def check_authorization(checks):
    """The token rule, without needing a second server on a second port."""

    class StubRequest:
        def __init__(self, token, query=None, header=None):
            self.app = {"token": token}
            self.query = {"token": query} if query else {}
            self.headers = {"X-Caption-Token": header} if header else {}

    checks.section("The operator token")
    checks.check("no token configured lets anyone in",
                 server.authorized(StubRequest(None)))
    checks.check("a configured token refuses a request without one",
                 not server.authorized(StubRequest("secret")))
    checks.check("the token is accepted in the query string",
                 server.authorized(StubRequest("secret", query="secret")))
    checks.check("the token is accepted in a header",
                 server.authorized(StubRequest("secret", header="secret")))
    checks.check("a wrong token is refused",
                 not server.authorized(StubRequest("secret", query="wrong")))


def check_server(checks):
    """Boot server.py for real and exercise every route.

    A missing config file is deliberate: resolve falls through to the
    defaults, so the checks below do not depend on whatever config.toml
    happens to say on this machine.
    """
    check_authorization(checks)
    checks.section("The running server")
    port = free_port()
    environment = dict(
        os.environ,
        DEEPGRAM_API_KEY="selftest",
        LLM_API_KEY="selftest",
        LLM_BASE_URL="http://invalid.invalid/v1",
        # load_env only fills variables that are not already set, so setting
        # this empty keeps a real .env token from locking the checks out.
        OPERATOR_TOKEN="",
    )
    process = subprocess.Popen(
        [sys.executable, "-u", "server.py",
         "--config", "selftest-no-such-config.toml",
         "--model", "selftest-model", "--languages", "French,Swahili",
         "--host", "127.0.0.1", "--port", str(port)],
        cwd=HERE, env=environment, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True)
    try:
        if not wait_for_port(port, process):
            output = process.stdout.read() if process.stdout else ""
            checks.check(f"server.py came up on port {port}", False, output)
            return

        status, body = request(port, "/api/channels")
        checks.check("channels are fixed at startup",
                     json.loads(body)["channels"] == ["English", "French",
                                                      "Swahili"], body)

        status, body = request(port, "/api/status")
        state = json.loads(body)
        checks.check("status starts stopped", state["state"] == "stopped",
                     state["state"])
        checks.check("no language is active before anyone reads one",
                     all(not language["active"]
                         for language in state["languages"]),
                     state["languages"])

        for path, expected in (("/", 200), ("/operator", 200),
                               ("/api/devices", 200)):
            status, _ = request(port, path)
            checks.check(f"{path} answers {expected}", status == expected,
                         status)

        status, body = request(port, "/qr.svg")
        checks.check("the QR endpoint explains a missing public_url",
                     status == 404 and "public_url" in body, f"{status} {body}")

        status, _ = request(port, "/stream/Klingon")
        checks.check("an unknown channel is a 404", status == 404, status)

        content_type, preamble = sse_preamble(port, "English")
        checks.check("the event stream announces itself correctly",
                     content_type == "text/event-stream", content_type)
        checks.check("the event stream opens with a reconnect interval",
                     preamble.startswith("retry:"), repr(preamble))

        status, body = request(port, "/api/start", "POST", {})
        checks.check("starting without a device is refused with a reason",
                     json.loads(body)["ok"] is False
                     and "audio source" in json.loads(body)["message"], body)

        status, body = request(port, "/api/stop", "POST", {})
        checks.check("stopping when idle is refused",
                     json.loads(body)["ok"] is False, body)

        status, body = request(port, "/api/language", "POST",
                               {"language": "French", "mode": "on"})
        checks.check("a language can be forced on", json.loads(body)["ok"],
                     body)
        state = json.loads(request(port, "/api/status")[1])
        french = [item for item in state["languages"]
                  if item["name"] == "French"][0]
        checks.check("the override shows in status and makes it active",
                     french["override"] == "on" and french["active"], french)

        status, body = request(port, "/api/language", "POST",
                               {"language": "Klingon", "mode": "on"})
        checks.check("an unknown language is refused",
                     json.loads(body)["ok"] is False, body)
    finally:
        # A graceful exit can lag by up to the event stream keepalive, since
        # the handler for a reader who has walked away only finds out at its
        # next write. Waiting that out would double the time this takes, so
        # give it a moment and then insist.
        process.terminate()
        try:
            output = process.communicate(timeout=3)[0]
        except subprocess.TimeoutExpired:
            process.kill()
            output = process.communicate()[0]

    checks.check("the startup banner prints the reader address",
                 f"http://127.0.0.1:{port}/" in output, output)
    checks.check("binding to loopback says so, because a phone cannot reach it",
                 "Bound to this machine only" in output, output)
    checks.check("the server exited when asked",
                 process.returncode is not None, process.returncode)


# -- entry point -------------------------------------------------------------


def main():
    wanted = sys.argv[1:] or ["pipeline", "session", "server"]
    unknown = [name for name in wanted
               if name not in ("pipeline", "session", "server")]
    if unknown:
        sys.exit(f"Unknown section {unknown[0]!r}. "
                 f"Choose from pipeline, session, server.")

    checks = Checks()
    if "pipeline" in wanted:
        asyncio.run(check_pipeline(checks))
    if "session" in wanted:
        asyncio.run(check_session(checks))
    if "server" in wanted:
        check_server(checks)
    return checks.report()


if __name__ == "__main__":
    sys.exit(main())
