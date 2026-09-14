# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Live captioning and translation for an in-person meeting, delivered to phones over server-sent events.
English speech in, English captions plus translations out, one channel per language.

The users are people who cannot hear well and people whose first language is not English, reading on their own phones during a church meeting.
The operator is a volunteer with a laptop who has five minutes before the meeting starts.

## Non-negotiables

The model must never invent a proper noun, name, number, date, or scripture reference that is not in the source audio.
A reader of a translated channel cannot hear the room and has no way to detect a fabricated name, so a confident wrong name is worse than visible confusion.
This rule is stated first in `SYSTEM_PROMPT` and should stay there.
If a change would loosen it, do not make the change.

The English channel is a transcript, not a summary.
It may be corrected for recognition errors using the glossary, but false starts, repetitions, and informal grammar stay as spoken, because for a deaf reader that line is the record of what was said.
Translations get latitude on grammar and phrasing because fluency requires it, never on content.

Captions must never silently stall.
When translation fails or times out, the English text is published to the translated channels so a reader sees something rather than a gap.

## Commands

Setup, once:

```
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env               # two API keys and the operator token
cp config.example.toml config.toml # everything else
```

Find an audio source before anything else, because recognition accuracy is decided almost entirely here:

```
python3 server.py --list-devices
```

A device marked as playback captures what the computer plays rather than what the microphone hears, which is the most common setup mistake.

Check the audio path without needing a translation key:

```
python3 pipeline.py --no-translate
```

Run the whole thing, which is the entire weekly command:

```
python3 server.py
```

Any setting may be overridden for a one-off, for example `python3 server.py --ceiling 6`.
The three entry points take overlapping but different flag sets, built from `SETTINGS` by `add_settings_arguments`: `server.py` alone has `--host`, `--port`, `--grace`, and `--idle-stop`, and `pipeline.py` alone has `--no-translate` and `--no-color`.

Translate a transcript offline and produce a document a native speaker can mark up:

```
python3 review.py --input transcript.txt --review review.md
```

Check the software itself, with no keys, no audio device, and no network:

```
python3 selftest.py             everything, in about four seconds
python3 selftest.py pipeline    the shared loops, against both sinks
python3 selftest.py session     a whole Session start and stop
python3 selftest.py server      boot server.py and exercise the routes
```

There is no build step and no linter configured, so `selftest.py` and `python3 -m py_compile` are the mechanical checks there are.

## Architecture

```
capture -> Deepgram websocket -> Segmenter -> Translator -> Hub -> SSE -> phones
```

`capture.py` presents one interface over two backends: `sounddevice` (PortAudio, all platforms) and `parec` (Linux only).
Both yield 16 kHz mono signed 16-bit chunks; nothing downstream knows which produced them.
An empty chunk means end of stream, which is how a device that disappears becomes a reconnect rather than a hang.

`pipeline.py` holds the shared pieces: `SETTINGS`, `resolve`, `Segmenter`, `Translator`, `SYSTEM_PROMPT`, `load_env`.
It also runs standalone as a terminal tool, which is the fastest way to debug the pipeline without the web layer.

`server.py` imports from `pipeline.py` and adds `Hub` (ring buffers and subscribers), `Session` (start, stop, supervise, reconnect), and the aiohttp routes.
The two must stay in step; the prompt lives in one place on purpose.

`review.py` is an offline tool that shares `Translator` and the prompt, differing only in retry policy: the live pipeline gets one fast retry because a meeting cannot wait, while review retries harder because nothing is waiting on the answer.

### One pipeline, two sinks

`pump_audio`, `listen`, `publish`, and `build_asr_url` live in `pipeline.py` and are the only copy.
Both entry points run the same three tasks over one websocket and differ only in where finished text goes, so that difference sits behind a sink.

A sink provides `fragment(text)`, `unit(unit)` returning the output names worth requesting, `translated`, `timed_out`, and `failed`.
`TerminalSink` prints; `Session` is its own sink and publishes to the `Hub` while keeping the counters the operator page shows.
An empty list from `unit` means do not call the model for that sentence at all, which is how an unread language costs nothing.

These loops were duplicated once, and the copies drifted until `Session._asr_url` was building a Deepgram URL from `SAMPLE_RATE` and `CHANNELS`, names `server.py` had never imported.
Every session start raised `NameError` and the supervisor retried forever, so the server could not hold a session at all.
New behavior belongs in the shared loop or in a sink method, not in a second copy of the loop.

What remains deliberately separate is task lifecycle, because the two really do differ.
`pipeline.py` waits for Ctrl-C and then gives the reader and the writer a bounded chance to drain, so the last sentence still prints.
`server.py` waits on `FIRST_COMPLETED` and cancels the rest, because a supervisor is standing by to reconnect.

### Translation is demand-driven

Recognition runs continuously while a session is on, but a language is only translated while somebody is reading it.
`Session.active_languages` decides, from operator overrides plus `Hub.wanted`, which honors `grace` so a phone locking its screen does not shut a language off.
`Translator.translate` takes an explicit `outputs` list for exactly this reason, and when that list is empty no model call is made at all and `stats["skipped"]` counts the sentence.
`outputs=None` means every configured output; an empty list means nothing.
The `None` check in `translate` is explicit rather than a truthiness test, because an empty list silently expanding to every language would bill for what nobody is reading.

### Web layer

`/` is the reader page, `/operator` the controls, `/stream/<channel>` the SSE feed for one language, plus `/api/status`, `/api/devices`, `/api/channels`, `/api/start`, `/api/stop`, `/api/language`, and `/qr.svg`.
Both pages in `static/` are single files with inline CSS and JavaScript and no build step or framework.
The reader page keeps chosen language and text size in `localStorage` and holds a screen wake lock, which is why HTTPS matters.

## Things that look wrong but are not

Segmentation exists because Deepgram finalizes text mid-sentence.
Translating each finalized fragment separately produces output like "on the heater" with no subject, which is unusable in languages that need a verb or a case ending.
Do not remove the buffer to reduce latency.

Interim recognition results are disabled.
Finalized results arrive 0.2 to 0.3 seconds behind the speaker, so interim hypotheses add flicker and buy nothing.

`Hub.publish` revises an existing entry in place when the sequence number already exists.
That is how a glossary-corrected English line replaces the raw one on phones already showing it.
Clients key on `seq`, so duplicates are replacements, not new lines.

Translation output order is enforced even though calls run concurrently.
A slow call delays the ones behind it rather than scrambling the transcript, and the `--hold` timeout bounds that delay.

`Session.start` refuses unless the state is exactly `stopped`, because `reconnecting` also means a supervisor task is alive.

`Session.stop` skips cancelling the idle watchdog when the watchdog is the caller, because a task awaiting its own completion deadlocks.

`_run_once` waits on `FIRST_COMPLETED` rather than gathering.
Whichever leg finishes first ends the session, and the other two are cancelled, because leaving them running against a socket nobody reads is how a session appears alive with no audio.

`_pump` sends `CloseStream` in a `finally` block.
Without it, a dead capture device leaves the websocket open and the session hangs in `running` with no audio and no reconnect.

The 48 kHz fallback averages groups of three samples rather than taking every third one.
Plain decimation would alias everything above 8 kHz back into the speech band.

`install_stop_handler` exists because `loop.add_signal_handler` raises `NotImplementedError` on Windows.
Platform assumptions belong in `capture.py` and that function, nowhere else.

## Conventions

Settings live in `config.toml`, resolved by the `SETTINGS` table in `pipeline.py`.
Adding a setting means adding one row there, then naming it in the `add_settings_arguments` call of whichever entry points should expose it as a flag.
Every argparse default is `None` so `resolve` can distinguish an absent flag from one that happens to match the default.
Secrets stay in `.env` and never move into `config.toml`.

Standard library first, few dependencies.
`websockets`, `httpx`, and `aiohttp` are the whole list, and adding a fourth needs a real reason.
`sounddevice` and `segno` are optional at runtime and both failures are caught and reported rather than raised.

No em-dashes in prose or comments.
Headings use colons, not dashes.
One sentence per line in Markdown files, so diffs isolate the sentence that changed.

Comments explain why, not what.
Where a line encodes a decision that was reached the hard way, say what would go wrong without it.

Prefer concrete nouns over pronouns in anything a volunteer might read.

## Testing

`selftest.py` is the whole suite, and it needs no keys, no audio device, and no network.
It uses no test framework, only the standard library, and exits non-zero if anything fails.
Run it after any change to the pipeline, the sinks, or the routes.

It has three sections, and `python3 selftest.py <section>` runs one of them.
`pipeline` drives the shared loops against both sinks with a fake recognizer and a fake translator, covering segmentation, the gap rule, revision in place under one `seq`, an unread language never reaching the model, and the English fallback on both failure and timeout.
`session` swaps `capture.open_capture` and `websockets.connect` for fakes and runs a whole `Session` from start to stop, which is the section that catches the wiring between the two files coming apart; run against the code before the sink refactor it fails eleven checks and names the `NameError`.
`server` boots `server.py` on a spare port, with a config path that does not exist so the checks do not depend on whatever `config.toml` says on this machine, and exercises every route.

Adding a check means adding one `checks.check(label, condition, detail)` line.
`FakeSocket`, `FakeSource`, and `FakeTranslator` are already there, and `FakeTranslator` takes `mode="fail"` and a `delay`, so the failure and timeout paths cost nothing to exercise.

What `selftest.py` cannot tell you is whether the captions are any good.
Recognition accuracy is decided by the microphone feed and can only be judged in the actual room.
Translation quality needs a native speaker and `review.py`.

The three-layer settings precedence was verified by resolving the example config with and without overrides.

Latency claims should come from an actual run, not an estimate.
`python3 pipeline.py --no-translate` tags each line with how far behind real time it arrived, and a full `pipeline.py` run prints translation median and p95 on exit.
`review.py` prints the same two numbers for an offline transcript.

## Open work

The reader view is a plain web page by choice, not an installable app.
Readers open a link and pick a language; a manifest and service worker would add install friction and offline machinery that a live caption feed cannot use anyway.
HTTPS is still required, because the screen wake lock needs a secure context, and a tunnel in front is the current answer.
`public_url` exists because the bind address is not the address a phone can reach; the QR endpoint renders that value, not the listener.
Nothing is persisted, so there is no transcript after a session ends.
The operator token is thin security appropriate for a local network and nothing more.
