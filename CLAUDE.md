# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.
`README.md` covers setup, tunnels, and tuning for the volunteer who runs this; the notes here are about the code.

## What this is

Live subtitling and translation for an in-person meeting, delivered to phones over server-sent events.
English speech in, English subtitles plus translations out, one channel per language.

The readers are people who cannot hear well and people whose first language is not English, on their own phones during a church meeting.
The operator is a volunteer with a laptop who has five minutes before the meeting starts.

## Non-negotiables

The model must never invent a proper noun, name, number, date, or scripture reference that is not in the source audio.
A reader of a translated channel cannot hear the room and has no way to detect a fabricated name, so a confident wrong name is worse than visible confusion.
This rule is stated first in `SYSTEM_PROMPT` and should stay there.
If a change would loosen it, do not make the change.

The English channel is a transcript, not a summary.
It may be corrected for recognition errors using the glossary, but false starts, repetitions, and informal grammar stay as spoken, because for a deaf reader that line is the record of what was said.
Translations get latitude on grammar and phrasing because fluency requires it, never on content.

Subtitles must never silently stall.
When a translation fails, times out, or comes back missing a language, the English text goes to that channel so a reader sees something rather than a gap.

## Commands

```
python3 server.py                     the whole thing, and the entire weekly command
python3 server.py --list-devices      find an audio source
python3 pipeline.py --no-translate    check the audio path, no translation key needed
python3 selftest.py [section]         lint, pipeline, session, store, server
ruff check .                          the linter alone, as selftest runs it
python3 review.py --input t.txt --review review.md    offline translation review
python3 record.py --session last --out review/sunday.md    read a session back
./transept-start.sh                   stop leftovers, start, open the funnel
./transept-stop.sh                    the funnel, then the server
```

Any setting may be overridden for a one-off, for example `python3 server.py --ceiling 6`.
The entry points expose overlapping flag sets, built from `SETTINGS` by `add_settings_arguments`.
There is no build step and no CI, so `selftest.py` is the whole mechanical check, and it runs `ruff check` as its first section for that reason.

## Architecture

```
capture -> Deepgram websocket -> Segmenter -> Translator -> Hub -> SSE -> phones
```

`capture.py` presents one interface over two backends: `sounddevice` (PortAudio, all platforms) and `parec` (Linux only).
Both yield 16 kHz mono signed 16-bit chunks, and nothing downstream knows which produced them.
An empty chunk means end of stream, which is how a device that disappears becomes a reconnect rather than a hang.

`pipeline.py` holds everything the entry points share: `SETTINGS`, `resolve`, `Segmenter`, `Translator`, `SYSTEM_PROMPT`, `load_env`, and the three loops `pump_audio`, `listen`, and `publish`.
It also runs standalone as a terminal tool, which is the fastest way to debug the pipeline without the web layer.

`server.py` adds `Hub` (ring buffers and subscribers), `Session` (start, stop, supervise, reconnect), and the aiohttp routes.

`review.py` is an offline tool sharing `Translator` and the prompt, differing only in retry policy: the live pipeline gets one fast retry because a meeting cannot wait, while review retries harder because nothing is waiting on the answer.

`record.py` is both halves of persistence: the `Recorder` a `Session` writes through, and the command that reads a session back as a Markdown document.
Recording is off unless `[session] record` is set, and nothing is ever deleted without `--purge`.

### One pipeline, two sinks

Both entry points run the same three tasks over one websocket and differ only in where finished text goes, so that difference sits behind a sink.
A sink provides `fragment(text)`, `unit(unit)` returning the output names worth requesting, `translated`, `timed_out`, and `failed`.
`TerminalSink` prints; `Session` is its own sink, publishing to the `Hub` and keeping the counters the operator page shows.
An empty list from `unit` means do not call the model for that sentence at all, which is how an unread language costs nothing.

New behavior belongs in the shared loop or in a sink method, never in a second copy of the loop.
These loops were duplicated once and the copies drifted until one was building a Deepgram URL from names its own file had never imported, so every session start raised `NameError` and the supervisor retried forever.

Task lifecycle is deliberately not shared, because the two really do differ.
`pipeline.py` waits for Ctrl-C and then gives the reader and the writer a bounded chance to drain, so the last sentence still prints.
`server.py` waits on `FIRST_COMPLETED` and cancels the rest, because a supervisor is standing by to reconnect.

### Translation is demand-driven

Recognition runs continuously while a session is on, but a language is translated only while somebody is reading it.
`Session.demand` decides from operator overrides plus `Hub.wanted`, which honors `grace` so a phone locking its screen does not shut a language off.
`Translator.translate` takes an explicit `outputs` list for exactly this reason, and an empty list means no model call at all, counted in `stats["skipped"]`.
The `None` check in `translate` is explicit rather than a truthiness test, because an empty list silently expanding to every language would bill for what nobody is reading.

A reader who picks a different language is not a reader who left, so the switch gets no grace.
The reader page sends an id with each stream and `Hub.subscribe` retires that reader's previous stream with the `LEAVING` sentinel, which is what lets `unsubscribe` tell a deliberate departure from a dropped one and leave `last_seen` alone for the first.
Without it a phone switching language holds both channels until the old handler's next keepalive write, up to fifteen seconds later, and then buys the abandoned language a further `grace` seconds nobody is reading.

Demand is unauthenticated, so `max_languages` caps how many run at once: without it one client opening every channel makes every sentence pay for the whole list, in tokens and in the latency more output adds for real readers.
`demand` keeps the languages the operator forced on, then the ones with the most readers, and returns the rest as capped.
A capped language gets the English line rather than a gap, and the default is 0 because showing English to a real reader is worse than the tokens a cap saves.

### Web layer

Two listeners share one `Hub` and one `Session`.
`build_reader_app` serves `/`, `/stream/<channel>`, and `/api/channels` on the port a tunnel points at, and nothing on it can change anything.
`build_operator_app` serves `/operator`, `/api/status`, `/api/devices`, `/api/start`, `/api/stop`, `/api/language`, and `/qr.svg` on `OPERATOR_HOST`, which is always `127.0.0.1`.
A check inside the handlers could not replace this, because the tunnel daemon connects from this machine, so a visitor from the public internet arrives from `127.0.0.1` exactly as the operator does.
`serve` runs both under `AppRunner`, since `web.run_app` takes one application.

Every operator route is behind `authorized`, including the two that only read: `/api/status` carries the device, the model, the raw exception text and the last lines spoken, and `/api/devices` names the sound hardware and forks a process per request.
The token also stops a page in another tab of the operator's browser from posting a cross-origin form at the loopback port, which needs no CORS preflight and would otherwise reach `Session.start`.
A refusal has to keep the shape the operator page destructures, `ok` and `message` for status and `devices` and `error` for the device list, or the page renders a blank panel instead of saying the token is wrong.

Handlers share their event loop with the capture pipeline, so anything that blocks in one stalls the subtitle fan-out to every phone in the room.
`api_devices` runs `pactl` under `asyncio.to_thread` for that reason, and a handler that shells out, touches the disk, or calls a third party belongs in a thread too.

`max_listeners` bounds how many event streams the `Hub` holds, since nothing authenticates to open one and each costs a task, a queue, a socket, and a write on every published line.
Past the cap `stream` answers 503 with a `Retry-After` before `prepare`, and `Hub.refused` reaches the operator page.
A reader who leaves holds its slot until the next keepalive write fails, up to 15 seconds, which is why the default is generous rather than tight.

Both pages in `static/` are single files with inline CSS and JavaScript, no build step and no framework.
Server strings reach both pages, including exception text and device names, so they build nodes and set `textContent` rather than assembling `innerHTML`.
The reader page keeps chosen language and text size in `localStorage` and holds a screen wake lock, which is why HTTPS matters.
Its own two lines, the empty-feed message and the jump-to-newest button, are translated from the `PHRASES` table in the page rather than by the model: a reader who does not read English should not wait on a model call to be told nobody has spoken yet, and those two sentences never change.
A language missing from the table falls back to English, so adding one to `config.toml` is not a change to `reader.html`.

### Operator scripts

`transept-start.sh` and `transept-stop.sh` are the weekly pair for a room behind Tailscale Funnel, and `README.md` covers what they do for the operator.
They are bash and they assume Tailscale, which is a narrower bet than the rest of the project makes; everything else here runs on three platforms.
Nothing else may depend on them, and `server.py` must stay runnable on its own.

Both ports and `public_url` are read from `config.toml` with `tomllib`, never copied into the scripts, since a second copy of the port is a copy that will one day disagree with the one the server binds.
The start script warns when `public_url` differs from the address the funnel just published, which is the one setup mistake that survives a successful start and only shows up as a QR code nobody can open.

There is no PID file.
`transept-stop.sh` finds its target the way the operator would describe it, a python whose working directory is this one, running `server.py`, which also catches the copy somebody started by hand in a terminal they have since closed.
Both halves of that test earn their place, because matching the command line alone would also match an editor or a shell with the same words in it, and the next thing the script does is send that process a signal.

## Things that look wrong but are not

Segmentation exists because Deepgram finalizes text mid-sentence.
Translating each finalized fragment separately produces output like "on the heater" with no subject, which is unusable in languages that need a verb or a case ending.
Do not remove the buffer to reduce latency.

Interim recognition results are disabled, because finalized results arrive 0.2 to 0.3 seconds behind the speaker and interim hypotheses add flicker for nothing.

`Hub.publish` revises an existing entry in place when the sequence number already exists.
That is how a glossary-corrected English line replaces the raw one on phones already showing it, and clients key on `seq`, so duplicates are replacements rather than new lines.

`Segmenter._take` returns the sequence number alongside the text instead of leaving the caller to read `segmenter.seq`.
One fragment can close two sentences, the gap check closing the buffered one and the fragment itself closing the next, and both takes run before either unit is built.
A caller reading the counter afterwards stamps both with the second number, and `Hub.publish` then revises the first line away instead of publishing it.

`Session.unit` asks for the English correction whenever a session is being recorded, not only when somebody is reading that channel.
Without it, a Sunday where everyone reads French stores no corrected line, and the two things the record exists for, the keyterms worklist and the check for an invented name, are both empty.

Every recorder call from `Session` goes through `_record`, which swallows anything the recorder raises and switches recording off.
The `Recorder` is built not to raise and only ever puts a row on a queue, but these calls sit in the loop draining the Deepgram socket, so the guard belongs at the call site too.
A disk that has stopped answering costs the record of the meeting and must not cost the meeting.

`record.connect` passes `timeout=0`.
On a locked database sqlite does not raise, it blocks in the busy handler for the full timeout first, and the default is five seconds, which no `except` can catch.

`Session.translated` falls back to English on an empty string, not on a missing key.
`parse_translations` fills every requested language, so a language the model dropped arrives as `""` and a `dict.get` default would never fire.

Translation output order is enforced even though calls run concurrently.
A slow call delays the ones behind it rather than scrambling the transcript, and the `hold` timeout bounds that delay.

`publish` catches `CancelledError` separately and re-raises it.
Treating it as a translation failure publishes an English line into every channel after the operator pressed Stop, and counts a failure that never happened.

`Session.start` refuses unless the state is exactly `stopped`, because `reconnecting` also means a supervisor task is alive.

`Session.stop` skips cancelling the idle watchdog when the watchdog is the caller, because a task awaiting its own completion deadlocks.

`_run_once` waits on `FIRST_COMPLETED` rather than gathering.
Whichever leg finishes first ends the session and the other two are cancelled, because leaving them running against a socket nobody reads is how a session appears alive with no audio.

`HEALTHY_RUN` resets the reconnect backoff after a run that lasted a minute, so a few blips early in a meeting do not make a later one cost twenty seconds of silence.

`pump_audio` sends `CloseStream` in a `finally` block.
Without it, a dead capture device leaves the websocket open and the session hangs in `running` with no audio and no reconnect.

The 48 kHz fallback averages groups of three samples rather than taking every third one, because plain decimation would alias everything above 8 kHz back into the speech band.

`transept-start.sh` runs `python3 -u`.
Python block buffers stdout when it is a file rather than a terminal, and the operator address with its token is printed once at startup, so without `-u` the log file stays empty until the server exits, which is exactly when that address stops being worth having.

`transept-start.sh` runs `transept-stop.sh` before it starts anything.
A server left from a previous meeting holds both ports, and by then there is rarely a terminal left to press Ctrl-C in, so refusing would send a volunteer hunting for a process five minutes before the meeting.
Clearing first also means anything still holding either port afterwards is genuinely not Transept, which is the one case worth stopping for and what the `ss` check reports.

`transept-stop.sh` closes the funnel with `tailscale funnel reset` rather than naming the port.
The per-port `tailscale funnel 8080 off` form was removed, and current versions answer it with "the CLI for serve and funnel has changed" and a nonzero exit, which in a stop script means a meeting's address stays open to the internet afterwards.

`serve` calls `hub.close()` between stopping the session and cleaning up the runners.
A phone holds its event stream open for as long as the page is up, `AppRunner.cleanup` waits on the connections its site still has, and an event stream ends only when the handler returns.
Without it the process does not exit at all while one reader still has the page open, which is every meeting, and Ctrl-C appears to do nothing.
The operator then closes the terminal on a process that goes on holding the operator port, and next week's run dies of `address already in use` against an owner that has no window left to press Ctrl-C in.

`install_stop_handler` exists because `loop.add_signal_handler` raises `NotImplementedError` on Windows.
Platform assumptions belong in `capture.py` and that function, nowhere else.

## Conventions

The audio source is deliberately not a setting.
It changes with a reboot or a replugged cable, so a name in `config.toml` would be stale more often than right; the operator picks it on the page, `pipeline.py` takes a plain `--device` and otherwise opens `capture.default_device`, and `Session` keeps the chosen name on itself rather than writing it back into the config.
`capture.choose_default` holds the one rule for which input to offer first, since the operator page sorts by name and so no longer knows the order the backend listed them in.

Settings live in `config.toml`, resolved by the `SETTINGS` table in `pipeline.py`.
Adding a setting means adding one row there, then naming it in the `add_settings_arguments` call of whichever entry points should expose it as a flag.
Every argparse default is `None` so `resolve` can distinguish an absent flag from one that happens to match the default.
Secrets stay in `.env` and never move into `config.toml`.

Standard library first, few dependencies.
`websockets`, `httpx`, and `aiohttp` are the whole list at runtime, and adding a fourth needs a real reason.
`sounddevice` and `segno` are optional, and both failures are caught and reported rather than raised.
`ruff` is in `requirements.txt` as well, because a linter in a second file nobody installs is a linter nobody runs.

`ruff.toml` selects E, W, F, UP, B, and C4 at a line length of 79, which is the layout this code already followed.
Import sorting is deliberately not enabled, since ruff can only emit one import per line and the hanging indent style here is just as sorted and much shorter.
Fix what ruff reports rather than adding `noqa`, and if a rule is wrong for this project, remove the rule and say why in `ruff.toml`.

Comments explain why, not what.
Where a line encodes a decision that was reached the hard way, say what would go wrong without it, and keep it to a sentence or two.
Prefer concrete nouns over pronouns in anything a volunteer might read.

No em-dashes in prose or comments.
Headings use colons, not dashes.
One sentence per line in Markdown files, so diffs isolate the sentence that changed.

## Testing

`selftest.py` is the whole suite, and it needs no keys, no audio device, and no network.
It uses no test framework, only the standard library, and exits non-zero if anything fails.
Run it after any change to the pipeline, the sinks, or the routes.
`python3 selftest.py <section>` runs one of `lint`, `pipeline`, `session`, `store`, or `server`.
The `lint` section shells out to `ruff check .` and reports its output as one check, which is what keeps linting in the regular workflow when there is no CI to enforce it.

Adding a check means adding one `checks.check(label, condition, detail)` line.
`FakeSocket`, `FakeSource`, and `FakeTranslator` are already there, and `FakeTranslator` takes `mode="fail"`, `mode="empty"`, and a `delay`, so the failure, dropped language, and timeout paths cost nothing to exercise.

The `store` section ends with a `Recorder` that raises on every method, asserted against the `Hub` buffers.
An isolation wrapper with no test that exercises the raising path is a wrapper nobody knows works; this one was written before the guard existed and failed until it did.

A check that cannot fail is worse than no check, because it reads like coverage.
`FRAGMENTS` once punctuated its second fragment, so the buffer was always empty when the gap check ran and a sequence number collision lived for a release under a check whose label claimed to test the gap; it now closes one sentence each way, by gap, by endpoint on the same fragment, and by punctuation.
"the server exited when asked" terminated the process, waited three seconds, killed it, and then asserted a return code, which is true after a kill, so it passed for a release while the server could not in fact exit at all with a reader connected; it now holds an event stream open across the shutdown and asserts that killing was never needed.
After adding a check for a fix, revert the fix and confirm the check fails.

What `selftest.py` cannot tell you is whether the subtitles are any good.
Recognition accuracy is decided by the microphone feed and can only be judged in the actual room, and translation quality needs a native speaker and `review.py`.
Latency claims should come from an actual run: `python3 pipeline.py --no-translate` tags each line with how far behind real time it arrived, and a full `pipeline.py` or `review.py` run prints translation median and p95 on exit.

## Known limits

The reader view is a plain web page by choice, not an installable app.
A manifest and service worker would add install friction and offline machinery that a live subtitle feed cannot use anyway.
HTTPS is still required, because the screen wake lock needs a secure context, and a tunnel in front is the current answer.
`public_url` exists because the bind address is not the address a phone can reach, and the QR endpoint renders that value rather than the listener.

`operator_port` is a setting; the operator host is not, because the point of the second listener is that a tunnel cannot be pointed at it by mistake.
A headless machine wants an SSH forward rather than a wider bind.

The operator token is minted every run and printed with the address, and there is no tokenless mode.
`OPERATOR_TOKEN` in `.env` pins it, which exists for development, where a fresh address every restart is a fresh link to click every restart.
It stays in `.env` beside the keys rather than becoming a `config.toml` setting, because a setting invites a weak or forgotten token on the machine that runs the meetings.
The token still travels in the query string on that first load, so it reaches browser history, and stripping it from the address bar is not a client-side fix while `/operator` itself is gated on it.
