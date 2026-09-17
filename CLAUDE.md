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
python3 review.py --input t.txt --review review.md         offline translation review
python3 record.py --session last --out review/sunday.md    read a session back
./transept start                      stop leftovers, start, open the funnel
./transept stop                       the funnel, then the server
./transept status                     whether either is up, and the addresses
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

`pipeline.py` holds everything the entry points share: `SECRETS`, `SETTINGS`, `resolve`, `load_keys`, `Segmenter`, `Translator`, `SYSTEM_PROMPT`, and the three loops `pump_audio`, `listen`, and `publish`.
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

Demand comes from whoever holds the reader link, which is everyone in the room, so `max_languages` caps how many run at once: without it one client opening every channel makes every sentence pay for the whole list, in tokens and in the latency more output adds for real readers.
`demand` keeps the languages the operator forced on, then the ones with the most readers, and returns the rest as capped.
A capped language gets the English line rather than a gap, and the default is 0 because showing English to a real reader is worse than the tokens a cap saves.

### Web layer

Two listeners share one `Hub` and one `Session`, and each holds a token of its own under `app["token"]`.
`build_reader_app` serves `/read`, `/stream/<channel>`, and `/api/channels` on the port a tunnel points at, and nothing on it can change anything.
`build_operator_app` serves `/operator`, `/api/status`, `/api/devices`, `/api/start`, `/api/stop`, `/api/language`, and `/qr.svg` on `OPERATOR_HOST`, which is always `127.0.0.1`.
A check inside the handlers could not replace this, because the tunnel daemon connects from this machine, so a visitor from the public internet arrives from `127.0.0.1` exactly as the operator does.
`serve` runs both under `AppRunner`, since `web.run_app` takes one application.

Every route on both listeners is behind `authorized`, which reads the token its own application was given, so the link the whole room is handed opens nothing on the operator port.
The one exception is `/` on the reader port, which is `nothing_here`: a tunnel puts that address on the public internet, and a bot that finds it gets a 404 rather than a language picker.
Two separate tokens rather than one, because they are shared with different people and expire on different schedules: `reader_token` may be pinned so a printed card keeps working, while `operator_token` pinned in a meeting is a weak or forgotten password on the machine that runs it.

On the reader port the token is what stands between a public hostname and a meeting, and `stream` checks it before it looks the channel up, because opening a stream is what makes a language be translated and paid for.
On the operator port it also stops a page in another tab of the operator's browser from posting a cross-origin form at the loopback port, which needs no CORS preflight and would otherwise reach `Session.start`, and it covers the two routes that only read: `/api/status` carries the device, the model, the raw exception text and the last lines spoken, and `/api/devices` names the sound hardware and forks a process per request.
`/qr.svg` is behind it too, now that the image encodes the reader token; the operator page passes the token in the query string there because an `<img>` cannot carry a header.
A refusal has to keep the shape the page destructures, `ok` and `message` for status, `devices` and `error` for the device list, and `channels` and `error` for the channel list, or the page renders a blank panel instead of saying the token is wrong.

The reader page takes its token from `location.search` rather than storing it, since the address is what the QR code and the shared link both carry, and it puts it in the query string of the stream and the channel list because an `EventSource` cannot set a header either.

The address itself is built by `reader_address`, not typed into `config.toml`: `public_url` is the hostname a phone can reach, and the path and the token are this run's.
`main` puts the result in `config["reader_url"]`, which is what `/qr.svg` renders and what the operator page shows as the link to hand somebody, and it is empty until `public_url` is set so the page shows no share block rather than a link to nowhere.

Handlers share their event loop with the capture pipeline, so anything that blocks in one stalls the subtitle fan-out to every phone in the room.
`api_devices` runs `pactl` under `asyncio.to_thread` for that reason, and a handler that shells out, touches the disk, or calls a third party belongs in a thread too.

`max_listeners` bounds how many event streams the `Hub` holds, since the reader token is on a card the room passes around and each stream costs a task, a queue, a socket, and a write on every published line.
Past the cap `stream` answers 503 with a `Retry-After` before `prepare`, and `Hub.refused` reaches the operator page.
A reader who leaves holds its slot until the next keepalive write fails, up to 15 seconds, which is why the default is generous rather than tight.

Both pages in `static/` are single files with inline CSS and JavaScript, no build step and no framework.
Server strings reach both pages, including exception text and device names, so they build nodes and set `textContent` rather than assembling `innerHTML`.
The reader page keeps chosen language and text size in `localStorage` and holds a screen wake lock, which is why HTTPS matters.
Its own two lines, the empty-feed message and the jump-to-newest button, are translated from the `PHRASES` table in the page rather than by the model: a reader who does not read English should not wait on a model call to be told nobody has spoken yet, and those two sentences never change.
A language missing from the table falls back to English, so adding one to `config.toml` is not a change to `reader.html`.

### Operator scripts

### The transept script

`./transept start | stop | restart | status` is the weekly command for a room behind Tailscale Funnel, and `README.md` covers what it does for the operator.
It is bash and it assumes Tailscale, which is a narrower bet than the rest of the project makes; everything else here runs on three platforms.
Nothing else may depend on it, and `server.py` must stay runnable on its own, which is also the form a systemd unit would take.

A command is one `do_*` function and one line in the `case` at the bottom, and the pieces they share sit in `read_config`, `server_pids`, `funnel_url`, and `gone`.
`start` was a separate script that ran the stop script as a subprocess, so the shared half of that pair was a process boundary; `status` exists because once `server_pids` is a function, answering "is the room being subtitled" is three lines rather than a second copy of the rule.

The script does not `set -e`.
The stop path has to attempt every step even when an earlier one failed, because the point is to leave nothing running and nothing exposed, and the start path checks each step it cares about and says what went wrong, which is worth more to a volunteer than a silent nonzero exit.
`read_config` checks that the ports it read are digits for that reason: `read` succeeds on empty input, so a `config.toml` that would not parse otherwise reaches `ss` as `sport = :`.

Both ports and `public_url` are read from `config.toml` with `tomllib`, never copied into the script, since a second copy of the port is a copy that will one day disagree with the one the server binds.
`start` warns when `public_url` differs from the address the funnel just published, which is the one setup mistake that survives a successful start and only shows up as a QR code nobody can open.
Both addresses are read back out of the log rather than built here, because each carries a token the server minted, and the funnel hostname on its own now opens nothing.

There is no PID file.
`server_pids` finds its target the way the operator would describe it, a python whose working directory is this one, running `server.py`, which also catches the copy somebody started by hand in a terminal they have since closed.
Both halves of that test earn their place, because matching the command line alone would also match an editor or a shell with the same words in it, and the next thing the caller does is send that process a signal.

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

`transept start` runs `python3 -u`.
Python block buffers stdout when it is a file rather than a terminal, and both addresses with their tokens are printed once at startup, so without `-u` the log file stays empty until the server exits, which is exactly when those addresses stop being worth having.
The script greps them back out of that log, and `status` does too, so the buffering is not a cosmetic problem but the difference between having an address for the room and not.

`transept start` runs the whole of `stop` before it starts anything, which is also why `restart` is a synonym for `start` rather than a third code path.
A server left from a previous meeting holds both ports, and by then there is rarely a terminal left to press Ctrl-C in, so refusing would send a volunteer hunting for a process five minutes before the meeting.
Clearing first also means anything still holding either port afterwards is genuinely not Transept, which is the one case worth stopping for and what the `ss` check reports.

`transept stop` closes the funnel with `tailscale funnel reset` rather than naming the port.
The per-port `tailscale funnel 8080 off` form was removed, and current versions answer it with "the CLI for serve and funnel has changed" and a nonzero exit, which when stopping means a meeting's address stays open to the internet afterwards.

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
`SETTINGS`, `ARGUMENT_HELP`, and `config.example.toml` are in one order, section for section and key for key, because adding a setting means reading them side by side; `selftest.py` compares the first and the last and fails on drift.
The sections follow the path a sentence takes: `[audio]` in, `[recognition]`, `[segmentation]`, `[translation]`, then `[languages]`, the `[files]` that tune both models, `[session]`, and `[server]` out.
`endpointing` is a Deepgram parameter rather than a property of the sound card, so it sits in `[recognition]` beside the model it is sent with.
Every argparse default is `None` so `resolve` can distinguish an absent flag from one that happens to match the default.

The keys live in the same file, in `[keys]`, resolved by `load_keys` against the `SECRETS` table rather than by `resolve`.
A separate `.env` meant that changing translation providers was two edits in two files, one for the base URL and the key and one for the model name, which is the change most likely to be made in a hurry.
They are not rows in `SETTINGS` because `SETTINGS` becomes command line flags, and a key on a command line lands in the process list and in shell history.
An environment variable of the same name in capitals overrides the file, which is what a systemd unit, a container, or `selftest.py` uses; an empty variable counts as set, which is how a run asks for a freshly minted reader or operator token on a machine whose `config.toml` pins one.
`config.toml` is gitignored for this reason and the file itself says so at the top.
The third column of `SECRETS` is always the second in capitals, spelled out rather than derived so that grepping for `DEEPGRAM_API_KEY` finds the table, and `selftest.py` asserts the two agree so the rule the config file states cannot drift.

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
`public_url` exists because the bind address is not the address a phone can reach, and the QR endpoint renders `reader_address` of that value rather than the listener.

`operator_port` is a setting; the operator host is not, because the point of the second listener is that a tunnel cannot be pointed at it by mistake.
A headless machine wants an SSH forward rather than a wider bind.

Both tokens are minted every run and printed with their addresses, and there is no tokenless mode on either listener.
`reader_token` and `operator_token` under `[keys]` pin them.
Pinning the reader one is ordinary, because a printed card has to keep working; pinning the operator one exists for development, where a fresh address every restart is a fresh link to click every restart, and a meeting leaves it empty.
They sit with the keys rather than among the settings, and `mint_token()` takes the pinned value as an argument rather than reading the environment itself, so the one place that decides where a secret comes from stays `load_keys`.
Neither is a `SETTINGS` row and so neither has a flag, because a setting invites a weak or forgotten token on the machine that runs the meetings.

Both tokens travel in a query string, which is what reaches browser history, and on the reader side every request carries one because neither an `EventSource` nor an `<img>` can set a header.
Stripping it from the address bar is not a client-side fix while the page itself is gated on it, and on a phone it would break the reload that a locked screen eventually causes.
The reader token is a shared secret for a room, not a credential for a person: it keeps a public hostname from being a public meeting, and it is not meant to survive the card being photographed.
