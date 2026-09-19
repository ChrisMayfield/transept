# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.
`README.md` is the introduction, for somebody deciding whether Transept suits their meeting, and `SETUP.md` is the volunteer's manual: installing, keys, tunnels, running a meeting, tuning, and troubleshooting.
`HOSTED.md` proposes an optional deployment for a room whose network cannot carry a meeting, of which the first three steps of its order of work are built and the rest is not.
The notes here are about the code.

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
That channel may be corrected for recognition errors using the glossary, but false starts, repetitions, and informal grammar stay as spoken, because for a deaf reader that line is the record of what was said.
Translations get latitude on grammar and phrasing because fluency requires it, never on content.

Subtitles must never silently stall.
When a translation fails, times out, or comes back missing a language, the English text goes to that channel so a reader sees something rather than a gap.

## Commands

```
python3 server.py                     the whole thing, and the entire weekly command
python3 server.py --list-devices      find an audio source
python3 sender.py                     the room's half of a hosted deployment
python3 pipeline.py --no-translate    check the audio path, no translation key needed
python3 selftest.py [section]         lint, pipeline, session, store, server, script
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

`capture.py` presents one interface over three backends: `parec`, which `auto` prefers on Linux where it exists, `sounddevice` (PortAudio) everywhere else, and `remote`, which is audio arriving over the control socket from a sender on a laptop in the room.
All three yield 16 kHz mono signed 16-bit chunks, and nothing downstream knows which produced them, but each also says what it yields in `encoding`, because a sender may be sending compressed audio and `build_asr_url` has to declare the format actually going up the socket.
`auto` never chooses `remote`, which is a deployment rather than a preference and has to be asked for by name.
An empty chunk means end of stream, which is how a device that disappears becomes a reconnect rather than a hang.
`None` means a gap, which only `remote` produces: nothing has arrived just now and the sender is still inside `control_grace`, so `pump_audio` sends a `KeepAlive` rather than ending the run, and sends no silence, which would be billed as audio.

`pipeline.py` holds everything the entry points share: `SECRETS`, `SETTINGS`, `resolve`, `load_keys`, `Segmenter`, `Translator`, `SYSTEM_PROMPT`, and the three loops `pump_audio`, `listen`, and `publish`.
`pipeline.py` also runs standalone as a terminal tool, which is the fastest way to debug the pipeline without the web layer.

`open_encoder` and `Encoder` are there too, putting Opus between a capture object and whatever socket the audio goes up.
The encoder sits here rather than in `capture.py` so every backend keeps the promise it makes, which is 16 kHz mono signed 16-bit chunks; what a wrapped source changes is the one thing downstream reads, which is what it says it yields.
Compression is worth having in the default deployment as much as the hosted one, because a congested uplink out of a building is what a whole on-site beta failed on, so `[audio] encoding` defaults to `opus` and every entry point wraps its local capture.
A machine with no working `ffmpeg` falls back to PCM and says so, the way `sounddevice` and `segno` are already handled, and the fallback is decided before the recognizer socket opens because the format is named when that socket opens and cannot be changed afterwards.
Two of the flags in `ENCODER_COMMAND` are not tuning: without `-analyzeduration 0 -probesize 32` ffmpeg reads two seconds of a raw stream before deciding what it is, and without `-page_duration 20000` the ogg muxer holds a second of audio per page, and either one would be added to every sentence.

`server.py` adds `Hub` (ring buffers and subscribers), `Session` (start, stop, supervise, reconnect), and the aiohttp routes.

`controls.py` is the operator page and the routes behind it, which both entry points import and which imports neither.
It is `controls.py` and not `operator.py` because `operator` is a standard library module: a file of that name beside the code shadows it for everything started from this directory, and `collections` imports `operator`, so `import collections` fails and takes aiohttp, websockets, and httpx with it.

`sender.py` is the room's laptop for a hosted server: capture, one websocket, and a proxy of `Session`'s four methods that forwards over it.
It runs no `Segmenter`, no `Translator`, and no `publish`, so it is a pipe rather than a second pipeline.
It does not reuse `pump_audio`, and the reason is not the message that loop sends on the way out: `pump_audio` ends when its device does, which is right for a pipeline owning its socket end to end, while `Remote.pump` outlives both the socket and the microphone, because the microphone stays open across a reconnect and the socket comes and goes under it.

`review.py` is an offline tool sharing `Translator` and the prompt, differing only in retry policy: the live pipeline gets one fast retry because a meeting cannot wait, while review retries harder because nothing is waiting on the answer.

`record.py` is both halves of persistence: the `Recorder` a `Session` writes through, and the command that reads a session back as a Markdown document.
Recording is off unless `[session] record` is set, and nothing is ever deleted without `--purge`.

### One pipeline, two sinks

Both entry points run the same three tasks over one websocket and differ only in where finished text goes, so that difference sits behind a sink.
A sink provides `fragment(text)`, `unit(unit)` returning the output names worth requesting, `translated`, `timed_out`, and `failed`.
`TerminalSink` prints; `Session` is its own sink, publishing to the `Hub` and keeping the counters the operator page shows.
An empty list from `unit` means do not call the model for that sentence at all.

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
`build_reader_app` serves `/reader`, `/stream/<channel>`, and `/api/channels` on the port a tunnel points at, and nothing on it can change anything.
`build_operator_app`, in `controls.py`, serves `/operator`, `/api/status`, `/api/devices`, `/api/start`, `/api/stop`, `/api/language`, and `/qr.svg` on `OPERATOR_HOST`, which is always `127.0.0.1`.
It takes no `Hub`, because no route on it reads one and a sender has none to pass; what the page knows about readers arrives inside the status.
A check inside the handlers could not replace this, because the tunnel daemon connects from this machine, so a visitor from the public internet arrives from `127.0.0.1` exactly as the operator does.
`serve` runs both under `AppRunner`, since `web.run_app` takes one application.

Which listener joins the reader one follows from where the audio comes from and from nothing else.
A local device means the operator page on loopback; `capture = "remote"` means `build_control_app`, whose one route is the `/control` websocket a sender connects to, bound where that sender can reach it.
A separate `mode` setting could be changed without the backend being changed to match, which would leave a server opening a sound card while a sender waited, or waiting for a sender while a sound card ran.
That listener has to bind publicly, so what guards it is `control_token` and a refusal of any handshake carrying an `Origin` header at all, rather than the address it binds to: websockets are not subject to the same-origin policy and need no CORS preflight, the only legitimate client is a Python program that sends no `Origin`, and a browser stamps every handshake with one and cannot be made not to.
`ControlRoom` holds the one sender, its audio, and its grace, and hands `Session` a source per recognizer run rather than per sender, because a run that ends closes its source and audio arriving between runs has no socket to go up.
`HOSTED.md` describes that deployment; steps 1 to 3 of its order of work are built.

The operator page is the same page either way, because `controls.py` serves it against an object with four methods: `start`, `stop`, `set_override`, and `status`, plus a `config` the device list and the QR code read.
`Session` is that object in the default deployment and `sender.Remote` is the other implementation, which is the same "one interface, two implementations" as the capture backends and the sinks.
So new behavior on that page belongs in `controls.py` or in both objects, never in a second copy of a handler.
`Remote.status` passes the server's own snapshot through rather than rebuilding it, and lays over it the two things the snapshot cannot know: which microphone is open on this laptop, and whether the socket carrying the rest of it is up, since a snapshot from thirty seconds ago describing a running meeting is exactly what a dropped connection looks like.
The sender's own checks live in `session` for what the proxy decides alone and in `server` for what it and a real server have to agree on, and the shape check compares against a real `Session.status()`, because a table compared with itself is a check that cannot fail.

Every route on both listeners is behind `authorized`, which reads the token its own application was given, so the link the whole room is handed opens nothing on the operator port.
The one exception is `/` on the reader port, which is `nothing_here`: a tunnel puts that address on the public internet, and a bot that finds it gets a 404 rather than a language picker.

On the reader port the token is what stands between a public hostname and a meeting, and `stream` checks it before it looks the channel up, because opening a stream is what makes a language be translated and paid for.
On the operator port it also stops a page in another tab of the operator's browser from posting a cross-origin form at the loopback port, which needs no CORS preflight and would otherwise reach `Session.start`, and it covers the two routes that only read: `/api/status` carries the device, the model, the raw exception text and the last lines spoken, and `/api/devices` names the sound hardware and forks a process per request.
`/qr.svg` is behind it too, now that the image encodes the reader token.
A refusal has to keep the shape the page destructures, `ok` and `message` for status, `devices` and `error` for the device list, and `channels` and `error` for the channel list, or the page renders a blank panel instead of saying the token is wrong.

The reader page takes its token from `location.search` rather than storing it, since the address is what the QR code and the shared link both carry, and it passes the token on to the stream and the channel list.

The address itself is built by `reader_address`, not typed into `config.toml`: `public_url` is the hostname a phone can reach, and the path and the token are this run's.
`main` puts the result in `config["reader_url"]`, which is what `/qr.svg` renders and what the operator page shows as the link to hand somebody, and it is empty until `public_url` is set so the page shows no share block rather than a link to nowhere.

Handlers share their event loop with the capture pipeline, so anything that blocks in one stalls the subtitle fan-out to every phone in the room.
`api_devices` runs `pactl` under `asyncio.to_thread` for that reason, and a handler that shells out, touches the disk, or calls a third party belongs in a thread too.

`max_readers` bounds how many event streams the `Hub` holds, since each stream costs a task, a queue, a socket, and a write on every published line, and the reader link can travel further than the room the link was handed out in.
Past the cap `stream` answers 503 with a `Retry-After` before `prepare`, and `Hub.refused` reaches the operator page.
A reader who leaves holds its slot until the next keepalive write fails, up to 15 seconds, which is why the default is generous rather than tight.

Both pages in `static/` are single files with inline CSS and JavaScript, no build step and no framework.
Server strings reach both pages, including exception text and device names, so they build nodes and set `textContent` rather than assembling `innerHTML`.
The reader page keeps chosen language and text size in `localStorage` and holds a screen wake lock, which is why HTTPS matters.
Its own two lines, the empty-feed message and the jump-to-newest button, are translated from the `PHRASES` table in the page rather than by the model: a reader who does not read English should not wait on a model call to be told nobody has spoken yet, and those two sentences never change.
A language missing from the table falls back to English, so adding one to `config.toml` is not a change to `reader.html`.

### The transept script

`./transept start | stop | restart | status` is the weekly command for a room behind Tailscale Funnel, and `SETUP.md` covers what the script does for the operator.
The work is in `transept.py`; the `transept` beside it is a dozen lines of `sh` that pick an interpreter and hand over.
Two files rather than one because a shebang cannot choose between `.venv/bin/python3` and `.venv/Scripts/python.exe`, and on Windows there is frequently no `python3` at all, so one spelling of the command works on three platforms only if something looks first.
The launcher holds no logic beyond that search, and nothing new belongs in it.

Tailscale is assumed, which is a narrower bet than the rest of the project makes.
Nothing else may depend on either file, and `server.py` must stay runnable on its own, which is also the form a systemd unit would take.

A command is one `do_*` function and one `choices` entry in `main`, and the pieces they share sit in `read_config`, `server_record`, `funnel_url`, and `banner`.
`stop` is the one command that runs without reading `config.toml`, because the point of stopping is to leave nothing running and nothing exposed, and a config that will not parse must not stand in the way of that.
The ports it needs come out of the record instead.

Nothing raises its way out of a command.
The stop path attempts every step even when an earlier one failed, and the start path checks each step it cares about and says what went wrong, which is worth more to a volunteer than a traceback.
A missing `tailscale` is reported as a command that failed, because from the room's point of view there is no difference between absent and refusing.

Both ports and `public_url` are read from `config.toml` with `tomllib`, never copied into the script, since a second copy of the port is a copy that will one day disagree with the one the server binds.
`start` warns when `public_url` differs from the address the funnel just published, which is the one setup mistake that survives a successful start and only shows up as a QR code nobody can open.
Both addresses are read back out of the log rather than built here, because each carries a token the server minted, and the funnel hostname on its own now opens nothing.

### Finding the server to stop

`server.py` writes `transept.pid` in the directory it starts in, once both ports are bound, and removes the file on the way out.
The pid file replaced a search through `pgrep`, `ps`, and `/proc/<pid>/cwd` that only Linux could answer: macOS has no `/proc`, and Windows has no per-process working directory to ask about at all.
Writing it from the server rather than from the script keeps what that search was for, since the copy somebody started by hand out of this directory, in a terminal they have since closed, writes the same file.

The file is written after the listeners bind, so a second server that dies of "address already in use" cannot overwrite the record of the one holding the ports, and `remove_pid_file` checks that the recorded pid is still its own, so a server on its way out cannot delete a newer server's record.

A record alone is not a running server.
`server_record` requires the process to be alive *and* one of the recorded ports to still answer, because process ids come around again and the next thing the caller does is send a signal.
Anything failing that test is a leftover, and `stop` clears the file and says so.

`do_stop` then waits on the ports rather than on the process.
The ports are what the next server needs back, they are the last thing `serve` lets go of, and a process that has finished but has not been reaped by its parent yet still answers every liveness test there is, which would spend the whole grace period waiting for something that already stopped.

`alive` never calls `os.kill(pid, 0)` on Windows.
Every signal there but the two console events is `TerminateProcess`, so asking whether a process exists would kill the process being asked about; `tasklist` answers instead.

A Windows stop is abrupt and cannot be otherwise.
Windows delivers no `SIGTERM` between processes, and the server is started detached with no console for a Ctrl event to reach, so the handler `install_stop_handler` registers never runs there.
The session ends either way; what is lost is the drain of the last lines and the tidy close of the phones still connected.

## Things that look wrong but are not

Segmentation exists because Deepgram finalizes text mid-sentence.
Translating each finalized fragment separately produces output like "on the heater" with no subject, which is unusable in languages that need a verb or a case ending.
Do not remove the buffer to reduce latency.

Interim recognition results are disabled, because finalized results arrive 0.2 to 0.3 seconds behind the speaker and interim hypotheses add flicker for nothing.

`Hub.publish` revises an existing entry in place when the sequence number already exists.
Revising in place is how a glossary-corrected English line replaces the raw one on phones already showing that line, and clients key on `seq`, so duplicates are replacements rather than new lines.

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

`do_start` runs the server with `-u`.
Python block buffers stdout when it is a file rather than a terminal, and both addresses with their tokens are printed once at startup, so without `-u` the log file stays empty until the server exits, which is exactly when those addresses stop being worth having.
`start` reads them back out of that log, and `status` does too, so the buffering is not a cosmetic problem but the difference between having an address for the room and not.

`transept start` runs the whole of `stop` before it starts anything, which is also why `restart` is a synonym for `start` rather than a third code path.
A server left from a previous meeting holds both ports, and by then there is rarely a terminal left to press Ctrl-C in, so refusing would send a volunteer hunting for a process five minutes before the meeting.
Clearing first also means anything still holding either port afterwards is genuinely not Transept, which is the one case worth stopping for and what the `listening` check before the launch reports.

`transept.log` and `transept.pid` sit beside the code rather than in `/tmp`.
Windows has no `/tmp`, and two checkouts on one machine should not share a log that carries this run's tokens or a record of which process to signal.
`transept.py` anchors both to its own directory and starts `server.py` there, and `server.py` writes the record into whatever directory it starts in.

`transept stop` closes the funnel with `tailscale funnel reset` rather than naming the port.
The per-port `tailscale funnel 8080 off` form was removed, and current versions answer it with "the CLI for serve and funnel has changed" and a nonzero exit, which when stopping means a meeting's address stays open to the internet afterwards.

`serve` calls `hub.close()` between stopping the session and cleaning up the runners.
A phone holds its event stream open for as long as the page is up, `AppRunner.cleanup` waits on the connections its site still has, and an event stream ends only when the handler returns.
Without it the process does not exit at all while one reader still has the page open, which is every meeting, and Ctrl-C appears to do nothing.
The operator then closes the terminal on a process that goes on holding the operator port, and next week's run dies of `address already in use` against an owner that has no window left to press Ctrl-C in.

`control_port` defaults to the same 8081 as `operator_port`.
A server raises one of those listeners or the other and never both, since which pair it builds follows from the capture backend, so the two numbers cannot collide.

A control message that succeeds is not answered.
What happened reaches the sender a moment later in the status it is already being pushed, and a second path saying the same thing is a second path that can disagree.
Only failures get an `error` frame, which is what the operator page has to show.

`install_stop_handler` exists because `loop.add_signal_handler` raises `NotImplementedError` on Windows.
Platform assumptions belong in `capture.py` and that function, nowhere else.

`open_encoder` hands back a source it did not wrap, rather than raising or returning `None`.
A machine with no `ffmpeg` is an ordinary machine, and the caller's next line is the same either way, because the source says what it yields and `build_asr_url` reads that.
It also hands back an already-encoded source untouched, which is what a hosted server gets from a sender: a second codec pass there would add latency to save nothing, since nothing between capture and the recognizer looks at the bytes.

`Remote.start` opens the microphone before it tells the server anything.
What the encoder falls back to is what the hello has to declare, and the server builds its recognizer URL from that and cannot be told a different format later.
Doing it in this order also means a microphone that will not open is reported on the page with no session started anywhere.

The hello carries the Opus stream's first pages as well as its name.
An Ogg stream opens with an `OpusHead` page giving the sample rate and channel count, and nothing after it can be decoded without it, but the middle is where the server always joins: the encoder starts with the microphone here, before there is a socket, and a recognizer run starts over there whenever a session does.
Deepgram handed such a stream from the middle accepts the socket, holds it open, and returns nothing at all, which in a room is indistinguishable from nobody speaking, so `ControlRoom` keeps the pages and puts them in front of every `RemoteCapture` it opens, and an Opus sender that declares none is refused rather than started.
This is why `Encoder` notes its own headers on the way past instead of holding them back, and why `Remote.update` sets `redial` when it reopens a device: a new encoder is a new stream, and the hello is what carries it.
A PCM sender needs none of this, which is why the hosted deployment ran on `pcm` without anyone noticing the hole.

Pressing Start closes the control socket and opens another one.
Start is not a message in the protocol: a sender says what it means in its hello, because a reconnecting one has to say whether it is beginning a meeting or rejoining one, and a second laptop quietly taking over a running session is the thing that refusal exists to prevent.

`Remote.pump` reads a chunk and drops it while there is no socket, rather than waiting for one.
Left unread, the chunk stays in the encoder's pipe, and a blip would fill that pipe and then deliver a minute of stale audio to a meeting that had moved on.
`ControlRoom.feed` keeps the same rule at the other end.

`Remote.update` waits out `START_GRACE` before it believes a status that says stopped.
The hello is what begins a session, so the first statuses after a start still describe the session that had not begun when they were taken, and without the grace every Start would close its own microphone a second later.

A device that will not reopen is recorded in `Remote.declined` and not tried again.
That path runs on a pushed status, which arrives once a second, so without it a meeting running with a microphone this laptop cannot open would spend the rest of the hour opening it.

`Remote.detach` drops the outbox rather than keeping it for the next connection.
A stop queued during a blip and delivered a reconnect later would stop whatever the operator had started in between.

## Conventions

The audio source is deliberately not a setting.
A device name changes with a reboot or a replugged cable, so a name in `config.toml` would be stale more often than right; the operator picks the source on the page, `pipeline.py` takes a plain `--device` and otherwise opens `capture.default_device`, and `Session` keeps the chosen name on itself rather than writing it back into the config.
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
`ffmpeg` is the same bargain one level out: a system binary rather than a package, so a machine without one sends raw audio and is told why, and nothing anywhere requires it.
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
The suite uses no test framework, only the standard library, and exits non-zero if anything fails.
Run it after any change to the pipeline, the sinks, or the routes.
`python3 selftest.py <section>` runs one of `lint`, `pipeline`, `session`, `store`, `server`, or `script`.
There is no `sender` section: what the sender decides on its own is checked in `session`, and what it and a real server have to agree on is checked in `server`, where a server is already being booted.
The `script` section needs no funnel and starts no server: what it checks is the reasoning `transept.py` does on its own, above all that a stale record is never mistaken for a running server.
The `server` section runs `server.py` in a directory of its own, because a selftest that overwrote the pid file of a real server would leave a running meeting with nothing able to stop it.
It boots it three times: once as the default deployment, once with `--capture remote` because the control listener binds publicly and so what it serves and what it turns away is the whole of its guard, and once more for a real `sender.Remote` to connect to over a real socket.
No session is ever started in any of them, because starting one reaches for a recognizer and this suite needs no network.
The audio path itself is checked with both halves in one process, where `Session.start` can be replaced with something that opens no recognizer socket and the chunk can be read off the `RemoteCapture` on the far side.
The `lint` section shells out to `ruff check .` and reports its output as one check, which is what keeps linting in the regular workflow when there is no CI to enforce it.

Adding a check means adding one `checks.check(label, condition, detail)` line.
`FakeSocket`, `FakeSource`, and `FakeTranslator` are already there, and `FakeTranslator` takes `mode="fail"`, `mode="empty"`, and a `delay`, so the failure, dropped language, and timeout paths cost nothing to exercise.

The `store` section includes a `Recorder` that raises on every method, asserted against the `Hub` buffers.
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
`public_url` exists because the address this process binds is not the address a phone can reach.

`operator_port` is a setting; the operator host is not, because the point of the second listener is that a tunnel cannot be pointed at it by mistake.
A headless machine wants an SSH forward rather than a wider bind.

Both tokens are minted every run and printed with their addresses, and there is no tokenless mode on either listener.
`reader_token` and `operator_token` under `[keys]` pin them, and `control_token` is the third, for the sender a hosted server takes its audio from.
A minted control token is printed like the other two, which is enough to try the thing on one laptop, but a real room pins it, because the sender is configured once and has to keep working.
Pinning the reader one is ordinary, because a printed card has to keep working; pinning the operator one exists for development, where a fresh address every restart is a fresh link to click every restart, and a meeting leaves it empty.
They sit with the keys rather than among the settings, and `mint_token()` takes the pinned value as an argument rather than reading the environment itself, so the one place that decides where a secret comes from stays `load_keys`.
Neither is a `SETTINGS` row and so neither has a flag, because a setting invites a weak or forgotten token on the machine that runs the meetings.

Both tokens travel in the address that hands them over, which is what reaches browser history, and they stay in a query string on the requests that cannot carry a header: the reader page's `EventSource` streams, and the `<img>` holding the QR code on the operator page.
Stripping it from the address bar is not a client-side fix while the page itself is gated on it, and on a phone it would break the reload that a locked screen eventually causes.
The reader token is a shared secret for a room, not a credential for a person: it keeps a public hostname from being a public meeting, and it is not meant to survive the card being photographed.
