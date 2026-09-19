# HOSTED.md

An optional deployment, built: steps 1 to 3 and 5 of the order of work at the end of this file are in the code, and step 4's configuration, along with what a second room adds to it, is in `deploy/`.

The default Transept deployment is the one `SETUP.md` describes: one laptop in the room, running `server.py`, with a Tailscale Funnel in front of it.
That remains the architecture.
It needs no server to rent, no domain, and no third party holding a transcript, and for a room whose network behaves it is the better answer.

This file describes what to do when the room's network does not behave.
It moves recognition and translation onto a small rented server, leaves audio capture on a laptop in the room, and serves several rooms at once from that server.
Nothing here replaces the default: it adds a capture backend and a second way to run the same `server.py`.

## Why

The first on-site beta failed in two ways.
Both were the building's network, at different points in it, and they need different fixes.

Phones could not reach the funnel at all.
Tailscale Funnel routes each phone out to a Tailscale ingress node and back to the laptop over a relay, and a captive portal or DNS filtering on `.ts.net` blocks that outright.

The informative failure was that latency was bad even when demonstrating the reader on `localhost`, where no network sat between the server and the browser.
That puts the delay entirely in the two legs that leave the laptop: audio up to Deepgram, and translation round trips.
`pump_audio` pushes 256 kbps of raw PCM upstream continuously, which on a throttled or congested uplink keeps the queue full, and every translation request then waits behind the operator's own audio.

Running the same setup over a phone hotspot confirmed it.
English appeared within a second and translations a couple of seconds behind, which is what every other test has produced.

Hosting does not take the building's network out of the picture.
Phones reach the internet either way, since the funnel's address is on the internet too, and the audio still has to get out of the room.
Three things change.

The funnel gives a phone no path to the laptop except out through a Tailscale relay on the internet.
A subtitle therefore goes from the laptop up the building's uplink to that relay, and then back down the same uplink to a phone a few meters away.
Hosted, the server already holds the text, so it makes that trip once instead of twice.

The laptop's uplink also stops serving phones at all.
Today it carries the audio upload and every reader's event stream at the same time, competing for one queue.
Hosted it carries audio and nothing else.

The failure modes separate, which is the part that matters most.
Today a degraded laptop uplink breaks recognition, translation, and delivery to every phone together.
Hosted it delays the audio and nothing else, because a reader's connection to the server no longer depends on the laptop's.
A phone on the building's wifi is still subject to that wifi, but a phone on cellular becomes genuinely independent of the building, which it cannot be today: even on cellular it reads from a relay that has to reach the laptop over the building's connection.

## Scope

One or more rooms, running at the same time, at separate URLs.
Each room has its own operator on its own laptop.
Separate API keys per room where a room's spend is worth attributing, so it comes from the provider's own dashboard rather than being reconstructed, and the shared file's keys otherwise.

There is no account provisioning, no signup, no password database, and no per-user session.
A room's control token is the whole credential, set once when that laptop is configured.
What makes that enough is that the rooms belong to one organization sharing one server.

Multi-tenancy stays out of scope.
Transept is open source, so an unrelated organization runs its own copy rather than asking for an account on somebody else's.
The cost of accounts is also more than a login page: it is quotas, abuse limits, and holding strangers' transcripts.

## A room is a process

One room is one `server.py` process, with its own config file, its own ports, and its own working directory.

This falls out of what `server.py` already does.
`--config` exists, the ports are `SETTINGS` rows, `PID_FILE` is relative to the working directory, and `STATIC` is relative to the code.
So `WorkingDirectory=/srv/transept/chapel` with a shared checkout gives each room its own pid file, database, and log with no code change at all.

```
/srv/transept/config.toml             tuning shared by every room
/srv/transept/chapel/config.toml      this room's ports, keys, tokens, name
/srv/transept/chapel/sessions.db
/srv/transept/classroom/config.toml
/srv/transept/classroom/sessions.db
```

The alternative considered was one process with a room registry and the room in the request path.
That wants a per-request lookup at all nine `request.app[...]` sites, a token check that selects on both room and listener, and tests proving one room's token opens nothing in another.
Separate processes get the same isolation from the operating system, for free, and add the ability to restart one room mid-week without dropping the others.
It also keeps the blast radius of a blocking handler to one room, which matters because `CLAUDE.md` already warns that anything blocking in a handler stalls the subtitle fan-out to every phone.

### What lives in which file

`--config` is repeatable, and files merge left to right, so a room's unit passes the shared file and then its own:

```
ExecStart=... server.py --config ../config.toml --config config.toml
```

Precedence is the command line, then the last config file, then earlier ones, then the built-in default in `SETTINGS`.
A local run passes one file, or none, and behaves exactly as it did before.
The merge is key by key inside a section rather than section by section, because a room naming its own ports must not lose the shared `[server] host` beside them, and a missing file is skipped rather than refused, since the usual run names one file that may not exist yet.

Five values must be in a room file, and nothing else has to be:

```toml
[server]
room = "chapel"
reader_port = 8080
control_port = 8081

[keys]
reader_token = "..."
control_token = "..."
```

The ports must differ because the rooms run on the same rented server.
`port` is renamed to `reader_port` along the way, since three ports called `port`, `operator_port`, and `control_port` invite exactly the mistake the rename removes, and because it then matches `reader_token`.
The rename reaches `SETTINGS`, `ARGUMENT_HELP`, `config.example.toml`, the `--port` flag, the two reads in `transept.py`, and the key `write_pid_file` records.
A `transept.pid` written before the rename names a reader port under a key the script no longer reads, and is still a server it can stop, because the operator port beside it answers and `server_record` asks whether either does.
The two tokens must differ because per-room tokens are the whole access boundary.
The room name must differ because it is what tells the rooms apart.

Everything else can be inherited and usually should be.
`deepgram_api_key` and `llm_api_key` may be overridden per room when spend is worth separating, which is an option rather than a requirement.
`languages`, `glossary`, and `keyterms` are the same for one congregation meeting in two rooms, so they belong in the shared file until a room genuinely differs.

The shared file holds the rest: `capture`, `host`, `public_url`, `model`, `llm_base_url`, `ceiling`, `gap`, `hold`, `timeout`, and the tuning generally.
`CLAUDE.md` calls changing the translation provider the change most likely to be made in a hurry, and that change should not be one edit per room.

`SECRETS`, its environment variable column, and the `selftest.py` assertion that the table and `config.example.toml` agree are all unchanged, because each file in the merge is an ordinary config file.

## Where the pieces run

```
room laptop                          rented server
-----------                          -------------
capture.py  ------ audio ---------->  RemoteCapture
sender.py                             Segmenter, Translator
operator page (loopback)              Hub, Recorder, sessions.db
                                      Deepgram, translation provider
                                              |
                                              +--- SSE ---> phones
```

The operator laptop holds no API key.
Deepgram and translation keys live only in the server's per-room config file, which is the point of hosting them.
A sender's config is a room id, a control token, and a server address, so setting up a laptop does not involve a Deepgram key and there is nothing to keep in sync between machines.

## Audio capture stays in Python

Capture stays where it is, in `capture.py`, over `parec` or `sounddevice` as today.

Browser capture was considered and rejected.
It was proposed when zero-install mattered, which was a consequence of multi-tenancy, and multi-tenancy is out of scope.
On the merits it loses on every axis that matters here.
`getUserMedia` applies automatic gain control, noise suppression, and echo cancellation by default, and disabling them is a constraint request a browser may decline, where `parec` reads the device raw.
Bandwidth is a wash, since `MediaRecorder` gives Opus for free but the Python side can shell out to `ffmpeg` for the same result.
Reliability is worse, because a suspended tab or a sleeping machine stops an AudioContext with no error, while a Python process has the supervisor and reconnect logic that already exists.

Keeping capture in Python also keeps the operator page on loopback, which is what makes the rest of this proposal small.

### The remote backend

`capture.py` gains a third backend named `remote`, and `resolve_backend` gains a case.
`RemoteCapture.read()` awaits the next chunk that arrived over the room's control socket, and returns the same 16 kHz mono signed 16-bit chunks the other two backends return.
Everything downstream is untouched: `Segmenter`, `Translator`, `Hub`, the sinks, `publish`, the reader page.

`capture = "remote"` is also what tells `server.py` it is the hosted half, rather than a second setting saying so.
A separate `mode` row could be changed without the backend being changed to match, leaving a server that opens a local sound card while waiting for a sender to connect, or one that waits for audio a local device is already producing.

### A socket gap is not end of stream

`pump_audio` treats an empty chunk as end of stream, which is right for a capture device that died and wrong for a wifi blip.
`RemoteCapture.read()` holds through a gap up to a grace period instead of returning empty bytes, and the pump sends Deepgram a `KeepAlive` during the gap rather than silence, so the socket stays open without billing for an empty room.
Past the grace period the session stops and the supervisor behaves as it does now.

Only one control socket per room at a time.
A second is refused while the first is inside its grace window, because the alternative is a stray laptop silently taking over a running session.

A sender that reconnects says whether it is attaching or starting, and attaching rejoins the running session rather than starting a second one.

## The sender

`sender.py` is a new entry point on the room's laptop.
It opens `capture.open_capture`, connects a websocket to the server, pumps audio, and serves the operator page on `127.0.0.1` exactly as `server.py` does today.

It is a pipe, not a second pipeline.
It runs no `Segmenter`, no `Translator`, and no `publish`, so it is not the duplicated-loop mistake `CLAUDE.md` records.
`pump_audio` was not reused, and the reason is not the `CloseStream` message it sends on the way out.
That loop ends when the device does, which is right for a pipeline that owns its socket from one end to the other; `Remote.pump` outlives both the socket and the microphone, because the microphone stays open across a reconnect and the socket comes and goes under it.
It also has to read a chunk and drop it while there is no socket, rather than leave it in the encoder's pipe, which would fill during a blip and then deliver a minute of stale audio to a meeting that had moved on.
That is the rule `ControlRoom` already keeps at the other end, where audio arriving between recognizer runs has no socket to go up.

`/api/devices` stays on the sender and keeps working unchanged, because the audio device genuinely is local knowledge.
`/api/start`, `/api/stop`, and `/api/language` become messages the sender forwards over the control socket.
`/api/status` is served from the last status snapshot the server pushed down that socket, so the operator page did not change at all.
`/qr.svg` is rendered locally with `segno`, from the reader address the server sends when the socket opens.

Two things the snapshot cannot say, the sender lays over the top of it.
Which microphone is open, because that is this laptop's and the server only knows the name it was handed.
And whether the socket is up at all, because a snapshot from thirty seconds ago describing a running meeting is exactly what a dropped connection looks like, and the page would show a healthy meeting with no way to tell.

The two halves can also come to disagree about whether a meeting is on, in both directions.
A session can end without this laptop asking, because the idle watchdog stopped it, and a microphone left open here would read a room nobody is listening to.
A sender restarted mid-meeting finds a session running with nobody feeding it, which is what attaching is for: it reopens the device the status names and carries on.
Reconciling on the pushed status rather than on the `ready` frame is what makes both work, since `ready` is sent before the intent on the hello has started anything.
The cost is that a status saying stopped in the first few seconds after Start is the session that had not begun yet, rather than one that ended, so it is taken at its word only after a short grace.

### Where the shared operator code lives

`authorized`, `mint_token`, `read_body`, `operator_page`, `api_status`, `api_devices`, `api_start`, `api_stop`, `api_language`, `qr_code`, and `build_operator_app` all live in `server.py` today, and `sender.py` needs every one of them.
They move to a new `controls.py`, which both entry points import and which imports neither.
`server.py` still uses `authorized` and `page` for the reader application, so the shared helpers move too rather than being reached back for, along with `STATIC` and `OPERATOR_HOST`.

The file is `controls.py` and not `operator.py` because `operator` is a standard library module, and a file of that name beside the code shadows it for everything started from this directory.
Not a subtle failure: `collections` imports `operator`, so `import collections` fails, and with it aiohttp, websockets, and httpx.
`build_operator_app` also lost its `hub` argument, because no route on that listener ever read one, and a sender has no Hub to pass.

Copying them instead is the mistake this project has already made once, when the pipeline loops were duplicated and the copies drifted until a session start raised `NameError`.
Importing `server.py` from `sender.py` would work, since `server.py` defines only constants, classes, and functions, but it reads backwards and drags `Hub` and `Session` into a program that has neither.

The extraction is cheap because those handlers already talk to one object with four methods: `start`, `stop`, `set_override`, and `status`.
`Session` is that object in the default deployment, and the sender supplies a proxy of the same shape that forwards over the control socket.
This is the same "one interface, two implementations" the codebase already uses for capture backends and for sinks.

## The control protocol

One websocket per room, `/control`, gated on `control_token` and an `Origin` check.
Binary frames are audio and nothing else.
Text frames are JSON.

The `Origin` check refuses any handshake that carries the header at all, rather than allowing a list of origins.
Websockets are not subject to the same-origin policy, so a page on any site the operator visits can open one to any host without a CORS preflight standing in the way.
The only legitimate client here is `sender.py`, which is a Python program and sends no `Origin`, while a browser stamps every handshake with its own and cannot be made not to.
So a page that somehow learned the control token, from a pasted address or a shared screen, still cannot use it.

Sender to server:

```
hello     room, encoding ("pcm" or "opus"), device, intent ("start" or "attach")
stop      end the session
language  name, mode ("on", "off", or "auto")
```

Server to sender:

```
ready     reader_url, the language list, and the current session state
status    the dict Session.status() already returns, which the page already renders
error     text, for a start that failed
```

`hello` carries the encoding because the server has to build the Deepgram URL to match, and carries the intent because a page reload opens a new socket for a room that may already be running.
`status` is the existing snapshot rather than a new shape, so the operator page keeps rendering what it renders now.

## Listeners and tokens

The server keeps two listeners, as it does now, and which pair it builds follows from the capture backend.

Local, which is the default deployment and unchanged:

```
reader     host:reader_port          reader_token     the pages and streams
operator   127.0.0.1:operator_port   operator_token   the page and its API
```

Hosted:

```
reader     host:reader_port          reader_token     the pages and streams
control    host:control_port         control_token    WS /control, one route
```

`control_port` is a new `SETTINGS` row, defaulting to 8081, rather than a number written into the code.
It joins `SETTINGS`, `ARGUMENT_HELP`, and `config.example.toml` in the same position in each, since `selftest.py` fails on drift between the first and the last.

`OPERATOR_HOST` stays `127.0.0.1` and stays applied to `build_operator_app`, which the hosted server never builds, so the rule that a tunnel cannot be pointed at the operator page is unchanged.

The control application is governed separately, because it has a different problem to solve.
It has to bind publicly, since the sender that reaches it is in another building, and what guards it is `control_token` and the `Origin` check rather than the address it binds to.
It serves no page and exactly one websocket route, which is what keeps that a small thing to guard.

Three tokens, with three jobs:

- `reader_token`, per room, in the server's config, pinned so a printed card keeps working.
- `control_token`, per room, in the server's config and in that room's sender config, authenticating the sender to the server.
- `operator_token`, minted per run by the sender, gating the loopback operator page.
  Its reason survives hosting: it stops a page in another tab of the operator's browser from posting a cross-origin form at the loopback port.

The control socket's handshake is gated on `control_token` and additionally checks `Origin`.

## URLs

Caddy strips a per-room path prefix and forwards to that room's reader port.

```
https://transept.example.org/chapel/reader?token=...
https://transept.example.org/classroom/reader?token=...
```

`public_url` is the bare base, shared by every room, and the room name is the prefix:

```toml
public_url = "https://transept.example.org/"
```

`reader_address` takes the room as well as the base and the token, and includes the segment only when a room is named.
That the room name is also the path prefix is deliberate.
One value then names the working directory, the Caddy route, the recorded column, and the address, instead of four values that can disagree.

Which room it is handed is `card_room`, and the answer is the room only where `capture` is `remote`.
The prefix exists because something in front strips it, that something is Caddy, and Caddy is in front of a hosted room, which is the same thing the backend already says: the audio arrives from a laptop because the server is somewhere else.
The alternative, a room name that is always a prefix, breaks a laptop that names its room for the recorded transcript, since a funnel serves the root and the card would point at a path nothing answers.
A card that 404s is exactly the kind of failure this project would rather not ship, and the rule that avoids it is the one already used to decide which listener a server raises.

`reader.html` built `/stream/...` and `/api/channels` as root-absolute paths, which under a stripped prefix the browser would resolve against the host rather than the room, reaching whichever room answers the root with this room's token in the query string.
The page derives a base from `location.pathname` instead, dropping the last segment, which is empty at the root and `/chapel` under a prefix.
That is two lines, and only the reader page needs them, since the operator page stays on loopback where no prefix exists.

A subdomain per room would need no page change at all.
The path form is kept because two lines is a small price for the URL shape, and because one certificate and one DNS record is less to forget than one of each per room.

## Recording

Recording happens where `Session` runs, which is the server.
The transcript and its translations never reach the operator's laptop, and sending them back would be work spent to make the record harder to find.
Recording stays off unless `[session] record` is set, as it is now.

Each room records into its own working directory, so `record.py --session last` stays unambiguous.

### The room column

`sessions` gains a `room TEXT` column, filled from the `[server] room` setting, even though each room already has a database to itself.
The reason is export: several databases will eventually be read as one data set, and a room name added at that point is a name reconstructed from a file path rather than one the recorder wrote down.
`lines` and `translations` need nothing, because they reach the room through `session_id`.

The setting sits in `[server]` beside `public_url`, because the two compose into the room's address, and recording is not its only reader.

The name is a setting rather than the working directory's name.
Deriving it is tempting, since the pid file and the database already come from the directory, but those are places files go and this is a value written into a row that outlives the directory.
A `WorkingDirectory` edited in a unit file, or a test run from somewhere else, would silently relabel recorded data.

`SCHEMA` is all `CREATE TABLE IF NOT EXISTS`, so a database that already exists does not gain the column on its own.
`connect` checks `PRAGMA table_info(sessions)` and issues `ALTER TABLE sessions ADD COLUMN room TEXT` when it is missing, which sqlite does without rewriting the table.

## Deployment

One small VM, Caddy in front for automatic TLS, one systemd unit per room, from a template so a new room is `systemctl enable transept@chapel`.
A 1 vCPU and 1 GB box is ample for a few rooms: the work is a websocket each, a handful of HTTP calls, and a write per reader per line.

Any US region is fine.
The difference between sensible US regions is tens of milliseconds, against the hundreds lost to queueing on a bad uplink, so region is not worth optimizing.

`sessions.db` needs a volume that survives redeploys if recording is on.

`transept.py` and `transept.log` are unchanged and are not deleted.
They are the default deployment's weekly command, and a room whose network behaves should keep using them.
On the rented server a systemd unit takes their place, which is the form `CLAUDE.md` already says `server.py` must stay runnable in.

## Bandwidth and compression

Raw PCM upstream is 256 kbps.
Opus at 24 to 32 kbps carries 16 kHz mono speech well enough that recognition is unaffected, so the saving is roughly eight to one.

The quality and delay costs are small enough to ignore.
Opus uses 20 ms frames with about 6.5 ms of encoder lookahead, so the added delay is under 30 ms in a pipeline where finalized recognition already arrives 200 to 300 ms behind the speaker.
Audio already band-limited to 8 kHz is well inside what the codec carries transparently.

The building's uplink is the confirmed problem, so `encoding` defaults to `opus`.
Set it to `pcm` if a real run shows recognition or delay getting worse.

### Where the encoder sits

The encoder belongs in the pipeline, between capture and the socket, rather than in `capture.py`.
That keeps the promise every backend makes, which is 16 kHz mono signed 16-bit chunks with nothing downstream knowing which backend produced them.

Compression exists to protect a constrained uplink and for no other reason.
Deepgram bills by audio duration rather than by bytes, so encoding saves nothing there, and Deepgram recognizes Opus and PCM equally well.

The encoder sits on the machine that captures the audio, and the audio stays encoded from there to Deepgram.
Nothing decodes it in between.
In the default deployment the laptop encodes and talks to Deepgram directly, and this is the leg the beta was failing on, so compression is worth having there and not only when hosted.
Hosted, the sender encodes and the rented server forwards the frames it receives without touching them.
Deepgram receives Opus either way, which is one path rather than two.

Decoding on the server was considered and dropped.
It would put an `ffmpeg` process on the server and add latency to buy nothing, because the only thing downstream of capture that touches audio bytes is `pump_audio`, which forwards them.
`Segmenter` and `Translator` see text.

What has to stay true is that the Deepgram URL declares the format actually being sent.
So a capture object carries its own `encoding`, which is `pcm` for `parec` and `sounddevice` and whatever the sender declared for `remote`, and `build_asr_url` reads it.
The promise `capture.py` makes widens from "every backend yields PCM" to "every backend says what it yields," and the two local backends still always say PCM.
`_run_once` already opens the source before it builds the URL, so the ordering works as it stands.

This is also what carries the fallback.
A sender with no working `ffmpeg` says `pcm` in its opening message, the server builds a PCM URL, and the session runs uncompressed with nothing else needing to know.

### A missing ffmpeg is not a failure

Two ffmpeg defaults have to be overridden, and both are latency rather than tuning.
ffmpeg reads about two seconds of a raw stream before it decides what the stream is, which `-analyzeduration 0 -probesize 32` turns off, and the ogg muxer holds a full second of audio in a page before writing it, which `-page_duration 20000` cuts to 20 milliseconds.
Measured on the first attempt: without them the first byte out of the encoder arrived 1.9 seconds after the first byte in, which would have been added to every sentence in the meeting.

`ffmpeg` is a system binary rather than a package in `requirements.txt`, and defaulting to Opus would otherwise make it a requirement on every operator machine including Windows.
So an encoder that is missing or will not start falls back to PCM and says so, the way `sounddevice` and `segno` are already handled: optional, with the failure caught and reported rather than raised.

The fallback is decided when the encoder starts, before the Deepgram socket is opened, because a stream that changed format mid-session would have no way to say so.

The encoder's pipes need the care `parec`'s already got.
`parec` sends stderr to `DEVNULL` because nothing drains that pipe, and enough diagnostics fill the buffer and block the process mid-write, which stops audio with no error and no end of stream.
A second subprocess in the same path has the same failure available to it.

## Unchanged

The non-negotiables are untouched, and nothing here should be read as relaxing them.
`SYSTEM_PROMPT` still forbids inventing a proper noun, name, number, date, or scripture reference.
The English channel is still a transcript rather than a summary.
A failed, timed out, or missing translation still falls back to the English line.

Demand-driven translation is unchanged and matters more hosted, since the reader address is reachable between meetings rather than only while a funnel is open.
`idle_stop_minutes` bounds a session left running, and no session means no recognition and no translation, so the worst case for a leaked reader address is a stranger reading an empty feed.

`max_languages` keeps its default of 0, for the reason it already has: showing English to a real reader is worse than the tokens a cap saves.

Per-room keys are for attribution, not containment.
`Session.stats` and the recorded elapsed times already give per-room counts; separate keys are what makes Deepgram's own minute totals line up with a room without arithmetic.

## New settings and secrets

Each row joins `SETTINGS`, `ARGUMENT_HELP`, and `config.example.toml` at the same position in all three, since `selftest.py` fails on drift between the first and the last.

```
("encoding",      "audio",   "encoding",      str,   "opus")
("room",          "server",  "room",          str,   "")
("reader_port",   "server",  "reader_port",   int,   8080)    renamed from port
("control_port",  "server",  "control_port",  int,   8081)
("control_grace", "server",  "control_grace", float, 30.0)
("control_url",   "server",  "control_url",   str,   "")
```

`control_url` was not in the original list and is the sender's third setting, beside `room` and `control_token`: the whole address of a room's control socket, path and all, because a hosted room lives under a path prefix of its own.
It is a setting rather than a flag-only argument for the reason the tokens are pinned, which is that a sender is configured once and has to keep working.
Only `sender.py` reads it, and a room's server ignores it.

`control_grace` is spelled out rather than called `grace`, because `[languages] grace` already means how long a language survives its last reader, and the two are unrelated.

One new `SECRETS` row, whose third column is the second in capitals as the table requires:

```
("control_token", "control_token", "CONTROL_TOKEN")
```

`room`, `reader_port`, `control_port`, and `encoding` are worth naming in `server.py`'s `add_settings_arguments` call so they can be overridden for a one-off.
`control_token` is not a `SETTINGS` row and so gets no flag, for the reason the other tokens do not: a secret on a command line lands in the process list and in shell history.

## Checks to add

`selftest.py` stays the whole suite, needing no keys, no audio device, and no network.
The first four belong in `pipeline`, the next three in `session`, the room column in `store`, and the last two in `server`.
A `sender` section is still not worth adding: the sender's checks sit in `session` for what the proxy decides on its own, and in `server` for what it and a real server have to agree on.

- `RemoteCapture` through the pipeline: a fake socket feeding chunks produces units, in the shape `FakeSocket` and `FakeSource` already use. Added.
- A socket gap: the session survives a drop inside the grace window, `KeepAlive` goes out during it, and the session stops once the window passes. Added.
- A second control socket is refused while the first is live, and an attaching socket rejoins the running session instead of starting a second one. Added.
- The control application serves no route but `/control`, so a control token opens nothing a reader token should not, and the reader application still serves nothing that writes. Added.
- `reader.html` resolves its stream and channel URLs under a path prefix as well as at the root, and builds no address that goes around that base. Added.
- A database created before the room column gains it on open, and the recorded room survives a round trip through `record.py`. Added.
- Config files merge left to right, and a room file's value beats the shared file's while the rest of that section survives beside it. Added.
- A missing encoder falls back to PCM, and the Deepgram URL declares the encoding actually being sent. Added.

Three more came with the sender, because they are the state where the two halves can disagree.

- The sender answers the operator page in full before any server has spoken to it, checked against a real `Session.status()` rather than against the sender's own table.
- A stopped status just after Start is the session that had not begun yet, and a stopped status later is a session that ended without being asked, which closes the microphone here.
- A chunk captured on the laptop arrives at the source the recognizer reads, with the server holding the encoding the sender declared.

Each needs the revert test: add the check, undo the fix, confirm the check fails.
The gap and attach rules are exactly the kind of state where a check that always passes is easy to write by accident, and so is the shape of a status: a table checked against itself cannot fail, which is how the first version of that check was written.

## Order of work

1. The `room` setting, the `room` column, and its migration. Built.
   This depended on nothing else here and was worth landing on its own, so that recording carries the name from the first hosted meeting rather than from the second.
2. `RemoteCapture` and the control socket, with a throwaway Python client. Built.
   Tested on a single laptop, loopback to loopback, before anything was rented.
   The encoder is not part of it: nothing yet produces Opus, so there is no `encoding` setting, and a sender that declares `opus` in its hello gets a recognizer URL built for it and nothing else.
   That setting belongs with the `ffmpeg` encoder, which sits on the machine that captures, and so lands with the sender.
3. `sender.py`: capture, the loopback operator page, the forwarded controls. Built.
   The shared operator code moved to `controls.py` rather than `operator.py`, and the Opus encoder landed with it, as the sections below now describe.
4. Deploy one room. Written, in `deploy/`.
   A VM, a hostname, `deploy/Caddyfile`, `deploy/transept@.service`, `config.example.toml` copied to the root of the checkout as the shared file, and `deploy/room.example.toml` copied into the room's working directory, in the order `deploy/README.md` gives.
   Both listeners bind loopback and Caddy is the only public surface, so the control socket keeps its token and its `Origin` check and is additionally unreachable except through Caddy, which is a smaller thing to guard than this file assumed.
   The room's `encoding` is `pcm`, which is not the audio going up uncompressed but this server declining to re-encode what the sender already encoded, so no `ffmpeg` belongs on the rented machine.
   The first room was deployed at the root of its own hostname, before the path prefix work below, and moving it under one is the three edits at the end of `deploy/README.md`.
5. Add a second room. Built.
   The four things it needed are in the code: `--config` takes more than one file and merges left to right, `port` is `reader_port`, `reader_address` takes a room, and `reader.html` derives its base from `location.pathname`.
   A second room is then a copy of `room.example.toml` with its own name, tokens, and ports, a second `systemctl enable`, and a second block in the `Caddyfile`, and no code at all.
   A second hostname would work as well and needs even less, but one certificate and one DNS record is less to forget than one of each per room.

Steps 2 and 3 are the bulk of the code.
Step 4 is configuration, and step 5 was the test of whether a room really is just a process: four small changes, none of them about rooms knowing about each other, because none of them do.
