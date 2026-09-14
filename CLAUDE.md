# Notes for AI assistants

Context for working on this repository.
Read this before making changes.

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

## Architecture

```
capture -> Deepgram websocket -> Segmenter -> Translator -> Hub -> SSE -> phones
```

`capture.py` presents one interface over two backends: `sounddevice` (PortAudio, all platforms) and `parec` (Linux only).
Both yield 16 kHz mono signed 16-bit chunks; nothing downstream knows which produced them.
An empty chunk means end of stream, which is how a device that disappears becomes a reconnect rather than a hang.

`pipeline.py` holds the shared pieces: `Segmenter`, `Translator`, `SYSTEM_PROMPT`, `load_env`.
It also runs standalone as a terminal tool, which is the fastest way to debug the pipeline without the web layer.

`server.py` imports from `pipeline.py` and adds `Hub` (ring buffers and subscribers), `Session` (start, stop, supervise, reconnect), and the aiohttp routes.
The two must stay in step; the prompt lives in one place on purpose.

`review.py` is an offline tool that shares `Translator` and the prompt, differing only in retry policy: the live pipeline gets one fast retry because a meeting cannot wait, while review retries harder because nothing is waiting on the answer.

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

`_pump` sends `CloseStream` in a `finally` block.
Without it, a dead capture device leaves the websocket open and the session hangs in `running` with no audio and no reconnect.

The 48 kHz fallback averages groups of three samples rather than taking every third one.
Plain decimation would alias everything above 8 kHz back into the speech band.

`install_stop_handler` exists because `loop.add_signal_handler` raises `NotImplementedError` on Windows.
Platform assumptions belong in `capture.py` and that function, nowhere else.

## Conventions

Settings live in `config.toml`, resolved by the `SETTINGS` table in `pipeline.py`.
Adding a setting means adding one row there; both entry points build their flags from it.
Every argparse default is `None` so `resolve` can distinguish an absent flag from one that happens to match the default.
Secrets stay in `.env` and never move into `config.toml`.

Standard library first, few dependencies.
`websockets`, `httpx`, and `aiohttp` are the whole list, and adding a fourth needs a real reason.

No em-dashes in prose or comments.
Headings use colons, not dashes.
One sentence per line in Markdown files, so diffs isolate the sentence that changed.

Comments explain why, not what.
Where a line encodes a decision that was reached the hard way, say what would go wrong without it.

Prefer concrete nouns over pronouns in anything a volunteer might read.

## Testing

There is no test suite.
The three-layer settings precedence was verified by resolving the example config with and without overrides.
Logic changes to the segmenter or the activation rules should be exercised with a short inline script against real fragment timings before they are trusted; that is how the gap check and the language activation rules were verified.

Latency claims should come from an actual run, not an estimate.
`capture_test.py` reports recognition lag and `translate_test.py` reports translation median and p95.

## Open work

The reader view is a plain web page by choice, not an installable app.
Readers open a link and pick a language; a manifest and service worker would add install friction and offline machinery that a live caption feed cannot use anyway.
HTTPS is still required, because the screen wake lock needs a secure context, and a tunnel in front is the current answer.
`public_url` exists because the bind address is not the address a phone can reach; the QR endpoint renders that value, not the listener.
Nothing is persisted, so there is no transcript after a session ends.
The operator token is thin security appropriate for a local network and nothing more.
