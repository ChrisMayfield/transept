# Transept

Live captioning and translation for church meetings, delivered to phones.

Someone speaks into a microphone.
A few tenths of a second later the words appear on the phones of people who cannot hear well.
A second or two after that, the same sentence appears in French, Swahili, or whatever other languages anyone in the room has open.

Nothing is projected on a screen.
Each person chooses their own language and their own text size, on their own phone.

## Who this is for

A congregation with a sound system, a laptop, and somebody willing to press a button before the meeting starts.
It was built for a Sunday school class with hard-of-hearing members and members whose first language is not English, but nothing in it is specific to that setting.

It needs an existing microphone feed.
Recognition quality depends almost entirely on the audio it receives, so a line out from the sound board will work far better than a laptop microphone sitting on a table.

## What it costs

Speech recognition runs about $0.50 per hour through Deepgram, and new accounts include free credit that covers a great deal of use.
Translation through a small fast model runs a few cents per hour.
A weekly ninety-minute class costs well under a dollar.

Translation only runs for languages somebody is actually reading, so a language nobody opens costs nothing at all.

## Requirements

A computer running Linux, macOS, or Windows.
Python 3.11 or newer.
An audio input, ideally fed from the sound board.
No GPU is needed; the heavy work happens in the cloud.

Audio capture goes through PortAudio by way of the `sounddevice` package, so the laptop already plugged into the chapel sound system for Zoom will generally work as is.
On Linux you may also need `sudo apt install libportaudio2`.
There is a second backend, `parec`, available on Linux only; set `backend = "parec"` under `[audio]` if PortAudio misbehaves on a particular machine.

You will need two API keys: one from [Deepgram](https://console.deepgram.com) for speech recognition, and one from any provider with an OpenAI-compatible endpoint for translation.
Google AI Studio, Anthropic, OpenAI, a LiteLLM proxy, and a local Ollama install all work.

## Setup

```
git clone <this repository>
cd transept
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
cp config.example.toml config.toml
```

Put your two API keys and an operator password in `.env`.
Put everything else in `config.toml`.
Then find your audio source:

```
python3 server.py --list-devices
```

Pick the one that corresponds to your microphone input and put it in `config.toml` under `[audio]`.
A partial name is enough.
Anything marked as playback captures what the computer is playing rather than what the microphone hears, which is the most common setup mistake.

## Check the audio first

Before running the server, confirm the audio path works:

```
python3 pipeline.py --no-translate
```

Talk into the microphone.
You should see your words appear within a fraction of a second, each line tagged with how far behind real time it arrived.
If nothing appears, the problem is the audio source, not the software.

Do this in the actual room, with the actual microphones, before anything else.
Recognition accuracy is decided almost entirely here.

## Run it

```
python3 server.py
```

That is the whole weekly command.
Everything it needs is in `config.toml` and `.env`.
Any setting can still be overridden for a one-off, for example `python3 server.py --ceiling 6`.

Two addresses are printed.
The operator opens `/operator?token=...` on the laptop, picks the audio source, and presses Start.
Everyone else opens `/` on their phone and picks a language.

The language list is fixed when the server starts, which keeps the reader URLs stable so a printed card or a saved bookmark keeps working week to week.
Listing a language does not mean paying for it.

## Getting it onto phones

There is nothing to install.
Readers open a web page and pick a language, which is the whole interaction.

The intended setup is a tunnel, which gives a fixed HTTPS address that works whether phones are on wifi or cellular.
The server binds to `127.0.0.1` by default to suit this: readers arrive through the tunnel, and the operator uses `http://127.0.0.1:8080/operator` on the machine itself, which browsers treat as a secure context without a certificate.

Two tunnels are worth considering, and the difference is whether you want to own a domain.

**Tailscale Funnel** needs no domain and costs nothing.
The address is your machine's own name, `https://<machine>.<tailnet>.ts.net`, and it is the same every time the tunnel starts.
Readers install nothing and need no account; only the laptop runs Tailscale.
Funnel has to be enabled once for your tailnet in Access Controls.

```
tailscale funnel 8080
```

**Cloudflare Tunnel** needs a domain whose DNS Cloudflare manages, which is roughly ten to fifteen dollars a year.
Cloudflare's free Quick Tunnel needs no domain but mints a new random `trycloudflare.com` address every restart, so a printed QR code would stop working the first time the laptop reboots.
Use a named tunnel if you already have a domain:

```
cloudflared tunnel create chapel
cloudflared tunnel route dns chapel captions.example.org
```

Whichever you pick, put the resulting address in `config.toml`:

```toml
[server]
public_url = "https://chapel.your-tailnet.ts.net/"
```

The operator page then shows that address as a QR code, next to the link itself.
Print it on a card, leave it in the room, and it keeps working as long as the hostname does.
Nothing needs reprinting from week to week.

Without a tunnel, you can instead set `host = "0.0.0.0"` and have phones connect directly at `http://<laptop-ip>:8080/`.
That is fine for a first test, but it is plain HTTP, and the screen wake lock that keeps a phone from going dark mid-sentence only works in a secure context.
Over plain HTTP your readers will be tapping their screens every thirty seconds for an hour, which is a poor experience for exactly the people this is meant to serve.

## Making it accurate

Two files do most of the work, and both are worth ten minutes before your first real meeting.

`keyterms.txt` biases the speech recognizer toward words it would otherwise mishear: names of people who speak often, place names, vocabulary specific to your congregation.
Copy `keyterms.example.txt` and edit.

`glossary.txt` guides the translation model.
Use it for names, for terms that have an official published rendering in your target languages, and for set phrases a general model would translate too literally.
Copy `glossary.example.txt` and edit.

With `--correct-english`, the glossary also cleans up recognition errors on the English channel, so a name spelled wrong by the recognizer gets fixed for everyone.

## Reviewing a language before you offer it

You cannot evaluate a translation you cannot read, and neither can anyone else in the room.
Before offering a language, run a real transcript through `review.py` and have a native speaker mark it up:

```
python3 review.py --input transcript.txt --review review.md
```

Ask reviewers for wrong meaning first and awkward phrasing second.
Wrong meaning usually means a glossary entry is missing.
Awkward phrasing usually means a different model would serve better.

One thing worth settling with your speakers: which variety of a language they actually use.
Standard Swahili and Congolese Swahili are not the same, and a model will follow whichever you name in `--languages`.

## Tuning

Most defaults are fine. These are the ones that matter.

Change these in `config.toml`.

`ceiling` (default 4 seconds) is how long a half-finished sentence waits before being translated anyway.
The operator page and the terminal both tag each sentence with why it closed.
If you see a lot of `ceiling`, people are talking over each other and translations will read poorly; raising it trades latency for coherence.

`gap` (default 0.6 seconds) is the silence that separates one person's abandoned sentence from the next person's new one.
Lower it if unrelated turns are being glued together.

`idle_stop_minutes` (default 10) stops a session that has heard nothing for that long.
Somebody will eventually forget to press Stop, and recognition is billed by audio duration whether anyone is talking or not.
Set a spend limit in the Deepgram console as a second line of defence.

`grace` (default 90 seconds) is how long a language keeps running after its last reader leaves, so a phone locking its screen does not restart the language.

`reasoning_effort = "low"` is worth setting on models that think by default.
Translation does not benefit from deliberation, and on one test it cut median latency from about two seconds to under one with no loss in quality.

## How it works

Audio goes from the sound card to Deepgram over a websocket, captured through PortAudio or, on Linux, optionally through `parec`.
Deepgram returns finalized fragments, which often cut sentences in half.

A segmenter buffers those fragments into whole sentences, closing one when the recognizer reports an endpoint, when the text ends in terminal punctuation, when a silent gap opens, or when the ceiling expires.
This matters more than it sounds: translating "on the heater" by itself produces nonsense in any language that needs a verb.

Whole sentences go to the translation model, all languages in one call, running concurrently but published in source order.
English publishes immediately; translations replace nothing and simply arrive on their own channels a moment later.

Phones subscribe over server-sent events, one channel per language, with the last sixty lines replayed on connect so somebody arriving late has context.

## Files

`server.py` is the web server and session manager, and it is the only thing you run on a normal Sunday.
`pipeline.py` holds the segmenter, the translator, and the prompt.
It also runs standalone as a terminal tool, which is the fastest way to check a microphone or tune segmentation; add `--no-translate` to test the audio path without a translation key.
`review.py` translates a text file and writes the review document.
`capture.py` is the audio layer, with one interface over both backends.
`selftest.py` checks the software without a microphone or an API key, which is worth running after you change anything.
`static/` holds the two web pages.

Settings live in `config.toml` and secrets in `.env`.
Keeping them apart means your settings can be committed to your own fork and copied to a second room, while your keys never leave your machine.

## Privacy

Audio from your meeting is sent to Deepgram, and the resulting text is sent to your translation provider.
Both are commercial services with their own retention policies.
This is worth raising with whoever leads the meeting before you deploy it, particularly in a setting where people say personal things out loud.

Nothing is stored on disk by this software.
Captions live in memory and disappear when the session stops.

## A note on accuracy

The translation prompt forbids the model from inventing names, numbers, dates, or scripture references that are not in the source.
This is deliberate and it matters: a reader of a translated channel cannot hear the room and has no way to catch a confident wrong name.
When the audio is unclear, the intended behavior is visible confusion rather than a plausible guess.

Captions are an aid, not a record.
Tell people that.

## About the name

The transept is the crossing arm of a church, the part that runs side to side.
It also happens to begin like "translate."

## License

MIT. See `LICENSE`.
