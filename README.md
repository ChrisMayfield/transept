# Transept

Live captioning and translation for church meetings, delivered to phones.

Someone speaks into a microphone.
A few tenths of a second later the words appear on the phones of people who cannot hear well.
A second or two after that, the same sentence appears in French, Swahili, or whatever other languages anyone in the room has open.

Nothing is projected on a screen and readers install nothing.
Each person opens a link and chooses their own language and their own text size.

## How it works

Audio goes from the sound card to [Deepgram](https://deepgram.com/) (Speech-to-text API) over a websocket.
Deepgram returns finalized fragments, which often cut sentences in half, so a segmenter buffers them into whole sentences.
It closes one when the recognizer reports an endpoint, when the text ends in terminal punctuation, when a silent gap opens, or when the ceiling expires.
This matters more than it sounds: translating "on the heater" by itself produces nonsense in any language that needs a verb.

Whole sentences go to the translation model, all languages in one call, running concurrently but published in source order.
English publishes immediately, and translations arrive on their own channels a moment later.
Phones subscribe over server-sent events, one channel per language, with the last sixty lines replayed on connect so somebody arriving late has context.

**Source files:** `server.py` is the web server and session manager, and the only thing you run on a normal Sunday.
`pipeline.py` is the same pipeline without the web layer, which is the fastest way to check a microphone or tune segmentation.
`review.py` translates a text file offline, `record.py` keeps a session and turns it into a review document, `capture.py` is the audio layer, `selftest.py` checks the software without a microphone or an API key, and `static/` holds the two web pages.

**Config files:** Settings live in `config.toml` and secrets in `.env`.
Keeping them apart means your settings can be committed to your own fork and copied to a second room, while your keys never leave your machine.

## What you need

Transept was built for a Sunday school class with hard-of-hearing members and members whose first language is not English, but nothing in it is specific to that setting.
All it takes is a sound system, a laptop, and somebody willing to press a button before the meeting starts.

- A computer running Linux, macOS, or Windows, with Python 3.11 or newer.
  No GPU required, because the heavy work happens in the cloud.
- An audio input, ideally a line out from the sound board.
  Recognition quality is decided almost entirely here, and a laptop microphone on a table is far worse than a board feed.
- A key from [console.deepgram.com](https://console.deepgram.com) for speech recognition.
- A key for any provider with an OpenAI-compatible endpoint for translation.
  [Google AI Studio](https://aistudio.google.com/), [Claude Platform](https://platform.claude.com/), [OpenAI Platform](https://platform.openai.com/), a [LiteLLM](https://www.litellm.ai/) proxy, and a local [Ollama](https://ollama.com/) install all work.

Capture goes through PortAudio by way of the `sounddevice` package, so the laptop already plugged into the chapel sound system for Zoom should generally work as is.
A second backend, `parec`, is available on Linux and is used by default.
On Linux you may also need `sudo apt install libportaudio2`.

**What it costs (Sep 2026):** Speech recognition runs about $0.50 per hour through Deepgram, and new accounts include free credit that covers a great deal of use.
Translation through a small fast model runs a few cents per hour, and only for languages somebody is actually reading, so a language nobody opens costs nothing at all.
A weekly ninety-minute class costs well under a dollar.

## Setup

```sh
git clone <this repository>
cd transept
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env                # the two API keys
cp config.example.toml config.toml  # everything else
```

Then find your audio source:

```sh
python3 server.py --list-devices
```

Put the name in `config.toml` under `[audio]`, where a partial name is enough.
Note that anything marked as playback captures what the computer is playing rather than what the microphone hears.

## Before your first meeting

**Check the audio, in the actual room, with the actual microphones.**

```sh
python3 pipeline.py --no-translate
```

Talk into the microphone.
Your words should appear within a fraction of a second, each line tagged with how far behind real time it arrived.
If nothing appears, the problem is the audio source, not the software.

**(optional) Fill in two files:** `keyterms.txt` biases the speech recognizer toward words it would otherwise mishear: names of people who speak often, place names, vocabulary specific to your congregation.
`glossary.txt` guides the translation model, and is for names, terms that have an official published rendering in your target languages, and set phrases a general model would translate too literally.
Copy `keyterms.example.txt` and `glossary.example.txt` and edit.

With the `--correct-english` option, the glossary also cleans up recognition errors on the English channel, so a name the recognizer spelled wrong gets fixed for everyone.

Both files are easier to fill in after a real meeting than before one.
If you turn on `record`, this generates a list of suggested edits:

```sh
python3 record.py --session last --out review/sunday.md
```

The document leads with names the correction introduced that were not in the audio, then the terms the recognizer missed and the glossary had to repair, which are exactly the ones to add to `keyterms.txt` so they come out right the first time.

**(optional) Review a language before you offer it.**
You cannot evaluate a translation you cannot read, and neither can anyone else in the room.
Run a real transcript through `review.py` and have a native speaker mark up the result:

```sh
python3 record.py --session last --plain sunday.txt   # if you record
python3 review.py --input sunday.txt --review review.md
```

That pairing is also how to judge a change to the glossary or the prompt: export what the room actually said, edit, re-run, and compare against what shipped on the day.

Ask reviewers for wrong meaning first and awkward phrasing second.
Wrong meaning usually means a glossary entry is missing, and awkward phrasing usually means a different model would serve better.
Settle with your speakers which variety of a language they actually use, because Standard Swahili and Congolese Swahili are not the same and the model follows whichever you name in `--languages`.

## Running a meeting

```sh
python3 server.py
```

That is the whole weekly command, and everything it needs is in `config.toml` and `.env`.
Any setting can still be overridden for a one-off, for example `python3 server.py --ceiling 6`.

Two addresses are printed.
The operator opens the `/operator` one on the laptop, picks the audio source, and presses Start.
It carries a token minted for that run, so copy it from the terminal each week rather than saving a bookmark.
Everyone else opens `/` on their phone and picks a language.

The language list is fixed when the server starts, which keeps the reader URLs stable so a printed card or a saved bookmark keeps working week to week.
Listing a language does not mean paying for it.

## Getting it onto phones

The intended setup is a tunnel, which gives a fixed HTTPS address that works whether phones are on wifi or cellular.
The server binds to `127.0.0.1` by default to suit this: readers arrive through the tunnel, and the operator uses `http://127.0.0.1:8080/operator` on the machine itself, which browsers treat as a secure context without a certificate.

Two tunnels are worth considering, and the difference is whether you want to own a domain.

**Tailscale Funnel** needs no domain and costs nothing.
The address is your machine's own name, `https://<machine>.<tailnet>.ts.net`, and it is the same every time the tunnel starts.
Readers install nothing and need no account, since only the laptop runs Tailscale.
Funnel has to be enabled once for your tailnet in Access Controls.

```sh
tailscale funnel 8080
```

**Cloudflare Tunnel** needs a domain whose DNS Cloudflare manages, which is roughly ten to fifteen dollars a year.
Cloudflare's free Quick Tunnel needs no domain but mints a new random `trycloudflare.com` address every restart, so a printed QR code would stop working the first time the laptop reboots.

```sh
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

Without a tunnel, you can instead set `host = "0.0.0.0"` and have phones connect directly at `http://<laptop-ip>:8080/`.
That is fine for a first test, but it is plain HTTP, and the screen wake lock that keeps a phone from going dark mid-sentence only works in a secure context.
Over plain HTTP your readers will be tapping their screens every thirty seconds for an hour, which is a poor experience for exactly the people this is meant to serve.

## Tuning

Most defaults are fine.
These are the ones that matter, all in `config.toml`.

`ceiling` (4 seconds) is how long a half-finished sentence waits before being translated anyway.
The operator page and the terminal both tag each sentence with why it closed.
A lot of `ceiling` means people are talking over each other, and raising it trades latency for coherence.

`gap` (0.6 seconds) is the silence that separates one person's abandoned sentence from the next person's new one.
Lower it if unrelated turns are being glued together.

`idle_stop_minutes` (10) stops a session that has heard nothing for that long.
Somebody will eventually forget to press Stop, and recognition is billed by audio duration whether anyone is talking or not.
Set a spend limit in the Deepgram console as a second line of defense.

`grace` (90 seconds) is how long a language keeps running after its last reader leaves, so a phone locking its screen does not restart the language.

`reasoning_effort = "low"` is worth setting on models that think by default.
Translation does not benefit from deliberation, and on one test it cut median latency from about two seconds to under one with no loss in quality.

## Privacy and accuracy

Audio from your meeting is sent to Deepgram, and the resulting text is sent to your translation provider.
Both are commercial services with their own retention policies.
This is worth raising with whoever leads the meeting before you deploy it, particularly in a setting where people say personal things out loud.

By default nothing is stored on disk, so captions live in memory and disappear when the session stops.
Turning on `record` in `config.toml` changes that, and it is a decision to make with whoever leads the meeting.
A recorded session keeps every English sentence, every translation, and how long each one took, in `sessions.db` next to the code.
It is not encrypted, `.gitignore` keeps it out of your fork, and nothing is ever deleted automatically.
The operator page says "Recording this session to disk" the whole time one is being kept, because the person at the laptop is the one who has to tell the room.
`python3 record.py --purge --older-than 30` is the only thing that deletes anything.

The translation prompt forbids the model from inventing names, numbers, dates, or scripture references that are not in the source.
This is deliberate and it matters: a reader of a translated channel cannot hear the room and has no way to catch a confident wrong name.
When the audio is unclear, the intended behavior is visible confusion rather than a plausible guess.

Captions are an aid, not a record.
Tell people that.

## About the name

A *transept* is the section of a church that crosses the nave, giving the building its characteristic cross-shaped layout.
The word also plays on *transcription* and *translation*, reflecting the tool's purpose of carrying spoken words across languages and delivering them to people wherever they are in the meeting.

## License

MIT. See `LICENSE`.
