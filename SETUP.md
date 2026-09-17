# Setting up Transept

This page is the one-time setup: installing the software, getting API keys, and giving phones an address they can reach.
Plan on about half an hour.
Do the setup at a desk rather than in the meeting room, because only the audio check near the end needs the real microphones.

`README.md` covers what Transept is, what it costs, and what it sends where.
Read `README.md` first if you have not.

Transept is written in Python, but you don't need to know Python.
Everything below is typed at a command line, one line at a time.

## Before you begin

**Windows users:** install [Git Bash](https://git-scm.com/install/windows) and use Git Bash for every command on this page, rather than Command Prompt or PowerShell.
Git Bash gives Windows the same commands Linux and macOS already have, so there is one set of instructions instead of three.
Paths in Git Bash are written with forward slashes, so your home folder is `/c/Users/<username>`.

**macOS users:** open Terminal, in Applications then Utilities.

**Linux users:** open whatever terminal your desktop provides.

## Installing

### 1. Install Python

Transept needs Python 3.11 or newer.
Check what you have:

```sh
python3 --version
```

If the version printed is 3.11 or higher, skip to step 2.
On Windows, try `python --version` as well, because a Windows machine with Python installed often has no `python3` on it, and typing `python3` there may open the Microsoft Store instead of answering.

**Linux:** Python is usually already installed.
If Python is missing, or if the next step complains about `venv`, run `sudo apt install python3 python3-venv` (or your distribution's equivalent).

**macOS:** download the installer from [python.org/downloads](https://www.python.org/downloads/) and run it.
The Python that Apple ships is not always present and is often too old, so install your own rather than fighting with the system copy.

**Windows:** download the installer from [python.org/downloads](https://www.python.org/downloads/) and run it.
Check **Add python.exe to PATH** on the first screen of the installer, which is easy to miss and is what lets Git Bash find Python at all.
Close Git Bash and open a new window afterwards, so the new window picks up the change.

### 2. Download the code

Download and unzip the [transept code](https://github.com/ChrisMayfield/transept/archive/refs/heads/main.zip).
The zip unpacks into a folder called `transept-main`.
Put that folder somewhere you can find again, because your settings and keys will live inside the folder.

If you already use git and would rather clone:

```sh
git clone https://github.com/ChrisMayfield/transept.git
```

Either way, go to folder in your terminal, and stay there for everything that follows:

```sh
cd transept-main     # or cd transept, if you cloned
```

### 3. Create a virtual environment

A virtual environment is a private folder for the handful of packages Transept needs, so installing those packages cannot disturb anything else on the machine.
Create the environment once:

```sh
python3 -m venv .venv
```

Then switch the environment on:

```sh
source .venv/bin/activate        # Linux and macOS
source .venv/Scripts/activate    # Windows, in Git Bash
```

Your prompt gains a `(.venv)` at the front, which is how you know the environment is on.
From here on `python` means the Python inside that environment, on every operating system, which is why the commands below say `python` rather than `python3`.

Switching the environment on lasts only as long as that terminal window.
Every time you open a new window, `cd` back to this folder and run the `activate` line again before running any `python` command.

Exception: The `./transept` script under [Running a meeting](#running-a-meeting) finds the virtual environment on its own, and is the one command that does not need the `activate` line first.

### 4. Install the packages

```sh
pip install --upgrade pip
pip install -r requirements.txt
```

**Linux only:** the audio library also needs a system package.

```sh
sudo apt install libportaudio2
```

Now check that the software itself works:

```sh
python selftest.py
```

The self test needs no keys, no microphone, and no network, and should end by saying every check passed.
If a check fails, something went wrong above, and the cause is worth sorting out before adding keys and microphones to the picture.

### 5. Make your settings file

```sh
cp config.example.toml config.toml
```

Open `config.toml` in any plain text editor.
The file is a list of settings with an explanation above each one, and the only two you have to fill in are the API keys you are about to create.

One thing about the file is worth knowing now.
`config.toml` holds your API keys, so do not mail the file to anyone or commit the file to a public repository.
(Transept already excludes `config.toml` from git for that reason.)

While the file is open, find the `[languages]` section and set `available` to the languages your room actually needs:

```toml
[languages]
available = [
    "French",
    "Spanish",
    "Swahili",
]
```

Name the variety your speakers use, not just the language.
Standard Swahili and Congolese Swahili are not the same, and the model follows whichever name you write here.
A language on the list costs nothing until somebody opens the language.

Every other setting has a working default, so you can fill in the two keys below and leave the rest alone until you have run a meeting or two.

### 6. Get a speech recognition key

Transept sends the room's audio to [Deepgram](https://deepgram.com/), which turns the audio into English text.

1. Go to [console.deepgram.com](https://console.deepgram.com/) and create an account.
   New accounts come with $200 of credit and no payment details are required, which at roughly $0.50 an hour of audio is a great many meetings.
2. Find **API Keys** in the console and create one.
3. Copy the key immediately.
   The console shows a new key once and never again, though you can always delete a key and create another.
4. Paste the key into `config.toml`, between the quotes:

```toml
[keys]
deepgram_api_key = "paste-your-key-here"
```

### 7. Get a translation key

Transept sends the English text to a language model, which translates each sentence.
These instructions use [Google AI Studio](https://aistudio.google.com/), because most people already have a Google account and Google AI Studio is the quickest to set up.
If you already have a Claude or an OpenAI account, see the end of this step instead.

1. Go to [aistudio.google.com](https://aistudio.google.com/) and sign in with your Google account.
2. Create an API key.
3. Paste the key into `config.toml`, and leave `llm_base_url` alone, since the address already points at Google:

```toml
llm_base_url = "https://generativelanguage.googleapis.com/v1beta/openai"
llm_api_key = "paste-your-key-here"
```

4. Set up billing on the key's project and put about $10 of credit on the account.

That last step is not optional.
A free Google key is limited to a few requests per minute, and the faster models cut off after a couple of dozen requests a day.
A meeting sends a request every few seconds, so a free key stops translating within the first minute or two and readers spend the rest of the hour looking at English.
A paid key raises that ceiling far above anything a meeting will reach.
The $10 should last a while, because translation is relatively inexpensive and runs only for languages somebody is actually reading.

**If you use Claude or OpenAI instead:** create a key on [platform.claude.com](https://platform.claude.com/) or [platform.openai.com](https://platform.openai.com/), and change two settings in `config.toml`.
Set `llm_base_url` to the matching address from the list in the comments just above that setting, and set `model` under `[translation]` to a model your provider serves.
Any service with an OpenAI-compatible endpoint works, including a [LiteLLM](https://www.litellm.ai/) proxy or a local [Ollama](https://ollama.com/) install.

### 8. Check the audio

What the microphone hears decides whether the subtitles are any good, so do not skip this step.

First, see what the machine can listen to:

```sh
python server.py --list-devices
```

The result is a list of names:

```
  alsa_output.pci-0000_00_1f.3.analog-stereo.monitor  [playback, not a microphone]
  alsa_input.pci-0000_00_1f.3.analog-stereo.9
```

Anything marked **playback** captures what the computer is playing rather than what the microphone hears, which is almost never what you want.

Now listen through one of them.
Leave off `--device` to use whatever the system calls the default input, or paste one of the names above:

```sh
python pipeline.py --no-translate
python pipeline.py --no-translate --device "alsa_input.pci-0000_00_1f.3.analog-stereo.9"
```

Talk.
Your words should appear within a fraction of a second, each line tagged with how far behind real time the line arrived.
If nothing appears, the problem is the audio source rather than the software, so try another device from the list.
Press Ctrl-C to stop.

Once words are appearing, run the same command once more without `--no-translate`:

```sh
python pipeline.py
```

Each sentence is now followed by its translations.
The run costs a few cents and is the only check that proves both keys work before a room is depending on them.

The audio source is deliberately not a setting in `config.toml`.
A device name changes with a reboot or a replugged cable, so you pick the source on the operator page before each meeting instead.

### 9. Give phones an address

Phones need an HTTPS address that works whether they are on the building's wifi or on cellular, and Transept does not provide one by itself.
The usual solution is a tunnel, which is a small program on the laptop that publishes one port of the laptop at a fixed public address.

These instructions use [Tailscale](https://tailscale.com/), which needs no domain name and costs nothing.
If you own a domain that Cloudflare manages, Cloudflare Tunnel is a reasonable alternative and works the same way from Transept's point of view.

1. Go to [tailscale.com](https://tailscale.com/) and sign in, using a Google account or any of the other providers Tailscale offers.
   No payment details are needed.
2. Add your first device, which is the laptop you are setting up and the only device you need.
   Adding a device means downloading and installing Tailscale for your operating system, then signing in to link the laptop to your account.
   On macOS and Windows the app signs you in through a browser window.
   On Linux, run `sudo tailscale up` and open the link the command prints.
3. See what the machine is called:

```sh
tailscale status
```

The first line is this laptop, and the name shown there becomes the address your readers will use, something like `https://chapel.your-tailnet.ts.net`.
It is worth picking a name you would be happy to print on a card:

```sh
sudo tailscale set --hostname chapel
```

On macOS and Windows, drop the `sudo`, or rename the machine in the Tailscale admin console in your browser, which does the same thing.

4. **Linux only:** let your user account manage the tunnel, so you are not typing `sudo` every Sunday.

```sh
sudo tailscale set --operator=$USER
```

5. Open the tunnel on the reader port:

```sh
tailscale funnel 8080
```

8080 is the `port` setting in `config.toml`, so use whatever number is there if you have changed the port.

The first time you run the command, Tailscale does not have the permissions a funnel needs, so the command prints a link to the admin console.
Open the link, which is where you enable HTTPS certificates and grant this laptop the Funnel attribute, then run the same command again.
This time the command prints the public address, something like:

```
Available on the internet:

https://chapel.your-tailnet.ts.net/
```

Leave the funnel running for a moment and copy that address.
Started in the foreground this way, the funnel closes when you press Ctrl-C, which is what makes experimenting safe.

6. Put the address in `config.toml`, keeping the trailing slash:

```toml
[server]
public_url = "https://chapel.your-tailnet.ts.net/"
```

The QR code on the operator page is built from `public_url`, so a wrong or missing address there is a QR code that opens nothing.
Only the reader port goes through the tunnel.
The operator controls listen on a second port that never leaves the laptop, so nobody on the internet can control the server.

You are now set up. 🎉

## Running a meeting

One script starts the server and the tunnel together, and stops both:

```sh
./transept start      # the server, then the funnel
./transept status     # whether either is up, and the addresses
./transept stop       # the funnel, then the server
```

Run those from the Transept folder, in a terminal you have not had to activate anything in, since the script finds the virtual environment on its own.
If `./transept start` says permission denied, the zip download lost the file's executable bit, and `bash transept start` works just as well.

Starting prints two addresses, each carrying a token of its own:

```
Reader:   https://chapel.your-tailnet.ts.net/read?token=...
Operator: http://127.0.0.1:8081/operator?token=...
```

**The Operator address is for you**, on the laptop.
Open that address, pick the audio source from the list, and press Start.
That page also shows the reader link as a QR code people can point a camera at.
The tokens are new every run, so copy the address from the terminal each week rather than saving a bookmark.
(If the QR code does not appear, `public_url` in `config.toml` is empty or wrong; see step 9.)

**The Reader address is for everyone else.**
Hand the address out, or hold up the QR code.
Readers install nothing: they open the link, pick a language and a text size, and that is the whole experience.
The address stops working when the run ends, which is deliberate.
If your room reads from a printed card that has to keep working week after week, set `reader_token` under `[keys]` in `config.toml` to any long random string, and the address stays the same.

**Stop everything when the meeting ends.**
Speech recognition is billed by how long Deepgram listens, not by how much anyone says, so a session left running bills for an empty room.
A tunnel left open leaves the address answering all week.
Transept stops a session that has heard nothing for ten minutes as a backstop, but a watchdog is a safety net rather than a plan.

**Watch what you are spending**, especially for the first month, at [console.deepgram.com](https://console.deepgram.com/) and [aistudio.google.com](https://aistudio.google.com/).
Deepgram lets you set a spending limit in the console, which is worth doing.

## Optional tuning

Most defaults are fine, and none of the settings below are needed to run a meeting.
These are the settings in `config.toml` worth understanding once you have run a few meetings, listed in the order they appear in the file.

`endpointing` (400 milliseconds) is how much silence ends an utterance for the speech recognizer.
Lower is snappier but clips people who pause in the middle of a sentence.

`ceiling` (4 seconds) is how long a half-finished sentence waits before being translated anyway.
The operator page tags each sentence with why the sentence was sent.
A lot of `ceiling` means people are talking over each other, and raising `ceiling` trades a little delay for sentences that make sense.

`gap` (0.6 seconds) is the silence that separates one person's abandoned sentence from the next person's new one.
Lower `gap` if two unrelated remarks are being glued into one line.

`max_tokens` (2000) caps how long a single translation response may be.
Raise `max_tokens` if translations come out cut off mid-sentence.

`timeout` (15 seconds) is when a translation request is given up on, and `hold` (8 seconds) is how long a sentence waits for its translation before the English is shown instead.
A reader should always see something, so `hold` is the shorter of the two on purpose.

`grace` (90 seconds) is how long a language keeps running after its last reader closes it, so a phone locking its screen does not shut the language off.

`max_active` (0, meaning no limit) caps how many languages translate at once.
Anyone with the reader link can open any channel, and every open channel adds cost to every sentence, so set a cap if that prospect worries you.
A language over the cap shows English rather than nothing.

`idle_stop_minutes` (10) stops a session that has heard nothing for that long, because somebody will eventually forget to press Stop.

`max_readers` (100) caps how many phones can be connected at once, which is more than a room holds.
The cap is there because the reader link can travel further than the room, and every phone reading costs a connection the laptop has to write to on every line.

Most of these can also be changed for a single run without editing the file, for example `python server.py --ceiling 6`.
Run `python server.py --help` for the exact flag names, a few of which differ from the setting names.

## Troubleshooting

**Check the audio in the actual room, with the actual microphones.**
Recognition quality is decided almost entirely by what the microphone hears, and nothing downstream can repair a bad feed.
A line out from the sound board is far better than a laptop microphone on a table.
Take the laptop to the room, plug the laptop in the way it will be plugged in on Sunday, and run:

```sh
python pipeline.py --no-translate
```

Have somebody talk from where people actually sit.
That check is the single most valuable thing on this page, and takes ten minutes.

**Names and unusual words come out wrong: create `keyterms.txt`.**
`keyterms.txt` nudges the speech recognizer toward words the recognizer would otherwise mishear, such as the names of people who speak often, place names, and vocabulary particular to your congregation.

```sh
cp keyterms.example.txt keyterms.txt
```

One term per line, and keep the list to a few dozen, because a very long list dilutes the effect.

**Translations are wrong in a specific, repeated way: create `glossary.txt`.**
`glossary.txt` guides the translation model rather than the recognizer.
The glossary is for names, for terms that have an official published rendering in your target languages, and for set phrases a general model would translate too literally.

```sh
cp glossary.example.txt glossary.txt
```

The entries are free text, written as instructions, and the example file shows the shape.
By default the glossary also repairs recognition errors on the English channel, so a name the recognizer spelled wrong gets fixed for everyone.

Both files are much easier to write after a real meeting than before one.
If you turn on `record` under `[session]` in `config.toml`, Transept keeps each session and can hand you a worklist:

```sh
python record.py --list                                   # what has been kept
python record.py --session last --out review-sunday.md
```

That document leads with anything the correction introduced that was not in the audio, then the terms the recognizer missed, which are exactly the ones to add to `keyterms.txt`.
Recording keeps every word said in the room on the laptop's disk, unencrypted, so turning recording on is a decision to make with whoever leads the meeting.
Nothing is ever deleted unless you ask: `python record.py --purge --older-than 30`.

**Review a language before you offer it.**
You cannot judge a translation you cannot read, and neither can the readers depending on that language.
Run a real transcript through the offline reviewer and have a native speaker mark up the result:

```sh
python record.py --session last --plain transcript-sunday.txt   # if you record
python review.py --input transcript-sunday.txt --review review-french.md
```

Ask reviewers for wrong meaning first and awkward phrasing second.
Wrong meaning usually means a glossary entry is missing.
Awkward phrasing usually means a different model would serve better.
The same pairing is how to judge a change: export what the room actually said, edit the glossary, run the review again, and compare.

**The server will not start, or says the address is already in use.**
A server from a previous meeting is still holding the port, probably in a terminal window that has since been closed.
`./transept stop` clears the old server, and `./transept start` does the same before starting anything.
If the message names a port and `./transept status` says no server is running, something that is not Transept has the port, and the answer is to find that program rather than to restart anything.

**`./transept start` warns that `public_url` does not match.**
The address in `config.toml` is not the address the tunnel just published, so the QR code points somewhere that will not answer.
Copy the address from the warning into `public_url` as in step 9.

**Phones cannot reach the address.**
Check `./transept status`, or `tailscale funnel status`, to see whether the tunnel is actually open.
Remember that the address only works with the token on the end, so hand out the whole line or the QR code rather than just the hostname.

**Something is broken and you want to know whether Transept is the cause.**

```sh
python selftest.py            # everything, in a few seconds
python selftest.py server     # just the web server and its routes
```

The self test needs no keys, no microphone, and no network.
If every check passes and meetings still fail, the problem is the audio, the keys, or the network, in that order of likelihood.

What no self test can tell you is whether the subtitles are any good.
Subtitle quality is decided by the microphone feed, which only the room can tell you about, and by translation quality, which only a native speaker can confirm.
