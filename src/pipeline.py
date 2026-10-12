#!/usr/bin/env python3
"""
Live subtitling and translation, terminal edition.

Captures audio, streams it to Deepgram for speech recognition, buffers
finalized fragments into whole sentences, translates each sentence into every
target language concurrently, and prints the result in source order.

This is the same pipeline server.py runs, without the web layer. Use it to
check a microphone feed, tune segmentation, or measure translation latency,
because a terminal is a better place to see what is happening than a browser.

Usage, from inside local/ where config.toml is:
    python3 ../src/pipeline.py --model gemini-3.8-flash
    python3 ../src/pipeline.py --device <source> --model gemini-3.8-flash \\
        --languages "French,Congolese Swahili" \\
        --keyterms keyterms.txt --glossary glossary.txt --correct-english

Without --device it opens whatever the backend calls the default input, so
run --list-devices first if the wrong thing ends up being captured.

Output format:
    A dim line starting with a middle dot is a raw recognition fragment,
    printed as it arrives so the buffering is visible. A numbered block is a
    completed sentence with its translations, tagged with the reason the
    sentence closed: endpoint, punctuation, gap, or ceiling. A line marked
    en* is the transcript after glossary correction.

Ctrl-C to stop.
"""

import argparse
import array
import asyncio
import json
import math
import os
import re
import signal
import statistics
import sys
import time
import tomllib
from collections import deque
from urllib.parse import urlencode

try:
    import httpx
    import websockets
except ImportError:
    sys.exit("Missing dependencies: pip install -r requirements.txt")

import capture
from capture import CHANNELS, SAMPLE_RATE

# The keys, which live in the same file as every other setting so that
# switching translation providers is one edit rather than two. They are not
# rows in SETTINGS because SETTINGS becomes command line flags, and a key on
# a command line lands in the process list and in shell history.
# Attribute, key under [keys], and the environment variable that wins over
# it, which is always the key in capitals. The variable is spelled out rather
# than derived so that grepping for it finds this table.
SECRETS = [
    ("deepgram_key", "deepgram_api_key", "DEEPGRAM_API_KEY"),
    ("llm_base", "llm_base_url", "LLM_BASE_URL"),
    ("llm_key", "llm_api_key", "LLM_API_KEY"),
    ("reader_token", "reader_token", "READER_TOKEN"),
    ("operator_token", "operator_token", "OPERATOR_TOKEN"),
    ("control_token", "control_token", "CONTROL_TOKEN"),
]

# Fewest words a line may hold when it is split out of a longer passage.
# Shorter lines reach a phone sooner, but every line is a model call.
# Here rather than beside the segmenter because SETTINGS names it.
MIN_WORDS = 6

# Every other setting, in one place: attribute, config.toml section and key,
# type, and built-in default. Precedence is command line, then config.toml,
# then the default here. Adding a setting means adding one row. The order
# here follows config.example.toml, section for section and key for key, so
# that the two can be read side by side.
SETTINGS = [
    ("capture", "audio", "backend", str, "auto"),
    # Opus by default, because the leg that fails in a real building is
    # audio leaving it, and this is eight times less of it. A machine with
    # no ffmpeg falls back to PCM and says so.
    ("encoding", "audio", "encoding", str, "opus"),

    ("asr_model", "recognition", "model", str, "nova-3"),
    ("endpointing", "recognition", "endpointing", int, 400),
    # On by default, because a pause long enough for endpointing is rare in
    # a room: on the second field test most fragments ended on the
    # recognizer's own clock, four seconds or more after their first word.
    ("early_finalize", "recognition", "early_finalize", bool, True),

    ("ceiling", "segmentation", "ceiling", float, 5.0),
    ("gap", "segmentation", "gap", float, 0.6),
    ("min_words", "segmentation", "min_words", int, MIN_WORDS),

    ("model", "translation", "model", str, None),
    ("reasoning_effort", "translation", "reasoning_effort", str, None),
    ("correct_english", "translation", "correct_english", bool, False),
    ("max_tokens", "translation", "max_tokens", int, 2000),
    ("timeout", "translation", "timeout", float, 15.0),
    ("hold", "translation", "hold", float, 8.0),

    ("languages", "languages", "available", list, ["French", "Swahili"]),
    ("grace", "languages", "grace", float, 90.0),
    ("max_languages", "languages", "max_active", int, 0),

    ("glossary", "files", "glossary", str, "glossary.txt"),
    ("keyterms", "files", "keyterms", str, "keyterms.txt"),

    ("idle_stop", "session", "idle_stop_minutes", float, 10.0),
    # Off by default. Recording a meeting is a decision a congregation makes,
    # not something that should happen because nobody set anything.
    ("record", "session", "record", bool, False),
    ("database", "session", "database", str, "sessions.db"),

    # Loopback by default: opening a service to a shared network should be
    # a deliberate edit, not what happens when nobody sets anything.
    ("host", "server", "host", str, "127.0.0.1"),
    ("reader_port", "server", "reader_port", int, 8080),
    ("operator_port", "server", "operator_port", int, 8081),
    # The same number as operator_port, which costs nothing because the
    # two are never built together: a server captures from a local device
    # or from a sender, and which listener it raises follows from that.
    ("control_port", "server", "control_port", int, 8081),
    # Seconds a session outlives its sender's socket. Long enough for a
    # laptop's wifi to come back, short enough that a sender that went
    # home does not leave a meeting running.
    ("control_grace", "server", "control_grace", float, 30.0),
    ("max_readers", "server", "max_readers", int, 100),
    ("public_url", "server", "public_url", str, ""),
    # Beside public_url because the two compose into a room's addresses
    # once several rooms share one server: the one a phone opens and the
    # one a sender dials. Empty for a room alone at its hostname.
    ("room", "server", "room", str, ""),
]


def load_config(*paths):
    """Read one or more config files, merging them left to right.

    Several files are what lets a hosted server keep the tuning every room
    shares in one place and give each room a file holding only what that
    room has to say for itself: its ports, its tokens, and its name. The
    later file wins key by key rather than section by section, so a room
    naming reader_port does not lose the shared [server] host beside it.

    A missing file is not an error, because the usual run passes one name
    that may or may not exist yet and should fall through to the defaults.
    """
    merged = {}
    for path in paths or ("config.toml",):
        try:
            with open(path, "rb") as handle:
                loaded = tomllib.load(handle)
        except FileNotFoundError:
            continue
        except tomllib.TOMLDecodeError as exc:
            sys.exit(f"{path} is not valid TOML: {exc}")
        # A path under [files] means beside the file that names it. Read
        # against the working directory instead, the glossary a hosted
        # server's shared file names is looked for in each room's own
        # directory, is not there, and loads as nothing without a word.
        # Only [files]: sessions.db belongs to the room that writes it.
        base = os.path.dirname(path)
        files = loaded.get("files")
        if base and isinstance(files, dict):
            for key, value in files.items():
                if isinstance(value, str) and value:
                    files[key] = os.path.join(base, value)
        for section, values in loaded.items():
            if isinstance(values, dict) and isinstance(
                    merged.get(section), dict):
                merged[section].update(values)
            else:
                merged[section] = values
    return merged


def resolve(args, config):
    """Merge command line over config file over defaults.

    Every argparse default is None so that "not given" is distinguishable
    from "given the same value as the default", which is what makes the
    three-layer precedence work.
    """
    settings = {}
    for attr, section, key, kind, default in SETTINGS:
        value = getattr(args, attr, None)
        if value is None:
            value = config.get(section, {}).get(key)
        if value is None:
            value = default
        if kind is list and isinstance(value, str):
            # Command line gives a comma-separated string; TOML gives a list.
            value = [item.strip() for item in value.split(",") if item.strip()]
        settings[attr] = value
    return settings


def load_keys(config):
    """Read the [keys] section of config.toml, letting the environment win.

    A real environment variable overrides the file, so a systemd unit, a
    container, or one shell export can supply a key without editing the
    file, and so selftest can run a server on keys of its own. An empty
    variable counts as set, which is how a run asks for a freshly minted
    reader or operator token on a machine whose config.toml pins one.
    """
    section = config.get("keys", {})
    keys = {}
    for attr, name, variable in SECRETS:
        value = os.environ.get(variable)
        if value is None:
            value = section.get(name, "")
        keys[attr] = str(value).strip()
    return keys


CONTEXT_UNITS = 3
TERMINAL_PUNCTUATION = re.compile(r"[.!?]['\"]?$")
# A sentence boundary inside a buffer: terminal punctuation, an optional
# closing quote, space, and then a capital or a digit.
SENTENCE_BREAK = re.compile(
    r"(?:(?<=[.!?])|(?<=[.!?]['\"]))\s+(?=['\"]?[A-Z0-9])")
ABBREVIATIONS = {"Mr.", "Mrs.", "Ms.", "Dr.", "St.", "Jr.", "Sr.", "vs.",
                 "Bro.", "Sis.", "e.g.", "i.e."}

DIM = "\033[2m"
RESET = "\033[0m"

SYSTEM_PROMPT = """\
You are producing live subtitles for a Sunday school class. The source text \
comes from automatic speech recognition of people speaking aloud, so it may \
contain recognition errors, false starts, and incomplete sentences.

Return only a JSON object whose keys are exactly the output names listed in \
the request. No preamble, no markdown fences, no commentary.

These rules apply to every output without exception:

1. Never introduce a proper noun, a personal name, a place, a number, a date, \
or a scripture reference that is not present in the source. This is absolute. \
A reader of a translated channel cannot hear the room and has no way to catch \
an invented name, so a confident wrong name is worse than visible confusion.
2. When a garbled span looks like a name you cannot identify, carry the \
English words through unchanged rather than guessing at them.
3. Work on the CURRENT LINE only. The preceding lines are context for \
pronouns and continuity; do not translate them or repeat their content.
4. Preserve sentence boundaries. A reader sees one line at a time on a phone, \
so do not merge or split sentences.
5. A line may be the back half of a sentence the recognizer cut early. If it \
continues the previous line, render it so it reads as a natural continuation, \
and do not repeat what was already covered.

The English output is a correction of the transcript, not a rewrite. Start \
from the source text and change only what is clearly a recognition error: \
names and terms that appear in the glossary, and obvious mis-hearings of \
common words. A correction may introduce a name only if that name is in the \
glossary. Never turn a common word into a name, and never supply a name, a \
title, or the rest of a quotation from your own knowledge, however confident \
you are; leave the words as heard instead. Keep false starts, repetitions, \
filler words, and informal \
grammar exactly as transcribed, because for a reader who cannot hear well \
this line is the record of what was said. If nothing needs correcting, return \
the source text unchanged.

Every other output is a translation. A faithful translation has to be fluent \
in its own language, which means supplying grammatical elements that English \
left implicit: subjects, verbs, case, gender, agreement. That latitude covers \
grammar and phrasing only and never extends to content. Keep the spoken \
register of a conversational class rather than formal writing. Keep proper \
names in their original form unless the target language has a well \
established equivalent. Render scripture references using the conventional \
book names and formatting of the target language.\
"""


class Unit:
    """One sentence-ish span of English, ready to translate."""

    def __init__(self, seq, text, audio_end, reason, confidence=None):
        self.seq = seq
        self.text = text
        self.audio_end = audio_end
        self.reason = reason
        # Lowest recognizer confidence among the fragments that formed this
        # sentence. The shakiest lines are where a mis-heard name lives, so
        # this is what points at the next entry for keyterms.txt.
        self.confidence = confidence


class Segmenter:
    """Accumulates finalized ASR fragments into whole sentences.

    Deepgram finalizes text before the speaker finishes a sentence, so
    translating each result in isolation produces fragments like "on the
    heater" with no subject. Fragments are held until a silent gap opens,
    the recognizer reports an endpoint, the text ends in terminal
    punctuation, or the ceiling expires. A sentence that finishes inside
    the buffer goes out as soon as it arrives, and the unfinished rest
    stays.

    The gap check is what stops an abandoned half-sentence from capturing
    whatever the next person says. Without it, "you all can see if it" waits
    in the buffer and gets glued onto the following turn.
    """

    def __init__(self, ceiling_seconds, gap_seconds, start_seq=0,
                 min_words=MIN_WORDS):
        self.ceiling = ceiling_seconds
        self.gap = gap_seconds
        self.min_words = min_words
        self.parts = []
        self.first_seen = None
        self.audio_end = 0.0
        self.confidence = None
        # Counts on from where the last stream left off. A reconnect builds a
        # new segmenter, and a sequence number that restarted at 1 would make
        # the Hub and the reader page revise the opening lines of the meeting
        # instead of publishing the new ones.
        self.seq = start_seq

    def held_words(self):
        """Words in the buffer, waiting for the rest of their sentence."""
        return sum(len(part.split()) for part in self.parts)

    def add(self, text, speech_final, start, audio_end, confidence=None):
        """Feed one finalized fragment. Returns a list of units to emit.

        Two units can come out of one fragment: the gap check may close the
        buffered sentence before the new fragment opens the next one.
        """
        emits = []
        if self.parts and (start - self.audio_end) > self.gap:
            # Before the new fragment is folded in, so the sentence that
            # just closed keeps its own confidence rather than this one's.
            emits.extend(self._take("gap"))

        if self.first_seen is None:
            self.first_seen = time.monotonic()
        self.parts.append(text)
        self.audio_end = audio_end
        if confidence is not None:
            self.confidence = (confidence if self.confidence is None
                               else min(self.confidence, confidence))

        joined = " ".join(self.parts).strip()
        if speech_final or TERMINAL_PUNCTUATION.search(joined):
            emits.extend(
                self._take("endpoint" if speech_final else "punctuation"))
        else:
            # The fragment ended mid-sentence, but a sentence may have
            # finished inside it. Held until the rest arrived, that
            # sentence waited out the whole next phrase, which is how
            # lines covering thirteen seconds of speech reached phones.
            emits.extend(self._split())
        return emits

    def check_ceiling(self):
        """Units to emit if the buffer has been held too long.

        Finished sentences have already gone out as they arrived, so the
        cut falls at the last comma, and the rest waits for its own ending.
        Cutting wherever the buffer happened to stop left lines ending on
        "and" or "the", and a language that puts its verb last cannot
        translate the front half of a clause without guessing at the back.
        """
        if not self.parts or self.first_seen is None:
            return []
        if time.monotonic() - self.first_seen < self.ceiling:
            return []
        return self._split_clause() or self._take("ceiling")

    def drain(self):
        return self._take("drain")

    def _split(self):
        """Emit the finished sentences in the buffer, keep the unfinished.

        Nothing is split off shorter than min_words, so "Right?" or
        "Yeah." stays with what follows instead of costing a model call of
        its own.
        """
        pieces = sentences(" ".join(self.parts).strip())
        if len(pieces) < 2:
            return []
        lines, leftover = group(pieces[:-1], self.min_words)
        if not lines:
            return []
        self._keep(" ".join(filter(None, [leftover, pieces[-1]])))
        return [self._unit(line, "sentence") for line in lines]

    def _split_clause(self):
        """Emit up to the last comma, when there is enough before it."""
        text = " ".join(self.parts).strip()
        cut = text.rfind(", ")
        if cut < 0 or len(text[:cut].split()) < self.min_words:
            return []
        self._keep(text[cut + 2:])
        return [self._unit(text[:cut + 1], "ceiling")]

    def _keep(self, text):
        """Leave only this in the buffer, waiting a fresh ceiling.

        The confidence stays, because the fragment the text came from is
        still the shakiest thing known about it.
        """
        self.parts = [text]
        self.first_seen = time.monotonic()

    def _take(self, reason):
        """Close the buffer and return its units, one per sentence.

        The same min_words rule as a split, and a short last sentence joins
        the line before it rather than standing alone.
        """
        text = " ".join(self.parts).strip()
        self.parts = []
        self.first_seen = None
        if not text:
            self.confidence = None
            return []
        lines, leftover = group(sentences(text), self.min_words)
        if leftover:
            if lines:
                lines[-1] += " " + leftover
            else:
                lines.append(leftover)
        units = [self._unit(line, reason) for line in lines]
        self.confidence = None
        return units

    def _unit(self, text, reason):
        """One unit, numbered as it is made.

        The sequence number travels with the text rather than being read off
        the segmenter afterwards. One fragment can close several sentences,
        and all of them are built before any is emitted, so a caller reading
        self.seq later would stamp every one with the last number.
        """
        self.seq += 1
        return self.seq, text, reason, self.audio_end, self.confidence


def sentences(text):
    """Split text after terminal punctuation followed by a capital or digit.

    A capital is required because the recognizer capitalizes the sentences
    it punctuates, and an abbreviation such as "Dr." is rejoined, since the
    name after it is not the start of anything.
    """
    if not text:
        return []
    pieces = []
    for piece in SENTENCE_BREAK.split(text):
        if pieces and pieces[-1].split()[-1] in ABBREVIATIONS:
            pieces[-1] += " " + piece
        else:
            pieces.append(piece)
    return pieces


def group(pieces, min_words):
    """(lines, leftover): sentences joined until each holds min_words."""
    lines, current, words = [], [], 0
    for piece in pieces:
        current.append(piece)
        words += len(piece.split())
        if words >= min_words:
            lines.append(" ".join(current))
            current, words = [], 0
    return lines, " ".join(current)


class Translator:
    """Concurrent translation calls over one pooled HTTP client."""

    def __init__(self, base_url, api_key, model, languages, glossary,
                 max_tokens, timeout, reasoning_effort=None,
                 correct_english=False, max_attempts=2, retry_delay=0.4):
        self.url = base_url.rstrip("/") + "/chat/completions"
        self.model = model
        self.languages = languages
        # English first when enabled, so the model settles what was actually
        # said before deciding how to render it in another language.
        self.outputs = (["English"] if correct_english else []) + languages
        self.glossary = glossary
        self.max_tokens = max_tokens
        self.reasoning_effort = reasoning_effort
        # Live subtitling gets one fast retry, because a class cannot wait out
        # a long backoff and the caller falls back to English. Offline review
        # raises this, where waiting beats losing the line.
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay
        self.client = httpx.AsyncClient(
            timeout=timeout,
            headers={"Authorization": f"Bearer {api_key}"},
            limits=httpx.Limits(max_keepalive_connections=8,
                                max_connections=16),
        )

    async def close(self):
        await self.client.aclose()

    def _messages(self, text, context, outputs):
        context_block = "\n".join(context) if context else "(start of session)"
        return [
            {"role": "system", "content": SYSTEM_PROMPT + self.glossary},
            {"role": "user", "content": (
                f"Outputs: {', '.join(outputs)}\n\n"
                f"PRECEDING LINES (context only):\n{context_block}\n\n"
                f"CURRENT LINE:\n{text}"
            )},
        ]

    async def translate(self, text, context, outputs=None):
        """Returns (translations, elapsed). Raises on unrecoverable failure.

        Pass outputs to request a subset, which is how the server avoids
        paying for a language nobody is currently reading.
        """
        # An explicit None check, not a truthiness test: an empty list means
        # "nothing to do" and must never silently expand to every output.
        outputs = self.outputs if outputs is None else outputs
        started = time.monotonic()
        body = {
            "model": self.model,
            "messages": self._messages(text, context, outputs),
            "max_tokens": self.max_tokens,
        }
        # No temperature, top_p, or top_k: Gemini deprecated custom sampling
        # and will reject it, and the model's own defaults are tuned for it.
        if self.reasoning_effort:
            # Translation is mechanical and does not benefit from thinking.
            # Gemini 3 cannot disable it outright, but "low" sets a low
            # thinking_level. This is the portable spelling: a Google-only
            # thinking_config would break other providers, and Gemini refuses
            # both at once. Thinking tokens count against max_tokens, which
            # is what truncated responses earlier.
            body["reasoning_effort"] = self.reasoning_effort
        last_error = None
        for attempt in range(self.max_attempts):
            try:
                response = await self.client.post(self.url, json=body)
                if response.status_code != 200:
                    last_error = f"HTTP {response.status_code}"
                    if response.status_code not in (408, 429, 500, 502, 503,
                                                    504):
                        break
                else:
                    reply = response.json()
                    choice = reply["choices"][0]
                    if choice.get("finish_reason") == "length":
                        raise ValueError("truncated: raise --max-tokens")
                    parsed = Translations(parse_translations(
                        choice["message"]["content"], outputs))
                    parsed.usage = usage_of(reply)
                    return parsed, time.monotonic() - started
            # IndexError covers an empty choices array, which is what a
            # filtered or truncated response looks like. Letting it escape
            # kills the publisher task and, in server.py, the session.
            except (httpx.HTTPError, KeyError, IndexError, ValueError,
                    json.JSONDecodeError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            if attempt + 1 < self.max_attempts:
                await asyncio.sleep(self.retry_delay * (2 ** attempt))
        raise RuntimeError(last_error or "unknown translation failure")


class Translations(dict):
    """Output name to text, carrying what the call cost alongside.

    A dict still, so every caller that reads a language by name is
    unchanged, and the token counts ride with the answer they paid for
    rather than on the Translator, where concurrent calls would overwrite
    each other's.
    """

    usage = None


def usage_of(reply):
    """Token counts from a chat completion, or None if it sent none.

    Thinking tokens are named where the provider names them, and otherwise
    are whatever the total holds beyond the prompt and the answer, which is
    how Gemini's compatible endpoint reports them.
    """
    usage = reply.get("usage")
    if not isinstance(usage, dict):
        return None

    def count(value):
        return value if isinstance(value, int) else 0

    prompt = count(usage.get("prompt_tokens"))
    output = count(usage.get("completion_tokens"))
    details = usage.get("completion_tokens_details") or {}
    reasoning = count(details.get("reasoning_tokens"))
    if not reasoning:
        reasoning = max(0, count(usage.get("total_tokens")) - prompt - output)
    cached = count((usage.get("prompt_tokens_details") or {})
                   .get("cached_tokens"))
    return {"prompt_tokens": prompt, "output_tokens": output,
            "reasoning_tokens": reasoning, "cached_tokens": cached}


def parse_translations(text, languages):
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("```")[1]
        if cleaned.startswith("json"):
            cleaned = cleaned[4:]
    start, end = cleaned.find("{"), cleaned.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"no JSON object in response: {text[:120]}")
    parsed = json.loads(cleaned[start:end + 1])
    return {lang: str(parsed.get(lang, "")).strip() for lang in languages}


def load_file_lines(path):
    """Read a plain list file, ignoring blanks and comments.

    A missing file returns nothing rather than raising, so glossary.txt and
    keyterms.txt can be picked up automatically when present and simply
    skipped when they are not.
    """
    if not path:
        return []
    try:
        with open(path, encoding="utf-8") as handle:
            return [line.strip() for line in handle
                    if line.strip() and not line.startswith("#")]
    except OSError:
        return []


def describe_file(name, path, lines):
    """One startup line saying what a [files] entry actually loaded.

    load_file_lines is quiet about a missing file on purpose, so this line
    is the only place a run says it has no glossary, and a meeting run
    without one looks like a meeting run with one until a name comes out
    wrong.
    """
    if not path:
        return f"{name}: none configured"
    where = os.path.abspath(path)
    if lines:
        return f"{name}: {len(lines)} lines from {where}"
    if os.path.exists(where):
        return f"{name}: nothing usable in {where}"
    return f"{name}: not found at {where}"


def glossary_prompt(terms):
    """The glossary's lines as the block appended to SYSTEM_PROMPT."""
    if not terms:
        return ""
    return ("\n\nGlossary of names and terms used in this congregation. "
            "Follow it exactly:\n\n" + "\n".join(terms))


def build_asr_url(settings, keyterms, encoding="pcm"):
    """The recognizer socket for one run, declaring what will go up it.

    The format named here has to be the one actually being sent, which is
    whatever the capture object says it yields: linear16 from a local
    device, and whatever a sender declared over the control socket. Opus
    arrives inside a container that names its own rate and channel count,
    which Deepgram reads for itself, so that case declares nothing rather
    than repeating it in a second place that could disagree.
    """
    params = [
        ("model", settings["asr_model"]),
        ("language", "en"),
    ]
    if encoding != "opus":
        params += [
            ("encoding", "linear16"),
            ("sample_rate", str(SAMPLE_RATE)),
            ("channels", str(CHANNELS)),
        ]
    params += [
        # Guesses only when they are used to finish early, since nothing
        # else reads them.
        ("interim_results",
         "true" if settings.get("early_finalize") else "false"),
        ("smart_format", "true"),
        ("punctuate", "true"),
        ("endpointing", str(settings["endpointing"])),
    ]
    params.extend(("keyterm", term) for term in keyterms)
    return "wss://api.deepgram.com/v1/listen?" + urlencode(params)


# -- compression, between the microphone and the socket ----------------------
#
# Raw PCM upstream is 256 kbps, and Opus carries 16 kHz mono speech at
# roughly an eighth of that with no loss the recognizer can hear. The
# saving is worth having wherever the audio leaves the building, which is
# the default deployment as much as the hosted one, because a congested
# uplink is what a whole on-site beta failed on.
#
# The encoder sits here rather than in capture.py, so every backend keeps
# the promise it makes: 16 kHz mono signed 16-bit chunks, whatever produced
# them. What a wrapped source changes is the one thing downstream reads,
# which is what it says it yields, and build_asr_url declares that.

# What ffmpeg is told to produce. -analyzeduration and -probesize are not
# tuning: without them ffmpeg reads two seconds of a raw stream before it
# decides what the stream is, and every sentence arrives two seconds late.
# -page_duration is the same problem at the other end, where the ogg muxer
# holds a full second of audio per page by default.
ENCODER_COMMAND = [
    "ffmpeg", "-hide_banner", "-loglevel", "error",
    "-analyzeduration", "0", "-probesize", "32",
    "-f", "s16le", "-ar", str(SAMPLE_RATE), "-ac", str(CHANNELS),
    "-i", "pipe:0",
    "-c:a", "libopus", "-b:a", "24k", "-application", "voip",
    "-f", "ogg", "-page_duration", "20000", "-flush_packets", "1",
    "pipe:1",
]
# Seconds to let the encoder prove it started. An ffmpeg built without
# libopus exits during setup rather than on the first chunk, so the
# fallback is decided here and not several sentences into a meeting.
ENCODER_SETTLE = 0.3
# Bytes to take off the encoder at a time. A ceiling, not a wait: the read
# returns whatever has arrived, which at this bitrate is one small page.
ENCODED_READ = 4096
# Pages at the front of an Ogg stream that say what the stream is: OpusHead,
# which carries the sample rate and channel count, and OpusTags.
OGG_HEADER_PAGES = 2
# Fixed part of an Ogg page header, before the segment table that follows it.
OGG_PAGE_HEADER = 27


def ogg_page_end(buffer, start):
    """Where the Ogg page beginning at start ends, or None if it is not
    all here yet.

    A page is the fixed header, a segment table whose length the header
    gives, and a payload the table adds up to. A socket or a pipe splits a
    stream wherever it likes, so the page has to be measured, not assumed.
    """
    table = start + OGG_PAGE_HEADER
    if len(buffer) < table:
        return None
    segments = buffer[table - 1]
    if len(buffer) < table + segments:
        return None
    end = table + segments + sum(buffer[table:table + segments])
    return end if len(buffer) >= end else None


class OggHeaders:
    """The pages at the front of an Ogg stream, kept for a later listener.

    Opus does not travel as bare frames. Its first two pages say what the
    stream is, and everything after them is undecodable without them: a
    recognizer handed the stream from the middle accepts the socket, holds
    it open, and returns nothing at all, which in a room looks exactly like
    nobody speaking.

    Whoever needs them most is never listening when they go past. The
    encoder starts with the microphone, before there is a socket to send
    up, and a recognizer run starts later still, whenever a session does.
    So they are kept here and put in front of each new reader rather than
    left to whoever happened to be connected when ffmpeg started.
    """

    def __init__(self):
        self.buffer = b""
        self.headers = b""

    def feed(self, chunk):
        """One chunk of the stream, which costs nothing once it has them."""
        if self.headers or not chunk:
            return
        self.buffer += chunk
        start = self.buffer.find(b"OggS")
        if start < 0:
            # Only enough to catch a capture pattern split across two
            # chunks, so a stream that is not Ogg at all cannot grow this
            # without bound.
            self.buffer = self.buffer[-3:]
            return
        at = start
        for _ in range(OGG_HEADER_PAGES):
            at = ogg_page_end(self.buffer, at)
            if at is None:
                return
        self.headers = self.buffer[start:at]
        self.buffer = b""

    def reset(self):
        """A new encoder is a new stream, and its pages are not these."""
        self.buffer = b""
        self.headers = b""


async def open_encoder(source, encoding):
    """(source, note): wrap a PCM source in Opus, or hand it back as it is.

    ffmpeg is a system binary rather than a line in requirements.txt, so an
    encoder that is missing or will not start is a fallback to PCM and a
    sentence about it, the way sounddevice and segno are already handled.
    Deciding here, before the recognizer socket is opened, is what makes
    the fallback safe: a stream that changed format halfway through would
    have no way to say so, since the format is named when the socket opens.
    """
    if encoding != "opus" or source.encoding != "pcm":
        # A source already encoded goes up as it is: a second codec pass
        # would cost latency to save nothing.
        return source, ""
    try:
        process = await asyncio.create_subprocess_exec(
            *ENCODER_COMMAND,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            # Discarded for the reason parec's is: nothing drains this pipe,
            # and enough diagnostics fill it and block the encoder mid write,
            # which stops the audio with no error and no end of stream.
            stderr=asyncio.subprocess.DEVNULL)
    except OSError as exc:
        return source, (f"No Opus encoder, so the audio goes up uncompressed: "
                        f"{exc}. Install ffmpeg, or set [audio] encoding to "
                        f"pcm to stop asking.")
    await asyncio.sleep(ENCODER_SETTLE)
    if process.returncode is not None:
        return source, ("The Opus encoder exited immediately, so the audio "
                        "goes up uncompressed. This ffmpeg was probably "
                        "built without libopus.")
    return Encoder(source, process), ""


class Encoder:
    """A capture source with Opus between it and the socket.

    Nothing decodes it again. The recognizer reads Opus as happily as PCM,
    and the only thing downstream of capture that touches audio bytes is
    pump_audio, which forwards them; the segmenter and the translator see
    text.
    """

    backend = "encoder"
    encoding = "opus"

    def __init__(self, source, process):
        self.source = source
        self.process = process
        # Noted on the way past rather than held back, because the socket
        # this feeds may be the recognizer's own, and that one is listening
        # from the first byte. What needs them kept is a sender, whose
        # socket does not exist yet when ffmpeg writes them.
        self.headers = b""
        self.pages = OggHeaders()
        # The capture clock of the device underneath, which is where lag is
        # measured from: this side of ffmpeg a byte is not a duration.
        self.clock = getattr(source, "clock", None)
        # And its level meter, for the same reason: loudness is a property
        # of samples, and past ffmpeg there are none.
        self.meter = getattr(source, "meter", None)
        self.feeding = asyncio.create_task(self._feed())

    async def _feed(self):
        """Captured audio into the encoder, until the device stops."""
        try:
            while True:
                chunk = await self.source.read()
                if chunk is None:
                    # A gap, which only a remote source produces, and a
                    # remote source is never wrapped. Dropped rather than
                    # padded: silence is billed as audio.
                    continue
                if not chunk:
                    break
                self.process.stdin.write(chunk)
                await self.process.stdin.drain()
        except (OSError, ValueError):
            # The encoder died under us. Closing stdin below ends the read
            # side too, which is how the caller sees end of stream.
            pass
        finally:
            try:
                self.process.stdin.close()
            except (OSError, ValueError):
                pass

    async def read(self):
        """One encoded chunk, or empty bytes when the stream ends."""
        chunk = await self.process.stdout.read(ENCODED_READ)
        self.pages.feed(chunk)
        self.headers = self.pages.headers
        return chunk

    async def close(self):
        self.feeding.cancel()
        await asyncio.gather(self.feeding, return_exceptions=True)
        await self.source.close()
        try:
            self.process.terminate()
        except ProcessLookupError:
            # Ctrl-C reaches ffmpeg too, since it shares this process group.
            pass
        await self.process.wait()


# -- when the words were spoken ----------------------------------------------
#
# The recognizer stamps every result with seconds into the stream it was
# sent, and lag is how long ago those seconds were captured. Reading that
# off the time the socket opened was wrong twice over: the microphone opens
# first and its audio queues until the socket is ready, so the stream starts
# before the clock does and a line that arrived promptly measured negative;
# and a microphone that stops delivering, as one muted by a locked screen
# does, stops the stream while the clock runs on, so every line after it
# measured late by the length of the silence for the rest of the run.

BYTES_PER_SECOND = SAMPLE_RATE * CHANNELS * capture.BYTES_PER_SAMPLE
# Marks kept, one per chunk read, which at a tenth of a second each is ten
# minutes. A line is looked up seconds after its words, so this is margin.
CLOCK_MARKS = 6000


class StreamClock:
    """When each stretch of the stream was captured.

    Advanced by the capture wrapper as audio is read off the device, so a
    silence the device never delivered is simply absent from the stream,
    exactly as it is absent from what the recognizer counts.
    """

    def __init__(self):
        self.position = 0.0
        self.marks = deque(maxlen=CLOCK_MARKS)
        self.opened = time.monotonic()

    def silent_for(self, now=None):
        """Seconds since the device last delivered any audio at all.

        Not a quiet room, which still delivers audio: a microphone that has
        stopped, the way one muted by a locked screen does.
        """
        now = time.monotonic() if now is None else now
        last = self.marks[-1][1] if self.marks else self.opened
        return max(0.0, now - last)

    def advance(self, seconds, now=None):
        """Note that the stream grew by this much, read just now."""
        self.position += seconds
        self.marks.append((self.position,
                           time.monotonic() if now is None else now))

    def captured_at(self, position):
        """The monotonic time the audio at this stream position was read.

        None for a position older than the marks kept, which a line an
        hour behind would be, and which is better unmeasured than guessed.
        """
        found = None
        for end, read_at in reversed(self.marks):
            if end < position:
                break
            found = (end, read_at)
        else:
            if len(self.marks) == self.marks.maxlen:
                # The chunk holding this position has already been let go.
                return None
        if found is None:
            if self.marks and position > self.marks[-1][0]:
                # Past the end, by rounding in the recognizer's timestamps.
                return self.marks[-1][1]
            return None
        end, read_at = found
        # A chunk is read when its last sample arrives, so audio earlier in
        # the chunk was captured that much before the read.
        return read_at - (end - position)

    def lag(self, position, now=None):
        """Seconds since the audio at this position was captured, or None."""
        captured = self.captured_at(position)
        if captured is None:
            return None
        now = time.monotonic() if now is None else now
        return max(0.0, now - captured)


class Clocked:
    """A capture source that keeps a StreamClock and a LevelMeter as it is
    read. Everything else a source is asked, its backend and encoding among
    them, is the wrapped source's own answer.
    """

    def __init__(self, source):
        self.source = source
        self.clock = StreamClock()
        self.meter = LevelMeter()

    def __getattr__(self, name):
        return getattr(self.source, name)

    async def read(self):
        chunk = await self.source.read()
        if chunk:
            seconds = self.measure(chunk)
            if seconds:
                self.clock.advance(seconds)
        return chunk

    async def close(self):
        await self.source.close()


class Stamped(Clocked):
    """A local device, clocked and metered from its own samples.

    It goes directly around the device, before any encoder, because that is
    where a sample's arrival is closest to its being spoken, and PCM is
    where a byte count is a duration.
    """

    def measure(self, chunk):
        self.meter.add(chunk)
        return len(chunk) / BYTES_PER_SECOND


# Samples per second an Ogg Opus granule position counts, whatever rate the
# audio was captured at: the format fixes it.
OPUS_GRANULE_RATE = 48000


class OggGranules:
    """Seconds of Opus audio in a stream read piece by piece.

    Every Ogg page ends with a granule position, the count of samples
    decoded so far, so the difference between two pages is how much audio
    the later one carried.
    """

    def __init__(self):
        self.buffer = b""
        self.last = None

    def feed(self, chunk):
        """Seconds of audio completed by this chunk."""
        self.buffer += chunk
        seconds = 0.0
        while True:
            start = self.buffer.find(b"OggS")
            if start < 0:
                # Kept short, so a stream that is not Ogg cannot grow it.
                self.buffer = self.buffer[-3:]
                return seconds
            end = ogg_page_end(self.buffer, start)
            if end is None:
                self.buffer = self.buffer[start:]
                return seconds
            granule = int.from_bytes(self.buffer[start + 6:start + 14],
                                     "little", signed=True)
            # Zero is a header page, and -1 a page that finishes no
            # packet; neither says anything about how much was heard.
            if granule > 0:
                if self.last is not None and granule > self.last:
                    seconds += (granule - self.last) / OPUS_GRANULE_RATE
                # A smaller one is a new encoder starting again from zero,
                # which counts from here rather than backwards.
                self.last = granule
            self.buffer = self.buffer[end:]


class Received(Clocked):
    """A sender's audio, clocked as it reaches this server.

    The audio was captured on a laptop whose clock this machine cannot
    read, but the moment it reaches this server is still a mark the
    recognizer's timestamps can be measured against, and it is the only
    one that tells a gap in what arrived from a gap in what was said.

    The level meter is filled by the room from what the sender reports,
    since the samples it was measured on never left the laptop.
    """

    def __init__(self, source):
        super().__init__(source)
        self.pages = OggGranules() if source.encoding == "opus" else None

    def measure(self, chunk):
        if self.pages:
            return self.pages.feed(chunk)
        return len(chunk) / BYTES_PER_SECOND


# Quieter than this is reported as this, since the log of silence is
# minus infinity and sixteen bits cannot say anything quieter anyway.
LEVEL_FLOOR = -96.0
FULL_SCALE = 32768.0
# Seconds of audio in the reading the operator page shows, and how old
# that reading may be before it says nothing. The page asks every two
# seconds, so a shorter window would show whichever moment it landed on.
LIVE_WINDOW = 1.0
LIVE_STALE = 3.0


def decibels(value):
    """A fraction of full scale as dBFS, floored."""
    if value <= 0:
        return LEVEL_FLOOR
    return max(LEVEL_FLOOR, round(20 * math.log10(value), 1))


class LevelMeter:
    """How loud the audio was, gathered until somebody asks.

    A field test lost stretches of thirty seconds with the audio arriving
    the whole time and nothing recognized, and nothing recorded could say
    whether those stretches were silent, quiet, or ordinary speech. RMS
    over a stretch says which; the peak says whether it was clipping.

    The last second is kept apart for the operator page, because the
    recording takes the running totals every five seconds and a page
    reading those would see whatever was left since the last row.
    """

    def __init__(self):
        self.measured = False
        self.samples = 0
        self.energy = 0.0
        self.peak = 0.0
        self.window = [0, 0.0]
        self.live = None
        self.live_at = None

    def add(self, chunk):
        """Fold in one chunk of 16-bit PCM."""
        samples = array.array("h")
        samples.frombytes(chunk[:len(chunk) - len(chunk) % 2])
        if not samples:
            return
        energy = sum(sample * sample for sample in samples) \
            / (FULL_SCALE * FULL_SCALE)
        self.measured = True
        self.samples += len(samples)
        self.energy += energy
        self.peak = max(self.peak,
                        max(abs(min(samples)), max(samples)) / FULL_SCALE)
        self._fold(len(samples), energy)

    def merge(self, seconds, rms, peak):
        """Fold in a stretch somebody else measured, as a sender does."""
        samples = seconds * SAMPLE_RATE
        # Even at zero seconds: a sender saying its microphone delivered
        # nothing is a reading, and not the same as no sender reading.
        self.measured = True
        self.samples += samples
        self.energy += rms * rms * samples
        self.peak = max(self.peak, peak)
        self._fold(samples, rms * rms * samples)

    def _fold(self, samples, energy, now=None):
        self.window[0] += samples
        self.window[1] += energy
        if self.window[0] >= LIVE_WINDOW * SAMPLE_RATE:
            self.live = math.sqrt(self.window[1] / self.window[0])
            self.live_at = time.monotonic() if now is None else now
            self.window = [0, 0.0]

    def current(self, now=None):
        """The last second's level in dBFS, or None if there is none.

        None once it is old, since a microphone that stopped delivering
        has no level, and the page says so separately.
        """
        if self.live_at is None:
            return None
        now = time.monotonic() if now is None else now
        if now - self.live_at > LIVE_STALE:
            return None
        return decibels(self.live)

    def take(self):
        """(seconds, rms, peak) since the last take, then start again.

        rms and peak are fractions of full scale, which is what travels
        from a sender, and decibels() is for whoever reads them. seconds is
        None when nothing was measured at all.
        """
        samples = self.samples
        if samples:
            result = (samples / SAMPLE_RATE,
                      math.sqrt(self.energy / samples), self.peak)
        else:
            result = (0.0 if self.measured else None, 0.0, 0.0)
        self.measured = False
        self.samples = 0
        self.energy = 0.0
        self.peak = 0.0
        return result


def stream_lag(source, audio_end):
    """Seconds between words being spoken and this moment, or None.

    Read off the capture clock every source is wrapped in, which on a
    hosted server counts the audio as it reaches this machine. None between
    runs, when there is no source to ask.
    """
    clock = getattr(source, "clock", None)
    return None if clock is None else clock.lag(audio_end)


ARGUMENT_HELP = {
    "capture": "auto, sounddevice, parec, or remote",
    "encoding": "opus or pcm on the way to the recognizer",
    "asr_model": "Deepgram model name",
    "endpointing": "milliseconds of silence that ends an utterance",
    "early_finalize": "ask the recognizer to finish as soon as its running "
                      "guess ends a sentence",
    "ceiling": "seconds to hold fragments before forcing a sentence",
    "gap": "seconds of silence that closes the buffered sentence",
    "min_words": "fewest words in a line split out of a longer passage",
    "model": "translation model, e.g. gemini-3.8-flash",
    "reasoning_effort": "low is usually right; translation needs no thinking",
    "correct_english": "fix recognition errors on the English channel using "
                       "the glossary",
    "max_tokens": "raise if responses truncate; thinking counts against this",
    "timeout": "seconds before a translation request is abandoned",
    "hold": "seconds to wait for a translation before showing English",
    "languages": "comma separated language names",
    "grace": "seconds a language runs on after its last reader leaves",
    "max_languages": "most languages to translate at once, 0 for no cap",
    "glossary": "file of names and terms for the translation model",
    "keyterms": "file of terms to bias speech recognition toward",
    "idle_stop": "minutes of silence before the session stops itself; "
                 "0 disables",
    "record": "keep a transcript of the session; see record.py",
    "database": "where a recorded session is kept",
    "host": "address to bind",
    "reader_port": "port to bind, the one a tunnel points at",
    "operator_port": "port for the operator controls, always loopback",
    "control_port": "port a remote sender connects to",
    "control_grace": "seconds a session outlives its sender's socket",
    "max_readers": "most readers to serve at once, 0 for no cap",
    "public_url": "the address this room is reachable at, for the QR "
                  "code and for the sender to dial",
    "room": "name of this room, kept with each recorded session",
}


def install_stop_handler(stop):
    """Set Ctrl-C to raise the stop flag, on whatever platform this is.

    loop.add_signal_handler raises NotImplementedError on Windows, so fall
    back to the plain signal module there.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, AttributeError, ValueError):
            signal.signal(sig, lambda *_: stop.set())


# Where a running server or sender records itself so the transept script
# can stop it later. In the working directory rather than beside the code,
# because that is where config.toml, the glossary, and sessions.db already
# live, and "a process running out of this directory" is what is identified.
PID_FILE = "transept.pid"


def write_pid_file(role, ports):
    """Record this process, once it is really listening.

    Written after the ports are bound, so a second process that fails on
    "address already in use" cannot overwrite the record of the one that
    holds them. The ports are in the file as well as the pid, which is how
    stopping works without reading config.toml, and how a recycled pid is
    told from a process that is still serving. The role is "server" or
    "sender", which is what the status command names.

    A directory that will not take the file costs the operator a convenient
    stop, not the meeting, so the failure is reported and swallowed.
    """
    record = {"pid": os.getpid(), "role": role, **ports,
              "started_at": time.time()}
    try:
        with open(PID_FILE, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
    except OSError as failure:
        print(f"Could not write {PID_FILE}, so ./transept stop will not "
              f"find this {role}: {failure}")
        return False
    return True


def remove_pid_file():
    """Take the record away again, but only if it is still ours.

    A process that started after this one owns the file by then, and
    deleting that record would leave the running one unstoppable.
    """
    try:
        with open(PID_FILE, encoding="utf-8") as handle:
            if json.loads(handle.read())["pid"] == os.getpid():
                os.remove(PID_FILE)
    except (OSError, ValueError, KeyError, TypeError):
        pass


def add_config_argument(parser):
    """--config, repeatable and merged left to right by load_config, so a
    hosted room passes the tuning its server shares and then its own file.
    """
    parser.add_argument("--config", action="append", metavar="FILE",
                        help="config file; repeat for a shared file and "
                             "then a room's own")


def room_base(base, room):
    """public_url with the room's path segment on the end.

    Rooms sharing one hostname are told apart by a prefix the proxy in
    front strips before it forwards, so the address on the card and the one
    a sender dials both start here. One value then names the working
    directory, the proxy's route, the recorded column, and both addresses.
    """
    room = room.strip("/")
    return base.rstrip("/") + (f"/{room}" if room else "")


def add_settings_arguments(parser, names):
    """Add the named settings as command line flags.

    Defaults are deliberately None rather than the real default, so that
    resolve() can tell an explicit flag from an absent one and fall through
    to config.toml.
    """
    spec = {attr: (kind,) for attr, _, _, kind, _ in SETTINGS}
    for name in names:
        kind = spec[name][0]
        flag = "--" + name.replace("_", "-")
        help_text = ARGUMENT_HELP.get(name)
        if kind is bool:
            parser.add_argument(flag, dest=name, default=None,
                                action=argparse.BooleanOptionalAction,
                                help=help_text)
        elif kind in (int, float):
            parser.add_argument(flag, dest=name, default=None, type=kind,
                                help=help_text)
        else:
            parser.add_argument(flag, dest=name, default=None,
                                help=help_text)


# -- the pipeline ------------------------------------------------------------
#
# server.py and pipeline.py both run these three tasks over one websocket: a
# pump feeding audio in, a listener segmenting results, and a publisher
# emitting in source order. They differ only in where finished text goes, so
# that difference lives behind a sink and the loops themselves are shared.
# Keep it that way: these loops were duplicated once and the copies drifted
# until one was building a Deepgram URL from names it had never imported.
#
# A sink provides:
#     recognized(words)       the recognizer finalized a stretch of audio,
#                             and words says whether it heard any in it
#     fragment(text, start, audio_end, speech_final, confidence, finalized)
#                             a raw recognition result arrived, finalized
#                             when a Finalize this side sent closed it
#     unit(unit) -> outputs   a sentence closed; returns the output names
#                             worth requesting, where an empty list means do
#                             not call the model for this sentence at all
#     translated(unit, languages, translations, elapsed)
#     timed_out(unit, languages)
#     failed(unit, languages, error)

# Seconds a read from the device may take before the recognizer is sent a
# KeepAlive. Well inside the ten seconds after which it closes the socket.
STALL_KEEPALIVE = 3.0


async def pump_audio(source, socket, stop=None):
    """Captured audio to the recognizer, until either end stops.

    Closing the stream on the way out matters: without it, a capture device
    that dies mid-meeting leaves the websocket open, the reader task waits
    forever on a socket that will never produce another result, and the
    session sits in "running" with no audio and no reconnect.

    A source may also report a gap, which None means and which a local
    device never produces: a sender's connection blinked, and the sentence
    in progress is not over. A KeepAlive holds the recognizer socket open
    through it. Silence would do the same and be billed as audio.

    A local device can stall without saying so, as a microphone muted by a
    locked screen does, and the recognizer closes a socket that has heard
    nothing for about ten seconds. So a read that takes longer than
    STALL_KEEPALIVE sends a KeepAlive too, and goes on waiting on the same
    read rather than starting another: a read abandoned partway could lose
    the audio it had already taken off the device.
    """
    pending = None
    try:
        while stop is None or not stop.is_set():
            if pending is None:
                pending = asyncio.ensure_future(source.read())
            done, _ = await asyncio.wait({pending}, timeout=STALL_KEEPALIVE)
            if not done:
                await socket.send(json.dumps({"type": "KeepAlive"}))
                continue
            chunk = pending.result()
            pending = None
            if chunk is None:
                await socket.send(json.dumps({"type": "KeepAlive"}))
                continue
            if not chunk:
                return
            await socket.send(chunk)
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        try:
            await socket.send(json.dumps({"type": "CloseStream"}))
        except (websockets.ConnectionClosed, RuntimeError):
            pass


# Seconds of silence after a guess's last word before a sentence it ends
# is believed. A Finalize closes all the audio sent so far, so asked while
# the next word is already arriving it cuts that word in half: on a live
# test "after" came back as "F". Real sentence ends there left 0.23 to 0.31
# seconds, and most guesses taken mid-speech less than 0.2.
FINALIZE_QUIET = 0.2


def ends_sentence(guess, held, min_words, quiet=FINALIZE_QUIET):
    """Whether a running guess is worth finishing now.

    A guess ending a sentence, followed by quiet seconds of silence, with
    enough words behind it counting what the segmenter already holds,
    since "Right?" on its own is not a line and asking for it would only
    cut the recognizer's context short.
    """
    if quiet < FINALIZE_QUIET or not TERMINAL_PUNCTUATION.search(guess):
        return False
    return held + len(guess.split()) >= min_words


def trailing_quiet(payload):
    """Seconds between a result's last word and the end of its audio."""
    alternatives = payload.get("channel", {}).get("alternatives", [])
    words = alternatives[0].get("words") if alternatives else None
    if not words:
        return 0.0
    return (payload.get("start", 0.0) + payload.get("duration", 0.0)
            - words[-1].get("end", 0.0))


async def listen(socket, segmenter, translator, sink, queue,
                 finalize=False):
    """Read ASR results, segment them, and launch translations.

    With finalize, the recognizer's running guesses decide when to ask it
    to finish. Left alone it finalizes on a pause of endpointing or on its
    own clock, and in a room the pause seldom comes, so a sentence's first
    word waited four seconds or more for the rest of its fragment. A guess
    that ends a sentence is the moment the words are all there, and a
    Finalize hands them over then. The guesses themselves are never shown.
    """
    context = deque(maxlen=CONTEXT_UNITS)
    # A Finalize is sent once per stretch: the guesses keep arriving until
    # the finished result does, and each would otherwise ask again.
    asked = False

    async def emit(taken):
        seq, text, reason, audio_end, confidence = taken
        unit = Unit(seq, text, audio_end, reason, confidence)
        outputs = sink.unit(unit)
        task = None
        if outputs and translator is not None:
            task = asyncio.create_task(
                translator.translate(text, list(context), outputs))
        # English among the outputs is a correction of the transcript, not a
        # channel to fill; the publisher wants the translated names only.
        languages = [name for name in outputs if name != "English"]
        context.append(text)
        await queue.put((unit, task, languages))

    async def watch_ceiling():
        while True:
            await asyncio.sleep(0.25)
            for taken in segmenter.check_ceiling():
                await emit(taken)

    ceiling_task = asyncio.create_task(watch_ceiling())
    try:
        async for message in socket:
            if isinstance(message, bytes):
                continue
            payload = json.loads(message)
            if payload.get("type") != "Results":
                continue
            alternatives = payload.get("channel", {}).get("alternatives", [])
            text = (alternatives[0].get("transcript", "").strip()
                    if alternatives else "")
            if not payload.get("is_final"):
                if finalize and not asked and ends_sentence(
                        text, segmenter.held_words(), segmenter.min_words,
                        trailing_quiet(payload)):
                    asked = True
                    await socket.send(json.dumps({"type": "Finalize"}))
                continue
            asked = False
            if not alternatives:
                continue
            # Counted before an empty one is skipped, because a recognizer
            # answering with nothing is what tells a room it cannot hear
            # from a recognizer that is not answering at all.
            sink.recognized(bool(text))
            if not text:
                continue

            start = payload.get("start", 0.0)
            audio_end = start + payload.get("duration", 0.0)
            finalized = bool(payload.get("from_finalize"))
            # The recognizer marks every answer to a Finalize as the end of
            # speech, which it is not: the guess that asked may have ended a
            # sentence the finished text runs on from, and the segmenter
            # would close the line halfway through it.
            speech_final = (payload.get("speech_final", False)
                            and not finalized)
            confidence = alternatives[0].get("confidence")
            sink.fragment(text, start, audio_end, speech_final, confidence,
                          finalized)
            for taken in segmenter.add(text, speech_final, start, audio_end,
                                       confidence):
                await emit(taken)
    finally:
        ceiling_task.cancel()
        for taken in segmenter.drain():
            await emit(taken)
        await queue.put(None)


async def publish(queue, sink, hold_seconds):
    """Hand translations to the sink in source order as calls complete.

    Calls run concurrently, but output is ordered, so a slow call delays the
    ones behind it rather than scrambling the transcript. The hold timeout
    bounds that delay: past it, the English text stands in so a reader never
    sees a gap.
    """
    while True:
        item = await queue.get()
        if item is None:
            return
        unit, task, languages = item
        if task is None:
            continue
        try:
            translations, elapsed = await asyncio.wait_for(
                asyncio.shield(task), timeout=hold_seconds)
        except TimeoutError:
            task.cancel()
            sink.timed_out(unit, languages)
        except asyncio.CancelledError:
            # The shield means this is our own cancellation: the session is
            # stopping. An English fallback here would reach every channel
            # after the operator pressed Stop.
            task.cancel()
            raise
        except (RuntimeError, httpx.HTTPError) as exc:
            sink.failed(unit, languages, exc)
        else:
            sink.translated(unit, languages, translations, elapsed)


def latency_summary(values):
    """Median and p95 over a list of seconds, for an end of run report."""
    ordered = sorted(values)
    p95 = ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]
    return f"median {statistics.median(ordered):.2f}s, p95 {p95:.2f}s"


class TerminalSink:
    """Prints the transcript as it forms, in the format __doc__ describes."""

    def __init__(self, outputs, color, source=None):
        self.outputs = list(outputs)
        self.source = source
        self.dim, self.reset = (DIM, RESET) if color else ("", "")
        self.latencies = []
        self.tokens = {}
        self.failures = 0
        self.timeouts = 0

    def recognized(self, words):
        pass

    def fragment(self, text, start, audio_end, speech_final, confidence,
                 finalized=False):
        print(f"{self.dim}  . {text}{self.reset}")

    def unit(self, unit):
        lag = stream_lag(self.source, unit.audio_end)
        behind = "?" if lag is None else f"{lag:.1f}s"
        print(f"\n[{unit.seq}] en ({behind}, {unit.reason}): {unit.text}")
        return self.outputs

    def translated(self, unit, languages, translations, elapsed):
        self.latencies.append(elapsed)
        for name, value in (getattr(translations, "usage", None)
                            or {}).items():
            self.tokens[name] = self.tokens.get(name, 0) + value
        revised = translations.get("English")
        if revised and revised != unit.text:
            print(f"     en*: {revised}")
        for name in languages:
            # An empty string means the model omitted that language, which
            # parse_translations cannot distinguish from an empty answer.
            print(f"     {name[:2].lower()}: "
                  f"{translations.get(name) or '[en] ' + unit.text}"
                  f"  (+{elapsed:.1f}s)")

    def timed_out(self, unit, languages):
        self.timeouts += 1
        for name in languages:
            print(f"     {name[:2].lower()}: [slow] {unit.text}")

    def failed(self, unit, languages, error):
        self.failures += 1
        print(f"     !! translation failed: {error}")
        for name in languages:
            print(f"     {name[:2].lower()}: [en] {unit.text}")

    def summary(self):
        if not self.latencies:
            return None
        summary = (f"{len(self.latencies)} translated, {self.failures} "
                   f"failed, {self.timeouts} too slow. "
                   f"{latency_summary(self.latencies)}")
        if self.tokens:
            summary += "\nTokens: " + ", ".join(
                f"{value} {name.removesuffix('_tokens')}"
                for name, value in self.tokens.items())
        return summary


async def run(args, settings, keys):
    deepgram_key = keys["deepgram_key"]
    if not deepgram_key:
        sys.exit("Set deepgram_api_key under [keys] in config.toml")

    translator = None
    if not args.no_translate:
        llm_key = keys["llm_key"]
        llm_base = keys["llm_base"]
        if not llm_key or not llm_base:
            sys.exit("Set llm_api_key and llm_base_url under [keys] in "
                     "config.toml, or pass --no-translate to check the "
                     "audio path alone.")
        if not settings["model"]:
            sys.exit("No translation model. Set translation.model in "
                     "config.toml or pass --model.")
        glossary = load_file_lines(settings["glossary"])
        translator = Translator(
            llm_base, llm_key, settings["model"], settings["languages"],
            glossary_prompt(glossary), settings["max_tokens"],
            settings["timeout"], settings["reasoning_effort"],
            settings["correct_english"])
        print(describe_file("Glossary", settings["glossary"], glossary))

    keyterms = load_file_lines(settings["keyterms"])
    print(describe_file("Keyterms", settings["keyterms"], keyterms))
    segmenter = Segmenter(settings["ceiling"], settings["gap"],
                          min_words=settings["min_words"])
    queue = asyncio.Queue()

    try:
        # No --device means the default input, so the usual check of the
        # audio path is one command with nothing to look up first.
        device = args.device or capture.default_device(settings["capture"])
        source = Stamped(await capture.open_capture(device,
                                                    settings["capture"]))
    except capture.CaptureError as exc:
        sys.exit(str(exc))
    source, note = await open_encoder(source, settings["encoding"])
    if note:
        print(note, file=sys.stderr)

    stop = asyncio.Event()
    install_stop_handler(stop)

    describe = (", ".join(translator.languages) if translator
                else "transcription only")
    print(f"Listening on {device}. {describe}. Ctrl-C to stop.",
          file=sys.stderr)
    sink = TerminalSink(translator.outputs if translator else [],
                        not args.no_color, source)

    try:
        async with websockets.connect(
            # What is actually going up the socket, which is Opus unless
            # this machine had no encoder to fall back from.
            build_asr_url(settings, keyterms, source.encoding),
            additional_headers={"Authorization": f"Token {deepgram_key}"},
        ) as socket:
            pump = asyncio.create_task(pump_audio(source, socket, stop))
            reader = asyncio.create_task(
                listen(socket, segmenter, translator, sink, queue,
                       settings["early_finalize"]))
            writer = asyncio.create_task(
                publish(queue, sink, settings["hold"]))
            await stop.wait()
            pump.cancel()
            try:
                # Ctrl-C should still print the sentence left in the buffer
                # and the translations already in flight, so the reader and
                # the writer get a bounded chance to drain.
                await asyncio.wait_for(asyncio.gather(reader, writer),
                                       timeout=settings["hold"] + 2)
            except (TimeoutError, asyncio.CancelledError):
                reader.cancel()
                writer.cancel()
            finally:
                # Collect the pump, so a socket that closed under it does not
                # surface later as an unretrieved task exception.
                await asyncio.gather(pump, return_exceptions=True)
    finally:
        await source.close()
        if translator:
            await translator.close()

    summary = sink.summary()
    if summary:
        print("\n" + summary, file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Any setting may live in config.toml instead. The command "
               "line wins where both are given.")
    parser.add_argument("--list-devices", action="store_true",
                        help="list audio input devices and exit")
    parser.add_argument("--no-translate", action="store_true",
                        help="transcribe only, no translation model needed")
    parser.add_argument("--no-color", action="store_true")
    add_config_argument(parser)
    # Not a setting: the source changes with every reboot, so a name kept
    # in config.toml would be stale more often than it was right.
    parser.add_argument("--device",
                        help="audio input device name; see --list-devices")
    add_settings_arguments(parser, [
        "capture", "encoding",
        "asr_model", "endpointing", "early_finalize",
        "ceiling", "gap", "min_words",
        "model", "reasoning_effort", "correct_english", "max_tokens",
        "timeout", "hold",
        "languages",
        "glossary", "keyterms",
    ])
    args = parser.parse_args()

    if args.list_devices:
        capture.print_devices(args.capture or "auto")
        return

    config = load_config(*(args.config or []))
    asyncio.run(run(args, resolve(args, config), load_keys(config)))


if __name__ == "__main__":
    main()
