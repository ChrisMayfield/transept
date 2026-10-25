#!/usr/bin/env python3
"""
Keep each session, and turn it into something worth reading afterwards.

A meeting is recorded line by line into a SQLite file while it runs. Nothing
here is for the readers; it is for the three things that decide subtitle
quality and are otherwise edited on a hunch: SYSTEM_PROMPT, glossary.txt,
and keyterms.txt.

Recording is off unless [session] record is true in config.toml. Nothing is
ever deleted on its own; --purge is the only way, and it has to be typed.

Usage, from inside local/ where config.toml is:
    python3 ../src/record.py --list
    python3 ../src/record.py --session last --out review/2026-09-13.md
    python3 ../src/record.py --session last --plain sunday.txt
    python3 ../src/record.py --purge --older-than 30

--plain writes what review.py --input already parses, so trying a prompt or
glossary change means exporting a real transcript, editing the prompt, and
running review.py against the same sentences the room actually heard.
"""

import argparse
import asyncio
import json
import re
import sqlite3
import sys
import time
from pathlib import Path

from pipeline import CONTEXT_UNITS, latency_summary

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
  id INTEGER PRIMARY KEY,
  started_at REAL, ended_at REAL,
  device TEXT, asr_model TEXT, model TEXT, languages TEXT,
  correct_english INTEGER, settings TEXT,
  glossary TEXT, keyterms TEXT, prompt_hash TEXT,
  -- Last, which is where ALTER TABLE puts it on a database made before
  -- this column existed, so both ways of arriving here agree.
  room TEXT);

CREATE TABLE IF NOT EXISTS lines (
  id INTEGER PRIMARY KEY,
  session_id INTEGER NOT NULL, seq INTEGER NOT NULL, run INTEGER,
  at REAL, audio_end REAL, lag REAL, confidence REAL,
  heard TEXT, english TEXT, reason TEXT, latency REAL,
  outcome TEXT, requested TEXT,
  -- What the translation call cost, last for the reason room is.
  prompt_tokens INTEGER, output_tokens INTEGER,
  reasoning_tokens INTEGER, cached_tokens INTEGER,
  UNIQUE (session_id, seq));

CREATE TABLE IF NOT EXISTS translations (
  id INTEGER PRIMARY KEY,
  session_id INTEGER NOT NULL, seq INTEGER NOT NULL,
  language TEXT NOT NULL, text TEXT,
  source TEXT,
  UNIQUE (session_id, seq, language));

-- Every result the recognizer finalized, as it arrived, so a change to
-- the segmenter can be replayed against a real meeting. start and
-- audio_end are seconds into the run's stream, at is the wall clock.
CREATE TABLE IF NOT EXISTS fragments (
  id INTEGER PRIMARY KEY,
  session_id INTEGER NOT NULL, run INTEGER,
  at REAL, start REAL, audio_end REAL,
  speech_final INTEGER, confidence REAL, text TEXT,
  -- 1 when a Finalize sent on a guess ending a sentence closed it, last
  -- for the reason room is.
  finalized INTEGER,
  -- Each word as [word, start, end] in JSON, in the same seconds as start,
  -- and the lag of audio_end as it arrived, which together say on the
  -- wall clock when each word was spoken.
  words TEXT, lag REAL);

-- Every guess that ended a sentence and could have asked the recognizer
-- to finish: where its window ended, the quiet after its last word, the
-- audio sent after its window (unseen), and whether a Finalize was sent,
-- passed up as stale, or not sent because early_finalize was off.
CREATE TABLE IF NOT EXISTS guesses (
  id INTEGER PRIMARY KEY,
  session_id INTEGER NOT NULL, run INTEGER, at REAL,
  audio_end REAL, quiet REAL, unseen REAL, decision TEXT);

-- One row per few seconds of a run, saying what the audio did on its way
-- to the recognizer: seconds captured and how loud (rms and peak in dBFS,
-- as the microphone's own machine measured them), seconds that reached the
-- recognizer, and how many results it finalized, how many of them empty.
-- NULL means nothing measured it, which is not the same as zero.
CREATE TABLE IF NOT EXISTS audio (
  id INTEGER PRIMARY KEY,
  session_id INTEGER NOT NULL, run INTEGER, at REAL,
  captured REAL, received REAL, rms_db REAL, peak_db REAL,
  results INTEGER, empty INTEGER);

CREATE INDEX IF NOT EXISTS lines_by_session ON lines (session_id, seq);
CREATE INDEX IF NOT EXISTS fragments_by_session
  ON fragments (session_id, id);
CREATE INDEX IF NOT EXISTS translations_by_session
  ON translations (session_id, seq);
CREATE INDEX IF NOT EXISTS audio_by_session ON audio (session_id, id);
CREATE INDEX IF NOT EXISTS guesses_by_session ON guesses (session_id, id);
"""

# Queue depth. A meeting produces ten to twenty lines a minute and about as
# many fragments, so anything this side of a hundred is a disk that has
# stopped answering, and dropping the record of a sentence beats holding
# the recognizer up for it.
QUEUE_DEPTH = 256

# What a translation call reports it cost, in the order usage_of gives them.
TOKEN_COLUMNS = ("prompt_tokens", "output_tokens", "reasoning_tokens",
                 "cached_tokens")

# Columns added since the first release, each at the end of its table where
# ALTER TABLE puts it, as (table, column, type).
ADDED_COLUMNS = (
    ("sessions", "room", "TEXT"),
    *(("lines", column, "INTEGER") for column in TOKEN_COLUMNS),
    ("fragments", "finalized", "INTEGER"),
    ("fragments", "words", "TEXT"),
    ("fragments", "lag", "REAL"),
)

# Seconds of audio each row of the audio table describes, here because both
# the server writing them and the report reading them need it. Short enough
# that a thirty second dropout is six rows rather than a blur, long enough
# that an hour is seven hundred rows rather than one per fragment.
AUDIO_WINDOW = 5.0

# What a reviewer is asked, at the top of every side-by-side document this
# and review.py write.
REVIEW_INSTRUCTIONS = (
    "Please mark anything that is wrong, unnatural, or would confuse "
    "someone reading it on a phone during a meeting.\n"
    "Wrong meaning matters most.\n"
    "Note awkward phrasing separately, since the two need different "
    "fixes.\n\n")


def connect(path):
    """Open the database, ready to be written to from one task.

    timeout=0 because the alternative is worse than an error: on a locked
    database sqlite does not raise, it blocks in the busy handler for the
    full timeout first, and the default is five seconds.
    """
    handle = sqlite3.connect(path, timeout=0, isolation_level=None)
    handle.execute("PRAGMA journal_mode=WAL")
    # No fsync per commit. A church transcript is worth losing to a power
    # cut and not worth losing to a crash, which is exactly this setting.
    handle.execute("PRAGMA synchronous=NORMAL")
    handle.executescript(SCHEMA)
    migrate(handle)
    return handle


def migrate(handle):
    """Add columns a database from an earlier version does not have.

    SCHEMA is all CREATE TABLE IF NOT EXISTS, so a file that already exists
    keeps the columns it was made with and gains nothing on its own. Adding
    one to the end is something sqlite does without rewriting the table.
    """
    for table, column, kind in ADDED_COLUMNS:
        if column not in columns(handle, table):
            handle.execute(f"ALTER TABLE {table} ADD COLUMN {column} {kind}")


def columns(handle, table):
    """The names of a table's columns, empty if there is no such table."""
    return {row[1] for row in handle.execute(f"PRAGMA table_info({table})")}


class Recorder:
    """Writes one session to disk, off the path the subtitles run on.

    Every sink method here only puts a row on a queue. The writes happen in
    one task owned by the Session, because sink.unit runs inside the loop
    reading the Deepgram socket, and a disk that pauses there stops audio
    being drained. Nothing in this class may block, and nothing in it may
    raise into its caller.
    """

    def __init__(self, path):
        self.path = path
        self.queue = asyncio.Queue(maxsize=QUEUE_DEPTH)
        self.session_id = None
        self.handle = None
        self.task = None
        self.dropped = 0
        self.error = None

    # -- lifecycle, called from Session.start and Session.stop -------------

    async def open(self, header):
        """Begin a session. Returns an error string, or None on success.

        A database that cannot be opened must not stop a meeting from
        starting, so the caller reports this and carries on unrecorded.
        """
        try:
            self.handle = connect(self.path)
            cursor = self.handle.execute(
                "INSERT INTO sessions (started_at, device, asr_model, model,"
                " languages, correct_english, settings, glossary, keyterms,"
                " prompt_hash, room) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                header)
            self.session_id = cursor.lastrowid
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"
            self.handle = None
            return self.error
        self.task = asyncio.create_task(self._write())
        return None

    async def close(self):
        """Drain what is queued, stamp the end, and let go of the file."""
        if self.task:
            await self.queue.put(None)
            try:
                await asyncio.wait_for(self.task, timeout=5)
            except (TimeoutError, asyncio.CancelledError):
                self.task.cancel()
            self.task = None
        if self.handle:
            try:
                self.handle.execute(
                    "UPDATE sessions SET ended_at = ? WHERE id = ?",
                    (time.time(), self.session_id))
                self.handle.close()
            except Exception as exc:
                self.error = f"{type(exc).__name__}: {exc}"
            self.handle = None

    # -- what the sink hands over ------------------------------------------

    def line(self, **row):
        self._put(("line", row))

    def correction(self, **row):
        self._put(("correction", row))

    def translation(self, **row):
        self._put(("translation", row))

    def fragment(self, **row):
        self._put(("fragment", row))

    def usage(self, **row):
        self._put(("usage", row))

    def audio(self, **row):
        self._put(("audio", row))

    def guess(self, **row):
        self._put(("guess", row))

    def _put(self, item):
        if self.task is None:
            return
        try:
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            # The disk is not keeping up with a room talking, which means it
            # has stopped answering. Losing the record beats holding up the
            # recognizer, and the count reaches the operator page.
            self.dropped += 1

    # -- the only code here that touches the file --------------------------

    async def _write(self):
        while True:
            item = await self.queue.get()
            if item is None:
                return
            kind, row = item
            try:
                self._apply(kind, row)
            except Exception as exc:
                # Anything at all, not just sqlite3.Error: a TypeError while
                # building a row would otherwise kill this task and stop the
                # recording silently for the rest of the meeting.
                self.error = f"{type(exc).__name__}: {exc}"

    def _apply(self, kind, row):
        if kind == "line":
            self.handle.execute(
                "INSERT OR IGNORE INTO lines (session_id, seq, run, at,"
                " audio_end, lag, confidence, heard, reason, outcome,"
                " requested) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (self.session_id, row["seq"], row["run"], row["at"],
                 row["audio_end"], row["lag"], row["confidence"],
                 row["heard"], row["reason"], row["outcome"],
                 json.dumps(row["requested"])))
        elif kind == "correction":
            # english=None means leave the column alone, not write a NULL
            # over it. The fallback paths have no corrected text to offer
            # and must not erase one that arrived.
            if row["english"] is None:
                self.handle.execute(
                    "UPDATE lines SET latency = ?, outcome = ?"
                    " WHERE session_id = ? AND seq = ?",
                    (row["latency"], row["outcome"], self.session_id,
                     row["seq"]))
            else:
                self.handle.execute(
                    "UPDATE lines SET english = ?, latency = ?, outcome = ?"
                    " WHERE session_id = ? AND seq = ?",
                    (row["english"], row["latency"], row["outcome"],
                     self.session_id, row["seq"]))
        elif kind == "translation":
            self.handle.execute(
                "INSERT OR REPLACE INTO translations (session_id, seq,"
                " language, text, source) VALUES (?,?,?,?,?)",
                (self.session_id, row["seq"], row["language"], row["text"],
                 row["source"]))
        elif kind == "fragment":
            self.handle.execute(
                "INSERT INTO fragments (session_id, run, at, start,"
                " audio_end, speech_final, confidence, text, finalized,"
                " words, lag) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (self.session_id, row["run"], row["at"], row["start"],
                 row["audio_end"], int(bool(row["speech_final"])),
                 row["confidence"], row["text"],
                 int(bool(row.get("finalized"))),
                 json.dumps(row["words"]) if row.get("words") else None,
                 row.get("lag")))
        elif kind == "guess":
            self.handle.execute(
                "INSERT INTO guesses (session_id, run, at, audio_end, quiet,"
                " unseen, decision) VALUES (?,?,?,?,?,?,?)",
                (self.session_id, row["run"], row["at"], row["audio_end"],
                 row["quiet"], row["unseen"], row["decision"]))
        elif kind == "audio":
            self.handle.execute(
                "INSERT INTO audio (session_id, run, at, captured, received,"
                " rms_db, peak_db, results, empty)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (self.session_id, row["run"], row["at"], row["captured"],
                 row["received"], row["rms_db"], row["peak_db"],
                 row["results"], row["empty"]))
        elif kind == "usage":
            self.handle.execute(
                "UPDATE lines SET "
                + ", ".join(f"{column} = ?" for column in TOKEN_COLUMNS)
                + " WHERE session_id = ? AND seq = ?",
                (*(row[column] for column in TOKEN_COLUMNS),
                 self.session_id, row["seq"]))


# -- reading it back ---------------------------------------------------------


def open_read_only(path):
    """Open a recorded database for reading, or say plainly that there is none.

    A missing file and a file with no tables in it are the same thing to the
    operator: nothing has been recorded yet, which is not an error worth a
    traceback.
    """
    if not Path(path).exists():
        sys.exit(f"No recording at {path}. Set [session] record in "
                 f"config.toml to keep one.")
    handle = sqlite3.connect(path, timeout=0)
    handle.row_factory = sqlite3.Row
    if not columns(handle, "sessions"):
        sys.exit(f"{path} holds no sessions.")
    return handle


def sessions(handle):
    return handle.execute(
        "SELECT * FROM sessions ORDER BY started_at").fetchall()


def room_of(session):
    """Which room a session was recorded in, or "" if it does not say.

    A recording made before the room column existed has no column at all
    rather than a NULL, because reading a session back must not write to
    the file it is reading and so does not migrate it.
    """
    if "room" not in session.keys():
        return ""
    return session["room"] or ""


def resolve_session(handle, which):
    rows = sessions(handle)
    if not rows:
        sys.exit("No sessions recorded yet.")
    if which in (None, "last"):
        return rows[-1]
    for row in rows:
        if str(row["id"]) == str(which):
            return row
    sys.exit(f"No session {which!r}. Try --list.")


WORD = re.compile(r"[\w'-]+")
PROPER = re.compile(r"^[A-Z][\w'-]*$")

# Words a correction changes for reasons that have nothing to do with the
# recognizer missing a term: grammar mended around a repair, a word
# recapitalized after a full stop was added. They made up most of the first
# worklist a real meeting produced, above the names it was for.
COMMON_WORDS = {
    "a", "an", "the", "and", "or", "but", "so", "then", "if", "of", "in",
    "on", "at", "to", "for", "from", "with", "by", "as", "is", "are", "was",
    "were", "be", "been", "am", "it", "its", "it's", "this", "that", "these",
    "those", "what", "which", "who", "how", "i", "you", "he", "she", "we",
    "they", "me", "him", "her", "us", "them", "my", "your", "his", "our",
    "their", "right", "yes", "yeah", "okay", "oh", "well", "like", "kind",
    "look", "not", "no", "do", "did", "have", "has", "had",
    # Contractions mended and scripture's pronouns, which on the third field
    # test sat among the names to check as "I've", "Thee", and "O".
    "i'm", "i've", "i'll", "i'd", "we're", "we'll", "we've", "you're",
    "they're", "o", "thee", "thou", "thy", "thine", "ye", "pm",
}


def worth_a_keyterm(word, heard):
    """Whether a word a correction added is a term the recognizer missed.

    Not a common word, not a number, which is formatting rather than
    hearing, and not a name the recognizer already heard with a possessive
    on it, which is the correction editing grammar.
    """
    lowered = word.lower()
    if lowered in COMMON_WORDS or word.isdigit():
        return False
    heard = {token.lower() for token in heard}
    return lowered + "'s" not in heard and lowered + "\u2019s" not in heard


def changed_words(heard, english, fold=False):
    """Words the correction introduced, and the ones it replaced.

    A one-way word diff rather than a real alignment. What matters is which
    tokens appear on one side and not the other, because that is the term
    the recognizer missed and the glossary had to repair.

    Case matters by default, because capitalizing a name is the commonest
    repair of all and folding it away hides the entire signal. Sentence
    initial capitalization is the one piece of noise that produces, so the
    leading word is dropped when it differs only in case.

    Pass fold=True for the opposite question, which is whether a word is
    genuinely new rather than the same word respelled. That is what
    separates the glossary fixing "kalema" from the model inventing a name
    that was never spoken.
    """
    before = WORD.findall(heard or "")
    after = WORD.findall(english or "")
    if fold:
        before = [word.lower() for word in before]
        after = [word.lower() for word in after]
    elif (before and after and before[0] != after[0]
            and before[0].lower() == after[0].lower()):
        before, after = before[1:], after[1:]
    added = [word for word in after if word not in set(before)]
    removed = [word for word in before if word not in set(after)]
    return added, removed


def lines_of(handle, session_id):
    return handle.execute(
        "SELECT * FROM lines WHERE session_id = ? ORDER BY seq",
        (session_id,)).fetchall()


def translations_of(handle, session_id):
    grouped = {}
    for row in handle.execute(
            "SELECT * FROM translations WHERE session_id = ? ORDER BY seq",
            (session_id,)):
        grouped.setdefault(row["seq"], []).append(row)
    return grouped


def stamp(value):
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(value or 0))


def write_report(handle, session, out):
    """The document the operator reads on Sunday afternoon."""
    lines = lines_of(handle, session["id"])
    grouped = translations_of(handle, session["id"])
    languages = json.loads(session["languages"] or "[]")

    with open(out, "w", encoding="utf-8") as page:
        room = room_of(session)
        page.write(f"# Session {session['id']}, {room + ', ' if room else ''}"
                   f"{stamp(session['started_at'])}\n\n")
        minutes = ((session["ended_at"] or session["started_at"])
                   - session["started_at"]) / 60
        page.write(f"{len(lines)} sentences over {minutes:.0f} minutes. "
                   f"Device `{session['device']}`, recognizer "
                   f"`{session['asr_model']}`, translation "
                   f"`{session['model']}`.\n\n")
        page.write(f"Languages offered: {', '.join(languages) or 'none'}.\n\n")

        _fabrications(page, lines, session)
        _corrections(page, lines)
        _closings(page, lines)
        _latency(page, handle, session, lines)
        _pieces(page, handle, session)
        _silences(page, handle, session, lines)
        _tokens(page, lines)
        _fallbacks(page, grouped)
        _transcript(page, lines, grouped)


# Where a name a correction introduced could have come from, in the order
# the report lists them: the one to check first is the one from nowhere.
SOURCES = (
    ("neither",
     "From neither the glossary nor the preceding lines, so from the "
     "model's own knowledge, which the prompt forbids. Check these first."),
    ("opening",
     "From neither, but opening a sentence, where a capital is usually "
     "grammar, such as a first word the recognizer lost and the model "
     "restored. A name the model supplied lands here too when it opens "
     "the line."),
    ("context",
     "From the preceding lines, which the model may borrow a name from. "
     "Such a name is only as right as the recognizer's spelling of it "
     "there."),
    ("glossary", "From the glossary, which is the glossary doing its job."),
)


def sentence_starts(text):
    """Lowercased words that open a sentence of this text.

    A capital there is grammar, not a name, which is how "Each" and "If",
    restored where the recognizer lost them, would otherwise read.
    """
    starts = set()
    for sentence in re.split(r"(?<=[.!?])\s+", text or ""):
        found = WORD.search(sentence)
        if found:
            starts.add(found.group().lower())
    return starts


def name_sources(line, earlier, glossary):
    """The names a correction introduced, each with where it could be from.

    Earlier is the lines the model was given as context, and glossary the
    words of the glossary the session loaded.
    """
    # fold=True: a name that was merely recapitalized is the glossary doing
    # its job. What belongs here is a word with no counterpart in the audio.
    added, _ = changed_words(line["heard"], line["english"], fold=True)
    introduced = set(added)
    opening = sentence_starts(line["english"])
    middle = {word.lower() for sentence in re.split(
                  r"(?<=[.!?])\s+", line["english"])
              for word in WORD.findall(sentence)[1:]}
    context = {word.lower() for text in earlier
               for word in WORD.findall(text or "")}
    heard = {word.lower() for word in WORD.findall(line["heard"] or "")}
    found = {}
    for word in WORD.findall(line["english"]):
        lowered = word.lower()
        if (not PROPER.match(word) or lowered not in introduced
                or lowered in COMMON_WORDS or inflected(lowered, heard)):
            continue
        # Dropping a capital that opens a sentence hid "Runia", supplied
        # for "Rina" at the start of a line on the fourth field test.
        found[word] = ("glossary" if lowered in glossary
                       else "context" if lowered in context
                       else "opening" if (lowered in opening
                                          and lowered not in middle)
                       else "neither")
    return found


def inflected(word, heard):
    """Whether the word was heard with or without a plural or possessive.

    "Young Women's" corrected to "Young Women" and "Elders" to "Elder" are
    the same word heard, not a name the model brought in.
    """
    forms = {word + "s", word + "'s", word.removesuffix("'s"),
             word.removesuffix("s")} - {word}
    return bool(forms & heard)


def _fabrications(page, lines, session):
    """Proper nouns the correction introduced that were not in the audio.

    First, because it is the only mechanical check there is on the rule that
    the model must never invent a name, and a reader of a translated channel
    has no way to catch one. A name taken from the glossary or from the
    lines before is allowed; one from neither is the model's own, and on
    the third field test a name the model borrowed from the line before
    was taken for one of those until somebody read that line.
    """
    page.write("## Names to check\n\n")
    glossary = {word.lower()
                for word in WORD.findall(session["glossary"] or "")}
    grouped = {source: [] for source, _ in SOURCES}
    for index, line in enumerate(lines):
        if not line["english"]:
            continue
        # The context the model was given: the lines just before, in this
        # run, since a reconnect starts the context over.
        earlier = [other["heard"] for other in lines[:index]
                   if other["run"] == line["run"]][-CONTEXT_UNITS:]
        by_source = {}
        for word, source in name_sources(line, earlier, glossary).items():
            by_source.setdefault(source, []).append(word)
        for source, words in by_source.items():
            grouped[source].append((line, sorted(words)))
    if not any(grouped.values()):
        page.write("Nothing introduced a capitalized word that was not in "
                   "the audio.\n\n")
        return
    page.write("A capitalized word appears in the corrected line that was "
               "not in what the recognizer heard.\n\n")
    for source, heading in SOURCES:
        if not grouped[source]:
            continue
        page.write(f"**{heading}**\n\n")
        for line, names in grouped[source]:
            page.write(f"- **{', '.join(names)}** (line {line['seq']})\n")
            page.write(f"  - heard: {line['heard']}\n")
            page.write(f"  - shown: {line['english']}\n")
        page.write("\n")


def _corrections(page, lines):
    """The keyterms.txt worklist."""
    page.write("## Terms to add to keyterms.txt\n\n")
    available = [line for line in lines if line["english"]]
    page.write(f"Corrections available for {len(available)} of {len(lines)} "
               "lines.\n\n")
    if not available:
        page.write("No line was corrected, so there is nothing to learn "
                   "here. Set `correct_english` and record again.\n\n")
        return

    counts, recapitalized = {}, {}
    for line in available:
        added, removed = changed_words(line["heard"], line["english"])
        heard = WORD.findall(line["heard"] or "")
        spelled = {token.lower() for token in heard}
        for word in added:
            if not worth_a_keyterm(word, heard):
                continue
            key = word.lower()
            if key in spelled:
                # Heard with every letter right. On the third field test
                # these were half the table, "savior" and "brother" above
                # the words the recognizer actually missed.
                entry = recapitalized.setdefault(key, {"word": word, "n": 0})
                entry["n"] += 1
                continue
            entry = counts.setdefault(key, {"word": word, "n": 0, "was": []})
            entry["n"] += 1
            entry["was"].extend(removed)
    if not counts and not recapitalized:
        page.write("Every corrected line came back unchanged.\n\n")
        return
    if not counts:
        page.write("Nothing was misheard and repaired.\n\n")
        _recapitalized(page, recapitalized)
        return
    page.write("The recognizer missed these and the glossary repaired them "
               "afterwards. Putting them in keyterms.txt gets them right at "
               "the source, so the line never has to be revised on a "
               "phone. A term that is also under Names to check may be one "
               "the model invented, and adding that to keyterms.txt would "
               "teach the recognizer to hear it, so check those first.\n\n")
    page.write("| term | times | heard instead |\n|---|---|---|\n")
    for entry in sorted(counts.values(), key=lambda e: -e["n"]):
        was = ", ".join(sorted(set(entry["was"]))[:4]) or "-"
        page.write(f"| {entry['word']} | {entry['n']} | {was} |\n")
    page.write("\n")
    _recapitalized(page, recapitalized)


def _recapitalized(page, entries):
    """Words heard right and only given a capital, apart from the table.

    Mostly titles like "Brother" and "Savior", which need no key term, but
    a name the recognizer spelled right without knowing it was a name
    lands here too, and is worth one.
    """
    if not entries:
        return
    listed = ", ".join(
        f"{entry['word']} ({entry['n']})"
        for entry in sorted(entries.values(), key=lambda e: -e["n"]))
    page.write(f"Heard with the right letters and only capitalized, so a "
               f"key term is needed only for a name: {listed}.\n\n")


def _closings(page, lines):
    page.write("## How sentences closed\n\n")
    counts = {}
    for line in lines:
        counts[line["reason"]] = counts.get(line["reason"], 0) + 1
    for reason, count in sorted(counts.items(), key=lambda pair: -pair[1]):
        page.write(f"- {reason}: {count}\n")
    ceilings = [line for line in lines if line["reason"] == "ceiling"]
    if ceilings:
        page.write(f"\n{len(ceilings)} sentences hit the ceiling, which "
                   "means they were translated half-finished. A lot of "
                   "these means people are talking over each other; raising "
                   "`ceiling` trades latency for coherence.\n\n")
        for line in ceilings:
            page.write(f"- {line['heard']}\n")
    page.write("\n")


def spoken_times(handle, session_id, lines):
    """For each line's seq, the wall clock its first and last words were
    spoken, or nothing for a recording without word times.

    The words of a run's fragments, in order, are the words of its lines,
    since the segmenter regroups them and drops none, so each line takes
    the next as many words as it has. A run whose counts disagree is left
    out rather than misaligned.
    """
    if "words" not in columns(handle, "fragments"):
        return {}
    spoken = {}
    by_run = {}
    for row in handle.execute(
            "SELECT run, at, start, audio_end, text, words, lag"
            " FROM fragments WHERE session_id = ? AND text <> ''"
            " ORDER BY id", (session_id,)):
        by_run.setdefault(row["run"], []).append(row)
    for run, fragments in by_run.items():
        times = []
        for fragment in fragments:
            if fragment["lag"] is None:
                times = None
                break
            count = len(fragment["text"].split())
            words = [(start, finish) for _, start, finish
                     in json.loads(fragment["words"] or "[]")]
            if len(words) != count:
                # smart_format can write "5PM" for the words "5" and
                # "PM", so the transcript's words are spread over the
                # span its words were spoken in instead.
                begin, close = ((words[0][0], words[-1][1]) if words
                                else (fragment["start"],
                                      fragment["audio_end"]))
                step = (close - begin) / count
                words = [(begin + index * step, begin + (index + 1) * step)
                         for index in range(count)]
            # Where audio_end sat on the wall clock, from its lag as it
            # arrived, and every word back from there.
            end = fragment["at"] - fragment["lag"]
            times.extend((end - (fragment["audio_end"] - start),
                          end - (fragment["audio_end"] - finish))
                         for start, finish in words)
        ours = [line for line in lines if line["run"] == run]
        if times is None or len(times) != sum(
                len(line["heard"].split()) for line in ours):
            continue
        position = 0
        for line in ours:
            count = len(line["heard"].split())
            if not count:
                continue
            spoken[line["seq"]] = (times[position][0],
                                   times[position + count - 1][1])
            position += count
    return spoken


def _latency(page, handle, session, lines):
    page.write("## Latency\n\n")
    lags = [line["lag"] for line in lines if line["lag"] is not None]
    waits = [line["latency"] for line in lines
             if line["latency"] is not None]
    spoken = spoken_times(handle, session["id"], lines)
    timed = [line for line in lines
             if line["seq"] in spoken and line["at"] is not None]
    if timed:
        first = [line["at"] - spoken[line["seq"]][0] for line in timed]
        last = [line["at"] - spoken[line["seq"]][1] for line in timed]
        page.write(f"From a line's first word being spoken to the English "
                   f"line appearing: {latency_summary(first)}.\n\n")
        page.write(f"From its last word: {latency_summary(last)}.\n\n")
        translated = [line["at"] + line["latency"] - spoken[line["seq"]][0]
                      for line in timed if line["latency"] is not None]
        if translated:
            page.write(f"From its first word to the translation appearing: "
                       f"{latency_summary(translated)}.\n\n")
    if lags:
        # Not the reader's wait: the end of a piece is where the recognizer
        # stopped listening, which after a Finalize is the moment it was
        # sent, so this is mostly the recognizer's round trip.
        page.write(f"From the end of the audio the recognizer closed a "
                   f"line's last piece on, to the English line: "
                   f"{latency_summary(lags)}.\n\n")
    if waits:
        page.write(f"Translation, on top of that: "
                   f"{latency_summary(waits)}.\n\n")
        slowest = sorted(lines, key=lambda line: -(line["latency"] or 0))[:5]
        page.write("Slowest lines:\n\n")
        for line in slowest:
            if line["latency"]:
                page.write(f"- {line['latency']:.1f}s: {line['heard']}\n")
        page.write("\n")


def _pieces(page, handle, session):
    """How long the recognizer took to hand words over, and why it did.

    A word waits inside the recognizer until its whole piece is finished,
    so the length of a piece is a floor on the lag of its first word, and
    what ended the pieces says whether early_finalize is doing its work.
    """
    # A database from before the column is read as it is, not migrated,
    # and none of its pieces were asked to finish.
    finalized = ("finalized" if "finalized" in columns(handle, "fragments")
                 else "0")
    rows = handle.execute(
        f"SELECT audio_end - start AS length, speech_final,"
        f" {finalized} AS finalized"
        f" FROM fragments WHERE session_id = ? AND text <> ''",
        (session["id"],)).fetchall()
    if not rows:
        return
    settings = json.loads(session["settings"] or "{}")
    page.write("## Recognizer pieces\n\n")
    endings = {"finished on request at a sentence end": 0,
               "ended by a pause": 0, "ended on the recognizer's own clock": 0}
    for row in rows:
        if row["finalized"]:
            endings["finished on request at a sentence end"] += 1
        elif row["speech_final"]:
            endings["ended by a pause"] += 1
        else:
            endings["ended on the recognizer's own clock"] += 1
    page.write(f"{len(rows)} pieces, early_finalize "
               f"{'on' if settings.get('early_finalize') else 'off'}. "
               f"Length, which the first word of each waited out: "
               f"{latency_summary([row['length'] for row in rows])}.\n\n")
    for ending, count in endings.items():
        page.write(f"- {ending}: {count}\n")
    page.write("\n")
    _guesses(page, handle, session)


# What became of a guess that ended a sentence, as the report says it.
DECISIONS = {"sent": "finished on request", "stale": "passed up as stale",
             "off": "not asked, early_finalize off"}


# Seconds before its end that a word is taken to have begun, at the
# earliest. The recognizer pads a final word's start back to the end of
# the word before, so on the fourth field test the raw starts put 75
# guesses of 248 inside a word, where the onsets this allows put 55.
PADDED_ONSET = 0.3
# Seconds after a guess's last word before a word counts as the next one,
# since the guess and the final result place the same word slightly apart.
SAME_WORD = 0.03


def cut_into(guess, spoken):
    """Where the word after a guess had begun by the cut: quiet, unseen.

    The cut is all the audio sent when the guess arrived. Empty when the
    next word began after it, which is a finish that would cut nothing.
    """
    last = guess["audio_end"] - guess["quiet"]
    cut = guess["audio_end"] + guess["unseen"]
    for start, end in spoken:
        if start <= last + SAME_WORD:
            continue
        onset = max(start, end - PADDED_ONSET)
        if onset >= cut:
            return ""
        return "quiet" if onset < guess["audio_end"] else "unseen"
    return ""


def _guesses(page, handle, session):
    """How often a finish was asked for after speech had already resumed.

    A Finalize closes all the audio sent, which runs past what the guess
    asking for it heard. A word that began in between is cut in half or
    lost, as "Each" and "If" were on the third field test. With
    early_finalize off nothing is cut, and the words recorded afterwards
    say how many would have been.
    """
    if not columns(handle, "guesses"):
        return
    rows = handle.execute(
        "SELECT * FROM guesses WHERE session_id = ? ORDER BY id",
        (session["id"],)).fetchall()
    if not rows:
        return
    words = {}
    for fragment in handle.execute(
            "SELECT run, words FROM fragments WHERE session_id = ?"
            " AND words IS NOT NULL ORDER BY id", (session["id"],)):
        words.setdefault(fragment["run"], []).extend(
            (start, end) for _, start, end in json.loads(fragment["words"]))
    for spoken in words.values():
        spoken.sort()
    page.write("Guesses that ended a sentence, by what became of them, "
               "with the audio sent after each guess's window and how "
               "often the next word had already begun where a Finalize "
               "would cut, which is a word it then would cut. Most of "
               "those began inside the quiet the guess itself reported, "
               "too little of the word yet to transcribe. Where one was "
               "sent, a word it cut away entirely is not counted.\n\n")
    for decision, label in DECISIONS.items():
        chosen = [row for row in rows if row["decision"] == decision]
        if not chosen:
            continue
        unseen = [row["unseen"] for row in chosen
                  if row["unseen"] is not None]
        begun = [cut_into(row, words.get(row["run"], []))
                 for row in chosen if row["unseen"] is not None]
        cut = sum(1 for where in begun if where)
        inside = sum(1 for where in begun if where == "quiet")
        after = (f", unseen audio {latency_summary(unseen)}, a word already "
                 f"begun at the cut {cut} times, {inside} of them inside "
                 f"the guess's own quiet" if unseen else "")
        page.write(f"- {label}: {len(chosen)}{after}\n")
    page.write("\n")


# Seconds without a word before a stretch is worth a line in the report.
# A pause between speakers is a few seconds; what the second field test
# lost was half a minute at a time.
SILENCE_REPORTED = 20
# Below this RMS level a stretch is a room nobody is speaking into. Speech
# on a sound board feed sits around -35 to -15 dBFS, and an idle line is
# well under -60.
QUIET_DB = -50
# What share of a stretch has to have been measured, or to have reached the
# recognizer, before the next explanation down the list is the right one.
DELIVERED = 0.5
# Decibels a stretch has to sit from the session's own speech before it
# reads as louder or quieter than somebody talking. On the third field test
# the hymns were 8 to 15 above the talks and the opening prayer 7 below,
# and all of them were reported alike as speaking level.
LEVEL_MARGIN = 6
# A change this large from one window to the next splits a stretch, since
# one median over a hymn and the prayer after it describes neither.
LEVEL_SPLIT = 10


def audio_rows(handle, session_id):
    """The audio windows of one session, or none for an older recording.

    Reading a session back must not write to the file it is reading, so an
    older recording is never migrated and has only the tables it was made
    with.
    """
    if not columns(handle, "audio"):
        return []
    return handle.execute(
        "SELECT * FROM audio WHERE session_id = ? ORDER BY id",
        (session_id,)).fetchall()


def silent_stretches(rows):
    """Runs of consecutive windows in which nothing with words came back.

    Each is (start, end, windows). A row is stamped when its window closes,
    so a stretch starts where the row before it ended, and a stretch that
    opens a run starts one window before its first row.
    """
    stretches, current, start, previous = [], [], None, None
    for row in rows:
        silent = (row["results"] or 0) == (row["empty"] or 0)
        same_run = previous is not None and previous["run"] == row["run"]
        if current and not (silent and same_run):
            stretches.append((start, current))
            current = []
        if silent:
            if not current:
                start = previous["at"] if same_run else None
            current.append(row)
        previous = row
    if current:
        stretches.append((start, current))
    result = []
    for start, windows in stretches:
        if start is None:
            start = windows[0]["at"] - AUDIO_WINDOW
        result.extend(split_by_level(start, windows))
    return result


def split_by_level(start, windows):
    """One stretch as (start, end, windows) parts, cut where the level jumps.

    Each window is compared with the last few of the part it would join,
    and a part ends only where this window and the next both differ, so
    a single loud bang inside a hymn does not split it.
    """
    parts, current = [], []
    for index, row in enumerate(windows):
        recent = sorted(window["rms_db"] for window in current[-3:]
                        if window["rms_db"] is not None)
        ahead = [window["rms_db"] for window in windows[index:index + 2]]
        if (recent and len(ahead) == 2 and None not in ahead
                and all(abs(level - recent[len(recent) // 2]) > LEVEL_SPLIT
                        for level in ahead)):
            parts.append((start, current[-1]["at"], current))
            start, current = current[-1]["at"], []
        current.append(row)
    parts.append((start, current[-1]["at"], current))
    return parts


def speech_level(rows):
    """The median level of the windows the recognizer found words in.

    The yardstick for a stretch with none, since a feed's speaking level
    depends on the room, the microphone, and the gain, and a fixed number
    cannot know any of them.
    """
    levels = sorted(row["rms_db"] for row in rows
                    if row["rms_db"] is not None
                    and (row["results"] or 0) > (row["empty"] or 0))
    return levels[len(levels) // 2] if levels else None


def explain_silence(seconds, windows, speech=None):
    """Which link in the chain a stretch with no words points at.

    Walked in the order the audio travels, so the first link that lost it
    is the one named. Speech is the session's own speaking level, and
    without it a level above QUIET_DB can only be called speaking level.
    """
    measured = [row for row in windows if row["captured"] is not None]
    if measured and sum(row["captured"] for row in measured) \
            < DELIVERED * seconds:
        return "the microphone delivered little audio"
    received = [row["received"] for row in windows
                if row["received"] is not None]
    if received and sum(received) < DELIVERED * seconds:
        return ("little audio reached the recognizer: the microphone, or "
                "on a hosted server the sender or the network")
    if not measured:
        return "no levels were reported, so nothing more can be said"
    levels = sorted(row["rms_db"] for row in measured
                    if row["rms_db"] is not None)
    if levels and levels[len(levels) // 2] < QUIET_DB:
        return "the feed was quiet, so nobody was speaking into it"
    if sum(row["results"] or 0 for row in windows) == 0:
        return "audio at speaking level, and the recognizer sent nothing back"
    if speech is not None and levels:
        level = levels[len(levels) // 2]
        if level > speech + LEVEL_MARGIN:
            return ("louder than the speech in this session, with no words "
                    "in it: singing or music")
        if level < speech - LEVEL_MARGIN:
            return ("quieter than the speech in this session, with no words "
                    "in it: someone away from the microphone, or a quiet "
                    "room")
        return ("at the level of the speech in this session, and the "
                "recognizer heard no words in it")
    return "audio at speaking level, and the recognizer heard no words in it"


def _silences(page, handle, session, lines):
    """Stretches with no words, and where along the way they were lost.

    The second field test lost half minutes, often as the speaker changed,
    with nothing recorded to say whether the audio was silent, quiet,
    missing, or heard and not understood. Each window of the audio table
    answers that, and the lines either side say who was speaking.
    """
    rows = audio_rows(handle, session["id"])
    if not rows:
        return
    page.write("## Stretches with no words\n\n")
    stretches = [stretch for stretch in silent_stretches(rows)
                 if stretch[1] - stretch[0] >= SILENCE_REPORTED]
    if not stretches:
        page.write(f"None as long as {SILENCE_REPORTED} seconds.\n\n")
        return
    speech = speech_level(rows)
    against = (f" Speech in this session sat at {speech:.0f} dBFS, which "
               f"is what each stretch is compared with." if speech is not None
               else "")
    page.write(f"Every stretch of {SILENCE_REPORTED} seconds or more in "
               f"which the recognizer returned no words, with the median "
               f"level of the feed and the loudest moment in it, split "
               f"where the level changes.{against}\n\n")
    began = session["started_at"]
    for start, end, windows in stretches:
        seconds = end - start
        levels = sorted(row["rms_db"] for row in windows
                        if row["rms_db"] is not None)
        peaks = [row["peak_db"] for row in windows
                 if row["peak_db"] is not None]
        level = (f"level {levels[len(levels) // 2]:.0f} dBFS, "
                 f"peak {max(peaks):.0f} dBFS" if levels else "no levels")
        page.write(f"- **{(start - began) / 60:.1f} min**, "
                   f"{seconds:.0f}s, {level}: "
                   f"{explain_silence(seconds, windows, speech)}.\n")
        before = [line for line in lines if (line["at"] or 0) <= start]
        after = [line for line in lines if (line["at"] or 0) >= end]
        if before:
            page.write(f"  - before: {before[-1]['heard']}\n")
        if after:
            page.write(f"  - after: {after[0]['heard']}\n")
    page.write("\n")


def _tokens(page, lines):
    """What the translation calls cost, for weighing a segmentation change.

    Every line is a call, and the prompt goes with every call, so a change
    that makes lines shorter is paid for in prompt tokens.
    """
    counted = [line for line in lines
               if "prompt_tokens" in line.keys()
               and line["prompt_tokens"] is not None]
    if not counted:
        return
    page.write("## Tokens\n\n")
    page.write(f"{len(counted)} calls reported what they cost.\n\n")
    page.write("| | total | per call |\n|---|---|---|\n")
    for column in TOKEN_COLUMNS:
        total = sum(line[column] or 0 for line in counted)
        page.write(f"| {column.removesuffix('_tokens')} | {total} "
                   f"| {total / len(counted):.0f} |\n")
    page.write("\n")


def _fallbacks(page, grouped):
    page.write("## Where readers saw English instead\n\n")
    counts = {}
    for rows in grouped.values():
        for row in rows:
            if row["source"] == "model":
                continue
            counts.setdefault(row["language"], {})
            by_source = counts[row["language"]]
            by_source[row["source"]] = by_source.get(row["source"], 0) + 1
    if not counts:
        page.write("Every requested translation arrived.\n\n")
        return
    page.write("| language | timed out | failed | model dropped it "
               "| over the cap |\n")
    page.write("|---|---|---|---|---|\n")
    for language, by_source in sorted(counts.items()):
        page.write(f"| {language} | {by_source.get('timeout', 0)} "
                   f"| {by_source.get('failure', 0)} "
                   f"| {by_source.get('empty', 0)} "
                   f"| {by_source.get('capped', 0)} |\n")
    page.write("\n")


def _transcript(page, lines, grouped):
    """Side by side, in the shape review.py already writes."""
    page.write("## Transcript\n\n")
    page.write(REVIEW_INSTRUCTIONS)
    for line in lines:
        page.write(f"### {line['seq']}\n\n")
        page.write(f"**Heard.** {line['heard']}\n\n")
        if line["english"] and line["english"] != line["heard"]:
            page.write(f"**English.** {line['english']}\n\n")
        for row in grouped.get(line["seq"], []):
            mark = "" if row["source"] == "model" else f" _({row['source']})_"
            page.write(f"**{row['language']}.**{mark} {row['text']}\n\n")
            page.write("> Comments:\n\n")


def write_plain(handle, session, out):
    """One English sentence per line, which review.py --input already reads."""
    lines = lines_of(handle, session["id"])
    with open(out, "w", encoding="utf-8") as handle_out:
        for line in lines:
            handle_out.write((line["english"] or line["heard"]) + "\n")
    return len(lines)


def purge(handle, older_than_days):
    cutoff = time.time() - older_than_days * 86400
    doomed = [row["id"] for row in handle.execute(
        "SELECT id FROM sessions WHERE started_at < ?", (cutoff,))]
    for session_id in doomed:
        handle.execute("DELETE FROM translations WHERE session_id = ?",
                       (session_id,))
        handle.execute("DELETE FROM lines WHERE session_id = ?",
                       (session_id,))
        handle.execute("DELETE FROM fragments WHERE session_id = ?",
                       (session_id,))
        for table in ("audio", "guesses"):
            if columns(handle, table):
                handle.execute(f"DELETE FROM {table} WHERE session_id = ?",
                               (session_id,))
        handle.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
    handle.commit()
    return len(doomed)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database", default="sessions.db")
    parser.add_argument("--list", action="store_true",
                        help="show what has been recorded")
    parser.add_argument("--session", help="a session id, or last")
    parser.add_argument("--out", help="write the review document here")
    parser.add_argument("--plain", help="write plain sentences here, for "
                                        "review.py --input")
    parser.add_argument("--purge", action="store_true",
                        help="delete old sessions; needs --older-than")
    parser.add_argument("--older-than", type=float,
                        help="days, for --purge")
    args = parser.parse_args()

    handle = open_read_only(args.database)

    if args.purge:
        if args.older_than is None:
            sys.exit("--purge needs --older-than, in days. Nothing is "
                     "deleted without a number.")
        removed = purge(handle, args.older_than)
        print(f"Deleted {removed} session(s) older than "
              f"{args.older_than:g} days.")
        return

    if args.list or not (args.out or args.plain):
        rows = sessions(handle)
        if not rows:
            print(f"Nothing recorded in {args.database}.")
            return
        for row in rows:
            count = handle.execute(
                "SELECT count(*) FROM lines WHERE session_id = ?",
                (row["id"],)).fetchone()[0]
            print(f"  {row['id']:>4}  {stamp(row['started_at'])}  "
                  f"{count:>5} lines  {row['model']}")
        return

    session = resolve_session(handle, args.session)
    if args.out:
        write_report(handle, session, args.out)
        print(f"Wrote {args.out}")
    if args.plain:
        count = write_plain(handle, session, args.plain)
        print(f"Wrote {count} sentences to {args.plain}")


if __name__ == "__main__":
    main()
