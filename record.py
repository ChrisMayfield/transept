#!/usr/bin/env python3
"""
Keep each session, and turn it into something worth reading afterwards.

A meeting is recorded line by line into a SQLite file while it runs. Nothing
here is for the readers; it is for the three things that decide subtitle
quality and are otherwise edited on a hunch: SYSTEM_PROMPT, glossary.txt,
and keyterms.txt.

Recording is off unless [session] record is true in config.toml. Nothing is
ever deleted on its own; --purge is the only way, and it has to be typed.

Usage:
    python3 record.py --list
    python3 record.py --session last --out review/2026-09-13.md
    python3 record.py --session last --plain sunday.txt
    python3 record.py --purge --older-than 30

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

from pipeline import latency_summary

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
  id INTEGER PRIMARY KEY,
  started_at REAL, ended_at REAL,
  device TEXT, asr_model TEXT, model TEXT, languages TEXT,
  correct_english INTEGER, settings TEXT,
  glossary TEXT, keyterms TEXT, prompt_hash TEXT);

CREATE TABLE IF NOT EXISTS lines (
  id INTEGER PRIMARY KEY,
  session_id INTEGER NOT NULL, seq INTEGER NOT NULL, run INTEGER,
  at REAL, audio_end REAL, lag REAL, confidence REAL,
  heard TEXT, english TEXT, reason TEXT, latency REAL,
  outcome TEXT, requested TEXT,
  UNIQUE (session_id, seq));

CREATE TABLE IF NOT EXISTS translations (
  id INTEGER PRIMARY KEY,
  session_id INTEGER NOT NULL, seq INTEGER NOT NULL,
  language TEXT NOT NULL, text TEXT,
  source TEXT,
  UNIQUE (session_id, seq, language));

CREATE INDEX IF NOT EXISTS lines_by_session ON lines (session_id, seq);
CREATE INDEX IF NOT EXISTS translations_by_session
  ON translations (session_id, seq);
"""

# Queue depth. A meeting produces ten to twenty lines a minute, so anything
# this side of a hundred is a disk that has stopped answering, and dropping
# the record of a sentence beats holding the recognizer up for it.
QUEUE_DEPTH = 256


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
    return handle


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
                " prompt_hash) VALUES (?,?,?,?,?,?,?,?,?,?)",
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
    present = handle.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
        " AND name = 'sessions'").fetchone()
    if not present:
        sys.exit(f"{path} holds no sessions.")
    return handle


def sessions(handle):
    return handle.execute(
        "SELECT * FROM sessions ORDER BY started_at").fetchall()


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
        page.write(f"# Session {session['id']}, {stamp(session['started_at'])}"
                   "\n\n")
        minutes = ((session["ended_at"] or session["started_at"])
                   - session["started_at"]) / 60
        page.write(f"{len(lines)} sentences over {minutes:.0f} minutes. "
                   f"Device `{session['device']}`, recognizer "
                   f"`{session['asr_model']}`, translation "
                   f"`{session['model']}`.\n\n")
        page.write(f"Languages offered: {', '.join(languages) or 'none'}.\n\n")

        _fabrications(page, lines)
        _corrections(page, lines)
        _closings(page, lines)
        _latency(page, lines)
        _fallbacks(page, grouped)
        _transcript(page, lines, grouped)


def _fabrications(page, lines):
    """Proper nouns the correction introduced that were not in the audio.

    First, because it is the only mechanical check there is on the rule that
    the model must never invent a name, and a reader of a translated channel
    has no way to catch one.
    """
    page.write("## Names to check\n\n")
    suspects = []
    for line in lines:
        if not line["english"]:
            continue
        # fold=True: a name that was merely recapitalized is the glossary
        # doing its job. What belongs here is a word with no counterpart in
        # the audio at all.
        added, _ = changed_words(line["heard"], line["english"], fold=True)
        introduced = set(added)
        names = sorted({word for word in WORD.findall(line["english"])
                        if PROPER.match(word)
                        and word.lower() in introduced})
        if names:
            suspects.append((line, names))
    if not suspects:
        page.write("Nothing introduced a capitalized word that was not in "
                   "the audio.\n\n")
        return
    page.write("A capitalized word appears in the corrected line that was "
               "not in what the recognizer heard. Usually the glossary "
               "repairing a name, occasionally the model inventing one.\n\n")
    for line, names in suspects:
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

    counts = {}
    for line in available:
        added, removed = changed_words(line["heard"], line["english"])
        for word in added:
            key = word.lower()
            entry = counts.setdefault(key, {"word": word, "n": 0, "was": []})
            entry["n"] += 1
            entry["was"].extend(removed)
    if not counts:
        page.write("Every corrected line came back unchanged.\n\n")
        return
    page.write("The recognizer missed these and the glossary repaired them "
               "afterwards. Putting them in keyterms.txt gets them right at "
               "the source, so the line never has to be revised on a "
               "phone.\n\n")
    page.write("| term | times | heard instead |\n|---|---|---|\n")
    for entry in sorted(counts.values(), key=lambda e: -e["n"]):
        was = ", ".join(sorted(set(entry["was"]))[:4]) or "-"
        page.write(f"| {entry['word']} | {entry['n']} | {was} |\n")
    page.write("\n")


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


def _latency(page, lines):
    page.write("## Latency\n\n")
    lags = [line["lag"] for line in lines if line["lag"] is not None]
    waits = [line["latency"] for line in lines
             if line["latency"] is not None]
    if lags:
        page.write(f"Subtitle lag, from the words being spoken to the "
                   f"English line appearing: {latency_summary(lags)}.\n\n")
    if waits:
        page.write(f"Translation, on top of that: "
                   f"{latency_summary(waits)}.\n\n")
        slowest = sorted(lines, key=lambda line: -(line["latency"] or 0))[:5]
        page.write("Slowest lines:\n\n")
        for line in slowest:
            if line["latency"]:
                page.write(f"- {line['latency']:.1f}s: {line['heard']}\n")
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
    page.write("Please mark anything that is wrong, unnatural, or would "
               "confuse someone reading it on a phone during a meeting.\n")
    page.write("Wrong meaning matters most.\n")
    page.write("Note awkward phrasing separately, since the two need "
               "different fixes.\n\n")
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
