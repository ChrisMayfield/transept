#!/usr/bin/env python3
"""
Compare translation models and review their output offline.

Reads English sentences one per line, translates each into every target
language, reports per-call latency, and can write a side-by-side Markdown
file for a native speaker to mark up.

Use this before trusting a language in a live meeting. A reviewer cannot
judge text that only ever scrolled past on a phone, and running the same
input through two models produces documents you can compare blind.

This shares the translator, the prompt, and the glossary handling with the
live pipeline, so what you review here is what a reader will see. Only the
retry policy differs: waiting out a failure beats losing a line when nobody
is waiting on the answer.

Usage:
    python3 review.py --input transcript.txt --model gemini-3.8-flash
    python3 review.py --input transcript.txt --languages "French,Swahili" \\
        --review review.md

Input may be a plain transcript or raw pipeline.py output. In the second
case the sentence prefix comes off and the fragment and translation lines
are dropped, unless --keep-prefix is given.
"""

import argparse
import asyncio
import re
import sys

from pipeline import (CONTEXT_UNITS, Translator, add_settings_arguments,
                      latency_summary, load_config, load_glossary, load_keys,
                      resolve)

ANSI_RE = re.compile(r"\033\[[0-9;]*m")
# pipeline.py prints a finished sentence as "[12] en (0.4s, endpoint): text".
SENTENCE_RE = re.compile(r"^\[\d+\]\s+en\s+\([^)]*\):\s*")
# Everything else it prints is decoration: a middle dot line is a raw
# recognition fragment, and translations are indented under their sentence.
DECORATION_RE = re.compile(r"^(\s+\.\s|\s{5,}\S)")


def read_lines(path, keep_prefix, limit):
    """English sentences, one per line, as __doc__ describes."""
    handle = open(path, encoding="utf-8") if path else sys.stdin
    try:
        lines = []
        for raw in handle:
            text = ANSI_RE.sub("", raw).rstrip()
            if not keep_prefix:
                found = SENTENCE_RE.match(text)
                if found:
                    text = text[found.end():]
                elif DECORATION_RE.match(text):
                    continue
            text = text.strip()
            if text:
                lines.append(text)
    finally:
        if path:
            handle.close()
    return lines[:limit] if limit else lines


def write_review(path, model, outputs, rows):
    """Side-by-side Markdown for a native speaker to mark up."""
    with open(path, "w", encoding="utf-8") as out:
        out.write("# Translation review\n\n")
        out.write(f"Model: `{model}`\n\n")
        out.write(f"Lines: {len(rows)}\n\n")
        out.write("Please mark anything that is wrong, unnatural, or would "
                  "confuse someone reading it on a phone during a meeting.\n")
        out.write("Wrong meaning matters most.\n")
        out.write("Note awkward phrasing separately, since the two need "
                  "different fixes.\n\n")
        for index, row in enumerate(rows, start=1):
            out.write(f"## {index}\n\n")
            out.write(f"**Heard.** {row['source']}\n\n")
            for name in outputs:
                text = row["translations"].get(name, "")
                out.write(f"**{name}.** {text}\n\n")
                out.write("> Comments:\n\n")


async def run(args, settings, keys):
    api_key = keys["llm_key"]
    base_url = keys["llm_base"]
    if not api_key or not base_url:
        sys.exit("Set llm_api_key and llm_base_url under [keys] in "
                 "config.toml")
    if not settings["model"]:
        sys.exit("No model. Set translation.model in config.toml or pass "
                 "--model.")

    lines = read_lines(args.input, args.keep_prefix, args.limit)
    if not lines:
        sys.exit("No input lines.")

    translator = Translator(
        base_url, api_key, settings["model"], settings["languages"],
        load_glossary(settings["glossary"]), settings["max_tokens"],
        settings["timeout"], settings["reasoning_effort"],
        settings["correct_english"],
        max_attempts=args.attempts, retry_delay=0.5)

    context, rows, latencies, failures = [], [], [], 0
    try:
        for index, line in enumerate(lines, start=1):
            try:
                translations, elapsed = await translator.translate(
                    line, list(context))
            except RuntimeError as exc:
                failures += 1
                print(f"[{index}] failed: {exc}", file=sys.stderr)
                context = (context + [line])[-CONTEXT_UNITS:]
                continue
            latencies.append(elapsed)

            print(f"[{index}] {elapsed:.2f}s")
            print(f"  heard: {line}")
            for name in translator.outputs:
                print(f"  {name[:2].lower()}: {translations.get(name, '')}")
            print()

            rows.append({"source": line, "translations": translations})
            context = (context + [line])[-CONTEXT_UNITS:]
    finally:
        await translator.close()

    if latencies:
        print(f"{len(latencies)} translated, {failures} failed. "
              f"{latency_summary(latencies)}", file=sys.stderr)

    if args.review and rows:
        write_review(args.review, settings["model"], translator.outputs, rows)
        print(f"Wrote {args.review}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input",
                        help="one sentence per line; omit for stdin")
    parser.add_argument("--review", help="write side-by-side Markdown here")
    parser.add_argument("--limit", type=int, help="only the first N lines")
    parser.add_argument("--attempts", type=int, default=4,
                        help="retries per line; higher than the live pipeline "
                             "because nothing is waiting on the answer")
    parser.add_argument("--keep-prefix", action="store_true",
                        help="take every input line verbatim")
    parser.add_argument("--config", default="config.toml")
    add_settings_arguments(parser, [
        "model", "reasoning_effort", "correct_english", "max_tokens",
        "timeout",
        "languages",
        "glossary",
    ])
    args = parser.parse_args()
    config = load_config(args.config)
    asyncio.run(run(args, resolve(args, config), load_keys(config)))


if __name__ == "__main__":
    main()
