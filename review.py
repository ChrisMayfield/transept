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

Setup:
    pip install -r requirements.txt
    cp .env.example .env   and fill it in

Usage:
    python3 review.py --input transcript.txt --model gemini-3.8-flash
    python3 review.py --input transcript.txt --languages "French,Swahili" \\
        --review review.md

Input may be raw pipeline.py output; the timestamp prefix is stripped
automatically unless --keep-prefix is given.
"""

import argparse
import asyncio
import re
import statistics
import sys
import os

from pipeline import (Translator, add_settings_arguments, load_config,
                      load_env, load_glossary, resolve)

# Matches the "[  27.9s  lag  0.3s +]" prefix that pipeline.py prints.
PREFIX_RE = re.compile(r"^\[\s*[\d.]+s\s+lag\s+[\d.-]+s\s+\S\]\s*")


def read_lines(path, keep_prefix, limit):
    handle = open(path, encoding="utf-8") if path else sys.stdin
    try:
        lines = []
        for raw in handle:
            text = raw.strip()
            if not keep_prefix:
                text = PREFIX_RE.sub("", text)
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
                out.write(f"**{name}.** {row['translations'].get(name, '')}\n\n")
                out.write("> Comments:\n\n")


async def run(args, settings):
    load_env()
    api_key = os.environ.get("LLM_API_KEY")
    base_url = os.environ.get("LLM_BASE_URL")
    if not api_key or not base_url:
        sys.exit("Set LLM_API_KEY and LLM_BASE_URL in .env")
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
                context = (context + [line])[-3:]
                continue
            latencies.append(elapsed)

            print(f"[{index}] {elapsed:.2f}s")
            print(f"  heard: {line}")
            for name in translator.outputs:
                print(f"  {name[:2].lower()}: {translations.get(name, '')}")
            print()

            rows.append({"source": line, "translations": translations})
            context = (context + [line])[-3:]
    finally:
        await translator.close()

    if latencies:
        ordered = sorted(latencies)
        p95 = ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]
        print(f"{len(latencies)} translated, {failures} failed. "
              f"median {statistics.median(latencies):.2f}s, p95 {p95:.2f}s",
              file=sys.stderr)

    if args.review and rows:
        write_review(args.review, settings["model"], translator.outputs, rows)
        print(f"Wrote {args.review}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", help="one sentence per line; omit for stdin")
    parser.add_argument("--review", help="write side-by-side Markdown here")
    parser.add_argument("--limit", type=int, help="only the first N lines")
    parser.add_argument("--attempts", type=int, default=4,
                        help="retries per line; higher than the live pipeline "
                             "because nothing is waiting on the answer")
    parser.add_argument("--keep-prefix", action="store_true",
                        help="do not strip pipeline.py timestamps")
    parser.add_argument("--config", default="config.toml")
    add_settings_arguments(parser, [
        "model", "languages", "glossary", "max_tokens", "timeout",
        "reasoning_effort", "correct_english",
    ])
    args = parser.parse_args()
    asyncio.run(run(args, resolve(args, load_config(args.config))))


if __name__ == "__main__":
    main()
