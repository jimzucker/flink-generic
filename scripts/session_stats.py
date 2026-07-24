#!/usr/bin/env python3
#Copyright 2021-2023 Ness Digital Engineering
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.
"""
Claude Code build-session stats for this repository.

Parses the local Claude Code transcripts (JSONL) for this repo and reports token
usage, prompt/turn counts, active time, and the metered-API-equivalent cost.

Transcripts live under:
    ~/.claude/projects/<repo-path-with-each-"/"-replaced-by-"-">/*.jsonl

Usage:
    python3 scripts/session_stats.py                 # terminal summary (all sessions)
    python3 scripts/session_stats.py --md            # also write docs/SESSION_STATS.md
    python3 scripts/session_stats.py --transcript X   # a single .jsonl file
    python3 scripts/session_stats.py --latest         # only the most recent session file
    python3 scripts/session_stats.py --rate-output 25 --rate-cache-read 0.50 ...

This script is the single source of truth for `/stats` -- do not hand-calculate.

Default rates are Anthropic's published Opus 4.8 pricing (USD per million tokens):
    input $5, output $25, 5-min cache write $6.25, cache read $0.50
Override any of them with the --rate-* flags. The cost is the metered-API
equivalent; on a flat Claude Code subscription the build is effectively included.
Note: the 1M-context ("[1m]") Opus variant carries a long-context premium for
requests over 200K input tokens that this base estimate does not apply.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import subprocess
import sys
from datetime import datetime

IDLE_GAP_SECONDS = 300  # gaps longer than this are idle time (excluded from "active")

DEFAULT_RATES = {  # USD per million tokens (Opus 4.8)
    "input": 5.0,
    "output": 25.0,
    "cache_write": 6.25,
    "cache_read": 0.50,
}


def repo_root() -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], stderr=subprocess.DEVNULL
        )
        return out.decode().strip()
    except Exception:
        return os.getcwd()


def transcript_dir(root: str) -> str:
    # Claude Code sanitizes the absolute repo path by replacing every "/" with "-".
    sanitized = os.path.abspath(root).replace("/", "-")
    return os.path.join(os.path.expanduser("~/.claude/projects"), sanitized)


def load_records(files: list[str]) -> list[dict]:
    recs: list[dict] = []
    for f in files:
        try:
            with open(f, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        recs.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        except OSError:
            pass
    return recs


def is_human_prompt(rec: dict) -> bool:
    """A real human message: type==user, not meta, not a tool-result, not a slash/local command."""
    if rec.get("type") != "user" or rec.get("isMeta"):
        return False
    content = (rec.get("message") or {}).get("content")
    if isinstance(content, list):
        if any(isinstance(x, dict) and x.get("type") == "tool_result" for x in content):
            return False
        text = " ".join(x.get("text", "") for x in content if isinstance(x, dict))
    else:
        text = content or ""
    text = text.strip()
    if not text:
        return False
    markers = ("<command-name>", "<command-message>", "<local-command-stdout>", "local-command")
    if text.startswith("/") or any(mk in text for mk in markers):
        return False
    return True


def parse_ts(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def compute(recs: list[dict], rates: dict) -> dict:
    out = inp = cw = cr = 0
    turns = prompts = 0
    events = []  # (timestamp, type)

    for r in recs:
        t = r.get("type")
        ts = parse_ts(r.get("timestamp"))
        if ts is not None:
            events.append((ts, t))
        if t == "assistant":
            turns += 1
            u = (r.get("message") or {}).get("usage") or {}
            out += u.get("output_tokens", 0) or 0
            inp += u.get("input_tokens", 0) or 0
            cw += u.get("cache_creation_input_tokens", 0) or 0
            cr += u.get("cache_read_input_tokens", 0) or 0
        elif is_human_prompt(r):
            prompts += 1

    events.sort(key=lambda e: e[0])
    active = user_t = claude_t = 0.0
    for i in range(1, len(events)):
        gap = (events[i][0] - events[i - 1][0]).total_seconds()
        if 0 <= gap <= IDLE_GAP_SECONDS:
            active += gap
            if events[i][1] == "user":
                user_t += gap
            else:
                claude_t += gap
    span = (events[-1][0] - events[0][0]).total_seconds() if len(events) > 1 else 0.0

    cost = (
        inp / 1e6 * rates["input"]
        + out / 1e6 * rates["output"]
        + cw / 1e6 * rates["cache_write"]
        + cr / 1e6 * rates["cache_read"]
    )
    return {
        "output": out, "input": inp, "cache_write": cw, "cache_read": cr,
        "total_tokens": out + inp + cw + cr,
        "turns": turns, "prompts": prompts,
        "active": active, "user_time": user_t, "claude_time": claude_t, "span": span,
        "first": events[0][0] if events else None, "last": events[-1][0] if events else None,
        "cost": cost, "rates": rates,
    }


def fmt_h(sec: float) -> str:
    return f"{sec / 3600:.1f}h" if sec >= 3600 else f"{sec / 60:.0f}m"


def fmt_m(tok: int) -> str:
    return f"{tok / 1e6:.2f}M"


def render_terminal(s: dict, n_files: int) -> str:
    ra = s["rates"]
    dollars = {
        "output": s["output"] / 1e6 * ra["output"],
        "input": s["input"] / 1e6 * ra["input"],
        "cache_write": s["cache_write"] / 1e6 * ra["cache_write"],
        "cache_read": s["cache_read"] / 1e6 * ra["cache_read"],
    }
    span = fmt_h(s["span"])
    first = s["first"].strftime("%Y-%m-%d %H:%M") if s["first"] else "?"
    last = s["last"].strftime("%Y-%m-%d %H:%M") if s["last"] else "?"
    lines = [
        f"Claude Code session stats  ({n_files} transcript file(s))",
        f"  {first} -> {last}   span {span}",
        "-" * 60,
        f"  output tokens   : {fmt_m(s['output']):>8}   ${dollars['output']:,.2f}",
        f"  input tokens    : {fmt_m(s['input']):>8}   ${dollars['input']:,.2f}",
        f"  cache write     : {fmt_m(s['cache_write']):>8}   ${dollars['cache_write']:,.2f}",
        f"  cache read      : {fmt_m(s['cache_read']):>8}   ${dollars['cache_read']:,.2f}",
        f"  TOTAL tokens    : {fmt_m(s['total_tokens']):>8}",
        "-" * 60,
        f"  human prompts   : {s['prompts']}",
        f"  assistant turns : {s['turns']}",
        f"  active time     : {fmt_h(s['active'])}  (Claude {fmt_h(s['claude_time'])}, user {fmt_h(s['user_time'])})",
        f"  est. cost       : ${s['cost']:,.2f}   (metered-API equivalent)",
    ]
    return "\n".join(lines)


def render_md(s: dict, n_files: int) -> str:
    ra = s["rates"]
    d = {
        "output": s["output"] / 1e6 * ra["output"],
        "input": s["input"] / 1e6 * ra["input"],
        "cache_write": s["cache_write"] / 1e6 * ra["cache_write"],
        "cache_read": s["cache_read"] / 1e6 * ra["cache_read"],
    }
    first = s["first"].strftime("%Y-%m-%d %H:%M") if s["first"] else "?"
    last = s["last"].strftime("%Y-%m-%d %H:%M") if s["last"] else "?"
    return "\n".join([
        "# Session stats",
        "",
        f"_Generated from {n_files} Claude Code transcript file(s); "
        f"{first} → {last} (span {fmt_h(s['span'])})._",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Human prompts | {s['prompts']} |",
        f"| Assistant turns | {s['turns']} |",
        f"| Active time | {fmt_h(s['active'])} (Claude {fmt_h(s['claude_time'])}, user {fmt_h(s['user_time'])}) |",
        f"| Output tokens | {fmt_m(s['output'])} (${d['output']:,.2f}) |",
        f"| Input tokens | {fmt_m(s['input'])} (${d['input']:,.2f}) |",
        f"| Cache write | {fmt_m(s['cache_write'])} (${d['cache_write']:,.2f}) |",
        f"| Cache read | {fmt_m(s['cache_read'])} (${d['cache_read']:,.2f}) |",
        f"| **Total tokens** | **{fmt_m(s['total_tokens'])}** |",
        f"| **Est. cost (metered-API equiv.)** | **${s['cost']:,.2f}** |",
        "",
        "> Priced at Opus 4.8 rates "
        f"(input ${ra['input']}, output ${ra['output']}, cache-write ${ra['cache_write']}, "
        f"cache-read ${ra['cache_read']} per MTok). The cost is the metered-API equivalent; "
        "on a flat Claude Code subscription the build is effectively included.",
        "",
    ])


def main() -> int:
    ap = argparse.ArgumentParser(description="Claude Code build-session stats for this repo.")
    ap.add_argument("--md", action="store_true", help="also write the Markdown report")
    ap.add_argument("--out", default="docs/SESSION_STATS.md", help="Markdown output path (with --md)")
    ap.add_argument("--transcript", help="parse a single .jsonl transcript instead of all")
    ap.add_argument("--latest", action="store_true", help="only the most recently modified session file")
    for k, v in DEFAULT_RATES.items():
        ap.add_argument(f"--rate-{k.replace('_', '-')}", type=float, default=v,
                        help=f"{k} rate per MTok (default {v})")
    args = ap.parse_args()

    rates = {k: getattr(args, f"rate_{k}") for k in DEFAULT_RATES}

    root = repo_root()
    if args.transcript:
        files = [args.transcript]
    else:
        tdir = transcript_dir(root)
        files = sorted(glob.glob(os.path.join(tdir, "*.jsonl")))
        if not files:
            print(f"No transcripts found in {tdir}", file=sys.stderr)
            return 1
        if args.latest:
            files = [max(files, key=os.path.getmtime)]

    recs = load_records(files)
    if not recs:
        print("No records parsed from transcripts.", file=sys.stderr)
        return 1

    stats = compute(recs, rates)
    print(render_terminal(stats, len(files)))

    if args.md:
        out_path = os.path.join(root, args.out) if not os.path.isabs(args.out) else args.out
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write(render_md(stats, len(files)))
        print(f"\nWrote {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
