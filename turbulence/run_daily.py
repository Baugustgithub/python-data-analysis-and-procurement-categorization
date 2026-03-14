"""
Daily CLI runner
================
Run from project root:
    python -m turbulence.run_daily

Prints today's regime snapshot and optionally generates an AI memo.
Set ANTHROPIC_API_KEY to enable the AI memo.
"""

from __future__ import annotations

import os
import sys

import pandas as pd

from turbulence.engine import run, latest_snapshot, REGIME_COLORS
from turbulence.ai_summary import generate_summary


RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
ORANGE = "\033[38;5;208m"


def regime_color(r: str) -> str:
    return {"GREEN": GREEN, "YELLOW": YELLOW, "ORANGE": ORANGE, "RED": RED}.get(r, "")


def main() -> None:
    print("Loading turbulence model…")
    features = run(use_cache=True)
    snap     = latest_snapshot(features)

    r   = snap["regime"]
    c   = snap["composite"]
    rc  = regime_color(r)

    print(f"\n{'='*60}")
    print(f"  Date      : {snap['date']}")
    print(f"  Composite : {c:.2f}")
    print(f"  Regime    : {rc}{BOLD}{r}{RESET}")
    print(f"{'='*60}")

    sub_fields = [
        ("Turbulence z",     "turbulence_z"),
        ("Credit stress",    "credit_stress"),
        ("Financial rel",    "financial_rel"),
        ("Software rel",     "software_rel"),
        ("Alt-manager rel",  "alt_manager_rel"),
        ("BTC rel",          "btc_rel"),
        ("Corr break",       "corr_break"),
    ]
    print("\nSub-factor z-scores:")
    for label, key in sub_fields:
        v = snap.get(key)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        bar = "█" * int(abs(v)) if abs(v) < 10 else "█" * 9 + "+"
        sign = "+" if v > 0 else ""
        print(f"  {label:<20} {sign}{v:>6.2f}  {bar}")

    # 5-day trend
    recent = features["composite"].dropna().tail(6)
    trend  = recent.diff().mean()
    direction = "↑ rising" if trend > 0.05 else "↓ falling" if trend < -0.05 else "→ flat"
    print(f"\n  5-day trend: {trend:+.3f}/day  {direction}")

    # AI memo
    if os.environ.get("ANTHROPIC_API_KEY"):
        print("\n─── AI Regime Memo ─────────────────────────────────────")
        memo = generate_summary(features, snap)
        print(memo)
        print("────────────────────────────────────────────────────────")


if __name__ == "__main__":
    main()
