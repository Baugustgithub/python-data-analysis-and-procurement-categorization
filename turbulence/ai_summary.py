"""
AI Regime Summary
=================
Uses the Anthropic Claude API to generate a concise daily regime memo
from the latest turbulence signal values.
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd


def _build_prompt(features: pd.DataFrame, snapshot: dict) -> str:
    """Construct the regime-summary prompt with historical context."""
    # Pull a few known stress windows for analog comparison
    analogs = []
    for label, (lo, hi) in {
        "COVID crash (Feb–Mar 2020)":       ("2020-02-01", "2020-04-01"),
        "Fed tightening / growth unwind 2022": ("2022-01-01", "2022-10-31"),
        "Regional bank stress Mar 2023":    ("2023-03-01", "2023-06-01"),
    }.items():
        window = features.loc[lo:hi, "composite"].dropna()
        if not window.empty:
            analogs.append(
                f"  - {label}: peak composite = {window.max():.2f}, "
                f"regime reached {window.map(lambda x: 'RED' if x>=3 else 'ORANGE' if x>=2 else 'YELLOW' if x>=1 else 'GREEN').value_counts().idxmax()}"
            )

    # Recent 5-day trend
    recent = features["composite"].dropna().tail(6)
    trend  = recent.diff().mean()
    trend_str = f"+{trend:.2f}/day (rising)" if trend > 0 else f"{trend:.2f}/day (falling)"

    sub_cols = [
        "turbulence_z", "credit_stress", "financial_rel",
        "software_rel", "alt_manager_rel", "btc_rel", "corr_break",
    ]
    present = [(c, snapshot[c]) for c in sub_cols if c in snapshot and not pd.isna(snapshot.get(c))]
    present.sort(key=lambda t: abs(t[1]), reverse=True)
    top_drivers = "\n".join(f"  - {c}: {v:.2f}" for c, v in present[:4])

    analog_str = "\n".join(analogs) if analogs else "  - (insufficient data for analog comparison)"

    return f"""You are a quantitative risk analyst. Summarise today's market fragility reading concisely.

TODAY'S READING
  Date        : {snapshot['date']}
  Composite   : {snapshot['composite']:.2f}
  Regime      : {snapshot['regime']}
  5-day trend : {trend_str}

TOP SUB-FACTOR CONTRIBUTORS (highest absolute z-score)
{top_drivers}

HISTORICAL ANALOG WINDOWS
{analog_str}

Write a 6–8 sentence daily regime memo covering:
1. Current fragility level and what it means
2. Which sub-factors are driving the reading
3. How this compares to the historical analogs above
4. What a continuation vs reversal scenario looks like
5. What to watch to confirm or invalidate the signal

Be direct and quantitative. Avoid hype or certainty about market direction.
This is a fragility meter, not a prediction."""


def generate_summary(
    features: pd.DataFrame,
    snapshot: dict,
    model: str = "claude-opus-4-6",
    api_key: Optional[str] = None,
) -> str:
    """Call Claude to produce the regime memo. Returns the memo as a string."""
    try:
        import anthropic
    except ImportError:
        return "anthropic package not installed – pip install anthropic"

    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return (
            "Set ANTHROPIC_API_KEY environment variable to enable AI summaries."
        )

    client = anthropic.Anthropic(api_key=key)
    prompt = _build_prompt(features, snapshot)

    try:
        message = client.messages.create(
            model=model,
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text
    except Exception as exc:
        return f"AI summary unavailable: {exc}"
