"""
Turbulence / Fragility Engine
==============================
Computes a composite market fragility score inspired by Jordi Visser's
turbulence framework.  Four signal layers:
  1. Base turbulence   – Mahalanobis distance of daily returns (Ledoit-Wolf shrinkage)
  2. Dispersion stress – cross-asset and cross-sector divergence
  3. Credit contagion  – HYG/LQD stress, alt-manager weakness, software underperformance
  4. Correlation break – rolling correlation z-scores across key pairs

Composite regime: GREEN < 1 ≤ YELLOW < 2 ≤ ORANGE < 3 ≤ RED
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf
from sklearn.covariance import LedoitWolf

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Asset universe
# ---------------------------------------------------------------------------
MACRO_TICKERS = [
    "SPY", "QQQ", "IWM", "TLT", "HYG", "LQD", "GLD", "USO", "UUP",
]
CONTAGION_TICKERS = [
    "XLF", "KRE", "IGV", "ARKK", "BX", "KKR", "APO", "ARES",
]
RISK_PROXY_TICKERS = ["BTC-USD"]

ALL_TICKERS = MACRO_TICKERS + CONTAGION_TICKERS + RISK_PROXY_TICKERS

# Key correlation pairs to monitor
CORR_PAIRS = [
    ("QQQ", "IGV"),
    ("SPY", "HYG"),
    ("IGV", "HYG"),
    ("XLF", "KRE"),
    ("BTC-USD", "QQQ"),
]

LOOKBACK_LONG  = 126  # ~6 months
LOOKBACK_FAST  = 20   # ~1 month
MIN_OBS        = 60

COMPOSITE_WEIGHTS = {
    "turbulence":    0.35,
    "credit_stress": 0.20,
    "financial_rel": 0.15,
    "software_rel":  0.15,
    "corr_break":    0.10,
    "btc_rel":       0.05,
}

CACHE_FILE = Path(__file__).parent / "data_cache.parquet"


# ---------------------------------------------------------------------------
# Data layer
# ---------------------------------------------------------------------------

def fetch_prices(
    tickers: list[str] = ALL_TICKERS,
    start: str = "2018-01-01",
    use_cache: bool = True,
) -> pd.DataFrame:
    """Download adjusted close prices; optionally cache to disk."""
    if use_cache and CACHE_FILE.exists():
        cached = pd.read_parquet(CACHE_FILE)
        # Only refresh if last date is more than 1 trading day old
        last_cached = cached.index[-1]
        today = pd.Timestamp.today().normalize()
        if (today - last_cached).days <= 1:
            return cached

    raw = yf.download(
        tickers,
        start=start,
        auto_adjust=True,
        progress=False,
        threads=True,
    )
    prices = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw
    prices = prices.dropna(axis=1, how="all").ffill().dropna()

    if use_cache:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        prices.to_parquet(CACHE_FILE)

    return prices


# ---------------------------------------------------------------------------
# Signal 1: Base Turbulence (Mahalanobis distance)
# ---------------------------------------------------------------------------

def compute_turbulence(rets: pd.DataFrame, lookback: int = LOOKBACK_LONG) -> pd.Series:
    """
    T_t = (1/n) * (r_t - μ_t)' Σ_t^{-1} (r_t - μ_t)

    Uses Ledoit-Wolf shrinkage for covariance stability.
    """
    turbulence = pd.Series(index=rets.index, dtype=float, name="turbulence")

    for i in range(lookback, len(rets)):
        hist = rets.iloc[i - lookback : i]
        valid = hist.columns[hist.notna().all()]
        hist_v = hist[valid]
        x = rets.iloc[i][valid].values.reshape(1, -1)

        if len(hist_v) < MIN_OBS or hist_v.shape[1] < 3:
            continue

        mu = hist_v.mean().values.reshape(1, -1)
        try:
            lw = LedoitWolf().fit(hist_v.values)
            cov_inv = np.linalg.pinv(lw.covariance_)
        except Exception:
            continue

        diff = x - mu
        score = (diff @ cov_inv @ diff.T).item() / hist_v.shape[1]
        turbulence.iloc[i] = max(score, 0.0)

    return turbulence


# ---------------------------------------------------------------------------
# Signal 2 & 3: Relative / contagion metrics
# ---------------------------------------------------------------------------

def rolling_zscore(series: pd.Series, window: int = LOOKBACK_LONG) -> pd.Series:
    m = series.rolling(window).mean()
    s = series.rolling(window).std()
    return (series - m) / s.replace(0, np.nan)


def compute_contagion_signals(prices: pd.DataFrame) -> pd.DataFrame:
    """Build credit/financial/software/BTC relative-stress series."""
    def rel_chg(a: str, b: str, n: int = LOOKBACK_FAST) -> pd.Series:
        """Rolling n-day percentage change of price ratio a/b."""
        return (prices[a] / prices[b]).pct_change(n)

    signals: dict[str, pd.Series] = {}

    # Credit stress: HYG underperforming LQD → positive = stress
    if "HYG" in prices and "LQD" in prices:
        signals["credit_stress"] = rolling_zscore(-rel_chg("HYG", "LQD"))

    # Financial stress: XLF lagging SPY
    if "XLF" in prices and "SPY" in prices:
        signals["financial_rel"] = rolling_zscore(-rel_chg("XLF", "SPY"))

    # Software stress: IGV lagging SPY
    if "IGV" in prices and "SPY" in prices:
        signals["software_rel"] = rolling_zscore(-rel_chg("IGV", "SPY"))

    # Alt-manager / private-equity proxy: basket vs SPY
    alt_tickers = [t for t in ["BX", "KKR", "APO", "ARES"] if t in prices]
    if alt_tickers:
        basket = prices[alt_tickers].mean(axis=1)
        signals["alt_manager_rel"] = rolling_zscore(
            -(basket / prices["SPY"]).pct_change(LOOKBACK_FAST)
        )

    # BTC liquidity beta: BTC lagging QQQ
    if "BTC-USD" in prices and "QQQ" in prices:
        signals["btc_rel"] = rolling_zscore(-rel_chg("BTC-USD", "QQQ"))

    return pd.DataFrame(signals)


# ---------------------------------------------------------------------------
# Signal 4: Correlation breaks
# ---------------------------------------------------------------------------

def compute_corr_breaks(rets: pd.DataFrame) -> pd.Series:
    """
    For each monitored pair, compute rolling 20-day correlation then
    z-score it against 126-day history.  Sum of absolute z-scores = break signal.
    """
    breaks = pd.DataFrame(index=rets.index)

    for a, b in CORR_PAIRS:
        if a not in rets or b not in rets:
            continue
        roll_corr = rets[a].rolling(LOOKBACK_FAST).corr(rets[b])
        col = f"corr_{a}_{b}"
        breaks[col] = rolling_zscore(roll_corr).abs()

    if breaks.empty:
        return pd.Series(0.0, index=rets.index, name="corr_break")

    return breaks.mean(axis=1).rename("corr_break")


# ---------------------------------------------------------------------------
# Composite score
# ---------------------------------------------------------------------------

def build_composite(
    turbulence: pd.Series,
    contagion: pd.DataFrame,
    corr_break: pd.Series,
) -> pd.DataFrame:
    """
    Combine z-scored sub-signals into a weighted composite.
    """
    features = pd.DataFrame({"turbulence": turbulence})
    features = features.join(contagion, how="left")
    features["corr_break"] = corr_break

    # Z-score turbulence relative to its own history
    features["turbulence_z"] = rolling_zscore(features["turbulence"])

    score = pd.Series(0.0, index=features.index)
    total_w = 0.0

    for key, weight in COMPOSITE_WEIGHTS.items():
        col = "turbulence_z" if key == "turbulence" else key
        if col in features.columns:
            s = features[col].fillna(0)
            score += weight * s
            total_w += weight

    if total_w > 0:
        score /= total_w

    features["composite"] = score
    features["regime"] = score.map(_classify)
    return features


def _classify(x: float) -> str:
    if pd.isna(x):
        return "UNKNOWN"
    if x >= 3.0:
        return "RED"
    if x >= 2.0:
        return "ORANGE"
    if x >= 1.0:
        return "YELLOW"
    return "GREEN"


REGIME_COLORS = {
    "GREEN":   "#22c55e",
    "YELLOW":  "#eab308",
    "ORANGE":  "#f97316",
    "RED":     "#ef4444",
    "UNKNOWN": "#94a3b8",
}


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------

def run(
    start: str = "2018-01-01",
    use_cache: bool = True,
    lookback: int = LOOKBACK_LONG,
) -> pd.DataFrame:
    """Download data, compute all signals, return full feature DataFrame."""
    prices = fetch_prices(ALL_TICKERS, start=start, use_cache=use_cache)
    rets   = prices.pct_change().dropna()

    turbulence = compute_turbulence(rets, lookback=lookback)
    contagion  = compute_contagion_signals(prices)
    corr_break = compute_corr_breaks(rets)

    return build_composite(turbulence, contagion, corr_break)


def latest_snapshot(features: pd.DataFrame) -> dict:
    """Return a dict summarising the most-recent valid row."""
    valid = features.dropna(subset=["composite"])
    if valid.empty:
        return {"date": "N/A", "composite": float("nan"), "regime": "UNKNOWN"}
    last = valid.iloc[-1]
    snap = last.to_dict()
    snap["date"] = last.name.strftime("%Y-%m-%d")
    return snap


# ---------------------------------------------------------------------------
# Demo / synthetic data (for testing without network access)
# ---------------------------------------------------------------------------

def _make_synthetic_prices(
    tickers: list[str],
    start: str = "2018-01-01",
    n_days: int = 1500,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate correlated synthetic price series that mimic real market behaviour,
    including a few stress episodes that should trigger HIGH turbulence.
    """
    rng  = np.random.default_rng(seed)
    idx  = pd.bdate_range(start=start, periods=n_days)
    n    = len(tickers)

    # Base covariance structure (rough approximation)
    base_vol = np.array([
        0.012, 0.014, 0.016, 0.007, 0.006, 0.005, 0.009, 0.015, 0.006,
        0.014, 0.018, 0.018, 0.025, 0.020, 0.022, 0.019, 0.021, 0.040,
    ])[:n]

    # Build a simple factor model: 1 market factor + idiosyncratic
    market_factor = rng.standard_normal(len(idx))
    loadings      = rng.uniform(0.3, 0.9, n)

    rets = np.outer(market_factor, loadings) * base_vol
    rets += rng.standard_normal((len(idx), n)) * base_vol * 0.5

    # Inject 3 stress episodes (wider + correlated moves)
    stress_periods = [
        (int(len(idx) * 0.25), 40, 3.5),   # ~2020 COVID
        (int(len(idx) * 0.55), 60, 2.5),   # ~2022 tightening
        (int(len(idx) * 0.80), 25, 2.0),   # ~2023 regional bank
    ]
    for start_i, dur, mult in stress_periods:
        rets[start_i:start_i + dur] *= mult
        rets[start_i:start_i + dur] -= abs(rets[start_i:start_i + dur].mean())

    prices = 100 * np.exp(np.cumsum(rets, axis=0))
    return pd.DataFrame(prices, index=idx[:len(prices)], columns=tickers[:n])


def run_demo(lookback: int = LOOKBACK_LONG) -> pd.DataFrame:
    """Run the full model on synthetic data (no network needed)."""
    prices = _make_synthetic_prices(ALL_TICKERS)
    rets   = prices.pct_change().dropna()

    turbulence = compute_turbulence(rets, lookback=lookback)
    contagion  = compute_contagion_signals(prices)
    corr_break = compute_corr_breaks(rets)

    return build_composite(turbulence, contagion, corr_break)
