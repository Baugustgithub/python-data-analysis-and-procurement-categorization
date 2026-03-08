"""
Procurement Spend Categorization & Strategic Sourcing Report

Single entry point that:
  1. Reads input CSV/XLSX files
  2. Runs the multi-pass classification engine
  3. Writes categorized_output.csv
  4. Builds a strategic-sourcing Excel workbook (Procurement_Analysis.xlsx)

Usage:
  python run_categorization.py                          # auto-find template_po_search*.csv
  python run_categorization.py myfile.csv               # specific input
  python run_categorization.py myfile.csv out.csv       # specific input & CSV output
"""

import logging
import os
import sys
import glob
from datetime import datetime

import pandas as pd

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

from categorization import (
    categorize_dataframe,
    COLUMN_ALIASES,
    BUCKET_COLORS,
    _resolve_column,
)


# ═════════════════════════════════════════════════════════════════════════════
# HELPER UTILITIES
# ═════════════════════════════════════════════════════════════════════════════

def _read_file_robust(path: str) -> pd.DataFrame:
    """Read CSV or Excel with encoding fallback and clear error messages."""
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path)
    # CSV — try encodings in order
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return pd.read_csv(path, encoding=enc,
                               on_bad_lines="skip", engine="python")
        except UnicodeDecodeError:
            continue
        except Exception:
            break
    # Final fallback
    return pd.read_csv(path, encoding="latin-1",
                       on_bad_lines="skip", engine="python")


def _safe_num_series(s: pd.Series) -> pd.Series:
    """Parse dollar-formatted strings to numeric, filling NaN with 0."""
    return (
        s.astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0.0)
    )


def _find_spend_col(df: pd.DataFrame) -> str | None:
    """Find the best spend/amount column using COLUMN_ALIASES."""
    return _resolve_column(df.columns, "Extended Price")


def _find_vendor_col(df: pd.DataFrame) -> str | None:
    """Find the best vendor column using COLUMN_ALIASES."""
    return _resolve_column(df.columns, "Vendor Name")


def _coerce_date(df: pd.DataFrame) -> pd.Series:
    """Find and parse the best date column."""
    for col in ["Creation Date", "Date", "Invoice Date", "PO Date",
                "Order Date", "Transaction Date"]:
        if col in df.columns:
            return pd.to_datetime(df[col], errors="coerce")
    return pd.Series([pd.NaT] * len(df), index=df.index)


def _infer_on_contract(df: pd.DataFrame) -> pd.Series:
    """Infer on-contract status from contract number + procurement method columns."""
    ON_CONTRACT_METHODS = {"contract order", "contract", "blanket order",
                           "master agreement", "cooperative contract"}

    contract_cols = [c for c in df.columns
                     if any(k in c.lower() for k in ("contract no", "contract number",
                                                      "contract #", "contract"))]
    if contract_cols:
        has_contract_num = df[contract_cols].fillna("").astype(str).apply(
            lambda r: any(str(x).strip() and str(x).strip().lower() not in {"nan", "none", ""} for x in r),
            axis=1
        )
    else:
        has_contract_num = pd.Series(False, index=df.index)

    method_col = None
    for c in ["Procurement Method", "Method", "Payment Method", "PO Type",
              "Order Type", "Procurement Method (For Purchasing Use Only)"]:
        if c in df.columns:
            method_col = c
            break

    if method_col:
        method_norm = df[method_col].astype(str).str.strip().str.lower()
        on_method = method_norm.isin(ON_CONTRACT_METHODS)
    else:
        on_method = pd.Series(False, index=df.index)

    return has_contract_num | on_method


# ═════════════════════════════════════════════════════════════════════════════
# STRATEGIC SOURCING EXCEL BUILDER
# ═════════════════════════════════════════════════════════════════════════════

def build_excel_report(df: pd.DataFrame, output_path: str):
    """Build a multi-sheet strategic sourcing workbook.

    Sheets:
      1. Executive Summary     — KPIs and high-level metrics
      2. Spend by Category     — Master bucket + L2 spend breakdown
      3. Top Vendors           — Top 500 vendors with category, contract status
      4. Tail Spend Analysis   — Vendors below threshold (80/20 analysis)
      5. Contract Leakage      — Off-contract spend by category
      6. Category Deep Dive    — Full L2×L3 spend matrix
      7. Supplier Consolidation— Categories with many suppliers (consolidation opps)
      8. Services Review Queue — Rows flagged for manual review
      9. Uncategorized         — Rows the engine couldn't classify
    """
    log.info("Building strategic sourcing Excel report...")

    # ── Resolve spend & vendor columns ──────────────────────────────────────
    spend_col = _find_spend_col(df)
    vendor_col = _find_vendor_col(df)

    if spend_col:
        df["_spend"] = _safe_num_series(df[spend_col])
    else:
        log.warning("No spend/amount column found — financial analysis will be limited.")
        df["_spend"] = 0.0

    total_spend = float(df["_spend"].sum())
    total_rows = len(df)

    # ── Date / Fiscal Year ──────────────────────────────────────────────────
    dates = _coerce_date(df)
    good_dates = dates.notna().sum()
    if good_dates > 0:
        df["_month"] = dates.dt.to_period("M").astype(str)
        fy = dates.dt.year.copy()
        fy = fy.where(dates.dt.month < 7, fy + 1)  # VCU FY: Jul 1 – Jun 30
        df["_fy"] = fy
    else:
        df["_month"] = "Unknown"
        df["_fy"] = "Unknown"

    # ── Contract status ─────────────────────────────────────────────────────
    df["_on_contract"] = _infer_on_contract(df)
    on_contract_spend = float(df.loc[df["_on_contract"], "_spend"].sum())
    off_contract_spend = total_spend - on_contract_spend
    on_contract_pct = (on_contract_spend / total_spend * 100) if total_spend else 0.0

    # ── FY-end logic ────────────────────────────────────────────────────────
    period_cols = sorted(df["_month"].dropna().unique().tolist())
    fy_end_suffixes = ("-04", "-05", "-06")
    last3 = [p for p in period_cols if str(p).endswith(fy_end_suffixes)]
    if not last3:
        last3 = period_cols[-3:] if len(period_cols) >= 3 else period_cols
    df["_is_fy_end_month"] = df["_month"].isin(last3)
    fy_end_spend = float(df.loc[df["_is_fy_end_month"], "_spend"].sum())
    fy_end_pct = (fy_end_spend / total_spend * 100) if total_spend else 0.0

    # ── Confidence metrics ──────────────────────────────────────────────────
    avg_confidence = df["confidence_score"].mean() if "confidence_score" in df.columns else 0
    high_conf_pct = (
        (df["confidence_score"] >= 0.6).sum() / total_rows * 100
        if "confidence_score" in df.columns and total_rows else 0
    )
    uncategorized_count = (df["master_bucket"] == "Uncategorized").sum()
    uncategorized_pct = uncategorized_count / total_rows * 100 if total_rows else 0

    # ═══════════════════════════════════════════════════════════════════════
    # SHEET 1: Executive Summary
    # ═══════════════════════════════════════════════════════════════════════
    summary_data = [
        ["Total Line Items", f"{total_rows:,}"],
        ["Total Spend", f"${total_spend:,.2f}"],
        ["", ""],
        ["On-Contract Spend (inferred)", f"${on_contract_spend:,.2f}"],
        ["On-Contract %", f"{on_contract_pct:.1f}%"],
        ["Off-Contract Spend", f"${off_contract_spend:,.2f}"],
        ["", ""],
        ["Avg Classification Confidence", f"{avg_confidence:.0%}"],
        ["High-Confidence Rows (≥60%)", f"{high_conf_pct:.1f}%"],
        ["Uncategorized Rows", f"{uncategorized_count:,} ({uncategorized_pct:.1f}%)"],
        ["", ""],
        ["FY-End Months Analyzed", ", ".join(map(str, last3))],
        ["FY-End Spend", f"${fy_end_spend:,.2f} ({fy_end_pct:.1f}%)"],
        ["", ""],
        ["Unique Vendors", f"{df[vendor_col].nunique():,}" if vendor_col else "N/A"],
        ["Unique Categories (L2)", f"{df['sub_bucket_l2'].nunique():,}" if "sub_bucket_l2" in df.columns else "N/A"],
        ["", ""],
        ["Generated", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
    ]
    summary_df = pd.DataFrame(summary_data, columns=["Metric", "Value"])

    # ═══════════════════════════════════════════════════════════════════════
    # SHEET 2: Spend by Category (L1 + L2)
    # ═══════════════════════════════════════════════════════════════════════
    by_bucket = (
        df.groupby(["master_bucket", "sub_bucket_l2"])["_spend"]
        .agg(total_spend="sum", line_count="size")
        .sort_values("total_spend", ascending=False)
        .reset_index()
    )
    by_bucket["pct_of_total"] = by_bucket["total_spend"].apply(
        lambda x: round(x / total_spend * 100, 2) if total_spend else 0)
    by_bucket["cumulative_pct"] = by_bucket["pct_of_total"].cumsum().round(2)

    # ═══════════════════════════════════════════════════════════════════════
    # SHEET 3: Top Vendors
    # ═══════════════════════════════════════════════════════════════════════
    if vendor_col:
        # Group vendor with their primary category
        vendor_cat = (
            df.groupby([vendor_col, "master_bucket"])["_spend"]
            .sum().reset_index()
            .sort_values("_spend", ascending=False)
            .drop_duplicates(subset=[vendor_col], keep="first")
        )

        top_vendors = (
            df.groupby(vendor_col)
            .agg(
                total_spend=("_spend", "sum"),
                line_count=("_spend", "size"),
                on_contract_spend=("_spend", lambda x: x[df.loc[x.index, "_on_contract"]].sum()),
                avg_confidence=("confidence_score", "mean"),
            )
            .sort_values("total_spend", ascending=False)
            .reset_index()
            .head(500)
        )
        top_vendors = top_vendors.rename(columns={vendor_col: "Vendor"})
        top_vendors["pct_of_total"] = top_vendors["total_spend"].apply(
            lambda x: round(x / total_spend * 100, 2) if total_spend else 0)
        top_vendors["cumulative_pct"] = top_vendors["pct_of_total"].cumsum().round(2)
        top_vendors["on_contract_pct"] = (
            top_vendors["on_contract_spend"] / top_vendors["total_spend"] * 100
        ).round(1).fillna(0)

        # Merge primary category
        vendor_cat_map = vendor_cat.set_index(vendor_col)["master_bucket"].to_dict()
        top_vendors["primary_category"] = top_vendors["Vendor"].map(vendor_cat_map)
        top_vendors["avg_confidence"] = top_vendors["avg_confidence"].round(2)

        # Reorder columns for readability
        top_vendors = top_vendors[["Vendor", "primary_category", "total_spend", "pct_of_total",
                                    "cumulative_pct", "line_count", "on_contract_spend",
                                    "on_contract_pct", "avg_confidence"]]
    else:
        top_vendors = pd.DataFrame(columns=["Vendor", "primary_category", "total_spend",
                                             "pct_of_total", "line_count"])

    # ═══════════════════════════════════════════════════════════════════════
    # SHEET 4: Tail Spend Analysis (80/20)
    # ═══════════════════════════════════════════════════════════════════════
    if vendor_col:
        vendor_spend = (
            df.groupby(vendor_col)["_spend"]
            .agg(total_spend="sum", line_count="size")
            .sort_values("total_spend", ascending=False)
            .reset_index()
        )
        vendor_spend["pct_of_total"] = vendor_spend["total_spend"] / total_spend * 100 if total_spend else 0
        vendor_spend["cumulative_pct"] = vendor_spend["pct_of_total"].cumsum()

        # Strategic vendors = those comprising 80% of spend
        threshold_80 = vendor_spend[vendor_spend["cumulative_pct"] <= 80]
        strategic_count = len(threshold_80) + 1  # +1 for the one that crosses 80%
        tail_vendors = vendor_spend.iloc[strategic_count:]

        tail_summary = pd.DataFrame([
            ["Total Vendors", f"{len(vendor_spend):,}"],
            ["Strategic Vendors (80% of spend)", f"{strategic_count:,}"],
            ["Tail Vendors (remaining 20%)", f"{len(tail_vendors):,}"],
            ["Tail Spend", f"${tail_vendors['total_spend'].sum():,.2f}"],
            ["Avg Tail Vendor Spend", f"${tail_vendors['total_spend'].mean():,.2f}" if len(tail_vendors) else "$0"],
            ["", ""],
            ["Recommendation", "Consolidate tail vendors through preferred supplier programs, "
             "P-Card programs, or marketplace solutions to reduce transaction costs."],
        ], columns=["Metric", "Value"])

        tail_detail = tail_vendors.rename(columns={vendor_col: "Vendor"}).head(500)
    else:
        tail_summary = pd.DataFrame([["No vendor column found", ""]], columns=["Metric", "Value"])
        tail_detail = pd.DataFrame()

    # ═══════════════════════════════════════════════════════════════════════
    # SHEET 5: Contract Leakage
    # ═══════════════════════════════════════════════════════════════════════
    off_contract = df[~df["_on_contract"]].copy()
    leakage_by_cat = (
        off_contract.groupby(["master_bucket", "sub_bucket_l2"])["_spend"]
        .agg(off_contract_spend="sum", line_count="size")
        .sort_values("off_contract_spend", ascending=False)
        .reset_index()
    )
    leakage_by_cat["pct_of_category_total"] = leakage_by_cat.apply(
        lambda r: round(
            r["off_contract_spend"] /
            df.loc[df["master_bucket"] == r["master_bucket"], "_spend"].sum() * 100, 1
        ) if df.loc[df["master_bucket"] == r["master_bucket"], "_spend"].sum() else 0,
        axis=1
    )
    leakage_by_cat["sourcing_opportunity"] = leakage_by_cat["off_contract_spend"].apply(
        lambda x: "High" if x > 100000 else ("Medium" if x > 25000 else "Low")
    )

    # ═══════════════════════════════════════════════════════════════════════
    # SHEET 6: Category Deep Dive (L2 × L3)
    # ═══════════════════════════════════════════════════════════════════════
    deep_dive = (
        df.groupby(["master_bucket", "sub_bucket_l2", "sub_bucket_l3"])
        .agg(
            total_spend=("_spend", "sum"),
            line_count=("_spend", "size"),
            unique_vendors=(vendor_col, "nunique") if vendor_col else ("_spend", "size"),
            avg_confidence=("confidence_score", "mean"),
        )
        .sort_values(["master_bucket", "total_spend"], ascending=[True, False])
        .reset_index()
    )
    deep_dive["pct_of_total"] = deep_dive["total_spend"].apply(
        lambda x: round(x / total_spend * 100, 2) if total_spend else 0)
    deep_dive["avg_confidence"] = deep_dive["avg_confidence"].round(2)

    # ═══════════════════════════════════════════════════════════════════════
    # SHEET 7: Supplier Consolidation Opportunities
    # ═══════════════════════════════════════════════════════════════════════
    if vendor_col:
        consol = (
            df.groupby(["master_bucket", "sub_bucket_l2"])
            .agg(
                total_spend=("_spend", "sum"),
                unique_vendors=(vendor_col, "nunique"),
                line_count=("_spend", "size"),
            )
            .reset_index()
        )
        consol["avg_spend_per_vendor"] = (consol["total_spend"] / consol["unique_vendors"]).round(2)
        consol = consol[consol["unique_vendors"] >= 3].sort_values(
            "unique_vendors", ascending=False)
        consol["consolidation_priority"] = consol.apply(
            lambda r: "High" if r["unique_vendors"] >= 10 and r["total_spend"] >= 50000
            else ("Medium" if r["unique_vendors"] >= 5 else "Low"), axis=1
        )
    else:
        consol = pd.DataFrame()

    # ═══════════════════════════════════════════════════════════════════════
    # SHEET 8: Services Review Queue
    # ═══════════════════════════════════════════════════════════════════════
    if "services_review_flag" in df.columns:
        srvq = df[df["services_review_flag"].fillna(False).astype(bool)].copy()
    else:
        srvq = df.iloc[0:0].copy()

    # ═══════════════════════════════════════════════════════════════════════
    # SHEET 9: Uncategorized
    # ═══════════════════════════════════════════════════════════════════════
    unc = df[df["master_bucket"].fillna("") == "Uncategorized"].copy()

    # ── Drop internal working columns before writing ────────────────────────
    internal_cols = ["_spend", "_month", "_fy", "_on_contract", "_is_fy_end_month"]
    srvq_clean = srvq.drop(columns=[c for c in internal_cols if c in srvq.columns], errors="ignore")
    unc_clean = unc.drop(columns=[c for c in internal_cols if c in unc.columns], errors="ignore")

    # ── Write workbook ──────────────────────────────────────────────────────
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Executive Summary", index=False)
        by_bucket.to_excel(writer, sheet_name="Spend by Category", index=False)
        top_vendors.to_excel(writer, sheet_name="Top Vendors", index=False)

        if not tail_detail.empty:
            tail_summary.to_excel(writer, sheet_name="Tail Spend Analysis", index=False, startrow=0)
            tail_detail.to_excel(writer, sheet_name="Tail Spend Analysis", index=False,
                                 startrow=len(tail_summary) + 2)
        else:
            tail_summary.to_excel(writer, sheet_name="Tail Spend Analysis", index=False)

        leakage_by_cat.to_excel(writer, sheet_name="Contract Leakage", index=False)
        deep_dive.to_excel(writer, sheet_name="Category Deep Dive", index=False)

        if not consol.empty:
            consol.to_excel(writer, sheet_name="Supplier Consolidation", index=False)

        srvq_clean.to_excel(writer, sheet_name="Services Review", index=False)
        unc_clean.to_excel(writer, sheet_name="Uncategorized", index=False)

    log.info("Excel report written: %s", output_path)


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    # ── Decide input file(s) ────────────────────────────────────────────────
    if len(sys.argv) > 1:
        input_files = [sys.argv[1]]
        if not os.path.exists(input_files[0]):
            log.error("File not found: %s", input_files[0])
            sys.exit(1)
    else:
        input_files = glob.glob(os.path.join(script_dir, "template_po_search*.csv"))
        if not input_files:
            log.error("No source CSV found. Either pass a file path as an argument, "
                      "or place a template_po_search*.csv file in this folder.")
            sys.exit(1)

    csv_output = sys.argv[2] if len(sys.argv) > 2 else os.path.join(script_dir, "categorized_output.csv")
    xlsx_output = os.path.join(script_dir, "Procurement_Analysis.xlsx")

    # ── Read file(s) (incremental concat to limit peak memory) ──────────────
    df = None
    canonical_cols = None
    for f in input_files:
        log.info("Reading: %s", os.path.basename(f))
        try:
            chunk = _read_file_robust(f)
            if df is None:
                df = chunk
                canonical_cols = list(chunk.columns)
            else:
                chunk = chunk.reindex(columns=canonical_cols)
                df = pd.concat([df, chunk], ignore_index=True)
            del chunk
        except Exception as e:
            log.error("Failed to read %s: %s", os.path.basename(f), e)
            sys.exit(1)
    if df is None:
        log.error("No data could be read from input files.")
        sys.exit(1)
    log.info("Total rows: %s", f"{len(df):,}")

    if df.empty:
        log.error("Input file(s) contain no data rows.")
        sys.exit(1)

    # ── Categorize ──────────────────────────────────────────────────────────
    results = categorize_dataframe(df)

    # ── Build clean output (handles duplicate column names) ─────────────────
    cat_cols = ["master_bucket", "sub_bucket_l2", "sub_bucket_l3",
                "rule_pass", "rule_pass_label", "rule_hit",
                "confidence_score", "confidence_label", "services_review_flag"]

    base_cols = [c for c in results.columns if c not in cat_cols]
    out = results[base_cols].copy()

    col_positions = {col: [] for col in cat_cols}
    for i, c in enumerate(results.columns):
        if c in col_positions:
            col_positions[c].append(i)

    for col in cat_cols:
        pos = col_positions.get(col, [])
        if pos:
            out[col] = results.iloc[:, pos[-1]].values

    # ── Write CSV output ────────────────────────────────────────────────────
    out_dir = os.path.dirname(csv_output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    out.to_csv(csv_output, index=False, encoding="utf-8-sig")
    log.info("CSV written: %s", csv_output)

    # ── Console summary ─────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  CLASSIFICATION RESULTS")
    print("=" * 60)

    summary = out["master_bucket"].value_counts()
    for bucket, count in summary.items():
        pct = count / len(out) * 100
        print(f"  {bucket:<45} {count:>6,}  ({pct:.1f}%)")
    print(f"  {'TOTAL':<45} {len(out):>6,}")

    uncategorized = (out["master_bucket"] == "Uncategorized").sum()
    if uncategorized:
        print(f"\n  WARNING: {uncategorized:,} rows still uncategorized")
    else:
        print("\n  All rows categorized.")

    # Confidence breakdown
    if "confidence_score" in out.columns:
        print(f"\n  Avg Confidence: {out['confidence_score'].mean():.0%}")
        print(f"  High Confidence (≥60%): {(out['confidence_score'] >= 0.6).sum():,} rows "
              f"({(out['confidence_score'] >= 0.6).mean():.0%})")

    # Services review
    if "services_review_flag" in out.columns:
        review_count = int(out["services_review_flag"].sum())
        spend_col = _find_spend_col(out)
        review_spend = out.loc[out["services_review_flag"] == True, spend_col].sum() if spend_col else 0
        print(f"\n  Services Review Queue: {review_count:,} rows"
              + (f"  (${review_spend:,.0f} spend)" if review_spend else ""))

    # ── Build Excel report ──────────────────────────────────────────────────
    try:
        build_excel_report(out, xlsx_output)
        print(f"\n  Excel report: {xlsx_output}")
    except Exception as e:
        log.error("Excel report failed (CSV still saved): %s", e)

    print()


if __name__ == "__main__":
    main()
