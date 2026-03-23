#!/usr/bin/env python3
"""
Procurement Spend Analysis Tool
A professional data analysis tool for strategic procurement initiatives.

Usage:
    python procurement_analyzer.py <input.csv> [--output-dir OUTPUT_DIR] [--format {csv,excel,both}]

Features:
    - Intelligent spend categorization using multi-pass rule engine
    - Vendor override and commodity code crosswalk matching
    - Confidence scoring and rule pass tracking
    - Professional summary reports with spend analysis
    - Excel output with formatted sheets and charts
"""

import argparse
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd
import re

# Import categorization engine
from categorization import categorize_dataframe


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Procurement Spend Analysis Tool - Categorize and analyze procurement spend data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python procurement_analyzer.py spend_data.csv
    python procurement_analyzer.py spend_data.csv --output-dir ./reports --format excel
    python procurement_analyzer.py spend_data.csv --format both
        """
    )
    
    parser.add_argument(
        "input_file",
        help="Input CSV file with procurement data"
    )
    
    parser.add_argument(
        "--output-dir", "-o",
        default="./output",
        help="Output directory for reports (default: ./output)"
    )
    
    parser.add_argument(
        "--format", "-f",
        choices=["csv", "excel", "both"],
        default="both",
        help="Output format (default: both)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    return parser.parse_args()


def validate_input_file(filepath: str) -> pd.DataFrame:
    """Validate and read input file."""
    path = Path(filepath)
    
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {filepath}")
    
    if not path.suffix.lower() == ".csv":
        raise ValueError(f"Input file must be a CSV file. Got: {path.suffix}")
    
    # Try reading with different encodings
    encodings = ["utf-8-sig", "utf-8", "latin-1"]
    df = None
    last_error = None
    
    for enc in encodings:
        try:
            df = pd.read_csv(filepath, encoding=enc, on_bad_lines="skip", engine="python")
            break
        except UnicodeDecodeError as e:
            last_error = e
            continue
        except Exception as e:
            last_error = e
        break
    
    if df is None:
        raise ValueError(f"Could not read input file: {last_error}")
    
    if df.empty:
        raise ValueError("Input file is empty")
    
    return df


def clean_currency_column(s: pd.Series) -> pd.Series:
    """Clean currency strings to numeric values."""
    return (
        s.astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0.0)
    )


def analyze_data(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Run categorization analysis on the input data."""
    if verbose:
        print(f"Processing {len(df):,} rows...")
    
    # Clean Extended Price column if it exists
    if "Extended Price" in df.columns:
        df["Extended Price"] = clean_currency_column(df["Extended Price"])
    
    # Run categorization
    result = categorize_dataframe(df)
    
    if verbose:
        print(f"Categorization complete. {len(result)} rows processed.")
    
    return result


def generate_summary_report(df: pd.DataFrame) -> dict:
    """Generate summary statistics for the analysis."""
    # Ensure Extended Price is numeric
    if "Extended Price" in df.columns:
        spend = clean_currency_column(df["Extended Price"])
    else:
        spend = pd.Series([0] * len(df))
    
    summary = {
        "total_rows": len(df),
        "total_spend": spend.sum(),
        "bucket_counts": df["master_bucket"].value_counts().to_dict(),
        "bucket_spend": {},
        "confidence_distribution": df["confidence_label"].value_counts().to_dict(),
        "uncategorized_count": len(df[df["master_bucket"] == "Uncategorized"]),
        "services_review_count": len(df[df["services_review_flag"] == True]),
    }
    
    # Calculate spend by bucket
    for bucket in df["master_bucket"].unique():
        mask = df["master_bucket"] == bucket
        summary["bucket_spend"][bucket] = spend[mask].sum()
    
    return summary


def print_summary(summary: dict):
    """Print formatted summary to console."""
    print("\n" + "=" * 70)
    print("PROCUREMENT SPEND ANALYSIS REPORT")
    print("=" * 70)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total Rows Analyzed: {summary['total_rows']:,}")
    print(f"Total Spend: ${summary['total_spend']:,.2f}")
    print("\n" + "-" * 70)
    print("SPEND BY CATEGORY")
    print("-" * 70)
    
    # Sort buckets by spend (descending)
    sorted_buckets = sorted(
        summary["bucket_spend"].items(),
        key=lambda x: x[1],
        reverse=True
    )
    
    for bucket, spend in sorted_buckets:
        count = summary["bucket_counts"].get(bucket, 0)
        pct = (spend / summary["total_spend"] * 100) if summary["total_spend"] > 0 else 0
        print(f"  {bucket:45s} ${spend:>15,.2f}  ({pct:5.1f}%)  [{count:,} rows]")
    
    print("\n" + "-" * 70)
    print("CONFIDENCE DISTRIBUTION")
    print("-" * 70)
    for conf, count in summary["confidence_distribution"].items():
        pct = (count / summary["total_rows"] * 100)
        print(f"  {conf:20s}: {count:>6,} rows ({pct:5.1f}%)")
    
    print("\n" + "-" * 70)
    print("QUALITY INDICATORS")
    print("-" * 70)
    uncategorized_pct = (summary["uncategorized_count"] / summary["total_rows"] * 100)
    print(f"  Uncategorized:     {summary['uncategorized_count']:,} rows ({uncategorized_pct:.1f}%)")
    
    services_pct = (summary["services_review_count"] / summary["total_rows"] * 100)
    print(f"  Services Review:   {summary['services_review_count']:,} rows ({services_pct:.1f}%)")
    
    print("\n" + "=" * 70)


def write_csv_output(df: pd.DataFrame, output_dir: Path, base_name: str):
    """Write categorized data to CSV."""
    output_path = output_dir / f"{base_name}_categorized.csv"
    df.to_csv(output_path, index=False)
    print(f"  CSV output: {output_path}")
    return output_path


def write_excel_output(df: pd.DataFrame, output_dir: Path, base_name: str, summary: dict):
    """Write formatted Excel output with multiple sheets."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from openpyxl.utils.dataframe import dataframe_to_rows
    except ImportError:
        print("  Warning: openpyxl not installed. Skipping Excel output.")
        print("  Install with: pip install openpyxl")
        return None
    
    output_path = output_dir / f"{base_name}_report.xlsx"
    
    # Clean spend data
    if "Extended Price" in df.columns:
        spend = clean_currency_column(df["Extended Price"])
    else:
        spend = pd.Series([0] * len(df))
    
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Sheet 1: Summary Dashboard
        summary_df = pd.DataFrame({
            "Metric": ["Total Rows", "Total Spend", "Categories", "Uncategorized", "Services Review"],
            "Value": [
                f"{summary['total_rows']:,}",
                f"${summary['total_spend']:,.2f}",
                len(summary["bucket_counts"]),
                f"{summary['uncategorized_count']:,} ({summary['uncategorized_count']/summary['total_rows']*100:.1f}%)",
                f"{summary['services_review_count']:,} ({summary['services_review_count']/summary['total_rows']*100:.1f}%)"
            ]
        })
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        
        # Sheet 2: Spend by Category
        spend_df = pd.DataFrame([
            {"Category": bucket, "Spend": spend, "Row Count": summary["bucket_counts"].get(bucket, 0)}
            for bucket, spend in sorted(summary["bucket_spend"].items(), key=lambda x: x[1], reverse=True)
        ])
        spend_df["% of Total"] = spend_df["Spend"] / summary["total_spend"] * 100
        spend_df.to_excel(writer, sheet_name="Spend by Category", index=False)
        
        # Sheet 3: Categorized Detail
        df.to_excel(writer, sheet_name="Detail", index=False)
        
        # Sheet 4: Services Review Queue
        services_df = df[df["services_review_flag"] == True].copy()
        if not services_df.empty:
            services_df.to_excel(writer, sheet_name="Services Review", index=False)
        
        # Sheet 5: Uncategorized Items
        uncategorized_df = df[df["master_bucket"] == "Uncategorized"].copy()
        if not uncategorized_df.empty:
            uncategorized_df.to_excel(writer, sheet_name="Uncategorized", index=True)
    
    print(f"  Excel output: {output_path}")
    return output_path


def main():
    """Main entry point."""
    args = parse_args()
    
    print(f"\nProcurement Spend Analysis Tool")
    print(f"{'='*50}\n")
    
    # Validate input
    try:
        print(f"Reading input file: {args.input_file}")
        df = validate_input_file(args.input_file)
        print(f"  Loaded {len(df):,} rows")
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Run analysis
    try:
        result_df = analyze_data(df, verbose=args.verbose)
    except Exception as e:
        print(f"ERROR during analysis: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    
    # Generate summary
    summary = generate_summary_report(result_df)
    print_summary(summary)
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate base filename from input
    base_name = Path(args.input_file).stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"{base_name}_{timestamp}"
    
    print(f"\nWriting output files to: {output_dir}")
    
    # Write outputs based on format
    if args.format in ("csv", "both"):
        write_csv_output(result_df, output_dir, base_name)
    
    if args.format in ("excel", "both"):
        write_excel_output(result_df, output_dir, base_name, summary)
    
    print(f"\nAnalysis complete!")


if __name__ == "__main__":
    main()
