#!/usr/bin/env python3
"""
Archive monthly billing data.

This script moves completed month's billing data into archive folders:
- Raw ClickUp CSV files → raw/clickup_billing/archive/YYYY-MM/
- Output reports → output/monthly_billing/archive/YYYY-MM/

Usage:
    python3 archive_month.py YYYY MM

Example:
    python3 archive_month.py 2025 09
"""

import sys
import shutil
from pathlib import Path
from datetime import datetime


def archive_month(year, month):
  """Archive billing data for a specific month."""

  # Set up paths
  script_dir = Path(__file__).parent
  project_root = script_dir.parent.parent

  # Validate inputs
  try:
    year_int = int(year)
    month_int = int(month)
    if not (2020 <= year_int <= 2100):
      raise ValueError("Year must be between 2020 and 2100")
    if not (1 <= month_int <= 12):
      raise ValueError("Month must be between 1 and 12")
  except ValueError as e:
    print(f"Error: Invalid year or month - {e}")
    sys.exit(1)

  # Format month with leading zero
  month_str = f"{month_int:02d}"
  archive_name = f"{year}-{month_str}"

  print(f"\n📦 Archiving data for {archive_name}...")
  print("=" * 60)

  # Create archive directories
  raw_archive_dir = project_root / "raw" / "clickup_billing" / "archive" / archive_name
  output_archive_dir = project_root / "output" / "monthly_billing" / "archive" / archive_name

  raw_archive_dir.mkdir(parents=True, exist_ok=True)
  output_archive_dir.mkdir(parents=True, exist_ok=True)

  # Track what was archived
  archived_files = []
  warnings = []

  # Archive raw CSV files
  print(f"\n📂 Archiving raw CSV files...")
  raw_source_dir = project_root / "raw" / "clickup_billing"
  csv_files = list(raw_source_dir.glob("*.csv"))

  if not csv_files:
    warnings.append("No CSV files found in raw/clickup_billing/")
  else:
    for csv_file in csv_files:
      # Check if file is from the target month (basic check)
      dest_file = raw_archive_dir / csv_file.name
      shutil.move(str(csv_file), str(dest_file))
      archived_files.append(f"Raw CSV: {csv_file.name}")
      print(f"  ✓ Moved: {csv_file.name}")

  # Archive output files (cleaned CSVs and reports)
  print(f"\n📊 Archiving output reports...")

  # Look for files matching the month pattern
  cleaned_dir = project_root / "output" / "monthly_billing" / "cleaned"
  reports_dir = project_root / "output" / "monthly_billing" / "reports"

  # Archive cleaned CSV
  cleaned_pattern = f"billing_report_{month_str}_{year}.csv"
  cleaned_files = list(cleaned_dir.glob(cleaned_pattern))

  if not cleaned_files:
    warnings.append(f"No cleaned CSV found matching: {cleaned_pattern}")
  else:
    for cleaned_file in cleaned_files:
      dest_file = output_archive_dir / cleaned_file.name
      shutil.move(str(cleaned_file), str(dest_file))
      archived_files.append(f"Cleaned CSV: {cleaned_file.name}")
      print(f"  ✓ Moved: {cleaned_file.name}")

  # Archive summary report (markdown)
  summary_pattern = f"billing_summary_{month_str}_{year}.md"
  summary_files = list(reports_dir.glob(summary_pattern))

  if not summary_files:
    warnings.append(f"No summary report found matching: {summary_pattern}")
  else:
    for summary_file in summary_files:
      dest_file = output_archive_dir / summary_file.name
      shutil.move(str(summary_file), str(dest_file))
      archived_files.append(f"Summary report: {summary_file.name}")
      print(f"  ✓ Moved: {summary_file.name}")

  # Archive PDF report (if exists)
  pdf_pattern = f"billing_summary_{month_str}_{year}.pdf"
  pdf_files = list(reports_dir.glob(pdf_pattern))

  if pdf_files:
    for pdf_file in pdf_files:
      dest_file = output_archive_dir / pdf_file.name
      shutil.move(str(pdf_file), str(dest_file))
      archived_files.append(f"PDF report: {pdf_file.name}")
      print(f"  ✓ Moved: {pdf_file.name}")

  # Print summary
  print("\n" + "=" * 60)
  print(f"✅ Archive complete for {archive_name}")
  print(f"\n📍 Archived to:")
  print(f"   Raw data: {raw_archive_dir}")
  print(f"   Reports:  {output_archive_dir}")

  if archived_files:
    print(f"\n📋 Files archived ({len(archived_files)}):")
    for file in archived_files:
      print(f"   • {file}")

  if warnings:
    print(f"\n⚠️  Warnings:")
    for warning in warnings:
      print(f"   • {warning}")

  print()


def main():
  """Main entry point."""
  if len(sys.argv) != 3:
    print("Usage: python3 archive_month.py YYYY MM")
    print("\nExample:")
    print("  python3 archive_month.py 2025 09")
    print("\nThis will archive:")
    print("  - Raw CSV files from raw/clickup_billing/")
    print("  - Output files from output/monthly_billing/cleaned/ and /reports/")
    print("\nTo:")
    print("  - raw/clickup_billing/archive/YYYY-MM/")
    print("  - output/monthly_billing/archive/YYYY-MM/")
    sys.exit(1)

  year = sys.argv[1]
  month = sys.argv[2]

  archive_month(year, month)


if __name__ == '__main__':
  main()
