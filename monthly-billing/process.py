#!/usr/bin/env python3
"""
Process ClickUp billing export CSV files.

This script:
1. Reads raw ClickUp CSV export
2. Cleans and transforms data according to config
3. Generates clean CSV for accounting
4. Creates summary report in Markdown
"""

import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime
import sys


def load_config(config_path='config.yaml'):
  """Load configuration from YAML file."""
  config_file = Path(__file__).parent / config_path
  with open(config_file, 'r') as f:
    return yaml.safe_load(f)


def parse_duration_to_decimal(duration_str):
  """
  Convert ClickUp duration format to decimal hours.

  Supports multiple formats:
    '1:15:00' -> 1.25
    '2:30' -> 2.5
    '2 h 30 m' -> 2.5
    '1 h' -> 1.0
    '45 m' -> 0.75
  """
  if pd.isna(duration_str) or duration_str == '':
    return 0.0

  duration_str = str(duration_str).strip()

  # Handle new ClickUp format: "2 h 30 m" or "1 h" or "45 m"
  if 'h' in duration_str or 'm' in duration_str:
    hours = 0
    minutes = 0

    # Extract hours
    if 'h' in duration_str:
      h_parts = duration_str.split('h')
      try:
        hours = int(h_parts[0].strip())
        duration_str = h_parts[1] if len(h_parts) > 1 else ''
      except (ValueError, IndexError):
        pass

    # Extract minutes
    if 'm' in duration_str:
      m_parts = duration_str.split('m')
      try:
        minutes = int(m_parts[0].strip())
      except (ValueError, IndexError):
        pass

    return round(hours + (minutes / 60), 2)

  # Handle format like "1:15:00" or "1:15"
  parts = duration_str.split(':')

  try:
    if len(parts) >= 2:
      hours = int(parts[0])
      minutes = int(parts[1])
      return round(hours + (minutes / 60), 2)
    else:
      # If no colon, assume it's already decimal
      return float(duration_str)
  except (ValueError, IndexError):
    print(f"Warning: Could not parse duration '{duration_str}', defaulting to 0.0")
    return 0.0


def transform_category(category, transforms):
  """Transform category names according to config mapping."""
  return transforms.get(category, category)


def parse_date(date_str, format_str=None):
  """Parse date string to datetime object."""
  # Use pandas auto-detection which handles timezones better
  return pd.to_datetime(date_str)


def process_clickup_export(input_file, config):
  """Process ClickUp export and return cleaned DataFrame."""

  # Read CSV
  df = pd.read_csv(input_file)

  # Select only columns we need (using original ClickUp names)
  required_cols = list(config['column_mapping'].keys())
  df = df[required_cols].copy()

  # Parse and format dates (just extract the date, ignore time and timezone)
  # Remove timezone from date string (e.g., "09/29/2025, 1:23:21 PM EDT" -> "09/29/2025")
  df['Date_Only'] = df['Start Text'].str.split(',').str[0]
  df['Start_DateTime'] = pd.to_datetime(df['Date_Only'], format='%m/%d/%Y')
  df['Date'] = df['Start_DateTime'].dt.date
  df['Month-Year'] = df['Start_DateTime'].dt.strftime('%Y-%m')

  # Convert duration to decimal hours (Time Tracked Text is in HH:MM:SS format)
  df['Billable Hours'] = df['Time Tracked Text'].apply(parse_duration_to_decimal)

  # Transform categories
  df['Client'] = df['CATEGORY'].apply(
    lambda x: transform_category(x, config['category_transforms'])
  )

  # Rename columns for final output
  df = df.rename(columns={
    'Username': 'Name',
    'Task Name': 'Task',
    'Custom Task ID': 'Task ID'
  })

  # Select final columns in desired order
  final_cols = ['Date', 'Month-Year', 'Client', 'Name', 'Billable Hours', 'Task', 'Task ID']
  df = df[final_cols]

  # Sort by date, then client
  df = df.sort_values(['Date', 'Client', 'Name'])

  return df


def calculate_billing_amounts(df, config):
  """Calculate billing amounts based on rates."""
  rates = config['billing_rates']

  df['Billing Rate'] = df['Client'].map(rates)
  df['Amount'] = (df['Billable Hours'] * df['Billing Rate']).round(2)

  return df


def generate_summary_report(df, config, output_path):
  """Generate Markdown summary report."""

  rates = config['billing_rates']

  # Calculate aggregations
  total_by_client = df.groupby('Client').agg({
    'Billable Hours': 'sum',
    'Amount': 'sum'
  }).round(2)

  total_by_name = df.groupby('Name').agg({
    'Billable Hours': 'sum',
    'Amount': 'sum'
  }).round(2)

  total_by_client_month = df.groupby(['Client', 'Month-Year']).agg({
    'Billable Hours': 'sum',
    'Amount': 'sum'
  }).round(2)

  grand_total_hours = df['Billable Hours'].sum()
  grand_total_amount = df['Amount'].sum()

  # Get date range - use full month based on the data
  actual_min_date = df['Date'].min()
  actual_max_date = df['Date'].max()

  # Convert to datetime to extract year and month
  min_dt = pd.to_datetime(actual_min_date)
  max_dt = pd.to_datetime(actual_max_date)

  # Create full month range
  report_start = f"{min_dt.year:04d}-{min_dt.month:02d}-01"

  # Calculate last day of the month
  if max_dt.month == 12:
    last_day = 31
  else:
    from calendar import monthrange
    last_day = monthrange(max_dt.year, max_dt.month)[1]

  report_end = f"{max_dt.year:04d}-{max_dt.month:02d}-{last_day:02d}"

  # Build markdown report
  report = f"""# Billing Summary Report

**Report Period:** {report_start} to {report_end}
**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}

---

## Summary by Client

| Client | Billable Hours | Rate | Amount |
|--------|----------------|------|--------|
"""

  for client, row in total_by_client.iterrows():
    rate = rates.get(client, 0)
    this_month_amount = row['Amount']
    report += f"| {client} | {row['Billable Hours']:.2f} | ${rate:.2f} | ${this_month_amount:.2f} |\n"

  report += f"\n**Grand Total:** {grand_total_hours:.2f} hours = ${grand_total_amount:.2f}\n\n"

  report += """---

## Summary by Team Member

| Name | Billable Hours | Amount |
|------|----------------|--------|
"""

  for name, row in total_by_name.iterrows():
    report += f"| {name} | {row['Billable Hours']:.2f} | ${row['Amount']:.2f} |\n"

  report += """\n---

## Summary by Client and Month

| Client | Month | Billable Hours | Amount |
|--------|-------|----------------|--------|
"""

  for (client, month), row in total_by_client_month.iterrows():
    report += f"| {client} | {month} | {row['Billable Hours']:.2f} | ${row['Amount']:.2f} |\n"

  report += """\n---

## Detailed Billing Log

| Date | Client | Name | Hours | Rate | Amount | Task |
|------|--------|------|-------|------|--------|------|
"""

  for _, row in df.iterrows():
    report += f"| {row['Date']} | {row['Client']} | {row['Name']} | {row['Billable Hours']:.2f} | ${row['Billing Rate']:.2f} | ${row['Amount']:.2f} | {row['Task']} |\n"

  # Write report
  with open(output_path, 'w') as f:
    f.write(report)

  print(f"✓ Summary report written to: {output_path}")


def main():
  """Main processing function."""

  # Set up paths
  script_dir = Path(__file__).parent
  input_dir = script_dir / 'input'
  output_csv_dir = script_dir / 'output' / 'cleaned'
  output_report_dir = script_dir / 'output' / 'reports'

  # Load configuration
  config = load_config()

  # Find most recent CSV in input directory
  csv_files = sorted(input_dir.glob('*.csv'))

  if not csv_files:
    print(f"Error: No CSV files found in {input_dir}")
    print("Please place your ClickUp export CSV in the monthly-billing/input/ directory")
    sys.exit(1)

  input_file = csv_files[-1]  # Use most recent
  print(f"Processing: {input_file.name}")

  # Process the export
  df = process_clickup_export(input_file, config)

  # Calculate billing amounts
  df = calculate_billing_amounts(df, config)

  # Get actual date range from the data for filenames
  min_date = df['Date'].min()
  max_date = df['Date'].max()

  # Convert to datetime to extract components
  min_dt = pd.to_datetime(min_date)
  max_dt = pd.to_datetime(max_date)

  # CSV filename: Full date range of actual work performed (YYYY-MM-DD_to_YYYY-MM-DD)
  date_range = f"{min_dt.strftime('%Y-%m-%d')}_to_{max_dt.strftime('%Y-%m-%d')}"
  csv_filename = f"billing_report_{date_range}.csv"

  # Report filename: Current billing month/year (YYYY-MM)
  # This is for the current month's billing, covering work done in previous period
  current_month_year = datetime.now().strftime('%Y-%m')
  report_filename = f"billing_summary_{current_month_year}.md"

  # Save cleaned CSV (without billing amounts - for accounting)
  output_csv = output_csv_dir / csv_filename
  df_for_accounting = df[['Date', 'Month-Year', 'Client', 'Name', 'Billable Hours', 'Task', 'Task ID']]
  df_for_accounting.to_csv(output_csv, index=False)
  print(f"✓ Clean CSV written to: {output_csv}")

  # Generate summary report
  output_report = output_report_dir / report_filename
  generate_summary_report(df, config, output_report)

  print("\n✅ Processing complete!")
  print(f"\nTotal records processed: {len(df)}")
  print(f"Total billable hours: {df['Billable Hours'].sum():.2f}")
  print(f"Total amount: ${df['Amount'].sum():.2f}")


if __name__ == '__main__':
  main()
