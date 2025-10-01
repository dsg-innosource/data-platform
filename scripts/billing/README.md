# ClickUp Billing Report Processing

This directory contains scripts for processing monthly ClickUp time tracking exports into clean billing reports.

## Overview

The process takes raw ClickUp CSV exports and:
1. Cleans and transforms the data
2. Converts time durations to decimal hours
3. Maps categories to client names
4. Calculates billing amounts based on hourly rates
5. Generates a clean CSV for accounting
6. Creates a detailed Markdown summary report

## Monthly Workflow

### 1. Export from ClickUp

Export your monthly billable time report from ClickUp and save it to:
```
raw/clickup_billing/
```

Name it something like: `clickup_export_2025_01.csv`

### 2. Update Configuration (if needed)

Before processing, check `config.yaml` and update:

- **Billing rates**: If rates changed for any client
- **Remaining hours**: Update the remaining contract hours for each client

```yaml
billing_rates:
  'Job News': 75.00
  'Visiting Angels': 65.00

remaining_hours:
  'Job News': 100.0
  'Visiting Angels': 150.0
```

### 3. Run the Processing Script

From the project root:
```bash
python scripts/billing/process_clickup_billing.py
```

Or from the billing directory:
```bash
cd scripts/billing
python process_clickup_billing.py
```

### 4. Review Outputs

The script generates two files in `output/monthly_billing/`:

**Clean CSV** (`cleaned/billing_report_MM_YYYY.csv`)
- Simplified format for accounting
- Contains: Date, Month-Year, Client, Name, Billable Hours, Task, Task ID

**Summary Report** (`reports/billing_summary_MM_YYYY.md`)
- Internal reference document with:
  - Total hours and amounts by client
  - Remaining hours and budget per client
  - Total hours by team member
  - Monthly breakdown by client
  - Detailed billing log

### 5. Update Remaining Hours

After reviewing the report, update `config.yaml` with the new remaining hours for next month:

```yaml
remaining_hours:
  'Job News': 75.0      # Was 100, billed 25 this month
  'Visiting Angels': 135.0  # Was 150, billed 15 this month
```

## Configuration Reference

### Column Mapping

Controls how ClickUp columns are renamed:

```yaml
column_mapping:
  Username: Name
  'Start Date': Date
  'Time Tracked': Billable Hours
  'Task name': Task
  'Custom Task ID': Task ID
  Category: Client
```

### Category Transforms

Maps ClickUp category tags to client names:

```yaml
category_transforms:
  'JN Billable': 'Job News'
  'VA Billable': 'Visiting Angels'
```

Add new clients here as needed.

### Output Settings

Customize output filenames:

```yaml
output:
  csv_filename_template: 'billing_report_{month}_{year}.csv'
  report_filename_template: 'billing_summary_{month}_{year}.md'
```

## Troubleshooting

### No CSV files found

Make sure you've placed your ClickUp export in `raw/clickup_billing/`

### Duration parsing errors

If you see warnings about duration parsing, check the format of the 'Time Tracked' column in your export.

### Missing clients in billing rates

If a client appears in the data but isn't in `config.yaml`, add them to both:
- `category_transforms` (if they use a different category name)
- `billing_rates`
- `remaining_hours`

## Dependencies

Install required Python packages:

```bash
pip install pandas pyyaml
```

Or add to your project's `requirements.txt`:
```
pandas>=2.0.0
pyyaml>=6.0
```
