# ClickUp Billing Report Processing

This directory contains scripts for processing monthly ClickUp time tracking exports into clean billing reports.

## Overview

The process takes raw ClickUp CSV exports and:
1. Cleans and transforms the data
2. Converts time durations to decimal hours
3. Maps categories to client names
4. Calculates billing amounts based on hourly rates
5. Tracks remaining budget and burn rate
6. Generates budget alerts when < 2 months remaining
7. Generates a clean CSV for accounting (with actual work date range)
8. Creates a detailed Markdown summary report (with billing month)
9. Archives previous month's data

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
- **Remaining budget**: Update the remaining budget dollars for each client

```yaml
billing_rates:
  "Job News": 175.00
  "Tri County Home Care": 150.00

remaining_budget:
  "Job News": 200.0
  "Tri County Home Care": 3000.0
```

### 3. Run the Processing Script

From the project root:
```bash
python3 scripts/billing/process_clickup_billing.py
```

Or from the billing directory:
```bash
cd scripts/billing
python3 process_clickup_billing.py
```

### 4. Review Outputs

The script generates files in `output/monthly_billing/`:

**Clean CSV** (`cleaned/billing_report_YYYY-MM-DD_to_YYYY-MM-DD.csv`)
- Date range shows actual work period
- Example: `billing_report_2025-09-04_to_2025-09-29.csv`
- Simplified format for accounting
- Contains: Date, Month-Year, Client, Name, Billable Hours, Task, Task ID

**Summary Report** (`reports/billing_summary_YYYY-MM.md`)
- Month/year shows current billing month
- Example: `billing_summary_2025-10.md` (October billing for September work)
- Internal reference document with:
  - Total hours and amounts by client
  - Remaining budget and burn rate (months left)
  - Budget alerts when < 2 months remaining
  - Total hours by team member
  - Monthly breakdown by client
  - Detailed billing log

### 5. Archive Previous Month

Before processing the next month, archive the previous month's data:
```bash
python3 scripts/billing/archive_month.py YYYY MM
```

Example: `python3 scripts/billing/archive_month.py 2025 09`

This moves:
- Raw CSV from `raw/clickup_billing/` → `raw/clickup_billing/archive/YYYY-MM/`
- Reports from `output/monthly_billing/` → `output/monthly_billing/archive/YYYY-MM/`

### 6. Update Remaining Budget

After reviewing the report, update `config.yaml` with the new remaining budget for next month:

```yaml
remaining_budget:
  "Job News": 25.0        # Was 200, billed 175 this month
  "Tri County Home Care": 1312.50   # Was 3000, billed 1687.50 this month
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
  "BILLABLE - JN": "Job News"
  "BILLABLE - VA": "Tri County Home Care"
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
- `remaining_budget`

## Dependencies

### Python Packages

Install required Python packages:

```bash
pip3 install pandas pyyaml
```

Or they're already in your project's `requirements.txt`:
```
pandas>=2.0.0
pyyaml>=6.0
```
