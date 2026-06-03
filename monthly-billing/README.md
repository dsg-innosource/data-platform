# ClickUp Billing Report Processing

Processes the monthly ClickUp time tracking export into a clean billing CSV for accounting and a Markdown summary report.

## What it does

1. Reads the most recent ClickUp CSV in `input/`
2. Parses durations to decimal hours
3. Maps ClickUp categories to client names
4. Calculates billing amounts using rates in `config.yaml`
5. Writes a clean CSV to `output/cleaned/` (date range = actual work period)
6. Writes a Markdown summary to `output/reports/` (filename = current billing month)

## Monthly workflow

### 1. Export from ClickUp

Export the monthly billable time report from ClickUp and drop the CSV into `input/`. The file Clickup generates already has a timestamped name like `2026-05-01T16_55_11.395Z_D5I.csv` — leave it as-is.

### 2. Update billing rates (only if they changed)

Edit `config.yaml` if any client rate changed:

```yaml
billing_rates:
  "Job News": 250.00
  "Tri County Home Care": 200.00
```

### 3. Run

From the project root:

```bash
python3 monthly-billing/process.py
```

### 4. Review outputs

- `output/cleaned/billing_report_YYYY-MM-DD_to_YYYY-MM-DD.csv` — clean CSV for accounting (Date, Month-Year, Client, Name, Billable Hours, Task, Task ID)
- `output/reports/billing_summary_YYYY-MM.md` — internal summary with totals by client, by team member, by client/month, and the detailed billing log

### 5. Archive

After the run is reviewed and sent, move the input CSV and both outputs into `archive/YYYY-MM/` (folder named for the billing month, matching the summary filename):

```bash
mkdir -p monthly-billing/archive/2026-05
mv monthly-billing/input/*.csv monthly-billing/archive/2026-05/
mv monthly-billing/output/cleaned/*.csv monthly-billing/archive/2026-05/
mv monthly-billing/output/reports/*.md monthly-billing/archive/2026-05/
```

## Configuration reference

### Column mapping

Maps ClickUp export columns to internal names:

```yaml
column_mapping:
  Username: Name
  "Start Text": Date
  "Time Tracked Text": Billable Hours
  "Task Name": Task
  "Custom Task ID": Task ID
  CATEGORY: Client
```

### Category transforms

Maps ClickUp category tags to client names:

```yaml
category_transforms:
  "BILLABLE - JN": "Job News"
  "BILLABLE - VA": "Tri County Home Care"
```

Add new clients here and to `billing_rates` when onboarding.

## Troubleshooting

- **No CSV files found** — confirm the export landed in `monthly-billing/input/`
- **Duration parsing warnings** — check the format of the `Time Tracked Text` column in the export
- **Missing client in output amounts** — add the client to `billing_rates` (and `category_transforms` if the ClickUp tag differs from the desired display name)

## Dependencies

`pandas` and `pyyaml`, both already in the repo's `requirements.txt`.
