# Monthly Billing Process - Technical Documentation

## Overview

The monthly billing process transforms ClickUp time tracking exports into clean billing reports for accounting. The system handles data extraction, transformation, rate application, budget tracking, and report generation automatically.

## Architecture

### Processing Flow

```
1. Extract   → Read ClickUp CSV export
2. Transform → Map categories to clients, convert durations
3. Calculate → Apply billing rates, compute amounts
4. Track     → Monitor budgets and burn rates
5. Generate  → Create accounting CSV and summary report
```

### Directory Structure

```
monthly-billing/
├── input/              # Place ClickUp CSV exports here
├── output/
│   ├── cleaned/       # CSV files for accounting import
│   └── reports/       # Markdown summary reports
├── archive/           # Previous months' reports
├── testing/           # Future test suite
├── process.py         # Main processing script
├── config.yaml        # Billing configuration
└── README.md          # Basic documentation
```

## Configuration

### config.yaml Structure

```yaml
billing:
  # Hourly billing rates for each client
  rates:
    job_news: 175.00
    tri_county_home_care: 150.00

  # Budget tracking
  budgets:
    job_news:
      amount: 27500.00
      start_date: "2024-09-01"
    tri_county_home_care:
      amount: 8000.00
      start_date: "2024-10-01"

  # Map ClickUp categories to client names
  category_mapping:
    "Foreflow - Job News - Dev Time": "job_news"
    "Foreflow - Job News - Meetings": "job_news"
    "Foreflow - Job News - Planning & Design": "job_news"
    "Foreflow - Tri County Home Care - Dev Time": "tri_county_home_care"
    "Foreflow - Tri County Home Care - Meetings": "tri_county_home_care"
```

### ClickUp Column Mapping

```yaml
# Required columns from ClickUp export
column_mapping:
  "Task Name": task
  "Username": name
  "Start Text": date
  "Time Tracked Text": duration
  "CATEGORY": category
  "Custom Task ID": task_id
```

## Data Transformation

### Duration Conversion

ClickUp exports time in `HH:MM:SS` format. The script converts to decimal hours:

```python
"1:15:00"  →  1.25 hours
"2:30:00"  →  2.5 hours
"0:45:00"  →  0.75 hours
```

**Implementation**: `parse_duration_to_decimal()` function

### Category Mapping

ClickUp categories are mapped to billing clients using `category_mapping`:

```
"Foreflow - Job News - Dev Time"  →  "job_news"
→ Uses rate: $175/hour
→ Tracks against Job News budget
```

**Unmapped categories**: Entries with categories not in the mapping are skipped (not billable).

### Date Processing

Dates are extracted and formatted:

```python
# Input: "09/29/2025, 1:23:21 PM EDT"
# Extract: "09/29/2025"
# Parse to: datetime.date(2025, 9, 29)
# Format: "YYYY-MM-DD" for output
```

## Budget Tracking

### How It Works

For each client with a defined budget:

1. **Sum all billing** since `start_date`
2. **Calculate remaining** = `budget.amount` - `total_billed`
3. **Compute burn rate** = Average monthly billing
4. **Estimate runway** = `remaining / burn_rate`

### Budget Warnings

Warnings appear in summary report when:

- **< 2 months remaining** at current burn rate
- **< 20% of budget remaining**
- **Budget exceeded** (negative remaining)

**Example warning**:
```markdown
⚠️ **Budget Alert**: Less than 2 months of budget remaining at current burn rate.

- **Job News**: Only 1.5 months of budget remaining ($9,300.00)
```

### Resetting Budgets

When starting a new budget period:

```yaml
budgets:
  job_news:
    amount: 30000.00        # New budget amount
    start_date: "2024-12-01"  # New period start
```

**Effect**: Budget tracking only sums billing since the new `start_date`.

## Output Files

### Accounting CSV Format

**Filename**: `billing_report_YYYY-MM-DD_to_YYYY-MM-DD.csv`

**Columns**:
```csv
Date,Month-Year,Client,Name,Billable Hours,Task,Task ID
2024-10-15,2024-10,job_news,John Doe,4.50,"Implement authentication","FJ-123"
2024-10-18,2024-10,job_news,John Doe,1.00,"Client meeting","FJ-125"
```

**Purpose**: Import into QuickBooks, Xero, or other accounting software.

**Key features**:
- Decimal hours (not HH:MM)
- One row per time entry
- Sorted by date, client, name
- No billing amounts (internal only)

### Summary Report Format

**Filename**: `billing_summary_YYYY-MM.md`

**Sections**:

1. **Overview**: Total hours and amount for the period
2. **Client Breakdown**: Hours, amounts, and rates by client
3. **Budget Status**: Remaining budget, burn rate, months left
4. **Team Member Summary**: Hours and amounts by person
5. **Monthly Breakdown**: Trends by client and month
6. **Detailed Log**: Every time entry with calculations

**Purpose**: Executive summary with budget insights and warnings.

## Adding a New Client

### Step 1: Add Billing Rate

```yaml
billing:
  rates:
    new_client_name: 160.00
```

### Step 2: Add Budget (Optional)

```yaml
billing:
  budgets:
    new_client_name:
      amount: 15000.00
      start_date: "2024-11-01"
```

### Step 3: Add Category Mappings

```yaml
billing:
  category_mapping:
    "Foreflow - New Client - Dev Time": "new_client_name"
    "Foreflow - New Client - Meetings": "new_client_name"
```

### Step 4: Create Categories in ClickUp

1. Go to ClickUp Settings
2. Create Custom Field: "Category"
3. Add options matching your mapping exactly:
   - "Foreflow - New Client - Dev Time"
   - "Foreflow - New Client - Meetings"

## Updating Billing Rates

### Mid-Month Rate Change

**Option 1: Effective next month**
- Update `config.yaml` with new rate
- Run next month's billing with new rate
- Previous months unaffected

**Option 2: Reprocess current month**
1. Update rate in `config.yaml`
2. Delete current month's output files
3. Re-run: `python process.py`

### Rate Change History

To track rate changes, add comments to `config.yaml`:

```yaml
billing:
  rates:
    job_news: 185.00  # Updated 2024-12-01 from $175
```

## Troubleshooting

### Missing Python Modules

**Error**:
```
ModuleNotFoundError: No module named 'pandas'
```

**Solution**:
```bash
pip install pandas pyyaml
```

If using virtual environment:
```bash
source venv/bin/activate  # Mac/Linux
pip install pandas pyyaml
```

### Category Not Found Warnings

**Warning**:
```
Warning: Category 'Foreflow - New Project - Dev Time' not found in mapping, skipping entry
```

**Cause**: ClickUp export contains categories not in `config.yaml`

**Solutions**:

1. **If billable**: Add to category_mapping
2. **If not billable**: Ignore (expected behavior)

**To add**:
```yaml
billing:
  category_mapping:
    "Foreflow - New Project - Dev Time": "client_name"
```

### Invalid Duration Format

**Error**:
```
Warning: Could not parse duration '75 minutes', defaulting to 0.0
```

**Cause**: ClickUp export format changed

**Solution**:

1. Check export settings in ClickUp
2. Ensure time format is "HH:MM:SS"
3. If format is different, update `parse_duration_to_decimal()` in `process.py`

### Empty Output

**Symptom**: Script runs but output CSV has no data rows

**Possible causes**:

1. **All categories unmapped**
   - Check warnings during script execution
   - Verify category_mapping includes your ClickUp categories

2. **Wrong date range in export**
   - Ensure ClickUp export covers the intended period

3. **Zero hours logged**
   - Verify time tracking entries exist in ClickUp

**Diagnosis**:
```bash
# Check raw CSV has data
wc -l input/clickup_export.csv

# Should show line count > 1 (more than just headers)
```

### Budget Calculations Look Wrong

**Symptom**: Remaining budget or burn rate doesn't match expectations

**Common causes**:

1. **Wrong start_date**
   - Verify `budgets.start_date` matches when you started tracking
   - Script sums ALL billing since this date

2. **Missing historical months**
   - If you skipped processing some months, totals will be low
   - Process historical months to catch up

3. **Budget amount changed**
   - Ensure `budgets.amount` reflects total budget, not remaining

**Verify**:
```bash
# Review all historical reports
ls -lh output/reports/billing_summary_*.md

# Check for gaps in months
```

### Missing Columns in Export

**Error**:
```
KeyError: 'CATEGORY' not found in CSV columns
```

**Cause**: ClickUp export missing required custom fields

**Solution**:

1. In ClickUp export settings, include:
   - Task Name ✓
   - Username ✓
   - Start Text (date) ✓
   - Time Tracked Text ✓
   - Custom Field: Category ✓
   - Custom Task ID ✓

2. Ensure "Show custom fields" is enabled

## Advanced Usage

### Manual File Selection

Process a specific CSV (not most recent):

```bash
python process.py
# Then manually edit script or rename file to be most recent
```

**Note**: Script always processes most recent CSV in `input/`. To process specific file, ensure it's the newest.

### Batch Processing

Process multiple months at once:

```bash
# Place all CSVs in input/
# Process one at a time, moving each to archive after

for file in input/*.csv; do
    python process.py
    mv input/*.csv archive/
done
```

### Custom Date Ranges

**Current limitation**: Script uses actual dates from CSV data.

**Workaround**: Filter ClickUp export to desired date range before exporting.

## Data Formats

### Input CSV Format (from ClickUp)

```csv
Task Name,Username,Start Text,Time Tracked Text,CATEGORY,Custom Task ID
"Implement feature","John Doe","09/29/2025, 1:23:21 PM EDT","4:30:00","Foreflow - Job News - Dev Time","FJ-123"
```

### Output CSV Format (for accounting)

```csv
Date,Month-Year,Client,Name,Billable Hours,Task,Task ID
2024-09-29,2024-09,job_news,John Doe,4.50,"Implement feature","FJ-123"
```

### Summary Report Format

Markdown file with tables and sections. Example snippet:

```markdown
# Billing Summary Report

**Report Period:** 2024-10-01 to 2024-10-31
**Generated:** 2024-11-01 09:30

## Summary by Client

| Client | Billable Hours | Rate | Amount | Remaining Budget | Months Left |
|--------|----------------|------|--------|------------------|-------------|
| job_news | 52.00 | $175.00 | $9,100.00 | $18,400.00 | 2.0 |
```

## Security Considerations

### Sensitive Data

Billing files contain:
- Client names and rates
- Employee names and hours
- Budget information
- Task details

**Protection**:
- Do not commit output files to git
- Add to `.gitignore`:
  ```
  monthly-billing/output/
  monthly-billing/archive/
  monthly-billing/input/
  ```
- Restrict file permissions:
  ```bash
  chmod 700 monthly-billing/
  ```

### Configuration Security

**Best practices**:
- Keep `config.yaml` out of public repos if rates are confidential
- Use environment variables for sensitive data (future enhancement)
- Limit access to configuration files

## Performance

### Typical Processing Times

- **Small export** (50-100 entries): < 1 second
- **Large export** (500+ entries): 2-3 seconds

### File Sizes

- **Input CSV**: 5-50 KB typical
- **Output CSV**: Similar to input
- **Summary report**: 2-10 KB

## Monitoring

### Regular Checks

**Monthly**:
- ✅ Verify all hours were captured in ClickUp
- ✅ Check for category mapping warnings
- ✅ Review budget warnings
- ✅ Confirm CSV sent to accounting

**Quarterly**:
- 📊 Review burn rates vs. budget allocations
- 🔄 Update budgets for renewals
- 🗑️ Archive old reports (keep last 12-24 months accessible)

**Annually**:
- 💰 Review and update billing rates
- 📋 Audit budget tracking accuracy
- 🔍 Check for process improvements

## Related Documentation

- **User Guide**: [How to run monthly billing](../guides/Monthly_Billing.md)
- **Database Schema**: [Complete schema documentation](../database/schemas.md) (if applicable)

---

**Last Updated**: 2025-11-03
**Version**: 2.0.0
