# Monthly Billing Process

Quick guide for processing monthly billing from ClickUp time tracking.

## Prerequisites

- Python 3.8+ installed with pandas and pyyaml
- Access to ClickUp with time tracking permissions
- Billing rates configured in `monthly-billing/config.yaml`

## Monthly Workflow

### Step 1: Export Time Tracking from ClickUp

1. **Log into ClickUp**
2. **Go to Time Tracking**
   - Click "Time" icon in left sidebar
   - Or navigate to Reports → Time Tracking
3. **Set Date Range**
   - Select the previous month (e.g., October 1-31)
4. **Export to CSV**
   - Ensure these columns are included:
     - Task name
     - Time tracked
     - Custom Field: Category
     - Date
     - Username
   - Click "Export to CSV"
5. **Save File**
   - Save to: `monthly-billing/input/`
   - Suggested name: `clickup_export_2024_10.csv`

### Step 2: Run the Billing Script

```bash
cd monthly-billing
python process.py
```

The script will automatically:
- Find the most recent CSV in `input/`
- Process and transform the data
- Calculate billing amounts
- Generate two output files

### Step 3: Review the Summary Report

Open the generated summary report:

```bash
# Example path
cat output/reports/billing_summary_2024-11.md
```

**Check for**:
- ✅ Total hours look correct
- ✅ Billing amounts match expectations
- ⚠️ Any budget warnings
- 📊 Breakdown by client and team member

### Step 4: Review the Accounting CSV

Preview the CSV for accounting:

```bash
head output/cleaned/billing_report_2024-10-01_to_2024-10-31.csv
```

**Verify**:
- Date range is correct
- All entries are categorized properly
- Hours are in decimal format
- Amounts are calculated correctly

### Step 5: Deliver Billing

1. **Send CSV to accounting**
   - Attach the file from `output/cleaned/`
   - Include any notes from summary report

2. **Review budget status**
   - Share budget warnings with stakeholders
   - Discuss renewals if budgets are low

3. **Archive the raw export**
   ```bash
   mv input/clickup_export_2024_10.csv archive/
   ```

## Output Files

| File | Location | Purpose |
|------|----------|---------|
| Billing CSV | `output/cleaned/billing_report_YYYY-MM-DD_to_YYYY-MM-DD.csv` | For accounting software |
| Summary Report | `output/reports/billing_summary_YYYY-MM.md` | Monthly insights and budget tracking |
| Raw Export (archived) | `archive/` | Original ClickUp data for reference |

## Quick Troubleshooting

**"No CSV files found"**: Place your ClickUp export in `monthly-billing/input/`

**"Category not found in mapping"**: Some ClickUp categories aren't configured for billing (they'll be skipped)

**Budget warnings**: See summary report for details on which clients need budget renewal

For detailed configuration and troubleshooting, see [Technical Documentation](../technical/Billing_Process.md).

## Common Billing Rates

Current rates (as of config):
- Job News: $175/hour
- Tri County Home Care: $150/hour

To update rates, see [Technical Documentation](../technical/Billing_Process.md).

---

**For configuration, technical details, and troubleshooting, see [Billing Process Technical Documentation](../technical/Billing_Process.md)**
