# ADP Headcount Monthly Process

Quick guide for running the monthly ADP headcount data load.

## Prerequisites

- Python 3.8+ installed
- Database credentials configured in `adp-headcount/config.yaml`
- ADP Excel export file (.xls format)

## Monthly Workflow

### Step 1: Get the ADP Export

1. Log into ADP
2. Navigate to Reports → Tenure Report
3. Export as Excel (.xls format)
4. Download the file

### Step 2: Place File in Input Folder

```bash
# Save the downloaded file to:
cp ~/Downloads/Foreflow_Tenure_*.xls adp-headcount/input/
```

### Step 3: Test with DuckDB (Recommended First)

Always test with DuckDB before running against production:

```bash
python adp-headcount/process.py --test-mode
```

**What this does:**
- Processes the file using a local DuckDB database
- Validates data transformation
- Tests calculations
- Does NOT affect production database

**Verify the test:**
```bash
# Check that records were loaded
# Bronze table should show employee records
# Silver table should show headcount metrics
```

### Step 4: Run Production Load

Once test looks good, run against production PostgreSQL:

```bash
python adp-headcount/process.py
```

**What this does:**
- Loads data to production database
- Creates records in `bronze.adp_tenure_history`
- Calculates headcount in `silver.fact_active_headcount`
- Automatically archives the processed file to `adp-headcount/archive/`

### Step 5: Verify Production Data

Check that data loaded correctly:

```sql
-- Verify bronze data
SELECT COUNT(*) FROM bronze.adp_tenure_history
WHERE snapshot_date = '[CURRENT_MONDAY]';

-- Verify silver calculations
SELECT * FROM silver.fact_active_headcount
WHERE snapshot_date = '[CURRENT_MONDAY]'
ORDER BY department_number;
```

## Common Options

**Process a specific file:**
```bash
python adp-headcount/process.py --file "adp-headcount/input/specific_file.xls"
```

**Use custom dates:**
```bash
python adp-headcount/process.py --snapshot-date "2025-11-04" --report-date "2025-10-28"
```

**Reprocess (overwrite existing data):**
```bash
python adp-headcount/process.py --force
```

## Quick Troubleshooting

**"No Excel files found"**: Place your .xls file in `adp-headcount/input/`

**"Data already exists"**: Use `--force` flag to overwrite, or check if you already processed this week

**Database connection error**: Verify credentials in `adp-headcount/config.yaml`

For detailed troubleshooting, see [Technical Documentation](../technical/ADP_Pipeline.md).

## File Locations

| Item | Location |
|------|----------|
| Input files | `adp-headcount/input/` |
| Processed files | `adp-headcount/archive/` |
| Configuration | `adp-headcount/config.yaml` |
| Test config | `adp-headcount/config.test.yaml` |
| Process script | `adp-headcount/process.py` |

---

**For technical details, architecture, and advanced configuration, see [ADP Pipeline Technical Documentation](../technical/ADP_Pipeline.md)**
