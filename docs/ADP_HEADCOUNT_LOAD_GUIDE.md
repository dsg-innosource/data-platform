# ADP Headcount Load Guide

This guide provides step-by-step instructions for loading ADP headcount data into the data warehouse. Follow this process to extract, transform, and load employee headcount data from ADP Excel reports.

> **💡 Recommended Workflow:** Always run with `--test-mode` first to validate your data with DuckDB before loading to production PostgreSQL. No credentials needed for testing!

## Overview

The ADP headcount load process is an automated ETL pipeline that:
1. **Extracts** data from ADP Excel files (.xls or .xlsx)
2. **Transforms** the data (cleans, standardizes, validates)
3. **Loads** to Bronze table (`bronze.adp_tenure_history`)
4. **Calculates** active headcount metrics in Silver table (`silver.fact_active_headcount`)

**Key Features:**
- **Test Mode**: Safe testing with DuckDB before production runs
- **Automatic Archiving**: Processed files moved to archive with timestamps (NEW!)
- Automatic file detection (uses most recent file in `raw/headcount/`)
- Automatic Monday date calculation for reporting periods
- Data validation and error handling
- Excluded client code filtering (configurable)
- Comprehensive logging

## Prerequisites

### 1. Database Configuration

**For Testing (Recommended First):**

No configuration needed! Test mode uses a local DuckDB file (`test_adp.db`) that requires no credentials.

**For Production:**

Create and configure your database connection file:

```bash
# Navigate to transformations directory
cd transformations

# Copy the example environment file
cp .env.example .env
```

Edit `.env` with your database credentials:
```bash
DB_HOST=your-postgres-host.com
DB_PORT=5432
DB_NAME=your_database_name
DB_USERNAME=your_username
DB_PASSWORD=your_password
```

**Important:** You do NOT need a `.env` file for testing with `--test-mode`.

### 2. Python Dependencies

Ensure all required Python packages are installed:

```bash
# From the project root
pip install -r requirements.txt
```

**Required packages include:**
- pandas
- openpyxl (for .xlsx files)
- xlrd (for .xls files)
- sqlalchemy
- psycopg2-binary
- python-dotenv
- pyyaml
- duckdb (for test mode)

### 3. Database Tables

**For Test Mode:**
- Tables are automatically created in DuckDB when running with `--test-mode`
- No manual setup required

**For Production (PostgreSQL):**

The pipeline requires these tables to exist:

**Bronze Table:** `bronze.adp_tenure_history`
- Stores raw ADP tenure data with snapshot dates
- Includes all employee records from each ADP export

**Silver Table:** `silver.fact_active_headcount`
- Stores calculated active headcount metrics by department
- Uses snapshot_date and report_date for period tracking

*Note: In production, if tables don't exist, you'll need to create them before running the pipeline.*

### 4. ADP Excel File

Obtain the ADP tenure report with these required columns:
- File Number
- Payroll Name
- Hire Date
- Position Status
- Home Department Code
- Clock Full Code
- Associate ID
- Plus additional fields (see technical details below)

## Step-by-Step Process

### Step 1: Prepare Your ADP File

1. Download the ADP Tenure report from ADP portal
2. Verify the file is in `.xls` or `.xlsx` format
3. Check that the filename is descriptive (e.g., `Foreflow Tenure_JP.xls`)

### Step 2: Place File in Headcount Directory

Move your ADP file to the designated location:

```bash
# Copy file to the raw/headcount directory
cp ~/Downloads/your-adp-file.xls raw/headcount/
```

**Important:** The pipeline automatically detects the **most recent file** by modification time, so you don't need to specify the filename.

### Step 3: Test the Pipeline (Recommended First)

**Always test with a real file in test mode before running in production!**

Run the pipeline in test mode using DuckDB (safe, no production impact):

```bash
python -m transformations.pipelines.adp_tenure_pipeline --test-mode
```

This will:
- 🧪 Use DuckDB (`test_adp.db`) instead of PostgreSQL
- Automatically find the most recent file in `raw/headcount/`
- Calculate snapshot_date as current/most recent Monday
- Calculate report_date as previous Monday
- Process the complete ETL pipeline safely
- Create schemas and tables automatically

**What You'll See:**
```
INFO - 🧪 Running in TEST MODE - data will be loaded to DuckDB (config.test.yaml)
INFO - Auto-detected most recent file: raw/headcount/Foreflow Tenure.xls
INFO - Starting ADP pipeline for Week of 2025-10-27 through 2025-11-02
INFO - Successfully read 16178 records from file
INFO - Pipeline completed successfully. Bronze: 16178, Silver: 80 records
```

### Step 4: Verify Test Results

After running in test mode, verify the data loaded correctly:

```bash
python -c "
import duckdb
conn = duckdb.connect('test_adp.db', read_only=True)

print('Bronze records:', conn.execute('SELECT COUNT(*) FROM bronze.adp_tenure_history').fetchone()[0])
print('Silver records:', conn.execute('SELECT COUNT(*) FROM silver.fact_active_headcount').fetchone()[0])
print()
print('Headcount by department (top 10):')
print(conn.execute('SELECT department_number, active_count FROM silver.fact_active_headcount ORDER BY active_count DESC LIMIT 10').fetchdf())

conn.close()
"
```

### Step 5: Run in Production (After Testing)

Once you've verified the test results, run against production PostgreSQL:

#### Option A: Production Mode (Default)

```bash
python -m transformations.pipelines.adp_tenure_pipeline
```

**What You'll See:**
```
INFO - 🚀 Running in PRODUCTION MODE - data will be loaded to PostgreSQL (config.yaml)
INFO - Auto-detected most recent file: raw/headcount/Foreflow Tenure_JP.xls
INFO - Pipeline completed successfully. Bronze: 16178, Silver: 80 records
```

#### Option B: Specify File Explicitly

```bash
python -m transformations.pipelines.adp_tenure_pipeline \
  --file "raw/headcount/Foreflow Tenure_JP.xls"
```

#### Option C: Custom Dates

Process with specific snapshot and report dates:

```bash
python -m transformations.pipelines.adp_tenure_pipeline \
  --snapshot-date "2025-01-20" \
  --report-date "2025-01-13"
```

**Date Requirements:**
- Dates must be in `YYYY-MM-DD` format
- Both dates should be Mondays (validated by the pipeline)
- Report date should be 7 days before snapshot date

#### Option D: Force Reprocess

If data already exists for the snapshot date and you want to replace it:

```bash
python -m transformations.pipelines.adp_tenure_pipeline --force
```

This will delete existing data for that snapshot_date before loading new data.

#### Option E: Test Mode with Custom Options

You can combine test mode with other flags:

```bash
# Test with specific file
python -m transformations.pipelines.adp_tenure_pipeline --test-mode --file "raw/headcount/specific_file.xls"

# Test with custom dates
python -m transformations.pipelines.adp_tenure_pipeline --test-mode --snapshot-date "2025-01-20" --report-date "2025-01-13"

# Test with force reprocess
python -m transformations.pipelines.adp_tenure_pipeline --test-mode --force
```

### Step 6: Monitor the Process

The pipeline will output logs to the console showing progress:

**Test Mode:**
```
INFO - 🧪 Running in TEST MODE - data will be loaded to DuckDB (config.test.yaml)
INFO - Auto-detected most recent file: raw/headcount/Foreflow Tenure_JP.xls
INFO - Starting ADP pipeline for Week of 2025-01-13 to 2025-01-20
INFO - Snapshot date: 2025-01-20, Report date: 2025-01-13
INFO - Reading ADP file: raw/headcount/Foreflow Tenure_JP.xls
INFO - Successfully read 150 records from file
INFO - ADP file structure validation passed
INFO - Cleaning and transforming data
INFO - Data validation passed: 150 records
INFO - Loading data to bronze table
INFO - Loaded 150 records to bronze.adp_tenure_history
INFO - Executing headcount calculation
INFO - Loaded 12 records to silver.fact_active_headcount
INFO - Pipeline completed successfully. Bronze: 150, Silver: 12 records
```

**Production Mode:**
```
INFO - 🚀 Running in PRODUCTION MODE - data will be loaded to PostgreSQL (config.yaml)
INFO - Auto-detected most recent file: raw/headcount/Foreflow Tenure_JP.xls
INFO - Starting ADP pipeline for Week of 2025-01-13 to 2025-01-20
INFO - Snapshot date: 2025-01-20, Report date: 2025-01-13
INFO - Reading ADP file: raw/headcount/Foreflow Tenure_JP.xls
INFO - Successfully read 150 records from file
INFO - ADP file structure validation passed
INFO - Cleaning and transforming data
INFO - Data validation passed: 150 records
INFO - Loading data to bronze table
INFO - Loaded 150 records to bronze.adp_tenure_history
INFO - Executing headcount calculation
INFO - Loaded 12 records to silver.fact_active_headcount
INFO - Pipeline completed successfully. Bronze: 150, Silver: 12 records
```

### Step 7: Verify the Results

After successful completion, verify the data was loaded:

#### For Test Mode (DuckDB)

Use Python to query the test database:

```bash
python -c "
import duckdb
conn = duckdb.connect('test_adp.db', read_only=True)

# Check Bronze Table
print('=== Bronze Table ===')
result = conn.execute('''
    SELECT snapshot_date, COUNT(*) as record_count,
           COUNT(DISTINCT home_department_code) as department_count
    FROM bronze.adp_tenure_history
    WHERE snapshot_date = \'2025-01-20\'
    GROUP BY snapshot_date
''').fetchdf()
print(result)

print('\n=== Silver Table ===')
result = conn.execute('''
    SELECT department_number, active_count, snapshot_date, report_date
    FROM silver.fact_active_headcount
    WHERE snapshot_date = \'2025-01-20\'
    ORDER BY active_count DESC
    LIMIT 10
''').fetchdf()
print(result)

conn.close()
"
```

#### For Production Mode (PostgreSQL)

Use your PostgreSQL client or psql:

**Check Bronze Table:**
```sql
SELECT
    snapshot_date,
    COUNT(*) as record_count,
    COUNT(DISTINCT home_department_code) as department_count
FROM bronze.adp_tenure_history
WHERE snapshot_date = '2025-01-20'
GROUP BY snapshot_date;
```

**Check Silver Table:**
```sql
SELECT
    snapshot_date,
    report_date,
    department_number,
    active_count,
    created_at
FROM silver.fact_active_headcount
WHERE snapshot_date = '2025-01-20'
ORDER BY department_number;
```

**Expected Results:**
- Bronze table should have one record per employee in the ADP file
- Silver table should have one record per department with active employee counts
- Employees with excluded client codes (01100) should not appear in active counts

## What to Expect

### Successful Execution

When the pipeline runs successfully, you should see:
1. **File Detection:** Confirmation of which file was selected
2. **Date Calculation:** Display of snapshot and report dates
3. **Record Counts:** Number of records read from Excel
4. **Validation:** Confirmation that required columns exist
5. **Bronze Load:** Number of records inserted to bronze table
6. **Silver Calculation:** Number of department records created
7. **File Archiving:** File moved to archive with timestamp (NEW!)
8. **Completion Message:** Final record counts

### Processing Time

- **Small files (< 200 records):** 5-10 seconds
- **Medium files (200-500 records):** 10-20 seconds
- **Large files (500+ records):** 20-30 seconds

### Data Protection

The pipeline includes built-in protections:
- **Duplicate Prevention:** Won't load data if snapshot_date already exists (unless `--force` is used)
- **Validation:** Checks for required columns before processing
- **Error Logging:** All errors are logged to `transformations/logs/pipeline_errors.md`

### Automatic File Archiving

After successful processing, the source file is automatically moved to the archive directory with a timestamp:

**Archive Location:** `raw/headcount/archive/`

**Filename Format:**
```
original_filename_snapshot_YYYYMMDD_processed_YYYYMMDDHHMMSS.ext
```

**Example:**
```
Original:  Foreflow Tenure.xls
Archived:  Foreflow Tenure_snapshot_20251103_processed_20251103095756.xls
```

**Why This Format?**
- **Chronological sorting:** Files sorted by processing timestamp (newest first)
- **Snapshot tracking:** Know which data period the file represents
- **Audit trail:** See exactly when each file was processed
- **No duplicates:** Each processing creates a unique filename

**Behavior:**
- ✅ Works in both test mode and production mode
- ✅ Happens only after successful completion (both bronze and silver loads)
- ✅ Overwrites if a file with the same name exists (unlikely due to timestamp)
- ⚠️ If pipeline fails, the original file remains in `raw/headcount/` for retry

**Finding Archived Files:**
```bash
# List all archived files (newest first)
ls -lt raw/headcount/archive/

# Find files for a specific snapshot date
ls raw/headcount/archive/*snapshot_20251103*

# Count total archived files
ls -1 raw/headcount/archive/ | wc -l
```

## Troubleshooting

### "No Excel files found in raw/headcount/"

**Problem:** The headcount directory is empty or doesn't contain .xls/.xlsx files.

**Solution:**
```bash
# Verify the directory exists
ls -la raw/headcount/

# Copy your ADP file to the directory
cp ~/Downloads/your-file.xls raw/headcount/
```

### "Data already exists for [date]"

**Problem:** Data has already been loaded for this snapshot date.

**Solution:**
- Use `--force` flag to overwrite existing data
- OR specify a different snapshot date with `--snapshot-date`

```bash
python -m transformations.pipelines.adp_tenure_pipeline --force
```

### "Missing required columns: [...]"

**Problem:** The ADP Excel file is missing required columns.

**Solution:**
- Verify you downloaded the correct ADP report type (Tenure Report)
- Check that column names match exactly (case-sensitive)
- Required columns are listed in the error message

### "Unable to connect to database"

**Problem:** Database credentials in `.env` are incorrect or database is unreachable.

**Solution:**

**For Testing:** Use test mode instead (no credentials needed):
```bash
python -m transformations.pipelines.adp_tenure_pipeline --test-mode
```

**For Production:**
1. Verify `.env` file exists in `transformations/` directory
2. Check database credentials are correct
3. Test database connection:
   ```bash
   psql -h your-host -U your-username -d your-database
   ```

### "DuckDB database locked" (Test Mode)

**Problem:** Test database file is locked by another process (e.g., DataGrip, DBeaver).

**Solution:**
1. Close any database GUI tools that might have the `test_adp.db` file open
2. Check for locked connections:
   ```bash
   lsof test_adp.db
   ```
3. Close the connections or restart the pipeline

### Pipeline Fails Mid-Process

**Problem:** Pipeline encounters an error during processing.

**Solution:**
1. Check error log: `transformations/logs/pipeline_errors.md`
2. Review the specific error message and context
3. Common issues:
   - Data type mismatches (check date formats)
   - Missing or null values in required fields
   - Database connection timeouts

### "Previous Monday" Date Seems Wrong

**Problem:** The automatically calculated report_date doesn't match expectations.

**Solution:**
The Monday date logic uses:
- **Snapshot Date:** Current week's Monday (or most recent Monday if today is not Monday)
- **Report Date:** Previous week's Monday (7 days before snapshot)

To use specific dates instead:
```bash
python -m transformations.pipelines.adp_tenure_pipeline \
  --snapshot-date "2025-01-20" \
  --report-date "2025-01-13"
```

## Configuration

### Excluded Client Codes

Some client codes are excluded from active headcount calculations. Configure in `transformations/config.yaml`:

```yaml
adp:
  bronze_table: "bronze.adp_tenure_history"
  silver_table: "silver.fact_active_headcount"
  excluded_client_codes: ["01100"]
```

To modify:
1. Edit `transformations/config.yaml`
2. Add or remove client codes from the `excluded_client_codes` list
3. Re-run the pipeline

### Logging Level

Adjust logging verbosity in `transformations/config.yaml`:

```yaml
logging:
  level: "INFO"  # Options: DEBUG, INFO, WARNING, ERROR
  error_log_file: "transformations/logs/pipeline_errors.md"
```

## Technical Details

### File Structure Requirements

The ADP Excel file must contain these columns:

**Required Columns:**
- File Number
- Payroll Name
- Hire Date
- Position Status
- Home Department Code
- Clock Full Code
- Associate ID

**Additional Columns:**
- Rehire Date
- Previous Termination Date
- Termination Date
- Termination Reason Description
- Leave of Absence Start Date
- Leave of Absence Return Date
- Home Department Description
- Payroll Company Code
- Position ID
- Clock Full Description
- Regular Pay Rate Amount
- Recruited by
- Business Unit Description
- Requisition Key
- Personal Contact: Personal Email
- Requisition_id
- applicant_id
- Regular Hours Total
- Overtime Hours Total
- Other hours
- Holiday
- Voluntary/Involuntary Termination Flag
- Personal Contact: Home Phone

### Data Transformations

The pipeline performs these transformations:

1. **Column Renaming:** Converts "Position Status" → "position_status"
2. **Date Parsing:** Converts string dates to proper date objects
3. **Department Code Padding:** Pads department codes to 6 digits with leading zeros
4. **Client Code Extraction:** Extracts client code from "Clock Full Code"
5. **Snapshot Date Addition:** Adds snapshot_date to each record

### Active Headcount Calculation

Active headcount is calculated using this SQL logic:

```sql
-- Counts active employees per department
-- Excludes employees with client codes in excluded list
-- Filters by Position Status = 'Active'
-- Groups by department_number
```

The calculation creates one record per department with:
- `snapshot_date`: The date of the data snapshot
- `active_count`: Number of active employees in that department
- `department_number`: The 6-digit padded department code
- `report_date`: The Monday date for reporting period
- `created_at`: Timestamp when the calculation was performed

## Advanced Usage

### Skip Headcount Calculation

Load to bronze only (skip silver calculation):

```bash
python -m transformations.pipelines.adp_tenure_pipeline --skip-calculation
```

Use this when you want to:
- Load raw data first and calculate later
- Troubleshoot bronze data issues
- Run custom calculations separately

### Process Historical Data

Load multiple historical files with specific dates:

```bash
# Week 1
python -m transformations.pipelines.adp_tenure_pipeline \
  --file "raw/headcount/2025-01-06.xls" \
  --snapshot-date "2025-01-06" \
  --report-date "2024-12-30"

# Week 2
python -m transformations.pipelines.adp_tenure_pipeline \
  --file "raw/headcount/2025-01-13.xls" \
  --snapshot-date "2025-01-13" \
  --report-date "2025-01-06"
```

### Testing with DuckDB

Run the full test suite without affecting production:

```bash
# Run all tests (uses DuckDB, not PostgreSQL)
python transformations/run_tests.py

# Run only integration tests
python transformations/run_tests.py --test-type integration

# Verbose output
python transformations/run_tests.py --verbose
```

## Support

### Log Files

Check these locations for detailed information:

- **Pipeline Errors:** `transformations/logs/pipeline_errors.md`
- **General Logs:** Console output during execution

### Testing

Sample data and tests are available:

```bash
# Location of sample data
transformations/tests/sample_data/sample_adp_data.xls

# Run tests to verify pipeline functionality
python transformations/run_tests.py
```

### Documentation

Additional documentation:

- **Pipeline Code:** `transformations/pipelines/adp_tenure_pipeline.py`
- **Transform Logic:** `transformations/adp/transform.py`
- **Load Logic:** `transformations/adp/load.py`
- **SQL Calculation:** `transformations/sql/adp_headcount_calculation.sql`

## Quick Reference

### Most Common Commands

```bash
# TEST MODE (recommended first - safe, no production impact)
python -m transformations.pipelines.adp_tenure_pipeline --test-mode

# Verify test results
python -c "import duckdb; conn = duckdb.connect('test_adp.db', read_only=True); print('Bronze:', conn.execute('SELECT COUNT(*) FROM bronze.adp_tenure_history').fetchone()[0]); print('Silver:', conn.execute('SELECT COUNT(*) FROM silver.fact_active_headcount').fetchone()[0]); conn.close()"

# PRODUCTION MODE - Standard weekly load (after testing)
python -m transformations.pipelines.adp_tenure_pipeline

# Production with force reload
python -m transformations.pipelines.adp_tenure_pipeline --force

# Production with custom dates
python -m transformations.pipelines.adp_tenure_pipeline \
  --snapshot-date "2025-01-20" --report-date "2025-01-13"

# Test mode with custom options
python -m transformations.pipelines.adp_tenure_pipeline --test-mode --file "raw/headcount/specific_file.xls"
python -m transformations.pipelines.adp_tenure_pipeline --test-mode --snapshot-date "2025-01-20" --report-date "2025-01-13"

# Run automated unit/integration tests
python transformations/run_tests.py
```

### Test vs Production Modes

| Feature | Test Mode (`--test-mode`) | Production Mode (default) |
|---------|---------------------------|---------------------------|
| Database | DuckDB (`test_adp.db`) | PostgreSQL |
| Configuration | `config.test.yaml` | `config.yaml` |
| .env Required | ❌ No | ✅ Yes |
| Auto-creates Tables | ✅ Yes | ❌ No |
| Safe for Testing | ✅ Yes | ⚠️ No |
| Indicator | 🧪 TEST MODE | 🚀 PRODUCTION MODE |

### File Locations

| Item | Location |
|------|----------|
| ADP Files (New) | `raw/headcount/*.xls` |
| Archived Files | `raw/headcount/archive/*.xls` |
| Pipeline Script | `transformations/pipelines/adp_tenure_pipeline.py` |
| Production Config | `transformations/config.yaml` |
| Test Config | `transformations/config.test.yaml` |
| Environment (Production) | `transformations/.env` |
| Test Database | `test_adp.db` (project root) |
| Error Logs | `transformations/logs/pipeline_errors.md` |
| Test Error Logs | `transformations/logs/test_pipeline_errors.md` |
| Tests | `transformations/tests/` |

---

**Last Updated:** 2025-11-03
**Version:** 3.0 (Added automatic file archiving)
