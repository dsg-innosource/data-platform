# ADP Headcount Pipeline - Technical Documentation

## Overview

The ADP Headcount Pipeline is a complete ETL (Extract, Transform, Load) system that processes ADP tenure data into a data warehouse for analysis and reporting.

## Architecture

### Pipeline Flow

```
1. Extract   → Read ADP Excel file (.xls)
2. Transform → Clean and standardize data
3. Load      → Insert to bronze.adp_tenure_history
4. Calculate → Generate silver.fact_active_headcount metrics
5. Archive   → Move processed file with timestamp
```

### Directory Structure

```
adp-headcount/
├── input/              # Place raw ADP .xls files here
├── archive/           # Processed files stored here
├── logs/              # Error logs and execution logs
├── modules/           # Core pipeline modules
│   ├── extract.py    # File reading and validation
│   ├── transform.py  # Data cleaning and mapping
│   ├── load.py       # Database operations
│   ├── config.py     # Configuration management
│   └── date_utils.py # Date calculation utilities
├── testing/          # Test suite (DuckDB-based)
├── config.yaml       # Production database config
├── config.test.yaml  # Test database config (DuckDB)
└── process.py        # Main orchestration script
```

## Database Schema

### Bronze Layer: `bronze.adp_tenure_history`

Raw data from ADP with minimal transformation.

```sql
CREATE TABLE bronze.adp_tenure_history (
    employee_id VARCHAR(50) NOT NULL,
    full_name VARCHAR(255),
    department_number VARCHAR(6),
    department_name VARCHAR(255),
    position_title VARCHAR(255),
    hire_date DATE,
    tenure_years DECIMAL(10, 2),
    snapshot_date DATE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (employee_id, snapshot_date)
);
```

**Purpose**: Historical archive of all ADP tenure snapshots

**Key columns**:
- `employee_id`: Unique identifier from ADP
- `snapshot_date`: Monday date when snapshot was taken
- `tenure_years`: Years of service calculated by ADP

### Silver Layer: `silver.fact_active_headcount`

Aggregated metrics by department.

```sql
CREATE TABLE silver.fact_active_headcount (
    department_number VARCHAR(6),
    snapshot_date DATE NOT NULL,
    report_date DATE,
    active_count INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (department_number, snapshot_date)
);
```

**Purpose**: Point-in-time headcount metrics by department

**Calculation logic**:
```sql
-- Counts active employees per department at snapshot_date
SELECT
    department_number,
    snapshot_date,
    report_date,
    COUNT(*) as active_count,
    CURRENT_TIMESTAMP as created_at
FROM bronze.adp_tenure_history
WHERE snapshot_date = ?
GROUP BY department_number, snapshot_date, report_date
```

## Configuration

### Production Config (`config.yaml`)

```yaml
database:
  type: postgresql
  host: your-db-host.com
  port: 5432
  database: your_database_name
  user: your_username
  password: ${DB_PASSWORD}  # Use environment variable

bronze_table: bronze.adp_tenure_history
silver_table: silver.fact_active_headcount

logging:
  level: INFO
  error_log_file: logs/pipeline_errors.md
```

### Test Config (`config.test.yaml`)

```yaml
database:
  type: duckdb
  database: test_adp.db

bronze_table: bronze.adp_tenure_history
silver_table: silver.fact_active_headcount

logging:
  level: DEBUG
  error_log_file: logs/pipeline_errors.md
```

**Key difference**: Test mode uses DuckDB (local file database) instead of PostgreSQL.

## Data Transformation Logic

### Date Calculations

The pipeline uses "Monday logic" for consistent weekly snapshots:

- **Snapshot Date**: Current Monday (or most recent Monday)
- **Report Date**: Previous Monday (7 days before snapshot)

This ensures data is captured at consistent weekly intervals.

### Column Mapping

| ADP Column | Database Column | Transformation |
|------------|----------------|----------------|
| Employee ID | employee_id | Direct mapping |
| Full Name | full_name | Trim whitespace |
| Department # | department_number | Cast to string, pad to 6 chars |
| Department Name | department_name | Trim whitespace |
| Position/Title | position_title | Trim whitespace |
| Hire Date | hire_date | Parse MM/DD/YYYY format |
| Tenure (Years) | tenure_years | Cast to decimal |

### Data Validation

**Extract phase**:
- File must be .xls or .xlsx format
- Required columns must exist
- File must contain data rows

**Transform phase**:
- Employee ID cannot be null
- Snapshot date must be valid
- Department number must be 6 characters or less

## Error Handling

### Automatic Error Logging

All errors are logged to `adp-headcount/logs/pipeline_errors.md`:

```markdown
## Pipeline Error - 2025-11-03 14:32:15

**Context:** File: Foreflow_Tenure.xls, Snapshot: 2025-11-04

**Error:** Database connection failed: could not connect to server

---
```

### Common Errors and Solutions

**Database Lock Error (DuckDB)**
```
Error: Could not set lock on file "test_adp.db"
```
**Solution**: Close any database GUI tools (DataGrip, DBeaver) connected to test_adp.db

**Table Does Not Exist**
```
Error: Table with name adp_tenure_history does not exist
```
**Solution**: The pipeline creates tables automatically. Ensure database user has CREATE TABLE permission.

**Invalid Date Format**
```
Error: time data '2025-11-03' does not match format
```
**Solution**: Check that dates in ADP export use MM/DD/YYYY format

**Duplicate Data**
```
Warning: Data already exists for 2025-11-04 (Bronze: 150, Silver: 12)
```
**Solution**: Use `--force` flag to overwrite, or verify you haven't already processed this week

## Advanced Usage

### Custom Date Processing

Process data for a specific week:

```bash
python adp-headcount/process.py \
  --snapshot-date "2025-11-04" \
  --report-date "2025-10-28"
```

**Note**: Dates must be Mondays (validation enforced)

### Skip Silver Calculation

Load bronze data only (skip headcount calculation):

```bash
python adp-headcount/process.py --skip-calculation
```

**Use case**: When you want to load raw data but manually run calculations later

### Reprocess Historical Data

Reprocess and overwrite existing data:

```bash
python adp-headcount/process.py \
  --file "adp-headcount/archive/old_file.xls" \
  --snapshot-date "2025-10-28" \
  --force
```

## File Archiving

### Automatic Archiving

After successful processing, files are automatically moved to `adp-headcount/archive/` with timestamp:

**Format**: `{filename}_snapshot_{YYYYMMDD}_processed_{YYYYMMDDHHMMSS}.xls`

**Example**: `Foreflow_Tenure_snapshot_20251104_processed_20251104143052.xls`

### Archive Benefits

- **Audit trail**: Know exactly when each file was processed
- **Reprocessing**: Can reprocess archived files if needed
- **Clean input folder**: Input folder stays clean for next month
- **Chronological sorting**: Timestamp ensures proper ordering

## Testing

### Test Mode (DuckDB)

Always test with DuckDB before production:

```bash
python adp-headcount/process.py --test-mode
```

**What gets tested**:
- File reading and parsing
- Data transformation logic
- Database schema creation
- SQL calculation logic
- Archiving functionality

**What doesn't change**:
- Production database (uses test_adp.db instead)
- Real archived files (test mode still archives to same location)

### Verifying Test Results

Connect to test database:

```bash
sqlite3 test_adp.db  # or use DBeaver/DataGrip

# Check bronze data
SELECT COUNT(*) FROM bronze.adp_tenure_history;

# Check silver metrics
SELECT * FROM silver.fact_active_headcount;

# Exit
.quit
```

### Test Suite

Run automated tests (development):

```bash
python adp-headcount/testing/run_tests.py
```

**Test coverage**:
- Extract module: File reading, validation
- Transform module: Data cleaning, mapping
- Load module: Database operations
- Integration: Complete end-to-end pipeline

## Performance

### Typical Processing Times

- **Small file** (100-200 employees): 2-5 seconds
- **Large file** (500+ employees): 5-10 seconds

### Optimization Notes

- File reading: Pandas handles Excel efficiently
- Database inserts: Batch inserts for better performance
- Calculations: SQL-based aggregation (fast)

## Security Considerations

### PII Data

ADP files contain Personally Identifiable Information (PII):

- Employee names
- Employee IDs
- Hire dates
- Department information

**Protection measures**:
- Input and archive folders excluded from git (`.gitignore`)
- Database access controlled via credentials
- No PII logged in error messages

### Configuration Security

**Best practices**:
- Use environment variables for database passwords
- Don't commit `config.yaml` with real credentials
- Restrict file permissions on config files

```bash
chmod 600 adp-headcount/config.yaml
```

## Monitoring and Maintenance

### Log Review

Regularly review error logs:

```bash
cat adp-headcount/logs/pipeline_errors.md
```

### Archive Cleanup

Periodically archive old processed files:

```bash
# Move files older than 6 months to long-term storage
find adp-headcount/archive/ -name "*.xls" -mtime +180 -exec mv {} /archive/long-term/ \;
```

### Database Maintenance

Monitor table sizes:

```sql
-- Check row counts
SELECT
    'bronze' as layer,
    COUNT(*) as rows,
    COUNT(DISTINCT snapshot_date) as snapshots
FROM bronze.adp_tenure_history
UNION ALL
SELECT
    'silver',
    COUNT(*),
    COUNT(DISTINCT snapshot_date)
FROM silver.fact_active_headcount;
```

## Troubleshooting

### Enable Debug Logging

For detailed logs during troubleshooting:

1. Edit `config.yaml` or `config.test.yaml`
2. Change `logging.level: INFO` to `logging.level: DEBUG`
3. Re-run the pipeline
4. Review detailed logs in console output

### Common Issues

**Issue**: Pipeline runs but no data in database

**Diagnosis**:
1. Check if file had data: `wc -l adp-headcount/input/file.xls`
2. Check transformation: Add debug prints in `modules/transform.py`
3. Verify database connection: Try manual SELECT query

**Issue**: Wrong date calculated

**Diagnosis**:
1. Check system date: `date`
2. Manually specify dates: `--snapshot-date` and `--report-date`
3. Review date logic in `modules/date_utils.py`

**Issue**: Archive folder filling up

**Solution**: Implement retention policy (keep last 12 months, archive older)

## Development

### Adding New Transformations

Edit `modules/transform.py`:

```python
def clean_adp_data(df, snapshot_date):
    # Add your transformation here
    df['new_column'] = df['old_column'].apply(your_function)
    return df
```

### Adding New Validations

Edit `modules/extract.py`:

```python
def validate_adp_file_structure(df):
    # Add validation
    if 'required_column' not in df.columns:
        raise ValueError("Missing required column")
```

### Modifying Database Schema

1. Update CREATE TABLE statements in this doc
2. Update schema in `modules/load.py`
3. Run migration on production database
4. Update pipeline code to use new columns

## Related Documentation

- **User Guide**: [How to run monthly ADP process](../guides/ADP_Headcount.md)
- **Database Schema**: [Complete schema documentation](../database/schemas.md)

---

**Last Updated**: 2025-11-03
**Version**: 2.0.0
