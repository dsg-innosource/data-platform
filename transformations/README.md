# Transformations Directory

This directory contains custom transformation layers as an alternative to traditional ELT tools like AirByte.

## Purpose

- Custom Python-based data transformations
- ETL/ELT pipeline components
- Data ingestion and processing scripts
- Integration with external data sources

## Available Pipelines

### ADP Tenure Pipeline

Complete ETL pipeline for processing ADP tenure data into the data warehouse.

**Quick Start:**
```bash
# 1. Setup environment (for production use)
cp transformations/.env.example transformations/.env
# Edit .env file with your database credentials

# 2. Install dependencies
pip install -r requirements.txt

# 3. Place your ADP file in raw/headcount/ directory

# 4. TEST MODE: Process with DuckDB (safe, no production impact)
python -m transformations.pipelines.adp_tenure_pipeline --test-mode

# 5. PRODUCTION: Process to PostgreSQL
python -m transformations.pipelines.adp_tenure_pipeline

# 6. Process specific file
python -m transformations.pipelines.adp_tenure_pipeline --file "Foreflow Tenure_JP.xls"

# 7. Process with custom dates
python -m transformations.pipelines.adp_tenure_pipeline --snapshot-date "2025-07-21" --report-date "2025-07-07"
```

**Pipeline Flow:**
1. Extract: Read ADP Excel file (.xls format)
2. Transform: Clean and standardize data
3. Load: Insert to `bronze.adp_tenure_history`
4. Calculate: Generate headcount metrics in `silver.fact_active_headcount`
5. Archive: Move processed file to `raw/headcount/archive/` with timestamp

**Configuration:** Pipeline settings are in `config.yaml`

**Error Handling:** Errors logged to `logs/pipeline_errors.md`

**Testing:** Run tests with DuckDB: `python transformations/run_tests.py`

**Archiving:** Processed files automatically archived with format:
`filename_snapshot_YYYYMMDD_processed_YYYYMMDDHHMMSS.ext`

## Directory Structure

```
transformations/
├── adp/                    # ADP-specific modules
├── pipelines/              # Main pipeline orchestration scripts
├── sql/                    # SQL templates and queries
├── tests/                  # Unit and integration tests
├── logs/                   # Error and execution logs
├── config.yaml            # Pipeline configuration
├── config.test.yaml       # Test configuration (DuckDB)
└── run_tests.py           # Test runner script
```

## Testing

The pipeline includes comprehensive tests using DuckDB as a test database:

```bash
# Run all tests
python transformations/run_tests.py

# Run only unit tests
python transformations/run_tests.py --test-type unit

# Run only integration tests  
python transformations/run_tests.py --test-type integration

# Verbose output
python transformations/run_tests.py --verbose
```

**Test Coverage:**
- Extract module: File reading, validation, data type preservation
- Transform module: Data cleaning, column mapping, validation
- Load module: Database operations with DuckDB
- Integration: Complete pipeline from Excel to database

**Safe Testing:** All tests use DuckDB instead of PostgreSQL, so you can test the complete pipeline without affecting production data.