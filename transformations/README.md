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
# 1. Setup environment
cp transformations/.env.example transformations/.env
# Edit .env file with your database credentials

# 2. Install dependencies
pip install -r requirements.txt

# 3. Process ADP file with automatic Monday date logic
python -m transformations.pipelines.adp_tenure_pipeline --file "Foreflow Tenure_JP.xls"

# 4. Process with custom dates
python -m transformations.pipelines.adp_tenure_pipeline --file "data.xls" --snapshot-date "2025-07-21" --report-date "2025-07-07"
```

**Pipeline Flow:**
1. Extract: Read ADP Excel file (.xls format)
2. Transform: Clean and standardize data
3. Load: Insert to `bronze.adp_tenure_history`
4. Calculate: Generate headcount metrics in `silver.fact_active_headcount`

**Configuration:** Pipeline settings are in `config.yaml`

**Error Handling:** Errors logged to `logs/pipeline_errors.md`

## Directory Structure

```
transformations/
├── adp/                    # ADP-specific modules
├── pipelines/              # Main pipeline orchestration scripts
├── sql/                    # SQL templates and queries
├── logs/                   # Error and execution logs
└── config.yaml            # Pipeline configuration
```