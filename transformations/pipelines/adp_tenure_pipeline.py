#!/usr/bin/env python3
"""
ADP Tenure Pipeline

Main orchestration script for processing ADP tenure data.
Handles the complete ETL process from file input to database loading.

Usage:
    # Process with default dates (current Monday as snapshot, previous Monday as report)
    python -m transformations.pipelines.adp_tenure_pipeline --file "Foreflow Tenure_JP.xls"
    
    # Process with custom dates
    python -m transformations.pipelines.adp_tenure_pipeline --file "data.xls" --snapshot-date "2025-07-21" --report-date "2025-07-07"
    
    # Force reprocess (overwrite existing data)
    python -m transformations.pipelines.adp_tenure_pipeline --file "data.xls" --force
"""

import sys
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add the transformations directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from adp.extract import read_adp_file, validate_adp_file_structure
from adp.transform import clean_adp_data, validate_cleaned_data
from adp.load import (
    load_to_bronze_table, 
    execute_headcount_calculation,
    check_existing_data,
    delete_existing_data
)
from adp.config import get_logging_config
from adp.date_utils import get_monday_dates, validate_monday_dates, format_business_period


def setup_logging():
    """Setup logging configuration."""
    log_config = get_logging_config()
    
    # Create logs directory if it doesn't exist
    log_file = Path(__file__).parent.parent / log_config.get('error_log_file', 'logs/pipeline_errors.md')
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_config.get('level', 'INFO')),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file)
        ]
    )


# get_monday_dates function moved to adp.date_utils module


def log_error_to_markdown(error_msg: str, context: str = ""):
    """
    Log error to markdown file.
    
    Args:
        error_msg: Error message to log
        context: Additional context about the error
    """
    log_config = get_logging_config()
    log_file = Path(__file__).parent.parent / log_config.get('error_log_file', 'logs/pipeline_errors.md')
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    error_entry = f"""
## Pipeline Error - {timestamp}

**Context:** {context}

**Error:** {error_msg}

---

"""
    
    # Append to markdown log file
    with open(log_file, 'a') as f:
        f.write(error_entry)


def main():
    """Main pipeline orchestration function."""
    parser = argparse.ArgumentParser(description='ADP Tenure Data Pipeline')
    parser.add_argument('--file', required=True, help='Path to ADP Excel file')
    parser.add_argument('--snapshot-date', help='Snapshot date (YYYY-MM-DD). Defaults to current Monday')
    parser.add_argument('--report-date', help='Report date (YYYY-MM-DD). Defaults to previous Monday')
    parser.add_argument('--force', action='store_true', help='Force reprocess (overwrite existing data)')
    parser.add_argument('--skip-calculation', action='store_true', help='Skip headcount calculation (bronze only)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Determine dates
        if args.snapshot_date and args.report_date:
            snapshot_date = args.snapshot_date
            report_date = args.report_date
            # Validate provided dates
            validate_monday_dates(snapshot_date, report_date)
        elif args.snapshot_date:
            # If only snapshot_date provided, calculate report_date as snapshot_date - 7 days
            snapshot_dt = datetime.strptime(args.snapshot_date, '%Y-%m-%d')
            report_dt = snapshot_dt - timedelta(days=7)
            snapshot_date = args.snapshot_date
            report_date = report_dt.strftime('%Y-%m-%d')
            validate_monday_dates(snapshot_date, report_date)
        else:
            # Use Monday logic (automatic calculation)
            snapshot_date, report_date = get_monday_dates()
        
        logger.info(f"Starting ADP pipeline for {format_business_period(snapshot_date, report_date)}")
        logger.info(f"Snapshot date: {snapshot_date}, Report date: {report_date}")
        
        # Step 1: Extract
        logger.info(f"Reading ADP file: {args.file}")
        raw_df = read_adp_file(args.file)
        validate_adp_file_structure(raw_df)
        
        # Step 2: Transform
        logger.info("Cleaning and transforming data")
        cleaned_df = clean_adp_data(raw_df, snapshot_date)
        validate_cleaned_data(cleaned_df)
        
        # Step 3: Check for existing data
        existing_bronze = check_existing_data(snapshot_date, 'bronze')
        existing_silver = check_existing_data(snapshot_date, 'silver')
        
        if (existing_bronze > 0 or existing_silver > 0) and not args.force:
            logger.warning(
                f"Data already exists for {snapshot_date} "
                f"(Bronze: {existing_bronze}, Silver: {existing_silver}). "
                f"Use --force to overwrite."
            )
            return
        
        # Step 4: Delete existing data if force is enabled
        if args.force:
            if existing_silver > 0:
                delete_existing_data(snapshot_date, 'silver')
            if existing_bronze > 0:
                delete_existing_data(snapshot_date, 'bronze')
        
        # Step 5: Load to bronze
        logger.info("Loading data to bronze table")
        bronze_records = load_to_bronze_table(cleaned_df)
        
        # Step 6: Execute headcount calculation (unless skipped)
        if not args.skip_calculation:
            logger.info("Executing headcount calculation")
            silver_records = execute_headcount_calculation(snapshot_date, report_date)
            logger.info(f"Pipeline completed successfully. Bronze: {bronze_records}, Silver: {silver_records} records")
        else:
            logger.info(f"Pipeline completed successfully. Bronze: {bronze_records} records (calculation skipped)")
            
    except Exception as e:
        error_msg = f"Pipeline failed: {str(e)}"
        logger.error(error_msg)
        log_error_to_markdown(error_msg, f"File: {args.file}, Snapshot: {snapshot_date}")
        sys.exit(1)


if __name__ == '__main__':
    main()