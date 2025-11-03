#!/usr/bin/env python3
"""
ADP Tenure Pipeline

Main orchestration script for processing ADP tenure data.
Handles the complete ETL process from file input to database loading.
After successful processing, automatically archives the source file with timestamp.

Usage:
    # Test mode: Process most recent file with DuckDB (safe testing)
    python -m transformations.pipelines.adp_tenure_pipeline --test-mode

    # Production: Process most recent file to PostgreSQL
    python -m transformations.pipelines.adp_tenure_pipeline

    # Process specific file with default dates
    python -m transformations.pipelines.adp_tenure_pipeline --file "Foreflow Tenure_JP.xls"

    # Process with custom dates
    python -m transformations.pipelines.adp_tenure_pipeline --file "data.xls" --snapshot-date "2025-07-21" --report-date "2025-07-07"

    # Force reprocess (overwrite existing data)
    python -m transformations.pipelines.adp_tenure_pipeline --force

    # Test with real file but DuckDB database
    python -m transformations.pipelines.adp_tenure_pipeline --test-mode --file "raw/headcount/real_data.xls"

Note: Processed files are automatically moved to raw/headcount/archive/ with format:
      filename_snapshot_YYYYMMDD_processed_YYYYMMDDHHMMSS.ext
"""

import sys
import argparse
import logging
import shutil
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


def find_most_recent_headcount_file() -> Path:
    """
    Find the most recent file in the raw/headcount/ directory.

    Returns:
        Path to the most recent .xls or .xlsx file

    Raises:
        FileNotFoundError: If no files found in directory
    """
    # Get the project root (two levels up from this file)
    project_root = Path(__file__).parent.parent.parent
    headcount_dir = project_root / 'raw' / 'headcount'

    if not headcount_dir.exists():
        raise FileNotFoundError(f"Headcount directory not found: {headcount_dir}")

    # Find all .xls and .xlsx files
    excel_files = list(headcount_dir.glob('*.xls')) + list(headcount_dir.glob('*.xlsx'))

    if not excel_files:
        raise FileNotFoundError(f"No Excel files found in {headcount_dir}")

    # Get the most recently modified file
    most_recent = max(excel_files, key=lambda p: p.stat().st_mtime)

    return most_recent


def archive_processed_file(file_path: Path, snapshot_date: str) -> Path:
    """
    Archive the processed file to the archive directory with timestamp.

    Args:
        file_path: Path to the file that was processed
        snapshot_date: The snapshot date used for processing (YYYY-MM-DD)

    Returns:
        Path to the archived file

    Naming format: filename_snapshot_YYYYMMDD_processed_YYYYMMDDHHMMSS.ext
    Example: Foreflow Tenure_snapshot_20251103_processed_20251103092815.xls
    """
    # Get project root (two levels up from this file)
    project_root = Path(__file__).parent.parent.parent
    archive_dir = project_root / 'raw' / 'headcount' / 'archive'

    # Ensure archive directory exists
    archive_dir.mkdir(parents=True, exist_ok=True)

    # Parse snapshot date and format as YYYYMMDD
    snapshot_dt = datetime.strptime(snapshot_date, '%Y-%m-%d')
    snapshot_str = snapshot_dt.strftime('%Y%m%d')

    # Get current timestamp for processing time
    process_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

    # Build new filename
    file_stem = file_path.stem  # filename without extension
    file_ext = file_path.suffix  # .xls or .xlsx
    new_filename = f"{file_stem}_snapshot_{snapshot_str}_processed_{process_timestamp}{file_ext}"

    # Full path to archived file
    archive_path = archive_dir / new_filename

    # Move the file (overwrite if exists)
    shutil.move(str(file_path), str(archive_path))

    logging.info(f"Archived processed file to: {archive_path}")

    return archive_path


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
    parser.add_argument('--file', help='Path to ADP Excel file. If not provided, uses most recent file in raw/headcount/')
    parser.add_argument('--snapshot-date', help='Snapshot date (YYYY-MM-DD). Defaults to current Monday')
    parser.add_argument('--report-date', help='Report date (YYYY-MM-DD). Defaults to previous Monday')
    parser.add_argument('--force', action='store_true', help='Force reprocess (overwrite existing data)')
    parser.add_argument('--skip-calculation', action='store_true', help='Skip headcount calculation (bronze only)')
    parser.add_argument('--test-mode', action='store_true', help='Use test configuration (DuckDB) instead of production (PostgreSQL)')

    args = parser.parse_args()

    # Determine which config file to use
    config_file = "config.test.yaml" if args.test_mode else "config.yaml"

    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)

    if args.test_mode:
        logger.info("🧪 Running in TEST MODE - data will be loaded to DuckDB (config.test.yaml)")
    else:
        logger.info("🚀 Running in PRODUCTION MODE - data will be loaded to PostgreSQL (config.yaml)")

    try:
        # Determine which file to process
        if args.file:
            file_path = args.file
            logger.info(f"Using specified file: {file_path}")
        else:
            file_path = find_most_recent_headcount_file()
            logger.info(f"Auto-detected most recent file: {file_path}")

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
        logger.info(f"Reading ADP file: {file_path}")
        raw_df = read_adp_file(file_path)
        validate_adp_file_structure(raw_df)
        
        # Step 2: Transform
        logger.info("Cleaning and transforming data")
        cleaned_df = clean_adp_data(raw_df, snapshot_date)
        validate_cleaned_data(cleaned_df)
        
        # Step 3: Check for existing data
        existing_bronze = check_existing_data(snapshot_date, 'bronze', config_file)
        existing_silver = check_existing_data(snapshot_date, 'silver', config_file)

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
                delete_existing_data(snapshot_date, 'silver', config_file)
            if existing_bronze > 0:
                delete_existing_data(snapshot_date, 'bronze', config_file)

        # Step 5: Load to bronze
        logger.info("Loading data to bronze table")
        bronze_records = load_to_bronze_table(cleaned_df, config_file)

        # Step 6: Execute headcount calculation (unless skipped)
        if not args.skip_calculation:
            logger.info("Executing headcount calculation")
            silver_records = execute_headcount_calculation(snapshot_date, report_date, config_file)
            logger.info(f"Pipeline completed successfully. Bronze: {bronze_records}, Silver: {silver_records} records")
        else:
            logger.info(f"Pipeline completed successfully. Bronze: {bronze_records} records (calculation skipped)")

        # Step 7: Archive the processed file
        logger.info("Archiving processed file")
        archived_path = archive_processed_file(Path(file_path), snapshot_date)
        logger.info(f"File archived successfully: {archived_path.name}")

    except Exception as e:
        error_msg = f"Pipeline failed: {str(e)}"
        logger.error(error_msg)
        # Try to include file_path in error context if it was determined
        context = f"File: {file_path if 'file_path' in locals() else 'unknown'}"
        if 'snapshot_date' in locals():
            context += f", Snapshot: {snapshot_date}"
        log_error_to_markdown(error_msg, context)
        sys.exit(1)


if __name__ == '__main__':
    main()