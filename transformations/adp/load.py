import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
import logging
from typing import Dict, Any
from .config import get_database_url, get_adp_config


def get_database_engine(config_file: str = "config.yaml"):
    """Create and return a database engine."""
    database_url = get_database_url(config_file)
    engine = create_engine(database_url)
    return engine


def load_to_bronze_table(df: pd.DataFrame, config_file: str = "config.yaml") -> int:
    """
    Load cleaned ADP data to bronze.adp_tenure_history table.
    
    Args:
        df: Cleaned DataFrame to load
        
    Returns:
        Number of records inserted
        
    Raises:
        Exception: If database operation fails
    """
    config = get_adp_config(config_file)
    table_name = config.get('bronze_table', 'bronze.adp_tenure_history')
    
    engine = get_database_engine(config_file)
    
    try:
        # Insert data to database
        # Check if we're using DuckDB (doesn't support schemas the same way)
        if 'duckdb' in str(engine.url):
            # For DuckDB, use simple table name without schema
            table_name_simple = table_name.replace('bronze.', '').replace('silver.', '')
            records_inserted = df.to_sql(
                name=table_name_simple,
                con=engine,
                if_exists='append',
                index=False,
                method='multi'
            )
        else:
            # For PostgreSQL, use schema
            records_inserted = df.to_sql(
                name='adp_tenure_history',
                schema='bronze',
                con=engine,
                if_exists='append',
                index=False,
                method='multi'
            )
        
        logging.info(f"Successfully inserted {len(df)} records to {table_name}")
        return len(df)
        
    except Exception as e:
        logging.error(f"Failed to insert data to {table_name}: {str(e)}")
        raise
    finally:
        engine.dispose()


def execute_headcount_calculation(snapshot_date: str, report_date: str, config_file: str = "config.yaml") -> int:
    """
    Execute the headcount calculation SQL to populate silver.fact_active_headcount.
    
    Args:
        snapshot_date: Date string in YYYY-MM-DD format
        report_date: Date string in YYYY-MM-DD format
        
    Returns:
        Number of records inserted
        
    Raises:
        Exception: If database operation fails
    """
    config = get_adp_config(config_file)
    bronze_table = config.get('bronze_table', 'bronze.adp_tenure_history')
    silver_table = config.get('silver_table', 'silver.fact_active_headcount')
    excluded_codes = config.get('excluded_client_codes', ['01100'])
    
    # Convert excluded codes to SQL-compatible format
    excluded_codes_str = "', '".join(excluded_codes)
    
    # Handle DuckDB vs PostgreSQL table naming
    engine = get_database_engine(config_file)
    if 'duckdb' in str(engine.url):
        bronze_table_name = bronze_table.replace('bronze.', '').replace('silver.', '')
        silver_table_name = silver_table.replace('bronze.', '').replace('silver.', '')
    else:
        bronze_table_name = bronze_table
        silver_table_name = silver_table
    
    sql_query = f"""
    WITH daily_active AS (
        SELECT 
            COUNT(DISTINCT adp_id) as active_count,
            home_department_code, 
            home_department_description
        FROM {bronze_table_name} a
        WHERE a.position_status = 'Active'
            AND a.file_number IS NOT NULL
            AND a.client_code NOT IN ('{excluded_codes_str}')
            AND COALESCE(a.rehire_date, a.hire_date) < a.snapshot_date
            AND a.snapshot_date = :snapshot_date
        GROUP BY home_department_code, home_department_description
    )
    INSERT INTO {silver_table_name} (
        department_number, 
        snapshot_date, 
        report_date, 
        active_count, 
        created_at
    )
    SELECT
        a.home_department_code,
        :snapshot_date as snapshot_date,
        :report_date as report_date,
        a.active_count,
        NOW()
    FROM daily_active a
    WHERE a.home_department_code IS NOT NULL;
    """
    
    engine = get_database_engine(config_file)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(sql_query),
                {
                    'snapshot_date': snapshot_date,
                    'report_date': report_date
                }
            )
            conn.commit()
            
            records_inserted = result.rowcount
            logging.info(
                f"Successfully inserted {records_inserted} records to {silver_table} "
                f"for snapshot_date {snapshot_date}"
            )
            return records_inserted
            
    except Exception as e:
        logging.error(f"Failed to execute headcount calculation: {str(e)}")
        raise
    finally:
        engine.dispose()


def check_existing_data(snapshot_date: str, table_type: str = 'bronze', config_file: str = "config.yaml") -> int:
    """
    Check if data already exists for the given snapshot date.
    
    Args:
        snapshot_date: Date string in YYYY-MM-DD format
        table_type: 'bronze' or 'silver'
        
    Returns:
        Number of existing records for the snapshot date
    """
    config = get_adp_config(config_file)
    
    if table_type == 'bronze':
        table_name = config.get('bronze_table', 'bronze.adp_tenure_history')
    else:
        table_name = config.get('silver_table', 'silver.fact_active_headcount')
    
    # Handle DuckDB vs PostgreSQL table naming
    engine = get_database_engine(config_file)
    if 'duckdb' in str(engine.url):
        table_name_clean = table_name.replace('bronze.', '').replace('silver.', '')
    else:
        table_name_clean = table_name
    
    sql_query = f"""
    SELECT COUNT(*) as record_count
    FROM {table_name_clean}
    WHERE snapshot_date = :snapshot_date;
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(sql_query),
                {'snapshot_date': snapshot_date}
            )
            count = result.fetchone()[0]
            
            logging.info(f"Found {count} existing records in {table_name} for {snapshot_date}")
            return count
            
    except Exception as e:
        logging.error(f"Failed to check existing data in {table_name}: {str(e)}")
        raise
    finally:
        engine.dispose()


def delete_existing_data(snapshot_date: str, table_type: str = 'bronze', config_file: str = "config.yaml") -> int:
    """
    Delete existing data for the given snapshot date.
    
    Args:
        snapshot_date: Date string in YYYY-MM-DD format
        table_type: 'bronze' or 'silver'
        
    Returns:
        Number of records deleted
    """
    config = get_adp_config(config_file)
    
    if table_type == 'bronze':
        table_name = config.get('bronze_table', 'bronze.adp_tenure_history')
    else:
        table_name = config.get('silver_table', 'silver.fact_active_headcount')
    
    # Handle DuckDB vs PostgreSQL table naming
    engine = get_database_engine(config_file)
    if 'duckdb' in str(engine.url):
        table_name_clean = table_name.replace('bronze.', '').replace('silver.', '')
    else:
        table_name_clean = table_name
    
    sql_query = f"""
    DELETE FROM {table_name_clean}
    WHERE snapshot_date = :snapshot_date;
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(sql_query),
                {'snapshot_date': snapshot_date}
            )
            conn.commit()
            
            records_deleted = result.rowcount
            logging.info(f"Deleted {records_deleted} existing records from {table_name} for {snapshot_date}")
            return records_deleted
            
    except Exception as e:
        logging.error(f"Failed to delete existing data from {table_name}: {str(e)}")
        raise
    finally:
        engine.dispose()