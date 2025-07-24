"""
Shared test utilities for ADP pipeline testing.
"""
from sqlalchemy import create_engine, text
from pathlib import Path
import tempfile
import os


def create_test_tables(engine, use_schemas=False):
    """
    Create test tables for ADP pipeline testing.
    
    Args:
        engine: SQLAlchemy engine
        use_schemas: Whether to use schema names (for PostgreSQL) or simple names (for DuckDB)
    """
    
    # Determine table names based on database type
    if use_schemas:
        bronze_table = "bronze.adp_tenure_history"
        silver_table = "silver.fact_active_headcount"
        schema_setup = [
            "CREATE SCHEMA IF NOT EXISTS bronze",
            "CREATE SCHEMA IF NOT EXISTS silver"
        ]
    else:
        bronze_table = "adp_tenure_history"
        silver_table = "fact_active_headcount"
        schema_setup = []
    
    # DDL for bronze table
    bronze_ddl = f"""
    CREATE TABLE IF NOT EXISTS {bronze_table} (
        file_number TEXT,
        payroll_name TEXT,
        hire_date DATE,
        rehire_date DATE,
        previous_termination_date DATE,
        termination_date DATE,
        termination_reason TEXT,
        position_status TEXT,
        leave_of_absence_start_date TEXT,
        leave_of_absence_return_date TEXT,
        home_department_code VARCHAR(6),
        home_department_description TEXT,
        payroll_company_code TEXT,
        position_id TEXT,
        client_code TEXT,
        client TEXT,
        regular_pay_rate TEXT,
        recruited_by TEXT,
        business_unit TEXT,
        requisition_key TEXT,
        email TEXT,
        adp_id TEXT,
        requisition_id INTEGER,
        applicant_id INTEGER,
        regular_hours TEXT,
        ot_hours TEXT,
        pto_sick_hours TEXT,
        holiday_hours TEXT,
        voluntary_involuntary_flag TEXT,
        home_phone TEXT,
        snapshot_date DATE
    )
    """
    
    # DDL for silver table
    silver_ddl = f"""
    CREATE TABLE IF NOT EXISTS {silver_table} (
        snapshot_date DATE NOT NULL,
        active_count INTEGER NOT NULL,
        created_at TIMESTAMP NOT NULL,
        department_number VARCHAR(6),
        report_date DATE
    )
    """
    
    with engine.connect() as conn:
        # Create schemas if needed
        for schema_sql in schema_setup:
            try:
                conn.execute(text(schema_sql))
            except Exception:
                pass  # Ignore if schemas not supported
        
        # Create tables
        conn.execute(text(bronze_ddl))
        conn.execute(text(silver_ddl))
        conn.commit()


def cleanup_test_tables(engine, use_schemas=False):
    """
    Clean up test tables after testing.
    
    Args:
        engine: SQLAlchemy engine
        use_schemas: Whether to use schema names or simple names
    """
    if use_schemas:
        tables = ["bronze.adp_tenure_history", "silver.fact_active_headcount"]
    else:
        tables = ["adp_tenure_history", "fact_active_headcount"]
    
    with engine.connect() as conn:
        for table in tables:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
            except Exception:
                pass  # Ignore if table doesn't exist
        conn.commit()


def get_test_database_engine(config_file="config.test.yaml"):
    """
    Get a database engine for testing with automatic table setup.
    
    Args:
        config_file: Configuration file to use
        
    Returns:
        SQLAlchemy engine with tables created
    """
    from transformations.adp.load import get_database_engine
    
    engine = get_database_engine(config_file)
    
    # Determine if we need schemas
    use_schemas = 'postgresql' in str(engine.url)
    
    # Create tables
    create_test_tables(engine, use_schemas)
    
    return engine


def create_sample_test_data():
    """
    Create sample test data for unit tests.
    
    Returns:
        DataFrame with sample ADP data
    """
    import pandas as pd
    
    return pd.DataFrame({
        'file_number': ['123456', '789012', '345678'],
        'payroll_name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
        'hire_date': ['2023-01-15', '2022-06-01', '2023-03-10'],
        'rehire_date': [None, '2023-02-01', None],
        'previous_termination_date': [None, '2022-12-15', None],
        'termination_date': [None, None, None],
        'termination_reason': [None, None, None],
        'position_status': ['Active', 'Active', 'Active'],
        'leave_of_absence_start_date': [None, None, None],
        'leave_of_absence_return_date': [None, None, None],
        'home_department_code': ['000123', '000456', '000789'],
        'home_department_description': ['IT', 'HR', 'Engineering'],
        'payroll_company_code': ['ABC', 'ABC', 'ABC'],
        'position_id': ['POS001', 'POS002', 'POS003'],
        'client_code': ['01200', '01200', '01100'],  # Note: 01100 should be excluded
        'client': ['Main Office', 'Main Office', 'Excluded Office'],
        'regular_pay_rate': ['25.50', '30.00', '28.75'],
        'recruited_by': ['HR Team', 'Recruiter', 'Manager'],
        'business_unit': ['Corporate', 'Corporate', 'Engineering'],
        'requisition_key': ['REQ001', 'REQ002', 'REQ003'],
        'email': ['john@company.com', 'jane@company.com', 'bob@company.com'],
        'adp_id': ['EMP001', 'EMP002', 'EMP003'],
        'requisition_id': [1001, 1002, 1003],
        'applicant_id': [2001, 2002, 2003],
        'regular_hours': ['160', '160', '160'],
        'ot_hours': ['10', '5', '15'],
        'pto_sick_hours': ['8', '4', '0'],
        'holiday_hours': ['8', '8', '8'],
        'voluntary_involuntary_flag': ['Voluntary', 'Voluntary', 'Voluntary'],
        'home_phone': ['555-0123', '555-0456', '555-0789'],
        'snapshot_date': ['2024-01-15', '2024-01-15', '2024-01-15']
    })


def create_temporary_excel_file(data=None):
    """
    Create a temporary Excel file for testing.
    
    Args:
        data: DataFrame to write to Excel. If None, uses sample data.
        
    Returns:
        Path to temporary Excel file
    """
    import pandas as pd
    
    if data is None:
        # Create sample data with proper ADP column names
        data = pd.DataFrame({
            'File Number': ['123456', '789012', '345678'],
            'Payroll Name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
            'Hire Date': ['2023-01-15', '2022-06-01', '2023-03-10'],
            'Rehire Date': ['', '2023-02-01', ''],
            'Previous Termination Date': ['', '2022-12-15', ''],
            'Termination Date': ['', '', ''],
            'Termination Reason Description': ['', '', ''],
            'Position Status': ['Active', 'Active', 'Active'],
            'Leave of Absence Start Date': ['', '', ''],
            'Leave of Absence Return Date': ['', '', ''],
            'Home Department Code': ['123', '456', '789'],
            'Home Department Description': ['IT', 'HR', 'Engineering'],
            'Payroll Company Code': ['ABC', 'ABC', 'ABC'],
            'Position ID': ['POS001', 'POS002', 'POS003'],
            'Clock Full Code': ['01200', '01200', '01100'],
            'Clock Full Description': ['Main Office', 'Main Office', 'Excluded Office'],
            'Regular Pay Rate Amount': ['25.50', '30.00', '28.75'],
            'Recruited by': ['HR Team', 'Recruiter', 'Manager'],
            'Business Unit Description': ['Corporate', 'Corporate', 'Engineering'],
            'Requisition Key': ['REQ001', 'REQ002', 'REQ003'],
            'Personal Contact: Personal Email': ['john@company.com', 'jane@company.com', 'bob@company.com'],
            'Associate ID': ['EMP001', 'EMP002', 'EMP003'],
            'Requisition_id': [1001, 1002, 1003],
            'applicant_id': [2001, 2002, 2003],
            'Regular Hours Total': [160, 160, 160],
            'Overtime Hours Total': [10, 5, 15],
            'Other hours': [8, 4, 0],
            'Holiday': [8, 8, 8],
            'Voluntary/Involuntary Termination Flag': ['Voluntary', 'Voluntary', 'Voluntary'],
            'Personal Contact: Home Phone': ['555-0123', '555-0456', '555-0789']
        })
    
    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    data.to_excel(temp_file.name, index=False)
    temp_file.close()
    
    return temp_file.name