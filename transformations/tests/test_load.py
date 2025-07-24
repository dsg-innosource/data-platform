import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from sqlalchemy import create_engine, text

from transformations.adp.load import (
    get_database_engine, 
    load_to_bronze_table,
    execute_headcount_calculation,
    check_existing_data,
    delete_existing_data
)


class TestLoad:
    """Test cases for the ADP load module using DuckDB."""
    
    @pytest.fixture
    def test_config_file(self):
        """Fixture providing path to test configuration."""
        return "config.test.yaml"
    
    @pytest.fixture
    def sample_cleaned_data(self):
        """Fixture providing sample cleaned ADP data for testing."""
        return pd.DataFrame({
            'file_number': ['123456', '789012'],
            'payroll_name': ['John Doe', 'Jane Smith'],
            'hire_date': ['2023-01-15', '2022-06-01'],
            'rehire_date': [None, '2023-02-01'],
            'previous_termination_date': [None, '2022-12-15'],
            'termination_date': [None, None],
            'termination_reason': [None, None],
            'position_status': ['Active', 'Active'],
            'leave_of_absence_start_date': [None, None],
            'leave_of_absence_return_date': [None, None],
            'home_department_code': ['000123', '000456'],
            'home_department_description': ['IT', 'HR'],
            'payroll_company_code': ['ABC', 'ABC'],
            'position_id': ['POS001', 'POS002'],
            'client_code': ['01200', '01200'],
            'client': ['Main Office', 'Main Office'],
            'regular_pay_rate': ['25.50', '30.00'],
            'recruited_by': ['HR Team', 'Recruiter'],
            'business_unit': ['Corporate', 'Corporate'],
            'requisition_key': ['REQ001', 'REQ002'],
            'email': ['john@company.com', 'jane@company.com'],
            'adp_id': ['EMP001', 'EMP002'],
            'requisition_id': [1001, 1002],
            'applicant_id': [2001, 2002],
            'regular_hours': ['160', '160'],
            'ot_hours': ['10', '5'],
            'pto_sick_hours': ['8', '4'],
            'holiday_hours': ['8', '8'],
            'voluntary_involuntary_flag': ['Voluntary', 'Voluntary'],
            'home_phone': ['555-0123', '555-0456'],
            'snapshot_date': ['2024-01-15', '2024-01-15']
        })

    def test_get_database_engine_duckdb(self, test_config_file):
        """Test database engine creation with DuckDB configuration."""
        engine = get_database_engine(test_config_file)
        
        assert engine is not None
        assert 'duckdb' in str(engine.url)

    def test_get_database_engine_postgresql_config(self):
        """Test database engine creation with PostgreSQL configuration."""
        engine = get_database_engine("config.yaml")  # Default config
        
        assert engine is not None
        # Should work with either postgresql or duckdb depending on env vars

    @pytest.fixture
    def setup_test_tables(self, test_config_file):
        """Fixture to set up test database tables."""
        engine = get_database_engine(test_config_file)
        
        # Create bronze table
        bronze_sql = """
        CREATE TABLE IF NOT EXISTS bronze.adp_tenure_history (
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
        
        # Create silver table
        silver_sql = """
        CREATE TABLE IF NOT EXISTS silver.fact_active_headcount (
            snapshot_date DATE NOT NULL,
            active_count INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            department_number VARCHAR(6),
            report_date DATE
        )
        """
        
        try:
            with engine.connect() as conn:
                # Create schemas
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
                conn.commit()
                
                # Create tables
                conn.execute(text(bronze_sql))
                conn.execute(text(silver_sql))
                conn.commit()
                
        except Exception as e:
            # If schema creation fails, try without schemas (some test DBs don't support them)
            bronze_sql_simple = bronze_sql.replace("bronze.adp_tenure_history", "adp_tenure_history")
            silver_sql_simple = silver_sql.replace("silver.fact_active_headcount", "fact_active_headcount")
            
            with engine.connect() as conn:
                conn.execute(text(bronze_sql_simple))
                conn.execute(text(silver_sql_simple))
                conn.commit()
        
        yield engine
        
        # Cleanup
        try:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS bronze.adp_tenure_history"))
                conn.execute(text("DROP TABLE IF EXISTS silver.fact_active_headcount"))
                conn.execute(text("DROP TABLE IF EXISTS adp_tenure_history"))
                conn.execute(text("DROP TABLE IF EXISTS fact_active_headcount"))
                conn.commit()
        except:
            pass
        
        engine.dispose()

    def test_load_to_bronze_table(self, sample_cleaned_data, test_config_file, setup_test_tables):
        """Test loading data to bronze table."""
        result = load_to_bronze_table(sample_cleaned_data, test_config_file)
        
        assert result == len(sample_cleaned_data)
        
        # Verify data was inserted
        engine = setup_test_tables
        with engine.connect() as conn:
            try:
                result = conn.execute(text("SELECT COUNT(*) FROM bronze.adp_tenure_history"))
            except:
                result = conn.execute(text("SELECT COUNT(*) FROM adp_tenure_history"))
            count = result.fetchone()[0]
            assert count == len(sample_cleaned_data)

    def test_check_existing_data(self, sample_cleaned_data, test_config_file, setup_test_tables):
        """Test checking for existing data."""
        # Load some data first
        load_to_bronze_table(sample_cleaned_data, test_config_file)
        
        # Check existing data
        count = check_existing_data('2024-01-15', 'bronze', test_config_file)
        assert count == len(sample_cleaned_data)
        
        # Check non-existent date
        count = check_existing_data('2024-12-31', 'bronze', test_config_file)
        assert count == 0

    def test_delete_existing_data(self, sample_cleaned_data, test_config_file, setup_test_tables):
        """Test deleting existing data."""
        # Load some data first
        load_to_bronze_table(sample_cleaned_data, test_config_file)
        
        # Verify data exists
        count = check_existing_data('2024-01-15', 'bronze', test_config_file)
        assert count == len(sample_cleaned_data)
        
        # Delete data
        deleted_count = delete_existing_data('2024-01-15', 'bronze', test_config_file)
        assert deleted_count == len(sample_cleaned_data)
        
        # Verify data is gone
        count = check_existing_data('2024-01-15', 'bronze', test_config_file)
        assert count == 0

    def test_execute_headcount_calculation(self, sample_cleaned_data, test_config_file, setup_test_tables):
        """Test headcount calculation execution."""
        # Load bronze data first
        load_to_bronze_table(sample_cleaned_data, test_config_file)
        
        # Execute calculation
        result = execute_headcount_calculation('2024-01-15', '2024-01-08', test_config_file)
        
        # Should insert records for each unique department
        assert result >= 1
        
        # Verify silver data was created
        engine = setup_test_tables
        with engine.connect() as conn:
            try:
                result = conn.execute(text("SELECT COUNT(*) FROM silver.fact_active_headcount"))
            except:
                result = conn.execute(text("SELECT COUNT(*) FROM fact_active_headcount"))
            count = result.fetchone()[0]
            assert count >= 1

    def test_database_error_handling(self, test_config_file):
        """Test error handling for database operations."""
        # Test with invalid data that should cause an error
        invalid_data = pd.DataFrame({
            'invalid_column': ['test']
        })
        
        with pytest.raises(Exception):
            load_to_bronze_table(invalid_data, test_config_file)

    def test_config_file_not_found(self):
        """Test handling of missing configuration file."""
        with pytest.raises(FileNotFoundError):
            get_database_engine("nonexistent_config.yaml")