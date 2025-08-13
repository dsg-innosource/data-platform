import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from sqlalchemy import create_engine, text

from transformations.adp.extract import read_adp_file
from transformations.adp.transform import clean_adp_data
from transformations.adp.load import (
    get_database_engine,
    load_to_bronze_table, 
    execute_headcount_calculation,
    check_existing_data
)


class TestIntegration:
    """Integration tests for the complete ADP pipeline."""
    
    @pytest.fixture
    def test_config_file(self):
        """Fixture providing path to test configuration."""
        return "config.test.yaml"
    
    @pytest.fixture
    def sample_excel_file(self):
        """Fixture providing a sample Excel file for testing."""
        # Create a temporary Excel file with sample data
        sample_data = pd.DataFrame({
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
            'Clock Full Code': ['01200', '01200', '01100'],  # 01100 should be excluded
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
        
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            sample_data.to_excel(tmp.name, index=False)
            yield tmp.name
        
        # Cleanup
        os.unlink(tmp.name)

    @pytest.fixture
    def setup_test_database(self, test_config_file):
        """Fixture to set up test database with tables."""
        engine = get_database_engine(test_config_file)
        
        # Create bronze table
        bronze_sql = """
        CREATE TABLE IF NOT EXISTS bronze_adp_tenure_history (
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
        CREATE TABLE IF NOT EXISTS silver_fact_active_headcount (
            snapshot_date DATE NOT NULL,
            active_count INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            department_number VARCHAR(6),
            report_date DATE
        )
        """
        
        with engine.connect() as conn:
            conn.execute(text(bronze_sql))
            conn.execute(text(silver_sql))
            conn.commit()
        
        yield engine
        
        # Cleanup
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS bronze_adp_tenure_history"))
            conn.execute(text("DROP TABLE IF EXISTS silver_fact_active_headcount"))
            conn.commit()
        
        engine.dispose()

    @pytest.fixture
    def test_config_with_simple_tables(self, tmp_path):
        """Create a test config that uses simple table names (no schemas)."""
        config_content = """
database:
  engine: "duckdb"
  database: "test_integration.db"
  
logging:
  level: "DEBUG"
  error_log_file: "transformations/logs/test_integration_errors.md"
  
adp:
  bronze_table: "bronze_adp_tenure_history"
  silver_table: "silver_fact_active_headcount"
  excluded_client_codes: ["01100"]
        """
        
        config_file = tmp_path / "test_integration_config.yaml"
        config_file.write_text(config_content.strip())
        
        return str(config_file)

    def test_full_pipeline_extract_transform_load(self, sample_excel_file, test_config_with_simple_tables, setup_test_database):
        """Test the complete ETL pipeline from Excel file to database."""
        config_file = test_config_with_simple_tables
        
        # Step 1: Extract
        raw_df = read_adp_file(sample_excel_file)
        assert len(raw_df) == 3
        assert 'File Number' in raw_df.columns
        
        # Step 2: Transform
        cleaned_df = clean_adp_data(raw_df, snapshot_date='2024-01-15')
        assert len(cleaned_df) == 3
        assert 'file_number' in cleaned_df.columns
        assert 'snapshot_date' in cleaned_df.columns
        # Convert to string for comparison since cleaned data may have date objects
        snapshot_dates = cleaned_df['snapshot_date'].astype(str)
        assert all(snapshot_dates == '2024-01-15')
        
        # Verify department code padding
        assert cleaned_df['home_department_code'].iloc[0] == '000123'
        
        # Step 3: Load to bronze
        bronze_records = load_to_bronze_table(cleaned_df, config_file)
        assert bronze_records == 3
        
        # Verify bronze data
        bronze_count = check_existing_data('2024-01-15', 'bronze', config_file)
        assert bronze_count == 3
        
        # Step 4: Execute headcount calculation
        silver_records = execute_headcount_calculation('2024-01-15', '2024-01-08', config_file)
        
        # Should have records for each unique department (excluding client code 01100)
        # We expect 2 records since one employee has excluded client code 01100
        assert silver_records == 2
        
        # Verify silver data
        silver_count = check_existing_data('2024-01-15', 'silver', config_file)
        assert silver_count == 2

    def test_pipeline_with_existing_data_protection(self, sample_excel_file, test_config_with_simple_tables, setup_test_database):
        """Test that pipeline handles existing data appropriately."""
        config_file = test_config_with_simple_tables
        
        # Run pipeline once
        raw_df = read_adp_file(sample_excel_file)
        cleaned_df = clean_adp_data(raw_df, snapshot_date='2024-01-15')
        load_to_bronze_table(cleaned_df, config_file)
        
        # Check that data exists
        initial_count = check_existing_data('2024-01-15', 'bronze', config_file)
        assert initial_count == 3
        
        # Running again should add more records (no built-in protection in load function)
        load_to_bronze_table(cleaned_df, config_file)
        new_count = check_existing_data('2024-01-15', 'bronze', config_file)
        assert new_count == 6  # Doubled

    def test_pipeline_monday_date_logic(self, sample_excel_file, test_config_with_simple_tables, setup_test_database):
        """Test pipeline with different date scenarios."""
        config_file = test_config_with_simple_tables
        
        # Test with Monday dates
        monday_snapshot = '2024-01-15'  # This was a Monday
        monday_report = '2024-01-08'    # Previous Monday
        
        raw_df = read_adp_file(sample_excel_file)
        cleaned_df = clean_adp_data(raw_df, snapshot_date=monday_snapshot)
        load_to_bronze_table(cleaned_df, config_file)
        
        silver_records = execute_headcount_calculation(monday_snapshot, monday_report, config_file)
        assert silver_records > 0
        
        # Verify the report_date was correctly set
        engine = setup_test_database
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT DISTINCT report_date FROM silver_fact_active_headcount WHERE snapshot_date = :snapshot_date"
            ), {"snapshot_date": monday_snapshot})
            
            report_dates = [row[0] for row in result.fetchall()]
            assert str(report_dates[0]) == monday_report

    def test_pipeline_error_handling(self, test_config_with_simple_tables):
        """Test pipeline error handling with invalid inputs."""
        config_file = test_config_with_simple_tables
        
        # Test with non-existent file
        with pytest.raises(FileNotFoundError):
            read_adp_file("nonexistent_file.xlsx")
        
        # Test with invalid data for database
        invalid_df = pd.DataFrame({'invalid_column': ['test']})
        
        with pytest.raises(Exception):
            load_to_bronze_table(invalid_df, config_file)

    def test_excluded_client_codes_logic(self, sample_excel_file, test_config_with_simple_tables, setup_test_database):
        """Test that excluded client codes are properly filtered in calculations."""
        config_file = test_config_with_simple_tables
        
        # Load data (includes one record with client_code 01100 which should be excluded)
        raw_df = read_adp_file(sample_excel_file)
        cleaned_df = clean_adp_data(raw_df, snapshot_date='2024-01-15')
        load_to_bronze_table(cleaned_df, config_file)
        
        # Execute calculation
        silver_records = execute_headcount_calculation('2024-01-15', '2024-01-08', config_file)
        
        # Verify that excluded client code records are not counted
        engine = setup_test_database
        with engine.connect() as conn:
            # Check total records in bronze (should be 3)
            bronze_result = conn.execute(text("SELECT COUNT(*) FROM bronze_adp_tenure_history"))
            bronze_count = bronze_result.fetchone()[0]
            assert bronze_count == 3
            
            # Check total active count in silver (should exclude client_code 01100)
            silver_result = conn.execute(text("SELECT SUM(active_count) FROM silver_fact_active_headcount"))
            total_active = silver_result.fetchone()[0]
            assert total_active == 2  # Should exclude the 01100 client_code record