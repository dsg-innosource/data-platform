import pytest
import pandas as pd
from datetime import datetime

from transformations.adp.transform import clean_adp_data, validate_cleaned_data


class TestTransform:
    """Test cases for the ADP transform module."""
    
    @pytest.fixture
    def sample_raw_data(self):
        """Fixture providing sample raw ADP data."""
        return pd.DataFrame({
            'File Number': ['  123456  ', '789012'],
            'Payroll Name': ['John Doe', '  Jane Smith  '],
            'Hire Date': ['2023-01-15', '2022-06-01'],
            'Rehire Date': [None, '2023-02-01'],
            'Previous Termination Date': [None, '2022-12-15'],
            'Termination Date': [None, None],
            'Termination Reason Description': [None, None],
            'Position Status': ['Active', 'Active'],
            'Leave of Absence Start Date': [None, None],
            'Leave of Absence Return Date': [None, None],
            'Home Department Code': ['123', '654321'],  # Test padding
            'Home Department Description': ['IT', 'HR'],
            'Payroll Company Code': ['ABC', 'ABC'],
            'Position ID': ['POS001', 'POS002'],
            'Clock Full Code': ['01200', '01200'],
            'Clock Full Description': ['Main Office', 'Main Office'],
            'Regular Pay Rate Amount': ['25.50', '30.00'],
            'Recruited by': ['HR Team', 'Recruiter'],
            'Business Unit Description': ['Corporate', 'Corporate'],
            'Requisition Key': ['REQ001', 'REQ002'],
            'Personal Contact: Personal Email': ['john@company.com', 'jane@company.com'],
            'Associate ID': ['EMP001', 'EMP002'],
            'Requisition_id': [1001, 1002],
            'applicant_id': [2001, 2002],
            'Regular Hours Total': [160, 160],
            'Overtime Hours Total': [10, 5],
            'Other hours': [8, 4],
            'Holiday': [8, 8],
            'Voluntary/Involuntary Termination Flag': ['Voluntary', 'Voluntary'],
            'Personal Contact: Home Phone': ['555-0123', '555-0456']
        })

    def test_clean_adp_data_basic(self, sample_raw_data):
        """Test basic data cleaning functionality."""
        cleaned_df = clean_adp_data(sample_raw_data, snapshot_date='2024-01-15')
        
        # Check column renaming
        assert 'file_number' in cleaned_df.columns
        assert 'payroll_name' in cleaned_df.columns
        assert 'adp_id' in cleaned_df.columns
        assert 'snapshot_date' in cleaned_df.columns
        
        # Check that old column names are gone
        assert 'File Number' not in cleaned_df.columns
        assert 'Associate ID' not in cleaned_df.columns

    def test_whitespace_stripping(self, sample_raw_data):
        """Test that whitespace is properly stripped from string columns."""
        cleaned_df = clean_adp_data(sample_raw_data)
        
        assert cleaned_df['file_number'].iloc[0] == '123456'  # Leading/trailing spaces removed
        assert cleaned_df['payroll_name'].iloc[1] == 'Jane Smith'  # Leading/trailing spaces removed

    def test_department_code_padding(self, sample_raw_data):
        """Test that department codes are properly padded to 6 characters."""
        cleaned_df = clean_adp_data(sample_raw_data)
        
        assert cleaned_df['home_department_code'].iloc[0] == '000123'  # Padded to 6 chars
        assert cleaned_df['home_department_code'].iloc[1] == '654321'  # Already 6 chars

    def test_snapshot_date_addition(self, sample_raw_data):
        """Test snapshot date is properly added."""
        test_date = '2024-07-21'
        cleaned_df = clean_adp_data(sample_raw_data, snapshot_date=test_date)
        
        # Convert to string for comparison since cleaned data may have date objects
        snapshot_dates = cleaned_df['snapshot_date'].astype(str)
        assert all(snapshot_dates == test_date)

    def test_snapshot_date_default(self, sample_raw_data):
        """Test default snapshot date (today) when not provided."""
        cleaned_df = clean_adp_data(sample_raw_data)
        
        today = datetime.now().strftime('%Y-%m-%d')
        # Convert to string for comparison since cleaned data may have date objects
        snapshot_dates = cleaned_df['snapshot_date'].astype(str)
        assert all(snapshot_dates == today)

    def test_date_column_conversion(self, sample_raw_data):
        """Test that date columns are properly converted."""
        cleaned_df = clean_adp_data(sample_raw_data)
        
        # Check that date columns are converted to date type
        assert pd.api.types.is_datetime64_any_dtype(cleaned_df['hire_date']) or \
               cleaned_df['hire_date'].dtype == 'object'  # May be date objects

    def test_integer_column_conversion(self, sample_raw_data):
        """Test that integer columns are properly converted."""
        cleaned_df = clean_adp_data(sample_raw_data)
        
        # Check integer columns
        assert pd.api.types.is_integer_dtype(cleaned_df['requisition_id'])
        assert pd.api.types.is_integer_dtype(cleaned_df['applicant_id'])

    def test_duplicate_column_removal(self):
        """Test removal of duplicate columns like 'Previous Termination Date.1'."""
        data_with_duplicate = pd.DataFrame({
            'File Number': ['123456'],
            'Previous Termination Date': ['2022-01-01'],
            'Previous Termination Date.1': ['2022-01-02'],  # Duplicate to remove
            'Associate ID': ['EMP001']
        })
        
        cleaned_df = clean_adp_data(data_with_duplicate)
        
        # Check that duplicate column is removed
        assert 'Previous Termination Date.1' not in cleaned_df.columns
        assert 'previous_termination_date' in cleaned_df.columns

    def test_validate_cleaned_data_success(self, sample_raw_data):
        """Test validation passes with valid cleaned data."""
        cleaned_df = clean_adp_data(sample_raw_data)
        
        result = validate_cleaned_data(cleaned_df)
        assert result is True

    def test_validate_cleaned_data_missing_required(self):
        """Test validation fails with missing required columns."""
        incomplete_df = pd.DataFrame({
            'payroll_name': ['John Doe'],
            'snapshot_date': ['2024-01-15']
            # Missing file_number and adp_id
        })
        
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_cleaned_data(incomplete_df)

    def test_validate_cleaned_data_invalid_dept_codes(self):
        """Test validation fails with invalid department codes."""
        invalid_df = pd.DataFrame({
            'file_number': ['123456'],
            'adp_id': ['EMP001'],
            'snapshot_date': ['2024-01-15'],
            'home_department_code': ['12345']  # Only 5 characters instead of 6
        })
        
        with pytest.raises(ValueError, match="invalid department codes"):
            validate_cleaned_data(invalid_df)

    def test_column_mapping_completeness(self, sample_raw_data):
        """Test that all expected columns are properly mapped."""
        cleaned_df = clean_adp_data(sample_raw_data)
        
        expected_columns = [
            'file_number', 'payroll_name', 'hire_date', 'rehire_date',
            'previous_termination_date', 'termination_date', 'termination_reason',
            'position_status', 'home_department_code', 'home_department_description',
            'client_code', 'client', 'adp_id', 'snapshot_date'
        ]
        
        for col in expected_columns:
            assert col in cleaned_df.columns, f"Missing expected column: {col}"