import pytest
import pandas as pd
from pathlib import Path
import tempfile
import os

from transformations.adp.extract import read_adp_file, validate_adp_file_structure


class TestExtract:
    """Test cases for the ADP extract module."""
    
    def test_read_adp_file_success(self):
        """Test successful reading of ADP file."""
        # Skip if no sample file exists
        sample_file = Path(__file__).parent / "sample_data" / "sample_adp_data.xls"
        if not sample_file.exists():
            pytest.skip("Sample ADP file not found")
        
        df = read_adp_file(sample_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert 'File Number' in df.columns
        assert 'Associate ID' in df.columns

    def test_read_adp_file_nonexistent(self):
        """Test handling of non-existent file."""
        with pytest.raises(FileNotFoundError):
            read_adp_file("nonexistent_file.xls")

    def test_read_adp_file_wrong_format(self):
        """Test handling of unsupported file format."""
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
            tmp.write(b"test content")
            tmp_path = tmp.name
        
        try:
            with pytest.raises(ValueError, match="Unsupported file format"):
                read_adp_file(tmp_path)
        finally:
            os.unlink(tmp_path)

    def test_validate_adp_file_structure_valid(self):
        """Test validation with valid ADP structure."""
        # Create a minimal valid DataFrame
        df = pd.DataFrame({
            'File Number': ['123456'],
            'Payroll Name': ['John Doe'],
            'Hire Date': ['2023-01-15'],
            'Position Status': ['Active'],
            'Home Department Code': ['123456'],
            'Clock Full Code': ['01200'],
            'Associate ID': ['EMP001']
        })
        
        result = validate_adp_file_structure(df)
        assert result is True

    def test_validate_adp_file_structure_missing_columns(self):
        """Test validation with missing required columns."""
        # Create DataFrame missing required columns
        df = pd.DataFrame({
            'File Number': ['123456'],
            'Payroll Name': ['John Doe']
            # Missing other required columns
        })
        
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_adp_file_structure(df)

    def test_data_types_preservation(self):
        """Test that specific data types are preserved during reading."""
        # Create a test Excel file with specific data types
        test_data = pd.DataFrame({
            'File Number': ['001234', '005678'],  # Should remain as string
            'Home Department Code': ['000123', '000456'],  # Should remain as string
            'Clock Full Code': ['01200', '01300'],  # Should remain as string
            'Associate ID': ['EMP001', 'EMP002']
        })
        
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            test_data.to_excel(tmp.name, index=False)
            tmp_path = tmp.name
        
        try:
            df = read_adp_file(tmp_path)
            
            # Verify data types are preserved as strings
            assert df['File Number'].dtype == 'object'
            assert df['Home Department Code'].dtype == 'object' 
            assert df['Clock Full Code'].dtype == 'object'
            
            # Verify leading zeros are preserved
            assert df['File Number'].iloc[0] == '001234'
            assert df['Home Department Code'].iloc[0] == '000123'
            
        finally:
            os.unlink(tmp_path)