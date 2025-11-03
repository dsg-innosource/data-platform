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
from .test_utils import get_test_database_engine, create_sample_test_data, cleanup_test_tables


class TestLoad:
    """Test cases for the ADP load module using DuckDB."""
    
    @pytest.fixture
    def test_config_file(self):
        """Fixture providing path to test configuration."""
        return "config.test.yaml"
    
    @pytest.fixture
    def sample_cleaned_data(self):
        """Fixture providing sample cleaned ADP data for testing."""
        return create_sample_test_data()[:2]  # Use first 2 records

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
        engine = get_test_database_engine(test_config_file)
        
        yield engine
        
        # Cleanup
        cleanup_test_tables(engine, use_schemas='postgresql' in str(engine.url))
        engine.dispose()

    def test_load_to_bronze_table(self, sample_cleaned_data, test_config_file, setup_test_tables):
        """Test loading data to bronze table."""
        # Clean any existing data first
        engine = setup_test_tables
        with engine.connect() as conn:
            try:
                conn.execute(text("DELETE FROM bronze.adp_tenure_history"))
            except:
                conn.execute(text("DELETE FROM adp_tenure_history"))
            conn.commit()
        
        result = load_to_bronze_table(sample_cleaned_data, test_config_file)
        
        assert result == len(sample_cleaned_data)
        
        # Verify data was inserted
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