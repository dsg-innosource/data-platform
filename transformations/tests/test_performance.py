import pytest
import pandas as pd
import time
from pathlib import Path

from transformations.adp.extract import read_adp_file, validate_adp_file_structure
from transformations.adp.transform import clean_adp_data, validate_cleaned_data
from transformations.adp.load import load_to_bronze_table, execute_headcount_calculation
from .test_utils import get_test_database_engine, cleanup_test_tables


class TestPerformance:
    """Performance tests using the full ADP dataset."""
    
    @pytest.fixture
    def test_config_file(self):
        """Fixture providing path to test configuration."""
        return "config.test.yaml"
    
    @pytest.fixture
    def real_adp_file(self):
        """Fixture providing path to real ADP file."""
        sample_file = Path(__file__).parent / "sample_data" / "sample_adp_data.xls"
        if not sample_file.exists():
            pytest.skip("Real ADP sample file not found")
        return sample_file
    
    @pytest.fixture
    def setup_performance_db(self, test_config_file):
        """Fixture to set up database for performance testing."""
        engine = get_test_database_engine(test_config_file)
        
        # Clean any existing data
        with engine.connect() as conn:
            try:
                conn.execute("DELETE FROM adp_tenure_history")
                conn.execute("DELETE FROM fact_active_headcount")
            except:
                pass
            conn.commit()
        
        yield engine
        
        # Cleanup
        cleanup_test_tables(engine, use_schemas='postgresql' in str(engine.url))
        engine.dispose()

    def test_full_dataset_extraction_performance(self, real_adp_file):
        """Test extraction performance with full dataset."""
        start_time = time.time()
        
        df = read_adp_file(real_adp_file)
        
        extraction_time = time.time() - start_time
        
        print(f"\n📊 Extraction Performance:")
        print(f"   Records: {len(df):,}")
        print(f"   Columns: {len(df.columns)}")
        print(f"   Time: {extraction_time:.2f} seconds")
        print(f"   Rate: {len(df)/extraction_time:,.0f} records/second")
        
        # Performance assertions
        assert len(df) > 10000, "Should have substantial dataset for performance testing"
        assert extraction_time < 30, f"Extraction took too long: {extraction_time:.2f}s"
        
        # Validate structure
        validate_adp_file_structure(df)

    def test_full_dataset_transformation_performance(self, real_adp_file):
        """Test transformation performance with full dataset."""
        # Extract data first
        df = read_adp_file(real_adp_file)
        
        start_time = time.time()
        
        cleaned_df = clean_adp_data(df, snapshot_date='2024-01-15')
        
        transformation_time = time.time() - start_time
        
        print(f"\n🔄 Transformation Performance:")
        print(f"   Input records: {len(df):,}")
        print(f"   Output records: {len(cleaned_df):,}")
        print(f"   Input columns: {len(df.columns)}")
        print(f"   Output columns: {len(cleaned_df.columns)}")
        print(f"   Time: {transformation_time:.2f} seconds")
        print(f"   Rate: {len(df)/transformation_time:,.0f} records/second")
        
        # Performance assertions
        assert len(cleaned_df) == len(df), "Should not lose records during transformation"
        assert transformation_time < 60, f"Transformation took too long: {transformation_time:.2f}s"
        
        # Validate cleaned data
        validate_cleaned_data(cleaned_df)

    def test_full_dataset_loading_performance(self, real_adp_file, test_config_file, setup_performance_db):
        """Test loading performance with full dataset."""
        # Extract and transform data first
        df = read_adp_file(real_adp_file)
        cleaned_df = clean_adp_data(df, snapshot_date='2024-01-15')
        
        start_time = time.time()
        
        result = load_to_bronze_table(cleaned_df, test_config_file)
        
        loading_time = time.time() - start_time
        
        print(f"\n💾 Loading Performance:")
        print(f"   Records loaded: {result:,}")
        print(f"   Time: {loading_time:.2f} seconds")
        print(f"   Rate: {result/loading_time:,.0f} records/second")
        
        # Performance assertions
        assert result == len(cleaned_df), "All records should be loaded"
        assert loading_time < 120, f"Loading took too long: {loading_time:.2f}s"
        
        # Verify data in database
        engine = setup_performance_db
        with engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) FROM adp_tenure_history")
            count = result.fetchone()[0]
            assert count == len(cleaned_df)

    def test_full_dataset_calculation_performance(self, real_adp_file, test_config_file, setup_performance_db):
        """Test headcount calculation performance with full dataset."""
        # Load data first
        df = read_adp_file(real_adp_file)
        cleaned_df = clean_adp_data(df, snapshot_date='2024-01-15')
        load_to_bronze_table(cleaned_df, test_config_file)
        
        start_time = time.time()
        
        result = execute_headcount_calculation('2024-01-15', '2024-01-08', test_config_file)
        
        calculation_time = time.time() - start_time
        
        print(f"\n🧮 Calculation Performance:")
        print(f"   Input records: {len(cleaned_df):,}")
        print(f"   Department groups: {result}")
        print(f"   Time: {calculation_time:.2f} seconds")
        
        # Performance assertions
        assert result > 0, "Should create headcount records"
        assert calculation_time < 30, f"Calculation took too long: {calculation_time:.2f}s"
        
        # Verify calculation results
        engine = setup_performance_db
        with engine.connect() as conn:
            result = conn.execute("SELECT SUM(active_count) FROM fact_active_headcount")
            total_count = result.fetchone()[0]
            print(f"   Total active employees: {total_count:,}")
            assert total_count > 0

    def test_complete_pipeline_performance(self, real_adp_file, test_config_file, setup_performance_db):
        """Test complete end-to-end pipeline performance."""
        print(f"\n🚀 Complete Pipeline Performance Test")
        
        pipeline_start = time.time()
        
        # Step 1: Extract
        extract_start = time.time()
        df = read_adp_file(real_adp_file)
        validate_adp_file_structure(df)
        extract_time = time.time() - extract_start
        
        # Step 2: Transform
        transform_start = time.time()
        cleaned_df = clean_adp_data(df, snapshot_date='2024-01-15')
        validate_cleaned_data(cleaned_df)
        transform_time = time.time() - transform_start
        
        # Step 3: Load to Bronze
        load_start = time.time()
        bronze_result = load_to_bronze_table(cleaned_df, test_config_file)
        load_time = time.time() - load_start
        
        # Step 4: Calculate Silver
        calc_start = time.time()
        silver_result = execute_headcount_calculation('2024-01-15', '2024-01-08', test_config_file)
        calc_time = time.time() - calc_start
        
        total_time = time.time() - pipeline_start
        
        print(f"\n📈 Pipeline Performance Summary:")
        print(f"   Total records processed: {len(df):,}")
        print(f"   Extract time: {extract_time:.2f}s ({len(df)/extract_time:,.0f} rec/s)")
        print(f"   Transform time: {transform_time:.2f}s ({len(df)/transform_time:,.0f} rec/s)")
        print(f"   Load time: {load_time:.2f}s ({bronze_result/load_time:,.0f} rec/s)")
        print(f"   Calculation time: {calc_time:.2f}s")
        print(f"   Total pipeline time: {total_time:.2f}s")
        print(f"   Overall rate: {len(df)/total_time:,.0f} records/second")
        
        # Performance benchmarks
        assert total_time < 300, f"Complete pipeline took too long: {total_time:.2f}s"  # 5 minutes max
        assert bronze_result == len(cleaned_df), "All records should be in bronze"
        assert silver_result > 0, "Should generate silver records"

    def test_memory_usage_with_large_dataset(self, real_adp_file):
        """Test memory usage with large dataset."""
        import psutil
        import gc
        
        process = psutil.Process()
        
        # Measure initial memory
        gc.collect()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Load and process data
        df = read_adp_file(real_adp_file)
        peak_memory_after_load = process.memory_info().rss / 1024 / 1024
        
        cleaned_df = clean_adp_data(df, snapshot_date='2024-01-15')
        peak_memory_after_clean = process.memory_info().rss / 1024 / 1024
        
        # Clean up
        del df, cleaned_df
        gc.collect()
        final_memory = process.memory_info().rss / 1024 / 1024
        
        print(f"\n💽 Memory Usage Analysis:")
        print(f"   Initial memory: {initial_memory:.1f} MB")
        print(f"   After data load: {peak_memory_after_load:.1f} MB (+{peak_memory_after_load-initial_memory:.1f} MB)")
        print(f"   After cleaning: {peak_memory_after_clean:.1f} MB (+{peak_memory_after_clean-initial_memory:.1f} MB)")
        print(f"   Final memory: {final_memory:.1f} MB")
        print(f"   Memory released: {peak_memory_after_clean-final_memory:.1f} MB")
        
        # Memory usage assertions
        memory_increase = peak_memory_after_clean - initial_memory
        assert memory_increase < 1000, f"Memory usage too high: {memory_increase:.1f} MB"  # Less than 1GB
        
        memory_leaked = final_memory - initial_memory
        assert memory_leaked < 100, f"Potential memory leak: {memory_leaked:.1f} MB"  # Less than 100MB leak

    @pytest.mark.slow
    def test_stress_test_multiple_runs(self, real_adp_file, test_config_file):
        """Stress test with multiple pipeline runs."""
        print(f"\n🔥 Stress Test - Multiple Pipeline Runs")
        
        run_times = []
        
        for run in range(3):  # Run 3 times
            print(f"   Run {run + 1}/3...")
            
            # Create fresh engine for each run
            engine = get_test_database_engine(test_config_file)
            
            run_start = time.time()
            
            # Full pipeline
            df = read_adp_file(real_adp_file)
            cleaned_df = clean_adp_data(df, snapshot_date=f'2024-01-{15+run}')  # Different dates
            load_to_bronze_table(cleaned_df, test_config_file)
            execute_headcount_calculation(f'2024-01-{15+run}', f'2024-01-{8+run}', test_config_file)
            
            run_time = time.time() - run_start
            run_times.append(run_time)
            
            print(f"      Time: {run_time:.2f}s")
            
            # Cleanup
            cleanup_test_tables(engine, use_schemas='postgresql' in str(engine.url))
            engine.dispose()
        
        avg_time = sum(run_times) / len(run_times)
        max_time = max(run_times)
        min_time = min(run_times)
        
        print(f"\n   Stress Test Results:")
        print(f"      Average time: {avg_time:.2f}s")
        print(f"      Min time: {min_time:.2f}s")
        print(f"      Max time: {max_time:.2f}s")
        print(f"      Time variation: {(max_time-min_time)/avg_time*100:.1f}%")
        
        # Consistency assertions
        assert max_time < avg_time * 1.5, "Performance should be consistent across runs"
        assert avg_time < 300, f"Average pipeline time too slow: {avg_time:.2f}s"