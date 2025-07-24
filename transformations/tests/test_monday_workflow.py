import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from transformations.adp.extract import read_adp_file, validate_adp_file_structure
from transformations.adp.transform import clean_adp_data, validate_cleaned_data
from transformations.adp.load import (
    load_to_bronze_table, 
    execute_headcount_calculation,
    check_existing_data,
    delete_existing_data
)
from transformations.adp.date_utils import (
    get_monday_dates, 
    is_monday, 
    validate_monday_dates,
    format_business_period
)
from .test_utils import get_test_database_engine, cleanup_test_tables, create_temporary_excel_file


class TestMondayWorkflow:
    """Test the complete Monday morning workflow that users will actually run."""
    
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
    def setup_monday_db(self, test_config_file):
        """Fixture to set up database for Monday workflow testing."""
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

    def test_monday_date_calculation_logic(self):
        """Test the Monday date calculation business logic."""
        # Test with a known Wednesday
        test_wednesday = datetime(2024, 1, 17)  # January 17, 2024 was a Wednesday
        snapshot_date, report_date = get_monday_dates(test_wednesday)
        
        assert snapshot_date == '2024-01-15', f"Expected 2024-01-15, got {snapshot_date}"
        assert report_date == '2024-01-08', f"Expected 2024-01-08, got {report_date}"
        
        # Test with a Monday
        test_monday = datetime(2024, 1, 15)  # January 15, 2024 was a Monday
        snapshot_date, report_date = get_monday_dates(test_monday)
        
        assert snapshot_date == '2024-01-15'
        assert report_date == '2024-01-08'
        
        # Test with a Sunday  
        test_sunday = datetime(2024, 1, 21)  # January 21, 2024 was a Sunday
        snapshot_date, report_date = get_monday_dates(test_sunday)
        
        assert snapshot_date == '2024-01-15'  # Still the same week's Monday
        assert report_date == '2024-01-08'

    def test_monday_date_validation(self):
        """Test validation of Monday dates."""
        # Valid Monday dates
        assert validate_monday_dates('2024-01-15', '2024-01-08') is True
        
        # Invalid: not Mondays
        with pytest.raises(ValueError, match="not a Monday"):
            validate_monday_dates('2024-01-16', '2024-01-08')  # Tuesday
        
        with pytest.raises(ValueError, match="not a Monday"):
            validate_monday_dates('2024-01-15', '2024-01-09')  # Tuesday
        
        # Invalid: wrong order
        with pytest.raises(ValueError, match="should be before"):
            validate_monday_dates('2024-01-08', '2024-01-15')  # Reversed
        
        # Invalid: not exactly 7 days apart
        with pytest.raises(ValueError, match="exactly 7 days apart"):
            validate_monday_dates('2024-01-15', '2024-01-01')  # 14 days apart

    def test_business_period_formatting(self):
        """Test business period formatting for reports."""
        period = format_business_period('2024-01-15', '2024-01-08')
        expected = "Week of 2024-01-08 through 2024-01-14 (reported 2024-01-15)"
        assert period == expected

    def test_complete_monday_workflow_with_real_data(self, real_adp_file, test_config_file, setup_monday_db):
        """Test the complete Monday workflow that users will actually run."""
        print(f"\n🗓️  Monday Workflow Integration Test")
        
        # Step 1: Calculate Monday dates (automatic)
        snapshot_date, report_date = get_monday_dates(datetime(2024, 1, 17))  # Wednesday
        print(f"   Snapshot Date: {snapshot_date}")
        print(f"   Report Date: {report_date}")
        print(f"   Period: {format_business_period(snapshot_date, report_date)}")
        
        # Step 2: Check if this is a rerun (existing data)
        existing_bronze = check_existing_data(snapshot_date, 'bronze', test_config_file)
        existing_silver = check_existing_data(snapshot_date, 'silver', test_config_file)
        print(f"   Existing data - Bronze: {existing_bronze}, Silver: {existing_silver}")
        
        # Step 3: Extract data from ADP file
        print(f"   Reading ADP file...")
        raw_df = read_adp_file(real_adp_file)
        validate_adp_file_structure(raw_df)
        print(f"   ✅ Extracted {len(raw_df):,} records")
        
        # Step 4: Transform data
        print(f"   Transforming data...")
        cleaned_df = clean_adp_data(raw_df, snapshot_date=snapshot_date)
        validate_cleaned_data(cleaned_df)
        print(f"   ✅ Cleaned {len(cleaned_df):,} records")
        
        # Step 5: Load to bronze
        print(f"   Loading to bronze table...")
        bronze_records = load_to_bronze_table(cleaned_df, test_config_file)
        print(f"   ✅ Loaded {bronze_records:,} records to bronze")
        
        # Step 6: Calculate headcount metrics
        print(f"   Calculating headcount metrics...")
        silver_records = execute_headcount_calculation(snapshot_date, report_date, test_config_file)
        print(f"   ✅ Generated {silver_records} department headcount records")
        
        # Step 7: Validate results
        engine = setup_monday_db
        with engine.connect() as conn:
            from sqlalchemy import text
            # Check bronze data
            bronze_result = conn.execute(text("SELECT COUNT(*) FROM adp_tenure_history WHERE snapshot_date = :snapshot_date"), {"snapshot_date": snapshot_date})
            bronze_count = bronze_result.fetchone()[0]
            
            # Check silver data  
            silver_result = conn.execute(text("SELECT COUNT(*), SUM(active_count) FROM fact_active_headcount WHERE snapshot_date = :snapshot_date"), {"snapshot_date": snapshot_date})
            silver_count, total_active = silver_result.fetchone()
            
            print(f"   📊 Final Results:")
            print(f"      Bronze records: {bronze_count:,}")
            print(f"      Silver records: {silver_count}")
            print(f"      Total active employees: {total_active:,}")
        
        # Business rule validations
        assert bronze_count == len(cleaned_df), "All cleaned records should be in bronze"
        assert silver_count > 0, "Should generate headcount records"
        assert total_active > 0, "Should have active employees"

    def test_monday_workflow_rerun_scenario(self, test_config_file, setup_monday_db):
        """Test the rerun scenario - what happens when data already exists."""
        snapshot_date, report_date = get_monday_dates(datetime(2024, 1, 15))
        
        # Create sample data and load it first time
        sample_file = create_temporary_excel_file()
        try:
            # First run
            df = read_adp_file(sample_file)
            cleaned_df = clean_adp_data(df, snapshot_date=snapshot_date)
            load_to_bronze_table(cleaned_df, test_config_file)
            execute_headcount_calculation(snapshot_date, report_date, test_config_file)
            
            # Check that data exists
            bronze_count_1 = check_existing_data(snapshot_date, 'bronze', test_config_file)
            silver_count_1 = check_existing_data(snapshot_date, 'silver', test_config_file)
            
            print(f"   After first run - Bronze: {bronze_count_1}, Silver: {silver_count_1}")
            assert bronze_count_1 > 0, "Should have bronze data after first run"
            assert silver_count_1 > 0, "Should have silver data after first run"
            
            # Simulate rerun - delete existing data first
            print(f"   Simulating rerun scenario...")
            deleted_bronze = delete_existing_data(snapshot_date, 'bronze', test_config_file)
            deleted_silver = delete_existing_data(snapshot_date, 'silver', test_config_file)
            
            print(f"   Deleted - Bronze: {deleted_bronze}, Silver: {deleted_silver}")
            
            # Second run with same data
            load_to_bronze_table(cleaned_df, test_config_file)
            execute_headcount_calculation(snapshot_date, report_date, test_config_file)
            
            # Check final counts
            bronze_count_2 = check_existing_data(snapshot_date, 'bronze', test_config_file)
            silver_count_2 = check_existing_data(snapshot_date, 'silver', test_config_file)
            
            print(f"   After rerun - Bronze: {bronze_count_2}, Silver: {silver_count_2}")
            
            # Should have same counts as first run
            assert bronze_count_2 == bronze_count_1, "Rerun should produce same bronze count"
            assert silver_count_2 == silver_count_1, "Rerun should produce same silver count"
            
        finally:
            import os
            os.unlink(sample_file)

    def test_monday_workflow_business_rules(self, test_config_file, setup_monday_db):
        """Test that business rules are correctly applied in the Monday workflow."""
        snapshot_date, report_date = get_monday_dates(datetime(2024, 1, 15))
        
        # Create test data with specific business rule scenarios
        test_data = pd.DataFrame({
            'File Number': ['001', '002', '003', '004', '005'],
            'Payroll Name': ['Active Employee', 'Excluded Client', 'Inactive Employee', 'No Hire Date', 'Future Hire'],
            'Hire Date': ['2023-01-01', '2023-01-01', '2023-01-01', '', '2024-02-01'],  # Future hire
            'Position Status': ['Active', 'Active', 'Terminated', 'Active', 'Active'],
            'Clock Full Code': ['01200', '01100', '01200', '01200', '01200'],  # 01100 should be excluded
            'Home Department Code': ['123', '456', '789', '101', '112'],
            'Home Department Description': ['IT', 'HR', 'Engineering', 'Finance', 'Marketing'],
            'Associate ID': ['EMP001', 'EMP002', 'EMP003', 'EMP004', 'EMP005'],
            # Add other required columns with defaults
            'Clock Full Description': ['Main Office'] * 5,
            'Regular Pay Rate Amount': ['25.00'] * 5,
            'Business Unit Description': ['Corporate'] * 5,
            'Payroll Company Code': ['ABC'] * 5,
            'Position ID': [f'POS00{i}' for i in range(1, 6)],
            'Recruited by': ['HR'] * 5,
            'Requisition Key': [f'REQ00{i}' for i in range(1, 6)],
            'Personal Contact: Personal Email': [f'emp{i}@company.com' for i in range(1, 6)],
            'Requisition_id': [1000 + i for i in range(1, 6)],
            'applicant_id': [2000 + i for i in range(1, 6)],
            'Regular Hours Total': [160] * 5,
            'Overtime Hours Total': [0] * 5,
            'Other hours': [0] * 5,
            'Holiday': [8] * 5,
            'Voluntary/Involuntary Termination Flag': ['Voluntary'] * 5,
            'Personal Contact: Home Phone': [f'555-010{i}' for i in range(1, 6)]
        })
        
        # Save to temporary Excel file
        sample_file = create_temporary_excel_file(test_data)
        
        try:
            # Run the workflow
            df = read_adp_file(sample_file)
            cleaned_df = clean_adp_data(df, snapshot_date=snapshot_date)
            load_to_bronze_table(cleaned_df, test_config_file)
            silver_records = execute_headcount_calculation(snapshot_date, report_date, test_config_file)
            
            # Validate business rules were applied
            engine = setup_monday_db
            with engine.connect() as conn:
                from sqlalchemy import text
                # Should have all 5 records in bronze
                bronze_result = conn.execute(text("SELECT COUNT(*) FROM adp_tenure_history"))
                bronze_count = bronze_result.fetchone()[0]
                assert bronze_count == 5, f"Expected 5 bronze records, got {bronze_count}"
                
                # Silver should exclude:
                # - Excluded client code (01100) 
                # - Terminated employees
                # - Employees with no hire date or future hire dates
                silver_result = conn.execute(text("""
                    SELECT SUM(active_count), COUNT(*) 
                    FROM fact_active_headcount 
                    WHERE snapshot_date = :snapshot_date
                """), {"snapshot_date": snapshot_date})
                total_active, dept_count = silver_result.fetchone()
                
                print(f"   Business Rules Results:")
                print(f"      Total employees in bronze: {bronze_count}")
                print(f"      Active employees in silver: {total_active}")
                print(f"      Departments with active employees: {dept_count}")
                
                # Should only count:
                # - EMP001: Active, good client code, good hire date
                # Expected: 1 active employee in 1 department
                assert total_active == 1, f"Expected 1 active employee, got {total_active}"
                assert dept_count == 1, f"Expected 1 department, got {dept_count}"
                
        finally:
            import os
            os.unlink(sample_file)

    def test_monday_workflow_different_weeks(self, test_config_file, setup_monday_db):
        """Test workflow across different weeks to ensure data separation."""
        # Week 1
        week1_date = datetime(2024, 1, 15)  # Monday
        snapshot1, report1 = get_monday_dates(week1_date)
        
        # Week 2  
        week2_date = datetime(2024, 1, 22)  # Next Monday
        snapshot2, report2 = get_monday_dates(week2_date)
        
        print(f"   Week 1: {snapshot1} (report: {report1})")
        print(f"   Week 2: {snapshot2} (report: {report2})")
        
        # Create sample data for both weeks
        sample_file = create_temporary_excel_file()
        
        try:
            # Process Week 1
            df = read_adp_file(sample_file)
            cleaned_df1 = clean_adp_data(df, snapshot_date=snapshot1)
            load_to_bronze_table(cleaned_df1, test_config_file)
            execute_headcount_calculation(snapshot1, report1, test_config_file)
            
            # Process Week 2  
            cleaned_df2 = clean_adp_data(df, snapshot_date=snapshot2)
            load_to_bronze_table(cleaned_df2, test_config_file)
            execute_headcount_calculation(snapshot2, report2, test_config_file)
            
            # Validate data separation
            engine = setup_monday_db
            with engine.connect() as conn:
                from sqlalchemy import text
                # Check week 1 data
                week1_result = conn.execute(text("""
                    SELECT COUNT(*) FROM adp_tenure_history WHERE snapshot_date = :snapshot_date
                """), {"snapshot_date": snapshot1})
                week1_count = week1_result.fetchone()[0]
                
                # Check week 2 data
                week2_result = conn.execute(text("""
                    SELECT COUNT(*) FROM adp_tenure_history WHERE snapshot_date = :snapshot_date
                """), {"snapshot_date": snapshot2})
                week2_count = week2_result.fetchone()[0]
                
                print(f"   Week 1 records: {week1_count}")
                print(f"   Week 2 records: {week2_count}")
                
                assert week1_count > 0, "Week 1 should have data"
                assert week2_count > 0, "Week 2 should have data"
                assert week1_count == week2_count, "Both weeks should have same record count"
                
        finally:
            import os
            os.unlink(sample_file)

    def test_is_monday_helper(self):
        """Test the is_monday helper function."""
        # Test known dates
        assert is_monday(datetime(2024, 1, 15)) is True   # Monday
        assert is_monday(datetime(2024, 1, 16)) is False  # Tuesday
        assert is_monday(datetime(2024, 1, 17)) is False  # Wednesday
        assert is_monday(datetime(2024, 1, 21)) is False  # Sunday