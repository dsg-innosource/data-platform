# ADP Tenure Pipeline Integration

Please help me integrate my existing ADP data processing workflow into the data-platform repository structure. I need to create a complete ETL pipeline for ADP tenure data.

## Current Process to Automate
1. Download ADP report CSV (currently manual)
2. Clean the data using Python script (I have this code)
3. Upload cleaned data to `bronze.adp_tenure_history` table
4. Run SQL calculation to populate `silver.fact_active_headcount`

## Required Structure
Create the following structure in the `transformations/` folder:

```
transformations/
├── adp/
│   ├── __init__.py
│   ├── extract.py          # Handle ADP file input
│   ├── transform.py        # Data cleaning logic
│   ├── load.py            # Database loading functions
│   └── config.py          # Configuration settings
├── pipelines/
│   ├── __init__.py
│   └── adp_tenure_pipeline.py  # Main orchestration
└── sql/
    └── adp_headcount_calculation.sql
```

## My Existing Cleaning Code
Here's my current Python cleaning script that needs to be integrated:

```python
import pandas as pd
df = pd.read_excel("Foreflow Tenure_JP.xls",
    dtype={
        'File Number': str,
        'Clock Full Code': str,
        'Home Department Code': str
    }
)
df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))
df.drop(["Previous Termination Date.1"], axis=1, inplace=True)
df.rename(columns={
    'File Number': 'file_number',
    'Payroll Name': 'payroll_name',
    'Hire Date': 'hire_date',
    'Rehire Date': 'rehire_date',
    'Previous Termination Date': 'previous_termination_date',
    'Termination Date': 'termination_date',
    'Termination Reason Description': 'termination_reason',
    'Position Status': 'position_status',
    'Leave of Absence Start Date': 'leave_of_absence_start_date',
    'Leave of Absence Return Date': 'leave_of_absence_return_date',
    'Home Department Code': 'home_department_code',
    'Home Department Description': 'home_department_description',
    'Payroll Company Code': 'payroll_company_code',
    'Position ID': 'position_id',
    'Clock Full Code': 'client_code',
    'Clock Full Description': 'client',
    'Previous Termination Date': 'previous_termination_date',
    'Regular Pay Rate Amount': 'regular_pay_rate',
    'Recruited by': 'recruited_by',
    'Business Unit Description': 'business_unit',
    'Requisition Key': 'requisition_key',
    'Personal Contact: Personal Email': 'email',
    'Associate ID': 'adp_id',
    'Requisition_id': 'requisition_id',
    'applicant_id': 'applicant_id',
    'Regular Hours Total': 'regular_hours',
    'Overtime Hours Total': 'OT_hours',
    'Other hours': 'PTO_sick_hours',
    'Holiday': 'holiday_hours',
    'Voluntary/Involuntary Termination Flag':'voluntary_involuntary_flag',
    'Personal Contact: Home Phone': 'home_phone'
}, inplace=True)
df['home_department_code'] = df['home_department_code'].str.zfill(6)
df['snapshot_date'] = pd.Timestamp.today().strftime('%Y-%m-%d')
df.to_csv("cleaned_adp_tenure.csv", index=False)
```

## My SQL Query to Parameterize
This SQL needs to be converted to a template with parameterized dates:

```sql
-- DAILY CALCULATION
with daily_active as (select count(distinct adp_id) as active_count
                           ,home_department_code, home_department_description
                      from bronze.adp_tenure_history a
                      where a.position_status = 'Active'
                        and a.file_number is not null
                        and a.client_code != '01100'
                        and coalesce(a.rehire_date, a.hire_date) < a.snapshot_date
                        and a.snapshot_date = '2025-07-21'  -- PARAMETERIZE THIS
                      group by home_department_code, home_department_description
)
insert into silver.fact_active_headcount (department_number, snapshot_date, report_date, active_count, created_at)
select
        a.home_department_code
        ,'2025-07-21' as snapshot_date  -- PARAMETERIZE THIS
        ,'2025-07-07' as report_date    -- PARAMETERIZE THIS
        ,a.active_count
        ,now()
from daily_active a
where a.home_department_code is not null;
```

## Requirements
1. **Configuration management**: Database connections, file paths, etc.
2. **Error handling**: Proper logging and error management
3. **Parameterized execution**: Allow custom snapshot_date and report_date
4. **Database integration**: Functions to connect and execute SQL
5. **Main pipeline script**: Orchestrate the full ETL process
6. **Flexible file input**: Handle both manual file drops and automated processing

## Additional Setup
- Add necessary dependencies to the root `requirements.txt` (pandas, sqlalchemy, psycopg2, etc.)
- Create example configuration files
- Add logging setup
- Include basic usage documentation

## Usage Goals
After setup, I should be able to run:
```bash
# Process with default dates (today's snapshot, derive report_date)
python -m transformations.pipelines.adp_tenure_pipeline --file "Foreflow Tenure_JP.xls"

# Process with custom dates
python -m transformations.pipelines.adp_tenure_pipeline --file "data.xls" --snapshot-date "2025-07-21" --report-date "2025-07-07"
```

Please create all the necessary files and structure to make this work seamlessly within the data platform repository.