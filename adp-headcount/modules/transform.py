import pandas as pd
from datetime import datetime
import logging
from typing import Optional


def clean_adp_data(df: pd.DataFrame, snapshot_date: Optional[str] = None) -> pd.DataFrame:
    """
    Clean and transform ADP tenure data.
    
    Args:
        df: Raw DataFrame from ADP file
        snapshot_date: Date string in YYYY-MM-DD format. If None, uses today's date.
        
    Returns:
        Cleaned DataFrame ready for database insertion
    """
    # Create a copy to avoid modifying the original
    cleaned_df = df.copy()
    
    # Strip whitespace from all string columns
    for col in cleaned_df.columns:
        if cleaned_df[col].dtype == 'object':
            cleaned_df[col] = cleaned_df[col].apply(
                lambda x: x.strip() if isinstance(x, str) else x
            )
    
    # Drop the duplicate column if it exists
    if "Previous Termination Date.1" in cleaned_df.columns:
        cleaned_df.drop(["Previous Termination Date.1"], axis=1, inplace=True)
    
    # Rename columns to match database schema
    column_mapping = {
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
        'Regular Pay Rate Amount': 'regular_pay_rate',
        'Recruited by': 'recruited_by',
        'Business Unit Description': 'business_unit',
        'Requisition Key': 'requisition_key',
        'Personal Contact: Personal Email': 'email',
        'Associate ID': 'adp_id',
        'Requisition_id': 'requisition_id',
        'applicant_id': 'applicant_id',
        'Regular Hours Total': 'regular_hours',
        'Overtime Hours Total': 'ot_hours',
        'Other hours': 'pto_sick_hours',
        'Holiday': 'holiday_hours',
        'Voluntary/Involuntary Termination Flag': 'voluntary_involuntary_flag',
        'Personal Contact: Home Phone': 'home_phone'
    }
    
    # Only rename columns that exist in the DataFrame
    existing_columns = {k: v for k, v in column_mapping.items() if k in cleaned_df.columns}
    cleaned_df.rename(columns=existing_columns, inplace=True)
    
    # Ensure home_department_code is 6 characters with leading zeros
    if 'home_department_code' in cleaned_df.columns:
        cleaned_df['home_department_code'] = cleaned_df['home_department_code'].astype(str).str.zfill(6)
    
    # Add snapshot_date
    if snapshot_date is None:
        snapshot_date = datetime.now().strftime('%Y-%m-%d')
    
    cleaned_df['snapshot_date'] = snapshot_date
    
    # Ensure data types match database schema
    date_columns = ['hire_date', 'rehire_date', 'previous_termination_date', 'termination_date', 'snapshot_date']
    for col in date_columns:
        if col in cleaned_df.columns:
            cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='coerce').dt.date
    
    # Convert integer columns
    integer_columns = ['requisition_id', 'applicant_id']
    for col in integer_columns:
        if col in cleaned_df.columns:
            cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce').astype('Int64')
    
    # Ensure home_department_code is exactly 6 characters (database constraint)
    if 'home_department_code' in cleaned_df.columns:
        cleaned_df['home_department_code'] = cleaned_df['home_department_code'].astype(str).str[:6].str.zfill(6)
    
    logging.info(f"Successfully cleaned {len(cleaned_df)} records with snapshot_date {snapshot_date}")
    
    return cleaned_df


def validate_cleaned_data(df: pd.DataFrame) -> bool:
    """
    Validate the cleaned data before database insertion.
    
    Args:
        df: Cleaned DataFrame
        
    Returns:
        True if validation passes
        
    Raises:
        ValueError: If validation fails
    """
    # Check for required columns
    required_columns = ['file_number', 'adp_id', 'snapshot_date']
    missing_required = [col for col in required_columns if col not in df.columns]
    
    if missing_required:
        raise ValueError(f"Missing required columns after cleaning: {missing_required}")
    
    # Check for null values in critical fields
    null_counts = df[required_columns].isnull().sum()
    if null_counts.any():
        logging.warning(f"Found null values in required columns: {null_counts[null_counts > 0].to_dict()}")
    
    # Validate home_department_code format
    if 'home_department_code' in df.columns:
        invalid_dept_codes = df[
            (df['home_department_code'].notna()) & 
            (df['home_department_code'].str.len() != 6)
        ]
        if len(invalid_dept_codes) > 0:
            raise ValueError(f"Found {len(invalid_dept_codes)} records with invalid department codes")
    
    logging.info("Data validation passed")
    return True