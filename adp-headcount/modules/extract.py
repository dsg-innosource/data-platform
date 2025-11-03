import pandas as pd
from pathlib import Path
from typing import Union
import logging


def read_adp_file(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    Read ADP tenure data from Excel file.
    
    Args:
        file_path: Path to the ADP Excel file
        
    Returns:
        Raw DataFrame from the Excel file
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If the file format is not supported
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"ADP file not found: {file_path}")
    
    if file_path.suffix.lower() not in ['.xls', '.xlsx']:
        raise ValueError(f"Unsupported file format: {file_path.suffix}. Expected .xls or .xlsx")
    
    try:
        # Read Excel file with specific data types to preserve leading zeros
        df = pd.read_excel(
            file_path,
            dtype={
                'File Number': str,
                'Clock Full Code': str,
                'Home Department Code': str
            }
        )
        
        logging.info(f"Successfully read {len(df)} records from {file_path}")
        return df
        
    except Exception as e:
        logging.error(f"Failed to read ADP file {file_path}: {str(e)}")
        raise


def validate_adp_file_structure(df: pd.DataFrame) -> bool:
    """
    Validate that the ADP file has expected columns.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        True if structure is valid
        
    Raises:
        ValueError: If required columns are missing
    """
    required_columns = [
        'File Number',
        'Payroll Name', 
        'Hire Date',
        'Position Status',
        'Home Department Code',
        'Clock Full Code',
        'Associate ID'
    ]
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    logging.info("ADP file structure validation passed")
    return True